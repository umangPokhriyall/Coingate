//! The supervisor: spawn a target binary as a child with a per-run env overlay, arm exactly one
//! crash point via `COINGATE_CHAOS_FIRE`, detect an armed abort (SIGABRT / non-zero exit),
//! kill, and restart. Pure process management — no DB/Redis/HTTP here. (Phase 2 §3.3.)

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use chaos_hooks::CrashPointId;

/// The env var the chaos scaffolding reads to arm exactly one crash point (chaos_hooks).
pub const ARM_ENV: &str = "COINGATE_CHAOS_FIRE";

/// How a child terminated, classified for the harness's purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// Exit code 0 — the disarmed / clean path.
    Clean,
    /// Killed by a signal. An armed `crash_point!` calls `std::process::abort()` → SIGABRT (6).
    /// This is the signature of a fired crash point.
    Aborted(i32),
    /// Non-zero exit code (a normal error, distinct from an armed abort).
    Failed(i32),
}

impl Exit {
    /// True iff the process died by signal — the shape an armed crash point produces.
    pub fn is_armed_abort(self) -> bool {
        matches!(self, Exit::Aborted(_))
    }
}

/// Every `CrashPointId` name, sourced from the registry (NOT hardcoded — Phase 2 §3.3). The
/// supervisor iterates this to arm each point by name.
pub fn all_crash_point_names() -> Vec<&'static str> {
    CrashPointId::ALL.iter().map(|id| id.name()).collect()
}

/// A target binary the supervisor can spawn (e.g. `target/debug/api`).
#[derive(Debug, Clone)]
pub struct Target {
    /// Human label for logs.
    pub name: String,
    /// Path to the compiled binary.
    pub bin: PathBuf,
    /// Working directory for the child (so it finds `.env`).
    pub cwd: PathBuf,
    /// Extra environment for the child (e.g. `MOCK_MPC_ADDR`).
    pub env: Vec<(String, String)>,
}

impl Target {
    pub fn new(name: impl Into<String>, bin: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            bin: bin.into(),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            env: Vec::new(),
        }
    }

    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = cwd.into();
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.env.push((key.into(), val.into()));
        self
    }
}

/// A spawned target under supervision.
pub struct Process {
    name: String,
    child: Child,
}

impl Process {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Non-blocking check: `Some(exit)` if the child has terminated, `None` if still running.
    pub fn try_exit(&mut self) -> io::Result<Option<Exit>> {
        Ok(self.child.try_wait()?.map(classify))
    }

    /// Block until the child exits and classify how.
    pub fn wait(&mut self) -> io::Result<Exit> {
        Ok(classify(self.child.wait()?))
    }

    /// Wait up to `timeout` for the child to exit on its own; `None` on timeout (still running).
    pub fn wait_timeout(&mut self, timeout: Duration) -> io::Result<Option<Exit>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(exit) = self.try_exit()? {
                return Ok(Some(exit));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Teardown: kill the child (idempotent if already dead) and reap it.
    pub fn kill(&mut self) -> io::Result<()> {
        let _ = self.child.kill();
        let _ = self.child.wait();
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // Never leak a child past the supervisor's lifetime.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn classify(status: std::process::ExitStatus) -> Exit {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return Exit::Aborted(sig);
        }
    }
    match status.code() {
        Some(0) => Exit::Clean,
        Some(code) => Exit::Failed(code),
        None => Exit::Aborted(0),
    }
}

/// Spawn `target` with an optional armed crash point. `armed = Some(name)` sets
/// `COINGATE_CHAOS_FIRE=<name>`; `None` explicitly REMOVES it so a disarmed run can never
/// inherit a stale arming from the parent.
pub fn spawn(target: &Target, armed: Option<&str>) -> io::Result<Process> {
    let mut cmd = Command::new(&target.bin);
    cmd.current_dir(&target.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (k, v) in &target.env {
        cmd.env(k, v);
    }
    match armed {
        Some(name) => {
            cmd.env(ARM_ENV, name);
        }
        None => {
            cmd.env_remove(ARM_ENV);
        }
    }
    let child = cmd.spawn()?;
    Ok(Process {
        name: target.name.clone(),
        child,
    })
}

/// Locate a sibling binary next to the currently running executable (both live in
/// `target/<profile>/`). Used to find `chaos_canary`, `mock-mpc`, `api`, etc. at runtime
/// without hardcoding the target dir.
pub fn sibling_bin(name: impl AsRef<OsStr>) -> io::Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let dir = exe
        .parent()
        .ok_or_else(|| io::Error::other("current_exe has no parent dir"))?;
    let candidate = dir.join(name.as_ref());
    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("binary not found: {} (build it first)", candidate.display()),
        ))
    }
}

/// Poll a TCP address until it accepts a connection or `timeout` elapses — used to confirm a
/// spawned server (api / mock-mpc) has bound its port before driving it.
pub fn wait_for_port(addr: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// True if `dir` looks like a Cargo `target/<profile>` directory (has the workspace binaries).
pub fn looks_like_target_dir(dir: &Path) -> bool {
    dir.join("harness").exists() || dir.join("chaos_canary").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classifier must distinguish an armed abort (death by signal) from a clean exit and
    /// from a plain non-zero failure. We use `/bin/sh` so the test needs no built binary or infra.
    #[cfg(unix)]
    #[test]
    fn classify_abort_vs_clean_vs_failure() {
        let sh = Target::new("sh-abort", "/bin/sh");

        // SIGABRT itself (signal 6) — exactly what `std::process::abort()` raises.
        let mut p = spawn_sh(&sh, "kill -ABRT $$");
        let exit = p.wait().expect("wait");
        assert_eq!(exit, Exit::Aborted(6));
        assert!(exit.is_armed_abort());

        // Clean exit.
        let mut p = spawn_sh(&sh, "exit 0");
        assert_eq!(p.wait().expect("wait"), Exit::Clean);

        // Plain non-zero failure is NOT an armed abort.
        let mut p = spawn_sh(&sh, "exit 3");
        let exit = p.wait().expect("wait");
        assert_eq!(exit, Exit::Failed(3));
        assert!(!exit.is_armed_abort());
    }

    fn spawn_sh(base: &Target, script: &str) -> Process {
        let mut cmd = Command::new(&base.bin);
        cmd.args(["-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        Process {
            name: base.name.clone(),
            child: cmd.spawn().expect("spawn sh"),
        }
    }

    #[test]
    fn registry_names_are_sourced_from_chaos_hooks() {
        let names = all_crash_point_names();
        // The Phase-1 registry closed at 15 (chaos_hooks). We read it, never hardcode it.
        assert_eq!(names.len(), CrashPointId::ALL.len());
        assert!(names.contains(&"SelfTest"));
    }
}
