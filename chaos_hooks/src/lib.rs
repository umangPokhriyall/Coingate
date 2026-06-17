//! Chaos fail-point scaffolding (Amendment 1 §A1).
//!
//! A `crash_point!(CrashPointId::X)` site models a real process death at a precise point in
//! a transaction. Without the `chaos` cargo feature the macro expands to **nothing** — zero
//! cost and zero risk in production/normal builds. With the feature, a single armed point
//! (selected via the `COINGATE_CHAOS_FIRE` env var) calls `std::process::abort()`.
//!
//! Phase 0 ships only the mechanism + a self-test. No fire-sites in real transaction code
//! yet — those land in Phase 1 as each transaction is written. See `README.md` for the
//! registry-closure rule.

/// Closed, enumerable registry of crash points. **APPEND-ONLY** across phases: variants are
/// added (never removed or renumbered) as fire-sites are placed. The Phase 2 harness iterates
/// [`CrashPointId::ALL`].
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum CrashPointId {
    /// Phase 0 canary only — proves the abort/supervisor model. Has a fire-site solely in the
    /// self-test, never in real code.
    SelfTest,
    // Phase 1 will append, e.g.:
    // ProcessorAfterDepositInsertBeforeCredit, ProcessorAfterCreditBeforeOrderPaid,
    // ProcessorAfterCommitBeforeXack, IdemAfterEffectBeforeComplete,
    // WorkerAfterSendBeforeXack, RelayAfterXaddBeforeMarkSent, ...
}

impl CrashPointId {
    /// Every crash point, for enumeration by the Phase 2 harness.
    pub const ALL: &'static [CrashPointId] = &[CrashPointId::SelfTest];

    /// Stable string name used to arm a point via `COINGATE_CHAOS_FIRE`. Must round-trip
    /// with [`CrashPointId::from_name`].
    pub fn name(self) -> &'static str {
        match self {
            CrashPointId::SelfTest => "SelfTest",
        }
    }

    /// Parse a crash point from its [`name`](CrashPointId::name).
    #[cfg(feature = "chaos")]
    pub fn from_name(s: &str) -> Option<CrashPointId> {
        CrashPointId::ALL.iter().copied().find(|id| id.name() == s)
    }
}

/// Place a crash point. Compiles to NOTHING without the `chaos` feature.
#[macro_export]
macro_rules! crash_point {
    ($id:expr) => {{
        #[cfg(feature = "chaos")]
        {
            $crate::__maybe_fire($id);
        }
    }};
}

/// On a match with the armed point, model a real process death. Only compiled with `chaos`.
#[cfg(feature = "chaos")]
pub fn __maybe_fire(id: CrashPointId) {
    if armed() == Some(id) {
        std::process::abort();
    }
}

/// The armed crash point, read once from `COINGATE_CHAOS_FIRE` into a process-wide static.
#[cfg(feature = "chaos")]
fn armed() -> Option<CrashPointId> {
    use std::sync::OnceLock;
    static ARMED: OnceLock<Option<CrashPointId>> = OnceLock::new();
    *ARMED.get_or_init(|| {
        std::env::var("COINGATE_CHAOS_FIRE")
            .ok()
            .and_then(|name| CrashPointId::from_name(name.trim()))
    })
}

#[cfg(all(test, not(feature = "chaos")))]
mod tests_without_chaos {
    use super::*;

    /// Without the feature, the macro expands to nothing — calling it just returns, and
    /// `CrashPointId::ALL` is still available for enumeration.
    #[test]
    fn crash_point_is_a_noop_without_the_feature() {
        crash_point!(CrashPointId::SelfTest);
        assert_eq!(CrashPointId::ALL.len(), 1);
        assert_eq!(CrashPointId::SelfTest.name(), "SelfTest");
    }
}

#[cfg(all(test, feature = "chaos"))]
mod tests_with_chaos {
    use super::*;

    /// Proves the abort/supervisor model the Phase 2 harness relies on: re-exec this test in
    /// a subprocess armed to fire `SelfTest`, hit a `crash_point!`, and assert the child
    /// aborts (SIGABRT) rather than exiting cleanly.
    #[test]
    fn self_test_models_a_process_abort() {
        // Child mode: fire the armed crash point. If it does NOT abort we exit 0, which makes
        // the parent's assertion fail loudly.
        if std::env::var("CHAOS_SELFTEST_CHILD").is_ok() {
            crash_point!(CrashPointId::SelfTest);
            std::process::exit(0);
        }

        // Parent mode: re-exec just this test in a child, armed for SelfTest.
        let exe = std::env::current_exe().expect("current_exe");
        let status = std::process::Command::new(exe)
            .args(["--exact", "--nocapture", "tests_with_chaos::self_test_models_a_process_abort"])
            .env("CHAOS_SELFTEST_CHILD", "1")
            .env("COINGATE_CHAOS_FIRE", "SelfTest")
            .status()
            .expect("spawn child process");

        assert!(
            !status.success(),
            "child should have aborted, but exited successfully: {status:?}"
        );

        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                status.signal(),
                Some(6),
                "expected SIGABRT (signal 6) from std::process::abort(), got {status:?}"
            );
        }
    }
}
