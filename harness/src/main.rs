//! `harness` CLI. Session 2.0 ships one subcommand — `selftest` — which proves the substrate:
//! the truncate/flush fixture resets cleanly at READ COMMITTED, the supervisor arms `SelfTest`
//! on a throwaway and observes the abort + restart, `mock-mpc` answers `/send`/`/lookup`/
//! `/__counts`, and a real target (`api`) can be spawned and killed. (Phase 2 §3.3 "Done when".)
//!
//! Run from the repo root (so `.env` is found), after building the binaries:
//!   cargo build -p harness --features chaos -p mock-mpc -p api
//!   target/debug/harness selftest

use std::time::Duration;

use anyhow::{Context, Result, bail};
use harness::fixtures::{Db, Redis, quiescent};
use harness::supervisor::{Exit, Target, all_crash_point_names, sibling_bin, spawn, wait_for_port};
use tracing::info;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    dotenv::dotenv().ok();

    match std::env::args().nth(1).as_deref() {
        Some("selftest") | None => selftest(),
        Some(other) => bail!("unknown subcommand '{other}' (Session 2.0 ships only `selftest`)"),
    }
}

fn env_var(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing env var {key} (is .env present?)"))
}

/// Extract `host:port` from a base URL like `http://127.0.0.1:8090`.
fn authority(url: &str) -> String {
    url.trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or(url)
        .to_string()
}

fn selftest() -> Result<()> {
    let db_url = env_var("DATABASE_URL")?;
    let redis_url = env_var("REDIS_URL")?;
    let listen_addr = env_var("LISTEN_ADDR")?;
    let mpc_base = env_var("MPC_BASE_URL")?;
    let mpc_addr = authority(&mpc_base);

    let db = Db::connect(&db_url)?;
    let mut redis = Redis::connect(&redis_url)?;

    // ── Step 1: the fixtures reset cleanly, at READ COMMITTED ──────────────────────────────
    info!("step 1/5: fixture reset (truncate + flush + recreate groups)");
    db.truncate_all()?;
    redis.flush_and_recreate_groups()?;
    let nonempty = db.nonempty_tables()?;
    if !nonempty.is_empty() {
        bail!("fixture did not reset cleanly; non-empty tables: {nonempty:?}");
    }
    let isolation = db.isolation_level()?;
    if isolation != "read committed" {
        bail!("isolation level is '{isolation}', expected 'read committed' (Amendment §A4)");
    }
    if !quiescent(&mut redis, &db)? {
        bail!("system not quiescent after reset");
    }
    info!(isolation = %isolation, "  ✓ clean slate; quiescent; isolation recorded");

    // The registry the supervisor arms by name — sourced from chaos_hooks, never hardcoded.
    let names = all_crash_point_names();
    info!(count = names.len(), "  ✓ crash-point registry read from chaos_hooks");

    // ── Step 2: arm SelfTest on the throwaway → observe SIGABRT ─────────────────────────────
    info!("step 2/5: arm SelfTest on chaos_canary → expect abort");
    let canary = Target::new("chaos_canary", sibling_bin("chaos_canary")?);
    let mut armed = spawn(&canary, Some("SelfTest"))?;
    match armed.wait_timeout(Duration::from_secs(10))? {
        Some(Exit::Aborted(sig)) => info!(sig, "  ✓ canary aborted by signal (armed crash fired)"),
        Some(other) => bail!("armed canary did not abort: {other:?}"),
        None => bail!("armed canary did not exit within timeout"),
    }

    // ── Step 3: restart disarmed → observe a clean exit ────────────────────────────────────
    info!("step 3/5: restart chaos_canary disarmed → expect clean exit");
    let mut restarted = spawn(&canary, None)?;
    match restarted.wait_timeout(Duration::from_secs(10))? {
        Some(Exit::Clean) => info!("  ✓ canary restarted and exited cleanly"),
        Some(other) => bail!("disarmed canary did not exit cleanly: {other:?}"),
        None => bail!("disarmed canary did not exit within timeout"),
    }

    // ── Step 4: mock-mpc answers /send (counts+dedups), /lookup (free), /__counts ──────────
    info!("step 4/5: mock-mpc /send + /lookup + /__counts");
    let mock = Target::new("mock-mpc", sibling_bin("mock-mpc")?).with_env("MOCK_MPC_ADDR", &mpc_addr);
    let mut mock_proc = spawn(&mock, None)?;
    if !wait_for_port(&mpc_addr, Duration::from_secs(10)) {
        let _ = mock_proc.kill();
        bail!("mock-mpc never bound {mpc_addr}");
    }
    let mock_result = exercise_mock_mpc(&mpc_base);
    let _ = mock_proc.kill();
    mock_result?;
    info!("  ✓ mock-mpc counts+dedups /send, frees /lookup, reports /__counts");

    // ── Step 5: spawn a real target (api), confirm it binds, kill it ───────────────────────
    info!("step 5/5: spawn api → confirm bound → kill");
    let api = Target::new("api", sibling_bin("api")?);
    let mut api_proc = spawn(&api, None)?;
    if !wait_for_port(&listen_addr, Duration::from_secs(20)) {
        let _ = api_proc.kill();
        bail!("api never bound {listen_addr}");
    }
    info!(pid = api_proc.pid(), "  ✓ api bound; killing");
    api_proc.kill()?;

    info!("SUBSTRATE OK — supervisor, fixtures, mock-mpc, and abort/restart all verified");
    Ok(())
}

/// Drive the three mock-mpc endpoints and assert the counting/dedup contract Invariant #2 relies
/// on: two `/send` calls for one key → count 2, identical signature; `/lookup` does not count.
fn exercise_mock_mpc(base: &str) -> Result<()> {
    let http = reqwest::blocking::Client::new();
    let key = "11112222-3333-4444-5555-666677778888";
    let send_url = format!("{}/wallets/throwaway/send", base.trim_end_matches('/'));
    let body = serde_json::json!({ "to": "addr", "amount": 1, "idempotency_key": key });

    let sig1: serde_json::Value = http.post(&send_url).json(&body).send()?.json()?;
    let sig2: serde_json::Value = http.post(&send_url).json(&body).send()?.json()?;
    let s1 = sig1.get("signature").and_then(|v| v.as_str()).context("send 1 signature")?;
    let s2 = sig2.get("signature").and_then(|v| v.as_str()).context("send 2 signature")?;
    if s1 != s2 {
        bail!("/send did not dedup the signature: {s1} != {s2}");
    }

    // /lookup returns the prior signature WITHOUT incrementing the counter.
    let lookup_url = format!("{}/lookup?key={}", base.trim_end_matches('/'), key);
    let looked: serde_json::Value = http.get(&lookup_url).send()?.json()?;
    let ls = looked.get("signature").and_then(|v| v.as_str()).context("lookup signature")?;
    if ls != s1 {
        bail!("/lookup signature {ls} != /send signature {s1}");
    }

    // /__counts must show exactly the two /send calls (lookup did not count).
    let counts_url = format!("{}/__counts", base.trim_end_matches('/'));
    let counts: serde_json::Value = http.get(&counts_url).send()?.json()?;
    let n = counts.get(key).and_then(|v| v.as_u64()).context("count for key")?;
    if n != 2 {
        bail!("expected count 2 for key after 2 sends + 1 lookup, got {n}");
    }
    Ok(())
}
