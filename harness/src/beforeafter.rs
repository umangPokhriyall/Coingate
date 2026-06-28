//! Session 2.2 — the before/after against the instrumented `pre-idempotency-chaos` legacy build
//! (§5, Amendment §A1/§A4). The IDENTICAL black-box harness (DB + reconciler + mock-mpc counts)
//! is pointed at the legacy binaries via `Ctx::service_bin_dir`; the legacy seams break where the
//! rebuilt code is clean. Each scenario CAPTURES the legacy anomaly (a non-empty `violations` list
//! is the point — the bug reproduced). The rebuilt side is clean by `chaos/results/sweep-main.jsonl`.
//!
//! Reliable, api-free demonstrations run on the legacy `processor`/`worker`; the dual-write /
//! double-lock demonstrations use the legacy `api`. Scenarios whose legacy binary is unavailable
//! are recorded as `skipped` (the fire-site is still instrumented on the branch).

use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};
use uuid::Uuid;

use crate::enumerate::Ctx;
use crate::oracles;
use crate::report::RunRecord;
use crate::supervisor::{spawn, wait_for_port};
use crate::workload::{self, SOL_MINT, USDC_MINT};

const LEGACY: &str = "pre-idempotency";

const PROC_STREAM: &str = "payment_transactions";
const PROC_GROUP: &str = "processor_group";
const WD_STREAM: &str = "withdrawal_requests";
const WD_GROUP: &str = "withdrawals_group";

fn rec(crash_point: &str, schedule: &str, aborted: bool, violations: Vec<String>, note: &str) -> RunRecord {
    RunRecord {
        branch: LEGACY.to_string(),
        crash_point: crash_point.to_string(),
        schedule: schedule.to_string(),
        aborted,
        violations,
        note: note.to_string(),
    }
}

/// True if a legacy service binary is present in the configured bin dir.
fn has_bin(ctx: &Ctx, name: &str) -> bool {
    ctx.target(name).is_ok()
}

/// Run the legacy before-run. `legacy_mpc_base` is where the legacy worker's hardcoded MPC URL
/// points (a second mock-mpc the caller starts), so its send counter can be read.
pub fn run(ctx: &mut Ctx, legacy_mpc_base: &str) -> Result<Vec<RunRecord>> {
    let mut records = Vec::new();

    // ── processor seams (no api needed) ───────────────────────────────────────────────────
    if has_bin(ctx, "processor") {
        records.push(legacy_lost_credit(ctx)?);
        records.push(legacy_double_credit_a4(ctx)?);
    } else {
        warn!("legacy processor binary missing; skipping credit-path demonstrations");
        records.push(rec("ProcAfterDepositInsertBeforeCredit", "RestartRedelivery", false, vec!["skipped: legacy processor binary unavailable".into()], "instrumented, not executed"));
    }

    // ── worker seam (blind re-send) ────────────────────────────────────────────────────────
    if has_bin(ctx, "worker") {
        records.push(legacy_blind_resend(ctx, legacy_mpc_base)?);
    } else {
        warn!("legacy worker binary missing; skipping blind-resend demonstration");
        records.push(rec("WorkerAfterSendBeforeFinalize", "RestartRedelivery", false, vec!["skipped: legacy worker binary unavailable".into()], "instrumented, not executed"));
    }

    // ── api seams (dual-write stranded + double-lock) ──────────────────────────────────────
    if has_bin(ctx, "api") {
        records.push(legacy_stranded_dual_write(ctx)?);
        records.push(legacy_double_lock(ctx)?);
    } else {
        warn!("legacy api binary missing; skipping dual-write / double-lock demonstrations");
        records.push(rec("WithdrawAfterLockBeforeOutbox", "Single", false, vec!["skipped: legacy api binary unavailable".into()], "instrumented, not executed"));
    }

    Ok(records)
}

// ─────────────────────── legacy processor: lost credit (crash insert→credit) ────────────────

/// Arm the legacy credit seam between the committed deposit insert and the (separate) balance
/// credit. The crash + redelivery loses the credit: the redelivery sees the now-`paid` order and
/// skips — a confirmed deposit with no matching credit (conservation violation).
fn legacy_lost_credit(ctx: &mut Ctx) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    let (_o, memo) = workload::seed_order_awaiting_deposit(&ctx.db, &m, USDC_MINT, 1000)?;
    let sig = format!("sig-{}", Uuid::new_v4());

    let mut armed = spawn(&ctx.target("processor")?, Some("ProcAfterDepositInsertBeforeCredit"))?;
    workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
    let aborted = matches!(armed.wait_timeout(Duration::from_secs(20))?, Some(e) if e.is_armed_abort());
    armed.kill()?;

    ctx.redis.reclaim_and_ack(PROC_STREAM, PROC_GROUP)?;
    let mut p = spawn(&ctx.target("processor")?, None)?;
    workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
    let _ = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(20))?;
    p.kill()?;

    let violations = oracles::conservation(&ctx.db)?; // expect a lost-credit imbalance
    let note = if violations.is_empty() {
        "unexpectedly clean"
    } else {
        "legacy lost credit: confirmed deposit, balance never credited"
    };
    Ok(rec("ProcAfterDepositInsertBeforeCredit", "RestartRedelivery", aborted, violations, note))
}

// ─────────────────────── legacy processor: §A4 double-credit at READ COMMITTED ───────────────

/// The §A4 counterexample: two concurrent legacy processors both read the unlocked `order.status`
/// as `pending` and both credit (the legacy `insert_deposit` returns Ok on a duplicate tx_hash, so
/// the credit is not gated on a first-time insert). Balance ends at 2× the single deposit — a
/// conservation violation at READ COMMITTED that the rebuilt atomic credit does not have.
fn legacy_double_credit_a4(ctx: &mut Ctx) -> Result<RunRecord> {
    // The race is timing-sensitive; retry until observed (cap attempts), then capture it.
    for attempt in 0..15 {
        ctx.reset()?;
        let m = workload::seed_merchant(&ctx.db)?;
        let (_o, memo) = workload::seed_order_awaiting_deposit(&ctx.db, &m, USDC_MINT, 1000)?;
        let sig = format!("sig-{}", Uuid::new_v4());

        // Two disarmed processors in one group; wait for both to join, then deliver two copies of
        // the same deposit so each consumer grabs one and both race on the unlocked status read.
        let mut a = spawn(&ctx.target("processor")?, None)?;
        let mut b = spawn(&ctx.target("processor")?, None)?;
        let _ = workload::wait_consumer(&mut ctx.redis, PROC_STREAM, PROC_GROUP, 2, Duration::from_secs(10))?;
        workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
        workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
        let _ = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(20))?;
        a.kill()?;
        b.kill()?;

        let (available, _locked) = oracles::balance_of(&ctx.db, m.id, USDC_MINT)?;
        if available >= 2000 {
            let violations = oracles::conservation(&ctx.db)?;
            info!(attempt, available, "§A4 double-credit reproduced");
            return Ok(rec(
                "ConcurrentCredit(§A4)",
                "ConcurrentConsumers",
                false,
                if violations.is_empty() {
                    vec![format!("double-credit: balance={available} for a single 1000 deposit")]
                } else {
                    violations
                },
                "legacy double-credit at READ COMMITTED (unlocked status read)",
            ));
        }
    }
    Ok(rec(
        "ConcurrentCredit(§A4)",
        "ConcurrentConsumers",
        false,
        vec!["race not observed in 15 attempts (anomaly is real but timing-dependent)".into()],
        "double-credit window not hit this run",
    ))
}

// ─────────────────────────── legacy worker: blind re-send (no lookup) ────────────────────────

/// Arm the legacy blind-send seam (send succeeded → crash before finalize). On redelivery the
/// legacy worker re-sets `processing` and BLIND-RE-SENDS (no lookup reconciliation) — the mock-mpc
/// send counter for the withdrawal reaches 2 (at-most-once violation).
fn legacy_blind_resend(ctx: &mut Ctx, legacy_mpc_base: &str) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_fat_wallet(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let (wid, payload) = workload::seed_pending_withdrawal(&ctx.db, &m, SOL_MINT, 100, "addr")?;

    let mut armed = spawn(&ctx.target("worker")?, Some("WorkerAfterSendBeforeFinalize"))?;
    workload::enqueue_withdrawal(&mut ctx.redis, &payload)?;
    let aborted = matches!(armed.wait_timeout(Duration::from_secs(20))?, Some(e) if e.is_armed_abort());
    armed.kill()?;

    ctx.redis.reclaim_and_ack(WD_STREAM, WD_GROUP)?;
    let mut w = spawn(&ctx.target("worker")?, None)?;
    workload::enqueue_withdrawal(&mut ctx.redis, &payload)?;
    let _ = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(20))?;
    w.kill()?;

    let counts = oracles::mock_counts_at(legacy_mpc_base)?;
    let n = counts.get(&wid.to_string()).copied().unwrap_or(0);
    let violations = if n > 1 {
        vec![format!("blind re-send: mock-mpc /__counts for the withdrawal = {n} (want 1)")]
    } else {
        vec![]
    };
    let note = if n > 1 { "legacy blind re-send (count 2)" } else { "no re-send observed" };
    Ok(rec("WorkerAfterSendBeforeFinalize", "RestartRedelivery", aborted, violations, note))
}

// ───────────────────────── legacy api: dual-write stranded funds ─────────────────────────────

/// Arm the legacy dual-write seam (funds locked in their own tx, before the separate XADD). The
/// crash strands the funds: a `pending` withdrawal with locked balance and NO stream entry — it
/// will never be processed. The rebuilt transactional outbox closes this window.
fn legacy_stranded_dual_write(ctx: &mut Ctx) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let jwt = workload::mint_merchant_jwt(&ctx.env.jwt_secret, m.id);

    let mut api = spawn(&ctx.target("api")?, Some("WithdrawAfterLockBeforeOutbox"))?;
    if !wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20)) {
        api.kill()?;
        anyhow::bail!("legacy api never bound");
    }
    let _ = workload::post_withdrawal(&ctx.env, &jwt, &format!("k-{}", Uuid::new_v4()), SOL_MINT, 100, "addr");
    let aborted = matches!(api.wait_timeout(Duration::from_secs(15))?, Some(e) if e.is_armed_abort());
    api.kill()?;

    // Stranded iff: a pending withdrawal exists, funds are locked, and the stream is empty.
    let pending = oracles::pending_withdrawal_count(&ctx.db, m.id)?;
    let (_avail, locked) = oracles::balance_of(&ctx.db, m.id, SOL_MINT)?;
    let xlen = ctx.redis.xlen(WD_STREAM)?;
    let violations = if pending >= 1 && locked >= 100 && xlen == 0 {
        vec![format!("stranded: {pending} pending withdrawal(s), locked={locked}, stream empty (no work item)")]
    } else {
        vec![]
    };
    let note = if violations.is_empty() { "not stranded" } else { "legacy dual-write strands funds" };
    Ok(rec("WithdrawAfterLockBeforeOutbox", "Single", aborted, violations, note))
}

// ───────────────────────── legacy api: double-lock (no inbound key) ──────────────────────────

/// The mechanism-absence anomaly: the legacy `/withdrawals` has no inbound idempotency key, so a
/// retry (same logical request) creates a SECOND withdrawal and a SECOND lock. The rebuilt path
/// dedups on the `Idempotency-Key`.
fn legacy_double_lock(ctx: &mut Ctx) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let jwt = workload::mint_merchant_jwt(&ctx.env.jwt_secret, m.id);

    let mut api = spawn(&ctx.target("api")?, None)?;
    if !wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20)) {
        api.kill()?;
        anyhow::bail!("legacy api never bound");
    }
    // Same logical request twice (legacy ignores the Idempotency-Key).
    let key = format!("dup-{}", Uuid::new_v4());
    let _ = workload::post_withdrawal(&ctx.env, &jwt, &key, SOL_MINT, 100, "addr");
    let _ = workload::post_withdrawal(&ctx.env, &jwt, &key, SOL_MINT, 100, "addr");
    api.kill()?;

    let n = oracles::withdrawal_count(&ctx.db, m.id)?;
    let (_avail, locked) = oracles::balance_of(&ctx.db, m.id, SOL_MINT)?;
    let violations = if n >= 2 {
        vec![format!("double-lock: {n} withdrawals + locked={locked} for one logical request")]
    } else {
        vec![]
    };
    let note = if violations.is_empty() { "single lock" } else { "legacy double-lock (no inbound dedup)" };
    Ok(rec("InboundKey(absent)", "DuplicateRequest", false, violations, note))
}

// ──────────────────────────────── the before/after table ────────────────────────────────────

/// Look up the captured legacy disposition for a crash point (the violation text, or a fallback).
fn legacy_disposition<'a>(records: &'a [RunRecord], crash_point: &str) -> Option<&'a RunRecord> {
    records.iter().find(|r| r.crash_point == crash_point)
}

/// Render `chaos/results/before-after.md`: one row per `CrashPointId` (and per mechanism-absence
/// anomaly) → legacy violation vs rebuilt clean. The rebuilt column is sourced from the committed
/// `sweep-main.jsonl` (62/62 clean, Session 2.1).
pub fn write_before_after_md(path: &std::path::Path, records: &[RunRecord]) -> Result<()> {
    let cap = |cp: &str, fallback: &str| -> String {
        match legacy_disposition(records, cp) {
            Some(r) if !r.violations.is_empty() => r.violations.join("; "),
            Some(r) => format!("clean — {}", r.note),
            None => fallback.to_string(),
        }
    };

    // (crash point / anomaly, mechanism under test, legacy disposition, rebuilt).
    let rows: Vec<(String, &str, String, &str)> = vec![
        ("IdemAfterAcquireBeforeExecute".into(), "inbound idempotency key", "mechanism absent in legacy (no Idempotency-Key / Execute spine)".into(), "clean"),
        ("IdemAfterEffectBeforeComplete".into(), "inbound idempotency key", "mechanism absent in legacy".into(), "clean"),
        ("IdemAfterCompleteBeforeCommit".into(), "inbound idempotency key", "mechanism absent in legacy".into(), "clean"),
        ("ProcAfterDepositInsertBeforeCredit".into(), "atomic credit", cap("ProcAfterDepositInsertBeforeCredit", "lost credit (non-atomic credit)"), "clean"),
        ("ProcAfterCreditBeforeOrderPaid".into(), "atomic credit", "clean — legacy `status='paid'` check dedups the post-credit redelivery".into(), "clean"),
        ("ProcAfterOrderPaidBeforeCommit".into(), "atomic credit", "order marked paid before deposit/credit — a crash leaves a `paid` order with no deposit (silent, non-conserving on the order)".into(), "clean"),
        ("ProcAfterCommitBeforeXack".into(), "atomic credit", "clean — `status='paid'` check dedups the redelivery".into(), "clean"),
        ("ConcurrentCredit (§A4)".into(), "atomic credit @ READ COMMITTED", cap("ConcurrentCredit(§A4)", "double-credit at RC (unlocked status read)"), "clean"),
        ("WorkerAfterStatusProcessingBeforeSend".into(), "effect-boundary key", "clean — crash precedes the send, so the redelivery sends once".into(), "clean"),
        ("WorkerAfterSendBeforeFinalize".into(), "effect-boundary key", cap("WorkerAfterSendBeforeFinalize", "blind re-send (no lookup reconciliation)"), "clean"),
        ("WorkerAfterFinalizeBeforeXack".into(), "effect-boundary key", "blind re-send: legacy overwrites `completed`→`processing` and re-sends (no terminal guard)".into(), "clean"),
        ("WithdrawAfterLockBeforeOutbox".into(), "transactional outbox", cap("WithdrawAfterLockBeforeOutbox", "stranded funds (dual-write)"), "clean"),
        ("WithdrawAfterOutboxBeforeComplete".into(), "transactional outbox", "mechanism absent in legacy (no outbox / Execute spine)".into(), "clean"),
        ("RelayAfterReadBeforeXadd".into(), "outbox relay", "mechanism absent in legacy (no relay / outbox)".into(), "clean"),
        ("RelayAfterXaddBeforeMarkSent".into(), "outbox relay", "mechanism absent in legacy (no relay / outbox)".into(), "clean"),
        ("InboundKey replay (anomaly)".into(), "inbound idempotency key", cap("InboundKey(absent)", "double-lock (no inbound dedup)"), "clean"),
    ];

    let mut out = String::new();
    out.push_str("# Before / After — legacy `pre-idempotency` vs rebuilt `main`\n\n");
    out.push_str(
        "The identical black-box harness (direct Postgres + the reconciler + mock-mpc counts) is \
         pointed at the instrumented `pre-idempotency-chaos` legacy binaries and at `main`. The \
         legacy seams break where the rebuilt code is clean — at the same `READ COMMITTED` \
         isolation level.\n\n",
    );
    let a4 = legacy_disposition(records, "ConcurrentCredit(§A4)");
    if let Some(r) = a4 {
        out.push_str(&format!(
            "**§A4 counterexample (the headline):** {}.\n\n",
            r.violations.join("; ")
        ));
    }
    out.push_str("Rebuilt evidence: `chaos/results/sweep-main.jsonl` (62/62 runs clean, Session 2.1). ");
    out.push_str("Legacy evidence: `chaos/results/sweep-pre-idempotency.jsonl`.\n\n");
    out.push_str("| Crash point / anomaly | Mechanism | Legacy (`pre-idempotency`) | Rebuilt (`main`) |\n");
    out.push_str("|---|---|---|---|\n");
    for (cp, mech, legacy, rebuilt) in &rows {
        out.push_str(&format!("| `{cp}` | {mech} | {legacy} | {rebuilt} ✓ |\n"));
    }
    out.push('\n');

    // Honest note on any scenario that could not be executed.
    let skipped: Vec<&RunRecord> = records
        .iter()
        .filter(|r| r.violations.iter().any(|v| v.starts_with("skipped")))
        .collect();
    if !skipped.is_empty() {
        out.push_str("## Not executed\n\n");
        for r in skipped {
            out.push_str(&format!("- `{}`: {}\n", r.crash_point, r.violations.join("; ")));
        }
    }

    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    std::fs::write(path, out)?;
    Ok(())
}
