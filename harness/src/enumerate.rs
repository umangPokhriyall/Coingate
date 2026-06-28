//! The exhaustive enumeration (§4.3), the closure/reachability check (§4.3), the scripted §A2/§A3
//! schedules (§4.4/§4.5), and the seeded interleaving sweep (§4.6). One crash point is armed per
//! run on its owning service; the workload drives it to abort; the service restarts; the system
//! drains to quiescence; the five oracles are asserted. All at READ COMMITTED.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use diesel::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tracing::{info, warn};
use uuid::Uuid;

use crate::fixtures::{Db, Redis};
use crate::oracles;
use crate::report::{ClosureReport, RunRecord};
use crate::schedules::Schedule;
use crate::supervisor::{Process, Target, sibling_bin, spawn, wait_for_port};
use crate::workload::{self, Env, SOL_MINT, USDC_MINT};

const BRANCH: &str = "main";
/// Committed seeds for the interleaving sweep (§4.6) — fixed, so the sweep is reproducible.
pub const SEEDS: &[u64] = &[1, 2, 3];

const PROC_STREAM: &str = "payment_transactions";
const PROC_GROUP: &str = "processor_group";
const WD_STREAM: &str = "withdrawal_requests";
const WD_GROUP: &str = "withdrawals_group";

/// Which service owns a crash point (by name prefix).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Owner {
    Api,
    Processor,
    Worker,
    Relay,
}

fn owner_of(crash_point: &str) -> Owner {
    if crash_point.starts_with("Idem") || crash_point.starts_with("Withdraw") {
        Owner::Api
    } else if crash_point.starts_with("Proc") {
        Owner::Processor
    } else if crash_point.starts_with("Worker") {
        Owner::Worker
    } else if crash_point.starts_with("Relay") {
        Owner::Relay
    } else {
        Owner::Api // unreachable for the enumerated set (SelfTest is excluded)
    }
}

/// Long-lived harness context for the whole sweep.
pub struct Ctx {
    pub env: Env,
    pub db: Db,
    pub redis: Redis,
    pub cwd: PathBuf,
    pub reconciler_bin: PathBuf,
    /// When set (Session 2.2 before-run), the api/processor/worker/relay binaries are spawned
    /// from this dir (the instrumented legacy build) instead of next to the harness.
    pub service_bin_dir: Option<PathBuf>,
}

impl Ctx {
    pub fn new(env: Env) -> Result<Ctx> {
        let db = Db::connect(&env.db_url)?;
        let redis = Redis::connect(&env.redis_url)?;
        let cwd = std::env::current_dir()?;
        let reconciler_bin = sibling_bin("reconciler")?;
        Ok(Ctx { env, db, redis, cwd, reconciler_bin, service_bin_dir: None })
    }

    /// Resolve a service binary: from `service_bin_dir` if set (legacy), else next to the harness.
    pub fn target(&self, name: &str) -> Result<Target> {
        let bin = match &self.service_bin_dir {
            Some(dir) => {
                let p = dir.join(name);
                if !p.exists() {
                    bail!("service binary not found: {}", p.display());
                }
                p
            }
            None => sibling_bin(name)?,
        };
        Ok(Target::new(name, bin).with_cwd(&self.cwd))
    }

    /// Reset DB + Redis to a clean slate (mock-mpc counts persist but keys are unique per run).
    pub fn reset(&mut self) -> Result<()> {
        self.db.truncate_all()?;
        self.redis.flush_and_recreate_groups()?;
        Ok(())
    }

    pub fn core(&self, replay_merchant: Option<(Uuid, usize)>) -> Result<Vec<String>> {
        let mut v = oracles::check_core(&self.db, &self.env, &self.reconciler_bin, &self.cwd)?;
        if let Some((m, expected)) = replay_merchant {
            v.extend(oracles::replay_safety_withdrawals(&self.db, m, expected)?);
        }
        Ok(v)
    }
}

/// The full Session 2.1 run: exhaustive grid + closure + §A2 + §A3 + seeded sweep.
pub fn run_all(ctx: &mut Ctx, all_crash_points: &[&str]) -> Result<(Vec<RunRecord>, ClosureReport)> {
    let mut records = Vec::new();
    let mut aborted_points: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Exhaustive: every crash point (except SelfTest) × every schedule.
    let enumerated: Vec<&str> = all_crash_points.iter().copied().filter(|c| *c != "SelfTest").collect();
    for cp in &enumerated {
        for sched in Schedule::ALL {
            info!(crash_point = cp, schedule = sched.name(), "run");
            let rec = run_one(ctx, cp, *sched)?;
            if rec.aborted {
                aborted_points.insert(cp.to_string());
            }
            if !rec.passed() {
                warn!(crash_point = cp, schedule = sched.name(), violations = ?rec.violations, "FAIL");
            }
            records.push(rec);
        }
    }

    // Scripted §A2 (two seams) + §A3.
    records.push(run_a2(ctx, "IdemAfterEffectBeforeComplete")?);
    records.push(run_a2(ctx, "IdemAfterAcquireBeforeExecute")?);
    records.push(run_a3(ctx)?);

    // Seeded interleaving sweep.
    for &seed in SEEDS {
        records.push(run_seeded(ctx, seed)?);
    }

    let closure = ClosureReport {
        total: enumerated.len(),
        aborted: aborted_points.len(),
        unreached: enumerated
            .iter()
            .filter(|c| !aborted_points.contains(**c))
            .map(|c| c.to_string())
            .collect(),
    };
    Ok((records, closure))
}

/// A fast smoke check: one representative run per owner + §A2 + §A3 + one seed. Surfaces bugs
/// before the full 61-run sweep.
pub fn run_smoke(ctx: &mut Ctx) -> Result<Vec<RunRecord>> {
    Ok(vec![
        run_one(ctx, "IdemAfterEffectBeforeComplete", Schedule::Single)?,
        run_one(ctx, "ProcAfterDepositInsertBeforeCredit", Schedule::Single)?,
        run_one(ctx, "WorkerAfterSendBeforeFinalize", Schedule::Single)?,
        run_one(ctx, "RelayAfterXaddBeforeMarkSent", Schedule::Single)?,
        run_a2(ctx, "IdemAfterEffectBeforeComplete")?,
        run_a3(ctx)?,
        run_seeded(ctx, 1)?,
    ])
}

/// Dispatch one (crash_point × schedule) run to its owner-specific driver.
fn run_one(ctx: &mut Ctx, cp: &str, sched: Schedule) -> Result<RunRecord> {
    match owner_of(cp) {
        Owner::Api => run_api_crash(ctx, cp, sched),
        Owner::Processor => run_processor_crash(ctx, cp, sched),
        Owner::Worker => run_worker_crash(ctx, cp, sched),
        Owner::Relay => run_relay_crash(ctx, cp, sched),
    }
}

fn record(cp: &str, sched: &str, aborted: bool, violations: Vec<String>, note: &str) -> RunRecord {
    RunRecord {
        branch: BRANCH.to_string(),
        crash_point: cp.to_string(),
        schedule: sched.to_string(),
        aborted,
        violations,
        note: note.to_string(),
    }
}

// ───────────────────────────── api-owned (Idem* / Withdraw*) ────────────────────────────────

/// An api seam: a crash inside the Execute spine commits NOTHING (atomicity). Drive a withdrawal,
/// observe the abort, restart, and assert the system is unchanged (no withdrawal, funds intact).
fn run_api_crash(ctx: &mut Ctx, cp: &str, sched: Schedule) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let jwt = workload::mint_merchant_jwt(&ctx.env.jwt_secret, m.id);

    let mut api = spawn(&ctx.target("api")?, Some(cp))?;
    if !wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20)) {
        api.kill()?;
        bail!("api never bound");
    }

    // The first request that reaches the seam aborts the process; extra requests just reset.
    let posts = if sched.redeliveries() > 1 || sched.consumers() > 1 { 2 } else { 1 };
    for _ in 0..posts {
        let _ = workload::post_withdrawal(&ctx.env, &jwt, &format!("k-{}", Uuid::new_v4()), SOL_MINT, 100, "addr");
    }

    let aborted = matches!(api.wait_timeout(Duration::from_secs(15))?, Some(e) if e.is_armed_abort());
    api.kill()?;

    // Restart disarmed (recovery) and confirm it binds.
    let mut api2 = spawn(&ctx.target("api")?, None)?;
    let rebound = wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20));
    api2.kill()?;
    if !rebound {
        bail!("api did not recover");
    }

    let mut violations = ctx.core(Some((m.id, 0)))?; // a crashed Execute commits no withdrawal
    if !aborted {
        violations.push(format!("closure: {cp} did not abort under its workload"));
    }
    Ok(record(cp, sched.name(), aborted, violations, "api seam: crash commits nothing"))
}

// ─────────────────────────────────── processor-owned (Proc*) ────────────────────────────────

fn run_processor_crash(ctx: &mut Ctx, cp: &str, sched: Schedule) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    let (_order, memo) = workload::seed_order_awaiting_deposit(&ctx.db, &m, USDC_MINT, 1000)?;
    let sig = format!("sig-{}", Uuid::new_v4());

    // Phase 1: armed processor only, so it is guaranteed to take the entry and abort.
    let mut armed = spawn(&ctx.target("processor")?, Some(cp))?;
    workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
    if sched == Schedule::DuplicateStream {
        workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
    }
    let aborted = matches!(armed.wait_timeout(Duration::from_secs(20))?, Some(e) if e.is_armed_abort());
    armed.kill()?;

    // Phase 2: clear the crashed PEL, restart disarmed (+ a concurrent consumer if scheduled),
    // redeliver the same signature (dedup must absorb), and drain.
    ctx.redis.reclaim_and_ack(PROC_STREAM, PROC_GROUP)?;
    let mut consumers: Vec<Process> = vec![spawn(&ctx.target("processor")?, None)?];
    for _ in 1..sched.consumers() {
        consumers.push(spawn(&ctx.target("processor")?, None)?);
    }
    for _ in 0..sched.redeliveries() {
        workload::enqueue_deposit(&mut ctx.redis, &memo, &sig, USDC_MINT, 1000)?;
    }
    let drained = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(30))?;
    for c in &mut consumers {
        c.kill()?;
    }

    let mut violations = ctx.core(None)?;
    // Deposit credited exactly once: one confirmed deposit row for the signature.
    let dep_count = ctx.db.with_conn(|c| {
        use diesel::sql_types::BigInt;
        #[derive(diesel::QueryableByName)]
        struct N {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        let r: N = diesel::sql_query("SELECT COUNT(*) AS n FROM deposits WHERE status='confirmed'")
            .get_result(c)?;
        Ok(r.n)
    })?;
    if dep_count != 1 {
        violations.push(format!("dedup: {dep_count} confirmed deposits (want 1)"));
    }
    if !drained {
        violations.push("drain: not quiescent within timeout".into());
    }
    if !aborted {
        violations.push(format!("closure: {cp} did not abort under its workload"));
    }
    Ok(record(cp, sched.name(), aborted, violations, "deposit dedup absorbs redelivery"))
}

// ──────────────────────────────────── worker-owned (Worker*) ────────────────────────────────

fn run_worker_crash(ctx: &mut Ctx, cp: &str, sched: Schedule) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_fat_wallet(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let (_wid, payload) = workload::seed_pending_withdrawal(&ctx.db, &m, SOL_MINT, 100, "addr")?;

    let mut armed = spawn(&ctx.target("worker")?, Some(cp))?;
    workload::enqueue_withdrawal(&mut ctx.redis, &payload)?;
    if sched == Schedule::DuplicateStream {
        workload::enqueue_withdrawal(&mut ctx.redis, &payload)?;
    }
    let aborted = matches!(armed.wait_timeout(Duration::from_secs(20))?, Some(e) if e.is_armed_abort());
    armed.kill()?;

    ctx.redis.reclaim_and_ack(WD_STREAM, WD_GROUP)?;
    let mut consumers: Vec<Process> = vec![spawn(&ctx.target("worker")?, None)?];
    for _ in 1..sched.consumers() {
        consumers.push(spawn(&ctx.target("worker")?, None)?);
    }
    for _ in 0..sched.redeliveries() {
        workload::enqueue_withdrawal(&mut ctx.redis, &payload)?;
    }
    let drained = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(30))?;
    for c in &mut consumers {
        c.kill()?;
    }

    let mut violations = ctx.core(Some((m.id, 1)))?;
    if !drained {
        violations.push("drain: not quiescent within timeout".into());
    }
    if !aborted {
        violations.push(format!("closure: {cp} did not abort under its workload"));
    }
    Ok(record(cp, sched.name(), aborted, violations, "worker reconciles via lookup; one send"))
}

// ───────────────────────────────────── relay-owned (Relay*) ─────────────────────────────────

fn run_relay_crash(ctx: &mut Ctx, cp: &str, sched: Schedule) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_fat_wallet(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let (_wid, payload) = workload::seed_pending_withdrawal(&ctx.db, &m, SOL_MINT, 100, "addr")?;
    workload::seed_outbox(&ctx.db, &payload)?;

    // Worker(s) downstream absorb whatever the relay (re)publishes.
    let mut workers: Vec<Process> = Vec::new();
    for _ in 0..sched.consumers() {
        workers.push(spawn(&ctx.target("worker")?, None)?);
    }
    let mut relay = spawn(&ctx.target("relay")?, Some(cp))?;
    let aborted = matches!(relay.wait_timeout(Duration::from_secs(20))?, Some(e) if e.is_armed_abort());
    relay.kill()?;

    // Restart relay disarmed: it re-reads the unsent outbox row and (re)publishes; the worker
    // absorbs the duplicate (pending->processing guard + lookup) — one send.
    let mut relay2 = spawn(&ctx.target("relay")?, None)?;
    let drained = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(30))?;
    relay2.kill()?;
    for w in &mut workers {
        w.kill()?;
    }

    let mut violations = ctx.core(Some((m.id, 1)))?;
    if !drained {
        violations.push("drain: not quiescent within timeout".into());
    }
    if !aborted {
        violations.push(format!("closure: {cp} did not abort under its workload"));
    }
    Ok(record(cp, sched.name(), aborted, violations, "relay republish absorbed by consumer dedup"))
}

// ─────────────────────────────── scripted §A2 in-progress-key ───────────────────────────────

/// §A2: crash mid-Execute leaves the key `in_progress`; replay-before-expiry → 409, replay-after-
/// expiry → takeover re-executes exactly once.
fn run_a2(ctx: &mut Ctx, cp: &str) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_fat_wallet(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?;
    let jwt = workload::mint_merchant_jwt(&ctx.env.jwt_secret, m.id);
    let key = format!("a2-{}", Uuid::new_v4());

    let mut violations = Vec::new();

    // Drive the crash mid-Execute.
    let mut api = spawn(&ctx.target("api")?, Some(cp))?;
    if !wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20)) {
        api.kill()?;
        bail!("api never bound");
    }
    let _ = workload::post_withdrawal(&ctx.env, &jwt, &key, SOL_MINT, 100, "addr");
    let aborted = matches!(api.wait_timeout(Duration::from_secs(15))?, Some(e) if e.is_armed_abort());
    api.kill()?;

    // Restart disarmed; relay + worker drain the eventual withdrawal.
    let mut api2 = spawn(&ctx.target("api")?, None)?;
    if !wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20)) {
        bail!("api did not recover");
    }
    let mut relay = spawn(&ctx.target("relay")?, None)?;
    let mut worker = spawn(&ctx.target("worker")?, None)?;

    // (a) Replay BEFORE lease expiry → 409 (in_progress + valid lease).
    match workload::post_withdrawal(&ctx.env, &jwt, &key, SOL_MINT, 100, "addr") {
        workload::PostOutcome::Status(409, _) => {}
        other => violations.push(format!("A2(a): expected 409 before expiry, got {other:?}")),
    }

    // (b) Expire the lease, replay → takeover re-executes exactly once (200 + a withdrawal).
    workload::expire_idem_lease(&ctx.db, &key)?;
    match workload::post_withdrawal(&ctx.env, &jwt, &key, SOL_MINT, 100, "addr") {
        workload::PostOutcome::Status(200, _) => {}
        other => violations.push(format!("A2(b): expected 200 after expiry, got {other:?}")),
    }

    let drained = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(30))?;
    api2.kill()?;
    relay.kill()?;
    worker.kill()?;

    violations.extend(ctx.core(Some((m.id, 1)))?); // exactly one withdrawal
    if !drained {
        violations.push("drain: not quiescent within timeout".into());
    }
    if !aborted {
        violations.push(format!("closure: {cp} did not abort"));
    }
    Ok(record(cp, "A2-takeover", aborted, violations, "409 before expiry; exactly-once after"))
}

// ─────────────────────────────── scripted §A3 relay republish ───────────────────────────────

/// §A3: arm the relay's XADD→mark-sent seam; the republished entry must be absorbed by the
/// consumer (one send, conservation clean).
fn run_a3(ctx: &mut Ctx) -> Result<RunRecord> {
    let mut rec = run_relay_crash(ctx, "RelayAfterXaddBeforeMarkSent", Schedule::Single)?;
    rec.schedule = "A3-relay-republish".to_string();
    rec.note = "relay republishes; worker absorbs duplicate (one send)".to_string();
    Ok(rec)
}

// ─────────────────────────────── seeded interleaving sweep ──────────────────────────────────

/// §4.6: drive concurrent mixed workloads (deposits with a duplicate + concurrent withdrawals)
/// through the running services with fixed-seed randomized delays; assert the five oracles.
fn run_seeded(ctx: &mut Ctx, seed: u64) -> Result<RunRecord> {
    ctx.reset()?;
    let m = workload::seed_merchant(&ctx.db)?;
    workload::seed_fat_wallet(&ctx.db)?;
    workload::seed_funded_balance(&ctx.db, &m, SOL_MINT, 1000)?; // funds for withdrawals
    let jwt = workload::mint_merchant_jwt(&ctx.env.jwt_secret, m.id);

    // Pre-create the deposit orders (USDC token, separate from the SOL withdrawal funds).
    let dep_amounts = [500i64, 300, 200];
    let mut memos = Vec::new();
    for amt in dep_amounts {
        let (_o, memo) = workload::seed_order_awaiting_deposit(&ctx.db, &m, USDC_MINT, amt)?;
        memos.push((memo, amt, format!("sig-{}", Uuid::new_v4())));
    }

    // All services up, disarmed.
    let mut api = spawn(&ctx.target("api")?, None)?;
    if !wait_for_port(&ctx.env.listen_addr, Duration::from_secs(20)) {
        bail!("api never bound");
    }
    let mut processor = spawn(&ctx.target("processor")?, None)?;
    let mut worker = spawn(&ctx.target("worker")?, None)?;
    let mut relay = spawn(&ctx.target("relay")?, None)?;

    let mut rng = StdRng::seed_from_u64(seed);
    let sleep = |rng: &mut StdRng| std::thread::sleep(Duration::from_millis(rng.gen_range(0..40)));

    // Deposits (with one duplicate of the first, to exercise dedup under interleaving).
    for (memo, amt, sig) in &memos {
        workload::enqueue_deposit(&mut ctx.redis, memo, sig, USDC_MINT, *amt)?;
        sleep(&mut rng);
    }
    let (memo0, amt0, sig0) = &memos[0];
    workload::enqueue_deposit(&mut ctx.redis, memo0, sig0, USDC_MINT, *amt0)?; // duplicate

    // Concurrent withdrawals (distinct keys), small randomized delays.
    let keys: Vec<String> = (0..2).map(|_| format!("seed{seed}-{}", Uuid::new_v4())).collect();
    std::thread::scope(|s| {
        for key in &keys {
            let env = ctx.env.clone();
            let jwt = jwt.clone();
            let delay = rng.gen_range(0..40);
            s.spawn(move || {
                std::thread::sleep(Duration::from_millis(delay));
                let _ = workload::post_withdrawal(&env, &jwt, key, SOL_MINT, 100, "addr");
            });
        }
    });

    let drained = workload::drain_to_quiescence(&mut ctx.redis, &ctx.db, Duration::from_secs(40))?;
    api.kill()?;
    processor.kill()?;
    worker.kill()?;
    relay.kill()?;

    let mut violations = ctx.core(Some((m.id, 2)))?; // exactly two withdrawals
    if !drained {
        violations.push("drain: not quiescent within timeout".into());
    }
    Ok(record("(none)", &format!("seed-{seed}"), false, violations, "concurrent mixed workload"))
}
