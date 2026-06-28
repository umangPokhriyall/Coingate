//! The five invariant oracles (Phase 2 §4.1), all black-box: direct Postgres (raw SQL) + the
//! `reconciler` binary + mock-mpc `/__counts`. Each returns a list of violation strings (empty =
//! pass), which the run record carries.
//!
//! 1. Conservation — per (merchant, token), Σ confirmed deposits == available + locked + Σ
//!    completed withdrawals (the same identity `store::reconcile` computes; asserted here
//!    independently and cross-checked via the reconciler in #5).
//! 2. At-most-once send — every completed withdrawal's mock-mpc `/__counts` is exactly 1.
//! 3. Replay-safety — exactly the expected number of withdrawal/order rows for a logical key.
//! 4. No-stranded-funds — no orphan lock, no unsent outbox row.
//! 5. Reconciler-clean — the `reconciler` binary exits 0 (`DriftReport::is_clean()`).

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};

use crate::fixtures::Db;
use crate::workload::Env;

#[derive(QueryableByName)]
struct MtAmount {
    #[diesel(sql_type = Text)]
    merchant: String,
    #[diesel(sql_type = Text)]
    token: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
}

#[derive(QueryableByName)]
struct MtStatusAmount {
    #[diesel(sql_type = Text)]
    merchant: String,
    #[diesel(sql_type = Text)]
    token: String,
    #[diesel(sql_type = BigInt)]
    amount: i64,
    #[diesel(sql_type = Text)]
    status: String,
}

#[derive(QueryableByName)]
struct Balance {
    #[diesel(sql_type = Text)]
    merchant: String,
    #[diesel(sql_type = Text)]
    token: String,
    #[diesel(sql_type = BigInt)]
    available: i64,
    #[diesel(sql_type = BigInt)]
    locked: i64,
}

#[derive(QueryableByName)]
struct IdText {
    #[diesel(sql_type = Text)]
    id: String,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

type Key = (String, String);

/// Σ confirmed deposits per (merchant, token), attributed deposit→order→app→merchant.
fn deposit_sums(db: &Db) -> Result<HashMap<Key, i64>> {
    let rows: Vec<MtAmount> = db.with_conn(|c| {
        sql_query(
            "SELECT a.merchant_id::text AS merchant, d.token_mint AS token, d.amount::bigint AS amount \
             FROM deposits d JOIN orders o ON d.order_id = o.id JOIN apps a ON o.app_id = a.id \
             WHERE d.status = 'confirmed' AND a.merchant_id IS NOT NULL AND d.token_mint IS NOT NULL",
        )
        .load(c)
    })?;
    let mut m = HashMap::new();
    for r in rows {
        *m.entry((r.merchant, r.token)).or_insert(0) += r.amount;
    }
    Ok(m)
}

/// Withdrawal rows as (merchant, token, amount, status).
fn withdrawals(db: &Db) -> Result<Vec<MtStatusAmount>> {
    db.with_conn(|c| {
        sql_query(
            "SELECT merchant_id::text AS merchant, token_mint AS token, amount::bigint AS amount, status \
             FROM withdrawals",
        )
        .load(c)
    })
}

fn balances(db: &Db) -> Result<Vec<Balance>> {
    db.with_conn(|c| {
        sql_query(
            "SELECT merchant_id::text AS merchant, token_mint AS token, \
             COALESCE(balance,0)::bigint AS available, COALESCE(locked_balance,0)::bigint AS locked \
             FROM balances",
        )
        .load(c)
    })
}

/// Invariant #1 — credit conservation.
pub fn conservation(db: &Db) -> Result<Vec<String>> {
    let deposits = deposit_sums(db)?;
    let wds = withdrawals(db)?;
    let bals = balances(db)?;

    let mut completed_out: HashMap<Key, i64> = HashMap::new();
    for w in &wds {
        if w.status == "completed" {
            *completed_out.entry((w.merchant.clone(), w.token.clone())).or_insert(0) += w.amount;
        }
    }
    let mut held: HashMap<Key, i64> = HashMap::new();
    for b in &bals {
        *held.entry((b.merchant.clone(), b.token.clone())).or_insert(0) += b.available + b.locked;
    }

    let mut keys: std::collections::HashSet<Key> = std::collections::HashSet::new();
    keys.extend(deposits.keys().cloned());
    keys.extend(held.keys().cloned());
    keys.extend(completed_out.keys().cloned());

    let mut violations = Vec::new();
    for k in keys {
        let dep = deposits.get(&k).copied().unwrap_or(0);
        let acc = held.get(&k).copied().unwrap_or(0) + completed_out.get(&k).copied().unwrap_or(0);
        if dep != acc {
            violations.push(format!(
                "conservation: merchant={} token={} deposits={} accounted={}",
                k.0, k.1, dep, acc
            ));
        }
    }
    Ok(violations)
}

/// Invariant #4 — no-stranded-funds: no orphan lock, no unsent outbox row.
pub fn no_stranded(db: &Db) -> Result<Vec<String>> {
    let wds = withdrawals(db)?;
    let bals = balances(db)?;
    let mut active_locked: HashMap<Key, i64> = HashMap::new();
    for w in &wds {
        if w.status == "pending" || w.status == "processing" {
            *active_locked.entry((w.merchant.clone(), w.token.clone())).or_insert(0) += w.amount;
        }
    }
    let mut violations = Vec::new();
    for b in &bals {
        let justified = active_locked.get(&(b.merchant.clone(), b.token.clone())).copied().unwrap_or(0);
        if b.locked > justified {
            violations.push(format!(
                "orphan-lock: merchant={} token={} locked={} justified={}",
                b.merchant, b.token, b.locked, justified
            ));
        }
    }
    let unsent: i64 = db.with_conn(|c| {
        let r: CountRow =
            sql_query("SELECT COUNT(*) AS n FROM outbox WHERE sent_at IS NULL").get_result(c)?;
        Ok(r.n)
    })?;
    if unsent != 0 {
        violations.push(format!("stranded-outbox: {unsent} unsent row(s)"));
    }
    Ok(violations)
}

/// Invariant #2 — at-most-once send: every completed withdrawal's mock-mpc call count is exactly 1,
/// and no key's count exceeds 1 (a blind re-send shows 2).
pub fn at_most_once(db: &Db, env: &Env) -> Result<Vec<String>> {
    let counts = mock_counts(env)?;
    let terminal: Vec<IdText> = db.with_conn(|c| {
        sql_query("SELECT id::text AS id FROM withdrawals WHERE status = 'completed'").load(c)
    })?;
    let mut violations = Vec::new();
    for w in &terminal {
        let n = counts.get(&w.id).copied().unwrap_or(0);
        if n != 1 {
            violations.push(format!("send-count: completed withdrawal {} has {n} sends (want 1)", w.id));
        }
    }
    for (key, n) in &counts {
        if *n > 1 {
            violations.push(format!("send-count: key {key} sent {n} times (>1)"));
        }
    }
    Ok(violations)
}

/// Invariant #3 — replay-safety: exactly `expected` withdrawal rows exist for `merchant` (a
/// duplicated/concurrent logical request must converge on one row + one debit).
pub fn replay_safety_withdrawals(db: &Db, merchant: uuid::Uuid, expected: usize) -> Result<Vec<String>> {
    let n: i64 = db.with_conn(|c| {
        let r: CountRow = sql_query("SELECT COUNT(*) AS n FROM withdrawals WHERE merchant_id = $1::uuid")
            .bind::<Text, _>(merchant.to_string())
            .get_result(c)?;
        Ok(r.n)
    })?;
    if n as usize != expected {
        return Ok(vec![format!("replay-safety: {n} withdrawal rows for merchant (want {expected})")]);
    }
    Ok(Vec::new())
}

/// Invariant #5 — reconciler-clean: shell out to the `reconciler` binary (exit 0 == clean).
pub fn reconciler_clean(reconciler_bin: &Path, cwd: &Path) -> Result<Vec<String>> {
    let status = Command::new(reconciler_bin)
        .current_dir(cwd)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .context("spawn reconciler")?;
    if status.success() {
        Ok(Vec::new())
    } else {
        Ok(vec![format!("reconciler: drift detected (exit {:?})", status.code())])
    }
}

/// GET mock-mpc `/__counts` → per-key send counts.
fn mock_counts(env: &Env) -> Result<HashMap<String, i64>> {
    mock_counts_at(&env.mpc_base_url)
}

/// GET `/__counts` from an arbitrary mock-mpc base URL (the legacy worker hardcodes a different
/// port, so Session 2.2 reads its counter from there).
pub fn mock_counts_at(base: &str) -> Result<HashMap<String, i64>> {
    let url = format!("{}/__counts", base.trim_end_matches('/'));
    reqwest::blocking::Client::new()
        .get(&url)
        .send()
        .context("GET /__counts")?
        .json()
        .context("parse /__counts")
}

/// Read `(available, locked)` base units for `(merchant, token)` (0,0 if no row). Used by the
/// Session 2.2 before-run to detect the legacy double-credit / double-lock directly.
pub fn balance_of(db: &Db, merchant: uuid::Uuid, token: &str) -> Result<(i64, i64)> {
    #[derive(QueryableByName)]
    struct B {
        #[diesel(sql_type = BigInt)]
        available: i64,
        #[diesel(sql_type = BigInt)]
        locked: i64,
    }
    let rows: Vec<B> = db.with_conn(|c| {
        sql_query(
            "SELECT COALESCE(balance,0)::bigint AS available, COALESCE(locked_balance,0)::bigint AS locked \
             FROM balances WHERE merchant_id = $1::uuid AND token_mint = $2",
        )
        .bind::<Text, _>(merchant.to_string())
        .bind::<Text, _>(token.to_string())
        .load(c)
    })?;
    Ok(rows.first().map(|b| (b.available, b.locked)).unwrap_or((0, 0)))
}

/// Count withdrawal rows for a merchant (replay-safety / double-lock signal).
pub fn withdrawal_count(db: &Db, merchant: uuid::Uuid) -> Result<i64> {
    let r: CountRow = db.with_conn(|c| {
        sql_query("SELECT COUNT(*) AS n FROM withdrawals WHERE merchant_id = $1::uuid")
            .bind::<Text, _>(merchant.to_string())
            .get_result(c)
    })?;
    Ok(r.n)
}

/// Count `pending` withdrawals for a merchant (stranded-funds signal when no stream entry exists).
pub fn pending_withdrawal_count(db: &Db, merchant: uuid::Uuid) -> Result<i64> {
    let r: CountRow = db.with_conn(|c| {
        sql_query("SELECT COUNT(*) AS n FROM withdrawals WHERE merchant_id = $1::uuid AND status = 'pending'")
            .bind::<Text, _>(merchant.to_string())
            .get_result(c)
    })?;
    Ok(r.n)
}

/// Run the four always-checkable oracles (#1, #2, #4, #5). Replay-safety (#3) is asserted by the
/// caller with its per-run expected counts.
pub fn check_core(db: &Db, env: &Env, reconciler_bin: &Path, cwd: &Path) -> Result<Vec<String>> {
    let mut v = Vec::new();
    v.extend(conservation(db)?);
    v.extend(at_most_once(db, env)?);
    v.extend(no_stranded(db)?);
    v.extend(reconciler_clean(reconciler_bin, cwd)?);
    Ok(v)
}
