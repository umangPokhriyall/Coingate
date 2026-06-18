//! The reconciler — a second, independent oracle (Brief §3.8, Phase 1 §8).
//!
//! `reconcile` computes a [`DriftReport`] over the live tables: it does NOT trust the running
//! services, it recomputes the invariants from scratch. The headline is **credit conservation**:
//! per (merchant, token), the sum of distinct confirmed deposits must equal what the balance
//! state accounts for. By the balance bookkeeping identity
//!
//! ```text
//!   available + locked  ==  Σ(credits applied)  −  Σ(completed-withdrawal amounts)
//! ```
//!
//! the credits applied are `available + locked + Σ(completed withdrawals)`, and a correct system
//! has that equal to `Σ(confirmed deposits)`. A discrepancy is a missing/extra credit.
//!
//! Phase 1 ships the function as a development oracle (it also cross-checks 1.3–1.5 while they are
//! built). Phase 2 (Amendment §A5) elevates it to Invariant #5 — run after every fault schedule,
//! asserting zero drift — alongside the per-message conservation checker; agreement between the
//! two independent oracles is itself signal. This module writes nothing and opens no transaction.

use crate::schema;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// A per-(merchant, token) credit-conservation violation: the confirmed deposits do not match what
/// the balance state accounts for.
#[derive(Debug, Clone)]
pub struct Imbalance {
    pub merchant_id: Uuid,
    pub token_mint: String,
    /// Σ of distinct confirmed deposit amounts attributed to this (merchant, token).
    pub confirmed_deposits: BigDecimal,
    /// `available + locked + Σ(completed withdrawals)` — the credits the balance accounts for.
    pub accounted: BigDecimal,
}

/// The drift oracle's output. A clean system yields all-empty vectors.
#[derive(Debug, Clone, Default)]
pub struct DriftReport {
    /// Per (merchant, token) credit-conservation breaks (Σ credits vs Σ confirmed deposits).
    pub credit_conservation: Vec<Imbalance>,
    /// Withdrawals stuck in `processing` past the deadline (worker liveness).
    pub stuck_processing: Vec<Uuid>,
    /// (merchant, token) whose `locked_balance` exceeds what active (pending/processing)
    /// withdrawals justify — funds locked with nothing using them.
    pub orphan_locks: Vec<(Uuid, String)>,
    /// Outbox rows unsent past the threshold (relay liveness).
    pub unsent_aged_outbox: Vec<Uuid>,
}

impl DriftReport {
    /// No drift in any category.
    pub fn is_clean(&self) -> bool {
        self.credit_conservation.is_empty()
            && self.stuck_processing.is_empty()
            && self.orphan_locks.is_empty()
            && self.unsent_aged_outbox.is_empty()
    }
}

/// Age thresholds the reconciler treats as drift. `now - deadline` is the cutoff.
#[derive(Debug, Clone, Copy)]
pub struct Deadlines {
    /// A `processing` withdrawal older than this is stuck.
    pub stuck_processing: Duration,
    /// An unsent outbox row older than this is aged (relay not keeping up / dead).
    pub unsent_outbox: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            stuck_processing: Duration::minutes(5),
            unsent_outbox: Duration::minutes(1),
        }
    }
}

/// A withdrawal row as loaded for reconciliation: (id, merchant, token, amount, status,
/// updated_at, created_at).
type WithdrawalRow = (
    Uuid,
    Uuid,
    String,
    BigDecimal,
    String,
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
);

fn zero() -> BigDecimal {
    BigDecimal::from(0)
}

fn add_amount(map: &mut HashMap<(Uuid, String), BigDecimal>, key: (Uuid, String), amount: &BigDecimal) {
    let entry = map.entry(key).or_insert_with(zero);
    *entry = &*entry + amount;
}

/// Recompute the four drift categories from the live tables. Pure read; never writes.
pub fn reconcile(
    conn: &mut PgConnection,
    now: DateTime<Utc>,
    deadlines: Deadlines,
) -> Result<DriftReport, diesel::result::Error> {
    use schema::apps::dsl as a;
    use schema::balances::dsl as b;
    use schema::deposits::dsl as d;
    use schema::orders::dsl as o;
    use schema::withdrawals::dsl as w;

    // Resolve deposit -> merchant via order -> app (deposits carry no merchant_id directly).
    let apps: Vec<(Uuid, Option<Uuid>)> = a::apps.select((a::id, a::merchant_id)).load(conn)?;
    let app_merchant: HashMap<Uuid, Uuid> =
        apps.into_iter().filter_map(|(id, m)| m.map(|m| (id, m))).collect();

    let orders: Vec<(Uuid, Option<Uuid>)> = o::orders.select((o::id, o::app_id)).load(conn)?;
    let order_merchant: HashMap<Uuid, Uuid> = orders
        .into_iter()
        .filter_map(|(id, app)| app.and_then(|app| app_merchant.get(&app).copied()).map(|m| (id, m)))
        .collect();

    // Σ confirmed deposits per (merchant, token). tx_hash is UNIQUE, so each row is distinct.
    let deposit_rows: Vec<(Option<Uuid>, Option<String>, BigDecimal)> = d::deposits
        .filter(d::status.eq("confirmed"))
        .select((d::order_id, d::token_mint, d::amount))
        .load(conn)?;
    let mut deposit_sum: HashMap<(Uuid, String), BigDecimal> = HashMap::new();
    for (order_id, token, amount) in deposit_rows {
        // A confirmed deposit we cannot attribute to a merchant (no order/app) is skipped here; a
        // genuinely orphaned deposit would surface as a missing credit on the balance side.
        let (Some(oid), Some(token)) = (order_id, token) else { continue };
        let Some(&merchant) = order_merchant.get(&oid) else { continue };
        add_amount(&mut deposit_sum, (merchant, token), &amount);
    }

    // Withdrawals: completed-out sums, active locks (pending/processing), and stuck-processing.
    let withdrawal_rows: Vec<WithdrawalRow> = w::withdrawals
        .select((w::id, w::merchant_id, w::token_mint, w::amount, w::status, w::updated_at, w::created_at))
        .load(conn)?;

    let mut completed_out: HashMap<(Uuid, String), BigDecimal> = HashMap::new();
    let mut active_locked: HashMap<(Uuid, String), BigDecimal> = HashMap::new();
    let mut stuck_processing = Vec::new();
    let stuck_cutoff = (now - deadlines.stuck_processing).naive_utc();
    for (id, merchant, token, amount, status, updated_at, created_at) in withdrawal_rows {
        let key = (merchant, token);
        match status.as_str() {
            "completed" => add_amount(&mut completed_out, key.clone(), &amount),
            "pending" | "processing" => add_amount(&mut active_locked, key.clone(), &amount),
            _ => {}
        }
        if status == "processing"
            && let Some(ts) = updated_at.or(created_at)
            && ts < stuck_cutoff
        {
            stuck_processing.push(id);
        }
    }

    // Balances: held (available + locked) per (merchant, token); orphan-lock detection.
    let balance_rows: Vec<(Uuid, String, Option<BigDecimal>, Option<BigDecimal>)> = b::balances
        .select((b::merchant_id, b::token_mint, b::balance, b::locked_balance))
        .load(conn)?;
    let mut held: HashMap<(Uuid, String), BigDecimal> = HashMap::new();
    let mut orphan_locks = Vec::new();
    for (merchant, token, balance, locked) in balance_rows {
        let key = (merchant, token.clone());
        let avail = balance.unwrap_or_else(zero);
        let lck = locked.unwrap_or_else(zero);
        held.insert(key.clone(), &avail + &lck);

        // A lock is orphaned when it exceeds what active (pending/processing) withdrawals justify:
        // completed/failed withdrawals must have released their lock, so nothing should remain.
        let justified = active_locked.get(&key).cloned().unwrap_or_else(zero);
        if lck.cmp(&justified) == Ordering::Greater {
            orphan_locks.push((merchant, token));
        }
    }

    // Conservation: Σ deposits == held + Σ(completed withdrawals), per (merchant, token).
    let mut keys: HashSet<(Uuid, String)> = HashSet::new();
    keys.extend(deposit_sum.keys().cloned());
    keys.extend(held.keys().cloned());
    keys.extend(completed_out.keys().cloned());

    let mut credit_conservation = Vec::new();
    for key in keys {
        let deposits = deposit_sum.get(&key).cloned().unwrap_or_else(zero);
        let accounted =
            &held.get(&key).cloned().unwrap_or_else(zero) + &completed_out.get(&key).cloned().unwrap_or_else(zero);
        // Compare by numeric value (BigDecimal `cmp` ignores scale, unlike `==`).
        if deposits.cmp(&accounted) != Ordering::Equal {
            credit_conservation.push(Imbalance {
                merchant_id: key.0,
                token_mint: key.1,
                confirmed_deposits: deposits,
                accounted,
            });
        }
    }

    // Aged unsent outbox (relay liveness). Reuses the relay's drain query; filter by age.
    let outbox_cutoff = (now - deadlines.unsent_outbox).naive_utc();
    let unsent_aged_outbox = crate::models::api::select_unsent_outbox(conn)?
        .into_iter()
        .filter(|row| row.created_at < outbox_cutoff)
        .map(|row| row.id)
        .collect();

    Ok(DriftReport {
        credit_conservation,
        stuck_processing,
        orphan_locks,
        unsent_aged_outbox,
    })
}

#[cfg(test)]
mod tests {
    //! DB-backed seeded-state tests (gated on DATABASE_URL; skip cleanly when unset). Because
    //! `reconcile` scans the whole DB, assertions are scoped to a freshly-seeded merchant so the
    //! tests are robust to a shared/parallel test database.
    //!   DATABASE_URL=postgres:///coingate_rec_test?host=/var/run/postgresql cargo test -p store
    use super::*;
    use crate::module::{App, Balance, Deposit, Merchant, Order, Withdrawal};

    fn pool_or_skip() -> Option<crate::Pool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let manager = diesel::r2d2::ConnectionManager::<PgConnection>::new(url);
        diesel::r2d2::Pool::builder().max_size(2).build(manager).ok()
    }

    struct Seed {
        merchant: Uuid,
        app: Uuid,
        token: String,
    }

    /// Seed an isolated merchant + app and return their ids plus a unique token mint.
    fn seed_merchant(conn: &mut PgConnection) -> Seed {
        let merchant = crate::insert_merchant(
            conn,
            Merchant {
                id: Uuid::new_v4(),
                email: format!("m-{}@rec.local", Uuid::new_v4()),
                password_hash: "x".into(),
                name: "t".into(),
                created_at: None,
            },
        )
        .expect("merchant");
        let app = crate::insert_app(
            conn,
            App {
                id: Uuid::new_v4(),
                merchant_id: Some(merchant.id),
                title: "t".into(),
                callback_url: None,
                token_hash: format!("h-{}", Uuid::new_v4()),
                created_at: None,
            },
        )
        .expect("app");
        Seed { merchant: merchant.id, app: app.id, token: format!("MINT-{}", Uuid::new_v4()) }
    }

    /// A confirmed deposit of `amount`, attributed to `seed`'s merchant via a fresh order.
    fn seed_confirmed_deposit(conn: &mut PgConnection, seed: &Seed, amount: i64) {
        let order = crate::insert_order_on_conflict(
            conn,
            Order {
                id: Uuid::new_v4(),
                app_id: Some(seed.app),
                order_id: format!("o-{}", Uuid::new_v4()),
                price_amount: BigDecimal::from(amount),
                price_currency: "USD".into(),
                receive_currency: "USDC".into(),
                memo_id: Uuid::new_v4().to_string(),
                status: "pending".into(),
                tx_hash: None,
                selected_mint: None,
                expected_amount: None,
                expected_decimals: None,
                callback_url: None,
                success_url: None,
                cancel_url: None,
                created_at: None,
                confirmed_at: None,
            },
        )
        .expect("order");
        diesel::insert_into(schema::deposits::table)
            .values(Deposit {
                id: Uuid::new_v4(),
                order_id: Some(order.id),
                tx_hash: format!("sig-{}", Uuid::new_v4()),
                chain: "solana".into(),
                slot: None,
                block_hash: None,
                from_address: None,
                to_address: None,
                token_mint: Some(seed.token.clone()),
                token_symbol: None,
                token_decimals: None,
                amount: BigDecimal::from(amount),
                memo_id: None,
                status: "confirmed".into(),
                confirmations: None,
                raw: None,
                processed: Some(true),
                processing_attempts: None,
                created_at: None,
                updated_at: None,
                confirmed_at: None,
            })
            .execute(conn)
            .expect("deposit");
    }

    fn seed_balance(conn: &mut PgConnection, seed: &Seed, available: i64, locked: i64) {
        diesel::insert_into(schema::balances::table)
            .values(Balance {
                id: Uuid::new_v4(),
                merchant_id: seed.merchant,
                token_mint: seed.token.clone(),
                balance: Some(BigDecimal::from(available)),
                locked_balance: Some(BigDecimal::from(locked)),
                updated_at: Some(Utc::now().naive_utc()),
            })
            .execute(conn)
            .expect("balance");
    }

    fn seed_withdrawal(
        conn: &mut PgConnection,
        seed: &Seed,
        amount: i64,
        status: &str,
        updated_at: NaiveDateTime,
    ) -> Uuid {
        let id = Uuid::new_v4();
        diesel::insert_into(schema::withdrawals::table)
            .values(Withdrawal {
                id,
                merchant_id: seed.merchant,
                token_mint: seed.token.clone(),
                amount: BigDecimal::from(amount),
                status: status.into(),
                target_address: "addr".into(),
                tx_hash: None,
                created_at: Some(updated_at),
                updated_at: Some(updated_at),
            })
            .execute(conn)
            .expect("withdrawal");
        id
    }

    fn imbalances_for(r: &DriftReport, m: Uuid) -> Vec<&Imbalance> {
        r.credit_conservation.iter().filter(|i| i.merchant_id == m).collect()
    }
    fn orphans_for(r: &DriftReport, m: Uuid) -> Vec<&(Uuid, String)> {
        r.orphan_locks.iter().filter(|(mm, _)| *mm == m).collect()
    }

    #[test]
    fn clean_on_a_correctly_processed_dataset() {
        let Some(pool) = pool_or_skip() else {
            eprintln!("skipping reconcile clean test: DATABASE_URL unset/unreachable");
            return;
        };
        let mut conn = crate::get_conn(&pool).expect("conn");
        let now = Utc::now();
        let seed = seed_merchant(&mut conn);

        // 1000 credited. Then a completed withdrawal of 400 (funds left the system) and an active
        // (fresh processing) withdrawal of 200 (locked). Bookkeeping:
        //   available = 1000 - 400 - 200 = 400 ; locked = 200
        //   accounted = (400 + 200) + 400(completed) = 1000 == Σ deposits(1000)  -> conserved
        seed_confirmed_deposit(&mut conn, &seed, 1000);
        seed_balance(&mut conn, &seed, 400, 200);
        seed_withdrawal(&mut conn, &seed, 400, "completed", now.naive_utc());
        seed_withdrawal(&mut conn, &seed, 200, "processing", now.naive_utc()); // fresh -> not stuck

        let report = reconcile(&mut conn, now, Deadlines::default()).expect("reconcile");
        assert!(imbalances_for(&report, seed.merchant).is_empty(), "conservation holds for our merchant");
        assert!(orphans_for(&report, seed.merchant).is_empty(), "active lock is justified");
        assert!(!report.stuck_processing.contains(&seed.merchant), "no stuck withdrawal for us");
    }

    #[test]
    fn pinpoints_a_missing_credit() {
        let Some(pool) = pool_or_skip() else {
            return;
        };
        let mut conn = crate::get_conn(&pool).expect("conn");
        let now = Utc::now();
        let seed = seed_merchant(&mut conn);

        // A confirmed deposit of 1000, but the balance was never credited (no balance row).
        seed_confirmed_deposit(&mut conn, &seed, 1000);

        let report = reconcile(&mut conn, now, Deadlines::default()).expect("reconcile");
        let mine = imbalances_for(&report, seed.merchant);
        assert_eq!(mine.len(), 1, "exactly one imbalance for our merchant");
        assert_eq!(mine[0].token_mint, seed.token);
        assert_eq!(mine[0].confirmed_deposits, BigDecimal::from(1000));
        assert_eq!(mine[0].accounted, BigDecimal::from(0), "nothing was credited");
    }

    #[test]
    fn pinpoints_a_stuck_processing_withdrawal() {
        let Some(pool) = pool_or_skip() else {
            return;
        };
        let mut conn = crate::get_conn(&pool).expect("conn");
        let now = Utc::now();
        let seed = seed_merchant(&mut conn);

        // Conservation-neutral seed (deposit credited and fully locked), but the processing
        // withdrawal's clock is 10 minutes old -> past the 5-minute deadline.
        seed_confirmed_deposit(&mut conn, &seed, 500);
        seed_balance(&mut conn, &seed, 0, 500);
        let old = (now - Duration::minutes(10)).naive_utc();
        let stuck_id = seed_withdrawal(&mut conn, &seed, 500, "processing", old);

        let report = reconcile(&mut conn, now, Deadlines::default()).expect("reconcile");
        assert!(report.stuck_processing.contains(&stuck_id), "the aged processing withdrawal is flagged");
        assert!(imbalances_for(&report, seed.merchant).is_empty(), "the seed conserves (lock is active)");
        assert!(orphans_for(&report, seed.merchant).is_empty(), "the lock is justified by the active withdrawal");
    }

    #[test]
    fn pinpoints_an_orphan_lock() {
        let Some(pool) = pool_or_skip() else {
            return;
        };
        let mut conn = crate::get_conn(&pool).expect("conn");
        let now = Utc::now();
        let seed = seed_merchant(&mut conn);

        // 500 locked but NO active withdrawal explains it (conservation-neutral: a 500 deposit
        // backs the held funds, so only the orphan-lock check should fire).
        seed_confirmed_deposit(&mut conn, &seed, 500);
        seed_balance(&mut conn, &seed, 0, 500);

        let report = reconcile(&mut conn, now, Deadlines::default()).expect("reconcile");
        let mine = orphans_for(&report, seed.merchant);
        assert_eq!(mine.len(), 1, "the unexplained lock is flagged");
        assert_eq!(mine[0].1, seed.token);
        assert!(imbalances_for(&report, seed.merchant).is_empty(), "conservation still holds");
    }

    #[test]
    fn pinpoints_an_aged_unsent_outbox_row() {
        let Some(pool) = pool_or_skip() else {
            return;
        };
        let mut conn = crate::get_conn(&pool).expect("conn");
        let id = crate::insert_outbox(&mut conn, "withdrawal_requests", &serde_json::json!({ "x": 1 }))
            .expect("outbox")
            .id;

        // Evaluate from one hour in the future: the just-inserted row is older than the 1-minute
        // default outbox deadline.
        let future = Utc::now() + Duration::hours(1);
        let report = reconcile(&mut conn, future, Deadlines::default()).expect("reconcile");
        assert!(report.unsent_aged_outbox.contains(&id), "the aged unsent row is flagged");
    }
}
