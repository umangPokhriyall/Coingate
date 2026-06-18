use crate::module::*;
use crate::schema::*;
use bigdecimal::BigDecimal;
use chrono::Utc;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error;
use uuid::Uuid;

// NOTE: Every function takes `conn: &mut PgConnection`. Single-statement work is called
// on a pooled connection (`store::get_conn`); the multi-statement fns that lock rows
// (`create_withdrawal_and_lock`, `revert_withdrawal_lock`, `finalize_*`) MUST be invoked
// through `store::with_tx`, which is the workspace's only transaction constructor. Their
// bodies were left intact apart from removing the now-redundant inner `conn.transaction`
// wrapper (Phase 0 hard rule #2). No idempotency/credit/send logic changed.

// ============ Merchants ============
pub fn insert_merchant(conn: &mut PgConnection, merchant: Merchant) -> Result<Merchant, Error> {
    diesel::insert_into(merchants::table)
        .values(&merchant)
        .returning(Merchant::as_returning())
        .get_result(conn)
}

pub fn find_merchant_by_email(conn: &mut PgConnection, email_: &str) -> Result<Merchant, Error> {
    use crate::schema::merchants::dsl::*;
    merchants
        .filter(email.eq(email_))
        .select(Merchant::as_select())
        .first(conn)
}

pub fn find_merchant_by_id(conn: &mut PgConnection, mid: Uuid) -> Result<Merchant, Error> {
    use crate::schema::merchants::dsl::*;
    merchants
        .filter(id.eq(mid))
        .select(Merchant::as_select())
        .first(conn)
}

pub fn get_merchant_id_for_app(conn: &mut PgConnection, app_uuid: Uuid) -> Result<Uuid, Error> {
    use crate::schema::apps::dsl::*;
    let app: App = apps
        .filter(id.eq(app_uuid))
        .select(App::as_select())
        .first(conn)?;
    Ok(app.merchant_id.expect("App must always have a merchant_id"))
}

// ============ Apps ============
pub fn insert_app(conn: &mut PgConnection, app: App) -> Result<App, Error> {
    diesel::insert_into(apps::table)
        .values(&app)
        .returning(App::as_returning())
        .get_result(conn)
}

pub fn list_apps_by_merchant(conn: &mut PgConnection, mid: Uuid) -> Result<Vec<App>, Error> {
    use crate::schema::apps::dsl::*;
    apps.filter(merchant_id.eq(mid))
        .select(App::as_select())
        .load(conn)
}

/// App tokens are formatted `"<app_id>.<secret>"`. The `app_id` is a non-secret, already
/// indexed (primary key) lookup id, so we fetch exactly one candidate row and bcrypt-verify
/// only that one — replacing the previous full-table-scan-then-bcrypt-every-row. (Bounded
/// auth fix only; not an auth redesign.)
pub fn find_app_by_token(conn: &mut PgConnection, token: &str) -> Result<App, diesel::result::Error> {
    use crate::schema::apps::dsl::*;
    use bcrypt::verify;

    // Parse the indexed lookup id out of the token; a malformed token is simply "not found".
    let lookup_id = token
        .split_once('.')
        .and_then(|(id_part, _)| Uuid::parse_str(id_part).ok())
        .ok_or(diesel::result::Error::NotFound)?;

    let candidate: App = apps
        .filter(id.eq(lookup_id))
        .select(App::as_select())
        .first(conn)?;

    if verify(token, &candidate.token_hash).unwrap_or(false) {
        Ok(candidate)
    } else {
        Err(diesel::result::Error::NotFound)
    }
}

// ============ Orders ============
pub fn insert_order(conn: &mut PgConnection, order: Order) -> Result<Order, Error> {
    use crate::schema::orders::dsl::*;
    diesel::insert_into(orders)
        .values(&order)
        .returning(Order::as_returning())
        .get_result(conn)
}

/// The natural-key backstop for `/orders` (Phase 1 §4): `INSERT ... ON CONFLICT (app_id,
/// order_id) DO NOTHING RETURNING`. If a row is returned it is the newly-created order; if no
/// row (a prior order with this `(app_id, order_id)` already exists), `SELECT` and return it.
/// Either way the result reflects the one real order, so two distinct idempotency keys for the
/// same business order converge on a single row. MUST be called inside `with_tx`.
pub fn insert_order_on_conflict(conn: &mut PgConnection, order: Order) -> Result<Order, Error> {
    use crate::schema::orders::dsl::*;

    let inserted: Option<Order> = diesel::insert_into(orders)
        .values(&order)
        .on_conflict((app_id, order_id))
        .do_nothing()
        .returning(Order::as_returning())
        .get_result(conn)
        .optional()?;

    match inserted {
        Some(o) => Ok(o),
        None => orders
            .filter(app_id.eq(order.app_id).and(order_id.eq(&order.order_id)))
            .select(Order::as_select())
            .first(conn),
    }
}

pub fn find_order(conn: &mut PgConnection, oid: Uuid) -> Result<Order, Error> {
    use crate::schema::orders::dsl::*;
    orders
        .filter(id.eq(oid))
        .select(Order::as_select())
        .first(conn)
}

pub fn find_order_by_app(conn: &mut PgConnection, oid: Uuid, app_uuid: Uuid) -> Result<Order, Error> {
    use crate::schema::orders::dsl::*;
    orders
        .filter(id.eq(oid).and(app_id.eq(app_uuid)))
        .select(Order::as_select())
        .first(conn)
}

pub fn find_order_by_memo_id(conn: &mut PgConnection, memo: &str) -> Result<Order, Error> {
    use crate::schema::orders::dsl::*;
    orders
        .filter(memo_id.eq(memo))
        .select(Order::as_select())
        .first(conn)
}

pub fn update_order(conn: &mut PgConnection, order: Order) -> Result<Order, Error> {
    use crate::schema::orders::dsl::*;
    diesel::update(orders.filter(id.eq(order.id)))
        .set((
            status.eq(order.status.clone()),
            tx_hash.eq(order.tx_hash.clone()),
            selected_mint.eq(order.selected_mint.clone()),
            expected_amount.eq(order.expected_amount.clone()),
            expected_decimals.eq(order.expected_decimals),
            confirmed_at.eq(order.confirmed_at),
        ))
        .returning(Order::as_returning())
        .get_result(conn)
}

// ============ Wallets ============
pub fn get_fat_wallet(conn: &mut PgConnection) -> Result<Wallet, diesel::result::Error> {
    use crate::schema::wallets::dsl::*;
    wallets.filter(type_.eq("fat")).first::<Wallet>(conn)
}

pub fn insert_wallet(conn: &mut PgConnection, wallet: Wallet) -> Result<Wallet, diesel::result::Error> {
    diesel::insert_into(crate::schema::wallets::table)
        .values(&wallet)
        .returning(Wallet::as_returning())
        .get_result(conn)
}

// ============ Audit Logs ============
pub fn insert_audit(conn: &mut PgConnection, log: AuditLog) -> Result<AuditLog, Error> {
    diesel::insert_into(audit_logs::table)
        .values(&log)
        .returning(AuditLog::as_returning())
        .get_result(conn)
}

pub fn get_token_decimals(conn: &mut PgConnection, token_mint_: &str) -> Result<u8, Error> {
    use crate::schema::deposits::dsl::*;
    deposits
        .filter(token_mint.eq(token_mint_))
        .select(token_decimals)
        .first::<Option<i32>>(conn)
        .map(|opt| opt.unwrap_or(0) as u8)
}

pub fn insert_deposit(conn: &mut PgConnection, mut dep: Deposit) -> Result<Deposit, Error> {
    use crate::schema::deposits::dsl::*;
    // set created_at if not set
    if dep.created_at.is_none() {
        dep.created_at = Some(Utc::now().naive_utc());
    }
    match diesel::insert_into(deposits)
        .values(&dep)
        .returning(Deposit::as_returning())
        .get_result(conn)
    {
        Ok(d) => Ok(d),
        Err(Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
            // if tx_hash exists, increment processing_attempts
            let existing: Deposit = deposits
                .filter(tx_hash.eq(&dep.tx_hash))
                .select(Deposit::as_select())
                .first(conn)?;
            let new_attempts = existing.processing_attempts.unwrap_or(0) + 1;
            let updated: Deposit = diesel::update(deposits.filter(id.eq(existing.id)))
                .set((
                    processing_attempts.eq(new_attempts),
                    updated_at.eq(Some(Utc::now().naive_utc())),
                ))
                .returning(Deposit::as_returning())
                .get_result(conn)?;
            Ok(updated)
        }
        Err(e) => Err(e),
    }
}

/// Atomic-credit dedup oracle (Phase 1 §5): `INSERT ... ON CONFLICT (tx_hash) DO NOTHING
/// RETURNING`. `Some(row)` is a first-time confirmed delivery (credit it); `None` means this
/// signature was already inserted (a duplicate delivery — credit NOTHING). MUST be called inside
/// `with_tx` so the dedup and the credit it gates commit together.
pub fn insert_deposit_on_conflict(
    conn: &mut PgConnection,
    mut dep: Deposit,
) -> Result<Option<Deposit>, Error> {
    use crate::schema::deposits::dsl::*;
    if dep.created_at.is_none() {
        dep.created_at = Some(Utc::now().naive_utc());
    }
    diesel::insert_into(deposits)
        .values(&dep)
        .on_conflict(tx_hash)
        .do_nothing()
        .returning(Deposit::as_returning())
        .get_result(conn)
        .optional()
}

/// Lifecycle-guarded mark-paid (Phase 1 §5): `UPDATE orders SET status='paid', tx_hash=?,
/// confirmed_at=now() WHERE id=? AND status<>'paid'`. Returns rows affected (0 if already paid).
/// The `status<>'paid'` predicate is the atomic backstop behind `order_can_mark_paid`. MUST be
/// called inside `with_tx`.
pub fn mark_order_paid(
    conn: &mut PgConnection,
    order_id_: Uuid,
    tx_hash_: &str,
) -> Result<usize, Error> {
    use crate::schema::orders::dsl::*;
    diesel::update(orders.filter(id.eq(order_id_)).filter(status.ne("paid")))
        .set((
            status.eq("paid"),
            tx_hash.eq(Some(tx_hash_.to_string())),
            confirmed_at.eq(Some(Utc::now().naive_utc())),
        ))
        .execute(conn)
}

pub fn mark_deposit_processed(
    conn: &mut PgConnection,
    txhash: &str,
    confirmed_at_ts: Option<chrono::NaiveDateTime>,
) -> Result<Deposit, Error> {
    use crate::schema::deposits::dsl::*;
    diesel::update(deposits.filter(tx_hash.eq(txhash)))
        .set((
            processed.eq(true),
            status.eq("verified"),
            confirmed_at.eq(confirmed_at_ts),
            updated_at.eq(Some(Utc::now().naive_utc())),
        ))
        .returning(Deposit::as_returning())
        .get_result(conn)
}

// === Balances ===
pub fn upsert_balance(
    conn: &mut PgConnection,
    merchant_id_: Uuid,
    token_mint_: &str,
    amount_: &BigDecimal,
) -> Result<Balance, Error> {
    use crate::schema::balances::dsl::*;

    diesel::insert_into(balances)
        .values((
            merchant_id.eq(merchant_id_),
            token_mint.eq(token_mint_.to_string()),
            balance.eq(amount_.clone()),
            locked_balance.eq(BigDecimal::from(0)), // ensure init
            updated_at.eq(Utc::now().naive_utc()),
        ))
        .on_conflict((merchant_id, token_mint))
        .do_update()
        .set((
            balance.eq(balance + amount_.clone()),
            updated_at.eq(Utc::now().naive_utc()),
        ))
        .returning(Balance::as_returning())
        .get_result(conn)
}

pub fn get_balance(conn: &mut PgConnection, merchant_id_: Uuid, token_mint_: &str) -> Result<Balance, Error> {
    use crate::schema::balances::dsl::*;
    balances
        .filter(merchant_id.eq(merchant_id_))
        .filter(token_mint.eq(token_mint_.to_string()))
        .select(Balance::as_select())
        .first(conn)
}

// === Withdrawals ===
pub fn create_withdrawal(
    conn: &mut PgConnection,
    merchant_id_: Uuid,
    token_mint_: &str,
    amount_: &BigDecimal,
    target_address_: &str,
) -> Result<Withdrawal, Error> {
    use crate::schema::withdrawals::dsl::*;

    diesel::insert_into(withdrawals)
        .values((
            merchant_id.eq(merchant_id_),
            token_mint.eq(token_mint_.to_string()),
            amount.eq(amount_.clone()),
            status.eq("pending"),
            target_address.eq(target_address_.to_string()),
            created_at.eq(Utc::now().naive_utc()),
        ))
        .returning(Withdrawal::as_returning())
        .get_result(conn)
}

pub fn update_withdrawal_status(
    conn: &mut PgConnection,
    withdrawal_id_: Uuid,
    new_status_: &str,
    tx_hash_: Option<&str>,
) -> Result<Withdrawal, Error> {
    use crate::schema::withdrawals::dsl::*;

    diesel::update(withdrawals.find(withdrawal_id_))
        .set((
            status.eq(new_status_.to_string()),
            tx_hash.eq(tx_hash_.map(|s| s.to_string())),
            updated_at.eq(Utc::now().naive_utc()),
        ))
        .returning(Withdrawal::as_returning())
        .get_result(conn)
}

/// Read a withdrawal by id (the worker dispatches on its `status`).
pub fn find_withdrawal(conn: &mut PgConnection, withdrawal_id_: Uuid) -> Result<Withdrawal, Error> {
    use crate::schema::withdrawals::dsl::*;
    withdrawals
        .find(withdrawal_id_)
        .select(Withdrawal::as_select())
        .first(conn)
}

/// Guarded `pending -> processing` transition (Phase 1 §6). Commits BEFORE the external send so
/// that every redelivery thereafter reads `processing` and reconciles via `Signer::lookup`
/// instead of re-sending. Returns rows affected: `1` = we advanced it (proceed to send), `0` =
/// it was no longer `pending` (a concurrent consumer advanced it — do not send).
pub fn set_withdrawal_processing(
    conn: &mut PgConnection,
    withdrawal_id_: Uuid,
) -> Result<usize, Error> {
    use crate::schema::withdrawals::dsl::*;
    diesel::update(withdrawals.find(withdrawal_id_).filter(status.eq("pending")))
        .set((
            status.eq("processing"),
            updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)
}

// Lock funds and create withdrawal. MUST be called through `store::with_tx` (it issues
// `FOR UPDATE` row locks and signals insufficient funds via `RollbackTransaction`). Logic
// unchanged from the prior `conn.transaction` body; the wrapper moved out to `with_tx`.
pub fn create_withdrawal_and_lock(
    conn: &mut PgConnection,
    merchant_id_: Uuid,
    token_mint_: &str,
    amount_: &BigDecimal,
    target_address_: &str,
) -> Result<Withdrawal, Error> {
    use crate::schema::balances::dsl as b;
    use crate::schema::withdrawals::dsl as w;

    // Fetch or create balance row for merchant + token
    // attempt to select the balance row FOR UPDATE
    let bal_opt = b::balances
        .filter(b::merchant_id.eq(merchant_id_))
        .filter(b::token_mint.eq(token_mint_.to_string()))
        .for_update() // lock row
        .first::<Balance>(conn)
        .optional()?;

    let balance_row = match bal_opt {
        Some(bal) => bal,
        None => {
            // create balance row with zeroes
            let new_bal = Balance {
                id: Uuid::new_v4(),
                merchant_id: merchant_id_,
                token_mint: token_mint_.to_string(),
                balance: Some(BigDecimal::from(0)),
                locked_balance: Some(BigDecimal::from(0)),
                updated_at: Some(Utc::now().naive_utc()),
            };
            diesel::insert_into(b::balances)
                .values(&new_bal)
                .execute(conn)?;
            new_bal
        }
    };

    // read current values (may be None => treat as 0)
    let curr_balance = balance_row
        .balance
        .clone()
        .unwrap_or_else(|| BigDecimal::from(0));
    let curr_locked = balance_row
        .locked_balance
        .clone()
        .unwrap_or_else(|| BigDecimal::from(0));

    // check sufficient funds
    if &curr_balance < amount_ {
        return Err(Error::RollbackTransaction); // signal insufficient funds
    }

    // new values
    let new_balance = &curr_balance - amount_;
    let new_locked = &curr_locked + amount_;

    // update balance row
    diesel::update(b::balances.filter(b::id.eq(balance_row.id)))
        .set((
            b::balance.eq(new_balance.clone()),
            b::locked_balance.eq(new_locked.clone()),
            b::updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)?;

    // create withdrawal record (status = pending)
    let withdrawal = Withdrawal {
        id: Uuid::new_v4(),
        merchant_id: merchant_id_,
        token_mint: token_mint_.to_string(),
        amount: amount_.clone(),
        status: "pending".to_string(),
        target_address: target_address_.to_string(),
        tx_hash: None,
        created_at: Some(Utc::now().naive_utc()),
        updated_at: Some(Utc::now().naive_utc()),
    };

    let inserted: Withdrawal = diesel::insert_into(w::withdrawals)
        .values(&withdrawal)
        .get_result(conn)?;

    Ok(inserted)
}

// If you failed to push to redis, revert lock: atomically move locked_balance -> balance.
// MUST be called through `store::with_tx`.
pub fn revert_withdrawal_lock(
    conn: &mut PgConnection,
    merchant_id_: Uuid,
    token_mint_: &str,
    amount_: &BigDecimal,
) -> Result<Balance, Error> {
    use crate::schema::balances::dsl::*;

    // lock balance row
    let bal = balances
        .filter(merchant_id.eq(merchant_id_))
        .filter(token_mint.eq(token_mint_.to_string()))
        .for_update()
        .first::<Balance>(conn)?;

    let curr_locked = bal
        .locked_balance
        .clone()
        .unwrap_or_else(|| BigDecimal::from(0));
    let curr_balance = bal.balance.clone().unwrap_or_else(|| BigDecimal::from(0));

    let new_locked = &curr_locked - amount_;
    let new_balance = &curr_balance + amount_;

    let updated: Balance = diesel::update(balances.filter(id.eq(bal.id)))
        .set((
            balance.eq(new_balance.clone()),
            locked_balance.eq(new_locked.clone()),
            updated_at.eq(Utc::now().naive_utc()),
        ))
        .get_result(conn)?;

    Ok(updated)
}

// Idempotent finalize-success (Phase 1 §6): the balance move is GATED on the
// `processing -> completed` transition actually firing, so a double-finalize moves money once.
// Returns `true` iff this call performed the transition (and the locked-balance decrement);
// `false` if the withdrawal was already terminal (no-op, balance untouched).
// MUST be called through `store::with_tx`.
pub fn finalize_withdrawal_success(
    conn: &mut PgConnection,
    withdrawal_id_: Uuid,
    tx_hash_: &str,
) -> Result<bool, Error> {
    use crate::schema::balances::dsl as b;
    use crate::schema::withdrawals::dsl as w;

    // Guarded transition: only a `processing` withdrawal becomes `completed`.
    let rows = diesel::update(w::withdrawals.find(withdrawal_id_).filter(w::status.eq("processing")))
        .set((
            w::status.eq("completed"),
            w::tx_hash.eq(Some(tx_hash_.to_string())),
            w::updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)?;
    if rows != 1 {
        return Ok(false); // already terminal: no-op, do NOT touch the balance.
    }

    // Transition fired: reduce locked_balance by the (already-debited-from-balance) amount.
    let wd = w::withdrawals
        .find(withdrawal_id_)
        .select(Withdrawal::as_select())
        .first(conn)?;
    let bal = b::balances
        .filter(b::merchant_id.eq(wd.merchant_id))
        .filter(b::token_mint.eq(&wd.token_mint))
        .for_update()
        .first::<Balance>(conn)?;
    let curr_locked = bal.locked_balance.clone().unwrap_or_else(|| BigDecimal::from(0));
    let new_locked = &curr_locked - &wd.amount;

    diesel::update(b::balances.filter(b::id.eq(bal.id)))
        .set((
            b::locked_balance.eq(new_locked),
            b::updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)?;

    Ok(true)
}

// === Dead letter (poison-message sink, Brief §3.7) ===
/// Record a stream entry that cannot become a valid credit (no matching order, verification
/// failure, or a parse failure) instead of silently dropping it. The caller XACKs after this
/// commits, so nothing is lost.
pub fn insert_dead_letter(
    conn: &mut PgConnection,
    source_stream_: &str,
    raw_: &serde_json::Value,
    reason_: &str,
) -> Result<DeadLetter, Error> {
    use crate::schema::dead_letter::dsl::*;
    diesel::insert_into(dead_letter)
        .values((
            source_stream.eq(source_stream_),
            raw.eq(raw_.clone()),
            reason.eq(reason_),
        ))
        .returning(DeadLetter::as_returning())
        .get_result(conn)
}

// Idempotent finalize-failed (Phase 1 §6): gated on the `processing -> failed` transition. On the
// firing call it restores locked -> balance; a redelivery after a prior failed-finalize is a
// no-op. Returns `true` iff this call performed the transition. MUST be called through `with_tx`.
pub fn finalize_withdrawal_failed(
    conn: &mut PgConnection,
    withdrawal_id_: Uuid,
    _failure_reason: &str,
) -> Result<bool, Error> {
    use crate::schema::balances::dsl as b;
    use crate::schema::withdrawals::dsl as w;

    let rows = diesel::update(w::withdrawals.find(withdrawal_id_).filter(w::status.eq("processing")))
        .set((
            w::status.eq("failed"),
            w::updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)?;
    if rows != 1 {
        return Ok(false); // already terminal: no-op.
    }

    // Transition fired: move the locked amount back to the available balance.
    let wd = w::withdrawals
        .find(withdrawal_id_)
        .select(Withdrawal::as_select())
        .first(conn)?;
    let bal = b::balances
        .filter(b::merchant_id.eq(wd.merchant_id))
        .filter(b::token_mint.eq(&wd.token_mint))
        .for_update()
        .first::<Balance>(conn)?;
    let curr_locked = bal.locked_balance.clone().unwrap_or_else(|| BigDecimal::from(0));
    let curr_balance = bal.balance.clone().unwrap_or_else(|| BigDecimal::from(0));
    let new_locked = &curr_locked - &wd.amount;
    let new_balance = &curr_balance + &wd.amount;

    diesel::update(b::balances.filter(b::id.eq(bal.id)))
        .set((
            b::balance.eq(new_balance),
            b::locked_balance.eq(new_locked),
            b::updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)?;

    Ok(true)
}
