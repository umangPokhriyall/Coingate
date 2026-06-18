use anyhow::{Result, anyhow};
use bigdecimal::{BigDecimal, ToPrimitive};
use chaos_hooks::crash_point;
use chrono::Utc;
use idempotency::order_can_mark_paid;
use redis::Value as RedisValue;
use serde_json::{Value, json};
use std::collections::HashMap;
use store::module::Deposit;
use store::{Config, Pool, build_pool, get_conn, with_tx};
use tracing::{error, info, warn};

// SOL native mint, used as the deposit token when a stream entry omits an explicit mint.
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Terminal outcome of handling one stream entry. Every variant is XACK-worthy (nothing is
/// silently dropped); only an infra error (returned as `Err`) leaves the entry un-acked to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreditOutcome {
    /// First-time confirmed deposit: balance credited, order marked paid.
    Credited,
    /// Redelivered signature already in `deposits` (UNIQUE tx_hash): credited nothing.
    Duplicate,
    /// Could not become a valid credit (no order / verification / parse failure): recorded in
    /// `dead_letter`.
    DeadLettered,
}

#[derive(Debug, Clone)]
struct StreamTransaction {
    stream_id: String,
    redis_id: String,
    signature: String,
    slot: u64,
    block_time: Option<i64>,
    memo_id: Option<String>,
    transaction_type: String,
    from_address: Option<String>,
    to_address: Option<String>,
    amount: Option<u64>,
    token_mint: Option<String>,
    token_decimals: Option<u8>,
    status: String,
    logs: Vec<String>,
}

fn redis_value_to_string(v: &RedisValue) -> Option<String> {
    match v {
        RedisValue::Data(d) => String::from_utf8(d.clone()).ok(),
        RedisValue::Int(i) => Some(i.to_string()),
        RedisValue::Okay => Some("OK".to_string()),
        _ => None,
    }
}

pub fn parse_xreadgroup_response(v: RedisValue) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    if let RedisValue::Bulk(streams) = v {
        for stream in streams {
            if let RedisValue::Bulk(mut stream_data) = stream {
                if stream_data.len() != 2 {
                    continue;
                }
                let stream_name = redis_value_to_string(&stream_data[0]).unwrap_or_default();
                if let RedisValue::Bulk(messages) = stream_data.remove(1) {
                    for msg in messages {
                        if let RedisValue::Bulk(mut entry) = msg {
                            if entry.len() != 2 {
                                continue;
                            }
                            let id = redis_value_to_string(&entry[0]).unwrap_or_default();
                            if let RedisValue::Bulk(fields) = &entry[1] {
                                let mut i = 0usize;
                                let mut map = HashMap::new();
                                while i + 1 < fields.len() {
                                    let k = redis_value_to_string(&fields[i]);
                                    let v = redis_value_to_string(&fields[i + 1]);
                                    if let (Some(k), Some(v)) = (k, v) {
                                        map.insert(k, v);
                                    }
                                    i += 2;
                                }
                                let json_data = serde_json::to_string(&map)
                                    .unwrap_or_else(|_| "{}".to_string());
                                out.push((stream_name.clone(), id, json_data));
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

/// Build a `StreamTransaction` from an already-parsed JSON value. Infallible: missing/odd fields
/// fall back to defaults (a structurally-invalid payload is caught earlier by `from_str` and
/// dead-lettered, so this never sees non-JSON).
fn parse_transaction_data(stream_id: &str, redis_id: &str, json: &Value) -> StreamTransaction {
    StreamTransaction {
        stream_id: stream_id.to_string(),
        redis_id: redis_id.to_string(),
        signature: json["signature"].as_str().unwrap_or("").to_string(),
        slot: json["slot"].as_u64().unwrap_or(0),
        block_time: json["block_time"].as_i64(),
        memo_id: json["memo_id"].as_str().map(|s| s.to_string()),
        transaction_type: json["transaction_type"]
            .as_str()
            .unwrap_or("UNKNOWN")
            .to_string(),
        from_address: json["from_address"].as_str().map(|s| s.to_string()),
        to_address: json["to_address"].as_str().map(|s| s.to_string()),
        amount: json["amount"].as_str().and_then(|s| s.parse::<u64>().ok()),
        token_mint: json["token_mint"].as_str().map(|s| s.to_string()),
        token_decimals: json["token_decimals"]
            .as_str()
            .and_then(|s| s.parse::<u8>().ok()),
        status: json["status"].as_str().unwrap_or("UNKNOWN").to_string(),
        logs: json["logs"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Record an entry that cannot become a valid credit, in its own small `with_tx`, then report
/// `DeadLettered` so the caller XACKs it (Brief §3.7 — nothing is silently dropped).
fn dead_letter(
    pool: &Pool,
    source_stream: &str,
    raw: &Value,
    reason: &str,
) -> Result<CreditOutcome> {
    with_tx(pool, |conn| {
        store::insert_dead_letter(conn, source_stream, raw, reason).map(|_| ())
    })?;
    warn!(source_stream, reason, "dead-lettered stream entry");
    Ok(CreditOutcome::DeadLettered)
}

/// The atomic credit (Phase 1 §5 / Amendment §A4). The dedup decision and the credit it gates
/// commit in ONE `with_tx` at READ COMMITTED: the deposit is inserted on its natural key
/// (`tx_hash`) and the balance is credited **only if** the deposit was newly inserted. Entries
/// that cannot become a valid credit are dead-lettered (no order, verification failure).
fn process_transaction(pool: &Pool, tx: &StreamTransaction, raw: &Value) -> Result<CreditOutcome> {
    let memo = match &tx.memo_id {
        Some(m) => m.clone(),
        None => return dead_letter(pool, &tx.stream_id, raw, "missing memo_id"),
    };

    // Read the order (orders are created before payment, so "no order" is a genuine error).
    let order = {
        let mut conn = get_conn(pool)?;
        match store::find_order_by_memo_id(&mut conn, &memo) {
            Ok(o) => o,
            Err(_) => {
                return dead_letter(pool, &tx.stream_id, raw, &format!("no order for memo {memo}"));
            }
        }
    };

    // Verify amount/mint. A mismatch is unverifiable -> dead-letter, never a silent credit.
    let verified = match tx.transaction_type.as_str() {
        "SOL" => verify_sol_transaction(&order, tx),
        "TOKEN" => verify_token_transaction(&order, tx),
        _ => Ok(false),
    }?;
    if !verified {
        return dead_letter(
            pool,
            &tx.stream_id,
            raw,
            &format!("verification failed for order {}", order.id),
        );
    }

    let app_id = match order.app_id {
        Some(a) => a,
        None => {
            return dead_letter(pool, &tx.stream_id, raw, &format!("order {} has no app_id", order.id));
        }
    };
    let merchant_id = {
        let mut conn = get_conn(pool)?;
        store::get_merchant_id_for_app(&mut conn, app_id)?
    };

    let amount = BigDecimal::from(tx.amount.unwrap_or(0));
    let token_mint = tx.token_mint.clone().unwrap_or_else(|| SOL_MINT.to_string());
    let deposit = Deposit {
        id: uuid::Uuid::new_v4(),
        order_id: Some(order.id),
        tx_hash: tx.signature.clone(),
        chain: "solana".to_string(),
        slot: Some(tx.slot as i64),
        block_hash: None,
        from_address: tx.from_address.clone(),
        to_address: tx.to_address.clone(),
        token_mint: Some(token_mint.clone()),
        token_symbol: None,
        token_decimals: tx.token_decimals.map(|d| d as i32),
        amount: amount.clone(),
        memo_id: tx.memo_id.clone(),
        status: "confirmed".to_string(),
        confirmations: Some(1),
        raw: None,
        processed: Some(true),
        processing_attempts: Some(1),
        created_at: Some(Utc::now().naive_utc()),
        updated_at: Some(Utc::now().naive_utc()),
        confirmed_at: Some(Utc::now().naive_utc()),
    };

    // The dedup + credit transaction. Crash points sit at every statement boundary (Amendment §A1).
    let outcome = with_tx(pool, |conn| {
        let inserted = store::insert_deposit_on_conflict(conn, deposit)?;
        crash_point!(chaos_hooks::CrashPointId::ProcAfterDepositInsertBeforeCredit);
        match inserted {
            // Duplicate delivery: the UNIQUE(tx_hash) index is the dedup oracle. Credit NOTHING.
            None => Ok(CreditOutcome::Duplicate),
            Some(_) => {
                // Atomic increment (DO UPDATE SET balance = balance + amount); no read-modify-write.
                store::upsert_balance(conn, merchant_id, &token_mint, &amount)?;
                crash_point!(chaos_hooks::CrashPointId::ProcAfterCreditBeforeOrderPaid);
                if order_can_mark_paid(&order.status) {
                    store::mark_order_paid(conn, order.id, &tx.signature)?;
                }
                crash_point!(chaos_hooks::CrashPointId::ProcAfterOrderPaidBeforeCommit);
                Ok(CreditOutcome::Credited)
            }
        }
    })?;

    match outcome {
        CreditOutcome::Credited => {
            info!(order_id = %order.id, %merchant_id, tx_hash = %tx.signature, "credited deposit")
        }
        CreditOutcome::Duplicate => {
            info!(tx_hash = %tx.signature, "duplicate delivery, credited nothing")
        }
        CreditOutcome::DeadLettered => {}
    }
    Ok(outcome)
}

fn verify_sol_transaction(
    order: &store::module::Order,
    tx: &StreamTransaction,
) -> Result<bool> {
    let expected_amount = order
        .expected_amount
        .as_ref()
        .map(|a| a.to_u64().unwrap_or(0))
        .unwrap_or(0);
    let actual_amount = tx.amount.unwrap_or(0);
    Ok((expected_amount as i64 - actual_amount as i64).abs() <= 1000)
}

fn verify_token_transaction(
    order: &store::module::Order,
    tx: &StreamTransaction,
) -> Result<bool> {
    let expected_mint = match &order.selected_mint {
        Some(mint) => mint,
        None => return Ok(false),
    };
    let actual_mint = match &tx.token_mint {
        Some(mint) => mint,
        None => return Ok(false),
    };
    if expected_mint != actual_mint {
        return Ok(false);
    }
    let expected_amount = order
        .expected_amount
        .as_ref()
        .map(|a| a.to_u64().unwrap_or(0))
        .unwrap_or(0);
    let actual_amount = tx.amount.unwrap_or(0);
    Ok(expected_amount == actual_amount)
}

/// Parse one raw stream entry and drive it to a terminal outcome. A non-JSON payload is
/// dead-lettered here (a parse failure cannot become a credit); everything else flows through
/// the atomic credit. Returns `Err` only on infra failure (caller leaves it un-acked to retry).
fn process_entry(pool: &Pool, stream: &str, id: &str, data_str: &str) -> Result<CreditOutcome> {
    let raw: Value = match serde_json::from_str(data_str) {
        Ok(v) => v,
        Err(e) => {
            return dead_letter(
                pool,
                stream,
                &json!({ "raw_payload": data_str }),
                &format!("parse error: {e}"),
            );
        }
    };
    let tx = parse_transaction_data(stream, id, &raw);
    process_transaction(pool, &tx, &raw)
}

fn xack(conn: &mut redis::Connection, stream: &str, group: &str, id: &str) -> Result<()> {
    let _: () = redis::cmd("XACK").arg(stream).arg(group).arg(id).query(conn)?;
    Ok(())
}

/// Process one entry then XACK it. The single `ProcAfterCommitBeforeXack` fire-site sits here,
/// between the (committed) credit/dead-letter work and the ack, so a crash redelivers the entry —
/// absorbed by the `tx_hash` dedup. Infra errors propagate WITHOUT acking, so the entry retries.
fn handle_entry(
    redis_conn: &mut redis::Connection,
    pool: &Pool,
    group: &str,
    stream: &str,
    id: &str,
    data_str: &str,
) -> Result<()> {
    process_entry(pool, stream, id, data_str)?;
    crash_point!(chaos_hooks::CrashPointId::ProcAfterCommitBeforeXack);
    xack(redis_conn, stream, group, id)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration (check required env vars)");
    let client = redis::Client::open(cfg.redis_url.clone())?;
    let mut conn = client.get_connection()?;
    let pool = build_pool(&cfg).expect("failed to build database pool");

    let group = "processor_group";
    let consumer = format!("consumer-{}", uuid::Uuid::new_v4());

    match redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("payment_transactions")
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query::<()>(&mut conn)
    {
        Ok(_) => info!(%group, "created consumer group"),
        Err(e) if e.to_string().contains("BUSYGROUP") => {}
        Err(e) => return Err(anyhow!(e)),
    }

    loop {
        // -- Normal XREADGROUP
        let resp: RedisValue = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(&consumer)
            .arg("BLOCK")
            .arg(5000)
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg("payment_transactions")
            .arg(">")
            .query(&mut conn)?;

        for (stream, id, data_str) in parse_xreadgroup_response(resp) {
            if let Err(e) = handle_entry(&mut conn, &pool, group, &stream, &id, &data_str) {
                // Infra failure: leave the entry un-acked so it is redelivered and retried.
                error!(error = %e, %stream, %id, "entry handling error, leaving un-acked for retry");
            }
        }

        // -- Periodically claim stuck messages
        let resp: RedisValue = redis::cmd("XAUTOCLAIM")
            .arg("payment_transactions")
            .arg(group)
            .arg(&consumer)
            .arg(60000) // 60s idle
            .arg("0-0")
            .arg("COUNT")
            .arg(10)
            .query(&mut conn)?;

        for (stream, id, data_str) in parse_xreadgroup_response(resp) {
            if let Err(e) = handle_entry(&mut conn, &pool, group, &stream, &id, &data_str) {
                error!(error = %e, %stream, %id, "entry handling error, leaving un-acked for retry");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::module::Order;
    use uuid::Uuid;

    fn mk_order(app_id: Option<Uuid>, memo: &str, expected: u64, mint: &str, status: &str) -> Order {
        Order {
            id: Uuid::new_v4(),
            app_id,
            order_id: format!("ord-{}", Uuid::new_v4()),
            price_amount: BigDecimal::from(expected),
            price_currency: "USD".to_string(),
            receive_currency: "USDC".to_string(),
            memo_id: memo.to_string(),
            status: status.to_string(),
            tx_hash: None,
            callback_url: None,
            success_url: None,
            cancel_url: None,
            created_at: None,
            confirmed_at: None,
            selected_mint: Some(mint.to_string()),
            expected_amount: Some(BigDecimal::from(expected)),
            expected_decimals: Some(6),
        }
    }

    fn mk_tx(memo: &str, signature: &str, ttype: &str, mint: &str, amount: u64) -> StreamTransaction {
        StreamTransaction {
            stream_id: "payment_transactions".to_string(),
            redis_id: "0-1".to_string(),
            signature: signature.to_string(),
            slot: 42,
            block_time: None,
            memo_id: Some(memo.to_string()),
            transaction_type: ttype.to_string(),
            from_address: Some("from".to_string()),
            to_address: Some("to".to_string()),
            amount: Some(amount),
            token_mint: Some(mint.to_string()),
            token_decimals: Some(6),
            status: "confirmed".to_string(),
            logs: vec![],
        }
    }

    // ── Pure verification (no DB) ───────────────────────────────────────────────────────────
    #[test]
    fn token_verification_requires_matching_mint_and_exact_amount() {
        let order = mk_order(None, "m", 1_000_000, "MINT_A", "pending");
        assert!(verify_token_transaction(&order, &mk_tx("m", "s", "TOKEN", "MINT_A", 1_000_000)).unwrap());
        // wrong amount
        assert!(!verify_token_transaction(&order, &mk_tx("m", "s", "TOKEN", "MINT_A", 999_999)).unwrap());
        // wrong mint
        assert!(!verify_token_transaction(&order, &mk_tx("m", "s", "TOKEN", "MINT_B", 1_000_000)).unwrap());
    }

    #[test]
    fn sol_verification_tolerates_small_fee_delta_only() {
        let order = mk_order(None, "m", 1_000_000, "SOL", "pending");
        assert!(verify_sol_transaction(&order, &mk_tx("m", "s", "SOL", "SOL", 1_000_500)).unwrap());
        assert!(!verify_sol_transaction(&order, &mk_tx("m", "s", "SOL", "SOL", 1_002_000)).unwrap());
    }

    #[test]
    fn parse_transaction_data_defaults_on_missing_fields() {
        let tx = parse_transaction_data("st", "0-1", &json!({ "memo_id": "abc" }));
        assert_eq!(tx.memo_id.as_deref(), Some("abc"));
        assert_eq!(tx.transaction_type, "UNKNOWN");
        assert_eq!(tx.amount, None);
        assert_eq!(tx.slot, 0);
    }

    // ── DB-backed: dedup + credit-if-inserted + dead-letter (gated on DATABASE_URL) ──────────
    //
    //   DATABASE_URL=postgres:///coingate_proc_test?host=/var/run/postgresql cargo test -p processor

    fn db_pool_or_skip() -> Option<Pool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let manager = store::diesel::r2d2::ConnectionManager::<store::PgConnection>::new(url);
        store::diesel::r2d2::Pool::builder().max_size(2).build(manager).ok()
    }

    fn seed_app(pool: &Pool) -> Uuid {
        let mut conn = get_conn(pool).expect("conn");
        let merchant = store::insert_merchant(
            &mut conn,
            store::module::Merchant {
                id: Uuid::new_v4(),
                email: format!("m-{}@t.local", Uuid::new_v4()),
                password_hash: "x".to_string(),
                name: "t".to_string(),
                created_at: None,
            },
        )
        .expect("merchant");
        store::insert_app(
            &mut conn,
            store::module::App {
                id: Uuid::new_v4(),
                merchant_id: Some(merchant.id),
                title: "t".to_string(),
                callback_url: None,
                token_hash: "x".to_string(),
                created_at: None,
            },
        )
        .expect("app")
        .id
    }

    #[test]
    fn db_credit_is_gated_on_first_insert_and_unverifiable_dead_letters() {
        let Some(pool) = db_pool_or_skip() else {
            eprintln!("skipping db processor test: DATABASE_URL unset/unreachable");
            return;
        };
        let app_id = seed_app(&pool);
        let mint = "MINT_USDC";

        // Seed a pending order and persist it.
        let order = mk_order(Some(app_id), &format!("memo-{}", Uuid::new_v4()), 1_000_000, mint, "pending");
        let memo = order.memo_id.clone();
        {
            let mut conn = get_conn(&pool).expect("conn");
            store::insert_order(&mut conn, order.clone()).expect("insert order");
        }
        let merchant_id = {
            let mut conn = get_conn(&pool).expect("conn");
            store::get_merchant_id_for_app(&mut conn, app_id).expect("merchant id")
        };

        let sig = format!("sig-{}", Uuid::new_v4());
        let tx = mk_tx(&memo, &sig, "TOKEN", mint, 1_000_000);

        // 1. First delivery credits.
        assert_eq!(
            process_transaction(&pool, &tx, &json!({})).expect("credit"),
            CreditOutcome::Credited
        );
        let bal_after_first = {
            let mut conn = get_conn(&pool).expect("conn");
            store::get_balance(&mut conn, merchant_id, mint).expect("balance").balance.unwrap()
        };
        assert_eq!(bal_after_first, BigDecimal::from(1_000_000), "credited once");

        // Order is now paid.
        {
            let mut conn = get_conn(&pool).expect("conn");
            assert_eq!(store::find_order(&mut conn, order.id).unwrap().status, "paid");
        }

        // 2. Redelivery of the same signature credits NOTHING (UNIQUE tx_hash dedup).
        assert_eq!(
            process_transaction(&pool, &tx, &json!({})).expect("dup"),
            CreditOutcome::Duplicate
        );
        let bal_after_dup = {
            let mut conn = get_conn(&pool).expect("conn");
            store::get_balance(&mut conn, merchant_id, mint).expect("balance").balance.unwrap()
        };
        assert_eq!(bal_after_dup, BigDecimal::from(1_000_000), "duplicate did not double-credit");

        // 3. An unverifiable deposit (amount mismatch) is dead-lettered, not credited.
        let order2 = mk_order(Some(app_id), &format!("memo-{}", Uuid::new_v4()), 1_000_000, mint, "pending");
        let memo2 = order2.memo_id.clone();
        {
            let mut conn = get_conn(&pool).expect("conn");
            store::insert_order(&mut conn, order2.clone()).expect("insert order2");
        }
        let dl_before = dead_letter_count(&pool);
        let bad_tx = mk_tx(&memo2, &format!("sig-{}", Uuid::new_v4()), "TOKEN", mint, 7);
        assert_eq!(
            process_transaction(&pool, &bad_tx, &json!({ "amount": "7" })).expect("dead-letter"),
            CreditOutcome::DeadLettered
        );
        assert_eq!(dead_letter_count(&pool), dl_before + 1, "one dead_letter row added");
        // order2 stays pending (never credited).
        {
            let mut conn = get_conn(&pool).expect("conn");
            assert_eq!(store::find_order(&mut conn, order2.id).unwrap().status, "pending");
        }
    }

    fn dead_letter_count(pool: &Pool) -> i64 {
        use store::diesel::prelude::*;
        use store::schema::dead_letter::dsl as d;
        let mut conn = get_conn(pool).expect("conn");
        d::dead_letter.count().get_result(&mut conn).expect("count")
    }
}
