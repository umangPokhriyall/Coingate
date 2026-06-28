use anyhow::Result;
use bigdecimal::{BigDecimal, ToPrimitive};
use chaos_hooks::crash_point;
use external::{MpcSigner, SendRequest, Signer, SignerError};
use redis::Value as RedisValue;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use store::{Config, Pool, build_pool, get_conn, with_tx};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// SOL native mint: a withdrawal in this mint is a native transfer (no `mint` on the send).
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone)]
struct WithdrawalPayload {
    withdrawal_id: Uuid,
    merchant_id: Uuid,
    token_mint: String, // now always a String
    amount: BigDecimal,
    target_address: String,
    created_at: String,
}

// Redis helpers
fn redis_value_to_string(v: &RedisValue) -> Option<String> {
    match v {
        RedisValue::Data(d) => String::from_utf8(d.clone()).ok(),
        RedisValue::Int(i) => Some(i.to_string()),
        RedisValue::Okay => Some("OK".to_string()),
        _ => None,
    }
}

fn parse_xreadgroup_response(v: RedisValue) -> Vec<(String, String, String)> {
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
                                    if let (Some(k), Some(v)) = (
                                        redis_value_to_string(&fields[i]),
                                        redis_value_to_string(&fields[i + 1]),
                                    ) {
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

/// Read a required string field, returning a typed error instead of panicking on a
/// malformed/missing field (the whole point: a bad stream entry must dead-letter, not abort).
fn req_str<'a>(json: &'a Value, key: &str) -> Result<&'a str> {
    json[key]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing or non-string field '{key}'"))
}

fn parse_withdrawal_payload(data_str: &str) -> Result<WithdrawalPayload> {
    debug!(raw = %data_str, "raw withdrawal payload");
    let wrapper: Value = serde_json::from_str(data_str)?;

    let inner_str = wrapper["data"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing 'data' field in payload"))?;

    let json: Value = serde_json::from_str(inner_str)?;

    // Exact money only: an amount string parses to BigDecimal; an integer is taken exactly.
    // f64 is rejected outright (no float round-trips on money).
    let amount = match &json["amount"] {
        Value::String(s) => BigDecimal::from_str(s)?,
        Value::Number(n) => match n.as_u64() {
            Some(u) => BigDecimal::from(u),
            None => return Err(anyhow::anyhow!("amount must be an integer or a decimal string")),
        },
        _ => return Err(anyhow::anyhow!("amount missing")),
    };

    Ok(WithdrawalPayload {
        withdrawal_id: Uuid::parse_str(req_str(&json, "withdrawal_id")?)?,
        merchant_id: Uuid::parse_str(req_str(&json, "merchant_id")?)?,
        token_mint: req_str(&json, "token_mint")?.to_string(),
        amount,
        target_address: req_str(&json, "target_address")?.to_string(),
        created_at: req_str(&json, "created_at")?.to_string(),
    })
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

    // The fat wallet (the MPC send endpoint) is fixed; resolve it once and inject the signer.
    // The MPC base URL comes from Config (no more hardcoded literal — Phase 1 §6).
    let mut db = get_conn(&pool).expect("failed to get DB connection");
    let fat_wallet = store::get_fat_wallet(&mut db).expect("fat wallet not configured");
    drop(db);
    let mpc_base = cfg.mpc_base_url.trim_end_matches('/');
    let send_url = format!("{}/wallets/{}/send", mpc_base, fat_wallet.id);
    // Reconciliation endpoint (Phase 2 §3.3): the worker calls this from `processing` instead of
    // re-sending, so a crash between send and finalize does not cause a second send.
    let lookup_url = format!("{}/lookup", mpc_base);
    let signer = MpcSigner::new(send_url, lookup_url);

    let group = "withdrawals_group";
    let consumer = format!("withdrawer-{}", uuid::Uuid::new_v4());

    let _: Result<(), _> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("withdrawal_requests")
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn);

    loop {
        let xauto: RedisValue = redis::cmd("XAUTOCLAIM")
            .arg("withdrawal_requests")
            .arg(group)
            .arg(&consumer)
            .arg(60000)
            .arg("0-0")
            .arg("COUNT")
            .arg(10)
            .query(&mut conn)?;

        for (stream, redis_id, data_str) in parse_xreadgroup_response(xauto) {
            match parse_withdrawal_payload(&data_str) {
                Ok(payload) => match process_withdrawal(&pool, &signer, &payload).await {
                    Ok(Disposition::Ack) => {
                        crash_point!(chaos_hooks::CrashPointId::WorkerAfterFinalizeBeforeXack);
                        if let Err(e) = xack(&mut conn, &stream, group, &redis_id) {
                            error!(error = %e, %stream, %redis_id, "XACK failed");
                        }
                    }
                    // Ambiguous/raced: leave un-acked so a redelivery reconciles it.
                    Ok(Disposition::Leave) => {}
                    // Infra error: leave un-acked rather than crash the consumer loop.
                    Err(e) => {
                        error!(error = %e, %stream, %redis_id, "withdrawal processing error, leaving un-acked")
                    }
                },
                Err(e) => warn!(error = %e, %stream, %redis_id, "malformed withdrawal payload, skipping"),
            }
        }

        let xread: RedisValue = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(&consumer)
            .arg("COUNT")
            .arg(10)
            .arg("BLOCK")
            .arg(5000)
            .arg("STREAMS")
            .arg("withdrawal_requests")
            .arg(">")
            .query(&mut conn)?;

        for (stream, redis_id, data_str) in parse_xreadgroup_response(xread) {
            match parse_withdrawal_payload(&data_str) {
                Ok(payload) => match process_withdrawal(&pool, &signer, &payload).await {
                    Ok(Disposition::Ack) => {
                        crash_point!(chaos_hooks::CrashPointId::WorkerAfterFinalizeBeforeXack);
                        if let Err(e) = xack(&mut conn, &stream, group, &redis_id) {
                            error!(error = %e, %stream, %redis_id, "XACK failed");
                        }
                    }
                    // Ambiguous/raced: leave un-acked so a redelivery reconciles it.
                    Ok(Disposition::Leave) => {}
                    // Infra error: leave un-acked rather than crash the consumer loop.
                    Err(e) => {
                        error!(error = %e, %stream, %redis_id, "withdrawal processing error, leaving un-acked")
                    }
                },
                Err(e) => warn!(error = %e, %stream, %redis_id, "malformed withdrawal payload, skipping"),
            }
        }
    }
}

/// Whether the caller should XACK the stream entry. `Leave` means the entry stays pending so a
/// redelivery can reconcile it (the ambiguous Transport case, or a lost pending->processing race).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Ack,
    Leave,
}

fn xack(conn: &mut redis::Connection, stream: &str, group: &str, redis_id: &str) -> Result<()> {
    let _: () = redis::cmd("XACK").arg(stream).arg(group).arg(redis_id).query(conn)?;
    Ok(())
}

/// Convert the payload's decimal amount into the chain's base units (lamports / token units).
fn to_base_units(pool: &Pool, payload: &WithdrawalPayload) -> Result<u64> {
    if payload.token_mint == SOL_MINT {
        Ok((payload.amount.clone() * BigDecimal::from(1_000_000_000u64))
            .to_u64()
            .unwrap_or(0))
    } else {
        let mut db = get_conn(pool)?;
        let decimals = store::get_token_decimals(&mut db, &payload.token_mint)?;
        let factor = BigDecimal::from(10u64.pow(decimals as u32));
        Ok((payload.amount.clone() * factor).to_u64().unwrap_or(0))
    }
}

/// Perform the external send (key = withdrawal_id) and finalize. Reached only when the send is
/// safe: from `pending` after a committed `pending->processing`, or from `processing` when
/// `lookup` confirms it was NOT yet sent. The Transport/Rejected/NoSignature three-way is
/// preserved: Transport is ambiguous (`Leave` -> redelivery reconciles); a definite rejection
/// finalizes failed and acks.
async fn send_and_finalize<S: Signer>(
    pool: &Pool,
    signer: &S,
    payload: &WithdrawalPayload,
) -> Result<Disposition> {
    let id = payload.withdrawal_id;
    let amount_u64 = to_base_units(pool, payload)?;
    let mint = if payload.token_mint == SOL_MINT {
        None
    } else {
        Some(payload.token_mint.as_str())
    };
    let req = SendRequest { key: id, to: &payload.target_address, amount: amount_u64, mint };

    match signer.send(req).await {
        Ok(sig) => {
            crash_point!(chaos_hooks::CrashPointId::WorkerAfterSendBeforeFinalize);
            let sig = sig.to_string();
            let _ = with_tx(pool, |c| store::finalize_withdrawal_success(c, id, &sig))?;
            info!(%id, %sig, "withdrawal completed successfully");
            Ok(Disposition::Ack)
        }
        Err(SignerError::Transport(e)) => {
            // Ambiguous: we don't know if the MPC sent. Leave `processing`, do NOT ack — the
            // redelivery lands in `processing` and reconciles via `lookup` (never blind-resends).
            error!(%id, error = %e, "transport error; leaving withdrawal processing for reconcile");
            Ok(Disposition::Leave)
        }
        Err(e) => {
            // Definite failure (rejected / no signature): finalize failed and ack.
            let _ = with_tx(pool, |c| store::finalize_withdrawal_failed(c, id, &e.to_string()))?;
            warn!(%id, error = %e, "withdrawal failed");
            Ok(Disposition::Ack)
        }
    }
}

/// Dispatch on the withdrawal's CURRENT state (Phase 1 §6). `send` is reached only from
/// `pending`; every redelivery thereafter lands in `processing` (or a terminal state) and
/// reconciles, so `send` is called exactly once per withdrawal. Returns the ack disposition; the
/// caller performs the XACK (so this function is pure of Redis and unit-testable on a DB alone).
async fn process_withdrawal<S: Signer>(
    pool: &Pool,
    signer: &S,
    payload: &WithdrawalPayload,
) -> Result<Disposition> {
    let id = payload.withdrawal_id;
    info!(%id, "processing withdrawal");

    let wd = with_tx(pool, |c| store::find_withdrawal(c, id))?;
    match wd.status.as_str() {
        // Terminal: a redelivery after finalize-then-crash-before-xack. Ack, no-op.
        "completed" | "failed" => {
            debug!(%id, status = %wd.status, "terminal withdrawal redelivery; acking no-op");
            Ok(Disposition::Ack)
        }
        // Ambiguous: we may already have sent. RECONCILE via lookup, never blind-resend.
        "processing" => match signer.lookup(id).await? {
            Some(sig) => {
                let sig = sig.to_string();
                let _ = with_tx(pool, |c| store::finalize_withdrawal_success(c, id, &sig))?;
                info!(%id, %sig, "reconciled: prior send found; finalized");
                Ok(Disposition::Ack)
            }
            None => {
                debug!(%id, "reconciled: not sent yet; sending now");
                send_and_finalize(pool, signer, payload).await
            }
        },
        // Fresh: commit pending->processing BEFORE sending, then send exactly once.
        "pending" => {
            let advanced = with_tx(pool, |c| store::set_withdrawal_processing(c, id))?;
            crash_point!(chaos_hooks::CrashPointId::WorkerAfterStatusProcessingBeforeSend);
            if advanced == 0 {
                // Another consumer advanced it past `pending`; reconcile on the next redelivery.
                warn!(%id, "withdrawal no longer pending (raced); will reconcile on redelivery");
                return Ok(Disposition::Leave);
            }
            send_and_finalize(pool, signer, payload).await
        }
        other => {
            warn!(%id, status = %other, "unknown withdrawal status; leaving un-acked");
            Ok(Disposition::Leave)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrap(inner: serde_json::Value) -> String {
        // Stream payloads arrive as {"data": "<inner json string>"}.
        serde_json::json!({ "data": inner.to_string() }).to_string()
    }

    fn valid_inner() -> serde_json::Value {
        serde_json::json!({
            "withdrawal_id": uuid::Uuid::new_v4().to_string(),
            "merchant_id": uuid::Uuid::new_v4().to_string(),
            "token_mint": "So11111111111111111111111111111111111111112",
            "amount": "1.5",
            "target_address": "addr",
            "created_at": "2026-01-01T00:00:00Z",
        })
    }

    #[test]
    fn valid_payload_parses() {
        assert!(parse_withdrawal_payload(&wrap(valid_inner())).is_ok());
    }

    #[test]
    fn hostile_payloads_error_and_never_panic() {
        // Each of these must return Err (dead-letter), never panic.
        let cases: Vec<String> = vec![
            "not json at all".to_string(),
            "".to_string(),
            "{}".to_string(),
            serde_json::json!({ "data": 123 }).to_string(), // data not a string
            serde_json::json!({ "data": "still not json" }).to_string(),
            wrap(serde_json::json!({ "merchant_id": "x" })), // missing fields
            wrap(serde_json::json!({
                "withdrawal_id": "not-a-uuid",
                "merchant_id": uuid::Uuid::new_v4().to_string(),
                "token_mint": "m", "amount": "1", "target_address": "a", "created_at": "t"
            })),
            // float amount must be rejected (no f64 on money)
            wrap(serde_json::json!({
                "withdrawal_id": uuid::Uuid::new_v4().to_string(),
                "merchant_id": uuid::Uuid::new_v4().to_string(),
                "token_mint": "m", "amount": 1.5, "target_address": "a", "created_at": "t"
            })),
            // amount as a wrong type
            wrap(serde_json::json!({
                "withdrawal_id": uuid::Uuid::new_v4().to_string(),
                "merchant_id": uuid::Uuid::new_v4().to_string(),
                "token_mint": "m", "amount": ["x"], "target_address": "a", "created_at": "t"
            })),
        ];

        for raw in cases {
            let result = parse_withdrawal_payload(&raw);
            assert!(result.is_err(), "expected Err for hostile input: {raw}");
        }
    }

    // ── DB-backed: send-once + idempotent finalize (gated on DATABASE_URL; redis not needed) ──
    //
    //   DATABASE_URL=postgres:///coingate_wrk_test?host=/var/run/postgresql cargo test -p worker
    use external::CountingMockSigner;

    fn db_pool_or_skip() -> Option<Pool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let manager = store::diesel::r2d2::ConnectionManager::<store::PgConnection>::new(url);
        store::diesel::r2d2::Pool::builder().max_size(2).build(manager).ok()
    }

    fn seed_merchant(pool: &Pool) -> Uuid {
        let mut c = get_conn(pool).expect("conn");
        store::insert_merchant(
            &mut c,
            store::module::Merchant {
                id: Uuid::new_v4(),
                email: format!("m-{}@t.local", Uuid::new_v4()),
                password_hash: "x".to_string(),
                name: "t".to_string(),
                created_at: None,
            },
        )
        .expect("merchant")
        .id
    }

    /// Fund a SOL balance and lock a `pending` withdrawal; return (withdrawal, payload).
    fn seed_locked_withdrawal(
        pool: &Pool,
        merchant_id: Uuid,
        fund: i64,
        amount: i64,
    ) -> (store::module::Withdrawal, WithdrawalPayload) {
        {
            let mut c = get_conn(pool).expect("conn");
            store::upsert_balance(&mut c, merchant_id, SOL_MINT, &BigDecimal::from(fund))
                .expect("fund");
        }
        let amount_bd = BigDecimal::from(amount);
        let wd = with_tx(pool, |c| {
            store::create_withdrawal_and_lock(c, merchant_id, SOL_MINT, &amount_bd, "addr")
        })
        .expect("lock");
        let payload = WithdrawalPayload {
            withdrawal_id: wd.id,
            merchant_id,
            token_mint: SOL_MINT.to_string(),
            amount: amount_bd,
            target_address: "addr".to_string(),
            created_at: "t".to_string(),
        };
        (wd, payload)
    }

    fn withdrawal_status(pool: &Pool, id: Uuid) -> String {
        let mut c = get_conn(pool).expect("conn");
        store::find_withdrawal(&mut c, id).expect("find").status
    }

    #[tokio::test]
    async fn db_send_count_is_one_across_redeliveries() {
        let Some(pool) = db_pool_or_skip() else {
            eprintln!("skipping worker db test: DATABASE_URL unset/unreachable");
            return;
        };
        let merchant_id = seed_merchant(&pool);
        let (wd, payload) = seed_locked_withdrawal(&pool, merchant_id, 10, 2);
        let signer = CountingMockSigner::new();

        // First delivery: pending -> processing -> send -> finalize.
        assert_eq!(process_withdrawal(&pool, &signer, &payload).await.unwrap(), Disposition::Ack);
        assert_eq!(signer.send_count(wd.id), 1);
        assert_eq!(withdrawal_status(&pool, wd.id), "completed");

        // Redelivery of a now-terminal withdrawal: ack no-op, NO resend.
        assert_eq!(process_withdrawal(&pool, &signer, &payload).await.unwrap(), Disposition::Ack);
        assert_eq!(signer.send_count(wd.id), 1, "redelivery must not resend");
    }

    #[tokio::test]
    async fn db_reconcile_from_processing_when_not_yet_sent_sends_once() {
        let Some(pool) = db_pool_or_skip() else { return };
        let merchant_id = seed_merchant(&pool);
        let (wd, payload) = seed_locked_withdrawal(&pool, merchant_id, 10, 2);

        // Simulate a crash AFTER pending->processing but BEFORE send: leave it in `processing`.
        let _ = with_tx(&pool, |c| store::set_withdrawal_processing(c, wd.id)).unwrap();
        let signer = CountingMockSigner::new();

        // Redelivery: processing -> lookup None -> send exactly once -> finalize.
        assert_eq!(process_withdrawal(&pool, &signer, &payload).await.unwrap(), Disposition::Ack);
        assert_eq!(signer.send_count(wd.id), 1);
        assert_eq!(withdrawal_status(&pool, wd.id), "completed");

        // Another redelivery: terminal, still one send.
        assert_eq!(process_withdrawal(&pool, &signer, &payload).await.unwrap(), Disposition::Ack);
        assert_eq!(signer.send_count(wd.id), 1);
    }

    #[tokio::test]
    async fn db_reconcile_from_processing_with_prior_send_does_not_resend() {
        let Some(pool) = db_pool_or_skip() else { return };
        let merchant_id = seed_merchant(&pool);
        let (wd, payload) = seed_locked_withdrawal(&pool, merchant_id, 10, 2);

        let _ = with_tx(&pool, |c| store::set_withdrawal_processing(c, wd.id)).unwrap();
        let signer = CountingMockSigner::new();
        // Simulate the send having happened (crash before finalize): record it in the signer.
        let _ = signer
            .send(SendRequest { key: wd.id, to: "addr", amount: 1, mint: None })
            .await
            .unwrap();
        assert_eq!(signer.send_count(wd.id), 1);

        // Redelivery: processing -> lookup Some -> finalize, with NO new send.
        assert_eq!(process_withdrawal(&pool, &signer, &payload).await.unwrap(), Disposition::Ack);
        assert_eq!(signer.send_count(wd.id), 1, "lookup path must not resend");
        assert_eq!(withdrawal_status(&pool, wd.id), "completed");
    }

    #[tokio::test]
    async fn db_double_finalize_moves_balance_once() {
        let Some(pool) = db_pool_or_skip() else { return };
        let merchant_id = seed_merchant(&pool);
        let (wd, _payload) = seed_locked_withdrawal(&pool, merchant_id, 10, 2);
        // fund 10, lock 2 -> balance 8, locked 2.
        let _ = with_tx(&pool, |c| store::set_withdrawal_processing(c, wd.id)).unwrap();

        let first = with_tx(&pool, |c| store::finalize_withdrawal_success(c, wd.id, "sig")).unwrap();
        assert!(first, "first finalize performs the transition");
        let second = with_tx(&pool, |c| store::finalize_withdrawal_success(c, wd.id, "sig")).unwrap();
        assert!(!second, "second finalize is a no-op");

        let bal = {
            let mut c = get_conn(&pool).expect("conn");
            store::get_balance(&mut c, merchant_id, SOL_MINT).expect("balance")
        };
        assert_eq!(bal.locked_balance.unwrap(), BigDecimal::from(0), "locked moved exactly once");
        assert_eq!(bal.balance.unwrap(), BigDecimal::from(8), "available balance unchanged by finalize");
    }
}
