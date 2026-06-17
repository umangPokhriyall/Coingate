use anyhow::Result;
use bigdecimal::{BigDecimal, ToPrimitive};
use external::{MpcSigner, SendRequest, Signer, SignerError};
use redis::Value as RedisValue;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use store::{Config, Pool, build_pool, get_conn, with_tx};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
    let mut db = get_conn(&pool).expect("failed to get DB connection");
    let fat_wallet = store::get_fat_wallet(&mut db).expect("fat wallet not configured");
    drop(db);
    let send_url = format!("http://127.0.0.1:3000/wallets/{}/send", fat_wallet.id);
    let signer = MpcSigner::new(send_url);

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
                Ok(payload) => {
                    process_withdrawal(&pool, &signer, &payload, &mut conn, group, &stream, &redis_id)
                        .await?;
                }
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
                Ok(payload) => {
                    process_withdrawal(&pool, &signer, &payload, &mut conn, group, &stream, &redis_id)
                        .await?;
                }
                Err(e) => warn!(error = %e, %stream, %redis_id, "malformed withdrawal payload, skipping"),
            }
        }
    }
}

async fn process_withdrawal<S: Signer>(
    pool: &Pool,
    signer: &S,
    payload: &WithdrawalPayload,
    conn: &mut redis::Connection,
    group: &str,
    stream: &str,
    redis_id: &str,
) -> Result<()> {
    let withdrawal_id = payload.withdrawal_id;

    info!(%withdrawal_id, %stream, %redis_id, "processing withdrawal");

    // Pooled connection for the pre-send, single-statement DB work.
    let mut db = get_conn(pool)?;

    // Update status to 'processing'
    let _ = store::update_withdrawal_status(&mut db, withdrawal_id, "processing", None)?;
    debug!(%withdrawal_id, "withdrawal status set to 'processing'");

    // Determine if token or SOL
    let is_sol = payload.token_mint == "So11111111111111111111111111111111111111112";

    // Convert amount to smallest unit (unchanged logic)
    let amount_u64 = if is_sol {
        let lamports_per_sol = 1_000_000_000u64;
        (payload.amount.clone() * BigDecimal::from(lamports_per_sol))
            .to_u64()
            .unwrap_or(0)
    } else {
        let decimals = store::get_token_decimals(&mut db, &payload.token_mint)?; // fetch decimals from DB
        let factor = BigDecimal::from(10u64.pow(decimals as u32));
        (payload.amount.clone() * factor).to_u64().unwrap_or(0)
    };

    // Done with pooled DB connection before the MPC network call.
    drop(db);

    debug!(amount = %payload.amount, amount_base_units = amount_u64, "converted withdrawal amount");

    // Send through the Signer trait, passing key = withdrawal_id (the effect idempotency key).
    // Phase 0 still calls send on the happy path; status-guard + lookup reconciliation is Phase 1.4.
    let mint = if is_sol {
        None
    } else {
        Some(payload.token_mint.as_str())
    };
    let req = SendRequest {
        key: withdrawal_id,
        to: &payload.target_address,
        amount: amount_u64,
        mint,
    };

    match signer.send(req).await {
        Ok(sig) => {
            let sig = sig.to_string();
            with_tx(pool, |c| {
                store::finalize_withdrawal_success(c, withdrawal_id, &sig)
            })?;
            let _: () = redis::cmd("XACK")
                .arg(stream)
                .arg(group)
                .arg(redis_id)
                .query(conn)?;
            info!(%withdrawal_id, %sig, "withdrawal completed successfully");
        }
        Err(SignerError::Transport(e)) => {
            // Ambiguous: we do not know if the MPC sent. Leave the withdrawal `processing`
            // and do NOT ack — Phase 1.4 reconciles via lookup instead of re-sending.
            error!(%withdrawal_id, error = %e, "transport error calling MPC; leaving withdrawal pending");
        }
        Err(e) => {
            // Definite failure (rejected / no signature): finalize as failed and ack.
            with_tx(pool, |c| {
                store::finalize_withdrawal_failed(c, withdrawal_id, &e.to_string())
            })?;
            let _: () = redis::cmd("XACK")
                .arg(stream)
                .arg(group)
                .arg(redis_id)
                .query(conn)?;
            warn!(%withdrawal_id, error = %e, "withdrawal failed");
        }
    }

    Ok(())
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
}
