use anyhow::{Result, anyhow};
use bigdecimal::{BigDecimal, ToPrimitive};
use redis::Value as RedisValue;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use store::store::Store;
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

fn parse_withdrawal_payload(data_str: &str) -> Result<WithdrawalPayload> {
    println!("🔎 Raw withdrawal payload: {}", data_str);
    let wrapper: Value = serde_json::from_str(data_str)?;

    // unwrap "data" field
    let inner_str = wrapper["data"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing 'data' field in payload"))?;

    println!("🔍 Inner withdrawal payload: {}", inner_str);

    let json: Value = serde_json::from_str(inner_str)?;

    let amount = match &json["amount"] {
        Value::String(s) => BigDecimal::from_str(s)?,
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                BigDecimal::from(u)
            } else if let Some(f) = n.as_f64() {
                BigDecimal::from_str(&f.to_string())?
            } else {
                return Err(anyhow::anyhow!("invalid amount format"));
            }
        }
        _ => return Err(anyhow::anyhow!("amount missing")),
    };

    Ok(WithdrawalPayload {
        withdrawal_id: Uuid::parse_str(json["withdrawal_id"].as_str().unwrap())?,
        merchant_id: Uuid::parse_str(json["merchant_id"].as_str().unwrap())?,
        token_mint: json["token_mint"].as_str().unwrap().to_string(),
        amount,
        target_address: json["target_address"].as_str().unwrap().to_string(),
        created_at: json["created_at"].as_str().unwrap().to_string(),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut conn = client.get_connection()?;
    let mut store = Store::new()?;

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
            if let Ok(payload) = parse_withdrawal_payload(&data_str) {
                process_withdrawal(&mut store, &payload, &mut conn, group, &stream, &redis_id)
                    .await?;
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
            if let Ok(payload) = parse_withdrawal_payload(&data_str) {
                process_withdrawal(&mut store, &payload, &mut conn, group, &stream, &redis_id)
                    .await?;
            }
        }
    }
}

async fn process_withdrawal(
    store: &mut Store,
    payload: &WithdrawalPayload,
    conn: &mut redis::Connection,
    group: &str,
    stream: &str,
    redis_id: &str,
) -> Result<()> {
    let withdrawal_id = payload.withdrawal_id;

    println!(
        "📌 Processing withdrawal {} from stream {} (Redis ID: {})",
        withdrawal_id, stream, redis_id
    );

    // Update status to 'processing'
    let _ = store.update_withdrawal_status(withdrawal_id, "processing", None)?;
    println!("⏳ Withdrawal {} status set to 'processing'", withdrawal_id);

    // Get the fat wallet
    let fat_wallet = store.get_fat_wallet()?;
    let mpc_url = format!("http://127.0.0.1:3000/wallets/{}/send", fat_wallet.id);
    println!("📡 Sending request to MPC at {}", mpc_url);

    // Determine if token or SOL
    let is_sol = payload.token_mint == "So11111111111111111111111111111111111111112";
    let (mint_param, token_param) = if is_sol {
        (None, None)
    } else {
        (
            Some(payload.token_mint.as_str()),
            Some(payload.token_mint.as_str()),
        )
    };

    // Convert amount to smallest unit
    let amount_u64 = if is_sol {
        let lamports_per_sol = 1_000_000_000u64;
        (payload.amount.clone() * BigDecimal::from(lamports_per_sol))
            .to_u64()
            .unwrap_or(0)
    } else {
        let decimals = store.get_token_decimals(&payload.token_mint)?; // fetch decimals from DB
        let factor = BigDecimal::from(10u64.pow(decimals as u32));
        (payload.amount.clone() * factor).to_u64().unwrap_or(0)
    };

    println!("💰 Original payload amount: {}", payload.amount);
    println!("💰 Converted to u64 smallest unit: {}", amount_u64);

    // Build request body for MPC
    let req_body = if mint_param.is_some() {
        serde_json::json!({
            "to": payload.target_address,
            "amount": amount_u64,
            "mint": mint_param,
            "token": token_param
        })
    } else {
        serde_json::json!({
            "to": payload.target_address,
            "amount": amount_u64
        })
    };

    println!("📤 MPC request body: {}", req_body);

    // Send request
    let cli = Client::new();
    match cli.post(&mpc_url).json(&req_body).send().await {
        Ok(r) => {
            if r.status().is_success() {
                let json: Value = r.json().await.unwrap_or_default();
                if let Some(sig) = json.get("signature").and_then(|s| s.as_str()) {
                    store.finalize_withdrawal_success(withdrawal_id, sig)?;
                    let _: () = redis::cmd("XACK")
                        .arg(stream)
                        .arg(group)
                        .arg(redis_id)
                        .query(conn)?;
                    println!(
                        "✅ Withdrawal {} completed successfully. TxSig={}",
                        withdrawal_id, sig
                    );
                } else {
                    store.finalize_withdrawal_failed(withdrawal_id, "mpc_no_sig")?;
                    let _: () = redis::cmd("XACK")
                        .arg(stream)
                        .arg(group)
                        .arg(redis_id)
                        .query(conn)?;
                    println!(
                        "❌ Withdrawal {} failed: no signature returned",
                        withdrawal_id
                    );
                }
            } else {
                let body = r.text().await.unwrap_or_default();
                store.finalize_withdrawal_failed(withdrawal_id, &format!("mpc_error: {}", body))?;
                let _: () = redis::cmd("XACK")
                    .arg(stream)
                    .arg(group)
                    .arg(redis_id)
                    .query(conn)?;
                println!(
                    "❌ Withdrawal {} failed: MPC error: {}",
                    withdrawal_id, body
                );
            }
        }
        Err(e) => {
            println!(
                "🌐 Network error calling MPC for withdrawal {}: {:?}",
                withdrawal_id, e
            );
        }
    }

    Ok(())
}
