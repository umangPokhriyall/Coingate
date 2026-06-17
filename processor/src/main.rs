use anyhow::{Result, anyhow};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{NaiveDateTime, Utc};
use redis::Value as RedisValue;
use serde_json::Value;
use std::collections::HashMap;
use store::module::Deposit;
use store::{Config, PgConnection, build_pool, get_conn};

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

fn parse_transaction_data(
    stream_id: &str,
    redis_id: &str,
    data_str: &str,
) -> Result<StreamTransaction> {
    let json: Value = serde_json::from_str(data_str)?;
    Ok(StreamTransaction {
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
    })
}

async fn process_transaction(conn: &mut PgConnection, tx: &StreamTransaction) -> Result<bool> {
    let memo = match &tx.memo_id {
        Some(m) => m.clone(),
        None => return Ok(false),
    };

    let order = match store::find_order_by_memo_id(conn, &memo) {
        Ok(o) => o,
        Err(_) => return Ok(false),
    };

    if order.status == "paid" {
        return Ok(true);
    }

    let verification_result = match tx.transaction_type.as_str() {
        "SOL" => verify_sol_transaction(&order, tx),
        "TOKEN" => verify_token_transaction(&order, tx),
        _ => Ok(false),
    }?;

    if verification_result {
        // ✅ Update order
        let mut updated_order = order.clone();
        updated_order.status = "paid".to_string();
        updated_order.tx_hash = Some(tx.signature.clone());
        updated_order.confirmed_at = Some(Utc::now().naive_utc());
        store::update_order(conn, updated_order)?;

        // ✅ Insert deposit
        let deposit_amount = BigDecimal::from(tx.amount.unwrap_or(0));
        let token_mint = tx
            .token_mint
            .clone()
            .unwrap_or_else(|| "So11111111111111111111111111111111111111112".to_string());

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
            amount: deposit_amount.clone(),
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
        store::insert_deposit(conn, deposit)?;

        // ✅ Update merchant balance
        if let Some(app_id) = order.app_id {
            let merchant_id = store::get_merchant_id_for_app(conn, app_id)?;
            store::upsert_balance(conn, merchant_id, &token_mint, &deposit_amount)?;
            println!("✅ Updated balance for merchant {}", merchant_id);
        }

        Ok(true)
    } else {
        Ok(false)
    }
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

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env().expect("invalid configuration (check required env vars)");
    let client = redis::Client::open(cfg.redis_url.clone())?;
    let mut conn = client.get_connection()?;
    let pool = build_pool(&cfg).expect("failed to build database pool");
    let mut db_conn = get_conn(&pool).expect("failed to get DB connection");

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
        Ok(_) => println!("✅ Created consumer group '{}'", group),
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
            match parse_transaction_data(&stream, &id, &data_str) {
                Ok(tx) => match process_transaction(&mut db_conn, &tx).await {
                    Ok(true) | Ok(false) => {
                        let _: () = redis::cmd("XACK")
                            .arg(&stream)
                            .arg(group)
                            .arg(&id)
                            .query(&mut conn)?;
                    }
                    Err(e) => println!("❌ Processing error: {}", e),
                },
                Err(e) => {
                    let _: () = redis::cmd("XACK")
                        .arg(&stream)
                        .arg(group)
                        .arg(&id)
                        .query(&mut conn)?;
                }
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
            match parse_transaction_data(&stream, &id, &data_str) {
                Ok(tx) => match process_transaction(&mut db_conn, &tx).await {
                    Ok(true) | Ok(false) => {
                        let _: () = redis::cmd("XACK")
                            .arg(&stream)
                            .arg(group)
                            .arg(&id)
                            .query(&mut conn)?;
                    }
                    Err(e) => println!("❌ Processing error: {}", e),
                },
                Err(e) => {
                    let _: () = redis::cmd("XACK")
                        .arg(&stream)
                        .arg(group)
                        .arg(&id)
                        .query(&mut conn)?;
                }
            }
        }
    }
}
