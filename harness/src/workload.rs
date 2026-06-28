//! Workload driver primitives (Phase 2 §2 `workload.rs`): black-box setup + driving of the real
//! binaries. Preconditions are seeded with raw SQL (never the `store` schema); withdrawals are
//! driven over HTTP through the real `api`; deposits/withdrawals are injected by `XADD`
//! (simulating the poller / relay). Nothing here links a target crate.
//!
//! Money is integer base units throughout (every amount is an integer), so the oracles can read
//! `NUMERIC` sums back as `BIGINT` exactly.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::Engine;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Text;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::fixtures::{Db, Redis, quiescent};

/// SOL native mint — withdrawal flows use it so the worker's `to_base_units` takes the native
/// (1e9) path and needs no `deposits` decimals lookup.
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// A stable custom token for deposit (processor) flows: the order's `selected_mint` +
/// `expected_amount` make the processor's TOKEN verification pass.
pub const USDC_MINT: &str = "MINT-HARNESS-USDC";

/// Process environment the harness needs (read directly — never `store::Config`, to stay black-box).
#[derive(Debug, Clone)]
pub struct Env {
    pub db_url: String,
    pub redis_url: String,
    pub jwt_secret: String,
    pub listen_addr: String,
    pub mpc_base_url: String,
}

impl Env {
    pub fn load() -> Result<Env> {
        dotenv::dotenv().ok();
        let get = |k: &str| std::env::var(k).with_context(|| format!("missing env var {k}"));
        Ok(Env {
            db_url: get("DATABASE_URL")?,
            redis_url: get("REDIS_URL")?,
            jwt_secret: get("JWT_SECRET")?,
            listen_addr: get("LISTEN_ADDR")?,
            mpc_base_url: get("MPC_BASE_URL")?,
        })
    }

    pub fn api_base(&self) -> String {
        format!("http://{}", self.listen_addr)
    }

    /// `host:port` of the mock-mpc server (for `wait_for_port` and `/__counts`).
    pub fn mpc_authority(&self) -> String {
        self.mpc_base_url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split('/')
            .next()
            .unwrap_or(&self.mpc_base_url)
            .to_string()
    }
}

// ───────────────────────────── JWT (HS256, minted to match the api) ─────────────────────────

fn b64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

/// HMAC-SHA256 (RFC 2104) over `msg` with `key`, using the in-tree `sha2` (no `hmac` dep).
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= block[i];
        opad[i] ^= block[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    let mut out = [0u8; 32];
    out.copy_from_slice(&outer.finalize());
    out
}

/// Mint the bearer JWT the api validates (`Claims { sub = merchant_id, exp }`, HS256 over
/// `JWT_SECRET`). Lets the harness authenticate as a seeded merchant without a sign-up round-trip.
pub fn mint_merchant_jwt(secret: &str, merchant_id: Uuid) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() + 24 * 3600)
        .unwrap_or(0);
    let header = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = b64url(json!({ "sub": merchant_id.to_string(), "exp": exp }).to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let sig = b64url(&hmac_sha256(secret.as_bytes(), signing_input.as_bytes()));
    format!("{signing_input}.{sig}")
}

// ───────────────────────────────────── SQL seeding ─────────────────────────────────────────

/// A seeded merchant + app (the minimum to attribute deposits and authorize withdrawals).
#[derive(Debug, Clone)]
pub struct Merchant {
    pub id: Uuid,
    pub app_id: Uuid,
}

/// Seed a merchant + one app, returning their ids.
pub fn seed_merchant(db: &Db) -> Result<Merchant> {
    let merchant = Uuid::new_v4();
    let app = Uuid::new_v4();
    db.with_conn(|c| {
        sql_query(
            "INSERT INTO merchants (id, email, password_hash, name, created_at) \
             VALUES ($1::uuid, $2, 'x', 'harness', now())",
        )
        .bind::<Text, _>(merchant.to_string())
        .bind::<Text, _>(format!("m-{merchant}@harness.local"))
        .execute(c)?;
        sql_query(
            "INSERT INTO apps (id, merchant_id, title, token_hash, created_at) \
             VALUES ($1::uuid, $2::uuid, 'harness', $3, now())",
        )
        .bind::<Text, _>(app.to_string())
        .bind::<Text, _>(merchant.to_string())
        .bind::<Text, _>(format!("h-{app}"))
        .execute(c)?;
        Ok(())
    })?;
    Ok(Merchant { id: merchant, app_id: app })
}

/// Seed the system fat wallet the worker resolves at startup (`get_fat_wallet`).
pub fn seed_fat_wallet(db: &Db) -> Result<Uuid> {
    let id = Uuid::new_v4();
    db.with_conn(|c| {
        sql_query(
            "INSERT INTO wallets (id, name, owner_type, chain, address, type, status, created_at) \
             VALUES ($1::uuid, 'Fat', 'system', 'solana', $2, 'fat', 'active', now())",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(format!("fat-{id}"))
        .execute(c)?;
        Ok(())
    })?;
    Ok(id)
}

/// Fund `(merchant, token)` with `amount` base units the conservation-clean way: a confirmed
/// deposit (attributed via a fresh order on the merchant's app) plus the credited balance row.
/// Returns nothing; the funds are immediately available.
pub fn seed_funded_balance(db: &Db, m: &Merchant, token: &str, amount: i64) -> Result<()> {
    let order = Uuid::new_v4();
    let deposit = Uuid::new_v4();
    db.with_conn(|c| {
        sql_query(
            "INSERT INTO orders (id, app_id, order_id, price_amount, price_currency, \
             receive_currency, memo_id, status, created_at) \
             VALUES ($1::uuid, $2::uuid, $3, 0, 'USD', 'USDC', $4, 'paid', now())",
        )
        .bind::<Text, _>(order.to_string())
        .bind::<Text, _>(m.app_id.to_string())
        .bind::<Text, _>(format!("ord-{order}"))
        .bind::<Text, _>(format!("memo-{order}"))
        .execute(c)?;
        sql_query(
            "INSERT INTO deposits (id, order_id, tx_hash, chain, token_mint, token_decimals, \
             amount, status, processed, created_at) \
             VALUES ($1::uuid, $2::uuid, $3, 'solana', $4, 9, CAST($5 AS NUMERIC), 'confirmed', true, now())",
        )
        .bind::<Text, _>(deposit.to_string())
        .bind::<Text, _>(order.to_string())
        .bind::<Text, _>(format!("fund-{deposit}"))
        .bind::<Text, _>(token.to_string())
        .bind::<Text, _>(amount.to_string())
        .execute(c)?;
        sql_query(
            "INSERT INTO balances (id, merchant_id, token_mint, balance, locked_balance, updated_at) \
             VALUES (gen_random_uuid(), $1::uuid, $2, CAST($3 AS NUMERIC), 0, now())",
        )
        .bind::<Text, _>(m.id.to_string())
        .bind::<Text, _>(token.to_string())
        .bind::<Text, _>(amount.to_string())
        .execute(c)?;
        Ok(())
    })
}

/// A seeded order awaiting a deposit, with `selected_mint`/`expected_amount` so the processor's
/// TOKEN verification passes. Returns `(order_id, memo)`.
pub fn seed_order_awaiting_deposit(db: &Db, m: &Merchant, token: &str, amount: i64) -> Result<(Uuid, String)> {
    let order = Uuid::new_v4();
    let memo = format!("memo-{order}");
    let memo2 = memo.clone();
    db.with_conn(move |c| {
        sql_query(
            "INSERT INTO orders (id, app_id, order_id, price_amount, price_currency, \
             receive_currency, memo_id, status, selected_mint, expected_amount, expected_decimals, created_at) \
             VALUES ($1::uuid, $2::uuid, $3, CAST($4 AS NUMERIC), 'USD', 'USDC', $5, 'pending', $6, CAST($4 AS NUMERIC), 6, now())",
        )
        .bind::<Text, _>(order.to_string())
        .bind::<Text, _>(m.app_id.to_string())
        .bind::<Text, _>(format!("ord-{order}"))
        .bind::<Text, _>(amount.to_string())
        .bind::<Text, _>(memo2)
        .bind::<Text, _>(token.to_string())
        .execute(c)?;
        Ok(())
    })?;
    Ok((order, memo))
}

/// Seed a `pending` withdrawal and move `amount` from available to locked (as `/withdrawals`
/// would). Returns the withdrawal id and the inner stream payload the worker parses.
pub fn seed_pending_withdrawal(db: &Db, m: &Merchant, token: &str, amount: i64, target: &str) -> Result<(Uuid, Value)> {
    let wid = Uuid::new_v4();
    db.with_conn(|c| {
        sql_query(
            "INSERT INTO withdrawals (id, merchant_id, token_mint, amount, status, target_address, created_at, updated_at) \
             VALUES ($1::uuid, $2::uuid, $3, CAST($4 AS NUMERIC), 'pending', $5, now(), now())",
        )
        .bind::<Text, _>(wid.to_string())
        .bind::<Text, _>(m.id.to_string())
        .bind::<Text, _>(token.to_string())
        .bind::<Text, _>(amount.to_string())
        .bind::<Text, _>(target.to_string())
        .execute(c)?;
        sql_query(
            "UPDATE balances SET balance = balance - CAST($3 AS NUMERIC), \
             locked_balance = locked_balance + CAST($3 AS NUMERIC) \
             WHERE merchant_id = $1::uuid AND token_mint = $2",
        )
        .bind::<Text, _>(m.id.to_string())
        .bind::<Text, _>(token.to_string())
        .bind::<Text, _>(amount.to_string())
        .execute(c)?;
        Ok(())
    })?;
    let payload = withdrawal_payload(wid, m.id, token, amount, target);
    Ok((wid, payload))
}

/// Insert an unsent outbox row carrying a withdrawal publish-intent (what `/withdrawals` writes;
/// the relay drains it). Returns the outbox row id.
pub fn seed_outbox(db: &Db, payload: &Value) -> Result<Uuid> {
    let id = Uuid::new_v4();
    let payload_str = payload.to_string();
    db.with_conn(move |c| {
        sql_query(
            "INSERT INTO outbox (id, topic, payload, created_at) \
             VALUES ($1::uuid, 'withdrawal_requests', CAST($2 AS JSONB), now())",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(payload_str)
        .execute(c)?;
        Ok(())
    })?;
    Ok(id)
}

/// The inner withdrawal payload the worker parses off the stream (`data` field).
pub fn withdrawal_payload(wid: Uuid, merchant: Uuid, token: &str, amount: i64, target: &str) -> Value {
    json!({
        "withdrawal_id": wid.to_string(),
        "merchant_id": merchant.to_string(),
        "token_mint": token,
        "amount": amount.to_string(),
        "target_address": target,
        "created_at": "2026-01-01T00:00:00Z",
    })
}

/// Force-expire an idempotency key's lease so a replay takes the takeover branch (used by the
/// §A2 after-expiry schedule without sleeping out the 30s lease).
pub fn expire_idem_lease(db: &Db, key: &str) -> Result<()> {
    db.with_conn(|c| {
        sql_query("UPDATE idempotency_keys SET lease_deadline = now() - interval '1 hour' WHERE key = $1")
            .bind::<Text, _>(key.to_string())
            .execute(c)?;
        Ok(())
    })
}

// ─────────────────────────────────── Redis injection ───────────────────────────────────────

/// `XADD payment_transactions` a TOKEN deposit for `memo`/`token`/`amount` — the exact flat field
/// layout the poller produces and the processor consumes. Returns the stream id.
pub fn enqueue_deposit(r: &mut Redis, memo: &str, sig: &str, token: &str, amount: i64) -> Result<String> {
    let amount_s = amount.to_string();
    r.xadd(
        "payment_transactions",
        &[
            ("signature", sig),
            ("memo_id", memo),
            ("transaction_type", "TOKEN"),
            ("from_address", "harness-from"),
            ("to_address", "harness-to"),
            ("amount", &amount_s),
            ("token_mint", token),
            ("token_decimals", "6"),
            ("status", "SUCCESS"),
        ],
    )
}

/// `XADD withdrawal_requests` a withdrawal job — the single `data` field carrying the inner JSON,
/// exactly as the relay republishes an outbox row.
pub fn enqueue_withdrawal(r: &mut Redis, payload: &Value) -> Result<String> {
    let data = payload.to_string();
    r.xadd("withdrawal_requests", &[("data", &data)])
}

// ─────────────────────────────────────── HTTP driving ──────────────────────────────────────

/// Outcome of an HTTP POST that may race a process abort.
#[derive(Debug)]
pub enum PostOutcome {
    /// The server responded with this status + JSON body.
    Status(u16, Value),
    /// The connection was reset/refused — the armed process aborted mid-request (expected).
    ConnReset,
}

fn post_json(url: &str, headers: &[(&str, &str)], body: &Value, timeout: Duration) -> PostOutcome {
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()
        .expect("build http client");
    let mut req = client.post(url).json(body);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    match req.send() {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body = resp.json::<Value>().unwrap_or(Value::Null);
            PostOutcome::Status(status, body)
        }
        // A reset/refused connection is the armed-abort signature, not a harness error.
        Err(_) => PostOutcome::ConnReset,
    }
}

/// `POST /withdrawals` with a bearer JWT + Idempotency-Key. Tolerates a mid-request abort.
pub fn post_withdrawal(
    env: &Env,
    jwt: &str,
    key: &str,
    token: &str,
    amount: i64,
    target: &str,
) -> PostOutcome {
    let url = format!("{}/api/v1/withdrawals", env.api_base());
    let auth = format!("Bearer {jwt}");
    let body = json!({ "token_mint": token, "amount": amount.to_string(), "target_address": target });
    post_json(
        &url,
        &[("Authorization", &auth), ("Idempotency-Key", key)],
        &body,
        Duration::from_secs(10),
    )
}

// ───────────────────────────────────────── Drain ───────────────────────────────────────────

/// Poll until the system is quiescent (streams drained, no unsent outbox) or `timeout` elapses.
pub fn drain_to_quiescence(redis: &mut Redis, db: &Db, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if quiescent(redis, db)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until a stream has at least `n` group consumers (a spawned consumer has connected and
/// joined), or time out. Lets the harness confirm the armed service is live before asserting it
/// must abort.
pub fn wait_consumer(redis: &mut Redis, stream: &str, group: &str, n: usize, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        let count = group_consumers(redis, stream, group)?;
        if count >= n {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn group_consumers(redis: &mut Redis, stream: &str, group: &str) -> Result<usize> {
    let groups: redis::RedisResult<Vec<std::collections::HashMap<String, redis::Value>>> =
        redis::cmd("XINFO").arg("GROUPS").arg(stream).query(redis.raw());
    let groups = match groups {
        Ok(g) => g,
        Err(_) => return Ok(0),
    };
    for g in &groups {
        let name = g.get("name").and_then(|v| match v {
            redis::Value::Data(d) => std::str::from_utf8(d).ok().map(str::to_string),
            _ => None,
        });
        if name.as_deref() == Some(group) {
            let consumers = g.get("consumers").and_then(|v| match v {
                redis::Value::Int(i) => Some(*i as usize),
                _ => None,
            });
            return Ok(consumers.unwrap_or(0));
        }
    }
    Ok(0)
}
