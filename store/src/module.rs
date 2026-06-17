use crate::schema::{
    apps, audit_logs, balances, dead_letter, deposits, idempotency_keys, merchants, orders, outbox,
    wallets, withdrawals,
};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

// ============ Merchants ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = merchants)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Merchant {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub name: String,
    pub created_at: Option<NaiveDateTime>,
}

// ============ Apps ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = apps)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct App {
    pub id: Uuid,
    pub merchant_id: Option<Uuid>,
    pub title: String,
    pub callback_url: Option<String>,
    pub token_hash: String, // ✅ must always exist
    pub created_at: Option<NaiveDateTime>,
}

// ============ Orders ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = orders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Order {
    pub id: Uuid,
    pub app_id: Option<Uuid>,
    pub order_id: String, // NOT NULL: the inbound idempotency natural key (UNIQUE with app_id)
    pub price_amount: BigDecimal,
    pub price_currency: String,
    pub receive_currency: String,
    pub memo_id: String,
    pub status: String,
    pub tx_hash: Option<String>,
    pub callback_url: Option<String>,
    pub success_url: Option<String>,
    pub cancel_url: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub confirmed_at: Option<NaiveDateTime>,

    // ✅ Add these (match schema)
    pub selected_mint: Option<String>,
    pub expected_amount: Option<BigDecimal>,
    pub expected_decimals: Option<i32>,
}

// ============ Audit Logs ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = audit_logs)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct AuditLog {
    pub id: Uuid,
    pub entity: String,
    pub entity_id: Uuid,
    pub action: String,
    pub payload: Option<Value>,
    pub created_at: Option<NaiveDateTime>,
}

// ============ Wallets ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = wallets)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Wallet {
    pub id: Uuid,
    pub name: Option<String>,
    pub owner_type: Option<String>,
    pub owner_id: Option<Uuid>,
    pub chain: String,
    pub address: String,
    pub type_: String, // use type_ instead of r#type
    pub status: Option<String>,
    pub created_at: Option<NaiveDateTime>,
}

// ============ Deposits ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = deposits)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Deposit {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub tx_hash: String,
    pub chain: String,
    pub slot: Option<i64>,
    pub block_hash: Option<String>,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
    pub token_mint: Option<String>,
    pub token_symbol: Option<String>,
    pub token_decimals: Option<i32>,
    pub amount: BigDecimal,
    pub memo_id: Option<String>,
    pub status: String,
    pub confirmations: Option<i32>,
    pub raw: Option<serde_json::Value>,
    pub processed: Option<bool>,
    pub processing_attempts: Option<i32>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
    pub confirmed_at: Option<NaiveDateTime>,
}

// ============ Balances ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = balances)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Balance {
    pub id: Uuid,
    pub merchant_id: Uuid,
    pub token_mint: String,
    pub balance: Option<BigDecimal>,
    pub locked_balance: Option<BigDecimal>,
    pub updated_at: Option<NaiveDateTime>,
}

// ============ Withdrawals ============
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = withdrawals)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Withdrawal {
    pub id: Uuid,
    pub merchant_id: Uuid,
    pub token_mint: String,
    pub amount: BigDecimal,
    pub status: String, // pending, processing, completed, failed
    pub target_address: String,
    pub tx_hash: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

// ============ Forward tables (Phase 1 fills these — Phase 0 lands types only) ============

// ============ Idempotency Keys (Amendment 1 §A2) ============
#[derive(Queryable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = idempotency_keys)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct IdempotencyKeyRow {
    pub key: String,
    pub request_fingerprint: String,
    pub status: String, // 'in_progress' | 'completed'
    pub lease_deadline: Option<NaiveDateTime>,
    pub lease_owner: Option<Uuid>,
    pub response_snapshot: Option<Value>,
    pub response_status: Option<i16>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

// ============ Outbox ============
#[derive(Queryable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = outbox)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct OutboxRow {
    pub id: Uuid,
    pub topic: String,
    pub payload: Value,
    pub created_at: NaiveDateTime,
    pub sent_at: Option<NaiveDateTime>,
}

// ============ Dead Letter (poison-message sink) ============
#[derive(Queryable, Selectable, Serialize, Deserialize, Debug, Clone)]
#[diesel(table_name = dead_letter)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DeadLetter {
    pub id: Uuid,
    pub source_stream: String,
    pub raw: Value,
    pub reason: String,
    pub created_at: NaiveDateTime,
}
