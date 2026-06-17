use crate::error::ApiError;
use crate::routes::merchant::{bearer_claims, merchant_uuid};
use actix_web::{HttpRequest, HttpResponse, post, web};
use bigdecimal::BigDecimal;
use serde::Deserialize;
use std::str::FromStr;

#[derive(Deserialize)]
pub struct CreateWithdrawalRequest {
    pub token_mint: String, // always a string; SOL can use its mint
    pub amount: String,     // string decimal; parsed to BigDecimal
    pub target_address: String,
}

#[post("/withdrawals")]
pub async fn create_withdrawal(
    http_req: HttpRequest,
    body: web::Json<CreateWithdrawalRequest>,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    // 1) Merchant verification via Bearer token
    let claims = bearer_claims(&http_req)?;
    let merchant_id = merchant_uuid(&claims)?;

    // 2) Parse amount (exact decimal; never f64). Withdrawal unit semantics unchanged.
    let amount_bd = BigDecimal::from_str(&body.amount)
        .map_err(|_| ApiError::BadRequest("invalid amount".into()))?;

    // 3) Create withdrawal & lock funds (single transaction entry point). A failure here is
    // insufficient funds or a DB error; both surface to the client as a 400, as before.
    let withdrawal = store::with_tx(&pool, |conn| {
        store::create_withdrawal_and_lock(
            conn,
            merchant_id,
            &body.token_mint,
            &amount_bd,
            &body.target_address,
        )
    })
    .map_err(|e| {
        tracing::warn!(error = %e, %merchant_id, "create_withdrawal_and_lock failed");
        ApiError::BadRequest("insufficient balance or DB error".into())
    })?;

    // 4) Push to Redis stream
    let client = redis::Client::open("redis://127.0.0.1:6379")
        .map_err(|e| ApiError::Internal(format!("redis client: {e}")))?;
    let mut conn = client
        .get_connection()
        .map_err(|e| ApiError::Internal(format!("redis connection: {e}")))?;

    let payload = serde_json::json!({
        "withdrawal_id": withdrawal.id.to_string(),
        "merchant_id": merchant_id.to_string(),
        "token_mint": body.token_mint.clone(),
        "amount": body.amount,
        "target_address": body.target_address,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });

    tracing::info!(withdrawal_id = %withdrawal.id, "enqueuing withdrawal to redis");

    let res: redis::RedisResult<String> = redis::cmd("XADD")
        .arg("withdrawal_requests")
        .arg("*")
        .arg("data")
        .arg(payload.to_string())
        .query(&mut conn);

    match res {
        Ok(_id) => Ok(HttpResponse::Ok().json(serde_json::json!({
            "withdrawal_id": withdrawal.id,
            "status": "pending"
        }))),
        Err(e) => {
            tracing::error!(error = %e, withdrawal_id = %withdrawal.id, "redis push failed");
            // revert DB lock on failure
            if let Err(err) = store::with_tx(&pool, |conn| {
                store::revert_withdrawal_lock(conn, merchant_id, &body.token_mint, &amount_bd)
            }) {
                tracing::error!(error = %err, "failed to revert lock after redis failure");
            }
            Err(ApiError::Internal("failed to enqueue withdrawal".into()))
        }
    }
}
