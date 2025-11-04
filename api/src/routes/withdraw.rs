use crate::routes::merchant::validate_jwt; // reuse your existing JWT validation
use actix_web::{HttpRequest, HttpResponse, post, web};
use bigdecimal::BigDecimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use store::store::Store;
use uuid::Uuid;

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
    store: web::Data<Arc<Mutex<Store>>>,
) -> HttpResponse {
    // 1) Merchant verification via Bearer token
    let merchant_id = if let Some(auth_header) = http_req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if let Some(claims) = validate_jwt(token) {
                    match Uuid::parse_str(&claims.sub) {
                        Ok(uuid) => uuid,
                        Err(_) => return HttpResponse::Unauthorized().body("invalid merchant id"),
                    }
                } else {
                    return HttpResponse::Unauthorized().body("invalid token");
                }
            } else {
                return HttpResponse::Unauthorized().body("expected Bearer token");
            }
        } else {
            return HttpResponse::Unauthorized().finish();
        }
    } else {
        return HttpResponse::Unauthorized().finish();
    };

    // 2) Parse amount
    let amount_bd = match BigDecimal::from_str(&body.amount) {
        Ok(v) => v,
        Err(_) => return HttpResponse::BadRequest().body("invalid amount"),
    };

    let mut s = match store.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };

    // 3) Attempt to create withdrawal & lock funds
    let withdrawal = match s.create_withdrawal_and_lock(
        merchant_id,
        &body.token_mint,
        &amount_bd,
        &body.target_address,
    ) {
        Ok(wd) => wd,
        Err(e) => {
            println!("create_withdrawal_and_lock error: {:?}", e);
            return HttpResponse::BadRequest().body("insufficient balance or DB error");
        }
    };

    // 4) Push to Redis stream
    let client = match redis::Client::open("redis://127.0.0.1:6379") {
        Ok(c) => c,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("Redis client error: {:?}", e));
        }
    };

    let mut conn = match client.get_connection() {
        Ok(c) => c,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .body(format!("Redis connection error: {:?}", e));
        }
    };

    let payload = serde_json::json!({
        "withdrawal_id": withdrawal.id.to_string(),
        "merchant_id": merchant_id.to_string(),
        "token_mint": body.token_mint.clone(),
        "amount": body.amount,
        "target_address": body.target_address,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });

    println!("📤 Enqueuing withdrawal payload to Redis: {}", payload);

    let res: redis::RedisResult<String> = redis::cmd("XADD")
        .arg("withdrawal_requests")
        .arg("*")
        .arg("data")
        .arg(payload.to_string())
        .query(&mut conn);

    match res {
        Ok(_id) => HttpResponse::Ok().json(serde_json::json!({
            "withdrawal_id": withdrawal.id,
            "status": "pending"
        })),
        Err(e) => {
            println!("redis push failed: {:?}", e);
            // revert DB lock on failure
            if let Err(err) = s.revert_withdrawal_lock(merchant_id, &body.token_mint, &amount_bd) {
                println!("failed to revert lock after redis failure: {:?}", err);
            }
            HttpResponse::InternalServerError().body("failed to enqueue withdrawal")
        }
    }
}
