use crate::error::ApiError;
use actix_web::{HttpRequest, HttpResponse, get, post, web};
use bcrypt::{hash, verify};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use store::module::*;
use std::sync::OnceLock;
use uuid::Uuid;

// USDC settlement precision (6 decimals). Orders are stored in integer base units.
const USDC_DECIMALS: u32 = 6;

// JWT signing secret, initialized once from Config at startup (was hardcoded).
static JWT_SECRET: OnceLock<Vec<u8>> = OnceLock::new();

/// Install the JWT secret. Called once from `main` with `Config::jwt_secret`.
pub fn set_jwt_secret(secret: String) {
    let _ = JWT_SECRET.set(secret.into_bytes());
}

fn jwt_secret() -> &'static [u8] {
    JWT_SECRET
        .get()
        .map(|v| v.as_slice())
        .expect("JWT secret not initialized (call set_jwt_secret in main)")
}

// ====== JWT ======
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

fn generate_jwt(sub: &str) -> Result<String, ApiError> {
    let expiration = Utc::now()
        .checked_add_signed(Duration::hours(24))
        .ok_or_else(|| ApiError::Internal("token expiry overflow".into()))?
        .timestamp();

    let claims = Claims {
        sub: sub.to_string(),
        exp: expiration as usize,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret()),
    )
    .map_err(|e| ApiError::Internal(format!("jwt encode: {e}")))
}

pub fn validate_jwt(token: &str) -> Option<Claims> {
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret()),
        &Validation::new(Algorithm::HS256),
    )
    .ok()
    .map(|data| data.claims)
}

// ====== Auth header helpers (return typed errors, never panic) ======
pub fn bearer_claims(req: &HttpRequest) -> Result<Claims, ApiError> {
    let header = req.headers().get("Authorization").ok_or(ApiError::Unauthorized)?;
    let value = header.to_str().map_err(|_| ApiError::Unauthorized)?;
    let token = value.strip_prefix("Bearer ").ok_or(ApiError::Unauthorized)?;
    validate_jwt(token).ok_or(ApiError::Unauthorized)
}

fn app_token_from(req: &HttpRequest) -> Result<String, ApiError> {
    let header = req.headers().get("Authorization").ok_or(ApiError::Unauthorized)?;
    let value = header.to_str().map_err(|_| ApiError::Unauthorized)?;
    let token = value.strip_prefix("Token ").ok_or(ApiError::Unauthorized)?;
    Ok(token.to_string())
}

pub fn merchant_uuid(claims: &Claims) -> Result<Uuid, ApiError> {
    Uuid::parse_str(&claims.sub).map_err(|_| ApiError::Unauthorized)
}

// ====== Requests/Responses ======
#[derive(Debug, Deserialize)]
pub struct SignUpRequest {
    pub email: String,
    pub password: String,
    pub name: String,
}
#[derive(Debug, Serialize)]
pub struct SignUpResponse {
    pub merchant_id: String,
    pub email: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct SignInRequest {
    pub email: String,
    pub password: String,
}
#[derive(Debug, Serialize)]
pub struct SignInResponse {
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct MerchantResponse {
    pub merchant_id: String,
    pub email: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateAppRequest {
    pub title: String,
    pub callback_url: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct CreateAppResponse {
    pub app_id: String,
    pub app_token: String,
    pub title: String,
}
#[derive(Debug, Serialize)]
pub struct AppListResponse {
    pub apps: Vec<CreateAppResponse>,
}

#[derive(Debug, Deserialize)]
pub struct CreateOrderRequest {
    pub order_id: String,
    // Exact money: a decimal **string** (e.g. "1.50"), never an f64.
    pub price_amount: String,
    pub price_currency: String,
    pub receive_currency: String,
    pub callback_url: Option<String>,
    pub success_url: Option<String>,
    pub cancel_url: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct CreateOrderResponse {
    pub id: String,
    pub payment_url: String,
    pub memo_id: String,
    pub status: String,
    // Echoed back as the original decimal string (no f64).
    pub amount: String,
    pub receive_currency: String,
}
#[derive(Debug, Serialize)]
pub struct OrderResponse {
    pub id: String,
    pub status: String,
    // Stored base units as a string (no f64).
    pub amount: String,
    pub receive_currency: String,
}

// ====== Handlers ======
#[post("/merchants/signup")]
pub async fn sign_up(
    req: web::Json<SignUpRequest>,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let mut conn = store::get_conn(&pool)?;

    let hashed_pw = hash(&req.password, 4).map_err(|e| ApiError::Internal(format!("bcrypt: {e}")))?;
    let merchant = Merchant {
        id: Uuid::new_v4(),
        email: req.email.clone(),
        password_hash: hashed_pw,
        name: req.name.clone(),
        created_at: None,
    };

    let inserted = store::insert_merchant(&mut conn, merchant)
        .map_err(|e| ApiError::Internal(format!("insert merchant: {e}")))?;
    Ok(HttpResponse::Ok().json(SignUpResponse {
        merchant_id: inserted.id.to_string(),
        email: inserted.email,
        name: inserted.name,
    }))
}

#[post("/merchants/signin")]
pub async fn sign_in(
    req: web::Json<SignInRequest>,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let mut conn = store::get_conn(&pool)?;

    let merchant =
        store::find_merchant_by_email(&mut conn, &req.email).map_err(|_| ApiError::Unauthorized)?;

    let ok = verify(&req.password, &merchant.password_hash)
        .map_err(|e| ApiError::Internal(format!("bcrypt: {e}")))?;
    if !ok {
        return Err(ApiError::Unauthorized);
    }

    let token = generate_jwt(&merchant.id.to_string())?;
    Ok(HttpResponse::Ok().json(SignInResponse { token }))
}

#[get("/merchants/me")]
pub async fn get_merchant(
    req: HttpRequest,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let claims = bearer_claims(&req)?;
    let merchant_id = merchant_uuid(&claims)?;
    let mut conn = store::get_conn(&pool)?;

    let merchant =
        store::find_merchant_by_id(&mut conn, merchant_id).map_err(|_| ApiError::NotFound)?;
    Ok(HttpResponse::Ok().json(MerchantResponse {
        merchant_id: merchant.id.to_string(),
        email: merchant.email,
        name: merchant.name,
    }))
}

#[post("/apps")]
pub async fn create_app(
    req: web::Json<CreateAppRequest>,
    http_req: HttpRequest,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let claims = bearer_claims(&http_req)?;
    let merchant_id = merchant_uuid(&claims)?;
    let mut conn = store::get_conn(&pool)?;

    let app_id = Uuid::new_v4();
    // Token format: "<app_id>.<secret>". The app_id is the indexed lookup id used by
    // find_app_by_token; the secret part is what bcrypt protects.
    let app_token = format!("{}.{}", app_id, Uuid::new_v4());
    let token_hash =
        hash(&app_token, 4).map_err(|e| ApiError::Internal(format!("bcrypt: {e}")))?;

    let app = App {
        id: app_id,
        merchant_id: Some(merchant_id),
        title: req.title.clone(),
        callback_url: req.callback_url.clone(),
        token_hash,
        created_at: None,
    };

    let inserted = store::insert_app(&mut conn, app)
        .map_err(|e| ApiError::Internal(format!("insert app: {e}")))?;
    Ok(HttpResponse::Ok().json(CreateAppResponse {
        app_id: inserted.id.to_string(),
        app_token,
        title: inserted.title,
    }))
}

#[get("/apps")]
pub async fn list_apps(
    http_req: HttpRequest,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let claims = bearer_claims(&http_req)?;
    let merchant_id = merchant_uuid(&claims)?;
    let mut conn = store::get_conn(&pool)?;

    let apps = store::list_apps_by_merchant(&mut conn, merchant_id)
        .map_err(|e| ApiError::Internal(format!("list apps: {e}")))?;
    let response = apps
        .into_iter()
        .map(|a| CreateAppResponse {
            app_id: a.id.to_string(),
            app_token: "hidden".to_string(), // don't leak token
            title: a.title,
        })
        .collect();
    Ok(HttpResponse::Ok().json(AppListResponse { apps: response }))
}

#[post("/orders")]
pub async fn create_order(
    req: web::Json<CreateOrderRequest>,
    http_req: HttpRequest,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let app_token = app_token_from(&http_req)?;
    let mut conn = store::get_conn(&pool)?;

    let app = store::find_app_by_token(&mut conn, &app_token).map_err(|_| ApiError::Unauthorized)?;

    let id = Uuid::new_v4();
    let memo_id = Uuid::new_v4().to_string();
    let payment_url = format!("https://pay.gateway.com/pay/{}", id);

    // Exact decimal-string -> integer base units (no f64 round-trip).
    let price_amount_base_units = store::parse_base_units(&req.price_amount, USDC_DECIMALS)
        .map_err(|e| ApiError::BadRequest(format!("invalid price_amount: {e}")))?;

    let order = Order {
        id,
        app_id: Some(app.id),
        order_id: Some(req.order_id.clone()),
        price_amount: price_amount_base_units,
        price_currency: req.price_currency.clone(),
        receive_currency: req.receive_currency.clone(),
        memo_id: memo_id.clone(),
        status: "pending".to_string(),
        tx_hash: None,
        selected_mint: None,
        expected_amount: None,
        expected_decimals: None,
        callback_url: req.callback_url.clone(),
        success_url: req.success_url.clone(),
        cancel_url: req.cancel_url.clone(),
        created_at: None,
        confirmed_at: None,
    };

    let inserted = store::insert_order(&mut conn, order)
        .map_err(|e| ApiError::Internal(format!("insert order: {e}")))?;
    Ok(HttpResponse::Ok().json(CreateOrderResponse {
        id: inserted.id.to_string(),
        payment_url,
        memo_id,
        status: inserted.status,
        amount: req.price_amount.clone(),
        receive_currency: inserted.receive_currency,
    }))
}

#[get("/orders/{id}")]
pub async fn get_order(
    path: web::Path<String>,
    http_req: HttpRequest,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let app_token = app_token_from(&http_req)?;
    let oid = Uuid::parse_str(&path.into_inner())
        .map_err(|_| ApiError::BadRequest("invalid order id".into()))?;
    let mut conn = store::get_conn(&pool)?;

    let app = store::find_app_by_token(&mut conn, &app_token).map_err(|_| ApiError::Unauthorized)?;

    let order = store::find_order_by_app(&mut conn, oid, app.id).map_err(|_| ApiError::NotFound)?;
    Ok(HttpResponse::Ok().json(OrderResponse {
        id: order.id.to_string(),
        status: order.status,
        amount: order.price_amount.to_string(),
        receive_currency: order.receive_currency,
    }))
}
