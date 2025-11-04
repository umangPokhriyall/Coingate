use actix_web::web::Data;
use actix_web::{HttpRequest, HttpResponse, Responder, get, post, web};
use bcrypt::{hash, verify};
use bigdecimal::BigDecimal;
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use store::module::*;
use store::store::Store;
use uuid::Uuid;

const JWT_SECRET: &[u8] = b"super-secret-key"; // TODO: load from env/config

// ====== JWT ======
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

fn generate_jwt(sub: &str) -> String {
    let expiration = Utc::now()
        .checked_add_signed(Duration::hours(24))
        .unwrap()
        .timestamp();

    let claims = Claims {
        sub: sub.to_string(),
        exp: expiration as usize,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET),
    )
    .unwrap()
}

pub fn validate_jwt(token: &str) -> Option<Claims> {
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET),
        &Validation::new(Algorithm::HS256),
    )
    .ok()
    .map(|data| data.claims)
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
    pub price_amount: f64,
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
    pub amount: f64,
    pub receive_currency: String,
}
#[derive(Debug, Serialize)]
pub struct OrderResponse {
    pub id: String,
    pub status: String,
    pub amount: f64,
    pub receive_currency: String,
}

// ====== Handlers ======
#[post("/merchants/signup")]
pub async fn sign_up(
    req: web::Json<SignUpRequest>,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    let mut store = store.lock().unwrap();

    let hashed_pw = hash(&req.password, 4).unwrap();
    let merchant = Merchant {
        id: Uuid::new_v4(),
        email: req.email.clone(),
        password_hash: hashed_pw,
        name: req.name.clone(),
        created_at: None,
    };

    match store.insert_merchant(merchant) {
        Ok(inserted) => HttpResponse::Ok().json(SignUpResponse {
            merchant_id: inserted.id.to_string(),
            email: inserted.email,
            name: inserted.name,
        }),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[post("/merchants/signin")]
pub async fn sign_in(
    req: web::Json<SignInRequest>,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    let mut store = store.lock().unwrap();

    match store.find_merchant_by_email(&req.email) {
        Ok(merchant) => {
            if verify(&req.password, &merchant.password_hash).unwrap() {
                let token = generate_jwt(&merchant.id.to_string());
                HttpResponse::Ok().json(SignInResponse { token })
            } else {
                HttpResponse::Unauthorized().finish()
            }
        }
        Err(_) => HttpResponse::Unauthorized().finish(),
    }
}

#[get("/merchants/me")]
pub async fn get_merchant(req: HttpRequest, store: web::Data<Arc<Mutex<Store>>>) -> impl Responder {
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if let Some(claims) = validate_jwt(token) {
                    let mut store = store.lock().unwrap();
                    if let Ok(merchant) =
                        store.find_merchant_by_id(Uuid::parse_str(&claims.sub).unwrap())
                    {
                        return HttpResponse::Ok().json(MerchantResponse {
                            merchant_id: merchant.id.to_string(),
                            email: merchant.email,
                            name: merchant.name,
                        });
                    }
                }
            }
        }
    }
    HttpResponse::Unauthorized().finish()
}

#[post("/apps")]
pub async fn create_app(
    req: web::Json<CreateAppRequest>,
    http_req: HttpRequest,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    if let Some(auth_header) = http_req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if let Some(claims) = validate_jwt(token) {
                    let mut store = store.lock().unwrap();
                    let app_id = Uuid::new_v4();
                    let app_token = Uuid::new_v4().to_string();

                    let app = App {
                        id: app_id,
                        merchant_id: Some(Uuid::parse_str(&claims.sub).unwrap()),
                        title: req.title.clone(),
                        callback_url: req.callback_url.clone(),
                        token_hash: hash(&app_token, 4).unwrap(),
                        created_at: None,
                    };

                    match store.insert_app(app) {
                        Ok(inserted) => {
                            return HttpResponse::Ok().json(CreateAppResponse {
                                app_id: inserted.id.to_string(),
                                app_token,
                                title: inserted.title,
                            });
                        }
                        Err(_) => return HttpResponse::InternalServerError().finish(),
                    }
                }
            }
        }
    }
    HttpResponse::Unauthorized().finish()
}

#[get("/apps")]
pub async fn list_apps(
    http_req: HttpRequest,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    if let Some(auth_header) = http_req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if let Some(claims) = validate_jwt(token) {
                    let mut store = store.lock().unwrap();
                    if let Ok(apps) =
                        store.list_apps_by_merchant(Uuid::parse_str(&claims.sub).unwrap())
                    {
                        let response = apps
                            .into_iter()
                            .map(|a| CreateAppResponse {
                                app_id: a.id.to_string(),
                                app_token: "hidden".to_string(), // don’t leak token
                                title: a.title,
                            })
                            .collect();
                        return HttpResponse::Ok().json(AppListResponse { apps: response });
                    }
                }
            }
        }
    }
    HttpResponse::Unauthorized().finish()
}
#[post("/orders")]
pub async fn create_order(
    req: web::Json<CreateOrderRequest>,
    http_req: HttpRequest,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    if let Some(auth_header) = http_req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(app_token) = auth_str.strip_prefix("Token ") {
                let mut store = store.lock().unwrap();

                // Validate app_token
                match store.find_app_by_token(app_token) {
                    Ok(app) => {
                        let id = Uuid::new_v4();
                        let memo_id = Uuid::new_v4().to_string();
                        let payment_url = format!("https://pay.gateway.com/pay/{}", id);

                        // Convert price_amount to USDC base units (6 decimals)
                        // If price_amount is 1.0, this becomes 1000000
                        let price_amount_decimal =
                            BigDecimal::from_str(&req.price_amount.to_string()).unwrap();
                        let usdc_decimals = BigDecimal::from(1_000_000); // 10^6 for USDC
                        let price_amount_base_units = price_amount_decimal * usdc_decimals;

                        let order = Order {
                            id,
                            app_id: Some(app.id),
                            order_id: Some(req.order_id.clone()),
                            price_amount: price_amount_base_units, // Now stored in base units
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

                        match store.insert_order(order) {
                            Ok(inserted) => {
                                return HttpResponse::Ok().json(CreateOrderResponse {
                                    id: inserted.id.to_string(),
                                    payment_url,
                                    memo_id,
                                    status: inserted.status,
                                    amount: req.price_amount, // Return original amount for response
                                    receive_currency: inserted.receive_currency,
                                });
                            }
                            Err(_) => return HttpResponse::InternalServerError().finish(),
                        }
                    }
                    Err(_) => return HttpResponse::Unauthorized().body("Invalid app token"),
                }
            }
        }
    }

    HttpResponse::Unauthorized().finish()
}

#[get("/orders/{id}")]
pub async fn get_order(
    path: web::Path<String>,
    http_req: HttpRequest,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    if let Some(auth_header) = http_req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(app_token) = auth_str.strip_prefix("Token ") {
                let mut store = store.lock().unwrap();

                // Validate app_token
                match store.find_app_by_token(app_token) {
                    Ok(app) => {
                        let oid = Uuid::parse_str(&path.into_inner()).unwrap();

                        match store.find_order_by_app(oid, app.id) {
                            Ok(order) => {
                                return HttpResponse::Ok().json(OrderResponse {
                                    id: order.id.to_string(),
                                    status: order.status,
                                    amount: order.price_amount.to_string().parse().unwrap(),
                                    receive_currency: order.receive_currency,
                                });
                            }
                            Err(_) => return HttpResponse::NotFound().finish(),
                        }
                    }
                    Err(_) => return HttpResponse::Unauthorized().body("Invalid app token"),
                }
            }
        }
    }

    HttpResponse::Unauthorized().finish()
}
