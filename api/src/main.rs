use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
use store::{build_pool, Config, Pool};
use tracing_subscriber::EnvFilter;

mod error;
mod idem;
mod routes;
use routes::merchant::{
    create_app, create_order, get_merchant, list_apps, set_jwt_secret, sign_in, sign_up, get_order,
};
use routes::payment::{create_payment_tx, get_payment_details, get_payment_status};
use routes::wallet::create_fat_wallet;
use routes::withdraw::create_withdrawal;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // Config is sourced entirely from the environment; fail fast if anything is missing.
    let cfg = Config::from_env().expect("invalid configuration (check required env vars)");

    // One pool for the whole process; shared via web::Data (no Arc<Mutex<Store>>).
    let pool: Pool = build_pool(&cfg).expect("failed to build database pool");
    let pool_data = web::Data::new(pool);

    // Hand the JWT signing secret to the auth helpers (was hardcoded).
    set_jwt_secret(cfg.jwt_secret.clone());

    let listen_addr = cfg.listen_addr.clone();

    HttpServer::new(move || {
        App::new().wrap(Logger::default()).service(
            web::scope("/api/v1")
                .app_data(pool_data.clone())
                .service(sign_up)
                .service(create_fat_wallet)
                .service(get_payment_details)
                .service(get_payment_status)
                .service(create_payment_tx)
                .service(sign_in)
                .service(get_merchant)
                .service(create_app)
                .service(create_withdrawal)
                .service(list_apps)
                .service(create_order)
                .service(get_order),
        )
    })
    .bind(listen_addr)?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use diesel::r2d2::ConnectionManager;
    use store::PgConnection;

    // A pool that is never expected to hand out a connection: every case below is rejected
    // (bad UUID / bad auth / bad body) before any DB access, so `build_unchecked` against a
    // bogus URL is enough and never blocks on a real connection.
    fn unchecked_pool() -> Pool {
        let manager = ConnectionManager::<PgConnection>::new("postgres://invalid:5432/invalid");
        diesel::r2d2::Pool::builder().build_unchecked(manager)
    }

    /// Every hostile request must yield a 4xx and, crucially, must never panic the worker
    /// thread (a panic would surface as a 500 from actix and is asserted against).
    #[actix_web::test]
    async fn hostile_input_never_panics_and_returns_4xx() {
        set_jwt_secret("test-secret".to_string());
        let pool = web::Data::new(unchecked_pool());

        let app = test::init_service(
            App::new().service(
                web::scope("/api/v1")
                    .app_data(pool.clone())
                    .service(sign_up)
                    .service(get_payment_details)
                    .service(get_payment_status)
                    .service(create_payment_tx)
                    .service(get_merchant)
                    .service(create_order)
                    .service(get_order)
                    .service(create_withdrawal),
            ),
        )
        .await;

        // (label, request) — all rejected before touching the DB.
        let requests = vec![
            (
                "malformed uuid in payment details",
                test::TestRequest::get().uri("/api/v1/payments/not-a-uuid"),
            ),
            (
                "malformed uuid in payment status",
                test::TestRequest::get().uri("/api/v1/payments/not-a-uuid/status"),
            ),
            (
                "malformed uuid in payment tx (valid body)",
                test::TestRequest::post()
                    .uri("/api/v1/payments/not-a-uuid/tx")
                    .set_json(serde_json::json!({ "from_address": "whatever" })),
            ),
            (
                "missing auth on /merchants/me",
                test::TestRequest::get().uri("/api/v1/merchants/me"),
            ),
            (
                "garbage bearer token on /merchants/me",
                test::TestRequest::get()
                    .uri("/api/v1/merchants/me")
                    .insert_header(("Authorization", "Bearer not.a.jwt")),
            ),
            (
                "missing auth on create order",
                test::TestRequest::post()
                    .uri("/api/v1/orders")
                    .set_json(serde_json::json!({
                        "order_id": "o1", "price_amount": "1.5",
                        "price_currency": "USD", "receive_currency": "USDC"
                    })),
            ),
            (
                "missing auth on get order",
                test::TestRequest::get().uri(&format!("/api/v1/orders/{}", uuid::Uuid::new_v4())),
            ),
            (
                "missing auth on withdrawal",
                test::TestRequest::post()
                    .uri("/api/v1/withdrawals")
                    .set_json(serde_json::json!({
                        "token_mint": "So11111111111111111111111111111111111111112",
                        "amount": "1.0", "target_address": "addr"
                    })),
            ),
            (
                "non-json body to signup",
                test::TestRequest::post()
                    .uri("/api/v1/merchants/signup")
                    .insert_header(("content-type", "application/json"))
                    .set_payload("this is not json"),
            ),
            (
                "missing fields in signup body",
                test::TestRequest::post()
                    .uri("/api/v1/merchants/signup")
                    .set_json(serde_json::json!({ "email": "a@b.c" })),
            ),
        ];

        for (label, req) in requests {
            let resp = test::call_service(&app, req.to_request()).await;
            let status = resp.status();
            assert!(
                status.is_client_error(),
                "[{label}] expected 4xx, got {status}"
            );
        }
    }
}
