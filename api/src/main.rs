use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
use store::{build_pool, Config, Pool};

mod routes;
use routes::merchant::{
    create_app, create_order, get_merchant, list_apps, set_jwt_secret, sign_in, sign_up, get_order,
};
use routes::payment::{create_payment_tx, get_payment_details, get_payment_status};
use routes::wallet::create_fat_wallet;
use routes::withdraw::create_withdrawal;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
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
