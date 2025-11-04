use actix_web::middleware::Logger;
use actix_web::{App, HttpServer, web};
use std::sync::{Arc, Mutex};
use store::store::Store;

mod routes;
use routes::merchant::{
    create_app, create_order, get_merchant, get_order, list_apps, sign_in, sign_up,
};
use routes::payment::{create_payment_tx, get_payment_details, get_payment_status};
use routes::wallet::create_fat_wallet;
use routes::withdraw::create_withdrawal;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "actix_web=info");
    // env_logger::init();

    let store = Arc::new(Mutex::new(Store::new().expect("DB connection failed")));

    HttpServer::new(move || {
        App::new().wrap(Logger::default()).service(
            web::scope("/api/v1")
                .app_data(web::Data::new(store.clone()))
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
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
