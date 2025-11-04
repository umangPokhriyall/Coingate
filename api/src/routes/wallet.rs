use actix_web::{HttpResponse, Responder, post, web};
use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use store::module::Wallet;
use store::store::Store;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateWalletResponse {
    pub wallet_id: Uuid,
    pub pubkey: String,
}

#[post("/wallets/fat")]
pub async fn create_fat_wallet(store: web::Data<Arc<Mutex<Store>>>) -> impl Responder {
    // 1. Call MPC service (mocked here)
    let (wallet_id, pubkey) = match mpc_create_wallet().await {
        Ok(res) => res,
        Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
    };

    // 2. Insert into DB

    let wallet = Wallet {
        id: wallet_id,
        name: Some("Fat Wallet".into()),
        owner_type: Some("system".into()),
        owner_id: None,
        chain: "solana".into(),
        address: pubkey.clone(), // instead of pubkey field
        type_: "fat".into(),     // use type_
        status: Some("active".into()),
        created_at: Some(chrono::Utc::now().naive_utc()),
    };

    let mut s = store.lock().unwrap();
    match s.insert_wallet(wallet) {
        Ok(w) => HttpResponse::Ok().json(CreateWalletResponse {
            wallet_id: w.id,
            pubkey: w.address,
        }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Deserialize)]
struct MpcWalletResponse {
    wallet_id: String,
    aggregate_pubkey: String,
}

pub async fn mpc_create_wallet() -> Result<(Uuid, String)> {
    let client = Client::new();

    // Example: request a 2-of-2 wallet
    let body = serde_json::json!({
        "threshold": 2,
        "participants": 2
    });

    let resp = client
        .post("http://127.0.0.1:3000/wallets")
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to call MPC service: {}", e))?;

    if !resp.status().is_success() {
        return Err(anyhow!("MPC service returned error: {}", resp.status()));
    }

    let parsed: MpcWalletResponse = resp
        .json()
        .await
        .map_err(|e| anyhow!("Failed to parse MPC response: {}", e))?;

    let id = Uuid::parse_str(&parsed.wallet_id)
        .map_err(|e| anyhow!("Invalid wallet_id UUID from MPC: {}", e))?;

    Ok((id, parsed.aggregate_pubkey))
}
