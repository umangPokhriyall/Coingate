use actix_web::{HttpResponse, Responder, get, post, web};
use serde::{Deserialize, Serialize};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use store::store::Store;
use uuid::Uuid;

use base64::{Engine as _, engine::general_purpose};
use bigdecimal::BigDecimal;
use num_traits::ToPrimitive;
use reqwest;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{instruction::Instruction, message::Message, pubkey::Pubkey, system_instruction};

use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};
use spl_token::instruction::transfer_checked;

#[derive(Deserialize)]
pub struct PaymentTxRequest {
    pub from_address: String,
    pub mint: Option<String>, // token user pays with (None = SOL)
    pub test: Option<bool>,   // if true, treat mint as USDC equivalent (for testing)
}

#[derive(Serialize)]
pub struct PaymentTxResponse {
    pub tx_base64: String,
}

#[derive(serde::Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
pub struct PaymentPageResponse {
    pub order_id: String,
    pub amount: String,
    pub currency: String,
    pub status: String,
}

#[get("/payments/{order_id}")]
pub async fn get_payment_details(
    path: web::Path<String>,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    let oid = Uuid::parse_str(&path.into_inner()).unwrap();
    let mut store = store.lock().unwrap();

    match store.find_order(oid) {
        Ok(order) => {
            // Convert base units back to human readable format for display
            let display_amount =
                order.price_amount.clone() / bigdecimal::BigDecimal::from(1_000_000);

            HttpResponse::Ok().json(PaymentPageResponse {
                order_id: order.id.to_string(),
                amount: display_amount.to_string(),
                currency: order.receive_currency, // always USDC settlement
                status: order.status,
            })
        }
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[get("/payments/{order_id}/status")]
pub async fn get_payment_status(
    path: web::Path<String>,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    // Parse UUID
    let order_id = match Uuid::parse_str(&path.into_inner()) {
        Ok(id) => id,
        Err(_) => return HttpResponse::BadRequest().body("Invalid order ID"),
    };

    // Access store
    let mut store = store.lock().unwrap();

    match store.find_order(order_id) {
        Ok(order) => {
            // Convert base units to display amount
            let display_amount = order.price_amount.clone() / BigDecimal::from(1_000_000);

            // Since status is NOT nullable, no need for Option
            println!("Order {} current status: {}", order.id, order.status);

            HttpResponse::Ok().json(PaymentPageResponse {
                order_id: order.id.to_string(),
                amount: display_amount.to_string(),
                currency: order.receive_currency,
                status: order.status, // just return it
            })
        }
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[post("/payments/{order_id}/tx")]
pub async fn create_payment_tx(
    path: web::Path<String>,
    req: web::Json<PaymentTxRequest>,
    store: web::Data<Arc<Mutex<Store>>>,
) -> impl Responder {
    let oid = Uuid::parse_str(&path.into_inner()).unwrap();
    let mut s = match store.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            eprintln!("⚠️ Store mutex poisoned, recovering...");
            poisoned.into_inner()
        }
    };

    // 1. Fetch order
    let mut order = match s.find_order(oid) {
        Ok(o) => o,
        Err(_) => {
            return HttpResponse::NotFound().json(ErrorResponse {
                error: "order not found".to_string(),
            });
        }
    };

    // 2. Fat wallet (merchant's settlement wallet)
    let fat_wallet = match s.get_fat_wallet() {
        Ok(w) => w,
        Err(_) => {
            return HttpResponse::InternalServerError().json(ErrorResponse {
                error: "fat wallet not configured".to_string(),
            });
        }
    };
    let fat_pubkey = Pubkey::from_str(&fat_wallet.address).unwrap();

    let from_pubkey = Pubkey::from_str(&req.from_address).expect("invalid sender pubkey");
    let to_pubkey = fat_pubkey;

    let rpc = RpcClient::new("https://api.devnet.solana.com".to_string());
    let recent_blockhash = rpc.get_latest_blockhash().await.unwrap();

    // 3. Resolve amount: Convert USDC → user's token via Jupiter
    let (expected_amount, decimals, mint_pubkey_opt) = {
        let usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"; // mainnet USDC

        // order.price_amount is already in USDC base units (1000000 = 1 USDC)
        let usdc_amount_base_units = order.price_amount.to_u64().unwrap();

        if let Some(mint_str) = &req.mint {
            // Check if this is a test transaction
            if req.test.unwrap_or(false) {
                // Test mode: treat the provided mint as USDC equivalent
                let mint_pubkey = Pubkey::from_str(&mint_str).unwrap();
                let supply = rpc.get_token_supply(&mint_pubkey).await.unwrap();
                let decimals = supply.decimals;

                // Use the USDC amount directly (no conversion needed in test mode)
                let expected_amount = usdc_amount_base_units;

                order.selected_mint = Some(mint_str.clone());
                order.expected_amount = Some(expected_amount.into());
                order.expected_decimals = Some(decimals.into());
                s.update_order(order.clone()).unwrap();

                (expected_amount, decimals, Some(mint_pubkey))
            } else {
                // Production mode: use Jupiter to get conversion rate
                let jup_url = format!(
                    "https://lite-api.jup.ag/swap/v1/quote?inputMint={}&outputMint={}&amount={}&slippageBps=50",
                    usdc_mint, mint_str, usdc_amount_base_units,
                );

                let resp = reqwest::get(&jup_url)
                    .await
                    .unwrap()
                    .json::<serde_json::Value>()
                    .await
                    .unwrap();

                let out_amount = resp["outAmount"].as_str().unwrap().parse::<u64>().unwrap();

                // fetch decimals for output mint
                let mint_pubkey = Pubkey::from_str(&mint_str).unwrap();
                let supply = rpc.get_token_supply(&mint_pubkey).await.unwrap();
                let decimals = supply.decimals;

                order.selected_mint = Some(mint_str.clone());
                order.expected_amount = Some(out_amount.into());
                order.expected_decimals = Some(decimals.into());
                s.update_order(order.clone()).unwrap();

                (out_amount, decimals, Some(mint_pubkey))
            }
        } else {
            // SOL payment - use Jupiter to get SOL amount
            let sol_mint = "So11111111111111111111111111111111111111112";
            let jup_url = format!(
                "https://lite-api.jup.ag/swap/v1/quote?inputMint={}&outputMint={}&amount={}&slippageBps=50",
                usdc_mint, sol_mint, usdc_amount_base_units,
            );

            let resp = reqwest::get(&jup_url)
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
            println!("Jupiter API response: {:?}", resp);

            let out_amount = resp["outAmount"].as_str().unwrap().parse::<u64>().unwrap();

            order.selected_mint = Some(sol_mint.to_string());
            order.expected_amount = Some(out_amount.into());
            order.expected_decimals = Some(9.into()); // SOL has 9 decimals
            s.update_order(order.clone()).unwrap();

            // For SOL, return None as mint_pubkey_opt to trigger native SOL transfer
            (out_amount, 9, None)
        }
    };

    // 4. Build transaction
    let message = if let Some(mint_pubkey) = mint_pubkey_opt {
        let sender_token_account = get_associated_token_address(&from_pubkey, &mint_pubkey);
        let recipient_token_account = get_associated_token_address(&to_pubkey, &mint_pubkey);

        let create_recipient_ata_ix = create_associated_token_account_idempotent(
            &from_pubkey,
            &to_pubkey,
            &mint_pubkey,
            &spl_token::ID,
        );

        let transfer_ix = transfer_checked(
            &spl_token::ID,
            &sender_token_account,
            &mint_pubkey,
            &recipient_token_account,
            &from_pubkey,
            &[],
            expected_amount,
            decimals,
        )
        .unwrap();

        let memo_program_id =
            Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap();
        let memo_ix = Instruction {
            program_id: memo_program_id,
            accounts: vec![],
            data: order.memo_id.as_bytes().to_vec(),
        };

        Message::new_with_blockhash(
            &[create_recipient_ata_ix, transfer_ix, memo_ix],
            Some(&from_pubkey),
            &recent_blockhash,
        )
    } else {
        let ix = system_instruction::transfer(&from_pubkey, &to_pubkey, expected_amount);

        let memo_program_id =
            Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap();
        let memo_ix = Instruction {
            program_id: memo_program_id,
            accounts: vec![],
            data: order.memo_id.as_bytes().to_vec(),
        };

        Message::new_with_blockhash(&[ix, memo_ix], Some(&from_pubkey), &recent_blockhash)
    };

    let msg_bytes = message.serialize();
    let tx_base64 = general_purpose::STANDARD.encode(&msg_bytes);

    HttpResponse::Ok().json(PaymentTxResponse { tx_base64 })
}
