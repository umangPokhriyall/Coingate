use crate::error::ApiError;
use actix_web::{HttpResponse, get, post, web};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

use base64::{Engine as _, engine::general_purpose};
use bigdecimal::BigDecimal;
use num_traits::ToPrimitive;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{instruction::Instruction, message::Message, pubkey::Pubkey, system_instruction};

use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};
use spl_token::instruction::transfer_checked;

// NOTE (deferred to config wiring): the devnet RPC URL below is still a literal. Session 0.2
// is logging/de-panic/money; threading Config into this handler is left for the config pass.
const DEVNET_RPC_URL: &str = "https://api.devnet.solana.com";
const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"; // mainnet USDC
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

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
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let oid = Uuid::parse_str(&path.into_inner())
        .map_err(|_| ApiError::BadRequest("invalid order id".into()))?;
    let mut conn = store::get_conn(&pool)?;

    let order = store::find_order(&mut conn, oid).map_err(|_| ApiError::NotFound)?;
    // Convert base units back to human readable format for display
    let display_amount = order.price_amount.clone() / BigDecimal::from(1_000_000);

    Ok(HttpResponse::Ok().json(PaymentPageResponse {
        order_id: order.id.to_string(),
        amount: display_amount.to_string(),
        currency: order.receive_currency, // always USDC settlement
        status: order.status,
    }))
}

#[get("/payments/{order_id}/status")]
pub async fn get_payment_status(
    path: web::Path<String>,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let order_id = Uuid::parse_str(&path.into_inner())
        .map_err(|_| ApiError::BadRequest("invalid order id".into()))?;
    let mut conn = store::get_conn(&pool)?;

    let order = store::find_order(&mut conn, order_id).map_err(|_| ApiError::NotFound)?;
    let display_amount = order.price_amount.clone() / BigDecimal::from(1_000_000);

    tracing::debug!(order_id = %order.id, status = %order.status, "payment status queried");

    Ok(HttpResponse::Ok().json(PaymentPageResponse {
        order_id: order.id.to_string(),
        amount: display_amount.to_string(),
        currency: order.receive_currency,
        status: order.status,
    }))
}

/// Ask Jupiter how much `output_mint` a given amount of `input_mint` converts to.
async fn jupiter_out_amount(
    input_mint: &str,
    output_mint: &str,
    amount: u64,
) -> Result<u64, ApiError> {
    let url = format!(
        "https://lite-api.jup.ag/swap/v1/quote?inputMint={input_mint}&outputMint={output_mint}&amount={amount}&slippageBps=50",
    );
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| ApiError::Internal(format!("jupiter request: {e}")))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| ApiError::Internal(format!("jupiter json: {e}")))?;
    tracing::debug!(%output_mint, "jupiter quote received");
    resp["outAmount"]
        .as_str()
        .ok_or_else(|| ApiError::Internal("jupiter outAmount missing".into()))?
        .parse::<u64>()
        .map_err(|e| ApiError::Internal(format!("jupiter outAmount parse: {e}")))
}

#[post("/payments/{order_id}/tx")]
pub async fn create_payment_tx(
    path: web::Path<String>,
    req: web::Json<PaymentTxRequest>,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    let oid = Uuid::parse_str(&path.into_inner())
        .map_err(|_| ApiError::BadRequest("invalid order id".into()))?;
    let mut conn = store::get_conn(&pool)?;

    // 1. Fetch order
    let mut order = store::find_order(&mut conn, oid).map_err(|_| ApiError::NotFound)?;

    // 2. Fat wallet (merchant's settlement wallet)
    let fat_wallet = store::get_fat_wallet(&mut conn)
        .map_err(|_| ApiError::Internal("fat wallet not configured".into()))?;
    let fat_pubkey = Pubkey::from_str(&fat_wallet.address)
        .map_err(|_| ApiError::Internal("fat wallet address invalid".into()))?;

    // Client-supplied; a bad pubkey is a 400, not a panic.
    let from_pubkey = Pubkey::from_str(&req.from_address)
        .map_err(|_| ApiError::BadRequest("invalid sender pubkey".into()))?;
    let to_pubkey = fat_pubkey;

    let rpc = RpcClient::new(DEVNET_RPC_URL.to_string());
    let recent_blockhash = rpc
        .get_latest_blockhash()
        .await
        .map_err(|e| ApiError::Internal(format!("get_latest_blockhash: {e}")))?;

    // 3. Resolve amount: Convert USDC → user's token via Jupiter
    // order.price_amount is already in USDC base units (1000000 = 1 USDC)
    let usdc_amount_base_units = order
        .price_amount
        .to_u64()
        .ok_or_else(|| ApiError::Internal("order amount out of range".into()))?;

    let (expected_amount, decimals, mint_pubkey_opt) = if let Some(mint_str) = &req.mint {
        let mint_pubkey = Pubkey::from_str(mint_str)
            .map_err(|_| ApiError::BadRequest("invalid mint".into()))?;
        let supply = rpc
            .get_token_supply(&mint_pubkey)
            .await
            .map_err(|e| ApiError::Internal(format!("get_token_supply: {e}")))?;
        let decimals = supply.decimals;

        let expected_amount = if req.test.unwrap_or(false) {
            // Test mode: treat the provided mint as USDC equivalent (no conversion).
            usdc_amount_base_units
        } else {
            // Production mode: use Jupiter to get the conversion rate.
            jupiter_out_amount(USDC_MINT, mint_str, usdc_amount_base_units).await?
        };

        order.selected_mint = Some(mint_str.clone());
        order.expected_amount = Some(expected_amount.into());
        order.expected_decimals = Some(decimals.into());
        store::update_order(&mut conn, order.clone())
            .map_err(|e| ApiError::Internal(format!("update order: {e}")))?;

        (expected_amount, decimals, Some(mint_pubkey))
    } else {
        // SOL payment - use Jupiter to get SOL amount
        let out_amount = jupiter_out_amount(USDC_MINT, SOL_MINT, usdc_amount_base_units).await?;

        order.selected_mint = Some(SOL_MINT.to_string());
        order.expected_amount = Some(out_amount.into());
        order.expected_decimals = Some(9); // SOL has 9 decimals
        store::update_order(&mut conn, order.clone())
            .map_err(|e| ApiError::Internal(format!("update order: {e}")))?;

        // For SOL, return None as mint_pubkey_opt to trigger native SOL transfer
        (out_amount, 9, None)
    };

    let memo_program_id = Pubkey::from_str(MEMO_PROGRAM_ID)
        .map_err(|_| ApiError::Internal("memo program id invalid".into()))?;

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
        .map_err(|e| ApiError::Internal(format!("transfer_checked: {e}")))?;

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

        let memo_ix = Instruction {
            program_id: memo_program_id,
            accounts: vec![],
            data: order.memo_id.as_bytes().to_vec(),
        };

        Message::new_with_blockhash(&[ix, memo_ix], Some(&from_pubkey), &recent_blockhash)
    };

    let msg_bytes = message.serialize();
    let tx_base64 = general_purpose::STANDARD.encode(&msg_bytes);

    Ok(HttpResponse::Ok().json(PaymentTxResponse { tx_base64 }))
}
