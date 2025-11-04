use anyhow::Result;
use dotenv::dotenv;
use redis::Commands;
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{
    EncodedTransaction, UiMessage, UiRawMessage, UiTransactionEncoding,
    option_serializer::OptionSerializer,
};
use std::str::FromStr;
use store::store::Store;
use tokio::time::{Duration, sleep};

#[derive(Debug)]
struct TransactionData {
    signature: String,
    slot: u64,
    block_time: Option<i64>,
    memo_id: Option<String>,
    transaction_type: String, // "SOL" or "TOKEN"
    from_address: Option<String>,
    to_address: Option<String>,
    amount: Option<u64>,
    token_mint: Option<String>,
    token_decimals: Option<u8>,
    status: String,
    logs: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Connect Redis
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let mut redis_conn = redis_client.get_connection()?;

    // Connect DB and get fat wallet
    let mut store = Store::new().expect("DB connection failed");
    let fat_wallet = store.get_fat_wallet().expect("fat wallet not configured");
    let fat_pubkey = Pubkey::from_str(&fat_wallet.address).expect("invalid pubkey");

    println!("🔑 Polling for wallet: {}", fat_pubkey);

    // Solana client
    let rpc = RpcClient::new_with_commitment(
        "https://api.devnet.solana.com".to_string(),
        CommitmentConfig::confirmed(),
    );

    // Get the initial latest signature to establish starting point
    let mut last_processed_sig: Option<String> = redis_conn.get("poller:last_sig").ok();

    // If no checkpoint exists, get the most recent transaction as starting point
    if last_processed_sig.is_none() {
        println!("🚀 No checkpoint found, getting latest transaction as starting point...");
        let latest_sigs = rpc
            .get_signatures_for_address_with_config(
                &fat_pubkey,
                GetConfirmedSignaturesForAddress2Config {
                    before: None,
                    until: None,
                    limit: Some(1),
                    commitment: Some(CommitmentConfig::confirmed()),
                },
            )
            .await?;

        if let Some(latest_sig) = latest_sigs.first() {
            last_processed_sig = Some(latest_sig.signature.clone());
            let _: () = redis_conn.set("poller:last_sig", &latest_sig.signature)?;
            println!("📍 Set checkpoint to: {}", latest_sig.signature);
        }
    }

    loop {
        // Get NEW transactions (those that came AFTER our last processed signature)
        let sigs = rpc
            .get_signatures_for_address_with_config(
                &fat_pubkey,
                GetConfirmedSignaturesForAddress2Config {
                    before: None, // Start from the newest
                    until: last_processed_sig
                        .clone()
                        .and_then(|s| s.parse::<Signature>().ok()), // Stop at our checkpoint
                    limit: Some(10), // Process up to 10 new transactions at once
                    commitment: Some(CommitmentConfig::confirmed()),
                },
            )
            .await?;

        if sigs.is_empty() {
            println!("😴 No new transactions, sleeping...");
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        println!("📜 Found {} NEW signatures", sigs.len());

        // Process transactions in reverse order (oldest first among the new ones)
        // This ensures we process them in chronological order
        for s in sigs.iter().rev() {
            println!("➡️ Processing sig: {}", s.signature);

            let Ok(signature) = s.signature.parse::<Signature>() else {
                println!("   ❌ Invalid signature format, skipping");
                continue;
            };

            let Ok(tx) = rpc
                .get_transaction(&signature, UiTransactionEncoding::Json)
                .await
            else {
                println!("   ❌ Failed to fetch transaction, skipping");
                continue;
            };

            if let Some(meta) = &tx.transaction.meta {
                if meta.err.is_some() {
                    println!("   ❌ Failed tx, skipping");
                    continue;
                }

                let mut tx_data = TransactionData {
                    signature: s.signature.clone(),
                    slot: tx.slot,
                    block_time: tx.block_time,
                    memo_id: None,
                    transaction_type: "UNKNOWN".to_string(),
                    from_address: None,
                    to_address: None,
                    amount: None,
                    token_mint: None,
                    token_decimals: None,
                    status: "SUCCESS".to_string(),
                    logs: vec![],
                };

                // Extract logs & memo
                if let OptionSerializer::Some(logs) = &meta.log_messages {
                    tx_data.logs = logs.clone();
                    for log in logs {
                        if log.starts_with("Program log: Memo (len") {
                            if let (Some(start), Some(end)) = (log.find('"'), log.rfind('"')) {
                                if start < end {
                                    tx_data.memo_id = Some(log[start + 1..end].to_string());
                                    println!(
                                        "   📝 Found memo: {}",
                                        tx_data.memo_id.as_ref().unwrap()
                                    );
                                }
                            }
                        }
                    }
                }

                if tx_data.memo_id.is_none() {
                    println!("   ⚠️ No memo found, skipping");
                    continue;
                }

                // Parse transaction details
                if let EncodedTransaction::Json(ui_tx) = &tx.transaction.transaction {
                    if let UiMessage::Raw(raw_msg) = &ui_tx.message {
                        parse_transaction_details(&mut tx_data, raw_msg, meta, &fat_wallet.address);
                    }
                }

                // Push to Redis stream as structured fields
                let _: String = redis_conn.xadd(
                    "payment_transactions",
                    "*",
                    &[
                        ("signature", tx_data.signature.as_str()),
                        ("memo_id", tx_data.memo_id.as_deref().unwrap_or("")),
                        ("transaction_type", tx_data.transaction_type.as_str()),
                        (
                            "from_address",
                            tx_data.from_address.as_deref().unwrap_or(""),
                        ),
                        ("to_address", tx_data.to_address.as_deref().unwrap_or("")),
                        ("amount", &tx_data.amount.unwrap_or(0).to_string()),
                        ("token_mint", tx_data.token_mint.as_deref().unwrap_or("")),
                        (
                            "token_decimals",
                            &tx_data.token_decimals.unwrap_or(0).to_string(),
                        ),
                        ("status", tx_data.status.as_str()),
                        ("logs", &serde_json::to_string(&tx_data.logs).unwrap()),
                    ],
                )?;

                println!("   ✅ Added tx to Redis stream: {:?}", tx_data);
            }

            // Update checkpoint after each successful processing
            last_processed_sig = Some(s.signature.clone());
            let _: () = redis_conn.set("poller:last_sig", &s.signature)?;
        }

        println!("🔄 Updated checkpoint to: {:?}", last_processed_sig);
        sleep(Duration::from_secs(3)).await;
    }
}

fn parse_transaction_details(
    tx_data: &mut TransactionData,
    raw_msg: &UiRawMessage,
    meta: &solana_transaction_status::UiTransactionStatusMeta,
    fat_wallet_address: &str,
) {
    let accounts = &raw_msg.account_keys;

    // --- TOKEN transfers ---
    if let (
        solana_transaction_status::option_serializer::OptionSerializer::Some(pre_token_balances),
        solana_transaction_status::option_serializer::OptionSerializer::Some(post_token_balances),
    ) = (&meta.pre_token_balances, &meta.post_token_balances)
    {
        if !post_token_balances.is_empty() {
            tx_data.transaction_type = "TOKEN".to_string();

            // Find the receiving account (fat wallet)
            if let Some(receiver) = post_token_balances.iter().find(|pb| {
                let owner: Option<String> = pb.owner.clone().into();
                owner.as_deref() == Some(fat_wallet_address)
            }) {
                // set receiver info
                tx_data.to_address = Some(fat_wallet_address.to_string());
                tx_data.token_mint = Some(receiver.mint.clone());
                tx_data.token_decimals = Some(receiver.ui_token_amount.decimals as u8);

                let post_amount = receiver.ui_token_amount.amount.parse::<u64>().unwrap_or(0);

                let pre_amount = pre_token_balances
                    .iter()
                    .find(|pb| pb.account_index == receiver.account_index)
                    .map(|pb| pb.ui_token_amount.amount.parse::<u64>().unwrap_or(0))
                    .unwrap_or(0);

                tx_data.amount = Some(post_amount.saturating_sub(pre_amount));

                // find the sender: account whose balance decreased
                if let Some(sender) = pre_token_balances.iter().find(|pb| {
                    let pre = pb.ui_token_amount.amount.parse::<u64>().unwrap_or(0);
                    let post = post_token_balances
                        .iter()
                        .find(|p| p.account_index == pb.account_index)
                        .map(|p| p.ui_token_amount.amount.parse::<u64>().unwrap_or(0))
                        .unwrap_or(0);
                    post < pre // sender's balance decreased
                }) {
                    tx_data.from_address = sender.owner.clone().into();
                }
            }

            if tx_data.amount.is_some() {
                return; // done processing token transfer
            }
        }
    }

    // --- SOL transfers ---
    let pre_balances = &meta.pre_balances;
    let post_balances = &meta.post_balances;

    tx_data.transaction_type = "SOL".to_string();

    if let Some(fat_wallet_index) = accounts.iter().position(|addr| addr == fat_wallet_address) {
        if fat_wallet_index < pre_balances.len() && fat_wallet_index < post_balances.len() {
            let pre_balance = pre_balances[fat_wallet_index];
            let post_balance = post_balances[fat_wallet_index];

            if post_balance > pre_balance {
                tx_data.amount = Some(post_balance - pre_balance);
                tx_data.to_address = Some(fat_wallet_address.to_string());

                if let Some(sender) = accounts.get(0) {
                    if sender != fat_wallet_address {
                        tx_data.from_address = Some(sender.clone());
                    }
                }
            }
        }
    }
}
