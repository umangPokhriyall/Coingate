use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::{
    EncodedTransaction, UiMessage, UiRawMessage, UiTransactionEncoding, UiTransactionStatusMeta,
};
use std::str::FromStr;
use std::sync::Mutex;

#[derive(thiserror::Error, Debug)]
pub enum ChainError {
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxKind {
    Sol,
    Token,
}

impl TxKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TxKind::Sol => "SOL",
            TxKind::Token => "TOKEN",
        }
    }
}

/// An opaque position in the chain's deposit history. For Solana this is the most recent
/// signature seen; the poller persists it as its checkpoint.
#[derive(Debug, Clone)]
pub struct Cursor {
    pub signature: String,
}

#[derive(Debug, Clone)]
pub struct DepositEvent {
    pub signature: String, // the natural idempotency key for the credit path
    pub slot: u64,
    pub memo_id: Option<String>,
    pub kind: TxKind,
    pub from: Option<String>,
    pub to: Option<String>,
    pub amount: Option<u64>, // base units
    pub token_mint: Option<String>,
    pub token_decimals: Option<u8>,
}

pub trait Chain {
    /// Return new deposit events plus the advanced cursor. A `None` cursor means nothing new
    /// (the caller should not advance its checkpoint).
    async fn deposits_since(
        &self,
        cursor: Option<Cursor>,
    ) -> Result<(Vec<DepositEvent>, Option<Cursor>), ChainError>;
}

// ===================== Real impl =====================

/// Wraps the current poller logic: an `RpcClient`, the fat wallet pubkey, and the
/// signature-scan + transaction-parse pipeline. Phase 0 preserves the existing behavior
/// exactly — pagination is NOT fixed here (that is Phase 1.5).
pub struct SolanaChain {
    rpc: RpcClient,
    fat_pubkey: Pubkey,
    fat_address: String,
}

impl SolanaChain {
    pub fn new(rpc_url: String, fat_address: String) -> Result<Self, ChainError> {
        let fat_pubkey = Pubkey::from_str(&fat_address)
            .map_err(|e| ChainError::Parse(format!("fat wallet address: {e}")))?;
        let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
        Ok(Self {
            rpc,
            fat_pubkey,
            fat_address,
        })
    }

    /// The newest signature on the account, used to bootstrap the checkpoint when none exists.
    pub async fn latest_cursor(&self) -> Result<Option<Cursor>, ChainError> {
        let sigs = self
            .rpc
            .get_signatures_for_address_with_config(
                &self.fat_pubkey,
                GetConfirmedSignaturesForAddress2Config {
                    before: None,
                    until: None,
                    limit: Some(1),
                    commitment: Some(CommitmentConfig::confirmed()),
                },
            )
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        Ok(sigs.first().map(|s| Cursor {
            signature: s.signature.clone(),
        }))
    }
}

impl Chain for SolanaChain {
    async fn deposits_since(
        &self,
        cursor: Option<Cursor>,
    ) -> Result<(Vec<DepositEvent>, Option<Cursor>), ChainError> {
        let until = cursor
            .as_ref()
            .and_then(|c| c.signature.parse::<Signature>().ok());

        let sigs = self
            .rpc
            .get_signatures_for_address_with_config(
                &self.fat_pubkey,
                GetConfirmedSignaturesForAddress2Config {
                    before: None, // newest first
                    until,        // stop at our checkpoint
                    limit: Some(10),
                    commitment: Some(CommitmentConfig::confirmed()),
                },
            )
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;

        // Advance the checkpoint to the newest signature SEEN (even ones we skip), matching
        // the previous poller behavior so skipped txs are not reprocessed.
        let new_cursor = sigs.first().map(|s| Cursor {
            signature: s.signature.clone(),
        });

        let mut events = Vec::new();
        // Process oldest-first among the new ones.
        for s in sigs.iter().rev() {
            let Ok(signature) = s.signature.parse::<Signature>() else {
                continue;
            };
            let Ok(tx) = self
                .rpc
                .get_transaction(&signature, UiTransactionEncoding::Json)
                .await
            else {
                continue;
            };
            let Some(meta) = &tx.transaction.meta else {
                continue;
            };
            if meta.err.is_some() {
                continue;
            }
            // Only memo'd deposits are actionable downstream; skip the rest (as before).
            let Some(memo) = extract_memo(meta) else {
                continue;
            };

            let mut event = DepositEvent {
                signature: s.signature.clone(),
                slot: tx.slot,
                memo_id: Some(memo),
                kind: TxKind::Sol,
                from: None,
                to: None,
                amount: None,
                token_mint: None,
                token_decimals: None,
            };
            if let EncodedTransaction::Json(ui_tx) = &tx.transaction.transaction
                && let UiMessage::Raw(raw_msg) = &ui_tx.message
            {
                parse_into_event(&mut event, raw_msg, meta, &self.fat_address);
            }
            events.push(event);
        }

        Ok((events, new_cursor))
    }
}

/// Extract a memo string from a transaction's log messages, if present.
fn extract_memo(meta: &UiTransactionStatusMeta) -> Option<String> {
    if let OptionSerializer::Some(logs) = &meta.log_messages {
        for log in logs {
            if log.starts_with("Program log: Memo (len")
                && let (Some(start), Some(end)) = (log.find('"'), log.rfind('"'))
                && start < end
            {
                return Some(log[start + 1..end].to_string());
            }
        }
    }
    None
}

/// Fill `from`/`to`/`amount`/`kind`/token fields from the raw message + meta. Ported verbatim
/// from the previous poller `parse_transaction_details` (logic unchanged).
fn parse_into_event(
    event: &mut DepositEvent,
    raw_msg: &UiRawMessage,
    meta: &UiTransactionStatusMeta,
    fat_wallet_address: &str,
) {
    let accounts = &raw_msg.account_keys;

    // --- TOKEN transfers ---
    if let (OptionSerializer::Some(pre_token_balances), OptionSerializer::Some(post_token_balances)) =
        (&meta.pre_token_balances, &meta.post_token_balances)
        && !post_token_balances.is_empty()
    {
        event.kind = TxKind::Token;

        if let Some(receiver) = post_token_balances.iter().find(|pb| {
            let owner: Option<String> = pb.owner.clone().into();
            owner.as_deref() == Some(fat_wallet_address)
        }) {
            event.to = Some(fat_wallet_address.to_string());
            event.token_mint = Some(receiver.mint.clone());
            event.token_decimals = Some(receiver.ui_token_amount.decimals);

            let post_amount = receiver.ui_token_amount.amount.parse::<u64>().unwrap_or(0);
            let pre_amount = pre_token_balances
                .iter()
                .find(|pb| pb.account_index == receiver.account_index)
                .map(|pb| pb.ui_token_amount.amount.parse::<u64>().unwrap_or(0))
                .unwrap_or(0);
            event.amount = Some(post_amount.saturating_sub(pre_amount));

            if let Some(sender) = pre_token_balances.iter().find(|pb| {
                let pre = pb.ui_token_amount.amount.parse::<u64>().unwrap_or(0);
                let post = post_token_balances
                    .iter()
                    .find(|p| p.account_index == pb.account_index)
                    .map(|p| p.ui_token_amount.amount.parse::<u64>().unwrap_or(0))
                    .unwrap_or(0);
                post < pre
            }) {
                event.from = sender.owner.clone().into();
            }
        }

        if event.amount.is_some() {
            return;
        }
    }

    // --- SOL transfers ---
    let pre_balances = &meta.pre_balances;
    let post_balances = &meta.post_balances;
    event.kind = TxKind::Sol;

    if let Some(fat_wallet_index) = accounts.iter().position(|addr| addr == fat_wallet_address)
        && fat_wallet_index < pre_balances.len()
        && fat_wallet_index < post_balances.len()
    {
        let pre_balance = pre_balances[fat_wallet_index];
        let post_balance = post_balances[fat_wallet_index];

        if post_balance > pre_balance {
            event.amount = Some(post_balance - pre_balance);
            event.to = Some(fat_wallet_address.to_string());

            if let Some(sender) = accounts.first()
                && sender != fat_wallet_address
            {
                event.from = Some(sender.clone());
            }
        }
    }
}

// ===================== Mock (scriptable) =====================

/// Deterministic, scriptable chain. Drains its scripted events on the first `deposits_since`
/// call (a burst), then returns nothing. Can be loaded with duplicates/bursts for Phase 2
/// schedules.
pub struct MockChain {
    script: Vec<DepositEvent>,
    pos: Mutex<usize>,
}

impl MockChain {
    pub fn new(script: Vec<DepositEvent>) -> Self {
        Self {
            script,
            pos: Mutex::new(0),
        }
    }
}

impl Chain for MockChain {
    async fn deposits_since(
        &self,
        _cursor: Option<Cursor>,
    ) -> Result<(Vec<DepositEvent>, Option<Cursor>), ChainError> {
        let mut pos = self.pos.lock().expect("MockChain mutex poisoned");
        let remaining: Vec<DepositEvent> = self.script[*pos..].to_vec();
        *pos = self.script.len();
        let cursor = remaining.last().map(|e| Cursor {
            signature: e.signature.clone(),
        });
        Ok((remaining, cursor))
    }
}
