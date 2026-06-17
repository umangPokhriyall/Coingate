use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum SignerError {
    /// Network / ambiguous failure: we do not know whether the MPC actually sent. The
    /// withdrawal must stay `processing` so Phase 1.4 can reconcile via `lookup` (not re-send).
    #[error("transport error: {0}")]
    Transport(String),
    /// The MPC responded but rejected the request (non-success status). A definite failure.
    #[error("mpc rejected: {0}")]
    Rejected(String),
    /// The MPC responded success but returned no signature.
    #[error("mpc returned no signature")]
    NoSignature,
    /// The MPC returned a signature we could not parse.
    #[error("invalid signature: {0}")]
    InvalidSignature(String),
}

/// A request to move funds. `key` is THE effect-boundary idempotency key — the
/// `withdrawal_id` (Brief §3.3). It is frozen into this signature now even though the worker
/// does not reconcile until Phase 1.
#[derive(Debug, Clone)]
pub struct SendRequest<'a> {
    pub key: Uuid,
    pub to: &'a str,
    pub amount: u64,           // base units
    pub mint: Option<&'a str>, // None = native SOL
}

pub trait Signer {
    /// Perform the send. Implementations MUST treat `req.key` as the dedup key.
    async fn send(&self, req: SendRequest<'_>) -> Result<Signature, SignerError>;

    /// Reconciliation: return a prior result for this key WITHOUT performing a new send. The
    /// worker calls this from the ambiguous `processing` state instead of re-sending
    /// (Brief §3.3). Wired in Phase 1.4.
    async fn lookup(&self, key: Uuid) -> Result<Option<Signature>, SignerError>;
}

// ===================== Real impl =====================

/// Forwards `key` to the MPC `/send` endpoint as `idempotency_key`, documenting the dedup
/// contract the real signer MUST honor. Untouched beyond wrapping the existing HTTP call.
pub struct MpcSigner {
    /// Full per-wallet send URL, e.g. `http://127.0.0.1:3000/wallets/<id>/send`.
    send_url: String,
    http: reqwest::Client,
}

impl MpcSigner {
    pub fn new(send_url: String) -> Self {
        Self {
            send_url,
            http: reqwest::Client::new(),
        }
    }
}

impl Signer for MpcSigner {
    async fn send(&self, req: SendRequest<'_>) -> Result<Signature, SignerError> {
        // Body mirrors the previous worker call, plus the idempotency key.
        let mut body = serde_json::json!({
            "to": req.to,
            "amount": req.amount,
            "idempotency_key": req.key.to_string(),
        });
        if let Some(mint) = req.mint {
            body["mint"] = mint.into();
            body["token"] = mint.into();
        }

        let resp = self
            .http
            .post(&self.send_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| SignerError::Transport(e.to_string()))?;

        if !resp.status().is_success() {
            let detail = resp.text().await.unwrap_or_default();
            return Err(SignerError::Rejected(detail));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SignerError::Transport(e.to_string()))?;
        let sig_str = json
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or(SignerError::NoSignature)?;
        sig_str
            .parse::<Signature>()
            .map_err(|e| SignerError::InvalidSignature(e.to_string()))
    }

    async fn lookup(&self, _key: Uuid) -> Result<Option<Signature>, SignerError> {
        // Phase 0: the MPC reconciliation endpoint is not wired yet (Phase 1.4).
        Ok(None)
    }
}

// ===================== Mock (the proof's instrument) =====================

/// Deterministic signature derived from the key, so the same key always yields the same
/// signature without any randomness (Phase 2 needs reproducibility).
fn deterministic_signature(key: Uuid) -> Signature {
    let k = key.as_bytes(); // [u8; 16]
    let mut bytes = [0u8; 64];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = k[i % 16];
    }
    Signature::from(bytes)
}

/// * `send` COUNTS every invocation (this is what Invariant #2 asserts == 1 per key).
/// * `lookup` returns the prior signature and does NOT count.
///
/// A worker that re-sends on redelivery (instead of reconciling via `lookup`) drives the
/// count to 2 and fails the invariant.
#[derive(Default)]
pub struct CountingMockSigner {
    inner: Mutex<HashMap<Uuid, (usize, Signature)>>,
}

impl CountingMockSigner {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many times `send` has been called for `key` (0 if never).
    pub fn send_count(&self, key: Uuid) -> usize {
        self.inner
            .lock()
            .expect("CountingMockSigner mutex poisoned")
            .get(&key)
            .map(|(count, _)| *count)
            .unwrap_or(0)
    }
}

impl Signer for CountingMockSigner {
    async fn send(&self, req: SendRequest<'_>) -> Result<Signature, SignerError> {
        let mut guard = self
            .inner
            .lock()
            .expect("CountingMockSigner mutex poisoned");
        let entry = guard
            .entry(req.key)
            .or_insert_with(|| (0, deterministic_signature(req.key)));
        entry.0 += 1;
        Ok(entry.1)
    }

    async fn lookup(&self, key: Uuid) -> Result<Option<Signature>, SignerError> {
        Ok(self
            .inner
            .lock()
            .expect("CountingMockSigner mutex poisoned")
            .get(&key)
            .map(|(_, sig)| *sig))
    }
}
