//! `mock-mpc` — the counting mock signer as an HTTP service (Phase 2 §3.3).
//!
//! The worker's real `MpcSigner` POSTs to `/wallets/{id}/send` with an `idempotency_key`
//! (the `withdrawal_id`). This server is the instrument Invariant #2 reads: it COUNTS every
//! `/send` call per key while DEDUPING the returned signature (the same key always yields the
//! same deterministic signature). A worker that blindly re-sends on redelivery drives a key's
//! count to 2 even though the signature is deduped — and the oracle catches it via `/__counts`.
//!
//! Endpoints:
//! - `POST /wallets/{id}/send` with body `{ "idempotency_key": "<uuid>", ... }` increments the
//!   per-key counter and returns `{ "signature": "<base58>" }` (the signature is deduped).
//! - `GET /lookup?key=<uuid>` returns the prior `{ "signature": ... | null }` WITHOUT counting
//!   (models the reconciliation path; the worker calls this instead of re-sending).
//! - `GET /__counts` returns `{ "<key>": <count>, ... }` for the oracle.
//!
//! The signature is byte-for-byte identical to `external::deterministic_signature`: the 16
//! UUID bytes tiled into 64 bytes, base58-encoded (Bitcoin/Solana alphabet), so the worker's
//! `Signature::from_str` round-trips it. base58 is hand-rolled to keep the dep set at
//! `actix-web` + `serde_json` (Phase 2 allowlist).

use std::collections::HashMap;
use std::sync::Mutex;

use actix_web::{App, HttpResponse, HttpServer, web};
use serde_json::{Value, json};

/// Per-key state: how many times `/send` was called, and the (deduped) signature returned.
#[derive(Default)]
struct Counts {
    inner: Mutex<HashMap<String, (usize, String)>>,
}

/// Derive the 16 seed bytes from an idempotency key. A canonical UUID string is hex-decoded
/// to its 16 bytes (matching `Uuid::as_bytes()`); any other string falls back to its raw bytes
/// (padded/truncated to 16) so the server never panics on hostile input — it stays deterministic.
fn seed_bytes(key: &str) -> [u8; 16] {
    let hex: Vec<u8> = key.chars().filter(|c| *c != '-').collect::<String>().into_bytes();
    let mut out = [0u8; 16];
    if hex.len() == 32 && hex.iter().all(|b| b.is_ascii_hexdigit()) {
        for (i, byte) in out.iter_mut().enumerate() {
            let hi = (hex[i * 2] as char).to_digit(16).unwrap_or(0) as u8;
            let lo = (hex[i * 2 + 1] as char).to_digit(16).unwrap_or(0) as u8;
            *byte = (hi << 4) | lo;
        }
    } else {
        let raw = key.as_bytes();
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = raw.get(i % raw.len().max(1)).copied().unwrap_or(0);
        }
    }
    out
}

/// The deterministic 64-byte signature for a key, base58-encoded — identical to
/// `external::deterministic_signature` so the worker parses it back to the same bytes.
fn deterministic_signature(key: &str) -> String {
    let seed = seed_bytes(key);
    let mut bytes = [0u8; 64];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = seed[i % 16];
    }
    base58_encode(&bytes)
}

/// Standard base58 encode with the Bitcoin/Solana alphabet (what `solana_sdk::Signature`'s
/// `Display`/`FromStr` use). Hand-rolled to avoid a `bs58` dependency.
fn base58_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 58] =
        b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let zeros = input.iter().take_while(|&&b| b == 0).count();
    let mut digits: Vec<u8> = Vec::new();
    for &byte in input {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) << 8;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut result = String::with_capacity(zeros + digits.len());
    for _ in 0..zeros {
        result.push('1');
    }
    for d in digits.iter().rev() {
        result.push(ALPHABET[*d as usize] as char);
    }
    if result.is_empty() {
        result.push('1');
    }
    result
}

/// `POST /wallets/{id}/send` — count this call for `idempotency_key`, return the deduped signature.
async fn send(_id: web::Path<String>, body: web::Bytes, state: web::Data<Counts>) -> HttpResponse {
    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(format!("invalid json: {e}")),
    };
    let key = match parsed.get("idempotency_key").and_then(Value::as_str) {
        Some(k) => k.to_string(),
        None => return HttpResponse::BadRequest().body("missing idempotency_key"),
    };

    let mut guard = state.inner.lock().expect("counts mutex poisoned");
    let entry = guard
        .entry(key.clone())
        .or_insert_with(|| (0, deterministic_signature(&key)));
    entry.0 += 1; // counts EVERY call (a re-send drives this to 2)
    let signature = entry.1.clone();
    HttpResponse::Ok().json(json!({ "signature": signature }))
}

/// `GET /lookup?key=<uuid>` — return the prior signature WITHOUT counting (reconciliation path).
async fn lookup(query: web::Query<HashMap<String, String>>, state: web::Data<Counts>) -> HttpResponse {
    let key = match query.get("key") {
        Some(k) => k,
        None => return HttpResponse::BadRequest().body("missing key"),
    };
    let guard = state.inner.lock().expect("counts mutex poisoned");
    let signature = guard.get(key).map(|(_, sig)| sig.clone());
    HttpResponse::Ok().json(json!({ "signature": signature }))
}

/// `GET /__counts` — the per-key call counts the oracle reads for Invariant #2.
async fn counts(state: web::Data<Counts>) -> HttpResponse {
    let guard = state.inner.lock().expect("counts mutex poisoned");
    let map: HashMap<&String, usize> = guard.iter().map(|(k, (c, _))| (k, *c)).collect();
    HttpResponse::Ok().json(map)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Bind to the address the worker's MPC_BASE_URL points at (default 127.0.0.1:8090).
    let addr = std::env::var("MOCK_MPC_ADDR").unwrap_or_else(|_| "127.0.0.1:8090".to_string());
    let state = web::Data::new(Counts::default());

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/wallets/{id}/send", web::post().to(send))
            .route("/lookup", web::get().to(lookup))
            .route("/__counts", web::get().to(counts))
    })
    .bind(&addr)?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_deterministic_and_base58() {
        let key = "00112233-4455-6677-8899-aabbccddeeff";
        let a = deterministic_signature(key);
        let b = deterministic_signature(key);
        assert_eq!(a, b, "same key → same signature (dedup contract)");
        // 64 bytes base58 → a non-empty string of alphabet chars only.
        assert!(!a.is_empty());
        assert!(a.chars().all(|c| "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(c)));
    }

    #[test]
    fn seed_matches_uuid_bytes() {
        // 00112233-4455-... decodes to bytes 0x00,0x11,0x22,... exactly like Uuid::as_bytes().
        let seed = seed_bytes("00112233-4455-6677-8899-aabbccddeeff");
        assert_eq!(seed[0], 0x00);
        assert_eq!(seed[1], 0x11);
        assert_eq!(seed[15], 0xff);
    }

    #[test]
    fn base58_known_vector() {
        // "hello world" → base58 (Bitcoin alphabet) is a well-known vector.
        assert_eq!(base58_encode(b"hello world"), "StV1DL6CwTryKyV");
        // All-zero input encodes to all '1's (one per leading zero byte).
        assert_eq!(base58_encode(&[0u8; 3]), "111");
    }
}
