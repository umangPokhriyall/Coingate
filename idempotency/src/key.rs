//! Idempotency key + request fingerprint.
//!
//! The fingerprint is the guard for the "same key, different payload" case (→ 409). It is a
//! pure function of the request and contains no I/O.

use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// The client-supplied `Idempotency-Key` header value. A newtype so a key cannot be confused
/// at a call site with some other string (e.g. the request path).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub String);

impl IdempotencyKey {
    /// Borrow the underlying key string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable fingerprint of a request: `sha256(method || '\n' || path || '\n' || raw_body)` as
/// lowercase hex.
///
/// MUST hash the **raw body bytes** exactly as received, *before* any deserialization, so that
/// (a) a semantically identical retry produces the identical fingerprint, and (b) any genuine
/// change to method, path, or payload produces a different one. The newline separators keep the
/// three fields unambiguous (so `"a" + "bc"` cannot collide with `"ab" + "c"`).
pub fn request_fingerprint(method: &str, path: &str, raw_body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(method.as_bytes());
    hasher.update(b"\n");
    hasher.update(path.as_bytes());
    hasher.update(b"\n");
    hasher.update(raw_body);
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // Infallible: writing to a String never errors.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_64_hex_chars() {
        let fp = request_fingerprint("POST", "/orders", b"{}");
        assert_eq!(fp.len(), 64);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn fingerprint_is_stable_across_calls() {
        let a = request_fingerprint("POST", "/orders", br#"{"amount":"1.50"}"#);
        let b = request_fingerprint("POST", "/orders", br#"{"amount":"1.50"}"#);
        assert_eq!(a, b, "identical inputs must yield identical fingerprints");
    }

    #[test]
    fn fingerprint_is_sensitive_to_method() {
        let post = request_fingerprint("POST", "/orders", b"{}");
        let put = request_fingerprint("PUT", "/orders", b"{}");
        assert_ne!(post, put);
    }

    #[test]
    fn fingerprint_is_sensitive_to_path() {
        let orders = request_fingerprint("POST", "/orders", b"{}");
        let withdrawals = request_fingerprint("POST", "/withdrawals", b"{}");
        assert_ne!(orders, withdrawals);
    }

    #[test]
    fn fingerprint_is_sensitive_to_body() {
        let one = request_fingerprint("POST", "/orders", br#"{"amount":"1.00"}"#);
        let two = request_fingerprint("POST", "/orders", br#"{"amount":"2.00"}"#);
        assert_ne!(one, two);
    }

    #[test]
    fn fingerprint_is_sensitive_to_raw_body_bytes() {
        // Whitespace that a JSON parser would ignore must still change the fingerprint, because
        // we hash the raw bytes before deserialization.
        let compact = request_fingerprint("POST", "/orders", br#"{"a":1}"#);
        let spaced = request_fingerprint("POST", "/orders", br#"{ "a": 1 }"#);
        assert_ne!(compact, spaced);
    }

    #[test]
    fn field_boundaries_are_unambiguous() {
        // Without the separators these two could collide; with them they must not.
        let a = request_fingerprint("POST", "/orders", b"x");
        let b = request_fingerprint("POST", "/order", b"sx");
        assert_ne!(a, b);
    }
}
