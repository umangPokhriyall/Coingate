//! The I/O boundary: the [`IdempotencyStore`] trait and the record types that cross it.
//!
//! The trait is the seam between this sans-IO crate and the Postgres implementation in `store`
//! (Session 1.2). Nothing here performs I/O — these are the *shapes* the implementation must
//! satisfy.
//!
//! ## A note on the connection and error types
//!
//! The Phase 1 spec writes `complete`'s signature with `&mut PgConnection` and
//! `diesel::result::Error`, and `acquire`/`takeover`/`read` against a `StoreError`. A literal
//! transcription is impossible here: this crate's hard rule is that no `diesel` type may appear
//! in its manifest or its source. We honour the *contract* while keeping the boundary real:
//!
//! * the database/transaction handle threaded through [`complete`](IdempotencyStore::complete)
//!   is the associated type [`IdempotencyStore::Conn`] — the Postgres impl binds it to
//!   `diesel::PgConnection`;
//! * a single concrete [`StoreError`] (sans-IO; carries a message) is the error for every
//!   method — the Postgres impl maps `diesel`/`r2d2` errors into it.
//!
//! `complete` still takes `&mut Self::Conn` (no `&self`) so it runs *inside* the caller's
//! `with_tx`, committing atomically with the guarded effect.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The status of an idempotency key record. `in_progress` means a request holding a lease is
/// executing; `completed` means a response snapshot has been durably captured.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyStatus {
    InProgress,
    Completed,
}

/// A persisted idempotency key, as the store materializes it for a decision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyRecord {
    pub status: KeyStatus,
    /// Fingerprint of the request that first acquired this key (see [`crate::request_fingerprint`]).
    pub request_fingerprint: String,
    /// When the in-progress lease expires. `None` once completed (or for a malformed record).
    pub lease_deadline: Option<DateTime<Utc>>,
    /// The owner that currently holds (or last held) the lease.
    pub lease_owner: Option<Uuid>,
    /// The captured response body, present once `status == Completed`.
    pub response_snapshot: Option<serde_json::Value>,
    /// The captured HTTP status, present once `status == Completed`.
    pub response_status: Option<i16>,
}

/// Result of an `INSERT ... ON CONFLICT (key) DO NOTHING` acquire attempt.
#[derive(Clone, Debug, PartialEq)]
pub enum Acquire {
    /// We inserted the row and now own the key — proceed to Execute.
    Acquired,
    /// The key already existed — consult [`crate::decide`] with this record.
    Existing(KeyRecord),
}

/// The single, sans-IO error type for every [`IdempotencyStore`] method. The Postgres impl maps
/// `diesel`/`r2d2` errors into [`StoreError::Backend`]; an unreadable persisted record maps to
/// [`StoreError::Malformed`].
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The underlying store (connection pool / database) failed.
    #[error("idempotency store backend error: {0}")]
    Backend(String),
    /// A persisted record could not be interpreted as a [`KeyRecord`].
    #[error("malformed idempotency key record: {0}")]
    Malformed(String),
}

/// The I/O boundary for the inbound-key (§A2) protocol. Implemented against `idempotency_keys`
/// in Session 1.2.
pub trait IdempotencyStore {
    /// The database/transaction handle threaded through [`complete`](Self::complete). The
    /// Postgres impl binds this to `diesel::PgConnection`.
    type Conn;

    /// Try to claim `key`. Its OWN committed statement (`INSERT ... ON CONFLICT (key) DO NOTHING
    /// RETURNING`), so concurrent replays SEE the `in_progress` row and never block: a fresh
    /// insert returns [`Acquire::Acquired`]; a pre-existing key returns
    /// [`Acquire::Existing`] with the current record.
    fn acquire(
        &self,
        key: &str,
        fingerprint: &str,
        lease_deadline: DateTime<Utc>,
        owner: Uuid,
    ) -> Result<Acquire, StoreError>;

    /// Atomic CAS lease takeover:
    /// `UPDATE ... SET lease_deadline = new_lease, lease_owner = owner
    ///  WHERE key = ? AND status = 'in_progress' AND lease_deadline < now RETURNING *`.
    /// `Some(record)` means we won the lease; `None` means someone else took it or it completed
    /// (the caller re-reads and re-decides).
    fn takeover(
        &self,
        key: &str,
        owner: Uuid,
        new_lease: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<KeyRecord>, StoreError>;

    /// Conditional completion, run **inside** the caller's `with_tx` (hence `&mut Self::Conn`,
    /// not `&self`): `UPDATE ... SET status = 'completed', response_snapshot = ?,
    /// response_status = ? WHERE key = ? AND status = 'in_progress' AND lease_owner = owner`.
    /// Returns `true` iff exactly one row changed (we won and may commit the effect); `false`
    /// means a takeover beat us — the caller rolls back its effect and replays the winner's
    /// snapshot.
    fn complete(
        conn: &mut Self::Conn,
        key: &str,
        owner: Uuid,
        snapshot: &serde_json::Value,
        status: i16,
    ) -> Result<bool, StoreError>;

    /// Read the current record for `key`, if any.
    fn read(&self, key: &str) -> Result<Option<KeyRecord>, StoreError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// A tiny in-memory [`IdempotencyStore`] used only to prove the trait is implementable with
    /// **zero I/O** — the sans-IO boundary is real. `Conn` binds to a plain map, standing in for
    /// the live transaction that the Postgres impl threads through `complete`.
    #[derive(Default)]
    struct MockStore {
        rows: RefCell<HashMap<String, KeyRecord>>,
    }

    impl IdempotencyStore for MockStore {
        type Conn = HashMap<String, KeyRecord>;

        fn acquire(
            &self,
            key: &str,
            fingerprint: &str,
            lease_deadline: DateTime<Utc>,
            owner: Uuid,
        ) -> Result<Acquire, StoreError> {
            let mut rows = self.rows.borrow_mut();
            if let Some(existing) = rows.get(key) {
                return Ok(Acquire::Existing(existing.clone()));
            }
            rows.insert(
                key.to_string(),
                KeyRecord {
                    status: KeyStatus::InProgress,
                    request_fingerprint: fingerprint.to_string(),
                    lease_deadline: Some(lease_deadline),
                    lease_owner: Some(owner),
                    response_snapshot: None,
                    response_status: None,
                },
            );
            Ok(Acquire::Acquired)
        }

        fn takeover(
            &self,
            key: &str,
            owner: Uuid,
            new_lease: DateTime<Utc>,
            now: DateTime<Utc>,
        ) -> Result<Option<KeyRecord>, StoreError> {
            let mut rows = self.rows.borrow_mut();
            match rows.get_mut(key) {
                Some(rec)
                    if rec.status == KeyStatus::InProgress
                        && rec.lease_deadline.is_some_and(|d| d < now) =>
                {
                    rec.lease_deadline = Some(new_lease);
                    rec.lease_owner = Some(owner);
                    Ok(Some(rec.clone()))
                }
                _ => Ok(None),
            }
        }

        fn complete(
            conn: &mut Self::Conn,
            key: &str,
            owner: Uuid,
            snapshot: &serde_json::Value,
            status: i16,
        ) -> Result<bool, StoreError> {
            match conn.get_mut(key) {
                Some(rec)
                    if rec.status == KeyStatus::InProgress && rec.lease_owner == Some(owner) =>
                {
                    rec.status = KeyStatus::Completed;
                    rec.response_snapshot = Some(snapshot.clone());
                    rec.response_status = Some(status);
                    rec.lease_deadline = None;
                    Ok(true)
                }
                _ => Ok(false),
            }
        }

        fn read(&self, key: &str) -> Result<Option<KeyRecord>, StoreError> {
            Ok(self.rows.borrow().get(key).cloned())
        }
    }

    #[test]
    fn mock_store_proves_the_trait_is_implementable_without_io() {
        let store = MockStore::default();
        let owner = Uuid::new_v4();
        let lease = Utc::now() + chrono::Duration::seconds(30);

        // First acquire wins the key.
        assert_eq!(store.acquire("k", "fp", lease, owner).unwrap(), Acquire::Acquired);

        // Second acquire sees the in-progress record.
        match store.acquire("k", "fp", lease, owner).unwrap() {
            Acquire::Existing(rec) => assert_eq!(rec.status, KeyStatus::InProgress),
            Acquire::Acquired => panic!("expected Existing on the second acquire"),
        }

        // `complete` runs against the (mock) transaction handle and flips the record.
        let mut conn = store.rows.borrow().clone();
        let won = MockStore::complete(&mut conn, "k", owner, &serde_json::json!({"ok": true}), 201)
            .unwrap();
        assert!(won, "the lease owner must win the conditional completion");
        // A second completion (already completed) does not win.
        let again =
            MockStore::complete(&mut conn, "k", owner, &serde_json::json!({"ok": true}), 201)
                .unwrap();
        assert!(!again, "a record that is no longer in_progress must not be completed twice");
    }
}
