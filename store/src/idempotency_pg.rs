//! Postgres implementation of the sans-IO `idempotency::IdempotencyStore` trait (Amendment §A2).
//!
//! Maps between the persisted `IdempotencyKeyRow` (diesel; `NaiveDateTime`, `String` status) and
//! the sans-IO `idempotency::KeyRecord` (`DateTime<Utc>`, `KeyStatus`). The Phase-0 contract is
//! untouched: only `complete` runs inside the caller's transaction (it takes `&mut PgConnection`
//! so it commits atomically with the guarded effect); `acquire`/`takeover`/`read` are each their
//! OWN committed statement, so concurrent replays SEE the `in_progress` row and never block.

use crate::module::IdempotencyKeyRow;
use crate::pool::Pool;
use crate::schema::idempotency_keys::dsl as ik;
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use idempotency::{Acquire, IdempotencyStore, KeyRecord, KeyStatus, StoreError};
use uuid::Uuid;

/// Postgres-backed [`IdempotencyStore`]. Holds a clone of the shared pool (r2d2 pools are
/// `Arc`-backed, so clones are cheap); construct one per request from `web::Data<Pool>`.
#[derive(Clone)]
pub struct IdempotencyStorePg {
    pool: Pool,
}

impl IdempotencyStorePg {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

/// Map any backend (pool/diesel) failure into the sans-IO error type.
fn backend<E: std::fmt::Display>(e: E) -> StoreError {
    StoreError::Backend(e.to_string())
}

/// Materialize a persisted row into the sans-IO record, validating the status enum. A row whose
/// `status` is neither `in_progress` nor `completed` is [`StoreError::Malformed`], not a panic.
fn to_record(row: IdempotencyKeyRow) -> Result<KeyRecord, StoreError> {
    let status = match row.status.as_str() {
        "in_progress" => KeyStatus::InProgress,
        "completed" => KeyStatus::Completed,
        other => {
            return Err(StoreError::Malformed(format!(
                "unknown idempotency status {other:?} for key {:?}",
                row.key
            )));
        }
    };
    Ok(KeyRecord {
        status,
        request_fingerprint: row.request_fingerprint,
        // Timestamptz is stored/read as NaiveDateTime in UTC throughout this codebase.
        lease_deadline: row.lease_deadline.map(|n| n.and_utc()),
        lease_owner: row.lease_owner,
        response_snapshot: row.response_snapshot,
        response_status: row.response_status,
    })
}

impl IdempotencyStore for IdempotencyStorePg {
    type Conn = PgConnection;

    fn acquire(
        &self,
        key: &str,
        fingerprint: &str,
        lease_deadline: DateTime<Utc>,
        owner: Uuid,
    ) -> Result<Acquire, StoreError> {
        let mut conn = self.pool.get().map_err(backend)?;
        let now = Utc::now().naive_utc();

        // INSERT ... ON CONFLICT (key) DO NOTHING RETURNING — its own committed statement.
        let inserted: Option<IdempotencyKeyRow> = diesel::insert_into(ik::idempotency_keys)
            .values((
                ik::key.eq(key),
                ik::request_fingerprint.eq(fingerprint),
                ik::status.eq("in_progress"),
                ik::lease_deadline.eq(lease_deadline.naive_utc()),
                ik::lease_owner.eq(owner),
                ik::created_at.eq(now),
                ik::updated_at.eq(now),
            ))
            .on_conflict(ik::key)
            .do_nothing()
            .returning(IdempotencyKeyRow::as_returning())
            .get_result(&mut conn)
            .optional()
            .map_err(backend)?;

        match inserted {
            Some(_) => Ok(Acquire::Acquired),
            None => {
                // The key already exists — read it so the caller can `decide`.
                let existing: IdempotencyKeyRow = ik::idempotency_keys
                    .filter(ik::key.eq(key))
                    .select(IdempotencyKeyRow::as_select())
                    .first(&mut conn)
                    .map_err(backend)?;
                Ok(Acquire::Existing(to_record(existing)?))
            }
        }
    }

    fn takeover(
        &self,
        key: &str,
        owner: Uuid,
        new_lease: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<KeyRecord>, StoreError> {
        let mut conn = self.pool.get().map_err(backend)?;

        // Atomic CAS: only an in_progress key whose lease has expired can be taken over.
        let updated: Option<IdempotencyKeyRow> = diesel::update(
            ik::idempotency_keys
                .filter(ik::key.eq(key))
                .filter(ik::status.eq("in_progress"))
                .filter(ik::lease_deadline.lt(now.naive_utc())),
        )
        .set((
            ik::lease_deadline.eq(new_lease.naive_utc()),
            ik::lease_owner.eq(owner),
            ik::updated_at.eq(Utc::now().naive_utc()),
        ))
        .returning(IdempotencyKeyRow::as_returning())
        .get_result(&mut conn)
        .optional()
        .map_err(backend)?;

        updated.map(to_record).transpose()
    }

    fn complete(
        conn: &mut PgConnection,
        key: &str,
        owner: Uuid,
        snapshot: &serde_json::Value,
        status: i16,
    ) -> Result<bool, StoreError> {
        // Conditional completion, run INSIDE the caller's with_tx. Wins (1 row) iff the key is
        // still ours and in_progress — a takeover would have changed lease_owner, matching 0 rows.
        let rows = diesel::update(
            ik::idempotency_keys
                .filter(ik::key.eq(key))
                .filter(ik::status.eq("in_progress"))
                .filter(ik::lease_owner.eq(owner)),
        )
        .set((
            ik::status.eq("completed"),
            ik::response_snapshot.eq(snapshot.clone()),
            ik::response_status.eq(status),
            ik::lease_deadline.eq(None::<NaiveDateTime>),
            ik::updated_at.eq(Utc::now().naive_utc()),
        ))
        .execute(conn)
        .map_err(backend)?;

        Ok(rows == 1)
    }

    fn read(&self, key: &str) -> Result<Option<KeyRecord>, StoreError> {
        let mut conn = self.pool.get().map_err(backend)?;
        let row: Option<IdempotencyKeyRow> = ik::idempotency_keys
            .filter(ik::key.eq(key))
            .select(IdempotencyKeyRow::as_select())
            .first(&mut conn)
            .optional()
            .map_err(backend)?;
        row.map(to_record).transpose()
    }
}
