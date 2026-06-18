//! The inbound `Idempotency-Key` machinery (Amendment §A2): the header/raw-body extractor and
//! the reusable **Execute orchestration spine**.
//!
//! The spine is `acquire → decide → with_tx { effect; complete } → replay-on-loss`, with
//! lease-based takeover. It is FROZEN after Phase 1: every keyed write path (`/orders` now,
//! `/withdrawals` in 1.5) runs its effect through this one function, so the protocol lives in
//! exactly one place. The crash-point fire-sites for the three Execute seams live here too.

use crate::error::ApiError;
use actix_web::http::StatusCode;
use actix_web::{HttpRequest, HttpResponse};
use chaos_hooks::crash_point;
use idempotency::{decide, idempotency_lease, Acquire, Decision, IdempotencyStore, KeyStatus};
use serde::Serialize;
use serde_json::Value;
use store::diesel;
use store::PgConnection;
use uuid::Uuid;

/// Pull the required `Idempotency-Key` header. Absent or empty → `400` (the contract on
/// `/orders` and `/withdrawals`).
pub fn extract_idempotency_key(req: &HttpRequest) -> Result<String, ApiError> {
    let value = req
        .headers()
        .get("Idempotency-Key")
        .ok_or_else(|| ApiError::BadRequest("missing Idempotency-Key header".into()))?
        .to_str()
        .map_err(|_| ApiError::BadRequest("invalid Idempotency-Key header".into()))?
        .trim()
        .to_string();

    if value.is_empty() {
        return Err(ApiError::BadRequest("empty Idempotency-Key header".into()));
    }
    Ok(value)
}

/// Build the response served both for a fresh Execute and for any replay: the stored snapshot
/// body at the stored HTTP status.
fn replay_response(status: i16, snapshot: Value) -> HttpResponse {
    let code = u16::try_from(status)
        .ok()
        .and_then(|c| StatusCode::from_u16(c).ok())
        .unwrap_or(StatusCode::OK);
    HttpResponse::build(code).json(snapshot)
}

/// The Execute orchestration spine. `run_effect` is the domain effect; it runs INSIDE the
/// Execute `with_tx` and returns `(response, http_status)`. The response is serialized into the
/// idempotency snapshot and returned to the client. `run_effect` must be idempotent-safe to call
/// more than once (the loop may re-enter it after a lost takeover) — for `/orders` the
/// `ON CONFLICT (app_id, order_id)` backstop guarantees that.
pub fn execute_idempotent<S, F, R>(
    store: &S,
    pool: &store::Pool,
    key: &str,
    fingerprint: &str,
    run_effect: F,
) -> Result<HttpResponse, ApiError>
where
    S: IdempotencyStore<Conn = PgConnection>,
    F: Fn(&mut PgConnection) -> Result<(R, i16), diesel::result::Error>,
    R: Serialize,
{
    let owner = Uuid::new_v4();

    loop {
        match store.acquire(key, fingerprint, chrono::Utc::now() + idempotency_lease(), owner)? {
            Acquire::Acquired => { /* we own a fresh key — fall through to Execute */ }
            Acquire::Existing(rec) => match decide(&rec, fingerprint, chrono::Utc::now()) {
                Decision::Replay { snapshot, status } => {
                    return Ok(replay_response(status, snapshot));
                }
                Decision::Conflict => return Err(ApiError::Conflict),
                Decision::RetryAfter { seconds } => return Err(ApiError::retry_after(seconds)),
                Decision::Takeover => {
                    match store.takeover(
                        key,
                        owner,
                        chrono::Utc::now() + idempotency_lease(),
                        chrono::Utc::now(),
                    )? {
                        Some(_) => { /* we won the lease — fall through to Execute */ }
                        // Someone else took it or completed it; re-acquire and re-decide.
                        None => continue,
                    }
                }
                // `decide` is only consulted for an existing record and never returns Execute.
                Decision::Execute => unreachable!("decide never returns Execute"),
            },
        }

        // Execute: the guarded effect AND the conditional completion commit in ONE with_tx (RC).
        crash_point!(chaos_hooks::CrashPointId::IdemAfterAcquireBeforeExecute);

        let outcome = store::with_tx(pool, |conn| {
            let (response, status) = run_effect(conn)?;
            let snapshot = serde_json::to_value(&response)
                .map_err(|e| diesel::result::Error::QueryBuilderError(Box::new(e)))?;

            crash_point!(chaos_hooks::CrashPointId::IdemAfterEffectBeforeComplete);

            let won = S::complete(conn, key, owner, &snapshot, status)
                .map_err(|e| diesel::result::Error::QueryBuilderError(Box::new(e)))?;
            if !won {
                // A takeover beat us — roll our effect back and replay the winner's snapshot.
                return Err(diesel::result::Error::RollbackTransaction);
            }

            crash_point!(chaos_hooks::CrashPointId::IdemAfterCompleteBeforeCommit);
            Ok((status, snapshot))
        });

        return match outcome {
            Ok((status, snapshot)) => Ok(replay_response(status, snapshot)),
            // We lost the conditional completion: the winner has (or will have) committed the
            // completed snapshot. Re-read and replay it.
            Err(store::StoreError::Query(diesel::result::Error::RollbackTransaction)) => {
                match store.read(key)? {
                    Some(rec) if matches!(rec.status, KeyStatus::Completed) => Ok(replay_response(
                        rec.response_status.unwrap_or(200),
                        rec.response_snapshot.unwrap_or(Value::Null),
                    )),
                    // The effect rolled back for some non-takeover reason and no winner exists.
                    _ => Err(ApiError::Internal(
                        "idempotent execute rolled back without a completed winner".into(),
                    )),
                }
            }
            Err(e) => Err(ApiError::from(e)),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::body::to_bytes;
    use actix_web::ResponseError;
    use chrono::{Duration, Utc};
    use idempotency::{KeyRecord, StoreError};

    /// A mock store that drives `acquire` to a chosen pre-existing record, so the pre-Execute
    /// decision branches (Replay / Conflict / RetryAfter) are exercised with no database. The
    /// Execute path (which needs `with_tx` + a real connection) is covered by the DB-backed test
    /// below and is never reached here.
    struct MockStore {
        existing: Option<KeyRecord>,
    }

    impl MockStore {
        fn with(existing: KeyRecord) -> Self {
            Self { existing: Some(existing) }
        }
    }

    impl IdempotencyStore for MockStore {
        type Conn = PgConnection;

        fn acquire(
            &self,
            _key: &str,
            _fp: &str,
            _lease: chrono::DateTime<Utc>,
            _owner: Uuid,
        ) -> Result<Acquire, StoreError> {
            match &self.existing {
                Some(rec) => Ok(Acquire::Existing(rec.clone())),
                None => Ok(Acquire::Acquired),
            }
        }

        fn takeover(
            &self,
            _key: &str,
            _owner: Uuid,
            _new_lease: chrono::DateTime<Utc>,
            _now: chrono::DateTime<Utc>,
        ) -> Result<Option<KeyRecord>, StoreError> {
            Ok(None)
        }

        fn complete(
            _conn: &mut PgConnection,
            _key: &str,
            _owner: Uuid,
            _snapshot: &Value,
            _status: i16,
        ) -> Result<bool, StoreError> {
            // Not reached by the no-DB decision tests; present to satisfy the trait.
            Ok(true)
        }

        fn read(&self, _key: &str) -> Result<Option<KeyRecord>, StoreError> {
            Ok(self.existing.clone())
        }
    }

    /// A never-connecting pool. The decision-branch tests return before opening `with_tx`, so the
    /// pool is never used.
    fn unused_pool() -> store::Pool {
        use diesel::r2d2::ConnectionManager;
        let manager = ConnectionManager::<PgConnection>::new("postgres://invalid:5432/invalid");
        diesel::r2d2::Pool::builder().build_unchecked(manager)
    }

    fn completed(fp: &str, status: i16, snapshot: Value) -> KeyRecord {
        KeyRecord {
            status: KeyStatus::Completed,
            request_fingerprint: fp.to_string(),
            lease_deadline: None,
            lease_owner: Some(Uuid::new_v4()),
            response_snapshot: Some(snapshot),
            response_status: Some(status),
        }
    }

    fn run(store: &MockStore, fingerprint: &str) -> Result<HttpResponse, ApiError> {
        let pool = unused_pool();
        execute_idempotent::<_, _, Value>(store, &pool, "k", fingerprint, |_conn| {
            // The decision branches under test return before the effect runs.
            panic!("run_effect must not be reached for a completed/in-progress key");
        })
    }

    #[actix_web::test]
    async fn completed_matching_fingerprint_replays_the_snapshot() {
        let snap = serde_json::json!({ "id": "order_42", "status": "pending" });
        let store = MockStore::with(completed("fp", 200, snap.clone()));

        let resp = run(&store, "fp").expect("replay should succeed");
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body()).await.expect("read body");
        let got: Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(got, snap, "replay returns the stored snapshot verbatim");
    }

    #[actix_web::test]
    async fn completed_with_custom_status_is_preserved_on_replay() {
        let snap = serde_json::json!({ "id": "order_7" });
        let store = MockStore::with(completed("fp", 201, snap.clone()));

        let resp = run(&store, "fp").expect("replay should succeed");
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[actix_web::test]
    async fn completed_mismatched_fingerprint_is_409_conflict() {
        let store = MockStore::with(completed("fp_original", 200, serde_json::json!({})));

        let err = run(&store, "fp_different").expect_err("must conflict");
        assert!(matches!(err, ApiError::Conflict));
        assert_eq!(err.status_code(), StatusCode::CONFLICT);
    }

    #[actix_web::test]
    async fn in_progress_valid_lease_is_409_with_retry_after_header() {
        let rec = KeyRecord {
            status: KeyStatus::InProgress,
            request_fingerprint: "fp".to_string(),
            lease_deadline: Some(Utc::now() + Duration::seconds(25)),
            lease_owner: Some(Uuid::new_v4()),
            response_snapshot: None,
            response_status: None,
        };
        let store = MockStore::with(rec);

        let err = run(&store, "fp").expect_err("must ask to retry");
        let seconds = match err {
            ApiError::RetryAfter(s) => s,
            other => panic!("expected RetryAfter, got {other:?}"),
        };
        assert!(seconds > 0 && seconds <= 25, "remaining lease in (0, 25], got {seconds}");

        // The response carries 409 + a Retry-After header.
        let resp = ApiError::retry_after(seconds).error_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert!(resp.headers().contains_key("Retry-After"));
    }

    // ── DB-backed end-to-end test (gated on DATABASE_URL; skips cleanly when unset) ──────────
    //
    // Exercises the REAL `IdempotencyStorePg` + Execute spine + `/orders` natural-key backstop
    // against Postgres: replay (snapshot), 409 on payload-mismatch, and distinct-key/same-order
    // converging on ONE order row. Run with a migrated DB, e.g.:
    //   DATABASE_URL=postgres:///coingate_idem_test?host=/var/run/postgresql cargo test -p api

    /// Build a pool from `DATABASE_URL`, or `None` to skip (no DB configured/reachable).
    fn db_pool_or_skip() -> Option<store::Pool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let manager = diesel::r2d2::ConnectionManager::<PgConnection>::new(url);
        diesel::r2d2::Pool::builder().max_size(2).build(manager).ok()
    }

    /// The `/orders` effect, mirroring the handler: insert with the `(app_id, order_id)`
    /// backstop and snapshot the resulting (possibly pre-existing) order.
    fn order_effect(
        app_id: Uuid,
        order_id: String,
        amount_base: i64,
    ) -> impl Fn(&mut PgConnection) -> Result<(Value, i16), diesel::result::Error> {
        move |conn| {
            let candidate = store::module::Order {
                id: Uuid::new_v4(),
                app_id: Some(app_id),
                order_id: order_id.clone(),
                price_amount: bigdecimal::BigDecimal::from(amount_base),
                price_currency: "USD".to_string(),
                receive_currency: "USDC".to_string(),
                memo_id: Uuid::new_v4().to_string(),
                status: "pending".to_string(),
                tx_hash: None,
                selected_mint: None,
                expected_amount: None,
                expected_decimals: None,
                callback_url: None,
                success_url: None,
                cancel_url: None,
                created_at: None,
                confirmed_at: None,
            };
            let order = store::insert_order_on_conflict(conn, candidate)?;
            Ok((
                serde_json::json!({ "id": order.id.to_string(), "status": order.status }),
                200,
            ))
        }
    }

    async fn body_json(resp: HttpResponse) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = to_bytes(resp.into_body()).await.expect("read body");
        (status, serde_json::from_slice(&bytes).expect("json body"))
    }

    #[actix_web::test]
    async fn db_orders_replay_conflict_and_natural_key_backstop() {
        let Some(pool) = db_pool_or_skip() else {
            eprintln!("skipping db_orders test: DATABASE_URL unset/unreachable");
            return;
        };

        // Seed merchant + app (orders.app_id -> apps.id -> merchants.id FKs).
        let app_id = {
            let mut conn = store::get_conn(&pool).expect("conn");
            let merchant = store::insert_merchant(
                &mut conn,
                store::module::Merchant {
                    id: Uuid::new_v4(),
                    email: format!("m-{}@test.local", Uuid::new_v4()),
                    password_hash: "x".to_string(),
                    name: "t".to_string(),
                    created_at: None,
                },
            )
            .expect("merchant");
            store::insert_app(
                &mut conn,
                store::module::App {
                    id: Uuid::new_v4(),
                    merchant_id: Some(merchant.id),
                    title: "t".to_string(),
                    callback_url: None,
                    token_hash: "x".to_string(),
                    created_at: None,
                },
            )
            .expect("app")
            .id
        };

        let idem = store::IdempotencyStorePg::new(pool.clone());
        let order_id = format!("o-{}", Uuid::new_v4());
        let k1 = format!("k1-{}", Uuid::new_v4());

        // 1. First POST creates the order.
        let (st1, b1) = body_json(
            execute_idempotent(&idem, &pool, &k1, "fpA", order_effect(app_id, order_id.clone(), 1_000_000))
                .expect("create"),
        )
        .await;
        assert_eq!(st1, StatusCode::OK);
        let created_id = b1["id"].as_str().expect("id").to_string();

        // 2. Identical replay (same key + fingerprint) returns the stored snapshot — same order.
        let (st2, b2) = body_json(
            execute_idempotent(&idem, &pool, &k1, "fpA", order_effect(app_id, order_id.clone(), 1_000_000))
                .expect("replay"),
        )
        .await;
        assert_eq!(st2, StatusCode::OK);
        assert_eq!(b2["id"].as_str().unwrap(), created_id, "replay returns the same order");

        // 3. Same key, DIFFERENT body (fingerprint) -> 409 Conflict.
        let err = execute_idempotent(
            &idem,
            &pool,
            &k1,
            "fpB",
            order_effect(app_id, order_id.clone(), 2_000_000),
        )
        .expect_err("payload mismatch must 409");
        assert!(matches!(err, ApiError::Conflict));

        // 4. DISTINCT key, SAME (app_id, order_id) -> natural-key backstop: one order, same id.
        let k2 = format!("k2-{}", Uuid::new_v4());
        let (st4, b4) = body_json(
            execute_idempotent(&idem, &pool, &k2, "fpC", order_effect(app_id, order_id.clone(), 1_000_000))
                .expect("distinct key"),
        )
        .await;
        assert_eq!(st4, StatusCode::OK);
        assert_eq!(
            b4["id"].as_str().unwrap(),
            created_id,
            "distinct key, same business order -> one order"
        );

        // Exactly one orders row exists for this (app_id, order_id).
        let count: i64 = {
            use store::diesel::prelude::*;
            use store::schema::orders::dsl as o;
            let mut conn = store::get_conn(&pool).expect("conn");
            o::orders
                .filter(o::app_id.eq(Some(app_id)))
                .filter(o::order_id.eq(&order_id))
                .count()
                .get_result(&mut conn)
                .expect("count")
        };
        assert_eq!(count, 1, "exactly one order for the (app_id, order_id)");
    }
}
