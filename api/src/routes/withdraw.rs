use crate::error::ApiError;
use crate::routes::merchant::{bearer_claims, merchant_uuid};
use actix_web::{HttpRequest, HttpResponse, post, web};
use bigdecimal::BigDecimal;
use chaos_hooks::crash_point;
use serde::Deserialize;
use std::str::FromStr;

#[derive(Deserialize)]
pub struct CreateWithdrawalRequest {
    pub token_mint: String, // always a string; SOL can use its mint
    pub amount: String,     // string decimal; parsed to BigDecimal
    pub target_address: String,
}

/// `/withdrawals` now runs the §A2 Execute spine with the effect = `create_withdrawal_and_lock`
/// **plus** `insert_outbox`, committed in ONE `with_tx` (Phase 1 §7). This closes the dual-write:
/// the funds lock and the durable publish-intent commit together, so funds are never locked
/// without a work item. The handler no longer touches Redis — the `relay` binary drains the
/// outbox at-least-once. Withdrawals have no natural business key, so the `Idempotency-Key`
/// header is the sole dedup for this path (a client retry with the same key replays the stored
/// response; without it, two identical requests are two legitimate withdrawals).
#[post("/withdrawals")]
pub async fn create_withdrawal(
    http_req: HttpRequest,
    body: web::Bytes,
    pool: web::Data<store::Pool>,
) -> Result<HttpResponse, ApiError> {
    // 1) Merchant verification via Bearer token.
    let claims = bearer_claims(&http_req)?;
    let merchant_id = merchant_uuid(&claims)?;

    // 2) Idempotency-Key required (400 if absent); fingerprint hashes the RAW body BEFORE deser.
    let key = crate::idem::extract_idempotency_key(&http_req)?;
    let fingerprint =
        idempotency::request_fingerprint(http_req.method().as_str(), http_req.path(), &body);

    // 3) Deserialize from the buffered bytes — a malformed body is a 400, never a panic.
    let req: CreateWithdrawalRequest = serde_json::from_slice(&body)
        .map_err(|e| ApiError::BadRequest(format!("invalid withdrawal body: {e}")))?;

    // 4) Parse amount (exact decimal; never f64).
    let amount_bd = BigDecimal::from_str(&req.amount)
        .map_err(|_| ApiError::BadRequest("invalid amount".into()))?;

    // 5) Pre-validate funds OUTSIDE the spine. `create_withdrawal_and_lock` signals insufficient
    //    funds via `RollbackTransaction`, which the Execute spine reads as a lost takeover — so a
    //    business shortfall must be rejected as a 400 *before* we acquire an idempotency key (and
    //    leave it stuck `in_progress`). The authoritative `FOR UPDATE` check inside the lock still
    //    guards the rare concurrent-drain race; that residual rollback self-heals on lease expiry.
    {
        let mut conn = store::get_conn(&pool)?;
        let available = store::get_balance(&mut conn, merchant_id, &req.token_mint)
            .ok()
            .and_then(|b| b.balance)
            .unwrap_or_else(|| BigDecimal::from(0));
        if available < amount_bd {
            return Err(ApiError::BadRequest("insufficient balance".into()));
        }
    }

    let idem_store = store::IdempotencyStorePg::new(pool.get_ref().clone());

    crate::idem::execute_idempotent(&idem_store, pool.get_ref(), &key, &fingerprint, |conn| {
        // The effect: lock funds AND write the publish-intent in ONE transaction.
        let withdrawal = store::create_withdrawal_and_lock(
            conn,
            merchant_id,
            &req.token_mint,
            &amount_bd,
            &req.target_address,
        )?;

        crash_point!(chaos_hooks::CrashPointId::WithdrawAfterLockBeforeOutbox);

        // The durable publish-intent. Field layout matches what the worker parses off the stream
        // (the relay republishes this verbatim as the `data` field).
        let payload = serde_json::json!({
            "withdrawal_id": withdrawal.id.to_string(),
            "merchant_id": merchant_id.to_string(),
            "token_mint": req.token_mint,
            "amount": req.amount,
            "target_address": req.target_address,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        store::insert_outbox(conn, "withdrawal_requests", &payload)?;

        crash_point!(chaos_hooks::CrashPointId::WithdrawAfterOutboxBeforeComplete);

        let response = serde_json::json!({
            "withdrawal_id": withdrawal.id.to_string(),
            "status": "pending",
        });
        Ok((response, 200))
    })
}

#[cfg(test)]
mod tests {
    //! DB-backed end-to-end test of the `/withdrawals` Execute spine (gated on DATABASE_URL).
    //! Proves the §7 invariants: the funds lock and the outbox publish-intent commit in ONE
    //! transaction, and a same-key replay returns the stored snapshot without a second
    //! withdrawal, a second outbox row, or a second lock.
    //!   DATABASE_URL=postgres:///coingate_wd_test?host=/var/run/postgresql cargo test -p api
    use actix_web::HttpResponse;
    use actix_web::body::to_bytes;
    use bigdecimal::BigDecimal;
    use serde_json::Value;
    use std::str::FromStr;
    use store::PgConnection;
    use store::diesel::r2d2::ConnectionManager;
    use uuid::Uuid;

    fn db_pool_or_skip() -> Option<store::Pool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let manager = ConnectionManager::<PgConnection>::new(url);
        store::diesel::r2d2::Pool::builder().max_size(2).build(manager).ok()
    }

    /// The handler's effect, extracted so the test exercises the SAME lock+outbox transaction.
    fn withdrawal_effect(
        merchant_id: Uuid,
        token_mint: String,
        amount: String,
        target: String,
    ) -> impl Fn(&mut PgConnection) -> Result<(Value, i16), store::diesel::result::Error> {
        move |conn| {
            let amount_bd = BigDecimal::from_str(&amount).expect("amount");
            let withdrawal =
                store::create_withdrawal_and_lock(conn, merchant_id, &token_mint, &amount_bd, &target)?;
            let payload = serde_json::json!({
                "withdrawal_id": withdrawal.id.to_string(),
                "merchant_id": merchant_id.to_string(),
                "token_mint": token_mint,
                "amount": amount,
                "target_address": target,
            });
            store::insert_outbox(conn, "withdrawal_requests", &payload)?;
            Ok((serde_json::json!({ "withdrawal_id": withdrawal.id.to_string(), "status": "pending" }), 200))
        }
    }

    async fn body_json(resp: HttpResponse) -> Value {
        let bytes = to_bytes(resp.into_body()).await.expect("read body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    #[actix_web::test]
    async fn withdrawals_commit_lock_and_outbox_atomically_and_replay_is_idempotent() {
        let Some(pool) = db_pool_or_skip() else {
            eprintln!("skipping withdrawals test: DATABASE_URL unset/unreachable");
            return;
        };

        // Seed a merchant with a funded balance on an isolated token mint.
        let token = format!("MINT-{}", Uuid::new_v4());
        let merchant_id = {
            let mut conn = store::get_conn(&pool).expect("conn");
            let m = store::insert_merchant(
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
            store::upsert_balance(&mut conn, m.id, &token, &BigDecimal::from(100))
                .expect("seed balance");
            m.id
        };

        let idem = store::IdempotencyStorePg::new(pool.clone());
        let key = format!("wk-{}", Uuid::new_v4());
        let effect = || withdrawal_effect(merchant_id, token.clone(), "10".to_string(), "addr1".to_string());

        // 1) First request: locks funds AND writes the outbox row in one tx.
        let b1 = body_json(
            crate::idem::execute_idempotent(&idem, &pool, &key, "fpW", effect()).expect("create"),
        )
        .await;
        let wid = b1["withdrawal_id"].as_str().expect("withdrawal_id").to_string();
        assert_eq!(b1["status"], "pending");

        // Exactly one withdrawal, exactly one unsent outbox row pointing at it; funds locked once.
        let outbox_for = |pool: &store::Pool, wid: &str| -> usize {
            let mut conn = store::get_conn(pool).expect("conn");
            store::select_unsent_outbox(&mut conn)
                .expect("unsent")
                .iter()
                .filter(|r| r.payload["withdrawal_id"].as_str() == Some(wid))
                .count()
        };
        let withdrawals_for = |pool: &store::Pool, mid: Uuid| -> i64 {
            use store::diesel::prelude::*;
            use store::schema::withdrawals::dsl as w;
            let mut conn = store::get_conn(pool).expect("conn");
            w::withdrawals.filter(w::merchant_id.eq(mid)).count().get_result(&mut conn).expect("count")
        };

        assert_eq!(withdrawals_for(&pool, merchant_id), 1, "one withdrawal");
        assert_eq!(outbox_for(&pool, &wid), 1, "one outbox row for the withdrawal");

        let bal = {
            let mut conn = store::get_conn(&pool).expect("conn");
            store::get_balance(&mut conn, merchant_id, &token).expect("balance")
        };
        assert_eq!(bal.balance.clone().unwrap(), BigDecimal::from(90), "available debited once");
        assert_eq!(bal.locked_balance.clone().unwrap(), BigDecimal::from(10), "locked once");

        // 2) Same key + fingerprint: replay the stored snapshot — no second withdrawal/outbox/lock.
        let b2 = body_json(
            crate::idem::execute_idempotent(&idem, &pool, &key, "fpW", effect()).expect("replay"),
        )
        .await;
        assert_eq!(b2["withdrawal_id"].as_str().unwrap(), wid, "replay returns the same withdrawal");
        assert_eq!(withdrawals_for(&pool, merchant_id), 1, "still exactly one withdrawal");
        assert_eq!(outbox_for(&pool, &wid), 1, "still exactly one outbox row");

        let bal2 = {
            let mut conn = store::get_conn(&pool).expect("conn");
            store::get_balance(&mut conn, merchant_id, &token).expect("balance")
        };
        assert_eq!(bal2.locked_balance.clone().unwrap(), BigDecimal::from(10), "no double lock on replay");
    }
}
