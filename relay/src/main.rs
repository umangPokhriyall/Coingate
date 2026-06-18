//! The transactional-outbox relay (Phase 1 §7, Amendment §A3).
//!
//! `/withdrawals` commits the funds lock and an `outbox` row in one transaction; this binary is
//! the at-least-once publisher that drains the outbox to Redis. The loop is:
//!
//!   for row in select_unsent_outbox(conn):   // WHERE sent_at IS NULL ORDER BY created_at
//!       XADD row.topic * data <row.payload>
//!       mark_outbox_sent(conn, row.id)        // SET sent_at = now()
//!
//! `XADD` then `mark-sent` CANNOT be atomic — Redis is not inside the DB transaction. That is the
//! design, not a flaw: the outbox is the durable intent and publish is at-least-once. A crash at
//! `RelayAfterXaddBeforeMarkSent` republishes the row on restart, and the **consumer-side dedup
//! absorbs the duplicate** (the worker's `pending -> processing` guard + `withdrawal_id`
//! reconciliation). Phase 1 places the seam; Phase 2 demonstrates the absorption.

use anyhow::Result;
use chaos_hooks::crash_point;
use store::{Config, build_pool, get_conn};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration (check required env vars)");

    let pool = build_pool(&cfg).expect("failed to build database pool");
    let redis_client = redis::Client::open(cfg.redis_url.clone())?;
    let mut redis_conn = redis_client.get_connection()?;

    info!("relay started: draining outbox -> redis (at-least-once)");

    loop {
        // Read the unsent backlog oldest-first. Its own pooled connection (single statement).
        let rows = {
            let mut conn = get_conn(&pool)?;
            store::select_unsent_outbox(&mut conn)?
        };

        if rows.is_empty() {
            debug!("outbox empty, sleeping");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        info!(count = rows.len(), "publishing unsent outbox rows");

        for row in rows {
            crash_point!(chaos_hooks::CrashPointId::RelayAfterReadBeforeXadd);

            // XADD topic * data <payload>. The worker reads a single `data` field whose value is
            // the inner JSON string, so we publish the payload serialized to text.
            let payload_str = row.payload.to_string();
            let xadd: redis::RedisResult<String> = redis::cmd("XADD")
                .arg(&row.topic)
                .arg("*")
                .arg("data")
                .arg(&payload_str)
                .query(&mut redis_conn);

            if let Err(e) = xadd {
                // Transient publish failure: leave the row unsent (sent_at IS NULL) for the next
                // pass. No DB write, so no risk of marking an unpublished row sent.
                error!(error = %e, outbox_id = %row.id, topic = %row.topic, "XADD failed; will retry");
                continue;
            }

            // The non-atomic seam: a crash here republishes `row` on restart (at-least-once).
            crash_point!(chaos_hooks::CrashPointId::RelayAfterXaddBeforeMarkSent);

            let mut conn = get_conn(&pool)?;
            match store::mark_outbox_sent(&mut conn, row.id) {
                Ok(_) => info!(outbox_id = %row.id, topic = %row.topic, "published and marked sent"),
                Err(e) => {
                    // Already published; failing to mark sent only means a benign re-publish next
                    // pass (consumer dedups). Log and move on.
                    error!(error = %e, outbox_id = %row.id, "marked-sent update failed; row may republish");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // The relay's contract is the three outbox queries: read unsent oldest-first, publish, mark
    // sent. The `XADD` itself needs Redis and is exercised in Phase 2's supervisor harness; here we
    // prove the DRAIN semantics the loop depends on against Postgres. Gated on DATABASE_URL.
    //   DATABASE_URL=postgres:///coingate_relay_test?host=/var/run/postgresql cargo test -p relay
    use store::PgConnection;
    use store::diesel::r2d2::ConnectionManager;

    fn db_pool_or_skip() -> Option<store::Pool> {
        let url = std::env::var("DATABASE_URL").ok()?;
        let manager = ConnectionManager::<PgConnection>::new(url);
        store::diesel::r2d2::Pool::builder().max_size(2).build(manager).ok()
    }

    fn unique_topic() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("relay_test_{nanos}")
    }

    #[test]
    fn drain_reads_oldest_first_then_marks_sent_so_it_is_not_re_read() {
        let Some(pool) = db_pool_or_skip() else {
            eprintln!("skipping relay drain test: DATABASE_URL unset/unreachable");
            return;
        };
        let topic = unique_topic();
        let mut conn = store::get_conn(&pool).expect("conn");

        // Insert two unsent rows; created_at ordering makes the first the oldest.
        let r1 = store::insert_outbox(&mut conn, &topic, &serde_json::json!({ "n": 1 }))
            .expect("insert 1");
        let r2 = store::insert_outbox(&mut conn, &topic, &serde_json::json!({ "n": 2 }))
            .expect("insert 2");

        // The drain reads unsent oldest-first. Filter to our topic (the table is shared).
        let unsent: Vec<_> = store::select_unsent_outbox(&mut conn)
            .expect("select unsent")
            .into_iter()
            .filter(|r| r.topic == topic)
            .collect();
        assert_eq!(unsent.len(), 2, "both rows are unsent");
        assert_eq!(unsent[0].id, r1.id, "oldest-first ordering");
        assert_eq!(unsent[1].id, r2.id);

        // Publish-then-mark the first row.
        let marked = store::mark_outbox_sent(&mut conn, r1.id).expect("mark sent");
        assert_eq!(marked, 1, "exactly one row transitioned to sent");

        // A second pass no longer re-reads the sent row.
        let after: Vec<_> = store::select_unsent_outbox(&mut conn)
            .expect("select unsent again")
            .into_iter()
            .filter(|r| r.topic == topic)
            .collect();
        assert_eq!(after.len(), 1, "the sent row is not re-read");
        assert_eq!(after[0].id, r2.id, "only the still-unsent row remains");

        // Marking an already-sent row again is a no-op count (idempotent-ish for the relay).
        let again = store::mark_outbox_sent(&mut conn, r1.id).expect("mark sent again");
        assert_eq!(again, 1, "the UPDATE matches by id regardless of prior sent_at");
    }
}
