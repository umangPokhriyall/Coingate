//! Between-run fixtures and the quiescence predicate (Phase 2 §3.3).
//!
//! * **DB:** `TRUNCATE … RESTART IDENTITY CASCADE` all business tables to a clean slate, and
//!   record the isolation level in use (must be `read committed` — Amendment §A4).
//! * **Redis:** `FLUSHDB` and recreate the consumer groups the services expect.
//! * **Quiescence:** both streams have no undelivered entries and empty PELs, AND no unsent
//!   outbox row — the harness drains to this before asserting oracles.
//!
//! Black-box: the DB is read with raw SQL (`diesel::sql_query`), never the `store` schema, so
//! the harness links no target crate.

use anyhow::{Context, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::sql_types::{BigInt, Text};
use diesel::sql_query;

/// The business tables truncated between runs (everything except `__diesel_schema_migrations`).
pub const BUSINESS_TABLES: &[&str] = &[
    "apps",
    "audit_logs",
    "balances",
    "dead_letter",
    "deposits",
    "idempotency_keys",
    "merchants",
    "orders",
    "outbox",
    "wallets",
    "withdrawals",
];

/// The Redis streams and the consumer group each service creates, recreated by the fixture so
/// the harness can `XADD` before a consumer boots. `(stream, group)`.
pub const STREAMS: &[(&str, &str)] = &[
    ("payment_transactions", "processor_group"),
    ("withdrawal_requests", "withdrawals_group"),
];

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// A direct Postgres handle for the harness (raw SQL only).
pub struct Db {
    pool: Pool<ConnectionManager<PgConnection>>,
}

impl Db {
    pub fn connect(db_url: &str) -> Result<Db> {
        let manager = ConnectionManager::<PgConnection>::new(db_url);
        let pool = Pool::builder()
            .max_size(4)
            .build(manager)
            .context("build harness DB pool")?;
        Ok(Db { pool })
    }

    fn conn(&self) -> Result<diesel::r2d2::PooledConnection<ConnectionManager<PgConnection>>> {
        self.pool.get().context("check out DB connection")
    }

    /// Lend a raw connection for black-box seeding/reading (raw SQL only — never the `store`
    /// schema). Used by `workload`/`oracles` to set up preconditions and read invariants.
    pub fn with_conn<T>(
        &self,
        f: impl FnOnce(&mut PgConnection) -> Result<T, diesel::result::Error>,
    ) -> Result<T> {
        let mut conn = self.conn()?;
        Ok(f(&mut conn)?)
    }

    /// Truncate every business table to a clean slate (faster + more deterministic than a
    /// per-run database — §3.3). `RESTART IDENTITY CASCADE`.
    pub fn truncate_all(&self) -> Result<()> {
        let mut conn = self.conn()?;
        let stmt = format!(
            "TRUNCATE {} RESTART IDENTITY CASCADE",
            BUSINESS_TABLES.join(", ")
        );
        sql_query(stmt).execute(&mut conn).context("truncate")?;
        Ok(())
    }

    /// The DB's default transaction isolation, recorded in every run record (§A4). `store::with_tx`
    /// additionally pins `read committed` per-transaction; this confirms the substrate default
    /// is not silently stronger.
    pub fn isolation_level(&self) -> Result<String> {
        let mut conn = self.conn()?;
        let row: TextRow =
            sql_query("SELECT current_setting('default_transaction_isolation') AS value")
                .get_result(&mut conn)
                .context("read isolation level")?;
        Ok(row.value)
    }

    /// Count rows in one business table (raw, parameter-free — table names are from the fixed
    /// allowlist above, never user input).
    pub fn count(&self, table: &str) -> Result<i64> {
        let mut conn = self.conn()?;
        let row: CountRow = sql_query(format!("SELECT COUNT(*) AS n FROM {table}"))
            .get_result(&mut conn)
            .with_context(|| format!("count {table}"))?;
        Ok(row.n)
    }

    /// Tables that are NOT empty — used to assert the truncate fixture reset cleanly.
    pub fn nonempty_tables(&self) -> Result<Vec<(String, i64)>> {
        let mut out = Vec::new();
        for &t in BUSINESS_TABLES {
            let n = self.count(t)?;
            if n != 0 {
                out.push((t.to_string(), n));
            }
        }
        Ok(out)
    }

    /// Unsent outbox rows — the DB half of the quiescence predicate.
    pub fn unsent_outbox(&self) -> Result<i64> {
        let mut conn = self.conn()?;
        let row: CountRow =
            sql_query("SELECT COUNT(*) AS n FROM outbox WHERE sent_at IS NULL")
                .get_result(&mut conn)
                .context("count unsent outbox")?;
        Ok(row.n)
    }
}

/// A direct Redis handle for the fixture + quiescence checks.
pub struct Redis {
    conn: redis::Connection,
}

impl Redis {
    pub fn connect(redis_url: &str) -> Result<Redis> {
        let client = redis::Client::open(redis_url).context("open redis client")?;
        let conn = client.get_connection().context("connect redis")?;
        Ok(Redis { conn })
    }

    /// `FLUSHDB`, then recreate each consumer group (`MKSTREAM`) the services expect, so the
    /// harness can `XADD` before a consumer boots. An already-existing group (`BUSYGROUP`) is fine.
    pub fn flush_and_recreate_groups(&mut self) -> Result<()> {
        redis::cmd("FLUSHDB")
            .query::<()>(&mut self.conn)
            .context("FLUSHDB")?;
        for (stream, group) in STREAMS {
            let res: redis::RedisResult<()> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(group)
                .arg("0")
                .arg("MKSTREAM")
                .query(&mut self.conn);
            if let Err(e) = res {
                // BUSYGROUP (group already exists) is benign; anything else is a real failure.
                if !e.to_string().contains("BUSYGROUP") {
                    return Err(anyhow::Error::new(e).context(format!("XGROUP CREATE {stream}")));
                }
            }
        }
        Ok(())
    }

    /// Raw connection for ad-hoc commands (enqueue, XADD) the workload driver needs.
    pub fn raw(&mut self) -> &mut redis::Connection {
        &mut self.conn
    }

    /// `XADD <stream> * <field value>...` — append one entry, returning its id.
    pub fn xadd(&mut self, stream: &str, fields: &[(&str, &str)]) -> Result<String> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream).arg("*");
        for (k, v) in fields {
            cmd.arg(*k).arg(*v);
        }
        let id: String = cmd.query(&mut self.conn).with_context(|| format!("XADD {stream}"))?;
        Ok(id)
    }

    /// Reclaim every pending entry on `(stream, group)` to a throwaway harness consumer (min-idle
    /// 0) and `XACK` it — clearing a crashed consumer's stranded PEL so the run can reach
    /// quiescence without waiting out the services' 60s `XAUTOCLAIM` idle window. Returns the
    /// number of entries cleared. (The redelivery itself is then modeled by re-`XADD` — see
    /// `workload::redeliver`.)
    pub fn reclaim_and_ack(&mut self, stream: &str, group: &str) -> Result<usize> {
        let claimed: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(stream)
            .arg(group)
            .arg("harness-reaper")
            .arg(0)
            .arg("0-0")
            .arg("COUNT")
            .arg(1000)
            .query(&mut self.conn)
            .with_context(|| format!("XAUTOCLAIM {stream}"))?;
        // XAUTOCLAIM reply: [cursor, [[id, [fields...]], ...], [deleted...]]. Pull the ids.
        let ids = autoclaim_ids(&claimed);
        for id in &ids {
            let _: () = redis::cmd("XACK")
                .arg(stream)
                .arg(group)
                .arg(id)
                .query(&mut self.conn)
                .with_context(|| format!("XACK {stream} {id}"))?;
        }
        Ok(ids.len())
    }

    /// Number of entries currently in a stream (`XLEN`).
    pub fn xlen(&mut self, stream: &str) -> Result<i64> {
        let len: i64 = redis::cmd("XLEN")
            .arg(stream)
            .query(&mut self.conn)
            .with_context(|| format!("XLEN {stream}"))?;
        Ok(len)
    }

    /// A stream is drained when, for every group on it, there are no undelivered entries
    /// (`lag == 0`) and an empty PEL (`pending == 0`). A non-existent stream is vacuously drained.
    pub fn stream_drained(&mut self, stream: &str) -> Result<bool> {
        let groups: redis::RedisResult<Vec<std::collections::HashMap<String, redis::Value>>> =
            redis::cmd("XINFO").arg("GROUPS").arg(stream).query(&mut self.conn);

        let groups = match groups {
            Ok(g) => g,
            // "no such key" → the stream was never created; nothing to drain.
            Err(e) if e.to_string().contains("no such key") => return Ok(true),
            Err(e) => return Err(anyhow::Error::new(e).context(format!("XINFO GROUPS {stream}"))),
        };

        for g in &groups {
            let pending = g.get("pending").and_then(redis_int).unwrap_or(0);
            if pending != 0 {
                return Ok(false);
            }
            // `lag` is the undelivered count; it can be Nil when `entries-read` is unknown — in
            // that case fall back to XLEN (an empty stream has nothing left regardless).
            match g.get("lag").and_then(redis_int) {
                Some(0) => {}
                Some(_) => return Ok(false),
                None => {
                    if self.xlen(stream)? != 0 {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// All known streams drained (the Redis half of quiescence).
    pub fn all_streams_drained(&mut self) -> Result<bool> {
        for (stream, _group) in STREAMS {
            if !self.stream_drained(stream)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Extract the entry ids from an `XAUTOCLAIM` reply (`[cursor, [[id, fields], ...], [...]]`).
fn autoclaim_ids(v: &redis::Value) -> Vec<String> {
    let mut ids = Vec::new();
    if let redis::Value::Bulk(top) = v
        && top.len() >= 2
        && let redis::Value::Bulk(entries) = &top[1]
    {
        for entry in entries {
            if let redis::Value::Bulk(pair) = entry
                && let Some(redis::Value::Data(id)) = pair.first()
                && let Ok(s) = std::str::from_utf8(id)
            {
                ids.push(s.to_string());
            }
        }
    }
    ids
}

/// Parse a Redis integer-ish value (`Int`, or a numeric `Data` bulk) to `i64`.
fn redis_int(v: &redis::Value) -> Option<i64> {
    match v {
        redis::Value::Int(i) => Some(*i),
        redis::Value::Data(d) => std::str::from_utf8(d).ok()?.trim().parse().ok(),
        _ => None,
    }
}

/// The full quiescence predicate (§3.3): every stream drained AND no unsent outbox row. The
/// harness drains to this before asserting the oracles.
pub fn quiescent(redis: &mut Redis, db: &Db) -> Result<bool> {
    Ok(redis.all_streams_drained()? && db.unsent_outbox()? == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db_or_skip() -> Option<Db> {
        let url = std::env::var("DATABASE_URL").ok()?;
        Db::connect(&url).ok()
    }

    fn redis_or_skip() -> Option<Redis> {
        let url = std::env::var("REDIS_URL").ok()?;
        Redis::connect(&url).ok()
    }

    #[test]
    fn db_truncate_resets_cleanly_and_isolation_is_read_committed() {
        let Some(db) = db_or_skip() else {
            eprintln!("skipping: DATABASE_URL unset/unreachable");
            return;
        };
        db.truncate_all().expect("truncate");
        let nonempty = db.nonempty_tables().expect("counts");
        assert!(nonempty.is_empty(), "tables not empty after truncate: {nonempty:?}");
        assert_eq!(db.unsent_outbox().expect("outbox"), 0);
        // Amendment §A4: the substrate default must be READ COMMITTED, not silently stronger.
        assert_eq!(db.isolation_level().expect("iso"), "read committed");
    }

    #[test]
    fn redis_flush_recreates_groups_and_is_quiescent() {
        let (Some(mut r), Some(db)) = (redis_or_skip(), db_or_skip()) else {
            eprintln!("skipping: REDIS_URL/DATABASE_URL unset/unreachable");
            return;
        };
        db.truncate_all().expect("truncate");
        r.flush_and_recreate_groups().expect("flush+recreate");
        // Empty, freshly-created groups: no undelivered, empty PEL → drained.
        assert!(r.all_streams_drained().expect("drained"));
        assert!(quiescent(&mut r, &db).expect("quiescent"));
    }
}
