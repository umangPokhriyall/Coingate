use crate::config::Config;
use crate::error::StoreError;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool as R2d2Pool, PooledConnection};

/// The workspace-wide connection pool. Replaces the old single-`PgConnection` `Store`
/// behind an `Arc<Mutex<_>>`: every binary builds one of these and shares it freely.
pub type Pool = R2d2Pool<ConnectionManager<PgConnection>>;

/// A connection checked out of the pool. Derefs to `PgConnection`, so it can be passed
/// (as `&mut`) to any store function.
pub type PooledConn = PooledConnection<ConnectionManager<PgConnection>>;

/// Build the pool from config. Fails fast on a bad/unreachable `db_url` because the
/// builder eagerly opens the initial connection.
pub fn build_pool(cfg: &Config) -> Result<Pool, StoreError> {
    let manager = ConnectionManager::<PgConnection>::new(&cfg.db_url);
    let pool = Pool::builder().max_size(16).build(manager)?;
    Ok(pool)
}

/// Check out a connection for non-transactional, single-statement work (reads, single
/// writes). Multi-statement atomic work must go through [`with_tx`] instead.
pub fn get_conn(pool: &Pool) -> Result<PooledConn, StoreError> {
    Ok(pool.get()?)
}

/// The SINGLE transaction entry point for the whole workspace.
///
/// Pins `READ COMMITTED` explicitly so no path silently depends on a stronger isolation
/// level (Amendment §A4). This is the only place in the codebase that constructs a
/// transaction or sets an isolation level (Phase 0 hard rule #2).
pub fn with_tx<T, F>(pool: &Pool, f: F) -> Result<T, StoreError>
where
    F: FnOnce(&mut PgConnection) -> Result<T, diesel::result::Error>,
{
    let mut conn = pool.get()?;
    let value = conn
        .build_transaction()
        .read_committed()
        .run(|c| f(c))?;
    Ok(value)
}
