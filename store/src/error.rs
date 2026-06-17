use diesel::r2d2::PoolError;
use diesel::result::Error as DieselError;

/// The crate-wide storage error. Wraps the two failure modes a pooled, transactional
/// store has: getting a connection out of the pool, and executing a query/transaction.
#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("connection pool error: {0}")]
    Pool(#[from] PoolError),

    #[error("database error: {0}")]
    Query(#[from] DieselError),
}
