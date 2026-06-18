pub mod config;
pub mod error;
pub mod idempotency_pg;
pub mod models;
pub mod module;
pub mod money;
pub mod pool;
pub mod reconcile;
pub mod schema;

pub use config::{Config, ConfigError};
// Re-export diesel so I/O-layer consumers (e.g. the api Execute spine) can name
// `diesel::result::Error` — the error type `with_tx`'s closure must return — without taking a
// direct diesel dependency of their own.
pub use diesel;
pub use diesel::pg::PgConnection;
pub use error::StoreError;
pub use idempotency_pg::IdempotencyStorePg;
pub use money::{parse_base_units, MoneyError};
pub use pool::{build_pool, get_conn, with_tx, Pool, PooledConn};
pub use reconcile::{reconcile, Deadlines, DriftReport, Imbalance};

// Store query functions (each takes `&mut PgConnection`).
pub use models::api::*;
