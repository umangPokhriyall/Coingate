//! # `idempotency` — the exactly-once core (sans-IO)
//!
//! Phase 1's product, distilled to a pure crate: given facts, it returns decisions. No I/O type
//! appears in it — no `diesel`, no `redis`, no `actix`, no `solana` (verify the manifest). It is
//! the analogue of a TCP server's frozen protocol `core`.
//!
//! * [`key`] — the [`IdempotencyKey`] newtype and the [`request_fingerprint`] guard.
//! * [`lifecycle`] — the Order and Withdrawal state machines and their transition guards.
//! * [`decision`] — [`decide`], the pure §A2 branch selector ([`Decision`]).
//! * [`store`] — the [`IdempotencyStore`] I/O boundary trait plus [`KeyRecord`] / [`KeyStatus`]
//!   / [`Acquire`] / [`StoreError`].
//!
//! The public API here is **FROZEN after Phase 1**.

pub mod decision;
pub mod key;
pub mod lifecycle;
pub mod store;

pub use decision::{decide, idempotency_lease, Decision, IDEMPOTENCY_LEASE_SECS};
pub use key::{request_fingerprint, IdempotencyKey};
pub use lifecycle::{order_can_mark_paid, withdrawal_can_finalize, withdrawal_next};
pub use store::{Acquire, IdempotencyStore, KeyRecord, KeyStatus, StoreError};
