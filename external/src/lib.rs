//! The mocked outside world behind traits: the chain and the signer become interfaces,
//! Solana/MPC and their deterministic mocks are implementations. The mocks are first-class
//! — the Phase 2 exactly-once proof runs on them.
//!
//! Phase 0 scope: define the trait boundary with the effect-key baked into `Signer::send`,
//! move the existing poller/worker logic behind the traits, and provide the mocks. NO
//! pagination fix and NO reconciliation here (those are Phase 1.4/1.5).

// We use native `async fn` in traits (no `async-trait` dependency). These traits are only
// ever used through generics/concrete types in this workspace, never as `dyn`, so the
// auto-trait-bound caveat the lint warns about does not apply.
#![allow(async_fn_in_trait)]

pub mod chain;
pub mod signer;

pub use chain::{Chain, ChainError, Cursor, DepositEvent, MockChain, SolanaChain, TxKind};
pub use signer::{CountingMockSigner, MpcSigner, SendRequest, Signer, SignerError};
