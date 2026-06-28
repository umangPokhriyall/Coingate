//! The Phase 2 black-box supervisor harness.
//!
//! Session 2.0 builds the SUBSTRATE only: the supervisor (spawn/arm/kill/restart + quiescence),
//! the truncate/flush fixtures, and the proof that an armed `SelfTest` aborts and restarts. The
//! oracles, the exhaustive enumeration, and the seeded sweep land in Session 2.1.
//!
//! It links nothing from the target crates (api/processor/worker/relay/store/idempotency): it
//! drives them as subprocesses and asserts via direct Postgres + the `reconciler` + `mock-mpc`
//! counts. The one in-workspace dependency is `chaos_hooks` — the crash-point registry it
//! iterates by name.

pub mod beforeafter;
pub mod enumerate;
pub mod fixtures;
pub mod oracles;
pub mod report;
pub mod schedules;
pub mod supervisor;
pub mod workload;
