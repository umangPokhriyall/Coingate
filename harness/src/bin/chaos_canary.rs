//! `chaos_canary` — a throwaway target the supervisor uses to prove the abort/restart model
//! (Phase 2 §3.3 "Done when … arm `SelfTest` on a throwaway, observe the abort, and restart it").
//!
//! `SelfTest` is the one `CrashPointId` with a fire-site that lives ONLY in test/canary code,
//! never in a real transaction (chaos_hooks docs). So we cannot arm it on `api`/`worker`/etc.;
//! this canary is its reachable fire-site. Built `--features chaos`:
//!   * armed   (`COINGATE_CHAOS_FIRE=SelfTest`) → `crash_point!` calls `std::process::abort()`
//!     → the child dies with SIGABRT, which the supervisor classifies as an armed crash.
//!   * disarmed                                  → the `crash_point!` is a no-op → exits 0.

fn main() {
    // The single reachable fire-site for `SelfTest`. Without `--features chaos` this is a
    // no-op (and the build prints the note below), so the canary is only meaningful when the
    // harness builds it with the feature on.
    chaos_hooks::crash_point!(chaos_hooks::CrashPointId::SelfTest);

    // Reached only when disarmed: a clean exit, the "restart" half of the proof.
    println!("chaos_canary: disarmed, exiting cleanly");
}
