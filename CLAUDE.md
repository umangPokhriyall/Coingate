## Authoritative specs
- docs/specs/kickoff-brief.md         — strategy, the audit, the primitives, the DoD
- docs/specs/kickoff-amendment-1.md   — chaos enumeration, in-progress key protocol, RC proof
- docs/specs/phase0-spec.md           — CURRENT: substrate, traits, schema baseline, chaos scaffolding

## Hard rules (Phase 0)
1. NO idempotency logic, NO credit-path atomicity rewrite, NO worker reconciliation,
   NO outbox relay, NO chaos harness. Those are Phase 1/2. Phase 0 makes them buildable.
2. The ONLY transaction constructor is store::with_tx, pinned to READ COMMITTED. No other path
   opens a transaction or sets an isolation level.
3. No Arc<Mutex<Store>>. Pool only. No f64 on money. No .unwrap()/.expect() on request/stream/RPC
   data — typed errors or dead-letter.
4. No println!/eprintln!. tracing only.
5. Signer::send MUST take the idempotency key (withdrawal_id) now, even though the worker does not
   reconcile until Phase 1. Trait signatures + with_tx + schema baseline are FROZEN after Phase 0.
6. chaos code lives behind the `chaos` cargo feature and MUST compile to nothing by default.
   CrashPointId is append-only.
7. Phase 0 deps only: diesel(r2d2,postgres,numeric,uuid,chrono,serde_json), r2d2, thiserror,
   tracing, tracing-subscriber. Nothing else.

## Scope discipline
One session = one deliverable. End each with cargo build + clippy + test (and
`cargo test --features chaos` for 0.5), list changes, STOP.
