## Authoritative specs
- docs/specs/kickoff-brief.md         — strategy, the audit, the primitives, the DoD
- docs/specs/kickoff-amendment-1.md   — chaos enumeration, in-progress key protocol, RC proof
- docs/specs/phase0-spec.md           — substrate, traits, schema baseline, chaos scaffolding (DONE)
- docs/specs/phase1-spec.md           — the idempotency core (DONE)
- docs/specs/phase2-spec.md           — the proof (DONE)

## Status
Phase 2 complete. The artifact is `chaos/results/` (the exhaustive crash-point sweep + the
before/after table), the teardown is `docs/DESIGN.md`, the state machines are `docs/LIFECYCLE.md`,
and `README.md` reframes the repo as an exactly-once core. See `chaos/results/summary.md` for the
headline (62/62, 0 conservation violations, 1 send per withdrawal, at READ COMMITTED).

## Hard rules (Phase 2)
1. NO product-logic change, NO DDL, NO new CrashPointId (closed at 15). Phase 2 only proves.
2. Do not touch any frozen contract: with_tx + READ COMMITTED, Chain/Signer, schema baseline,
   the Execute spine, the atomic-credit shape, the worker state machine.
3. The harness is BLACK-BOX: it spawns the real binaries and asserts via DB + reconciler +
   mock-mpc counts. It must not link target crates. This is what lets it run on legacy too.
4. The whole proof runs at READ COMMITTED. Never raise the isolation level. Record the level
   in every run record (Amendment §A4).
5. The crash-point axis is EXHAUSTIVE (every variant × every schedule). The interleaving sweep
   is seeded — state that boundary honestly in DESIGN.md; do not overclaim exhaustiveness there.
6. Build DESIGN.md ONLY from committed chaos/results/ numbers. No marketing language. Keep the
   pre-idempotency baseline runnable; show the bug, then show it gone.
7. Provisioning: the sudo block in phase2-spec §3.1 is operator-run; everything else the agent runs.

## Exception on record (Phase 2.1)
external::MpcSigner::lookup was wired to the mock-mpc GET /lookup endpoint (and the worker builds the
lookup URL from cfg.mpc_base_url). The Phase-1 stub returned None, so the real worker re-sent on a
crash between send and finalize. This completes the mocked-I/O adapter the spec's /lookup endpoint
was built for; the trait signature, worker state machine, schema, with_tx, and atomic-credit shape
are unchanged.

## Scope discipline
One session = one deliverable. End with cargo build + clippy + test green, list changes, STOP.
2.0 = substrate up; 2.1 = sweep green + committed; 2.2 = before/after committed; 2.3 = DESIGN/
LIFECYCLE sourced; 2.4 = README + DoD reported.
