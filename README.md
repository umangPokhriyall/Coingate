# An exactly-once processing core

A small, falsifiable demonstration of **exactly-once effects over an at-least-once transport** — the
problem at the center of any system that moves something exactly once (a payment, a job, a boot)
across crashes and redeliveries.

## The result

> **A process crash at every statement boundary in the pipeline, under every redelivery schedule:
> 0 conservation violations, 1 send per withdrawal — across 62 runs, at `READ COMMITTED`.**

Source: [`chaos/results/summary.md`](chaos/results/summary.md) (62/62 passed; 14/14 crash points
reached under their driving workload). The same black-box harness, pointed at the pre-rebuild
baseline, shows the bugs it removed — including a **double-credit at `READ COMMITTED`**
(`deposits=1000, accounted=2000`) that the rebuilt core does not have:
[`chaos/results/before-after.md`](chaos/results/before-after.md).

The teardown — each failure mode, its fix, and the committed row that proves it, plus the takeover
safety theorem and the isolation argument — is in **[`docs/DESIGN.md`](docs/DESIGN.md)**. The state
machines are in [`docs/LIFECYCLE.md`](docs/LIFECYCLE.md).

## What it is

Exactly-once is three things composed: **at-least-once delivery + idempotent processing + an
idempotency key at every external effect boundary.** This repo implements that triad and then
*proves* it under fault injection — a supervisor crashes the real binaries at each statement
boundary, restarts them, drains to quiescence, and checks five invariants by reading the database
directly, running an independent reconciler, and counting the effects at the boundary.

The money domain (orders, deposits, withdrawals) is the carrier, not the point. **The crypto surface
is mocked I/O:** the chain and the signer are traits (`external::Chain`, `external::Signer`) with
deterministic mock implementations; the proof runs entirely on those. Nothing here is pinned to a
payment product — the same core is the engine for any "do this exactly once" workload.

## Repository map

**The product** — the idempotency core, sans-IO (no Postgres, no Redis, no chain types):
- [`idempotency/`](idempotency/) — `decide`, the key lifecycle, the `IdempotencyStore` trait,
  request fingerprinting. The boundary is real: it compiles without a database.

**The wiring** — the I/O that drives the core:
- [`store/`](store/) — the pool, the single `with_tx` (pinned `READ COMMITTED`), the schema, the
  Postgres `IdempotencyStore`, the `reconcile` drift oracle.
- [`api/`](api/) — `/orders` and `/withdrawals` over the inbound-key Execute spine.
- [`processor/`](processor/) — the atomic deposit credit (`ON CONFLICT (tx_hash) DO NOTHING
  RETURNING`, credit only on first insert).
- [`worker/`](worker/) — the withdrawal state machine; reconciles from `processing` via `lookup`
  instead of re-sending.
- [`relay/`](relay/) + the outbox — replaces the API→Redis dual-write.
- [`external/`](external/) — `Chain` / `Signer` traits + their mocks (the mocked I/O).
- [`poller/`](poller/), [`reconciler/`](reconciler/) — gap-free backfill; the drift sweep as a binary.

**The proof** — the falsifiable artifact:
- [`chaos_hooks/`](chaos_hooks/) — the closed `CrashPointId` registry; `crash_point!` compiles to
  nothing without the `chaos` feature.
- [`harness/`](harness/) — the black-box supervisor: spawns the real binaries, arms one crash point
  per run, asserts via DB + reconciler + mock-mpc counts, links no target crate.
- [`mock-mpc/`](mock-mpc/) — the counting signer as an HTTP service (the `/__counts` Invariant #2
  reads).
- [`chaos/results/`](chaos/results/) — the committed sweep, the before/after table, the headline.

## The microVM bridge

The same primitives map directly onto an orchestrator that boots one microVM per job exactly once:

- **inbound idempotency key** → job-submission dedup (a retried submit is one job).
- **consumer-side atomic dedup** (`ON CONFLICT DO NOTHING RETURNING`, effect gated on first insert)
  → one microVM per job under redelivery.
- **effect-boundary key** (the `withdrawal_id` on `send`) → boot-exactly-once: the launch is the
  guarded external effect; a crash mid-launch reconciles, it does not double-boot.
- **transactional outbox** → enqueue a job without a dual-write between the database and the queue.
- **reconciler** → the orchestrator's drift sweep: recompute the invariant from the ground truth and
  assert no job is stranded or double-run.

## Reproduce it

The environment (Postgres + Redis + diesel) and the run are in
[`docs/specs/phase2-spec.md`](docs/specs/phase2-spec.md) §3. With those up:

```bash
cargo build -p api -p processor -p worker -p relay -p mock-mpc -p reconciler -p harness \
  --features "api/chaos processor/chaos worker/chaos relay/chaos"
target/debug/harness sweep        # writes chaos/results/sweep-main.jsonl + summary.md
```

The proof is **hardware-independent**: a conservation invariant (0 violations, 1 send per
withdrawal) is a *logical* property, not a timing one, and the crash-point enumeration is
deterministic. It reproduces on any x86-64 with Postgres + Redis + a stable Rust toolchain —
no PMU, no special silicon. That is the deliberate counterpart to the sibling repos whose
claims *are* silicon-dependent (the TCP-server I/O teardown, the low-latency order book, the
transcoding control plane), which were re-run and re-measured on rented AMD EPYC bare metal.
Each repo states which of its claims depend on hardware and which do not — that separation is
itself part of the signal.
