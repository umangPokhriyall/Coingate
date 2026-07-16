# Coingate — an exactly-once processing core

A demonstration of **exactly-once effects over an at-least-once transport**, verified by
crash injection: a supervisor crashes the real binaries at every statement boundary, under
every redelivery schedule, and asserts the invariants after recovery.

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
idempotency key at every external effect boundary.** This repository implements that triad and then
verifies it under fault injection — a supervisor crashes the real binaries at each statement
boundary, restarts them, drains to quiescence, and checks five invariants by reading the database
directly, running an independent reconciler, and counting the effects at the boundary.

The money domain (orders, deposits, withdrawals) is the carrier, not the point. **The crypto surface
is mocked I/O:** the chain and the signer are traits (`external::Chain`, `external::Signer`) with
deterministic mock implementations; the verification runs entirely on those. Nothing here is pinned
to a payment product — the same core applies to any "do this exactly once" workload.

## Repository map

**The core** — the idempotency logic, sans-IO (no Postgres, no Redis, no chain types):
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

**The verification:**
- [`chaos_hooks/`](chaos_hooks/) — the closed `CrashPointId` registry; `crash_point!` compiles to
  nothing without the `chaos` feature.
- [`harness/`](harness/) — the black-box supervisor: spawns the real binaries, arms one crash point
  per run, asserts via DB + reconciler + mock-mpc counts, links no target crate.
- [`mock-mpc/`](mock-mpc/) — the counting signer as an HTTP service (the `/__counts` Invariant #2
  reads).
- [`chaos/results/`](chaos/results/) — the committed sweep, the before/after table, the headline.

## Reproduce it

Requirements: PostgreSQL and Redis running locally, `libpq-dev`, and the diesel CLI
(`cargo install diesel_cli --no-default-features --features postgres`).

```bash
# a role and database matching the dev DATABASE_URL
sudo -u postgres psql -c "CREATE ROLE coingate WITH LOGIN PASSWORD 'coingate' CREATEDB;"
sudo -u postgres psql -c "CREATE DATABASE coingate OWNER coingate;"

# .env at the repo root (see .env.example)
cat > .env <<'EOF'
DATABASE_URL=postgres://coingate:coingate@localhost/coingate
REDIS_URL=redis://127.0.0.1:6379
JWT_SECRET=dev-only-not-a-real-secret
SOLANA_RPC_URL=http://127.0.0.1:9999/unused-by-harness
LISTEN_ADDR=127.0.0.1:8080
MPC_BASE_URL=http://127.0.0.1:8090
EOF

diesel migration run

cargo build -p api -p processor -p worker -p relay -p mock-mpc -p reconciler -p harness \
  --features "api/chaos processor/chaos worker/chaos relay/chaos"
target/debug/harness sweep        # writes chaos/results/sweep-main.jsonl + summary.md
```

`SOLANA_RPC_URL` is a placeholder — the harness injects deposits directly (simulating the poller)
and never calls a live chain. `MPC_BASE_URL` points at the `mock-mpc` server the harness starts.

The result is **hardware-independent**: a conservation invariant (0 violations, 1 send per
withdrawal) is a *logical* property, not a timing one, and the crash-point enumeration is
deterministic. It reproduces on any x86-64 with Postgres + Redis + a stable Rust toolchain.
