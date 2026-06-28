# Coingate — Phase 2 Specification: The Proof (the falsifiable artifact)

**Companion to:** `docs/specs/kickoff-brief.md`, `docs/specs/kickoff-amendment-1.md`, `docs/specs/phase0-spec.md`, `docs/specs/phase1-spec.md`. Read all five first.
**This is the complete, authoritative Phase 2 spec.** The executing agent needs no other context.
**Scope:** prove the Phase 1 guarantees under fault injection. Provision the environment; build a black-box supervisor harness; run the **exhaustive** crash-point × redelivery enumeration plus a seeded interleaving sweep, all at `READ COMMITTED`; produce the before/after table against the legacy `pre-idempotency` baseline; assert the **five invariants** (conservation, at-most-once send, replay-safety, no-stranded-funds, reconciler-clean) after every schedule; write `DESIGN.md` (the teardown, every claim sourced) and `LIFECYCLE.md`; reframe `README.md` and verify the full DoD.
**Audience:** Claude Code, with one operator-run provisioning step (clearly marked). Authoritative. This phase contains **no** new product logic and **no** DDL — it changes nothing about the system except adding the `harness/` crate, the `mock-mpc` server, `chaos/results/`, and docs. If a guarantee cannot be proven, the honest result is committed, not hidden (NORTH-STAR §3: an honest negative with a profile is elite signal).

### Amendment alignment (this phase is where §A1–§A5 are *demonstrated*)
- **§A1 exhaustive enumeration** → Session 2.1: every `CrashPointId` × every redelivery schedule, deterministically; seeded sweep on top for interleavings; registry-closure + reachability check.
- **§A2 in-progress key recovery** → Session 2.1: the crash-mid-Execute → replay-before-expiry (409) / replay-after-expiry (exactly-once) schedules.
- **§A3 relay duplicate-publish** → Session 2.1: kill at `RelayAfterXaddBeforeMarkSent`, assert consumer absorbs.
- **§A4 RC, demonstrate-don't-document** → whole harness runs at RC; Session 2.2 shows the legacy double-credit at RC as the counterexample.
- **§A5 reconciliation as Invariant #5** → Session 2.1: `store::reconcile` is run after every schedule and asserted clean.

---

## 1. Phase 2 in one paragraph

Phase 1 installed the guarantees by construction; nothing yet *proves* them under crashes. Phase 2 builds a supervisor that runs the real `api`/`processor`/`worker`/`relay` binaries (built `--features chaos`) as subprocesses against a live Postgres + Redis and a counting mock-MPC HTTP server, drives logical payments and withdrawals through them, arms exactly one crash point per run via `COINGATE_CHAOS_FIRE`, lets the armed process abort, restarts it, drains to quiescence, and asserts the five invariants by reading the database directly and running the `reconciler`. It does this for **every** `CrashPointId` crossed with **every** redelivery/concurrent-delivery schedule — an enumeration, not a sample — and layers a seeded random interleaving sweep on top for the thread-ordering it cannot enumerate. The identical black-box harness is then pointed at a legacy `pre-idempotency` baseline (Phase-0 logic, instrumented only with the same crash points) to produce the before/after table: which seam breaks the old code, and the rebuilt code clean beside it — all at `READ COMMITTED`. The committed output in `chaos/results/` is the artifact: *a process crash at every statement boundary in the pipeline, under every redelivery schedule — 0 conservation violations, 1 send per withdrawal.* `DESIGN.md` turns that output into the teardown; `README.md` reframes the repo around it.

### 1.1 What Phase 2 must NOT do
- No change to any Phase-0/Phase-1 frozen contract (`with_tx` + RC, `Chain`/`Signer`, the schema baseline, the Execute spine, the atomic-credit shape, the worker state machine).
- No DDL. No new product logic. `CrashPointId` is **not** extended (Phase 1 closed it at 15); Phase 2 only *consumes* the registry.
- No raising of the isolation level anywhere. The proof's value is that it holds at the default.

---

## 2. Workspace additions & dependencies

```
harness/                     # NEW — the black-box supervisor (a dev/test binary, not shipped)
  src/main.rs                # CLI: enumerate | sweep | before-after | one <variant> <schedule>
  src/supervisor.rs          # spawn/arm/kill/restart subprocesses; quiescence detection
  src/workload.rs            # driver primitives: POST /orders, POST /withdrawals, XADD deposit, drain
  src/oracles.rs             # the five invariant checkers (direct Postgres + reconciler + mock-mpc counts)
  src/schedules.rs           # the redelivery/concurrent-delivery schedule set; the A2/A3 scripted schedules
  src/report.rs              # machine-readable run records + the generated markdown summary
mock-mpc/                    # NEW — counting mock signer as an HTTP service (the worker's real MpcSigner calls it)
  src/main.rs                # POST /wallets/{id}/send (counts per idempotency_key, dedups), GET /lookup, GET /__counts
chaos/
  results/                   # COMMITTED outputs: the sweep, the before/after table, the headline
docs/
  DESIGN.md                  # the teardown — every claim sourced to chaos/results/
  LIFECYCLE.md               # the two state machines, drawn
README.md                    # reframed: exactly-once core
.env.example                 # the five+ env vars the binaries require (no secrets committed)
```

**Dependency allowlist (Phase 2 — reuse the workspace; add only what is named):**
- `harness`: `reqwest`, `redis`, `diesel`/`r2d2`, `tokio`, `serde`/`serde_json` (all already in-tree), plus `rand` (seeded RNG for the interleaving sweep — fixed seeds, committed). Reads the DB directly and shells out to the `reconciler` binary; links **nothing** from the target crates (black-box).
- `mock-mpc`: reuse `actix-web` (already a workspace dep) + `serde_json`. No new deps.
- No new deps in `api`/`processor`/`worker`/`relay`/`store`/`idempotency` — they are unchanged.

---

## 3. Session 2.0 — Environment provisioning & harness substrate

The harness needs a live Postgres + Redis and the libs diesel/`pq-sys` link against. The DB-backed tests from Phase 1 were skipped without `DATABASE_URL`; here they become mandatory infrastructure.

### 3.1 OPERATOR — run once, with sudo (Claude Code cannot sudo non-interactively)
Ubuntu/Debian (the Latitude.sh / bare-metal target). Hand these to the human; they are also reproduced in `docs/specs/phase2-spec.md` so the phase is self-contained.
```bash
# system packages: Postgres, Redis, the libpq headers diesel needs, build toolchain
sudo apt-get update
sudo apt-get install -y postgresql postgresql-contrib redis-server libpq-dev pkg-config build-essential

# start + enable the services
sudo systemctl enable --now postgresql
sudo systemctl enable --now redis-server

# a login role that can create databases (matches the dev DATABASE_URL below)
sudo -u postgres psql -c "CREATE ROLE coingate WITH LOGIN PASSWORD 'coingate' CREATEDB;"

# gen_random_uuid(): built-in on PG13+, but enable pgcrypto defensively for older servers
sudo -u postgres psql -c "CREATE DATABASE coingate OWNER coingate;"
sudo -u postgres psql -d coingate -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
```
Sanity (operator or agent): `pg_isready` returns ready; `redis-cli ping` returns `PONG`.

### 3.2 CLAUDE CODE — run these (no sudo)
```bash
# diesel CLI (Postgres only) — needs libpq-dev from 3.1
cargo install diesel_cli --no-default-features --features postgres

# .env at repo root (the binaries fail-fast on missing vars — Phase 0 Config::from_env)
cat > .env <<'EOF'
DATABASE_URL=postgres://coingate:coingate@localhost/coingate
REDIS_URL=redis://127.0.0.1:6379
JWT_SECRET=dev-only-not-a-real-secret
SOLANA_RPC_URL=http://127.0.0.1:9999/unused-by-harness
LISTEN_ADDR=127.0.0.1:8080
MPC_BASE_URL=http://127.0.0.1:8090
EOF
cp .env .env.example   # then blank the secret value in .env.example before committing

# schema
diesel migration run    # applies the Phase-0 baseline; verify with `diesel migration list`
```
`SOLANA_RPC_URL` is a placeholder — the harness injects deposits by `XADD` directly (simulating the poller) and never calls a live chain. `MPC_BASE_URL` points at the `mock-mpc` server the harness starts; the worker runs **unmodified** (real `MpcSigner`, real HTTP) against it.

### 3.3 The harness substrate (build this session)
- **Supervisor (`supervisor.rs`):** spawn a target binary as a child with a per-run env overlay (`COINGATE_CHAOS_FIRE=<VariantName>` to arm one point, or unset to disarm); detect an armed abort (non-zero/`SIGABRT` exit); restart on demand; tear down cleanly. The arming string is the `CrashPointId` name (Phase 1's `name()` mapping); the supervisor iterates `CrashPointId::ALL` by name (do not hardcode the list — read it from `chaos_hooks`).
- **DB fixture:** between runs, truncate all tables to a clean slate (`TRUNCATE ... RESTART IDENTITY CASCADE`) — faster and more deterministic than per-run databases. Confirm the isolation level in use is `READ COMMITTED` and record it in every run record (Amendment §A4).
- **Redis fixture:** between runs, `FLUSHDB` and recreate the consumer groups the services expect (`payment_transactions` / `withdrawal_requests`).
- **`mock-mpc` server:** `POST /wallets/{id}/send` with body carrying `idempotency_key` → increments a per-key **call counter**, dedups (returns the same deterministic signature on repeat calls for a key, modeling an idempotent signer), returns `{signature}`; `GET /lookup?key=` → returns the prior signature **without** incrementing (the worker's reconciliation path); `GET /__counts` → the per-key call counts for the oracle. The call counter is what Invariant #2 reads — a worker that re-sends drives a key's count to 2 even though the signature is deduped.
- **Quiescence:** a predicate that returns true when both streams have `XLEN`==0 with empty PELs **and** `outbox WHERE sent_at IS NULL` is empty — the harness drains to quiescence before asserting.

**Done when:** `pg_isready`/`redis-cli ping` green; `diesel migration run` applied; `.env` present and `.env.example` committed (secret blanked); the supervisor can spawn `api`, arm `SelfTest` on a throwaway, observe the abort, and restart it; `mock-mpc` answers `/send`,`/lookup`,`/__counts`; the truncate/flush fixture resets cleanly.

---

## 4. Session 2.1 — The oracles, the exhaustive enumeration, the seeded sweep

### 4.1 The five invariant oracles (`oracles.rs`) — all black-box (DB + reconciler + mock-mpc)
1. **Conservation (the headline):** per `(merchant, token)`, `Σ` confirmed deposit amounts (attributed deposit→order→app→merchant) equals the balance identity `available + locked + Σ(completed withdrawals)`. Compare by `BigDecimal` value (scale-insensitive). This is the same identity `store::reconcile` computes — assert it independently here *and* via the reconciler (Invariant #5), so agreement is cross-checked.
2. **At-most-once send:** for every withdrawal that reached a terminal state, the `mock-mpc` `/__counts` for its `withdrawal_id` is exactly **1**. A blind re-send shows 2 → fail.
3. **Replay-safety:** when the schedule issued M concurrent identical POSTs (same `Idempotency-Key`), exactly one `orders` row (per `(app_id,order_id)`) / one `withdrawals` row / one balance debit exists.
4. **No-stranded-funds:** after quiescence, no `locked_balance` lacks a corresponding withdrawal in a legal state; no `outbox` row is unsent; every locked withdrawal has been delivered (PEL empty).
5. **Reconciler-clean:** shell out to the `reconciler` binary against the same DB; assert exit 0 / `DriftReport::is_clean()`.

A run **passes** iff all five hold after drain. Each run emits a record `{branch, crash_point, schedule, isolation:"read committed", violations:[...]}`.

### 4.2 The redelivery / concurrent-delivery schedule set (`schedules.rs`)
The orthogonal axis to crash points. At minimum:
- `Single` — one delivery, no duplication.
- `DuplicateStream` — the same logical event enqueued twice (models the poller / relay double-publish).
- `ConcurrentConsumers` — two instances of the consuming service in one group, with a duplicate so the two entries can land on different consumers (the `XAUTOCLAIM`-steal / two-workers case).
- `RestartRedelivery` — the armed crash leaves the entry in the PEL; the restarted consumer `XAUTOCLAIM`s and reprocesses (this is the real redelivery mechanism, not a simulation).

### 4.3 The exhaustive enumeration (Amendment §A1)
For **every** `CrashPointId` in `ALL` (excluding `SelfTest`) **×** every schedule in 4.2: arm the point on the owning service, run the **reachability-driving workload** for that point (a fresh order+deposit reaches the `Proc*` seams; a fresh withdrawal reaches `Idem*`/`Withdraw*`/`Worker*`; an outbox row reaches `Relay*`), let it abort, restart, drain, assert all five oracles.

**Registry-closure + reachability check (replaces a static grep):** the armed run for each variant **must** produce an abort under its driving workload. If a variant never aborts, its fire-site is missing or unreachable — **fail the closure check**. This proves, dynamically, that every one of the 15 variants has a live, reachable fire-site (the Phase-1 DoD claim, now verified by execution).

### 4.4 The scripted §A2 in-progress-key schedules
Beyond the generic enumeration, add explicit recovery schedules:
- Arm `IdemAfterEffectBeforeComplete` (or `IdemAfterCompleteBeforeCommit`) on `api`; POST `/withdrawals` (the api aborts mid-Execute — neither effect nor completion committed; the key sits `in_progress`).
  - **(a)** Replay the same key **before** the 30s lease expires → assert `409` + `Retry-After`.
  - **(b)** Wait past the lease, replay → assert the **takeover** re-executes and there is **exactly one** withdrawal + one lock (exactly-once), conservation holds.
- Arm the `IdemAfterAcquireBeforeExecute` seam → same in_progress recovery, asserting the effect never partially applied.

These directly exercise the takeover safety theorem (Amendment §A2) end-to-end.

### 4.5 The scripted §A3 relay duplicate-publish schedule
Arm `RelayAfterXaddBeforeMarkSent`; let the relay publish then abort before marking sent; restart → the relay republishes the same outbox row (by design); assert the **worker absorbs the duplicate** (`pending→processing` guard + `withdrawal_id` reconciliation): one send (`/__counts`==1), conservation clean. This converts the "at-least-once is a feature" axiom into a committed demonstration.

### 4.6 The seeded interleaving sweep (on top of, not instead of, §4.3)
For the thread-ordering enumeration cannot cover: drive N concurrent mixed workloads (orders, withdrawals, duplicate deposits) with `ConcurrentConsumers` and small randomized delays from a **fixed seed set** (commit the seeds). Assert the five oracles after each. This catches races between, not within, the enumerated crash points.

**Output:** write machine-readable run records to `chaos/results/sweep-main.jsonl` (or `.csv`) and a generated `chaos/results/summary.md` with the headline line and the per-crash-point pass grid. All runs at `READ COMMITTED` (recorded in each record).

**Done when:** the full enumeration runs green on `main` (every variant × every schedule → all five invariants hold); the closure/reachability check confirms all 15 variants aborted under their driving workload; the §A2 and §A3 scripted schedules pass; the seeded sweep passes for the committed seeds; `chaos/results/` holds the machine-readable sweep + summary.

---

## 5. Session 2.2 — The before/after against the legacy baseline (Amendment §A1, §A4)

The headline is not "the rebuilt code passes" — it is "the old code fails *here*, and the rebuilt code is clean *there*, under the identical harness."

### 5.1 Construct the legacy baseline
1. Tag the **end-of-Phase-0** commit (legacy processing logic on the fixed substrate) as `pre-idempotency`. (At that commit: the processor writes order/deposit/balance as three separate statements, the worker blind-sends, `/withdrawals` dual-writes — and `chaos_hooks` exists but with only `SelfTest`.)
2. Branch `pre-idempotency-chaos` off the tag and apply **instrumentation only** — no logic change: copy the `CrashPointId` variants and place the `crash_point!` fire-sites at the legacy seams that *structurally exist* (the three-statement credit path → `ProcAfter*`; the blind-send → `WorkerAfter*`; the DB-commit→`XADD` dual-write window → the nearest seam). Commit.

### 5.2 Run the identical harness on both
Because the harness is **black-box**, it runs unchanged against either branch's binaries (point the supervisor at the `pre-idempotency-chaos` build). Crash points that guard mechanisms **absent** in legacy (`IdemAfter*`, `WithdrawAfterOutbox*`, `Relay*`) have no legacy fire-site; for those the table row reads *"mechanism absent in legacy"* and the corresponding anomaly is shown by a direct demonstration instead of a crash:
- legacy `/orders` replay (same key) → **two orders** (no inbound dedup);
- legacy `/withdrawals` retry → **double-lock** (no inbound dedup, no natural key);
- legacy dual-write: kill between DB commit and `XADD` → **funds locked, no work item** (stranded).

### 5.3 The §A4 counterexample
Run the legacy credit path under `DuplicateStream`/`ConcurrentConsumers` **at `READ COMMITTED`** → demonstrate the **double-credit** (legacy reads `order.status` unlocked then credits; the credit is not gated on a first-time insert). Capture it as the concrete counterexample, then show `main` clean under the same schedule at the same isolation level. "We removed the anomaly at the default isolation level" is shown, not asserted.

### 5.4 The table
Generate `chaos/results/before-after.md`: one row per `CrashPointId` (and one per mechanism-absence anomaly) → **legacy: <violation type>** | **rebuilt: clean**. Commit the raw legacy run records (`sweep-pre-idempotency.jsonl`). This table is the spine of `DESIGN.md`.

**Done when:** `pre-idempotency` tag + `pre-idempotency-chaos` branch exist; the identical harness produced violation records on legacy and clean records on `main`; `before-after.md` + both raw sweeps are committed; the legacy double-credit-at-RC counterexample is captured.

---

## 6. Session 2.3 — `DESIGN.md` (the teardown) + `LIFECYCLE.md`

`DESIGN.md` is the actual artifact a Principal Engineer reads. Build it **only** from committed numbers (NORTH-STAR §5: invent nothing). Structure:
1. **The thesis** — exactly-once = at-least-once delivery + idempotent processing + a key at every external effect boundary. One paragraph.
2. **Each failure mode → fix → proof.** For every §1 audit finding (non-atomic credit, blind-send, dual-write, dropped/lost messages, the in-progress-key hole): state the bug, the fix, and cite the `chaos/results/` row(s) that prove it. Paraphrase; no marketing language.
3. **The takeover safety theorem** — *verbatim from Amendment §A2*: the guarded effect and the completion flip commit in one transaction, so `completed ⟺ effect applied`; an observed `in_progress` proves the effect did not commit; takeover re-executes exactly once; safety is from atomicity, not the lease; the conditional-completion `UPDATE` admits one winner. Cite the §A2 scripted-schedule results (4.4).
4. **The isolation argument (§A4)** — why `INSERT … ON CONFLICT DO NOTHING RETURNING` (unique-index dedup, isolation-independent) + the atomic upsert increment + `SELECT … FOR UPDATE` suffice at `READ COMMITTED` with no `SERIALIZABLE` dependency, followed by the legacy double-credit-at-RC counterexample (5.3) as proof the bar is real.
5. **At-least-once is a feature (§A3)** — the relay duplicate-publish demonstration (4.5).
6. **The headline + the table** — the before/after table (5.4) inline, and the one-line claim: *a crash at every statement boundary in the pipeline, under every redelivery schedule — 0 conservation violations, 1 send per withdrawal*, with its source file.
7. **Threats to validity / residual risk (honesty-as-signal).** Name the boundaries: the real signer's idempotency is a *contract assumed of the signer*, modeled by `mock-mpc` (the system is safe **given** an idempotent signer + the reconciliation path; a non-idempotent signer with a lost ack remains a fundamental limit); the `SolanaChain` paginated backfill is RPC-bound and is gap-free-by-construction rather than crash-swept; the interleaving sweep is seeded, not exhaustive (only the crash-point axis is exhaustive — state this precisely so the categorical claim is not overstated).

`LIFECYCLE.md`: draw both state machines (Order: `pending→paid`; Withdrawal: `pending→processing→completed|failed`) with the legal-transition guards and where each crash point sits, as Mermaid `stateDiagram-v2` (renders on GitHub) with an ASCII fallback.

**Done when:** every quantitative claim in `DESIGN.md` cites a committed `chaos/results/` file; the theorem, the RC argument + counterexample, the §A3 demonstration, and the threats-to-validity section are all present; `LIFECYCLE.md` renders both machines.

---

## 7. Session 2.4 — `README.md` reframe + DoD verification

**README (60-second grasp, pinned-ready):** reframe the repo as an **exactly-once payment-processing core** — what it is; the conservation headline with its source file; the repository map (`idempotency` = the product; `store`/`api`/`processor`/`worker`/`relay` = the wiring; `harness`/`mock-mpc`/`chaos/results` = the proof); the microVM bridge (inbound keys = job-submission dedup; consumer-side atomic dedup = one microVM per job under redelivery; effect-boundary key = boot-exactly-once; outbox = enqueue without dual-write; reconciler = orchestrator drift sweep). **Demote the crypto surface explicitly to "mocked I/O"** (`Chain`/`Signer` behind traits) and **de-pin** from any payment-product framing. Link `DESIGN.md` as the deep dive.

**DoD verification:** walk the brief §6 (as amended by Amendment "Net effect on the DoD") and the Phase-2 items below, reporting each. In particular confirm: the proof is **exhaustive** on the crash-point axis (not sampled); the before/after table is self-explanatory; **0** conservation violations and **1** send per withdrawal on `main`; the run record shows `READ COMMITTED`; registry closure verified dynamically; the self-audit gate (NORTH-STAR §4) — the owner can re-derive the takeover safety argument and the credit-atomicity argument from memory.

**Done when:** README reframed and de-pinned; DoD reported item-by-item against committed evidence; `docs/specs` updated (this spec + the pending CLAUDE.md Phase-1/2 doc commit the agent flagged).

---

## 8. Phase 2 Definition of Done

1. **Environment** provisioned and reproducible: the operator sudo block + the agent steps in §3 bring up Postgres/Redis/diesel; `.env.example` committed (no secret); `diesel migration run` clean.
2. **Harness** (`harness/` + `mock-mpc/`) is a black-box supervisor: spawns/arms/kills/restarts the real binaries, drives them over HTTP+Redis, asserts via direct Postgres + the `reconciler` + `mock-mpc` counts, links nothing from the targets, runs at `READ COMMITTED`.
3. **Exhaustive enumeration (§A1):** every `CrashPointId` × every redelivery schedule on `main` → all five invariants hold; dynamic registry-closure/reachability confirms all 15 variants have a live fire-site; seeded interleaving sweep green for committed seeds.
4. **§A2 / §A3 scripted schedules** pass: crash-mid-Execute → 409-before-expiry and exactly-once-after-expiry; relay republish absorbed (one send, conservation clean).
5. **Before/after (§A1, §A4):** `pre-idempotency` tag + instrumented legacy branch; identical harness produced legacy violations and `main` clean; the **legacy double-credit at `READ COMMITTED`** counterexample captured; `chaos/results/before-after.md` + both raw sweeps committed.
6. **The five invariants** (conservation, at-most-once send, replay-safety, no-stranded-funds, reconciler-clean) asserted after **every** schedule — reconciliation is exercised under faults, not decoration (§A5).
7. **`DESIGN.md`**: each failure mode → fix → proof, every claim sourced; the takeover safety theorem (verbatim); the RC argument + counterexample; the §A3 demonstration; an honest threats-to-validity section. **`LIFECYCLE.md`**: both state machines rendered.
8. **`README.md`** reframed as the exactly-once core, crypto demoted to mocked I/O, de-pinned, headline + source above the fold.
9. **Headline result committed:** *a process crash at every statement boundary in the pipeline, under every redelivery schedule — 0 conservation violations, 1 send per withdrawal* — with the `chaos/results/` file behind it. Honest residual-risk stated.
10. No frozen contract changed; no DDL; `CrashPointId` unchanged at 15. `cargo build`/`clippy`/`test` green; per-crate `--features chaos` green; deps within §2.
11. **Self-audit gate:** the owner can re-derive, unaided, (a) why `ON CONFLICT DO NOTHING RETURNING` + the atomic upsert make the credit exactly-once at RC, and (b) why `completed ⟺ effect applied` makes takeover safe. If not, it is not owned and the repo is not done.

This is the artifact. With it, the central claim is categorical and falsifiable, the bug is shown and shown gone under the same harness, and the whole thing is reproducible from a clean machine via §3. Then — and only then — distribution (NORTH-STAR §7): the artifact is the opener, `DESIGN.md` is the deep dive, the headline is the post.

---

# Appendix A — `CLAUDE.md` update for Phase 2

```markdown
## Authoritative specs
- docs/specs/kickoff-brief.md         — strategy, the audit, the primitives, the DoD
- docs/specs/kickoff-amendment-1.md   — chaos enumeration, in-progress key protocol, RC proof
- docs/specs/phase0-spec.md           — substrate, traits, schema baseline, chaos scaffolding (DONE)
- docs/specs/phase1-spec.md           — the idempotency core (DONE)
- docs/specs/phase2-spec.md           — CURRENT: the proof

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
7. Provisioning: the sudo block in §3.1 is operator-run; everything else the agent runs.

## Scope discipline
One session = one deliverable. End with cargo build + clippy + test green, list changes, STOP.
2.0 ends when the substrate is up; 2.1 when the sweep is green + committed; 2.2 when before/after
is committed; 2.3 when DESIGN/LIFECYCLE are sourced; 2.4 when README+DoD are reported.
```

---

# Appendix B — Claude Code execution plan (5 sessions)

| # | Session | Deliverable | Done when |
|---|---|---|---|
| 2.0 | Provision + substrate | §3.1 (operator) then §3.2/§3.3: services up, migrations, supervisor, mock-mpc, fixtures | supervisor arms `SelfTest` and observes abort; mock-mpc answers; reset is clean |
| 2.1 | Oracles + exhaustive sweep | five oracles; every variant × every schedule; closure/reachability; §A2/§A3 schedules; seeded sweep | all green on `main`; 15/15 variants abort; `chaos/results/` sweep + summary committed |
| 2.2 | Before/after | `pre-idempotency` tag + instrumented legacy branch; identical harness on both; RC counterexample | legacy violations + `main` clean; `before-after.md` + raw sweeps committed |
| 2.3 | DESIGN + LIFECYCLE | the teardown (every claim sourced; theorem; RC argument; §A3; threats) + both state machines | every claim cites a results file; LIFECYCLE renders |
| 2.4 | README + DoD | reframe + de-pin; DoD §8 reported item-by-item; self-audit gate | 60-second grasp + headline above the fold; DoD reported |

2.1 is the heavy build; if context grows, split at oracles ↔ enumeration ↔ (A2/A3 + seeded). 2.0 cannot start until the operator has run §3.1.

### Exact prompts (paste one per session; verify + commit before the next)

**Session 2.0**
> Read `CLAUDE.md` and `docs/specs/phase2-spec.md` §3. **Operator has already run §3.1 (sudo).** Execute **Session 2.0 only**: run §3.2 (diesel CLI, `.env`/`.env.example`, `diesel migration run`); build the `harness` substrate — supervisor (spawn/arm via `COINGATE_CHAOS_FIRE`/kill/restart/quiescence, iterating `CrashPointId::ALL` by name from `chaos_hooks`), the truncate/flush fixture (record READ COMMITTED), and the `mock-mpc` server (`/send` counts+dedups, `/lookup` free, `/__counts`). Reuse in-tree deps + `rand`. Prove it by arming `SelfTest` on a throwaway and observing the abort + restart. Build+clippy+test green, commit, STOP.

**Session 2.1**
> Read `CLAUDE.md` and `phase2-spec.md` §4 (+ Amendment §A1/§A2/§A3/§A5). Execute **Session 2.1 only**: implement the five oracles (conservation, at-most-once send via `/__counts`, replay-safety, no-stranded-funds, reconciler-clean); the schedule set (Single/DuplicateStream/ConcurrentConsumers/RestartRedelivery); the exhaustive enumeration (every `CrashPointId` × every schedule) with the dynamic closure/reachability check (every variant must abort under its driving workload); the scripted §A2 (crash-mid-Execute → 409-before-expiry, exactly-once-after-expiry) and §A3 (relay republish absorbed) schedules; and the seeded interleaving sweep (committed seeds). Run all at READ COMMITTED; write `chaos/results/sweep-main.jsonl` + `summary.md`. Build+clippy+test green; commit, STOP.

**Session 2.2**
> Read `CLAUDE.md` and `phase2-spec.md` §5 (+ Amendment §A1/§A4). Execute **Session 2.2 only**: tag the end-of-Phase-0 commit `pre-idempotency`; branch `pre-idempotency-chaos` and apply instrumentation-only (copy `CrashPointId`, place fire-sites at the legacy credit/blind-send/dual-write seams — no logic change); point the black-box harness at both builds and run the identical sweep; capture the legacy double-credit at READ COMMITTED and the mechanism-absence anomalies (replay→2 orders, retry→double-lock, dual-write→stranded). Generate `chaos/results/before-after.md` + commit `sweep-pre-idempotency.jsonl`. Commit, STOP.

**Session 2.3**
> Read `CLAUDE.md` and `phase2-spec.md` §6. Execute **Session 2.3 only**: write `docs/DESIGN.md` (thesis; each failure mode → fix → proof with every claim citing a `chaos/results/` file; the takeover safety theorem verbatim from Amendment §A2; the RC isolation argument + the legacy double-credit counterexample; the §A3 at-least-once demonstration; the headline + before/after table; an honest threats-to-validity section) and `docs/LIFECYCLE.md` (both state machines as Mermaid `stateDiagram-v2` + ASCII fallback, with crash-point placement). No marketing language; build only from committed numbers. Commit, STOP.

**Session 2.4**
> Read `CLAUDE.md` and `phase2-spec.md` §7. Execute **Session 2.4 only**: reframe `README.md` as the exactly-once core (conservation headline + source file above the fold; repo map; microVM bridge; crypto demoted to mocked I/O; de-pinned; link `DESIGN.md`); commit the pending CLAUDE.md Phase-1/2 doc update the agent flagged; then verify the full DoD (§8 here + the amended brief §6) item-by-item against committed evidence and report it, including the self-audit gate. Commit, STOP — Phase 2 and the repo are done.
```
