# Coingate — Per-Project Kickoff Brief & Execution Spec

**Repo:** https://github.com/umangPokhriyall/Coingate
**Owner:** internet-native systems engineer, no formal industry experience, building falsifiable proof-of-work.
**This document is the complete spec.** The executing chat has no other context. Read it fully before writing code.
**Companion specs:** `docs/specs/phase0-spec.md`, `phase1-spec.md`, `phase2-spec.md` (drafted per phase from this brief).

---

## 0. Why this repo exists (the strategic frame)

This is **Repo 5** of the portfolio. Its job is to own one transferable distributed-systems primitive end-to-end: **exactly-once processing of an at-least-once event stream that drives non-idempotent, money-moving side effects.** Every other repo demonstrates CPU- or kernel-level depth. This one demonstrates *correctness under partial failure* — the discipline a Principal Engineer probes when the question is "what happens when this crashes between line 3 and line 4?"

It is **not** a crypto product. The prior framing — "a Solana payment gateway" — is a liability: it buries the one piece of senior signal under Jupiter quote-fetching and SPL token plumbing that no systems reviewer cares about. The hard reframe this brief mandates:

> **Coingate is an exactly-once payment-processing core. The chain and the signer are mocked behind traits. The product is the idempotency layer and the proof that it holds under fault injection.**

**Why this specific artifact neutralizes the lack of pedigree:**
1. **Exactly-once is the canonical distributed-systems trap**, and almost everyone gets it wrong in the same way (this repo currently does). A working, *proven* implementation is unfakeable seniority.
2. **The proof is falsifiable.** A chaos harness that injects crashes at every dangerous seam and asserts a conservation invariant produces a committed number — "N fault schedules, 0 conservation violations" — that no résumé can fake and no reviewer can wave away.
3. **The before/after is the killer move.** The same harness run against the *current* code produces violations; against the rebuilt core, zero. The diff in violation count *is* the artifact.
4. **It is the literal substrate of the flagship.** A microVM sandbox's job-submission API is structurally identical to a payment intake: a client retries on timeout (needs inbound idempotency keys so a job isn't run twice), the control plane dispatches over an at-least-once queue (needs consumer-side atomic dedup so a redelivered "start" doesn't boot two VMs / double-bill), and the worker performs a non-idempotent external effect — booting a VM — that needs an idempotency key at the effect boundary. Build it here on money; lift it there for jobs.

**Direct microVM mapping (state this in the README):** the inbound `Idempotency-Key` layer = the sandbox job-submission dedup; the consumer-side atomic dedup+effect = the dispatcher that guarantees one microVM per job under redelivery; the effect-boundary key on the signer = the "boot exactly once" guarantee against the hypervisor; the transactional outbox = how the control plane enqueues work without the dual-write race; the reconciliation job = the orchestrator sweep that detects and repairs a guest whose state drifted from the ledger.

---

## 1. Current-state audit (what is wrong, precisely)

The workspace has five crates: `api` (actix-web HTTP), `poller` (Solana chain watcher → Redis stream), `processor` (stream consumer → DB credit), `worker` (withdrawal stream consumer → MPC signer), `store` (diesel/Postgres). The event topology is sound and mirrors the flagship's shape. **The delivery substrate (Redis Streams consumer groups, `XREADGROUP`/`XACK`/`XAUTOCLAIM`) is the correct choice — at-least-once is exactly what you want.** The bug is never in delivery. It is in *processing*, every single time, and always the same root cause:

> **The dedup decision and the side effect it guards are never in the same atomic commit, and the one place a client hands the system a natural idempotency key, it is thrown away.**

### 1.1 The credit path is not exactly-once (highest priority) — `processor/src/main.rs`, `store/src/models/api.rs`

`process_transaction` performs three writes as three separate auto-commit statements with **no enclosing transaction**: `update_order(status="paid")` → `insert_deposit` → `upsert_balance`. The only dedup guard is a non-atomic read of `order.status == "paid"` at the top. Failure modes:

- **Crash between `update_order` and `upsert_balance`:** the message is never `XACK`ed, so it redelivers; on redelivery `order.status == "paid"` short-circuits to `Ok(true)` → `XACK` → **the balance is never credited. Money silently lost.**
- **Concurrent delivery of a duplicate** (the poller can emit the same signature twice — see §1.4): two processor consumers both read `status="pending"`, both proceed, both call `upsert_balance` → **double credit.** The `deposits.tx_hash` unique index *does* fire inside `insert_deposit`, but the code swallows the `UniqueViolation` and **still proceeds to credit the balance** (`insert_deposit` returns `Ok` either way; the credit is not gated on a *new* insert). The one correct guard the schema offers is bypassed by the control flow.
- **Verification failure** (`Ok(false)`, e.g. underpayment) is `XACK`ed and discarded. **No dead-letter, no record.** A payment that doesn't match expected amount vanishes.

The natural idempotency key (the on-chain signature) exists and is even enforced unique at the DB — but the money-moving effect is decoupled from it.

### 1.2 The withdrawal path double-spends real money (severity: critical) — `worker/src/main.rs`

The worker calls an external signer (`POST http://127.0.0.1:3000/wallets/{id}/send`) which broadcasts a real transfer. This is a **non-idempotent external effect with no idempotency key at the boundary.** The request body is `{to, amount, mint}` — nothing identifying the withdrawal.

- **Crash (or dropped response) after the signer broadcasts but before `XACK`:** redelivery re-calls `/send` → **the transfer goes out twice.** `process_withdrawal` has no status guard; it unconditionally sets `processing` and re-sends regardless of current state.
- **`XAUTOCLAIM` with a 60s idle timeout** will steal an in-flight message from a *slow* (not dead) worker mid-`/send` → two workers send the same withdrawal. The idle timeout is a liveness heuristic being relied on for correctness.
- **`finalize_withdrawal_success` / `finalize_withdrawal_failed` are not idempotent:** each unconditionally adjusts `locked_balance` (and `balance` on failure). Finalize the same withdrawal twice → `locked_balance` goes negative / balance inflates. No "only transition from `processing`" guard.

### 1.3 The API→Redis dual-write loses or strands money — `api/src/routes/withdraw.rs`

`create_withdrawal_and_lock` (DB transaction, correct) then `XADD` to Redis (separate call). Crash between commit and `XADD` → **funds locked in the DB forever with no stream entry; no worker will ever process it.** The handler's best-effort `revert_withdrawal_lock` on `XADD` *error* does not cover a *crash*. This is the textbook dual-write problem; it needs a transactional outbox.

### 1.4 The poller silently drops payments under burst — `poller/src/main.rs`

`get_signatures_for_address_with_config { before: None, until: checkpoint, limit: Some(10) }`: if more than 10 transactions arrive between 3-second polls, only the newest 10 are fetched and the checkpoint advances to the newest — **the older unprocessed transactions are skipped permanently.** Independent of idempotency: this is data loss. Separately, the checkpoint `SET` is not atomic with the `XADD`, producing duplicate stream entries on crash (harmless *once the consumer is fixed*, fatal while it isn't).

### 1.5 The substrate makes every concurrency claim a lie — `api/src/main.rs`, `store/src/store.rs`

`Arc<Mutex<Store>>` over a single `PgConnection`. **Every HTTP request serializes through one mutex around one connection.** actix-web's worker pool is decorative; throughput ceiling = one connection's round-trip latency. This must be a pool. (`PgConnection: !Sync` is *why* the mutex exists — the fix is a pool, e.g. `r2d2`/`deadpool`, not a bigger lock.)

### 1.6 Defects that disqualify on sight (fix, don't defend)

- **Money as `f64`.** `CreateOrderRequest.price_amount: f64` (`merchant.rs`). Money in floating point is an automatic fail. Integer base units / `BigDecimal` end to end.
- **Request-triggerable panics everywhere.** `Uuid::parse_str(...).unwrap()`, `.expect("invalid sender pubkey")`, `resp["outAmount"].as_str().unwrap()` in handlers. Each is a remote DoS — one crafted request panics the worker thread.
- **`find_app_by_token` loads the entire `apps` table and bcrypt-verifies in a loop** (`api.rs`) — O(n) bcrypt per authenticated request. (Auth is not the thesis; at minimum make it an indexed lookup.)
- **`println!`/`eprintln!`/emoji on every hot path.** Global stdout lock + syscall per request/event. Route through `tracing`, gated, structured.
- **Hardcoded `JWT_SECRET = b"super-secret-key"`, bcrypt cost 4, devnet RPC URL, Redis URL.** Config from env.
- **Two divergent `deposits` migrations** (`...171556` creates it `tx_hash ... UNIQUE`; `...132859` re-`CREATE TABLE IF NOT EXISTS` with different defaults — a silent no-op masking schema drift). Collapse to one clean, reviewable migration set.

### 1.7 What is already right (preserve and credit honestly)

- The **store layer's transactional balance discipline**: `create_withdrawal_and_lock`, `revert_withdrawal_lock`, `finalize_*` all use `conn.transaction` + `SELECT ... FOR UPDATE` row locks. This is correct, non-trivial, and the right instinct — it just isn't applied to the credit path and isn't idempotent on finalize.
- The **`deposits.tx_hash` unique index** and the **`balances (merchant_id, token_mint)` unique index** — the right keys exist; the code just doesn't use them as the dedup gate.
- The **Redis Streams consumer-group backbone** — keep it. At-least-once is the correct delivery contract.
- The **service decomposition** (intake / watch / process / settle) — keep it; it is the flagship's shape.

---

## 2. Target architecture

The governing principle is the one from the TCP server: **separate the *what* from the *how*.** There, sans-IO split protocol from I/O. Here, **the idempotency decision logic is separated from storage and from the outside world.** The idempotency core is pure, testable, and reused identically by the inbound API path and the consumer path. The chain and the signer become traits with a real impl and a mock impl, exactly as the `Server` trait had eleven impls.

```
coingate/
  Cargo.toml                    # workspace
  idempotency/                  # NEW — the product. sans-IO decision logic.
    src/
      key.rs                    # Idempotency key type, request fingerprint (hash of canonical body)
      lifecycle.rs              # the explicit state machines (Order, Withdrawal) + legal transitions + guards
      decision.rs              # pure fns: "given this key + stored record, what do we do?" (Execute | Replay | Conflict)
      store.rs                  # the IdempotencyStore trait (the I/O boundary — no Postgres here)
  external/                     # NEW — the mocked outside world, behind traits
    src/
      chain.rs                  # trait Chain { fn poll_deposits(...) }  + SolanaChain + MockChain
      signer.rs                 # trait Signer { fn send(key, to, amount) -> Sig } + MpcSigner + CountingMockSigner
  store/                        # diesel/Postgres — connection POOL, no global mutex
    src/...                     # IdempotencyStore impl; atomic dedup+effect fns live here, in ONE transaction each
    migrations/                 # ONE clean, collapsed migration set
  api/                          # actix-web; Idempotency-Key extractor; pooled store; panic-free handlers
  poller/                       # gap-free backfill; emits via Chain trait
  processor/                    # consumer-side atomic dedup+effect (deposit→credit)
  worker/                       # effect-boundary idempotency + reconciliation; status-guarded finalize
  relay/                        # NEW — transactional-outbox relay (DB outbox -> Redis)
  chaos/                        # NEW — the proof. fault-injection harness + invariant assertions.
    src/...                     # fault schedules, counting signer, conservation checker
    results/                    # COMMITTED: fault-schedule sweep output, before/after violation counts
  docs/
    DESIGN.md                   # the teardown — the actual artifact
    LIFECYCLE.md                # the two state machines, drawn
    specs/                      # this brief + phase specs
  README.md                     # reframed: exactly-once core. pinned-ready.
```

**The `IdempotencyStore` trait and the `decision` module are the product. The DB, Redis, chain, and signer are instances of I/O behind it.** The dedup logic lives **once** and is exercised by both the inbound and consumer paths. No copy-pasted dedup.

---

## 3. The high-signal primitives — what a Principal Security/Systems Engineer evaluates

These are the things a senior reviewer looks for *specifically* in a payments/exactly-once system. Each must be present, named, and demonstrated.

1. **Inbound idempotency keys (Stripe-style).** An `Idempotency-Key` header required on every unsafe POST (`/orders`, `/withdrawals`). An `idempotency_keys` table: `(key PK, request_fingerprint, response_snapshot JSONB, status, created_at, locked_at)`. First request inserts the key and executes the operation **in the same transaction that persists the response snapshot**; a replay returns the stored snapshot byte-for-byte; the same key with a *different* request fingerprint returns **409 Conflict** (key reuse with a different payload — the security-relevant case). The merchant-supplied `order_id`, currently discarded, becomes the natural key for `/orders` scoped to `app_id`, backed by a `unique (app_id, order_id)` constraint.

2. **Exactly-once = at-least-once delivery + idempotent processing.** State this as the design axiom. The consumer never tries to make delivery exactly-once (impossible); it makes *processing* idempotent. The deposit→credit effect becomes: `INSERT INTO deposits ... ON CONFLICT (tx_hash) DO NOTHING RETURNING id` — **credit the balance and mark the order paid only if a row was actually inserted**, all in one transaction. A redelivery inserts nothing, returns nothing, credits nothing.

3. **Idempotency at the external-effect boundary.** The signer is non-idempotent and external; the only correct contract is that the worker passes a deterministic key (the `withdrawal_id`) to `Signer::send`, and the signer dedups on it and returns the *same* signature on replay. Model this explicitly: the `MockSigner` enforces and counts it; the real `MpcSigner` documents the contract it requires. The worker never blind-resends — it sends only from `pending`, and from the ambiguous `processing` state it **reconciles** (queries the signer/chain by key) rather than re-broadcasting.

4. **Transactional outbox.** Kill the API→Redis dual-write: write the domain row **and** an `outbox` row in one DB transaction; a `relay` loop reads unsent outbox rows and publishes to Redis, marking them sent. Crash-safe by construction: the outbox row is the durable intent.

5. **Atomicity & isolation, made explicit.** Every dedup-and-effect is one transaction. `SELECT ... FOR UPDATE` where a read-modify-write on a balance occurs. Document the isolation level relied upon and why `ON CONFLICT DO NOTHING RETURNING` is the linchpin.

6. **Explicit lifecycle state machines.** `Order: pending → paid` (terminal) and `Withdrawal: pending → processing → completed | failed`, with **legal-transition guards** enforced in code (`finalize_*` transition only from `processing`). Drawn in `docs/LIFECYCLE.md`. A reviewer reads the state machine first.

7. **Poison-message / backpressure handling.** Unverifiable deposits (amount mismatch, no matching order) go to a **dead-letter stream**, not a silent `XACK`-drop. The accept/queue behavior under overload is decided and documented.

8. **Reconciliation.** A periodic sweep that asserts the conservation invariant against live data (sum of credits == sum of distinct confirmed deposits; no withdrawal stuck in `processing` past a deadline) and flags/repairs drift. This is the belt-and-suspenders a payments engineer expects to see, separate from the per-message guards.

9. **The falsifiable proof.** Everything above is asserted, not claimed — see §5.

---

## 4. The common bar — every changed path must pass this

A path is **not done** until all hold:

- **Idempotent under redelivery.** Processing the same event/request twice produces the same end state and exactly one effect. Demonstrated, not asserted.
- **Atomic dedup+effect.** The dedup decision and its guarded side effect commit in one transaction, or the design explains why that is impossible and what compensates.
- **Crash-safe.** No code path leaves money lost, double-credited, double-sent, or stranded (locked with no work item) across a crash at any statement boundary.
- **Panic-free on hostile input.** No `.unwrap()`/`.expect()` on anything derived from a request, a stream message, or an RPC response. One bad input never kills a worker.
- **Pooled, not globally locked.** DB access is via a connection pool. No `Arc<Mutex<Store>>`.
- **Money is exact.** Integer base units / `BigDecimal` end to end. No `f64` touches an amount.
- **Off-hot-path, structured logging.** `tracing`, gated. No `println!` per request/event.
- **Measured the moment it exists.** Any new guard goes through the chaos harness immediately. No guard is "done" without a fault schedule that exercises it.

---

## 5. The proof — the falsifiable artifact (`chaos/`)

This is the benchmark analogue for a correctness primitive, and it is what elevates this from a freeze to an elite artifact. It is **non-optional and is half the value.**

**Mechanism.** A deterministic driver pushes N logical payments and M withdrawals through the real pipeline (real Postgres, real Redis, `MockChain`, `CountingMockSigner`) while injecting faults at every dangerous seam, under a seeded sweep of fault schedules:
- kill the processor mid-transaction (between each statement boundary);
- emit duplicate stream entries (simulate the poller double-`XADD`);
- deliver the same message to two concurrent consumers (`XAUTOCLAIM` steal during a slow effect);
- crash the worker after `Signer::send` returns but before `XACK`;
- fire M concurrent identical API requests with the same `Idempotency-Key`.

**Invariants asserted (the committed, falsifiable numbers):**
1. **Conservation:** Σ(merchant balance credits) == Σ(distinct confirmed deposits). No double-credit, no lost credit — under every fault schedule in the sweep.
2. **At-most-once effect:** for each `withdrawal_id`, the `CountingMockSigner` observes **exactly one** send across all redelivery and concurrent-consumer faults.
3. **API replay safety:** M concurrent identical requests with one key ⇒ exactly one order / one withdrawal / one locked amount.
4. **No stranded funds:** after the relay drains the outbox, every locked withdrawal has a corresponding work item; no orphaned locks.

**The before/after (the headline).** Run the identical harness against the *current* `main` (tagged `pre-idempotency`) and against the rebuilt core. Commit both result sets. The current code produces a nonzero conservation-violation count and a nonzero double-send count; the rebuilt core produces zero, under the same seed sweep. **That delta is the proof-of-work.** Report it honestly, including any residual-risk case the harness cannot cover (e.g. the real signer's idempotency is a *contract you assume*, modeled by the mock — say so).

---

## 6. Hard Definition of Done

The repo is world-class-artifact-grade only when **all** are true:

1. **The idempotency core is isolated behind a trait/crate**, with the decision logic (`decision.rs`, `lifecycle.rs`) sans-IO — no Postgres, no Redis, no chain types in it. Unit-tested in isolation.
2. **Both patterns implemented:** inbound `Idempotency-Key` (with fingerprint-conflict 409) on `/orders` and `/withdrawals`; consumer-side atomic dedup+effect (`ON CONFLICT DO NOTHING RETURNING`, credit only on insert) on the deposit path.
3. **Effect-boundary idempotency** on the signer (deterministic key + dedup contract), and **status-guarded, idempotent** `finalize_*`. Worker reconciles from `processing` instead of re-sending.
4. **Transactional outbox + relay** replaces the API→Redis dual-write. Poller backfill is gap-free.
5. **The chaos harness is committed**, with `chaos/results/` populated: the fault-schedule sweep, the four invariants, and the **before/after violation counts** against the `pre-idempotency` tag. Conservation violations on the rebuilt core: **0**.
6. **The substrate is fixed:** connection pool (no global mutex); zero `f64` money; zero request-triggerable panics; `tracing` not `println!`; one collapsed migration set; config from env.
7. **`docs/DESIGN.md`** is the teardown: each failure mode from §1, the fix, the atomicity argument, the proof — every claim sourced to a committed file under `chaos/results/`. Includes an honest **threats-to-validity / residual-risk** section. **`docs/LIFECYCLE.md`** draws both state machines.
8. **`README.md`** reframes the repo as an exactly-once core (60-second grasp: what it is, the conservation result with its source file, the repository map, the microVM bridge). The crypto surface is explicitly demoted to "mocked I/O." **De-pinned from any payment-product framing.**
9. **Self-audit gate (the governing law):** the owner can re-derive, from memory, (a) why `ON CONFLICT DO NOTHING RETURNING` makes the credit idempotent and where the transaction boundary must sit, and (b) why exactly-once for the signer is impossible without a key at the effect boundary. If it can't be re-derived, it isn't owned, and the repo isn't done.
10. `cargo build` / `clippy` / `test` clean across the workspace; dependency additions limited to the allowlist per phase spec.

---

## 7. Non-negotiable engineering rules

1. **The dedup decision and the effect it guards commit atomically, or not at all.** This is the entire thesis. If you cannot make them atomic (external effects), you put an idempotency key at the boundary and reconcile. There is no third option.
2. **At-least-once delivery is a feature, not a bug.** Never try to make Redis delivery exactly-once. Make processing idempotent.
3. **The natural key beats a generated one.** Use the on-chain signature, the `(app_id, order_id)`, the `withdrawal_id`. Generate a key only when nature hands you none.
4. **Money is exact and never floating point.** Integer base units / `BigDecimal`, end to end.
5. **No hostile input may panic a worker.** Typed errors at every boundary that touches a request, a message, or an RPC.
6. **One abstraction, many implementations.** `Chain`, `Signer`, `IdempotencyStore` are the products; Solana/MPC/Postgres and their mocks are instances. The mocks are first-class — they drive the proof.
7. **Prove it, don't claim it.** Every correctness claim in `DESIGN.md` cites a committed `chaos/results/` file. An honest residual-risk admission is elite signal; a hand-wave is disqualifying.
8. **Simple beats clever.** The fix is mostly *moving the transaction boundary* and *gating the effect on the dedup result* — not new infrastructure. Resist adding a saga framework, a distributed lock service, or Kafka. Postgres + Redis + correct boundaries is the whole game.
9. **Honesty is the signal.** Keep the `pre-idempotency` tag runnable. Show the bug. Then show it gone. No marketing language, ever.

Use the vocabulary — *at-least-once, idempotent consumer, exactly-once effect, transactional outbox, dual-write, dead-letter, reconciliation, conservation invariant, compensating action* — **only after the technique is actually applied.** Earn the term, then use it.

---

## 8. Phase breakdown (sized for autonomous Claude Code sessions)

Three phases. Each session = one deliverable, ending with `cargo build`+`clippy`+`test` green, a commit, and STOP. Future phases are off-limits until reached.

### Phase 0 — Substrate & the trait boundary (de-risk before building)
Make the existing behavior correct-enough and *testable* before touching idempotency. Until the chain and signer are mocked, nothing is reproducible.

| # | Session | Deliverable | Done when |
|---|---|---|---|
| 0.1 | Pool + config | Replace `Arc<Mutex<Store>>` with a connection pool; config (DB/Redis/JWT/RPC) from env; `tracing` replaces all `println!`/`eprintln!` | API serves concurrently; no global mutex; build green |
| 0.2 | De-panic + exact money | Remove every request/message/RPC `.unwrap()`/`.expect()`; `price_amount` and all amounts to integer base units / `BigDecimal` (no `f64`) | A fuzzed bad request returns 4xx, never panics |
| 0.3 | Trait-ify the outside world | `external` crate: `Chain` (+ `SolanaChain`, `MockChain`), `Signer` (+ `MpcSigner`, `CountingMockSigner`); poller/worker depend on the traits | poller/worker compile against mocks; one happy-path e2e green on mocks |
| 0.4 | Migration hygiene | Collapse the two `deposits` migrations into one clean set; verify `unique(deposits.tx_hash)`, `unique(balances.merchant_id, token_mint)`; add `unique(orders.app_id, order_id)` | `diesel migration run` clean from empty DB; constraints present |

### Phase 1 — The idempotency core (the product)
| # | Session | Deliverable | Done when |
|---|---|---|---|
| 1.1 | `idempotency` crate | `key.rs` (key + request fingerprint), `lifecycle.rs` (both state machines + guards), `decision.rs` (pure Execute/Replay/Conflict), `IdempotencyStore` trait — all sans-IO | unit tests cover replay, conflict, illegal transitions; zero I/O deps in the crate |
| 1.2 | Inbound keys | actix `Idempotency-Key` extractor; `idempotency_keys` table + `IdempotencyStore` Postgres impl; `/orders` and `/withdrawals` execute-and-snapshot in one tx; 409 on fingerprint mismatch; `(app_id, order_id)` natural key wired | replaying a POST returns the stored response; mismatched payload → 409 |
| 1.3 | Atomic credit | Rewrite `process_transaction`: one transaction; `INSERT deposits ON CONFLICT (tx_hash) DO NOTHING RETURNING`; credit + mark-paid **only if inserted**; dead-letter for unverifiable | redelivery credits nothing; underpayment lands in dead-letter, not dropped |
| 1.4 | Effect-boundary + finalize guards | Worker passes `withdrawal_id` as the signer key; sends only from `pending`; reconciles from `processing`; `finalize_*` transition only from `processing` (idempotent) | double finalize is a no-op; double delivery → one send |
| 1.5 | Outbox + relay | `outbox` table; `/withdrawals` writes domain row + outbox row in one tx; `relay` loop drains to Redis and marks sent; poller backfill paginated (gap-free) | crash between commit and publish strands nothing; relay republishes on restart |

### Phase 2 — The proof & the artifact
| # | Session | Deliverable | Done when |
|---|---|---|---|
| 2.1 | Chaos harness | `chaos/` driver, seeded fault schedules at every seam, `CountingMockSigner`, the four invariant checkers (§5) | harness runs a sweep; emits machine-readable results |
| 2.2 | Before/after sweep | Tag current `main` as `pre-idempotency`; run identical harness on both; commit `chaos/results/` (violation counts, sweep size) | rebuilt core: 0 conservation violations, 1 send per withdrawal; old: nonzero, recorded |
| 2.3 | `DESIGN.md` + `LIFECYCLE.md` | The teardown (each failure mode → fix → proof, every number sourced; threats-to-validity); both state machines drawn | every claim cites a `chaos/results/` file; honest residual-risk section present |
| 2.4 | README + DoD | Reframe README (exactly-once core, conservation headline + source, repo map, microVM bridge); de-pin; verify DoD §6 item by item | 60-second grasp above the fold; DoD each item reported |

Session 1.2 and 1.3 are the heavy builds (transaction boundaries are subtle) — if context grows, split 1.3 at the insert/credit boundary. Session 2.1 carries the harness; keep 2.3–2.4 in clean windows (they read all of `chaos/results/`).

---

## 9. Out of scope — do NOT do these

- **No new crypto features.** No Jupiter routing improvements, no new chains, no SPL edge cases. The chain is `MockChain` for the proof; `SolanaChain` stays as the documented real impl, untouched beyond the trait fit.
- **No distributed-lock service, no saga framework, no Kafka, no Temporal.** Postgres transactions + Redis Streams + correct boundaries is the entire mechanism. Adding infrastructure to "solve" exactly-once is the anti-signal.
- **No auth overhaul.** Fix the `find_app_by_token` O(n) scan to an indexed lookup and move the JWT secret to config; stop there. Auth is not the thesis.
- **No frontend, no payment page, no styling.**
- **Do not chase a "real on-chain exactly-once" claim.** Be precise that the signer's idempotency is a *contract assumed of the signer*, modeled by the mock. State the boundary of the guarantee honestly.
- **Do not exceed the phase order.** No reconciliation job until the per-message guards exist; no harness until the traits and mocks exist.

---

## 10. First message for the executing chat

Paste this brief, then start with:

> "Execute Phase 0, Session 0.1 only. Replace `Arc<Mutex<Store>>` with a connection pool, move DB/Redis/JWT/RPC config to env, and route all logging through `tracing` (remove every `println!`/`eprintln!`). Do not touch idempotency logic, the credit path, or the worker yet. Show me the new `store` connection-acquisition API and the `api/src/main.rs` wiring before changing any route handler. End with build+clippy+test green, commit, and STOP."
