# DESIGN — the exactly-once payment core, and the proof

This document is built only from committed evidence in `chaos/results/`:

- `chaos/results/summary.md` — the exhaustive crash-point sweep on `main` (the rebuilt code).
- `chaos/results/sweep-main.jsonl` — one machine-readable record per run, each tagged
  `"isolation":"read committed"`.
- `chaos/results/before-after.md` — the legacy `pre-idempotency` vs rebuilt `main` table.
- `chaos/results/sweep-pre-idempotency.jsonl` — the raw legacy violation records.

Every quantitative claim below cites one of these. Nothing here is asserted that the harness did
not measure.

---

## 1. Thesis

Exactly-once is not one mechanism; it is three, composed:

> **at-least-once delivery + idempotent processing + an idempotency key at every external effect
> boundary.**

The transport (Redis Streams, the outbox) is allowed to deliver a message more than once. Each
processing step is written so that a duplicate delivery changes nothing the second time. And the
one place a duplicate would otherwise be observable from outside the system — the MPC `send` — is
guarded by a key (the `withdrawal_id`) so the external world also sees the effect at most once.
The result is a system that is correct under a crash at **every** statement boundary in the
pipeline, under **every** redelivery schedule: `chaos/results/summary.md` records **62/62 runs
passed, 0 conservation violations, 0 re-sends**, all at `READ COMMITTED`.

---

## 2. Each failure mode → fix → proof

The five findings are the original audit (`docs/specs/kickoff-brief.md` §1). For each: the bug, the
fix, and the committed rows that prove it. The legacy column below is the instrumented
`pre-idempotency` build run under the **identical** black-box harness.

### 2.1 Non-atomic credit (double-credit / lost-credit)

**Bug.** The legacy processor wrote *order-paid*, *deposit*, and *balance* as three separate
autocommitted statements, and gated the credit on an unlocked `order.status` read rather than on a
first-time insert. A redelivery that landed after the deposit row but before the credit lost the
credit; two concurrent deliveries both read `pending` and both credited.

**Fix.** The dedup decision and the credit it gates commit in **one** `with_tx` at `READ
COMMITTED`. `store::insert_deposit_on_conflict` does `INSERT … ON CONFLICT (tx_hash) DO NOTHING
RETURNING`; the balance credit (`store::upsert_balance`, an atomic `… DO UPDATE SET balance =
balance + amount`) runs **only when** that insert returned a row (first delivery). A duplicate
returns `None` and credits nothing. (`processor/src/main.rs`, `store/src/models/api.rs`.)

**Proof.** Rebuilt: every `ProcAfter*` crash point is clean across all four schedules
(`chaos/results/summary.md`, the per-crash-point grid). Legacy, same harness
(`chaos/results/before-after.md`, `chaos/results/sweep-pre-idempotency.jsonl`):

- `ProcAfterDepositInsertBeforeCredit` → `conservation: deposits=1000 accounted=0` (lost credit).
- `ConcurrentCredit (§A4)` → `conservation: deposits=1000 accounted=2000` (double credit at RC).

Rebuilt reads clean on both rows.

### 2.2 Blind send (re-send on redelivery)

**Bug.** The legacy worker set `processing` unconditionally and called the signer on the happy
path with no reconciliation. A crash after the send but before finalize led the redelivery to send
again.

**Fix.** The state machine commits `pending → processing` **before** the send, so a redelivery
never arrives in `pending` after a send has been attempted. From `processing` the worker
**reconciles via `signer.lookup(withdrawal_id)`** instead of re-sending: a prior signature
finalizes with no new send; a confirmed not-sent sends exactly once. `finalize_withdrawal_success`
is a guarded `UPDATE … WHERE status='processing'`, so finalize is idempotent. (`worker/src/main.rs`.)

**Proof.** Rebuilt: every `WorkerAfter*` crash point is clean across all schedules
(`chaos/results/summary.md`); the headline is **0 re-sends across 62 runs**
(`chaos/results/sweep-main.jsonl`, Invariant #2 read from mock-mpc `/__counts`). Legacy, same
harness: `WorkerAfterSendBeforeFinalize` → `blind re-send: /__counts for the withdrawal = 2`
(`chaos/results/before-after.md`).

### 2.3 Dual-write (stranded funds)

**Bug.** The legacy `/withdrawals` committed the funds lock in one transaction and then issued a
**separate** Redis `XADD`. A crash in that window left funds locked with no work item — stranded.

**Fix.** A transactional outbox. `store::create_withdrawal_and_lock` **and**
`store::insert_outbox` commit in **one** `with_tx`, so funds are never locked without a durable
publish-intent. The `relay` binary drains the outbox to the stream afterwards. (`api/src/routes/withdraw.rs`,
`relay/src/main.rs`.)

**Proof.** Rebuilt: `WithdrawAfterLockBeforeOutbox` and `WithdrawAfterOutboxBeforeComplete` are
clean across all schedules (`chaos/results/summary.md`). Legacy, same harness:
`WithdrawAfterLockBeforeOutbox` → `stranded: 1 pending withdrawal(s), locked=100, stream empty`
(`chaos/results/before-after.md`).

### 2.4 Dropped / duplicated messages

**Bug.** At-least-once transports redeliver. Without consumer-side dedup, redelivery double-acts;
without redelivery, a crash before `XACK` drops work.

**Fix.** Nothing is `XACK`-ed before its effect is durably committed, so a crash before the ack
simply redelivers — and the redelivery is absorbed: deposits by the `UNIQUE(tx_hash)` insert,
withdrawals by the `processing`/`completed` status guards plus `lookup`. The `reconciler` recomputes
the conservation identity independently (Invariant #5) after every schedule.

**Proof.** The `DuplicateStream` and `RestartRedelivery` columns are clean for **every** crash
point (`chaos/results/summary.md`), and the dynamic registry-closure check confirms **14/14**
variants actually aborted under their driving workload (same file) — the dedup is exercised, not
assumed.

### 2.5 The in-progress idempotency-key hole

**Bug.** The hardest inbound case: a first attempt that crashed *after* acquiring the key but
*before* persisting its response. Naively, a replay either blocks forever or re-executes.

**Fix.** The `in_progress → completed` lifecycle with a lease, and an Execute transaction whose
guarded effect and conditional completion commit together (`api/src/idem.rs`). A replay before the
lease expires gets `409 + Retry-After`; after expiry, a takeover re-executes exactly once. See the
theorem in §3.

**Proof.** Rebuilt: every `IdemAfter*` crash point is clean across all schedules, and the scripted
§A2 schedules pass: `A2-takeover` on both `IdemAfterEffectBeforeComplete` and
`IdemAfterAcquireBeforeExecute` record `409 before expiry; exactly-once after`
(`chaos/results/summary.md`).

---

## 3. The takeover safety theorem (Amendment §A2, verbatim)

> The guarded effect and the transition to `completed` commit in a single transaction; therefore
> `status = 'completed'` **if and only if** the effect has been durably applied. Consider an
> observed key that is `in_progress` with an expired lease. Its Execute transaction has not
> committed — had it committed, the status would be `completed`, contradicting the observation. By
> atomicity, the effect has **not** been applied. A takeover that re-runs Execute therefore applies
> the effect for the first and only time. *Safety comes from atomicity, not from the lease.* The
> lease only bounds how long a replay waits before a takeover is permitted, so two executors do not
> both attempt the effect while the first might still be live. If two executors do race (lease
> expired but original still running), the conditional completion `UPDATE ... WHERE
> status='in_progress' AND lease_owner=self` admits exactly one winner; the loser's `UPDATE` matches
> 0 rows and its effect rolls back with the aborted transaction.

This is exercised end-to-end by the §A2 scripted schedules (`chaos/results/summary.md`, the two
`A2-takeover` rows): the api aborts mid-Execute leaving the key `in_progress`; a replay before lease
expiry returns `409`; after expiry the takeover re-executes, leaving **exactly one** withdrawal and
one lock, conservation intact.

---

## 4. The isolation argument (Amendment §A4) and its counterexample

The whole proof runs at `READ COMMITTED` — recorded in every record of
`chaos/results/sweep-main.jsonl` (`"isolation":"read committed"`). There is no `SERIALIZABLE`
dependency anywhere, and that is by construction:

- **Deposit dedup** rests on the `UNIQUE(tx_hash)` index via `INSERT … ON CONFLICT (tx_hash) DO
  NOTHING RETURNING`. A unique constraint cannot be bypassed by a phantom at any isolation level, so
  no snapshot isolation is needed there.
- **The balance read-modify-write** is not a read-modify-write: `upsert_balance` is an atomic
  `… DO UPDATE SET balance = balance + amount`, resolved by the database, never a value read into
  the process and written back.
- **The withdrawal lock** takes a `SELECT … FOR UPDATE` pessimistic row lock
  (`create_withdrawal_and_lock`), which serializes concurrent writers on that balance row at RC.
- There is no read-only predicate whose stability we rely on, so write-skew is out of scope.

**The counterexample makes the bar real.** The legacy credit path reads `order.status` without a
lock and then credits — a textbook RC lost-update. Under two concurrent consumers at the same
`READ COMMITTED`, `chaos/results/before-after.md` records the `ConcurrentCredit (§A4)` row as
`conservation: deposits=1000 accounted=2000`: a single 1000 deposit credited twice. The rebuilt
path, run under the identical schedule at the identical isolation level, is clean
(`chaos/results/summary.md`). The anomaly is removed at the default isolation level — shown, not
claimed.

---

## 5. At-least-once is a feature (Amendment §A3)

The outbox relay crashing between its `XADD` and `mark-sent` republishes the same row on restart —
**by design**; the outbox is at-least-once. That is safe because the consumer absorbs the
duplicate. `chaos/results/summary.md` records the `A3-relay-republish` schedule on
`RelayAfterXaddBeforeMarkSent` as passing: `relay republishes; worker absorbs duplicate (one
send)`. The same property holds across the generic grid — every `RelayAfter*` crash point is clean
under all four schedules. At-least-once delivery is thus a committed, reproducible demonstration,
not a README axiom.

---

## 6. The headline and the before/after table

**Headline** (`chaos/results/summary.md`):

> A process crash at every statement boundary in the pipeline, under every redelivery schedule —
> **0 conservation violations, 1 send per withdrawal**, across 62 runs, at `READ COMMITTED`.

**Before / after** (`chaos/results/before-after.md`), legacy `pre-idempotency` vs rebuilt `main`:

| Crash point / anomaly | Legacy (`pre-idempotency`) | Rebuilt (`main`) |
|---|---|---|
| `ProcAfterDepositInsertBeforeCredit` | lost credit (`deposits=1000 accounted=0`) | clean |
| `ConcurrentCredit (§A4)` | double-credit at RC (`deposits=1000 accounted=2000`) | clean |
| `WorkerAfterSendBeforeFinalize` | blind re-send (`/__counts = 2`) | clean |
| `WithdrawAfterLockBeforeOutbox` | stranded (`pending, locked=100, stream empty`) | clean |
| `InboundKey replay` | double-lock (`2 withdrawals, locked=200`) | clean |
| `Idem*`, `WithdrawAfterOutbox*`, `Relay*` | mechanism absent in legacy | clean |

The full per-crash-point grid and the raw records are in `chaos/results/`.

---

## 7. Threats to validity / residual risk

Stated precisely so the categorical claim is not overstated:

- **The signer's idempotency is a contract assumed of the signer**, modeled here by `mock-mpc`
  (`/send` dedups its signature, `/lookup` returns the prior result). The system is exactly-once
  **given** an idempotent signer plus the `lookup` reconciliation path. A signer that is itself
  non-idempotent *and* loses its ack is a fundamental limit no client-side protocol can close; it is
  out of scope, not solved.
- **The `SolanaChain` paginated backfill is RPC-bound** and is gap-free *by construction* (the
  poller's cursor), not crash-swept by this harness. It is not part of the 62-run claim.
- **Only the crash-point axis is exhaustive.** The harness arms every `CrashPointId` × every
  redelivery schedule (`chaos/results/summary.md`, 14/14 closure). The thread-interleaving axis is a
  **seeded** sweep (committed seeds `[1,2,3]`, the `seed-*` rows), not exhaustive — it catches races
  *between* the enumerated crash points, but the categorical word "every" applies to the crash-point
  axis alone.
- **The redelivery of a crashed stream entry is modeled by re-enqueue + PEL reclaim**, because the
  services' `XAUTOCLAIM` min-idle is 60s; this is a faithful at-least-once duplicate, and the
  dedup/absorb property under test is independent of which mechanism redelivers.
- **The §A4 double-credit is a genuine race** and the before-run retries until it is observed; it is
  real and reproducible, but timing-dependent rather than deterministic.
