# Coingate — Kickoff Amendment 1 (Chaos Harness & Idempotency Hardening)

**Amends:** `docs/specs/kickoff-brief.md`. Does not supersede it. Each amendment below names the brief section it modifies and the phase that implements it.
**Source:** Chief Architect Directive §3 (Repo 5 Upgrade).
**Net effect:** the harness's central claim upgrades from a *statistical* one ("0 violations across N seeded schedules") to a *categorical* one ("a crash at every statement boundary, under every redelivery schedule, is clean"); the inbound-key protocol gains its missing crash-recovery semantics; the isolation-level guarantee becomes demonstrated rather than asserted.

Read this immediately after the brief and before any phase spec. The phase specs are drafted to satisfy both documents.

---

## A1 — Exhaustive crash-point enumeration, not sampled fault schedules
**Amends Brief §5 (the proof). Implemented in Phase 2; scaffolded in Phase 0.**

"N random fault schedules, 0 violations" is a coverage argument that depends on the sampler. We make it an enumeration argument that does not.

**Mechanism — compile-time fail points.** Instrument the pipeline with a closed, enumerable set of crash points behind a `chaos` cargo feature. In a normal build they compile to nothing — zero instructions, zero risk in production. A crash point models a real process death (`std::process::abort()`), not a recoverable panic, because the property under test is crash *recovery*.

- A `crash_point!(CrashPointId::X)` hook sits **between every statement inside each dedup+effect transaction** (so a crash can land in every intermediate state), and **at every `XADD`/`XACK`/outbox-mark boundary** in `processor`, `worker`, and `relay`.
- `CrashPointId` is a single workspace-wide enum with an `ALL` iterator. The set is expected to be K ≈ 20–30 seams. The scaffolding (macro, registry, enum, the abort mechanism, a self-test) is built in Phase 0; the fire-sites are placed as each transaction is written in Phase 1; Phase 2 asserts **every** `CrashPointId` has exactly one fire-site (registry closure check — an un-sited id is a hole in the proof).

**The harness (Phase 2) is a supervisor.** It runs `processor`/`worker`/`relay` as subprocesses. For each run it arms exactly one crash point (via env), drives a deterministic workload, lets the armed process abort, restarts it, and asserts the invariants (Brief §5 + A5 below). It enumerates **every crash point × every duplicate/concurrent-delivery schedule** deterministically. The seeded random sweep is *retained on top*, but now only for thread interleavings — the one part of the state space that cannot be enumerated.

**Headline claim (the deliverable wording).** Not "0 violations across N seeded schedules" but: **"A process crash at every statement boundary in the pipeline, under every redelivery schedule: 0 conservation violations, 1 send per withdrawal."** The before-run against the `pre-idempotency` tag produces a **crash-point → legacy violation** table (which seam, what anomaly), against which the rebuilt run reads "clean" row by row. That table is self-explanatory and is the core of `DESIGN.md`.

---

## A2 — The in-progress idempotency-key recovery protocol
**Amends Brief §3.1 (inbound idempotency keys). Implemented in Phase 1; schema landed in Phase 0.**

The brief specified the `idempotency_keys` table but was silent on the hardest case: the first attempt crashed *after* acquiring the key but *before* persisting the response. This is the part of Stripe-style keys everyone gets wrong. Specified now as a state machine.

**Key lifecycle.** `in_progress(lease_deadline, lease_owner) → completed(response_snapshot)`. There is no `failed` terminal state — a failed attempt leaves no committed effect (see the theorem), so it is indistinguishable from "never attempted" and is recovered by takeover.

**Columns (landed in Phase 0).** `key TEXT PRIMARY KEY, request_fingerprint TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('in_progress','completed')), lease_deadline TIMESTAMPTZ, lease_owner UUID, response_snapshot JSONB, response_status SMALLINT, created_at, updated_at`. Partial index on `(status, lease_deadline)` for takeover scans.

**Protocol.**

1. **Acquire (its own committed statement, so concurrent replays can *see* it and never block):**
   `INSERT INTO idempotency_keys (key, request_fingerprint, status, lease_deadline, lease_owner) VALUES (?, ?, 'in_progress', now()+lease, self) ON CONFLICT (key) DO NOTHING RETURNING *`.
   - Row returned → we own the key; go to Execute.
   - No row → the key exists; read it and branch:
     - `completed` **and** fingerprint matches → return the stored snapshot (true replay).
     - `completed` **and** fingerprint differs → **409 Conflict** (key reuse with a different payload — the security case from Brief §3.1).
     - `in_progress`, lease **not** expired → **409 Conflict + `Retry-After`** (Stripe semantics: never block, never double-execute).
     - `in_progress`, lease **expired** → attempt takeover via atomic CAS:
       `UPDATE idempotency_keys SET lease_deadline = now()+lease, lease_owner = self WHERE key = ? AND status = 'in_progress' AND lease_deadline < now() RETURNING *`.
       Row returned → we won the takeover; go to Execute. No row → someone else took over or it completed; re-read and branch again.

2. **Execute (one transaction):** the **guarded effect** (the credit / the funds lock + domain row + outbox row) **and** the completion flip commit together:
   ```
   BEGIN;  -- READ COMMITTED (see A4)
     <guarded effect: e.g. create_withdrawal_and_lock + outbox insert>
     UPDATE idempotency_keys
        SET status='completed', response_snapshot=?, response_status=?, updated_at=now()
      WHERE key=? AND status='in_progress' AND lease_owner=self;   -- conditional on still owning
     -- if that UPDATE affected 0 rows: a takeover won; abort so our effect rolls back, then re-read & return the now-completed snapshot.
   COMMIT;
   ```

**Takeover safety theorem (write this verbatim into `DESIGN.md`).**
The guarded effect and the transition to `completed` commit in a single transaction; therefore `status = 'completed'` **if and only if** the effect has been durably applied. Consider an observed key that is `in_progress` with an expired lease. Its Execute transaction has not committed — had it committed, the status would be `completed`, contradicting the observation. By atomicity, the effect has **not** been applied. A takeover that re-runs Execute therefore applies the effect for the first and only time. *Safety comes from atomicity, not from the lease.* The lease only bounds how long a replay waits before a takeover is permitted, so two executors do not both attempt the effect while the first might still be live. If two executors do race (lease expired but original still running), the conditional completion `UPDATE ... WHERE status='in_progress' AND lease_owner=self` admits exactly one winner; the loser's `UPDATE` matches 0 rows and its effect rolls back with the aborted transaction.

**Chaos schedules added (Phase 2):** arm a crash point inside Execute (between the effect statements and the conditional flip — a crash there commits neither). On recovery: (a) replay **before** lease expiry → assert `409 + Retry-After`; (b) replay **after** lease expiry → assert the takeover re-executes and the effect is applied **exactly once** (conservation holds, one withdrawal, one lock). Also arm the seam between Acquire's commit and Execute's start → same `in_progress`/recovery semantics.

---

## A3 — Relay duplicate-publish closure
**Amends Brief §3.4 (transactional outbox). Demonstrated in Phase 2; relay built in Phase 1.**

The outbox relay crashing between `XADD` and `mark-sent` republishes the same outbox row on restart — **by design**; the outbox is at-least-once. The brief asserted the consumer absorbs this. Assertion is not proof.

**Added (Phase 2):** a crash point at the relay's `XADD`→`mark-sent` seam. The schedule fires it, restarts the relay (which republishes), and asserts the **consumer-side dedup absorbs the duplicate** — conservation holds, the deposit/withdrawal is processed exactly once. This converts the design axiom ("at-least-once delivery is a feature") from a claim in the README into a committed, reproducible demonstration with a crash-point id behind it.

---

## A4 — Isolation level: demonstrate, don't document
**Amends Brief §3.5 (atomicity & isolation). Implemented in Phase 2; `with_tx` pins the level in Phase 0.**

The brief said "document the isolation level relied upon." We upgrade to a stronger, more deployable claim: **correct at the database's default isolation level, and here is the proof.**

- The Phase 0 `with_tx` helper pins **`READ COMMITTED`** explicitly in code (`build_transaction().read_committed()`), so the dependency is grep-able and no path silently inherits something stronger.
- The **entire harness runs under `READ COMMITTED`.** The argument (one paragraph in `DESIGN.md`): the deposit dedup relies on the **unique index** resolving the conflict in `INSERT … ON CONFLICT (tx_hash) DO NOTHING RETURNING` — a unique constraint cannot be bypassed by a phantom at any isolation level, so no snapshot isolation is needed there; the balance read-modify-write relies on the **`SELECT … FOR UPDATE`** pessimistic row lock, which serializes concurrent writers on that row at RC, so no `SERIALIZABLE` dependency exists. There is no read-only predicate whose stability we rely on, so write-skew is not in scope.
- **The counterexample is the before-run.** The legacy credit path reads `order.status` without a lock and then writes — a textbook RC lost-update: two concurrent deliveries both observe `pending` and both credit. The harness demonstrates this double-credit **at RC** in the before-run, then shows the rebuilt path clean **at the same RC level**. "We removed the anomaly" is shown, not claimed.

---

## A5 — Reconciliation joins the invariant set
**Amends Brief §5 (the proof) and §3.8 (reconciliation). Implemented in Phase 2.**

A reconciler that is never exercised under faults is decoration. It becomes **Invariant #5**:

> After every fault schedule completes (crash injected, processes restarted, queues drained), run the reconciler and assert it reports **zero drift**: Σ(credits) == Σ(distinct confirmed deposits); no withdrawal stranded in `processing` past its deadline; no locked balance without a corresponding completed/failed withdrawal; every outbox row marked sent has a stream entry.

This binds reconciliation (it exists) to the proof (it holds under the full crash-point enumeration), and gives the harness a second, independent oracle alongside the per-message conservation checker — agreement between the two is itself signal.

---

## Net effect on the Definition of Done (Brief §6)

The following DoD lines are upgraded; everything else in §6 stands.

- **§6.5 (the proof)** now reads: the harness commits the **exhaustive** crash-point sweep (every `CrashPointId` × every redelivery schedule) plus the seeded interleaving sweep; the `pre-idempotency` before-run produces the crash-point→violation table; the rebuilt run is clean row-by-row; **0** conservation violations and **1** send per withdrawal. Registry closure is verified (every `CrashPointId` has exactly one fire-site).
- **§6.2 (inbound keys)** now requires the full `in_progress → completed` lifecycle with lease-based takeover, the conditional-completion concurrency guard, and the three replay branches (snapshot / 409-conflict / 409-Retry-After). The takeover safety theorem is in `DESIGN.md`.
- **§6.7 (DESIGN.md)** now requires: the takeover safety theorem (A2), the RC isolation argument with the legacy counterexample (A4), and the at-least-once relay demonstration (A3).
- **Invariants** are now five (A5 adds reconciliation), asserted after every schedule.
- **Isolation**: the whole proof runs at `READ COMMITTED`; no `SERIALIZABLE` dependency anywhere.
