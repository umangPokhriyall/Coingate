# Coingate — Phase 1 Specification: The Idempotency Core (the product)

**Companion to:** `docs/specs/kickoff-brief.md`, `docs/specs/kickoff-amendment-1.md`, `docs/specs/phase0-spec.md`. Read all four first.
**This is the complete, authoritative Phase 1 spec.** The executing agent needs no other context.
**Scope:** build the exactly-once core on the Phase 0 substrate — the sans-IO idempotency decision logic and lifecycle state machines; the full in-progress→completed inbound-key protocol with lease takeover (Amendment §A2); the atomic dedup+credit path; the worker's effect-boundary reconciliation and idempotent finalize; the transactional outbox + relay (Amendment §A3); the gap-free poller; and the reconciler. Every dedup+effect transaction gets its crash-point fire-sites (Amendment §A1). **Phase 1 writes no DDL** (Phase 0 landed it) and **changes no Phase-0 frozen contract** (`with_tx`, `Chain`/`Signer`, the schema baseline, append-only `CrashPointId`).
**Audience:** Claude Code. Authoritative. This phase contains the guarantees; Phase 2 proves them.

---

## 1. Phase 1 in one paragraph

Phase 0 made the system poolable, exact, panic-free, and mockable. It changed **no** correctness behavior: the processor still writes order/deposit/balance as three separate statements, `/withdrawals` still dual-writes to Postgres then Redis, the worker still blind-sends, and there are no idempotency keys. Phase 1 installs the single design axiom everywhere it is currently violated: **the dedup decision and the side effect it guards commit in one transaction, and where the effect is external, an idempotency key sits at the boundary and the worker reconciles instead of resending.** Concretely: a sans-IO `idempotency` crate holds the decision logic and the two lifecycle state machines; the inbound `Idempotency-Key` protocol (acquire → decide → execute-and-snapshot in one `with_tx`, with lease-based takeover) lands on `/orders` and `/withdrawals`; the credit path becomes one transaction that inserts the deposit on its natural key (`tx_hash`) and credits **only if newly inserted**; the worker dispatches on withdrawal state and reconciles via `Signer::lookup` from the ambiguous `processing` state; `/withdrawals` writes the lock + an `outbox` row in one transaction and a new `relay` publishes to Redis at-least-once; the poller backfill becomes gap-free; and a reconciler computes drift as a second, independent oracle. Each dedup+effect transaction is instrumented with `crash_point!` fire-sites at every statement boundary. After Phase 1 the guarantees hold by construction; Phase 2 demonstrates them under an exhaustive crash sweep.

### 1.1 What becomes FROZEN after Phase 1
- The **`idempotency` crate public API** — `decide`, the lifecycle transition functions, the `IdempotencyStore` trait, and the `KeyRecord`/`Decision` types.
- The **Execute orchestration contract** — acquire → decide → `with_tx { effect; complete }` → replay-on-loss.
- The **atomic-credit transaction shape** and the **worker state-machine dispatch**.
- Every **`CrashPointId` variant added this phase** (append-only; Phase 2 verifies registry closure — exactly one fire-site per variant).

### 1.2 What Phase 1 inherits and must not re-litigate
`with_tx` is the only transaction constructor and is `READ COMMITTED` — every effect below runs through it; do not open a transaction any other way and do not raise the isolation level (Amendment §A4 — the proof runs at RC). The four locking fns already assume an ambient transaction (Phase 0 removed their inner wrappers) — call them inside `with_tx`. `store::money::parse_base_units` is the only decimal→base-units path. `ApiError::Conflict` already maps to 409.

---

## 2. Workspace additions & dependencies

```
idempotency/                 # NEW crate — sans-IO. NO diesel, NO redis, NO actix.
  src/key.rs                 # IdempotencyKey, request fingerprint (sha256 of raw body + method + path)
  src/lifecycle.rs           # Order + Withdrawal state machines; legal-transition guards
  src/decision.rs            # pure `decide(existing, fingerprint, now, lease, owner) -> Decision`
  src/store.rs               # the IdempotencyStore trait (the I/O boundary) + KeyRecord/KeyStatus/Decision
relay/                       # NEW binary — transactional-outbox relay (outbox -> Redis, at-least-once)
  src/main.rs
store/                       # IdempotencyStore impl; atomic-credit fn; idempotent finalize_*; outbox + reconciler queries
api/                         # Idempotency-Key extractor; Execute orchestration; /orders + /withdrawals wired
processor/                   # the one-transaction atomic credit; dead_letter for unverifiable
worker/                      # state-machine dispatch + lookup reconciliation; idempotent finalize; MPC URL from Config
poller/                      # gap-free paginated backfill (SolanaChain)
chaos_hooks/                 # CrashPointId gains the Phase-1 variants (append-only); fire-sites placed across crates
```

**Dependency allowlist (Phase 1 — add nothing else):**
- `idempotency`: `serde`, `serde_json`, `sha2` (deterministic request fingerprint), `chrono`, `uuid`, `thiserror`. **Sans-IO — no diesel/redis/actix/solana may appear in this crate's `Cargo.toml`.** (This is the line a reviewer checks first; if the crate compiles without a database, the boundary is real.)
- `store`: no new deps (diesel/r2d2/bigdecimal present); implements `IdempotencyStore`.
- `api`: `serde_json` (snapshot); actix present.
- `relay`: `redis`, `diesel`/`r2d2`, `tracing`, `tokio` — all present in-ecosystem; no new crate.
- Each crate placing fire-sites gains `[features] chaos = ["chaos_hooks/chaos"]`; verify `cargo build -p <crate> --features chaos` is green (per Phase 0's note, the virtual workspace can't take `--features` at the root — build per-crate).

---

## 3. Session 1.1 — The `idempotency` crate (sans-IO; the product)

The crate is pure: given facts, it returns decisions. No I/O type appears in it. It is the analogue of the TCP server's frozen `core`.

**`lifecycle.rs` — the two state machines (guards enforced here, used by `store`):**
```
Order:       pending ──► paid            (paid is terminal; paid→paid is a no-op, not an error)
Withdrawal:  pending ──► processing ──► completed
                                   └───► failed
                         (finalize_* are legal ONLY from `processing`; all others are no-ops)
```
```rust
pub fn order_can_mark_paid(current: &str) -> bool;        // true unless already paid
pub fn withdrawal_can_finalize(current: &str) -> bool;    // true iff current == "processing"
pub fn withdrawal_next(current: &str) -> &'static [&'static str];  // legal successors (for tests/assertions)
```

**`key.rs` — key + fingerprint:**
```rust
pub struct IdempotencyKey(pub String);                    // the client Idempotency-Key header value
/// Stable fingerprint of the request: sha256(method || '\n' || path || '\n' || raw_body) -> hex.
/// Guards "same key, different payload" -> 409. MUST hash the RAW body bytes, before deserialization.
pub fn request_fingerprint(method: &str, path: &str, raw_body: &[u8]) -> String;
```

**`decision.rs` — the pure decision (this is the §A2 protocol's brain):**
```rust
pub enum Decision {
    Execute,                          // we own the key (fresh acquire or won takeover) -> run the effect
    Replay { snapshot: serde_json::Value, status: i16 },  // completed + fingerprint match
    Conflict,                         // completed + fingerprint differs  -> 409
    RetryAfter { seconds: u64 },      // in_progress + lease NOT expired   -> 409 + Retry-After
    Takeover,                         // in_progress + lease expired       -> caller attempts the CAS takeover
}
/// Pure. Decides the branch given the EXISTING record (None means we just inserted -> Execute is implied
/// by the caller and `decide` is not consulted). `now`/`lease` drive the lease arithmetic.
pub fn decide(existing: &KeyRecord, fingerprint: &str, now: DateTime<Utc>) -> Decision;
```

**`store.rs` — the I/O boundary trait + record types (implemented in `store`, Session 1.2):**
```rust
pub enum KeyStatus { InProgress, Completed }
pub struct KeyRecord {
    pub status: KeyStatus,
    pub request_fingerprint: String,
    pub lease_deadline: Option<DateTime<Utc>>,
    pub lease_owner: Option<Uuid>,
    pub response_snapshot: Option<serde_json::Value>,
    pub response_status: Option<i16>,
}
pub enum Acquire { Acquired, Existing(KeyRecord) }     // result of INSERT ... ON CONFLICT DO NOTHING
pub trait IdempotencyStore {
    /// Its OWN committed statement so concurrent replays SEE in_progress and never block.
    fn acquire(&self, key: &str, fingerprint: &str, lease_deadline: DateTime<Utc>, owner: Uuid) -> Result<Acquire, StoreError>;
    /// Atomic CAS: UPDATE ... SET lease=..,owner=self WHERE status='in_progress' AND lease_deadline<now RETURNING.
    fn takeover(&self, key: &str, owner: Uuid, new_lease: DateTime<Utc>, now: DateTime<Utc>) -> Result<Option<KeyRecord>, StoreError>;
    /// Runs INSIDE the caller's with_tx (takes &mut PgConnection): conditional completion.
    /// UPDATE ... SET status='completed', snapshot=?, response_status=? WHERE key=? AND status='in_progress' AND lease_owner=self.
    /// Returns true iff one row changed (we won); false means a takeover beat us -> caller rolls back its effect.
    fn complete(conn: &mut PgConnection, key: &str, owner: Uuid, snapshot: &serde_json::Value, status: i16) -> Result<bool, diesel::result::Error>;
    fn read(&self, key: &str) -> Result<Option<KeyRecord>, StoreError>;
}
```

**Lease constant:** `IDEMPOTENCY_LEASE = 30s` (a module const; tunable, documented). `RetryAfter.seconds` = remaining lease.

**Unit tests (no DB):** fingerprint stability and method/path/body sensitivity; `decide` over every branch — completed+match→Replay, completed+mismatch→Conflict, in_progress+valid→RetryAfter(remaining), in_progress+expired→Takeover; lifecycle guards reject illegal transitions and accept legal ones; `paid→paid` is a no-op.

**Done when:** `cargo build -p idempotency` succeeds with zero I/O dependencies in its manifest; all branch/lifecycle/fingerprint unit tests pass.

---

## 4. Session 1.2 — Inbound keys on `/orders` + the Postgres store + the extractor

Implement the §A2 protocol end-to-end on the **simplest** effect (create order), so the machinery is proven before the harder `/withdrawals` combination in 1.5.

**The Idempotency-Key extractor (api).** `/orders` and `/withdrawals` require an `Idempotency-Key` header; absent → `400`. Capture the **raw body** (`web::Bytes`) to compute `request_fingerprint(method, path, &body)` before deserialization, then deserialize from the buffered bytes. Stash `(key, fingerprint)` for the handler. (A thin middleware or an extractor that buffers and re-injects the body — either is fine; the contract is: key present, fingerprint over raw bytes.)

**The `IdempotencyStore` Postgres impl (store).** Implement the four trait methods against `idempotency_keys` using the Phase 0 columns. `acquire` = `INSERT ... (status='in_progress', lease_deadline, lease_owner) ON CONFLICT (key) DO NOTHING RETURNING *`; if no row, `SELECT` the existing and return `Existing`. `takeover` / `complete` / `read` as specified. `complete` takes `&mut PgConnection` so it runs inside the handler's `with_tx`.

**The Execute orchestration (api helper — the reusable spine, FROZEN after this phase):**
```
let owner = Uuid::new_v4();
loop {
  match store.acquire(key, fingerprint, now()+LEASE, owner)? {
    Acquired => { /* fall through to Execute */ }
    Existing(rec) => match idempotency::decide(&rec, fingerprint, now()) {
        Replay{snapshot,status} => return replay_response(status, snapshot),
        Conflict               => return Err(ApiError::Conflict),               // 409, different payload
        RetryAfter{seconds}    => return Err(ApiError::retry_after(seconds)),   // 409 + Retry-After header
        Takeover => match store.takeover(key, owner, now()+LEASE, now())? {
            Some(_) => { /* we won; fall through to Execute */ }
            None    => continue,   // someone else took it / completed; re-acquire-read and re-decide
        },
        Execute => unreachable!(),
    }
  }
  // Execute: effect + conditional completion in ONE with_tx (READ COMMITTED).
  let outcome = with_tx(pool, |conn| {
      let response = run_effect(conn, ...)?;                  // domain effect (see below)
      let snapshot = serde_json::to_value(&response)?;
      crash_point!(CrashPointId::IdemAfterEffectBeforeComplete);
      let won = IdempotencyStorePg::complete(conn, key, owner, &snapshot, response.http_status)?;
      if !won { return Err(diesel::result::Error::RollbackTransaction); }  // takeover won -> roll back our effect
      crash_point!(CrashPointId::IdemAfterCompleteBeforeCommit);
      Ok((response, snapshot))
  });
  return match outcome {
      Ok((resp,_))                                   => Ok(resp),
      Err(RollbackTransaction)                       => { let rec = store.read(key)?.expect("completed by winner"); replay_response(rec) }
      Err(e)                                         => Err(e.into()),
  };
}
```
Place `crash_point!(CrashPointId::IdemAfterAcquireBeforeExecute)` between `acquire` returning `Acquired` and opening the Execute `with_tx`.

**`/orders` effect (the natural-key backstop).** `run_effect` = `INSERT INTO orders (...) ON CONFLICT (app_id, order_id) DO NOTHING RETURNING *`; if a row returned, that is the new order; if **no** row (a prior order with this `(app_id, order_id)` exists), `SELECT` and return it. Either way the snapshot reflects the real order. **Why both layers:** the header dedups request *replays*; the `(app_id, order_id)` unique constraint dedups *distinct keys for the same business order* (Brief §3.1). Orders have this natural backstop; withdrawals (1.5) do not, which is why the header is load-bearing there.

**CrashPointId variants added (append-only):** `IdemAfterAcquireBeforeExecute`, `IdemAfterEffectBeforeComplete`, `IdemAfterCompleteBeforeCommit`. Place exactly one fire-site each.

**Tests:** with Postgres — first POST creates and returns; identical replay returns the stored snapshot (same order id, no second row); same key + different body → 409; two distinct keys + same `(app_id, order_id)` → one order, both snapshots point to it. Without Postgres — the orchestration logic is unit-tested against a mock `IdempotencyStore`.

**Done when:** `/orders` is idempotent under replay and under distinct-key/same-business-order; 409 fires on payload mismatch; the three crash points exist; `cargo build -p api --features chaos` green.

---

## 5. Session 1.3 — The atomic credit (processor)

Rewrite `process_transaction` so the dedup decision and the credit commit together, and the credit is **gated on a first-time insert** (Brief §3.1 / Amendment §A4).

```rust
// inside with_tx(pool, |conn| { ... })  -- READ COMMITTED
let inserted: Option<DepositRow> = diesel::insert_into(deposits)
    .values(&deposit)                 // confirmed deposit row, tx_hash = the on-chain signature
    .on_conflict(tx_hash)
    .do_nothing()
    .returning(DepositRow::as_returning())
    .get_result(conn).optional()?;    // None == duplicate delivery (this signature already credited)

crash_point!(CrashPointId::ProcAfterDepositInsertBeforeCredit);

match inserted {
    None => Ok(CreditOutcome::Duplicate),                 // credit NOTHING. the unique index is the dedup oracle.
    Some(_) => {
        upsert_balance(conn, merchant_id, &token_mint, &amount)?;   // ON CONFLICT (merchant,token) DO UPDATE SET balance = balance + amount
        crash_point!(CrashPointId::ProcAfterCreditBeforeOrderPaid);
        if order_can_mark_paid(&order.status) {                     // lifecycle guard
            mark_order_paid(conn, order.id, &tx_hash)?;             // UPDATE ... SET status='paid', tx_hash, confirmed_at WHERE id=? AND status<>'paid'
        }
        crash_point!(CrashPointId::ProcAfterOrderPaidBeforeCommit);
        Ok(CreditOutcome::Credited)
    }
}
// after with_tx commits:
crash_point!(CrashPointId::ProcAfterCommitBeforeXack);
xack(stream, group, id);
```

**Isolation argument (A4), implemented:** the dedup rides the `UNIQUE(tx_hash)` index, which no isolation level can bypass — so no snapshot isolation is needed. The credit is the upsert's `DO UPDATE SET balance = balance + amount`, a single atomic statement (no app-level read-modify-write), so concurrent credits to one balance row serialize on the row lock at RC. There is no read-only predicate we depend on. Hence correct at `READ COMMITTED`.

**Dead-letter, not silent drop (Brief §3.7).** A stream entry that cannot become a valid credit — no matching order for the memo, or verification failure (amount/mint mismatch) — is inserted into `dead_letter` (own small `with_tx`, `reason` set), then `XACK`ed. Malformed entries (parse failure) likewise dead-letter. Nothing is silently discarded. (Assumption, documented: orders are created before payment in this flow, so "no matching order" is a genuine error, not a deposit-before-order race; if that flow assumption changes a bounded-retry buffer would be needed — out of scope.)

**CrashPointId variants added:** `ProcAfterDepositInsertBeforeCredit`, `ProcAfterCreditBeforeOrderPaid`, `ProcAfterOrderPaidBeforeCommit`, `ProcAfterCommitBeforeXack`.

**Done when:** a redelivered signature inserts nothing and credits nothing; an unverifiable deposit lands in `dead_letter` and is `XACK`ed; the credit is unreachable unless the deposit was newly inserted; the four crash points exist; `cargo build -p processor --features chaos` green. (DB-backed assertions run where Postgres is available; otherwise the transaction shape is compile-checked and the dedup branch unit-tested against the store fns.)

---

## 6. Session 1.4 — Worker effect-boundary reconciliation + idempotent finalize

Make the only non-transactional effect in the system — the external `Signer::send` — safe via a state-machine dispatch and `Signer::lookup` reconciliation, and make `finalize_*` idempotent.

```rust
// process_withdrawal<S: Signer>  -- the dispatch is on CURRENT withdrawal state
let wd = with_tx(pool, |c| find_withdrawal(c, id))?;
match wd.status.as_str() {
    "completed" | "failed" => { xack(...); return; }         // terminal: redelivery after finalize-then-crash-before-xack. no-op.
    "processing" => {
        // AMBIGUOUS — we may already have sent. RECONCILE, never blind-resend.
        match signer.lookup(id).await? {
            Some(sig) => finalize_success(pool, id, &sig)?,  // it went out; finalize idempotently
            None      => send_then_finalize(pool, &signer, &wd).await?,  // not sent; safe to send now
        }
        xack(...);
    }
    "pending" => {
        with_tx(pool, |c| set_withdrawal_processing(c, id))?;            // own tx; commit BEFORE sending
        crash_point!(CrashPointId::WorkerAfterStatusProcessingBeforeSend);
        match signer.send(SendRequest{ key: id, to, amount, mint }).await {  // key = withdrawal_id (the effect-boundary key)
            Ok(sig)                         => { crash_point!(CrashPointId::WorkerAfterSendBeforeFinalize);
                                                 finalize_success(pool, id, &sig)?;
                                                 crash_point!(CrashPointId::WorkerAfterFinalizeBeforeXack);
                                                 xack(...); }
            Err(SignerError::Transport(_))  => { /* AMBIGUOUS: leave in `processing`, DO NOT xack -> redelivery -> reconcile */ }
            Err(_rejected_or_no_sig)        => { finalize_failed(pool, id, reason)?; xack(...); }
        }
    }
}
```

**Idempotent `finalize_*` (store, rewritten).** Each gates its balance move on the status transition actually happening:
```
finalize_success:  UPDATE withdrawals SET status='completed', tx_hash=? WHERE id=? AND status='processing'   -- rows_affected
                   IF rows_affected == 1 { /* now move locked_balance down by amount, FOR UPDATE on the balance row */ }
                   ELSE { /* already terminal: no-op, do NOT touch balance */ }
finalize_failed:   UPDATE withdrawals SET status='failed' WHERE id=? AND status='processing'  -- rows_affected
                   IF rows_affected == 1 { /* restore locked->balance */ } ELSE no-op
```
This is the §A2 pattern applied to finalize: the balance effect fires only when the guarded state transition fires, so a double-finalize moves money once. (The Phase 0 `finalize_*` unconditionally adjusted balances — that is the bug being closed.)

**Reconciliation correctness (state for `DESIGN.md`):** the worker calls `send` exactly once per withdrawal because (a) `send` is reached only from `pending` after a committed `pending→processing`, and (b) every redelivery thereafter lands in `processing` and takes the `lookup` path, which does **not** invoke `send`. With the `CountingMockSigner`, `send_count(id) == 1` across all redelivery/concurrent-consumer schedules — the Phase 2 Invariant #2.

**Config cleanup (close the Phase 0 deferral):** thread the MPC base URL from `Config` into the worker's `MpcSigner` (delete the hardcoded `http://127.0.0.1:3000`). The `payment.rs` devnet RPC literal is off the exactly-once path — leave it flagged for a later sweep; do not expand scope to it here.

**CrashPointId variants added:** `WorkerAfterStatusProcessingBeforeSend`, `WorkerAfterSendBeforeFinalize`, `WorkerAfterFinalizeBeforeXack`.

**Done when:** redelivery in `processing` reconciles via `lookup` and never re-sends (`send_count == 1`); a `Transport` error leaves the withdrawal `processing` un-XACKed for retry; `Rejected`/`NoSignature` finalizes failed and XACKs; double-finalize is a balance no-op; the three crash points exist; `cargo build -p worker --features chaos` green.

---

## 7. Session 1.5 — Transactional outbox + relay + gap-free poller + `/withdrawals` Execute

This session closes the dual-write (Brief §3.4) and completes the inbound-key protocol on the money-out path — the hardest combination, which is why it is last.

**The outbox write (api, `/withdrawals`).** `/withdrawals` now runs the §A2 Execute orchestration (Session 1.2 spine) with the effect being the lock **and** the outbox row, atomically:
```
run_effect = with_tx body:
    create_withdrawal_and_lock(conn, merchant, token, amount, target)?;   // debits balance, creates withdrawal (pending)
    crash_point!(CrashPointId::WithdrawAfterLockBeforeOutbox);
    insert_outbox(conn, topic="withdrawal_requests", payload)?;            // the durable publish-intent
    crash_point!(CrashPointId::WithdrawAfterOutboxBeforeComplete);
    // (the orchestration's `complete` + IdemAfterCompleteBeforeCommit follow)
```
**Delete** the handler's direct `XADD` and the `revert_withdrawal_lock`-on-redis-failure compensation — the outbox makes them obsolete: the lock and the publish-intent commit together, and the relay guarantees eventual publish. There is no longer a window where funds are locked with no work item. Withdrawals have **no** natural business key, so the `Idempotency-Key` header is the sole dedup for this path (a client retry with the same key replays the stored response; without it, two identical withdrawals are two legitimate withdrawals).

**The relay (new binary).** A loop:
```
for row in select_unsent_outbox(conn) /* WHERE sent_at IS NULL ORDER BY created_at */ {
    redis XADD row.topic * data row.payload;
    crash_point!(CrashPointId::RelayAfterXaddBeforeMarkSent);
    mark_outbox_sent(conn, row.id);     // UPDATE ... SET sent_at = now() WHERE id = ?
}
```
`XADD` then `mark-sent` **cannot** be atomic (Redis is not in the DB transaction) — that is the design, not a flaw: the outbox is the durable intent and publish is at-least-once. A crash at `RelayAfterXaddBeforeMarkSent` republishes the row on restart; the **consumer-side dedup absorbs the duplicate** (the worker's `pending→processing` guard + `withdrawal_id` reconciliation). This is Amendment §A3 — Phase 1 places the seam; Phase 2 demonstrates absorption.

**Gap-free poller (SolanaChain, Brief §1.4).** Replace the `limit:10 + until:checkpoint` single-fetch with a **paginated backfill**: page backward with the `before` cursor (descending) until reaching the stored checkpoint, accumulating **all** intervening signatures, then emit oldest-first. No signature between the checkpoint and chain head is ever skipped, regardless of burst size. Because the consumer now dedups on `tx_hash`, poller duplicates are harmless and the checkpoint is a pure liveness optimization — but gap-freeness is still required to prevent *loss*. Keep this behind the frozen `Chain::deposits_since` signature (internal loop only).

**CrashPointId variants added:** `WithdrawAfterLockBeforeOutbox`, `WithdrawAfterOutboxBeforeComplete`, `RelayAfterReadBeforeXadd`, `RelayAfterXaddBeforeMarkSent`.

**Done when:** `/withdrawals` is idempotent under replay (header), commits lock+outbox atomically, and no longer touches Redis directly; the relay publishes unsent outbox rows at-least-once and marks them sent; a kill at the relay seam republishes and the worker absorbs it; the poller drains a >page-size burst with zero skipped signatures; the four crash points exist; `cargo build -p api -p relay -p poller --features chaos` green.

---

## 8. Session 1.6 — The reconciler (the second oracle)

Build the drift detector as reusable domain logic (Brief §3.8). **Its elevation to Invariant #5 — running after every fault schedule and asserting zero drift — is Phase 2 (Amendment §A5); Phase 1 delivers the function the invariant depends on.** A reconciler is domain query logic, not harness scaffolding, so it belongs here, where it also serves as a development oracle while 1.3–1.5 are built.

```rust
pub struct DriftReport {
    pub credit_conservation: Vec<Imbalance>,   // per (merchant, token): sum(credits) vs sum(distinct confirmed deposits)
    pub stuck_processing:    Vec<Uuid>,         // withdrawals in `processing` past a deadline
    pub orphan_locks:        Vec<(Uuid,String)>,// locked_balance with no non-terminal/terminal withdrawal explaining it
    pub unsent_aged_outbox:  Vec<Uuid>,         // outbox rows unsent past a threshold (relay liveness)
}
impl DriftReport { pub fn is_clean(&self) -> bool; }  // all empty
pub fn reconcile(conn: &mut PgConnection, now: DateTime<Utc>, deadlines: Deadlines) -> Result<DriftReport, diesel::result::Error>;
```
Expose via a CLI subcommand or a small binary for manual runs. The **conservation** check is the headline: `Σ(balance credits) == Σ(distinct confirmed deposit amounts)` per merchant/token — the same invariant the Phase 2 harness asserts independently, so agreement between the two is itself signal.

**Done when:** `reconcile` reports clean on a correctly-processed seeded dataset and reports the specific drift on hand-corrupted datasets (a missing credit, a stuck withdrawal, an orphan lock); unit-tested on seeded states.

---

## 9. Phase 1 Definition of Done

1. The **`idempotency` crate is sans-IO** — its manifest has no diesel/redis/actix/solana; `decide`, the lifecycle guards, the `IdempotencyStore` trait, and `KeyRecord`/`Decision` are implemented and unit-tested (every branch, every illegal transition).
2. **Inbound `Idempotency-Key` protocol** (Amendment §A2) on `/orders` and `/withdrawals`: required header (400 if absent); `acquire → decide → with_tx{effect; complete} → replay-on-loss`; the three replay branches (snapshot / 409-conflict / 409+Retry-After); lease-based takeover via the conditional-completion CAS. The `(app_id, order_id)` natural-key backstop on orders.
3. **Atomic credit:** one `with_tx`; `INSERT deposit ON CONFLICT (tx_hash) DO NOTHING RETURNING`; credit + mark-paid **only if inserted**; unverifiable → `dead_letter` then XACK. Correct at `READ COMMITTED` by the §A4 argument (unique-index dedup + atomic upsert increment; no serializable dependency).
4. **Effect-boundary safety:** worker dispatches on withdrawal state, reconciles from `processing` via `Signer::lookup`, never blind-resends; `finalize_*` are idempotent (balance moves gated on the `processing→terminal` transition). `send_count == 1` per withdrawal on the happy path and on redelivery.
5. **Transactional outbox + relay** replace the `/withdrawals` dual-write; the direct `XADD` and redis-failure revert are deleted; the relay publishes at-least-once and marks sent after `XADD`. **Gap-free** paginated poller backfill.
6. **Reconciler** built and unit-tested; reports clean on correct data and pinpoints drift on corrupted data.
7. **Crash-point fire-sites** placed at every statement boundary inside each dedup+effect transaction and at every `XADD`/`XACK`/outbox-mark seam (Amendment §A1). Every `CrashPointId` variant added this phase has **exactly one** fire-site; `cargo build --features chaos` is green for every affected crate. (Phase 2 verifies full registry closure.)
8. **No frozen contract changed:** `with_tx` remains the only transaction constructor and RC; `Chain`/`Signer` signatures unchanged; no DDL written; `CrashPointId` only appended to.
9. `cargo build`, `cargo clippy`, `cargo test` green workspace-wide; `cargo build -p <crate> --features chaos` green for each instrumented crate; dependencies limited to the §2 allowlist.

This phase installs the guarantees. It does **not** prove them under fault injection — that is Phase 2, which runs the exhaustive crash sweep, the before/after table against the `pre-idempotency` tag, the five invariants (conservation, at-most-once send, replay-safety, no-stranded-funds, reconciler-clean), all at `READ COMMITTED`.

---

# Appendix A — `CLAUDE.md` update for Phase 1

```markdown
## Authoritative specs
- docs/specs/kickoff-brief.md         — strategy, the audit, the primitives, the DoD
- docs/specs/kickoff-amendment-1.md   — chaos enumeration, in-progress key protocol, RC proof
- docs/specs/phase0-spec.md           — substrate, traits, schema baseline, chaos scaffolding (DONE)
- docs/specs/phase1-spec.md           — CURRENT: the idempotency core

## Hard rules (Phase 1)
1. The dedup decision and the effect it guards commit in ONE with_tx, or — for the external
   send — a key sits at the boundary and the worker RECONCILES (Signer::lookup), never resends.
2. with_tx is the ONLY transaction constructor and is READ COMMITTED. Do not raise the isolation
   level. Do not open a transaction any other way. The proof runs at RC (Amendment §A4).
3. The `idempotency` crate is sans-IO: no diesel/redis/actix/solana in its Cargo.toml. Ever.
4. Write NO DDL — Phase 0 landed the schema. Use idempotency_keys/outbox/dead_letter as-is.
5. Credit ONLY if the deposit was newly inserted (ON CONFLICT DO NOTHING RETURNING -> Some).
   finalize_* move balance ONLY if the processing->terminal transition fired.
6. Place one crash_point! fire-site at every statement boundary inside each dedup+effect
   transaction and at every XADD/XACK/outbox-mark seam. CrashPointId is APPEND-ONLY.
   `cargo build -p <crate> --features chaos` must stay green.
7. FROZEN after this phase: idempotency public API, the Execute orchestration spine, the
   atomic-credit shape, the worker state machine. Do not change Phase-0 frozen contracts.
8. Phase 1 deps only: idempotency -> serde/serde_json/sha2/chrono/uuid/thiserror (no I/O);
   relay -> existing redis/diesel/r2d2/tracing/tokio. Nothing else.

## Scope discipline
One session = one deliverable. End with cargo build + clippy + test (+ `-p <crate> --features
chaos` build for instrumented crates), list changes and the CrashPointId variants added, STOP.
```

---

# Appendix B — Claude Code execution plan (6 sessions)

| # | Session | Deliverable | Done when |
|---|---|---|---|
| 1.1 | idempotency crate | sans-IO: `decide`, lifecycle guards, `IdempotencyStore` trait, fingerprint | builds with no I/O deps; all branch/lifecycle/fingerprint tests pass |
| 1.2 | inbound keys on /orders | extractor, Postgres `IdempotencyStore`, Execute spine, natural-key backstop | replay returns snapshot; payload-mismatch 409; distinct-key/same-order → one order; 3 crash points |
| 1.3 | atomic credit | one-tx insert-on-conflict → credit-if-inserted; dead_letter | redelivery credits nothing; unverifiable dead-letters; 4 crash points |
| 1.4 | worker reconciliation | state-machine dispatch + `lookup`; idempotent finalize; MPC URL from Config | `send_count==1` under redelivery; double-finalize is a no-op; 3 crash points |
| 1.5 | outbox + relay + poller + /withdrawals | outbox replaces dual-write; relay; gap-free backfill | lock+outbox atomic; relay at-least-once; burst drained with 0 skips; 4 crash points |
| 1.6 | reconciler | `DriftReport` + `reconcile`; CLI | clean on correct data; pinpoints drift on corrupted data |

1.2 and 1.5 are the heavy builds. Split 1.2 at the extractor/orchestration boundary if context grows; split 1.5 at the outbox-write vs relay vs poller boundaries. Run sessions in order — 1.5 depends on the Execute spine from 1.2 and the worker dispatch from 1.4 (the relay's at-least-once publish is absorbed by 1.4's reconciliation).

### Exact prompts (paste one per session; verify + commit before the next)

**Session 1.1**
> Read `CLAUDE.md` and `docs/specs/phase1-spec.md` §1–§3. Execute **Session 1.1 only**: create the sans-IO `idempotency` crate — `key.rs` (IdempotencyKey + `request_fingerprint` over raw body), `lifecycle.rs` (Order/Withdrawal state machines + guards), `decision.rs` (`decide` → Execute/Replay/Conflict/RetryAfter/Takeover), `store.rs` (the `IdempotencyStore` trait + `KeyRecord`/`KeyStatus`/`Decision`). No diesel/redis/actix/solana in its Cargo.toml. Unit-test every decision branch, every lifecycle guard, and fingerprint stability/sensitivity. Build+clippy+test green, commit, STOP.

**Session 1.2**
> Read `CLAUDE.md` and `phase1-spec.md` §4 (and §A2 of the amendment). Execute **Session 1.2 only**: add the Idempotency-Key extractor (required header → 400; fingerprint over raw body); implement the Postgres `IdempotencyStore` (acquire/takeover/complete/read against `idempotency_keys`); build the Execute orchestration spine (acquire → decide → `with_tx{effect; complete}` → replay-on-loss, with lease takeover); wire `/orders` with the `ON CONFLICT (app_id, order_id) DO NOTHING RETURNING` natural-key backstop. Append CrashPointId `IdemAfterAcquireBeforeExecute`, `IdemAfterEffectBeforeComplete`, `IdemAfterCompleteBeforeCommit` with one fire-site each. Test replay/conflict/distinct-key-same-order. Build+clippy+test green; `cargo build -p api --features chaos` green; commit, STOP.

**Session 1.3**
> Read `CLAUDE.md` and `phase1-spec.md` §5. Execute **Session 1.3 only**: rewrite the processor credit path into one `with_tx` — `INSERT deposit ON CONFLICT (tx_hash) DO NOTHING RETURNING`, credit (`upsert_balance` atomic increment) and `mark_order_paid` (lifecycle-guarded) ONLY if a row was inserted; route unverifiable/malformed entries to `dead_letter` then XACK. Append CrashPointId `ProcAfterDepositInsertBeforeCredit`, `ProcAfterCreditBeforeOrderPaid`, `ProcAfterOrderPaidBeforeCommit`, `ProcAfterCommitBeforeXack` with one fire-site each. Build+clippy+test green; `cargo build -p processor --features chaos` green; commit, STOP.

**Session 1.4**
> Read `CLAUDE.md` and `phase1-spec.md` §6. Execute **Session 1.4 only**: rewrite `process_withdrawal` to dispatch on withdrawal state — terminal→XACK no-op; `processing`→reconcile via `Signer::lookup` (never resend); `pending`→commit `processing` then `send(key=withdrawal_id)`, with the Transport/Rejected/NoSignature three-way preserved. Make `finalize_success`/`finalize_failed` idempotent (balance move gated on the `processing→terminal` transition). Thread the MPC base URL from Config (delete the hardcoded literal). Append CrashPointId `WorkerAfterStatusProcessingBeforeSend`, `WorkerAfterSendBeforeFinalize`, `WorkerAfterFinalizeBeforeXack` with one fire-site each. Test `send_count==1` under redelivery and double-finalize-is-no-op. Build+clippy+test green; `cargo build -p worker --features chaos` green; commit, STOP.

**Session 1.5**
> Read `CLAUDE.md` and `phase1-spec.md` §7 (and §A3 of the amendment). Execute **Session 1.5 only**: make `/withdrawals` run the Execute spine with the effect = `create_withdrawal_and_lock` + `insert_outbox` in one `with_tx`; delete the handler's direct XADD and the redis-failure revert. Build the `relay` binary (drain `outbox WHERE sent_at IS NULL`, XADD, then mark sent). Make the poller backfill gap-free (paginate with `before` to the checkpoint; emit oldest-first) behind the unchanged `Chain` signature. Append CrashPointId `WithdrawAfterLockBeforeOutbox`, `WithdrawAfterOutboxBeforeComplete`, `RelayAfterReadBeforeXadd`, `RelayAfterXaddBeforeMarkSent` with one fire-site each. Build+clippy+test green; `cargo build -p api -p relay -p poller --features chaos` green; commit, STOP.

**Session 1.6**
> Read `CLAUDE.md` and `phase1-spec.md` §8. Execute **Session 1.6 only**: build the reconciler — `DriftReport` + `reconcile(conn, now, deadlines)` computing credit conservation (Σ credits vs Σ distinct confirmed deposits per merchant/token), stuck-processing withdrawals, orphan locks, and aged-unsent outbox — exposed via a CLI/binary for manual runs. Unit-test clean-on-correct and pinpoint-on-corrupted. Build+clippy+test green, commit, STOP. Phase 1 complete — report the DoD §9 items and the full list of CrashPointId variants now placed (for Phase 2 registry-closure).
```
