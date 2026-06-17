# Coingate — Phase 0 Specification: Substrate, Trait Boundary, Schema Baseline, Chaos Scaffolding

**Companion to:** `docs/specs/kickoff-brief.md` and `docs/specs/kickoff-amendment-1.md`. Read both first.
**This is the complete, authoritative Phase 0 spec.** The executing agent needs no other context.
**Scope:** make the existing behavior correct-enough and *reproducible* before any idempotency logic is written — pooled storage, exact money, panic-free boundaries, the `Chain`/`Signer` trait boundary with the effect-key baked in, one clean schema baseline (including the forward tables Phase 1 will fill), and the compile-out chaos fail-point scaffolding.
**Audience:** Claude Code. Authoritative. Foundation phase — nothing here implements exactly-once; it makes exactly-once *buildable and provable*.

---

## 1. Phase 0 in one paragraph

The current substrate makes the project untestable: a global `Mutex` over a single DB connection serializes everything, money is `f64`, hostile input aborts workers, the outside world (Solana RPC, MPC signer) is wired directly into handlers with `.unwrap()`, and the migration history already contains two conflicting `deposits` tables. None of the idempotency work in Phase 1 can be proven on that foundation. Phase 0 replaces the connection mutex with a pool fronted by one `with_tx` helper that pins `READ COMMITTED` explicitly, moves all amounts to exact base-unit integers, makes every request/message/RPC boundary return a typed error instead of panicking, hides the chain and the signer behind two traits (with the withdrawal-id idempotency key already in `Signer::send`'s signature) backed by a real impl and a deterministic mock, replaces the migration history with one reviewable baseline that also lands the `idempotency_keys`, `outbox`, and `dead_letter` tables Phase 1 will use, and stands up the `chaos`-feature fail-point scaffolding that compiles to nothing in normal builds. After Phase 0 the system behaves as before but is poolable, exact, crash-modelable, and mockable end-to-end.

### 1.1 What becomes FROZEN after Phase 0
These are contracts Phase 1+ must not change without a spec amendment:
- The **`Chain` and `Signer` trait signatures** (including `Signer::send` carrying the idempotency key).
- The **`with_tx` transaction entry point** and its `READ COMMITTED` pin.
- The **schema baseline** (all tables, constraints, and the `idempotency_keys` column set from Amendment 1 §A2).
- The **`CrashPointId` enum** is *append-only* (Phase 1 adds variants as fire-sites are placed; none are removed or renumbered).

---

## 2. Workspace additions & dependencies

```
store/                       # PgConnection-in-a-Mutex DELETED; r2d2 pool + with_tx + per-conn fns
  src/pool.rs                # NEW — Pool type, builder, with_tx (RC-pinned)
  src/config.rs              # REWRITE — from_env, fail-fast, no hardcoded secrets/URLs
  src/error.rs               # NEW — StoreError (thiserror)
  migrations/<new baseline>  # ONE collapsed migration set (replaces the conflicting history)
external/                    # NEW crate — the mocked outside world behind traits
  src/chain.rs               # trait Chain + SolanaChain + MockChain
  src/signer.rs              # trait Signer (send carries the key) + MpcSigner + CountingMockSigner
chaos_hooks/                 # NEW crate — crash_point! macro, CrashPointId, registry (feature = "chaos")
api/                         # web::Data<Pool>; ApiError; panic-free handlers; tracing
poller/  processor/  worker/ # depend on `external` traits; tracing; no .unwrap on inputs
```

**Dependency allowlist (Phase 0 only — add nothing else):**
- `store`: `diesel` with features `["postgres","r2d2","numeric","uuid","chrono","serde_json"]`, `r2d2`, `thiserror`. (Use diesel-native r2d2 pooling. **Rejected:** `deadpool`/`bb8` — they pull an async-pool dependency we do not need; diesel handlers here are synchronous and pooled per request.)
- workspace: `tracing`, `tracing-subscriber` (env-filter). Replaces every `println!`/`eprintln!`.
- `api`: `thiserror` for `ApiError` + `actix_web::ResponseError`.
- `external`: reuses existing `solana-client`/`solana-sdk`/`reqwest`; mocks need no new deps (use `std`/`parking_lot` only if a map lock is needed — prefer `std::sync::Mutex` to avoid a dep).
- `chaos_hooks`: **no external dependency.** Hand-rolled macro + registry. (**Rejected:** the `fail` crate — it is runtime-configured and heavier than needed; we want a *closed, enumerable* `CrashPointId` set and zero-cost compile-out via a cargo feature, which a 40-line hand-roll gives exactly.)

---

## 3. Session 0.1 — Connection pool, config, the one transaction entry point

Delete `store::Store { conn: PgConnection }` and every `&mut self` method's reliance on a single connection. Replace with a pool and free functions / a thin per-request handle taking `&mut PgConnection`.

```rust
// store/src/pool.rs
pub type Pool = diesel::r2d2::Pool<diesel::r2d2::ConnectionManager<diesel::pg::PgConnection>>;

pub fn build_pool(cfg: &Config) -> Result<Pool, StoreError>;   // sized pool; fail-fast on bad URL

/// The SINGLE transaction entry point for the whole workspace.
/// Pins READ COMMITTED explicitly so no path silently depends on a stronger level (Amendment §A4).
pub fn with_tx<T, F>(pool: &Pool, f: F) -> Result<T, StoreError>
where
    F: FnOnce(&mut diesel::pg::PgConnection) -> Result<T, diesel::result::Error>;
//  impl: let mut conn = pool.get()?;
//        conn.build_transaction().read_committed().run(|c| f(c))
```

```rust
// store/src/config.rs
pub struct Config {
    pub db_url: String,
    pub redis_url: String,
    pub jwt_secret: String,        // was the hardcoded b"super-secret-key"
    pub solana_rpc_url: String,    // was hardcoded devnet literal
    pub listen_addr: String,
}
impl Config { pub fn from_env() -> Result<Config, ConfigError>; }  // no defaults for secrets; fail fast
```

- `api/src/main.rs`: build the pool once, pass `web::Data<Pool>`; **delete `Arc<Mutex<Store>>`.** Each handler acquires a connection from the pool (or calls a store fn that does); no shared mutable store.
- Convert existing store methods (`find_order`, `update_order`, `insert_deposit`, `upsert_balance`, the withdrawal fns, etc.) to take `conn: &mut PgConnection`. The already-transactional withdrawal fns (`create_withdrawal_and_lock`, `revert_withdrawal_lock`, `finalize_*`) keep their `conn.transaction` bodies but are now invoked **through `with_tx`** so the isolation pin is uniform. Do not change their logic in Phase 0.
- **Do not** touch idempotency, the credit path's atomicity, or the worker's send logic this session. Pool + config + `with_tx` only.

**Done when:** `api` serves concurrent requests with no global lock; `with_tx` is the only place a transaction is constructed; secrets/URLs come from env; build green.

---

## 4. Session 0.2 — Structured logging, panic-free boundaries, exact money

**Logging.** Initialize `tracing-subscriber` in each binary's `main`. Replace **every** `println!`/`eprintln!`/emoji log in `api`, `poller`, `processor`, `worker`, `store` with `tracing` events (`info!`/`warn!`/`error!`), structured fields not string interpolation where it is on a per-event/per-request path.

**Panic-free.** Introduce `api/src/error.rs`:
```rust
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("not found")] NotFound,
    #[error("bad request: {0}")] BadRequest(String),
    #[error("unauthorized")] Unauthorized,
    #[error("conflict")] Conflict,            // reserved for Phase 1 idempotency
    #[error(transparent)] Store(#[from] StoreError),
}
impl actix_web::ResponseError for ApiError { /* map to 404/400/401/409/500 */ }
```
Every handler returns `Result<HttpResponse, ApiError>`. **Remove every `.unwrap()`/`.expect()` on input-derived data.** Canonical fix-list from the audit (not exhaustive — grep for `.unwrap()`/`.expect()` in handlers and stream parsers and convert each):
- `payment.rs`: `Uuid::parse_str(...).unwrap()` (×3), `Pubkey::from_str(&req.from_address).expect(...)`, `rpc.get_latest_blockhash().await.unwrap()`, `resp["outAmount"].as_str().unwrap().parse().unwrap()`, `get_token_supply(...).await.unwrap()`, `transfer_checked(...).unwrap()`.
- `merchant.rs`: `Uuid::parse_str(&claims.sub).unwrap()` (×3), `BigDecimal::from_str(&req.price_amount.to_string()).unwrap()`, `verify(...).unwrap()`, `hash(...).unwrap()`, `generate_jwt`'s `encode(...).unwrap()`.
- `worker`/`processor`: `json[...].as_str().unwrap()` in payload parsing (already partly guarded — finish it; a malformed stream entry must dead-letter, not abort the loop).

**Exact money.** Ban `f64` on any amount. `CreateOrderRequest.price_amount: f64` → a decimal **string** parsed to `BigDecimal`; all internal amounts are integer base units (`BigDecimal` with zero fractional digits, or `i128`/`u64` where the range is known). No amount is ever constructed via `f64::to_string()` round-trips. Add a focused unit test: a representative set of decimal-string amounts converts to base units exactly (no binary-float drift).

**Auth quick-fix (bounded — auth is not the thesis).** Replace `find_app_by_token`'s full-table-scan-then-bcrypt-each with an indexed lookup: store a non-secret lookup id (e.g. a token prefix or a fast hash) indexed in `apps`, fetch the single candidate, bcrypt-verify only that one. Do not redesign auth further.

**Done when:** a fuzz/negative test (malformed UUIDs, missing fields, oversized/garbage bodies, malformed stream entries) yields `4xx` or a dead-letter, **never** a panic or process abort; zero `f64` in any money path; zero `println!`/`eprintln!` remain; build green.

---

## 5. Session 0.3 — The `external` crate: `Chain` and `Signer` behind traits

This is the sans-IO move applied to the outside world. The chain and the signer become interfaces; Solana/MPC and their deterministic mocks are implementations. The mocks are first-class — the Phase 2 proof runs on them.

```rust
// external/src/signer.rs
pub struct SendRequest<'a> {
    pub key: uuid::Uuid,        // THE EFFECT-BOUNDARY IDEMPOTENCY KEY — the withdrawal_id (Brief §3.3)
    pub to: &'a str,
    pub amount: u64,            // base units
    pub mint: Option<&'a str>,  // None = native SOL
}
pub trait Signer {
    async fn send(&self, req: SendRequest<'_>) -> Result<Signature, SignerError>;
    /// Reconciliation: return a prior result for this key WITHOUT performing a new send.
    /// The worker calls this from the ambiguous `processing` state instead of re-sending (Brief §3.3).
    async fn lookup(&self, key: uuid::Uuid) -> Result<Option<Signature>, SignerError>;
}

// Real impl — forwards `key` to the MPC `/send` as `idempotency_key`, documenting the dedup
// contract the real signer MUST honor. Untouched beyond wrapping the existing HTTP call.
pub struct MpcSigner { base_url: String, http: reqwest::Client }

// Mock — the proof's instrument.
//  * `send` COUNTS every invocation (this is what Invariant #2 asserts == 1 per key).
//  * `lookup` returns the prior signature and does NOT count.
// A worker that re-sends on redelivery (instead of reconciling via `lookup`) drives the
// count to 2 and fails the invariant. Deterministic signatures (derived from `key`).
pub struct CountingMockSigner { /* Mutex<HashMap<Uuid,(usize, Signature)>> */ }
impl CountingMockSigner { pub fn send_count(&self, key: uuid::Uuid) -> usize; }
```

```rust
// external/src/chain.rs
pub enum TxKind { Sol, Token }
pub struct DepositEvent {
    pub signature: String,            // the natural idempotency key for the credit path
    pub slot: u64,
    pub memo_id: Option<String>,
    pub kind: TxKind,
    pub from: Option<String>, pub to: Option<String>,
    pub amount: Option<u64>,          // base units
    pub token_mint: Option<String>, pub token_decimals: Option<u8>,
}
pub trait Chain {
    /// Return new deposit events plus the advanced cursor.
    async fn deposits_since(&self, cursor: Option<Cursor>)
        -> Result<(Vec<DepositEvent>, Option<Cursor>), ChainError>;
}
pub struct SolanaChain { /* wraps current poller parse logic + RpcClient + fat pubkey */ }
// Deterministic, scriptable; can emit duplicates and bursts on demand for Phase 2 schedules.
pub struct MockChain { /* Vec<DepositEvent> script + position */ }
```

- `poller` depends on `Chain` (the gap-free backfill contract is **Phase 1.5** — here we only move the existing logic behind the trait; do not "fix" pagination yet, just preserve current behavior behind `SolanaChain`).
- `worker` depends on `Signer`. In Phase 0 it still calls `send` on the happy path; the status-guard + `lookup` reconciliation wiring is **Phase 1.4**. Here we only make the call go through the trait and pass `key = withdrawal_id`.

**Done when:** `poller` and `worker` compile against the traits; a happy-path end-to-end (create order → MockChain emits matching deposit → processor credits; create withdrawal → worker sends via CountingMockSigner) runs green on the mocks; `send_count(key) == 1` on that happy path.

---

## 6. Session 0.4 — One clean schema baseline (and the forward tables)

The migration history is broken: `2025-09-23-171556_deposits_schema` creates `deposits` with `tx_hash ... UNIQUE`, then `2025-09-27-132859_deposits` issues `CREATE TABLE IF NOT EXISTS deposits` with **different** defaults — a silent no-op that masks schema drift. This is pre-launch (devnet, no production data), so the right call is a **single collapsed baseline**, not incremental ALTERs preserving a broken history. (**Rejected:** schema-per-phase, adding `idempotency_keys`/`outbox` in Phase 1 — it scatters DDL across phases and leaves the migration set unreviewable as a whole. All schema lands here; Phase 1 writes **zero** DDL.)

Author one new baseline migration (`up.sql` + a real `down.sql`) that creates the full schema:

- All existing tables (`merchants`, `apps`, `orders`, `wallets`, `deposits`, `balances`, `withdrawals`, `audit_logs`) — one definition each, no `IF NOT EXISTS` masking.
- **Constraints the code already assumes (verify present):** `UNIQUE (deposits.tx_hash)`; `UNIQUE (balances.merchant_id, token_mint)`.
- **New natural-key constraint:** `UNIQUE (orders.app_id, order_id)` — the merchant-supplied `order_id` becomes the inbound idempotency natural key (Brief §3.1). Make `order_id` `NOT NULL` going forward.
- **`idempotency_keys`** with the exact Amendment 1 §A2 column set:
  `key TEXT PRIMARY KEY, request_fingerprint TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('in_progress','completed')), lease_deadline TIMESTAMPTZ, lease_owner UUID, response_snapshot JSONB, response_status SMALLINT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()` + partial index `(status, lease_deadline) WHERE status = 'in_progress'`.
- **`outbox`:** `id UUID PK DEFAULT gen_random_uuid(), topic TEXT NOT NULL, payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), sent_at TIMESTAMPTZ` + partial index `(created_at) WHERE sent_at IS NULL` (the relay scan).
- **`dead_letter`:** `id UUID PK DEFAULT gen_random_uuid(), source_stream TEXT NOT NULL, raw JSONB NOT NULL, reason TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()` (poison-message sink, Brief §3.7).
- Regenerate `store/src/schema.rs` from the baseline; update `module.rs` structs to match (add `IdempotencyKeyRow`, `OutboxRow`, `DeadLetter` model structs — types only, no query logic).

**Done when:** from an empty database `diesel migration run` applies cleanly and `diesel migration redo` round-trips; all four constraints exist; the three forward tables exist with the specified columns/indexes; `schema.rs` regenerated; workspace compiles against the new models. No query logic for the new tables yet.

---

## 7. Session 0.5 — Chaos fail-point scaffolding (compile-out)

Stand up the mechanism for Amendment 1 §A1. **No fire-sites in real transaction code yet** (those land in Phase 1 as each transaction is written) — only the macro, the registry, the abort mechanism, and a self-test.

```rust
// chaos_hooks/src/lib.rs
/// Closed, enumerable registry. APPEND-ONLY across phases. The Phase 2 harness iterates ALL.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum CrashPointId {
    SelfTest,                 // Phase 0 canary only
    // Phase 1 will append, e.g.:
    // ProcessorAfterDepositInsertBeforeCredit, ProcessorAfterCreditBeforeOrderPaid,
    // ProcessorAfterCommitBeforeXack, IdemAfterEffectBeforeComplete,
    // WorkerAfterSendBeforeXack, RelayAfterXaddBeforeMarkSent, ...
}
impl CrashPointId { pub const ALL: &'static [CrashPointId] = &[CrashPointId::SelfTest]; }

/// Compiles to NOTHING without the `chaos` feature — zero cost, zero risk in production builds.
#[macro_export]
macro_rules! crash_point {
    ($id:expr) => {{
        #[cfg(feature = "chaos")]
        { $crate::__maybe_fire($id); }
    }};
}

#[cfg(feature = "chaos")]
pub fn __maybe_fire(id: CrashPointId) {
    // Armed via env (e.g. COINGATE_CHAOS_FIRE=SelfTest) read once at startup into a static.
    // On match: model a real process death.
    if armed() == Some(id) { std::process::abort(); }
}
```

- The `chaos` feature is **off by default** in every crate's `Cargo.toml`; production/normal builds never compile `__maybe_fire`. (This realizes the directive's `#[cfg(chaos)]` intent via a cargo feature, which composes cleanly across the workspace and with `cargo test --features chaos`.)
- Self-test (gated `#[cfg(feature = "chaos")]`): a test that spawns a subprocess with `COINGATE_CHAOS_FIRE=SelfTest`, hits a `crash_point!(CrashPointId::SelfTest)` site, and asserts the subprocess aborts (non-zero exit). This proves the supervisor/abort model the Phase 2 harness will rely on.
- Document, in `chaos_hooks/README.md`, the **registry-closure rule**: every `CrashPointId` variant must have exactly one `crash_point!` fire-site by the end of Phase 1; Phase 2 asserts this.

**Done when:** normal `cargo build` contains zero chaos code (verify the macro expands to nothing without the feature); `cargo test --features chaos` runs the self-test and observes the modeled abort; `CrashPointId::ALL` exists for enumeration.

---

## 8. Phase 0 Definition of Done

1. `Arc<Mutex<Store>>` and the single-`PgConnection` `Store` are **deleted**; storage is an r2d2 pool; `with_tx` is the only transaction constructor and pins `READ COMMITTED` in code.
2. All config (DB, Redis, JWT secret, Solana RPC, listen addr) comes from env with fail-fast; **no** hardcoded secrets or URLs remain.
3. Zero `println!`/`eprintln!` in `api`/`poller`/`processor`/`worker`/`store`; logging is `tracing`.
4. Zero `f64` in any money path; amounts are exact base-unit integers / `BigDecimal`; the audit's `.unwrap()`/`.expect()` input sites are gone; a negative/fuzz test shows hostile input → `4xx`/dead-letter, **never** a panic or abort.
5. `external` crate exists: `Chain` (+`SolanaChain`,`MockChain`) and `Signer` (+`MpcSigner`,`CountingMockSigner`); `Signer::send` carries the withdrawal-id key and `Signer::lookup` exists for reconciliation; `poller`/`worker` run a happy-path e2e on the mocks with `send_count == 1`.
6. One collapsed migration baseline applies cleanly from an empty DB and round-trips on redo; `UNIQUE(deposits.tx_hash)`, `UNIQUE(balances.merchant_id,token_mint)`, `UNIQUE(orders.app_id,order_id)` all present; `idempotency_keys` (full §A2 columns), `outbox`, `dead_letter` tables exist; `schema.rs` regenerated; models compile.
7. `chaos_hooks` compiles to nothing without the `chaos` feature; with it, the self-test demonstrates a modeled process abort at an armed point; `CrashPointId::ALL` exists.
8. **Scope held:** no idempotency-key logic, no atomic-credit rewrite, no worker status-guard/reconciliation, no outbox relay, no harness. Those are Phase 1/2. The frozen contracts (§1.1) are in place.
9. `cargo build`, `cargo clippy` (workspace, warnings-as-errors acceptable), `cargo test`, and `cargo test --features chaos` all green; dependencies limited to the §2 allowlist.

This is the foundation phase. It contains no proof and no exactly-once guarantee — it makes both possible. Do not skip ahead.

---

# Appendix A — `CLAUDE.md` for Phase 0

```markdown
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
```

---

# Appendix B — Claude Code execution plan (5 sessions)

| # | Session | Deliverable | Done when |
|---|---|---|---|
| 0.1 | Pool + config + with_tx | r2d2 pool, `Config::from_env`, RC-pinned `with_tx`; delete the store mutex | API serves concurrently; `with_tx` is the only tx constructor; build green |
| 0.2 | Logging + de-panic + exact money | `tracing` everywhere; `ApiError`; remove input `.unwrap()`s; kill `f64`; bounded auth fix | hostile input → 4xx/dead-letter, never panic; no f64; no `println!` |
| 0.3 | `external` traits | `Chain`/`Signer` (+ real + mock); key in `Signer::send`; `lookup`; wire poller/worker | happy-path e2e on mocks; `send_count == 1` |
| 0.4 | Schema baseline | one collapsed migration; all constraints; `idempotency_keys`/`outbox`/`dead_letter`; regen `schema.rs` | clean apply + redo from empty DB; constraints/tables present |
| 0.5 | Chaos scaffolding | `chaos_hooks` crate: `crash_point!`, `CrashPointId`, abort, self-test (feature-gated) | no chaos code without the feature; self-test aborts; `ALL` exists |

Sessions are independent enough to run in order without splitting. 0.4 is mechanical but verify the round-trip (`redo`) — a baseline that cannot reverse is not done.

### Exact prompts (paste one per session; verify + commit before the next)

**Session 0.1**
> Read `CLAUDE.md`, `docs/specs/phase0-spec.md` §1–§3. Execute **Session 0.1 only**: replace `store`'s single-`PgConnection` `Store` and the `api` `Arc<Mutex<Store>>` with an r2d2 `Pool`; add `Config::from_env` (DB/Redis/JWT/Solana-RPC/listen-addr, fail-fast, no hardcoded values); add the `with_tx` helper pinned to READ COMMITTED as the single transaction entry point; convert store methods to take `&mut PgConnection`. Do not change any idempotency, credit, or send logic. Show me the new `pool.rs` API and the `api/src/main.rs` wiring before touching route handlers. End with build+clippy+test green, commit, STOP.

**Session 0.2**
> Read `CLAUDE.md` and `phase0-spec.md` §4. Execute **Session 0.2 only**: initialize `tracing` in every binary and replace all `println!`/`eprintln!`; add `ApiError` (+`ResponseError`) and make handlers return `Result`, removing every `.unwrap()`/`.expect()` on request/stream/RPC-derived data (use the §4 fix-list); ban `f64` on money (decimal-string → `BigDecimal` base units) and add the exactness unit test; apply the bounded `find_app_by_token` indexed-lookup fix. Add the negative/fuzz test proving hostile input never panics. Build+clippy+test green, commit, STOP.

**Session 0.3**
> Read `CLAUDE.md` and `phase0-spec.md` §5. Execute **Session 0.3 only**: create the `external` crate with `Chain` (+`SolanaChain` wrapping current logic, +`MockChain` scriptable) and `Signer` (+`MpcSigner` forwarding the key as `idempotency_key`, +`CountingMockSigner` that counts `send` and exposes `lookup`+`send_count`); `Signer::send` takes the withdrawal-id key. Wire `poller`/`worker` to the traits without fixing pagination or adding reconciliation yet. Add the happy-path e2e on mocks asserting `send_count == 1`. Build+clippy+test green, commit, STOP.

**Session 0.4**
> Read `CLAUDE.md` and `phase0-spec.md` §6. Execute **Session 0.4 only**: replace the conflicting migration history with one collapsed baseline (`up.sql`+`down.sql`) creating all tables once, with `UNIQUE(deposits.tx_hash)`, `UNIQUE(balances.merchant_id,token_mint)`, `UNIQUE(orders.app_id,order_id)` (order_id NOT NULL), plus `idempotency_keys` (full Amendment §A2 columns + partial index), `outbox` (+partial index), and `dead_letter`. Regenerate `schema.rs`; add the new model structs (types only). Verify clean apply + `migration redo` from an empty DB. No query logic for the new tables. Build green, commit, STOP.

**Session 0.5**
> Read `CLAUDE.md` and `phase0-spec.md` §7. Execute **Session 0.5 only**: create the `chaos_hooks` crate — `crash_point!` macro (compiles to nothing without the `chaos` feature), the append-only `CrashPointId` enum with `SelfTest` and `ALL`, `__maybe_fire` (env-armed, `std::process::abort()`), and a feature-gated self-test that spawns a subprocess, fires `SelfTest`, and asserts it aborts. Add a `README.md` stating the registry-closure rule. Verify `cargo build` contains no chaos code and `cargo test --features chaos` passes. Commit, STOP. Phase 0 complete — report the DoD §8 items.
