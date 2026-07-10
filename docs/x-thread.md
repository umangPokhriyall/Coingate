# Distribution thread — Coingate (exactly-once core)

Findings-first. Every claim cites a committed file; no adjective a number hasn't earned; no ask.
Reviewed against the repo before posting.

---

**1/**
"Exactly-once delivery" is a lie you tell yourself. You get at-least-once delivery, and you make
processing idempotent, or you don't. I rebuilt a payment core to do it properly, then proved it: a
process crash at every statement boundary, under every redelivery schedule, 62/62 runs, 0 double
credits, 1 send per withdrawal. (`chaos/results/summary.md`)

**2/**
The original code got it wrong the way almost everyone does. The dedup check and the money-moving
effect it guards were in separate transactions. So a crash between them, or a redelivered message,
either credits twice or loses the credit. The unique index that could have saved it was there, and the
control flow walked right past it.

**3/**
The fix isn't a framework. It's moving the transaction boundary. Credit the balance and mark the order
paid in one transaction, gated on `INSERT INTO deposits ... ON CONFLICT (tx_hash) DO NOTHING
RETURNING`. A redelivery inserts nothing, so it credits nothing. The dedup decision and the effect
commit together or not at all. (`docs/DESIGN.md`)

**4/**
Inbound side: Stripe-style idempotency keys with the part everyone skips. The key has a lifecycle,
`in_progress(lease) → completed(snapshot)`. A replay while in-progress gets 409 + Retry-After, never a
double-execute. If the first attempt died, an expired lease lets another executor take over and run the
effect exactly once. The safety comes from atomicity, not the lease. (`docs/DESIGN.md`, takeover theorem)

**5/**
The signer is external and non-idempotent. You can't make `send money` atomic with your database, so
you put a deterministic key at the boundary (`withdrawal_id`) and the worker reconciles from the
ambiguous `processing` state instead of blindly re-sending. That's the only correct contract with an
external effect.

**6/**
The API-to-Redis dual-write is the other classic leak: commit the DB row, crash before the enqueue,
and the money is locked with no work item. Killed it with a transactional outbox. Domain row and outbox
row commit together; a relay drains the outbox to Redis and marks it sent. At-least-once on that hop is
a feature, and the consumer-side dedup absorbs the duplicate.

**7/**
The proof is the artifact. A chaos harness runs the real binaries as subprocesses, arms one crash point
at a time (`std::process::abort`, not a catchable panic), and enumerates every crash point × every
redelivery schedule deterministically: 14 seams × 4 schedules, plus a seeded interleaving sweep on top.
All at READ COMMITTED. (`chaos/results/summary.md`)

**8/**
The before/after is the honest part. The same harness against the pre-idempotency tag breaks where the
rebuild is clean, at the same isolation level: a concurrent redelivery credits 2000 for a 1000 deposit,
a crash-after-send re-sends (mock signer count = 2), a crash mid-withdraw strands a locked balance with
no work item. Row by row, legacy red, rebuilt green. (`chaos/results/before-after.md`)

**9/**
What I don't claim: the signer's idempotency is a contract I *assume*, modeled and counted by a mock,
not proven on-chain. And correctness is at READ COMMITTED because the unique index and
`SELECT ... FOR UPDATE` carry it, with no serializable dependency. Both stated in the writeup, because
the caveat is the credential.

**10/**
Same shape as a sandbox job-submission API: a client retries, the control plane redelivers, and you
need exactly one VM per job, not two. Build it on money, lift it to VMs.
github.com/umangPokhriyall/Coingate — `docs/DESIGN.md`
