# Before / After — legacy `pre-idempotency` vs rebuilt `main`

The identical black-box harness (direct Postgres + the reconciler + mock-mpc counts) is pointed at the instrumented `pre-idempotency-chaos` legacy binaries and at `main`. The legacy seams break where the rebuilt code is clean — at the same `READ COMMITTED` isolation level.

**§A4 counterexample (the headline):** conservation: merchant=e1771ac8-d6c4-4c8c-8e6f-5914dbef4e61 token=MINT-HARNESS-USDC deposits=1000 accounted=2000.

Rebuilt evidence: `chaos/results/sweep-main.jsonl` (62/62 runs clean, Session 2.1). Legacy evidence: `chaos/results/sweep-pre-idempotency.jsonl`.

| Crash point / anomaly | Mechanism | Legacy (`pre-idempotency`) | Rebuilt (`main`) |
|---|---|---|---|
| `IdemAfterAcquireBeforeExecute` | inbound idempotency key | mechanism absent in legacy (no Idempotency-Key / Execute spine) | clean ✓ |
| `IdemAfterEffectBeforeComplete` | inbound idempotency key | mechanism absent in legacy | clean ✓ |
| `IdemAfterCompleteBeforeCommit` | inbound idempotency key | mechanism absent in legacy | clean ✓ |
| `ProcAfterDepositInsertBeforeCredit` | atomic credit | conservation: merchant=d47ec79f-a435-4ab5-bc54-73b7350fc5af token=MINT-HARNESS-USDC deposits=1000 accounted=0 | clean ✓ |
| `ProcAfterCreditBeforeOrderPaid` | atomic credit | clean — legacy `status='paid'` check dedups the post-credit redelivery | clean ✓ |
| `ProcAfterOrderPaidBeforeCommit` | atomic credit | order marked paid before deposit/credit — a crash leaves a `paid` order with no deposit (silent, non-conserving on the order) | clean ✓ |
| `ProcAfterCommitBeforeXack` | atomic credit | clean — `status='paid'` check dedups the redelivery | clean ✓ |
| `ConcurrentCredit (§A4)` | atomic credit @ READ COMMITTED | conservation: merchant=e1771ac8-d6c4-4c8c-8e6f-5914dbef4e61 token=MINT-HARNESS-USDC deposits=1000 accounted=2000 | clean ✓ |
| `WorkerAfterStatusProcessingBeforeSend` | effect-boundary key | clean — crash precedes the send, so the redelivery sends once | clean ✓ |
| `WorkerAfterSendBeforeFinalize` | effect-boundary key | blind re-send: mock-mpc /__counts for the withdrawal = 2 (want 1) | clean ✓ |
| `WorkerAfterFinalizeBeforeXack` | effect-boundary key | blind re-send: legacy overwrites `completed`→`processing` and re-sends (no terminal guard) | clean ✓ |
| `WithdrawAfterLockBeforeOutbox` | transactional outbox | stranded: 1 pending withdrawal(s), locked=100, stream empty (no work item) | clean ✓ |
| `WithdrawAfterOutboxBeforeComplete` | transactional outbox | mechanism absent in legacy (no outbox / Execute spine) | clean ✓ |
| `RelayAfterReadBeforeXadd` | outbox relay | mechanism absent in legacy (no relay / outbox) | clean ✓ |
| `RelayAfterXaddBeforeMarkSent` | outbox relay | mechanism absent in legacy (no relay / outbox) | clean ✓ |
| `InboundKey replay (anomaly)` | inbound idempotency key | double-lock: 2 withdrawals + locked=200 for one logical request | clean ✓ |

