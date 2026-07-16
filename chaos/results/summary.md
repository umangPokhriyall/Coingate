# Exhaustive crash-point sweep (main)

**Headline:** a process crash at every statement boundary in the pipeline, under every redelivery schedule — **0 conservation violations, 0 re-sends** across 62 runs.

- Runs: **62/62 passed** (all five invariants held).
- Isolation: **READ COMMITTED** (recorded in every record).
- Registry closure: **14/14 crash points aborted** under their driving workload (every variant has a live, reachable fire-site).

## Per-crash-point pass grid

| Crash point | Single | DuplicateStream | ConcurrentConsumers | RestartRedelivery |
|---|---|---|---|---|
| `IdemAfterAcquireBeforeExecute` | ✓ | ✓ | ✓ | ✓ |
| `IdemAfterCompleteBeforeCommit` | ✓ | ✓ | ✓ | ✓ |
| `IdemAfterEffectBeforeComplete` | ✓ | ✓ | ✓ | ✓ |
| `ProcAfterCommitBeforeXack` | ✓ | ✓ | ✓ | ✓ |
| `ProcAfterCreditBeforeOrderPaid` | ✓ | ✓ | ✓ | ✓ |
| `ProcAfterDepositInsertBeforeCredit` | ✓ | ✓ | ✓ | ✓ |
| `ProcAfterOrderPaidBeforeCommit` | ✓ | ✓ | ✓ | ✓ |
| `RelayAfterReadBeforeXadd` | ✓ | ✓ | ✓ | ✓ |
| `RelayAfterXaddBeforeMarkSent` | ✓ | ✓ | ✓ | ✓ |
| `WithdrawAfterLockBeforeOutbox` | ✓ | ✓ | ✓ | ✓ |
| `WithdrawAfterOutboxBeforeComplete` | ✓ | ✓ | ✓ | ✓ |
| `WorkerAfterFinalizeBeforeXack` | ✓ | ✓ | ✓ | ✓ |
| `WorkerAfterSendBeforeFinalize` | ✓ | ✓ | ✓ | ✓ |
| `WorkerAfterStatusProcessingBeforeSend` | ✓ | ✓ | ✓ | ✓ |

## Scripted schedules (§A2 / §A3) + seeded sweep

| Name | Crash point | Passed | Note |
|---|---|---|---|
| `A2-takeover` | `IdemAfterEffectBeforeComplete` | ✓ | 409 before expiry; exactly-once after |
| `A2-takeover` | `IdemAfterAcquireBeforeExecute` | ✓ | 409 before expiry; exactly-once after |
| `A3-relay-republish` | `RelayAfterXaddBeforeMarkSent` | ✓ | relay republishes; worker absorbs duplicate (one send) |
| `seed-1` | `(none)` | ✓ | concurrent mixed workload |
| `seed-2` | `(none)` | ✓ | concurrent mixed workload |
| `seed-3` | `(none)` | ✓ | concurrent mixed workload |
