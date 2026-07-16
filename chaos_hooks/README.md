# chaos_hooks

Compile-out fail-point scaffolding for the exactly-once verification harness.

A `crash_point!(CrashPointId::X)` site models a real process death (`std::process::abort()`)
at a precise point in a transaction. It is armed at runtime by setting
`COINGATE_CHAOS_FIRE=<name>` (e.g. `COINGATE_CHAOS_FIRE=SelfTest`), read once into a
process-wide static.

## Zero-cost by default

Without the `chaos` cargo feature, `crash_point!` expands to **nothing** and none of the
firing machinery (`__maybe_fire`, `armed`) is compiled. Production and normal builds contain
zero chaos code. The feature is off by default in every crate.

- Normal build/test: `cargo build`, `cargo test`
- Chaos build/test: `cargo test -p chaos_hooks --features chaos`

## The registry-closure rule

`CrashPointId` is a **closed, append-only** enum:

- Variants are only ever **added** — never removed or renumbered — so a crash point's identity
  is stable across phases.
- Every `CrashPointId` variant MUST have **exactly one**
  `crash_point!` fire-site in real code. (`SelfTest` is the exception: its only
  fire-site is the self-test.)
- **The harness asserts this closure** and iterates `CrashPointId::ALL` to drive the crash
  schedule across every point.

When you add a variant, you are promising a matching fire-site in real code.
