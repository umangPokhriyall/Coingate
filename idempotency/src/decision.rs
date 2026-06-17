//! The pure decision — the brain of the §A2 inbound-key protocol.
//!
//! [`decide`] takes the **existing** persisted record (the caller only consults it when
//! [`acquire`](crate::IdempotencyStore::acquire) returned [`Existing`](crate::Acquire::Existing))
//! and the current request's fingerprint, and returns the branch to take. It performs no I/O and
//! never returns [`Decision::Execute`]: a fresh acquire implies Execute at the call site, and a
//! won [`Decision::Takeover`] CAS is the other path into Execute.

use crate::store::{KeyRecord, KeyStatus};
use chrono::{DateTime, Duration, Utc};

/// Lease duration for an in-progress idempotency key (Amendment §A2). Tunable; documented here.
/// An `in_progress` key whose `lease_deadline` is at or before `now` is eligible for takeover.
pub const IDEMPOTENCY_LEASE_SECS: i64 = 30;

/// [`IDEMPOTENCY_LEASE_SECS`] as a [`chrono::Duration`]. (`chrono`'s constructors are not
/// `const`, so the lease is exposed as a function rather than a `Duration` constant.)
pub fn idempotency_lease() -> Duration {
    Duration::seconds(IDEMPOTENCY_LEASE_SECS)
}

/// The branch the Execute orchestration must take for an existing key.
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    /// We own the key (a fresh acquire or a won takeover) — run the effect. `decide` never
    /// *returns* this; it is the call site's conclusion. Present in the enum so the full set of
    /// outcomes is nameable.
    Execute,
    /// Completed and the fingerprint matches — return the stored snapshot verbatim.
    Replay { snapshot: serde_json::Value, status: i16 },
    /// Completed but the fingerprint differs (same key, different payload) — `409`.
    Conflict,
    /// In progress and the lease has NOT expired — `409` + `Retry-After: seconds`.
    RetryAfter { seconds: u64 },
    /// In progress and the lease HAS expired — the caller attempts the CAS takeover.
    Takeover,
}

/// Decide the branch for an existing key, given the current request `fingerprint` and the clock
/// `now`. Pure. See [`Decision`] for the meaning of each outcome.
pub fn decide(existing: &KeyRecord, fingerprint: &str, now: DateTime<Utc>) -> Decision {
    match existing.status {
        KeyStatus::Completed => {
            if existing.request_fingerprint == fingerprint {
                Decision::Replay {
                    // A completed record is expected to carry both; default defensively rather
                    // than panic on a malformed row (Phase 0 rule: no unwrap on data).
                    snapshot: existing
                        .response_snapshot
                        .clone()
                        .unwrap_or(serde_json::Value::Null),
                    status: existing.response_status.unwrap_or(200),
                }
            } else {
                Decision::Conflict
            }
        }
        KeyStatus::InProgress => match existing.lease_deadline {
            // Lease still in the future → the original request is presumed live; tell the caller
            // to retry after the remaining lease.
            Some(deadline) if deadline > now => {
                let seconds = (deadline - now).num_seconds().max(0) as u64;
                Decision::RetryAfter { seconds }
            }
            // Expired lease, or a malformed in-progress row with no deadline → eligible for
            // takeover so a stalled key cannot wedge the protocol forever.
            _ => Decision::Takeover,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed(fingerprint: &str) -> KeyRecord {
        KeyRecord {
            status: KeyStatus::Completed,
            request_fingerprint: fingerprint.to_string(),
            lease_deadline: None,
            lease_owner: Some(uuid::Uuid::new_v4()),
            response_snapshot: Some(serde_json::json!({"id": "order_1"})),
            response_status: Some(201),
        }
    }

    fn in_progress(lease_deadline: Option<DateTime<Utc>>) -> KeyRecord {
        KeyRecord {
            status: KeyStatus::InProgress,
            request_fingerprint: "fp".to_string(),
            lease_deadline,
            lease_owner: Some(uuid::Uuid::new_v4()),
            response_snapshot: None,
            response_status: None,
        }
    }

    #[test]
    fn completed_with_matching_fingerprint_replays() {
        let rec = completed("fp");
        match decide(&rec, "fp", Utc::now()) {
            Decision::Replay { snapshot, status } => {
                assert_eq!(snapshot, serde_json::json!({"id": "order_1"}));
                assert_eq!(status, 201);
            }
            other => panic!("expected Replay, got {other:?}"),
        }
    }

    #[test]
    fn completed_with_mismatched_fingerprint_conflicts() {
        let rec = completed("fp_original");
        assert_eq!(decide(&rec, "fp_different", Utc::now()), Decision::Conflict);
    }

    #[test]
    fn completed_without_snapshot_defaults_rather_than_panicking() {
        let mut rec = completed("fp");
        rec.response_snapshot = None;
        rec.response_status = None;
        match decide(&rec, "fp", Utc::now()) {
            Decision::Replay { snapshot, status } => {
                assert_eq!(snapshot, serde_json::Value::Null);
                assert_eq!(status, 200);
            }
            other => panic!("expected Replay, got {other:?}"),
        }
    }

    #[test]
    fn in_progress_with_valid_lease_says_retry_after_remaining() {
        let now = Utc::now();
        let rec = in_progress(Some(now + Duration::seconds(20)));
        match decide(&rec, "fp", now) {
            Decision::RetryAfter { seconds } => assert_eq!(seconds, 20),
            other => panic!("expected RetryAfter, got {other:?}"),
        }
    }

    #[test]
    fn retry_after_rounds_down_partial_seconds() {
        let now = Utc::now();
        // 9.5s remaining → 9 whole seconds.
        let rec = in_progress(Some(now + Duration::milliseconds(9_500)));
        match decide(&rec, "fp", now) {
            Decision::RetryAfter { seconds } => assert_eq!(seconds, 9),
            other => panic!("expected RetryAfter, got {other:?}"),
        }
    }

    #[test]
    fn in_progress_with_expired_lease_says_takeover() {
        let now = Utc::now();
        let rec = in_progress(Some(now - Duration::seconds(1)));
        assert_eq!(decide(&rec, "fp", now), Decision::Takeover);
    }

    #[test]
    fn in_progress_at_exact_deadline_says_takeover() {
        // Boundary: deadline == now is NOT "in the future", so the key is takeover-eligible.
        let now = Utc::now();
        let rec = in_progress(Some(now));
        assert_eq!(decide(&rec, "fp", now), Decision::Takeover);
    }

    #[test]
    fn in_progress_without_deadline_says_takeover() {
        let rec = in_progress(None);
        assert_eq!(decide(&rec, "fp", Utc::now()), Decision::Takeover);
    }

    #[test]
    fn decide_never_returns_execute() {
        // `decide` is only consulted for an existing record; Execute is the caller's conclusion.
        let now = Utc::now();
        for rec in [
            completed("fp"),
            completed("other"),
            in_progress(Some(now + Duration::seconds(5))),
            in_progress(Some(now - Duration::seconds(5))),
            in_progress(None),
        ] {
            assert_ne!(decide(&rec, "fp", now), Decision::Execute);
        }
    }

    #[test]
    fn lease_constant_is_thirty_seconds() {
        assert_eq!(IDEMPOTENCY_LEASE_SECS, 30);
        assert_eq!(idempotency_lease(), Duration::seconds(30));
    }
}
