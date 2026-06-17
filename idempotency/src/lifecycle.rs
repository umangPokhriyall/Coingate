//! The two domain state machines and their legal-transition guards.
//!
//! These are pure predicates over the *current* status string. `store` calls them to gate
//! every state transition; the §A2 idempotency pattern leans on them so that a guarded effect
//! (a balance move, a mark-paid) fires only when the guarded transition actually fires.
//!
//! ```text
//! Order:       pending ──► paid               (paid is terminal; paid→paid is a no-op, not an error)
//! Withdrawal:  pending ──► processing ──► completed
//!                                    └────► failed
//!                          (finalize_* are legal ONLY from `processing`; all others are no-ops)
//! ```

/// `true` iff an order in `current` may be marked `paid`. Marking an already-`paid` order is a
/// no-op (not an error), so this returns `false` only for `paid`.
pub fn order_can_mark_paid(current: &str) -> bool {
    current != "paid"
}

/// `true` iff a withdrawal in `current` may be finalized (to `completed` or `failed`). Finalize
/// is legal *only* from `processing`; from any terminal or earlier state it is a no-op.
pub fn withdrawal_can_finalize(current: &str) -> bool {
    current == "processing"
}

/// The legal successor states of a withdrawal in `current`. Empty for terminal (`completed`,
/// `failed`) and unknown states. Used by tests/assertions and by callers that need to validate
/// a proposed transition.
pub fn withdrawal_next(current: &str) -> &'static [&'static str] {
    match current {
        "pending" => &["processing"],
        "processing" => &["completed", "failed"],
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_can_mark_paid_from_pending() {
        assert!(order_can_mark_paid("pending"));
    }

    #[test]
    fn order_mark_paid_is_a_noop_when_already_paid() {
        assert!(!order_can_mark_paid("paid"));
    }

    #[test]
    fn order_can_mark_paid_tolerates_unknown_states() {
        // An unexpected status is not `paid`, so marking is permitted (the SQL guard
        // `WHERE status <> 'paid'` is the real backstop).
        assert!(order_can_mark_paid("expired"));
    }

    #[test]
    fn withdrawal_finalize_only_from_processing() {
        assert!(withdrawal_can_finalize("processing"));
        assert!(!withdrawal_can_finalize("pending"));
        assert!(!withdrawal_can_finalize("completed"));
        assert!(!withdrawal_can_finalize("failed"));
        assert!(!withdrawal_can_finalize("bogus"));
    }

    #[test]
    fn withdrawal_next_enumerates_legal_successors() {
        assert_eq!(withdrawal_next("pending"), &["processing"]);
        assert_eq!(withdrawal_next("processing"), &["completed", "failed"]);
        assert!(withdrawal_next("completed").is_empty());
        assert!(withdrawal_next("failed").is_empty());
        assert!(withdrawal_next("unknown").is_empty());
    }

    #[test]
    fn finalize_guard_agrees_with_successor_table() {
        // `withdrawal_can_finalize` is true exactly when there are terminal successors to move to.
        for state in ["pending", "processing", "completed", "failed", "weird"] {
            let has_terminal_successor = withdrawal_next(state)
                .iter()
                .any(|s| *s == "completed" || *s == "failed");
            assert_eq!(withdrawal_can_finalize(state), has_terminal_successor);
        }
    }
}
