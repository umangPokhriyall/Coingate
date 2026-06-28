//! The redelivery / concurrent-delivery schedule set (Phase 2 §4.2) — the axis orthogonal to the
//! crash points. The crash-point axis is exhaustive; this axis is the duplicate/concurrent
//! delivery patterns each crash point is crossed with.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Schedule {
    /// One delivery, no duplication.
    Single,
    /// The same logical event enqueued twice (poller / relay double-publish).
    DuplicateStream,
    /// Two instances of the consuming service in one group, with a duplicate so the entries can
    /// land on different consumers (the two-workers / XAUTOCLAIM-steal case).
    ConcurrentConsumers,
    /// The armed crash leaves the entry pending; redelivery re-drives the same logical event
    /// (modeling the reclaimed PEL entry).
    RestartRedelivery,
}

impl Schedule {
    pub const ALL: &'static [Schedule] = &[
        Schedule::Single,
        Schedule::DuplicateStream,
        Schedule::ConcurrentConsumers,
        Schedule::RestartRedelivery,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Schedule::Single => "Single",
            Schedule::DuplicateStream => "DuplicateStream",
            Schedule::ConcurrentConsumers => "ConcurrentConsumers",
            Schedule::RestartRedelivery => "RestartRedelivery",
        }
    }

    /// How many redelivery copies of the logical event to drive (after the armed crash).
    pub fn redeliveries(self) -> usize {
        match self {
            Schedule::DuplicateStream => 2,
            _ => 1,
        }
    }

    /// How many instances of the consuming service to run (one armed, the rest disarmed).
    pub fn consumers(self) -> usize {
        match self {
            Schedule::ConcurrentConsumers => 2,
            _ => 1,
        }
    }
}
