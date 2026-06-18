//! Manual reconciler run (Phase 1 §8). Recomputes the drift oracle over the live DB and reports.
//! Exits non-zero if any drift is found, so it doubles as a CI/ops gate. Phase 2 (Amendment §A5)
//! runs the same `store::reconcile` after every fault schedule as Invariant #5.
//!
//! Usage: `DATABASE_URL=... cargo run -p reconciler` (reads all required env via `store::Config`).

use anyhow::Result;
use chrono::Utc;
use store::{Config, Deadlines, build_pool, get_conn, reconcile};
use tracing::{error, info, warn};

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration (check required env vars)");
    let pool = build_pool(&cfg).expect("failed to build database pool");
    let mut conn = get_conn(&pool).expect("failed to get DB connection");

    let report = reconcile(&mut conn, Utc::now(), Deadlines::default())?;

    if report.is_clean() {
        info!("reconciler: CLEAN — no drift detected");
        return Ok(());
    }

    error!("reconciler: DRIFT DETECTED");
    for imb in &report.credit_conservation {
        warn!(
            merchant_id = %imb.merchant_id,
            token = %imb.token_mint,
            confirmed_deposits = %imb.confirmed_deposits,
            accounted = %imb.accounted,
            "credit conservation violated"
        );
    }
    for id in &report.stuck_processing {
        warn!(withdrawal_id = %id, "withdrawal stuck in processing past deadline");
    }
    for (merchant_id, token) in &report.orphan_locks {
        warn!(%merchant_id, %token, "orphan lock: locked balance with no active withdrawal");
    }
    for id in &report.unsent_aged_outbox {
        warn!(outbox_id = %id, "outbox row unsent past threshold (relay liveness)");
    }

    std::process::exit(1);
}
