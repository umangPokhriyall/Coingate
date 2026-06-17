use anyhow::Result;
use external::{Chain, Cursor, SolanaChain};
use redis::Commands;
use store::{Config, build_pool, get_conn};
use tokio::time::{Duration, sleep};
use tracing::{debug, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration (check required env vars)");

    // Connect Redis
    let redis_client = redis::Client::open(cfg.redis_url.clone())?;
    let mut redis_conn = redis_client.get_connection()?;

    // Fetch the fat wallet address from the DB (pooled, one-shot).
    let pool = build_pool(&cfg).expect("failed to build database pool");
    let mut db_conn = get_conn(&pool).expect("failed to get DB connection");
    let fat_wallet = store::get_fat_wallet(&mut db_conn).expect("fat wallet not configured");
    drop(db_conn);

    // The chain is now behind the trait; SolanaChain wraps the RPC + parse pipeline.
    let chain = SolanaChain::new(cfg.solana_rpc_url.clone(), fat_wallet.address.clone())
        .expect("invalid fat wallet address");
    info!(wallet = %fat_wallet.address, "polling for wallet");

    // Bootstrap the checkpoint from Redis, or from the latest signature if none exists.
    let mut cursor: Option<Cursor> = redis_conn
        .get::<_, Option<String>>("poller:last_sig")
        .ok()
        .flatten()
        .map(|signature| Cursor { signature });

    if cursor.is_none() {
        info!("no checkpoint found, using latest transaction as starting point");
        cursor = chain.latest_cursor().await?;
        if let Some(c) = &cursor {
            let _: () = redis_conn.set("poller:last_sig", &c.signature)?;
            info!(checkpoint = %c.signature, "set initial checkpoint");
        }
    }

    loop {
        // Pagination is intentionally unchanged here (Phase 1.5 owns the gap-free backfill).
        let (events, new_cursor) = chain.deposits_since(cursor.clone()).await?;

        if events.is_empty() && new_cursor.is_none() {
            debug!("no new transactions, sleeping");
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!(count = events.len(), "found new deposit events");

        for ev in &events {
            // Same stream field layout the processor already consumes.
            let _: String = redis_conn.xadd(
                "payment_transactions",
                "*",
                &[
                    ("signature", ev.signature.as_str()),
                    ("memo_id", ev.memo_id.as_deref().unwrap_or("")),
                    ("transaction_type", ev.kind.as_str()),
                    ("from_address", ev.from.as_deref().unwrap_or("")),
                    ("to_address", ev.to.as_deref().unwrap_or("")),
                    ("amount", &ev.amount.unwrap_or(0).to_string()),
                    ("token_mint", ev.token_mint.as_deref().unwrap_or("")),
                    (
                        "token_decimals",
                        &ev.token_decimals.unwrap_or(0).to_string(),
                    ),
                    ("status", "SUCCESS"),
                ],
            )?;
            info!(signature = %ev.signature, "added deposit to redis stream");
        }

        // Advance the checkpoint to the newest signature seen this round.
        if let Some(c) = new_cursor {
            let _: () = redis_conn.set("poller:last_sig", &c.signature)?;
            cursor = Some(c);
        }

        sleep(Duration::from_secs(3)).await;
    }
}
