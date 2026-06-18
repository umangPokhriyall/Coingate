use std::env;

/// Process configuration, sourced entirely from the environment. No secret or URL has a
/// hardcoded default: a missing variable is a hard, fail-fast error at startup.
#[derive(Debug, Clone)]
pub struct Config {
    pub db_url: String,
    pub redis_url: String,
    pub jwt_secret: String,
    pub solana_rpc_url: String,
    pub listen_addr: String,
    /// Base URL of the MPC signing service, e.g. `http://127.0.0.1:3000`. The worker builds the
    /// per-wallet send URL from this (was a hardcoded literal in the worker — Phase 1 §6).
    pub mpc_base_url: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    Missing(&'static str),
}

impl Config {
    /// Read every value from the environment, failing fast if any is absent.
    /// Loads a `.env` file first if one is present (best-effort, never required).
    pub fn from_env() -> Result<Config, ConfigError> {
        dotenv::dotenv().ok();

        Ok(Config {
            db_url: required("DATABASE_URL")?,
            redis_url: required("REDIS_URL")?,
            jwt_secret: required("JWT_SECRET")?,
            solana_rpc_url: required("SOLANA_RPC_URL")?,
            listen_addr: required("LISTEN_ADDR")?,
            mpc_base_url: required("MPC_BASE_URL")?,
        })
    }
}

fn required(key: &'static str) -> Result<String, ConfigError> {
    env::var(key).map_err(|_| ConfigError::Missing(key))
}
