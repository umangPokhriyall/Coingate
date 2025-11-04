use crate::config::Config;
use diesel::prelude::*;
pub struct Store {
    pub conn: PgConnection,
}

impl Store {
    pub fn new() -> Result<Self, ConnectionError> {
        let config: Config = Config::default();

        let conn: PgConnection = PgConnection::establish(&config.db_url)?;
        Ok(Self { conn })
    }
}
