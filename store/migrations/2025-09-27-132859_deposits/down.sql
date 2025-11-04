-- This file should undo anything in `up.sql`
DROP TRIGGER IF EXISTS deposits_set_updated_at ON deposits;
DROP FUNCTION IF EXISTS deposits_update_timestamp();

DROP INDEX IF EXISTS idx_deposits_status;
DROP INDEX IF EXISTS idx_deposits_processed;
DROP INDEX IF EXISTS idx_deposits_memo_id;
DROP INDEX IF EXISTS idx_deposits_tx_hash;

DROP TABLE IF EXISTS deposits;
