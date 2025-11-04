-- This file should undo anything in `up.sql`
ALTER TABLE apps DROP COLUMN IF EXISTS token_hash;
ALTER TABLE orders DROP COLUMN IF EXISTS app_id;
ALTER TABLE orders DROP COLUMN IF EXISTS memo_id;
ALTER TABLE orders DROP COLUMN IF EXISTS tx_hash;
ALTER TABLE orders DROP COLUMN IF EXISTS status;
ALTER TABLE orders DROP COLUMN IF EXISTS confirmed_at;
DROP TABLE IF EXISTS deposits;
DROP TABLE IF EXISTS wallets;
