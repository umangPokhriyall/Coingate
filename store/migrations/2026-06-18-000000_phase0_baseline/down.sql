-- Reverse of up.sql: drop every table (indexes drop with their tables), in an order that
-- respects foreign keys. The pgcrypto extension is intentionally left in place.
DROP TABLE IF EXISTS dead_letter;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS withdrawals;
DROP TABLE IF EXISTS balances;
DROP TABLE IF EXISTS deposits;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS audit_logs;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS apps;
DROP TABLE IF EXISTS merchants;
