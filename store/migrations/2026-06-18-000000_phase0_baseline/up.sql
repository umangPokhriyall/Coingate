-- Phase 0 collapsed baseline.
--
-- Replaces the prior conflicting migration history (the two divergent `deposits`
-- definitions) with a single reviewable schema, and lands the forward tables Phase 1 will
-- fill (idempotency_keys, outbox, dead_letter). Phase 1 writes ZERO DDL.

-- gen_random_uuid(): built in on PG13+, in pgcrypto otherwise. Keep the baseline self-contained.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============ merchants ============
CREATE TABLE merchants (
    id UUID PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ============ apps ============
CREATE TABLE apps (
    id UUID PRIMARY KEY,
    merchant_id UUID REFERENCES merchants(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    callback_url TEXT,
    token_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ============ orders ============
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    app_id UUID REFERENCES apps(id) ON DELETE CASCADE,
    order_id TEXT NOT NULL,
    price_amount NUMERIC NOT NULL,
    price_currency TEXT NOT NULL,
    receive_currency TEXT NOT NULL DEFAULT 'USDC',
    memo_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'cancelled')),
    tx_hash TEXT,
    callback_url TEXT,
    success_url TEXT,
    cancel_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    selected_mint TEXT,
    expected_amount NUMERIC,
    expected_decimals INT,
    -- the merchant-supplied order_id is the inbound idempotency natural key (Brief §3.1)
    CONSTRAINT orders_app_order_unique UNIQUE (app_id, order_id)
);

-- ============ audit_logs ============
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY,
    entity TEXT NOT NULL,
    entity_id UUID NOT NULL,
    action TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ============ wallets ============
CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    name TEXT,
    owner_type TEXT,
    owner_id UUID,
    chain TEXT NOT NULL,
    address TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ============ deposits ============
CREATE TABLE deposits (
    id UUID PRIMARY KEY,
    order_id UUID REFERENCES orders(id) ON DELETE SET NULL,
    tx_hash TEXT NOT NULL UNIQUE,
    chain TEXT NOT NULL,
    slot BIGINT,
    block_hash TEXT,
    from_address TEXT,
    to_address TEXT,
    token_mint TEXT,
    token_symbol TEXT,
    token_decimals INT,
    amount NUMERIC NOT NULL,
    memo_id TEXT,
    status TEXT NOT NULL DEFAULT 'unconfirmed',
    confirmations INT DEFAULT 0,
    raw JSONB,
    processed BOOLEAN DEFAULT FALSE,
    processing_attempts INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    confirmed_at TIMESTAMPTZ
);
CREATE INDEX idx_deposits_memo_id ON deposits (memo_id);
CREATE INDEX idx_deposits_to_address ON deposits (to_address);
CREATE INDEX idx_deposits_status ON deposits (status);

-- ============ balances ============
CREATE TABLE balances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id UUID NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
    token_mint TEXT NOT NULL,
    balance NUMERIC DEFAULT 0,
    locked_balance NUMERIC DEFAULT 0,
    updated_at TIMESTAMP DEFAULT now(),
    CONSTRAINT balances_merchant_token_unique UNIQUE (merchant_id, token_mint)
);

-- ============ withdrawals ============
CREATE TABLE withdrawals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id UUID NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
    token_mint TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    target_address TEXT NOT NULL,
    tx_hash TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

-- ============ idempotency_keys (Amendment 1 §A2 — forward table, no query logic yet) ============
CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('in_progress', 'completed')),
    lease_deadline TIMESTAMPTZ,
    lease_owner UUID,
    response_snapshot JSONB,
    response_status SMALLINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- partial index for the in-progress lease scan
CREATE INDEX idempotency_keys_in_progress_idx
    ON idempotency_keys (status, lease_deadline)
    WHERE status = 'in_progress';

-- ============ outbox (forward table) ============
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);
-- partial index for the relay scan (unsent rows only)
CREATE INDEX outbox_unsent_idx ON outbox (created_at) WHERE sent_at IS NULL;

-- ============ dead_letter (poison-message sink, Brief §3.7 — forward table) ============
CREATE TABLE dead_letter (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_stream TEXT NOT NULL,
    raw JSONB NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
