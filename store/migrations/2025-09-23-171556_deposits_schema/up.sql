-- Your SQL goes here
-- Apps table must have app_token hash
ALTER TABLE apps ADD COLUMN IF NOT EXISTS token_hash TEXT UNIQUE;

-- Orders: add app_id link and memo_id
ALTER TABLE orders ADD COLUMN IF NOT EXISTS app_id UUID REFERENCES apps(id);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS memo_id TEXT UNIQUE;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS tx_hash TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ;

-- Wallets
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

-- Deposits
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

CREATE INDEX idx_deposits_memo_id ON deposits(memo_id);
CREATE INDEX idx_deposits_to_address ON deposits(to_address);
CREATE INDEX idx_deposits_status ON deposits(status);
