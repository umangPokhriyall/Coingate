-- Your SQL goes here
-- create deposits table (idempotent)
CREATE TABLE IF NOT EXISTS deposits (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id uuid NULL,
  tx_hash text NOT NULL,
  chain text NOT NULL DEFAULT 'solana',
  slot bigint NULL,
  block_hash text NULL,
  from_address text NULL,
  to_address text NULL,
  token_mint text NULL,
  token_symbol text NULL,
  token_decimals integer NULL,
  amount numeric NOT NULL,
  memo_id text NULL,
  status text NOT NULL DEFAULT 'pending', -- pending, verified, failed
  confirmations integer NULL,
  raw jsonb NULL,
  processed boolean NOT NULL DEFAULT false,
  processing_attempts integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  confirmed_at timestamptz NULL
);

-- unique tx hash to prevent double-insert
CREATE UNIQUE INDEX IF NOT EXISTS idx_deposits_tx_hash ON deposits (tx_hash);

-- index memo for fast lookup by memo_id
CREATE INDEX IF NOT EXISTS idx_deposits_memo_id ON deposits (memo_id);

-- index processed / status for worker queries
CREATE INDEX IF NOT EXISTS idx_deposits_processed ON deposits (processed);
CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits (status);

-- trigger to keep updated_at current
CREATE OR REPLACE FUNCTION deposits_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS deposits_set_updated_at ON deposits;
CREATE TRIGGER deposits_set_updated_at
BEFORE UPDATE ON deposits
FOR EACH ROW
EXECUTE PROCEDURE deposits_update_timestamp();
