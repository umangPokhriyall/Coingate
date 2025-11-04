-- Your SQL goes here
-- Orders table: ensure settlement token is explicitly USDC
ALTER TABLE orders
    ALTER COLUMN receive_currency SET DEFAULT 'USDC';

-- Add clarity: selected_mint and expected fields already exist,
-- so just make sure they're nullable (for SOL payments).
ALTER TABLE orders
    ALTER COLUMN selected_mint DROP NOT NULL,
    ALTER COLUMN expected_amount DROP NOT NULL,
    ALTER COLUMN expected_decimals DROP NOT NULL;
