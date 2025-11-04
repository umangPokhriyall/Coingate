-- This file should undo anything in `up.sql`
ALTER TABLE orders
    DROP COLUMN selected_mint,
    DROP COLUMN expected_amount,
    DROP COLUMN expected_decimals;
