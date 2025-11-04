-- This file should undo anything in `up.sql`
ALTER TABLE orders
    ALTER COLUMN receive_currency DROP DEFAULT;

-- No rollback needed for nullability (optional).
