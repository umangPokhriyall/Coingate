-- Your SQL goes here
ALTER TABLE orders
    ADD COLUMN selected_mint TEXT NULL,
    ADD COLUMN expected_amount NUMERIC NULL,
    ADD COLUMN expected_decimals INT NULL;
