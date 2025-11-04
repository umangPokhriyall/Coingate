-- Your SQL goes here
-- ============ Balances ============
create table balances (
    id uuid primary key default gen_random_uuid(),
    merchant_id uuid not null references merchants(id) on delete cascade,
    token_mint text not null,
    balance numeric default 0,
    locked_balance numeric default 0,
    updated_at timestamp default now()
);

create unique index balances_merchant_token_idx
on balances (merchant_id, token_mint);

-- ============ Withdrawals ============
create table withdrawals (
    id uuid primary key default gen_random_uuid(),
    merchant_id uuid not null references merchants(id) on delete cascade,
    token_mint text not null,
    amount numeric not null,
    status text not null default 'pending', -- pending, processing, completed, failed
    target_address text not null,
    tx_hash text,
    created_at timestamp default now(),
    updated_at timestamp default now()
);
