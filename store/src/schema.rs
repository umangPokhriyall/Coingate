// @generated automatically by Diesel CLI.

diesel::table! {
    apps (id) {
        id -> Uuid,
        merchant_id -> Nullable<Uuid>,
        title -> Text,
        callback_url -> Nullable<Text>,
        token_hash -> Text,
        created_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    audit_logs (id) {
        id -> Uuid,
        entity -> Text,
        entity_id -> Uuid,
        action -> Text,
        payload -> Nullable<Jsonb>,
        created_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    balances (id) {
        id -> Uuid,
        merchant_id -> Uuid,
        token_mint -> Text,
        balance -> Nullable<Numeric>,
        locked_balance -> Nullable<Numeric>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    dead_letter (id) {
        id -> Uuid,
        source_stream -> Text,
        raw -> Jsonb,
        reason -> Text,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    deposits (id) {
        id -> Uuid,
        order_id -> Nullable<Uuid>,
        tx_hash -> Text,
        chain -> Text,
        slot -> Nullable<Int8>,
        block_hash -> Nullable<Text>,
        from_address -> Nullable<Text>,
        to_address -> Nullable<Text>,
        token_mint -> Nullable<Text>,
        token_symbol -> Nullable<Text>,
        token_decimals -> Nullable<Int4>,
        amount -> Numeric,
        memo_id -> Nullable<Text>,
        status -> Text,
        confirmations -> Nullable<Int4>,
        raw -> Nullable<Jsonb>,
        processed -> Nullable<Bool>,
        processing_attempts -> Nullable<Int4>,
        created_at -> Nullable<Timestamptz>,
        updated_at -> Nullable<Timestamptz>,
        confirmed_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    idempotency_keys (key) {
        key -> Text,
        request_fingerprint -> Text,
        status -> Text,
        lease_deadline -> Nullable<Timestamptz>,
        lease_owner -> Nullable<Uuid>,
        response_snapshot -> Nullable<Jsonb>,
        response_status -> Nullable<Int2>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    merchants (id) {
        id -> Uuid,
        email -> Text,
        password_hash -> Text,
        name -> Text,
        created_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    orders (id) {
        id -> Uuid,
        app_id -> Nullable<Uuid>,
        order_id -> Text,
        price_amount -> Numeric,
        price_currency -> Text,
        receive_currency -> Text,
        memo_id -> Text,
        status -> Text,
        tx_hash -> Nullable<Text>,
        callback_url -> Nullable<Text>,
        success_url -> Nullable<Text>,
        cancel_url -> Nullable<Text>,
        created_at -> Nullable<Timestamptz>,
        confirmed_at -> Nullable<Timestamptz>,
        selected_mint -> Nullable<Text>,
        expected_amount -> Nullable<Numeric>,
        expected_decimals -> Nullable<Int4>,
    }
}

diesel::table! {
    outbox (id) {
        id -> Uuid,
        topic -> Text,
        payload -> Jsonb,
        created_at -> Timestamptz,
        sent_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    wallets (id) {
        id -> Uuid,
        name -> Nullable<Text>,
        owner_type -> Nullable<Text>,
        owner_id -> Nullable<Uuid>,
        chain -> Text,
        address -> Text,
        #[sql_name = "type"]
        type_ -> Text,
        status -> Nullable<Text>,
        created_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    withdrawals (id) {
        id -> Uuid,
        merchant_id -> Uuid,
        token_mint -> Text,
        amount -> Numeric,
        status -> Text,
        target_address -> Text,
        tx_hash -> Nullable<Text>,
        created_at -> Nullable<Timestamp>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::joinable!(apps -> merchants (merchant_id));
diesel::joinable!(balances -> merchants (merchant_id));
diesel::joinable!(deposits -> orders (order_id));
diesel::joinable!(orders -> apps (app_id));
diesel::joinable!(withdrawals -> merchants (merchant_id));

diesel::allow_tables_to_appear_in_same_query!(
    apps,
    audit_logs,
    balances,
    dead_letter,
    deposits,
    idempotency_keys,
    merchants,
    orders,
    outbox,
    wallets,
    withdrawals,
);
