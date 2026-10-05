-- 0003_create_ledger_and_trade_history.sql

CREATE TABLE IF NOT EXISTS orders (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    market VARCHAR(16) NOT NULL DEFAULT 'SOL_USD',
    side VARCHAR(8) NOT NULL,
    price BIGINT NOT NULL,
    original_qty BIGINT NOT NULL,
    remaining_qty BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_created ON orders (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);

CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    maker_order_id BIGINT NOT NULL,
    taker_order_id BIGINT NOT NULL,
    buyer_id BIGINT NOT NULL,
    seller_id BIGINT NOT NULL,
    market VARCHAR(16) NOT NULL DEFAULT 'SOL_USD',
    price BIGINT NOT NULL,
    qty BIGINT NOT NULL,
    quote_amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_buyer ON trades (buyer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_seller ON trades (seller_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades (market, created_at DESC);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    currency VARCHAR(16) NOT NULL,
    amount BIGINT NOT NULL,
    balance_type VARCHAR(16) NOT NULL,
    operation_type VARCHAR(32) NOT NULL,
    reference_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger_entries (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_user_currency ON ledger_entries (user_id, currency, created_at DESC);

CREATE TABLE IF NOT EXISTS execution_checkpoints (
    id INT PRIMARY KEY DEFAULT 1,
    last_stream_id VARCHAR(64) NOT NULL DEFAULT '0-0',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO execution_checkpoints (id, last_stream_id) VALUES (1, '0-0') ON CONFLICT DO NOTHING;
