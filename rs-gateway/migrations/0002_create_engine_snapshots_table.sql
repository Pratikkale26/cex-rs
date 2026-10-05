-- 0002_create_engine_snapshots_table.sql
CREATE TABLE IF NOT EXISTS engine_snapshots (
    id BIGSERIAL PRIMARY KEY,
    last_stream_id VARCHAR(64) NOT NULL,
    state_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_engine_snapshots_created ON engine_snapshots (created_at DESC);
