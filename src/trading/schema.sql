-- Separate lifecycle from token monitoring. Reset must never delete this ledger.
CREATE TABLE IF NOT EXISTS trade_orders (
    id TEXT PRIMARY KEY,
    ca TEXT NOT NULL,
    b_round INTEGER NOT NULL CHECK(b_round = 1),
    state TEXT NOT NULL,
    wallet TEXT,
    nonce INTEGER,
    tx_hash TEXT,
    signed_raw TEXT,
    created_at_ms INTEGER NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE(ca,b_round)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_wallet_nonce ON trade_orders(wallet,nonce)
    WHERE wallet IS NOT NULL AND nonce IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_hash ON trade_orders(tx_hash) WHERE tx_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trade_time ON trade_orders(created_at_ms);
CREATE INDEX IF NOT EXISTS idx_trade_state ON trade_orders(state);
-- Settings survive monitor reset; contain no RPC credentials, wallet password, or live mode.
CREATE TABLE IF NOT EXISTS trade_settings (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    payload TEXT NOT NULL
);
