BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS b_history (
    ca TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal IN (1,2)),
    confirmed_at_ms INTEGER NOT NULL,
    precision TEXT NOT NULL CHECK(precision IN ('ms','s')),
    chain_at_ms INTEGER,
    PRIMARY KEY(ca, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_b_history_time ON b_history(confirmed_at_ms DESC,ca);
-- Legacy history has no verifiable chain evidence. Never synthesize it from local time.
INSERT OR IGNORE INTO b_history(ca,ordinal,confirmed_at_ms,precision,chain_at_ms)
SELECT ca,1,COALESCE(first_qualified_at_ms,first_b_at*1000),
       CASE WHEN first_qualified_at_ms IS NULL THEN 's' ELSE 'ms' END,NULL
FROM tokens WHERE first_b_at IS NOT NULL;
INSERT OR IGNORE INTO b_history(ca,ordinal,confirmed_at_ms,precision,chain_at_ms)
SELECT ca,2,
       CASE WHEN last_qualified_at_ms/1000=reentry_b_at THEN last_qualified_at_ms ELSE reentry_b_at*1000 END,
       CASE WHEN last_qualified_at_ms/1000=reentry_b_at THEN 'ms' ELSE 's' END,NULL
FROM tokens WHERE reentry_b_at IS NOT NULL;
COMMIT;
