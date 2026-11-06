//! SQLite 持久化。引擎仍是唯一状态所有者；数据库只是每次状态变更后的耐久镜像。

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

/// Evidence observed by the engine, not a claim of complete historical-chain replay.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BTiming {
    pub buyer_at: Option<u64>,
    pub green_at: Option<u64>,
    pub first_chain_at_ms: Option<u64>,
    pub second_chain_at_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BHistoryRow {
    pub ca: String,
    pub name: String,
    pub symbol: String,
    pub platform: String,
    pub state: String,
    pub first_at_ms: Option<u64>,
    pub first_precision: Option<String>,
    pub first_chain_at_ms: Option<u64>,
    pub second_at_ms: Option<u64>,
    pub second_precision: Option<String>,
    pub second_chain_at_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BHistoryPage {
    pub rows: Vec<BHistoryRow>,
    pub total: u64,
    pub offset: u64,
    pub limit: u64,
}

#[derive(Debug, Clone)]
pub struct PersistedToken {
    pub b_timing: BTiming,
    pub ca: String,
    pub name: String,
    pub symbol: String,
    pub creator: String,
    pub source_platform: String,
    pub creation_block: u64,
    pub creation_tx_hash: String,
    pub source_ref: Option<String>,
    pub total_supply_tokens: f64,
    pub created_at: u64,
    pub current_state: String,
    pub buy_tax_bps: u64,
    pub sell_tax_bps: u64,
    pub smart_money_hit: bool,
    pub bull_streak_seen: bool,
    pub first_b_at: Option<u64>,
    pub first_qualified_at_ms: Option<u64>,
    pub first_open_sent_at_ms: Option<u64>,
    pub first_open_acked_at_ms: Option<u64>,
    pub last_qualified_at_ms: Option<u64>,
    pub last_open_sent_at_ms: Option<u64>,
    pub last_open_acked_at_ms: Option<u64>,
    pub c_entered_at: Option<u64>,
    pub reentry_used: bool,
    pub reentry_b_at: Option<u64>,
    pub current_cycle_start: Option<u64>,
    pub cycle_max_market_cap: f64,
    pub current_slot: Option<String>,
    pub user_discarded: bool,
    pub quote_token: String,
    pub quote_symbol: Option<String>,
    pub last_price: f64,
    pub last_mcap_quote: f64,
    pub last_mcap_usd: Option<f64>,
    pub streak: u64,
    pub streak_end: Option<u64>,
    pub c_streak: u64,
    pub c_streak_end: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> PersistedToken {
        PersistedToken {
            b_timing: BTiming::default(),
            ca: "0x1111111111111111111111111111111111111111".into(),
            name: "T".into(),
            symbol: "T".into(),
            creator: "0x0".into(),
            source_platform: "FOUR_MEME".into(),
            creation_block: 99,
            creation_tx_hash: "0xabc".into(),
            source_ref: None,
            total_supply_tokens: 1_000_000_000.0,
            created_at: 100,
            current_state: "B_REENTRY_PENDING".into(),
            buy_tax_bps: 100,
            sell_tax_bps: 200,
            smart_money_hit: true,
            bull_streak_seen: true,
            first_b_at: Some(160),
            first_qualified_at_ms: Some(160_123),
            first_open_sent_at_ms: Some(160_130),
            first_open_acked_at_ms: Some(160_900),
            last_qualified_at_ms: Some(220_100),
            last_open_sent_at_ms: Some(220_120),
            last_open_acked_at_ms: Some(220_800),
            c_entered_at: Some(200),
            reentry_used: true,
            reentry_b_at: Some(220),
            current_cycle_start: Some(220),
            cycle_max_market_cap: 12_345.0,
            current_slot: None,
            user_discarded: false,
            quote_token: "0x0000000000000000000000000000000000000000".into(),
            quote_symbol: Some("BNB".into()),
            last_price: 1.0,
            last_mcap_quote: 2.0,
            last_mcap_usd: Some(1500.0),
            streak: 2,
            streak_end: Some(50),
            c_streak: 1,
            c_streak_end: Some(70),
        }
    }

    fn history(store: &Store, limit: u64, offset: u64, query: &str) -> BHistoryPage {
        history_page(&store.0.lock().unwrap(), limit, offset, query).unwrap()
    }

    #[test]
    fn deferred_market_cannot_overwrite_discard_reentry_or_slot_reservation() {
        let store = Store::memory().unwrap();
        let old = sample_token();
        store.upsert_token(&old, 1).unwrap();
        store.queue_market(old.clone());
        let mut discarded = old.clone();
        discarded.current_state = "USER_DISCARDED".into();
        discarded.user_discarded = true;
        discarded.current_slot = None;
        discarded.last_price = 5.0;
        store.upsert_token(&discarded, 2).unwrap();
        // Even a late old sample arriving after the critical write is rejected by SQL.
        store.queue_market(old.clone());
        store.flush_market().unwrap();
        let saved = &store.load_tokens().unwrap()[0];
        assert_eq!(saved.current_state, "USER_DISCARDED");
        assert_eq!(saved.last_price, 5.0);
        assert!(saved.user_discarded && saved.reentry_used);
        assert!(saved.current_slot.is_none());
    }

    #[test]
    fn market_writer_preserves_transient_peak_and_does_not_touch_new_cycle() {
        let store = Store::memory().unwrap();
        let mut t = sample_token();
        t.cycle_max_market_cap = 0.0;
        store.upsert_token(&t, 1).unwrap();
        t.last_mcap_usd = Some(12_000.0);
        t.cycle_max_market_cap = 12_000.0;
        store.queue_market(t.clone());
        t.last_mcap_usd = Some(8_000.0);
        t.cycle_max_market_cap = 8_000.0;
        store.queue_market(t.clone());
        store.flush_market().unwrap();
        let saved = &store.load_tokens().unwrap()[0];
        assert_eq!(saved.last_mcap_usd, Some(8_000.0));
        assert_eq!(saved.cycle_max_market_cap, 12_000.0);
        let mut next = t.clone();
        next.current_cycle_start = Some(820);
        next.cycle_max_market_cap = 0.0;
        store.upsert_token(&next, 2).unwrap();
        t.cycle_max_market_cap = 50_000.0;
        store.queue_market(t);
        store.flush_market().unwrap();
        let saved = &store.load_tokens().unwrap()[0];
        assert_eq!(saved.current_cycle_start, Some(820));
        assert_eq!(saved.cycle_max_market_cap, 0.0);
    }

    #[test]
    fn sqlite_roundtrip_keeps_reentry_and_cycle_fields() {
        let store = Store::memory().unwrap();
        let token = sample_token();
        store.upsert_token(&token, 999).unwrap();
        store.set_meta_u64("last_processed_block", 123).unwrap();
        let rows = store.load_tokens().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].reentry_used);
        assert_eq!(rows[0].current_state, "B_REENTRY_PENDING");
        assert_eq!(rows[0].cycle_max_market_cap, 12_345.0);
        assert_eq!(rows[0].first_open_acked_at_ms, Some(160_900));
        assert_eq!(rows[0].last_open_acked_at_ms, Some(220_800));
        assert_eq!(
            store.get_meta_u64("last_processed_block").unwrap(),
            Some(123)
        );
    }

    #[test]
    fn history_is_immutable_and_survives_discard_reset_and_migration() {
        let store = Store::memory().unwrap();
        let mut token = sample_token();
        token.reentry_b_at = None;
        token.b_timing.buyer_at = Some(155);
        token.b_timing.green_at = Some(159);
        token.b_timing.first_chain_at_ms = Some(159_000);
        store.upsert_token(&token, 200).unwrap();
        // Entering C (reentry_used=true) is not a second B promotion.
        assert!(history(&store, 50, 0, "").rows[0].second_at_ms.is_none());
        assert_eq!(store.load_tokens().unwrap()[0].b_timing.buyer_at, Some(155));
        token.reentry_b_at = Some(220);
        token.b_timing.second_chain_at_ms = Some(219_000);
        store.upsert_token(&token, 221).unwrap();
        token.first_qualified_at_ms = Some(999_123);
        token.last_qualified_at_ms = Some(220_999);
        token.current_state = "USER_DISCARDED".into();
        token.user_discarded = true;
        store.upsert_token(&token, 999).unwrap();
        store.reset_monitoring(1000, 1000, 1000).unwrap();
        // Startup backfill is idempotent, and cannot replace precise/evidence values.
        store
            .0
            .lock()
            .unwrap()
            .execute_batch(include_str!("b_history.sql"))
            .unwrap();
        let page = history(&store, 50, 0, "");
        assert_eq!(page.total, 1);
        let row = &page.rows[0];
        assert_eq!(row.first_at_ms, Some(160_123));
        assert_eq!(row.first_chain_at_ms, Some(159_000));
        assert_eq!(row.second_at_ms, Some(220_100));
        assert_eq!(row.second_chain_at_ms, Some(219_000));
        assert_eq!(row.state, "USER_DISCARDED");
    }

    #[test]
    fn legacy_backfill_keeps_precision_and_unknown_chain_times() {
        let store = Store::memory().unwrap();
        let precise = sample_token();
        let mut legacy = precise.clone();
        legacy.ca = "0x2222222222222222222222222222222222222222".into();
        legacy.first_qualified_at_ms = None;
        legacy.last_qualified_at_ms = Some(160_123); // First B, not reentry.
        store.upsert_token(&precise, 300).unwrap();
        store.upsert_token(&legacy, 300).unwrap();
        let conn = store.0.lock().unwrap();
        // Simulate an old DB with tokens but without the new history table/column.
        conn.execute_batch("DROP TABLE b_history; ALTER TABLE tokens DROP COLUMN b_timing;")
            .unwrap();
        drop(conn);
        let conn = Arc::try_unwrap(store.0).ok().unwrap().into_inner().unwrap();
        let migrated = Store::from_connection(conn).unwrap();
        let rows = history(&migrated, 50, 0, "").rows;
        assert_eq!(rows.len(), 2);
        let row = rows.iter().find(|r| r.ca == precise.ca).unwrap();
        assert_eq!(row.first_at_ms, Some(160_123));
        assert_eq!(row.second_at_ms, Some(220_100));
        assert_eq!(row.first_precision.as_deref(), Some("ms"));
        assert!(row.first_chain_at_ms.is_none());
        let row = rows.iter().find(|r| r.ca == legacy.ca).unwrap();
        assert_eq!(row.first_at_ms, Some(160_000));
        assert_eq!(row.second_at_ms, Some(220_000));
        assert_eq!(row.first_precision.as_deref(), Some("s"));
        assert_eq!(row.second_precision.as_deref(), Some("s"));
        assert!(row.second_chain_at_ms.is_none());
    }

    #[test]
    fn history_search_pagination_and_token_write_are_consistent() {
        let store = Store::memory().unwrap();
        let mut token = sample_token();
        store.upsert_token(&token, 300).unwrap();
        token.ca = "0x2222222222222222222222222222222222222222".into();
        token.name = "历史测试".into();
        token.symbol = "HISTORY".into();
        token.reentry_b_at = None;
        store.upsert_token(&token, 300).unwrap();
        assert_eq!(history(&store, 1, 0, "").total, 2);
        assert_eq!(history(&store, 1, 1, "").rows[0].ca, token.ca);
        assert!(history(&store, 1, 2, "").rows.is_empty());
        assert_eq!(history(&store, 50, 0, " hiSTory ").total, 1);
        assert_eq!(history(&store, 50, 0, "历史").total, 1);
        assert_eq!(history(&store, 50, 0, "222222").total, 1);
        assert_eq!(history(&store, 50, 0, "' OR 1=1 --").total, 0);
        // Failure to save history must also roll back the token update.
        store.0.lock().unwrap().execute_batch("CREATE TRIGGER fail_history BEFORE INSERT ON b_history BEGIN SELECT RAISE(ABORT,'test failure'); END;").unwrap();
        token.name = "must roll back".into();
        assert!(store.upsert_token(&token, 400).is_err());
        let rows = store.load_tokens().unwrap();
        assert_eq!(
            rows.iter().find(|r| r.ca == token.ca).unwrap().name,
            "历史测试"
        );
    }
}

#[derive(Clone)]
pub struct Store(pub(crate) Arc<Mutex<Connection>>, Arc<MarketQueue>);

#[derive(Default)]
struct MarketQueue {
    pending: Mutex<HashMap<String, PersistedToken>>,
    ready: Condvar,
    started: OnceLock<()>,
}

/// Dedicated read-only connection: browsing history never holds the engine's write mutex.
pub fn read_b_history(
    path: &Path,
    limit: u64,
    offset: u64,
    query: &str,
) -> Result<BHistoryPage, String> {
    let mut conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| e.to_string())?;
    conn.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|e| e.to_string())?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let page = history_page(&tx, limit.clamp(1, 100), offset, query)?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok(page)
}

fn history_page(
    conn: &Connection,
    limit: u64,
    offset: u64,
    query: &str,
) -> Result<BHistoryPage, String> {
    let filter = query.trim().to_lowercase();
    // One row per CA, including the rare case of a legacy reentry-only record.
    let base = " FROM (SELECT ca,MAX(confirmed_at_ms) latest FROM b_history GROUP BY ca) h
        JOIN tokens t ON t.ca=h.ca
        LEFT JOIN b_history f ON f.ca=h.ca AND f.ordinal=1
        LEFT JOIN b_history s ON s.ca=h.ca AND s.ordinal=2
        WHERE (?1='' OR instr(lower(t.ca),?1)>0 OR instr(lower(t.name),?1)>0 OR instr(lower(t.symbol),?1)>0)";
    let total = conn
        .query_row(&format!("SELECT COUNT(*){base}"), [&filter], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    let sql = format!(
        "SELECT t.ca,t.name,t.symbol,t.source_platform,t.current_state,
        f.confirmed_at_ms,f.precision,f.chain_at_ms,s.confirmed_at_ms,s.precision,s.chain_at_ms
        {base} ORDER BY h.latest DESC,h.ca LIMIT ?2 OFFSET ?3"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params![filter, limit, offset.min(i64::MAX as u64)], |r| {
            Ok(BHistoryRow {
                ca: r.get(0)?,
                name: r.get(1)?,
                symbol: r.get(2)?,
                platform: r.get(3)?,
                state: r.get(4)?,
                first_at_ms: r.get(5)?,
                first_precision: r.get(6)?,
                first_chain_at_ms: r.get(7)?,
                second_at_ms: r.get(8)?,
                second_precision: r.get(9)?,
                second_chain_at_ms: r.get(10)?,
            })
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(BHistoryPage {
        rows,
        total,
        offset,
        limit,
    })
}

fn flush_market_batch(
    connection: &Mutex<Connection>,
    queue: &MarketQueue,
) -> Result<usize, String> {
    // Lock order is identical to critical upsert_token_and_order.
    let mut connection = connection.lock().map_err(|_| "数据库锁中毒".to_string())?;
    let batch: Vec<_> = {
        let mut pending = queue
            .pending
            .lock()
            .map_err(|_| "行情队列锁中毒".to_string())?;
        let keys: Vec<_> = pending.keys().take(16).cloned().collect();
        keys.into_iter()
            .filter_map(|ca| pending.remove(&ca))
            .collect()
    };
    if batch.is_empty() {
        return Ok(0);
    }
    let result = (|| {
        let tx = connection.transaction().map_err(|e| e.to_string())?;
        for t in &batch {
            tx.execute(
                "UPDATE tokens SET last_price=?1,last_mcap_quote=?2,last_mcap_usd=?3,
                streak=?4,streak_end=?5,c_streak=?6,c_streak_end=?7,
                cycle_max_market_cap=MAX(cycle_max_market_cap,?8)
                WHERE ca=?9 AND current_state=?10 AND current_cycle_start IS ?11
                AND current_state NOT IN ('DEAD','USER_DISCARDED') AND user_discarded=0",
                params![
                    t.last_price,
                    t.last_mcap_quote,
                    t.last_mcap_usd,
                    t.streak as i64,
                    t.streak_end.map(|v| v as i64),
                    t.c_streak as i64,
                    t.c_streak_end.map(|v| v as i64),
                    t.cycle_max_market_cap,
                    t.ca,
                    t.current_state,
                    t.current_cycle_start.map(|v| v as i64)
                ],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    })();
    if let Err(e) = result {
        // Preserve newer queued samples; retry old samples only if not superseded.
        let mut pending = queue
            .pending
            .lock()
            .map_err(|_| "行情队列锁中毒".to_string())?;
        for t in batch {
            pending.entry(t.ca.clone()).or_insert(t);
        }
        return Err(e);
    }
    Ok(batch.len())
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("无法创建数据库目录 {}: {e}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .map_err(|e| format!("无法打开数据库 {}: {e}", path.display()))?;
        Self::from_connection(conn)
    }

    #[cfg(test)]
    pub fn memory() -> Result<Self, String> {
        Self::from_connection(Connection::open_in_memory().map_err(|e| e.to_string())?)
    }

    fn from_connection(conn: Connection) -> Result<Self, String> {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS tokens (
               ca TEXT PRIMARY KEY,
               name TEXT NOT NULL, symbol TEXT NOT NULL, creator TEXT NOT NULL,
               created_at INTEGER NOT NULL, current_state TEXT NOT NULL,
               buy_tax_bps INTEGER NOT NULL, sell_tax_bps INTEGER NOT NULL,
               smart_money_hit INTEGER NOT NULL, bull_streak_seen INTEGER NOT NULL,
               first_b_at INTEGER, c_entered_at INTEGER,
               reentry_used INTEGER NOT NULL, reentry_b_at INTEGER,
               current_cycle_start INTEGER, cycle_max_market_cap REAL NOT NULL,
               current_slot TEXT, user_discarded INTEGER NOT NULL,
               quote_token TEXT NOT NULL, quote_symbol TEXT,
               last_price REAL NOT NULL, last_mcap_quote REAL NOT NULL,
            last_mcap_usd REAL, streak INTEGER NOT NULL DEFAULT 0, streak_end INTEGER,
            c_streak INTEGER NOT NULL DEFAULT 0, c_streak_end INTEGER,
            source_platform TEXT NOT NULL DEFAULT 'FLAP', creation_block INTEGER NOT NULL DEFAULT 0,
            creation_tx_hash TEXT NOT NULL DEFAULT '', source_ref TEXT,
            total_supply_tokens REAL NOT NULL DEFAULT 1000000000,
            first_qualified_at_ms INTEGER, first_open_sent_at_ms INTEGER,
            first_open_acked_at_ms INTEGER, last_qualified_at_ms INTEGER,
            last_open_sent_at_ms INTEGER, last_open_acked_at_ms INTEGER,
            updated_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_tokens_state ON tokens(current_state);
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .map_err(|e| format!("数据库迁移失败: {e}"))?;
        // 兼容 0.2.0 早期数据库；重复列错误可安全忽略。
        for sql in [
            "ALTER TABLE tokens ADD COLUMN b_timing TEXT NOT NULL DEFAULT '{}'",
            "ALTER TABLE tokens ADD COLUMN streak INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE tokens ADD COLUMN streak_end INTEGER",
            "ALTER TABLE tokens ADD COLUMN c_streak INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE tokens ADD COLUMN c_streak_end INTEGER",
            "ALTER TABLE tokens ADD COLUMN source_platform TEXT NOT NULL DEFAULT 'FLAP'",
            "ALTER TABLE tokens ADD COLUMN creation_block INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE tokens ADD COLUMN creation_tx_hash TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE tokens ADD COLUMN source_ref TEXT",
            "ALTER TABLE tokens ADD COLUMN total_supply_tokens REAL NOT NULL DEFAULT 1000000000",
            "ALTER TABLE tokens ADD COLUMN first_qualified_at_ms INTEGER",
            "ALTER TABLE tokens ADD COLUMN first_open_sent_at_ms INTEGER",
            "ALTER TABLE tokens ADD COLUMN first_open_acked_at_ms INTEGER",
            "ALTER TABLE tokens ADD COLUMN last_qualified_at_ms INTEGER",
            "ALTER TABLE tokens ADD COLUMN last_open_sent_at_ms INTEGER",
            "ALTER TABLE tokens ADD COLUMN last_open_acked_at_ms INTEGER",
        ] {
            let _ = conn.execute(sql, []);
        }
        conn.execute_batch(include_str!("b_history.sql"))
            .map_err(|e| format!("B 历史迁移失败: {e}"))?;
        conn.execute_batch(include_str!("trading/schema.sql"))
            .map_err(|e| format!("交易记录迁移失败: {e}"))?;
        Ok(Self(
            Arc::new(Mutex::new(conn)),
            Arc::new(MarketQueue::default()),
        ))
    }

    /// 行情镜像由一个后台 writer 合并写入，不占用状态机执行时间。
    /// 不写生命周期、Slot、买入意图、用户删除或 B 历史。
    pub fn queue_market(&self, mut t: PersistedToken) {
        if matches!(t.current_state.as_str(), "DEAD" | "USER_DISCARDED") {
            return;
        }
        {
            let mut pending = self.1.pending.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(previous) = pending.get(&t.ca) {
                if previous.current_state == t.current_state
                    && previous.current_cycle_start == t.current_cycle_start
                {
                    t.cycle_max_market_cap =
                        t.cycle_max_market_cap.max(previous.cycle_max_market_cap);
                }
            }
            pending.insert(t.ca.clone(), t);
        }
        self.1.started.get_or_init(|| {
            let connection = Arc::downgrade(&self.0);
            let queue = Arc::downgrade(&self.1);
            std::thread::spawn(move || loop {
                let (Some(connection), Some(queue)) = (connection.upgrade(), queue.upgrade())
                else {
                    break;
                };
                let failed = if let Err(e) = flush_market_batch(&connection, &queue) {
                    eprintln!("[warn] async market persistence: {e}");
                    true
                } else {
                    false
                };
                // 不在休眠期间持有数据库引用；Store 释放后线程可自行退出。
                drop(connection);
                let pending = queue.pending.lock().unwrap_or_else(|e| e.into_inner());
                let delay = if !failed && !pending.is_empty() {
                    1
                } else {
                    50
                };
                let _ = queue
                    .ready
                    .wait_timeout(pending, std::time::Duration::from_millis(delay));
            });
        });
        self.1.ready.notify_one();
    }

    pub fn flush_market(&self) -> Result<(), String> {
        while flush_market_batch(&self.0, &self.1)? > 0 {}
        Ok(())
    }

    pub fn upsert_token(&self, t: &PersistedToken, now: u64) -> Result<(), String> {
        self.upsert_token_and_order(t, now, None)
    }

    /// Slot reservation and first-buy intent are committed in one SQLite transaction.
    pub fn upsert_token_and_order(
        &self,
        t: &PersistedToken,
        now: u64,
        order: Option<&crate::trading::types::Order>,
    ) -> Result<(), String> {
        let mut guard = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        let conn = guard.transaction().map_err(|e| e.to_string())?;
        conn.execute(
            "INSERT INTO tokens (ca,name,symbol,creator,created_at,current_state,buy_tax_bps,sell_tax_bps,
             smart_money_hit,bull_streak_seen,first_b_at,c_entered_at,reentry_used,reentry_b_at,
             current_cycle_start,cycle_max_market_cap,current_slot,user_discarded,quote_token,quote_symbol,
             last_price,last_mcap_quote,last_mcap_usd,streak,streak_end,c_streak,c_streak_end,
             source_platform,creation_block,creation_tx_hash,source_ref,total_supply_tokens,
             first_qualified_at_ms,first_open_sent_at_ms,first_open_acked_at_ms,
             last_qualified_at_ms,last_open_sent_at_ms,last_open_acked_at_ms,updated_at,b_timing)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40)
             ON CONFLICT(ca) DO UPDATE SET
             name=excluded.name,symbol=excluded.symbol,creator=excluded.creator,created_at=excluded.created_at,
             current_state=excluded.current_state,buy_tax_bps=excluded.buy_tax_bps,sell_tax_bps=excluded.sell_tax_bps,
             smart_money_hit=excluded.smart_money_hit,bull_streak_seen=excluded.bull_streak_seen,
             first_b_at=excluded.first_b_at,c_entered_at=excluded.c_entered_at,reentry_used=excluded.reentry_used,
             reentry_b_at=excluded.reentry_b_at,current_cycle_start=excluded.current_cycle_start,
             cycle_max_market_cap=excluded.cycle_max_market_cap,current_slot=excluded.current_slot,
             user_discarded=excluded.user_discarded,quote_token=excluded.quote_token,quote_symbol=excluded.quote_symbol,
             last_price=excluded.last_price,last_mcap_quote=excluded.last_mcap_quote,
             last_mcap_usd=excluded.last_mcap_usd,streak=excluded.streak,streak_end=excluded.streak_end,
             c_streak=excluded.c_streak,c_streak_end=excluded.c_streak_end,
             source_platform=excluded.source_platform,creation_block=excluded.creation_block,
             creation_tx_hash=excluded.creation_tx_hash,source_ref=excluded.source_ref,
             total_supply_tokens=excluded.total_supply_tokens,
             first_qualified_at_ms=excluded.first_qualified_at_ms,
             first_open_sent_at_ms=excluded.first_open_sent_at_ms,
             first_open_acked_at_ms=excluded.first_open_acked_at_ms,
             last_qualified_at_ms=excluded.last_qualified_at_ms,
             last_open_sent_at_ms=excluded.last_open_sent_at_ms,
             last_open_acked_at_ms=excluded.last_open_acked_at_ms,
             updated_at=excluded.updated_at,b_timing=excluded.b_timing",
            params![
                t.ca, t.name, t.symbol, t.creator, t.created_at as i64, t.current_state,
                t.buy_tax_bps as i64, t.sell_tax_bps as i64, t.smart_money_hit as i64,
                t.bull_streak_seen as i64, t.first_b_at.map(|v| v as i64),
                t.c_entered_at.map(|v| v as i64), t.reentry_used as i64,
                t.reentry_b_at.map(|v| v as i64), t.current_cycle_start.map(|v| v as i64),
                t.cycle_max_market_cap, t.current_slot, t.user_discarded as i64,
                t.quote_token, t.quote_symbol, t.last_price, t.last_mcap_quote,
                t.last_mcap_usd, t.streak as i64, t.streak_end.map(|v| v as i64),
                t.c_streak as i64, t.c_streak_end.map(|v| v as i64),
                t.source_platform, t.creation_block as i64, t.creation_tx_hash, t.source_ref,
                t.total_supply_tokens,
                t.first_qualified_at_ms.map(|v| v as i64),
                t.first_open_sent_at_ms.map(|v| v as i64),
                t.first_open_acked_at_ms.map(|v| v as i64),
                t.last_qualified_at_ms.map(|v| v as i64),
                t.last_open_sent_at_ms.map(|v| v as i64),
                t.last_open_acked_at_ms.map(|v| v as i64),
                now as i64, serde_json::to_string(&t.b_timing).map_err(|e| e.to_string())?
            ],
        ).map_err(|e| format!("保存代币 {} 失败: {e}", t.ca))?;
        // The unique key makes both promotion times immutable and deduplicates retries.
        for (ordinal, seconds, precise, chain) in [
            (
                1,
                t.first_b_at,
                t.first_qualified_at_ms,
                t.b_timing.first_chain_at_ms,
            ),
            (
                2,
                t.reentry_b_at,
                t.last_qualified_at_ms
                    .filter(|ms| t.reentry_b_at.is_some_and(|s| *ms / 1000 == s)),
                t.b_timing.second_chain_at_ms,
            ),
        ] {
            if let Some(seconds) = seconds {
                conn.execute("INSERT OR IGNORE INTO b_history(ca,ordinal,confirmed_at_ms,precision,chain_at_ms) VALUES (?1,?2,?3,?4,?5)",
                    params![t.ca, ordinal, precise.unwrap_or(seconds * 1000), if precise.is_some() { "ms" } else { "s" }, chain]).map_err(|e| e.to_string())?;
            }
        }
        if let Some(order) = order {
            if order.intent.ca != t.ca
                || t.current_slot.as_deref() != Some(&order.intent.slot_id)
                || !matches!(t.current_state.as_str(), "B_PENDING" | "B_ACTIVE")
                || t.user_discarded
                || t.reentry_b_at.is_some()
                || order.intent.round != 1
                || t.first_qualified_at_ms != Some(order.intent.b_at_ms)
            {
                return Err("买入意图与 Slot / Token 状态不一致".into());
            }
            crate::trading::book::insert(&conn, order)?;
        }
        conn.commit().map_err(|e| e.to_string())?;
        // 与 writer 一样先持数据库锁再持队列锁。已提交的关键状态使旧快照
        // 失效；后台绝不能用旧 A/B/C 快照覆盖一次删除、重入或 Slot 预留。
        self.1
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&t.ca);
        Ok(())
    }

    pub fn load_tokens(&self) -> Result<Vec<PersistedToken>, String> {
        self.flush_market()?;
        let conn = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        let mut stmt = conn.prepare(
            "SELECT ca,name,symbol,creator,created_at,current_state,buy_tax_bps,sell_tax_bps,
             smart_money_hit,bull_streak_seen,first_b_at,c_entered_at,reentry_used,reentry_b_at,
             current_cycle_start,cycle_max_market_cap,current_slot,user_discarded,quote_token,quote_symbol,
             last_price,last_mcap_quote,last_mcap_usd,streak,streak_end,c_streak,c_streak_end,
             source_platform,creation_block,creation_tx_hash,source_ref,total_supply_tokens,
             first_qualified_at_ms,first_open_sent_at_ms,first_open_acked_at_ms,
             last_qualified_at_ms,last_open_sent_at_ms,last_open_acked_at_ms,b_timing FROM tokens"
        ).map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok(PersistedToken {
                    b_timing: serde_json::from_str(&r.get::<_, String>(38)?).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            38,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?,
                    ca: r.get(0)?,
                    name: r.get(1)?,
                    symbol: r.get(2)?,
                    creator: r.get(3)?,
                    created_at: r.get::<_, i64>(4)? as u64,
                    current_state: r.get(5)?,
                    buy_tax_bps: r.get::<_, i64>(6)? as u64,
                    sell_tax_bps: r.get::<_, i64>(7)? as u64,
                    smart_money_hit: r.get::<_, i64>(8)? != 0,
                    bull_streak_seen: r.get::<_, i64>(9)? != 0,
                    first_b_at: r.get::<_, Option<i64>>(10)?.map(|v| v as u64),
                    c_entered_at: r.get::<_, Option<i64>>(11)?.map(|v| v as u64),
                    reentry_used: r.get::<_, i64>(12)? != 0,
                    reentry_b_at: r.get::<_, Option<i64>>(13)?.map(|v| v as u64),
                    current_cycle_start: r.get::<_, Option<i64>>(14)?.map(|v| v as u64),
                    cycle_max_market_cap: r.get(15)?,
                    current_slot: r.get(16)?,
                    user_discarded: r.get::<_, i64>(17)? != 0,
                    quote_token: r.get(18)?,
                    quote_symbol: r.get(19)?,
                    last_price: r.get(20)?,
                    last_mcap_quote: r.get(21)?,
                    last_mcap_usd: r.get(22)?,
                    streak: r.get::<_, i64>(23)? as u64,
                    streak_end: r.get::<_, Option<i64>>(24)?.map(|v| v as u64),
                    c_streak: r.get::<_, i64>(25)? as u64,
                    c_streak_end: r.get::<_, Option<i64>>(26)?.map(|v| v as u64),
                    source_platform: r.get(27)?,
                    creation_block: r.get::<_, i64>(28)? as u64,
                    creation_tx_hash: r.get(29)?,
                    source_ref: r.get(30)?,
                    total_supply_tokens: r.get(31)?,
                    first_qualified_at_ms: r.get::<_, Option<i64>>(32)?.map(|v| v as u64),
                    first_open_sent_at_ms: r.get::<_, Option<i64>>(33)?.map(|v| v as u64),
                    first_open_acked_at_ms: r.get::<_, Option<i64>>(34)?.map(|v| v as u64),
                    last_qualified_at_ms: r.get::<_, Option<i64>>(35)?.map(|v| v as u64),
                    last_open_sent_at_ms: r.get::<_, Option<i64>>(36)?.map(|v| v as u64),
                    last_open_acked_at_ms: r.get::<_, Option<i64>>(37)?.map(|v| v as u64),
                })
            })
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }

    pub fn set_meta_u64(&self, key: &str, value: u64) -> Result<(), String> {
        let conn = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        conn.execute(
            "INSERT INTO meta(key,value) VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value.to_string()],
        ).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Unversioned highest-live-block values are not evidence of a complete scan.
    /// Persist a conservative migration baseline atomically before any live event
    /// can advance the old cursor. Future restarts retain the actor-ACKed cursor.
    pub fn initialize_scan_cursor(&self, baseline: u64) -> Result<u64, String> {
        let mut conn = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let version: Option<String> = tx
            .query_row(
                "SELECT value FROM meta WHERE key='scan_cursor_version'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT value FROM meta WHERE key='last_scanned_block'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let cursor = if version.as_deref() == Some("1") {
            saved
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(baseline)
        } else {
            baseline
        };
        for (key, value) in [("last_scanned_block", cursor), ("scan_cursor_version", 1)] {
            tx.execute("INSERT INTO meta(key,value) VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![key, value.to_string()])
                .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(cursor)
    }

    /// Commit the tombstones and replay barrier together; never delete permanent history.
    pub fn reset_monitoring(&self, block: u64, id: u64, now: u64) -> Result<(), String> {
        let mut conn = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute("UPDATE tokens SET current_state='DEAD', current_slot=NULL, updated_at=?1 WHERE current_state NOT IN ('DEAD','USER_DISCARDED')", [now]).map_err(|e| e.to_string())?;
        for (key, value) in [
            ("reset_cutoff_block", block),
            ("last_processed_block", block),
            ("last_scanned_block", block),
            ("scan_cursor_version", 1),
            ("browser_reset_pending", id),
        ] {
            tx.execute("INSERT INTO meta(key,value) VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![key, value.to_string()]).map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    }

    pub fn get_meta_u64(&self, key: &str) -> Result<Option<u64>, String> {
        let conn = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        let v: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
            .optional()
            .map_err(|e| e.to_string())?;
        Ok(v.and_then(|s| s.parse().ok()))
    }

    /// 断线补扫 GeniusFun curve 成交所需的动态合约地址。
    pub fn source_refs(&self) -> Result<Vec<String>, String> {
        let conn = self.0.lock().map_err(|_| "数据库锁中毒".to_string())?;
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT source_ref FROM tokens
                 WHERE source_ref IS NOT NULL AND source_ref <> ''
                 AND current_state NOT IN ('USER_DISCARDED','DEAD')",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }
}
