//! Independent, read-only HTTP reconciliation. A connected WebSocket is not
//! evidence that its logs subscription still delivers events. All recovered
//! events enter the existing single actor; only that actor commits scan progress.

use crate::{
    engine::Shared,
    launchpads,
    persistence::Store,
    rpc::{self, RawLog},
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashSet, sync::atomic::Ordering, time::Duration};
use tokio::sync::broadcast;

pub const CHECKPOINT_TOPIC: &str = "__scan_checkpoint";
const CHUNK: u64 = 50;
const POLL: Duration = Duration::from_secs(3);
// Do not consume the trading NodeReal quota or share the metadata/sender pool.
const ENDPOINTS: &[&str] = &[
    "https://bsc-rpc.publicnode.com",
    "https://bsc-mainnet.public.blastapi.io",
    "https://bsc-dataseed.bnbchain.org",
];

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryHealth {
    pub state: String,
    pub head_block: u64,
    pub scanned_block: u64,
    pub gap_blocks: u64,
    pub last_success_at_ms: u64,
    pub last_error: Option<String>,
    pub transport: String,
    pub stream: Option<rpc::StreamSnapshot>,
    pub consecutive_failures: u32,
    pub next_retry_at_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetHead {
    pub block: u64,
    pub timestamp: u64,
    pub checked_at_ms: u64,
    pub source: String,
}

fn checked_head(value: &Value, now_ms: u64) -> Result<(u64, u64), String> {
    let block = hex(value, "number")?;
    let timestamp = hex(value, "timestamp")?;
    if block == 0
        || timestamp == 0
        || timestamp.saturating_mul(1000) > now_ms.saturating_add(5000)
        || now_ms.saturating_sub(timestamp.saturating_mul(1000)) > 30_000
    {
        return Err("节点区块头无效或已过期，未清空任何数据".into());
    }
    Ok((block, timestamp))
}

/// Fresh public HTTP pool, separate from recovery cooldown and trading quotas.
pub async fn latest_reset_head(ws: Option<rpc::Client>) -> Result<ResetHead, String> {
    let mut reader = Reader::new();
    reader.ws = ws;
    reader.reset_head().await
}

fn retry_delay(failures: u32, error: &str) -> Duration {
    let limited = error.to_ascii_lowercase();
    let base = if limited.contains("rate limit")
        || limited.contains("429")
        || limited.contains("limit exceeded")
    {
        10u64
    } else {
        3
    };
    Duration::from_secs((base * (1u64 << failures.saturating_sub(1).min(4))).min(60))
}

struct Reader {
    http: reqwest::Client,
    endpoints: Vec<String>,
    health: rpc::EndpointHealth,
    ws: Option<rpc::Client>,
}

#[derive(Clone, Copy)]
struct Head {
    endpoint: usize,
    block: u64,
    wss_generation: Option<u64>,
}

impl Reader {
    fn new() -> Self {
        Self::with_endpoints(ENDPOINTS.iter().map(|s| s.to_string()).collect())
    }
    fn with_endpoints(endpoints: Vec<String>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_millis(1500))
                .timeout(Duration::from_secs(4))
                .build()
                .expect("read-only recovery HTTP configuration"),
            health: rpc::EndpointHealth::new(endpoints.len()),
            endpoints,
            ws: None,
        }
    }
    async fn at(&self, endpoint: usize, method: &str, params: Value) -> Result<Value, String> {
        // This helper has no signing, wallet, or arbitrary RPC entry point.
        if !matches!(method, "eth_getBlockByNumber" | "eth_getLogs") {
            return Err("补扫通道仅允许只读区块和日志查询".into());
        }
        let result = async {
            let response = self
                .http
                .post(&self.endpoints[endpoint])
                .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
                .send()
                .await
                .map_err(|e| format!("HTTP 请求失败：{e}"))?;
            if !response.status().is_success() {
                return Err(format!("HTTP {}", response.status()));
            }
            let value: Value = response
                .json()
                .await
                .map_err(|e| format!("HTTP JSON：{e}"))?;
            if let Some(error) = value.get("error") {
                return Err(format!("RPC {error}"));
            }
            value
                .get("result")
                .filter(|v| !v.is_null())
                .cloned()
                .ok_or_else(|| "RPC 缺少 result".to_string())
        }
        .await;
        match &result {
            Ok(_) => self.health.report_success(endpoint),
            Err(_) => self
                .health
                .report_fault(endpoint, rpc::EndpointFault::Transport),
        }
        result
    }
    async fn reset_head(&self) -> Result<ResetHead, String> {
        let mut requests = FuturesUnordered::new();
        for endpoint in 0..self.endpoints.len() {
            requests.push(async move {
                let value = self
                    .at(endpoint, "eth_getBlockByNumber", json!(["latest", false]))
                    .await?;
                let checked_at_ms = rpc::unix_ms();
                let (block, timestamp) = checked_head(&value, checked_at_ms)?;
                Ok::<_, String>(ResetHead {
                    block,
                    timestamp,
                    checked_at_ms,
                    source: self.endpoints[endpoint].clone(),
                })
            });
        }
        let mut errors = Vec::new();
        while let Some(result) = requests.next().await {
            match result {
                Ok(head) => return Ok(head),
                Err(e) => errors.push(e),
            }
        }
        if let Some(ws) = self.ws.as_ref().filter(|w| w.is_live()) {
            let generation = ws.stream_snapshot().generation;
            match self
                .ws_read(generation, "eth_getBlockByNumber", json!(["latest", false]))
                .await
            {
                Ok(value) => {
                    let checked_at_ms = rpc::unix_ms();
                    let (block, timestamp) = checked_head(&value, checked_at_ms)?;
                    return Ok(ResetHead {
                        block,
                        timestamp,
                        checked_at_ms,
                        source: "public_wss_fallback".into(),
                    });
                }
                Err(e) => errors.push(e),
            }
        }
        Err(format!(
            "无法确认最新区块（已尝试独立公共备用节点），未清空任何数据：{}",
            errors.join("; ")
        ))
    }
    async fn head(&self) -> Result<Head, String> {
        let [healthy, _cooling] = self.health.candidate_groups();
        let mut errors = Vec::new();
        // Cooling endpoints stay quiet until expiry. No tight-loop retry storms.
        for endpoint in healthy {
            let reply = self
                .at(endpoint, "eth_getBlockByNumber", json!(["latest", false]))
                .await;
            match reply.and_then(|v| {
                let block = hex(&v, "number")?;
                let ts = hex(&v, "timestamp")?;
                if block == 0 || rpc::unix_ms().saturating_sub(ts.saturating_mul(1000)) > 30_000 {
                    return Err("HTTP 区块头过期，不能确认补扫完整性".into());
                }
                Ok(Head {
                    endpoint,
                    block,
                    wss_generation: None,
                })
            }) {
                Ok(head) => return Ok(head),
                Err(e) => {
                    self.health
                        .report_fault(endpoint, rpc::EndpointFault::Transport);
                    errors.push(e);
                }
            }
        }
        // HTTP/WSS have separate public-node limits. Independent HTTP is primary;
        // JSON-RPC may still work on WSS even when only its logs stream failed.
        if let Some(ws) = self.ws.as_ref().filter(|ws| ws.is_live()) {
            let generation = ws.stream_snapshot().generation;
            match self
                .ws_read(generation, "eth_getBlockByNumber", json!(["latest", false]))
                .await
            {
                Ok(v) => {
                    let block = hex(&v, "number")?;
                    let ts = hex(&v, "timestamp")?;
                    if block > 0 && rpc::unix_ms().saturating_sub(ts.saturating_mul(1000)) <= 30_000
                    {
                        return Ok(Head {
                            endpoint: usize::MAX,
                            block,
                            wss_generation: Some(generation),
                        });
                    }
                    errors.push("WSS 只读兜底区块头过期".into());
                }
                Err(e) => errors.push(e),
            }
        }
        Err(if errors.is_empty() {
            "公共补扫节点均在冷却，稍后重试".into()
        } else {
            errors.join("; ")
        })
    }
    async fn ws_read(&self, generation: u64, method: &str, params: Value) -> Result<Value, String> {
        if !matches!(method, "eth_getBlockByNumber" | "eth_getLogs") {
            return Err("补扫 WSS 仅允许只读查询".into());
        }
        let ws = self.ws.as_ref().ok_or("未配置只读 WSS 兜底")?;
        if !ws.is_live() || ws.stream_snapshot().generation != generation {
            return Err("WSS 补扫连接已变化".into());
        }
        let result = tokio::time::timeout(Duration::from_secs(4), ws.call(method, params))
            .await
            .map_err(|_| "WSS 只读补扫超时".to_string())??;
        if ws.stream_snapshot().generation != generation {
            return Err("WSS 补扫期间发生重连，整批重试".into());
        }
        Ok(result)
    }
    async fn pinned_read(&self, head: Head, method: &str, params: Value) -> Result<Value, String> {
        if let Some(generation) = head.wss_generation {
            self.ws_read(generation, method, params).await
        } else {
            self.at(head.endpoint, method, params).await
        }
    }
    async fn logs(
        &self,
        head: Head,
        filter: &launchpads::BackfillFilter,
        from: u64,
        to: u64,
    ) -> Result<Vec<RawLog>, String> {
        if filter.addresses.is_empty() || filter.topics.is_empty() {
            return Ok(Vec::new());
        }
        // Pin EVERY filter in a chunk to the endpoint which supplied its fresh
        // head. A lagging fallback returning [] must not make a gap look scanned.
        let address = if filter.addresses.len() == 1 {
            json!(filter.addresses[0])
        } else {
            json!(filter.addresses)
        };
        let result = self
            .pinned_read(
                head,
                "eth_getLogs",
                json!([{
                    "address": address, "topics":[filter.topics],
                    "fromBlock":format!("0x{from:x}"), "toBlock":format!("0x{to:x}")
                }]),
            )
            .await
            .and_then(|value| parse_logs(&value, from, to));
        if result.is_err() && head.wss_generation.is_none() {
            // A successful head read must not keep resetting repeated getLogs
            // failures before they reach the cooldown threshold.
            self.health
                .report_fault(head.endpoint, rpc::EndpointFault::Transport);
        }
        result
    }
    async fn chunk(
        &self,
        head: Head,
        store: &Store,
        from: u64,
        to: u64,
    ) -> Result<Vec<RawLog>, String> {
        let mut registry = launchpads::Registry::default();
        let mut known: HashSet<String> = store
            .source_refs()?
            .into_iter()
            .map(|s| s.to_lowercase())
            .collect();
        let mut filters = registry.fixed_backfill_filters();
        filters
            .extend(registry.dynamic_backfill_filters(&known.iter().cloned().collect::<Vec<_>>()));
        let mut logs = Vec::new();
        for filter in filters {
            logs.extend(self.logs(head, &filter, from, to).await?);
        }
        let mut new_refs = Vec::new();
        for log in &logs {
            for event in registry.decode(log) {
                if let launchpads::NormalizedEvent::NewToken(token) = event {
                    if let Some(source_ref) = token.source_ref {
                        let reference = source_ref.to_lowercase();
                        if known.insert(reference.clone()) {
                            new_refs.push(reference);
                        }
                    }
                }
            }
        }
        for filter in registry.dynamic_backfill_filters(&new_refs) {
            logs.extend(self.logs(head, &filter, from, to).await?);
        }
        let mut seen = HashSet::new();
        logs.retain(|l| seen.insert(rpc::chain_order(l)));
        logs.sort_by_key(rpc::chain_order);
        // Supply missing HISTORICAL block timestamps through the same working
        // reader. Do not make the actor wait behind a rate-limited metadata RPC.
        // Headers fill its timestamp cache only; they never close candles.
        let mut timestamps = std::collections::BTreeMap::new();
        let mut needed = std::collections::BTreeSet::new();
        for log in &logs {
            for event in registry.decode(log) {
                let ts = match event {
                    launchpads::NormalizedEvent::NewToken(t) => Some(t.created_at),
                    launchpads::NormalizedEvent::Trade(t) => Some(t.ts),
                    _ => None,
                };
                match ts {
                    Some(0) if head.block.saturating_sub(log.block_number) > 100 => {
                        needed.insert(log.block_number);
                    }
                    Some(ts) if ts > 0 => {
                        timestamps.insert(log.block_number, ts);
                    }
                    _ => {}
                }
            }
        }
        for block in needed {
            if !timestamps.contains_key(&block) {
                let v = self
                    .pinned_read(
                        head,
                        "eth_getBlockByNumber",
                        json!([format!("0x{block:x}"), false]),
                    )
                    .await?;
                if hex(&v, "number")? != block {
                    return Err("补扫时间戳区块不匹配".into());
                }
                timestamps.insert(block, hex(&v, "timestamp")?);
            }
        }
        let mut complete: Vec<RawLog> = timestamps
            .into_iter()
            .map(|(block, ts)| RawLog {
                address: String::new(),
                tx_hash: String::new(),
                tx_from: None,
                block_number: block,
                tx_index: 0,
                log_index: 0,
                topic0: "__block_timestamp".into(),
                topics: vec![],
                data: ts.to_be_bytes().to_vec(),
            })
            .collect();
        complete.extend(logs);
        Ok(complete)
    }
}

fn hex(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(Value::as_str)
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .ok_or_else(|| format!("RPC 非法 {key}"))
}

fn parse_logs(value: &Value, from: u64, to: u64) -> Result<Vec<RawLog>, String> {
    let items = value
        .as_array()
        .ok_or("eth_getLogs 不是数组，禁止推进补扫进度")?;
    let mut logs = Vec::new();
    for item in items {
        let log =
            rpc::parse_log_result(item).ok_or("eth_getLogs 包含非法/removed 日志，禁止跳过")?;
        if !(from..=to).contains(&log.block_number) {
            return Err("eth_getLogs 返回范围外日志".into());
        }
        logs.push(log);
    }
    Ok(logs)
}

pub fn checkpoint(from: u64, to: u64, lagged: u64) -> RawLog {
    RawLog {
        address: String::new(),
        tx_hash: String::new(),
        tx_from: None,
        block_number: to,
        tx_index: 0,
        log_index: 0,
        topic0: CHECKPOINT_TOPIC.into(),
        topics: vec![],
        data: [from.to_be_bytes(), to.to_be_bytes(), lagged.to_be_bytes()].concat(),
    }
}

pub fn decode_checkpoint(log: &RawLog) -> Option<(u64, u64, u64)> {
    if log.topic0 != CHECKPOINT_TOPIC || log.data.len() != 24 {
        return None;
    }
    let from = u64::from_be_bytes(log.data[0..8].try_into().ok()?);
    let to = u64::from_be_bytes(log.data[8..16].try_into().ok()?);
    let lagged = u64::from_be_bytes(log.data[16..24].try_into().ok()?);
    (from <= to && to == log.block_number).then_some((from, to, lagged))
}

fn relay(tx: &broadcast::Sender<RawLog>, logs: Vec<RawLog>) -> Result<(), String> {
    for log in logs {
        tx.send(log)
            .map_err(|_| "状态机事件接收器已关闭".to_string())?;
    }
    Ok(())
}

async fn commit(
    tx: &broadcast::Sender<RawLog>,
    shared: &Shared,
    from: u64,
    to: u64,
    lagged: u64,
) -> Result<(), String> {
    tx.send(checkpoint(from, to, lagged))
        .map_err(|_| "状态机事件接收器已关闭")?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while shared.last_scanned_block.load(Ordering::Relaxed) < to {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| "等待状态机补扫 ACK 超时，原进度保留并重试".to_string())
}

fn range(cursor: u64, floor: u64, head: u64) -> Option<(u64, u64)> {
    let from = cursor.saturating_sub(2).max(floor).saturating_add(1);
    (from <= head).then(|| (from, from.saturating_add(CHUNK - 1).min(head)))
}

/// Optional explicit seed. Bootstrap skips only history which the existing
/// SEED_BLOCKS policy explicitly excludes. Routine recovery never skips gaps.
pub async fn seed(
    client: rpc::Client,
    tx: &broadcast::Sender<RawLog>,
    shared: &Shared,
    store: &Store,
    blocks: u64,
) -> Result<(), String> {
    let mut reader = Reader::new();
    reader.ws = Some(client);
    let head = reader.head().await?;
    let mut from = head
        .block
        .saturating_sub(blocks)
        .max(store.get_meta_u64("reset_cutoff_block")?.unwrap_or(0) + 1);
    if shared.last_scanned_block.load(Ordering::Relaxed) == 0 && from > 1 {
        commit(
            tx,
            shared,
            0,
            from - 1,
            shared.ingest_lagged.load(Ordering::Relaxed),
        )
        .await?;
    }
    while from <= head.block {
        let to = (from + CHUNK - 1).min(head.block);
        let lagged = shared.ingest_lagged.load(Ordering::Relaxed);
        relay(tx, reader.chunk(head, store, from, to).await?)?;
        commit(tx, shared, from, to, lagged).await?;
        from = to + 1;
    }
    Ok(())
}

pub async fn run(client: rpc::Client, tx: broadcast::Sender<RawLog>, shared: Shared, store: Store) {
    let mut reader = Reader::new();
    reader.ws = Some(client.clone());
    let mut health = RecoveryHealth::default();
    let mut last_tail = std::time::Instant::now() - Duration::from_secs(6);
    loop {
        let generation = client.stream_snapshot().generation;
        let result = async {
            let head = reader.head().await?;
            health.head_block = head.block;
            health.transport = if head.wss_generation.is_some() {
                "public_wss_read_fallback"
            } else {
                "public_http"
            }
            .into();
            let floor = store.get_meta_u64("reset_cutoff_block")?.unwrap_or(0);
            let mut cursor = shared.last_scanned_block.load(Ordering::Relaxed);
            if cursor == 0 {
                // Empty database + SEED_BLOCKS=0 means start now, as before.
                commit(
                    &tx,
                    &shared,
                    0,
                    head.block,
                    shared.ingest_lagged.load(Ordering::Relaxed),
                )
                .await?;
                cursor = head.block;
            }
            // Do not let hours of historical catch-up delay discovery of today's
            // current new coins, or detection of a silent logs subscription.
            if head.block.saturating_sub(cursor) > CHUNK * 2
                && last_tail.elapsed() >= Duration::from_secs(5)
            {
                let from = head.block.saturating_sub(12).max(floor + 1);
                if from <= head.block {
                    let logs = reader.chunk(head, &store, from, head.block).await?;
                    let newest = logs.iter().map(|l| l.block_number).max().unwrap_or(0);
                    client.verify_scan(generation, head.block, head.block, newest);
                    relay(&tx, logs)?;
                }
                last_tail = std::time::Instant::now();
            }
            if let Some((from, to)) = range(cursor, floor, head.block) {
                let lagged = shared.ingest_lagged.load(Ordering::Relaxed);
                let logs = reader.chunk(head, &store, from, to).await?;
                let newest = logs.iter().map(|l| l.block_number).max().unwrap_or(0);
                client.verify_scan(generation, head.block, to, newest);
                let count = logs.len();
                relay(&tx, logs)?;
                commit(&tx, &shared, from, to, lagged).await?;
                if head.block.saturating_sub(cursor) > CHUNK * 2 {
                    eprintln!(
                        "[recovery] 完整补扫 {from}..{to}：{count} 条；剩余 {} 块",
                        head.block.saturating_sub(to)
                    );
                }
            }
            Ok::<(), String>(())
        }
        .await;
        health.scanned_block = shared.last_scanned_block.load(Ordering::Relaxed);
        health.gap_blocks = health.head_block.saturating_sub(health.scanned_block);
        health.stream = Some(client.stream_snapshot());
        match result {
            Ok(()) => {
                health.consecutive_failures = 0;
                health.next_retry_at_ms = 0;
                health.last_success_at_ms = rpc::unix_ms();
                health.last_error = None;
                health.state = if health.gap_blocks > CHUNK {
                    "catching_up"
                } else {
                    "verified"
                }
                .into();
            }
            Err(error) => {
                if health.last_error.as_deref() != Some(&error) {
                    eprintln!("[warn] 补扫失败，进度不推进，自动重试：{error}");
                }
                health.state = "retrying".into();
                health.consecutive_failures = health.consecutive_failures.saturating_add(1);
                health.next_retry_at_ms = rpc::unix_ms()
                    + retry_delay(health.consecutive_failures, &error).as_millis() as u64;
                health.last_error = Some(error);
            }
        }
        *shared.ingestion.write().await = health.clone();
        let delay = if health.state == "retrying" {
            Duration::from_millis(health.next_retry_at_ms.saturating_sub(rpc::unix_ms()))
        } else if health.state == "catching_up" {
            Duration::from_secs(1)
        } else {
            POLL
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {},
            _ = shared.recovery_wake.notified() => {
                // Explicit user reset supplies a new persisted scan boundary.
                // Recheck immediately, not after the old 60s cooldown. This is
                // not an automatic history skip; only the actor can reset it.
                reader = Reader::new();
                reader.ws = Some(client.clone());
                health.consecutive_failures = 0;
                health.next_retry_at_ms = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_head_validation_and_backoff_are_bounded() {
        let now = rpc::unix_ms();
        assert!(checked_head(
            &json!({"number":"0x1","timestamp":format!("0x{:x}",now/1000)}),
            now
        )
        .is_ok());
        for ts in [0, now / 1000 - 60, now / 1000 + 60] {
            assert!(checked_head(
                &json!({"number":"0x1","timestamp":format!("0x{ts:x}")}),
                now
            )
            .is_err());
        }
        assert!(checked_head(&Value::Null, now).is_err());
        assert_eq!(
            retry_delay(1, "Rate limit exceeded"),
            Duration::from_secs(10)
        );
        assert_eq!(retry_delay(2, "HTTP 429"), Duration::from_secs(20));
        assert_eq!(retry_delay(999, "rate limit"), Duration::from_secs(60));
        assert_eq!(retry_delay(1, "network failure"), Duration::from_secs(3));
    }

    #[tokio::test]
    async fn reset_uses_independent_http_and_skips_limited_or_stale_nodes() {
        let limited = axum::Router::new().route(
            "/",
            axum::routing::post(|| async {
                axum::Json(json!({"error":{"message":"rate limit exceeded"}}))
            }),
        );
        let stale = axum::Router::new().route(
            "/",
            axum::routing::post(|| async {
                axum::Json(json!({"result":{"number":"0x64","timestamp":"0x1"}}))
            }),
        );
        let good = axum::Router::new().route("/",axum::routing::post(|axum::Json(v):axum::Json<Value>|async move {
            assert_eq!(v["method"],"eth_getBlockByNumber");
            axum::Json(json!({"result":{"number":"0x65","timestamp":format!("0x{:x}",rpc::unix_ms()/1000)}}))
        }));
        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for app in [limited, stale, good] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(format!("http://{}/", listener.local_addr().unwrap()));
            servers.push(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
        }
        let reader = Reader::with_endpoints(endpoints.clone());
        let head = reader.reset_head().await.unwrap();
        assert_eq!(head.block, 101);
        assert_eq!(head.source, endpoints[2]);
        let bad = Reader::with_endpoints(endpoints[..2].to_vec());
        assert!(bad.reset_head().await.is_err());
        for server in servers {
            server.abort();
        }
    }

    #[tokio::test]
    async fn blocked_http_can_use_generation_fenced_readonly_ws() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_endpoint = format!("http://{}/", http_listener.local_addr().unwrap());
        let http_server = tokio::spawn(async move {
            axum::serve(
                http_listener,
                axum::Router::new().route(
                    "/",
                    axum::routing::post(|| async { axum::http::StatusCode::FORBIDDEN }),
                ),
            )
            .await
            .unwrap();
        });
        let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_endpoint = format!("ws://{}", ws_listener.local_addr().unwrap());
        let ws_server = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            while let Some(Ok(Message::Text(body))) = ws.next().await {
                let v: Value = serde_json::from_str(&body).unwrap();
                let result = match v["method"].as_str().unwrap() {
                    "eth_subscribe" => json!(format!("sub-{}", v["id"])),
                    "eth_getBlockByNumber" => {
                        json!({"number":"0x64","timestamp":format!("0x{:x}",rpc::unix_ms()/1000)})
                    }
                    "eth_getLogs" => json!([]),
                    other => panic!("Unexpected non-read-only method: {other}"),
                };
                ws.send(Message::Text(
                    json!({"id":v["id"],"result":result}).to_string(),
                ))
                .await
                .unwrap();
            }
        });
        let (logs, _logs_rx) = broadcast::channel(16);
        let (status, _status_rx) = tokio::sync::watch::channel("initial".into());
        let client =
            rpc::spawn_with_endpoints(logs, status, vec!["0xfeed".into()], vec![ws_endpoint]).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !client.is_live() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let mut reader = Reader::with_endpoints(vec![http_endpoint]);
        reader.ws = Some(client);
        let head = reader.head().await.unwrap();
        assert_eq!(head.endpoint, usize::MAX);
        assert!(head.wss_generation.is_some());
        assert!(reader
            .chunk(head, &Store::memory().unwrap(), 90, 100)
            .await
            .unwrap()
            .is_empty());
        let generation = head.wss_generation.unwrap();
        assert!(reader
            .ws_read(generation + 1, "eth_getLogs", json!([]))
            .await
            .is_err());
        assert!(reader
            .ws_read(generation, "eth_sendRawTransaction", json!([]))
            .await
            .is_err());
        http_server.abort();
        ws_server.abort();
    }

    #[tokio::test]
    async fn historical_zero_timestamp_trade_gets_its_header_before_relay() {
        let app = axum::Router::new().route("/", axum::routing::post(|axum::Json(v): axum::Json<Value>| async move {
            let result = if v["method"] == "eth_getBlockByNumber" {
                json!({"number":"0x1","timestamp":"0x7b"})
            } else if v["params"][0]["address"] == crate::config::FOUR_MEME_TOKEN_MANAGER2 {
                json!([{"address":crate::config::FOUR_MEME_TOKEN_MANAGER2,"transactionHash":"0xabc",
                    "blockNumber":"0x1","transactionIndex":"0x0","logIndex":"0x0",
                    "topics":[crate::abi::topic0(crate::config::SIG_FOUR_TOKEN_PURCHASE)],
                    "data":format!("0x{}", "00".repeat(32 * 8))}])
            } else { json!([]) };
            axum::Json(json!({"result":result}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let reader = Reader::with_endpoints(vec![endpoint]);
        let logs = reader
            .chunk(
                Head {
                    endpoint: 0,
                    block: 1000,
                    wss_generation: None,
                },
                &Store::memory().unwrap(),
                1,
                1,
            )
            .await
            .unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].topic0, "__block_timestamp");
        assert_eq!(
            u64::from_be_bytes(logs[0].data.clone().try_into().unwrap()),
            123
        );
        assert_eq!(
            logs[1].topic0,
            crate::abi::topic0(crate::config::SIG_FOUR_TOKEN_PURCHASE)
        );
        server.abort();
    }

    #[test]
    fn recovery_cursor_is_contiguous_and_reset_fenced() {
        assert_eq!(range(100, 0, 200), Some((99, 148)));
        assert_eq!(range(100, 150, 200), Some((151, 200)));
        assert_eq!(range(100, 200, 200), None);
        assert_eq!(range(200, 0, 150), None);
    }
    #[test]
    fn invalid_results_are_not_empty_successful_ranges() {
        for value in [Value::Null, json!({}), json!([{"data":"0x"}])] {
            assert!(parse_logs(&value, 1, 2).is_err());
        }
        assert!(parse_logs(&json!([]), 1, 2).unwrap().is_empty());
    }
    #[test]
    fn checkpoint_payload_cannot_claim_a_different_block() {
        let mut log = checkpoint(3, 8, 42);
        assert_eq!(decode_checkpoint(&log), Some((3, 8, 42)));
        log.block_number = 9;
        assert_eq!(decode_checkpoint(&log), None);
        assert_eq!(decode_checkpoint(&checkpoint(8, 3, 0)), None);
    }

    #[tokio::test]
    async fn public_http_errors_cannot_be_accepted_as_empty_logs() {
        let app = axum::Router::new().route(
            "/",
            axum::routing::post(|| async {
                axum::Json(
                    json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limit"}}),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let reader = Reader::with_endpoints(vec![endpoint]);
        let filter = launchpads::Registry::default()
            .fixed_backfill_filters()
            .remove(0);
        assert!(reader
            .logs(
                Head {
                    endpoint: 0,
                    block: 100,
                    wss_generation: None
                },
                &filter,
                90,
                100
            )
            .await
            .is_err());
        assert!(reader
            .at(0, "eth_sendRawTransaction", json!([]))
            .await
            .is_err());
        server.abort();
    }

    #[tokio::test]
    async fn failed_filter_does_not_relay_partial_chunk_or_checkpoint() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let app = axum::Router::new().route(
            "/",
            axum::routing::post(move || {
                let seen = seen.clone();
                async move {
                    if seen.fetch_add(1, Ordering::Relaxed) == 0 {
                        axum::Json(json!({"result":[]}))
                    } else {
                        axum::Json(json!({"error":{"message":"blocked"}}))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let reader = Reader::with_endpoints(vec![endpoint]);
        assert!(reader
            .chunk(
                Head {
                    endpoint: 0,
                    block: 100,
                    wss_generation: None
                },
                &Store::memory().unwrap(),
                90,
                100
            )
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        server.abort();
    }
}
