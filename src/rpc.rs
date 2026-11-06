//! WSS 客户端：单条连接复用「日志订阅」与「JSON-RPC 查询」，断线自动重连。
//!
//! 设计依据（实测于 wss://bsc-rpc.publicnode.com）：
//! - 同一条连接上 `eth_subscribe` 与 `eth_call` 可以混用，互不干扰；
//! - 单个订阅用 `topics: [[a, b, c]]` 形式即可 OR 匹配多种事件；
//! - BSC 约 0.4 秒出一个块，3 秒 K 线粒度有充足样本。

use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot, watch, Semaphore};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::abi;
use crate::config;

const HEADS_SUBSCRIPTION_ID: u64 = 1;

/// 一条已剥离订阅信封的链上日志。
///
/// 只保留 `topic0`：本项目订阅的六类事件实测全部为非 indexed 签名
/// （日志 topics 长度为 1，参数一律编码在 data 中），因此不需要 `topics[1..]`。
///
/// `tx_index` / `log_index` 用于还原链上真实执行顺序。注意**不能**用 tx_hash
/// 排序代替：实测同一区块内按哈希字典序排列与按 (transactionIndex, logIndex)
/// 排列结果不同，顺序错了会让 K 线的开/收盘价张冠李戴，进而影响阳线判定。
#[derive(Debug, Clone)]
pub struct RawLog {
    pub address: String,
    pub tx_hash: String,
    /// 已知的交易发起人。实时订阅通知不携带该字段，通常为 None，由引擎异步
    /// 补查；测试或未来能直接提供完整交易对象的数据源可预先填入。
    pub tx_from: Option<String>,
    pub block_number: u64,
    pub tx_index: u64,
    pub log_index: u64,
    pub topic0: String,
    /// 完整 topics；带 indexed 参数的平台事件由适配器从这里取地址。
    pub topics: Vec<String>,
    pub data: Vec<u8>,
}

/// 可克隆的 RPC 句柄。断线期间的调用会立即失败返回，由调用方决定重试。
#[derive(Clone)]
pub struct Client {
    req_tx: mpsc::UnboundedSender<(Value, oneshot::Sender<Value>)>,
    next_id: Arc<AtomicU64>,
    live: Arc<AtomicBool>,
    progress: Arc<StreamProgress>,
    reconnect_tx: mpsc::Sender<(u64, String)>,
}

#[derive(Default)]
struct StreamProgress {
    generation: AtomicU64,
    connected_at_ms: AtomicU64,
    last_log_at_ms: AtomicU64,
    last_log_block: AtomicU64,
    last_header_at_ms: AtomicU64,
    last_header_block: AtomicU64,
    verified_at_ms: AtomicU64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamSnapshot {
    pub generation: u64,
    pub last_log_at_ms: u64,
    pub last_log_block: u64,
    pub last_header_at_ms: u64,
    pub last_header_block: u64,
}

pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// ACKs, actual headers and logs have separate clocks. Pongs/RPC replies do not
/// prove either subscription works. Silence alone is NOT proof of missing logs.
struct SubscriptionWatch {
    started: Instant,
    header_at: Instant,
    log_at: Instant,
    logs_id: Option<String>,
    heads_id: Option<String>,
}

impl SubscriptionWatch {
    fn new(now: Instant) -> Self {
        Self {
            started: now,
            header_at: now,
            log_at: now,
            logs_id: None,
            heads_id: None,
        }
    }
    fn ready(&self) -> bool {
        self.logs_id.is_some() && self.heads_id.is_some()
    }
    fn failure(&self, now: Instant) -> Option<&'static str> {
        if !self.ready() && now.duration_since(self.started) >= Duration::from_secs(10) {
            Some("订阅 ACK 超时（logs/newHeads 必须分别确认）")
        } else if self.ready() && now.duration_since(self.header_at) >= Duration::from_secs(20) {
            Some("区块头订阅停止推进（RPC/Pong 不代表链上在线）")
        } else {
            None
        }
    }
}

/// `tx.from` 专用 HTTP RPC 客户端。
///
/// 订阅 WSS 只负责接收日志与必要的低频查询；高频买入的真实发起人
/// 通过独立 HTTP 连接池查询。这样即使某个 HTTP 节点限流，也不会拖慢
/// 实时订阅的消费。
#[derive(Clone)]
pub struct SenderRpc {
    http: reqwest::Client,
    health: EndpointHealth,
    permits: Arc<Semaphore>,
    sender_batch: Arc<tokio::sync::OnceCell<mpsc::Sender<SenderRequest>>>,
}

type SenderRequest = (String, oneshot::Sender<Result<String, String>>, Instant);

/// 端点失败的性质。
///
/// 必须区分「传输层不可达」与「已正常应答但数据尚不可见」：后者说明节点
/// 活着、只是同步略微滞后，计入冷却会把健康节点误杀。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointFault {
    /// 连接超时、连接被拒、TLS 失败、JSON 解析失败、HTTP 非 2xx。
    Transport,
    /// 节点已应答，只是该交易或区块尚未可见。
    Lagging,
}

#[derive(Debug, Default, Clone)]
struct EndpointState {
    failure_streak: u32,
    cooldown_until: Option<Instant>,
    /// 已经历的退避轮数，用于把冷却时长按 2 的幂次拉长。
    cooldown_rounds: u32,
}

/// 退避轮数上限，防止位移溢出；对应时长早已被 `max_cooldown` 截断。
const MAX_BACKOFF_ROUNDS: u32 = 16;

/// 按退避轮数计算冷却时长：`base * 2^rounds`，封顶于 `cap`。
fn backoff_delay(base: Duration, rounds: u32, cap: Duration) -> Duration {
    let mut delay = base;
    for _ in 0..rounds.min(MAX_BACKOFF_ROUNDS) {
        delay = delay.saturating_mul(2);
        if delay >= cap {
            return cap;
        }
    }
    delay.min(cap)
}

/// HTTP RPC 端点健康度。
///
/// 实测（2026-09-27）三个候选端点里有两个 binance dataseed 域名 TCP 连接
/// 全部超时，而批量查询固定从 index 0 串行尝试，于是每批都先耗满
/// `connect_timeout` 2 秒才回落到可用节点，把 `tx.from` 查询 p50 抬到
/// 2399ms（27214 样本）。本类型按连续失败次数把端点暂时移出候选，
/// 冷却期满自动放行探测流量复活，端点恢复后无需人工干预。
#[derive(Clone)]
pub struct EndpointHealth {
    states: Arc<Mutex<Vec<EndpointState>>>,
    next_start: Arc<AtomicU64>,
    failure_threshold: u32,
    cooldown: Duration,
    max_cooldown: Duration,
}

impl EndpointHealth {
    pub fn new(len: usize) -> Self {
        Self::build(
            len,
            config::HTTP_RPC_FAILURE_THRESHOLD,
            config::HTTP_RPC_COOLDOWN,
            config::HTTP_RPC_MAX_COOLDOWN,
        )
    }

    /// 指定阈值与基础冷却时长；上限自动推导为基础时长的 4 倍，
    /// 供测试在不等待真实冷却的情况下验证行为。
    #[cfg(test)]
    fn with_policy(len: usize, failure_threshold: u32, cooldown: Duration) -> Self {
        Self::build(len, failure_threshold, cooldown, cooldown * 4)
    }

    /// 完整指定阈值、基础冷却与冷却上限，供退避测试精确控制时长。
    #[cfg(test)]
    fn with_policy_capped(
        len: usize,
        failure_threshold: u32,
        cooldown: Duration,
        max_cooldown: Duration,
    ) -> Self {
        Self::build(len, failure_threshold, cooldown, max_cooldown)
    }

    fn build(
        len: usize,
        failure_threshold: u32,
        cooldown: Duration,
        max_cooldown: Duration,
    ) -> Self {
        Self {
            states: Arc::new(Mutex::new(vec![EndpointState::default(); len])),
            next_start: Arc::new(AtomicU64::new(0)),
            failure_threshold: failure_threshold.max(1),
            cooldown,
            max_cooldown,
        }
    }

    /// 按给定起点把端点拆成「健康组」与「冷却兜底组」。
    ///
    /// 健康组内部按起点轮换排序，避免固定压第一个；冷却组按到期时间从近到
    /// 远排列，优先探测最可能已恢复的端点。
    fn partition(&self, start: usize) -> (Vec<usize>, Vec<usize>) {
        let states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let len = states.len();
        if len == 0 {
            return (Vec::new(), Vec::new());
        }
        let now = Instant::now();
        let mut healthy: Vec<usize> = Vec::new();
        let mut cooling: Vec<(Instant, usize)> = Vec::new();
        for offset in 0..len {
            let index = (start + offset) % len;
            match states[index].cooldown_until {
                Some(until) if until > now => cooling.push((until, index)),
                _ => healthy.push(index),
            }
        }
        cooling.sort_by_key(|(until, _)| *until);
        (
            healthy,
            cooling.into_iter().map(|(_, index)| index).collect(),
        )
    }

    fn take_start(&self) -> usize {
        self.next_start.fetch_add(1, Ordering::Relaxed) as usize
    }

    /// 返回按优先级排序的端点下标（健康在前、冷却兜底在后）。仅供测试使用。
    ///
    /// **不要在生产路径调用本方法**：它把两组合并成扁平序列，调用方无法感知
    /// 分组边界，于是「健康节点已应答、只是数据尚不可见」会让循环继续走到
    /// 冷却端点，每批白等一个连接超时，使冷却优化在常见路径上失效。
    /// 生产路径一律使用 [`Self::candidate_groups`] 或
    /// [`Self::candidates_partitioned`]，并遵守健康组边界。
    #[cfg(test)]
    pub fn candidates(&self) -> Vec<usize> {
        let (healthy, cooling) = self.candidates_partitioned();
        let mut all = healthy;
        all.extend(cooling);
        all
    }

    /// 把候选拆成「健康组」与「冷却兜底组」两个独立列表。
    ///
    /// 调用方应先试健康组，**只有健康组中没有任何端点正常应答时**才退到
    /// 兜底组。否则「健康节点已应答、只是数据尚不可见」会让循环继续走到
    /// 冷却端点，又白等一个连接超时 —— 对新挖出的交易 Lagging 是常态，
    /// 该退化会频繁发生，使冷却优化在常见路径上失效。
    pub fn candidates_partitioned(&self) -> (Vec<usize>, Vec<usize>) {
        self.partition(self.take_start())
    }

    /// 以数组形式返回两组候选，便于调用方按组迭代。
    pub fn candidate_groups(&self) -> [Vec<usize>; 2] {
        let (healthy, cooling) = self.candidates_partitioned();
        [healthy, cooling]
    }

    /// 记录一次传输层失败。
    ///
    /// 两种进入冷却的情形：
    /// 1. 连续失败达到阈值（容忍单次网络抖动）；
    /// 2. **冷却已到期后的探测再次失败** —— 立即重新冷却，不重新攒阈值。
    ///
    /// 第 2 条是关键：若冷却时长固定，低流量时段每批请求都可能恰好落在
    /// 「冷却刚到期」的窗口上，死端点被反复探测，优化收益归零。因此每轮
    /// 探测失败都把冷却时长按 2 的幂次拉长，封顶于 `max_cooldown`。
    pub fn report_fault(&self, index: usize, fault: EndpointFault) {
        if fault == EndpointFault::Lagging {
            return;
        }
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let Some(state) = states.get_mut(index) else {
            return;
        };
        let now = Instant::now();
        // cooldown_until 已过期说明本次失败来自「到期探测」，无需再攒阈值。
        let probing = state.cooldown_until.is_some_and(|until| until <= now);
        state.failure_streak += 1;
        if probing || state.failure_streak >= self.failure_threshold {
            let delay = backoff_delay(self.cooldown, state.cooldown_rounds, self.max_cooldown);
            state.cooldown_until = Some(now + delay);
            state.cooldown_rounds = state.cooldown_rounds.saturating_add(1);
            state.failure_streak = 0;
        }
    }

    /// 记录一次成功，立即清空失败计数、冷却与退避轮数。
    pub fn report_success(&self, index: usize) {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = states.get_mut(index) {
            state.failure_streak = 0;
            state.cooldown_until = None;
            state.cooldown_rounds = 0;
        }
    }

    /// 当前处于冷却中的端点数，用于观测。
    pub fn cooling_count(&self) -> usize {
        let now = Instant::now();
        self.states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|s| s.cooldown_until.is_some_and(|until| until > now))
            .count()
    }
}

async fn sender_request(
    http: reqwest::Client,
    endpoint: String,
    hashes: Vec<String>,
) -> Result<HashMap<String, String>, String> {
    let body: Vec<_> = hashes.iter().enumerate().map(|(id, hash)|
        json!({"jsonrpc":"2.0","id":id,"method":"eth_getTransactionByHash","params":[hash]})
    ).collect();
    let response = http
        .post(&endpoint)
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "{}: HTTP {}",
            endpoint_host(&endpoint),
            response.status()
        ));
    }
    let value: Value = response.json().await.map_err(|e| e.to_string())?;
    let results = value
        .as_array()
        .ok_or_else(|| "invalid sender batch response".to_string())?;
    let mut resolved = HashMap::new();
    for result in results {
        if let (Some(id), Some(from)) = (result["id"].as_u64(), result["result"]["from"].as_str()) {
            if let Some(hash) = hashes.get(id as usize) {
                resolved.insert(hash.clone(), from.to_lowercase());
            }
        }
    }
    Ok(resolved)
}

async fn sender_batches(
    mut rx: mpsc::Receiver<SenderRequest>,
    http: reqwest::Client,
    endpoints: Vec<String>,
    health: EndpointHealth,
) {
    let mut jobs = tokio::task::JoinSet::new();
    loop {
        while jobs.len() >= 4 {
            let _ = jobs.join_next().await;
        }
        let Some(first) = rx.recv().await else {
            break;
        };
        let mut batch = vec![first];
        tokio::time::sleep(Duration::from_millis(20)).await;
        while batch.len() < 64 {
            match rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        batch.retain(|(_, reply, _)| !reply.is_closed());
        if batch.is_empty() {
            continue;
        }
        let client = http.clone();
        let endpoints = endpoints.clone();
        let health = health.clone();
        jobs.spawn(async move {
            let started = Instant::now();
            let queue_ms = batch.iter().map(|(_, _, at)| at.elapsed().as_millis()).max().unwrap_or(0);
            let mut pending = batch;
            let mut last_error = "sender batch unavailable".to_string();
            let mut healthy_responded = false;
            for (group_index, group) in health.candidate_groups().into_iter().enumerate() {
                // A valid-but-lagging healthy response must never trigger dead-node fallback.
                if group_index > 0 && healthy_responded { break; }
                let mut requests = tokio::task::JoinSet::new();
                let mut candidates = group.into_iter();
                let mut launch_at = Instant::now();
                loop {
                    pending.retain(|(_, reply, _)| !reply.is_closed());
                    if pending.is_empty() { break; }
                    if requests.is_empty() || (requests.len() < 2 && Instant::now() >= launch_at) {
                        if let Some(index) = candidates.next() {
                            if let Some(endpoint) = endpoints.get(index).cloned() {
                                let hashes = pending.iter().map(|(hash, _, _)| hash.clone()).collect();
                                let client = client.clone();
                                requests.spawn(async move { (index, sender_request(client, endpoint, hashes).await) });
                                // At most one delayed hedge; fast requests remain single-node.
                                launch_at = Instant::now() + Duration::from_millis(300);
                            }
                        } else if requests.is_empty() { break; }
                    }
                    if requests.is_empty() { continue; }
                    let outcome = if requests.len() == 1 {
                        tokio::select! {
                            result = requests.join_next() => result,
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(launch_at)),
                                if candidates.len() > 0 => { continue; }
                        }
                    } else { requests.join_next().await };
                    let Some(Ok((index, outcome))) = outcome else { continue; };
                    match outcome {
                        Ok(resolved) => {
                            health.report_success(index);
                            if group_index == 0 { healthy_responded = true; }
                            pending = pending.into_iter().filter_map(|(hash, reply, at)| {
                                if let Some(from) = resolved.get(&hash) { let _ = reply.send(Ok(from.clone())); None }
                                else { Some((hash, reply, at)) }
                            }).collect();
                            // Partial results may need another healthy node immediately.
                            launch_at = Instant::now();
                        }
                        Err(error) => {
                            last_error = error;
                            health.report_fault(index, EndpointFault::Transport);
                            launch_at = Instant::now();
                        }
                    }
                }
                // Read-only losing requests are cancelled, never counted as transport faults.
                requests.abort_all();
                if pending.is_empty() { break; }
            }
            let unresolved = pending.len();
            for (_, reply, _) in pending { let _ = reply.send(Err(last_error.clone())); }
            if queue_ms > 1000 || started.elapsed() > Duration::from_secs(2) || unresolved > 0 {
                eprintln!("[sender-batch] queue_ms={queue_ms} rpc_ms={} unresolved={unresolved} cooling={}",
                    started.elapsed().as_millis(), health.cooling_count());
            }
        });
        while jobs.try_join_next().is_some() {}
    }
    while jobs.join_next().await.is_some() {}
}

impl SenderRpc {
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(3))
            .pool_idle_timeout(Duration::from_secs(60))
            .build()
            .expect("HTTP RPC client configuration must be valid");
        Self {
            http,
            // 健康度按端点表长度建立；批量任务与本结构体共享同一份状态，
            // 批量路径探测出的冷却会立刻作用于 eth_call / 时间戳查询。
            health: EndpointHealth::new(config::HTTP_RPC_ENDPOINTS.len()),
            // 限制热门币爆发时的并发，但不让单个慢请求阻塞其他交易。
            permits: Arc::new(Semaphore::new(32)),
            sender_batch: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// 当前处于冷却中的端点数，用于状态接口观测。
    pub fn cooling_endpoints(&self) -> usize {
        self.health.cooling_count()
    }

    /// 查询单笔交易的真实发起人。使用小响应的
    /// `eth_getTransactionByHash`，不再为一笔买入下载整个 BSC 区块。
    pub async fn transaction_sender(&self, tx_hash: &str) -> Result<String, String> {
        let queue = self
            .sender_batch
            .get_or_init(|| async {
                let (tx, rx) = mpsc::channel(2048);
                tokio::spawn(sender_batches(
                    rx,
                    self.http.clone(),
                    config::HTTP_RPC_ENDPOINTS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    self.health.clone(),
                ));
                tx
            })
            .await;
        let (reply, result) = oneshot::channel();
        queue
            .send((tx_hash.to_string(), reply, Instant::now()))
            .await
            .map_err(|_| "sender worker closed".to_string())?;
        result
            .await
            .map_err(|_| "sender worker dropped reply".to_string())?
    }

    #[allow(dead_code)]
    pub async fn transaction_sender_single(&self, tx_hash: &str) -> Result<String, String> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| "HTTP RPC 并发门已关闭".to_string())?;
        let endpoints = config::HTTP_RPC_ENDPOINTS;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
        });
        let mut errors = Vec::new();
        let mut answered = false;
        for (group_index, group) in self.health.candidate_groups().into_iter().enumerate() {
            // 健康组已有节点正常应答（只是数据尚不可见）时不再进冷却组：
            // 死端点同样没有更新的数据，却要先耗满连接超时。
            if group_index > 0 && answered {
                break;
            }
            for endpoint_index in group {
                let endpoint = endpoints[endpoint_index];
                let response = match self.http.post(endpoint).json(&body).send().await {
                    Ok(response) => response,
                    Err(error) => {
                        errors.push(format!("{}: {error}", endpoint_host(endpoint)));
                        self.health
                            .report_fault(endpoint_index, EndpointFault::Transport);
                        continue;
                    }
                };
                if !response.status().is_success() {
                    errors.push(format!(
                        "{}: HTTP {}",
                        endpoint_host(endpoint),
                        response.status()
                    ));
                    self.health
                        .report_fault(endpoint_index, EndpointFault::Transport);
                    continue;
                }
                let value: Value = match response.json().await {
                    Ok(value) => value,
                    Err(error) => {
                        errors.push(format!("{}: JSON {error}", endpoint_host(endpoint)));
                        self.health
                            .report_fault(endpoint_index, EndpointFault::Transport);
                        continue;
                    }
                };
                if let Some(error) = value.get("error") {
                    errors.push(format!("{}: {error}", endpoint_host(endpoint)));
                    self.health
                        .report_fault(endpoint_index, EndpointFault::Transport);
                    continue;
                }
                // 到这里节点已正常应答，后续分支都属于「数据尚未可见」而非端点故障。
                self.health.report_success(endpoint_index);
                answered = true;
                let Some(result) = value.get("result").filter(|v| !v.is_null()) else {
                    // 新区块刚到时 HTTP 节点可能尚未同步，继续尝试下一个。
                    errors.push(format!("{}: 交易尚未可见", endpoint_host(endpoint)));
                    continue;
                };
                if let Some(sender) = result.get("from").and_then(Value::as_str) {
                    return Ok(sender.to_lowercase());
                }
                errors.push(format!("{}: 缺少 from", endpoint_host(endpoint)));
            }
        }
        Err(errors.join("; "))
    }

    /// 通过独立 HTTP RPC 读取区块时间。实时日志订阅不再为了时间戳在主事件
    /// 循环里等待 WSS 请求；即使公共节点短暂变慢，也只会让对应区块留在有序
    /// 待处理队列中，不会阻塞后续日志的接收。
    pub async fn block_timestamp(&self, block_number: u64) -> Result<u64, String> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| "HTTP RPC 并发门已关闭".to_string())?;
        let endpoints = config::HTTP_RPC_ENDPOINTS;
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{block_number:x}"), false],
        });
        let mut errors = Vec::new();
        let mut answered = false;
        for (group_index, group) in self.health.candidate_groups().into_iter().enumerate() {
            // 健康组已有节点正常应答（只是区块尚不可见）时不再进冷却组：
            // 死端点同样没有该区块，却要先耗满连接超时。本方法是 newHeads
            // 未按时给出时间戳时的兜底，多等 2 秒会卡住 pending_decoded_logs
            // 的 FIFO 队头，进而拖慢整条日志处理链路。
            if group_index > 0 && answered {
                break;
            }
            for endpoint_index in group {
                let endpoint = endpoints[endpoint_index];
                let response = match self.http.post(endpoint).json(&body).send().await {
                    Ok(response) => response,
                    Err(error) => {
                        errors.push(format!("{}: {error}", endpoint_host(endpoint)));
                        self.health
                            .report_fault(endpoint_index, EndpointFault::Transport);
                        continue;
                    }
                };
                if !response.status().is_success() {
                    errors.push(format!(
                        "{}: HTTP {}",
                        endpoint_host(endpoint),
                        response.status()
                    ));
                    self.health
                        .report_fault(endpoint_index, EndpointFault::Transport);
                    continue;
                }
                let value: Value = match response.json().await {
                    Ok(value) => value,
                    Err(error) => {
                        errors.push(format!("{}: JSON {error}", endpoint_host(endpoint)));
                        self.health
                            .report_fault(endpoint_index, EndpointFault::Transport);
                        continue;
                    }
                };
                if let Some(error) = value.get("error") {
                    errors.push(format!("{}: {error}", endpoint_host(endpoint)));
                    self.health
                        .report_fault(endpoint_index, EndpointFault::Transport);
                    continue;
                }
                self.health.report_success(endpoint_index);
                answered = true;
                let Some(timestamp) = value
                    .get("result")
                    .filter(|v| !v.is_null())
                    .and_then(|block| block.get("timestamp"))
                    .and_then(Value::as_str)
                    .and_then(|raw| u64::from_str_radix(raw.trim_start_matches("0x"), 16).ok())
                else {
                    errors.push(format!(
                        "{}: 区块或 timestamp 尚不可见",
                        endpoint_host(endpoint)
                    ));
                    continue;
                };
                return Ok(timestamp);
            }
        }
        Err(errors.join("; "))
    }

    async fn request_result(&self, method: &str, params: Value) -> Result<Value, String> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| "HTTP RPC 并发门已关闭".to_string())?;
        let endpoints = config::HTTP_RPC_ENDPOINTS;
        let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let mut errors = Vec::new();
        let mut answered = false;
        for (group_index, group) in self.health.candidate_groups().into_iter().enumerate() {
            // 健康组已有节点正常应答时不再进冷却组：换节点不会改变 revert 结果，
            // 却要先耗满连接超时。本通道承载税率与 BNB 价格读取，白等会直接
            // 推迟 A 币定级与市值换算。
            if group_index > 0 && answered {
                break;
            }
            for endpoint_index in group {
                let endpoint = endpoints[endpoint_index];
                let response = match self.http.post(endpoint).json(&body).send().await {
                    Ok(response) => response,
                    Err(error) => {
                        errors.push(format!("{}: {error}", endpoint_host(endpoint)));
                        self.health
                            .report_fault(endpoint_index, EndpointFault::Transport);
                        continue;
                    }
                };
                if !response.status().is_success() {
                    errors.push(format!(
                        "{}: HTTP {}",
                        endpoint_host(endpoint),
                        response.status()
                    ));
                    self.health
                        .report_fault(endpoint_index, EndpointFault::Transport);
                    continue;
                }
                let value: Value = match response.json().await {
                    Ok(value) => value,
                    Err(error) => {
                        errors.push(format!("{}: JSON {error}", endpoint_host(endpoint)));
                        self.health
                            .report_fault(endpoint_index, EndpointFault::Transport);
                        continue;
                    }
                };
                // 节点已正常应答，以下分支均不代表端点故障，一律记为成功以免误杀：
                // eth_call 的 error 可能是合约 revert，
                // 属于正常业务结果，若计入冷却，一个会 revert 的合约就能拖垮健康节点。
                self.health.report_success(endpoint_index);
                answered = true;
                if let Some(error) = value.get("error") {
                    errors.push(format!("{}: {error}", endpoint_host(endpoint)));
                    continue;
                }
                let Some(result) = value.get("result").filter(|value| !value.is_null()) else {
                    errors.push(format!("{}: result 尚不可见", endpoint_host(endpoint)));
                    continue;
                };
                return Ok(result.clone());
            }
        }
        Err(errors.join("; "))
    }

    pub async fn eth_call(&self, to: &str, data: &str) -> Result<Vec<u8>, String> {
        let value = self
            .request_result("eth_call", json!([{ "to": to, "data": data }, "latest"]))
            .await?;
        let raw = value.as_str().unwrap_or("0x");
        abi::unhex(raw).ok_or_else(|| "HTTP RPC 返回值不是合法 hex".to_string())
    }

    pub async fn fetch_u128(&self, contract: &str, signature: &str) -> Result<u128, String> {
        let raw = self.eth_call(contract, &abi::selector(signature)).await?;
        abi::words(&raw)
            .first()
            .map(abi::Word::as_u128)
            .ok_or_else(|| format!("{contract} 的 {signature} 返回值无法解码"))
    }

    /// TaxToken5: feeRate is bps. TaxToken8/9: feeRateBuy/Sell are percentages.
    /// A revert/missing field for a known tax type is unknown, never silently zero.
    pub async fn fetch_four_taxes(&self, token: &str) -> Result<(u64, u64), String> {
        let token = token.trim().to_ascii_lowercase();
        if !token.starts_with("0x") || abi::unhex(&token).is_none_or(|b| b.len() != 20) {
            return Err("invalid Four.meme token address".into());
        }
        let data = format!(
            "{}{}{}",
            abi::selector("_tokenInfos(address)"),
            "0".repeat(24),
            &token[2..]
        );
        let info = self
            .eth_call(config::FOUR_MEME_TOKEN_MANAGER2, &data)
            .await?;
        match abi::four_creator_type(&info, &token)
            .ok_or("未知 Four.meme TokenInfo / creatorType")?
        {
            5 => {
                let rate = u64::try_from(self.fetch_u128(&token, "feeRate()").await?)
                    .map_err(|_| "税率溢出")?;
                Ok((rate, rate))
            }
            8 | 9 => {
                let (buy, sell) = tokio::try_join!(
                    self.fetch_u128(&token, "feeRateBuy()"),
                    self.fetch_u128(&token, "feeRateSell()")
                )?;
                let to_bps = |v: u128| -> Result<u64, String> {
                    u64::try_from(v.checked_mul(100).ok_or("税率溢出")?)
                        .map_err(|_| "税率溢出".into())
                };
                Ok((to_bps(buy)?, to_bps(sell)?))
            }
            _ => Ok((0, 0)),
        }
    }

    pub async fn fetch_symbol(&self, token: &str) -> Option<String> {
        let raw = self
            .eth_call(token, &abi::selector("symbol()"))
            .await
            .ok()?;
        decode_erc20_string(&raw)
    }

    pub async fn fetch_name(&self, token: &str) -> Option<String> {
        let raw = self.eth_call(token, &abi::selector("name()")).await.ok()?;
        decode_erc20_string(&raw)
    }

    pub async fn fetch_four_quote(&self, token: &str) -> Option<String> {
        let token_bytes = abi::unhex(token)?;
        if token_bytes.len() != 20 {
            return None;
        }
        let mut data = abi::selector("getTokenInfo(address)");
        data.push_str(&"0".repeat(24));
        data.push_str(&abi::hex(&token_bytes));
        let raw = self.eth_call(config::FOUR_MEME_HELPER3, &data).await.ok()?;
        abi::words(&raw).get(2).map(abi::Word::as_address)
    }
}

impl Client {
    /// 发起一次 JSON-RPC 调用，带超时。失败或超时返回 Err。
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        if !self.live.load(Ordering::Relaxed) {
            return Err("wss 未连接".into());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send((body, tx))
            .map_err(|_| "wss 请求通道已关闭".to_string())?;
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(v)) => {
                if v.is_null() {
                    Err("连接在请求途中断开".into())
                } else if let Some(e) = v.get("error") {
                    Err(e
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("rpc error")
                        .to_string())
                } else {
                    Ok(v.get("result").cloned().unwrap_or(Value::Null))
                }
            }
            Ok(Err(_)) => Err("响应通道已关闭".into()),
            Err(_) => Err("rpc 超时".into()),
        }
    }

    /// 只读合约调用，返回解码用的原始字节。
    pub async fn eth_call(&self, to: &str, data: &str) -> Result<Vec<u8>, String> {
        let v = self
            .call("eth_call", json!([{ "to": to, "data": data }, "latest"]))
            .await?;
        let s = v.as_str().unwrap_or("0x");
        abi::unhex(s).ok_or_else(|| "返回值不是合法 hex".to_string())
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }

    pub fn stream_snapshot(&self) -> StreamSnapshot {
        StreamSnapshot {
            generation: self.progress.generation.load(Ordering::Relaxed),
            last_log_at_ms: self.progress.last_log_at_ms.load(Ordering::Relaxed),
            last_log_block: self.progress.last_log_block.load(Ordering::Relaxed),
            last_header_at_ms: self.progress.last_header_at_ms.load(Ordering::Relaxed),
            last_header_block: self.progress.last_header_block.load(Ordering::Relaxed),
        }
    }

    /// Called ONLY after a successful, fresh HTTP scan of all platform filters.
    /// A quiet launchpad is healthy; actual missing logs trigger a generation-
    /// fenced reconnect so an old probe cannot tear down a newer connection.
    pub fn verify_scan(&self, generation: u64, head: u64, scanned_to: u64, newest_log: u64) {
        if !self.is_live() {
            return;
        }
        if generation != self.progress.generation.load(Ordering::Relaxed) {
            return;
        }
        let now = unix_ms();
        let last_at = self
            .progress
            .last_log_at_ms
            .load(Ordering::Relaxed)
            .max(self.progress.connected_at_ms.load(Ordering::Relaxed));
        let last_block = self.progress.last_log_block.load(Ordering::Relaxed);
        if scanned_to + 2 >= head {
            self.progress.verified_at_ms.store(now, Ordering::Relaxed);
        }
        if newest_log > last_block
            && head.saturating_sub(newest_log) <= 100
            && now.saturating_sub(last_at) >= 15_000
        {
            let _ = self.reconnect_tx.try_send((
                generation,
                format!("HTTP 核验发现日志订阅漏流（WSS={last_block}，HTTP={newest_log}）"),
            ));
        }
    }
}

/// 测试专用：构造一个不连接网络的离线客户端。
///
/// 引擎单测需要 `Client` 但绝不能真的发起网络请求。用这个构造出的客户端
/// `is_live()` 恒为 false，所有 RPC 调用立即返回 Err，正好覆盖「链上查询不可用」
/// 的降级路径——本项目税率与成交全部来自事件流，离线客户端不影响判定逻辑。
#[cfg(test)]
pub fn offline_client() -> Client {
    let (req_tx, _req_rx) = mpsc::unbounded_channel();
    let (reconnect_tx, _reconnect_rx) = mpsc::channel(1);
    Client {
        req_tx,
        next_id: Arc::new(AtomicU64::new(HEADS_SUBSCRIPTION_ID)),
        live: Arc::new(AtomicBool::new(false)),
        progress: Arc::new(StreamProgress::default()),
        reconnect_tx,
    }
}

/// 启动 WSS 后台任务。
///
/// - `logs_tx`：订阅到的日志广播给引擎；
/// - `status_tx`：连接状态变化（用于前端健康显示）；
/// 返回可克隆的 `Client`。
pub async fn spawn(
    logs_tx: broadcast::Sender<RawLog>,
    status_tx: watch::Sender<String>,
    subscription_topics: Vec<String>,
) -> Client {
    spawn_with_endpoints(
        logs_tx,
        status_tx,
        subscription_topics,
        config::WSS_ENDPOINTS
            .iter()
            .map(|s| s.to_string())
            .collect(),
    )
    .await
}

pub(crate) async fn spawn_with_endpoints(
    logs_tx: broadcast::Sender<RawLog>,
    status_tx: watch::Sender<String>,
    subscription_topics: Vec<String>,
    endpoints: Vec<String>,
) -> Client {
    let (req_tx, req_rx) = mpsc::unbounded_channel();
    let (reconnect_tx, reconnect_rx) = mpsc::channel(1);
    let client = Client {
        req_tx,
        next_id: Arc::new(AtomicU64::new(HEADS_SUBSCRIPTION_ID)),
        live: Arc::new(AtomicBool::new(false)),
        progress: Arc::new(StreamProgress::default()),
        reconnect_tx,
    };
    let live = client.live.clone();
    let progress = client.progress.clone();
    tokio::spawn(async move {
        supervise(
            req_rx,
            reconnect_rx,
            logs_tx,
            status_tx,
            live,
            progress,
            subscription_topics,
            endpoints,
        )
        .await;
    });
    client
}

/// 连接守护：断开后指数退避重连，并在重连成功后重新订阅。
///
/// 收发合并在同一个 `select!` 循环里，而不是拆成写任务 + 读任务：
/// `req_rx` 必须留在外层函数，才能在多次重连迭代中继续被使用。
async fn supervise(
    mut req_rx: mpsc::UnboundedReceiver<(Value, oneshot::Sender<Value>)>,
    mut reconnect_rx: mpsc::Receiver<(u64, String)>,
    logs_tx: broadcast::Sender<RawLog>,
    status_tx: watch::Sender<String>,
    live: Arc<AtomicBool>,
    progress: Arc<StreamProgress>,
    subscription_topics: Vec<String>,
    endpoints: Vec<String>,
) {
    let mut backoff = Duration::from_millis(500);
    // 当前尝试的端点下标；连接成功即固定，失败则在下一轮轮换到下一个。
    let mut ep_idx = 0usize;
    loop {
        let endpoint = &endpoints[ep_idx % endpoints.len()];
        let _ = status_tx.send(format!("connecting: {}", endpoint_host(endpoint)));
        match tokio::time::timeout(Duration::from_secs(12), connect_async(endpoint)).await {
            Ok(Ok((ws, _resp))) => {
                backoff = Duration::from_millis(500);
                live.store(false, Ordering::Relaxed);
                let generation = progress.generation.fetch_add(1, Ordering::Relaxed) + 1;
                progress.connected_at_ms.store(unix_ms(), Ordering::Relaxed);
                progress.last_log_at_ms.store(0, Ordering::Relaxed);
                progress.last_log_block.store(0, Ordering::Relaxed);
                progress.last_header_at_ms.store(0, Ordering::Relaxed);
                progress.last_header_block.store(0, Ordering::Relaxed);
                progress.verified_at_ms.store(0, Ordering::Relaxed);
                let _ = status_tx.send(format!("subscribing: {}", endpoint_host(endpoint)));
                let mut subscriptions = SubscriptionWatch::new(Instant::now());
                let (mut sink, mut stream) = ws.split();
                // 每次重连都重建 pending 表：旧连接的 in-flight 请求已无法完成。
                let mut pending: HashMap<u64, oneshot::Sender<Value>> = HashMap::new();

                // 一个 topic0 OR 订阅覆盖所有已注册 Launchpad。地址不在节点侧过滤：
                // GeniusFun 每个币都会创建新的 curve 地址，只有收到 TokenLaunched
                // 后才知道；适配器会按 factory/manager/已登记 source_ref 二次校验。
                let sub = json!({
                    "jsonrpc":"2.0","id":0,"method":"eth_subscribe",
                    "params":["logs", { "topics":[subscription_topics] }]
                });
                if sink.send(Message::Text(sub.to_string())).await.is_err() {
                    live.store(false, Ordering::Relaxed);
                    continue;
                }
                // Headers supply timestamps without one HTTP round trip per block.
                let headers = json!({"jsonrpc":"2.0","id":HEADS_SUBSCRIPTION_ID,"method":"eth_subscribe","params":["newHeads"]});
                if sink.send(Message::Text(headers.to_string())).await.is_err() {
                    continue;
                }

                let mut last_msg = Instant::now();
                let mut watchdog = tokio::time::interval(Duration::from_secs(2));
                watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut checking = false;
                let reason = loop {
                    // 读空闲超时：订阅成功后节点会持续推事件，长时间静默说明连接已哑。
                    let idle = Duration::from_secs(120).saturating_sub(last_msg.elapsed());
                    tokio::select! {
                        Some((requested_generation, reason)) = reconnect_rx.recv() => {
                            if requested_generation == generation { break reason; }
                        }
                        _ = watchdog.tick() => {
                            let now = Instant::now();
                            if let Some(reason) = subscriptions.failure(now) { break reason.to_string(); }
                            let verified = unix_ms().saturating_sub(progress.verified_at_ms.load(Ordering::Relaxed)) < 15_000;
                            let needs_check = subscriptions.ready() && now.duration_since(subscriptions.log_at) > Duration::from_secs(30) && !verified;
                            if needs_check != checking {
                                checking = needs_check;
                                let state = if checking { "checking: 平台日志静默，等待 HTTP 补扫核验".to_string() }
                                    else { format!("live: {}", endpoint_host(endpoint)) };
                                if subscriptions.ready() { let _ = status_tx.send(state); }
                            }
                            pending.retain(|_, reply| !reply.is_closed());
                        }
                        // 出站：转发引擎的 JSON-RPC 请求。
                        Some((body, reply)) = req_rx.recv() => {
                            let id = body.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
                            if id != 0 {
                                pending.insert(id, reply);
                            }
                            if sink.send(Message::Text(body.to_string())).await.is_err() {
                                break "发送失败".to_string();
                            }
                        }
                        // 入站：响应与订阅通知。
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    last_msg = Instant::now();
                                    let Ok(v) = serde_json::from_str::<Value>(&text) else {
                                        continue;
                                    };
                                    if let Some(id) = v.get("id").and_then(|i| i.as_u64()) {
                                        if id == 0 || id == HEADS_SUBSCRIPTION_ID {
                                            let Some(sub_id) = v.get("result").and_then(Value::as_str).filter(|s| !s.is_empty()) else {
                                                break format!("订阅被拒绝: {v}");
                                            };
                                            if id == 0 { subscriptions.logs_id = Some(sub_id.to_string()); }
                                            else { subscriptions.heads_id = Some(sub_id.to_string()); }
                                            if subscriptions.ready() {
                                                live.store(true, Ordering::Relaxed);
                                                let _ = status_tx.send(format!("live: {}", endpoint_host(endpoint)));
                                            }
                                        } else if let Some(reply) = pending.remove(&id) { let _ = reply.send(v); }
                                        continue;
                                    }
                                    let incoming_id = v.pointer("/params/subscription").and_then(Value::as_str);
                                    if incoming_id.is_some() && incoming_id == subscriptions.heads_id.as_deref() {
                                      if let Some(header) = v.pointer("/params/result") {
                                        if let (Some(number), Some(timestamp)) = (header["number"].as_str(), header["timestamp"].as_str()) {
                                            if let (Ok(block), Ok(ts)) = (u64::from_str_radix(number.trim_start_matches("0x"), 16), u64::from_str_radix(timestamp.trim_start_matches("0x"), 16)) {
                                                if unix_ms().saturating_sub(ts.saturating_mul(1000)) > 30_000 {
                                                    break "区块头时间落后超过 30 秒".to_string();
                                                }
                                                if block > progress.last_header_block.load(Ordering::Relaxed) {
                                                    subscriptions.header_at = Instant::now();
                                                    progress.last_header_at_ms.store(unix_ms(), Ordering::Relaxed);
                                                    progress.last_header_block.store(block, Ordering::Relaxed);
                                                }
                                                let _ = logs_tx.send(RawLog {
                                                    address: String::new(), tx_hash: String::new(), tx_from: None,
                                                    block_number: block, tx_index: 0, log_index: 0,
                                                    topic0: "__block_timestamp".into(), topics: vec![], data: ts.to_be_bytes().to_vec(),
                                                });
                                                continue;
                                            }
                                        }
                                    }
                                    }
                                    if incoming_id.is_some() && incoming_id == subscriptions.logs_id.as_deref() {
                                      if let Some(log) = parse_notification(&v) {
                                        subscriptions.log_at = Instant::now();
                                        progress.last_log_at_ms.store(unix_ms(), Ordering::Relaxed);
                                        progress.last_log_block.fetch_max(log.block_number, Ordering::Relaxed);
                                        // 订阅方全部退出时 send 会失败，无需处理。
                                        let _ = logs_tx.send(log);
                                      }
                                    }
                                }
                                // Pong/Ping 由 tungstenite 自动应答，仅刷新空闲计时。
                                Some(Ok(_)) => last_msg = Instant::now(),
                                Some(Err(e)) => break format!("websocket 错误: {e}"),
                                None => break "连接流结束".to_string(),
                            }
                        }
                        _ = tokio::time::sleep(idle.max(Duration::from_secs(1))) => {
                            break "读超时（120 秒无消息）".to_string();
                        }
                    }
                };

                live.store(false, Ordering::Relaxed);
                // 唤醒所有等待中的调用，避免它们一直挂到超时。
                for (_, reply) in pending.drain() {
                    let _ = reply.send(Value::Null);
                }
                let _ = status_tx.send(format!("reconnecting: {reason}"));
                eprintln!("[warn] WSS 重订阅：{reason}");
                ep_idx += 1;
            }
            Ok(Err(e)) => {
                let _ = status_tx.send(format!(
                    "reconnecting: {} 握手失败 {e}",
                    endpoint_host(endpoint)
                ));
                // 握手失败说明该端点此刻不可达，下一轮换到候选列表中的下一个；
                // 连接建立后才断开的（上方分支）保持当前端点优先重试。
                ep_idx += 1;
            }
            Err(_) => {
                let _ = status_tx.send(format!(
                    "reconnecting: {} 握手超时",
                    endpoint_host(endpoint)
                ));
                ep_idx += 1;
            }
        }
        // 断线期间到达的请求立即失败返回，不让调用方干等。
        while let Ok((_, reply)) = req_rx.try_recv() {
            let _ = reply.send(Value::Null);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}

/// 从 RPC URL 中取主机名，仅用于日志与状态展示，避免完整 URL 刷屏。
fn endpoint_host(url: &str) -> String {
    url.trim_start_matches("wss://")
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or(url)
        .to_string()
}

/// 把 `eth_subscription` 通知解析成 RawLog。非日志通知返回 None。
fn parse_notification(v: &Value) -> Option<RawLog> {
    let params = v.get("params")?;
    if params.get("subscription").is_none() {
        return None;
    }
    parse_log_result(params.get("result")?)
}

pub(crate) fn parse_log_result(r: &Value) -> Option<RawLog> {
    if r.get("removed").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    // 只取 topic0（事件签名）。本项目六类事件的其余参数全在 data 中。
    let topics: Vec<String> = r
        .get("topics")?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str().map(str::to_lowercase))
        .collect();
    let topic0 = topics.first()?.clone();
    let data = abi::unhex(r.get("data")?.as_str()?)?;
    Some(RawLog {
        address: r.get("address")?.as_str()?.to_lowercase(),
        tx_hash: r.get("transactionHash")?.as_str()?.to_lowercase(),
        tx_from: None,
        block_number: u64::from_str_radix(
            r.get("blockNumber")?.as_str()?.trim_start_matches("0x"),
            16,
        )
        .ok()?,
        tx_index: u64::from_str_radix(
            r.get("transactionIndex")?
                .as_str()?
                .trim_start_matches("0x"),
            16,
        )
        .ok()?,
        log_index: u64::from_str_radix(r.get("logIndex")?.as_str()?.trim_start_matches("0x"), 16)
            .ok()?,
        topic0,
        topics,
        data,
    })
}

/// 链上真实执行顺序的排序键。回填时必须用它，不能用 tx_hash。
pub fn chain_order(l: &RawLog) -> (u64, u64, u64) {
    (l.block_number, l.tx_index, l.log_index)
}

// ---------------------------------------------------------------- 派生查询

/// 读 Chainlink BNB/USD（8 位精度）。实测返回 0x117d5f5b2b => $751.33。
pub async fn fetch_bnb_usd(c: &Client) -> Result<f64, String> {
    let raw = c
        .eth_call(config::BNB_USD_FEED, &abi::selector("latestAnswer()"))
        .await?;
    let v = abi::decode_int256(&raw).ok_or("无法解码预言机返回值")?;
    if v <= 0 {
        return Err("预言机返回非正数".into());
    }
    Ok(v as f64 / 1e8)
}

fn decode_erc20_string(raw: &[u8]) -> Option<String> {
    let w = abi::words(raw);
    let first = w.first()?;
    let off = first.as_u128() as usize;
    let bytes = if off + 32 <= raw.len() {
        let mut lw = [0u8; 32];
        lw.copy_from_slice(raw.get(off..off + 32)?);
        let len = abi::Word(lw).as_u128() as usize;
        if len == 0 || len > 256 {
            return None;
        }
        raw.get(off + 32..off + 32 + len)?
    } else {
        // 老式 ERC20 可能直接返回 bytes32。
        first.0.split(|b| *b == 0).next()?
    };
    let value = String::from_utf8_lossy(bytes).trim().to_string();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod subscription_tests {
    use super::*;

    #[tokio::test]
    async fn websocket_requires_both_acks_routes_subscription_ids_and_does_not_collide_with_rpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (heads_sent, heads_seen) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            for expected in [0, HEADS_SUBSCRIPTION_ID] {
                let body = ws.next().await.unwrap().unwrap();
                let value: Value = serde_json::from_str(body.to_text().unwrap()).unwrap();
                assert_eq!(value["id"].as_u64(), Some(expected));
            }
            ws.send(Message::Text(
                json!({"id":HEADS_SUBSCRIPTION_ID,"result":"heads"}).to_string(),
            ))
            .await
            .unwrap();
            heads_sent.send(()).unwrap();
            release_rx.await.unwrap();
            ws.send(Message::Text(json!({"id":0,"result":"logs"}).to_string()))
                .await
                .unwrap();
            let raw = json!({"address":"0x123","transactionHash":"0xabc","blockNumber":"0x64",
                "transactionIndex":"0x0","logIndex":"0x1","topics":["0xfeed"],"data":"0x"});
            for id in ["unknown", "logs"] {
                ws.send(Message::Text(
                    json!({"method":"eth_subscription","params":{"subscription":id,"result":raw}})
                        .to_string(),
                ))
                .await
                .unwrap();
            }
            ws.send(Message::Text(
                json!({"params":{"subscription":"heads","result":{
                "number":"0x65","timestamp":format!("0x{:x}",unix_ms()/1000)}}})
                .to_string(),
            ))
            .await
            .unwrap();
            while let Some(Ok(Message::Text(body))) = ws.next().await {
                let value: Value = serde_json::from_str(&body).unwrap();
                let id = value["id"].as_u64().unwrap();
                assert!(id > HEADS_SUBSCRIPTION_ID);
                ws.send(Message::Text(json!({"id":id,"result":"0x65"}).to_string()))
                    .await
                    .unwrap();
            }
        });
        let (logs, mut received) = broadcast::channel(32);
        let (status, _status_rx) = watch::channel("initial".into());
        let client =
            spawn_with_endpoints(logs, status, vec!["0xfeed".into()], vec![endpoint]).await;
        heads_seen.await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !client.is_live(),
            "A single ACK must not enable RPC/readiness"
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !client.is_live() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.topic0, "0xfeed");
        assert_eq!(second.topic0, "__block_timestamp");
        assert!(
            received.try_recv().is_err(),
            "Unknown subscription must be ignored"
        );
        assert_eq!(
            client.call("eth_blockNumber", json!([])).await.unwrap(),
            "0x65"
        );
        assert_eq!(client.stream_snapshot().last_log_block, 100);
        assert_eq!(client.stream_snapshot().last_header_block, 101);
        server.abort();
    }

    #[test]
    fn both_subscriptions_must_ack_even_if_headers_arrive() {
        let now = Instant::now();
        let mut watch = SubscriptionWatch::new(now);
        watch.heads_id = Some("heads".into());
        watch.header_at = now + Duration::from_secs(9);
        assert!(!watch.ready());
        assert!(watch
            .failure(now + Duration::from_secs(11))
            .unwrap()
            .contains("ACK"));
        watch.logs_id = Some("logs".into());
        assert!(watch.ready());
        assert!(watch.failure(now + Duration::from_secs(11)).is_none());
    }

    #[test]
    fn logs_and_pongs_cannot_mask_stalled_headers() {
        let now = Instant::now();
        let mut watch = SubscriptionWatch::new(now);
        watch.logs_id = Some("logs".into());
        watch.heads_id = Some("heads".into());
        watch.log_at = now + Duration::from_secs(25);
        // Receipt of ordinary messages never changes header_at.
        assert!(watch
            .failure(now + Duration::from_secs(25))
            .unwrap()
            .contains("区块头"));
    }

    #[test]
    fn quiet_logs_with_fresh_headers_do_not_force_reconnect() {
        let now = Instant::now();
        let mut watch = SubscriptionWatch::new(now);
        watch.logs_id = Some("logs".into());
        watch.heads_id = Some("heads".into());
        watch.header_at = now + Duration::from_secs(120);
        assert!(watch.failure(now + Duration::from_secs(121)).is_none());
    }

    #[tokio::test]
    async fn http_gap_detection_is_evidence_based_and_generation_fenced() {
        let mut client = offline_client();
        let (tx, mut rx) = mpsc::channel(1);
        client.reconnect_tx = tx;
        client.live.store(true, Ordering::Relaxed);
        client.progress.generation.store(2, Ordering::Relaxed);
        client
            .progress
            .connected_at_ms
            .store(unix_ms() - 20_000, Ordering::Relaxed);
        client.progress.last_log_block.store(100, Ordering::Relaxed);
        client.verify_scan(1, 200, 200, 195); // Old connection's delayed probe.
        client.verify_scan(2, 200, 200, 0); // A quiet interval, not a broken stream.
        client.verify_scan(2, 200, 200, 95); // Historical duplicate, already received.
        client.verify_scan(2, 500, 200, 195); // Ancient catch-up, not current silence.
        assert!(rx.try_recv().is_err());
        client.verify_scan(2, 200, 200, 195);
        let (generation, reason) = rx.try_recv().unwrap();
        assert_eq!(generation, 2);
        assert!(reason.contains("漏流"));
        client
            .progress
            .connected_at_ms
            .store(unix_ms(), Ordering::Relaxed);
        client.verify_scan(2, 200, 200, 195); // New subscription gets startup grace.
        assert!(rx.try_recv().is_err());
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    #[tokio::test]
    async fn delayed_hedge_wins_without_cooling_cancelled_slow_node() {
        async fn endpoint(
            delay: Duration,
            hits: Arc<AtomicUsize>,
        ) -> (String, tokio::task::JoinHandle<()>) {
            let app = axum::Router::new().route("/", axum::routing::post(move |axum::Json(body): axum::Json<Vec<Value>>| {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(delay).await;
                    axum::Json(body.iter().map(|item|
                        json!({"id":item["id"],"result":{"from":item["params"][0]}})).collect::<Vec<_>>())
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            (
                url,
                tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                }),
            )
        }
        let slow_hits = Arc::new(AtomicUsize::new(0));
        let fast_hits = Arc::new(AtomicUsize::new(0));
        let (slow, slow_server) = endpoint(Duration::from_secs(2), slow_hits.clone()).await;
        let (fast, fast_server) = endpoint(Duration::ZERO, fast_hits.clone()).await;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let health = EndpointHealth::with_policy(2, 1, Duration::from_secs(30));
        let (tx, rx) = mpsc::channel(32);
        let worker = tokio::spawn(sender_batches(rx, http, vec![slow, fast], health.clone()));
        let (reply, result) = oneshot::channel();
        tx.send(("wallet0".into(), reply, Instant::now()))
            .await
            .unwrap();
        let from = tokio::time::timeout(Duration::from_millis(1200), result)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(from, "wallet0");
        assert_eq!(slow_hits.load(Ordering::Relaxed), 1);
        assert_eq!(fast_hits.load(Ordering::Relaxed), 1);
        assert_eq!(
            health.cooling_count(),
            0,
            "losing read request cancellation is not a node fault"
        );
        worker.abort();
        slow_server.abort();
        fast_server.abort();
    }

    #[tokio::test]
    async fn fast_batch_does_not_send_speculative_second_rpc_request() {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app =
            axum::Router::new().route(
                "/",
                axum::routing::post(move |axum::Json(body): axum::Json<Vec<Value>>| {
                    counter.fetch_add(1, Ordering::Relaxed);
                    async move {
                        axum::Json(body.iter().map(|item|
                json!({"id":item["id"],"result":{"from":item["params"][0]}})).collect::<Vec<_>>())
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (tx, rx) = mpsc::channel(32);
        let worker = tokio::spawn(sender_batches(
            rx,
            reqwest::Client::new(),
            vec![url.clone(), url],
            EndpointHealth::new(2),
        ));
        let (reply, result) = oneshot::channel();
        tx.send(("wallet0".into(), reply, Instant::now()))
            .await
            .unwrap();
        assert_eq!(result.await.unwrap().unwrap(), "wallet0");
        assert_eq!(hits.load(Ordering::Relaxed), 1);
        worker.abort();
        server.abort();
    }

    #[tokio::test]
    async fn batch_maps_reversed_ids_and_skips_cancelled_requests() {
        let count = Arc::new(AtomicU64::new(0));
        let seen = count.clone();
        let app = axum::Router::new().route("/", axum::routing::post(move |axum::Json(body): axum::Json<Vec<Value>>| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(body.len() as u64, Ordering::Relaxed);
                axum::Json(body.into_iter().rev().map(|item| json!({"id":item["id"], "result":{"from":item["params"][0]}})).collect::<Vec<_>>())
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (tx, rx) = mpsc::channel(32);
        let mut replies = Vec::new();
        for i in 0..12 {
            let (reply, result) = oneshot::channel();
            tx.send((format!("wallet{i}"), reply, Instant::now()))
                .await
                .unwrap();
            if i == 5 {
                drop(result);
            } else {
                replies.push((i, result));
            }
        }
        drop(tx);
        sender_batches(
            rx,
            reqwest::Client::new(),
            vec![endpoint],
            EndpointHealth::new(1),
        )
        .await;
        for (i, reply) in replies {
            assert_eq!(reply.await.unwrap().unwrap(), format!("wallet{i}"));
        }
        assert_eq!(count.load(Ordering::Relaxed), 11);
        server.abort();
    }

    /// 复现生产故障形态：候选表首位是「能连上但永不响应」的死端点，第二位正常。
    ///
    /// 这正是实测到的 binance dataseed 表现（TCP 可连、无响应，每批先耗满
    /// 连接超时）。断言冷却生效后**死端点不再被建立任何新连接**，即提速来自
    /// 跳过而非快速失败。
    #[tokio::test]
    async fn batch_stops_touching_dead_endpoint_after_cooldown() {
        // 死端点：接受连接后一直挂着不响应，迫使客户端走满超时。
        let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_endpoint = format!("http://{}/", dead_listener.local_addr().unwrap());
        let dead_hits = Arc::new(AtomicUsize::new(0));
        let dead_counter = dead_hits.clone();
        let dead_server = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                match dead_listener.accept().await {
                    Ok((stream, _)) => {
                        dead_counter.fetch_add(1, Ordering::Relaxed);
                        held.push(stream); // 保持连接不关闭，模拟无响应节点
                    }
                    Err(_) => break,
                }
            }
        });

        // 健康端点：正常应答，按 JSON-RPC id 回填 from。
        let healthy_app = axum::Router::new().route(
            "/",
            axum::routing::post(|axum::Json(body): axum::Json<Vec<Value>>| async move {
                axum::Json(
                    body.iter()
                        .map(
                            |item| json!({"id": item["id"], "result": {"from": item["params"][0]}}),
                        )
                        .collect::<Vec<_>>(),
                )
            }),
        );
        let healthy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_endpoint = format!("http://{}/", healthy_listener.local_addr().unwrap());
        let healthy_server = tokio::spawn(async move {
            axum::serve(healthy_listener, healthy_app).await.unwrap();
        });

        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(150))
            .timeout(Duration::from_millis(150))
            .build()
            .unwrap();
        // 阈值 1：一次失败即冷却，便于测试；生产为 2 次以容忍抖动。
        let health = EndpointHealth::with_policy(2, 1, Duration::from_secs(30));
        let (tx, rx) = mpsc::channel(32);
        let worker = tokio::spawn(sender_batches(
            rx,
            http,
            vec![dead_endpoint, healthy_endpoint],
            health.clone(),
        ));

        async fn run_batch(tx: &mpsc::Sender<SenderRequest>, count: usize) -> Vec<String> {
            let mut replies = Vec::new();
            for i in 0..count {
                let (reply, result) = oneshot::channel();
                tx.send((format!("wallet{i}"), reply, Instant::now()))
                    .await
                    .unwrap();
                replies.push(result);
            }
            let mut senders = Vec::new();
            for reply in replies {
                senders.push(reply.await.unwrap().unwrap());
            }
            senders
        }

        // 第一批：仍会先撞死端点，超时后回落健康端点并把它冷却。
        let first = run_batch(&tx, 8).await;
        assert_eq!(first.len(), 8, "首批请求应全部由健康端点兜底成功");
        assert_eq!(first[0], "wallet0");
        assert_eq!(health.cooling_count(), 1, "死端点应已进入冷却");
        let hits_after_first = dead_hits.load(Ordering::Relaxed);
        assert!(hits_after_first >= 1, "首批应至少触碰死端点一次");

        // 第二批及以后：死端点必须被完全跳过。
        for round in 0..3 {
            let senders = run_batch(&tx, 8).await;
            assert_eq!(senders.len(), 8, "第 {round} 轮仍应全部成功");
            assert_eq!(
                dead_hits.load(Ordering::Relaxed),
                hits_after_first,
                "冷却期内不得再向死端点建立任何新连接（第 {round} 轮）"
            );
        }

        drop(tx);
        worker.abort();
        dead_server.abort();
        healthy_server.abort();
    }

    /// 回归测试：健康端点已应答、但只解析出部分交易（Lagging）时，
    /// 不得再去撞冷却中的死端点。
    ///
    /// 这是分组语义存在的全部理由：新挖出的交易在健康节点上「尚不可见」是常态，
    /// 若把冷却端点混在同一序列末尾，循环会因 pending 非空而继续走到死端点，
    /// 每批白等一个连接超时，冷却优化在常见路径上完全失效。
    #[tokio::test]
    async fn partial_healthy_reply_does_not_fall_through_to_cooled_endpoint() {
        // 死端点：接受连接后不响应，用连接计数证明是否被触碰。
        let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_endpoint = format!("http://{}/", dead_listener.local_addr().unwrap());
        let dead_hits = Arc::new(AtomicUsize::new(0));
        let dead_counter = dead_hits.clone();
        let dead_server = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                match dead_listener.accept().await {
                    Ok((stream, _)) => {
                        dead_counter.fetch_add(1, Ordering::Relaxed);
                        held.push(stream);
                    }
                    Err(_) => break,
                }
            }
        });

        // 健康端点：只解析 wallet0，其余留空，模拟「交易尚不可见」。
        let partial_app = axum::Router::new().route(
            "/",
            axum::routing::post(|axum::Json(body): axum::Json<Vec<Value>>| async move {
                axum::Json(
                    body.iter()
                        .filter(|item| item["params"][0] == json!("wallet0"))
                        .map(
                            |item| json!({"id": item["id"], "result": {"from": item["params"][0]}}),
                        )
                        .collect::<Vec<_>>(),
                )
            }),
        );
        let partial_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let partial_endpoint = format!("http://{}/", partial_listener.local_addr().unwrap());
        let partial_server = tokio::spawn(async move {
            axum::serve(partial_listener, partial_app).await.unwrap();
        });

        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(150))
            .timeout(Duration::from_millis(150))
            .build()
            .unwrap();
        // 预先把死端点置于冷却，使本批请求一开始就只有健康组可用。
        let health = EndpointHealth::with_policy(2, 1, Duration::from_secs(30));
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 1, "前置条件：死端点应已在冷却中");

        let (tx, rx) = mpsc::channel(32);
        let worker = tokio::spawn(sender_batches(
            rx,
            http,
            vec![dead_endpoint, partial_endpoint],
            health.clone(),
        ));

        let mut replies = Vec::new();
        for i in 0..6 {
            let (reply, result) = oneshot::channel();
            tx.send((format!("wallet{i}"), reply, Instant::now()))
                .await
                .unwrap();
            replies.push(result);
        }
        drop(tx);

        let mut ok_count = 0;
        for (i, reply) in replies.into_iter().enumerate() {
            let outcome = reply.await.unwrap();
            if i == 0 {
                assert_eq!(outcome.unwrap(), "wallet0", "健康端点应解析出 wallet0");
                ok_count += 1;
            } else {
                assert!(outcome.is_err(), "未解析项应返回错误而非挂起");
            }
        }
        assert_eq!(ok_count, 1);

        assert_eq!(
            dead_hits.load(Ordering::Relaxed),
            0,
            "健康组已应答后，不得再向冷却中的死端点建立任何连接"
        );
        assert_eq!(health.cooling_count(), 1, "死端点应仍在冷却中");

        worker.abort();
        dead_server.abort();
        partial_server.abort();
    }
}

#[cfg(test)]
mod endpoint_health_tests {
    use super::*;
    use std::collections::HashSet;

    /// 每次调用都固定起点，便于断言顺序（candidates 内部会轮换起点）。
    fn candidates_from(health: &EndpointHealth, start: usize) -> Vec<usize> {
        health.next_start.store(start as u64, Ordering::Relaxed);
        health.candidates()
    }

    #[test]
    fn candidates_cover_every_endpoint_exactly_once() {
        let health = EndpointHealth::new(3);
        for start in 0..3 {
            let list = candidates_from(&health, start);
            assert_eq!(list.len(), 3, "候选数必须等于端点数");
            assert_eq!(
                list.iter().copied().collect::<HashSet<_>>(),
                (0..3).collect::<HashSet<_>>(),
                "候选必须是端点下标的一个排列，不能漏也不能重"
            );
        }
    }

    #[test]
    fn single_transport_fault_below_threshold_keeps_endpoint_hot() {
        // 阈值 2：单次网络抖动不该把端点误杀。
        let health = EndpointHealth::with_policy(3, 2, Duration::from_secs(30));
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 0);
        assert!(
            candidates_from(&health, 0).contains(&0),
            "未达阈值时端点仍应留在候选中"
        );
    }

    #[test]
    fn reaching_threshold_cools_endpoint_and_deprioritises_it() {
        let health = EndpointHealth::with_policy(3, 2, Duration::from_secs(30));
        health.report_fault(0, EndpointFault::Transport);
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 1);
        let list = candidates_from(&health, 0);
        assert_eq!(
            list[0], 1,
            "健康端点必须排在冷却端点之前，避免每批先撞死端点耗满连接超时"
        );
        assert_eq!(*list.last().unwrap(), 0, "冷却端点应沉到末尾兜底");
    }

    #[test]
    fn lagging_fault_never_cools_endpoint() {
        // 「节点已应答但数据尚不可见」不是端点故障，不得降权。
        let health = EndpointHealth::with_policy(3, 1, Duration::from_secs(30));
        for _ in 0..5 {
            health.report_fault(1, EndpointFault::Lagging);
        }
        assert_eq!(health.cooling_count(), 0, "数据滞后不得计入冷却");
        // 让端点 0 真正冷却，验证只有滞后记录的端点 1 仍属健康组、排在它前面。
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 1, "仅端点 0 应进入冷却");
        let list = candidates_from(&health, 0);
        assert_eq!(
            list[0], 1,
            "只有滞后记录的端点必须保持健康优先级，排在冷却端点之前"
        );
        assert_eq!(*list.last().unwrap(), 0, "冷却端点沉到末尾兜底");
    }

    #[test]
    fn success_revives_cooled_endpoint_immediately() {
        let health = EndpointHealth::with_policy(3, 2, Duration::from_secs(30));
        health.report_fault(2, EndpointFault::Transport);
        health.report_fault(2, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 1);
        health.report_success(2);
        assert_eq!(health.cooling_count(), 0, "成功应答必须立即清空冷却");
        assert!(candidates_from(&health, 0).contains(&2));
    }

    #[test]
    fn cooldown_expiry_restores_endpoint_without_manual_intervention() {
        // 用 30ms 冷却模拟到期，端点应自动复活而非永久剔除。
        let health = EndpointHealth::with_policy(2, 1, Duration::from_millis(30));
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 1);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(health.cooling_count(), 0, "冷却到期后应自动恢复候选");
    }

    #[test]
    fn all_endpoints_cooling_still_returns_full_list() {
        // 全冷却时若返回空列表，所有请求会立即失败。
        let health = EndpointHealth::with_policy(3, 1, Duration::from_secs(30));
        for index in 0..3 {
            health.report_fault(index, EndpointFault::Transport);
        }
        assert_eq!(health.cooling_count(), 3);
        let list = candidates_from(&health, 0);
        assert_eq!(list.len(), 3, "全冷却时仍须逐个尝试，不能直接失败");
    }

    #[test]
    fn cooling_endpoints_ordered_by_nearest_expiry() {
        // 先冷却的先到期，应排在冷却组前面，优先被探测复活。
        let health = EndpointHealth::with_policy(4, 1, Duration::from_secs(30));
        health.report_fault(3, EndpointFault::Transport);
        std::thread::sleep(Duration::from_millis(15));
        health.report_fault(2, EndpointFault::Transport);
        let list = candidates_from(&health, 0);
        assert_eq!(&list[0..2], &[0, 1], "健康端点在前");
        assert_eq!(&list[2..4], &[3, 2], "冷却组按到期时间从近到远");
    }

    #[test]
    fn zero_length_endpoint_table_yields_empty_candidates() {
        let health = EndpointHealth::new(0);
        assert!(health.candidates().is_empty());
        health.report_fault(0, EndpointFault::Transport); // 不应 panic
        health.report_success(0);
    }

    #[test]
    fn out_of_range_index_is_ignored_not_panicking() {
        let health = EndpointHealth::with_policy(2, 1, Duration::from_secs(30));
        health.report_fault(99, EndpointFault::Transport);
        health.report_success(99);
        assert_eq!(health.cooling_count(), 0);
    }

    /// 回归测试：冷却时长固定时，低流量时段每批请求都恰好落在「冷却刚到期」
    /// 上，死端点被反复探测、收益归零。到期后的探测若再失败，必须**立即**
    /// 重新冷却并加倍时长，而不是重新从阈值攒起。
    #[test]
    fn probe_failure_after_expiry_recools_immediately_with_doubled_delay() {
        // 阈值 2：验证探测失败无需再攒两次。基础冷却 30ms，上限 120ms。
        let health = EndpointHealth::with_policy_capped(
            1,
            2,
            Duration::from_millis(30),
            Duration::from_millis(120),
        );
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 0, "首次失败未达阈值");
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(health.cooling_count(), 1, "达阈值进入冷却");

        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(health.cooling_count(), 0, "首轮冷却已到期，端点回到候选");

        // 到期探测再次失败：单次即应重新冷却（不需再攒到阈值 2）。
        health.report_fault(0, EndpointFault::Transport);
        assert_eq!(
            health.cooling_count(),
            1,
            "到期探测失败必须立即重新冷却，否则低流量时段收益归零"
        );
    }

    #[test]
    fn repeated_probe_failures_back_off_exponentially_and_cap() {
        let health = EndpointHealth::with_policy_capped(
            1,
            1,
            Duration::from_millis(20),
            Duration::from_millis(80),
        );

        // 每轮到期后再失败，冷却应逐轮拉长：20 → 40 → 80，并被上限截断。
        let mut observed = Vec::new();
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(90));
            assert_eq!(health.cooling_count(), 0, "冷却应已到期");
            health.report_fault(0, EndpointFault::Transport);
            assert_eq!(health.cooling_count(), 1);
            // 通过「睡眠不同时长后是否仍在冷却」间接测量实际冷却时长。
            let still_cooling_at = |d: Duration| -> bool {
                std::thread::sleep(d);
                health.cooling_count() == 1
            };
            observed.push(still_cooling_at(Duration::from_millis(25)));
        }
        // 首轮 20ms 冷却在额外睡 25ms 后已到期；后续轮次更长，应仍在冷却。
        assert!(!observed[0], "首轮冷却最短，25ms 后应已到期");
        assert!(observed[2], "退避后冷却应显著变长");
        assert!(observed[3], "冷却应持续封顶在较长时长");
    }

    #[test]
    fn success_resets_backoff_rounds_to_base_delay() {
        let health = EndpointHealth::with_policy_capped(
            1,
            1,
            Duration::from_millis(20),
            Duration::from_millis(160),
        );
        // 攒多轮退避，把冷却拉长到上限附近。
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(170));
            health.report_fault(0, EndpointFault::Transport);
        }
        // 端点恢复：成功应答必须把退避轮数清零。
        health.report_success(0);
        assert_eq!(health.cooling_count(), 0);
        health.report_fault(0, EndpointFault::Transport);
        // 回到基础冷却 20ms：睡 40ms 后应已到期，而非仍停留在上限。
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(
            health.cooling_count(),
            0,
            "成功后应回到基础冷却，不得保留退避时长"
        );
    }
}
