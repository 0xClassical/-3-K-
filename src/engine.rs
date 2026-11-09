//! 核心引擎：单 actor 持有全部状态，串行处理链上日志、定时 tick 与前端指令。
//!
//! 状态流转（严格对应需求）：
//!
//! ```text
//!   建币事件 ──> Watching（A 币）
//!                   │  买卖税均 <=4%  -> A 币，进入 60 秒观察窗
//!                   │  任一税率  >4%  -> Rejected（永久剔除）
//!                   │
//!                   ├── 观察窗内目标地址买入 + 连续三根阳线 -> Live（B 币，进前端）
//!                   └── 观察窗结束仍未满足        -> Expired（永久剔除，永不再理）
//!
//!   Live ──用户点 X──> Removed（永久剔除，永不再理）
//! ```
//!
//! 「永久剔除」由 `blacklist` 集合承载：一旦进入，该地址后续任何事件都被忽略，
//! 即使它再次发出建币事件也不会复活。
//!
//! 关于「连续」的判定口径：按实际出现且已封口的 K 线计数，不要求桶号相邻。
//! 无成交的 3 秒空档不画 K 线，也不打断连阳；有成交的阴线或平线才清零。
//! 未封口的桶 close 仍可能翻转，不参与判断。A 与 C 使用同一连阳口径。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, watch, RwLock};

use crate::abi::{self, Trade};
use crate::config;
use crate::launchpads::{LaunchPlatform, NewTokenEvent, NormalizedEvent, Registry};
use crate::persistence::{BTiming, PersistedToken, Store};
use crate::rpc::{Client, RawLog};

// ---------------------------------------------------------------- 数据模型

/// K 线（3 秒一根）。`time` 为桶起始的链上 Unix 秒，图表可直接使用。
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// 该桶成交量（报价代币单位，已按 18 位精度折算）。
    pub volume: f64,
    pub trades: u32,
    /// 是否已封口。未封口时 close 仍会变，不参与阳线判定。
    pub closed: bool,
}

/// 一笔成交明细。
///
/// 只承载**报价代币单位**的数值，美元换算统一由前端做。两个原因：
/// 1. 成交到达时 Chainlink 报价可能还没取到（启动握手、回填期间 RPC 被占满），
///    此刻固化下来的 null 会永久留在明细里，即使几秒后价格就绪也不会自愈；
/// 2. K 线纵轴本来就必须在前端按 BNB/USD 缩放，明细若在后端另算一套，
///    同一个币的图表与明细可能因取价时刻不同而对不上。
/// 前端拿到 `bnbUsd` 事件后统一换算，报价代币非原生 BNB 时如实回退到报价单位。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeRecord {
    /// 链上成交时间（Unix 秒）。
    pub ts: u64,
    /// 同一秒内多笔成交的稳定排序键。
    pub seq: u64,
    pub wallet: String,
    /// "buy" / "sell"。
    pub side: &'static str,
    /// 成交代币数量（已格式化的可读字符串）。
    pub token_amount: String,
    /// 成交金额 Q（报价代币，已格式化）。
    pub quote_amount: String,
    /// 成交金额 Q 的数值形式（报价代币单位），供前端做美元换算。
    pub quote_value: f64,
    /// 成交单价 = 有效成交额 / Amount（报价代币单位，即 BNB/Token）。
    /// BUY 取 Q - Fee，SELL 取 Q + Fee，见 `abi::unit_price`。
    pub price: f64,
    /// 该笔成交对应的市值（报价代币单位）= 有效成交额 × totalSupply / Amount。
    pub mcap_quote: f64,
    pub tx: String,
    pub block: u64,
}

/// 每个 CA 唯一、显式且可持久化的生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stage {
    #[serde(rename = "NEW")]
    New,
    #[serde(rename = "A_WATCHING")]
    AWatching,
    #[serde(rename = "B_PENDING")]
    BPending,
    #[serde(rename = "B_ACTIVE")]
    BActive,
    #[serde(rename = "C_WATCHING")]
    CWatching,
    #[serde(rename = "B_REENTRY_PENDING")]
    BReentryPending,
    #[serde(rename = "B_REENTRY_ACTIVE")]
    BReentryActive,
    #[serde(rename = "USER_DISCARDED")]
    UserDiscarded,
    #[serde(rename = "DEAD")]
    Dead,
}

impl Stage {
    // 兼容原有 K 线单元测试与内部术语；实际持久化值仍是上面的显式状态。
    #[allow(non_upper_case_globals)]
    const Watching: Stage = Stage::AWatching;
    #[allow(non_upper_case_globals)]
    #[cfg(test)]
    const Live: Stage = Stage::BPending;

    pub fn is_pending(self) -> bool {
        matches!(self, Stage::BPending | Stage::BReentryPending)
    }
    pub fn is_active(self) -> bool {
        matches!(self, Stage::BActive | Stage::BReentryActive)
    }
    pub fn is_b(self) -> bool {
        self.is_pending() || self.is_active()
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, Stage::UserDiscarded | Stage::Dead)
    }
    fn db_name(self) -> &'static str {
        match self {
            Stage::New => "NEW",
            Stage::AWatching => "A_WATCHING",
            Stage::BPending => "B_PENDING",
            Stage::BActive => "B_ACTIVE",
            Stage::CWatching => "C_WATCHING",
            Stage::BReentryPending => "B_REENTRY_PENDING",
            Stage::BReentryActive => "B_REENTRY_ACTIVE",
            Stage::UserDiscarded => "USER_DISCARDED",
            Stage::Dead => "DEAD",
        }
    }
    fn from_db(s: &str) -> Option<Self> {
        Some(match s {
            "NEW" => Stage::New,
            "A_WATCHING" => Stage::AWatching,
            "B_PENDING" => Stage::BPending,
            "B_ACTIVE" => Stage::BActive,
            "C_WATCHING" => Stage::CWatching,
            "B_REENTRY_PENDING" => Stage::BReentryPending,
            "B_REENTRY_ACTIVE" => Stage::BReentryActive,
            "USER_DISCARDED" => Stage::UserDiscarded,
            "DEAD" => Stage::Dead,
            _ => return None,
        })
    }
}

struct TokenState {
    b_timing: BTiming,
    token: String,
    name: String,
    symbol: String,
    creator: String,
    source_platform: LaunchPlatform,
    creation_block: u64,
    creation_tx_hash: String,
    source_ref: Option<String>,
    total_supply_tokens: f64,
    created_ts: u64,
    /// 观察窗截止的链上时间：仅 bucket 起点 < 此值的 K 线参与晋升判定。
    observe_until_ts: u64,
    /// 观察窗截止的真实时刻，用于实时模式下的到期清理。
    deadline_wall: Instant,

    buy_tax_bps: u64,
    sell_tax_bps: u64,
    /// 是否已计入 A 币或税率剔除统计。幂等守卫：每个币只计一次，
    /// 避免「建币时按 0 税计入 A 币、税率事件到达后又计入剔除」的双重计数。
    ///
    /// 定级时机：税率事件到达时立即定级；若该币是标准币（不发税率事件），
    /// 则在首笔成交或下一次 tick 时补定级。之所以首笔成交可作为「税率已确定」
    /// 的标志：成交必然发生在建币交易的后续交易里，而日志按链上顺序处理，
    /// 因此看到成交时，建币交易内的 QuoteSet / TaxSet / AsymTax 都已处理完毕。
    counted: bool,
    /// 观察窗口内是否有目标地址买入过。只有命中目标买家且达到三连阳，
    /// 两个条件都满足时才晋升为 B 币；两者先后顺序不影响结果。
    target_buyer_seen: bool,
    /// 尚在补查 `tx.from` 的买入交易数。观察窗到期时先等这些查询返回，避免
    /// 发生在第 60 秒内的合法买入因 RPC 结果稍晚到达而被提前剔除。
    pending_sender_lookups: usize,
    /// 观察窗口内是否曾经完成过三根连续阳线。该条件一旦在窗口内成立就锁存，
    /// 即使之后出现阴线，也不能抹掉此前已经发生过的合格三连阳。
    bull_streak_seen: bool,
    bull_streak_value: usize,
    /// 报价代币地址；零地址表示原生 BNB。
    quote_token: String,
    quote_symbol: Option<String>,

    stage: Stage,
    promoted_at: Option<u64>,
    first_b_at: Option<u64>,
    /// 首次 B 生命周期的精确调度时间线（Unix 毫秒）。
    first_qualified_at_ms: Option<u64>,
    first_open_sent_at_ms: Option<u64>,
    first_open_acked_at_ms: Option<u64>,
    /// 当前/最近一次 B 生命周期的时间线，C 复活时会更新。
    last_qualified_at_ms: Option<u64>,
    last_open_sent_at_ms: Option<u64>,
    last_open_acked_at_ms: Option<u64>,
    c_entered_at: Option<u64>,
    reentry_used: bool,
    reentry_b_at: Option<u64>,
    current_cycle_start: Option<u64>,
    cycle_max_market_cap: f64,
    current_slot: Option<String>,
    user_discarded: bool,
    last_mcap_usd: Option<f64>,
    /// C 状态专属连阳，进入 C 时清零，绝不继承 A/B 的 streak。
    c_streak: usize,
    c_streak_end: Option<u64>,
    c_first_bucket: Option<u64>,
    /// 晋升为 B 币那一刻的连续阳线根数。
    ///
    /// 必须单独存一份：`streak` 是实时计数器，晋升后该币仍在继续成交，
    /// 一旦出现阴线就会归零。前端若直接显示 `streak`，会出现
    /// 「因为三连阳入选、却显示连阳 0」的自相矛盾。
    promote_streak: usize,

    /// 桶序号 -> K 线。BTreeMap 保证按时间有序遍历。
    candles: BTreeMap<u64, Candle>,
    streak: usize,
    streak_end: Option<u64>,
    /// 观察期内达到过的最大连续阳线数。仅用于诊断：剔除时打印它，
    /// 就能区分「确实没有三连阳」和「有但判定逻辑漏了」。
    peak_streak: usize,

    trades: Vec<TradeRecord>,
    trade_seq: u64,
    last_price: f64,
    /// 最近一笔成交对应的市值（报价代币单位）。
    ///
    /// 只存报价单位的值：市值是「此刻」的指标，美元换算由前端按实时 BNB/USD
    /// 完成，因此预言机一刷新，前端显示的市值就跟着变，无需回改任何历史数据。
    last_mcap_quote: f64,
    /// 最近一次成交的真实时刻，用于满载时淘汰最久未动的币。
    last_activity: Instant,
}

/// 已经从日志解码、但仍在等待区块 Unix 时间的事件。所有日志都先进入该
/// FIFO；队首时间戳未就绪时只暂停状态机消费，不暂停 WSS 日志接收，因此既
/// 保持链上顺序，也不会再把 RPC 延迟乘到每一个新币上。
struct PendingDecodedLog {
    log: RawLog,
    events: Vec<NormalizedEvent>,
    needs_block_timestamp: bool,
}

// ---------------------------------------------------------------- 对外事件

/// 推送给前端 SSE 的事件。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    /// 新晋 B 币。
    TokenUp(TokenSnapshot),
    /// 代币从前端消失，附中文原因。
    TokenDown {
        token: String,
        reason: String,
    },
    /// K 线更新（含未封口的实时桶）。
    Candle {
        token: String,
        candle: Candle,
    },
    /// 新成交明细。
    Trade {
        token: String,
        trade: TradeRecord,
    },
    /// 观察中的 A 币进度。
    Watching {
        token: String,
        name: String,
        streak: usize,
        seconds_left: u64,
    },
    Stats(Stats),
    BnbUsd {
        price: f64,
    },
    Status {
        state: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    // 全部计数统一用 u64：混用 usize 会让「a_tokens + rejected_tax == created_seen」
    // 这类自洽校验在断言时反复 as 转换，也容易在边界处出错。
    pub created_seen: u64,
    pub a_tokens: u64,
    pub rejected_tax: u64,
    pub expired: u64,
    pub b_tokens: u64,
    pub live_now: u64,
    pub watching_now: u64,
    pub blacklisted: u64,
    /// 因广播缓冲溢出而丢弃的事件累计条数。非零即说明前端数据不完整。
    pub logs_lagged: u64,
    pub uptime_secs: u64,
}

/// 前端渲染所需的完整快照。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenSnapshot {
    pub token: String,
    pub name: String,
    pub symbol: String,
    pub platform: String,
    pub creator: String,
    pub buy_tax_bps: u64,
    pub sell_tax_bps: u64,
    pub quote_symbol: String,
    /// 报价代币是否为原生 BNB；原生时可直接乘 BNB/USD 得美元。
    pub quote_is_native: bool,
    /// 报价代币地址（原生 BNB 时为零地址）。前端拿它去公开行情接口
    /// 解析该代币的美元价，使非 BNB 计价的币也能统一显示美元市值。
    pub quote_address: String,
    pub created_ts: u64,
    pub promoted_at: Option<u64>,
    pub state: Stage,
    pub market_cap_usd: Option<f64>,
    pub first_b_at: Option<u64>,
    pub first_qualified_at_ms: Option<u64>,
    pub first_open_sent_at_ms: Option<u64>,
    pub first_open_acked_at_ms: Option<u64>,
    pub last_qualified_at_ms: Option<u64>,
    pub last_open_sent_at_ms: Option<u64>,
    pub last_open_acked_at_ms: Option<u64>,
    pub c_entered_at: Option<u64>,
    pub reentry_used: bool,
    pub current_cycle_start: Option<u64>,
    pub cycle_max_market_cap: f64,
    pub current_slot: Option<String>,
    /// 最新单价（报价代币单位）。
    pub last_price: f64,
    /// 最新市值（报价代币单位）= 最新单价 × 总供应量。
    pub mcap_quote: f64,
    /// 总供应量（枚）。随快照下发，前端可据此标注市值口径而不必硬编码。
    pub total_supply: f64,
    /// 实时连续阳线数。晋升后仍会随行情变化，可能出现阴线而归零。
    pub streak: usize,
    /// 晋升那一刻的连续阳线数，恒为 >= 3，用于解释「为什么它是 B 币」。
    pub promote_streak: usize,
    pub trade_count: usize,
    pub candles: Vec<Candle>,
    pub trades: Vec<TradeRecord>,
}

/// 前端指令。
#[derive(Debug, Clone)]
pub enum Command {
    PrepareShutdown {
        reply: mpsc::UnboundedSender<Result<Vec<SlotSnapshot>, String>>,
    },
    RequestReset {
        reply: mpsc::UnboundedSender<Result<u64, String>>,
    },
    CommitReset {
        boundary: Result<u64, String>,
        reply: mpsc::UnboundedSender<Result<u64, String>>,
    },
    BrowserResetAck {
        reset_id: u64,
        slots: Vec<BrowserSlotReport>,
    },
    BnbPrice(Result<f64, String>),
    /// 用户点 X：永久剔除。
    Blacklist(String),
    /// 浏览器扩展重连时上报四个页面的真实状态。
    BrowserHello {
        slots: Vec<BrowserSlotReport>,
    },
    /// OPEN 后实际 URL 已进入指定 CA。
    BrowserAck {
        slot_id: String,
        command_id: String,
        token: String,
        window_id: i64,
        tab_id: i64,
    },
    /// 标签页 URL 发生变化；首页 URL 代表用户主动释放。
    BrowserUrl {
        slot_id: String,
        window_id: i64,
        tab_id: i64,
        url: String,
    },
    /// 页面侧观测到 K 线可见的时刻。ACK 只代表导航完成，页面加载与图表
    /// 渲染都发生在其后，而这一段原先完全没有测量。时间戳来自页面
    /// `Date.now()`，与后端同读本机系统时钟，因此可直接与
    /// `last_open_acked_at_ms` 相减，无需换算。
    BrowserChartReady {
        slot_id: String,
        window_id: i64,
        tab_id: i64,
        token: String,
        page_clock_ms: u64,
    },
    /// 浏览器 WebSocket 断开，只标记连接状态，不猜测页面仍正确。
    BrowserDisconnected,
    BrowserCommandFailed {
        slot_id: String,
        command_id: String,
        token: String,
        error: String,
    },
    /// 诊断事件只记时间，不参与生命周期/买入/ACK 判定。
    BrowserProgress {
        slot_id: String,
        command_id: String,
        token: String,
        window_id: i64,
        tab_id: i64,
        phase: BrowserPhase,
        page_clock_ms: u64,
    },
    /// 非 BNB 报价代币美元价格后台查询结果。
    QuoteUsd {
        quote: String,
        price: Option<f64>,
    },
    /// 后台任务解析出的报价代币符号（非阻塞回填，不在事件循环里 await RPC）。
    QuoteSymbol {
        quote: String,
        symbol: Option<String>,
    },
    /// 后台补齐不参与实时策略判定的 Token metadata。Four.meme 的报价资产也
    /// 通过这里回填；这些查询无论多慢都不能阻塞链上日志主循环。
    TokenMetadata {
        token: String,
        name: Option<String>,
        symbol: Option<String>,
        total_supply_tokens: Option<f64>,
        quote_token: Option<String>,
    },
    /// 独立 HTTP RPC 返回的区块 Unix 时间。
    BlockTimestamp {
        block: u64,
        result: Result<u64, String>,
    },
    /// Four.meme / GeniusFun 税率只读查询结果。网络错误保持待定并重试，
    /// 绝不能伪造成 4.01% 后永久剔除。
    TaxLookup {
        token: String,
        result: Result<(u64, u64), String>,
    },
    /// 独立 HTTP RPC 补查出的单笔交易发起人。
    TransactionSender {
        tx_hash: String,
        sender: Result<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserSlotReport {
    pub slot_id: String,
    pub window_id: Option<i64>,
    pub tab_id: Option<i64>,
    pub url: String,
    #[serde(default)]
    pub recreated: bool,
    #[serde(default)]
    pub pending_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SlotState {
    Empty,
    Opening,
    Occupied,
    /// 页面未知/用户跳到其他页面：不可当作空闲，也不触发 C。
    Blocked,
    Disconnected,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlotSnapshot {
    pub slot_id: String,
    pub token_ca: Option<String>,
    pub browser_window_id: Option<i64>,
    pub browser_tab_id: Option<i64>,
    pub state: SlotState,
    pub command_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrowserPhase {
    Received,
    Navigating,
    UrlConfirmed,
    DocumentReady,
    ChartContainer,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserTiming {
    pub slot_id: String,
    pub command_id: String,
    pub ca: String,
    pub open_sent_at_ms: u64,
    pub received_at_ms: Option<u64>,
    pub navigation_requested_at_ms: Option<u64>,
    pub url_confirmed_at_ms: Option<u64>,
    pub document_ready_at_ms: Option<u64>,
    /// 仅代表识别到图表容器，绝不宣称 K 线已渲染。
    pub chart_container_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
pub enum BrowserCommand {
    PrepareClose {
        request_id: String,
        slots: Vec<SlotSnapshot>,
    },
    CloseWindows {
        request_id: String,
    },
    SyncRequest,
    Reset {
        reset_id: u64,
    },
    Open {
        command_id: String,
        slot_id: String,
        ca: String,
        url: String,
    },
}

struct Slot {
    slot_id: String,
    token_ca: Option<String>,
    browser_window_id: Option<i64>,
    browser_tab_id: Option<i64>,
    state: SlotState,
    command_id: Option<String>,
    command_sent_at: Option<Instant>,
}

/// HTTP 层读取的共享视图。引擎在关键节点写入。
#[derive(Clone, Default)]
pub struct Shared {
    pub recovery_wake: Arc<tokio::sync::Notify>,
    pub system: Arc<crate::system::Control>,
    pub trading: Arc<OnceLock<crate::trading::Trading>>,
    pub reset_pending: Arc<AtomicU64>,
    pub snapshots: Arc<RwLock<Vec<TokenSnapshot>>>,
    pub slots: Arc<RwLock<Vec<SlotSnapshot>>>,
    pub stats: Arc<RwLock<Stats>>,
    pub bnb_usd: Arc<RwLock<Option<f64>>>,
    pub status: Arc<RwLock<String>>,
    pub last_processed_block: Arc<AtomicU64>,
    pub last_scanned_block: Arc<AtomicU64>,
    pub ingest_lagged: Arc<AtomicU64>,
    pub ingestion: Arc<RwLock<crate::recovery::RecoveryHealth>>,
    pub browser_timings: Arc<RwLock<Vec<BrowserTiming>>>,
}

/// HTTP 层持有的引擎句柄。
#[derive(Clone)]
pub struct Handle {
    pub cmd_tx: mpsc::UnboundedSender<Command>,
    pub event_tx: broadcast::Sender<Event>,
    pub browser_tx: broadcast::Sender<BrowserCommand>,
    pub shared: Shared,
}

// ---------------------------------------------------------------- 引擎

/// CA 仍只有一份权威 TokenState；active 只是可重建的运行索引。
/// Tombstone 保留在 all 中，但不参与 tick、调度和快照的全表扫描。
#[derive(Default)]
struct TokenTable {
    all: HashMap<String, TokenState>,
    active: HashSet<String>,
}

impl TokenTable {
    fn insert(&mut self, ca: String, st: TokenState) -> Option<TokenState> {
        if st.stage.is_terminal() {
            self.active.remove(&ca);
        } else {
            self.active.insert(ca.clone());
        }
        self.all.insert(ca, st)
    }

    fn live_iter(&self) -> impl Iterator<Item = (&String, &TokenState)> {
        self.active
            .iter()
            .filter_map(|ca| self.all.get_key_value(ca))
            .filter(|(_, st)| !st.stage.is_terminal())
    }

    fn live_values(&self) -> impl Iterator<Item = &TokenState> {
        self.live_iter().map(|(_, st)| st)
    }

    fn live_keys(&self) -> Vec<String> {
        self.live_iter().map(|(ca, _)| ca.clone()).collect()
    }
}

impl Deref for TokenTable {
    type Target = HashMap<String, TokenState>;
    fn deref(&self) -> &Self::Target {
        &self.all
    }
}

impl DerefMut for TokenTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.all
    }
}

pub struct Engine {
    reset_cutoff: u64,
    reset_pending: u64,
    reset_fetching: bool,
    reset_sent: Option<Instant>,
    tokens: TokenTable,
    slot_retry_not_before: HashMap<String, Instant>,
    browser_timings: HashMap<String, BrowserTiming>,
    /// 永久剔除名单。命中即忽略一切后续事件。
    blacklist: HashSet<String>,
    slots: Vec<Slot>,
    logs_rx: broadcast::Receiver<RawLog>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    /// 自身的指令发送端，供后台解析任务回填结果（不阻塞事件循环）。
    cmd_tx: mpsc::UnboundedSender<Command>,
    event_tx: broadcast::Sender<Event>,
    browser_tx: broadcast::Sender<BrowserCommand>,
    status_rx: watch::Receiver<String>,
    client: Client,
    sender_rpc: crate::rpc::SenderRpc,
    /// 区块时间使用独立 HTTP 客户端/并发门，不与 tx.from 抢占连接池。
    block_rpc: crate::rpc::SenderRpc,
    /// metadata 与税率共用的低频 HTTP RPC 池；与 WSS 和 tx.from 完全隔离。
    read_rpc: crate::rpc::SenderRpc,
    shared: Shared,
    store: Store,
    launchpads: Registry,
    started: Instant,

    bnb_usd: Option<f64>,
    stats_created: u64,
    stats_a: u64,
    stats_rejected: u64,
    stats_expired: u64,
    stats_b: u64,
    /// 因广播缓冲溢出而丢弃的事件累计条数。非零即说明数据不完整。
    logs_lagged: u64,

    /// 已见到的最大链上时间戳。用于封口 K 线，实时与回填模式都成立，
    /// 避免用墙上时钟误封尚未到达的桶。
    latest_chain_ts: u64,
    /// 诊断用途；未经日志完整性验证的区块头不能直接提前封 K 线。
    latest_head_ts: u64,

    /// 建币与税率/报价事件的配对缓冲。实测三者恒定同交易发出，
    /// 仍做缓冲以防事件乱序到达。
    pending_tax: HashMap<String, (u64, u64)>,
    pending_quote: HashMap<String, String>,
    /// 报价代币符号缓存。只存解析成功的结果；失败不入缓存以便重试。
    quote_symbols: HashMap<String, Option<String>>,
    /// 正在解析中的报价代币地址，用于去重，避免每个 tick 重复派发同一查询。
    quote_lookups_inflight: HashSet<String>,
    metadata_lookups_inflight: HashSet<String>,
    metadata_retry_not_before: HashMap<String, Instant>,
    metadata_permits: Arc<tokio::sync::Semaphore>,
    token_query_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Four.meme / GeniusFun 的税率 RPC 查询状态。失败后保留 NEW 状态，
    /// 按退避时间重试；确认结果前不允许定级为 A 或 DEAD。
    tax_lookups_inflight: HashSet<String>,
    tax_retry_not_before: HashMap<String, Instant>,
    quote_usd: HashMap<String, f64>,
    quote_usd_lookups_inflight: HashSet<String>,
    seen_log_keys: HashSet<(u64, u64, u64)>,
    seen_log_order: VecDeque<(u64, u64, u64)>,
    /// 正在经独立 HTTP RPC 查询的交易。同一 tx 可以有多个归一化成交事件，
    /// 等待项依次为 (token, 链上成交时间, 首次派发时刻)。
    sender_lookups_inflight: HashMap<String, Vec<(String, u64, Instant)>>,
    sender_tasks: HashMap<String, tokio::task::JoinHandle<()>>,
    bnb_price_inflight: bool,
    /// 最近交易的 tx.from 缓存及 FIFO 淘汰队列。
    sender_tx_cache: HashMap<String, String>,
    sender_tx_cache_order: VecDeque<String>,
    /// 高频成交字段的 SQLite 写入节流。生命周期转换仍立即持久化。
    trade_persist_not_before: HashMap<String, Instant>,
    durable_cycle_peaks: HashMap<String, u64>,
    block_timestamp_cache: HashMap<u64, u64>,
    block_timestamp_inflight: HashSet<u64>,
    block_timestamp_tasks: HashMap<u64, tokio::task::JoinHandle<()>>,
    pending_decoded_logs: VecDeque<PendingDecodedLog>,
}

impl Engine {
    pub fn new(
        logs_rx: broadcast::Receiver<RawLog>,
        cmd_rx: mpsc::UnboundedReceiver<Command>,
        cmd_tx: mpsc::UnboundedSender<Command>,
        event_tx: broadcast::Sender<Event>,
        browser_tx: broadcast::Sender<BrowserCommand>,
        status_rx: watch::Receiver<String>,
        client: Client,
        shared: Shared,
        store: Store,
    ) -> Self {
        let mut tokens = TokenTable::default();
        let now = Instant::now();
        match store.load_tokens() {
            Ok(rows) => {
                for row in rows {
                    if let Some(st) = token_from_persisted(row, now) {
                        tokens.insert(st.token.clone(), st);
                    }
                }
            }
            Err(e) => eprintln!("[warn] 恢复 SQLite 代币状态失败: {e}"),
        }
        let last_processed = store
            .get_meta_u64("last_processed_block")
            .ok()
            .flatten()
            .unwrap_or(0);
        shared
            .last_processed_block
            .store(last_processed, Ordering::Relaxed);
        let floor = store
            .get_meta_u64("reset_cutoff_block")
            .ok()
            .flatten()
            .unwrap_or(0);
        let baseline = if floor > 0 {
            floor
        } else {
            tokens
                .values()
                .filter(|t| t.creation_block > 0)
                .map(|t| t.creation_block)
                .min()
                .map(|block| block.saturating_sub(1))
                .unwrap_or(0)
        };
        let scanned = store.initialize_scan_cursor(baseline).unwrap_or_else(|e| {
            eprintln!("[warn] 补扫进度迁移未持久化，保留保守起点：{e}");
            baseline
        });
        shared
            .last_scanned_block
            .store(scanned.max(floor), Ordering::Relaxed);
        let blacklist = tokens
            .iter()
            .filter(|(_, t)| t.stage.is_terminal())
            .map(|(ca, _)| ca.clone())
            .collect();
        let mut slots: Vec<Slot> = (0..config::SLOT_COUNT)
            .map(|i| Slot {
                slot_id: ((b'A' + i as u8) as char).to_string(),
                token_ca: None,
                browser_window_id: None,
                browser_tab_id: None,
                state: SlotState::Disconnected,
                command_id: None,
                command_sent_at: None,
            })
            .collect();
        for st in tokens.live_values() {
            if let Some(slot_id) = &st.current_slot {
                if let Some(slot) = slots.iter_mut().find(|s| &s.slot_id == slot_id) {
                    slot.token_ca = Some(st.token.clone());
                }
            }
        }
        let reset_cutoff = store
            .get_meta_u64("reset_cutoff_block")
            .ok()
            .flatten()
            .unwrap_or(0);
        let reset_pending = store
            .get_meta_u64("browser_reset_pending")
            .ok()
            .flatten()
            .unwrap_or(0);
        shared.reset_pending.store(reset_pending, Ordering::Relaxed);
        Self {
            reset_cutoff,
            reset_pending,
            reset_fetching: false,
            reset_sent: None,
            tokens,
            slot_retry_not_before: HashMap::new(),
            browser_timings: HashMap::new(),
            blacklist,
            slots,
            logs_rx,
            cmd_rx,
            cmd_tx,
            event_tx,
            browser_tx,
            status_rx,
            client,
            sender_rpc: crate::rpc::SenderRpc::new(),
            block_rpc: crate::rpc::SenderRpc::new(),
            read_rpc: crate::rpc::SenderRpc::new(),
            shared,
            store,
            launchpads: Registry::default(),
            started: Instant::now(),
            bnb_usd: None,
            stats_created: 0,
            stats_a: 0,
            stats_rejected: 0,
            stats_expired: 0,
            stats_b: 0,
            logs_lagged: 0,
            latest_chain_ts: 0,
            latest_head_ts: 0,
            pending_tax: HashMap::new(),
            pending_quote: HashMap::new(),
            quote_symbols: HashMap::new(),
            quote_lookups_inflight: HashSet::new(),
            metadata_lookups_inflight: HashSet::new(),
            metadata_retry_not_before: HashMap::new(),
            metadata_permits: Arc::new(tokio::sync::Semaphore::new(16)),
            token_query_tasks: Vec::new(),
            tax_lookups_inflight: HashSet::new(),
            tax_retry_not_before: HashMap::new(),
            quote_usd: HashMap::new(),
            quote_usd_lookups_inflight: HashSet::new(),
            seen_log_keys: HashSet::new(),
            seen_log_order: VecDeque::new(),
            sender_lookups_inflight: HashMap::new(),
            sender_tasks: HashMap::new(),
            bnb_price_inflight: false,
            sender_tx_cache: HashMap::new(),
            sender_tx_cache_order: VecDeque::new(),
            trade_persist_not_before: HashMap::new(),
            durable_cycle_peaks: HashMap::new(),
            block_timestamp_cache: HashMap::new(),
            block_timestamp_inflight: HashSet::new(),
            block_timestamp_tasks: HashMap::new(),
            pending_decoded_logs: VecDeque::new(),
        }
    }

    pub fn attach_trading(&self, trading: crate::trading::Trading) -> Result<(), String> {
        self.shared
            .trading
            .set(trading)
            .map_err(|_| "交易执行器已绑定".into())
    }

    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(config::TICK_INTERVAL);
        let mut price_t = tokio::time::interval(config::PRICE_REFRESH);
        let mut stats_t = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        price_t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        stats_t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut command_burst = 0usize;
        let mut diagnostics_at = Instant::now();
        // watch channel may already contain a connection/backfill status before the
        // engine task starts. Mirror that initial value so the dashboard never
        // presents an empty backend status until the next transition.
        let initial_status = self.status_rx.borrow().clone();
        *self.shared.status.write().await = initial_status;
        // 启动即取一次 BNB 价，避免首批成交的 usd 字段为空。
        self.refresh_bnb_price().await;

        loop {
            // Once shutdown is requested, no log/tick/URL may promote a token,
            // release a slot into C, or dispatch another OPEN. HTTP remains alive
            // for shutdown ACK/retry until all state is safely saved.
            if self.shared.system.is_stopping() {
                if let Some(cmd) = self.cmd_rx.recv().await {
                    self.on_command(cmd).await;
                } else {
                    break;
                }
                continue;
            }
            // cmd 与 tick 都必须排在持续到来的日志之前：用户永久剔除拥有最高
            // 优先级；tick 则负责 60 秒/240 秒/600 秒生命周期以及失败 RPC 重试。
            // 旧实现把 logs 放在 tick 前，在热门时段日志永远 ready，导致数百个
            // A 币超过 60 秒仍不清理。到期判定现在只依赖 deadline_wall，不再用
            // 最新链上时间跨越窗口来提前剔除，因此 tick 可安全地按固定频率运行。
            tokio::select! {
                biased;
                Some(cmd) = self.cmd_rx.recv(), if command_burst < 32 => {
                    command_burst += 1;
                    let started = Instant::now();
                    self.on_command(cmd).await;
                    if started.elapsed() > Duration::from_millis(100) { eprintln!("[latency] command_ms={}", started.elapsed().as_millis()); }
                },
                _ = tick.tick() => { command_burst = 0; self.on_tick().await; },
                // 必须显式处理 Err(Lagged)：广播缓冲溢出时 recv() 返回错误，
                // 若用 `Ok(log) = recv()` 的写法，该分支会被静默禁用、事件被丢弃，
                // 表现为「A币识别正常但成交缺失、永远晋升不了 B币」，且日志毫无痕迹。
                res = self.logs_rx.recv() => match res {
                    Ok(log) => { command_burst = 0; self.on_log(log).await; },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        self.logs_lagged += n;
                        self.shared.ingest_lagged.store(self.logs_lagged, Ordering::Relaxed);
                        eprintln!("[warn] 事件通道滞后，丢弃 {n} 条（累计 {}）", self.logs_lagged);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        eprintln!("[warn] 事件通道已关闭");
                        break;
                    }
                },
                _ = price_t.tick() => self.refresh_bnb_price().await,
                _ = stats_t.tick() => self.sync_shared().await,
                Ok(()) = self.status_rx.changed() => {
                    let s = self.status_rx.borrow().clone();
                    *self.shared.status.write().await = s.clone();
                    let _ = self.event_tx.send(Event::Status { state: s });
                },
                _ = tokio::task::yield_now(), if command_burst >= 32 => { command_burst = 0; }
            }
            if diagnostics_at.elapsed() >= Duration::from_secs(10) {
                // cooling_* 分别对应三个独立 SenderRpc 实例的端点健康度：
                // 批量 tx.from / 区块时间戳 / eth_call 只读查询。各自独立学习
                // 死端点，因此需要分别观测，单个数字会掩盖某条路径仍在撞墙。
                eprintln!("[latency] logs_queued={} decoded_queued={} commands_queued={} sender_waiting={} timestamp_waiting={} cooling_sender={} cooling_block={} cooling_read={} head_clock_gap_secs={}",
                    self.logs_rx.len(), self.pending_decoded_logs.len(), self.cmd_rx.len(),
                    self.sender_lookups_inflight.len(), self.block_timestamp_inflight.len(),
                    self.sender_rpc.cooling_endpoints(),
                    self.block_rpc.cooling_endpoints(),
                    self.read_rpc.cooling_endpoints(), self.latest_head_ts.saturating_sub(self.latest_chain_ts));
                diagnostics_at = Instant::now();
            }
        }
    }

    async fn refresh_bnb_price(&mut self) {
        if !self.bnb_price_inflight {
            self.bnb_price_inflight = true;
            let rpc = self.read_rpc.clone();
            let tx = self.cmd_tx.clone();
            tokio::spawn(async move {
                let result = rpc
                    .eth_call(config::BNB_USD_FEED, &abi::selector("latestAnswer()"))
                    .await
                    .and_then(|raw| {
                        abi::decode_int256(&raw)
                            .filter(|v| *v > 0)
                            .map(|v| v as f64 / 1e8)
                            .ok_or_else(|| "Invalid BNB oracle price".into())
                    });
                let _ = tx.send(Command::BnbPrice(result));
            });
        }
        let quotes: HashSet<String> = self
            .tokens
            .live_values()
            .filter(|t| !t.stage.is_terminal() && t.quote_token != config::ZERO_ADDRESS)
            .map(|t| t.quote_token.clone())
            .collect();
        for quote in quotes {
            self.refresh_quote_usd(quote);
        }
    }

    fn refresh_market_caps_for_quote(&mut self, quote: &str) {
        let usd = if quote == config::ZERO_ADDRESS {
            self.bnb_usd
        } else {
            self.quote_usd.get(quote).copied()
        };
        let Some(usd) = usd else {
            return;
        };
        let mut touched = Vec::new();
        for ca in self.tokens.live_keys() {
            let t = self.tokens.get_mut(&ca).expect("active token");
            if t.quote_token == quote {
                let mcap = t.last_mcap_quote * usd;
                t.last_mcap_usd = (mcap.is_finite() && mcap >= 0.0).then_some(mcap);
                record_b_market_cap(t, now_unix(), mcap);
                touched.push(ca);
            }
        }
        for token in touched {
            self.persist_trade_throttled(&token);
        }
    }

    fn ensure_quote_usd(&mut self, quote: &str) {
        if quote == config::ZERO_ADDRESS || self.quote_usd.contains_key(quote) {
            return;
        }
        self.refresh_quote_usd(quote.to_string());
    }

    fn refresh_quote_usd(&mut self, quote: String) {
        if quote == config::ZERO_ADDRESS || !self.quote_usd_lookups_inflight.insert(quote.clone()) {
            return;
        }
        let tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            let price = fetch_quote_usd(&quote).await;
            let _ = tx.send(Command::QuoteUsd { quote, price });
        });
    }

    fn stats_now(&self) -> Stats {
        let (mut live, mut watching) = (0u64, 0u64);
        for t in self.tokens.live_values() {
            if t.stage.is_b() || t.stage == Stage::CWatching {
                live += 1;
            }
            if t.stage == Stage::AWatching {
                watching += 1;
            }
        }
        Stats {
            created_seen: self.stats_created,
            a_tokens: self.stats_a,
            rejected_tax: self.stats_rejected,
            expired: self.stats_expired,
            b_tokens: self.stats_b,
            live_now: live,
            watching_now: watching,
            blacklisted: self.blacklist.len() as u64,
            // logs_lagged 非零说明有事件被丢弃、前端数据不完整。
            logs_lagged: self.logs_lagged,
            uptime_secs: self.started.elapsed().as_secs(),
        }
    }

    /// 把当前视图写入共享区，供 HTTP 首次加载与 SSE 重连时使用。
    async fn sync_shared(&self) {
        let stream = self.client.stream_snapshot();
        {
            let mut health = self.shared.ingestion.write().await;
            health.head_block = health.head_block.max(stream.last_header_block);
            health.scanned_block = self.shared.last_scanned_block.load(Ordering::Relaxed);
            health.gap_blocks = health.head_block.saturating_sub(health.scanned_block);
            health.stream = Some(stream);
        }
        let mut snaps: Vec<TokenSnapshot> = self
            .tokens
            .live_values()
            .filter(|t| {
                !t.stage.is_terminal() && t.stage != Stage::AWatching && t.stage != Stage::New
            })
            .map(snapshot_of)
            .collect();
        // 最新晋升的排前面。
        snaps.sort_by(|a, b| {
            b.promoted_at
                .cmp(&a.promoted_at)
                .then_with(|| a.token.cmp(&b.token))
        });
        *self.shared.snapshots.write().await = snaps;
        *self.shared.slots.write().await = self.slots.iter().map(slot_snapshot).collect();
        let mut timings: Vec<_> = self.browser_timings.values().cloned().collect();
        timings.sort_by(|a, b| a.slot_id.cmp(&b.slot_id));
        *self.shared.browser_timings.write().await = timings;
        let stats = self.stats_now();
        *self.shared.stats.write().await = stats;
        let _ = self.event_tx.send(Event::Stats(stats));
    }

    fn send_reset(&mut self) {
        let _ = self.browser_tx.send(BrowserCommand::Reset {
            reset_id: self.reset_pending,
        });
        self.reset_sent = Some(Instant::now());
    }

    async fn reset_monitoring(&mut self, block: u64) -> Result<u64, String> {
        if let Some(trading) = self.shared.trading.get() {
            trading.pause("重置监控：暂停买入，保留交易与持仓记录", false)?;
        }
        // Include everything already received so queued/pre-reset work cannot cross the barrier.
        let boundary = block
            .max(self.shared.last_processed_block.load(Ordering::Relaxed))
            .max(
                self.pending_decoded_logs
                    .iter()
                    .map(|p| p.log.block_number)
                    .max()
                    .unwrap_or(0),
            );
        let id = now_unix_ms().max(self.reset_pending + 1);
        self.store.reset_monitoring(boundary, id, now_unix())?;
        self.reset_cutoff = boundary;
        self.reset_pending = id;
        self.shared
            .last_processed_block
            .store(boundary, Ordering::Relaxed);
        self.shared
            .last_scanned_block
            .store(boundary, Ordering::Relaxed);
        self.shared.reset_pending.store(id, Ordering::Relaxed);
        self.shared.recovery_wake.notify_one();
        for (ca, token) in self.tokens.iter_mut() {
            if !token.stage.is_terminal() {
                token.stage = Stage::Dead;
            }
            token.current_slot = None;
            token.candles.clear();
            token.trades.clear();
            token.pending_sender_lookups = 0;
            self.blacklist.insert(ca.clone());
        }
        self.tokens.active.clear();
        self.slot_retry_not_before.clear();
        self.browser_timings.clear();
        for (_, task) in self.sender_tasks.drain() {
            task.abort();
        }
        for task in self.token_query_tasks.drain(..) {
            task.abort();
        }
        for (_, task) in self.block_timestamp_tasks.drain() {
            task.abort();
        }
        self.sender_lookups_inflight.clear();
        self.block_timestamp_inflight.clear();
        self.pending_decoded_logs.clear();
        self.pending_tax.clear();
        self.pending_quote.clear();
        self.metadata_lookups_inflight.clear();
        self.metadata_retry_not_before.clear();
        self.tax_lookups_inflight.clear();
        self.tax_retry_not_before.clear();
        self.trade_persist_not_before.clear();
        self.durable_cycle_peaks.clear();
        for slot in &mut self.slots {
            slot.token_ca = None;
            slot.command_id = None;
            slot.command_sent_at = None;
            slot.state = SlotState::Disconnected;
        }
        self.send_reset();
        self.sync_shared().await;
        eprintln!("[reset] 清空观察池，保留终态记录；仅接受区块 {boundary} 之后的新币，等待四窗口 ACK，reset={id}");
        Ok(boundary)
    }

    async fn on_command(&mut self, cmd: Command) {
        if self.shared.system.is_stopping() {
            match cmd {
                Command::PrepareShutdown { reply } => {
                    let result = (|| {
                        for t in self.tokens.live_values() {
                            self.store.upsert_token(&persisted_of(t), now_unix())?;
                        }
                        self.store.flush_market()?;
                        Ok(self.slots.iter().map(slot_snapshot).collect())
                    })();
                    *self.shared.status.write().await =
                        "系统关闭中：停止监控及调度，数据已保留".into();
                    let _ = reply.send(result);
                }
                Command::RequestReset { reply } | Command::CommitReset { reply, .. } => {
                    let _ = reply.send(Err("系统关闭中，不能清空或重新监控".into()));
                }
                _ => {} // In particular: closing windows is NOT a manual B -> C release.
            }
            return;
        }
        match cmd {
            Command::PrepareShutdown { reply } => {
                let _ = reply.send(Err("未请求关闭".into()));
            }
            Command::RequestReset { reply } => {
                if let Some(trading) = self.shared.trading.get() {
                    let _ = trading.pause("请求清空监控：暂停买入并取消未广播意图", false);
                }
                if self.reset_fetching || self.reset_pending != 0 {
                    let _ = reply.send(Err("上次清空仍在进行；请检查四窗口插件连接".into()));
                    return;
                }
                self.reset_fetching = true;
                let client = self.client.clone();
                let commands = self.cmd_tx.clone();
                tokio::spawn(async move {
                    let boundary = match tokio::time::timeout(
                        Duration::from_secs(10),
                        crate::recovery::latest_reset_head(Some(client)),
                    )
                    .await
                    {
                        Ok(Ok(head)) => {
                            eprintln!(
                                "[reset] 已确认独立区块头 block={} source={}",
                                head.block, head.source
                            );
                            Ok(head.block)
                        }
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err("独立节点查询超时，未清空任何数据，请稍后重试".into()),
                    };
                    let _ = commands.send(Command::CommitReset { boundary, reply });
                });
            }
            Command::CommitReset { boundary, reply } => {
                self.reset_fetching = false;
                let result = match boundary {
                    Ok(block) => self.reset_monitoring(block).await,
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
            }
            Command::BrowserResetAck { reset_id, slots } => {
                if reset_id == 0
                    || reset_id != self.reset_pending
                    || slots.len() != 4
                    || slots
                        .iter()
                        .map(|s| s.slot_id.as_str())
                        .collect::<HashSet<_>>()
                        != HashSet::from(["A", "B", "C", "D"])
                    || slots
                        .iter()
                        .map(|s| s.window_id)
                        .collect::<HashSet<_>>()
                        .len()
                        != 4
                    || slots.iter().map(|s| s.tab_id).collect::<HashSet<_>>().len() != 4
                    || slots.iter().any(|s| {
                        s.window_id.is_none() || s.tab_id.is_none() || !is_gmgn_home(&s.url)
                    })
                {
                    return;
                }
                if self.store.set_meta_u64("browser_reset_pending", 0).is_err() {
                    return;
                }
                for report in slots {
                    let slot = self
                        .slots
                        .iter_mut()
                        .find(|s| s.slot_id == report.slot_id)
                        .unwrap();
                    slot.browser_window_id = report.window_id;
                    slot.browser_tab_id = report.tab_id;
                    slot.state = SlotState::Empty;
                }
                self.reset_pending = 0;
                self.shared.reset_pending.store(0, Ordering::Relaxed);
                self.schedule(false).await;
                self.sync_shared().await;
            }
            Command::BnbPrice(result) => {
                self.bnb_price_inflight = false;
                match result {
                    Ok(price) => {
                        self.bnb_usd = Some(price);
                        self.refresh_market_caps_for_quote(config::ZERO_ADDRESS);
                        *self.shared.bnb_usd.write().await = Some(price);
                        let _ = self.event_tx.send(Event::BnbUsd { price });
                    }
                    Err(error) => eprintln!("[warn] BNB/USD 后台查询失败: {error}"),
                }
            }
            Command::Blacklist(token) => {
                let token = token.to_lowercase();
                if let Some(trading) = self.shared.trading.get() {
                    trading.cancel_token(&token, "人工永久淘汰，取消未广播买入");
                }
                self.cancel_sender_lookups_for_token(&token);
                let existed = self.tokens.contains_key(&token);
                self.blacklist.insert(token.clone());
                self.tokens.active.remove(&token);
                self.release_token_from_slot(&token);
                if let Some(st) = self.tokens.get_mut(&token) {
                    st.stage = Stage::UserDiscarded;
                    st.user_discarded = true;
                    st.current_slot = None;
                }
                self.persist(&token);
                if existed {
                    let _ = self.event_tx.send(Event::TokenDown {
                        token: token.clone(),
                        reason: "已手动剔除".into(),
                    });
                }
                eprintln!(
                    "[info] 永久剔除 {}（黑名单累计 {} 个）",
                    tail8(&token),
                    self.blacklist.len()
                );
                self.sync_shared().await;
                self.schedule(false).await;
            }
            Command::BrowserHello { slots } => {
                if self.reset_pending != 0 {
                    self.send_reset();
                    return;
                }
                let valid = slots.len() == 4
                    && slots
                        .iter()
                        .all(|s| s.window_id.is_some() && s.tab_id.is_some())
                    && slots
                        .iter()
                        .map(|s| s.window_id)
                        .collect::<HashSet<_>>()
                        .len()
                        == 4
                    && slots.iter().map(|s| s.tab_id).collect::<HashSet<_>>().len() == 4
                    && slots
                        .iter()
                        .map(|s| s.slot_id.as_str())
                        .collect::<HashSet<_>>()
                        == HashSet::from(["A", "B", "C", "D"]);
                if !valid {
                    eprintln!("[warn] 拒绝无效 HELLO，保留现有槽位绑定");
                    return;
                }
                // 完整 HELLO 本身已覆盖四个 Slot。不要先清空绑定，否则每次
                // 正常心跳都会被 reconcile 误判为窗口变更并取消买入。
                let mut window_ids = HashSet::new();
                let mut tab_ids = HashSet::new();
                for report in slots {
                    let (Some(window_id), Some(tab_id)) = (report.window_id, report.tab_id) else {
                        eprintln!(
                            "[warn] Slot {} 未上报 windowId/tabId，拒绝绑定",
                            report.slot_id
                        );
                        continue;
                    };
                    if window_ids.contains(&window_id) || tab_ids.contains(&tab_id) {
                        eprintln!(
                            "[warn] Slot {} 与其他 Slot 共用 windowId/tabId，拒绝绑定",
                            report.slot_id
                        );
                        continue;
                    }
                    window_ids.insert(window_id);
                    tab_ids.insert(tab_id);
                    self.reconcile_browser_report(report).await;
                }
                self.schedule(false).await;
                self.sync_shared().await;
            }
            Command::BrowserAck {
                slot_id,
                command_id,
                token,
                window_id,
                tab_id,
            } => {
                let token = token.to_lowercase();
                let accepted = self
                    .slots
                    .iter()
                    .position(|s| s.slot_id == slot_id)
                    .is_some_and(|i| {
                        let s = &self.slots[i];
                        s.state == SlotState::Opening
                            && s.command_id.as_deref() == Some(command_id.as_str())
                            && s.token_ca.as_deref() == Some(token.as_str())
                            && s.browser_window_id.is_none_or(|id| id == window_id)
                            && s.browser_tab_id.is_none_or(|id| id == tab_id)
                    });
                if accepted {
                    let acked_ms = now_unix_ms();
                    if let Some(slot) = self.slots.iter_mut().find(|s| s.slot_id == slot_id) {
                        slot.state = SlotState::Occupied;
                        slot.browser_window_id = Some(window_id);
                        slot.browser_tab_id = Some(tab_id);
                        slot.command_id = None;
                        slot.command_sent_at = None;
                    }
                    if let Some(st) = self.tokens.get_mut(&token) {
                        st.stage = if st.reentry_used && st.reentry_b_at.is_some() {
                            Stage::BReentryActive
                        } else {
                            Stage::BActive
                        };
                        st.current_slot = Some(slot_id);
                        st.first_open_acked_at_ms.get_or_insert(acked_ms);
                        st.last_open_acked_at_ms = Some(acked_ms);
                        let elapsed = st
                            .last_open_sent_at_ms
                            .map(|sent| acked_ms.saturating_sub(sent));
                        eprintln!(
                            "[浏览器] ACK {} OPEN→ACK {}ms",
                            tail8(&token),
                            elapsed
                                .map(|ms| ms.to_string())
                                .unwrap_or_else(|| "?".into())
                        );
                    }
                    self.persist(&token);
                    self.sync_shared().await;
                }
            }
            Command::BrowserUrl {
                slot_id,
                window_id,
                tab_id,
                url,
            } => {
                self.handle_browser_url(&slot_id, window_id, tab_id, &url)
                    .await;
            }
            Command::BrowserChartReady {
                slot_id,
                window_id,
                tab_id,
                token,
                page_clock_ms,
            } => {
                let token = token.to_lowercase();
                // 只接受当前确实归属该 Slot 的 Token：Slot 可能已经换手，
                // 上一个币迟到的观测结果不能算到新币头上。
                // 这里同时接受 Opening 与 Occupied：图表可能比 ACK 先渲染出来
                // （SPA 命中缓存时），若只认 Occupied 会把这类样本整条丢掉，
                // 而丢的恰恰是延迟最低、最该看的那一批。
                let owned = self.slots.iter().any(|s| {
                    s.slot_id == slot_id
                        && s.token_ca.as_deref() == Some(token.as_str())
                        && matches!(s.state, SlotState::Opening | SlotState::Occupied)
                        && s.browser_window_id.is_none_or(|id| id == window_id)
                        && s.browser_tab_id.is_none_or(|id| id == tab_id)
                });
                if !owned {
                    return;
                }
                // 页面 Date.now() 与 now_unix_ms() 同读本机系统时钟，可直接相减。
                let ready_ms = page_clock_ms;
                let Some(st) = self.tokens.get(&token) else {
                    return;
                };
                let ack_to_ready = st
                    .last_open_acked_at_ms
                    .map(|acked| ready_ms.saturating_sub(acked));
                let qualified_to_ready = st
                    .last_qualified_at_ms
                    .map(|qualified| ready_ms.saturating_sub(qualified));
                let fmt = |v: Option<u64>| v.map(|ms| ms.to_string()).unwrap_or_else(|| "?".into());
                eprintln!(
                    "[浏览器] LEGACY_CHART_HINT {} ACK→旧版容器提示 {}ms 资格确认→旧版容器提示 {}ms（不能证明K线已渲染）",
                    tail8(&token),
                    fmt(ack_to_ready),
                    fmt(qualified_to_ready)
                );
            }
            Command::BrowserDisconnected => {
                if let Some(trading) = self.shared.trading.get() {
                    let _ = trading.pause("浏览器插件断线/暂停：买入已暂停", false);
                }
                for slot in &mut self.slots {
                    slot.state = SlotState::Disconnected;
                }
                self.sync_shared().await;
            }
            Command::BrowserCommandFailed {
                slot_id,
                command_id,
                token,
                error,
            } => {
                let token = token.to_lowercase();
                let Some(index) = self.slots.iter().position(|s| {
                    s.slot_id == slot_id
                        && s.state == SlotState::Opening
                        && s.command_id.as_deref() == Some(command_id.as_str())
                        && s.token_ca.as_deref() == Some(token.as_str())
                }) else {
                    return;
                };
                // 只取消这次未广播意图；恢复图表绝不再次产生买入信号。
                if let Some(trading) = self.shared.trading.get() {
                    trading.cancel_token(&token, "浏览器 OPEN 明确失败，取消未广播买入");
                }
                if let Some(st) = self.tokens.get_mut(&token) {
                    st.current_slot = None;
                    st.stage = match st.stage {
                        Stage::BActive => Stage::BPending,
                        Stage::BReentryActive => Stage::BReentryPending,
                        other => other,
                    };
                }
                let slot = &mut self.slots[index];
                slot.token_ca = None;
                slot.command_id = None;
                slot.command_sent_at = None;
                slot.state = SlotState::Blocked;
                self.slot_retry_not_before
                    .insert(slot_id.clone(), Instant::now() + Duration::from_secs(2));
                self.persist(&token);
                eprintln!(
                    "[browser] OPEN failed slot={slot_id} token={} error={error}; resync now",
                    tail8(&token)
                );
                let _ = self.browser_tx.send(BrowserCommand::SyncRequest);
                self.sync_shared().await;
            }
            Command::BrowserProgress {
                slot_id,
                command_id,
                token,
                window_id,
                tab_id,
                phase,
                page_clock_ms,
            } => {
                let token = token.to_lowercase();
                let owned = self.slots.iter().any(|s| {
                    s.slot_id == slot_id
                        && s.token_ca.as_deref() == Some(token.as_str())
                        && s.browser_window_id == Some(window_id)
                        && s.browser_tab_id == Some(tab_id)
                });
                if !owned || page_clock_ms.abs_diff(now_unix_ms()) > 120_000 {
                    return;
                }
                let Some(timing) = self
                    .browser_timings
                    .get_mut(&slot_id)
                    .filter(|t| t.command_id == command_id && t.ca == token)
                else {
                    return;
                };
                let point = match phase {
                    BrowserPhase::Received => &mut timing.received_at_ms,
                    BrowserPhase::Navigating => &mut timing.navigation_requested_at_ms,
                    BrowserPhase::UrlConfirmed => &mut timing.url_confirmed_at_ms,
                    BrowserPhase::DocumentReady => &mut timing.document_ready_at_ms,
                    BrowserPhase::ChartContainer => &mut timing.chart_container_at_ms,
                };
                if point.is_none() {
                    *point = Some(page_clock_ms);
                    eprintln!("[browser-latency] slot={slot_id} command={command_id} token={} phase={phase:?} page_ms={page_clock_ms} open_ms={} elapsed_ms={}",
                        tail8(&token), timing.open_sent_at_ms, page_clock_ms.saturating_sub(timing.open_sent_at_ms));
                }
            }
            Command::QuoteUsd { quote, price } => {
                self.quote_usd_lookups_inflight.remove(&quote);
                if let Some(price) = price.filter(|p| p.is_finite() && *p > 0.0) {
                    self.quote_usd.insert(quote.clone(), price);
                    self.refresh_market_caps_for_quote(&quote);
                }
            }
            Command::QuoteSymbol { quote, symbol } => {
                // 无论成败都先清除在途标记，失败的查询才能被后续 tick 重试。
                self.quote_lookups_inflight.remove(&quote);
                // 只有解析成功才写缓存。失败（None）不缓存，否则一次回填期间的
                // 超时会被永久记住，前端就永远显示「未知」——实测确实发生过。
                let mut touched = false;
                if let Some(sym) = &symbol {
                    self.quote_symbols.insert(quote.clone(), Some(sym.clone()));
                    for ca in self.tokens.live_keys() {
                        let st = self.tokens.get_mut(&ca).expect("active token");
                        if st.quote_token == quote && st.quote_symbol.is_none() {
                            st.quote_symbol = Some(sym.clone());
                            touched = true;
                        }
                    }
                }
                if touched {
                    self.sync_shared().await;
                }
            }
            Command::TokenMetadata {
                token,
                name,
                symbol,
                total_supply_tokens,
                quote_token,
            } => {
                self.metadata_lookups_inflight.remove(&token);
                let Some(platform) = self.tokens.get(&token).map(|st| st.source_platform.clone())
                else {
                    self.metadata_retry_not_before.remove(&token);
                    return;
                };
                if self
                    .tokens
                    .get(&token)
                    .is_some_and(|st| st.stage.is_terminal())
                {
                    self.metadata_retry_not_before.remove(&token);
                    return;
                }

                let mut resolved_quote = None;
                if let Some(st) = self.tokens.get_mut(&token) {
                    if let Some(value) = name.filter(|value| !value.is_empty()) {
                        st.name = value;
                    }
                    if let Some(value) = symbol.filter(|value| !value.is_empty()) {
                        st.symbol = value;
                    }
                    if let Some(value) =
                        total_supply_tokens.filter(|value| value.is_finite() && *value > 0.0)
                    {
                        st.total_supply_tokens = value;
                    }
                    if let Some(value) = quote_token.filter(|value| !value.is_empty()) {
                        st.quote_token = value.to_lowercase();
                        resolved_quote = Some(st.quote_token.clone());
                    }
                }
                if let Some(quote) = resolved_quote {
                    self.pending_quote.insert(token.clone(), quote.clone());
                    let symbol = self.quote_symbol_now(&quote);
                    if let Some(st) = self.tokens.get_mut(&token) {
                        st.quote_symbol = symbol;
                    }
                }

                let complete = self.tokens.get(&token).is_some_and(|st| match platform {
                    LaunchPlatform::GeniusFun => {
                        !st.name.is_empty() && !st.symbol.is_empty() && st.total_supply_tokens > 0.0
                    }
                    LaunchPlatform::FourMeme => {
                        self.pending_quote.contains_key(&token)
                            || st.quote_token != config::ZERO_ADDRESS
                    }
                    _ => true,
                });
                if complete {
                    self.metadata_retry_not_before.remove(&token);
                } else {
                    self.metadata_retry_not_before
                        .insert(token.clone(), Instant::now() + Duration::from_secs(2));
                }
                self.persist(&token);
                if self.tokens.get(&token).is_some_and(|st| st.stage.is_b()) {
                    self.sync_shared().await;
                }
            }
            Command::BlockTimestamp { block, result } => {
                if self.reset_cutoff != 0 && block <= self.reset_cutoff {
                    return;
                }
                self.block_timestamp_inflight.remove(&block);
                self.block_timestamp_tasks.remove(&block);
                if self.block_timestamp_cache.contains_key(&block) {
                    return;
                }
                match result {
                    Ok(timestamp) => {
                        self.block_timestamp_cache.insert(block, timestamp);
                        self.trim_block_timestamp_cache();
                        self.drain_pending_decoded_logs().await;
                    }
                    Err(error) => {
                        eprintln!(
                            "[warn] 区块 {block} 时间查询失败，后台重试且不停止日志接收: {error}"
                        );
                        self.dispatch_block_timestamp_lookup(block, Duration::from_millis(250));
                    }
                }
            }
            Command::TaxLookup { token, result } => {
                self.tax_lookups_inflight.remove(&token);
                if !self.tokens.contains_key(&token)
                    || self
                        .tokens
                        .get(&token)
                        .is_some_and(|st| st.stage.is_terminal())
                {
                    self.tax_retry_not_before.remove(&token);
                    return;
                }
                match result {
                    Ok((buy_bps, sell_bps)) => {
                        self.tax_retry_not_before.remove(&token);
                        self.pending_tax.insert(token.clone(), (buy_bps, sell_bps));
                        if let Some(st) = self.tokens.get_mut(&token) {
                            st.buy_tax_bps = buy_bps;
                            st.sell_tax_bps = sell_bps;
                        }
                        self.finalize_classification(&token).await;
                    }
                    Err(error) => {
                        self.tax_retry_not_before
                            .insert(token.clone(), Instant::now() + Duration::from_secs(2));
                        eprintln!(
                            "[warn] {} 税率暂不可用（{}），保持 TAX_PENDING 并重试: {error}",
                            tail8(&token),
                            self.tokens
                                .get(&token)
                                .map(|st| st.source_platform.to_string())
                                .unwrap_or_else(|| "未知平台".into())
                        );
                    }
                }
            }
            Command::TransactionSender { tx_hash, sender } => {
                let tx_hash = tx_hash.to_lowercase();
                self.sender_tasks.remove(&tx_hash);
                let Ok(sender) = sender else {
                    // HTTP 节点短暂不可用/交易尚未同步不等于“没有目标买家”。
                    // 保留等待项，但重试走独立 HTTP，绝不占用订阅 WSS。
                    let keep = self
                        .sender_lookups_inflight
                        .get_mut(&tx_hash)
                        .map(|waiters| {
                            waiters.retain(|(token, _, _)| {
                                self.tokens
                                    .get(token)
                                    .is_some_and(|st| !st.stage.is_terminal())
                            });
                            !waiters.is_empty()
                        })
                        .unwrap_or(false);
                    if keep {
                        self.dispatch_sender_lookup(tx_hash, Duration::from_millis(500));
                    } else {
                        self.sender_lookups_inflight.remove(&tx_hash);
                    }
                    return;
                };
                let waiters = self
                    .sender_lookups_inflight
                    .remove(&tx_hash)
                    .unwrap_or_default();
                let mut promote = Vec::new();
                let mut matched_tokens = Vec::new();
                for (token, trade_ts, lookup_started) in waiters {
                    let Some(st) = self.tokens.get_mut(&token) else {
                        continue;
                    };
                    st.pending_sender_lookups = st.pending_sender_lookups.saturating_sub(1);
                    let matched = crate::targets::is_target_buyer(&sender);
                    if matched && accepts_a_window(st) && trade_ts < st.observe_until_ts {
                        st.target_buyer_seen = true;
                        st.b_timing.buyer_at.get_or_insert(trade_ts);
                        eprintln!(
                            "[买家] {} tx.from 命中，HTTP 确认 {}ms",
                            tail8(&token),
                            lookup_started.elapsed().as_millis()
                        );
                        matched_tokens.push(token.clone());
                        if st.stage == Stage::AWatching && st.bull_streak_seen {
                            promote.push(token);
                        }
                    }
                }
                matched_tokens.sort();
                matched_tokens.dedup();
                for token in matched_tokens {
                    self.cancel_sender_lookups_for_token(&token);
                    if !promote.contains(&token) {
                        self.persist(&token);
                    }
                }
                promote.sort();
                promote.dedup();
                for token in promote {
                    self.promote(&token).await;
                }
                self.sender_tx_cache
                    .insert(tx_hash.clone(), sender.to_lowercase());
                self.sender_tx_cache_order.push_back(tx_hash);
                while self.sender_tx_cache_order.len() > 50_000 {
                    if let Some(oldest) = self.sender_tx_cache_order.pop_front() {
                        self.sender_tx_cache.remove(&oldest);
                    }
                }
            }
        }
    }

    fn persist(&self, token: &str) {
        self.persist_checked(token);
    }

    fn persist_checked(&self, token: &str) -> bool {
        let started = Instant::now();
        let mut saved = false;
        if let Some(st) = self.tokens.get(token) {
            match self.store.upsert_token(&persisted_of(st), now_unix()) {
                Ok(()) => saved = true,
                Err(e) => eprintln!("[warn] SQLite 保存 {} 失败: {e}", tail8(token)),
            }
        }
        if started.elapsed() > Duration::from_millis(20) {
            eprintln!(
                "[latency] sqlite_ms={} token={}",
                started.elapsed().as_millis(),
                tail8(token)
            );
        }
        saved
    }

    /// 行情类字段最多每个 Token 每 500ms 落盘一次。状态转换仍调用
    /// `persist` 立即写入，因此这个节流只减少热门币每秒上百笔成交对
    /// 单 actor 的同步 SQLite 阻塞。
    fn persist_trade_throttled(&mut self, token: &str) {
        // 短暂突破 $10K 的第一次证据仍同步耐久保存，不能被合并/重启丢掉。
        let new_peak = self
            .tokens
            .get(token)
            .filter(|st| st.stage.is_b() && st.cycle_max_market_cap >= config::B_CYCLE_MIN_USD)
            .and_then(|st| st.current_cycle_start)
            .filter(|start| self.durable_cycle_peaks.get(token) != Some(start));
        if let Some(start) = new_peak {
            if self.persist_checked(token) {
                self.durable_cycle_peaks.insert(token.to_string(), start);
            }
            return;
        }
        let now = Instant::now();
        if self
            .trade_persist_not_before
            .get(token)
            .is_some_and(|not_before| now < *not_before)
        {
            return;
        }
        if let Some(st) = self.tokens.get(token) {
            self.store.queue_market(persisted_of(st));
        }
        self.trade_persist_not_before
            .insert(token.to_string(), now + Duration::from_millis(500));
    }

    /// 一枚 Token 已命中目标买家后，其余买入的 tx.from 结果已无意义。
    /// 及时移除等待项，避免某个慢节点让 `pending_sender_lookups` 一直大于 0，
    /// 进而阻止未达到三连阳的 A 币正常到期。
    fn cancel_sender_lookups_for_token(&mut self, token: &str) {
        let mut removed = 0usize;
        self.sender_lookups_inflight.retain(|_, waiters| {
            let before = waiters.len();
            waiters.retain(|(waiting_token, _, _)| waiting_token != token);
            removed += before - waiters.len();
            !waiters.is_empty()
        });
        self.sender_tasks.retain(|hash, task| {
            if self.sender_lookups_inflight.contains_key(hash) {
                true
            } else {
                task.abort();
                false
            }
        });
        if removed > 0 {
            if let Some(st) = self.tokens.get_mut(token) {
                st.pending_sender_lookups = st.pending_sender_lookups.saturating_sub(removed);
            }
        }
    }

    fn release_token_from_slot(&mut self, token: &str) {
        if let Some(trading) = self.shared.trading.get() {
            trading.cancel_token(token, "Slot 分配取消，停止未广播买入");
        }
        for slot in &mut self.slots {
            if slot.token_ca.as_deref() == Some(token) {
                slot.token_ca = None;
                slot.command_id = None;
                slot.command_sent_at = None;
                slot.state = if slot.browser_window_id.is_some() && slot.browser_tab_id.is_some() {
                    SlotState::Empty
                } else {
                    SlotState::Disconnected
                };
            }
        }
    }

    async fn reconcile_browser_report(&mut self, report: BrowserSlotReport) {
        let Some(index) = self.slots.iter().position(|s| s.slot_id == report.slot_id) else {
            return;
        };
        let binding_changed = self.slots[index].browser_window_id != report.window_id
            || self.slots[index].browser_tab_id != report.tab_id;
        if report.recreated || binding_changed {
            if let (Some(trading), Some(ca)) = (
                self.shared.trading.get(),
                self.slots[index].token_ca.as_deref(),
            ) {
                trading.cancel_token(ca, "浏览器 Slot 绑定变化，取消未广播买入");
            }
        }
        self.slots[index].browser_window_id = report.window_id;
        self.slots[index].browser_tab_id = report.tab_id;
        // HELLO may snapshot the old page while tabs.update is still in flight.
        // Preserve its command/ACK deadline; only explicit URL release cancels it.
        if !binding_changed
            && !report.recreated
            && self.slots[index].state == SlotState::Opening
            && self.slots[index]
                .command_sent_at
                .is_some_and(|at| at.elapsed() < config::OPEN_ACK_TIMEOUT)
            && (gmgn_token_from_url(&report.url) != self.slots[index].token_ca
                || report.pending_url.as_deref().and_then(gmgn_token_from_url)
                    == self.slots[index].token_ca)
        {
            return;
        }
        if is_gmgn_home(&report.url) {
            // 窗口意外丢失后由扩展重建出来的首页，不是用户主动释放。
            // 若后端仍绑定着 Token，应在这个新窗口的唯一 Tab 中恢复原页面。
            if report.recreated || binding_changed {
                if let Some(ca) = self.slots[index].token_ca.clone() {
                    let command_id = uuid::Uuid::new_v4().to_string();
                    let sent_ms = now_unix_ms();
                    self.record_browser_open(&report.slot_id, &command_id, &ca, sent_ms);
                    self.slots[index].state = SlotState::Opening;
                    self.slots[index].command_id = Some(command_id.clone());
                    self.slots[index].command_sent_at = Some(Instant::now());
                    if let Some(st) = self.tokens.get_mut(&ca) {
                        st.first_open_sent_at_ms.get_or_insert(sent_ms);
                        st.last_open_sent_at_ms = Some(sent_ms);
                        st.last_open_acked_at_ms = None;
                    }
                    self.persist(&ca);
                    let _ = self.browser_tx.send(BrowserCommand::Open {
                        command_id,
                        slot_id: report.slot_id,
                        ca: ca.clone(),
                        url: format!("https://gmgn.ai/bsc/token/{ca}"),
                    });
                    return;
                }
            }
            self.release_slot(&report.slot_id).await;
            return;
        }
        if let Some(page_token) = gmgn_token_from_url(&report.url) {
            let expected = self.slots[index].token_ca.clone();
            if expected.as_deref() == Some(page_token.as_str()) {
                let needs_persist = self.tokens.get(&page_token).is_some_and(|st| {
                    st.stage.is_pending()
                        || st.current_slot.as_deref() != Some(report.slot_id.as_str())
                });
                self.slots[index].state = SlotState::Occupied;
                self.slots[index].command_id = None;
                self.slots[index].command_sent_at = None;
                if let Some(st) = self.tokens.get_mut(&page_token) {
                    st.current_slot = Some(report.slot_id.clone());
                    if st.stage.is_pending() {
                        st.stage = if st.stage == Stage::BReentryPending {
                            Stage::BReentryActive
                        } else {
                            Stage::BActive
                        };
                    }
                }
                if needs_persist {
                    self.persist(&page_token);
                }
            } else {
                if let Some(expected) = expected {
                    if let Some(trading) = self.shared.trading.get() {
                        trading.cancel_token(&expected, "窗口手动切换到其他 Token，取消未广播买入");
                    }
                    if let Some(st) = self.tokens.get_mut(&expected) {
                        st.current_slot = None;
                        st.stage = match st.stage {
                            Stage::BActive => Stage::BPending,
                            Stage::BReentryActive => Stage::BReentryPending,
                            other => other,
                        };
                    }
                    self.persist(&expected);
                }
                self.slots[index].state = SlotState::Blocked;
                self.slots[index].token_ca = None;
                self.slots[index].command_id = None;
                self.slots[index].command_sent_at = None;
            }
        } else {
            // 非首页/非目标 Token 的 URL（例如 about:blank 或其他 chain）
            // 不能成为 EMPTY，更不能伪造一次主动释放。
            self.slots[index].state = SlotState::Blocked;
        }
    }

    async fn handle_browser_url(&mut self, slot_id: &str, window_id: i64, tab_id: i64, url: &str) {
        if self.reset_pending != 0 {
            return;
        }
        if let Some(slot) = self.slots.iter_mut().find(|s| s.slot_id == slot_id) {
            if slot.browser_window_id.is_some_and(|id| id != window_id)
                || slot.browser_tab_id.is_some_and(|id| id != tab_id)
            {
                eprintln!("[warn] Slot {slot_id} 收到非绑定窗口/标签页的 URL，已忽略");
                return;
            }
            slot.browser_window_id = Some(window_id);
            slot.browser_tab_id = Some(tab_id);
        } else {
            return;
        }
        if is_gmgn_home(url) {
            self.release_slot(slot_id).await;
            self.schedule(true).await;
            self.sync_shared().await;
        }
    }

    async fn release_slot(&mut self, slot_id: &str) {
        let token = self
            .slots
            .iter()
            .find(|s| s.slot_id == slot_id)
            .and_then(|s| s.token_ca.clone());
        if let Some(slot) = self.slots.iter_mut().find(|s| s.slot_id == slot_id) {
            slot.token_ca = None;
            slot.command_id = None;
            slot.command_sent_at = None;
            slot.state = if slot.browser_window_id.is_some() && slot.browser_tab_id.is_some() {
                SlotState::Empty
            } else {
                SlotState::Disconnected
            };
        }
        let Some(token) = token else {
            return;
        };
        if let Some(trading) = self.shared.trading.get() {
            trading.cancel_token(&token, "用户释放 Slot，取消未广播买入");
        }
        if let Some(st) = self.tokens.get_mut(&token) {
            st.current_slot = None;
            match st.stage {
                Stage::BActive if !st.reentry_used => {
                    let entered = now_unix();
                    st.stage = Stage::CWatching;
                    st.c_entered_at = Some(entered);
                    st.reentry_used = true;
                    st.c_streak = 0;
                    st.c_streak_end = None;
                    st.c_first_bucket = Some(entered.div_ceil(config::CANDLE_PERIOD_SECS));
                }
                Stage::BReentryActive | Stage::BActive => st.stage = Stage::Dead,
                _ => {}
            }
        }
        if self
            .tokens
            .get(&token)
            .is_some_and(|st| st.stage.is_terminal())
        {
            self.tokens.active.remove(&token);
            self.blacklist.insert(token.clone());
        }
        self.persist(&token);
    }

    async fn schedule(&mut self, released_slot: bool) {
        self.schedule_for(released_slot, None).await;
    }

    fn record_browser_open(&mut self, slot_id: &str, command_id: &str, ca: &str, sent_ms: u64) {
        self.browser_timings.insert(
            slot_id.to_string(),
            BrowserTiming {
                slot_id: slot_id.into(),
                command_id: command_id.into(),
                ca: ca.into(),
                open_sent_at_ms: sent_ms,
                received_at_ms: None,
                navigation_requested_at_ms: None,
                url_confirmed_at_ms: None,
                document_ready_at_ms: None,
                chart_container_at_ms: None,
            },
        );
    }

    async fn schedule_for(&mut self, released_slot: bool, new_buy_ca: Option<&str>) {
        if self.shared.system.is_stopping() || self.reset_pending != 0 || self.reset_fetching {
            return;
        }
        loop {
            if self.shared.system.is_stopping() {
                break;
            }
            let Some(slot_index) = self.slots.iter().position(|s| {
                s.state == SlotState::Empty
                    && s.token_ca.is_none()
                    && self
                        .slot_retry_not_before
                        .get(&s.slot_id)
                        .is_none_or(|at| Instant::now() >= *at)
            }) else {
                break;
            };
            let mut pending: Vec<&TokenState> = self
                .tokens
                .live_values()
                .filter(|t| t.stage.is_pending() && t.current_slot.is_none() && !t.user_discarded)
                .collect();
            if released_slot {
                pending.sort_by(|a, b| {
                    b.last_mcap_usd
                        .unwrap_or(-1.0)
                        .total_cmp(&a.last_mcap_usd.unwrap_or(-1.0))
                        .then_with(|| a.promoted_at.cmp(&b.promoted_at))
                });
            } else {
                pending.sort_by(|a, b| {
                    a.promoted_at
                        .cmp(&b.promoted_at)
                        .then_with(|| a.token.cmp(&b.token))
                });
            }
            // A fresh first-B with an available Slot reserves it now. Older full-slot
            // candidates may later enter charts, but never consume this buy signal.
            let chosen = new_buy_ca
                .and_then(|new_ca| pending.iter().find(|t| t.token == new_ca))
                .or_else(|| pending.first());
            let Some(chosen) = chosen else {
                break;
            };
            let ca = chosen.token.clone();
            let command_id = uuid::Uuid::new_v4().to_string();
            let slot_id = self.slots[slot_index].slot_id.clone();
            self.slots[slot_index].token_ca = Some(ca.clone());
            self.slots[slot_index].state = SlotState::Opening;
            self.slots[slot_index].command_id = Some(command_id.clone());
            self.slots[slot_index].command_sent_at = Some(Instant::now());
            if let Some(st) = self.tokens.get_mut(&ca) {
                st.current_slot = Some(slot_id.clone());
                st.last_open_acked_at_ms = None;
            }
            // Buy only the exact newly promoted first-B selected for a real bound Slot.
            // Persistence of its reservation + intent precedes any signing, but no chart ACK wait.
            if new_buy_ca == Some(ca.as_str())
                && self.slots[slot_index].browser_window_id.is_some()
                && self.slots[slot_index].browser_tab_id.is_some()
            {
                if let (Some(trading), Some(st)) = (self.shared.trading.get(), self.tokens.get(&ca))
                {
                    if let Err(error) = trading.offer(&persisted_of(st), &command_id) {
                        eprintln!("[trade] {} 买入意图跳过: {error}", tail8(&ca));
                    }
                }
            }
            // 纯监控仍先发 OPEN 后写盘；交易模式的 Slot + 意图必须先耐久保存。
            // 所有报价/模拟/签名/回执均由独立执行器处理，不阻塞监控 actor。
            let sent_ms = now_unix_ms();
            self.record_browser_open(&slot_id, &command_id, &ca, sent_ms);
            let _ = self.browser_tx.send(BrowserCommand::Open {
                command_id,
                slot_id: slot_id.clone(),
                ca: ca.clone(),
                url: format!("https://gmgn.ai/bsc/token/{ca}"),
            });
            // 统计放在真正发送之后：原先在发送前取值，这个数字会漏掉发送
            // 本身的开销，等于长期少报。
            if let Some(st) = self.tokens.get_mut(&ca) {
                st.first_open_sent_at_ms.get_or_insert(sent_ms);
                st.last_open_sent_at_ms = Some(sent_ms);
                let elapsed = st
                    .last_qualified_at_ms
                    .map(|qualified| sent_ms.saturating_sub(qualified));
                eprintln!(
                    "[调度] OPEN Slot {slot_id} {} 资格确认→OPEN {}ms",
                    tail8(&ca),
                    elapsed
                        .map(|ms| ms.to_string())
                        .unwrap_or_else(|| "?".into())
                );
            }
            self.persist(&ca);
        }
    }

    /// 后台解析报价代币符号，结果经指令通道回填。
    ///
    /// 绝不在事件循环里 await 链上查询：那样会阻塞事件消费，导致广播缓冲
    /// 溢出、成交事件被丢弃（表现为 A 币识别正常却永远晋升不了 B 币）。
    fn spawn_quote_lookup(&mut self, quote: String) {
        self.quote_lookups_inflight.insert(quote.clone());
        let rpc = self.read_rpc.clone();
        let tx = self.cmd_tx.clone();
        let permits = self.metadata_permits.clone();
        tokio::spawn(async move {
            let Ok(_permit) = permits.acquire_owned().await else {
                return;
            };
            let sym = rpc.fetch_symbol(&quote).await;
            let _ = tx.send(Command::QuoteSymbol { quote, symbol: sym });
        });
    }

    /// 派发 Token metadata 查询。这里只负责展示信息和 Four.meme 报价资产，
    /// 绝不在日志热路径中等待。策略所需的成交/K 线会在这些字段尚未补齐时照常
    /// 收集；税率另走高优先级的 `ensure_tax_lookup`。
    fn ensure_token_metadata(&mut self, token: &str) {
        if self.metadata_lookups_inflight.contains(token)
            || self
                .metadata_retry_not_before
                .get(token)
                .is_some_and(|at| Instant::now() < *at)
        {
            return;
        }
        let Some(st) = self.tokens.get(token) else {
            return;
        };
        if st.stage.is_terminal() {
            return;
        }
        let platform = st.source_platform.clone();
        let needs_lookup = match platform {
            LaunchPlatform::GeniusFun => {
                st.name.is_empty() || st.symbol.is_empty() || st.total_supply_tokens <= 0.0
            }
            LaunchPlatform::FourMeme => {
                !self.pending_quote.contains_key(token) && st.quote_token == config::ZERO_ADDRESS
            }
            _ => false,
        };
        if !needs_lookup {
            self.metadata_retry_not_before.remove(token);
            return;
        }

        self.metadata_lookups_inflight.insert(token.to_string());
        let token = token.to_string();
        let rpc = self.read_rpc.clone();
        let tx = self.cmd_tx.clone();
        let permits = self.metadata_permits.clone();
        let task = tokio::spawn(async move {
            let Ok(_permit) = permits.acquire_owned().await else {
                return;
            };
            let (name, symbol, total_supply_tokens, quote_token) = match platform {
                LaunchPlatform::GeniusFun => {
                    let (name, symbol, supply) = tokio::join!(
                        rpc.fetch_name(&token),
                        rpc.fetch_symbol(&token),
                        rpc.fetch_u128(&token, "totalSupply()"),
                    );
                    (
                        name,
                        symbol,
                        supply.ok().map(|value| value as f64 / 1e18),
                        None,
                    )
                }
                LaunchPlatform::FourMeme => (None, None, None, rpc.fetch_four_quote(&token).await),
                _ => return,
            };
            let _ = tx.send(Command::TokenMetadata {
                token,
                name,
                symbol,
                total_supply_tokens,
                quote_token,
            });
        });
        self.token_query_tasks.retain(|task| !task.is_finished());
        self.token_query_tasks.push(task);
    }

    fn dispatch_block_timestamp_lookup(&mut self, block: u64, delay: Duration) {
        if self.block_timestamp_cache.contains_key(&block)
            || !self.block_timestamp_inflight.insert(block)
        {
            return;
        }
        let rpc = self.block_rpc.clone();
        let tx = self.cmd_tx.clone();
        let task = tokio::spawn(async move {
            // Allow the matching header to arrive before issuing an HTTP fallback.
            tokio::time::sleep(delay.max(Duration::from_millis(200))).await;
            let result = rpc.block_timestamp(block).await;
            let _ = tx.send(Command::BlockTimestamp { block, result });
        });
        self.block_timestamp_tasks.insert(block, task);
    }

    fn trim_block_timestamp_cache(&mut self) {
        while self.block_timestamp_cache.len() > 4096 {
            let Some(oldest) = self.block_timestamp_cache.keys().min().copied() else {
                break;
            };
            self.block_timestamp_cache.remove(&oldest);
        }
    }

    /// 非阻塞解析 Four.meme / GeniusFun 税率。失败结果只会安排重试，
    /// 不会被伪装成超过阈值的税率。
    fn ensure_tax_lookup(&mut self, token: &str) {
        if self.tax_lookups_inflight.contains(token)
            || self
                .tax_retry_not_before
                .get(token)
                .is_some_and(|at| Instant::now() < *at)
        {
            return;
        }
        let Some(st) = self.tokens.get(token) else {
            return;
        };
        if st.counted
            || st.stage.is_terminal()
            || !requires_rpc_tax(&st.source_platform)
            || self.pending_tax.contains_key(token)
        {
            return;
        }
        let platform = st.source_platform.clone();
        let contract = match platform {
            LaunchPlatform::FourMeme => st.token.clone(),
            LaunchPlatform::GeniusFun => {
                let Some(curve) = st.source_ref.clone() else {
                    self.tax_retry_not_before
                        .insert(token.to_string(), Instant::now() + Duration::from_secs(2));
                    eprintln!("[warn] {} 缺少 GeniusFun curve，税率保持待定", tail8(token));
                    return;
                };
                curve
            }
            _ => return,
        };
        self.tax_lookups_inflight.insert(token.to_string());
        let token = token.to_string();
        let rpc = self.read_rpc.clone();
        let tx = self.cmd_tx.clone();
        let task = tokio::spawn(async move {
            let result = match platform {
                LaunchPlatform::FourMeme => rpc.fetch_four_taxes(&contract).await,
                LaunchPlatform::GeniusFun => rpc
                    .fetch_u128(&contract, "creatorTaxBps()")
                    .await
                    .map(|value| (value as u64, value as u64)),
                _ => return,
            };
            let _ = tx.send(Command::TaxLookup { token, result });
        });
        self.token_query_tasks.retain(|task| !task.is_finished());
        self.token_query_tasks.push(task);
    }

    /// 非阻塞补查买入交易的发起地址。事件 actor 只负责成交归属，目标地址
    /// 判定以交易 `from` 为准，与 GMGN 的买家展示保持一致。
    fn spawn_sender_lookup(&mut self, tx_hash: String, token: String, trade_ts: u64) {
        let tx_hash = tx_hash.to_lowercase();
        if let Some(waiters) = self.sender_lookups_inflight.get_mut(&tx_hash) {
            if !waiters
                .iter()
                .any(|(t, ts, _)| t == &token && *ts == trade_ts)
            {
                waiters.push((token.clone(), trade_ts, Instant::now()));
                if let Some(st) = self.tokens.get_mut(&token) {
                    st.pending_sender_lookups += 1;
                }
            }
            return;
        }
        self.sender_lookups_inflight.insert(
            tx_hash.clone(),
            vec![(token.clone(), trade_ts, Instant::now())],
        );
        if let Some(st) = self.tokens.get_mut(&token) {
            st.pending_sender_lookups += 1;
        }
        self.dispatch_sender_lookup(tx_hash, Duration::ZERO);
    }

    /// 派发（或延迟重派）一次独立 HTTP `tx.from` 查询。
    fn dispatch_sender_lookup(&mut self, tx_hash: String, delay: Duration) {
        let sender_rpc = self.sender_rpc.clone();
        let tx = self.cmd_tx.clone();
        let key = tx_hash.clone();
        let task = tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let sender = sender_rpc.transaction_sender(&tx_hash).await;
            let _ = tx.send(Command::TransactionSender { tx_hash, sender });
        });
        if let Some(old) = self.sender_tasks.insert(key, task) {
            old.abort();
        }
    }

    async fn on_log(&mut self, log: RawLog) {
        if self.shared.system.is_stopping() {
            return;
        }
        if self.reset_cutoff != 0 && log.block_number <= self.reset_cutoff {
            return;
        }
        if log.topic0 == crate::recovery::CHECKPOINT_TOPIC {
            // Marker follows the batch on the SAME FIFO channel. It cannot ACK
            // past logs still awaiting block timestamps, unlike a priority command.
            self.pending_decoded_logs.push_back(PendingDecodedLog {
                log,
                events: Vec::new(),
                needs_block_timestamp: false,
            });
            self.drain_pending_decoded_logs().await;
            return;
        }
        if log.topic0 == "__block_timestamp" {
            if let Ok(bytes) = <[u8; 8]>::try_from(log.data.as_slice()) {
                self.latest_head_ts = self.latest_head_ts.max(u64::from_be_bytes(bytes));
                if let Some(task) = self.block_timestamp_tasks.remove(&log.block_number) {
                    task.abort();
                }
                self.block_timestamp_inflight.remove(&log.block_number);
                self.block_timestamp_cache
                    .insert(log.block_number, u64::from_be_bytes(bytes));
                self.trim_block_timestamp_cache();
                self.drain_pending_decoded_logs().await;
            }
            return;
        }
        let key = (log.block_number, log.tx_index, log.log_index);
        if log.block_number > 0 && !self.seen_log_keys.insert(key) {
            return;
        }
        if log.block_number > 0 {
            self.seen_log_order.push_back(key);
            while self.seen_log_order.len() > 50_000 {
                if let Some(old) = self.seen_log_order.pop_front() {
                    self.seen_log_keys.remove(&old);
                }
            }
        }

        let events = self.launchpads.decode(&log);
        // Platform timestamps are on-chain block timestamps. Reuse them for
        // timestamp-free events in the same block, preserving FIFO execution.
        for event in &events {
            let timestamp = match event {
                NormalizedEvent::NewToken(token) => token.created_at,
                NormalizedEvent::Trade(trade) => trade.ts,
                _ => 0,
            };
            if timestamp > 0 {
                self.block_timestamp_cache
                    .insert(log.block_number, timestamp);
                if let Some(task) = self.block_timestamp_tasks.remove(&log.block_number) {
                    task.abort();
                }
                self.block_timestamp_inflight.remove(&log.block_number);
                break;
            }
        }
        let needs_block_timestamp = events.iter().any(|event| match event {
            NormalizedEvent::NewToken(token) => token.created_at == 0,
            NormalizedEvent::Trade(trade) => trade.ts == 0,
            _ => false,
        });
        if needs_block_timestamp && !self.block_timestamp_cache.contains_key(&log.block_number) {
            self.dispatch_block_timestamp_lookup(log.block_number, Duration::ZERO);
        }
        self.pending_decoded_logs.push_back(PendingDecodedLog {
            log,
            events,
            needs_block_timestamp,
        });
        self.drain_pending_decoded_logs().await;
    }

    /// 严格按日志到达顺序消费。队首若仍等区块时间就立即返回，让主循环继续
    /// 接收 WSS；时间回填命令到达后再一次性快速排空，不会改变区块内顺序。
    async fn drain_pending_decoded_logs(&mut self) {
        loop {
            let Some(front) = self.pending_decoded_logs.front() else {
                return;
            };
            let block_timestamp = if front.needs_block_timestamp {
                let Some(timestamp) = self
                    .block_timestamp_cache
                    .get(&front.log.block_number)
                    .copied()
                else {
                    return;
                };
                Some(timestamp)
            } else {
                None
            };
            let pending = self
                .pending_decoded_logs
                .pop_front()
                .expect("front was present");
            self.process_decoded_log(pending.log, pending.events, block_timestamp)
                .await;
        }
    }

    async fn process_decoded_log(
        &mut self,
        log: RawLog,
        events: Vec<NormalizedEvent>,
        block_timestamp: Option<u64>,
    ) {
        let processed_block = log.block_number;
        if log.topic0 == crate::recovery::CHECKPOINT_TOPIC {
            if let Some((from, to, lagged_before)) = crate::recovery::decode_checkpoint(&log) {
                let old = self.shared.last_scanned_block.load(Ordering::Relaxed);
                if lagged_before == self.logs_lagged && from <= old.saturating_add(1) && to > old {
                    match self.store.set_meta_u64("last_scanned_block", to) {
                        Ok(()) => {
                            self.shared.last_scanned_block.store(to, Ordering::Relaxed);
                        }
                        Err(e) => eprintln!("[warn] 补扫进度未持久化，将重试：{e}"),
                    }
                }
            }
            return;
        }
        for event in events {
            match event {
                NormalizedEvent::NewToken(mut new_token) => {
                    if new_token.created_at == 0 {
                        new_token.created_at = block_timestamp.unwrap_or_else(now_unix);
                    }
                    // 策略热路径只需要 CA、来源、创建时间与 curve 映射。展示字段
                    // 在后台补齐；供应量查询期间使用平台默认值，避免市值成为 0。
                    if new_token.total_supply_tokens <= 0.0 {
                        new_token.total_supply_tokens = config::TOTAL_SUPPLY_TOKENS;
                    }
                    self.latest_chain_ts = self.latest_chain_ts.max(new_token.created_at);
                    let token = new_token.ca.to_lowercase();
                    self.on_created(new_token).await;
                    self.ensure_token_metadata(&token);
                    self.ensure_tax_lookup(&token);
                }
                NormalizedEvent::Tax {
                    token,
                    buy_bps,
                    sell_bps,
                } => {
                    let token = token.to_lowercase();
                    self.pending_tax.insert(token.clone(), (buy_bps, sell_bps));
                    if let Some(st) = self.tokens.get_mut(&token) {
                        st.buy_tax_bps = buy_bps;
                        st.sell_tax_bps = sell_bps;
                    }
                    self.finalize_classification(&token).await;
                }
                NormalizedEvent::Quote { token, quote } => {
                    let token = token.to_lowercase();
                    let quote = quote.to_lowercase();
                    self.pending_quote.insert(token.clone(), quote.clone());
                    let sym = self.quote_symbol_now(&quote);
                    if let Some(st) = self.tokens.get_mut(&token) {
                        st.quote_token = quote;
                        st.quote_symbol = sym;
                    }
                }
                NormalizedEvent::Trade(trade) => {
                    let token = trade.token.clone().map(|v| v.to_lowercase()).or_else(|| {
                        let venue = trade.source_ref.as_deref()?;
                        self.tokens
                            .live_values()
                            .find(|st| {
                                st.source_ref
                                    .as_deref()
                                    .is_some_and(|r| r.eq_ignore_ascii_case(venue))
                            })
                            .map(|st| st.token.clone())
                    });
                    let Some(token) = token else {
                        continue;
                    };
                    let ts = if trade.ts == 0 {
                        block_timestamp.unwrap_or_else(now_unix)
                    } else {
                        trade.ts
                    };
                    let normalized = Trade {
                        ts,
                        token,
                        wallet: trade.wallet,
                        token_amount: trade.token_amount,
                        quote_amount: trade.quote_amount,
                        fee: trade.fee,
                    };
                    self.on_trade(
                        normalized,
                        trade.is_buy,
                        log.tx_hash.clone(),
                        log.tx_from.clone(),
                        log.block_number,
                        trade.wallet_is_actor,
                    )
                    .await;
                }
            }
        }
        if processed_block > 0 {
            let old = self
                .shared
                .last_processed_block
                .fetch_max(processed_block, Ordering::Relaxed);
            if processed_block > old {
                let _ = self
                    .store
                    .set_meta_u64("last_processed_block", processed_block);
            }
        }
    }

    /// 建币：登记进观察表，但**不立即定级**。
    ///
    /// 实测同一建币交易内日志顺序恒为 Created → QuoteSet → TaxSet → AsymTax
    /// （30/30 样本一致），即建币事件到达时税率尚未可知。若此刻按 0 税计入
    /// A 币，稍后税率事件显示超标又会计入剔除，造成同一币双重计数
    /// （曾出现 aTokens + rejectedTax > createdSeen）。故改为延迟定级。
    async fn on_created(&mut self, c: NewTokenEvent) {
        if self.reset_cutoff != 0 && c.creation_block <= self.reset_cutoff {
            return;
        }
        let token = c.ca.to_lowercase();
        // 永久剔除过的币，即使重新建币也不再理。
        if self.blacklist.contains(&token) {
            return;
        }
        // Aggregator 以 CA 为唯一 ID。重复发现不创建第二个状态，只补缺失 metadata。
        if let Some(existing) = self.tokens.get_mut(&token) {
            if existing.name.is_empty() {
                existing.name = c.name;
            }
            if existing.symbol.is_empty() {
                existing.symbol = c.symbol;
            }
            if existing.creation_tx_hash.is_empty() {
                existing.creation_tx_hash = c.creation_tx_hash;
            }
            if existing.source_ref.is_none() {
                existing.source_ref = c.source_ref;
            }
            self.persist(&token);
            return;
        }

        self.stats_created += 1;
        if let Some(tax) = c.tax_bps {
            self.pending_tax.insert(token.clone(), tax);
        }

        self.pending_quote
            .entry(token.clone())
            .or_insert_with(|| c.quote_token.to_lowercase());
        let quote_token = self
            .pending_quote
            .get(&token)
            .cloned()
            .unwrap_or_else(|| config::ZERO_ADDRESS.to_string());
        // 不在建币路径上 await 链上查询（会阻塞事件消费）。统一走
        // quote_symbol_now：缓存命中直接给值，未命中派发后台任务且由在途集合去重。
        let quote_symbol = self.quote_symbol_now(&quote_token);

        let now = Instant::now();
        // 税率此刻未知，先按 0 占位；finalize_classification 会在税率确定后校正。
        let (buy_tax, sell_tax) = self.pending_tax.get(&token).copied().unwrap_or((0, 0));
        self.tokens.insert(
            token.clone(),
            TokenState {
                token: token.clone(),
                name: c.name.clone(),
                symbol: c.symbol.clone(),
                creator: c.creator,
                source_platform: c.source.clone(),
                creation_block: c.creation_block,
                creation_tx_hash: c.creation_tx_hash,
                source_ref: c.source_ref,
                total_supply_tokens: c.total_supply_tokens,
                created_ts: c.created_at,
                observe_until_ts: c.created_at + config::WATCH_WINDOW.as_secs(),
                deadline_wall: now + config::WATCH_WINDOW,
                buy_tax_bps: buy_tax,
                sell_tax_bps: sell_tax,
                counted: false,
                target_buyer_seen: false,
                pending_sender_lookups: 0,
                bull_streak_seen: false,
                bull_streak_value: 0,
                quote_token,
                quote_symbol: quote_symbol.clone(),
                stage: Stage::New,
                promoted_at: None,
                first_b_at: None,
                b_timing: BTiming::default(),
                first_qualified_at_ms: None,
                first_open_sent_at_ms: None,
                first_open_acked_at_ms: None,
                last_qualified_at_ms: None,
                last_open_sent_at_ms: None,
                last_open_acked_at_ms: None,
                c_entered_at: None,
                reentry_used: false,
                reentry_b_at: None,
                current_cycle_start: None,
                cycle_max_market_cap: 0.0,
                current_slot: None,
                user_discarded: false,
                last_mcap_usd: None,
                c_streak: 0,
                c_streak_end: None,
                c_first_bucket: None,
                promote_streak: 0,
                candles: BTreeMap::new(),
                streak: 0,
                streak_end: None,
                peak_streak: 0,
                trades: Vec::new(),
                trade_seq: 0,
                last_price: 0.0,
                last_mcap_quote: 0.0,
                last_activity: now,
            },
        );
        self.persist(&token);
        eprintln!(
            "[新币:{}] {} ({}) 报价 {}，块 {}，待定级",
            c.source,
            short(&c.symbol),
            tail8(&token),
            quote_symbol.as_deref().unwrap_or("?"),
            c.creation_block
        );
        if c.tax_bps.is_some() {
            self.finalize_classification(&token).await;
        }
    }

    /// 幂等地完成定级：税率超标则永久剔除，否则计为 A 币。
    ///
    /// 调用时机（三者任一，先到先定级，靠 `counted` 保证只算一次）：
    /// 1. 税率事件到达时——此时税率已确定；
    /// 2. 首笔成交到达时——成交必然发生在建币交易的后续交易，而日志按链上
    ///    顺序处理，故看到成交即代表建币交易内的税率事件已处理完毕；
    ///    仍无税率事件则为标准币，按 0 税处理（与官方文档语义一致）。
    /// 3. tick 时——兜底从未成交的标准币。主循环用 biased select 且日志分支
    ///    置首，tick 只在日志队列排空后运行，同样满足「建币交易已处理完」。
    async fn finalize_classification(&mut self, token: &str) -> bool {
        let Some(st) = self.tokens.get(token) else {
            return false;
        };
        if st.counted || st.stage.is_terminal() || self.blacklist.contains(token) {
            return false;
        }
        // Four/Genius 必须拿到链上 RPC 的明确结果才能定级。None 代表
        // TAX_PENDING；网络错误既不能当 0 税放行，也不能伪造成 4.01% 剔除。
        if requires_rpc_tax(&st.source_platform) && !self.pending_tax.contains_key(token) {
            return false;
        }
        let (buy, sell) = self.pending_tax.get(token).copied().unwrap_or((0, 0));
        let (name, symbol) = (st.name.clone(), st.symbol.clone());

        let exceeded = !tax_passes(buy, sell);
        if let Some(st) = self.tokens.get_mut(token) {
            st.counted = true;
            st.buy_tax_bps = buy;
            st.sell_tax_bps = sell;
        }

        if exceeded {
            self.stats_rejected += 1;
            eprintln!(
                "[筛除] {} ({}) 买税 {:.2}% / 卖税 {:.2}%，超过 {:.0}% 上限",
                short(&name),
                tail8(token),
                buy as f64 / 100.0,
                sell as f64 / 100.0,
                config::MAX_TAX_BPS as f64 / 100.0
            );
            self.purge(token, "买卖税超过 4% 上限").await;
            return true;
        }

        self.stats_a += 1;
        if let Some(st) = self.tokens.get_mut(token) {
            st.stage = Stage::AWatching;
        }
        self.persist(token);
        eprintln!(
            "[A币] {} ({}) 买税 {:.2}% / 卖税 {:.2}%，进入 {} 秒观察窗",
            short(&symbol),
            tail8(token),
            buy as f64 / 100.0,
            sell as f64 / 100.0,
            config::WATCH_WINDOW.as_secs()
        );
        if self.tokens.get(token).is_some_and(|st| {
            st.stage == Stage::AWatching && st.target_buyer_seen && st.bull_streak_seen
        }) {
            self.promote(token).await;
        }
        false
    }

    /// 成交：聚合 K 线、记录明细、推进阳线判定。
    async fn on_trade(
        &mut self,
        tr: Trade,
        is_buy: bool,
        tx: String,
        tx_from: Option<String>,
        block: u64,
        wallet_is_actor: bool,
    ) {
        let token = tr.token.to_lowercase();
        if self.blacklist.contains(&token)
            || self
                .tokens
                .get(&token)
                .is_some_and(|t| t.stage.is_terminal())
        {
            return;
        }
        // 首笔成交即可确认税率已知：成交必然发生在建币交易的后续交易，而日志
        // 按链上顺序处理，故此刻建币交易内的税率事件已处理完毕。标准币不发税率
        // 事件，在此按 0 税定级。若这里判定超标，说明它是高税币，直接剔除、
        // 不再为它聚合 K 线。
        if self.finalize_classification(&token).await {
            return;
        }
        // 只关心已在观察窗内的 A/B 币。不在 map 中的地址（高税币、已剔除币、
        // 历史币）一律忽略，避免为它们维护无用的 K 线内存。
        // 枚举只有 Watching 与 Live 两态，故此处无需再判断阶段。
        if !self.tokens.contains_key(&token) {
            return;
        }

        // 目标地址名单是交易发起人（GMGN 买家）口径，不是 Flap 事件里的
        // 收币/执行 wallet。只为观察窗内、尚未命中目标地址的 BUY 异步补查
        // tx.from；绝不在事件循环内 await RPC，避免阻塞扫链。
        let in_target_window = is_buy
            && self.tokens.get(&token).is_some_and(|st| {
                accepts_a_window(st) && !st.target_buyer_seen && tr.ts < st.observe_until_ts
            });
        if in_target_window {
            if wallet_is_actor && crate::targets::is_target_buyer(&tr.wallet) {
                if let Some(st) = self.tokens.get_mut(&token) {
                    st.target_buyer_seen = true;
                }
            } else if tx_from
                .as_deref()
                .is_some_and(crate::targets::is_target_buyer)
            {
                if let Some(st) = self.tokens.get_mut(&token) {
                    st.target_buyer_seen = true;
                }
            } else if !wallet_is_actor && tx_from.is_none() {
                if let Some(sender) = self.sender_tx_cache.get(&tx.to_lowercase()) {
                    if crate::targets::is_target_buyer(sender) {
                        if let Some(st) = self.tokens.get_mut(&token) {
                            st.target_buyer_seen = true;
                        }
                    }
                } else {
                    self.spawn_sender_lookup(tx.clone(), token.clone(), tr.ts);
                }
            }
        }
        if in_target_window
            && self
                .tokens
                .get(&token)
                .is_some_and(|st| st.target_buyer_seen)
        {
            if let Some(st) = self.tokens.get_mut(&token) {
                st.b_timing.buyer_at.get_or_insert(tr.ts);
            }
            self.cancel_sender_lookups_for_token(&token);
            // Keep the first buyer evidence across restart even if no green streak yet.
            if self
                .tokens
                .get(&token)
                .is_some_and(|st| !st.bull_streak_seen)
            {
                self.persist(&token);
            }
        }

        // 单价与市值都必须知道方向：BUY 的有效成交额是 Q - Fee，SELL 是 Q + Fee。
        let price = abi::unit_price(&tr, is_buy);
        if !(price > 0.0) || !price.is_finite() {
            return;
        }
        let supply = self
            .tokens
            .get(&token)
            .map(|st| st.total_supply_tokens)
            .unwrap_or(config::TOTAL_SUPPLY_TOKENS);
        let mcap_quote = price * supply;
        let quote_value = tr.quote_amount as f64 / 1e18;
        self.latest_chain_ts = self.latest_chain_ts.max(tr.ts);

        // 「上一笔成交结束后的市场状态」，用作新 K 线的 Open。
        // 必须在下面覆盖 last_price 之前捕获，否则拿到的就是本笔价格，
        // Open 会退化成「桶内第一笔成交价」，K 线之间出现跳空。
        // last_price > 0 等价于「此前有过成交」（每笔 price 都经过 >0 校验），
        // 故可用它区分首根 K 与后续 K。
        let prior_price = self
            .tokens
            .get(&token)
            .map(|st| st.last_price)
            .unwrap_or(0.0);

        let rec = {
            let Some(st) = self.tokens.get_mut(&token) else {
                return;
            };
            st.last_price = price;
            st.last_mcap_quote = mcap_quote;
            let usd = if st.quote_token == config::ZERO_ADDRESS {
                self.bnb_usd
            } else {
                self.quote_usd.get(&st.quote_token).copied()
            };
            st.last_mcap_usd = usd.map(|p| mcap_quote * p);
            if let Some(mcap) = st.last_mcap_usd {
                record_b_market_cap(st, now_unix(), mcap);
            }
            st.last_activity = Instant::now();
            st.trade_seq += 1;
            let rec = TradeRecord {
                ts: tr.ts,
                seq: st.trade_seq,
                wallet: tr.wallet.clone(),
                side: if is_buy { "buy" } else { "sell" },
                token_amount: fmt_amount(tr.token_amount, 18),
                quote_amount: fmt_amount(tr.quote_amount, 18),
                quote_value,
                price,
                mcap_quote,
                tx,
                block,
            };
            if st.trades.len() >= config::MAX_TRADES_PER_TOKEN {
                let drop = config::MAX_TRADES_PER_TOKEN / 4;
                st.trades.drain(0..drop);
            }
            st.trades.push(rec.clone());
            rec
        };
        if let Some(quote) = self.tokens.get(&token).map(|s| s.quote_token.clone()) {
            self.ensure_quote_usd(&quote);
        }
        self.persist_trade_throttled(&token);

        // 封口所有时间上已经完结的桶（可能触发晋升）。
        self.close_finished_buckets(&token, tr.ts).await;

        // 更新当前桶。已封口的桶是最终值，不再回改。
        let bucket = tr.ts / config::CANDLE_PERIOD_SECS;
        if let Some(st) = self.tokens.get_mut(&token) {
            let already_closed = st.candles.get(&bucket).map(|c| c.closed).unwrap_or(false);
            if !already_closed {
                // Open 口径（与常见交易所不同，刻意让 K 线视觉连续）：
                // - 首根 K：无先前成交，Open = 第一笔成交价；
                // - 后续 K：Open = 上一笔成交结束后的市场状态，而非本桶第一笔成交价。
                //   中间空桶不生成 K，但 last_price 保留，故继承依然成立。
                let open = if prior_price > 0.0 {
                    prior_price
                } else {
                    price
                };
                let e = st.candles.entry(bucket).or_insert_with(|| Candle {
                    time: bucket * config::CANDLE_PERIOD_SECS,
                    open,
                    // High/Low 必须把 Open 也算进去：桶内若只涨不破开盘，
                    // 低点仍应是 Open（例：O=5.0K，桶内 5.2/5.8/5.4 → L=5.0K）。
                    high: open.max(price),
                    low: open.min(price),
                    close: price,
                    volume: 0.0,
                    trades: 0,
                    closed: false,
                });
                e.high = e.high.max(price);
                e.low = e.low.min(price);
                e.close = price;
                e.volume += quote_value;
                e.trades += 1;
                let candle = *e;
                let _ = self.event_tx.send(Event::Candle {
                    token: token.clone(),
                    candle,
                });
            }
            // K 线数量上限，丢弃最旧的。
            while st.candles.len() > config::MAX_CANDLES_PER_TOKEN {
                if let Some((&oldest, _)) = st.candles.iter().next() {
                    st.candles.remove(&oldest);
                } else {
                    break;
                }
            }
        }

        let _ = self.event_tx.send(Event::Trade {
            token: token.clone(),
            trade: rec,
        });

        // 如果三连阳先形成，目标地址买入会在这里补齐第二个条件；如果目标地址
        // 先买入，则通常由 on_candle_closed 在第三根 K 线封口时完成晋升。
        let promote = self.tokens.get(&token).is_some_and(|st| {
            st.stage == Stage::Watching && st.target_buyer_seen && st.bull_streak_seen
        });
        if promote {
            self.promote(&token).await;
        }
    }

    /// 封口所有 `桶结束时间 <= ts` 的未封口 K 线，并按时间顺序逐根判定。
    ///
    /// 顺序很关键：**先在 map 里置 closed，再取副本**。若先取副本，返回的 K 线
    /// 会带着过期的 `closed = false` 经 SSE 发给前端，而快照读 map 却是 true，
    /// 同一根 K 线在两条通道上互相矛盾。
    fn close_finished_buckets_blocking(&mut self, token: &str, ts: u64) -> Vec<(u64, Candle)> {
        let Some(st) = self.tokens.get(token) else {
            return Vec::new();
        };
        let pending: Vec<u64> = st
            .candles
            .range(..)
            .filter(|(b, c)| !c.closed && bucket_is_finished(**b, ts))
            .map(|(&b, _)| b)
            .collect();
        let Some(st) = self.tokens.get_mut(token) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for b in pending {
            if let Some(c) = st.candles.get_mut(&b) {
                c.closed = true;
                // 封口后的终值，与前端快照所见完全一致。
                out.push((b, *c));
            }
        }
        out
    }

    async fn close_finished_buckets(&mut self, token: &str, ts: u64) {
        let closed = self.close_finished_buckets_blocking(token, ts);
        for (b, c) in closed {
            let _ = self.event_tx.send(Event::Candle {
                token: token.to_string(),
                candle: c,
            });
            self.on_candle_closed(token, b, &c).await;
        }
    }

    /// 一根 K 线封口后更新连续阳线计数；目标地址条件已满足时，达标即晋升 B 币。
    async fn on_candle_closed(&mut self, token: &str, bucket: u64, c: &Candle) {
        // Empty intervals are not candles. Never count an open candle or an
        // empty placeholder, nor let either one interrupt an existing streak.
        if !c.closed || c.trades == 0 {
            return;
        }
        let is_c = self
            .tokens
            .get(token)
            .is_some_and(|s| s.stage == Stage::CWatching);
        if is_c {
            let reenter = {
                let Some(st) = self.tokens.get_mut(token) else {
                    return;
                };
                // C 只接受进入时刻之后开始、且已封口的新 K 线。
                let allowed = st.c_first_bucket.is_some_and(|first| bucket >= first)
                    && st.c_entered_at.is_some_and(|entered| {
                        c.time + config::CANDLE_PERIOD_SECS
                            <= entered + config::C_WATCH_WINDOW.as_secs()
                    });
                if !allowed {
                    return;
                }
                if !(c.close > c.open) {
                    st.c_streak = 0;
                    st.c_streak_end = None;
                } else {
                    // No-trade gaps are skipped, exactly as in the A phase.
                    st.c_streak += 1;
                    st.c_streak_end = Some(bucket);
                }
                st.c_streak >= config::BULL_STREAK
            };
            if reenter {
                let qualified_ms = now_unix_ms();
                let now = qualified_ms / 1000;
                if let Some(st) = self.tokens.get_mut(token) {
                    st.b_timing.second_chain_at_ms =
                        Some((bucket + 1) * config::CANDLE_PERIOD_SECS * 1000);
                    st.stage = Stage::BReentryPending;
                    st.reentry_b_at = Some(now);
                    st.promoted_at = Some(now);
                    st.last_qualified_at_ms = Some(qualified_ms);
                    st.last_open_sent_at_ms = None;
                    st.last_open_acked_at_ms = None;
                    st.current_cycle_start = Some(now);
                    st.cycle_max_market_cap = st.last_mcap_usd.unwrap_or(0.0);
                    st.current_slot = None;
                }
                // 同 promote：调度先行，避免一次同步写盘挡在 OPEN 之前。
                self.schedule(false).await;
                self.persist(token);
                self.sync_shared().await;
            }
            return;
        }
        let (promote, new_evidence) = {
            let Some(st) = self.tokens.get_mut(token) else {
                return;
            };
            // 阳线定义：收盘价严格高于开盘价。十字星（相等）不算阳线。
            if !(c.close > c.open) {
                st.streak = 0;
                st.streak_end = None;
            } else {
                // 只看实际 K 线的连续颜色，无成交空档不重置计数。
                st.streak += 1;
                st.streak_end = Some(bucket);
                st.peak_streak = st.peak_streak.max(st.streak);
            }
            // 必须在观察窗截止前完整收线；空档跳过不延长 60 秒观察窗。
            // 防止用窗口之后的行情把已过期的币重新拉起。三连阳条件一旦在窗口内
            // 成立就锁存，方便目标地址稍后买入时按条件的先后无关规则晋升。
            let in_window = c.time + config::CANDLE_PERIOD_SECS <= st.observe_until_ts;
            let new_evidence =
                in_window && st.streak >= config::BULL_STREAK && !st.bull_streak_seen;
            if new_evidence {
                st.bull_streak_seen = true;
                st.b_timing
                    .green_at
                    .get_or_insert((bucket + 1) * config::CANDLE_PERIOD_SECS);
                st.bull_streak_value = st.streak;
            }
            let promote =
                st.stage == Stage::Watching && st.target_buyer_seen && st.bull_streak_seen;
            (promote, new_evidence)
        };
        if promote {
            self.promote(token).await;
        } else if new_evidence {
            self.persist(token);
        }
    }

    /// A 币 -> B 币：进入前端展示。
    async fn promote(&mut self, token: &str) {
        let snap = {
            let Some(st) = self.tokens.get_mut(token) else {
                return;
            };
            if st.stage != Stage::Watching || st.first_b_at.is_some() {
                return;
            }
            let qualified_ms = now_unix_ms();
            let now = qualified_ms / 1000;
            st.b_timing.first_chain_at_ms = st
                .b_timing
                .buyer_at
                .zip(st.b_timing.green_at)
                .map(|(buyer, green)| buyer.max(green) * 1000);
            st.stage = Stage::BPending;
            st.promoted_at = Some(now);
            st.first_b_at = Some(now);
            st.first_qualified_at_ms = Some(qualified_ms);
            st.last_qualified_at_ms = Some(qualified_ms);
            st.current_cycle_start = Some(now);
            st.cycle_max_market_cap = st.last_mcap_usd.unwrap_or(0.0);
            st.promote_streak = st.bull_streak_value.max(config::BULL_STREAK);
            self.stats_b += 1;
            eprintln!(
                "[B币] {} ({}) 目标地址已买入，连续 {} 根阳线，进入前端",
                short(&st.symbol),
                tail8(token),
                st.promote_streak
            );
            snapshot_of(st)
        };
        let _ = self.event_tx.send(Event::TokenUp(snap));

        // 先调度再落盘：schedule 会把 OPEN 发给浏览器，把它排在这次同步
        // SQLite 写之后，等于白白让资格确认到打开窗口之间多等一次磁盘 IO。
        self.schedule_for(false, Some(token)).await;
        self.persist(token);
        self.sync_shared().await;
    }

    /// 永久剔除并从前端移除。
    async fn purge(&mut self, token: &str, reason: &str) {
        if let Some(trading) = self.shared.trading.get() {
            trading.cancel_token(token, reason);
        }
        self.cancel_sender_lookups_for_token(token);
        self.tokens.active.remove(token);
        self.durable_cycle_peaks.remove(token);
        if let Some(st) = self.tokens.get_mut(token) {
            st.candles.clear();
            st.trades.clear();
            st.stage = Stage::Dead;
            st.current_slot = None;
            self.blacklist.insert(token.to_string());
            let _ = self.event_tx.send(Event::TokenDown {
                token: token.to_string(),
                reason: reason.to_string(),
            });
        }
        self.release_token_from_slot(token);
        self.persist(token);
        self.sync_shared().await;
    }

    /// 定时清理：封口滞留 K 线、到期观察窗、推送观察进度。
    async fn on_tick(&mut self) {
        if self.shared.system.is_stopping() {
            return;
        }
        if self.reset_pending != 0
            && self
                .reset_sent
                .is_none_or(|at| at.elapsed() > Duration::from_secs(3))
        {
            self.send_reset();
        }
        // 1) 封口所有已随链上时间完结的桶（无后续成交时也要收口）。
        if self.latest_chain_ts > 0 {
            let open: Vec<String> = self
                .tokens
                .live_iter()
                .filter(|(_, st)| {
                    // c.time 恒为 bucket*PERIOD，故可由其还原桶号后复用同一判定。
                    st.candles.values().any(|c| {
                        !c.closed
                            && bucket_is_finished(
                                c.time / config::CANDLE_PERIOD_SECS,
                                self.latest_chain_ts,
                            )
                    })
                })
                .map(|(k, _)| k.clone())
                .collect();
            for tok in open {
                self.close_finished_buckets(&tok, self.latest_chain_ts)
                    .await;
            }
        }

        // 2) 后台补齐 metadata。只派发任务，不在 tick 内等待 RPC；失败按退避
        //    重试。Name/Symbol/总供应量/报价资产不得阻塞成交日志热路径。
        let metadata_pending: Vec<String> = self
            .tokens
            .live_iter()
            .filter(|(_, st)| !st.stage.is_terminal())
            .filter(|(token, st)| match st.source_platform {
                LaunchPlatform::GeniusFun => st.name.is_empty() || st.symbol.is_empty(),
                LaunchPlatform::FourMeme => {
                    !self.pending_quote.contains_key(*token)
                        && st.quote_token == config::ZERO_ADDRESS
                }
                _ => false,
            })
            .map(|(token, _)| token.clone())
            .collect();
        for token in metadata_pending {
            self.ensure_token_metadata(&token);
        }

        // 3) 兜底定级：从未成交的标准币不发税率事件，若不在这里定级，
        //    它会被直接计入「到期剔除」而从未计入 A 币，统计就漏了。
        //    tick 在主循环中的优先级高于链上日志，即使日志持续涌入也不会被饿死。
        //    标准币在首笔成交时仍会优先定级；这里是无成交情况的兜底。
        let tax_pending: Vec<String> = self
            .tokens
            .live_iter()
            .filter(|(token, st)| {
                !st.counted
                    && requires_rpc_tax(&st.source_platform)
                    && !self.pending_tax.contains_key(*token)
            })
            .map(|(token, _)| token.clone())
            .collect();
        for token in tax_pending {
            self.ensure_tax_lookup(&token);
        }

        let uncounted: Vec<String> = self
            .tokens
            .live_iter()
            .filter(|(_, st)| !st.counted)
            .map(|(k, _)| k.clone())
            .collect();
        for tok in uncounted {
            self.finalize_classification(&tok).await;
        }

        // 3.5) 重试仍未解析出符号的报价代币。回填期间单条 WSS 连接被海量事件
        //      占满，symbol() 查询会超时；失败不入缓存，故这里在连接空闲后重试，
        //      前端「计价=未知」能自愈。只重试仍在前端展示的币，避免无谓查询。
        let need_lookup: Vec<String> = self
            .tokens
            .live_values()
            .filter(|st| st.stage.is_b() && st.quote_symbol.is_none())
            .map(|st| st.quote_token.clone())
            .filter(|q| q != config::ZERO_ADDRESS)
            .collect();
        let mut seen = HashSet::new();
        for q in need_lookup {
            if seen.insert(q.clone()) {
                // 走 quote_symbol_now 而非 ensure_quote_symbol：前者在缓存命中时
                // 会把符号补填给仍缺失的存量代币，后者命中即返回、不会补填。
                self.quote_symbol_now(&q);
            }
        }

        // 3) 观察窗到期处理。
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut watching = Vec::new();
        for (tok, st) in self.tokens.live_iter() {
            if st.stage != Stage::Watching {
                continue;
            }
            // 只按墙上 deadline 到期。恢复/回填中的旧币会获得短暂处理宽限，
            // 条件本身仍严格按 created_at..created_at+60 的链上时间过滤。
            if now >= st.deadline_wall && st.pending_sender_lookups == 0 {
                expired.push((
                    tok.clone(),
                    st.peak_streak,
                    st.candles.len(),
                    st.target_buyer_seen,
                    st.bull_streak_seen,
                ));
            } else {
                let left = st.deadline_wall.saturating_duration_since(now).as_secs();
                watching.push((tok.clone(), st.name.clone(), st.streak, left));
            }
        }
        for (tok, peak, nc, target_buyer_seen, bull_streak_seen) in expired {
            self.stats_expired += 1;
            // 同时打印峰值连阳与目标地址命中状态，区分「连阳不足」和「连阳达标
            // 但没有目标地址买入」这两种合法的到期原因。
            eprintln!(
                "[剔除] {} 60 秒内未达标（峰值连阳 {peak}，三连阳达标 {bull_streak_seen}，目标地址买入 {target_buyer_seen}，K线 {nc} 根），永不再理",
                tail8(&tok),
            );
            self.purge(&tok, "60 秒内未满足目标地址买入 + 连续三根阳线")
                .await;
        }
        for (tok, name, streak, left) in watching {
            let _ = self.event_tx.send(Event::Watching {
                token: tok,
                name,
                streak,
                seconds_left: left,
            });
        }

        // 4) C 的 240 秒窗口和 B 的 600 秒市值周期都使用 Unix 时间，重启后可续算。
        let unix = now_unix();
        let mut c_expired = Vec::new();
        let mut b_expired = Vec::new();
        let mut advanced = Vec::new();
        for (ca, st) in self.tokens.live_iter() {
            if st.stage == Stage::CWatching
                && st
                    .c_entered_at
                    .is_some_and(|at| unix >= at + config::C_WATCH_WINDOW.as_secs())
            {
                c_expired.push(ca.clone());
            }
        }
        for ca in self.tokens.live_keys() {
            let st = self.tokens.get_mut(&ca).expect("active token");
            match settle_b_cycle(st, unix) {
                BCycleOutcome::Expired => b_expired.push(ca),
                BCycleOutcome::Advanced => advanced.push(ca),
                BCycleOutcome::Unchanged => {}
            }
        }
        for ca in c_expired {
            self.purge(&ca, "C 币 240 秒内未重新出现三连阳").await;
        }
        for ca in b_expired {
            self.purge(
                &ca,
                "B 币 600 秒周期没有达到 $10,000 的有效记录（未知市值不豁免）",
            )
            .await;
        }

        for ca in advanced {
            self.persist(&ca);
        }

        // OPEN 未获 ACK：回滚成候补，避免后端与页面永久分叉。
        let timed_out: Vec<usize> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                s.state == SlotState::Opening
                    && s.command_sent_at
                        .is_some_and(|at| at.elapsed() >= config::OPEN_ACK_TIMEOUT)
            })
            .map(|(i, _)| i)
            .collect();
        for i in timed_out {
            if let Some(ca) = self.slots[i].token_ca.take() {
                if let Some(trading) = self.shared.trading.get() {
                    trading.cancel_token(&ca, "浏览器 OPEN 超时，取消未广播买入");
                }
                if let Some(st) = self.tokens.get_mut(&ca) {
                    st.current_slot = None;
                }
                self.persist(&ca);
            }
            self.slots[i].state = SlotState::Empty;
            self.slots[i].command_id = None;
            self.slots[i].command_sent_at = None;
        }
        self.schedule(false).await;
    }

    // ------------------------------------------------------------ 快照

    // ------------------------------------------------------------ 报价代币符号

    /// 确保某报价代币的符号已被解析（非阻塞）。
    ///
    /// 命中缓存直接返回；未命中则派发后台任务解析，结果经指令通道回填。
    /// 原生 BNB（零地址）无需查询，直接就是 "BNB"。
    fn ensure_quote_symbol(&mut self, quote: &str) {
        if quote == config::ZERO_ADDRESS {
            self.quote_symbols
                .insert(quote.to_string(), Some("BNB".to_string()));
            return;
        }
        // 已成功解析则不再查询。
        if matches!(self.quote_symbols.get(quote), Some(Some(_))) {
            return;
        }
        // 失败不写缓存，因此用「在途集合」去重，避免每次 tick 重复派发同一查询。
        if self.quote_lookups_inflight.contains(quote) {
            return;
        }
        self.spawn_quote_lookup(quote.to_string());
    }

    /// 取某报价代币的当前符号，顺手把缓存值补填给仍缺失的代币。
    ///
    /// 补填是必要的：回填期间解析超时的币晋升为 B 币后，若缓存已在更早的币上
    /// 解析成功，旧实现只把符号写给「当时还活着的币」，后晋升的币就永远显示
    /// 「未知」——截图实测发现过。快照每秒由 sync_shared 重建，补填后自然可见。
    fn quote_symbol_now(&mut self, quote: &str) -> Option<String> {
        if quote == config::ZERO_ADDRESS {
            return Some("BNB".to_string());
        }
        if let Some(Some(sym)) = self.quote_symbols.get(quote).cloned() {
            for ca in self.tokens.live_keys() {
                let st = self
                    .tokens
                    .get_mut(&ca)
                    .expect("active index refers to TokenState");
                if st.quote_token == quote && st.quote_symbol.is_none() {
                    st.quote_symbol = Some(sym.clone());
                }
            }
            return Some(sym);
        }
        self.ensure_quote_symbol(quote);
        None
    }
}

// ---------------------------------------------------------------- 工具函数

/// 从代币状态构造前端快照。
///
/// 做成自由函数而非 `&self` 方法：`promote` 需要在持有 `get_mut` 可变借用时
/// 同时读取状态，方法形式会触发借用冲突。
#[derive(Debug, PartialEq, Eq)]
enum BCycleOutcome {
    Unchanged,
    Advanced,
    Expired,
}

fn settle_b_cycle(st: &mut TokenState, now: u64) -> BCycleOutcome {
    if !st.stage.is_b() {
        return BCycleOutcome::Unchanged;
    }
    let Some(start) = st.current_cycle_start else {
        return BCycleOutcome::Unchanged;
    };
    let end = start.saturating_add(config::B_CYCLE.as_secs());
    if now < end {
        return BCycleOutcome::Unchanged;
    }
    if !st.cycle_max_market_cap.is_finite() || st.cycle_max_market_cap < config::B_CYCLE_MIN_USD {
        return BCycleOutcome::Expired;
    }
    // Exactly one completed cycle can pass on its own peak. An old price must
    // not prove every missed 600s interval after a long shutdown.
    st.current_cycle_start = Some(end);
    st.cycle_max_market_cap = 0.0;
    if now >= end.saturating_add(config::B_CYCLE.as_secs()) {
        BCycleOutcome::Expired
    } else {
        BCycleOutcome::Advanced
    }
}

fn record_b_market_cap(st: &mut TokenState, now: u64, cap: f64) {
    if st.stage.is_b()
        && settle_b_cycle(st, now) != BCycleOutcome::Expired
        && cap.is_finite()
        && cap >= 0.0
    {
        st.cycle_max_market_cap = st.cycle_max_market_cap.max(cap);
    }
}

fn snapshot_of(st: &TokenState) -> TokenSnapshot {
    let mut candles: Vec<Candle> = st.candles.values().copied().collect();
    // 只回传最近窗口，避免长时间运行后快照过大。
    if candles.len() > 600 {
        candles = candles.split_off(candles.len() - 600);
    }
    let trades = if st.trades.len() > 1000 {
        st.trades[st.trades.len() - 1000..].to_vec()
    } else {
        st.trades.clone()
    };
    TokenSnapshot {
        token: st.token.clone(),
        name: st.name.clone(),
        symbol: st.symbol.clone(),
        platform: st.source_platform.to_string(),
        creator: st.creator.clone(),
        buy_tax_bps: st.buy_tax_bps,
        sell_tax_bps: st.sell_tax_bps,
        // 符号未解析出来时不谎报 BNB，标记为未知。
        quote_symbol: st
            .quote_symbol
            .clone()
            .unwrap_or_else(|| "未知".to_string()),
        // 前端据此判断能否折算美元：非原生 BNB 计价时没有可靠报价源。
        quote_is_native: st.quote_token == config::ZERO_ADDRESS,
        quote_address: st.quote_token.clone(),
        created_ts: st.created_ts,
        promoted_at: st.promoted_at,
        state: st.stage,
        market_cap_usd: st.last_mcap_usd,
        first_b_at: st.first_b_at,
        first_qualified_at_ms: st.first_qualified_at_ms,
        first_open_sent_at_ms: st.first_open_sent_at_ms,
        first_open_acked_at_ms: st.first_open_acked_at_ms,
        last_qualified_at_ms: st.last_qualified_at_ms,
        last_open_sent_at_ms: st.last_open_sent_at_ms,
        last_open_acked_at_ms: st.last_open_acked_at_ms,
        c_entered_at: st.c_entered_at,
        reentry_used: st.reentry_used,
        current_cycle_start: st.current_cycle_start,
        cycle_max_market_cap: st.cycle_max_market_cap,
        current_slot: st.current_slot.clone(),
        last_price: st.last_price,
        mcap_quote: st.last_mcap_quote,
        total_supply: st.total_supply_tokens,
        streak: st.streak,
        promote_streak: st.promote_streak,
        trade_count: st.trades.len(),
        candles,
        trades,
    }
}

fn slot_snapshot(slot: &Slot) -> SlotSnapshot {
    SlotSnapshot {
        slot_id: slot.slot_id.clone(),
        token_ca: slot.token_ca.clone(),
        browser_window_id: slot.browser_window_id,
        browser_tab_id: slot.browser_tab_id,
        state: slot.state,
        command_id: slot.command_id.clone(),
    }
}

fn persisted_of(st: &TokenState) -> PersistedToken {
    PersistedToken {
        b_timing: st.b_timing.clone(),
        ca: st.token.clone(),
        name: st.name.clone(),
        symbol: st.symbol.clone(),
        creator: st.creator.clone(),
        source_platform: st.source_platform.db_name(),
        creation_block: st.creation_block,
        creation_tx_hash: st.creation_tx_hash.clone(),
        source_ref: st.source_ref.clone(),
        total_supply_tokens: st.total_supply_tokens,
        created_at: st.created_ts,
        current_state: st.stage.db_name().to_string(),
        buy_tax_bps: st.buy_tax_bps,
        sell_tax_bps: st.sell_tax_bps,
        smart_money_hit: st.target_buyer_seen,
        bull_streak_seen: st.bull_streak_seen,
        first_b_at: st.first_b_at,
        first_qualified_at_ms: st.first_qualified_at_ms,
        first_open_sent_at_ms: st.first_open_sent_at_ms,
        first_open_acked_at_ms: st.first_open_acked_at_ms,
        last_qualified_at_ms: st.last_qualified_at_ms,
        last_open_sent_at_ms: st.last_open_sent_at_ms,
        last_open_acked_at_ms: st.last_open_acked_at_ms,
        c_entered_at: st.c_entered_at,
        reentry_used: st.reentry_used,
        reentry_b_at: st.reentry_b_at,
        current_cycle_start: st.current_cycle_start,
        cycle_max_market_cap: st.cycle_max_market_cap,
        current_slot: st.current_slot.clone(),
        user_discarded: st.user_discarded,
        quote_token: st.quote_token.clone(),
        quote_symbol: st.quote_symbol.clone(),
        last_price: st.last_price,
        last_mcap_quote: st.last_mcap_quote,
        last_mcap_usd: st.last_mcap_usd,
        streak: st.streak as u64,
        streak_end: st.streak_end,
        c_streak: st.c_streak as u64,
        c_streak_end: st.c_streak_end,
    }
}

fn token_from_persisted(row: PersistedToken, now: Instant) -> Option<TokenState> {
    let stage = Stage::from_db(&row.current_state)?;
    let elapsed = now_unix().saturating_sub(row.created_at);
    let remaining = config::WATCH_WINDOW.as_secs().saturating_sub(elapsed);
    // 重启时已过期的 A/NEW 仍给恢复补扫一个完整观察窗的处理时间；条件本身
    // 继续按 created_at..created_at+60 的链上时间过滤，不会把窗口外成交算进去。
    let restart_grace = if matches!(stage, Stage::New | Stage::AWatching) && remaining == 0 {
        config::WATCH_WINDOW.as_secs()
    } else {
        remaining
    };
    Some(TokenState {
        b_timing: row.b_timing,
        token: row.ca,
        name: row.name,
        symbol: row.symbol,
        creator: row.creator,
        source_platform: LaunchPlatform::from_db(&row.source_platform),
        creation_block: row.creation_block,
        creation_tx_hash: row.creation_tx_hash,
        source_ref: row.source_ref,
        total_supply_tokens: if row.total_supply_tokens > 0.0 {
            row.total_supply_tokens
        } else {
            config::TOTAL_SUPPLY_TOKENS
        },
        created_ts: row.created_at,
        observe_until_ts: row.created_at + config::WATCH_WINDOW.as_secs(),
        deadline_wall: now + Duration::from_secs(restart_grace),
        buy_tax_bps: row.buy_tax_bps,
        sell_tax_bps: row.sell_tax_bps,
        counted: stage != Stage::New,
        target_buyer_seen: row.smart_money_hit,
        pending_sender_lookups: 0,
        bull_streak_seen: row.bull_streak_seen,
        bull_streak_value: if row.bull_streak_seen {
            config::BULL_STREAK
        } else {
            0
        },
        quote_token: row.quote_token,
        quote_symbol: row.quote_symbol,
        stage,
        promoted_at: row.reentry_b_at.or(row.first_b_at),
        first_b_at: row.first_b_at,
        first_qualified_at_ms: row.first_qualified_at_ms,
        first_open_sent_at_ms: row.first_open_sent_at_ms,
        first_open_acked_at_ms: row.first_open_acked_at_ms,
        last_qualified_at_ms: row.last_qualified_at_ms,
        last_open_sent_at_ms: row.last_open_sent_at_ms,
        last_open_acked_at_ms: row.last_open_acked_at_ms,
        c_entered_at: row.c_entered_at,
        reentry_used: row.reentry_used,
        reentry_b_at: row.reentry_b_at,
        current_cycle_start: row.current_cycle_start,
        cycle_max_market_cap: row.cycle_max_market_cap,
        current_slot: row.current_slot,
        user_discarded: row.user_discarded,
        last_mcap_usd: row.last_mcap_usd,
        c_streak: row.c_streak as usize,
        c_streak_end: row.c_streak_end,
        c_first_bucket: row
            .c_entered_at
            .map(|v| v.div_ceil(config::CANDLE_PERIOD_SECS)),
        promote_streak: if row.first_b_at.is_some() {
            config::BULL_STREAK
        } else {
            0
        },
        candles: BTreeMap::new(),
        streak: row.streak as usize,
        streak_end: row.streak_end,
        peak_streak: 0,
        trades: Vec::new(),
        trade_seq: 0,
        last_price: row.last_price,
        last_mcap_quote: row.last_mcap_quote,
        last_activity: now,
    })
}

fn is_gmgn_home(raw: &str) -> bool {
    url::Url::parse(raw).ok().is_some_and(|u| {
        u.host_str()
            .is_some_and(|h| h.eq_ignore_ascii_case("gmgn.ai"))
            && u.path() == "/"
            && u.query_pairs()
                .any(|(k, v)| k == "chain" && v.eq_ignore_ascii_case("bsc"))
    })
}

fn gmgn_token_from_url(raw: &str) -> Option<String> {
    let u = url::Url::parse(raw).ok()?;
    if !u.host_str()?.eq_ignore_ascii_case("gmgn.ai") {
        return None;
    }
    let mut parts: Vec<_> = u.path_segments()?.collect();
    if parts.len() == 4 && parts[3].is_empty() {
        parts.pop();
    }
    if parts.len() != 3 || parts[0] != "bsc" || parts[1] != "token" {
        return None;
    }
    let ca = parts[2].to_lowercase();
    (ca.len() == 42 && ca.starts_with("0x") && ca[2..].chars().all(|c| c.is_ascii_hexdigit()))
        .then_some(ca)
}

async fn fetch_quote_usd(quote: &str) -> Option<f64> {
    let client = reqwest::Client::new();
    let gecko =
        format!("https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/{quote}");
    if let Ok(response) = client
        .get(gecko)
        .timeout(Duration::from_secs(8))
        .send()
        .await
    {
        if let Ok(value) = response.json::<serde_json::Value>().await {
            let price = value
                .get("data")
                .and_then(|v| v.get("attributes"))
                .and_then(|v| v.get("token_prices"))
                .and_then(|v| v.as_object())
                .and_then(|prices| {
                    prices
                        .values()
                        .find_map(|v| v.as_str()?.parse::<f64>().ok())
                })
                .filter(|p| *p > 0.0);
            if price.is_some() {
                return price;
            }
        }
    }

    let dex = format!("https://api.dexscreener.com/latest/dex/tokens/{quote}");
    let value: serde_json::Value = client
        .get(dex)
        .timeout(Duration::from_secs(8))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    let mut values = Vec::new();
    for pair in value.get("pairs")?.as_array()? {
        if pair.get("chainId").and_then(|v| v.as_str()) != Some("bsc") {
            continue;
        }
        let base = pair
            .pointer("/baseToken/address")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let quoted = pair
            .pointer("/quoteToken/address")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let base_usd = pair
            .get("priceUsd")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok());
        let base_per_quote = pair
            .get("priceNative")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<f64>().ok());
        if base.eq_ignore_ascii_case(quote) {
            if let Some(v) = base_usd.filter(|v| *v > 0.0) {
                values.push(v);
            }
        }
        if quoted.eq_ignore_ascii_case(quote) {
            if let (Some(usd), Some(rate)) = (base_usd, base_per_quote.filter(|v| *v > 0.0)) {
                values.push(usd / rate);
            }
        }
    }
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied()
}

/// 把 18 位精度的原始整数格式化为可读字符串，最多 8 位小数并去掉尾随零。
/// 把 18 位精度的原始整数格式化为可读字符串。
///
/// 顺序必须是「先截断到 8 位小数，再去尾随零」。反过来做（先去零再截断）
/// 会把 1e-18 这类非零微小值显示成 "0.00000000"，即把有成交显示成零，
/// 对量化判断是实质性误导。低于显示精度时用 "<0.00000001" 明确标注，
/// 而不是伪装成 0。
fn fmt_amount(raw: u128, decimals: u32) -> String {
    let scale = 10u128.pow(decimals);
    let int_part = raw / scale;
    let frac_part = raw % scale;
    if frac_part == 0 {
        return int_part.to_string();
    }
    // 左侧补零到 decimals 位，保证小数点对齐。
    let frac_str = format!("{:0>width$}", frac_part, width = decimals as usize);
    // 先截断到 8 位小数（Flap 单价在 1e-8 量级，更长无意义），
    let truncated = &frac_str[..frac_str.len().min(8)];
    // 再去掉尾随零。
    let keep = truncated.trim_end_matches('0');
    match keep.len() {
        // 非零但低于 8 位小数精度：不能显示成 0。
        0 if int_part == 0 => "<0.00000001".to_string(),
        0 => int_part.to_string(),
        _ => format!("{int_part}.{keep}"),
    }
}

/// 封口判定：桶 `bucket` 是否已在链上时间 `ts` 处完结。
///
/// 抽成独立纯函数，是为了让边界（恰好等于结束时刻 / 早一秒）可以被单元测试
/// 直接覆盖，而不是把表达式埋在遍历里无法验证。
fn bucket_is_finished(bucket: u64, ts: u64) -> bool {
    (bucket + 1) * config::CANDLE_PERIOD_SECS <= ts
}

/// 税率判定：买税与卖税都 <= 4%（400 bps）才算 A 币。
///
/// 需求原文是「买卖税都小于等于 4%」，因此是**与**关系，任一超标即剔除。
/// 抽成纯函数以便覆盖边界值（400 通过 / 401 剔除），避免判定逻辑散落两处。
fn requires_rpc_tax(platform: &LaunchPlatform) -> bool {
    matches!(
        platform,
        LaunchPlatform::FourMeme | LaunchPlatform::GeniusFun
    )
}

/// A_WATCHING 与“等待平台税率 RPC 的 NEW”都必须继续收集窗口内成交。
/// 后者只有在税率确认通过后才允许真正晋升。
fn accepts_a_window(st: &TokenState) -> bool {
    st.stage == Stage::AWatching
        || (st.stage == Stage::New && requires_rpc_tax(&st.source_platform))
}

fn tax_passes(buy_bps: u64, sell_bps: u64) -> bool {
    buy_bps <= config::MAX_TAX_BPS && sell_bps <= config::MAX_TAX_BPS
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

/// 日志里截短显示名称，避免刷屏。
fn short(s: &str) -> String {
    let t: String = s.chars().take(14).collect();
    if t.is_empty() {
        "(无名)".to_string()
    } else {
        t
    }
}

/// 取地址后 8 位用于日志。
fn tail8(addr: &str) -> &str {
    if addr.len() > 8 {
        &addr[addr.len() - 8..]
    } else {
        addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(bucket: u64, open: f64, close: f64) -> Candle {
        Candle {
            time: bucket * config::CANDLE_PERIOD_SECS,
            open,
            high: open.max(close),
            low: open.min(close),
            close,
            volume: 1.0,
            trades: 1,
            closed: true,
        }
    }

    fn state(created_ts: u64) -> TokenState {
        let now = Instant::now();
        TokenState {
            b_timing: BTiming::default(),
            token: "0xabc".into(),
            name: "T".into(),
            symbol: "T".into(),
            creator: "0x0".into(),
            source_platform: LaunchPlatform::Flap,
            creation_block: 1,
            creation_tx_hash: "0xtest".into(),
            source_ref: None,
            total_supply_tokens: config::TOTAL_SUPPLY_TOKENS,
            created_ts,
            observe_until_ts: created_ts + config::WATCH_WINDOW.as_secs(),
            deadline_wall: now + config::WATCH_WINDOW,
            buy_tax_bps: 100,
            sell_tax_bps: 100,
            // 测试用状态代表已定级的 A 币。
            counted: true,
            // 纯 K 线单测默认把目标地址条件视为已满足，专门测试连阳规则。
            target_buyer_seen: true,
            pending_sender_lookups: 0,
            bull_streak_seen: false,
            bull_streak_value: 0,
            quote_token: config::ZERO_ADDRESS.into(),
            quote_symbol: Some("BNB".into()),
            stage: Stage::Watching,
            promoted_at: None,
            first_b_at: None,
            first_qualified_at_ms: None,
            first_open_sent_at_ms: None,
            first_open_acked_at_ms: None,
            last_qualified_at_ms: None,
            last_open_sent_at_ms: None,
            last_open_acked_at_ms: None,
            c_entered_at: None,
            reentry_used: false,
            reentry_b_at: None,
            current_cycle_start: None,
            cycle_max_market_cap: 0.0,
            current_slot: None,
            user_discarded: false,
            last_mcap_usd: None,
            c_streak: 0,
            c_streak_end: None,
            c_first_bucket: None,
            promote_streak: 0,
            candles: BTreeMap::new(),
            streak: 0,
            streak_end: None,
            peak_streak: 0,
            trades: Vec::new(),
            trade_seq: 0,
            last_price: 0.0,
            last_mcap_quote: 0.0,
            last_activity: now,
        }
    }

    fn bare_engine() -> Engine {
        engine_with_store(Store::memory().expect("test sqlite"))
    }

    #[tokio::test]
    async fn recovery_checkpoint_waits_behind_unprocessed_timestamp_logs() {
        let mut engine = bare_engine();
        let mut log = crate::recovery::checkpoint(0, 10, 0);
        log.topic0 = "synthetic".into();
        engine.pending_decoded_logs.push_back(PendingDecodedLog {
            log,
            events: vec![],
            needs_block_timestamp: true,
        });
        engine.on_log(crate::recovery::checkpoint(0, 10, 0)).await;
        assert_eq!(engine.shared.last_scanned_block.load(Ordering::Relaxed), 0);
        assert_eq!(engine.pending_decoded_logs.len(), 2);
        engine.block_timestamp_cache.insert(10, now_unix());
        engine.drain_pending_decoded_logs().await;
        assert_eq!(engine.shared.last_scanned_block.load(Ordering::Relaxed), 10);
        assert_eq!(
            engine.store.get_meta_u64("last_scanned_block").unwrap(),
            Some(10)
        );
    }

    #[tokio::test]
    async fn recovery_progress_cannot_jump_a_gap_or_ack_a_lagged_batch() {
        let mut engine = bare_engine();
        engine
            .on_log(crate::recovery::checkpoint(100, 200, 0))
            .await;
        assert_eq!(engine.shared.last_scanned_block.load(Ordering::Relaxed), 0);
        engine.logs_lagged = 5;
        engine.on_log(crate::recovery::checkpoint(0, 99, 0)).await;
        assert_eq!(engine.shared.last_scanned_block.load(Ordering::Relaxed), 0);
        engine.on_log(crate::recovery::checkpoint(0, 99, 5)).await;
        engine
            .on_log(crate::recovery::checkpoint(100, 200, 5))
            .await;
        assert_eq!(
            engine.shared.last_scanned_block.load(Ordering::Relaxed),
            200
        );
    }

    #[tokio::test]
    async fn reset_advances_scan_barrier_and_late_old_markers_cannot_undo_it() {
        let mut engine = bare_engine();
        engine.reset_monitoring(500).await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            engine.shared.recovery_wake.notified(),
        )
        .await
        .expect("explicit reset must wake recovery without waiting for cooldown");
        engine.on_log(crate::recovery::checkpoint(0, 499, 0)).await;
        assert_eq!(
            engine.shared.last_scanned_block.load(Ordering::Relaxed),
            500
        );
        assert_eq!(
            engine.store.get_meta_u64("last_scanned_block").unwrap(),
            Some(500)
        );
    }

    #[test]
    fn restart_uses_complete_scan_cursor_not_highest_live_event() {
        let store = Store::memory().unwrap();
        store.set_meta_u64("last_processed_block", 999).unwrap();
        store.set_meta_u64("last_scanned_block", 100).unwrap();
        store.set_meta_u64("scan_cursor_version", 1).unwrap();
        let engine = engine_with_store(store);
        assert_eq!(
            engine.shared.last_processed_block.load(Ordering::Relaxed),
            999
        );
        assert_eq!(
            engine.shared.last_scanned_block.load(Ordering::Relaxed),
            100
        );
    }

    #[test]
    fn legacy_restart_cannot_use_high_live_block_to_skip_an_unscanned_gap() {
        let store = Store::memory().unwrap();
        store.set_meta_u64("last_processed_block", 999).unwrap();
        store.set_meta_u64("last_scanned_block", 997).unwrap();
        store.set_meta_u64("reset_cutoff_block", 100).unwrap();
        let first = engine_with_store(store.clone());
        assert_eq!(first.shared.last_scanned_block.load(Ordering::Relaxed), 100);
        assert_eq!(store.get_meta_u64("scan_cursor_version").unwrap(), Some(1));
        let second = engine_with_store(store);
        assert_eq!(
            second.shared.last_scanned_block.load(Ordering::Relaxed),
            100
        );
    }

    fn engine_with_store(store: Store) -> Engine {
        let (_logs_tx, logs_rx) = broadcast::channel(32);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, _) = broadcast::channel(32);
        let (browser_tx, _) = broadcast::channel(32);
        let (_status_tx, status_rx) = watch::channel("test".to_string());
        Engine::new(
            logs_rx,
            cmd_rx,
            cmd_tx,
            event_tx,
            browser_tx,
            status_rx,
            crate::rpc::offline_client(),
            Shared::default(),
            store,
        )
    }

    async fn engine_with_dry_trade() -> (Engine, crate::trading::Trading, mpsc::Receiver<String>) {
        use crate::trading::{
            rpc::HttpRpc,
            types::{Mode, Policy},
            Trading,
        };
        let store = Store::memory().unwrap();
        let engine = engine_with_store(store.clone());
        let policy = Policy {
            simulation_from: Some("0x1a642f0e3c3af545e7acbd38b07251b3990914f1".into()),
            slippage_bps: Some(500),
            session_budget_wei: "100000000000000000".into(),
            daily_budget_wei: "100000000000000000".into(),
            ..Policy::default()
        };
        // No actual RPC configured and no worker started: tests inspect only intent wiring.
        let (trading, queue) =
            Trading::new(store, policy, Arc::new(HttpRpc::new(None).unwrap())).unwrap();
        trading.set_mode(Mode::DryRun).await.unwrap();
        assert!(engine.shared.trading.set(trading.clone()).is_ok());
        (engine, trading, queue)
    }
    fn trade_candidate(ca: &str) -> TokenState {
        let now = now_unix();
        let mut st = state(now - 30);
        st.token = ca.into();
        st.bull_streak_seen = true;
        st.b_timing.buyer_at = Some(now);
        st.b_timing.green_at = Some(now);
        st
    }
    fn bind_empty_slot(engine: &mut Engine, i: usize) {
        engine.slots[i].state = SlotState::Empty;
        engine.slots[i].browser_window_id = Some(17 + i as i64);
        engine.slots[i].browser_tab_id = Some(31 + i as i64);
    }

    #[test]
    fn b_unknown_market_cap_expires_and_peak_does_not_roll_across_cycles() {
        let mut t = state(100);
        t.stage = Stage::BPending;
        t.current_cycle_start = Some(100);
        t.last_mcap_usd = None;
        t.cycle_max_market_cap = 0.0;
        assert_eq!(settle_b_cycle(&mut t, 699), BCycleOutcome::Unchanged);
        assert_eq!(settle_b_cycle(&mut t, 700), BCycleOutcome::Expired);
        record_b_market_cap(&mut t, 701, 50_000.0);
        assert_eq!(
            t.cycle_max_market_cap, 0.0,
            "late update cannot rescue an already failed cycle"
        );
        t.cycle_max_market_cap = 12_000.0;
        t.last_mcap_usd = Some(50_000.0);
        assert_eq!(settle_b_cycle(&mut t, 700), BCycleOutcome::Advanced);
        assert_eq!(t.current_cycle_start, Some(700));
        assert_eq!(t.cycle_max_market_cap, 0.0);
        assert_eq!(settle_b_cycle(&mut t, 1300), BCycleOutcome::Expired);
    }

    #[test]
    fn b_new_observation_counts_in_new_cycle_but_gap_or_c_does_not_inherit() {
        let mut t = state(100);
        t.stage = Stage::BReentryPending;
        t.current_cycle_start = Some(100);
        t.cycle_max_market_cap = 12_000.0;
        record_b_market_cap(&mut t, 701, 11_000.0);
        assert_eq!(t.current_cycle_start, Some(700));
        assert_eq!(t.cycle_max_market_cap, 11_000.0);
        t.current_cycle_start = Some(100);
        t.cycle_max_market_cap = 99_000.0;
        assert_eq!(settle_b_cycle(&mut t, 10_000), BCycleOutcome::Expired);
        t.stage = Stage::CWatching;
        assert_eq!(settle_b_cycle(&mut t, 10_000), BCycleOutcome::Unchanged);
    }

    #[tokio::test]
    async fn tick_removes_overdue_unknown_b_and_preserves_tombstone() {
        let mut engine = bare_engine();
        let ca = "0x2222222222222222222222222222222222222222";
        let mut t = state(now_unix() - 1200);
        t.token = ca.into();
        t.stage = Stage::BPending;
        t.first_b_at = Some(now_unix() - 1200);
        t.current_cycle_start = Some(now_unix() - 1200);
        t.last_mcap_usd = None;
        t.cycle_max_market_cap = 0.0;
        engine.tokens.insert(ca.into(), t);
        engine.on_tick().await;
        assert_eq!(engine.tokens[ca].stage, Stage::Dead);
        assert!(engine.blacklist.contains(ca));
        assert!(engine
            .store
            .load_tokens()
            .unwrap()
            .iter()
            .any(|t| t.ca == ca && t.current_state == "DEAD"));
    }

    #[test]
    fn tombstones_stay_canonical_but_do_not_enter_hot_iteration() {
        let mut engine = bare_engine();
        for i in 0..10_000 {
            let ca = format!("0x{i:040x}");
            let mut st = state(1);
            st.token = ca.clone();
            st.stage = Stage::Dead;
            engine.tokens.insert(ca, st);
        }
        let ca = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        engine.tokens.insert(ca.into(), state(now_unix()));
        assert_eq!(engine.tokens.len(), 10_001);
        assert_eq!(engine.tokens.live_iter().count(), 1);
        assert_eq!(engine.stats_now().watching_now, 1);
        assert_eq!(
            engine.tokens["0x0000000000000000000000000000000000000001"].stage,
            Stage::Dead
        );
    }

    #[tokio::test]
    async fn shutdown_saves_without_releasing_into_c_or_dispatching() {
        let (mut engine, trading, _queue) = engine_with_dry_trade().await;
        for i in 0..4 {
            bind_empty_slot(&mut engine, i);
        }
        let ca = "0x2222222222222222222222222222222222222222";
        engine.tokens.insert(ca.into(), trade_candidate(ca));
        engine.promote(ca).await;
        let stage = engine.tokens[ca].stage;
        let before = engine.tokens[ca].first_b_at;
        let mut browser = engine.browser_tx.subscribe();
        engine.shared.system.browser_connected(true);
        let _ = engine.shared.system.begin().unwrap();
        trading.stop_for_shutdown().unwrap();
        let (reply, mut rx) = mpsc::unbounded_channel();
        engine.on_command(Command::PrepareShutdown { reply }).await;
        assert_eq!(rx.recv().await.unwrap().unwrap().len(), 4);
        engine
            .on_command(Command::BrowserUrl {
                slot_id: "A".into(),
                window_id: 17,
                tab_id: 31,
                url: "https://gmgn.ai/?chain=bsc".into(),
            })
            .await;
        engine.on_tick().await;
        engine.schedule(true).await;
        assert_eq!(engine.tokens[ca].stage, stage);
        assert_eq!(engine.tokens[ca].first_b_at, before);
        assert_eq!(engine.tokens[ca].current_slot.as_deref(), Some("A"));
        assert!(engine.tokens[ca].c_entered_at.is_none());
        assert!(browser.try_recv().is_err());
        let saved = engine.store.load_tokens().unwrap();
        assert!(saved
            .iter()
            .any(|t| t.ca == ca && t.current_slot.as_deref() == Some("A")));
    }

    #[tokio::test]
    async fn normal_hello_preserves_opening_reservation_and_unsent_buy() {
        let (mut engine, trading, _queue) = engine_with_dry_trade().await;
        for i in 0..4 {
            bind_empty_slot(&mut engine, i);
        }
        let ca = "0x2222222222222222222222222222222222222222";
        engine.tokens.insert(ca.into(), trade_candidate(ca));
        engine.promote(ca).await;
        let command = engine.slots[0].command_id.clone();
        engine
            .on_command(Command::BrowserHello {
                slots: (0..4)
                    .map(|i| {
                        browser_report(
                            &((b'A' + i as u8) as char).to_string(),
                            17 + i,
                            31 + i,
                            "https://gmgn.ai/?chain=bsc",
                            false,
                        )
                    })
                    .collect(),
            })
            .await;
        assert_eq!(engine.slots[0].command_id, command);
        assert_eq!(engine.slots[0].state, SlotState::Opening);
        assert_eq!(
            trading.view().unwrap().orders[0].state,
            crate::trading::types::OrderState::Queued
        );
        assert!(engine.tokens[ca].c_entered_at.is_none());
        // Expiry/discard replacement may still show an old Token, not a homepage.
        engine
            .reconcile_browser_report(browser_report(
                "A",
                17,
                31,
                "https://gmgn.ai/bsc/token/0x1111111111111111111111111111111111111111",
                false,
            ))
            .await;
        assert_eq!(engine.slots[0].command_id, command);
        assert_eq!(
            trading.view().unwrap().orders[0].state,
            crate::trading::types::OrderState::Queued
        );
    }

    #[tokio::test]
    async fn unknown_page_blocks_slot_without_faking_release_or_c() {
        let mut engine = bare_engine();
        bind_empty_slot(&mut engine, 0);
        let ca = "0x2222222222222222222222222222222222222222";
        let mut st = state(1);
        st.stage = Stage::BActive;
        st.token = ca.into();
        st.current_slot = Some("A".into());
        engine.tokens.insert(ca.into(), st);
        engine.slots[0].token_ca = Some(ca.into());
        engine
            .reconcile_browser_report(browser_report(
                "A",
                17,
                31,
                "https://gmgn.ai/?chain=eth",
                false,
            ))
            .await;
        assert_eq!(engine.slots[0].state, SlotState::Blocked);
        assert_eq!(engine.slots[0].token_ca.as_deref(), Some(ca));
        assert_eq!(engine.tokens[ca].stage, Stage::BActive);
        assert!(!engine.tokens[ca].reentry_used);
        engine
            .reconcile_browser_report(browser_report(
                "A",
                17,
                31,
                "https://gmgn.ai/?chain=bsc",
                false,
            ))
            .await;
        assert_eq!(engine.slots[0].state, SlotState::Empty);
        assert!(engine.slots[0].token_ca.is_none());
        assert_eq!(engine.tokens[ca].stage, Stage::CWatching);
    }

    #[tokio::test]
    async fn explicit_open_failure_resyncs_now_and_never_replays_buy() {
        let (mut engine, trading, _queue) = engine_with_dry_trade().await;
        bind_empty_slot(&mut engine, 0);
        let ca = "0x2222222222222222222222222222222222222222";
        engine.tokens.insert(ca.into(), trade_candidate(ca));
        engine.promote(ca).await;
        let command = engine.slots[0].command_id.clone().unwrap();
        let mut browser = engine.browser_tx.subscribe();
        engine
            .on_command(Command::BrowserCommandFailed {
                slot_id: "A".into(),
                command_id: command,
                token: ca.into(),
                error: "missing tab".into(),
            })
            .await;
        assert!(matches!(
            browser.try_recv().unwrap(),
            BrowserCommand::SyncRequest
        ));
        assert_eq!(engine.slots[0].state, SlotState::Blocked);
        assert!(engine.slots[0].token_ca.is_none());
        assert!(engine.tokens[ca].current_slot.is_none());
        assert_eq!(
            trading.view().unwrap().orders[0].state,
            crate::trading::types::OrderState::Cancelled
        );
        engine
            .reconcile_browser_report(browser_report(
                "A",
                17,
                31,
                "https://gmgn.ai/?chain=bsc",
                false,
            ))
            .await;
        engine.slot_retry_not_before.remove("A");
        engine.schedule(false).await;
        assert_eq!(engine.slots[0].token_ca.as_deref(), Some(ca));
        assert_eq!(trading.view().unwrap().orders.len(), 1);
        assert_eq!(
            trading.view().unwrap().orders[0].state,
            crate::trading::types::OrderState::Cancelled
        );
        let newer = engine.slots[0].command_id.clone();
        engine
            .on_command(Command::BrowserCommandFailed {
                slot_id: "A".into(),
                command_id: "stale".into(),
                token: ca.into(),
                error: "late error".into(),
            })
            .await;
        assert_eq!(engine.slots[0].command_id, newer);
    }

    #[tokio::test]
    async fn progress_is_diagnostic_and_rejects_old_command_or_wrong_tab() {
        let mut engine = bare_engine();
        bind_empty_slot(&mut engine, 0);
        let ca = "0x2222222222222222222222222222222222222222";
        engine.tokens.insert(ca.into(), trade_candidate(ca));
        engine.promote(ca).await;
        let command = engine.slots[0].command_id.clone().unwrap();
        for (id, tab, expected) in [
            ("old".to_string(), 31, false),
            (command.clone(), 99, false),
            (command.clone(), 31, true),
        ] {
            engine
                .on_command(Command::BrowserProgress {
                    slot_id: "A".into(),
                    command_id: id,
                    token: ca.into(),
                    window_id: 17,
                    tab_id: tab,
                    phase: BrowserPhase::Received,
                    page_clock_ms: now_unix_ms(),
                })
                .await;
            assert_eq!(
                engine.browser_timings["A"].received_at_ms.is_some(),
                expected
            );
        }
        assert_eq!(engine.tokens[ca].stage, Stage::BPending);
        assert_eq!(engine.slots[0].state, SlotState::Opening);
    }

    #[tokio::test]
    async fn trade_intent_is_atomic_only_for_first_b_selected_now_and_discard_cancels_it() {
        let (mut engine, trading, mut queue) = engine_with_dry_trade().await;
        let ca = "0x2222222222222222222222222222222222222222";
        bind_empty_slot(&mut engine, 0);
        engine.tokens.insert(ca.into(), trade_candidate(ca));
        engine.promote(ca).await;
        let view = trading.view().unwrap();
        assert_eq!(view.orders.len(), 1);
        let order = &view.orders[0];
        assert_eq!(order.state, crate::trading::types::OrderState::Queued);
        assert_eq!(order.mode, crate::trading::types::Mode::DryRun);
        assert_eq!(order.intent.slot_id, "A");
        assert_eq!(
            engine.slots[0].command_id.as_deref(),
            Some(order.intent.slot_command_id.as_str())
        );
        assert_eq!(queue.try_recv().unwrap(), order.id);
        let persisted = engine.store.load_tokens().unwrap().remove(0);
        assert_eq!(persisted.current_slot.as_deref(), Some("A"));
        assert_eq!(persisted.first_qualified_at_ms, Some(order.intent.b_at_ms));
        assert!(order.nonce.is_none() && order.tx_hash.is_none());
        // No second intent from ACK or a duplicate promotion/scheduler call.
        engine.promote(ca).await;
        engine.schedule(false).await;
        assert_eq!(trading.view().unwrap().orders.len(), 1);
        engine.on_command(Command::Blacklist(ca.into())).await;
        assert_eq!(
            trading.view().unwrap().orders[0].state,
            crate::trading::types::OrderState::Cancelled
        );
        assert!(engine.tokens[ca].user_discarded);
    }

    #[tokio::test]
    async fn full_slots_later_pending_and_c_reentry_never_generate_buy_intents() {
        let (mut engine, trading, _queue) = engine_with_dry_trade().await;
        let ca = "0x2222222222222222222222222222222222222222";
        engine.tokens.insert(ca.into(), trade_candidate(ca));
        engine.promote(ca).await; // No connected/available slot.
        assert!(trading.view().unwrap().orders.is_empty());
        bind_empty_slot(&mut engine, 0);
        engine.schedule(true).await; // Later market-cap-selected pending fill.
        assert!(trading.view().unwrap().orders.is_empty());
        engine.tokens.get_mut(ca).unwrap().stage = Stage::BActive;
        engine.release_slot("A").await;
        let entered = engine.tokens[ca].c_entered_at.unwrap();
        let first = entered.div_ceil(config::CANDLE_PERIOD_SECS);
        for b in first..first + 3 {
            engine.on_candle_closed(ca, b, &candle(b, 1.0, 2.0)).await;
        }
        assert_eq!(engine.tokens[ca].stage, Stage::BReentryPending);
        assert_eq!(engine.tokens[ca].current_slot.as_deref(), Some("A"));
        assert!(trading.view().unwrap().orders.is_empty());
    }

    #[tokio::test]
    async fn fresh_first_b_reserves_available_slot_ahead_of_old_non_buyable_pending() {
        let (mut engine, trading, _queue) = engine_with_dry_trade().await;
        let old = "0x1111111111111111111111111111111111111111";
        let new = "0x2222222222222222222222222222222222222222";
        let mut prior = trade_candidate(old);
        prior.stage = Stage::BPending;
        prior.promoted_at = Some(now_unix() - 1);
        prior.first_b_at = prior.promoted_at;
        prior.first_qualified_at_ms = Some((now_unix() - 1) * 1000);
        engine.tokens.insert(old.into(), prior);
        engine.tokens.insert(new.into(), trade_candidate(new));
        bind_empty_slot(&mut engine, 0);
        engine.promote(new).await;
        assert_eq!(engine.slots[0].token_ca.as_deref(), Some(new));
        assert!(engine.tokens[old].current_slot.is_none());
        assert_eq!(trading.view().unwrap().orders.len(), 1);
        assert_eq!(trading.view().unwrap().orders[0].intent.ca, new);
    }

    fn pending_created_log(block: u64, ca: &str) -> PendingDecodedLog {
        PendingDecodedLog {
            log: RawLog {
                address: config::PORTAL.into(),
                tx_hash: format!("0x{block:x}"),
                tx_from: None,
                block_number: block,
                tx_index: 0,
                log_index: 0,
                topic0: String::new(),
                topics: Vec::new(),
                data: Vec::new(),
            },
            events: vec![NormalizedEvent::NewToken(NewTokenEvent {
                ca: ca.into(),
                name: "FAST".into(),
                symbol: "FAST".into(),
                source: LaunchPlatform::Flap,
                created_at: 0,
                creation_block: block,
                creation_tx_hash: format!("0x{block:x}"),
                creator: "0x0000000000000000000000000000000000000001".into(),
                source_ref: None,
                quote_token: config::ZERO_ADDRESS.into(),
                total_supply_tokens: config::TOTAL_SUPPLY_TOKENS,
                tax_bps: Some((0, 0)),
            })],
            needs_block_timestamp: true,
        }
    }

    #[tokio::test]
    async fn reset_is_durable_and_old_work_cannot_revive_tokens() {
        let mut engine = bare_engine();
        let mut st = state(1);
        st.stage = Stage::BActive;
        st.current_slot = Some("A".into());
        st.reentry_used = true;
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);
        engine.persist(&ca);
        engine.slots[0].token_ca = Some(ca.clone());
        engine.slots[0].state = SlotState::Occupied;
        let mut commands = engine.browser_tx.subscribe();
        assert_eq!(engine.reset_monitoring(100).await.unwrap(), 100);
        assert!(matches!(
            commands.try_recv().unwrap(),
            BrowserCommand::Reset { .. }
        ));
        assert_eq!(engine.tokens[&ca].stage, Stage::Dead);
        assert!(engine.slots.iter().all(|s| s.token_ca.is_none()));
        let rows = engine.store.load_tokens().unwrap();
        assert_eq!(rows[0].current_state, "DEAD");
        assert!(rows[0].reentry_used);
        assert_eq!(
            engine.store.get_meta_u64("reset_cutoff_block").unwrap(),
            Some(100)
        );
        engine
            .on_command(Command::TaxLookup {
                token: ca.clone(),
                result: Ok((0, 0)),
            })
            .await;
        assert_eq!(engine.tokens[&ca].stage, Stage::Dead);
        let NormalizedEvent::NewToken(old) = pending_created_log(100, "0xold").events.remove(0)
        else {
            panic!()
        };
        engine.on_created(old).await;
        assert!(!engine.tokens.contains_key("0xold"));
        let id = engine.reset_pending;
        let restored = engine_with_store(engine.store.clone());
        assert_eq!(restored.reset_pending, id);
        assert_eq!(restored.reset_cutoff, 100);
        assert_eq!(restored.tokens[&ca].stage, Stage::Dead);
        assert!(restored.tokens[&ca].reentry_used);
        engine
            .on_command(Command::BrowserResetAck {
                reset_id: id,
                slots: vec![],
            })
            .await;
        assert_eq!(engine.reset_pending, id);
        let reports = (0..4)
            .map(|i| {
                browser_report(
                    &((b'A' + i) as char).to_string(),
                    10 + i as i64,
                    20 + i as i64,
                    "https://gmgn.ai/?chain=bsc",
                    false,
                )
            })
            .collect();
        engine
            .on_command(Command::BrowserResetAck {
                reset_id: id,
                slots: reports,
            })
            .await;
        assert_eq!(engine.reset_pending, 0);
        assert_eq!(
            engine.store.get_meta_u64("browser_reset_pending").unwrap(),
            Some(0)
        );
        assert!(engine.slots.iter().all(|s| s.state == SlotState::Empty));
    }

    #[tokio::test]
    async fn reset_offline_leaves_existing_tokens_untouched() {
        let mut engine = bare_engine();
        let st = state(1);
        engine.tokens.insert(st.token.clone(), st);
        let (reply, mut rx) = mpsc::unbounded_channel();
        engine
            .on_command(Command::CommitReset {
                boundary: Err("offline".into()),
                reply,
            })
            .await;
        assert!(rx.recv().await.unwrap().is_err());
        assert_eq!(engine.tokens["0xabc"].stage, Stage::Watching);
        assert_eq!(engine.reset_pending, 0);
    }

    #[tokio::test]
    async fn async_block_timestamp_keeps_log_order_and_drains_without_metadata_rpc() {
        let mut engine = bare_engine();
        let first = "0x1111111111111111111111111111111111111111";
        let second = "0x2222222222222222222222222222222222222222";
        engine
            .pending_decoded_logs
            .push_back(pending_created_log(10, first));
        engine
            .pending_decoded_logs
            .push_back(pending_created_log(11, second));
        engine.block_timestamp_cache.insert(11, 1_001);

        engine.drain_pending_decoded_logs().await;
        assert!(engine.tokens.is_empty(), "队首未解析时不得越序处理后块");
        assert_eq!(engine.pending_decoded_logs.len(), 2);

        engine
            .on_command(Command::BlockTimestamp {
                block: 10,
                result: Ok(1_000),
            })
            .await;
        assert!(engine.pending_decoded_logs.is_empty());
        assert_eq!(engine.tokens[first].created_ts, 1_000);
        assert_eq!(engine.tokens[second].created_ts, 1_001);
        assert_eq!(
            engine.shared.last_processed_block.load(Ordering::Relaxed),
            11
        );
    }

    #[tokio::test]
    async fn cancel_sender_aborts_task_and_clears_waiter_count() {
        let mut engine = bare_engine();
        let st = state(1);
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);
        engine
            .sender_lookups_inflight
            .insert("tx".into(), vec![(ca.clone(), 1, Instant::now())]);
        engine.tokens.get_mut(&ca).unwrap().pending_sender_lookups = 1;
        let task = tokio::spawn(std::future::pending::<()>());
        let abort = task.abort_handle();
        engine.sender_tasks.insert("tx".into(), task);
        engine.cancel_sender_lookups_for_token(&ca);
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
        assert!(engine.sender_tasks.is_empty());
        assert!(engine.sender_lookups_inflight.is_empty());
        assert_eq!(engine.tokens[&ca].pending_sender_lookups, 0);
    }

    #[tokio::test]
    async fn header_unblocks_pending_log_without_http() {
        let mut engine = bare_engine();
        let ca = "0x1111111111111111111111111111111111111111";
        engine
            .pending_decoded_logs
            .push_back(pending_created_log(10, ca));
        let header = RawLog {
            address: String::new(),
            tx_hash: String::new(),
            tx_from: None,
            block_number: 10,
            tx_index: 0,
            log_index: 0,
            topic0: "__block_timestamp".into(),
            topics: vec![],
            data: 1000u64.to_be_bytes().to_vec(),
        };
        engine.on_log(header).await;
        assert_eq!(engine.tokens[ca].created_ts, 1000);
        assert!(engine.pending_decoded_logs.is_empty());
        assert!(
            engine.seen_log_keys.is_empty(),
            "headers must not claim a real log index"
        );
    }

    #[test]
    fn gmgn_home_matching_ignores_ref_but_requires_bsc() {
        assert!(is_gmgn_home("https://gmgn.ai/?chain=bsc&ref=anything"));
        assert!(is_gmgn_home("https://gmgn.ai/?ref=changed&chain=BSC"));
        assert!(!is_gmgn_home("https://gmgn.ai/?chain=eth"));
        assert!(!is_gmgn_home(
            "https://gmgn.ai/bsc/token/0x1111111111111111111111111111111111111111"
        ));
    }

    #[test]
    fn token_url_accepts_single_trailing_slash_but_not_extra_path_or_wrong_host() {
        let ca = "0x1111111111111111111111111111111111111111";
        assert_eq!(
            gmgn_token_from_url(&format!("https://gmgn.ai/bsc/token/{ca}/?ref=x")),
            Some(ca.into())
        );
        assert!(gmgn_token_from_url(&format!("https://gmgn.ai/bsc/token/{ca}/other")).is_none());
        assert!(gmgn_token_from_url(&format!("https://evil.test/bsc/token/{ca}")).is_none());
    }

    #[tokio::test]
    async fn late_tax_classification_cannot_revive_uncounted_discarded_token() {
        let mut engine = bare_engine();
        let ca = "0x1111111111111111111111111111111111111111";
        let mut st = state(1);
        st.token = ca.into();
        st.counted = false;
        st.stage = Stage::UserDiscarded;
        engine.tokens.insert(ca.into(), st);
        engine.pending_tax.insert(ca.into(), (0, 0));
        assert!(!engine.finalize_classification(ca).await);
        assert_eq!(engine.tokens[ca].stage, Stage::UserDiscarded);
        assert_eq!(engine.tokens.live_iter().count(), 0);
    }

    #[tokio::test]
    async fn candidates_api_excludes_active_slots_and_never_copies_chart_arrays() {
        let engine = bare_engine();
        let mut active = state(1);
        active.stage = Stage::BActive;
        active.current_slot = Some("A".into());
        let mut pending = state(1);
        pending.token = "0xpending".into();
        pending.stage = Stage::BPending;
        for i in 0..600 {
            pending.candles.insert(i, candle(i, 1.0, 2.0));
        }
        *engine.shared.snapshots.write().await = vec![snapshot_of(&active), snapshot_of(&pending)];
        let axum::Json(body) =
            crate::server::api_candidates(axum::extract::State(crate::server::AppState {
                handle: Handle {
                    cmd_tx: engine.cmd_tx.clone(),
                    event_tx: engine.event_tx.clone(),
                    browser_tx: engine.browser_tx.clone(),
                    shared: engine.shared.clone(),
                },
                web_root: std::path::PathBuf::from("."),
                control_token: None,
            }))
            .await;
        let tokens = body["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["token"], "0xpending");
        assert!(tokens[0].get("candles").is_none() && tokens[0].get("trades").is_none());
        assert!(body.to_string().len() < 2048);
    }

    fn browser_report(
        slot_id: &str,
        window_id: i64,
        tab_id: i64,
        url: &str,
        recreated: bool,
    ) -> BrowserSlotReport {
        BrowserSlotReport {
            slot_id: slot_id.into(),
            window_id: Some(window_id),
            tab_id: Some(tab_id),
            url: url.into(),
            recreated,
            pending_url: None,
        }
    }

    #[tokio::test]
    async fn browser_hello_rejects_slots_that_share_a_window() {
        let mut engine = bare_engine();
        engine.slots[0].browser_window_id = Some(99);
        engine.slots[0].browser_tab_id = Some(100);
        engine.slots[0].state = SlotState::Empty;
        engine
            .on_command(Command::BrowserHello {
                slots: vec![
                    browser_report("A", 1, 10, "https://gmgn.ai/?chain=bsc", false),
                    browser_report("B", 1, 11, "https://gmgn.ai/?chain=bsc", false),
                ],
            })
            .await;
        assert_eq!(engine.slots[0].browser_window_id, Some(99));
        assert_eq!(engine.slots[0].browser_tab_id, Some(100));
        assert_eq!(engine.slots[0].state, SlotState::Empty);
        assert_eq!(engine.slots[1].browser_window_id, None);
        assert_eq!(engine.slots[1].state, SlotState::Disconnected);
    }

    #[tokio::test]
    async fn recreated_window_restores_active_token_instead_of_releasing_it() {
        let mut engine = bare_engine();
        let mut commands = engine.browser_tx.subscribe();
        let ca = "0x1111111111111111111111111111111111111111".to_string();
        let mut st = state(1);
        st.token = ca.clone();
        st.stage = Stage::BActive;
        st.current_slot = Some("A".into());
        engine.tokens.insert(ca.clone(), st);
        engine.slots[0].token_ca = Some(ca.clone());

        engine
            .on_command(Command::BrowserHello {
                slots: vec![
                    browser_report("A", 20, 30, "https://gmgn.ai/?chain=bsc", true),
                    browser_report("B", 21, 31, "https://gmgn.ai/?chain=bsc", false),
                    browser_report("C", 22, 32, "https://gmgn.ai/?chain=bsc", false),
                    browser_report("D", 23, 33, "https://gmgn.ai/?chain=bsc", false),
                ],
            })
            .await;

        let BrowserCommand::Open {
            slot_id,
            ca: reopened,
            ..
        } = commands.try_recv().expect("应向重建窗口重新发送 OPEN")
        else {
            panic!("expected OPEN")
        };
        assert_eq!(slot_id, "A");
        assert_eq!(reopened, ca);
        assert_eq!(engine.tokens[&ca].stage, Stage::BActive);
        assert_eq!(engine.tokens[&ca].current_slot.as_deref(), Some("A"));
        assert_eq!(engine.slots[0].state, SlotState::Opening);
        assert_eq!(engine.slots[0].browser_window_id, Some(20));
    }

    #[tokio::test]
    async fn reported_pingheng_candles_qualify_at_211715_despite_no_trade_gap() {
        // Actual chain OHLC for 0x35df...c7727777 on 2026-09-27 (UTC+8).
        // The original engine incorrectly required adjacent bucket numbers,
        // delaying qualification from 21:17:15 until a later streak at 21:17:27.
        let mut engine = bare_engine();
        let mut st = state(1_790_515_014); // 21:16:54
        st.token = "0x35df2708643912255113ec56998f679dc7727777".into();
        st.target_buyer_seen = false;
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);
        let first = 1_790_515_014 / 3;
        engine
            .on_candle_closed(
                &ca,
                first,
                &candle(first, 3.6050283671834355e-6, 3.8002720148990676e-6),
            )
            .await;
        assert_eq!(engine.tokens[&ca].streak, 1);
        // Four empty 3-second intervals: no candles and no reset.
        let mut empty = candle(first + 1, 3.8002720148990676e-6, 3.8002720148990676e-6);
        empty.trades = 0;
        engine.on_candle_closed(&ca, first + 1, &empty).await;
        assert_eq!(engine.tokens[&ca].streak, 1);
        {
            let st = engine.tokens.get_mut(&ca).unwrap();
            st.target_buyer_seen = true;
            st.b_timing.buyer_at = Some(1_790_515_030); // 21:17:10
        }
        engine
            .on_candle_closed(
                &ca,
                first + 5,
                &candle(first + 5, 3.8002720148990676e-6, 5.3158855517885606e-6),
            )
            .await;
        assert_eq!(engine.tokens[&ca].streak, 2);
        let mut third = candle(first + 6, 5.3158855517885606e-6, 5.782748442492319e-6);
        third.closed = false;
        engine.on_candle_closed(&ca, first + 6, &third).await;
        assert_eq!(
            engine.tokens[&ca].stage,
            Stage::AWatching,
            "未收线不能提前成为 B"
        );
        third.closed = true;
        engine.on_candle_closed(&ca, first + 6, &third).await;
        assert_eq!(engine.tokens[&ca].stage, Stage::BPending);
        assert_eq!(
            engine.tokens[&ca].b_timing.first_chain_at_ms,
            Some(1_790_515_035_000)
        );
        assert_eq!(engine.tokens[&ca].promote_streak, 3);
    }

    #[tokio::test]
    async fn c_reentry_skips_empty_intervals_without_inheriting_pre_entry_candles() {
        let mut engine = bare_engine();
        let mut st = state(1);
        st.stage = Stage::CWatching;
        st.c_entered_at = Some(301);
        st.c_first_bucket = Some(101); // Entry is inside bucket 100.
        st.reentry_used = true;
        st.streak = 2; // A/B streak must never count toward C.
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);
        engine
            .on_candle_closed(&ca, 100, &candle(100, 1.0, 2.0))
            .await;
        assert_eq!(engine.tokens[&ca].c_streak, 0);
        for bucket in [101, 106] {
            engine
                .on_candle_closed(&ca, bucket, &candle(bucket, 1.0, 2.0))
                .await;
            assert_eq!(engine.tokens[&ca].stage, Stage::CWatching);
        }
        let mut empty = candle(107, 2.0, 2.0);
        empty.trades = 0;
        engine.on_candle_closed(&ca, 107, &empty).await;
        assert_eq!(engine.tokens[&ca].c_streak, 2);
        engine
            .on_candle_closed(&ca, 111, &candle(111, 1.0, 2.0))
            .await;
        assert_eq!(engine.tokens[&ca].stage, Stage::BReentryPending);
        assert_eq!(
            engine.tokens[&ca].b_timing.second_chain_at_ms,
            Some(336_000)
        );
    }

    #[tokio::test]
    async fn actual_red_and_doji_candles_reset_a_and_c_streaks_even_with_gaps() {
        for stage in [Stage::AWatching, Stage::CWatching] {
            let mut engine = bare_engine();
            let mut st = state(300);
            st.stage = stage;
            st.c_entered_at = Some(300);
            st.c_first_bucket = Some(100);
            let ca = st.token.clone();
            engine.tokens.insert(ca.clone(), st);
            for (bucket, open, close, expected) in [
                (100, 1.0, 2.0, 1),
                (102, 2.0, 3.0, 2),
                (104, 3.0, 2.0, 0), // Actual red candle breaks.
                (106, 2.0, 3.0, 1),
                (108, 3.0, 3.0, 0), // Actual doji breaks.
                (110, 3.0, 4.0, 1),
                (112, 4.0, 5.0, 2),
            ] {
                engine
                    .on_candle_closed(&ca, bucket, &candle(bucket, open, close))
                    .await;
                let st = &engine.tokens[&ca];
                assert_eq!(
                    if stage == Stage::CWatching {
                        st.c_streak
                    } else {
                        st.streak
                    },
                    expected
                );
                assert_eq!(st.stage, stage);
            }
            engine
                .on_candle_closed(&ca, 114, &candle(114, 5.0, 6.0))
                .await;
            assert_eq!(
                engine.tokens[&ca].stage,
                if stage == Stage::CWatching {
                    Stage::BReentryPending
                } else {
                    Stage::BPending
                }
            );
        }
    }

    #[tokio::test]
    async fn no_trade_gaps_do_not_extend_a_or_c_observation_deadlines() {
        for (stage, last_bucket) in [(Stage::AWatching, 120), (Stage::CWatching, 180)] {
            let mut engine = bare_engine();
            let mut st = state(301);
            st.stage = stage;
            st.c_entered_at = Some(301);
            st.c_first_bucket = Some(101);
            let ca = st.token.clone();
            engine.tokens.insert(ca.clone(), st);
            for bucket in [101, 110, last_bucket] {
                // Last candle starts inside the window but ends after its deadline.
                engine
                    .on_candle_closed(&ca, bucket, &candle(bucket, 1.0, 2.0))
                    .await;
            }
            assert_eq!(engine.tokens[&ca].stage, stage);
            assert!(engine.tokens[&ca].first_b_at.is_none());
            assert!(engine.tokens[&ca].reentry_b_at.is_none());
        }
        // Finishing exactly at the deadline is still valid.
        for (stage, last_bucket) in [(Stage::AWatching, 119), (Stage::CWatching, 179)] {
            let mut engine = bare_engine();
            let mut st = state(300);
            st.stage = stage;
            st.c_entered_at = Some(300);
            st.c_first_bucket = Some(100);
            let ca = st.token.clone();
            engine.tokens.insert(ca.clone(), st);
            for bucket in [100, 110, last_bucket] {
                engine
                    .on_candle_closed(&ca, bucket, &candle(bucket, 1.0, 2.0))
                    .await;
            }
            assert!(engine.tokens[&ca].stage.is_b());
        }
    }

    #[tokio::test]
    async fn b_history_timing_is_fixed_at_promotion_and_restored() {
        let mut engine = bare_engine();
        let mut st = state(300);
        st.b_timing.buyer_at = Some(305);
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);
        for bucket in 100..103 {
            engine
                .on_candle_closed(&ca, bucket, &candle(bucket, 1.0, 2.0))
                .await;
        }
        let st = &engine.tokens[&ca];
        assert_eq!(st.b_timing.first_chain_at_ms, Some(309_000));
        let first_ms = st.first_qualified_at_ms.unwrap();
        assert_eq!(st.first_b_at, Some(first_ms / 1000));
        engine.promote(&ca).await; // Duplicate promotion must not change history.
        assert_eq!(engine.tokens[&ca].first_qualified_at_ms, Some(first_ms));
        let store = engine.store.clone();
        let restored = engine_with_store(store);
        assert_eq!(restored.tokens[&ca].first_qualified_at_ms, Some(first_ms));
        assert_eq!(
            restored.tokens[&ca].b_timing.first_chain_at_ms,
            Some(309_000)
        );
        assert!(restored.tokens[&ca].reentry_b_at.is_none());
    }

    #[tokio::test]
    async fn green_evidence_survives_waiting_for_buyer_and_old_evidence_stays_unknown() {
        let mut engine = bare_engine();
        let mut st = state(300);
        st.target_buyer_seen = false;
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);
        for bucket in 100..103 {
            engine
                .on_candle_closed(&ca, bucket, &candle(bucket, 1.0, 2.0))
                .await;
        }
        let rows = engine.store.load_tokens().unwrap();
        assert_eq!(rows[0].b_timing.green_at, Some(309));
        assert!(rows[0].first_b_at.is_none());
        // Legacy hit flag alone is not evidence of a precise buyer time.
        engine.tokens.get_mut(&ca).unwrap().target_buyer_seen = true;
        engine.promote(&ca).await;
        assert!(engine.tokens[&ca].b_timing.first_chain_at_ms.is_none());
    }

    #[tokio::test]
    async fn c_streak_starts_after_c_entry_and_reenters_only_once() {
        let mut engine = bare_engine();
        let mut st = state(1);
        st.token = "0x1111111111111111111111111111111111111111".into();
        st.stage = Stage::CWatching;
        st.first_b_at = Some(290);
        st.first_qualified_at_ms = Some(290_123);
        st.c_entered_at = Some(300);
        st.reentry_used = true;
        st.c_first_bucket = Some(100);
        let ca = st.token.clone();
        engine.tokens.insert(ca.clone(), st);

        engine
            .on_candle_closed(&ca, 99, &candle(99, 1.0, 2.0))
            .await;
        engine
            .on_candle_closed(&ca, 100, &candle(100, 1.0, 2.0))
            .await;
        engine
            .on_candle_closed(&ca, 101, &candle(101, 2.0, 3.0))
            .await;
        assert_eq!(
            engine.tokens[&ca].stage,
            Stage::CWatching,
            "进入 C 前的阳线不得继承"
        );
        engine
            .on_candle_closed(&ca, 102, &candle(102, 3.0, 4.0))
            .await;
        assert_eq!(engine.tokens[&ca].stage, Stage::BReentryPending);
        assert!(engine.tokens[&ca].reentry_b_at.is_some());
        let second_ms = engine.tokens[&ca].last_qualified_at_ms;
        assert_eq!(
            engine.tokens[&ca].b_timing.second_chain_at_ms,
            Some(309_000)
        );
        assert_eq!(engine.tokens[&ca].first_qualified_at_ms, Some(290_123));
        let rows = engine.store.load_tokens().unwrap();
        assert_eq!(rows[0].last_qualified_at_ms, second_ms);
        assert_eq!(rows[0].b_timing.second_chain_at_ms, Some(309_000));

        engine.tokens.get_mut(&ca).unwrap().stage = Stage::BReentryActive;
        engine.tokens.get_mut(&ca).unwrap().current_slot = Some("A".into());
        engine.slots[0].token_ca = Some(ca.clone());
        engine.slots[0].browser_tab_id = Some(10);
        engine.slots[0].state = SlotState::Occupied;
        engine.release_slot("A").await;
        assert_eq!(
            engine.tokens[&ca].stage,
            Stage::Dead,
            "二次释放必须直接终止"
        );
        assert_eq!(engine.tokens[&ca].last_qualified_at_ms, second_ms);
    }

    #[tokio::test]
    async fn released_slot_schedules_highest_market_cap_without_duplicates() {
        let mut engine = bare_engine();
        engine.slots[0].state = SlotState::Empty;
        engine.slots[0].browser_tab_id = Some(1);
        engine.slots[1].state = SlotState::Empty;
        engine.slots[1].browser_tab_id = Some(2);
        for (suffix, cap) in [('1', 8_000.0), ('2', 20_000.0), ('3', 13_000.0)] {
            let mut st = state(1);
            st.token = format!("0x{}{}", "0".repeat(39), suffix);
            st.stage = Stage::BPending;
            st.promoted_at = Some(1);
            st.first_b_at = Some(1);
            st.current_cycle_start = Some(1);
            st.last_mcap_usd = Some(cap);
            engine.tokens.insert(st.token.clone(), st);
        }
        engine.schedule(true).await;
        assert_eq!(
            engine.slots[0].token_ca.as_deref(),
            Some("0x0000000000000000000000000000000000000002")
        );
        assert_eq!(
            engine.slots[1].token_ca.as_deref(),
            Some("0x0000000000000000000000000000000000000003")
        );
        assert_ne!(engine.slots[0].token_ca, engine.slots[1].token_ca);
        let chosen = "0x0000000000000000000000000000000000000002";
        let sent = engine.tokens[chosen]
            .first_open_sent_at_ms
            .expect("OPEN 必须记录毫秒时间");
        let command_id = engine.slots[0].command_id.clone().unwrap();
        engine
            .on_command(Command::BrowserAck {
                slot_id: "A".into(),
                command_id,
                token: chosen.into(),
                window_id: 10,
                tab_id: 1,
            })
            .await;
        assert!(engine.tokens[chosen]
            .first_open_acked_at_ms
            .is_some_and(|acked| acked >= sent));
    }

    /// 复刻 on_candle_closed 的连阳判定分支，并返回是否应晋升。
    fn feed(st: &mut TokenState, bucket: u64, c: &Candle) -> bool {
        if !c.closed || c.trades == 0 {
            return false;
        }
        if !(c.close > c.open) {
            st.streak = 0;
            st.streak_end = None;
        } else {
            st.streak += 1;
            st.streak_end = Some(bucket);
        }
        let in_window = c.time + config::CANDLE_PERIOD_SECS <= st.observe_until_ts;
        let newly_seen = in_window && st.streak >= config::BULL_STREAK && !st.bull_streak_seen;
        if newly_seen {
            st.bull_streak_seen = true;
            st.bull_streak_value = st.streak;
        }
        st.stage == Stage::Watching && st.target_buyer_seen && newly_seen
    }

    #[test]
    fn three_adjacent_green_candles_trigger_b_token() {
        let mut st = state(1_788_699_923);
        // 建币后紧接着的三根阳线。
        let base = 1_788_699_923 / config::CANDLE_PERIOD_SECS;
        assert!(!feed(&mut st, base, &candle(base, 1.0, 1.1)));
        assert_eq!(st.streak, 1);
        assert!(!feed(&mut st, base + 1, &candle(base + 1, 1.1, 1.2)));
        assert_eq!(st.streak, 2);
        assert!(
            feed(&mut st, base + 2, &candle(base + 2, 1.2, 1.3)),
            "第三根相邻阳线应触发晋升"
        );
        assert_eq!(st.streak, 3);
    }

    #[test]
    fn red_candle_breaks_streak() {
        let mut st = state(1_788_699_923);
        let b = 1_788_699_923 / config::CANDLE_PERIOD_SECS;
        feed(&mut st, b, &candle(b, 1.0, 1.1));
        feed(&mut st, b + 1, &candle(b + 1, 1.1, 1.2));
        assert_eq!(st.streak, 2);
        feed(&mut st, b + 2, &candle(b + 2, 1.2, 1.1)); // 阴线
        assert_eq!(st.streak, 0, "阴线必须清零");
        feed(&mut st, b + 3, &candle(b + 3, 1.1, 1.2));
        assert_eq!(st.streak, 1, "清零后重新从 1 开始");
    }

    #[test]
    fn doji_is_not_green() {
        let mut st = state(1_788_699_923);
        let b = 1_788_699_923 / config::CANDLE_PERIOD_SECS;
        feed(&mut st, b, &candle(b, 1.0, 1.0));
        assert_eq!(st.streak, 0, "开收相等不算阳线");
    }

    #[test]
    fn gap_does_not_break_consecutive_green_candles() {
        // 无成交空桶不存在，跳过，不计一根，也不打断已有阳线。
        let mut st = state(1_788_699_923);
        let b = 1_788_699_923 / config::CANDLE_PERIOD_SECS;
        feed(&mut st, b, &candle(b, 1.0, 1.1));
        feed(&mut st, b + 1, &candle(b + 1, 1.1, 1.2));
        assert_eq!(st.streak, 2);
        // 桶 b+2 缺失，直接跳到 b+3。
        assert!(feed(&mut st, b + 3, &candle(b + 3, 1.2, 1.3)));
        assert_eq!(st.streak, 3, "缺桶后仍延续已有两根阳线");
    }

    #[test]
    fn alternating_gaps_still_allow_three_green_candles() {
        let mut st = state(1_788_699_923);
        let b = 1_788_699_923 / config::CANDLE_PERIOD_SECS;
        let mut promoted = false;
        for i in 0..10u64 {
            promoted |= feed(&mut st, b + i * 2, &candle(b + i * 2, 1.0, 1.1));
        }
        assert_eq!(st.streak, 10);
        assert!(promoted, "隔桶阳线只要中间没有实际阴/平线就延续计数");
    }

    #[test]
    fn candles_after_window_do_not_promote() {
        // 观察窗之后的行情不能把已过期的币重新拉起（回填模式尤其重要）。
        let created = 1_788_699_923u64;
        let mut st = state(created);
        let late = (created + 120) / config::CANDLE_PERIOD_SECS;
        for i in 0..3u64 {
            assert!(!feed(&mut st, late + i, &candle(late + i, 1.0, 1.1)));
        }
        assert_eq!(st.streak, 3, "连阳计数仍在，但不允许晋升");
    }

    #[test]
    fn already_live_token_is_not_repromoted() {
        let mut st = state(1_788_699_923);
        st.stage = Stage::Live;
        let b = 1_788_699_923 / config::CANDLE_PERIOD_SECS;
        for i in 0..3u64 {
            assert!(!feed(&mut st, b + i, &candle(b + i, 1.0, 1.1)));
        }
    }

    #[test]
    fn amount_formatting_is_exact() {
        assert_eq!(fmt_amount(0, 18), "0");
        assert_eq!(fmt_amount(10u128.pow(18), 18), "1");
        assert_eq!(fmt_amount(15 * 10u128.pow(17), 18), "1.5");
        // 实测 BUY 样本 quote = 0xbbbb338c5b34 = 206412698114868。
        assert_eq!(fmt_amount(206412698114868, 18), "0.00020641");
        // 实测 token = 0x3db7e14a09e62004d3a = 18216021463701750041914。
        // 小数部分 021463701750041914 截断到 8 位得 "02146370"，去尾零后为 "0214637"。
        assert_eq!(fmt_amount(0x3db7e14a09e62004d3au128, 18), "18216.0214637");
        // 非零但低于 8 位小数精度：绝不能显示成 0。
        assert_eq!(fmt_amount(1, 18), "<0.00000001");
        // 尾随零必须去掉，不能出现 "1.50000000"。
        assert_eq!(fmt_amount(15 * 10u128.pow(17), 18), "1.5");
        // 恰好 8 位小数不被误判为低于精度。
        assert_eq!(fmt_amount(10u128.pow(10), 18), "0.00000001");
    }

    #[test]
    fn bucketing_uses_three_second_windows() {
        let p = config::CANDLE_PERIOD_SECS;
        assert_eq!(p, 3);
        // 实测建币 ts = 0x6a9d6513 = 1788699923。注意 ts % 3 == 2，
        // 即它落在桶 [1788699921, 1788699924) 的最后一秒，并非桶起点。
        let ts = 1788699923u64;
        assert_eq!(ts % p, 2);
        let b = ts / p;
        assert_eq!(b, 596233307);
        assert_eq!(b * p, 1788699921, "桶起点应为 3 的整数倍");
        // 桶内三个时刻必须落同一桶。
        assert_eq!((b * p) / p, b);
        assert_eq!((b * p + 1) / p, b);
        assert_eq!((b * p + 2) / p, b);
        // 跨过桶末的下一秒进入下一桶。
        assert_eq!((b * p + 3) / p, b + 1);
        assert_eq!(ts / p, b, "实测 ts 应落在该桶");
        assert_eq!((ts + 1) / p, b + 1, "ts 已在桶末，+1 秒即跨桶");
    }

    #[test]
    fn bucket_close_rule_matches_period() {
        // 桶在 (bucket+1)*3 秒时刻完结：早一秒不得封口，恰好等于必须封口。
        let p = config::CANDLE_PERIOD_SECS;
        let b = 100u64;
        let end = (b + 1) * p; // = 303
        assert!(!bucket_is_finished(b, end - 1), "早 1 秒不得封口");
        assert!(bucket_is_finished(b, end), "恰好等于结束时刻应封口");
        assert!(bucket_is_finished(b, end + 1), "晚于结束时刻应封口");
        // 上一桶此时早已封口，不受影响。
        assert!(bucket_is_finished(b - 1, end));
        // 下一桶在 end 时刻才刚开始，不得封口。
        assert!(!bucket_is_finished(b + 1, end));
        // 链上时间为 0 时（尚未收到任何成交）不应封口任何桶。
        assert!(!bucket_is_finished(0, 0));
    }

    #[test]
    fn tax_filter_boundary_is_inclusive_at_four_percent() {
        // 需求：买卖税都 <= 4% 才算 A 币。
        assert_eq!(config::MAX_TAX_BPS, 400);
        // 边界：恰好 4.00% 通过，4.01% 剔除。
        assert!(tax_passes(400, 400), "买卖各 4.00% 应通过");
        assert!(!tax_passes(401, 400), "买税 4.01% 应剔除");
        assert!(!tax_passes(400, 401), "卖税 4.01% 应剔除");
        // 0 税（标准币）通过。
        assert!(tax_passes(0, 0), "无税币应通过");
        // 实测样本：buy=100 sell=0 -> A 币；buy=300 sell=500 -> 剔除。
        assert!(tax_passes(100, 0), "实测样本 1%/0% 应通过");
        assert!(!tax_passes(300, 500), "实测样本 3%/5% 应剔除");
        // 非对称：只要一侧超标就剔除（与关系，不是或）。
        assert!(!tax_passes(0, 1000), "仅卖税 10% 也应剔除");
        assert!(!tax_passes(1000, 0), "仅买税 10% 也应剔除");
    }

    #[tokio::test]
    async fn rpc_tax_failure_stays_pending_instead_of_becoming_401_bps() {
        let mut engine = bare_engine();
        let token = "0x1111111111111111111111111111111111114010".to_string();
        let mut st = state(now_unix());
        st.token = token.clone();
        st.source_platform = LaunchPlatform::GeniusFun;
        st.source_ref = Some("0x2222222222222222222222222222222222222222".into());
        st.stage = Stage::New;
        st.counted = false;
        st.buy_tax_bps = 0;
        st.sell_tax_bps = 0;
        st.target_buyer_seen = false;
        engine.tokens.insert(token.clone(), st);

        engine
            .on_command(Command::TaxLookup {
                token: token.clone(),
                result: Err("wss 未连接".into()),
            })
            .await;

        let st = engine.tokens.get(&token).unwrap();
        assert_eq!(st.stage, Stage::New);
        assert!(!st.counted);
        assert_eq!((st.buy_tax_bps, st.sell_tax_bps), (0, 0));
        assert!(!engine.blacklist.contains(&token));
        assert!(engine.tax_retry_not_before.contains_key(&token));
    }

    #[tokio::test]
    async fn confirmed_rpc_tax_can_promote_or_reject() {
        let mut engine = bare_engine();
        let pass = "0x1111111111111111111111111111111111114000".to_string();
        let reject = "0x1111111111111111111111111111111111114001".to_string();
        for token in [&pass, &reject] {
            let mut st = state(now_unix());
            st.token = token.clone();
            st.source_platform = LaunchPlatform::GeniusFun;
            st.source_ref = Some("0x2222222222222222222222222222222222222222".into());
            st.stage = Stage::New;
            st.counted = false;
            st.buy_tax_bps = 0;
            st.sell_tax_bps = 0;
            st.target_buyer_seen = token == &pass;
            st.bull_streak_seen = token == &pass;
            st.bull_streak_value = if token == &pass {
                config::BULL_STREAK
            } else {
                0
            };
            engine.tokens.insert(token.clone(), st);
        }

        engine
            .on_command(Command::TaxLookup {
                token: pass.clone(),
                result: Ok((0, 0)),
            })
            .await;
        assert_eq!(engine.tokens[&pass].stage, Stage::BPending);
        assert_eq!(engine.tokens[&pass].first_b_at.is_some(), true);

        engine
            .on_command(Command::TaxLookup {
                token: reject.clone(),
                result: Ok((401, 401)),
            })
            .await;
        assert_eq!(engine.tokens[&reject].stage, Stage::Dead);
        assert!(engine.blacklist.contains(&reject));
    }

    #[test]
    fn observe_window_is_sixty_seconds() {
        assert_eq!(config::WATCH_WINDOW, Duration::from_secs(60));
        let created = 1788699923u64;
        let st = state(created);
        assert_eq!(st.observe_until_ts, created + 60);
    }

    /// 快照只下发报价单位的数值与 `quote_is_native` 标志，美元换算在前端做。
    ///
    /// 这里锁定的是「标志与数值必须齐全且自洽」：前端全靠 `quoteIsNative` 决定
    /// 能不能乘 BNB/USD，标志错了就会把 QQQB 计价的市值当成美元显示出来——
    /// 那是一个看起来完全合理、量级却错得离谱的数字。
    #[test]
    fn snapshot_carries_quote_values_and_native_flag() {
        let mut st = state(1_800_000_000);
        // 规格算例：单价 4.95e-9 BNB，市值 4.95 BNB。
        st.last_price = 4.95e-9;
        st.last_mcap_quote = 4.95;

        let s = snapshot_of(&st);
        assert!(s.quote_is_native, "零地址报价应判定为原生 BNB");
        assert_eq!(s.quote_symbol, "BNB");
        assert!((s.last_price - 4.95e-9).abs() < 1e-18);
        assert!((s.mcap_quote - 4.95).abs() < 1e-12);
        assert_eq!(s.total_supply, config::TOTAL_SUPPLY_TOKENS);
        // 前端按「单价 × 供应量」理解市值，两者必须自洽。
        assert!((s.mcap_quote - s.last_price * s.total_supply).abs() < 1e-12);

        // 非原生 BNB 计价：标志必须翻转，前端才会拒绝用 BNB 价折算。
        st.quote_token = "0x205812cdbed920aff76c6580abd681a46d11efc7".into();
        st.quote_symbol = Some("QQQB".into());
        let s = snapshot_of(&st);
        assert!(!s.quote_is_native, "非零地址报价不得判定为原生 BNB");
        assert_eq!(s.quote_symbol, "QQQB");
        // 报价单位的值必须原样保留，前端才能回退显示「4.95 QQQB」。
        assert!((s.mcap_quote - 4.95).abs() < 1e-12);
        assert!((s.last_price - 4.95e-9).abs() < 1e-18);
    }

    #[test]
    fn tail8_and_short_do_not_panic_on_short_input() {
        assert_eq!(tail8("0xabc"), "0xabc");
        assert_eq!(
            tail8("0xf9c5919ec1880bbe4bb722f59e152220f1db7777"),
            "f1db7777"
        );
        assert_eq!(tail8(""), "");
        // 空名称不能显示成空白，日志里要能看出「没有名字」。
        assert_eq!(short(""), "(无名)");
        // 13 字符 < 上限 14，不应截断。
        let s13 = "一个很长很长的代币名称测试";
        assert_eq!(s13.chars().count(), 13);
        assert_eq!(short(s13), s13);
        // 恰好 14 字符，边界不截断。
        let s14 = "一个很长很长的代币名称测试测";
        assert_eq!(s14.chars().count(), 14);
        assert_eq!(short(s14), s14);
        // 17 字符 > 14，必须截断到 14。
        let s17 = "一个很长很长的代币名称测试测试测试";
        assert_eq!(s17.chars().count(), 17);
        let got = short(s17);
        assert_eq!(got.chars().count(), 14, "应截断到 14 字符: {got}");
        assert_eq!(got, "一个很长很长的代币名称测试测");
        // 截断必须按字符而非字节，不能把 UTF-8 字符切成半个。
        assert!(got.chars().all(|c| s17.contains(c)));
    }

    #[tokio::test]
    async fn shared_snapshot_starts_empty() {
        let s = Shared::default();
        assert!(s.snapshots.read().await.is_empty());
        assert!(s.bnb_usd.read().await.is_none());
    }

    // ============================================================ 引擎集成测试
    //
    // 前面的单元测试只覆盖纯函数（连阳判定、格式化、分桶），但实际发现的缺陷
    // 全部出在引擎的**接线**上：统计双重计数、回填竞态导致提前剔除。
    // 纯函数测试对这些缺陷完全无感，因为它们各自「都对」。
    // 因此这里用真实引擎 + 合成链上事件做端到端验证。

    /// 32 字节字编码辅助。
    fn w256(v: u128) -> Vec<u8> {
        let mut b = [0u8; 32];
        b[16..].copy_from_slice(&v.to_be_bytes());
        b.to_vec()
    }
    fn w_addr(a: &str) -> Vec<u8> {
        let mut b = [0u8; 32];
        let raw = crate::abi::unhex(a.trim_start_matches("0x")).expect("地址需为合法 hex");
        assert_eq!(raw.len(), 20, "地址须为 20 字节");
        b[12..].copy_from_slice(&raw);
        b.to_vec()
    }
    /// ABI 动态 string：长度字 + 数据（右侧补零到 32 的倍数）。
    fn w_string(s: &str) -> Vec<u8> {
        let bytes = s.as_bytes();
        let mut out = w256(bytes.len() as u128);
        out.extend_from_slice(bytes);
        let pad = (32 - bytes.len() % 32) % 32;
        out.extend(std::iter::repeat(0u8).take(pad));
        out
    }

    /// 编码 TokenCreated 的 data（7 个 head 字 + 三段动态 string）。
    fn enc_created(ts: u64, token: &str, name: &str, symbol: &str) -> Vec<u8> {
        let name_body = w_string(name);
        let sym_body = w_string(symbol);
        let meta_body = w_string("QmTestCid");
        let off_name = 7 * 32;
        let off_sym = off_name + name_body.len();
        let off_meta = off_sym + sym_body.len();
        let mut d = Vec::new();
        d.extend(w256(ts as u128));
        d.extend(w_addr("0x634aa73273b1e7cd02f61eee2d1c7c1135d72ab2"));
        d.extend(w256(1)); // nonce
        d.extend(w_addr(token));
        d.extend(w256(off_name as u128));
        d.extend(w256(off_sym as u128));
        d.extend(w256(off_meta as u128));
        d.extend(name_body);
        d.extend(sym_body);
        d.extend(meta_body);
        d
    }

    /// 编码 TokenBought / TokenSold 的 data（7 个定长字）。
    /// 单价 = 有效成交额 / token_amount，见 `abi::unit_price`。
    fn enc_trade(
        ts: u64,
        token: &str,
        wallet: &str,
        token_amount: u128,
        quote_amount: u128,
    ) -> Vec<u8> {
        enc_trade_fee(ts, token, wallet, token_amount, quote_amount, 0)
    }

    /// 同上，但可指定手续费。验证「方向是否被正确传递」时必须带非零 fee，
    /// fee=0 时买卖两个方向的公式退化成同一个，传错方向也测不出来。
    fn enc_trade_fee(
        ts: u64,
        token: &str,
        wallet: &str,
        token_amount: u128,
        quote_amount: u128,
        fee: u128,
    ) -> Vec<u8> {
        let mut d = Vec::new();
        d.extend(w256(ts as u128));
        d.extend(w_addr(token));
        d.extend(w_addr(wallet));
        d.extend(w256(token_amount));
        d.extend(w256(quote_amount));
        d.extend(w256(fee));
        d.extend(w256(0)); // price（本项目自行计算，不依赖此字段）
        d
    }

    /// 编码 FlapTokenTaxSet 的 data。
    fn enc_tax(token: &str, bps: u64) -> Vec<u8> {
        let mut d = w_addr(token);
        d.extend(w256(bps as u128));
        d
    }

    /// 编码 FlapTokenAsymmetricTaxSet 的 data。
    fn enc_asym_tax(token: &str, buy: u64, sell: u64) -> Vec<u8> {
        let mut d = w_addr(token);
        d.extend(w256(buy as u128));
        d.extend(w256(sell as u128));
        d
    }

    /// 编码 TokenQuoteSet(token, quote) 的 data。
    fn enc_quote(token: &str, quote: &str) -> Vec<u8> {
        let mut d = w_addr(token);
        d.extend(w_addr(quote));
        d
    }

    struct Harness {
        logs_tx: broadcast::Sender<RawLog>,
        cmd_tx: mpsc::UnboundedSender<Command>,
        event_rx: broadcast::Receiver<Event>,
        shared: Shared,
        task: tokio::task::JoinHandle<()>,
        log_seq: AtomicU64,
    }

    /// 启动一个真实引擎（离线 RPC 客户端，不产生任何网络请求）。
    fn harness() -> Harness {
        let (logs_tx, logs_rx) = broadcast::channel::<RawLog>(4096);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (event_tx, event_rx) = broadcast::channel::<Event>(4096);
        let (browser_tx, _) = broadcast::channel::<BrowserCommand>(64);
        let (status_tx, status_rx) = watch::channel("live".to_string());
        let shared = Shared::default();
        let engine = Engine::new(
            logs_rx,
            cmd_rx,
            cmd_tx.clone(),
            event_tx,
            browser_tx,
            status_rx,
            crate::rpc::offline_client(),
            shared.clone(),
            Store::memory().expect("test sqlite"),
        );
        let task = tokio::spawn(engine.run());
        let _ = status_tx; // 引擎已持有 receiver，sender 由 watch 内部管理
        Harness {
            logs_tx,
            cmd_tx,
            event_rx,
            shared,
            task,
            log_seq: AtomicU64::new(0),
        }
    }

    impl Harness {
        fn feed(&self, topic_sig: &str, data: Vec<u8>, tx: &str, block: u64) {
            // 离线测试用事件里的 wallet 模拟一笔直接交易的 tx.from。生产实时
            // 订阅没有这个字段，会走异步 RPC 补查。
            let tx_from = if topic_sig == config::SIG_TOKEN_BOUGHT {
                abi::decode_trade(&data).map(|tr| tr.wallet)
            } else {
                None
            };
            self.feed_with_from(topic_sig, data, tx, tx_from, block);
        }

        fn feed_with_from(
            &self,
            topic_sig: &str,
            data: Vec<u8>,
            tx: &str,
            tx_from: Option<String>,
            block: u64,
        ) {
            let log = RawLog {
                address: config::PORTAL.into(),
                tx_hash: tx.to_string(),
                tx_from,
                block_number: block,
                tx_index: 0,
                log_index: self.log_seq.fetch_add(1, Ordering::Relaxed),
                topic0: crate::abi::topic0(topic_sig),
                topics: vec![crate::abi::topic0(topic_sig)],
                data,
            };
            self.logs_tx.send(log).expect("事件通道应可用");
        }

        /// 等待引擎把已投递的事件处理完（biased select 保证日志优先）。
        async fn settle(&self) {
            for _ in 0..40 {
                tokio::time::sleep(Duration::from_millis(25)).await;
                if self.logs_tx.receiver_count() > 0 && self.logs_tx.len() == 0 {
                    // 再等一拍，确保事件处理后的 sync_shared 也完成。
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    return;
                }
            }
        }

        async fn stats(&self) -> Stats {
            *self.shared.stats.read().await
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// 三个相邻桶、每桶收高于开 => 必须晋升 B 币并出现在前端快照里。
    /// 这是核心需求的端到端验证：纯函数测试无法覆盖引擎接线。
    #[tokio::test]
    async fn engine_promotes_b_token_on_three_adjacent_green_candles() {
        let h = harness();
        // 1_800_000_000 可被 3 整除，便于推算桶边界。
        let t = 1_800_000_000u64;
        let tok = "0x1111111111111111111111111111111111117777";
        let one = 10u128.pow(18);

        // 建币 -> 税率（1%，A 币）-> 目标地址买入并由 7 笔成交构成三根相邻阳线。
        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "测试币", "TEST"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);
        let w = crate::targets::TARGET_BUYERS[0];
        // 桶0：开 1.0 -> 收 2.0（阳）
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, tok, w, one, one),
            "0xtx2",
            2,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 1, tok, w, one, 2 * one),
            "0xtx3",
            2,
        );
        // 桶1：开 2.0 -> 收 3.0（阳），且 t+3 会封口桶0
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 3, tok, w, one, 2 * one),
            "0xtx4",
            3,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 4, tok, w, one, 3 * one),
            "0xtx5",
            3,
        );
        // 桶2：开 3.0 -> 收 4.0（阳），t+6 封口桶1
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 6, tok, w, one, 3 * one),
            "0xtx6",
            4,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 7, tok, w, one, 4 * one),
            "0xtx7",
            4,
        );
        // t+9 封口桶2 -> 触发第三根阳线 -> 晋升
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 9, tok, w, one, 4 * one),
            "0xtx8",
            5,
        );
        h.settle().await;

        let snaps = h.shared.snapshots.read().await.clone();
        assert_eq!(snaps.len(), 1, "应恰好晋升 1 个 B 币");
        let s = &snaps[0];
        assert_eq!(s.token, tok);
        assert_eq!(s.name, "测试币");
        assert_eq!(s.symbol, "TEST");
        assert_eq!(s.buy_tax_bps, 100);
        assert_eq!(s.sell_tax_bps, 100);
        assert_eq!(s.promote_streak, 3, "晋升连阳数应固化为 3");
        assert!(s.streak >= 3, "实时连阳数 {}", s.streak);
        assert_eq!(s.quote_is_native, true, "未发 QuoteSet 应默认原生 BNB");
        assert_eq!(s.quote_symbol, "BNB");
        // 3 根 K 线应已封口且按时间升序、均为 3 秒整数倍。
        let times: Vec<u64> = s.candles.iter().map(|c| c.time).collect();
        assert_eq!(times, sorted_unique(&times), "K 线时间须升序唯一");
        assert!(
            times.iter().all(|x| x % config::CANDLE_PERIOD_SECS == 0),
            "须为 3 秒整数倍"
        );
        assert!(
            s.candles.iter().filter(|c| c.closed).count() >= 3,
            "至少 3 根已封口"
        );
        assert_eq!(s.trades.len(), 7, "7 笔成交都应记录");
        assert!(s.trades.iter().all(|tr| tr.side == "buy"));
        // 成交金额的数值形式必须下发，前端靠它换算美元。
        assert!(s.trades.iter().all(|tr| tr.quote_value > 0.0));

        // 市值口径：市值必须恒等于「单价 × 总供应量」，逐笔与快照都要成立。
        // 这两处一旦裂开，前端就会同时显示互相矛盾的单价与市值。
        let supply = config::TOTAL_SUPPLY_TOKENS;
        for tr in &s.trades {
            let expect = tr.price * supply;
            assert!(
                (tr.mcap_quote - expect).abs() / expect < 1e-12,
                "mcapQuote={} 应等于 price*supply={}",
                tr.mcap_quote,
                expect
            );
        }
        assert!(
            (s.mcap_quote - s.last_price * supply).abs() < 1e-6,
            "快照市值 {} 应等于最新单价 {} × 供应量",
            s.mcap_quote,
            s.last_price
        );
        assert_eq!(s.total_supply, supply);
        // 该币未发 QuoteSet，应默认原生 BNB 计价，前端才会做美元换算。
        assert!(s.quote_is_native);

        let st = h.stats().await;
        assert_eq!(st.created_seen, 1);
        assert_eq!(st.a_tokens, 1);
        assert_eq!(st.b_tokens, 1);
        assert_eq!(st.rejected_tax, 0);
        assert_eq!(st.expired, 0);
        assert_eq!(st.live_now, 1);
    }

    /// 回归：聚合器交易的事件 wallet 与交易发起人不同。目标名单必须匹配
    /// tx.from；这是 GMGN 展示买家、而旧实现漏掉真实目标买入的链上口径。
    #[tokio::test]
    async fn target_buyer_uses_transaction_sender_not_event_wallet() {
        let h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x1111111111111111111111111111111111111009";
        let event_wallet = "0x2222222222222222222222222222222222222222";
        let target_sender = crate::targets::TARGET_BUYERS[0].to_string();
        let one = 10u128.pow(18);

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "代理买入", "PX"),
            "0xc",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xc", 1);
        h.feed_with_from(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, tok, event_wallet, one, one),
            "0xb0",
            Some(target_sender),
            2,
        );
        for (i, (ts, q)) in [
            (t + 1, 2u128),
            (t + 3, 2),
            (t + 4, 3),
            (t + 6, 3),
            (t + 7, 4),
            (t + 9, 4),
        ]
        .into_iter()
        .enumerate()
        {
            h.feed_with_from(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(ts, tok, event_wallet, one, q * one),
                &format!("0xb{}", i + 1),
                Some(event_wallet.to_string()),
                3 + i as u64,
            );
        }
        h.settle().await;

        let snaps = h.shared.snapshots.read().await.clone();
        assert_eq!(snaps.len(), 1, "目标 tx.from 买入且三连阳时必须晋升");
        assert_eq!(snaps[0].token, tok);
    }

    /// 晋升条件的顺序无关：目标地址可以先买入，也可以在三连阳形成后买入；
    /// 只有三连阳但没有目标地址 BUY（即使目标地址 SELL）仍不得晋升。
    #[tokio::test]
    async fn engine_requires_target_buyer_and_allows_either_order() {
        let h = harness();
        let t = 1_800_000_000u64;
        let one = 10u128.pow(18);
        let target = crate::targets::TARGET_BUYERS[0];
        let other = "0x2222222222222222222222222222222222222222";
        let first = "0x1111111111111111111111111111111111111001";
        let late = "0x1111111111111111111111111111111111111002";
        let no_buy = "0x1111111111111111111111111111111111111003";

        for (i, tok) in [first, late, no_buy].iter().enumerate() {
            h.feed(
                config::SIG_TOKEN_CREATED,
                enc_created(t, tok, &format!("顺序{i}"), &format!("O{i}")),
                &format!("0xcreate{i}"),
                10 + i as u64,
            );
            h.feed(
                config::SIG_TAX_SET,
                enc_tax(tok, 100),
                &format!("0xcreate{i}"),
                10 + i as u64,
            );
        }

        // first：目标地址先买入，再形成三根相邻阳线。
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, first, target, one, one),
            "0xfirst1",
            20,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 1, first, other, one, 2 * one),
            "0xfirst2",
            20,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 3, first, other, one, 2 * one),
            "0xfirst3",
            21,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 4, first, other, one, 3 * one),
            "0xfirst4",
            21,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 6, first, other, one, 3 * one),
            "0xfirst5",
            22,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 7, first, other, one, 4 * one),
            "0xfirst6",
            22,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 9, first, other, one, 4 * one),
            "0xfirst7",
            23,
        );

        // late：先形成三根已封口阳线，再经过一根阴线，之后目标地址买入，
        // 仍应晋升；这验证两个条件是已发生事实，而不是要求当前连阳计数仍为 3。
        for (i, (ts, q)) in [
            (t, 1u128),
            (t + 1, 2),
            (t + 3, 2),
            (t + 4, 3),
            (t + 6, 3),
            (t + 7, 4),
            (t + 9, 4),
        ]
        .into_iter()
        .enumerate()
        {
            h.feed(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(ts, late, other, one, q * one),
                &format!("0xlate{i}"),
                30 + i as u64,
            );
        }
        h.feed(
            config::SIG_TOKEN_SOLD,
            enc_trade(t + 10, late, other, one, 3 * one),
            "0xlate-red",
            39,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 13, late, target, one, 5 * one),
            "0xlate-target",
            40,
        );

        // no_buy：三连阳之后只有目标地址卖出，不能满足「目标地址买入」。
        for (i, (ts, q)) in [
            (t, 1u128),
            (t + 1, 2),
            (t + 3, 2),
            (t + 4, 3),
            (t + 6, 3),
            (t + 7, 4),
            (t + 9, 4),
        ]
        .into_iter()
        .enumerate()
        {
            h.feed(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(ts, no_buy, other, one, q * one),
                &format!("0xnone{i}"),
                50 + i as u64,
            );
        }
        h.feed(
            config::SIG_TOKEN_SOLD,
            enc_trade(t + 10, no_buy, target, one, 5 * one),
            "0xnone-target-sell",
            60,
        );

        h.settle().await;

        let snaps = h.shared.snapshots.read().await.clone();
        let promoted: std::collections::HashSet<_> =
            snaps.iter().map(|s| s.token.as_str()).collect();
        assert_eq!(snaps.len(), 2, "目标地址先买和后三连阳两种顺序都应晋升");
        assert!(promoted.contains(first));
        assert!(promoted.contains(late));
        assert!(!promoted.contains(no_buy), "目标地址只有卖出时不得晋升");
        assert_eq!(h.stats().await.b_tokens, 2);
    }

    /// 回归：单价与市值必须按买卖方向取「有效成交额」（BUY 扣费、SELL 加费）。
    ///
    /// 方向参数漏传或传反时，价格与市值整体偏移约 2%（费率的 2 倍），
    /// 而 K 线形态、阳线判定、晋升流程全部照常工作——肉眼和既有测试都发现不了。
    /// 因此必须用非零 fee 的买卖各一笔做对照。
    #[tokio::test]
    async fn engine_prices_trades_by_side_using_effective_quote() {
        let mut h = harness();
        let t = 1_800_000_000u64;
        let buy_tok = "0x7777777777777777777777777777777777777777";
        let sell_tok = "0x8888888888888888888888888888888888887777";
        let w = "0x2222222222222222222222222222222222222222";
        let one = 10u128.pow(18);
        // Amount = 1 枚，Q = 0.1，Fee = 0.001（实测 Flap 费率为 Q 的 1%）。
        let (amt, q, fee) = (one, one / 10, one / 1000);

        for (tok, name) in [(buy_tok, "买"), (sell_tok, "卖")] {
            h.feed(
                config::SIG_TOKEN_CREATED,
                enc_created(t, tok, name, "T"),
                "0xtx1",
                1,
            );
            h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);
        }
        // 两个币各一笔成交，数据完全相同，只有方向不同。
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade_fee(t + 1, buy_tok, w, amt, q, fee),
            "0xtx2",
            2,
        );
        h.feed(
            config::SIG_TOKEN_SOLD,
            enc_trade_fee(t + 1, sell_tok, w, amt, q, fee),
            "0xtx3",
            2,
        );
        h.settle().await;

        // 观察窗内的 A 币同样会推 Trade 事件，无需先晋升为 B 币。
        let (mut buy, mut sell) = (None, None);
        while let Ok(ev) = h.event_rx.try_recv() {
            if let Event::Trade { token, trade } = ev {
                if token == buy_tok {
                    buy = Some(trade);
                } else if token == sell_tok {
                    sell = Some(trade);
                }
            }
        }
        let buy = buy.expect("BUY 成交应被记录");
        let sell = sell.expect("SELL 成交应被记录");

        let supply = config::TOTAL_SUPPLY_TOKENS;
        // BUY：有效成交额 0.1 - 0.001 = 0.099，单价 0.099，市值 0.099 × 10 亿。
        assert!((buy.price - 0.099).abs() < 1e-12, "buy price={}", buy.price);
        assert!(
            (buy.mcap_quote - 0.099 * supply).abs() / (0.099 * supply) < 1e-12,
            "buy mcap={}",
            buy.mcap_quote
        );
        // SELL：有效成交额 0.1 + 0.001 = 0.101。
        assert!(
            (sell.price - 0.101).abs() < 1e-12,
            "sell price={}",
            sell.price
        );
        assert!(
            (sell.mcap_quote - 0.101 * supply).abs() / (0.101 * supply) < 1e-12,
            "sell mcap={}",
            sell.mcap_quote
        );
        // 方向不得搞反：同样的 Q/Fee/Amount，卖出单价必须高于买入。
        assert!(sell.price > buy.price, "SELL 加费、BUY 扣费，方向不能反");
        // 不含费口径是 0.1，必须与两个方向都可区分，否则说明 Fee 根本没被用上。
        assert!((buy.price - 0.1).abs() > 1e-6, "BUY 单价必须偏离不含费口径");
        assert!(
            (sell.price - 0.1).abs() > 1e-6,
            "SELL 单价必须偏离不含费口径"
        );
        // 成交金额仍是链上的 Q 本身（买方付出/卖方到手的钱），不做扣费调整。
        assert!(
            (buy.quote_value - 0.1).abs() < 1e-12,
            "quoteValue={}",
            buy.quote_value
        );
        assert_eq!(buy.quote_amount, "0.1");
        assert_eq!(buy.token_amount, "1");
    }

    /// 回归：统计必须自洽，不得出现 createdSeen < aTokens + rejectedTax 的双重计数。
    ///
    /// 这是实测发现过的真实缺陷：建币事件先于税率事件到达，旧实现建币时就计入
    /// A 币，税率超标后又计入剔除，同一个币被算了两次（曾出现 97+16>97）。
    #[tokio::test]
    async fn engine_does_not_double_count_rejected_tokens() {
        let h = harness();
        let t = 1_800_000_000u64;
        // 4 个币：2 个低税（A 币），2 个高税（剔除）。全部先发建币、再发税率，
        // 复现链上真实顺序 Created → TaxSet。
        let cases = [
            (
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa7777",
                100u64,
                0u64,
                true,
            ),
            ("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb7777", 400, 400, true),
            ("0xcccccccccccccccccccccccccccccccccccc7777", 401, 0, false),
            (
                "0xdddddddddddddddddddddddddddddddddddd7777",
                300,
                500,
                false,
            ),
        ];
        for (i, (tok, buy, sell, _)) in cases.iter().enumerate() {
            h.feed(
                config::SIG_TOKEN_CREATED,
                enc_created(t, tok, &format!("币{i}"), &format!("T{i}")),
                &format!("0xtx{i}"),
                10 + i as u64,
            );
            h.feed(
                config::SIG_ASYM_TAX_SET,
                enc_asym_tax(tok, *buy, *sell),
                &format!("0xtx{i}"),
                10 + i as u64,
            );
        }
        h.settle().await;

        let st = h.stats().await;
        assert_eq!(st.created_seen, 4);
        assert_eq!(st.a_tokens, 2, "400bps 恰好通过，401/500 剔除");
        assert_eq!(st.rejected_tax, 2);
        // 核心不变量：每个建币只被计入一次。
        assert_eq!(
            st.a_tokens + st.rejected_tax,
            st.created_seen,
            "A币 + 税率剔除 必须等于建币数（不得双重计数）"
        );
        assert_eq!(st.blacklisted, 2, "两个高税币应进入永久黑名单");
        assert_eq!(st.b_tokens, 0, "无成交，不应有 B 币");
        assert!(h.shared.snapshots.read().await.is_empty());
    }

    /// 回归：观察窗内出现阴线则连阳中断，不得晋升。
    #[tokio::test]
    async fn engine_does_not_promote_when_red_candle_breaks_streak() {
        let mut h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x3333333333333333333333333333333333337777";
        let one = 10u128.pow(18);
        let w = "0x2222222222222222222222222222222222222222";

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "阴", "Y"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);
        // 桶0 阳（1->2）
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, tok, w, one, one),
            "0xtx2",
            2,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 1, tok, w, one, 2 * one),
            "0xtx3",
            2,
        );
        // 桶1 阴（3->2），封口桶0
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 3, tok, w, one, 3 * one),
            "0xtx4",
            3,
        );
        h.feed(
            config::SIG_TOKEN_SOLD,
            enc_trade(t + 4, tok, w, one, 2 * one),
            "0xtx5",
            3,
        );
        // 桶2 阳（2->3），封口桶1 -> 连阳应重新从 1 开始
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 6, tok, w, one, 2 * one),
            "0xtx6",
            4,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 7, tok, w, one, 3 * one),
            "0xtx7",
            4,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 9, tok, w, one, 3 * one),
            "0xtx8",
            5,
        );
        h.settle().await;

        assert!(
            h.shared.snapshots.read().await.is_empty(),
            "出现阴线后不得晋升"
        );
        let st = h.stats().await;
        assert_eq!(st.b_tokens, 0);
        assert_eq!(st.a_tokens, 1);
        // 买卖两个方向都应被记录。
        let mut has_sell = false;
        while let Ok(ev) = h.event_rx.try_recv() {
            if let Event::Trade { trade, .. } = ev {
                if trade.side == "sell" {
                    has_sell = true;
                }
            }
        }
        assert!(has_sell, "TokenSold 事件应被解析为 sell 方向");
    }

    /// 回归：观察窗结束未达标 -> 永久剔除，且此后成交不再被处理。
    ///
    /// 到期清理使用墙上 deadline，避免回填过程中的较新区块时间戳把
    /// 刚恢复、仍在处理历史日志的 Token 提前清理。
    #[tokio::test]
    async fn engine_permanently_blacklists_expired_token() {
        let mut engine = bare_engine();
        let tok = "0x4444444444444444444444444444444444447777";
        let mut st = state(1_800_000_000);
        st.token = tok.into();
        st.target_buyer_seen = false;
        st.deadline_wall = Instant::now() - Duration::from_secs(1);
        engine.tokens.insert(tok.into(), st);

        engine.on_tick().await;

        assert_eq!(engine.stats_expired, 1, "应有一个币到期剔除");
        assert!(engine.blacklist.contains(tok), "到期币应进入永久黑名单");
        assert_eq!(engine.tokens[tok].stage, Stage::Dead);

        // 永不再理：后续成交在入口即被 tombstone 拦截。
        engine
            .on_trade(
                Trade {
                    ts: 1_800_000_100,
                    token: tok.into(),
                    wallet: crate::targets::TARGET_BUYERS[0].into(),
                    token_amount: 10u128.pow(18),
                    quote_amount: 10u128.pow(18),
                    fee: 0,
                },
                true,
                "0xtx-after-dead".into(),
                Some(crate::targets::TARGET_BUYERS[0].into()),
                2,
                false,
            )
            .await;
        assert_eq!(engine.tokens[tok].stage, Stage::Dead);
        assert!(engine.tokens[tok].trades.is_empty());
        assert_eq!(engine.stats_b, 0, "永久剔除的币不得复活");
    }

    /// 回归：用户点 X 后永久剔除，同地址不得复活。
    #[tokio::test]
    async fn engine_blacklists_token_on_user_command() {
        let h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x5555555555555555555555555555555555557777";
        let one = 10u128.pow(18);
        let w = crate::targets::TARGET_BUYERS[0];

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "手剔", "X"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);
        // 每笔价格严格递增，使每个 3 秒桶内「开 < 收」，构成阳线。
        // 注意不能用 (1 + i/2)：那样同桶内首尾价格相同，是十字星而非阳线。
        for (i, ts) in [t, t + 1, t + 3, t + 4, t + 6, t + 7, t + 9]
            .iter()
            .enumerate()
        {
            let q = (1 + i) as u128 * one;
            h.feed(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(*ts, tok, w, one, q),
                "0xtx",
                2,
            );
        }
        h.settle().await;
        assert_eq!(h.shared.snapshots.read().await.len(), 1, "应先晋升为 B 币");

        // 用户点 X。
        h.cmd_tx.send(Command::Blacklist(tok.to_string())).unwrap();
        h.settle().await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(
            h.shared.snapshots.read().await.is_empty(),
            "点 X 后应从前端消失"
        );
        // b_tokens 是累计计数：该币此前已合法晋升过一次，值为 1。
        // 复活校验的正确不变量是「不再产生第二次晋升」，即计数保持不变。
        let b_before = h.stats().await.b_tokens;
        assert_eq!(b_before, 1, "点 X 前应已晋升过一次");
        assert_eq!(h.stats().await.blacklisted, 1);

        // 再给三连阳，不得复活。这里的价格序列必须真能构成三根阳线，
        // 否则测试会因为「压根没有阳线」而通过，验证不到黑名单的作用。
        let t2 = t + 300;
        for (i, ts) in [t2, t2 + 1, t2 + 3, t2 + 4, t2 + 6, t2 + 7, t2 + 9]
            .iter()
            .enumerate()
        {
            let q = (1 + i) as u128 * one;
            h.feed(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(*ts, tok, w, one, q),
                "0xtx",
                9,
            );
        }
        h.settle().await;
        assert!(
            h.shared.snapshots.read().await.is_empty(),
            "已手动剔除的币不得复活"
        );
        assert_eq!(
            h.stats().await.b_tokens,
            b_before,
            "复活会产生第二次 B 币晋升计数"
        );
    }

    /// 无成交的币：窗口内一根 K 线都没有，不得晋升（对应「3s 内没有 K 线就不画」）。
    #[tokio::test]
    async fn engine_leaves_token_without_trades_unpromoted() {
        let h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x6666666666666666666666666666666666667777";
        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "无成交", "N"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);
        h.settle().await;

        assert!(h.shared.snapshots.read().await.is_empty());
        let st = h.stats().await;
        assert_eq!(st.a_tokens, 1, "无成交的低税币仍是 A 币");
        assert_eq!(st.b_tokens, 0);
    }

    /// 取每个桶的最终 K 线。
    ///
    /// 同一桶会收到多条事件（每次聚合一条、封口时再一条），封口事件在后，
    /// 故按桶号覆盖后取到的就是终值。
    fn final_candles(h: &mut Harness, token: &str) -> std::collections::BTreeMap<u64, Candle> {
        let mut m = std::collections::BTreeMap::new();
        while let Ok(ev) = h.event_rx.try_recv() {
            if let Event::Candle { token: tk, candle } = ev {
                if tk == token {
                    m.insert(candle.time / config::CANDLE_PERIOD_SECS, candle);
                }
            }
        }
        m
    }

    /// 本项目的 K 线口径与常见交易所不同，刻意让 K 线视觉连续：
    /// 上一笔成交结束时市值 $5.00K，桶内三笔 $5.20K / $5.80K / $5.40K，
    /// 必须得到 O=5.00 H=5.80 L=5.00 C=5.40 —— 而不是 O=5.20。
    ///
    /// 现有晋升测试无法覆盖这一点：它每桶首笔价恰好等于上桶末笔价，
    /// 两种口径结果相同。这里特意让两者分叉。
    #[tokio::test]
    async fn candle_open_inherits_previous_trade_not_bucket_first_trade() {
        let mut h = harness();
        let t = 1_800_000_000u64; // 可被 3 整除，便于推算桶边界
        let tok = "0x1111111111111111111111111111111111110001";
        let one = 10u128.pow(18);
        let w = "0x2222222222222222222222222222222222222222";

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "口径币", "KJ"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);

        // 上一笔成交：5.00（桶0，也是首根 K）
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, tok, w, one, 5 * one),
            "0xtx2",
            2,
        );
        // 桶1 内三笔：5.20 / 5.80 / 5.40
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 3, tok, w, one, 52 * one / 10),
            "0xtx3",
            3,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 4, tok, w, one, 58 * one / 10),
            "0xtx4",
            3,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 5, tok, w, one, 54 * one / 10),
            "0xtx5",
            3,
        );
        // t+6 的成交封口桶1（同时开桶2，不影响本断言）
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 6, tok, w, one, 54 * one / 10),
            "0xtx6",
            4,
        );
        h.settle().await;

        let cs = final_candles(&mut h, tok);
        let b0 = t / config::CANDLE_PERIOD_SECS;

        // 需求 1：首根 K 的 Open = 第一笔成交对应市值
        let c0 = cs.get(&b0).expect("桶0 应有 K 线");
        assert!(
            (c0.open - 5.0).abs() < 1e-12,
            "首根 K 的 Open 应等于第一笔成交价 5.0，实际 {}",
            c0.open
        );

        // 需求 2 + 4：Open 继承上一笔状态，High/Low 把 Open 算进去
        let c1 = cs.get(&(b0 + 1)).expect("桶1 应有 K 线");
        assert!(c1.closed, "桶1 应已封口，否则 Close 还不是终值");
        assert!(
            (c1.open - 5.0).abs() < 1e-12,
            "Open 须继承上一笔成交状态 5.0，而非桶内首笔 5.2；实际 {}",
            c1.open
        );
        assert!(
            (c1.high - 5.8).abs() < 1e-12,
            "High 须为 max(Open, 桶内成交) = 5.8；实际 {}",
            c1.high
        );
        assert!(
            (c1.low - 5.0).abs() < 1e-12,
            "Low 须为 min(Open, 桶内成交) = 5.0（来自 Open，不是桶内最低 5.2）；实际 {}",
            c1.low
        );
        assert!(
            (c1.close - 5.4).abs() < 1e-12,
            "Close 须为桶内最后一笔 5.4；实际 {}",
            c1.close
        );
        assert_eq!(c1.trades, 3, "桶1 应记录 3 笔成交");
    }

    /// 需求 3 的后半句：空桶不画 K，但价格状态必须保留，
    /// 下一笔成交出现时新 K 的 Open 仍继承上一笔成交状态。
    #[tokio::test]
    async fn empty_buckets_draw_nothing_but_price_state_persists() {
        let mut h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x1111111111111111111111111111111111110002";
        let one = 10u128.pow(18);
        let w = "0x2222222222222222222222222222222222222222";

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "空桶币", "KT"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);

        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, tok, w, one, 3 * one),
            "0xtx2",
            2,
        );
        // 跳过桶1、桶2（连续 6 秒无任何成交），直接在桶3 成交。
        // 桶3 只此一笔，close 即终值，无需再喂成交去封口（否则会多出一根桶4 的 K）。
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 9, tok, w, one, 4 * one),
            "0xtx3",
            3,
        );
        h.settle().await;

        let cs = final_candles(&mut h, tok);
        let b0 = t / config::CANDLE_PERIOD_SECS;
        assert!(cs.get(&(b0 + 1)).is_none(), "空桶不得生成 K 线");
        assert!(cs.get(&(b0 + 2)).is_none(), "空桶不得生成 K 线");
        assert_eq!(cs.len(), 2, "只应有桶0 与桶3 两根 K，实际 {}", cs.len());

        let c3 = cs.get(&(b0 + 3)).expect("桶3 应有 K 线");
        assert!(
            (c3.open - 3.0).abs() < 1e-12,
            "跨越两个空桶后 Open 仍须继承上一笔状态 3.0，实际 {}",
            c3.open
        );
        assert!((c3.close - 4.0).abs() < 1e-12, "Close 应为本桶最后一笔 4.0");
    }

    /// High/Low 含 Open 的另一半验证：桶内一路下跌时，
    /// High 必须是 Open（5.0），而不是桶内最高成交价（4.0）。
    #[tokio::test]
    async fn candle_high_includes_open_when_bucket_only_declines() {
        let mut h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x1111111111111111111111111111111111110003";
        let one = 10u128.pow(18);
        let w = "0x2222222222222222222222222222222222222222";

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "下跌币", "XD"),
            "0xtx1",
            1,
        );
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);

        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t, tok, w, one, 5 * one),
            "0xtx2",
            2,
        );
        // 桶1 内两笔：4.0 / 3.0（都低于上一笔状态 5.0）
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 3, tok, w, one, 4 * one),
            "0xtx3",
            3,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 4, tok, w, one, 3 * one),
            "0xtx4",
            3,
        );
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 6, tok, w, one, 3 * one),
            "0xtx5",
            4,
        );
        h.settle().await;

        let cs = final_candles(&mut h, tok);
        let b0 = t / config::CANDLE_PERIOD_SECS;
        let c1 = cs.get(&(b0 + 1)).expect("桶1 应有 K 线");
        assert!(c1.closed, "桶1 应已封口");
        assert!(
            (c1.open - 5.0).abs() < 1e-12,
            "Open 应继承 5.0，实际 {}",
            c1.open
        );
        assert!(
            (c1.high - 5.0).abs() < 1e-12,
            "High 须含 Open，应为 5.0 而非桶内最高成交 4.0；实际 {}",
            c1.high
        );
        assert!(
            (c1.low - 3.0).abs() < 1e-12,
            "Low 应为桶内最低 3.0，实际 {}",
            c1.low
        );
        assert!(
            (c1.close - 3.0).abs() < 1e-12,
            "Close 应为最后一笔 3.0，实际 {}",
            c1.close
        );
        // 收盘低于开盘 => 阴线，不得计入连阳。
        assert!(c1.close < c1.open, "该桶应为阴线");
    }

    /// 回归：建币交易内 `Created → QuoteSet(原生BNB) → TaxSet` 到达后，
    /// 原生计价币的 quote_symbol 必须是 "BNB"，不得是「未知」。
    ///
    /// 曾经的 bug：QuoteSet 分支把符号清成 None，再调 ensure_quote_symbol，
    /// 而后者对零地址只写缓存、不回填符号；tick 重试又把零地址过滤掉，
    /// 于是原生 BNB 计价的币永久显示「计价未知」。
    #[tokio::test]
    async fn native_bnb_quote_keeps_symbol_after_quote_set() {
        let h = harness();
        let t = 1_800_000_000u64;
        let tok = "0x1111111111111111111111111111111111110004";
        let zero = config::ZERO_ADDRESS;

        h.feed(
            config::SIG_TOKEN_CREATED,
            enc_created(t, tok, "原生币", "YS"),
            "0xtx1",
            1,
        );
        // 建币交易内紧随其后声明原生 BNB 计价（零地址）
        h.feed(config::SIG_QUOTE_SET, enc_quote(tok, zero), "0xtx1", 1);
        h.feed(config::SIG_TAX_SET, enc_tax(tok, 100), "0xtx1", 1);
        h.settle().await;

        // 快照只包含已晋升的 B 币，故先构造三连阳让该币进入快照，再断言符号。
        let one = 10u128.pow(18);
        let w = crate::targets::TARGET_BUYERS[0];
        // 桶0 开1收2 / 桶1 开2收3 / 桶2 开3收4，t+9 封口桶2 触发晋升
        for (i, (a, b)) in [(0u64, (1u128, 2u128)), (3, (2, 3)), (6, (3, 4))] {
            h.feed(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(t + i, tok, w, one, a * one),
                "0xa",
                2,
            );
            h.feed(
                config::SIG_TOKEN_BOUGHT,
                enc_trade(t + i + 1, tok, w, one, b * one),
                "0xb",
                2,
            );
        }
        h.feed(
            config::SIG_TOKEN_BOUGHT,
            enc_trade(t + 9, tok, w, one, 4 * one),
            "0xc",
            3,
        );
        h.settle().await;

        let snaps = h.shared.snapshots.read().await.clone();
        let s = snaps
            .iter()
            .find(|s| s.token == tok)
            .expect("应晋升为 B 币");
        assert!(s.quote_is_native, "零地址计价应判定为原生 BNB");
        assert_eq!(
            s.quote_symbol, "BNB",
            "原生 BNB 计价的符号必须是 BNB，不得是「未知」"
        );
    }

    fn sorted_unique(v: &[u64]) -> Vec<u64> {
        let mut s = v.to_vec();
        s.sort_unstable();
        s.dedup();
        s
    }
}
