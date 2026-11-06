//! 全局配置与已在 BSC 主网实测验证的链上常量。
//!
//! 这里的事件签名、合约地址、字段含义都经过真实链上数据交叉验证，
//! 不是猜测值。修改前请先阅读 README 的「链上事实核验」一节。

use std::time::Duration;

// ---------------------------------------------------------------- 节点

/// BSC 公共 WebSocket 节点候选列表。
///
/// 单节点会随网络波动断连（实测 publicnode 出现过握手超时导致整条监测停摆），
/// 因此重连时按顺序尝试多个端点，并优先沿用上一次成功的端点。
/// 已探测（2026-09-12）：publicnode 两个域名可用；drpc/onfinality 握手成功但
/// 首条响应异常，仅作末位兜底。
pub const WSS_ENDPOINTS: &[&str] = &[
    "wss://bsc-rpc.publicnode.com",
    "wss://bsc.publicnode.com",
    "wss://bsc-rpc.publicnode.com:443",
];

/// 交易发起人查询专用 HTTP RPC。与链上日志订阅 WSS 完全隔离，
/// 避免热门时段的订阅消息与大量只读请求互相阻塞。请求会按起点
/// 轮换并在单节点失败时自动切换。
///
/// 实测（2026-09-27）定位到的根因是**域名过时**：原先配置的
/// `bsc-dataseed.binance.org` 与 `bsc-dataseed1.binance.org` 已停用，
/// TCP 连接全部超时，而 binance 官方域名早已迁移到 `bnbchain.org`。
/// 批量查询又固定从 index 0 串行尝试，于是每批都先耗满 `connect_timeout`
/// 2 秒才回落到 publicnode，把 `tx.from` 查询的 p50 抬到 2399ms
/// （27214 样本，59.9% 落在 1500-2500ms，几乎不存在低于 1500ms 的样本）。
///
/// 下表按实测批量 p50 升序排列（64 项 `eth_getTransactionByHash`，5 轮，
/// 解析率均 100%）：
/// - bsc-dataseed.bnbchain.org   256ms
/// - bsc-dataseed2.bnbchain.org  264ms
/// - bsc-dataseed3.bnbchain.org  350ms
/// - bsc-rpc.publicnode.com      578ms
/// - bsc-mainnet.public.blastapi.io 628ms
///
/// 刻意跨三个服务商（bnbchain / publicnode / blastapi）以保证异构：同一家
/// 整体故障时仍有其他家兜底。同时避免修好域名后只剩单一健康端点 ——
/// 那会让 publicnode 一旦故障就完全无法补查 `tx.from`，目标地址买入漏判、
/// B 币晋升直接失效（历史日志中已累计 2490 笔 unresolved）。
///
/// 端点仍可能随时间失效，因此由 [`crate::rpc::EndpointHealth`] 按连续失败
/// 次数把端点暂时移出候选，冷却期满再放探测流量复活它，无需人工干预。
///
/// 实测不可用、不要加回来：`1rpc.io/bnb`、`rpc.ankr.com/bsc`（返回 HTTP 200
/// 但非预期响应）、`bsc.drpc.org`（429 限流）、`bsc.meowrpc.com`（不支持批量）、
/// `api.zan.top`（429 限流，批量仅 3/5 成功）、`bsc-dataseed1.defibit.io`、
/// `bsc-dataseed1.ninicoin.io`、`bsc.blockraz.io`、`binance.llamarpc.com`、
/// `endpoints.omniatech.io`（连接失败或超时）。
pub const HTTP_RPC_ENDPOINTS: &[&str] = &[
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed2.bnbchain.org",
    "https://bsc-rpc.publicnode.com",
    "https://bsc-dataseed3.bnbchain.org",
    "https://bsc-mainnet.public.blastapi.io",
];

/// 连续失败达到该次数即进入冷却，避免单次网络抖动误杀健康端点。
pub const HTTP_RPC_FAILURE_THRESHOLD: u32 = 2;

/// 冷却时长。到期后重新纳入候选并放行探测流量；仍失败则再次冷却。
pub const HTTP_RPC_COOLDOWN: Duration = Duration::from_secs(30);

/// 冷却时长上限。
///
/// 反复失败的端点按 [`HTTP_RPC_COOLDOWN`] 指数退避（30s → 60s → 120s → …），
/// 到该上限后不再增长。退避是必需的：若冷却时长固定，低流量时段每批请求都
/// 恰好落在「冷却刚到期」上，死端点会被反复探测，优化收益归零。
/// 上限 5 分钟保证端点真恢复后仍能在可接受时间内被重新发现。
pub const HTTP_RPC_MAX_COOLDOWN: Duration = Duration::from_secs(300);

// ------------------------------------------- Flap.sh 合约（BNB Chain 主网）

/// Portal：联合曲线交易与建币入口，本项目关心的事件全部由它发出。
pub const PORTAL: &str = "0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0";

/// Four.meme TokenManager2 (V2)：2024-09-05 之后的建币与联合曲线成交入口。
pub const FOUR_MEME_TOKEN_MANAGER2: &str = "0x5c952063c7fc8610ffdb798152d69f0b9550762b";
pub const FOUR_MEME_HELPER3: &str = "0xf251f83e40a78868fcfa3fa4599dad6494e46034";

/// GeniusFun 主网 LaunchFactory。每次发射同时给出 token 与其独立 curve 地址；
/// 后续 CurveBuy/CurveSell 由 curve 合约发出。
pub const GENIUS_FUN_FACTORY: &str = "0x78eae9537c0ef90dfe9b7ae964682fe8138afe31";
// TaxTokenHelper（0x53841c73217735F37BC1775538b03b23feFD8346）的
// getTaxTokenInfoV2(address) 也能读税率，返回值前 6 个字段依次为
// marketBps, deflationBps, lpBps, dividendBps, buyTaxRate, sellTaxRate（均 bps）。
// 本项目不用它：实测 30/30 个建币交易都在同笔交易内发出税率事件，
// 官方文档亦说明「缺失即非税币（0 税）」，且在事件循环里 await RPC 会拖垮吞吐。
/// Chainlink BNB/USD 喂价（8 位精度），把 BNB 计价金额换算成美元。
pub const BNB_USD_FEED: &str = "0x0567f2323251f0aab15c8dfb1967e4e8a7d42aee";

// ------------------------------------------- 事件签名（实测匹配链上 topic0）

/// 建币事件。每次发射必然发出，是所有监测的起点。
pub const SIG_TOKEN_CREATED: &str =
    "TokenCreated(uint256,address,uint256,address,string,string,string)";
/// 联合曲线买入。字段：ts, token, wallet, tokenAmount, quoteAmount, fee, price。
pub const SIG_TOKEN_BOUGHT: &str =
    "TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)";
/// 联合曲线卖出。字段布局与 TokenBought 完全一致。
pub const SIG_TOKEN_SOLD: &str =
    "TokenSold(uint256,address,address,uint256,uint256,uint256,uint256)";
/// 税率事件，携带 max(buyTax, sellTax)，单位 bps。
pub const SIG_TAX_SET: &str = "FlapTokenTaxSet(address,uint256)";
/// Tax Token V3 的非对称税率事件，携带精确的买卖税率，优先采用。
pub const SIG_ASYM_TAX_SET: &str = "FlapTokenAsymmetricTaxSet(address,uint256,uint256)";
/// 报价代币设置事件。缺失时默认原生 BNB（零地址）。
pub const SIG_QUOTE_SET: &str = "TokenQuoteSet(address,address)";

// ------------------------------------------- Four.meme V2 事件
pub const SIG_FOUR_TOKEN_CREATE: &str =
    "TokenCreate(address,address,uint256,string,string,uint256,uint256,uint256)";
pub const SIG_FOUR_TOKEN_PURCHASE: &str =
    "TokenPurchase(address,address,uint256,uint256,uint256,uint256,uint256,uint256)";
pub const SIG_FOUR_TOKEN_SALE: &str =
    "TokenSale(address,address,uint256,uint256,uint256,uint256,uint256,uint256)";

// ------------------------------------------- GeniusFun V3 事件
pub const SIG_GENIUS_TOKEN_LAUNCHED: &str =
    "TokenLaunched(address,address,address,address,uint256,uint256)";
pub const SIG_GENIUS_CURVE_BUY: &str = "CurveBuy(address,address,uint256,uint256,uint256,uint256)";
pub const SIG_GENIUS_CURVE_SELL: &str =
    "CurveSell(address,address,uint256,uint256,uint256,uint256)";

// ---------------------------------------------------------------- 代币供给

/// Flap 代币的总供应量（18 位精度原始值）：10 亿枚。
///
/// 市值 = 单价 × 总供应量，这个值错则全站市值都错，故必须实测而非照抄文档。
/// 核验（2026-09-12，BSC 主网）：对 12 个不同时段建币的 Flap 代币调用
/// `totalSupply()`，成功读取的 11 个返回值全部恒为 `0x33b2e3c9fd0803ce8000000`
/// = 1e27（1 个因节点瞬时原因读取失败，不影响结论）；
/// 样本同时覆盖原生 BNB 计价与非 BNB 计价的币，供给量与计价方式无关。
#[cfg_attr(not(test), allow(dead_code))]
pub const TOTAL_SUPPLY: u128 = 1_000_000_000 * 10u128.pow(18);

/// 总供应量的人类可读值（枚）。给前端展示与市值换算用，与 `TOTAL_SUPPLY` 同源。
pub const TOTAL_SUPPLY_TOKENS: f64 = 1_000_000_000.0;

// ---------------------------------------------------------------- 筛选与判定

/// A 币阈值：买税与卖税都必须 <= 4%（400 bps）。
pub const MAX_TAX_BPS: u64 = 400;
/// 每个 A 币的观察窗口。窗口结束仍未同时满足目标地址买入与三连阳即永久剔除。
pub const WATCH_WINDOW: Duration = Duration::from_secs(60);
/// K 线周期：3 秒一根。窗口内无成交则不生成 K 线。
pub const CANDLE_PERIOD_SECS: u64 = 3;
/// B 币触发条件：连续阳线根数。
pub const BULL_STREAK: usize = 3;
/// C 币重新观察窗口。
pub const C_WATCH_WINDOW: Duration = Duration::from_secs(240);
/// B 币每个市值考核周期。
pub const B_CYCLE: Duration = Duration::from_secs(600);
/// B 币每周期至少需要曾达到的美元市值。
pub const B_CYCLE_MIN_USD: f64 = 10_000.0;
/// 四个等价 GMGN Slot 的固定数量。
pub const SLOT_COUNT: usize = 4;
/// OPEN 发出后等待浏览器确认实际 URL 的最长时间。
pub const OPEN_ACK_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------- 服务

/// 前端与 API 监听地址。
pub const LISTEN_ADDR: &str = "127.0.0.1:8787";

/// Keep the fixed port because the extension/control Origin checks use 8787.
pub fn listen_addr() -> Result<std::net::SocketAddr, String> {
    let value = std::env::var("MEME_QUANT_LISTEN_ADDR").unwrap_or_else(|_| LISTEN_ADDR.into());
    let addr: std::net::SocketAddr = value.parse().map_err(|_| "监听地址无效".to_string())?;
    if addr.port() != 8787 {
        return Err("插件固定使用 8787 端口".into());
    }
    Ok(addr)
}

/// Portable runtime paths: no developer-machine compile-time absolute path.
pub fn resource_root() -> std::path::PathBuf {
    std::env::var_os("MEME_QUANT_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("."))
}

pub fn targets_path() -> std::path::PathBuf {
    std::env::var_os("MEME_QUANT_TARGETS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| resource_root().join("config/targets.local.json"))
}
/// BNB/USD 价格刷新间隔。
pub const PRICE_REFRESH: Duration = Duration::from_secs(30);
/// 每个币最多保留的成交明细条数，防止长时间运行内存无限增长。
pub const MAX_TRADES_PER_TOKEN: usize = 3000;
/// 每个币最多保留的 K 线根数。
pub const MAX_CANDLES_PER_TOKEN: usize = 2000;
/// 引擎 tick 间隔：负责闭合超时 K 线、清理过期观察窗口。
pub const TICK_INTERVAL: Duration = Duration::from_millis(400);

/// 零地址，Flap 用它表示原生 BNB 计价。
pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// 启动时可选回填的区块数。BSC 约 0.4 秒一个块，150 块约 1 分钟。
/// 默认 0 表示严格只监测启动之后新建的币；设为非零可立即看到效果。
pub fn seed_blocks() -> u64 {
    std::env::var("SEED_BLOCKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// SQLite 文件位置。可用 `MEME_QUANT_DB` 覆盖。
pub fn database_path() -> std::path::PathBuf {
    std::env::var_os("MEME_QUANT_DB")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| resource_root().join("data/meme-quant.sqlite3"))
}
