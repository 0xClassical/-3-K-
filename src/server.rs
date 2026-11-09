//! HTTP 服务：静态前端 + 只读状态 API + SSE 实时事件流。
//!
//! SSE 而非 WebSocket：前端只需要「服务端 -> 浏览器」的单向推送，
//! 用户点 X 这类指令走一次普通 POST 即可，无需维持双向长连接。

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, DefaultBodyLimit, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use futures_util::{SinkExt, StreamExt as _};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

use crate::engine::{
    BrowserPhase, BrowserSlotReport, BrowserTiming, Event, Handle, Shared, SlotSnapshot,
    TokenSnapshot,
};

#[derive(Clone)]
pub struct AppState {
    pub handle: Handle,
    /// 前端静态资源目录。
    pub web_root: PathBuf,
    pub control_token: Option<Arc<zeroize::Zeroizing<String>>>,
}

/// `/api/state` 的全量返回体。前端首次加载与 SSE 重连后都用它对齐状态。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullState {
    pub reset_pending: bool,
    pub tokens: Vec<TokenSnapshot>,
    pub slots: Vec<SlotSnapshot>,
    pub stats: crate::engine::Stats,
    pub bnb_usd: Option<f64>,
    pub status: String,
    pub config: RuntimeConfig,
    pub browser_timings: Vec<BrowserTiming>,
}

/// 暴露给前端的判定参数，方便页面标注「口径」。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeConfig {
    pub max_tax_bps: u64,
    pub watch_window_secs: u64,
    pub candle_period_secs: u64,
    pub bull_streak: usize,
    pub target_buyer_count: usize,
}

#[derive(Deserialize)]
pub struct BlacklistReq {
    pub token: String,
}

pub async fn serve(
    handle: Handle,
    web_root: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState {
        handle,
        web_root: web_root.clone(),
        control_token: std::env::var("DASHBOARD_CONTROL_TOKEN")
            .ok()
            .filter(|s| s.len() >= 24)
            .map(|s| Arc::new(zeroize::Zeroizing::new(s))),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/index.html", get(index))
        .route("/api/state", get(api_state))
        .route("/api/candidates", get(api_candidates))
        .route("/latency.js", get(latency_js))
        .route("/health.js", get(health_js))
        .route("/api/reset-preview", get(api_reset_preview))
        .route("/api/b-history", get(api_b_history))
        .route("/history.js", get(history_js))
        .route("/trading.js", get(trading_js))
        .route("/system.js", get(system_js))
        .route("/api/system", get(api_system))
        .route("/api/system/shutdown", post(api_system_shutdown))
        .route("/api/trading", get(api_trading))
        .route("/api/trading/settings", post(api_trading_settings))
        .route("/api/trading/mode", post(api_trading_mode))
        .route("/api/trading/unlock", post(api_trading_unlock))
        .route("/api/trading/lock", post(api_trading_lock))
        .route("/api/events", get(api_events))
        .route("/ws/browser", get(browser_ws))
        .route("/api/blacklist", post(api_blacklist))
        .route("/api/reset", post(api_reset))
        .route("/api/health", get(api_health))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(state);

    let addr = crate::config::listen_addr()?;
    eprintln!("[info] 前端已就绪 -> http://{addr}/");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------- 静态资源

async fn index(State(st): State<AppState>) -> Response {
    serve_file(&st.web_root.join("index.html"), "text/html; charset=utf-8").await
}

async fn history_js(State(st): State<AppState>) -> Response {
    serve_file(
        &st.web_root.join("history.js"),
        "application/javascript; charset=utf-8",
    )
    .await
}

async fn latency_js(State(st): State<AppState>) -> Response {
    serve_file(
        &st.web_root.join("latency.js"),
        "application/javascript; charset=utf-8",
    )
    .await
}

async fn trading_js(State(st): State<AppState>) -> Response {
    serve_file(
        &st.web_root.join("trading.js"),
        "application/javascript; charset=utf-8",
    )
    .await
}

async fn health_js(State(st): State<AppState>) -> Response {
    serve_file(
        &st.web_root.join("health.js"),
        "application/javascript; charset=utf-8",
    )
    .await
}

/// Read-only validation of reset's independent head path. Never sends a reset
/// command, touches SQLite, pauses buying or navigates browser windows.
async fn api_reset_preview(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    let head = tokio::time::timeout(
        Duration::from_secs(6),
        crate::recovery::latest_reset_head(None),
    )
    .await
    .map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "预检查询超时，未清空任何数据".into(),
        )
    })?
    .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;
    Ok(Json(
        serde_json::json!({"head":head,"readOnly":true,"dataChanged":false}),
    ))
}

async fn system_js(State(st): State<AppState>) -> Response {
    serve_file(
        &st.web_root.join("system.js"),
        "application/javascript; charset=utf-8",
    )
    .await
}

async fn api_system(State(st): State<AppState>) -> Json<crate::system::View> {
    Json(st.handle.shared.system.view())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShutdownReq {
    confirm: bool,
}

async fn api_system_shutdown(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ShutdownReq>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    if !req.confirm {
        return Err((StatusCode::BAD_REQUEST, "需要明确确认关闭系统".into()));
    }
    let (id, rx) = st
        .handle
        .shared
        .system
        .begin()
        .map_err(|e| (StatusCode::CONFLICT, e))?;
    // Synchronous sticky cancellation barrier; a competing LIVE/unlock request
    // cannot turn trading back on after this request, even if preflight was in flight.
    if let Some(t) = st.handle.shared.trading.get() {
        if let Err(e) = t.stop_for_shutdown() {
            st.handle
                .shared
                .system
                .set_phase(crate::system::Phase::Failed, &e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e));
        }
    }
    let handle = st.handle.clone();
    let request_id = id.clone();
    // The job survives HTTP cancellation/disconnect. The POST returns BEFORE
    // closing windows or stopping the listener, so the UI never blindly retries.
    tokio::spawn(async move {
        use crate::system::Phase;
        match shutdown_workflow(&handle, id, rx).await {
            Ok(confirmed) => {
                handle.shared.system.set_phase(Phase::Exiting, if confirmed {
                    "四个专用窗口已关闭，数据已保存，后端正在退出。"
                } else {
                    "插件已确认停用并执行关闭；最终窗口回执未收到，请检查桌面。数据已保存，后端正在退出。"
                });
                tokio::time::sleep(Duration::from_secs(2)).await;
                handle.shared.system.request_exit();
            }
            Err(e) => {
                eprintln!("[system] shutdown not completed: {e}");
                handle.shared.system.set_phase(
                    Phase::Failed,
                    format!("关闭未完成：{e}。买入及调度保持关闭；可处理后重试。"),
                );
            }
        }
    });
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"accepted":true,"requestId":request_id})),
    ))
}

async fn shutdown_workflow(
    handle: &Handle,
    id: String,
    mut acks: tokio::sync::mpsc::UnboundedReceiver<crate::system::BrowserAck>,
) -> Result<bool, String> {
    use crate::system::{BrowserAckPhase, Phase};
    let (reply, mut rx) = tokio::sync::mpsc::unbounded_channel();
    handle
        .cmd_tx
        .send(crate::engine::Command::PrepareShutdown { reply })
        .map_err(|_| "状态机已退出")?;
    let slots = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .map_err(|_| "保存状态超时，未退出后端")?
        .ok_or("保存任务未完成")??;
    if slots.len() != 4
        || slots
            .iter()
            .any(|s| s.browser_window_id.is_none() || s.browser_tab_id.is_none())
    {
        return Err("缺少四个可靠的专用窗口绑定".into());
    }
    handle.shared.system.set_phase(
        Phase::PreparingWindows,
        "已停止买入及调度，正在核对专用窗口…",
    );
    handle
        .browser_tx
        .send(crate::engine::BrowserCommand::PrepareClose {
            request_id: id.clone(),
            slots,
        })
        .map_err(|_| "插件断线，未关闭窗口")?;
    let ready = tokio::time::timeout(Duration::from_secs(12), acks.recv())
        .await
        .map_err(|_| "插件未确认停用，未退出后端")?
        .ok_or("插件未确认停用")?;
    if ready.phase != BrowserAckPhase::Ready {
        return Err(ready.error.unwrap_or_else(|| "插件拒绝关闭窗口".into()));
    }
    handle.shared.system.set_phase(
        Phase::ClosingWindows,
        "插件已停用自动补建，正在关闭甲乙丙丁…",
    );
    handle
        .browser_tx
        .send(crate::engine::BrowserCommand::CloseWindows { request_id: id })
        .map_err(|_| "关闭命令未送达插件")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        match tokio::time::timeout_at(deadline, acks.recv()).await {
            Ok(Some(ack)) if ack.phase == BrowserAckPhase::Closed => return Ok(true),
            Ok(Some(ack)) if ack.phase == BrowserAckPhase::Failed => {
                return Err(ack.error.unwrap_or_else(|| "部分窗口关闭失败".into()));
            }
            Ok(Some(_)) => continue, // Duplicate READY does not extend the deadline.
            // Closing the last Edge window can terminate its service worker
            // before CLOSED arrives. Exit only AFTER a validated READY + CLOSE;
            // report unconfirmed, never pretend to have received CLOSED.
            _ => return Ok(false),
        }
    }
}

type ApiError = (StatusCode, String);

fn authorize_control(
    peer: SocketAddr,
    headers: &HeaderMap,
    token: Option<&str>,
    local_only: bool,
) -> Result<(), ApiError> {
    if local_only && !peer.ip().is_loopback() {
        return Err((StatusCode::FORBIDDEN, "钱包解锁仅允许本机".into()));
    }
    if headers.get("x-meme-control").and_then(|h| h.to_str().ok()) != Some("1") {
        return Err((StatusCode::FORBIDDEN, "缺少控制请求标记".into()));
    }
    if let Some(raw) = headers.get(header::ORIGIN) {
        let origin = raw
            .to_str()
            .ok()
            .and_then(|v| url::Url::parse(v).ok())
            .ok_or((StatusCode::FORBIDDEN, "无效 Origin".into()))?;
        let origin_host = origin.host_str().unwrap_or("").trim_matches(['[', ']']);
        let same_host = headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|host| origin.origin().ascii_serialization() == format!("http://{host}"));
        let loopback_host = origin_host == "localhost"
            || origin_host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        if origin.scheme() != "http"
            || origin.port_or_known_default() != Some(8787)
            || !same_host
            || (peer.ip().is_loopback() && !loopback_host)
        {
            return Err((
                StatusCode::FORBIDDEN,
                "跨站或非本机域名控制请求已拒绝".into(),
            ));
        }
    }
    if !peer.ip().is_loopback() {
        let expected = token.ok_or((
            StatusCode::FORBIDDEN,
            "局域网控制尚未配置口令；只读访问可用".into(),
        ))?;
        let supplied = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        use sha2::{Digest, Sha256};
        // Fixed-size comparison avoids leaking prefix-match timing.
        let a = Sha256::digest(expected.as_bytes());
        let b = Sha256::digest(supplied.as_bytes());
        let diff = a.iter().zip(b.iter()).fold(0u8, |v, (a, b)| v | (a ^ b));
        if diff != 0 {
            return Err((StatusCode::UNAUTHORIZED, "局域网控制口令无效".into()));
        }
    }
    Ok(())
}
fn trading(st: &AppState) -> Result<crate::trading::Trading, ApiError> {
    st.handle.shared.trading.get().cloned().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "交易执行器未初始化；监控不受影响".into(),
    ))
}
async fn api_trading(State(st): State<AppState>) -> Result<Json<crate::trading::View>, ApiError> {
    let trading = trading(&st)?;
    tokio::task::spawn_blocking(move || trading.view())
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "读取订单任务失败".into()))?
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ModeReq {
    mode: crate::trading::types::Mode,
    #[serde(default)]
    expected_generation: Option<u64>,
    #[serde(default)]
    confirm_no_budget: bool,
    #[serde(default)]
    confirmed_fees: Option<crate::trading::types::FeeSettings>,
}
async fn api_trading_mode(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ModeReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    if req.mode != crate::trading::types::Mode::Disabled && req.expected_generation.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "开启必须携带当前页面状态版本".into(),
        ));
    }
    trading(&st)?
        .set_mode_with_fees(
            req.mode,
            req.expected_generation,
            req.confirm_no_budget,
            req.confirmed_fees,
        )
        .await
        .map_err(|e| (StatusCode::CONFLICT, e))?;
    Ok(Json(serde_json::json!({"ok":true})))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SettingsReq {
    amount_wei: String,
    slippage_bps: u64,
    fees: crate::trading::types::FeeSettings,
    expected_generation: u64,
}
async fn api_trading_settings(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<SettingsReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    let trading = trading(&st)?;
    tokio::task::spawn_blocking(move || {
        trading.update_settings(
            crate::trading::types::BuySettings {
                amount_wei: req.amount_wei,
                slippage_bps: Some(req.slippage_bps),
                fees: Some(req.fees),
            },
            req.expected_generation,
        )
    })
    .await
    .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "保存参数任务失败".into()))?
    .map_err(|e| (StatusCode::CONFLICT, e))?;
    Ok(Json(serde_json::json!({"ok":true,"mode":"DISABLED"})))
}
#[derive(Deserialize)]
struct UnlockReq {
    password: String,
}
async fn api_trading_unlock(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<UnlockReq>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let password = zeroize::Zeroizing::new(req.password);
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        true,
    )?;
    if password.len() > 1024 {
        return Err((StatusCode::BAD_REQUEST, "密码过长".into()));
    }
    trading(&st)?
        .unlock(password)
        .await
        .map_err(|e| (StatusCode::CONFLICT, e))?;
    Ok(Json(serde_json::json!({"ok":true,"mode":"DISABLED"})))
}
async fn api_trading_lock(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    trading(&st)?
        .pause("已暂停并锁定钱包", true)
        .map_err(|e| (StatusCode::CONFLICT, e))?;
    Ok(Json(serde_json::json!({"ok":true})))
}

#[derive(Default, Deserialize)]
struct HistoryQuery {
    limit: Option<u64>,
    offset: Option<u64>,
    q: Option<String>,
}

async fn api_b_history(
    Query(query): Query<HistoryQuery>,
) -> Result<Json<crate::persistence::BHistoryPage>, (StatusCode, String)> {
    let q = query.q.unwrap_or_default();
    if q.chars().count() > 100 {
        return Err((StatusCode::BAD_REQUEST, "搜索内容过长".into()));
    }
    tokio::task::spawn_blocking(move || {
        crate::persistence::read_b_history(
            &crate::config::database_path(),
            query.limit.unwrap_or(50),
            query.offset.unwrap_or(0),
            &q,
        )
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map(Json)
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}

async fn serve_file(path: &PathBuf, ctype: &str) -> Response {
    match tokio::fs::read(path).await {
        Ok(body) => ([(header::CONTENT_TYPE, ctype)], body).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            format!("无法读取 {}: {e}", path.display()),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------- API

async fn api_state(State(st): State<AppState>) -> Json<FullState> {
    let shared: &Shared = &st.handle.shared;
    let tokens = shared.snapshots.read().await.clone();
    let slots = shared.slots.read().await.clone();
    let stats = *shared.stats.read().await;
    let bnb_usd = *shared.bnb_usd.read().await;
    let status = shared.status.read().await.clone();
    let browser_timings = shared.browser_timings.read().await.clone();
    Json(FullState {
        reset_pending: shared
            .reset_pending
            .load(std::sync::atomic::Ordering::Relaxed)
            != 0,
        tokens,
        slots,
        stats,
        bnb_usd,
        status,
        browser_timings,
        config: RuntimeConfig {
            max_tax_bps: crate::config::MAX_TAX_BPS,
            watch_window_secs: crate::config::WATCH_WINDOW.as_secs(),
            candle_period_secs: crate::config::CANDLE_PERIOD_SECS,
            bull_streak: crate::config::BULL_STREAK,
            target_buyer_count: crate::targets::target_count(),
        },
    })
}

/// Laptop/dashboard 不需要每秒下载所有币的 600 根 K 线和 1000 笔成交。
/// 原完整诊断 API 仍保留；此路径只复制候补显示字段，不复制行情大数组。
pub(crate) async fn api_candidates(State(st): State<AppState>) -> Json<serde_json::Value> {
    let shared = &st.handle.shared;
    let tokens: Vec<_> = shared
        .snapshots
        .read()
        .await
        .iter()
        .filter(|t| {
            (t.state.is_pending() && t.current_slot.is_none())
                || t.state == crate::engine::Stage::CWatching
        })
        .map(|t| {
            serde_json::json!({
                "token":t.token,"name":t.name,"symbol":t.symbol,"platform":t.platform,
                "state":t.state,"marketCapUsd":t.market_cap_usd,"createdTs":t.created_ts,
                "cEnteredAt":t.c_entered_at,"currentSlot":t.current_slot
            })
        })
        .collect();
    let status = shared.status.read().await.clone();
    let timings = shared.browser_timings.read().await.clone();
    let slot_states: Vec<_> = shared
        .slots
        .read()
        .await
        .iter()
        .map(|s| serde_json::json!({"slotId":s.slot_id,"state":s.state}))
        .collect();
    Json(
        serde_json::json!({"tokens":tokens,"status":status,"browserTimings":timings,
        "ingestion":shared.ingestion.read().await.clone(),"serverTimeMs":crate::rpc::unix_ms(),
        "slotStates":slot_states,"resetPending":shared.reset_pending.load(std::sync::atomic::Ordering::Relaxed)!=0}),
    )
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
enum BrowserIncoming {
    ShutdownAck {
        request_id: String,
        phase: crate::system::BrowserAckPhase,
        #[serde(default)]
        error: Option<String>,
    },
    Ping,
    CommandError {
        command_type: String,
        #[serde(default)]
        reset_id: u64,
        error: String,
        #[serde(default)]
        slot_id: Option<String>,
        #[serde(default)]
        command_id: Option<String>,
        #[serde(default)]
        ca: Option<String>,
    },
    Progress {
        slot_id: String,
        command_id: String,
        ca: String,
        window_id: i64,
        tab_id: i64,
        phase: BrowserPhase,
        page_clock_ms: u64,
    },
    ResetAck {
        reset_id: u64,
        slots: Vec<BrowserSlotReport>,
    },
    Hello {
        slots: Vec<BrowserSlotReport>,
        #[serde(default)]
        version: String,
        #[serde(default)]
        supports_close_windows: bool,
    },
    Ack {
        slot_id: String,
        command_id: String,
        ca: String,
        window_id: i64,
        tab_id: i64,
    },
    Url {
        slot_id: String,
        window_id: i64,
        tab_id: i64,
        url: String,
    },
    /// 页面侧观测到的「K 线可见」时刻（毫秒，与本机时钟同源）。
    ChartReady {
        slot_id: String,
        window_id: i64,
        tab_id: i64,
        ca: String,
        page_clock_ms: u64,
    },
}

async fn browser_ws(
    ws: WebSocketUpgrade,
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let allowed_origin = headers.get(header::ORIGIN).is_none_or(|o| {
        o.to_str()
            .ok()
            .and_then(|s| url::Url::parse(s).ok())
            .is_some_and(|u| {
                matches!(u.scheme(), "chrome-extension" | "extension")
                    || (u.scheme() == "http"
                        && u.port_or_known_default() == Some(8787)
                        && matches!(
                            u.host_str(),
                            Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
                        ))
            })
    });
    if !peer.ip().is_loopback() || !allowed_origin {
        return (StatusCode::FORBIDDEN, "浏览器调度连接仅允许本机扩展").into_response();
    }
    ws.on_upgrade(move |socket| browser_session(socket, st.handle))
        .into_response()
}

async fn browser_session(socket: WebSocket, handle: Handle) {
    let (mut sender, mut receiver) = socket.split();
    // A connection acquires ownership only after a complete, valid handshake.
    // Other clients cannot issue commands or mark the owner disconnected.
    static OWNER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let first = tokio::time::timeout(Duration::from_secs(10), receiver.next()).await;
    let Ok(Some(Ok(Message::Text(text)))) = first else {
        return;
    };
    let Ok(BrowserIncoming::Hello {
        slots,
        version,
        supports_close_windows,
    }) = serde_json::from_str(&text)
    else {
        return;
    };
    let valid = slots.len() == 4
        && slots
            .iter()
            .all(|s| s.window_id.is_some() && s.tab_id.is_some())
        && slots
            .iter()
            .map(|s| s.window_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            == 4
        && slots
            .iter()
            .map(|s| s.tab_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            == 4
        && slots
            .iter()
            .map(|s| s.slot_id.as_str())
            .collect::<std::collections::HashSet<_>>()
            == std::collections::HashSet::from(["A", "B", "C", "D"]);
    if !valid {
        return;
    }
    let Ok(_owner) = OWNER.try_lock() else {
        return;
    };
    let mut commands = handle.browser_tx.subscribe();
    handle
        .shared
        .system
        .browser_connected(supports_close_windows);
    eprintln!("[browser] connected version={version:?} slots={slots:?}");
    let cmd = handle.cmd_tx.clone();
    let _ = cmd.send(crate::engine::Command::BrowserHello { slots });
    let writer = tokio::spawn(async move {
        loop {
            let message = match commands.recv().await {
                Ok(message) => message,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // 丢失 OPEN 不能静默等 15 秒；立即请求真实四窗口状态。
                    eprintln!("[browser] command stream lagged={n}; requesting resync");
                    crate::engine::BrowserCommand::SyncRequest
                }
                Err(_) => break,
            };
            let Ok(json) = serde_json::to_string(&message) else {
                continue;
            };
            if sender.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(message)) = receiver.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        match serde_json::from_str::<BrowserIncoming>(&text) {
            Ok(BrowserIncoming::ShutdownAck {
                request_id,
                phase,
                error,
            }) => {
                handle
                    .shared
                    .system
                    .ack(&request_id, crate::system::BrowserAck { phase, error });
            }
            Ok(BrowserIncoming::Ping) => {}
            Ok(BrowserIncoming::CommandError {
                command_type,
                reset_id,
                error,
                slot_id,
                command_id,
                ca,
            }) => {
                eprintln!(
                    "[browser] command failed type={command_type} reset={reset_id} error={error}"
                );
                if command_type == "OPEN" {
                    if let (Some(slot_id), Some(command_id), Some(token)) =
                        (slot_id, command_id, ca)
                    {
                        let _ = cmd.send(crate::engine::Command::BrowserCommandFailed {
                            slot_id,
                            command_id,
                            token,
                            error,
                        });
                    }
                }
            }
            Ok(BrowserIncoming::Progress {
                slot_id,
                command_id,
                ca,
                window_id,
                tab_id,
                phase,
                page_clock_ms,
            }) => {
                let _ = cmd.send(crate::engine::Command::BrowserProgress {
                    slot_id,
                    command_id,
                    token: ca,
                    window_id,
                    tab_id,
                    phase,
                    page_clock_ms,
                });
            }
            Ok(BrowserIncoming::ResetAck { reset_id, slots }) => {
                eprintln!("[browser] RESET_ACK id={reset_id} slots={slots:?}");
                let _ = cmd.send(crate::engine::Command::BrowserResetAck { reset_id, slots });
            }
            Ok(BrowserIncoming::Hello { slots, .. }) => {
                if handle
                    .shared
                    .reset_pending
                    .load(std::sync::atomic::Ordering::Relaxed)
                    != 0
                {
                    eprintln!("[browser] reset pending HELLO slots={slots:?}");
                }
                let _ = cmd.send(crate::engine::Command::BrowserHello { slots });
            }
            Ok(BrowserIncoming::Ack {
                slot_id,
                command_id,
                ca,
                window_id,
                tab_id,
            }) => {
                let _ = cmd.send(crate::engine::Command::BrowserAck {
                    slot_id,
                    command_id,
                    token: ca,
                    window_id,
                    tab_id,
                });
            }
            Ok(BrowserIncoming::Url {
                slot_id,
                window_id,
                tab_id,
                url,
            }) => {
                let _ = cmd.send(crate::engine::Command::BrowserUrl {
                    slot_id,
                    window_id,
                    tab_id,
                    url,
                });
            }
            Ok(BrowserIncoming::ChartReady {
                slot_id,
                window_id,
                tab_id,
                ca,
                page_clock_ms,
            }) => {
                let _ = cmd.send(crate::engine::Command::BrowserChartReady {
                    slot_id,
                    window_id,
                    tab_id,
                    token: ca,
                    page_clock_ms,
                });
            }
            Err(e) => eprintln!("[warn] 浏览器消息格式错误: {e}"),
        }
    }
    writer.abort();
    handle.shared.system.browser_disconnected();
    let _ = handle
        .cmd_tx
        .send(crate::engine::Command::BrowserDisconnected);
}

#[derive(Deserialize)]
struct ResetReq {
    confirm: bool,
}

async fn api_reset(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ResetReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    if !req.confirm {
        return Err((StatusCode::BAD_REQUEST, "需要明确确认清空".into()));
    }
    if st.handle.shared.system.is_stopping() {
        return Err((StatusCode::CONFLICT, "系统关闭中，不能重新监控".into()));
    }
    if let Some(trading) = st.handle.shared.trading.get() {
        trading
            .pause("清空监控请求：已暂停买入", false)
            .map_err(|e| (StatusCode::CONFLICT, e))?;
    }
    let (reply, mut rx) = tokio::sync::mpsc::unbounded_channel();
    st.handle
        .cmd_tx
        .send(crate::engine::Command::RequestReset { reply })
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "引擎已停止".into()))?;
    match tokio::time::timeout(Duration::from_secs(20), rx.recv()).await {
        Ok(Some(Ok(boundary))) => Ok(Json(serde_json::json!({"ok":true,"boundary":boundary}))),
        Ok(Some(Err(e))) => Err((StatusCode::CONFLICT, e)),
        _ => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "结果暂未确认，请刷新状态，勿重复提交".into(),
        )),
    }
}

async fn api_health(State(st): State<AppState>) -> Json<serde_json::Value> {
    let status = st.handle.shared.status.read().await.clone();
    let stats = *st.handle.shared.stats.read().await;
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "serverTimeMs":crate::rpc::unix_ms(),
        "liveBuild": cfg!(feature="live-trading"),
        "wss": status,
        "live": stats.live_now,
        "watching": stats.watching_now,
        "uptimeSecs": stats.uptime_secs,
        "ingestion": st.handle.shared.ingestion.read().await.clone(),
    }))
}

/// 永久剔除一个代币。前端点 X 时调用。
async fn api_blacklist(
    State(st): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<BlacklistReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize_control(
        peer,
        &headers,
        st.control_token.as_ref().map(|s| s.as_str()),
        false,
    )?;
    let token = req.token.trim().to_lowercase();
    if crate::trading::types::address(&token).is_err() {
        return Err((StatusCode::BAD_REQUEST, "代币地址格式不合法".into()));
    }
    if let Some(trading) = st.handle.shared.trading.get() {
        trading.cancel_token(&token, "人工删除请求：取消未广播买入");
    }
    st.handle
        .cmd_tx
        .send(crate::engine::Command::Blacklist(token.clone()))
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "引擎已停止".to_string()))?;
    Ok(Json(serde_json::json!({ "ok": true, "token": token })))
}

/// SSE 事件流。
///
/// 订阅方消费过慢时 broadcast 会返回 Lagged；此时不中断连接，而是让前端
/// 通过 `resync` 事件重新拉全量状态，避免界面出现静默的数据缺口。
async fn api_events(
    State(st): State<AppState>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let rx = st.handle.event_tx.subscribe();
    let stream = BroadcastStream::new(rx).flat_map(|item| {
        // 订阅方消费过慢时 broadcast 会返回 Lagged(丢失条数)。此处不中断连接，
        // 而是发一个 resync 事件让前端重新拉全量状态，避免界面出现静默缺口。
        let events: Vec<Result<SseEvent, Infallible>> = match item {
            Ok(ev) => {
                let kind = ev_kind(&ev);
                match serde_json::to_string(&ev) {
                    Ok(json) => vec![Ok(SseEvent::default().event(kind).data(json))],
                    // 序列化失败不该发生；真发生时跳过这一条，不让流中断。
                    Err(e) => {
                        eprintln!("[warn] 事件序列化失败，已跳过: {e}");
                        vec![]
                    }
                }
            }
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                eprintln!("[warn] SSE 订阅方落后 {n} 条，通知前端全量重同步");
                vec![Ok(SseEvent::default().event("resync").data("{}"))]
            }
        };
        futures_util::stream::iter(events)
    });

    Sse::new(stream).keep_alive(
        KeepAlive::default()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

/// SSE 事件名，供前端 addEventListener 精确订阅。
fn ev_kind(ev: &Event) -> &'static str {
    match ev {
        Event::TokenUp(_) => "tokenUp",
        Event::TokenDown { .. } => "tokenDown",
        Event::Candle { .. } => "candle",
        Event::Trade { .. } => "trade",
        Event::Watching { .. } => "watching",
        Event::Stats(_) => "stats",
        Event::BnbUsd { .. } => "bnbUsd",
        Event::Status { .. } => "status",
    }
}

// 供 main 在启动时打印前端地址。
pub fn public_url() -> String {
    "http://127.0.0.1:8787/".to_string()
}

/// 便于测试：确认共享视图可独立读写。
pub async fn _assert_shared_is_send_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>(_: &T) {}
    let s = Shared::default();
    assert_send_sync(&s);
    let _ = Arc::new(s);
}

#[cfg(test)]
mod control_tests {
    use super::*;
    fn headers(origin: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-meme-control", "1".parse().unwrap());
        h.insert(header::HOST, "127.0.0.1:8787".parse().unwrap());
        h.insert(header::ORIGIN, origin.parse().unwrap());
        h
    }
    fn test_handle() -> (
        Handle,
        tokio::sync::mpsc::UnboundedReceiver<crate::engine::Command>,
    ) {
        let (cmd_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = Handle {
            cmd_tx,
            event_tx: tokio::sync::broadcast::channel(16).0,
            browser_tx: tokio::sync::broadcast::channel(16).0,
            shared: Shared::default(),
        };
        (handle, rx)
    }
    #[tokio::test]
    async fn shutdown_requires_confirmation_and_same_control_auth() {
        let (handle, _rx) = test_handle();
        handle.shared.system.browser_connected(true);
        let st = AppState {
            handle: handle.clone(),
            web_root: PathBuf::new(),
            control_token: None,
        };
        assert!(api_system_shutdown(
            State(st.clone()),
            ConnectInfo("127.0.0.1:30000".parse().unwrap()),
            headers("https://evil.test"),
            Json(ShutdownReq { confirm: true })
        )
        .await
        .is_err());
        assert!(api_system_shutdown(
            State(st),
            ConnectInfo("127.0.0.1:30000".parse().unwrap()),
            headers("http://127.0.0.1:8787"),
            Json(ShutdownReq { confirm: false })
        )
        .await
        .is_err());
        assert!(!handle.shared.system.is_stopping());
        assert!(
            serde_json::from_str::<ShutdownReq>(r#"{"confirm":true,"deleteData":true}"#).is_err()
        );
        assert!(serde_json::from_str::<ShutdownReq>("{}").is_err());
    }
    #[tokio::test]
    async fn shutdown_workflow_requires_saved_state_and_two_phase_window_ack() {
        for fail_close in [false, true] {
            let (handle, mut rx) = test_handle();
            handle.shared.system.browser_connected(true);
            let (id, acks) = handle.shared.system.begin().unwrap();
            let mut browser = handle.browser_tx.subscribe();
            let system = handle.shared.system.clone();
            let request_id = id.clone();
            let mock = tokio::spawn(async move {
                let Some(crate::engine::Command::PrepareShutdown { reply }) = rx.recv().await
                else {
                    panic!("missing save");
                };
                reply
                    .send(Ok((0..4)
                        .map(|i| SlotSnapshot {
                            slot_id: ["A", "B", "C", "D"][i].into(),
                            token_ca: None,
                            browser_window_id: Some(100 + i as i64),
                            browser_tab_id: Some(200 + i as i64),
                            state: crate::engine::SlotState::Empty,
                            command_id: None,
                        })
                        .collect()))
                    .unwrap();
                assert!(matches!(
                    browser.recv().await.unwrap(),
                    crate::engine::BrowserCommand::PrepareClose { .. }
                ));
                system.ack(
                    &request_id,
                    crate::system::BrowserAck {
                        phase: crate::system::BrowserAckPhase::Ready,
                        error: None,
                    },
                );
                assert!(matches!(
                    browser.recv().await.unwrap(),
                    crate::engine::BrowserCommand::CloseWindows { .. }
                ));
                system.ack(
                    &request_id,
                    crate::system::BrowserAck {
                        phase: if fail_close {
                            crate::system::BrowserAckPhase::Failed
                        } else {
                            crate::system::BrowserAckPhase::Closed
                        },
                        error: Some("personal tab protected".into()),
                    },
                );
            });
            let result = shutdown_workflow(&handle, id, acks).await;
            if fail_close {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap());
            }
            mock.await.unwrap();
            // Workflow by itself never calls process::exit or closes real windows.
        }
    }
    #[test]
    fn cross_site_dns_rebinding_and_missing_marker_are_rejected() {
        let peer = "127.0.0.1:30000".parse().unwrap();
        assert!(authorize_control(peer, &headers("http://127.0.0.1:8787"), None, true).is_ok());
        for origin in [
            "https://gmgn.ai",
            "http://evil.test:8787",
            "http://127.0.0.1:9999",
            "null",
        ] {
            assert!(authorize_control(peer, &headers(origin), None, true).is_err());
        }
        let mut h = headers("http://127.0.0.1:8787");
        h.remove("x-meme-control");
        assert!(authorize_control(peer, &h, None, false).is_err());
    }
    #[test]
    fn lan_can_control_with_token_but_cannot_unlock_wallet() {
        let peer = "192.168.1.30:30000".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("x-meme-control", "1".parse().unwrap());
        let token = "offline-control-token-at-least-24-characters";
        assert!(authorize_control(peer, &h, None, false).is_err());
        assert!(authorize_control(peer, &h, Some(token), false).is_err());
        h.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        assert!(authorize_control(peer, &h, Some(token), false).is_ok());
        assert!(authorize_control(peer, &h, Some(token), true).is_err());
    }
    #[test]
    fn trading_requests_cannot_inject_rpc_wallet_or_mode_into_settings() {
        let settings = serde_json::json!({"amountWei":"5000000000000000","slippageBps":100,"expectedGeneration":1,
            "fees":crate::trading::types::FeeSettings::from_policy(&crate::trading::types::Policy::default())});
        assert!(serde_json::from_value::<SettingsReq>(settings.clone()).is_ok());
        let mut injected = settings;
        injected["allowLive"] = serde_json::json!(true);
        assert!(serde_json::from_value::<SettingsReq>(injected).is_err());
        // A cached old page must not silently reset newly confirmed fee authorization.
        assert!(serde_json::from_value::<SettingsReq>(serde_json::json!({
            "amountWei":"5000000000000000","slippageBps":100,"expectedGeneration":1
        }))
        .is_err());
        let mut endpoint_injection = serde_json::json!({"amountWei":"1","slippageBps":100,"expectedGeneration":1,
            "fees":crate::trading::types::FeeSettings::from_policy(&crate::trading::types::Policy::default())});
        endpoint_injection["fees"]["rpcUrl"] = serde_json::json!("https://example.invalid");
        assert!(serde_json::from_value::<SettingsReq>(endpoint_injection).is_err());
        let request: ModeReq =
            serde_json::from_value(serde_json::json!({"mode":"DISABLED"})).unwrap();
        assert!(request.expected_generation.is_none());
        assert!(!request.confirm_no_budget);
        assert!(serde_json::from_value::<ModeReq>(
            serde_json::json!({"mode":"LIVE","walletPassword":"not-accepted"})
        )
        .is_err());
    }
}
