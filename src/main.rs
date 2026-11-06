//! meme 量化监测：BSC / Flap.sh 建币捕获 -> 税率筛选(A币) ->
//! 目标地址买入 + 三连阳判定(B币) -> SSE 推前端。
//!
//! 启动流程：
//! 1. 建立 WSS 日志订阅；高频 `tx.from` 补查使用独立 HTTP RPC 连接池；
//! 2. 可选回填最近若干区块，让页面启动即有内容（环境变量 `SEED_BLOCKS`，默认 0）；
//! 3. 订阅 Portal 的六类事件，进入实时监测；
//! 4. 起 HTTP/SSE 服务，浏览器打开 `/` 即可。

mod abi;
mod config;
mod engine;
mod launchpads;
mod persistence;
mod recovery;
mod rpc;
mod server;
mod system;
mod targets;
mod trading;
mod wallet;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, watch};

use engine::{BrowserCommand, Command, Engine, Event, Shared};
use rpc::RawLog;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

// Windows 控制台密码输入（隐藏回显）
#[cfg(windows)]
extern "system" {
    fn GetStdHandle(nStdHandle: u32) -> *mut std::ffi::c_void;
    fn SetConsoleMode(hConsoleHandle: *mut std::ffi::c_void, dwMode: u32) -> i32;
    fn GetConsoleMode(hConsoleHandle: *mut std::ffi::c_void, lpMode: *mut u32) -> i32;
}
const STD_INPUT_HANDLE: u32 = 0xfffffff6; // -10 as u32
const ENABLE_ECHO_INPUT: u32 = 0x0004;

/// 读取一行密码，Windows 下隐藏回显。
fn read_password(prompt: &str) -> Result<zeroize::Zeroizing<String>, BoxErr> {
    eprint!("{prompt}");
    #[cfg(windows)]
    {
        unsafe {
            let handle = GetStdHandle(STD_INPUT_HANDLE);
            let mut old_mode: u32 = 0;
            if GetConsoleMode(handle, &mut old_mode) == 0
                || SetConsoleMode(handle, old_mode & !ENABLE_ECHO_INPUT) == 0
            {
                return Err("密码输入需要可见的本机控制台；隐藏启动不会自动解锁".into());
            }
            let mut buf = zeroize::Zeroizing::new(String::new());
            let result = std::io::stdin().read_line(&mut buf);
            SetConsoleMode(handle, old_mode);
            eprintln!();
            result?;
            let trimmed = buf.trim_end_matches(['\r', '\n']).to_string();
            return Ok(zeroize::Zeroizing::new(trimmed));
        }
    }
    #[cfg(not(windows))]
    {
        Err("该工具的密码输入仅支持 Windows 本机控制台".into())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxErr> {
    // --encrypt-wallet 子命令：交互式加密私钥并写入文件
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && args[1] == "--check-wallet-console" {
        #[cfg(windows)]
        unsafe {
            let handle = GetStdHandle(STD_INPUT_HANDLE);
            let mut mode = 0;
            if GetConsoleMode(handle, &mut mode) == 0 || SetConsoleMode(handle, mode) == 0 {
                return Err("钱包输入需要独立的本机控制台；不要使用管道或重定向".into());
            }
            println!("本机钱包控制台检查通过；没有读取私钥或密码、写入文件或连接 RPC。");
            return Ok(());
        }
        #[cfg(not(windows))]
        return Err("钱包控制台检查仅支持 Windows".into());
    }
    if args.len() >= 3 && args[1] == "--encrypt-wallet" {
        let expected_address = match args.len() {
            3 => None,
            5 if args[3] == "--expected-address" => Some(args[4].as_str()),
            _ => return Err("只支持 --encrypt-wallet PATH [--expected-address ADDRESS]；不要将私钥或密码放入命令参数".into()),
        };
        let output_path = PathBuf::from(&args[2]);
        if output_path.exists() {
            return Err("目标钱包文件已存在，禁止覆盖".into());
        }
        let pk = read_password("输入私钥（hex，可带 0x 前缀）: ")?;
        if pk.is_empty() {
            eprintln!("[error] 私钥不能为空");
            return Err("私钥不能为空".into());
        }
        let password = read_password("输入加密密码（至少12个字符）: ")?;
        if password.is_empty() {
            eprintln!("[error] 密码不能为空");
            return Err("密码不能为空".into());
        }
        let confirm = read_password("再次输入密码确认: ")?;
        if password != confirm {
            eprintln!("[error] 两次密码不一致");
            return Err("两次密码不一致".into());
        }
        match wallet::encrypt_private_key_checked(&pk, &password, &output_path, expected_address) {
            Ok(address) => {
                println!("已核对账户地址: {address}");
                println!("私钥已加密写入 {}", output_path.display());
                println!("仅离线加密；没有连接 RPC、解锁运行中的钱包或开启买入。");
                return Ok(());
            }
            Err(e) => {
                eprintln!("[error] 加密失败: {e}");
                return Err(e.into());
            }
        }
    }
    if args.len() == 4 && args[1] == "--migrate-wallet" {
        let old = read_password("旧密钥文件密码: ")?;
        let new = read_password("新密码（至少12个字符）: ")?;
        let confirm = read_password("再次输入新密码: ")?;
        if new != confirm {
            return Err("新密码不一致".into());
        }
        wallet::migrate_legacy(
            std::path::Path::new(&args[2]),
            &old,
            &new,
            std::path::Path::new(&args[3]),
        )?;
        println!("已离线迁移到新文件；原文件保留，未连接节点或启用交易。");
        return Ok(());
    }
    if args.len() == 2 && args[1] == "--check-config" {
        targets::initialize(&config::targets_path())?;
        let addr = config::listen_addr()?;
        if !addr.ip().is_loopback()
            && std::env::var("DASHBOARD_CONTROL_TOKEN")
                .ok()
                .is_none_or(|s| s.len() < 24)
        {
            return Err("局域网监听必须设置至少 24 个字符的 DASHBOARD_CONTROL_TOKEN".into());
        }
        if !config::resource_root().join("web/index.html").is_file() {
            return Err("未找到 web/index.html；请在项目目录启动或设置 MEME_QUANT_HOME".into());
        }
        println!(
            "配置有效：监控地址 {} 个；监听 {addr}。未连接 RPC、未启动后端、未读取钱包或启用交易。",
            targets::target_count()
        );
        return Ok(());
    }
    if args.len() != 1 {
        return Err(
            "不支持的参数；支持 --check-config、监控启动、--encrypt-wallet PATH [--expected-address ADDRESS]、--migrate-wallet OLD NEW".into(),
        );
    }

    targets::initialize(&config::targets_path())?;
    let listen = config::listen_addr()?;
    if !listen.ip().is_loopback()
        && std::env::var("DASHBOARD_CONTROL_TOKEN")
            .ok()
            .is_none_or(|s| s.len() < 24)
    {
        return Err("局域网监听必须设置至少 24 个字符的 DASHBOARD_CONTROL_TOKEN".into());
    }
    if !config::resource_root().join("web/index.html").is_file() {
        return Err("未找到 web/index.html；请在项目目录启动或设置 MEME_QUANT_HOME".into());
    }
    println!("=== meme 量化监测（BSC / Flap + Four.meme + GeniusFun） ===");
    println!(
        "筛选口径：买卖税均 <= {}%（A 币）",
        config::MAX_TAX_BPS as f64 / 100.0
    );
    println!(
        "判定口径：每个 A 币只观察 {} 秒，{} 秒一根 K 线，目标地址买入且连续 {} 根阳线即晋升 B 币（目标地址 {} 个）",
        config::WATCH_WINDOW.as_secs(),
        config::CANDLE_PERIOD_SECS,
        config::BULL_STREAK,
        targets::target_count()
    );

    let (logs_tx, logs_rx) = broadcast::channel::<RawLog>(65536);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Command>();
    let (event_tx, _) = broadcast::channel::<Event>(8192);
    let (browser_tx, _) = broadcast::channel::<BrowserCommand>(256);
    let (status_tx, status_rx) = watch::channel("connecting".to_string());

    let launchpad_registry = launchpads::Registry::default();
    println!(
        "[info] Launchpad adapters：{}",
        launchpad_registry
            .platforms()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" / ")
    );
    let subscription_topics = launchpad_registry.subscription_topics();
    let client = rpc::spawn(logs_tx.clone(), status_tx, subscription_topics).await;
    // 回填与报价符号解析都依赖连接就绪，最多等 10 秒。
    for _ in 0..40 {
        if client.is_live() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    if !client.is_live() {
        eprintln!("[warn] WSS 尚未连上；后台会继续自动重连，期间事件可能缺失。");
    }

    let shared = Shared::default();
    let db_path = config::database_path();
    let store = persistence::Store::open(&db_path)?;
    let recovery_store = store.clone();
    println!("[info] SQLite 状态库：{}", db_path.display());
    let handle = engine::Handle {
        cmd_tx: cmd_tx.clone(),
        event_tx: event_tx.clone(),
        browser_tx: browser_tx.clone(),
        shared: shared.clone(),
    };

    let engine = Engine::new(
        logs_rx,
        cmd_rx,
        cmd_tx,
        event_tx.clone(),
        browser_tx,
        status_rx,
        client.clone(),
        shared,
        store.clone(),
    );

    // No startup password prompt, key-file read, or inherited-env auto activation.
    let trading_config = std::env::var("TRADING_CONFIG_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| config::resource_root().join("trading-config.json"));
    let policy = trading::types::Policy::load(&trading_config).unwrap_or_else(|e| {
        eprintln!("[trade] {e}；退回禁用交易配置");
        trading::types::Policy::default()
    });
    let trade_rpc = trading::rpc::HttpRpc::new(policy.rpc_url.clone()).unwrap_or_else(|e| {
        eprintln!("[trade] {e}；交易 RPC 已禁用");
        trading::rpc::HttpRpc::new(None).expect("disabled RPC")
    });
    let (trading, trade_jobs) = trading::Trading::new(store.clone(), policy, Arc::new(trade_rpc))?;
    engine.attach_trading(trading.clone())?;
    let trade_worker = trading.clone();
    tokio::spawn(trade_worker.run(trade_jobs));
    println!(
        "[trade] 买入关闭，钱包锁定；本构建广播能力：{}。WALLET_KEY_FILE 不再自动解锁。",
        if cfg!(feature = "live-trading") {
            "受保护（不会自动启用）"
        } else {
            "无"
        }
    );

    let engine_task = tokio::spawn(engine.run());
    let recovery_shared = handle.shared.clone();
    let recovery_client = client.clone();
    let recovery_logs = logs_tx.clone();

    // 可选回填：把最近 N 个区块的历史事件灌进引擎，启动即有内容。
    // 默认 0，严格只监测启动之后新建的币。
    let seed = config::seed_blocks();
    if seed > 0 {
        println!("[info] 回填最近 {seed} 个区块…");
        if let Err(e) = recovery::seed(client.clone(), &logs_tx, &handle.shared, &store, seed).await
        {
            eprintln!("[warn] 回填失败（不影响实时监测）: {e}");
        }
    }

    // HTTP reconciliation runs even when WSS still says live or is disconnected.
    // One worker owns bounded scans; metadata and trading RPC pools stay isolated.
    tokio::spawn(async move {
        recovery::run(
            recovery_client,
            recovery_logs,
            recovery_shared,
            recovery_store,
        )
        .await;
    });

    let web_root = config::resource_root().join("web");
    if !web_root.join("index.html").exists() {
        eprintln!("[warn] 未找到前端文件：{}", web_root.display());
    }
    let shutdown_control = handle.shared.system.clone();
    let server_task = tokio::spawn(async move {
        if let Err(e) = server::serve(handle, web_root).await {
            eprintln!("[error] HTTP 服务异常: {e}");
        }
    });

    println!("[info] 前端已就绪，浏览器打开 {}", server::public_url());
    println!("[info] Ctrl+C 停止");

    tokio::select! {
        r = engine_task => { eprintln!("[error] 引擎退出: {r:?}"); }
        r = server_task => { eprintln!("[error] HTTP 服务退出: {r:?}"); }
        _ = shutdown_signal() => { println!("\n[info] 收到退出信号，正在关闭…"); }
        _ = shutdown_control.wait_exit() => { println!("[info] 网页请求关闭，状态已保存，正在退出…"); }
    }
    let _ = trading.pause("程序退出：暂停买入并锁定钱包", true);
    if let Err(e) = store.flush_market() {
        eprintln!("[error] 最终行情保存失败: {e}");
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
