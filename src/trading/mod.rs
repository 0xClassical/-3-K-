//! An isolated serial executor. All strategy/slot mutations still belong to Engine.
//! Default = disabled + locked. Dry-run cannot sign/send. Restart never resumes buys.
pub mod book;
pub mod routes;
pub mod rpc;
pub mod types;

use crate::persistence::{PersistedToken, Store};
use crate::wallet::{Transaction, Wallet};
use rpc::TradeRpc;
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use types::*;
use zeroize::Zeroizing;

struct Control {
    stopping: bool,
    mode: Mode,
    generation: u64,
    settings: BuySettings,
    wallet: Option<Arc<Wallet>>,
    nonce_ready: bool,
    cancelled: HashSet<String>,
    bindings: HashMap<String, String>,
    consecutive_failures: u32,
    last_error: Option<String>,
    bnb_balance_wei: Option<String>,
    balance_at_ms: Option<u64>,
}

#[derive(Clone)]
pub struct Trading {
    store: Store,
    policy: Arc<Policy>,
    control: Arc<Mutex<Control>>,
    rpc: Arc<dyn TradeRpc>,
    jobs: mpsc::Sender<String>,
    session: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct View {
    pub mode: Mode,
    pub generation: u64,
    pub live_build: bool,
    pub live_allowed: bool,
    pub wallet_unlocked: bool,
    pub wallet: Option<String>,
    pub nonce_ready: bool,
    pub policy_ready: bool,
    pub rpc_configured: bool,
    pub wallet_configured: bool,
    pub can_simulate: bool,
    pub amount_wei: String,
    pub slippage_bps: Option<u64>,
    pub fees: Option<FeeSettings>,
    pub gas_price_ceiling_wei: String,
    pub gas_fee_ceiling_wei: String,
    pub gas_limit_cap: u64,
    pub session_budget_wei: String,
    pub daily_budget_wei: String,
    pub bnb_balance_wei: Option<String>,
    pub balance_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub orders: Vec<Order>,
}
impl Trading {
    pub fn new(
        store: Store,
        policy: Policy,
        rpc: Arc<dyn TradeRpc>,
    ) -> Result<(Self, mpsc::Receiver<String>), String> {
        store.cancel_unsent(None, "程序重启：取消未广播意图；已签名订单需核对")?;
        let (jobs, rx) = mpsc::channel(policy.max_queue.clamp(1, 64));
        let mut settings = store
            .buy_settings()?
            .unwrap_or_else(|| BuySettings::from_policy(&policy));
        if settings.fees.is_none() {
            settings.fees = Some(FeeSettings::from_policy(&policy));
        }
        let error = settings.validate(&policy).err();
        Ok((
            Self {
                store,
                policy: Arc::new(policy),
                rpc,
                jobs,
                session: uuid::Uuid::new_v4().to_string(),
                control: Arc::new(Mutex::new(Control {
                    stopping: false,
                    mode: Mode::Disabled,
                    generation: 1,
                    settings,
                    wallet: None,
                    nonce_ready: false,
                    cancelled: HashSet::new(),
                    bindings: HashMap::new(),
                    consecutive_failures: 0,
                    last_error: error,
                    bnb_balance_wei: None,
                    balance_at_ms: None,
                })),
            },
            rx,
        ))
    }
    pub fn view(&self) -> Result<View, String> {
        let c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        Ok(View {
            mode: c.mode,
            generation: c.generation,
            live_build: cfg!(feature = "live-trading"),
            live_allowed: self.policy.allow_live,
            wallet_unlocked: c.wallet.is_some(),
            wallet: c.wallet.as_ref().map(|w| w.address()),
            nonce_ready: c.nonce_ready,
            policy_ready: c.settings.validate(&self.policy).is_ok(),
            rpc_configured: self.policy.rpc_url.is_some(),
            wallet_configured: self.policy.wallet_key_file.is_some(),
            can_simulate: c.wallet.is_some() || self.policy.simulation_from.is_some(),
            amount_wei: c.settings.amount_wei.clone(),
            slippage_bps: c.settings.slippage_bps,
            fees: c.settings.fees.clone(),
            gas_price_ceiling_wei: self.policy.gas_price_cap_wei.clone(),
            gas_fee_ceiling_wei: FeeSettings::from_policy(&self.policy).gas_fee_cap_wei,
            gas_limit_cap: self.policy.gas_limit_cap,
            session_budget_wei: self.policy.session_budget_wei.clone(),
            daily_budget_wei: self.policy.daily_budget_wei.clone(),
            bnb_balance_wei: c.bnb_balance_wei.clone(),
            balance_at_ms: c.balance_at_ms,
            last_error: c.last_error.clone(),
            orders: self.store.orders(Some(100))?,
        })
    }
    /// Synchronous barrier invoked by the owning actor before delete/reset/release.
    pub fn cancel_token(&self, ca: &str, reason: &str) {
        if let Ok(mut c) = self.control.lock() {
            c.cancelled.insert(ca.to_string());
            c.bindings.remove(ca);
            if let Err(e) = self.store.cancel_unsent(Some(ca), reason) {
                c.mode = Mode::Disabled;
                c.generation += 1;
                c.last_error = Some(e);
            }
        }
    }
    pub fn pause(&self, reason: &str, lock_wallet: bool) -> Result<(), String> {
        let mut c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        self.pause_locked(&mut c, reason, lock_wallet).map(|_| ())
    }
    /// Sticky for this process: concurrent unlock/activation must never rearm it.
    pub fn stop_for_shutdown(&self) -> Result<(), String> {
        let mut c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        c.stopping = true;
        self.pause_locked(&mut c, "系统关闭：取消未广播买入并锁定钱包", true)
            .map(|_| ())
    }
    fn pause_locked(
        &self,
        c: &mut Control,
        reason: &str,
        lock_wallet: bool,
    ) -> Result<u64, String> {
        c.mode = Mode::Disabled;
        c.generation += 1;
        if lock_wallet {
            c.wallet = None;
            c.nonce_ready = false;
        }
        c.last_error = Some(reason.into());
        self.store.cancel_unsent(None, reason)?;
        Ok(c.generation)
    }
    pub fn update_settings(
        &self,
        settings: BuySettings,
        expected_generation: u64,
    ) -> Result<(), String> {
        settings.validate(&self.policy)?;
        let mut c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        if c.stopping {
            return Err("系统关闭中，不能修改买入参数".into());
        }
        if c.generation != expected_generation {
            return Err("页面状态已变化，请刷新后保存".into());
        }
        if c.mode != Mode::Disabled {
            return Err("请先关闭自动买入/模拟，再修改金额、滑点与 Gas 参数".into());
        }
        // Invalidates an in-flight activation request; do not change already-created orders.
        self.pause_locked(&mut c, "修改参数：取消旧未广播意图", false)?;
        self.store.save_buy_settings(&settings)?;
        c.settings = settings;
        c.last_error = Some("参数已保存；自动买入仍关闭".into());
        Ok(())
    }
    /// Unlocking does NOT enable buying. Failed chain/nonce validation leaves the wallet locked.
    pub async fn unlock(&self, password: Zeroizing<String>) -> Result<(), String> {
        let generation = {
            let mut c = self
                .control
                .lock()
                .map_err(|_| "trading control poisoned")?;
            if c.stopping {
                return Err("系统关闭中，不能解锁钱包".into());
            }
            self.pause_locked(&mut c, "正在本机解锁，买入仍关闭", true)?
        };
        let path = self
            .policy
            .wallet_key_file
            .clone()
            .ok_or("未配置 V2 钱包文件路径")?;
        let wallet = tokio::task::spawn_blocking(move || {
            Wallet::load_encrypted(std::path::Path::new(&path), &password)
        })
        .await
        .map_err(|_| "wallet decryption task failed")?
        .map_err(|e| e.to_string())?;
        let account = wallet.address();
        let (_, balance) = self.account_check(&account).await?;
        let mut c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        if c.stopping || generation != c.generation {
            return Err("解锁已被暂停/锁定取消".into());
        }
        c.wallet = Some(Arc::new(wallet));
        c.nonce_ready = true;
        c.bnb_balance_wei = Some(balance.to_string());
        c.balance_at_ms = Some(now_ms());
        c.last_error = Some("钱包已解锁；自动买入仍关闭".into());
        Ok(())
    }
    #[cfg(test)]
    pub async fn set_mode(&self, mode: Mode) -> Result<(), String> {
        self.set_mode_checked(mode, None, true).await
    }
    pub async fn set_mode_with_fees(
        &self,
        mode: Mode,
        expected_generation: Option<u64>,
        confirm_no_budget: bool,
        confirmed_fees: Option<FeeSettings>,
    ) -> Result<(), String> {
        if mode != Mode::Disabled {
            let c = self
                .control
                .lock()
                .map_err(|_| "trading control poisoned")?;
            if confirmed_fees.is_none() || confirmed_fees != c.settings.fees {
                return Err("必须确认当前完整 Gas 参数；旧网页请刷新，参数变更后请重新确认".into());
            }
            if expected_generation.is_none() {
                return Err("启用必须携带当前状态版本".into());
            }
        }
        self.set_mode_checked(mode, expected_generation, confirm_no_budget)
            .await
    }
    async fn set_mode_checked(
        &self,
        mode: Mode,
        expected_generation: Option<u64>,
        confirm_no_budget: bool,
    ) -> Result<(), String> {
        if mode == Mode::Disabled {
            return self.pause("已人工暂停买入", false);
        }
        let (generation, account) = {
            let mut c = self
                .control
                .lock()
                .map_err(|_| "trading control poisoned")?;
            if expected_generation.is_some_and(|g| g != c.generation) {
                return Err("页面状态已变化或开启请求已被关闭取消，请刷新后重试".into());
            }
            if c.stopping {
                return Err("系统关闭中，不能开启买入或模拟".into());
            }
            // Capture generation in the SAME critical section as pausing. A stop must win
            // whether it arrives before, during, or after the asynchronous activation preflight.
            let generation = self.pause_locked(
                &mut c,
                "切换模式：旧队列已取消，买入保持关闭直至校验完成",
                false,
            )?;
            c.settings.validate(&self.policy)?;
            if mode == Mode::Live && (!cfg!(feature = "live-trading") || !self.policy.allow_live) {
                return Err("实盘被构建/配置保护禁止；当前不可启用".into());
            }
            if mode == Mode::Live
                && !confirm_no_budget
                && (decimal(&self.policy.session_budget_wei)? == 0
                    || decimal(&self.policy.daily_budget_wei)? == 0)
            {
                return Err("未设置累计预算，必须明确确认后才能开启自动买入".into());
            }
            let account = c
                .wallet
                .as_ref()
                .map(|w| w.address())
                .or_else(|| self.policy.simulation_from.clone());
            (
                generation,
                account.ok_or("请设置模拟钱包地址或在本机解锁钱包")?,
            )
        };
        let account = address(&account)?;
        if mode == Mode::Live {
            self.no_unsettled()?;
            self.account_check(&account).await?;
            let finalized = self
                .rpc
                .call("eth_getBlockByNumber", json!(["finalized", false]))
                .await?;
            as_u64(quantity(
                finalized
                    .get("number")
                    .ok_or("交易节点不支持 finalized 区块")?,
            )?)?;
            self.store.trade_durability()?;
        }
        let mut c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        if c.stopping || generation != c.generation {
            return Err("启用请求已被暂停取消".into());
        }
        if mode == Mode::Live && (c.wallet.is_none() || !c.nonce_ready) {
            return Err("钱包尚未安全解锁".into());
        }
        c.mode = mode;
        c.generation += 1;
        c.consecutive_failures = 0;
        c.last_error = None;
        Ok(())
    }
    /// Called only for a newly promoted first-B token actually selected for a bound Slot.
    pub fn offer(&self, t: &PersistedToken, command_id: &str) -> Result<bool, String> {
        let mut c = self
            .control
            .lock()
            .map_err(|_| "trading control poisoned")?;
        if c.mode == Mode::Disabled {
            return Ok(false);
        }
        let policy = c.settings.policy(&self.policy);
        c.settings.validate(&self.policy)?;
        let reservation = c.settings.reservation(&self.policy)?;
        if c.cancelled.contains(&t.ca) {
            return Err("Token 已取消买入".into());
        }
        let slot = t.current_slot.clone().ok_or("买入必须先预留 Slot")?;
        let b_at = t.first_qualified_at_ms.ok_or("缺少首次 B 时间")?;
        let chain_at = t
            .b_timing
            .first_chain_at_ms
            .ok_or("缺少链上 B 条件完成时间，不追买")?;
        let now = now_ms();
        if !fresh(b_at, now, self.policy.max_signal_age_ms)
            || !fresh(chain_at, now, self.policy.max_signal_age_ms)
        {
            return Err("B 信号过期/历史补扫：不追买".into());
        }
        let rows = self.store.orders(None)?;
        if rows.iter().filter(|o| o.state.unsent()).count() >= self.policy.max_queue {
            return Err("交易队列已满".into());
        }
        if c.mode == Mode::Live {
            self.check_budget(&rows, reservation)?;
        }
        let order = Order {
            id: uuid::Uuid::new_v4().to_string(),
            intent: BuyIntent {
                ca: t.ca.clone(),
                platform: t.source_platform.clone(),
                round: 1,
                slot_id: slot,
                slot_command_id: command_id.into(),
                b_at_ms: b_at,
                chain_at_ms: t.b_timing.first_chain_at_ms,
                buy_tax_bps: t.buy_tax_bps,
                sell_tax_bps: t.sell_tax_bps,
            },
            mode: c.mode,
            generation: c.generation,
            session: self.session.clone(),
            state: OrderState::Queued,
            created_at_ms: now,
            updated_at_ms: now,
            quote_at_ms: None,
            signed_at_ms: None,
            broadcast_at_ms: None,
            included_at_ms: None,
            confirmed_at_ms: None,
            wallet: None,
            nonce: None,
            tx_hash: None,
            route: None,
            to: None,
            sent_value_wei: None,
            gas_price_wei: None,
            gas_limit: None,
            amount_wei: policy.amount_wei.clone(),
            slippage_bps: policy.slippage_bps,
            fees: c.settings.fees.clone(),
            reserved_wei: reservation.to_string(),
            expected_out_raw: None,
            min_out_raw: None,
            receipt_block: None,
            receipt_block_hash: None,
            received_raw: None,
            token_balance_raw: None,
            token_balance_at_ms: None,
            gas_paid_wei: None,
            cancel_requested: false,
            reason: None,
        };
        if let Err(e) = self
            .store
            .upsert_token_and_order(t, now / 1000, Some(&order))
        {
            c.last_error = Some(e.clone());
            return Err(e);
        }
        c.bindings.insert(t.ca.clone(), command_id.into());
        if self.jobs.try_send(order.id.clone()).is_err() {
            self.store
                .cancel_unsent(Some(&t.ca), "队列不可用，未发送")?;
            return Err("交易队列不可用".into());
        }
        Ok(true)
    }
    fn check_budget(&self, rows: &[Order], cost: u128) -> Result<(), String> {
        let mut day = 0u128;
        let mut session = 0u128;
        let today = now_ms() / 86_400_000; // documented UTC day, persisted across restarts
        for o in rows.iter().filter(|o| {
            o.mode == Mode::Live && (o.state != OrderState::Cancelled || o.nonce.is_some())
        }) {
            let amount = decimal(&o.reserved_wei)?;
            if o.created_at_ms / 86_400_000 == today {
                day = day.checked_add(amount).ok_or("budget overflow")?;
            }
            if o.session == self.session {
                session = session.checked_add(amount).ok_or("budget overflow")?;
            }
        }
        let day_limit = decimal(&self.policy.daily_budget_wei)?;
        let session_limit = decimal(&self.policy.session_budget_wei)?;
        if (day_limit != 0 && day.checked_add(cost).ok_or("budget overflow")? > day_limit)
            || (session_limit != 0
                && session.checked_add(cost).ok_or("budget overflow")? > session_limit)
        {
            return Err("累计预算不足（包括保守预留的 Gas）".into());
        }
        Ok(())
    }
    fn no_unsettled(&self) -> Result<(), String> {
        if self.store.unsettled_orders()?.iter().any(|o| {
            o.nonce.is_some()
                && matches!(
                    o.state,
                    OrderState::Signed
                        | OrderState::BroadcastUnknown
                        | OrderState::Submitted
                        | OrderState::Included
                        | OrderState::NeedsReview
                )
        }) {
            return Err("存在未结算/待核对交易；暂停新买入，先核对回执".into());
        }
        Ok(())
    }
    fn authorized(&self, c: &Control, o: &Order) -> Result<(), String> {
        if c.mode == Mode::Disabled
            || c.mode != o.mode
            || c.generation != o.generation
            || c.cancelled.contains(&o.intent.ca)
            || c.bindings.get(&o.intent.ca) != Some(&o.intent.slot_command_id)
            || !self.store.eligible_order(o)?
        {
            return Err("订单已被暂停、人工删除、释放或状态变化取消".into());
        }
        if !fresh(o.intent.b_at_ms, now_ms(), self.policy.max_signal_age_ms)
            || o.intent
                .chain_at_ms
                .is_some_and(|at| !fresh(at, now_ms(), self.policy.max_signal_age_ms))
        {
            return Err("B 信号已过期，取消追买".into());
        }
        Ok(())
    }
    async fn account_check(&self, account: &str) -> Result<(u64, u128), String> {
        let chain = quantity(&self.rpc.call("eth_chainId", json!([])).await?)?;
        if chain != 56 {
            return Err("交易 RPC 不是 BSC 主网".into());
        }
        let pending = self
            .rpc
            .call("eth_getTransactionCount", json!([account, "pending"]));
        let latest = self
            .rpc
            .call("eth_getTransactionCount", json!([account, "latest"]));
        let balance = self.rpc.call("eth_getBalance", json!([account, "pending"]));
        let (pending, latest, balance) = tokio::try_join!(pending, latest, balance)?;
        let pending = as_u64(quantity(&pending)?)?;
        if pending != as_u64(quantity(&latest)?)? {
            return Err("钱包已有待处理交易；禁止越过 nonce 继续发送".into());
        }
        Ok((pending, quantity(&balance)?))
    }
    pub async fn run(self, mut jobs: mpsc::Receiver<String>) {
        let mut poll = tokio::time::interval(std::time::Duration::from_secs(2));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _=poll.tick()=> { if let Err(e)=self.reconcile().await { self.note(e); } },
                id=jobs.recv()=> {
                    let Some(id)=id else { break; };
                    if let Err(e)=self.process(&id).await { self.failed(&id,e); }
                }
            }
        }
    }
    async fn process(&self, id: &str) -> Result<(), String> {
        let mut o = self.store.order(id)?.ok_or("order missing")?;
        if o.state != OrderState::Queued {
            return Err("非排队订单禁止重复执行".into());
        }
        let fees = o
            .fees
            .clone()
            .ok_or("旧订单缺少费用授权快照，禁止重新执行")?;
        fees.validate(&self.policy)?;
        let account = {
            let c = self
                .control
                .lock()
                .map_err(|_| "trading control poisoned")?;
            self.authorized(&c, &o)?;
            c.wallet
                .as_ref()
                .map(|w| w.address())
                .or_else(|| self.policy.simulation_from.clone())
                .ok_or("没有交易/模拟钱包地址")?
        };
        self.no_unsettled()?;
        o.state = OrderState::Preparing;
        self.store.save_order(&mut o, None)?;
        let (nonce, balance) = self.account_check(&account).await?;
        let head = self.rpc.call("eth_blockNumber", json!([])).await?;
        let block = as_u64(quantity(&head)?)?;
        let block_tag = format!("0x{block:x}");
        let header = self
            .rpc
            .call("eth_getBlockByNumber", json!([block_tag, false]))
            .await?;
        let chain_ms = as_u64(quantity(
            header.get("timestamp").ok_or("missing block timestamp")?,
        )?)?
        .checked_mul(1000)
        .ok_or("timestamp overflow")?;
        if !fresh(chain_ms, now_ms(), 10_000) {
            return Err("交易 RPC 区块已落后/时间异常".into());
        }
        let registry = routes::Registry::default();
        let plan = registry
            .prepare(routes::Request {
                intent: &o.intent,
                account: &account,
                amount: decimal(&o.amount_wei)?,
                slippage: o.slippage_bps.ok_or("订单缺少已确认的滑点参数，禁止重放")?,
                deadline: now_ms() / 1000 + self.policy.deadline_secs,
                block: &block_tag,
                rpc: &*self.rpc,
            })
            .await?;
        o.quote_at_ms = Some(plan.quote_at_ms);
        o.route = Some(plan.route.clone());
        o.to = Some(plan.to.clone());
        o.sent_value_wei = Some(plan.value.to_string());
        o.expected_out_raw = Some(plan.expected.to_string());
        o.min_out_raw = Some(plan.minimum.to_string());
        o.wallet = Some(account.clone());
        self.store.save_order(&mut o, None)?;
        let suggested = if fees.gas_mode == GasMode::Automatic {
            Some(quantity(&self.rpc.call("eth_gasPrice", json!([])).await?)?)
        } else {
            None
        };
        let gas_price = fees.select_price(suggested)?;
        let mut tx_call = routes::tx_call(&account, &plan);
        tx_call["gasPrice"] = json!(format!("0x{gas_price:x}"));
        tx_call["gas"] = json!(format!("0x{:x}", self.policy.gas_limit_cap));
        let (simulation, estimate) = tokio::try_join!(
            self.rpc.call("eth_call", json!([tx_call, block_tag])),
            self.rpc.call("eth_estimateGas", json!([tx_call]))
        )?;
        if simulation.as_str().and_then(crate::abi::unhex).is_none() {
            return Err("无效交易模拟结果".into());
        }
        let gas = as_u64(quantity(&estimate)?)?
            .checked_mul(125)
            .ok_or("gas overflow")?
            / 100;
        if gas < 21_000 || gas > self.policy.gas_limit_cap {
            return Err("估算 Gas 超出上限".into());
        }
        let gas_cost = fees.check_total(gas_price, gas)?;
        let required = gas_cost
            .checked_add(plan.value)
            .and_then(|v| v.checked_add(decimal(&self.policy.gas_reserve_wei).ok()?))
            .ok_or("cost overflow")?;
        if balance < required {
            return Err("BNB 余额不足（含 Gas 与余额保留）".into());
        }
        o.gas_price_wei = Some(gas_price.to_string());
        o.gas_limit = Some(gas);
        // Recheck pending nonce and spendable balance after potentially slow preflight.
        let (final_nonce, final_balance) = self.account_check(&account).await?;
        if final_nonce != nonce || final_balance < required {
            return Err("预检期间 nonce/余额发生变化，取消买入".into());
        }
        if !fresh(plan.quote_at_ms, now_ms(), self.policy.quote_ttl_ms) {
            return Err("报价过期，不签名/广播".into());
        }
        {
            let mut c = self
                .control
                .lock()
                .map_err(|_| "trading control poisoned")?;
            self.authorized(&c, &o)?;
            c.bnb_balance_wei = Some(final_balance.to_string());
            c.balance_at_ms = Some(now_ms());
            if o.mode == Mode::DryRun {
                o.state = OrderState::SimulationPassed;
                o.reason = Some("模拟通过；未签名，未广播，不代表实际成交或保证可卖出".into());
                self.store.save_order(&mut o, None)?;
                return Ok(());
            }
            if !cfg!(feature = "live-trading") || !self.policy.allow_live || !c.nonce_ready {
                return Err("实盘保护禁止广播".into());
            }
            self.no_unsettled()?;
            let wallet = c.wallet.as_ref().ok_or("wallet locked")?;
            if wallet.address() != account {
                return Err("wallet changed".into());
            }
            let to = hex::decode(&address(&plan.to)?[2..]).map_err(|_| "invalid destination")?;
            let signed = wallet
                .sign(&Transaction {
                    nonce,
                    gas_price,
                    gas_limit: gas,
                    to: to.try_into().map_err(|_| "invalid destination")?,
                    value: plan.value,
                    data: plan.data,
                })
                .map_err(|e| e.to_string())?;
            o.nonce = Some(nonce);
            o.tx_hash = Some(signed.hash.clone());
            o.signed_at_ms = Some(now_ms());
            o.state = OrderState::Signed;
            self.store.save_order(&mut o, Some(&signed.raw))?;
            // Durable point of no guaranteed cancellation; commit BEFORE calling the network.
            o.state = OrderState::BroadcastUnknown;
            o.broadcast_at_ms = Some(now_ms());
            self.store.save_order(&mut o, None)?;
        }
        self.broadcast_once(&mut o).await
    }
    async fn broadcast_once(&self, o: &mut Order) -> Result<(), String> {
        // This entire write branch is absent from ordinary builds.
        #[cfg(feature = "live-trading")]
        {
            // A pause/discard arriving between durable handoff and network invocation
            // still prevents this attempt; uncertain signed nonce remains for review.
            let still_authorized = {
                let c = self
                    .control
                    .lock()
                    .map_err(|_| "trading control poisoned")?;
                self.authorized(&c, o)
            };
            if let Err(reason) = still_authorized {
                o.state = OrderState::NeedsReview;
                o.reason = Some(format!("发送前取消：{reason}；保留签名交易并核对 nonce"));
                return self.store.save_order(o, None);
            }
            let raw = self.signed_raw(&o.id)?;
            match self.rpc.call("eth_sendRawTransaction", json!([raw])).await {
                Ok(hash)
                    if hash
                        .as_str()
                        .is_some_and(|h| o.tx_hash.as_deref() == Some(h)) =>
                {
                    o.state = OrderState::Submitted;
                    o.reason = Some("仅已提交，尚未确认成交".into());
                }
                Ok(_) => {
                    o.state = OrderState::NeedsReview;
                    o.reason = Some("RPC 返回哈希与本机签名哈希不一致；禁止重买".into());
                    self.pause("交易哈希不一致，已熔断", false)?;
                }
                Err(e) => {
                    o.reason = Some(format!("广播结果未知：{e}；按原哈希查证，不重新买入"));
                    self.pause("广播结果未知，暂停新买入", false)?;
                }
            }
            self.store.save_order(o, None)
        }
        #[cfg(not(feature = "live-trading"))]
        {
            let _ = o;
            Err("ordinary build cannot broadcast".into())
        }
    }
    #[cfg(feature = "live-trading")]
    fn signed_raw(&self, id: &str) -> Result<String, String> {
        self.store
            .0
            .lock()
            .map_err(|_| "database lock poisoned")?
            .query_row(
                "SELECT signed_raw FROM trade_orders WHERE id=?1",
                [id],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())
    }
    fn failed(&self, id: &str, error: String) {
        if let Ok(Some(mut o)) = self.store.order(id) {
            if o.state.unsent() {
                o.state = OrderState::Failed;
                o.reason = Some(error.clone());
            }
            // Never turn a potentially broadcast transaction into a retryable failure.
            if o.state == OrderState::Signed {
                o.state = OrderState::NeedsReview;
                o.reason = Some(error.clone());
            }
            let _ = self.store.save_order(&mut o, None);
        }
        if let Ok(mut c) = self.control.lock() {
            c.last_error = Some(error);
            c.consecutive_failures += 1;
            if c.consecutive_failures >= self.policy.max_consecutive_failures {
                c.mode = Mode::Disabled;
                c.generation += 1;
                let _ = self.store.cancel_unsent(None, "连续失败触发熔断");
            }
        }
    }
    fn note(&self, error: String) {
        if let Ok(mut c) = self.control.lock() {
            c.last_error = Some(error);
        }
    }
    async fn reconcile(&self) -> Result<(), String> {
        let orders = self.store.unsettled_orders()?;
        for mut o in orders
            .into_iter()
            .filter(|o| o.state.reconcile() && o.tx_hash.is_some())
        {
            self.reconcile_one(&mut o).await?;
        }
        Ok(())
    }
    async fn reconcile_one(&self, o: &mut Order) -> Result<(), String> {
        let hash = o.tx_hash.as_deref().ok_or("tx hash missing")?;
        let receipt = self
            .rpc
            .call("eth_getTransactionReceipt", json!([hash]))
            .await?;
        if receipt.is_null() {
            if o.receipt_block.is_some() {
                o.state = OrderState::Submitted;
                clear_inclusion(o);
                o.reason = Some("回执消失，可能发生重组；继续查证，不重买".into());
            }
            if o.broadcast_at_ms.is_some_and(|at| {
                now_ms().saturating_sub(at) > self.policy.pending_timeout_secs * 1000
            }) {
                o.state = OrderState::NeedsReview;
                o.reason = Some("长时间未确认；原交易可能仍待处理，禁止自动重买".into());
                self.pause("存在长时间未确认交易", false)?;
            }
            return self.store.save_order(o, None);
        }
        if receipt.get("transactionHash").and_then(|v| v.as_str()) != o.tx_hash.as_deref()
            || receipt
                .get("from")
                .and_then(|v| v.as_str())
                .map(str::to_ascii_lowercase)
                != o.wallet
            || receipt
                .get("to")
                .and_then(|v| v.as_str())
                .map(str::to_ascii_lowercase)
                != o.to
        {
            return Err("回执与已签名订单不匹配".into());
        }
        let block = as_u64(quantity(
            receipt.get("blockNumber").ok_or("receipt block missing")?,
        )?)?;
        let block_hash = receipt
            .get("blockHash")
            .and_then(|v| v.as_str())
            .ok_or("receipt block hash missing")?;
        let canonical = self
            .rpc
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{block:x}"), false]),
            )
            .await?;
        if canonical.get("hash").and_then(|v| v.as_str()) != Some(block_hash) {
            o.state = OrderState::Submitted;
            clear_inclusion(o);
            o.reason = Some("回执所在区块非当前主链，继续核对".into());
            return self.store.save_order(o, None);
        }
        let status = quantity(receipt.get("status").ok_or("receipt status missing")?)?;
        if status > 1 {
            return Err("invalid receipt status".into());
        }
        o.state = OrderState::Included;
        o.receipt_block = Some(block);
        o.receipt_block_hash = Some(block_hash.into());
        o.included_at_ms = Some(
            as_u64(quantity(
                canonical
                    .get("timestamp")
                    .ok_or("block timestamp missing")?,
            )?)?
            .checked_mul(1000)
            .ok_or("timestamp overflow")?,
        );
        let gas_used = quantity(receipt.get("gasUsed").ok_or("gasUsed missing")?)?;
        let price = match receipt.get("effectiveGasPrice") {
            Some(v) => quantity(v)?,
            None => decimal(o.gas_price_wei.as_deref().ok_or("gas price missing")?)?,
        };
        o.gas_paid_wei = Some(
            gas_used
                .checked_mul(price)
                .ok_or("gas cost overflow")?
                .to_string(),
        );
        o.reason = Some("已入块，正在等待确认数和 finalized 高度".into());
        self.store.save_order(o, None)?;
        let head = as_u64(quantity(
            &self.rpc.call("eth_blockNumber", json!([])).await?,
        )?)?;
        if head < block {
            return Err("RPC head behind receipt".into());
        }
        let finalized = self
            .rpc
            .call("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        let finalized_height = as_u64(quantity(
            finalized.get("number").ok_or("RPC 未返回 finalized 高度")?,
        )?)?;
        let chain_failed = status == 0
            && head - block + 1 >= self.policy.confirmations
            && finalized_height >= block;
        if head - block + 1 >= self.policy.confirmations && finalized_height >= block {
            o.confirmed_at_ms = Some(now_ms());
            if status == 0 {
                o.state = OrderState::Failed;
                o.reason = Some("链上执行失败；不自动重买".into());
            } else {
                o.state = OrderState::Confirmed;
                o.received_raw = Some(
                    received(
                        &receipt,
                        &o.intent.ca,
                        o.wallet.as_deref().ok_or("wallet missing")?,
                    )?
                    .to_string(),
                );
                let balance = routes::read(
                    &*self.rpc,
                    &o.intent.ca,
                    routes::call(
                        "balanceOf(address)",
                        vec![routes::addr_word(o.wallet.as_deref().unwrap())?],
                    ),
                    "latest",
                )
                .await?;
                o.token_balance_raw = Some(routes::uint(&balance, 0)?.to_string());
                o.token_balance_at_ms = Some(now_ms());
                o.reason =
                    Some("已确认；到账量按本交易 Transfer 净流入统计，余额为查询时链上余额".into());
                let received = decimal(o.received_raw.as_deref().ok_or("receipt amount missing")?)?;
                let minimum = decimal(o.min_out_raw.as_deref().unwrap_or("1"))?;
                if received < minimum {
                    self.pause("回执净流入低于订单最低值，请人工核对", false)?;
                    o.reason =
                        Some("回执成功但净流入低于订单最低值；已暂停新买入，不自动重买".into());
                } else if let Ok(mut c) = self.control.lock() {
                    c.consecutive_failures = 0;
                }
            }
        }
        self.store.save_order(o, None)?;
        if chain_failed {
            self.failed(&o.id, "链上执行失败，计入连续失败熔断".into());
        }
        Ok(())
    }
}
fn clear_inclusion(o: &mut Order) {
    o.receipt_block = None;
    o.receipt_block_hash = None;
    o.included_at_ms = None;
    o.confirmed_at_ms = None;
    o.received_raw = None;
    o.token_balance_raw = None;
    o.token_balance_at_ms = None;
    o.gas_paid_wei = None;
}
fn received(receipt: &serde_json::Value, token: &str, wallet: &str) -> Result<u128, String> {
    let signature = crate::abi::topic0("Transfer(address,address,uint256)");
    let mut incoming = 0u128;
    let mut outgoing = 0u128;
    for log in receipt
        .get("logs")
        .and_then(|v| v.as_array())
        .ok_or("receipt logs missing")?
    {
        if log
            .get("address")
            .and_then(|v| v.as_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
            != Some(token)
        {
            continue;
        }
        let Some(topics) = log.get("topics").and_then(|v| v.as_array()) else {
            continue;
        };
        if topics.len() != 3 || topics[0].as_str() != Some(&signature) {
            continue;
        }
        let topic_address = |v: &serde_json::Value| -> Result<String, String> {
            let bytes = crate::abi::unhex(v.as_str().ok_or("invalid Transfer topic")?)
                .ok_or("invalid Transfer topic")?;
            if bytes.len() != 32 {
                return Err("invalid Transfer topic size".into());
            }
            routes::addr(&bytes, 0)
        };
        let from = topic_address(&topics[1])?;
        let to = topic_address(&topics[2])?;
        let bytes = crate::abi::unhex(
            log.get("data")
                .and_then(|v| v.as_str())
                .ok_or("Transfer data missing")?,
        )
        .ok_or("invalid Transfer data")?;
        let amount = routes::uint(&bytes, 0)?;
        if to == wallet {
            incoming = incoming
                .checked_add(amount)
                .ok_or("receipt amount overflow")?;
        }
        if from == wallet {
            outgoing = outgoing
                .checked_add(amount)
                .ok_or("receipt amount overflow")?;
        }
    }
    incoming
        .checked_sub(outgoing)
        .ok_or_else(|| "negative net Token receipt; needs review".into())
}

#[cfg(test)]
mod tests;
