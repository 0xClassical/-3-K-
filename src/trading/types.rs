use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    #[default]
    Disabled,
    DryRun,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderState {
    Queued,
    Preparing,
    SimulationPassed,
    Signed,
    BroadcastUnknown,
    Submitted,
    Included,
    Confirmed,
    Failed,
    Cancelled,
    NeedsReview,
}
impl OrderState {
    pub fn unsent(self) -> bool {
        matches!(self, Self::Queued | Self::Preparing)
    }
    pub fn reconcile(self) -> bool {
        matches!(
            self,
            Self::BroadcastUnknown | Self::Submitted | Self::Included | Self::NeedsReview
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuyIntent {
    pub ca: String,
    pub platform: String,
    pub round: u8,
    pub slot_id: String,
    pub slot_command_id: String,
    pub b_at_ms: u64,
    pub chain_at_ms: Option<u64>,
    pub buy_tax_bps: u64,
    pub sell_tax_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub id: String,
    pub intent: BuyIntent,
    pub mode: Mode,
    pub generation: u64,
    pub session: String,
    pub state: OrderState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub quote_at_ms: Option<u64>,
    pub signed_at_ms: Option<u64>,
    pub broadcast_at_ms: Option<u64>,
    pub included_at_ms: Option<u64>,
    pub confirmed_at_ms: Option<u64>,
    pub wallet: Option<String>,
    pub nonce: Option<u64>,
    pub tx_hash: Option<String>,
    pub route: Option<String>,
    pub to: Option<String>,
    pub sent_value_wei: Option<String>,
    pub gas_price_wei: Option<String>,
    pub gas_limit: Option<u64>,
    pub amount_wei: String,
    /// Immutable per-order snapshot; old orders deserialize without it and cannot be replayed.
    #[serde(default)]
    pub slippage_bps: Option<u64>,
    /// Immutable fee authorization; legacy orders without it must not be replayed.
    #[serde(default)]
    pub fees: Option<FeeSettings>,
    /// Conservative all-in budget reservation, never mistaken for actual spent.
    pub reserved_wei: String,
    pub expected_out_raw: Option<String>,
    pub min_out_raw: Option<String>,
    pub receipt_block: Option<u64>,
    pub receipt_block_hash: Option<String>,
    pub received_raw: Option<String>,
    pub token_balance_raw: Option<String>,
    pub token_balance_at_ms: Option<u64>,
    pub gas_paid_wei: Option<String>,
    pub cancel_requested: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GasMode {
    Automatic,
    Fixed,
}

/// Non-secret dashboard settings for legacy gasPrice transactions, not a builder tip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FeeSettings {
    pub gas_mode: GasMode,
    /// 11000 = suggested price * 1.10; integer arithmetic only.
    pub gas_multiplier_bps: u64,
    pub fixed_gas_price_wei: Option<String>,
    pub gas_price_cap_wei: String,
    /// Maximum authorized gasPrice * estimated/padded gasLimit, excluding buy value.
    pub gas_fee_cap_wei: String,
}
impl FeeSettings {
    pub fn from_policy(base: &Policy) -> Self {
        let ceiling = decimal(&base.gas_price_cap_wei)
            .ok()
            .and_then(|price| price.checked_mul(base.gas_limit_cap as u128));
        Self {
            gas_mode: GasMode::Automatic,
            gas_multiplier_bps: 11_000,
            fixed_gas_price_wei: None,
            gas_price_cap_wei: base.gas_price_cap_wei.clone(),
            gas_fee_cap_wei: ceiling.unwrap_or(0).to_string(),
        }
    }
    pub fn validate(&self, base: &Policy) -> Result<(), String> {
        let price_cap = decimal(&self.gas_price_cap_wei)?;
        let fee_cap = decimal(&self.gas_fee_cap_wei)?;
        let base_price_cap = decimal(&base.gas_price_cap_wei)?;
        let base_fee_cap = base_price_cap
            .checked_mul(base.gas_limit_cap as u128)
            .ok_or("Gas 保护上限溢出")?;
        if price_cap == 0 || price_cap > base_price_cap || fee_cap == 0 || fee_cap > base_fee_cap {
            return Err("Gas 上限必须大于零且不超过后端配置的保护上限".into());
        }
        if !(10_000..=50_000).contains(&self.gas_multiplier_bps) {
            return Err("Gas 加价率必须在 0% 至 400% 之间".into());
        }
        match (self.gas_mode, self.fixed_gas_price_wei.as_deref()) {
            (GasMode::Automatic, None) => {}
            (GasMode::Fixed, Some(raw)) if decimal(raw)? > 0 && decimal(raw)? <= price_cap => {}
            _ => {
                return Err(
                    "固定 Gas 价格必须大于零且不超过价格上限；自动模式不接受固定价格".into(),
                )
            }
        }
        Ok(())
    }
    pub fn select_price(&self, suggested: Option<u128>) -> Result<u128, String> {
        let price = match self.gas_mode {
            GasMode::Fixed => decimal(
                self.fixed_gas_price_wei
                    .as_deref()
                    .ok_or("缺少固定 Gas 价格")?,
            )?,
            GasMode::Automatic => {
                let suggested = suggested.filter(|p| *p > 0).ok_or("节点 Gas 建议价无效")?;
                let product = suggested
                    .checked_mul(self.gas_multiplier_bps as u128)
                    .ok_or("Gas 价格溢出")?;
                // Round up by at most one wei; never turn a positive price into zero.
                (product / 10_000)
                    .checked_add(u128::from(product % 10_000 != 0))
                    .ok_or("Gas 价格溢出")?
            }
        };
        if price == 0 || price > decimal(&self.gas_price_cap_wei)? {
            return Err("Gas 价格超过上限或无效；不自动提高上限".into());
        }
        Ok(price)
    }
    pub fn check_total(&self, price: u128, gas: u64) -> Result<u128, String> {
        let total = price.checked_mul(gas as u128).ok_or("Gas 手续费溢出")?;
        if total == 0 || total > decimal(&self.gas_fee_cap_wei)? {
            return Err("预计最高 Gas 手续费超过单笔上限；未签名、未广播".into());
        }
        Ok(total)
    }
    pub fn reservation(&self, base: &Policy) -> Result<u128, String> {
        let by_price = decimal(&self.gas_price_cap_wei)?
            .checked_mul(base.gas_limit_cap as u128)
            .ok_or("Gas 预留溢出")?;
        Ok(by_price.min(decimal(&self.gas_fee_cap_wei)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct Policy {
    pub allow_live: bool,
    pub wallet_key_file: Option<String>,
    pub rpc_url: Option<String>,
    pub simulation_from: Option<String>,
    pub amount_wei: String,
    pub slippage_bps: Option<u64>,
    pub session_budget_wei: String,
    pub daily_budget_wei: String,
    pub gas_price_cap_wei: String,
    pub gas_limit_cap: u64,
    pub gas_reserve_wei: String,
    pub quote_ttl_ms: u64,
    pub max_signal_age_ms: u64,
    pub deadline_secs: u64,
    pub confirmations: u64,
    pub pending_timeout_secs: u64,
    pub max_consecutive_failures: u32,
    pub max_queue: usize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            allow_live: false,
            wallet_key_file: None,
            rpc_url: None,
            simulation_from: None,
            amount_wei: "5000000000000000".into(),
            slippage_bps: None,
            session_budget_wei: "0".into(),
            daily_budget_wei: "0".into(),
            gas_price_cap_wei: "5000000000".into(),
            gas_limit_cap: 600_000,
            gas_reserve_wei: "2000000000000000".into(),
            quote_ttl_ms: 5_000,
            max_signal_age_ms: 10_000,
            deadline_secs: 60,
            confirmations: 3,
            pending_timeout_secs: 120,
            max_consecutive_failures: 3,
            max_queue: 16,
        }
    }
}
impl Policy {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        if content.len() > 8192 {
            return Err("trading config too large".into());
        }
        serde_json::from_str(&content).map_err(|e| format!("交易配置错误: {e}"))
    }
    pub fn validate(&self) -> Result<(), String> {
        if decimal(&self.amount_wei)? == 0
            || !(21_000..=2_000_000).contains(&self.gas_limit_cap)
            || decimal(&self.gas_price_cap_wei)? == 0
            || !(500..=30_000).contains(&self.quote_ttl_ms)
            || !(1_000..=60_000).contains(&self.max_signal_age_ms)
            || !(10..=120).contains(&self.deadline_secs)
            || !(1..=64).contains(&self.confirmations)
            || !(10..=600).contains(&self.pending_timeout_secs)
            || !(1..=100).contains(&self.max_consecutive_failures)
            || !(1..=64).contains(&self.max_queue)
        {
            return Err("交易参数超出安全范围".into());
        }
        match self.slippage_bps {
            Some(v) if v < 10_000 => {}
            _ => return Err("必须明确配置 slippageBps，且小于 10000".into()),
        }
        // Explicitly requested: zero = no cumulative budget limit, not an activation switch.
        for budget in [&self.session_budget_wei, &self.daily_budget_wei] {
            let limit = decimal(budget)?;
            if limit != 0 && limit < self.reservation()? {
                return Err("已设置的累计预算不足以覆盖单笔金额和 Gas 上限".into());
            }
        }
        decimal(&self.gas_reserve_wei)?;
        Ok(())
    }
    pub fn reservation(&self) -> Result<u128, String> {
        decimal(&self.gas_price_cap_wei)?
            .checked_mul(self.gas_limit_cap as u128)
            .and_then(|gas| decimal(&self.amount_wei).ok()?.checked_add(gas))
            .ok_or_else(|| "金额溢出".into())
    }
}

/// Only non-secret, user-editable parameters are persisted. Never persist the armed mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BuySettings {
    pub amount_wei: String,
    pub slippage_bps: Option<u64>,
    /// Missing only in older persisted settings; migrated from existing policy at startup.
    #[serde(default)]
    pub fees: Option<FeeSettings>,
}
impl BuySettings {
    pub fn from_policy(policy: &Policy) -> Self {
        Self {
            amount_wei: policy.amount_wei.clone(),
            slippage_bps: policy.slippage_bps,
            fees: Some(FeeSettings::from_policy(policy)),
        }
    }
    pub fn policy(&self, base: &Policy) -> Policy {
        let mut p = base.clone();
        p.amount_wei = self.amount_wei.clone();
        p.slippage_bps = self.slippage_bps;
        p
    }
    pub fn validate(&self, base: &Policy) -> Result<(), String> {
        self.policy(base).validate()?;
        self.fees
            .as_ref()
            .ok_or("费用参数不完整，请刷新页面并保存")?
            .validate(base)
    }
    pub fn reservation(&self, base: &Policy) -> Result<u128, String> {
        decimal(&self.amount_wei)?
            .checked_add(
                self.fees
                    .as_ref()
                    .ok_or("缺少费用参数")?
                    .reservation(base)?,
            )
            .ok_or_else(|| "买入金额与 Gas 预留溢出".into())
    }
}

pub fn decimal(v: &str) -> Result<u128, String> {
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid decimal amount".into());
    }
    v.parse()
        .map_err(|_| "amount exceeds u128; refusing to truncate".into())
}
pub fn quantity(v: &serde_json::Value) -> Result<u128, String> {
    let s = v
        .as_str()
        .and_then(|v| v.strip_prefix("0x"))
        .ok_or("invalid RPC quantity")?;
    if s.is_empty() || s.len() > 32 {
        return Err("invalid/overflowing RPC quantity".into());
    }
    u128::from_str_radix(s, 16).map_err(|_| "invalid RPC quantity".into())
}
pub fn as_u64(v: u128) -> Result<u64, String> {
    u64::try_from(v).map_err(|_| "quantity exceeds u64".into())
}
pub fn address(v: &str) -> Result<String, String> {
    let v = v.trim().to_ascii_lowercase();
    if v.len() != 42 || !v.starts_with("0x") || !v[2..].bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid address".into());
    }
    Ok(v)
}
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
pub fn fresh(at: u64, now: u64, ttl: u64) -> bool {
    at <= now.saturating_add(2000) && now.saturating_sub(at) <= ttl
}
pub fn min_output(quote: u128, tax_bps: u64, slippage_bps: u64) -> Result<u128, String> {
    if tax_bps > 400 || slippage_bps >= 10_000 {
        return Err("tax/slippage rejected".into());
    }
    // Divide first to avoid intermediate overflow without losing precision.
    fn bps(v: u128, factor: u128) -> Result<u128, String> {
        (v / 10_000)
            .checked_mul(factor)
            .and_then(|a| a.checked_add((v % 10_000) * factor / 10_000))
            .ok_or_else(|| "quote overflow".into())
    }
    let out = bps(
        bps(quote, 10_000 - tax_bps as u128)?,
        10_000 - slippage_bps as u128,
    )?;
    if out == 0 {
        Err("zero minimum output forbidden".into())
    } else {
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fee_modes_and_caps_use_exact_checked_integer_math() {
        let base = Policy::default();
        let mut f = FeeSettings::from_policy(&base);
        f.validate(&base).unwrap();
        assert_eq!(f.select_price(Some(1_000_000_000)).unwrap(), 1_100_000_000);
        assert_eq!(f.select_price(Some(1)).unwrap(), 2);
        assert!(f.select_price(Some(0)).is_err());
        assert!(f.select_price(Some(u128::MAX)).is_err());
        f.gas_price_cap_wei = "1000000000".into();
        assert!(f.select_price(Some(1_000_000_000)).is_err());
        f.gas_mode = GasMode::Fixed;
        f.fixed_gas_price_wei = Some("900000001".into());
        f.validate(&base).unwrap();
        assert_eq!(f.select_price(None).unwrap(), 900_000_001);
        f.gas_fee_cap_wei = "18900000021000".into();
        assert_eq!(
            f.check_total(900_000_001, 21_000).unwrap(),
            18_900_000_021_000
        );
        f.gas_fee_cap_wei = "18900000020999".into();
        assert!(f.check_total(900_000_001, 21_000).is_err());
        assert!(f.check_total(u128::MAX, 21_000).is_err());
    }
    #[test]
    fn fee_validation_cannot_raise_policy_ceiling_or_accept_partial_settings() {
        let base = Policy::default();
        let initial = FeeSettings::from_policy(&base);
        for (price, total, multiplier) in [
            ("0", "1", 11000),
            ("5000000001", "1", 11000),
            ("1", "0", 11000),
            ("1", "3000000000000001", 11000),
            ("1", "1", 9999),
            ("1", "1", 50001),
        ] {
            let f = FeeSettings {
                gas_price_cap_wei: price.into(),
                gas_fee_cap_wei: total.into(),
                gas_multiplier_bps: multiplier,
                ..initial.clone()
            };
            assert!(f.validate(&base).is_err());
        }
        let mut fixed = initial.clone();
        fixed.gas_mode = GasMode::Fixed;
        for raw in [
            None,
            Some("0"),
            Some("5000000001"),
            Some("0.1"),
            Some("1e9"),
            Some("-1"),
        ] {
            fixed.fixed_gas_price_wei = raw.map(str::to_string);
            assert!(fixed.validate(&base).is_err());
        }
        let mut automatic = initial;
        automatic.fixed_gas_price_wei = Some("1".into());
        assert!(automatic.validate(&base).is_err());
        assert!(
            serde_json::from_value::<FeeSettings>(serde_json::json!({"gasMode":"AUTOMATIC"}))
                .is_err()
        );
    }
    #[test]
    fn output_protection_and_overflow_are_fail_closed() {
        assert_eq!(min_output(1_000_000, 400, 500).unwrap(), 912_000);
        assert!(min_output(100, 0, 10_000).is_err());
        assert!(min_output(0, 0, 10).is_err());
        assert!(min_output(1, 400, 1).is_err());
        assert_eq!(min_output(u128::MAX, 0, 0).unwrap(), u128::MAX);
        assert!(quantity(&serde_json::json!("0x100000000000000000000000000000000")).is_err());
    }
    #[test]
    fn default_cannot_be_armed_and_stale_signals_rejected() {
        assert!(!Policy::default().allow_live);
        assert!(Policy::default().validate().is_err());
        assert!(!fresh(100, 11_000, 10_000));
        assert!(!fresh(20_000, 10_000, 10_000));
    }
    #[test]
    fn budgets_are_optional_but_amount_slippage_and_gas_guards_are_not() {
        let mut p = Policy {
            slippage_bps: Some(100),
            ..Policy::default()
        };
        assert!(p.validate().is_ok());
        p.session_budget_wei = "1".into();
        assert!(p.validate().is_err());
        p.session_budget_wei = "0".into();
        for amount in [
            "0",
            "-1",
            "0.1",
            "1e18",
            "340282366920938463463374607431768211456",
        ] {
            p.amount_wei = amount.into();
            assert!(p.validate().is_err());
        }
        p.amount_wei = "5000000000000000".into();
        p.slippage_bps = Some(10000);
        assert!(p.validate().is_err());
        p.slippage_bps = Some(0);
        assert!(p.validate().is_ok());
    }
}
