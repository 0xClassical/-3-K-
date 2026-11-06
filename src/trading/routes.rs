//! Trade adapters are deliberately separate from LaunchpadMonitor and A/B/C rules.
//! ABI sources and supported/unsupported paths are documented in TRADING-SAFETY.md.
use super::rpc::TradeRpc;
use super::types::{address, min_output, now_ms, BuyIntent};
use crate::{abi, config};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

pub const ROUTER: &str = "0x10ed43c718714eb63d5aa57b78b54704e256024e";
pub const WBNB: &str = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c";
pub const FACTORY: &str = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73";

pub struct Plan {
    pub route: String,
    pub to: String,
    pub value: u128,
    pub data: Vec<u8>,
    pub expected: u128,
    pub minimum: u128,
    pub quote_at_ms: u64,
}
pub struct Request<'a> {
    pub intent: &'a BuyIntent,
    pub account: &'a str,
    pub amount: u128,
    pub slippage: u64,
    pub deadline: u64,
    pub block: &'a str,
    pub rpc: &'a dyn TradeRpc,
}
type PrepareFuture<'a> = Pin<Box<dyn Future<Output = Result<Plan, String>> + Send + 'a>>;
pub trait TradeAdapter: Send + Sync {
    fn platform(&self) -> &str;
    fn prepare<'a>(&'a self, request: Request<'a>) -> PrepareFuture<'a>;
}
pub struct Registry {
    adapters: Vec<Box<dyn TradeAdapter>>,
}
impl Default for Registry {
    fn default() -> Self {
        let mut r = Self { adapters: vec![] };
        r.register(Flap);
        r.register(FourMeme);
        // GeniusFun trading ABI has not been independently validated: fail closed.
        r
    }
}
impl Registry {
    pub fn register<A: TradeAdapter + 'static>(&mut self, adapter: A) {
        self.adapters.push(Box::new(adapter));
    }
    pub async fn prepare(&self, request: Request<'_>) -> Result<Plan, String> {
        let a = self
            .adapters
            .iter()
            .find(|a| a.platform() == request.intent.platform)
            .ok_or_else(|| {
                format!(
                    "{} 尚无已验证的买入 Adapter；跳过交易",
                    request.intent.platform
                )
            })?;
        a.prepare(request).await
    }
}
pub struct Flap;
impl TradeAdapter for Flap {
    fn platform(&self) -> &str {
        "FLAP"
    }
    fn prepare<'a>(&'a self, r: Request<'a>) -> PrepareFuture<'a> {
        Box::pin(async move {
            let token = address(&r.intent.ca)?;
            let info = read(
                r.rpc,
                config::PORTAL,
                call("getTokenV8Safe(address)", vec![addr_word(&token)?]),
                r.block,
            )
            .await?;
            require_words(&info, 18)?;
            let status = uint(&info, 0)?;
            let quote = addr(&info, 9)?;
            let buy_tax = u64::try_from(uint(&info, 12)?).map_err(|_| "buy tax overflow")?;
            let sell_tax = u64::try_from(uint(&info, 13)?).map_err(|_| "sell tax overflow")?;
            check_tax(buy_tax, sell_tax)?;
            if quote != config::ZERO_ADDRESS {
                return Err("Flap 非 BNB 报价/多跳路径尚未验证".into());
            }
            if info[11 * 32..12 * 32].iter().any(|b| *b != 0) {
                return Err("Flap 扩展 Token 交易路径尚未验证".into());
            }
            match status {
                1 => {
                    if buy_tax != 0 || sell_tax != 0 {
                        return Err("Flap 内盘税币最低净到账语义尚未验证；跳过".into());
                    }
                    let at = now_ms(); // Start-of-quote, not end-of-slow-RPC, for freshness.
                    let result = read(
                        r.rpc,
                        config::PORTAL,
                        call(
                            "quoteExactInput((address,address,uint256))",
                            vec![
                                addr_word(config::ZERO_ADDRESS)?,
                                addr_word(&token)?,
                                word(r.amount),
                            ],
                        ),
                        r.block,
                    )
                    .await?;
                    let expected = uint(&result, 0)?;
                    let minimum = min_output(expected, 0, r.slippage)?;
                    // Dynamic tuple: argument offset, tuple fields, bytes offset, empty permitData.
                    let data = call(
                        "swapExactInput((address,address,uint256,uint256,bytes))",
                        vec![
                            word(32),
                            addr_word(config::ZERO_ADDRESS)?,
                            addr_word(&token)?,
                            word(r.amount),
                            word(minimum),
                            word(160),
                            word(0),
                        ],
                    );
                    Ok(Plan {
                        route: "FLAP_CURVE_NATIVE".into(),
                        to: config::PORTAL.into(),
                        value: r.amount,
                        data,
                        expected,
                        minimum,
                        quote_at_ms: at,
                    })
                }
                4 => pancake(r, &token, buy_tax, Some(addr(&info, 14)?)).await,
                _ => Err("Flap Token 当前不可交易".into()),
            }
        })
    }
}
pub struct FourMeme;
impl TradeAdapter for FourMeme {
    fn platform(&self) -> &str {
        "FOUR_MEME"
    }
    fn prepare<'a>(&'a self, r: Request<'a>) -> PrepareFuture<'a> {
        Box::pin(async move {
            let token = address(&r.intent.ca)?;
            let info = read(
                r.rpc,
                config::FOUR_MEME_HELPER3,
                call("getTokenInfo(address)", vec![addr_word(&token)?]),
                r.block,
            )
            .await?;
            require_words(&info, 12)?;
            let manager = addr(&info, 1)?;
            if uint(&info, 0)? != 2 || manager != config::FOUR_MEME_TOKEN_MANAGER2 {
                return Err("Four.meme 非 V2 / 未知 Manager：不猜测 ABI".into());
            }
            if addr(&info, 2)? != config::ZERO_ADDRESS {
                return Err("Four.meme 非 BNB 报价路径尚未验证".into());
            }
            let ti = read(
                r.rpc,
                &manager,
                call("_tokenInfos(address)", vec![addr_word(&token)?]),
                r.block,
            )
            .await?;
            require_words(&ti, 13)?;
            if addr(&ti, 0)? != token || addr(&ti, 1)? != config::ZERO_ADDRESS {
                return Err("Four.meme TokenInfo 不匹配".into());
            }
            let creator_type = (uint(&ti, 2)? >> 10) & 63;
            let (buy_tax, sell_tax) = four_tax(r.rpc, &token, creator_type, r.block).await?;
            check_tax(buy_tax, sell_tax)?;
            match uint(&info, 11)? {
                1 => pancake(r, &token, buy_tax, None).await,
                0 => {
                    if buy_tax != 0 || sell_tax != 0 {
                        return Err("Four.meme 内盘税币净到账语义尚未验证；跳过".into());
                    }
                    let at = now_ms();
                    let quote = read(
                        r.rpc,
                        config::FOUR_MEME_HELPER3,
                        call(
                            "tryBuy(address,uint256,uint256)",
                            vec![addr_word(&token)?, word(0), word(r.amount)],
                        ),
                        r.block,
                    )
                    .await?;
                    require_words(&quote, 8)?;
                    if addr(&quote, 0)? != manager
                        || addr(&quote, 1)? != config::ZERO_ADDRESS
                        || uint(&quote, 6)? != 0
                    {
                        return Err("Four.meme 报价路由/审批资产不匹配".into());
                    }
                    let value = uint(&quote, 5)?;
                    let funds = uint(&quote, 7)?;
                    if value == 0 || value > r.amount || funds == 0 {
                        return Err("Four.meme 报价超过单笔 BNB 上限".into());
                    }
                    let expected = uint(&quote, 2)?;
                    let minimum = min_output(expected, 0, r.slippage)?;
                    let data = call(
                        "buyTokenAMAP(address,uint256,uint256)",
                        vec![addr_word(&token)?, word(funds), word(minimum)],
                    );
                    Ok(Plan {
                        route: "FOUR_MEME_V2_CURVE_NATIVE".into(),
                        to: manager,
                        value,
                        data,
                        expected,
                        minimum,
                        quote_at_ms: at,
                    })
                }
                _ => Err("invalid liquidityAdded flag".into()),
            }
        })
    }
}
async fn four_tax(
    rpc: &dyn TradeRpc,
    token: &str,
    kind: u128,
    block: &str,
) -> Result<(u64, u64), String> {
    let read_rate = |sig: &str| call(sig, vec![]);
    match kind {
        5 => {
            let raw = read(rpc, token, read_rate("feeRate()"), block).await?;
            let tax = u64::try_from(uint(&raw, 0)?).map_err(|_| "tax overflow")?;
            Ok((tax, tax))
        }
        8 | 9 => {
            let buy = read(rpc, token, read_rate("feeRateBuy()"), block).await?;
            let sell = read(rpc, token, read_rate("feeRateSell()"), block).await?;
            let buy = uint(&buy, 0)?.checked_mul(100).ok_or("tax overflow")?;
            let sell = uint(&sell, 0)?.checked_mul(100).ok_or("tax overflow")?;
            Ok((
                u64::try_from(buy).map_err(|_| "tax overflow")?,
                u64::try_from(sell).map_err(|_| "tax overflow")?,
            ))
        }
        0..=7 => Ok((0, 0)), // authoritative template is not a documented tax type
        _ => Err("未知 Four.meme creatorType；跳过交易".into()),
    }
}
async fn pancake(
    r: Request<'_>,
    token: &str,
    tax: u64,
    expected_pool: Option<String>,
) -> Result<Plan, String> {
    let pair = read(
        r.rpc,
        FACTORY,
        call(
            "getPair(address,address)",
            vec![addr_word(WBNB)?, addr_word(token)?],
        ),
        r.block,
    )
    .await?;
    let pair = addr(&pair, 0)?;
    if pair == config::ZERO_ADDRESS || expected_pool.is_some_and(|p| p != pair) {
        return Err("没有匹配的 Pancake V2 池；V3/Infinity 等迁移路径尚未实现".into());
    }
    let at = now_ms();
    let quote = read(
        r.rpc,
        ROUTER,
        call(
            "getAmountsOut(uint256,address[])",
            vec![
                word(r.amount),
                word(64),
                word(2),
                addr_word(WBNB)?,
                addr_word(token)?,
            ],
        ),
        r.block,
    )
    .await?;
    require_words(&quote, 4)?;
    if uint(&quote, 0)? != 32 || uint(&quote, 1)? != 2 || uint(&quote, 2)? != r.amount {
        return Err("Pancake quote ABI/path mismatch".into());
    }
    let expected = uint(&quote, 3)?;
    let minimum = min_output(expected, tax, r.slippage)?;
    let data = call(
        "swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)",
        vec![
            word(minimum),
            word(128),
            addr_word(r.account)?,
            word(r.deadline as u128),
            word(2),
            addr_word(WBNB)?,
            addr_word(token)?,
        ],
    );
    Ok(Plan {
        route: "PANCAKE_V2_NATIVE_FEE_SUPPORT".into(),
        to: ROUTER.into(),
        value: r.amount,
        data,
        expected,
        minimum,
        quote_at_ms: at,
    })
}
fn check_tax(buy: u64, sell: u64) -> Result<(), String> {
    if buy > config::MAX_TAX_BPS || sell > config::MAX_TAX_BPS {
        Err("买入前复查税率超过 4%".into())
    } else {
        Ok(())
    }
}
pub async fn read(
    rpc: &dyn TradeRpc,
    to: &str,
    data: Vec<u8>,
    block: &str,
) -> Result<Vec<u8>, String> {
    let v = rpc
        .call(
            "eth_call",
            json!([{"to":to,"data":format!("0x{}",hex::encode(data))},block]),
        )
        .await?;
    abi::unhex(v.as_str().ok_or("eth_call returned non-string")?)
        .ok_or_else(|| "invalid ABI hex".into())
}
pub fn call(signature: &str, args: Vec<[u8; 32]>) -> Vec<u8> {
    let mut data = abi::unhex(&abi::selector(signature)).expect("selector");
    for arg in args {
        data.extend(arg);
    }
    data
}
pub fn word(v: u128) -> [u8; 32] {
    let mut w = [0; 32];
    w[16..].copy_from_slice(&v.to_be_bytes());
    w
}
pub fn addr_word(v: &str) -> Result<[u8; 32], String> {
    let v = address(v)?;
    let mut out = [0; 32];
    out[12..].copy_from_slice(&hex::decode(&v[2..]).map_err(|_| "invalid address")?);
    Ok(out)
}
pub fn require_words(data: &[u8], n: usize) -> Result<(), String> {
    if data.len() < n * 32 || data.len() % 32 != 0 {
        Err("short/malformed ABI result".into())
    } else {
        Ok(())
    }
}
pub fn uint(data: &[u8], i: usize) -> Result<u128, String> {
    require_words(data, i + 1)?;
    let word = &data[i * 32..(i + 1) * 32];
    if word[..16].iter().any(|b| *b != 0) {
        return Err("uint256 exceeds u128; refusing truncation".into());
    }
    Ok(u128::from_be_bytes(word[16..].try_into().unwrap()))
}
pub fn addr(data: &[u8], i: usize) -> Result<String, String> {
    require_words(data, i + 1)?;
    let w = &data[i * 32..(i + 1) * 32];
    if w[..12].iter().any(|b| *b != 0) {
        return Err("invalid ABI address".into());
    }
    Ok(format!("0x{}", hex::encode(&w[12..])))
}
pub fn tx_call(account: &str, p: &Plan) -> Value {
    json!({"from":account,"to":p.to,"value":format!("0x{:x}",p.value),"data":format!("0x{}",hex::encode(&p.data))})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uint_does_not_saturate_and_dynamic_tuple_offsets_are_correct() {
        let mut huge = [0; 32];
        huge[0] = 1;
        assert!(uint(&huge, 0).is_err());
        let d = call(
            "swapExactInput((address,address,uint256,uint256,bytes))",
            vec![
                word(32),
                [0; 32],
                [0; 32],
                word(5),
                word(8),
                word(160),
                word(0),
            ],
        );
        assert_eq!(uint(&d[4..], 0).unwrap(), 32);
        assert_eq!(uint(&d[4..], 5).unwrap(), 160);
        assert_eq!(uint(&d[4..], 6).unwrap(), 0);
    }
}
