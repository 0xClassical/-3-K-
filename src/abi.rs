//! 极简 ABI 解码：只实现本项目用到的类型，不引入 web3 全家桶。
//!
//! 覆盖 uint256 / address / string / uint16 / bool，以及 eth_call 返回值解析。
//! 监控字段沿用链上验证；新增交易字段来源和验证范围见 TRADING-SAFETY.md。

use tiny_keccak::{Hasher, Keccak};

/// 事件签名的 keccak256，即日志的 topic0。
pub fn topic0(signature: &str) -> String {
    let mut k = Keccak::v256();
    k.update(signature.as_bytes());
    let mut out = [0u8; 32];
    k.finalize(&mut out);
    format!("0x{}", hex(&out))
}

/// 函数选择器（前 4 字节）。
pub fn selector(signature: &str) -> String {
    let t = topic0(signature);
    t[..10].to_string()
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// "0x1a2b" -> [0x1a, 0x2b]。奇数长度或含非十六进制字符时返回 None。
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() % 2 != 0 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i < b.len() {
        let hi = nibble(b[i])?;
        let lo = nibble(b[i + 1])?;
        out.push(hi << 4 | lo);
        i += 2;
    }
    Some(out)
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// 一个 32 字节 ABI 字。
#[derive(Clone, Copy, Debug)]
pub struct Word(pub [u8; 32]);

impl Word {
    pub fn checked_u128(&self) -> Option<u128> {
        if self.0[..16].iter().any(|b| *b != 0) {
            return None;
        }
        Some(u128::from_be_bytes(self.0[16..].try_into().ok()?))
    }
    /// 按 u128 读取低 16 字节；高 16 字节非零时饱和为 u128::MAX。
    /// Flap 实测最大值约 6.3e27，远在 u128（3.4e38）范围内。
    pub fn as_u128(&self) -> u128 {
        if self.0[..16].iter().any(|&b| b != 0) {
            return u128::MAX;
        }
        let mut v: u128 = 0;
        for &b in &self.0[16..] {
            v = (v << 8) | b as u128;
        }
        v
    }

    /// 取后 20 字节作为地址，统一小写带 0x 前缀。
    pub fn as_address(&self) -> String {
        format!("0x{}", hex(&self.0[12..]))
    }
}

/// Four.meme V2 authoritative creator type: template bits 10..15.
pub fn four_creator_type(data: &[u8], token: &str) -> Option<u8> {
    let w = words(data);
    if data.len() % 32 != 0
        || w.len() < 13
        || w[0].0[..12].iter().any(|b| *b != 0)
        || w[0].as_address() != token.to_ascii_lowercase()
    {
        return None;
    }
    let kind = ((w[2].checked_u128()? >> 10) & 63) as u8;
    (kind <= 9).then_some(kind)
}

/// 把 data 切成 32 字节字。长度不足 32 的整数倍时丢弃尾部残块。
pub fn words(data: &[u8]) -> Vec<Word> {
    let mut out = Vec::with_capacity(data.len() / 32);
    for chunk in data.chunks_exact(32) {
        let mut w = [0u8; 32];
        w.copy_from_slice(chunk);
        out.push(Word(w));
    }
    out
}

/// 按 ABI 规则读取 string：offset 处是 [长度字][数据...]。
pub fn read_string(data: &[u8], offset: usize) -> String {
    let Some(len_word) = data.get(offset..offset + 32) else {
        return String::new();
    };
    let mut lw = [0u8; 32];
    lw.copy_from_slice(len_word);
    let len = Word(lw).as_u128() as usize;
    let start = offset + 32;
    match data.get(start..start + len) {
        Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        None => String::new(),
    }
}

// ------------------------------------------------------------- 事件负载

/// TokenCreated 解码结果。
#[derive(Debug, Clone)]
pub struct Created {
    pub ts: u64,
    pub creator: String,
    pub token: String,
    pub name: String,
    pub symbol: String,
}

/// 解码建币事件。布局：ts, creator, nonce, token, name(off), symbol(off), meta(off)。
/// nonce 与 IPFS meta CID 本项目用不到，解码后直接丢弃。
pub fn decode_token_created(data: &[u8]) -> Option<Created> {
    let w = words(data);
    if w.len() < 7 {
        return None;
    }
    Some(Created {
        ts: w[0].as_u128() as u64,
        creator: w[1].as_address(),
        token: w[3].as_address(),
        name: read_string(data, w[4].as_u128() as usize),
        symbol: read_string(data, w[5].as_u128() as usize),
    })
}

/// TokenBought / TokenSold 解码结果。两者字段布局完全相同。
///
/// 字段依次为 ts, token, wallet, tokenAmount, quoteAmount, fee, price。
/// 第 7 个字段（合约自带的瞬时价格）本项目**不使用**，原因见 `unit_price`。
#[derive(Debug, Clone)]
pub struct Trade {
    pub ts: u64,
    pub token: String,
    pub wallet: String,
    /// 成交的代币数量 Amount（18 位精度原始值）。
    pub token_amount: u128,
    /// 成交的报价金额 Q（报价代币精度，BNB 为 18 位）。
    pub quote_amount: u128,
    /// 协议手续费 Fee，与 Q 同精度。实测恒为 Q 的 1.00%。
    pub fee: u128,
}

/// 有效成交额（与 Q 同精度的原始值）：
///
/// - BUY：`Q - Fee`。买方付出的 Q 中有一部分是手续费，真正推动联合曲线的是
///   扣费后的部分，单价须按扣费后计。
/// - SELL：`Q + Fee`。卖方到手的 Q 已是扣过费的净额，还原到扣费前才是曲线口径。
///
/// 用饱和运算：链上正常不会出现 Fee > Q，真出现时按 0 计，而不是让 u128
/// 下溢 panic 或回绕成天文数字。
pub fn effective_quote(t: &Trade, is_buy: bool) -> u128 {
    if is_buy {
        t.quote_amount.saturating_sub(t.fee)
    } else {
        t.quote_amount.saturating_add(t.fee)
    }
}

/// 单币成交价：`price = 有效成交额 / Amount`。
///
/// - BUY  price = (Q - Fee) / Amount
/// - SELL price = (Q + Fee) / Amount
///
/// Quote 为 BNB 时，该值即 BNB/Token。分子分母同为 18 位精度，直接相除即为单价。
///
/// 不使用事件第 7 个字段（合约瞬时价格）：实测它是不含费的 `Q/Amount` 口径，
/// 与本项目的有效成交额口径相差正好一个 Fee 项（1.1331e-08 vs 1.1218e-08）。
/// 混用会让价格与市值口径不一致。
pub fn unit_price(t: &Trade, is_buy: bool) -> f64 {
    if t.token_amount == 0 {
        return 0.0;
    }
    effective_quote(t, is_buy) as f64 / t.token_amount as f64
}

/// 市值（报价代币单位）：`有效成交额 × totalSupply / Amount`。
///
/// - BUY  市值 = (Q - Fee) × totalSupply / Amount
/// - SELL 市值 = (Q + Fee) × totalSupply / Amount
///
/// 实现为 `unit_price × 供应量`，与上式代数等价。刻意复用 `unit_price` 而不是
/// 把公式再抄一遍：单价与市值必须同源，否则前端会出现「单价 × 供应量 ≠ 市值」
/// 的自相矛盾，而这种不一致在两个函数各自演化时极易发生。
///
/// `total_supply` 为 18 位精度原始值，由调用方传入（见 `config::TOTAL_SUPPLY`），
/// 解码层不依赖全局配置。
#[cfg_attr(not(test), allow(dead_code))]
pub fn market_cap(t: &Trade, is_buy: bool, total_supply: u128) -> f64 {
    if t.token_amount == 0 {
        return 0.0;
    }
    unit_price(t, is_buy) * (total_supply as f64 / 1e18)
}

pub fn decode_trade(data: &[u8]) -> Option<Trade> {
    let w = words(data);
    if w.len() < 7 {
        return None;
    }
    Some(Trade {
        ts: w[0].as_u128() as u64,
        token: w[1].as_address(),
        wallet: w[2].as_address(),
        token_amount: w[3].as_u128(),
        quote_amount: w[4].as_u128(),
        fee: w[5].as_u128(),
    })
}

/// FlapTokenTaxSet：(token, tax bps)。
pub fn decode_tax_set(data: &[u8]) -> Option<(String, u64)> {
    let w = words(data);
    if w.len() < 2 {
        return None;
    }
    Some((w[0].as_address(), w[1].as_u128() as u64))
}

/// FlapTokenAsymmetricTaxSet：(token, buyTax bps, sellTax bps)。
pub fn decode_asym_tax(data: &[u8]) -> Option<(String, u64, u64)> {
    let w = words(data);
    if w.len() < 3 {
        return None;
    }
    Some((
        w[0].as_address(),
        w[1].as_u128() as u64,
        w[2].as_u128() as u64,
    ))
}

/// TokenQuoteSet：(token, quoteToken)。
pub fn decode_quote_set(data: &[u8]) -> Option<(String, String)> {
    let w = words(data);
    if w.len() < 2 {
        return None;
    }
    Some((w[0].as_address(), w[1].as_address()))
}

// ------------------------------------------------------------- eth_call 返回值

/// Chainlink latestAnswer()，BNB/USD 为 8 位精度。
pub fn decode_int256(ret: &[u8]) -> Option<i128> {
    let w = words(ret);
    if w.is_empty() {
        return None;
    }
    if w[0].0[0] & 0x80 != 0 {
        // 负值走补码。BNB/USD 实测不会为负，此分支只为完备性。
        let mut v: i128 = 0;
        for &b in w[0].0.iter() {
            v = (v << 8) | (!b) as i128;
        }
        Some(-(v + 1))
    } else {
        Some(w[0].as_u128() as i128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 下列 payload 全部直接取自 BSC 主网真实日志（区块 0x72bbf00 起），
    // 未经手工改写，用于锁定解码行为不回退。

    #[test]
    fn signatures_match_observed_chain_data() {
        // 这些哈希是从真实链上日志抓到的 topic0，必须逐一命中。
        assert_eq!(
            topic0(crate::config::SIG_TOKEN_CREATED),
            "0x504e7f360b2e5fe33cbaaae4c593bc55305328341bf79009e43e0e3b7f699603"
        );
        assert_eq!(
            topic0(crate::config::SIG_TOKEN_BOUGHT),
            "0xa800a2038683844fac66747f771bfdfae862eb28b16bcfa387afa9fbacce8ff7"
        );
        assert_eq!(
            topic0(crate::config::SIG_TOKEN_SOLD),
            "0x03a4693e592f5e75dc7c136acb39b146d2b4966c0e509c34f362dee02b3b861a"
        );
        assert_eq!(
            topic0(crate::config::SIG_TAX_SET),
            "0x1546924f4680b1b2e093fd251c437c8781d81a21ed8ad1895e1f2c9b78db0cd0"
        );
        assert_eq!(
            topic0(crate::config::SIG_ASYM_TAX_SET),
            "0x46cc246a238d1ca0951a15200994903e2d56cbb0389e63f09d66412a787aa3c0"
        );
        assert_eq!(
            topic0(crate::config::SIG_QUOTE_SET),
            "0x3ceb902d3c555c21c3415b6aa839104b18e4825b2f8324011ff979089a507a8c"
        );
    }

    #[test]
    fn selectors_are_correct() {
        assert_eq!(selector("getTaxTokenInfoV2(address)"), "0x6c06b371");
        assert_eq!(selector("latestAnswer()"), "0x50d25bcd");
    }

    #[test]
    fn decodes_real_token_created_ascii() {
        // 区块 0x72bbf9a，BENCOIN。
        let d = unhex(concat!(
            "000000000000000000000000000000000000000000000000000000006a9d6558",
            "0000000000000000000000003c907f0be7fedb62b4fcd56a9c41bf653b52f775",
            "00000000000000000000000000000000000000000000000000000000002e0fbf",
            "0000000000000000000000008365ca5f38a0da8cfdc99e6294ffd07eb9717777",
            "00000000000000000000000000000000000000000000000000000000000000e0",
            "0000000000000000000000000000000000000000000000000000000000000120",
            "0000000000000000000000000000000000000000000000000000000000000160",
            "0000000000000000000000000000000000000000000000000000000000000007",
            "42454e434f494e00000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000007",
            "42454e434f494e00000000000000000000000000000000000000000000000000",
            "000000000000000000000000000000000000000000000000000000000000003b",
            "6261666b7265696732646f36346f636337337364336874657173323436747561",
            "3565646e72706f723333696e68696f7a6569713471776f717078650000000000"
        ))
        .unwrap();
        assert_eq!(d.len(), 448);
        let c = decode_token_created(&d).expect("should decode");
        assert_eq!(c.ts, 0x6a9d6558);
        assert_eq!(c.creator, "0x3c907f0be7fedb62b4fcd56a9c41bf653b52f775");
        assert_eq!(c.token, "0x8365ca5f38a0da8cfdc99e6294ffd07eb9717777");
        assert_eq!(c.name, "BENCOIN");
        assert_eq!(c.symbol, "BENCOIN");
    }

    #[test]
    fn decodes_real_token_created_utf8() {
        // 区块 0x72bbf65，名称为 9 字节 UTF-8（"BSC" + 两个 3 字节汉字）。
        let d = unhex(concat!(
            "000000000000000000000000000000000000000000000000000000006a9d6540",
            "000000000000000000000000634aa73273b1e7cd02f61eee2d1c7c1135d72ab2",
            "00000000000000000000000000000000000000000000000000000000002e0fbd",
            "000000000000000000000000f9c5919ec1880bbe4bb722f59e152220f1db7777",
            "00000000000000000000000000000000000000000000000000000000000000e0",
            "0000000000000000000000000000000000000000000000000000000000000120",
            "0000000000000000000000000000000000000000000000000000000000000160",
            "0000000000000000000000000000000000000000000000000000000000000009",
            "425343e4ba94e8998e0000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000009",
            "425343e4ba94e8998e0000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000050",
            "68747470733a2f2f697066732e696f2f697066732f6261666b72656961736e6d",
            "727a6a69666966726a35687569346e6f3774787335716374746c746e65737135",
            "79727370366232616271676f64686f3400000000000000000000000000000000"
        ))
        .unwrap();
        assert_eq!(d.len(), 480);
        let c = decode_token_created(&d).expect("should decode");
        assert_eq!(c.token, "0xf9c5919ec1880bbe4bb722f59e152220f1db7777");
        // 多字节名称必须按字节长度切片，不能按字符数截断。
        // "BSC五虎" = 3 个 ASCII + 2 个 3 字节汉字 = 9 字节 / 5 字符。
        assert_eq!(c.name.len(), 9);
        assert_eq!(c.symbol.len(), 9);
        assert!(c.name.starts_with("BSC"));
        assert_eq!(c.name.chars().count(), 5);
    }

    #[test]
    fn decodes_real_buy_payload_and_price() {
        // 区块 0x72bbf00，token f14b...7777，买入方向。
        let d = unhex(concat!(
            "000000000000000000000000000000000000000000000000000000006a9d6513",
            "000000000000000000000000f14b53d3618d5118d5d3867cfc0a5d60d7c67777",
            "000000000000000000000000296b00198dc7ec3410e12da814d9267bb8df506a",
            "0000000000000000000000000000000000000000000003db7e14a09e62004d3a",
            "0000000000000000000000000000000000000000000000000000bbbb338c5b34",
            "000000000000000000000000000000000000000000000000000001e0978e341c",
            "000000000000000000000000000000000000000000000000000000028f281c35"
        ))
        .unwrap();
        assert_eq!(d.len(), 224);
        let t = decode_trade(&d).expect("should decode");
        assert_eq!(t.token, "0xf14b53d3618d5118d5d3867cfc0a5d60d7c67777");
        assert_eq!(t.wallet, "0x296b00198dc7ec3410e12da814d9267bb8df506a");
        assert_eq!(t.token_amount, 0x3db7e14a09e62004d3au128);
        assert_eq!(t.quote_amount, 0xbbbb338c5b34u128);
        assert_eq!(t.fee, 0x1e0978e341cu128);
        // fee = Q / 100 向下取整（Flap 协议费率 1%）。必须与整除结果比，
        // 写成 `fee * 100 == quote_amount` 会因截断余数（实测 68）恒失败。
        // 这条断言的真正目的是锁定 fee 字段在事件里的位置。
        assert_eq!(
            t.fee,
            t.quote_amount / 100,
            "fee 应为 quote 的 1%（向下取整）"
        );
        // BUY：有效成交额 = Q - Fee，单价 = 1.1218068201e-08。
        assert_eq!(effective_quote(&t, true), t.quote_amount - t.fee);
        let p = unit_price(&t, true);
        assert!(
            (p - 1.1218068201e-08).abs() / 1.1218068201e-08 < 1e-9,
            "buy price={p}"
        );
        // 旧的不含费口径是 1.1331382021e-08，必须与新口径可区分，
        // 否则说明 fee 根本没被用上（回归保护）。
        let p_old = t.quote_amount as f64 / t.token_amount as f64;
        assert!((p_old - 1.1331382021e-08).abs() / 1.1331382021e-08 < 1e-9);
        assert!(p < p_old, "BUY 扣费后单价必须低于不含费单价");
        // 方向不能搞反：卖出方向加费，应高于买入方向。
        assert!(unit_price(&t, false) > p);
    }

    #[test]
    fn decodes_real_sell_payload() {
        // 区块 0x72bbf01，token e9b3...7777，卖出方向。
        let d = unhex(concat!(
            "000000000000000000000000000000000000000000000000000000006a9d6513",
            "000000000000000000000000e9b3b3e4ea1d0ca04063409a1794141867d17777",
            "000000000000000000000000296b00198dc7ec3410e12da814d9267bb8df506a",
            "000000000000000000000000000000000000000000016c495eddbba59613f709",
            "000000000000000000000000000000000000000000000000008043b9e85c7fe7",
            "00000000000000000000000000000000000000000000000000014f0ef54f4fac",
            "00000000000000000000000000000000000000000000000000000004f863501f"
        ))
        .unwrap();
        assert_eq!(d.len(), 224);
        let t = decode_trade(&d).expect("should decode");
        assert_eq!(t.token, "0xe9b3b3e4ea1d0ca04063409a1794141867d17777");
        assert_eq!(t.fee, 0x14f0ef54f4facu128);
        // SELL：有效成交额 = Q + Fee，单价 = 2.1200821156e-08。
        assert_eq!(effective_quote(&t, false), t.quote_amount + t.fee);
        let p = unit_price(&t, false);
        assert!(
            (p - 2.1200821156e-08).abs() / 2.1200821156e-08 < 1e-9,
            "sell price={p}"
        );
        // 卖出不含费口径是 2.0986671447e-08，加费后必须更高。
        let p_old = t.quote_amount as f64 / t.token_amount as f64;
        assert!((p_old - 2.0986671447e-08).abs() / 2.0986671447e-08 < 1e-9);
        assert!(p > p_old, "SELL 加回手续费后单价必须高于不含费单价");
        assert!(unit_price(&t, true) < p, "同一笔数据按买入方向算应更低");
    }

    #[test]
    fn effective_price_matches_spec_example() {
        // 规格给出的算例：Q=0.10 BNB, Fee=0.001 BNB, Amount=2000万枚，
        // totalSupply=10亿枚，BNB=$760。
        let one = 10u128.pow(18);
        let t = Trade {
            ts: 1,
            token: "0x0".into(),
            wallet: "0x0".into(),
            token_amount: 20_000_000 * one,
            quote_amount: one / 10, // 0.10 BNB
            fee: one / 1000,        // 0.001 BNB
        };
        let supply = crate::config::TOTAL_SUPPLY;
        // 有效成交额 = 0.099 BNB
        assert_eq!(effective_quote(&t, true), 99 * one / 1000);
        // 成交价 = 4.95e-9 BNB/token
        let p = unit_price(&t, true);
        assert!((p - 4.95e-9).abs() < 1e-18, "price={p}");
        // 市值 = 有效成交额 × totalSupply / Amount = 4.95 BNB
        let mc_bnb = market_cap(&t, true, supply);
        assert!((mc_bnb - 4.95).abs() < 1e-9, "mcap={mc_bnb}");
        // USD 市值 = 4.95 × 760 = $3762
        assert!((mc_bnb * 760.0 - 3762.0).abs() < 1e-6);
        // 不变量：市值必须等于「单价 × 供应量」。两个函数各自演化时最容易在这里裂开，
        // 一旦裂开前端就会出现单价与市值互相矛盾的数字。
        let supply_tokens = supply as f64 / 1e18;
        assert_eq!(supply_tokens, crate::config::TOTAL_SUPPLY_TOKENS);
        assert!((p * supply_tokens - mc_bnb).abs() / mc_bnb < 1e-12);
    }

    #[test]
    fn market_cap_uses_signed_effective_quote() {
        // 卖出方向加回手续费，同一笔数据的市值必须高于买入方向。
        let one = 10u128.pow(18);
        let t = Trade {
            ts: 1,
            token: "0x0".into(),
            wallet: "0x0".into(),
            token_amount: 20_000_000 * one,
            quote_amount: one / 10,
            fee: one / 1000,
        };
        let supply = crate::config::TOTAL_SUPPLY;
        let buy = market_cap(&t, true, supply);
        let sell = market_cap(&t, false, supply);
        // SELL 有效成交额 0.101 BNB -> 市值 5.05 BNB。
        assert!((sell - 5.05).abs() < 1e-9, "sell mcap={sell}");
        assert!(sell > buy, "加费后的市值必须更高");
        // Amount 为 0 时不得除零产生 NaN/inf，市值按 0 处理。
        let zero = Trade {
            token_amount: 0,
            ..t.clone()
        };
        assert_eq!(market_cap(&zero, true, supply), 0.0);
        assert_eq!(unit_price(&zero, true), 0.0);
    }

    #[test]
    fn effective_quote_saturates_instead_of_underflowing() {
        // Fee > Q 属异常数据，必须按 0 处理而不是回绕成天文数字。
        let t = Trade {
            ts: 1,
            token: "0x0".into(),
            wallet: "0x0".into(),
            token_amount: 10u128.pow(18),
            quote_amount: 100,
            fee: 10_000, // 远大于 Q
        };
        assert_eq!(effective_quote(&t, true), 0);
        assert_eq!(unit_price(&t, true), 0.0);
        // 卖出方向是加法，正常放大。
        assert_eq!(effective_quote(&t, false), 10_100);
    }

    #[test]
    fn decodes_real_tax_events() {
        // FlapTokenTaxSet：tax = 100 bps = 1%。
        let d = unhex(concat!(
            "000000000000000000000000f9c5919ec1880bbe4bb722f59e152220f1db7777",
            "0000000000000000000000000000000000000000000000000000000000000064"
        ))
        .unwrap();
        assert_eq!(d.len(), 64);
        let (tok, tax) = decode_tax_set(&d).unwrap();
        assert_eq!(tok, "0xf9c5919ec1880bbe4bb722f59e152220f1db7777");
        assert_eq!(tax, 100);

        // FlapTokenAsymmetricTaxSet：buy=100, sell=100。
        let d = unhex(concat!(
            "000000000000000000000000f9c5919ec1880bbe4bb722f59e152220f1db7777",
            "0000000000000000000000000000000000000000000000000000000000000064",
            "0000000000000000000000000000000000000000000000000000000000000064"
        ))
        .unwrap();
        assert_eq!(d.len(), 96);
        let (tok, buy, sell) = decode_asym_tax(&d).unwrap();
        assert_eq!(tok, "0xf9c5919ec1880bbe4bb722f59e152220f1db7777");
        assert_eq!((buy, sell), (100, 100));
    }

    #[test]
    fn decodes_real_quote_set() {
        // TokenQuoteSet：报价代币 0x2058...efc7（实测为 QQQB，并非原生 BNB）。
        let d = unhex(concat!(
            "000000000000000000000000f9c5919ec1880bbe4bb722f59e152220f1db7777",
            "000000000000000000000000205812cdbed920aff76c6580abd681a46d11efc7"
        ))
        .unwrap();
        let (tok, quote) = decode_quote_set(&d).unwrap();
        assert_eq!(tok, "0xf9c5919ec1880bbe4bb722f59e152220f1db7777");
        assert_eq!(quote, "0x205812cdbed920aff76c6580abd681a46d11efc7");
    }

    #[test]
    fn decodes_chainlink_answer() {
        // 实测 latestAnswer 返回 0x117d5f5b2b = 75117845291，8 位精度 => $751.18。
        let mut w = [0u8; 32];
        w[24..].copy_from_slice(&0x117d5f5b2bu64.to_be_bytes());
        let v = decode_int256(&w).unwrap();
        assert_eq!(v, 75117845291);
        assert!((v as f64 / 1e8 - 751.1785).abs() < 1e-3);
    }

    #[test]
    fn truncates_incomplete_data_instead_of_panicking() {
        assert!(decode_trade(&[0u8; 100]).is_none());
        assert!(decode_token_created(&[0u8; 64]).is_none());
        assert!(decode_tax_set(&[]).is_none());
        assert!(decode_asym_tax(&[0u8; 40]).is_none());
        assert!(decode_quote_set(&[0u8; 31]).is_none());
    }

    #[test]
    fn malformed_offsets_yield_empty_string_not_panic() {
        // offset 指向 data 之外时应返回空串而非 panic。
        let mut d = vec![0u8; 7 * 32];
        // 把 name offset 设成一个远超数据长度的值。
        d[4 * 32 + 31] = 0xff;
        let c = decode_token_created(&d).expect("结构长度足够，应能解码");
        assert_eq!(c.name, "");
        assert_eq!(c.symbol, "");
    }

    #[test]
    fn unhex_rejects_garbage() {
        assert!(unhex("0xzz").is_none());
        assert!(unhex("0x123").is_none());
        assert_eq!(unhex("0x0aFf").unwrap(), vec![0x0a, 0xff]);
        assert_eq!(unhex("0aFf").unwrap(), vec![0x0a, 0xff]);
    }

    #[test]
    fn u128_read_saturates_on_high_words() {
        let mut w = [0u8; 32];
        w[0] = 1; // 高 16 字节非零
        assert_eq!(Word(w).as_u128(), u128::MAX);
        assert!(Word(w).checked_u128().is_none());
    }

    #[test]
    fn four_template_tax_types_and_unknown_values_are_checked() {
        let ca = "0x2222222222222222222222222222222222222222";
        for kind in [0u8, 5, 8, 9] {
            let mut data = vec![0; 13 * 32];
            data[12..32].copy_from_slice(&hex::decode(&ca[2..]).unwrap());
            data[2 * 32 + 16..3 * 32].copy_from_slice(&((kind as u128) << 10).to_be_bytes());
            assert_eq!(four_creator_type(&data, ca), Some(kind));
            assert!(
                four_creator_type(&data, "0x1111111111111111111111111111111111111111").is_none()
            );
            data[2 * 32] = 1;
            assert!(four_creator_type(&data, ca).is_none());
        }
        let mut data = vec![0; 13 * 32];
        data[12..32].copy_from_slice(&hex::decode(&ca[2..]).unwrap());
        data[2 * 32 + 16..3 * 32].copy_from_slice(&(63u128 << 10).to_be_bytes());
        assert!(four_creator_type(&data, ca).is_none());
        assert!(four_creator_type(&data[..data.len() - 1], ca).is_none());
    }
}
