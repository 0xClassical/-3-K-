//! Launchpad 适配层。
//!
//! 每个平台只负责把自己的合约日志翻译成统一事件；A/B/C 状态机完全不认识
//! Factory、Router 或平台专属 ABI。增加平台时实现 `LaunchpadMonitor` 并在
//! `Registry::default` 注册即可。

mod flap;
mod four_meme;
mod geniusfun;

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{abi, config, rpc::RawLog};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LaunchPlatform {
    Flap,
    FourMeme,
    GeniusFun,
    Other(String),
}

impl LaunchPlatform {
    pub fn db_name(&self) -> String {
        match self {
            Self::Flap => "FLAP".into(),
            Self::FourMeme => "FOUR_MEME".into(),
            Self::GeniusFun => "GENIUS_FUN".into(),
            Self::Other(name) => format!("OTHER:{name}"),
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "FLAP" => Self::Flap,
            "FOUR_MEME" => Self::FourMeme,
            "GENIUS_FUN" => Self::GeniusFun,
            other => Self::Other(other.strip_prefix("OTHER:").unwrap_or(other).to_string()),
        }
    }
}

impl fmt::Display for LaunchPlatform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Flap => write!(f, "Flap"),
            Self::FourMeme => write!(f, "Four.meme"),
            Self::GeniusFun => write!(f, "GeniusFun"),
            Self::Other(name) => f.write_str(name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewTokenEvent {
    pub ca: String,
    pub name: String,
    pub symbol: String,
    pub source: LaunchPlatform,
    pub created_at: u64,
    pub creation_block: u64,
    pub creation_tx_hash: String,
    pub creator: String,
    /// 平台专属成交来源。GeniusFun 为 curve 地址，其余为空。
    pub source_ref: Option<String>,
    pub quote_token: String,
    pub total_supply_tokens: f64,
    /// 适配器能在建币日志中直接确定的税率。None 表示由引擎只读查询补齐。
    pub tax_bps: Option<(u64, u64)>,
}

#[derive(Debug, Clone)]
pub struct NormalizedTrade {
    pub token: Option<String>,
    pub source_ref: Option<String>,
    pub ts: u64,
    pub wallet: String,
    pub token_amount: u128,
    pub quote_amount: u128,
    pub fee: u128,
    pub is_buy: bool,
    /// 平台事件已明确给出最终交易发起人时为 true；只要可能经过 Router/代理，
    /// 就必须为 false，并由引擎补查 tx.from。
    pub wallet_is_actor: bool,
}

#[derive(Debug, Clone)]
pub enum NormalizedEvent {
    NewToken(NewTokenEvent),
    Tax {
        token: String,
        buy_bps: u64,
        sell_bps: u64,
    },
    Quote {
        token: String,
        quote: String,
    },
    Trade(NormalizedTrade),
}

pub trait LaunchpadMonitor: Send + Sync {
    fn platform(&self) -> LaunchPlatform;
    fn topics(&self) -> Vec<String>;
    fn fixed_backfill_filters(&self) -> Vec<BackfillFilter>;
    fn dynamic_backfill_filters(&self, _source_refs: &[String]) -> Vec<BackfillFilter> {
        Vec::new()
    }
    fn decode(&mut self, log: &RawLog) -> Vec<NormalizedEvent>;
}

#[derive(Debug, Clone)]
pub struct BackfillFilter {
    pub addresses: Vec<String>,
    pub topics: Vec<String>,
}

pub struct Registry {
    monitors: Vec<Box<dyn LaunchpadMonitor>>,
}

impl Default for Registry {
    fn default() -> Self {
        let mut registry = Self {
            monitors: Vec::new(),
        };
        registry.register_monitor(flap::FlapMonitor);
        registry.register_monitor(four_meme::FourMemeMonitor);
        registry.register_monitor(geniusfun::GeniusFunMonitor);
        registry
    }
}

impl Registry {
    pub fn register_monitor<M: LaunchpadMonitor + 'static>(&mut self, monitor: M) {
        self.monitors.push(Box::new(monitor));
    }

    pub fn platforms(&self) -> Vec<LaunchPlatform> {
        self.monitors.iter().map(|m| m.platform()).collect()
    }

    pub fn subscription_topics(&self) -> Vec<String> {
        let mut topics: Vec<_> = self.monitors.iter().flat_map(|m| m.topics()).collect();
        topics.sort();
        topics.dedup();
        topics
    }

    pub fn decode(&mut self, log: &RawLog) -> Vec<NormalizedEvent> {
        self.monitors
            .iter_mut()
            .flat_map(|monitor| monitor.decode(log))
            .collect()
    }

    pub fn fixed_backfill_filters(&self) -> Vec<BackfillFilter> {
        self.monitors
            .iter()
            .flat_map(|monitor| monitor.fixed_backfill_filters())
            .collect()
    }

    pub fn dynamic_backfill_filters(&self, source_refs: &[String]) -> Vec<BackfillFilter> {
        self.monitors
            .iter()
            .flat_map(|monitor| monitor.dynamic_backfill_filters(source_refs))
            .collect()
    }
}

pub(crate) fn topic(signature: &str) -> String {
    abi::topic0(signature)
}

pub(crate) fn topic_address(log: &RawLog, index: usize) -> Option<String> {
    let raw = abi::unhex(log.topics.get(index)?)?;
    (raw.len() == 32).then(|| format!("0x{}", abi::hex(&raw[12..])))
}

pub(crate) fn default_supply() -> f64 {
    config::TOTAL_SUPPLY_TOKENS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word_u128(value: u128) -> Vec<u8> {
        let mut out = vec![0u8; 32];
        out[16..].copy_from_slice(&value.to_be_bytes());
        out
    }

    fn word_address(address: &str) -> Vec<u8> {
        let mut out = vec![0u8; 32];
        let raw = abi::unhex(address).unwrap();
        out[12..].copy_from_slice(&raw);
        out
    }

    fn topic_addr(address: &str) -> String {
        format!("0x{}", abi::hex(&word_address(address)))
    }

    fn dynamic_string(value: &str) -> Vec<u8> {
        let mut out = word_u128(value.len() as u128);
        let padded = value.len().div_ceil(32) * 32;
        out.extend_from_slice(value.as_bytes());
        out.resize(32 + padded, 0);
        out
    }

    fn raw(address: &str, signature: &str, topics: Vec<String>, data: Vec<u8>) -> RawLog {
        RawLog {
            address: address.into(),
            tx_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            tx_from: None,
            block_number: 123,
            tx_index: 0,
            log_index: 0,
            topic0: topic(signature),
            topics,
            data,
        }
    }

    #[test]
    fn genius_signature_and_indexed_layout_match_mainnet_log() {
        assert_eq!(
            topic(config::SIG_GENIUS_TOKEN_LAUNCHED),
            "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
        );
        let token = "0x5a6b204c2e5585fa50532ddd786a9f37c707210c";
        let curve = "0x5fb55b67d9dada3809e3c3f8ffad27fa9ed31780";
        let deployer = "0xef7c03e246520ab4114eecd8da296aae7df3d061";
        let mut data = word_address(config::ZERO_ADDRESS);
        data.extend(word_u128(0));
        data.extend(word_u128(1_000_000_000_000_000_000));
        let topics = vec![
            topic(config::SIG_GENIUS_TOKEN_LAUNCHED),
            topic_addr(token),
            topic_addr(curve),
            topic_addr(deployer),
        ];
        let mut monitor = geniusfun::GeniusFunMonitor;
        let events = monitor.decode(&raw(
            config::GENIUS_FUN_FACTORY,
            config::SIG_GENIUS_TOKEN_LAUNCHED,
            topics,
            data,
        ));
        let NormalizedEvent::NewToken(event) = &events[0] else {
            panic!("应解码为新币")
        };
        assert_eq!(event.ca, token);
        assert_eq!(event.source_ref.as_deref(), Some(curve));
        assert_eq!(event.creator, deployer);
    }

    #[test]
    fn genius_trade_requires_transaction_sender_lookup() {
        let curve = "0x5fb55b67d9dada3809e3c3f8ffad27fa9ed31780";
        // 主网实测 indexed actor 可能是 0x1de4... 执行合约，而 tx.from 才是
        // 监控名单中的真实买家，因此适配器绝不能宣称 actor 已是最终钱包。
        let actor = "0x1de460f363af910f51726def188f9004276bf4bc";
        let mut data = word_u128(10u128.pow(18));
        data.extend(word_u128(1_000_000 * 10u128.pow(18)));
        data.extend(word_u128(0));
        data.extend(word_u128(0));
        let topics = vec![
            topic(config::SIG_GENIUS_CURVE_BUY),
            topic_addr(actor),
            topic_addr(actor),
        ];
        let mut monitor = geniusfun::GeniusFunMonitor;
        let events = monitor.decode(&raw(curve, config::SIG_GENIUS_CURVE_BUY, topics, data));
        let NormalizedEvent::Trade(trade) = &events[0] else {
            panic!("应解码为 GeniusFun 成交")
        };
        assert!(!trade.wallet_is_actor, "GeniusFun 必须补查 tx.from");
    }

    #[test]
    fn four_create_is_normalized_without_strategy_knowledge() {
        let creator = "0x1111111111111111111111111111111111111111";
        let token = "0x2222222222222222222222222222222222222222";
        let mut data = word_address(creator);
        data.extend(word_address(token));
        data.extend(word_u128(7));
        data.extend(word_u128(8 * 32));
        data.extend(word_u128(10 * 32));
        data.extend(word_u128(1_000_000_000 * 10u128.pow(18)));
        data.extend(word_u128(1_800_000_000));
        data.extend(word_u128(0));
        data.extend(dynamic_string("Four Dog"));
        data.extend(dynamic_string("FDOG"));
        let topics = vec![topic(config::SIG_FOUR_TOKEN_CREATE)];
        let mut monitor = four_meme::FourMemeMonitor;
        let events = monitor.decode(&raw(
            config::FOUR_MEME_TOKEN_MANAGER2,
            config::SIG_FOUR_TOKEN_CREATE,
            topics,
            data,
        ));
        let NormalizedEvent::NewToken(event) = &events[0] else {
            panic!("应解码为新币")
        };
        assert_eq!(event.ca, token);
        assert_eq!(event.name, "Four Dog");
        assert_eq!(event.symbol, "FDOG");
        assert_eq!(event.created_at, 1_800_000_000);
        assert_eq!(event.total_supply_tokens, 1_000_000_000.0);
    }

    #[test]
    fn registry_exposes_unique_topics_for_all_platforms() {
        let topics = Registry::default().subscription_topics();
        assert!(topics.contains(&topic(config::SIG_TOKEN_CREATED)));
        assert!(topics.contains(&topic(config::SIG_FOUR_TOKEN_CREATE)));
        assert!(topics.contains(&topic(config::SIG_GENIUS_TOKEN_LAUNCHED)));
        assert!(topics.contains(&topic(config::SIG_GENIUS_CURVE_BUY)));
        let unique: std::collections::HashSet<_> = topics.iter().collect();
        assert_eq!(unique.len(), topics.len());
    }
}
