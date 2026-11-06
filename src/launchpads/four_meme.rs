use crate::{abi, config, rpc::RawLog};

use super::{
    topic, BackfillFilter, LaunchPlatform, LaunchpadMonitor, NewTokenEvent, NormalizedEvent,
    NormalizedTrade,
};

pub struct FourMemeMonitor;

impl LaunchpadMonitor for FourMemeMonitor {
    fn platform(&self) -> LaunchPlatform {
        LaunchPlatform::FourMeme
    }

    fn topics(&self) -> Vec<String> {
        [
            config::SIG_FOUR_TOKEN_CREATE,
            config::SIG_FOUR_TOKEN_PURCHASE,
            config::SIG_FOUR_TOKEN_SALE,
        ]
        .into_iter()
        .map(topic)
        .collect()
    }

    fn fixed_backfill_filters(&self) -> Vec<BackfillFilter> {
        vec![BackfillFilter {
            addresses: vec![config::FOUR_MEME_TOKEN_MANAGER2.into()],
            topics: self.topics(),
        }]
    }

    fn decode(&mut self, log: &RawLog) -> Vec<NormalizedEvent> {
        if !log
            .address
            .eq_ignore_ascii_case(config::FOUR_MEME_TOKEN_MANAGER2)
        {
            return Vec::new();
        }
        let words = abi::words(&log.data);
        if log.topic0 == topic(config::SIG_FOUR_TOKEN_CREATE) {
            if words.len() < 8 {
                return Vec::new();
            }
            let supply = words[5].as_u128() as f64 / 1e18;
            return vec![NormalizedEvent::NewToken(NewTokenEvent {
                ca: words[1].as_address(),
                name: abi::read_string(&log.data, words[3].as_u128() as usize),
                symbol: abi::read_string(&log.data, words[4].as_u128() as usize),
                source: LaunchPlatform::FourMeme,
                created_at: words[6].as_u128() as u64,
                creation_block: log.block_number,
                creation_tx_hash: log.tx_hash.clone(),
                creator: words[0].as_address(),
                source_ref: None,
                quote_token: config::ZERO_ADDRESS.into(),
                total_supply_tokens: supply,
                tax_bps: None,
            })];
        }
        let is_buy = log.topic0 == topic(config::SIG_FOUR_TOKEN_PURCHASE);
        if is_buy || log.topic0 == topic(config::SIG_FOUR_TOKEN_SALE) {
            if words.len() < 8 {
                return Vec::new();
            }
            return vec![NormalizedEvent::Trade(NormalizedTrade {
                token: Some(words[0].as_address()),
                source_ref: None,
                ts: 0,
                wallet: words[1].as_address(),
                token_amount: words[3].as_u128(),
                quote_amount: words[4].as_u128(),
                fee: words[5].as_u128(),
                is_buy,
                // Router/代理路径下事件 wallet 不一定是交易发起人。统一补查
                // tx.from，避免把执行合约误当成监控名单里的真实钱包。
                wallet_is_actor: false,
            })];
        }
        Vec::new()
    }
}
