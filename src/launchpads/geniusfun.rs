use crate::{abi, config, rpc::RawLog};

use super::{
    topic, topic_address, BackfillFilter, LaunchPlatform, LaunchpadMonitor, NewTokenEvent,
    NormalizedEvent, NormalizedTrade,
};

pub struct GeniusFunMonitor;

impl LaunchpadMonitor for GeniusFunMonitor {
    fn platform(&self) -> LaunchPlatform {
        LaunchPlatform::GeniusFun
    }

    fn topics(&self) -> Vec<String> {
        [
            config::SIG_GENIUS_TOKEN_LAUNCHED,
            config::SIG_GENIUS_CURVE_BUY,
            config::SIG_GENIUS_CURVE_SELL,
        ]
        .into_iter()
        .map(topic)
        .collect()
    }

    fn fixed_backfill_filters(&self) -> Vec<BackfillFilter> {
        vec![BackfillFilter {
            addresses: vec![config::GENIUS_FUN_FACTORY.into()],
            topics: vec![topic(config::SIG_GENIUS_TOKEN_LAUNCHED)],
        }]
    }

    fn dynamic_backfill_filters(&self, source_refs: &[String]) -> Vec<BackfillFilter> {
        source_refs
            // publicnode accepts address arrays, but starts returning
            // "Request blocked" around ten addresses. Five keeps dynamic curve
            // recovery below that provider limit while still batching calls.
            .chunks(5)
            .map(|chunk| BackfillFilter {
                addresses: chunk.to_vec(),
                topics: vec![
                    topic(config::SIG_GENIUS_CURVE_BUY),
                    topic(config::SIG_GENIUS_CURVE_SELL),
                ],
            })
            .collect()
    }

    fn decode(&mut self, log: &RawLog) -> Vec<NormalizedEvent> {
        if log.topic0 == topic(config::SIG_GENIUS_TOKEN_LAUNCHED)
            && log.address.eq_ignore_ascii_case(config::GENIUS_FUN_FACTORY)
        {
            let words = abi::words(&log.data);
            if words.len() < 3 {
                return Vec::new();
            }
            let (Some(token), Some(curve), Some(deployer)) = (
                topic_address(log, 1),
                topic_address(log, 2),
                topic_address(log, 3),
            ) else {
                return Vec::new();
            };
            return vec![NormalizedEvent::NewToken(NewTokenEvent {
                ca: token,
                name: String::new(),
                symbol: String::new(),
                source: LaunchPlatform::GeniusFun,
                created_at: 0,
                creation_block: log.block_number,
                creation_tx_hash: log.tx_hash.clone(),
                creator: deployer,
                source_ref: Some(curve),
                quote_token: words[0].as_address(),
                total_supply_tokens: 0.0,
                tax_bps: None,
            })];
        }

        let is_buy = log.topic0 == topic(config::SIG_GENIUS_CURVE_BUY);
        if is_buy || log.topic0 == topic(config::SIG_GENIUS_CURVE_SELL) {
            let words = abi::words(&log.data);
            if words.len() < 4 {
                return Vec::new();
            }
            let Some(actor) = topic_address(log, 1) else {
                return Vec::new();
            };
            let (token_amount, quote_amount) = if is_buy {
                (words[1].as_u128(), words[0].as_u128())
            } else {
                (words[0].as_u128(), words[1].as_u128())
            };
            return vec![NormalizedEvent::Trade(NormalizedTrade {
                token: None,
                source_ref: Some(log.address.to_lowercase()),
                ts: 0,
                wallet: actor,
                token_amount,
                quote_amount,
                fee: words[2].as_u128().saturating_add(words[3].as_u128()),
                is_buy,
                // GeniusFun 的 indexed actor 可能是平台执行合约；真实买家必须
                // 以交易 tx.from 为准，由引擎异步补查。
                wallet_is_actor: false,
            })];
        }
        Vec::new()
    }
}
