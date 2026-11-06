use crate::{abi, config, rpc::RawLog};

use super::{
    default_supply, topic, BackfillFilter, LaunchPlatform, LaunchpadMonitor, NewTokenEvent,
    NormalizedEvent, NormalizedTrade,
};

pub struct FlapMonitor;

impl LaunchpadMonitor for FlapMonitor {
    fn platform(&self) -> LaunchPlatform {
        LaunchPlatform::Flap
    }

    fn topics(&self) -> Vec<String> {
        [
            config::SIG_TOKEN_CREATED,
            config::SIG_TOKEN_BOUGHT,
            config::SIG_TOKEN_SOLD,
            config::SIG_TAX_SET,
            config::SIG_ASYM_TAX_SET,
            config::SIG_QUOTE_SET,
        ]
        .into_iter()
        .map(topic)
        .collect()
    }

    fn fixed_backfill_filters(&self) -> Vec<BackfillFilter> {
        vec![BackfillFilter {
            addresses: vec![config::PORTAL.into()],
            topics: self.topics(),
        }]
    }

    fn decode(&mut self, log: &RawLog) -> Vec<NormalizedEvent> {
        if !log.address.eq_ignore_ascii_case(config::PORTAL) {
            return Vec::new();
        }
        let t = log.topic0.as_str();
        if t == topic(config::SIG_TOKEN_CREATED) {
            return abi::decode_token_created(&log.data)
                .map(|c| {
                    vec![NormalizedEvent::NewToken(NewTokenEvent {
                        ca: c.token,
                        name: c.name,
                        symbol: c.symbol,
                        source: LaunchPlatform::Flap,
                        created_at: c.ts,
                        creation_block: log.block_number,
                        creation_tx_hash: log.tx_hash.clone(),
                        creator: c.creator,
                        source_ref: None,
                        quote_token: config::ZERO_ADDRESS.into(),
                        total_supply_tokens: default_supply(),
                        tax_bps: None,
                    })]
                })
                .unwrap_or_default();
        }
        if t == topic(config::SIG_TAX_SET) {
            return abi::decode_tax_set(&log.data)
                .map(|(token, tax)| {
                    vec![NormalizedEvent::Tax {
                        token,
                        buy_bps: tax,
                        sell_bps: tax,
                    }]
                })
                .unwrap_or_default();
        }
        if t == topic(config::SIG_ASYM_TAX_SET) {
            return abi::decode_asym_tax(&log.data)
                .map(|(token, buy_bps, sell_bps)| {
                    vec![NormalizedEvent::Tax {
                        token,
                        buy_bps,
                        sell_bps,
                    }]
                })
                .unwrap_or_default();
        }
        if t == topic(config::SIG_QUOTE_SET) {
            return abi::decode_quote_set(&log.data)
                .map(|(token, quote)| vec![NormalizedEvent::Quote { token, quote }])
                .unwrap_or_default();
        }
        let is_buy = t == topic(config::SIG_TOKEN_BOUGHT);
        if is_buy || t == topic(config::SIG_TOKEN_SOLD) {
            return abi::decode_trade(&log.data)
                .map(|tr| {
                    vec![NormalizedEvent::Trade(NormalizedTrade {
                        token: Some(tr.token),
                        source_ref: None,
                        ts: tr.ts,
                        wallet: tr.wallet,
                        token_amount: tr.token_amount,
                        quote_amount: tr.quote_amount,
                        fee: tr.fee,
                        is_buy,
                        wallet_is_actor: false,
                    })]
                })
                .unwrap_or_default();
        }
        Vec::new()
    }
}
