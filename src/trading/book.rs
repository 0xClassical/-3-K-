use super::types::{now_ms, BuySettings, Order, OrderState};
use crate::persistence::Store;
use rusqlite::{params, Connection, OptionalExtension};

fn state(order: &Order) -> Result<String, String> {
    serde_json::to_value(order.state)
        .map_err(|e| e.to_string())?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| "invalid order state".into())
}
pub fn insert(conn: &Connection, order: &Order) -> Result<(), String> {
    conn.execute(
        "INSERT INTO trade_orders(id,ca,b_round,state,wallet,nonce,tx_hash,created_at_ms,payload)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            order.id,
            order.intent.ca,
            order.intent.round,
            state(order)?,
            order.wallet,
            order.nonce,
            order.tx_hash,
            order.created_at_ms,
            serde_json::to_string(order).map_err(|e| e.to_string())?
        ],
    )
    .map(|_| ())
    .map_err(|e| format!("买入意图未保存（重复或数据库错误）: {e}"))
}

impl Store {
    pub fn buy_settings(&self) -> Result<Option<BuySettings>, String> {
        let c = self.0.lock().map_err(|_| "database lock poisoned")?;
        let raw: Option<String> = c
            .query_row("SELECT payload FROM trade_settings WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()
            .map_err(|e| e.to_string())?;
        raw.map(|s| serde_json::from_str(&s).map_err(|_| "保存的买入参数无效".to_string()))
            .transpose()
    }
    pub fn save_buy_settings(&self, settings: &BuySettings) -> Result<(), String> {
        let c = self.0.lock().map_err(|_| "database lock poisoned")?;
        c.execute("INSERT INTO trade_settings(id,payload) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
            [serde_json::to_string(settings).map_err(|e| e.to_string())?]).map_err(|e| e.to_string())?;
        Ok(())
    }
    pub fn trade_durability(&self) -> Result<(), String> {
        self.0
            .lock()
            .map_err(|_| "database lock poisoned")?
            .execute_batch("PRAGMA synchronous=FULL;")
            .map_err(|e| e.to_string())
    }
    pub fn orders(&self, limit: Option<usize>) -> Result<Vec<Order>, String> {
        let c = self.0.lock().map_err(|_| "database lock poisoned")?;
        let mut stmt = c
            .prepare(
                "SELECT payload FROM trade_orders ORDER BY created_at_ms DESC,id DESC LIMIT ?1",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(
                [limit.unwrap_or(i32::MAX as usize).min(i32::MAX as usize) as i64],
                |r| r.get::<_, String>(0),
            )
            .map_err(|e| e.to_string())?;
        rows.map(|r| {
            serde_json::from_str(&r.map_err(|e| e.to_string())?).map_err(|e| e.to_string())
        })
        .collect()
    }
    pub fn order(&self, id: &str) -> Result<Option<Order>, String> {
        let c = self.0.lock().map_err(|_| "database lock poisoned")?;
        get(&c, id)
    }
    pub fn unsettled_orders(&self) -> Result<Vec<Order>, String> {
        let c = self.0.lock().map_err(|_| "database lock poisoned")?;
        let mut stmt=c.prepare("SELECT payload FROM trade_orders WHERE state IN
            ('SIGNED','BROADCAST_UNKNOWN','SUBMITTED','INCLUDED','NEEDS_REVIEW') ORDER BY created_at_ms")
            .map_err(|e|e.to_string())?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        rows.map(|r| {
            serde_json::from_str(&r.map_err(|e| e.to_string())?).map_err(|e| e.to_string())
        })
        .collect()
    }
    pub fn save_order(&self, order: &mut Order, raw: Option<&str>) -> Result<(), String> {
        let mut c = self.0.lock().map_err(|_| "database lock poisoned")?;
        let tx = c.transaction().map_err(|e| e.to_string())?;
        let old = get(&tx, &order.id)?.ok_or("order missing")?;
        if old.intent != order.intent
            || old.mode != order.mode
            || old.generation != order.generation
            || old.session != order.session
            || old.amount_wei != order.amount_wei
            || old.slippage_bps != order.slippage_bps
            || old.fees != order.fees
            || old.reserved_wei != order.reserved_wei
        {
            return Err("禁止更改订单的不可变身份/预算/模式".into());
        }
        if !transition(old.state, order.state) {
            return Err("非法交易订单状态转换".into());
        }
        if old.state == OrderState::Cancelled {
            return Err("订单已取消".into());
        }
        if old.nonce.is_none()
            && order.nonce.is_some()
            && (raw.is_none() || order.state != OrderState::Signed)
        {
            return Err("nonce 只能随完整已签名交易原子保存".into());
        }
        if old.nonce.is_some()
            && (old.nonce != order.nonce
                || old.wallet != order.wallet
                || old.tx_hash != order.tx_hash)
        {
            return Err("禁止修改已签名订单 nonce/wallet/hash".into());
        }
        if old.cancel_requested {
            order.cancel_requested = true;
            if order.state.unsent() {
                order.state = OrderState::Cancelled;
                order.reason = old.reason;
            }
        }
        if let Some(raw) = raw {
            let bytes = crate::abi::unhex(raw).ok_or("invalid signed raw")?;
            if bytes.is_empty()
                || bytes.len() > 8192
                || order.tx_hash.as_deref()
                    != Some(format!("0x{}", hex::encode(crate::wallet::keccak256(&bytes))).as_str())
            {
                return Err("签名交易哈希不匹配".into());
            }
            let rlp = rlp::Rlp::new(&bytes);
            if rlp.item_count().map_err(|_| "invalid signed RLP")? != 9
                || rlp.val_at::<u64>(0).map_err(|_| "invalid nonce RLP")?
                    != order.nonce.ok_or("nonce missing")?
            {
                return Err("签名交易 nonce / RLP 不匹配".into());
            }
            let existing: Option<String> = tx
                .query_row(
                    "SELECT signed_raw FROM trade_orders WHERE id=?1",
                    [&order.id],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())?;
            if existing.is_some_and(|r| r != raw) {
                return Err("禁止替换已签名交易".into());
            }
        }
        order.updated_at_ms = now_ms();
        tx.execute(
            "UPDATE trade_orders SET state=?2,wallet=?3,nonce=?4,tx_hash=?5,
            signed_raw=COALESCE(signed_raw,?6),payload=?7 WHERE id=?1",
            params![
                order.id,
                state(order)?,
                order.wallet,
                order.nonce,
                order.tx_hash,
                raw,
                serde_json::to_string(order).map_err(|e| e.to_string())?
            ],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
    pub fn cancel_unsent(&self, ca: Option<&str>, reason: &str) -> Result<(), String> {
        let mut c = self.0.lock().map_err(|_| "database lock poisoned")?;
        let tx = c.transaction().map_err(|e| e.to_string())?;
        let mut stmt = tx
            .prepare(
                "SELECT payload FROM trade_orders WHERE (?1 IS NULL OR ca=?1)
            AND state IN ('QUEUED','PREPARING','SIGNED')",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([ca], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        let texts: Vec<String> = rows.collect::<Result<_, _>>().map_err(|e| e.to_string())?;
        drop(stmt);
        for text in texts {
            let mut order: Order = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if order.state.unsent() || order.state == OrderState::Signed {
                order.cancel_requested = true;
                order.state = if order.nonce.is_some() {
                    OrderState::NeedsReview
                } else {
                    OrderState::Cancelled
                };
                order.reason = Some(reason.into());
                order.updated_at_ms = now_ms();
                tx.execute(
                    "UPDATE trade_orders SET state=?2,payload=?3 WHERE id=?1",
                    params![
                        order.id,
                        state(&order)?,
                        serde_json::to_string(&order).map_err(|e| e.to_string())?
                    ],
                )
                .map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())
    }
    /// Independent final check immediately before irrevocable broadcast handoff.
    pub fn eligible_order(&self, order: &Order) -> Result<bool, String> {
        let c = self.0.lock().map_err(|_| "database lock poisoned")?;
        let live = get(&c, &order.id)?.ok_or("order missing")?;
        if live.cancel_requested || live.state == OrderState::Cancelled {
            return Ok(false);
        }
        let found:i64=c.query_row("SELECT count(*) FROM tokens WHERE ca=?1 AND current_slot=?2
            AND first_qualified_at_ms=?3 AND current_state IN ('B_PENDING','B_ACTIVE')
            AND user_discarded=0 AND reentry_b_at IS NULL AND buy_tax_bps<=400 AND sell_tax_bps<=400",
            params![order.intent.ca,order.intent.slot_id,order.intent.b_at_ms],|r|r.get(0))
            .map_err(|e|e.to_string())?;
        Ok(found == 1)
    }
}
fn transition(from: OrderState, to: OrderState) -> bool {
    use OrderState::*;
    if from == to {
        return true;
    }
    match from {
        Queued => matches!(to, Preparing | Failed | Cancelled),
        Preparing => matches!(to, Signed | SimulationPassed | Failed | Cancelled),
        Signed => matches!(to, BroadcastUnknown | NeedsReview),
        BroadcastUnknown | Submitted | Included | NeedsReview => {
            matches!(to, Submitted | Included | Confirmed | Failed | NeedsReview)
        }
        Confirmed | Failed | SimulationPassed | Cancelled => false,
    }
}
fn get(c: &Connection, id: &str) -> Result<Option<Order>, String> {
    let text: Option<String> = c
        .query_row("SELECT payload FROM trade_orders WHERE id=?1", [id], |r| {
            r.get(0)
        })
        .optional()
        .map_err(|e| e.to_string())?;
    text.map(|v| serde_json::from_str(&v).map_err(|e| e.to_string()))
        .transpose()
}
