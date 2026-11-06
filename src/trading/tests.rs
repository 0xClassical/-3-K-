//! Offline fixtures only: mock RPC, in-memory SQLite and public dummy signing key.
use super::*;
use crate::persistence::BTiming;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Notify;

const TOKEN: &str = "0x2222222222222222222222222222222222222222";
const ACCOUNT: &str = "0x1a642f0e3c3af545e7acbd38b07251b3990914f1";
const HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
#[tokio::test]
async fn shutdown_is_sticky_cancels_unsent_and_blocks_future_activation() {
    let rpc = Arc::new(Mock::default());
    let (t, _jobs) = Trading::new(Store::memory().unwrap(), policy(), rpc.clone()).unwrap();
    t.set_mode(Mode::DryRun).await.unwrap();
    assert!(t.offer(&token(), "shutdown-test").unwrap());
    t.stop_for_shutdown().unwrap();
    let v = t.view().unwrap();
    assert_eq!(v.mode, Mode::Disabled);
    assert!(!v.wallet_unlocked);
    assert!(!v.nonce_ready);
    assert_eq!(v.orders[0].state, OrderState::Cancelled);
    assert!(t.set_mode(Mode::DryRun).await.is_err());
    let settings = t.control.lock().unwrap().settings.clone();
    assert!(t.update_settings(settings, v.generation).is_err());
    assert!(t
        .unlock(Zeroizing::new("offline-password-never-loaded".into()))
        .await
        .is_err());
    assert!(!t.offer(&token(), "shutdown-test-2").unwrap());
    assert!(rpc.calls.lock().unwrap().is_empty());
}
fn token() -> PersistedToken {
    let now = now_ms();
    PersistedToken {
        b_timing: BTiming {
            first_chain_at_ms: Some(now),
            ..BTiming::default()
        },
        ca: TOKEN.into(),
        name: "offline fixture".into(),
        symbol: "TEST".into(),
        creator: ACCOUNT.into(),
        source_platform: "FLAP".into(),
        creation_block: 90,
        creation_tx_hash: HASH.into(),
        source_ref: None,
        total_supply_tokens: 1e9,
        created_at: now / 1000 - 30,
        current_state: "B_PENDING".into(),
        buy_tax_bps: 0,
        sell_tax_bps: 0,
        smart_money_hit: true,
        bull_streak_seen: true,
        first_b_at: Some(now / 1000),
        first_qualified_at_ms: Some(now),
        first_open_sent_at_ms: None,
        first_open_acked_at_ms: None,
        last_qualified_at_ms: Some(now),
        last_open_sent_at_ms: None,
        last_open_acked_at_ms: None,
        c_entered_at: None,
        reentry_used: false,
        reentry_b_at: None,
        current_cycle_start: Some(now / 1000),
        cycle_max_market_cap: 12000.0,
        current_slot: Some("A".into()),
        user_discarded: false,
        quote_token: crate::config::ZERO_ADDRESS.into(),
        quote_symbol: Some("BNB".into()),
        last_price: 1.0,
        last_mcap_quote: 1.0,
        last_mcap_usd: Some(12000.0),
        streak: 3,
        streak_end: Some(now / 3),
        c_streak: 0,
        c_streak_end: None,
    }
}
fn policy() -> Policy {
    Policy {
        simulation_from: Some(ACCOUNT.into()),
        slippage_bps: Some(500),
        session_budget_wei: "100000000000000000".into(),
        daily_budget_wei: "100000000000000000".into(),
        ..Policy::default()
    }
}
#[derive(Default)]
struct Mock {
    calls: Mutex<Vec<String>>,
    requests: Mutex<Vec<(String, Value)>>,
    hold: AtomicBool,
    started: Notify,
    release: Notify,
    receipt: Mutex<Value>,
    head: AtomicU64,
    estimate_fail: AtomicBool,
    nonce_fail: AtomicBool,
    pending_conflict: AtomicBool,
    finalized_missing: AtomicBool,
    four_migrated: AtomicBool,
    tax: AtomicU64,
    #[cfg(feature = "live-trading")]
    allow_mock_send: AtomicBool,
    #[cfg(feature = "live-trading")]
    hold_activation: AtomicBool,
}
fn abi_result(words: Vec<[u8; 32]>) -> Value {
    json!(format!(
        "0x{}",
        hex::encode(words.into_iter().flatten().collect::<Vec<_>>())
    ))
}
impl TradeRpc for Mock {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> rpc::RpcFuture<'a> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(method.into());
            self.requests
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            match method {
                "eth_chainId" => Ok(json!("0x38")),
                "eth_getTransactionCount" => {
                    if self.nonce_fail.load(Ordering::Relaxed) {
                        Err("offline nonce failure".into())
                    } else if self.pending_conflict.load(Ordering::Relaxed)
                        && params[1] == "pending"
                    {
                        Ok(json!("0x1"))
                    } else {
                        Ok(json!("0x0"))
                    }
                }
                "eth_getBalance" => Ok(json!("0xde0b6b3a7640000")),
                "eth_gasPrice" => Ok(json!("0x3b9aca00")),
                "eth_blockNumber" => Ok(json!(format!(
                    "0x{:x}",
                    self.head.load(Ordering::Relaxed).max(100)
                ))),
                "eth_getBlockByNumber" => {
                    #[cfg(feature = "live-trading")]
                    if params[0] == "finalized" && self.hold_activation.load(Ordering::Relaxed) {
                        self.started.notify_one();
                        self.release.notified().await;
                    }
                    if self.finalized_missing.load(Ordering::Relaxed) && params[0] == "finalized" {
                        return Ok(Value::Null);
                    }
                    Ok(
                        json!({"timestamp":format!("0x{:x}",now_ms()/1000),"hash":HASH,
                        "number":format!("0x{:x}",self.head.load(Ordering::Relaxed).max(100).saturating_sub(2))}),
                    )
                }
                "eth_estimateGas" => {
                    if self.estimate_fail.load(Ordering::Relaxed) {
                        Err("offline simulation reverted".into())
                    } else {
                        Ok(json!("0x186a0"))
                    }
                }
                "eth_getTransactionReceipt" => Ok(self.receipt.lock().unwrap().clone()),
                "eth_call" => {
                    let data = params[0]["data"].as_str().unwrap_or("");
                    let signature = |s: &str| data.starts_with(&crate::abi::selector(s));
                    if signature("getTokenV8Safe(address)") {
                        let mut words=vec![[0;32];18]; words[0]=routes::word(1);
                        words[12]=routes::word(self.tax.load(Ordering::Relaxed) as u128);
                        words[13]=words[12];
                        Ok(abi_result(words))
                    } else if signature("quoteExactInput((address,address,uint256))") {
                        Ok(abi_result(vec![routes::word(1_000_000)]))
                    } else if signature("getTokenInfo(address)") {
                        let mut words=vec![[0;32];12];
                        words[0]=routes::word(2);
                        words[1]=routes::addr_word(crate::config::FOUR_MEME_TOKEN_MANAGER2).unwrap();
                        words[11]=routes::word(self.four_migrated.load(Ordering::Relaxed) as u128);
                        Ok(abi_result(words))
                    } else if signature("_tokenInfos(address)") {
                        let mut words=vec![[0;32];13];
                        words[0]=routes::addr_word(TOKEN).unwrap();
                        if self.tax.load(Ordering::Relaxed)>0 { words[2]=routes::word(8<<10); }
                        Ok(abi_result(words))
                    } else if signature("feeRateBuy()") || signature("feeRateSell()") {
                        Ok(abi_result(vec![routes::word(self.tax.load(Ordering::Relaxed) as u128)]))
                    } else if signature("tryBuy(address,uint256,uint256)") {
                        let bytes=crate::abi::unhex(data).unwrap();
                        let funds=routes::uint(&bytes[4..],2).unwrap();
                        Ok(abi_result(vec![routes::addr_word(crate::config::FOUR_MEME_TOKEN_MANAGER2).unwrap(),
                            [0;32],routes::word(1_000_000),routes::word(funds),[0;32],routes::word(funds),[0;32],routes::word(funds)]))
                    } else if signature("getPair(address,address)") {
                        Ok(abi_result(vec![routes::addr_word("0x3333333333333333333333333333333333333333").unwrap()]))
                    } else if signature("getAmountsOut(uint256,address[])") {
                        let bytes=crate::abi::unhex(data).unwrap();
                        let input=routes::uint(&bytes[4..],0).unwrap();
                        Ok(abi_result(vec![routes::word(32),routes::word(2),routes::word(input),routes::word(1_000_000)]))
                    } else if signature("buyTokenAMAP(address,uint256,uint256)")
                        || signature("swapExactETHForTokensSupportingFeeOnTransferTokens(uint256,address[],address,uint256)") {
                        Ok(json!("0x"))
                    } else if signature("balanceOf(address)") {
                        Ok(abi_result(vec![routes::word(900_000)]))
                    } else if signature("swapExactInput((address,address,uint256,uint256,bytes))") {
                        if self.hold.load(Ordering::Relaxed) {
                            self.started.notify_one();
                            self.release.notified().await;
                        }
                        Ok(abi_result(vec![routes::word(950_000)]))
                    } else { Err(format!("offline mock unknown selector: {data}")) }
                }
                "eth_sendRawTransaction" => {
                    #[cfg(feature = "live-trading")]
                    if self.allow_mock_send.load(Ordering::Relaxed) {
                        return Err("offline mock broadcast timeout (no network)".into());
                    }
                    panic!("dry run must never invoke a broadcast")
                }
                _ => Err(format!("offline mock unknown method: {method}")),
            }
        })
    }
}

#[tokio::test]
async fn four_curve_and_migrated_v2_have_distinct_protected_routes() {
    for migrated in [false, true] {
        let (t, rpc, _queue) = fixture().await;
        rpc.four_migrated.store(migrated, Ordering::Relaxed);
        let mut tok = token();
        tok.source_platform = "FOUR_MEME".into();
        t.offer(&tok, "cmd").unwrap();
        t.process(&queued(&t).id).await.unwrap();
        let o = queued(&t);
        assert_eq!(o.state, OrderState::SimulationPassed);
        assert_eq!(
            o.route.as_deref(),
            Some(if migrated {
                "PANCAKE_V2_NATIVE_FEE_SUPPORT"
            } else {
                "FOUR_MEME_V2_CURVE_NATIVE"
            })
        );
        assert_eq!(o.min_out_raw.as_deref(), Some("950000"));
    }
    // TaxToken8 4% is converted to 400 bps, and net Pancake minimum includes tax.
    let (t, rpc, _queue) = fixture().await;
    rpc.four_migrated.store(true, Ordering::Relaxed);
    rpc.tax.store(4, Ordering::Relaxed);
    let mut tok = token();
    tok.source_platform = "FOUR_MEME".into();
    t.offer(&tok, "cmd").unwrap();
    t.process(&queued(&t).id).await.unwrap();
    assert_eq!(queued(&t).min_out_raw.as_deref(), Some("912000"));
}
#[tokio::test]
async fn fresh_excess_tax_or_unverified_tax_curve_is_not_traded() {
    for rate in [100, 401] {
        let (t, rpc, _queue) = fixture().await;
        rpc.tax.store(rate, Ordering::Relaxed);
        t.offer(&token(), "cmd").unwrap();
        assert!(t.process(&queued(&t).id).await.is_err());
        assert!(queued(&t).tx_hash.is_none());
    }
}
#[cfg(feature = "live-trading")]
#[tokio::test]
async fn live_branch_mock_timeout_keeps_one_signed_hash_and_never_rebuys() {
    let mut p = policy();
    p.allow_live = true;
    let rpc = Arc::new(Mock::default());
    rpc.allow_mock_send.store(true, Ordering::Relaxed);
    let (t, _queue) = Trading::new(Store::memory().unwrap(), p.clone(), rpc.clone()).unwrap();
    {
        let mut c = t.control.lock().unwrap();
        c.wallet = Some(Arc::new(Wallet::test_wallet()));
        c.nonce_ready = true;
    }
    t.set_mode(Mode::Live).await.unwrap();
    t.offer(&token(), "cmd").unwrap();
    let id = queued(&t).id;
    t.process(&id).await.unwrap();
    assert_eq!(queued(&t).state, OrderState::BroadcastUnknown);
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    assert!(queued(&t).tx_hash.is_some());
    assert!(t.process(&id).await.is_err());
    let (restored, _) = Trading::new(t.store.clone(), p, rpc.clone()).unwrap();
    restored.reconcile().await.unwrap();
    assert!(restored.no_unsettled().is_err());
    assert_eq!(
        rpc.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.as_str() == "eth_sendRawTransaction")
            .count(),
        1
    );
}
async fn fixture() -> (Trading, Arc<Mock>, mpsc::Receiver<String>) {
    let store = Store::memory().unwrap();
    let rpc = Arc::new(Mock::default());
    let (trading, queue) = Trading::new(store, policy(), rpc.clone()).unwrap();
    trading.set_mode(Mode::DryRun).await.unwrap();
    (trading, rpc, queue)
}
fn queued(trading: &Trading) -> Order {
    trading.store.orders(None).unwrap().remove(0)
}

#[tokio::test]
async fn default_disabled_locked_and_live_requires_both_guards() {
    let rpc = Arc::new(Mock::default());
    let (t, _) = Trading::new(Store::memory().unwrap(), Policy::default(), rpc.clone()).unwrap();
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    assert!(!t.view().unwrap().wallet_unlocked);
    assert!(!t.offer(&token(), "cmd").unwrap());
    assert!(t.set_mode(Mode::Live).await.is_err());
    assert!(rpc.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn dry_run_preflight_cannot_sign_or_send_and_intent_is_unique() {
    let (t, rpc, _queue) = fixture().await;
    let tok = token();
    assert!(t.offer(&tok, "cmd-A").unwrap());
    let mut duplicate = tok.clone();
    duplicate.name = "must roll back".into();
    assert!(t.offer(&duplicate, "cmd-B").is_err());
    assert_eq!(t.store.load_tokens().unwrap()[0].name, "offline fixture");
    let id = queued(&t).id;
    t.process(&id).await.unwrap();
    let o = queued(&t);
    assert_eq!(o.state, OrderState::SimulationPassed);
    assert!(
        o.nonce.is_none()
            && o.tx_hash.is_none()
            && o.signed_at_ms.is_none()
            && o.broadcast_at_ms.is_none()
    );
    assert_eq!(o.min_out_raw.as_deref(), Some("950000"));
    assert_eq!(t.store.orders(None).unwrap().len(), 1);
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "eth_sendRawTransaction"));
}
#[tokio::test]
async fn manual_discard_wins_while_simulation_is_waiting() {
    let (t, rpc, _queue) = fixture().await;
    rpc.hold.store(true, Ordering::Relaxed);
    t.offer(&token(), "cmd-A").unwrap();
    let id = queued(&t).id;
    let worker = t.clone();
    let task = tokio::spawn(async move { worker.process(&id).await });
    rpc.started.notified().await;
    t.cancel_token(TOKEN, "人工删除");
    rpc.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert_eq!(queued(&t).state, OrderState::Cancelled);
    assert!(queued(&t).tx_hash.is_none());
}
#[tokio::test]
async fn pause_restart_reset_and_slot_change_do_not_replay_buys() {
    let (t, rpc, _queue) = fixture().await;
    t.offer(&token(), "cmd-A").unwrap();
    let id = queued(&t).id;
    t.pause("暂停", false).unwrap();
    assert!(t.process(&id).await.is_err());
    let (restored, _) = Trading::new(t.store.clone(), policy(), rpc).unwrap();
    assert_eq!(restored.view().unwrap().mode, Mode::Disabled);
    assert_eq!(queued(&restored).state, OrderState::Cancelled);
    restored
        .store
        .reset_monitoring(100, 123, now_ms() / 1000)
        .unwrap();
    assert_eq!(restored.store.orders(None).unwrap().len(), 1);
    assert_eq!(queued(&restored).intent.ca, TOKEN);

    let (t, _, _queue) = fixture().await;
    t.offer(&token(), "cmd-A").unwrap();
    let mut moved = t.store.load_tokens().unwrap().remove(0);
    moved.current_slot = Some("B".into());
    t.store.upsert_token(&moved, now_ms() / 1000).unwrap();
    assert!(t.process(&queued(&t).id).await.is_err());
}
#[tokio::test]
async fn failed_nonce_simulation_unknown_platform_never_broadcast() {
    for issue in ["nonce", "pending-conflict", "simulation", "platform"] {
        let (t, rpc, _queue) = fixture().await;
        rpc.nonce_fail.store(issue == "nonce", Ordering::Relaxed);
        rpc.pending_conflict
            .store(issue == "pending-conflict", Ordering::Relaxed);
        rpc.estimate_fail
            .store(issue == "simulation", Ordering::Relaxed);
        let mut tok = token();
        if issue == "platform" {
            tok.source_platform = "GENIUS_FUN".into();
        }
        t.offer(&tok, "cmd-A").unwrap();
        let id = queued(&t).id;
        let error = t.process(&id).await.unwrap_err();
        t.failed(&id, error);
        assert_eq!(queued(&t).state, OrderState::Failed);
        assert!(queued(&t).nonce.is_none());
        assert!(!rpc
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|m| m == "eth_sendRawTransaction"));
    }
}

fn signed_order(t: &Trading) -> Order {
    let mut o = queued(t);
    o.state = OrderState::Preparing;
    t.store.save_order(&mut o, None).unwrap();
    let wallet = Wallet::test_wallet();
    assert_eq!(wallet.address(), ACCOUNT);
    let signed = wallet
        .sign(&Transaction {
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 200_000,
            value: 5_000_000_000_000_000,
            to: hex::decode(&crate::config::PORTAL[2..])
                .unwrap()
                .try_into()
                .unwrap(),
            data: vec![1, 2, 3, 4],
        })
        .unwrap();
    o.wallet = Some(ACCOUNT.into());
    o.nonce = Some(0);
    o.tx_hash = Some(signed.hash);
    o.state = OrderState::Signed;
    o.to = Some(crate::config::PORTAL.into());
    o.gas_price_wei = Some("1000000000".into());
    o.broadcast_at_ms = Some(now_ms());
    t.store.save_order(&mut o, Some(&signed.raw)).unwrap();
    o.state = OrderState::BroadcastUnknown;
    t.store.save_order(&mut o, None).unwrap();
    o
}
#[tokio::test]
async fn unknown_broadcast_survives_restart_and_is_reconciled_not_retried() {
    let (t, rpc, _queue) = fixture().await;
    t.offer(&token(), "cmd-A").unwrap();
    let o = signed_order(&t);
    let (restored, _) = Trading::new(t.store.clone(), policy(), rpc.clone()).unwrap();
    assert_eq!(queued(&restored).tx_hash, o.tx_hash);
    assert!(restored.no_unsettled().is_err());
    restored.reconcile().await.unwrap();
    assert_eq!(queued(&restored).state, OrderState::BroadcastUnknown);
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "eth_sendRawTransaction"));
}
#[tokio::test]
async fn receipt_requires_confirmations_handles_reorg_and_tracks_net_receipt() {
    let (t, rpc, _queue) = fixture().await;
    t.offer(&token(), "cmd-A").unwrap();
    let order = signed_order(&t);
    let topic = |a: &str| format!("0x{}", hex::encode(routes::addr_word(a).unwrap()));
    *rpc.receipt.lock().unwrap() = json!({
        "transactionHash":order.tx_hash,"from":ACCOUNT,"to":crate::config::PORTAL,
        "blockNumber":"0x64","blockHash":HASH,"status":"0x1","gasUsed":"0x186a0","effectiveGasPrice":"0x3b9aca00",
        "logs":[{"address":TOKEN,"topics":[crate::abi::topic0("Transfer(address,address,uint256)"),
            topic(crate::config::PORTAL),topic(ACCOUNT)],"data":format!("0x{}",hex::encode(routes::word(900_000)))}]
    });
    t.reconcile().await.unwrap();
    assert_eq!(queued(&t).state, OrderState::Included);
    *rpc.receipt.lock().unwrap() = Value::Null;
    t.reconcile().await.unwrap();
    assert_eq!(queued(&t).state, OrderState::Submitted);
    assert!(queued(&t).included_at_ms.is_none());
    assert!(queued(&t).gas_paid_wei.is_none());
    *rpc.receipt.lock().unwrap() = json!({
        "transactionHash":order.tx_hash,"from":ACCOUNT,"to":crate::config::PORTAL,
        "blockNumber":"0x64","blockHash":HASH,"status":"0x1","gasUsed":"0x186a0","effectiveGasPrice":"0x3b9aca00",
        "logs":[{"address":TOKEN,"topics":[crate::abi::topic0("Transfer(address,address,uint256)"),
            topic(crate::config::PORTAL),topic(ACCOUNT)],"data":format!("0x{}",hex::encode(routes::word(900_000)))}]
    });
    rpc.head.store(102, Ordering::Relaxed);
    t.reconcile().await.unwrap();
    let o = queued(&t);
    assert_eq!(o.state, OrderState::Confirmed);
    assert_eq!(o.received_raw.as_deref(), Some("900000"));
    assert_eq!(o.token_balance_raw.as_deref(), Some("900000"));
    assert_eq!(o.gas_paid_wei.as_deref(), Some("100000000000000"));
}

#[tokio::test]
async fn missing_finalized_keeps_inclusion_unsettled_and_failed_receipt_trips_circuit() {
    let (t, rpc, _queue) = fixture().await;
    t.offer(&token(), "cmd-A").unwrap();
    let order = signed_order(&t);
    *rpc.receipt.lock().unwrap() = json!({
        "transactionHash":order.tx_hash,"from":ACCOUNT,"to":crate::config::PORTAL,
        "blockNumber":"0x64","blockHash":HASH,"status":"0x0","gasUsed":"0x186a0","logs":[]
    });
    rpc.head.store(102, Ordering::Relaxed);
    rpc.finalized_missing.store(true, Ordering::Relaxed);
    assert!(t.reconcile().await.is_err());
    assert_eq!(queued(&t).state, OrderState::Included);
    assert!(queued(&t).confirmed_at_ms.is_none());
    assert!(t.no_unsettled().is_err());
    rpc.finalized_missing.store(false, Ordering::Relaxed);
    t.control.lock().unwrap().consecutive_failures = 2;
    t.reconcile().await.unwrap();
    assert_eq!(queued(&t).state, OrderState::Failed);
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    assert!(queued(&t).confirmed_at_ms.is_some());
    let failures = t.control.lock().unwrap().consecutive_failures;
    t.reconcile().await.unwrap();
    assert_eq!(t.control.lock().unwrap().consecutive_failures, failures);
}

#[tokio::test]
async fn cancelled_intent_invalid_chain_time_and_failure_circuit_never_sign() {
    let (t, rpc, _queue) = fixture().await;
    let mut missing = token();
    missing.b_timing.first_chain_at_ms = None;
    assert!(t.offer(&missing, "cmd").is_err());
    let mut stale = token();
    stale.b_timing.first_chain_at_ms = Some(now_ms() - 20_000);
    assert!(t.offer(&stale, "cmd").is_err());
    t.offer(&token(), "cmd").unwrap();
    let id = queued(&t).id;
    t.cancel_token(TOKEN, "discard");
    t.set_mode(Mode::DryRun).await.unwrap();
    assert!(t.process(&id).await.is_err());
    assert!(t.offer(&token(), "new-cmd").is_err());
    assert_eq!(queued(&t).state, OrderState::Cancelled);
    assert!(rpc.calls.lock().unwrap().is_empty());
    for _ in 0..3 {
        t.failed(&id, "offline fault".into());
    }
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    assert!(queued(&t).nonce.is_none() && queued(&t).tx_hash.is_none());
}
#[tokio::test]
async fn budget_is_persistent_and_nonce_cannot_be_reassigned() {
    let (t, _, _queue) = fixture().await;
    t.offer(&token(), "cmd-A").unwrap();
    signed_order(&t);
    let old = queued(&t);
    let mut changed = old.clone();
    changed.nonce = Some(1);
    assert!(t.store.save_order(&mut changed, None).is_err());
    assert!(t.no_unsettled().is_err());
    let mut p = policy();
    p.session_budget_wei = p.reservation().unwrap().to_string();
    let (budget, _) = Trading::new(t.store.clone(), p, Arc::new(Mock::default())).unwrap();
    let mut rows = vec![old];
    rows[0].session = budget.session.clone();
    rows[0].mode = Mode::Live;
    assert!(budget
        .check_budget(&rows, budget.policy.reservation().unwrap())
        .is_err());
}

#[tokio::test]
async fn user_settings_persist_without_arming_and_orders_snapshot_exact_parameters() {
    let rpc = Arc::new(Mock::default());
    let p = Policy {
        simulation_from: Some(ACCOUNT.into()),
        ..Policy::default()
    };
    let (t, _queue) = Trading::new(Store::memory().unwrap(), p.clone(), rpc.clone()).unwrap();
    let first = BuySettings {
        amount_wei: "10000000000000001".into(),
        slippage_bps: Some(123),
        fees: Some(FeeSettings::from_policy(&p)),
    };
    t.update_settings(first.clone(), t.view().unwrap().generation)
        .unwrap();
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    assert!(t.view().unwrap().policy_ready);
    assert!(rpc.calls.lock().unwrap().is_empty());
    t.set_mode(Mode::DryRun).await.unwrap();
    assert!(t
        .update_settings(first.clone(), t.view().unwrap().generation)
        .is_err());
    t.offer(&token(), "cmd").unwrap();
    let order = queued(&t);
    assert_eq!(order.amount_wei, first.amount_wei);
    assert_eq!(order.slippage_bps, first.slippage_bps);
    assert_eq!(order.fees, first.fees);
    t.process(&order.id).await.unwrap();
    assert_eq!(
        queued(&t).sent_value_wei.as_deref(),
        Some("10000000000000001")
    );
    assert_eq!(queued(&t).min_out_raw.as_deref(), Some("987700"));
    let mut tampered = queued(&t);
    tampered.slippage_bps = Some(9000);
    assert!(t.store.save_order(&mut tampered, None).is_err());
    let mut tampered_fee = queued(&t);
    tampered_fee.fees.as_mut().unwrap().gas_fee_cap_wei = "1".into();
    assert!(t.store.save_order(&mut tampered_fee, None).is_err());
    t.pause("offline stop", true).unwrap();
    let second = BuySettings {
        amount_wei: "20000000000000000".into(),
        slippage_bps: Some(200),
        fees: Some(FeeSettings {
            gas_multiplier_bps: 12_000,
            ..FeeSettings::from_policy(&p)
        }),
    };
    t.update_settings(second.clone(), t.view().unwrap().generation)
        .unwrap();
    assert_eq!(queued(&t).slippage_bps, Some(123));
    assert_eq!(queued(&t).fees, first.fees);
    t.store.reset_monitoring(100, 123, now_ms() / 1000).unwrap();
    let (restored, _) = Trading::new(t.store.clone(), p, rpc.clone()).unwrap();
    assert_eq!(restored.view().unwrap().amount_wei, second.amount_wei);
    assert_eq!(restored.view().unwrap().slippage_bps, second.slippage_bps);
    assert_eq!(restored.view().unwrap().fees, second.fees);
    assert_eq!(restored.view().unwrap().mode, Mode::Disabled);
    assert!(!restored.view().unwrap().wallet_unlocked);
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "eth_sendRawTransaction"));
}

#[tokio::test]
async fn stop_invalidates_delayed_start_and_settings_requests() {
    let (t, _, _) = fixture().await;
    let old = t.view().unwrap().generation;
    t.pause("stop wins", false).unwrap();
    assert!(t
        .set_mode_checked(Mode::DryRun, Some(old), false)
        .await
        .is_err());
    let settings = BuySettings {
        amount_wei: "10000000000000000".into(),
        slippage_bps: Some(100),
        fees: Some(FeeSettings::from_policy(&policy())),
    };
    assert!(t.update_settings(settings.clone(), old).is_err());
    let g = t.view().unwrap().generation;
    t.update_settings(settings, g).unwrap();
    assert!(t
        .set_mode_checked(Mode::DryRun, Some(g), false)
        .await
        .is_err());
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    let fresh = t.view().unwrap().generation;
    t.set_mode_checked(Mode::DryRun, Some(fresh), false)
        .await
        .unwrap();
    assert_eq!(t.view().unwrap().mode, Mode::DryRun);
}

#[tokio::test]
async fn fee_modes_preflight_with_exact_price_and_fixed_skips_gas_oracle() {
    for fixed in [false, true] {
        let rpc = Arc::new(Mock::default());
        let p = policy();
        let (t, _queue) = Trading::new(Store::memory().unwrap(), p.clone(), rpc.clone()).unwrap();
        let mut settings = BuySettings::from_policy(&p);
        let fees = settings.fees.as_mut().unwrap();
        fees.gas_multiplier_bps = 20_000;
        if fixed {
            fees.gas_mode = GasMode::Fixed;
            fees.fixed_gas_price_wei = Some("1234567890".into());
        }
        t.update_settings(settings.clone(), t.view().unwrap().generation)
            .unwrap();
        t.set_mode(Mode::DryRun).await.unwrap();
        t.offer(&token(), "fee-cmd").unwrap();
        let id = queued(&t).id;
        t.process(&id).await.unwrap();
        let o = queued(&t);
        assert_eq!(o.state, OrderState::SimulationPassed);
        let expected = if fixed {
            1_234_567_890u128
        } else {
            2_000_000_000
        };
        assert_eq!(o.gas_price_wei, Some(expected.to_string()));
        assert_eq!(o.gas_limit, Some(125_000));
        assert_eq!(o.fees, settings.fees);
        assert_eq!(
            rpc.calls
                .lock()
                .unwrap()
                .iter()
                .any(|m| m == "eth_gasPrice"),
            !fixed
        );
        let requests = rpc.requests.lock().unwrap();
        let estimate = requests
            .iter()
            .find(|(m, _)| m == "eth_estimateGas")
            .unwrap();
        assert_eq!(estimate.1[0]["gasPrice"], json!(format!("0x{expected:x}")));
        assert!(o.signed_at_ms.is_none() && o.broadcast_at_ms.is_none());
        assert!(!requests.iter().any(|(m, _)| m == "eth_sendRawTransaction"));
    }
}

#[tokio::test]
async fn price_or_total_fee_cap_reject_before_signature_or_broadcast() {
    for price_cap in [false, true] {
        let rpc = Arc::new(Mock::default());
        let p = policy();
        let (t, _queue) = Trading::new(Store::memory().unwrap(), p.clone(), rpc.clone()).unwrap();
        let mut settings = BuySettings::from_policy(&p);
        if price_cap {
            settings.fees.as_mut().unwrap().gas_price_cap_wei = "1000000000".into();
        } else {
            settings.fees.as_mut().unwrap().gas_fee_cap_wei = "137499999999999".into();
        }
        t.update_settings(settings, t.view().unwrap().generation)
            .unwrap();
        t.set_mode(Mode::DryRun).await.unwrap();
        t.offer(&token(), "cap-cmd").unwrap();
        let error = t.process(&queued(&t).id).await.unwrap_err();
        assert!(error.contains(if price_cap {
            "Gas 价格超过"
        } else {
            "Gas 手续费超过"
        }));
        let o = queued(&t);
        assert!(o.nonce.is_none() && o.signed_at_ms.is_none() && o.broadcast_at_ms.is_none());
        assert!(!rpc
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|m| m == "eth_sendRawTransaction"));
        assert!(t.offer(&token(), "duplicate").is_err());
        assert_eq!(t.store.orders(None).unwrap().len(), 1);
    }
}

#[tokio::test]
async fn total_fee_boundary_and_reservation_exclude_buy_value_and_gas_reserve() {
    let rpc = Arc::new(Mock::default());
    let p = policy();
    let (t, _queue) = Trading::new(Store::memory().unwrap(), p.clone(), rpc.clone()).unwrap();
    let mut settings = BuySettings::from_policy(&p);
    settings.fees.as_mut().unwrap().gas_fee_cap_wei = "137500000000000".into();
    t.update_settings(settings, t.view().unwrap().generation)
        .unwrap();
    t.set_mode(Mode::DryRun).await.unwrap();
    t.offer(&token(), "boundary-cmd").unwrap();
    t.process(&queued(&t).id).await.unwrap();
    assert_eq!(queued(&t).reserved_wei, "5137500000000000");
    assert_eq!(queued(&t).state, OrderState::SimulationPassed);
}

#[tokio::test]
async fn old_saved_settings_migrate_without_reads_or_activation() {
    let store = Store::memory().unwrap();
    let old: BuySettings =
        serde_json::from_value(json!({"amountWei":"7000000000000000","slippageBps":234})).unwrap();
    assert!(old.fees.is_none());
    store.save_buy_settings(&old).unwrap();
    let p = policy();
    let rpc = Arc::new(Mock::default());
    let (t, _) = Trading::new(store, p.clone(), rpc.clone()).unwrap();
    let v = t.view().unwrap();
    assert_eq!(v.amount_wei, "7000000000000000");
    assert_eq!(v.slippage_bps, Some(234));
    assert_eq!(v.fees, Some(FeeSettings::from_policy(&p)));
    assert_eq!(v.mode, Mode::Disabled);
    assert!(!v.wallet_unlocked);
    assert!(rpc.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn legacy_queued_order_without_fee_authorization_cannot_be_replayed() {
    let (t, rpc, _queue) = fixture().await;
    t.offer(&token(), "legacy-cmd").unwrap();
    let o = queued(&t);
    let mut payload = serde_json::to_value(&o).unwrap();
    payload.as_object_mut().unwrap().remove("fees");
    t.store
        .0
        .lock()
        .unwrap()
        .execute(
            "UPDATE trade_orders SET payload=?2 WHERE id=?1",
            rusqlite::params![o.id, payload.to_string()],
        )
        .unwrap();
    rpc.calls.lock().unwrap().clear();
    assert!(t
        .process(&o.id)
        .await
        .unwrap_err()
        .contains("旧订单缺少费用授权"));
    assert!(rpc.calls.lock().unwrap().is_empty());
    assert!(queued(&t).signed_at_ms.is_none());
}

#[tokio::test]
async fn stale_or_missing_fee_confirmation_cannot_start_but_stop_is_unconditional() {
    let rpc = Arc::new(Mock::default());
    let p = policy();
    let (t, _) = Trading::new(Store::memory().unwrap(), p, rpc.clone()).unwrap();
    let v = t.view().unwrap();
    assert!(t
        .set_mode_with_fees(Mode::DryRun, Some(v.generation), false, None)
        .await
        .is_err());
    let mut changed = v.fees.clone().unwrap();
    changed.gas_multiplier_bps = 12000;
    assert!(t
        .set_mode_with_fees(Mode::DryRun, Some(v.generation), false, Some(changed))
        .await
        .is_err());
    assert!(rpc.calls.lock().unwrap().is_empty());
    t.set_mode_with_fees(Mode::DryRun, Some(v.generation), false, v.fees)
        .await
        .unwrap();
    t.set_mode_with_fees(Mode::Disabled, None, false, None)
        .await
        .unwrap();
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
}

#[tokio::test]
async fn stop_during_slow_preflight_cancels_without_signing_or_sending() {
    let (t, rpc, _queue) = fixture().await;
    rpc.hold.store(true, Ordering::Relaxed);
    t.offer(&token(), "cmd").unwrap();
    let id = queued(&t).id;
    let worker = t.clone();
    let job = tokio::spawn(async move { worker.process(&id).await });
    tokio::time::timeout(std::time::Duration::from_secs(3), rpc.started.notified())
        .await
        .unwrap();
    t.set_mode_checked(Mode::Disabled, None, false)
        .await
        .unwrap();
    assert_eq!(queued(&t).state, OrderState::Cancelled);
    rpc.release.notify_one();
    assert!(job.await.unwrap().is_err());
    assert!(queued(&t).signed_at_ms.is_none() && queued(&t).tx_hash.is_none());
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "eth_sendRawTransaction"));
}

#[cfg(feature = "live-trading")]
#[tokio::test]
async fn stop_wins_during_activation_and_unlimited_budget_requires_confirmation() {
    let p = Policy {
        allow_live: true,
        slippage_bps: Some(100),
        ..Policy::default()
    };
    let rpc = Arc::new(Mock::default());
    let (t, _) = Trading::new(Store::memory().unwrap(), p, rpc.clone()).unwrap();
    {
        let mut c = t.control.lock().unwrap();
        c.wallet = Some(Arc::new(Wallet::test_wallet()));
        c.nonce_ready = true;
    }
    let g = t.view().unwrap().generation;
    assert!(t
        .set_mode_checked(Mode::Live, Some(g), false)
        .await
        .is_err());
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    rpc.hold_activation.store(true, Ordering::Relaxed);
    let start = t.clone();
    let g = t.view().unwrap().generation;
    let job = tokio::spawn(async move { start.set_mode_checked(Mode::Live, Some(g), true).await });
    tokio::time::timeout(std::time::Duration::from_secs(3), rpc.started.notified())
        .await
        .unwrap();
    t.set_mode_checked(Mode::Disabled, None, false)
        .await
        .unwrap();
    rpc.release.notify_one();
    assert!(job.await.unwrap().is_err());
    assert_eq!(t.view().unwrap().mode, Mode::Disabled);
    assert!(t.store.orders(None).unwrap().is_empty());
    assert!(!rpc
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "eth_sendRawTransaction"));
}
