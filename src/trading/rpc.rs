//! Dedicated trading HTTP transport: no monitor WSS contention, no automatic write retries.
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

pub type RpcFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;
pub trait TradeRpc: Send + Sync {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a>;
}
pub struct HttpRpc {
    url: Option<String>,
    client: reqwest::Client,
    id: AtomicU64,
}
impl HttpRpc {
    pub fn new(url: Option<String>) -> Result<Self, String> {
        if let Some(raw) = &url {
            let u = url::Url::parse(raw).map_err(|_| "invalid trading RPC URL")?;
            if u.scheme() != "https" || u.host_str().is_none() {
                return Err("交易 RPC 必须使用 HTTPS".into());
            }
        }
        Ok(Self {
            url,
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(2))
                .timeout(std::time::Duration::from_secs(4))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| e.to_string())?,
            id: AtomicU64::new(1),
        })
    }
}
impl TradeRpc for HttpRpc {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> RpcFuture<'a> {
        Box::pin(async move {
            if method == "eth_sendRawTransaction" && !cfg!(feature = "live-trading") {
                return Err("普通构建没有广播能力".into());
            }
            let url = self.url.as_ref().ok_or("未配置独立交易 RPC")?;
            let id = self.id.fetch_add(1, Ordering::Relaxed);
            let mut response = self
                .client
                .post(url)
                .json(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .send()
                .await
                .map_err(|_| "交易 RPC 请求失败/超时；写请求结果可能未知".to_string())?;
            if !response.status().is_success() {
                return Err(format!("交易 RPC HTTP {}", response.status()));
            }
            if response.content_length().is_some_and(|n| n > 2_000_000) {
                return Err("RPC response too large".into());
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "incomplete trading RPC response")?
            {
                if body.len().saturating_add(chunk.len()) > 2_000_000 {
                    return Err("RPC response too large".into());
                }
                body.extend_from_slice(&chunk);
            }
            let value: Value =
                serde_json::from_slice(&body).map_err(|_| "invalid trading RPC response")?;
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                return Err("RPC response id mismatch".into());
            }
            if let Some(error) = value.get("error") {
                // Never put request URL (possibly containing API credentials) into logs.
                return Err(format!(
                    "RPC {} rejected request (code {})",
                    method,
                    error.get("code").and_then(Value::as_i64).unwrap_or(0)
                ));
            }
            value
                .get("result")
                .cloned()
                .ok_or_else(|| "missing RPC result".into())
        })
    }
}
