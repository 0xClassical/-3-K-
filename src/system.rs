//! Runtime-only shutdown latch. No wallet data or token lifecycle state lives here.
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase {
    Running,
    Saving,
    PreparingWindows,
    ClosingWindows,
    Exiting,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrowserAckPhase {
    Ready,
    Closed,
    Failed,
}

#[derive(Debug)]
pub struct BrowserAck {
    pub phase: BrowserAckPhase,
    pub error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct View {
    pub phase: Phase,
    pub stopping: bool,
    pub browser_connected: bool,
    pub close_supported: bool,
    pub request_id: Option<String>,
    pub message: String,
}

struct Inner {
    phase: Phase,
    browser_connected: bool,
    close_supported: bool,
    request_id: Option<String>,
    message: String,
    ack_tx: Option<mpsc::UnboundedSender<BrowserAck>>,
}

pub struct Control {
    stopping: AtomicBool,
    inner: Mutex<Inner>,
    exit: watch::Sender<bool>,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            stopping: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                phase: Phase::Running,
                browser_connected: false,
                close_supported: false,
                request_id: None,
                message: "关闭会保留数据，不会卖出持仓。".into(),
                ack_tx: None,
            }),
            exit: watch::channel(false).0,
        }
    }
}

impl Control {
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }
    pub fn view(&self) -> View {
        let c = self.inner.lock().expect("system control");
        View {
            phase: c.phase,
            stopping: self.is_stopping(),
            browser_connected: c.browser_connected,
            close_supported: c.close_supported,
            request_id: c.request_id.clone(),
            message: c.message.clone(),
        }
    }
    pub fn browser_connected(&self, supported: bool) {
        let mut c = self.inner.lock().expect("system control");
        c.browser_connected = true;
        c.close_supported = supported;
    }
    pub fn browser_disconnected(&self) {
        let mut c = self.inner.lock().expect("system control");
        c.browser_connected = false;
        c.close_supported = false;
    }
    pub fn begin(&self) -> Result<(String, mpsc::UnboundedReceiver<BrowserAck>), String> {
        let mut c = self.inner.lock().map_err(|_| "system control poisoned")?;
        if !matches!(c.phase, Phase::Running | Phase::Failed) {
            return Err("关闭请求已在执行，请勿重复提交".into());
        }
        if !c.browser_connected || !c.close_supported {
            return Err("请连接/重新加载新版 Meme Quant 插件；旧插件不能安全关闭四窗口".into());
        }
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        c.request_id = Some(id.clone());
        c.ack_tx = Some(tx);
        c.phase = Phase::Saving;
        c.message = "正在停止买入和调度，并保存状态…".into();
        self.stopping.store(true, Ordering::SeqCst);
        Ok((id, rx))
    }
    pub fn set_phase(&self, phase: Phase, message: impl Into<String>) {
        let mut c = self.inner.lock().expect("system control");
        c.phase = phase;
        c.message = message.into();
        if matches!(phase, Phase::Failed | Phase::Exiting) {
            c.ack_tx = None;
        }
    }
    pub fn ack(&self, id: &str, ack: BrowserAck) {
        let c = self.inner.lock().expect("system control");
        if c.request_id.as_deref() == Some(id) {
            if let Some(tx) = &c.ack_tx {
                let _ = tx.send(ack);
            }
        }
    }
    pub fn request_exit(&self) {
        self.exit.send_replace(true);
    }
    pub async fn wait_exit(&self) {
        let mut rx = self.exit.subscribe();
        let _ = rx.wait_for(|v| *v).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn shutdown_requires_capable_browser_and_is_idempotent() {
        let c = Control::default();
        assert!(c.begin().is_err());
        assert!(!c.is_stopping());
        c.browser_connected(false);
        assert!(c.begin().is_err());
        c.browser_connected(true);
        let (id, mut rx) = c.begin().unwrap();
        assert!(c.is_stopping());
        assert!(c.begin().is_err());
        c.ack(
            "stale-id",
            BrowserAck {
                phase: BrowserAckPhase::Ready,
                error: None,
            },
        );
        assert!(rx.try_recv().is_err());
        c.ack(
            &id,
            BrowserAck {
                phase: BrowserAckPhase::Ready,
                error: None,
            },
        );
        assert_eq!(rx.recv().await.unwrap().phase, BrowserAckPhase::Ready);
        c.set_phase(Phase::Failed, "retry");
        let (new_id, mut new_rx) = c.begin().unwrap();
        assert_ne!(id, new_id);
        c.ack(
            &id,
            BrowserAck {
                phase: BrowserAckPhase::Closed,
                error: None,
            },
        );
        assert!(new_rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn exit_notification_cannot_be_lost() {
        let c = Control::default();
        c.request_exit();
        tokio::time::timeout(std::time::Duration::from_millis(100), c.wait_exit())
            .await
            .unwrap();
    }
}
