// 只回传页面/容器观察，不把“空 canvas 存在”冒充 K 线已经绘制完成。
// 后台另有独立 WebSocket 心跳，页面休眠不再是维持连接的唯一手段。
const CHART_CONTAINER_SELECTORS = [
  'iframe[src*="tradingview"]',
  'iframe[id*="tradingview"]',
  'iframe[src*="charting_library"]',
  '[id*="tradingview"] canvas',
  '[data-name="chart-container"] canvas',
  '.tv-lightweight-charts canvas',
  '[class*="chart-container"] canvas',
];
let currentToken = null;
let documentReported = false;
let containerReported = false;
let observer = null;
let framePending = false;
let stopTimer = null;

function tokenFromPath() {
  return location.pathname.match(/^\/bsc\/token\/(0x[0-9a-f]{40})\/?$/i)?.[1].toLowerCase() || null;
}

function send(payload) {
  try { chrome.runtime.sendMessage(payload).catch(() => {}); } catch (_) {}
}

function report(phase) {
  send({ type: "GMGN_PAGE_PROGRESS", token: currentToken, phase, pageClockMs: Date.now() });
}

function chartContainerVisible() {
  return CHART_CONTAINER_SELECTORS.some(selector => [...document.querySelectorAll(selector)].some(el => {
    const rect = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    return rect.width >= 200 && rect.height >= 100 && rect.bottom > 0 && rect.right > 0
      && rect.top < innerHeight && rect.left < innerWidth
      && style.display !== "none" && style.visibility !== "hidden";
  }));
}

function stopObserver() {
  observer?.disconnect();
  observer = null;
  clearTimeout(stopTimer);
}

function checkPage() {
  framePending = false;
  if (tokenFromPath() !== currentToken) { watchPage(); return; }
  if (!currentToken) return;
  if (!documentReported && document.readyState !== "loading") {
    documentReported = true;
    report("DOCUMENT_READY");
  }
  if (!containerReported && chartContainerVisible()) {
    containerReported = true;
    report("CHART_CONTAINER");
    stopObserver();
  }
}

function scheduleCheck() {
  if (framePending) return;
  framePending = true;
  requestAnimationFrame(checkPage);
}

function watchPage() {
  const token = tokenFromPath();
  if (token === currentToken) return;
  stopObserver();
  currentToken = token;
  documentReported = false;
  containerReported = false;
  if (!token) return;
  checkPage();
  if (!containerReported && document.documentElement) {
    observer = new MutationObserver(scheduleCheck);
    observer.observe(document.documentElement, { childList: true, subtree: true });
    stopTimer = setTimeout(stopObserver, 30000);
  }
}

function heartbeat() {
  send({ type: "GMGN_HEARTBEAT", url: location.href });
  watchPage();
}

document.addEventListener("DOMContentLoaded", () => { watchPage(); checkPage(); }, { once: true });
document.addEventListener("visibilitychange", heartbeat);
heartbeat();
setInterval(heartbeat, 1000);
