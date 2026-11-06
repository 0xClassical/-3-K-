(function(root) {
  "use strict";
  const escape = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  function interval(start, end) {
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end)) return "待观测";
    if (end < start) return "时钟偏差";
    return `${end - start} ms`;
  }
  function render(state) {
    const records = state.browserTimings || [];
    if (!records.length) return '<tr><td colspan="6" class="empty">本次后端启动后尚无跳转记录；旧版插件需重新加载</td></tr>';
    return records.map(t => `<tr><td>${escape(t.slotId)}</td>
      <td>${interval(t.openSentAtMs,t.receivedAtMs)}</td>
      <td>${interval(t.receivedAtMs,t.navigationRequestedAtMs)}</td>
      <td>${interval(t.navigationRequestedAtMs,t.urlConfirmedAtMs)}</td>
      <td>${interval(t.navigationRequestedAtMs,t.documentReadyAtMs)}</td>
      <td>${interval(t.navigationRequestedAtMs,t.chartContainerAtMs)}</td></tr>`).join("");
  }
  root.renderBrowserLatency = state => {
    const element = root.document?.getElementById("latencyRows");
    if (element) element.innerHTML = render(state);
    const blocked = (state.slotStates || []).filter(s => s.state === "BLOCKED").map(s => s.slotId);
    const warning = root.document?.getElementById("slotWarning");
    if (warning) warning.textContent = blocked.length ? `窗口 ${blocked.join("、")} 当前不是已确认的可调度页面；返回 GMGN BSC 首页后才允许补位。` : "";
  };
  if (typeof module !== "undefined") module.exports = { interval, render };
})(globalThis);
