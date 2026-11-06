(function () {
  'use strict';
  const el = id => document.getElementById(id);
  let busy = false, accepted = false, exiting = false, loading = false, view = null;
  function gate() {
    window.systemStopping = busy || accepted || !!view?.stopping;
    el('shutdownSystem').disabled = busy || !view || !view.browserConnected || !view.closeSupported
      || !['RUNNING','FAILED'].includes(view.phase);
    if (window.systemStopping) {
      ['tradeStart','tradeDryRun','tradeSave','resetMonitor'].forEach(id => { if (el(id)) el(id).disabled = true; });
    }
  }
  async function refresh() {
    if (loading) return;
    loading = true;
    try {
      const r = await fetch('/api/system', {cache:'no-store'});
      if (r.status === 404) throw Error('后端尚未更新，请更新后端并重新加载插件');
      if (!r.ok) throw Error('HTTP '+r.status);
      view = await r.json();
      exiting ||= view.phase === 'EXITING';
      if (view.phase === 'FAILED') accepted = false;
      el('systemStatus').textContent = view.phase === 'RUNNING' && (!view.browserConnected || !view.closeSupported)
        ? '插件未连接或版本较旧；请重新加载 Meme Quant 插件后使用关闭按钮。' : view.message;
    } catch (e) {
      el('systemStatus').textContent = exiting ? '后端连接已关闭。'+(view?.message || '')+' 再次使用请运行 start-system.ps1，再通过插件创建/修复四窗口。'
        : accepted ? '关闭请求已接收，后端连接已断开；最终结果未确认，请检查四个窗口和后端。'
        : '无法读取系统状态：'+e.message;
      if (!accepted) view = null;
    } finally { loading = false; gate(); }
  }
  el('shutdownSystem').onclick = async () => {
    if (el('shutdownSystem').disabled || busy) return;
    if (!confirm('关闭后端和甲乙丙丁四个 GMGN 专用窗口？\n\n将停止买入及调度、锁定钱包，并保存现有数据。\n不会卖出持仓；已广播的交易不能撤回。\n其他浏览器窗口不受影响。再次启动使用 start-system.ps1 和插件。')) return;
    busy = true; gate(); el('systemStatus').textContent = '正在提交关闭请求…';
    try {
      const r = await fetch('/api/system/shutdown', {method:'POST', headers:window.controlHeaders(), body:JSON.stringify({confirm:true})});
      if (!r.ok) throw Error(await r.text());
      const result = await r.json();
      if (!result.accepted || !result.requestId) throw Error('关闭请求未获确认');
      accepted = true; await refresh();
    } catch (e) {
      el('systemStatus').textContent = '关闭提交未确认：'+e.message+'。请刷新状态后核对，勿重复盲点。';
    } finally { busy = false; gate(); }
  };
  refresh(); setInterval(refresh, 500);
})();
