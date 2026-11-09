(function(root){
  'use strict';
  function evaluate(h, now, status) {
    if (!h) return {level:'warn',summary:'后端在线；监控完整性尚未确认',detail:status||''};
    const age = t => Number(t)>0 ? Math.max(0,(now-Number(t))/1000) : Infinity;
    const gap = Number(h.gapBlocks||0), headAge=age(h.stream?.lastHeaderAtMs), scanAge=age(h.lastSuccessAtMs);
    const issues=[];
    if (!String(status||'').startsWith('live:')) issues.push('WebSocket 未确认在线');
    if (headAge>20) issues.push('区块头停止更新或尚未收到');
    if (h.state==='retrying') issues.push(/rate limit|limit exceeded|429/i.test(h.lastError||'')?'节点限流':'补扫请求失败');
    if (gap>100) issues.push('补扫落后 '+gap.toLocaleString('en-US')+' 个区块');
    if (scanAge>20) issues.push(Number.isFinite(scanAge)?'完整补扫已停滞 '+Math.floor(scanAge)+' 秒':'完整补扫尚未验证');
    if (h.state!=='verified' && !issues.length) issues.push('正在验证补扫完整性');
    const retry=Math.max(0,Math.ceil((Number(h.nextRetryAtMs||0)-now)/1000));
    const detail='链头 '+(h.headBlock||'未知')+' · 已完整补扫 '+(h.scannedBlock||'未知')
      +' · 区块头 '+(Number.isFinite(headAge)?headAge.toFixed(1)+' 秒前':'未知')
      +(retry?' · '+retry+' 秒后重试':'')
      +(h.lastError?' · '+String(h.lastError).slice(0,600):'');
    return {level:issues.length?'warn':'ok', summary:issues.length?'监控异常：'+issues.join('；'):'监控正常：订阅在线且补扫已验证',detail};
  }
  if (typeof module!=='undefined' && module.exports) module.exports={evaluate};
  if (!root.document) return;
  let lastHealth=null, serverTime=0, received=0, wsStatus='', failed=false, busy=false;
  const el=id=>root.document.getElementById(id);
  function render() {
    const now=(serverTime||Date.now())+(serverTime?Math.max(0,Date.now()-received):0);
    const v=failed?{level:'warn',summary:'后端未连接或健康状态无法读取',detail:'请检查后端；不要把页面缓存当作实时数据。'}:evaluate(lastHealth,now,wsStatus);
    if(el('dot')) el('dot').className='dot '+v.level;
    if(el('status')) el('status').textContent=v.summary;
    if(el('monitorHealthSummary')) el('monitorHealthSummary').textContent=v.summary;
    if(el('monitorHealthDetails')) el('monitorHealthDetails').textContent=v.detail;
  }
  root.updateMonitorConnection=function(status,h,time){
    wsStatus=status||''; failed=false;
    if(h){lastHealth=h;serverTime=Number(time)||0;received=Date.now();}
    render();
  };
  root.monitorConnectionFailed=function(){failed=true;render();};
  async function poll(){
    if(busy)return;busy=true;
    try{const r=await root.fetch('/api/health',{cache:'no-store'});if(!r.ok)throw Error('health HTTP '+r.status);
      const v=await r.json();lastHealth=v.ingestion;wsStatus=v.wss||'';serverTime=Number(v.serverTimeMs)||0;received=Date.now();failed=false;
    }catch(_){failed=true;}finally{busy=false;render();}
  }
  poll();root.setInterval(poll,3000);
})(typeof window!=='undefined'?window:globalThis);
