(function () {
  'use strict';
  const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const formatter = new Intl.DateTimeFormat('sv-SE', {timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
  function timeText(ms, precision = 'ms') {
    if (ms == null || !Number.isFinite(ms)) return '—';
    return formatter.format(new Date(ms)) + (precision === 'ms' ? '.' + String(ms % 1000).padStart(3,'0') : '');
  }
  function timingCell(confirmed, precision, chain) {
    if (confirmed == null) return '<td>—</td>';
    const delta = chain == null ? null : confirmed-chain;
    const delay = delta == null ? '' : delta < 0 ? '<small>本地与链上时间异常，未计算延迟</small>' : `<small>相差 ${(delta/1000).toFixed(3)} 秒（链上时间为秒级）</small>`;
    return `<td class="historyTime"><strong>${escapeHtml(timeText(confirmed,precision))}</strong>${precision==='s'?'<small>秒级旧记录</small>':''}<small>链上条件：${chain == null?'未知':escapeHtml(timeText(chain,'s'))}</small>${delay}</td>`;
  }
  function historyRow(row) {
    const platform = ({FLAP:'Flap',FOUR_MEME:'Four.meme',GENIUS_FUN:'GeniusFun'})[row.platform] || row.platform;
    return `<tr><td>${escapeHtml(row.name||row.symbol||'未命名')}</td><td>${escapeHtml(platform)}</td><td class="ca">${escapeHtml(row.ca)}</td>${timingCell(row.firstAtMs,row.firstPrecision,row.firstChainAtMs)}${timingCell(row.secondAtMs,row.secondPrecision,row.secondChainAtMs)}<td>${escapeHtml(row.state)}</td></tr>`;
  }
  if (typeof module !== 'undefined') module.exports = {timeText,timingCell,historyRow};
  if (typeof document === 'undefined') return;
  const el=id=>document.getElementById(id);
  let active=false,offset=0,total=0,query='',request=0,controller=null;
  const limit=50;
  async function refreshHistory() {
    if (!active) return;
    const id=++request;
    controller?.abort();
    controller=new AbortController();
    el('historyStatus').textContent='读取中…';
    try {
      const params=new URLSearchParams({limit:String(limit),offset:String(offset),q:query});
      const response=await fetch('/api/b-history?'+params,{cache:'no-store',signal:controller.signal});
      if (!response.ok) throw Error('HTTP '+response.status+'：'+await response.text());
      const page=await response.json();
      if(id!==request || !active) return;
      total=page.total;
      if(offset>=total && offset>0){offset=Math.max(0,Math.ceil(total/limit)-1)*limit;return refreshHistory();}
      el('historyRows').innerHTML=page.rows.length?page.rows.map(historyRow).join(''):'<tr><td colspan="6" class="empty">没有符合条件的 B 币历史</td></tr>';
      el('historyStatus').textContent=`共 ${total} 个币`;
      el('historyPage').textContent=`第 ${Math.floor(offset/limit)+1} / ${Math.max(1,Math.ceil(total/limit))} 页`;
      el('historyPrev').disabled=offset===0;
      el('historyNext').disabled=offset+limit>=total;
    } catch(error) {
      if(error.name!=='AbortError' && id===request) el('historyStatus').textContent='历史读取失败：'+error.message;
    }
  }
  function selectHistory(value) {
    active=value;
    el('candidatePanel').hidden=value;el('historyPanel').hidden=!value;
    el('candidateTab').className=value?'':'active';el('historyTab').className=value?'active':'';
    el('candidateTab').setAttribute('aria-pressed',String(!value));el('historyTab').setAttribute('aria-pressed',String(value));
    if(value)refreshHistory();else{request++;controller?.abort();}
  }
  el('candidateTab').onclick=()=>selectHistory(false);
  el('historyTab').onclick=()=>selectHistory(true);
  el('historyRefresh').onclick=refreshHistory;
  el('historySearchForm').onsubmit=event=>{event.preventDefault();query=el('historySearch').value.trim();offset=0;refreshHistory();};
  el('historyPrev').onclick=()=>{offset=Math.max(0,offset-limit);refreshHistory();};
  el('historyNext').onclick=()=>{if(offset+limit<total){offset+=limit;refreshHistory();}};
  setInterval(()=>{if(active&&!document.hidden)refreshHistory();},5000);
})();
