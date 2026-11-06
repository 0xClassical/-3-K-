(function () {
  'use strict';
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const time = ms => ms == null ? '—' : new Intl.DateTimeFormat('sv-SE', {
    timeZone:'Asia/Shanghai', year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', second:'2-digit', hourCycle:'h23'
  }).format(new Date(ms)) + '.' + String(ms % 1000).padStart(3,'0');
  const states = {QUEUED:'排队',PREPARING:'预检',SIMULATION_PASSED:'模拟通过（未交易）',
    SIGNED:'已签名（待核对）',BROADCAST_UNKNOWN:'广播结果未知',SUBMITTED:'已提交，未成交确认',
    INCLUDED:'已入块，等待确认',CONFIRMED:'已确认',FAILED:'失败（不重买）',CANCELLED:'未广播已取消',NEEDS_REVIEW:'需人工核对'};
  function row(o) {
    const times = (...values) => values.map(([label,ms]) => `${esc(label)}：${esc(time(ms))}`).join('<br>');
    const hash = /^0x[0-9a-f]{64}$/i.test(o.txHash || '') ?
      `<a href="https://bscscan.com/tx/${esc(o.txHash)}" target="_blank" rel="noopener noreferrer">${esc(o.txHash)}</a>` : '—';
    return `<tr><td class="ca">${esc(o.intent.ca)}<br>Slot ${esc(o.intent.slotId)} · ${esc(o.mode)}<br>金额：${esc(amountText(o.amountWei)||'未知')} BNB · 滑点：${esc(o.slippageBps==null?'未知':slippageText(o.slippageBps)+'%')}<br>费用授权：${esc(feeSummary(o.fees))}<br>预检 Gas：${esc(o.gasPriceWei==null?'未知':gweiText(o.gasPriceWei)+' Gwei')} × ${esc(o.gasLimit??'未知')}</td><td>${esc(states[o.state]||o.state)}</td><td class="historyTime">${times(['B',o.intent.bAtMs],['报价',o.quoteAtMs],['签名',o.signedAtMs])}</td><td class="historyTime">${times(['广播交接',o.broadcastAtMs],['入块',o.includedAtMs],['确认',o.confirmedAtMs])}</td><td>净流入：${esc(o.receivedRaw??'未知')}<br>余额：${esc(o.tokenBalanceRaw??'未知')}<br>Gas 实付 wei：${esc(o.gasPaidWei??'未知')}<br><small>余额时间：${esc(time(o.tokenBalanceAtMs))}</small></td><td>${esc(o.reason??'—')}<br>${hash}</td></tr>`;
  }
  function bnb(wei) {
    if (wei == null || !/^\d+$/.test(String(wei))) return '未知';
    const n = BigInt(wei), unit = 10n**18n;
    return String(n/unit)+'.'+String(n%unit).padStart(18,'0').slice(0,6);
  }
  function amountText(wei) {
    return unitText(wei,18);
  }
  function unitText(wei,digits) {
    if (wei == null || !/^\d+$/.test(String(wei))) return '';
    const n=BigInt(wei), unit=10n**BigInt(digits), fraction=String(n%unit).padStart(digits,'0').replace(/0+$/,'');
    return String(n/unit)+(fraction?'.'+fraction:'');
  }
  const gweiText=wei=>unitText(wei,9);
  function scaled(raw, digits, label) {
    const s=String(raw).trim();
    if (!new RegExp('^\\d+(?:\\.\\d{1,'+digits+'})?$').test(s)) throw Error(label+'格式错误：不能使用科学计数法或超出小数精度');
    const [whole,fraction='']=s.split('.');
    return BigInt(whole)*10n**BigInt(digits)+BigInt(fraction.padEnd(digits,'0'));
  }
  function parseSettings(amount, slippage) {
    const wei=scaled(amount,18,'金额'), bps=scaled(slippage,2,'滑点');
    if (wei<=0n || wei>2n**128n-1n) throw Error('单笔金额必须大于零，且不能溢出');
    if (bps>=10000n) throw Error('滑点必须在 0% 至 99.99% 之间，最多两位小数');
    return {amountWei:String(wei),slippageBps:Number(bps)};
  }
  function parseFees(mode,markup,fixed,priceCap,feeCap) {
    if (!['AUTOMATIC','FIXED'].includes(mode)) throw Error('Gas 模式无效');
    const cap=scaled(priceCap,9,'Gas 价格上限'), total=scaled(feeCap,18,'Gas 手续费上限');
    const positive=(v,label)=>{if(v<=0n || v>2n**128n-1n)throw Error(label+'必须大于零且不能溢出');};
    positive(cap,'Gas 价格上限'); positive(total,'Gas 手续费上限');
    const extra=mode==='AUTOMATIC'?scaled(markup,2,'Gas 加价率'):1000n;
    if(extra>40000n)throw Error('Gas 加价率必须在 0% 至 400% 之间');
    let fixedWei=null;
    if(mode==='FIXED') {const n=scaled(fixed,9,'固定 Gas 价格'); positive(n,'固定 Gas 价格'); if(n>cap)throw Error('固定 Gas 价格不能超过价格上限'); fixedWei=String(n);}
    return {gasMode:mode,gasMultiplierBps:Number(10000n+extra),fixedGasPriceWei:fixedWei,gasPriceCapWei:String(cap),gasFeeCapWei:String(total)};
  }
  function feeSummary(f) {
    if(!f)return '旧记录：未知（不能重新执行）';
    if(!['AUTOMATIC','FIXED'].includes(f.gasMode) || !Number.isSafeInteger(f.gasMultiplierBps)
      || f.gasMultiplierBps<10000 || f.gasMultiplierBps>50000)return '费用参数无效';
    const mode=f.gasMode==='FIXED'?'固定 '+gweiText(f.fixedGasPriceWei)+' Gwei':'建议价 + '+slippageText(f.gasMultiplierBps-10000)+'%';
    return mode+'；价格上限 '+gweiText(f.gasPriceCapWei)+' Gwei；手续费上限 '+amountText(f.gasFeeCapWei)+' BNB';
  }
  const slippageText = bps => bps == null ? '' : amountText(String(BigInt(bps)*10n**16n));
  const unlimited = v => v.sessionBudgetWei === '0' || v.dailyBudgetWei === '0';
  function readiness(v) {
    const missing=[];
    if (!v.liveBuild) missing.push('后端未包含实盘功能');
    if (!v.liveAllowed) missing.push('实盘配置 allowLive 尚未许可');
    if (!v.rpcConfigured) missing.push('未配置独立交易 RPC');
    if (!v.walletConfigured) missing.push('未配置加密钱包文件');
    if (!v.walletUnlocked || !v.nonceReady) missing.push('请先在台式机本机解锁钱包（解锁不会开启买入）');
    if (!v.policyReady) missing.push('买入参数尚未通过校验');
    if (!v.fees) missing.push('后端未提供费用授权，请部署新版并刷新页面');
    return missing;
  }
  if (typeof module !== 'undefined') module.exports = {row,bnb,time,esc,amountText,parseSettings,parseFees,gweiText,feeSummary,slippageText,readiness,unlimited};
  if (typeof document === 'undefined') return;
  const el = id => document.getElementById(id);
  const local = ['localhost','127.0.0.1','[::1]','::1'].includes(location.hostname);
  window.controlHeaders = function () {
    const headers = {'content-type':'application/json','x-meme-control':'1'};
    const token = el('controlToken').value.trim();
    if (token) headers.authorization = 'Bearer '+token;
    return headers;
  };
  let busy = false, loading = false, stopping = false, dirty = false, view = null;
  function buttons() {
    const editable=!!view?.fees && view.mode==='DISABLED' && !busy && !window.systemStopping;
    el('tradeSave').disabled=!editable;
    el('tradeAmount').disabled=!editable;
    el('tradeSlippage').disabled=!editable;
    ['tradeGasMode','tradeGasPriceCap','tradeGasFeeCap'].forEach(id=>el(id).disabled=!editable);
    el('tradeGasMarkup').disabled=!editable || el('tradeGasMode').value!=='AUTOMATIC';
    el('tradeFixedGas').disabled=!editable || el('tradeGasMode').value!=='FIXED';
    el('tradeStart').disabled=!view || busy || stopping || window.systemStopping || dirty || view.mode!=='DISABLED' || readiness(view).length>0;
    el('tradeDryRun').disabled=!view || busy || stopping || window.systemStopping || dirty || !view.rpcConfigured || !view.canSimulate || !view.policyReady || view.mode!=='DISABLED';
    // Stop is deliberately independent of busy: it must work during a slow enable/unlock.
    el('tradePause').disabled=stopping;
    el('tradeLock').disabled=stopping;
  }
  async function refresh() {
    if (loading) return;
    loading = true;
    try {
      const r = await fetch('/api/trading',{cache:'no-store'});
      if (r.status === 404) throw Error('当前后端尚未部署交易安全新版；原监控仍可运行');
      if (!r.ok) throw Error(await r.text());
      const v = await r.json();
      view=v;
      el('tradeModeBadge').textContent={DISABLED:'已关闭',DRY_RUN:'只读模拟',LIVE:'已开启'}[v.mode]||v.mode;
      el('tradeModeBadge').classList.toggle('live',v.mode==='LIVE');
      el('tradeStatus').textContent = `钱包${v.walletUnlocked?'已解锁':'锁定'}${v.wallet?' · 地址：'+v.wallet:''} · BNB 余额：${bnb(v.bnbBalanceWei)} · ${v.lastError||'正常'}`;
      if (!dirty) {
        el('tradeAmount').value=amountText(v.amountWei);
        el('tradeSlippage').value=slippageText(v.slippageBps);
        if(v.fees) {
          el('tradeGasMode').value=v.fees.gasMode;
          el('tradeGasMarkup').value=slippageText(v.fees.gasMultiplierBps-10000);
          el('tradeFixedGas').value=gweiText(v.fees.fixedGasPriceWei);
          el('tradeGasPriceCap').value=gweiText(v.fees.gasPriceCapWei);
          el('tradeGasFeeCap').value=amountText(v.fees.gasFeeCapWei);
        }
      }
      el('tradeGasCeiling').textContent=v.fees?`后端保护上限：价格 ${gweiText(v.gasPriceCeilingWei)} Gwei；单笔 Gas ${amountText(v.gasFeeCeilingWei)} BNB；Gas Limit ${v.gasLimitCap}。网页不能突破这些保护上限。`:'费用控制新版尚未加载，请刷新并确认后端版本。';
      el('tradeBudgetNote').textContent=`会话预算：${v.sessionBudgetWei==='0'?'不设限':amountText(v.sessionBudgetWei)+' BNB'} · 每日预算：${v.dailyBudgetWei==='0'?'不设限':amountText(v.dailyBudgetWei)+' BNB'}。未设置的预算不限制累计买入次数或金额；单笔金额、Gas 和余额保护仍生效。`;
      const missing=readiness(v);
      el('tradeReadiness').textContent=missing.length?'开启前还需要：'+missing.join('；'):'开启准备就绪；只有你点击并确认后才开始买入。';
      el('tradeRows').innerHTML = v.orders.length ? v.orders.map(row).join('') : '<tr><td colspan="6" class="empty">无订单；默认不产生买入，不解锁钱包</td></tr>';
      el('walletUnlockForm').hidden = !local;
    } catch (e) {
      view=null;
      el('tradeStatus').textContent = e.message;
      el('walletUnlockForm').hidden = true;
    } finally { loading = false; buttons(); }
  }
  async function post(path,body) {
    if (busy) return;
    busy = true;
    buttons();
    try {
      const r = await fetch(path,{method:'POST',headers:window.controlHeaders(),body:JSON.stringify(body)});
      if (!r.ok) throw Error(await r.text());
      if (path==='/api/trading/settings') {
        dirty=false;
        el('tradeSettingsStatus').textContent='参数已保存到后端；自动买入仍关闭。';
      }
      await refresh();
    } catch (e) { alert(e.message); }
    finally { busy = false; buttons(); await refresh(); }
  }
  async function stop(lockWallet) {
    if (stopping) return;
    stopping=true; buttons();
    el('tradeStatus').textContent='正在关闭买入并取消未广播意图…';
    try {
      const r=await fetch(lockWallet?'/api/trading/lock':'/api/trading/mode',{
        method:'POST',headers:window.controlHeaders(),body:JSON.stringify(lockWallet?{}:{mode:'DISABLED'})});
      if (!r.ok) throw Error(await r.text());
      el('tradeStatus').textContent='后端已确认关闭；已广播交易仍需核对，无法撤回。';
    } catch(e) {
      alert('关闭请求尚未得到后端确认，不能认为已停止：'+e.message);
    } finally { stopping=false; buttons(); await refresh(); }
  }
  el('tradePause').onclick=()=>stop(false);
  el('tradeLock').onclick=()=>stop(true);
  function changed() {
    dirty=true; el('tradeSettingsStatus').textContent='参数尚未保存；请先保存，再开启。'; buttons();
  }
  ['tradeAmount','tradeSlippage','tradeGasMarkup','tradeFixedGas','tradeGasPriceCap','tradeGasFeeCap'].forEach(id=>el(id).oninput=changed);
  el('tradeGasMode').onchange=changed;
  el('tradeSettingsForm').onsubmit=event=>{
    event.preventDefault();
    if (!view || view.mode!=='DISABLED') return;
    try {
      const fees=parseFees(el('tradeGasMode').value,el('tradeGasMarkup').value,el('tradeFixedGas').value,el('tradeGasPriceCap').value,el('tradeGasFeeCap').value);
      if(BigInt(fees.gasPriceCapWei)>BigInt(view.gasPriceCeilingWei)||BigInt(fees.gasFeeCapWei)>BigInt(view.gasFeeCeilingWei))throw Error('Gas 设置不能超过后端保护上限');
      post('/api/trading/settings',{...parseSettings(el('tradeAmount').value,el('tradeSlippage').value),fees,expectedGeneration:view.generation});
    } catch(e) { alert(e.message); }
  };
  el('tradeStart').onclick=()=>{
    if (!view || el('tradeStart').disabled) return;
    const v=view, noBudget=unlimited(v);
    if (confirm(`开启真实自动买入？\n单笔：${amountText(v.amountWei)} BNB\n滑点：${slippageText(v.slippageBps)}%\nGas：${feeSummary(v.fees)}\n原交易 RPC 发送；私有通道/Builder 小费未接入。\n${noBudget?'至少一项累计预算未设限，可能持续消耗 BNB。':'按已配置累计预算限制。'}\n只买开启后首次成为 B 且当场有空闲 Slot 的新币。候补补位和 C 复活不买，每币最多一次。\n关闭不能撤回已广播交易。`)) {
      post('/api/trading/mode',{mode:'LIVE',expectedGeneration:v.generation,confirmNoBudget:noBudget,confirmedFees:v.fees});
    }
  };
  el('tradeDryRun').onclick = () => {
    if (view && !el('tradeDryRun').disabled && confirm('只对接下来符合条件的新 B 币做只读报价/模拟预检？\nGas：'+feeSummary(view.fees)+'\n不会签名或发送交易，也不补买旧币。')) post('/api/trading/mode',{mode:'DRY_RUN',expectedGeneration:view.generation,confirmedFees:view.fees});
  };
  el('walletUnlockForm').onsubmit = event => {
    event.preventDefault();
    const password = el('walletPassword').value;
    el('walletPassword').value = '';
    if (password && confirm('仅在本机解锁钱包，自动买入仍保持关闭？')) post('/api/trading/unlock',{password});
  };
  refresh();
  setInterval(()=>{if(!document.hidden)refresh();},2000);
})();
