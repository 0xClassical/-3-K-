const $=id=>document.getElementById(id);
async function send(message){const r=await chrome.runtime.sendMessage(message);if(r?.ok===false)throw Error(r.error||'操作失败');return r}
function short(ca){return ca?`${ca.slice(0,8)}…${ca.slice(-6)}`:'等待后端分配'}
function render(s){$('dot').className='dot'+(s.backendOk&&s.invariantOk?' ok':'');$('status').textContent=s.enabled?(s.backendOk?(s.invariantOk?'后端已连接 · 四个独立窗口':'正在修复四窗口'):'等待 Rust 后端'):'已暂停';$('toggle').textContent=s.enabled?'暂停':'继续';$('slots').innerHTML=(s.slots||[]).map(x=>`<div class="slot"><b>窗口 ${x.slotId}</b><div class="ca">Window ${x.windowId??'—'} · Tab ${x.tabId??'—'}<br>${short(x.token)}</div></div>`).join('')}
async function refresh(){try{render(await send({type:'GET_STATUS'}))}catch(e){$('status').textContent=e.message}}
$('setup').onclick=async()=>{render(await send({type:'INIT_AND_START'}))};
$('toggle').onclick=async()=>{const s=await send({type:'GET_STATUS'});render(await send({type:'SET_ENABLED',enabled:!s.enabled}))};
$('sync').onclick=async()=>render(await send({type:'POLL_NOW'}));
refresh();setInterval(refresh,1000);
