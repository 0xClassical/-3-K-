const assert=require('node:assert/strict');
const {timeText,timingCell,historyRow}=require('./history.js');
assert.equal(timeText(1790476839278),'2026-09-27 10:40:39.278');
assert.equal(timeText(1790476839000,'s'),'2026-09-27 10:40:39');
assert.equal(timingCell(null,null,null),'<td>—</td>');
assert.match(timingCell(1790476839000,'s',null),/秒级旧记录/);
assert.match(timingCell(1790476839278,'ms',1790476809000),/30.278 秒/);
assert.match(timingCell(1000,'ms',2000),/时间异常/);
const rendered=historyRow({name:'<img onerror="bad()">',ca:'<script>',state:'DEAD',firstAtMs:1000,firstPrecision:'ms'});
assert.ok(!rendered.includes('<img'));
assert.ok(rendered.includes('&lt;script&gt;'));
assert.ok(rendered.includes('链上条件：未知'));
console.log('history formatting / timezone / precision / escaping tests passed');

// Exercise tab switching, pagination, search and stale requests without contacting
// a live backend or invoking any discard/reset endpoint.
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
async function interactions() {
  const html=fs.readFileSync(path.join(__dirname,'index.html'),'utf8');
  const ids=[...html.matchAll(/id="([^"]+)"/g)].map(m=>m[1]);
  assert.equal(new Set(ids).size,ids.length,'DOM ids must be unique');
  const elements=Object.fromEntries(ids.map(id=>[id,{value:'',hidden:false,disabled:false,setAttribute(name,value){this[name]=value;}}]));
  const pending=[];
  let poll;
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'history.js'),'utf8'),{
    document:{getElementById:id=>elements[id],hidden:false},Intl,URLSearchParams,AbortController,
    setInterval:callback=>{poll=callback;},
    fetch:(url,options)=>new Promise(resolve=>pending.push({url,options,resolve})),
  });
  const flush=()=>new Promise(resolve=>setImmediate(resolve));
  const respond=async(request,body,status=200)=>{
    request.resolve({ok:status===200,status,json:async()=>body,text:async()=>String(body)});
    await flush();
  };
  assert.equal(pending.length,0,'no history polling on candidate tab');
  elements.historyTab.onclick();
  assert.ok(elements.candidatePanel.hidden);
  assert.equal(elements.historyPanel.hidden,false);
  await respond(pending[0],{rows:[{name:'First',ca:'0x1',firstAtMs:1000,firstPrecision:'ms'}],total:51});
  assert.match(elements.historyRows.innerHTML,/First/);
  assert.equal(elements.historyPrev.disabled,true);
  assert.equal(elements.historyNext.disabled,false);
  elements.historyNext.onclick();
  assert.match(pending[1].url,/offset=50/);
  await respond(pending[1],{rows:[],total:51});
  assert.equal(elements.historyNext.disabled,true);
  elements.historySearch.value='  0xABC  ';
  elements.historySearchForm.onsubmit({preventDefault(){}});
  assert.match(pending[2].url,/offset=0&q=0xABC/);
  elements.candidateTab.onclick();
  assert.ok(pending[2].options.signal.aborted);
  const previous=elements.historyRows.innerHTML;
  await respond(pending[2],{rows:[{name:'STALE'}],total:1});
  assert.equal(elements.historyRows.innerHTML,previous,'late response cannot overwrite hidden panel');
  poll();
  assert.equal(pending.length,3,'hidden history must stop polling');
  elements.historyTab.onclick();
  await respond(pending[3],'temporary error',500);
  assert.match(elements.historyStatus.textContent,/历史读取失败/);
  poll();
  await respond(pending[4],{rows:[],total:0});
  assert.match(elements.historyRows.innerHTML,/没有符合条件/);
  assert.ok(pending.every(r=>r.url.startsWith('/api/b-history?')&&r.options.cache==='no-store'));
  console.log('history tabs / pagination / search / stale-response / recovery tests passed');
}
interactions().catch(error=>{console.error(error);process.exitCode=1;});
