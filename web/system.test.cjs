'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const test = require('node:test');
const code = fs.readFileSync(require('node:path').join(__dirname,'system.js'),'utf8');
const running = {phase:'RUNNING',stopping:false,browserConnected:true,closeSupported:true,message:'ready'};
function fixture() {
  const elements = new Map();
  const element = id => { if (!elements.has(id)) elements.set(id,{disabled:false,textContent:''}); return elements.get(id); };
  const ctx = {document:{getElementById:element},window:{controlHeaders:()=>({'x-meme-control':'1','content-type':'application/json'})},
    confirm:()=>true,setInterval:f=>{ctx.poll=f}, calls:[], status:running, fail:false};
  ctx.fetch = async (url, init={}) => {
    ctx.calls.push({url,init});
    if (ctx.fail) throw Error('offline');
    return {ok:true,status: url.endsWith('shutdown')?202:200,
      json:async()=>url.endsWith('shutdown')?{accepted:true,requestId:'fixture'}:ctx.status};
  };
  vm.runInNewContext(code,ctx); return {ctx,element};
}
const flush = async () => { for (let i=0;i<5;i++) await new Promise(r=>setImmediate(r)); };
test('page load is read only; close requires explicit confirmation and authenticated POST',async()=>{
  const {ctx,element}=fixture(); await flush();
  assert.equal(ctx.calls.length,1); assert.equal(ctx.calls[0].url,'/api/system');
  ctx.confirm=()=>false; await element('shutdownSystem').onclick();
  assert.equal(ctx.calls.filter(c=>c.init.method==='POST').length,0);
  ctx.confirm=()=>true; await element('shutdownSystem').onclick();
  const writes=ctx.calls.filter(c=>c.init.method==='POST'); assert.equal(writes.length,1);
  assert.equal(writes[0].url,'/api/system/shutdown');
  assert.equal(writes[0].init.headers['x-meme-control'],'1');
  assert.deepEqual(JSON.parse(writes[0].init.body),{confirm:true});
  assert.equal(element('tradeStart').disabled,true);
  assert.equal(element('resetMonitor').disabled,true);
});
test('old/disconnected plugin cannot close the system; UI does not claim unconfirmed disconnect is success',async()=>{
  const {ctx,element}=fixture(); await flush();
  ctx.status={...running,closeSupported:false}; await ctx.poll();
  assert.equal(element('shutdownSystem').disabled,true);
  await element('shutdownSystem').onclick(); assert.equal(ctx.calls.filter(c=>c.init.method==='POST').length,0);
  ctx.status=running; await ctx.poll(); await element('shutdownSystem').onclick();
  ctx.fail=true; await ctx.poll(); assert.match(element('systemStatus').textContent,/未确认/);
});
test('repeat clicks cannot double-submit; confirmed exit remains visible after backend disconnect',async()=>{
  const {ctx,element}=fixture(); await flush();
  ctx.status={...running,phase:'SAVING',stopping:true};
  await element('shutdownSystem').onclick(); await element('shutdownSystem').onclick();
  assert.equal(ctx.calls.filter(c=>c.init.method==='POST').length,1);
  ctx.status={...running,phase:'EXITING',stopping:true,message:'四窗已关闭，数据已保存'}; await ctx.poll();
  ctx.fail=true; await ctx.poll(); assert.match(element('systemStatus').textContent,/四窗已关闭，数据已保存/);
});
