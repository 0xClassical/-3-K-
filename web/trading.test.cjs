const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {row,bnb,time,amountText,parseSettings,parseFees,gweiText,feeSummary,slippageText,readiness} = require('./trading.js');
test('raw integer quantities stay precise and UTC+8 timing is explicit', () => {
  assert.equal(bnb('5000000000000000'), '0.005000');
  assert.equal(bnb('9007199254740993000000000000000000'), '9007199254740993.000000');
  assert.equal(bnb(null), '未知');
  assert.equal(time(0), '1970-01-01 08:00:00.000');
});
test('unknown broadcast is not presented as success; all untrusted text is escaped', () => {
  const html = row({intent:{ca:'<img onerror=alert(1)>',slotId:'A',bAtMs:0},mode:'DRY_RUN',
    state:'BROADCAST_UNKNOWN',reason:'<script>evil()</script>',txHash:'javascript:alert(1)'});
  assert.ok(html.includes('广播结果未知'));
  assert.ok(!html.includes('<script>') && !html.includes('<img') && !html.includes('javascript:'));
});
test('UI arms only from explicit button confirmation and never persists passwords or tokens', () => {
  const source=fs.readFileSync(require.resolve('./trading.js'),'utf8');
  assert.ok(source.includes("el('tradeStart').onclick"));
  assert.ok(source.includes("mode:'LIVE',expectedGeneration:v.generation,confirmNoBudget:noBudget,confirmedFees:v.fees"));
  assert.ok(!source.includes('localStorage') && !source.includes('sessionStorage'));
  assert.ok(source.includes("'x-meme-control':'1'"));
  const html=fs.readFileSync(require.resolve('./index.html'),'utf8');
  assert.ok(html.includes('window.controlHeaders()'));
  assert.ok(html.includes('不会卖出'));
  assert.ok(html.includes('关闭自动买入') && html.includes('允许滑点（%）'));
});

test('BNB and slippage controls use exact integer conversion and reject malformed inputs', () => {
  assert.equal(amountText('10000000000000001'),'0.010000000000000001');
  assert.deepEqual(parseSettings('0.010000000000000001','1.23'),{amountWei:'10000000000000001',slippageBps:123});
  assert.deepEqual(parseSettings('0.005','0'),{amountWei:'5000000000000000',slippageBps:0});
  assert.equal(slippageText(500),'5');
  assert.equal(slippageText(1),'0.01');
  assert.equal(slippageText(null),'');
  for (const value of ['0','-1','1e-3','NaN','0.0000000000000000001','1.']) {
    assert.throws(()=>parseSettings(value,'1'));
  }
  for (const value of ['100','-1','1e2','0.001','NaN','']) {
    assert.throws(()=>parseSettings('0.005',value));
  }
});

const readyView = () => ({mode:'DISABLED',generation:1,liveBuild:true,liveAllowed:true,
  rpcConfigured:true,walletConfigured:true,walletUnlocked:true,nonceReady:true,policyReady:true,
  canSimulate:true,amountWei:'5000000000000000',slippageBps:100,sessionBudgetWei:'0',dailyBudgetWei:'0',orders:[],
  fees:parseFees('AUTOMATIC','10','','5','0.003'),gasPriceCeilingWei:'5000000000',gasFeeCeilingWei:'3000000000000000',gasLimitCap:600000});
function uiHarness(initial, holdStart=false) {
  const elements=new Map(), posts=[], alerts=[];
  let current={...initial}, releaseStart;
  const get=id=>{
    if(!elements.has(id)) elements.set(id,{value:'',disabled:false,hidden:false,textContent:'',innerHTML:'',classList:{toggle(){}}});
    return elements.get(id);
  };
  const fetch=async(path,options)=>{
    if(!options?.method) return {ok:true,json:async()=>({...current})};
    const body=JSON.parse(options.body); posts.push({path,body});
    if(path==='/api/trading/mode' && body.mode==='LIVE' && holdStart) {
      return new Promise(resolve=>releaseStart=()=>resolve({ok:false,text:async()=> 'stale activation rejected'}));
    }
    if(path==='/api/trading/mode') current={...current,mode:body.mode,generation:current.generation+1};
    if(path==='/api/trading/settings') current={...current,amountWei:body.amountWei,slippageBps:body.slippageBps,fees:body.fees,generation:current.generation+1};
    return {ok:true,json:async()=>({ok:true})};
  };
  vm.runInNewContext(fs.readFileSync(require.resolve('./trading.js'),'utf8'),{
    document:{getElementById:get,hidden:false},location:{hostname:'127.0.0.1'},window:{},
    fetch,confirm:()=>true,alert:message=>alerts.push(message),setInterval:()=>{},Intl,Date,BigInt
  });
  return {get,posts,alerts,release:()=>releaseStart?.(),flush:()=>new Promise(resolve=>setImmediate(resolve))};
}

test('page load never enables buying; changed inputs require save before start',async()=>{
  const h=uiHarness(readyView()); await h.flush();
  assert.equal(h.posts.length,0);
  assert.equal(h.get('tradeStart').disabled,false);
  h.get('tradeAmount').value='0.01'; h.get('tradeAmount').oninput();
  h.get('tradeSlippage').value='1.23'; h.get('tradeSlippage').oninput();
  assert.equal(h.get('tradeStart').disabled,true);
  h.get('tradeSettingsForm').onsubmit({preventDefault(){}}); await h.flush();
  assert.equal(h.posts[0].path,'/api/trading/settings');
  assert.equal(h.posts[0].body.amountWei,'10000000000000000');
  assert.equal(h.posts[0].body.slippageBps,123);
  assert.equal(h.get('tradeStart').disabled,false);
  h.get('tradeStart').onclick(); await h.flush();
  assert.equal(h.posts[1].body.mode,'LIVE');
  assert.equal(h.posts[1].body.confirmNoBudget,true);
  assert.equal(h.posts[1].body.expectedGeneration,2);
  assert.deepEqual(h.posts[1].body.confirmedFees,h.posts[0].body.fees);
});

test('stop remains usable while a start request is waiting and stale response cannot rearm UI',async()=>{
  const h=uiHarness(readyView(),true); await h.flush();
  h.get('tradeStart').onclick(); await h.flush();
  assert.equal(h.get('tradePause').disabled,false);
  h.get('tradePause').onclick(); await h.flush();
  assert.equal(h.posts.length,2);
  assert.equal(h.posts[1].body.mode,'DISABLED');
  h.release(); await h.flush();
  assert.equal(h.get('tradeModeBadge').textContent,'已关闭');
  assert.equal(h.alerts.length,1);
});

test('missing wallet or execution configuration blocks start without a hidden activation',async()=>{
  const v={...readyView(),walletUnlocked:false,nonceReady:false,rpcConfigured:false};
  assert.ok(readiness(v).length>=2);
  const h=uiHarness(v); await h.flush();
  assert.equal(h.get('tradeStart').disabled,true);
  h.get('tradeStart').onclick(); await h.flush();
  assert.equal(h.posts.length,0);
});

test('fee inputs use exact Gwei/BNB integers, bounded markup, and mode-specific validation',()=>{
  assert.equal(gweiText('1234567890'),'1.23456789');
  const auto=parseFees('AUTOMATIC','12.34','not-used','1.000000001','0.000137500000000001');
  assert.equal(auto.gasMultiplierBps,11234);
  assert.equal(auto.fixedGasPriceWei,null);
  assert.equal(auto.gasPriceCapWei,'1000000001');
  assert.equal(auto.gasFeeCapWei,'137500000000001');
  const fixed=parseFees('FIXED','not-used','0.000000001','5','0.003');
  assert.equal(fixed.fixedGasPriceWei,'1');
  assert.ok(feeSummary(fixed).includes('固定 0.000000001 Gwei'));
  assert.ok(feeSummary(null).includes('不能重新执行'));
  for(const args of [
    ['BAD','10','','5','0.003'],['AUTOMATIC','400.01','','5','0.003'],
    ['AUTOMATIC','-1','','5','0.003'],['AUTOMATIC','1e2','','5','0.003'],
    ['AUTOMATIC','0.001','','5','0.003'],['AUTOMATIC','10','','0','0.003'],
    ['AUTOMATIC','10','','5','0'],['AUTOMATIC','10','','0.0000000001','0.003'],
    ['FIXED','','','5','0.003'],['FIXED','','0','5','0.003'],['FIXED','','6','5','0.003'],
    ['AUTOMATIC','10','','5','0.0000000000000000001']
  ]) assert.throws(()=>parseFees(...args));
});

test('Gas mode changes require save; fee authorization is sent on activation and persisted only server-side',async()=>{
  const h=uiHarness(readyView()); await h.flush();
  assert.equal(h.get('tradeFixedGas').disabled,true);
  assert.equal(h.get('tradeGasMarkup').disabled,false);
  h.get('tradeGasMode').value='FIXED'; h.get('tradeGasMode').onchange();
  assert.equal(h.get('tradeStart').disabled,true);
  assert.equal(h.get('tradeFixedGas').disabled,false);
  assert.equal(h.get('tradeGasMarkup').disabled,true);
  h.get('tradeFixedGas').value='1.23456789'; h.get('tradeFixedGas').oninput();
  h.get('tradeGasFeeCap').value='0.0002'; h.get('tradeGasFeeCap').oninput();
  h.get('tradeSettingsForm').onsubmit({preventDefault(){}}); await h.flush();
  assert.equal(h.posts[0].body.fees.fixedGasPriceWei,'1234567890');
  assert.equal(h.posts[0].body.fees.gasFeeCapWei,'200000000000000');
  assert.equal(h.get('tradeModeBadge').textContent,'已关闭');
  h.get('tradeStart').onclick(); await h.flush();
  assert.deepEqual(h.posts[1].body.confirmedFees,h.posts[0].body.fees);
});

test('policy ceiling cannot be raised by the page; old backend fee data keeps start disabled',async()=>{
  const h=uiHarness(readyView()); await h.flush();
  h.get('tradeGasPriceCap').value='5.000000001'; h.get('tradeGasPriceCap').oninput();
  h.get('tradeSettingsForm').onsubmit({preventDefault(){}}); await h.flush();
  assert.equal(h.posts.length,0); assert.equal(h.alerts.length,1);
  assert.equal(h.get('tradeStart').disabled,true);
  const old=uiHarness({...readyView(),fees:null}); await old.flush();
  assert.equal(old.get('tradeStart').disabled,true);
  assert.equal(old.get('tradePause').disabled,false);
  old.get('tradeStart').onclick(); await old.flush();
  assert.equal(old.posts.length,0);
});
