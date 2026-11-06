"use strict";
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const path = require("node:path");
const messages = [];
let container = null;
let tick;
const callbacks = {};
const ca = `0x${"a".repeat(40)}`;
const context = {
  location: { pathname:`/bsc/token/${ca}`, href:`https://gmgn.ai/bsc/token/${ca}` },
  document: {
    readyState: "loading", documentElement: {},
    querySelectorAll(selector) {
      assert.notEqual(selector, "canvas", "不允许全页 canvas 猜测 K 线可见");
      assert.notEqual(selector, "svg", "普通页面图标不是 K 线");
      return container ? [container] : [];
    },
    addEventListener(name, fn) { callbacks[name] = fn; },
  },
  chrome: { runtime: { sendMessage(m) { messages.push(m); return Promise.resolve(); } } },
  MutationObserver: class { constructor(fn) { this.fn=fn; } observe() {} disconnect() {} },
  setInterval(fn) { tick=fn; return 1; }, setTimeout() { return 1; }, clearTimeout() {},
  requestAnimationFrame(fn) { fn(); }, Date, innerWidth:1000, innerHeight:500,
  getComputedStyle() { return { display:"block", visibility:"visible" }; },
};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,"content.js"),"utf8"), context);
assert.equal(messages.filter(m=>m.type === "GMGN_PAGE_PROGRESS").length, 0);
context.document.readyState="interactive";
callbacks.DOMContentLoaded();
assert.equal(messages.filter(m=>m.phase === "DOCUMENT_READY").length, 1);
assert.equal(messages.filter(m=>m.phase === "CHART_CONTAINER").length, 0);
context.location.pathname="/"; context.location.href="https://gmgn.ai/?chain=bsc"; tick();
container = { getBoundingClientRect() { return { width:500,height:200,bottom:210,right:510,top:10,left:10 }; } };
context.location.pathname=`/bsc/token/${ca}`; context.location.href=`https://gmgn.ai/bsc/token/${ca}`; tick();
assert.equal(messages.filter(m=>m.phase === "DOCUMENT_READY").length, 2);
assert.equal(messages.filter(m=>m.phase === "CHART_CONTAINER").length, 1);
tick();
assert.equal(messages.filter(m=>m.phase === "CHART_CONTAINER").length, 1);
assert.ok(messages.every(m=>m.type !== "GMGN_CHART_READY"), "容器出现不能冒充渲染完成");
console.log("content observation tests passed");
