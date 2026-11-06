"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const HOME_URL = "https://gmgn.ai/?chain=bsc";

const storage = { memeQuantBrowserV2: { enabled: true, tabIds: [10, 11, 12, 13] } };
const windows = new Map();
const tabs = new Map();
const listeners = {
  runtimeMessage: [], tabUpdated: [], tabCreated: [], tabRemoved: [], windowRemoved: [],
  alarm: [], startup: [], installed: [],
};
let nextWindowId = 100;
let nextTabId = 1000;
let createCalls = 0;
let tabUpdateCalls = 0;
let windowUpdateCalls = 0;
let storageWriteCalls = 0;
let failNextTabUpdate = false;

function addWindow(windowId, tabSpecs, bounds = {}) {
  const win = { id: windowId, type: "normal", left: 0, top: 0, width: 1920, height: 1040, ...bounds, tabs: [] };
  windows.set(windowId, win);
  for (const spec of tabSpecs) {
    const tab = { id: spec.id, windowId, url: spec.url };
    tabs.set(tab.id, tab);
    win.tabs.push(tab);
  }
  return win;
}

addWindow(1, [{ id: 9, url: "edge://newtab/" }]);
for (let index = 0; index < 4; index += 1) {
  const tab = { id: 10 + index, windowId: 1, url: `https://gmgn.ai/bsc/token/0x${String(index + 1).repeat(40)}` };
  tabs.set(tab.id, tab);
  windows.get(1).tabs.push(tab);
}

function event(name) {
  return { addListener(fn) { listeners[name].push(fn); } };
}

globalThis.chrome = {
  storage: {
    local: {
      async get(key) { return { [key]: storage[key] }; },
      async set(value) { storageWriteCalls++; Object.assign(storage, value); },
    },
  },
  system: {
    display: {
      async getInfo() {
        return [{ isPrimary: true, isEnabled: true, workArea: { left: 0, top: 0, width: 1920, height: 1040 } }];
      },
    },
  },
  windows: {
    async create(options) {
      createCalls += 1;
      const windowId = nextWindowId++;
      const tabId = nextTabId++;
      return addWindow(windowId, [{ id: tabId, url: options.url }], options);
    },
    async get(windowId) {
      const win = windows.get(windowId);
      if (!win) throw new Error("window missing");
      return win;
    },
    async getAll() { return [...windows.values()]; },
    async getLastFocused() { return [...windows.values()][0]; },
    async update(windowId, info) {
      windowUpdateCalls++;
      const win = windows.get(windowId);
      if (!win) throw new Error("window missing");
      Object.assign(win, info);
      return win;
    },
    async remove(windowId) {
      const win = windows.get(windowId);
      if (!win) throw new Error("window missing");
      for (const tab of [...win.tabs]) {
        tabs.delete(tab.id);
        for (const fn of listeners.tabRemoved) fn(tab.id, { windowId, isWindowClosing: true });
      }
      windows.delete(windowId);
      for (const fn of listeners.windowRemoved) fn(windowId);
    },
    onRemoved: event("windowRemoved"),
  },
  tabs: {
    async get(tabId) {
      const tab = tabs.get(tabId);
      if (!tab) throw new Error("tab missing");
      return tab;
    },
    async update(tabId, info) {
      tabUpdateCalls += 1;
      if (failNextTabUpdate) { failNextTabUpdate = false; throw new Error("test navigation failed"); }
      const tab = tabs.get(tabId);
      if (!tab) throw new Error("tab missing");
      Object.assign(tab, info);
      for (const fn of listeners.tabUpdated) fn(tabId, { url: info.url }, tab);
      return tab;
    },
    async remove(input) {
      for (const tabId of Array.isArray(input) ? input : [input]) {
        const tab = tabs.get(tabId);
        if (!tab) continue;
        tabs.delete(tabId);
        const win = windows.get(tab.windowId);
        if (win) win.tabs = win.tabs.filter((item) => item.id !== tabId);
        for (const fn of listeners.tabRemoved) fn(tabId, { windowId: tab.windowId, isWindowClosing: false });
        if (win && win.tabs.length === 0) {
          windows.delete(win.id);
          for (const fn of listeners.windowRemoved) fn(win.id);
        }
      }
    },
    onUpdated: event("tabUpdated"),
    onCreated: event("tabCreated"),
    onRemoved: event("tabRemoved"),
  },
  runtime: {
    onMessage: event("runtimeMessage"), onStartup: event("startup"), onInstalled: event("installed"),
  },
  alarms: { create() {}, onAlarm: event("alarm") },
};

const sockets = [];
class FakeWebSocket {
  static OPEN = 1;
  static CONNECTING = 0;
  constructor() {
    this.readyState = FakeWebSocket.CONNECTING;
    this.sent = [];
    sockets.push(this);
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN;
      this.onopen?.();
    });
  }
  send(value) { this.sent.push(JSON.parse(value)); }
  close() { this.readyState = 3; this.onclose?.(); }
}
globalThis.WebSocket = FakeWebSocket;
globalThis.importScripts = (...files) => {
  for (const file of files) require(path.join(__dirname, file));
};

function waitUntil(predicate, label) {
  return new Promise((resolve, reject) => {
    let attempts = 0;
    const poll = () => {
      if (predicate()) return resolve();
      if (++attempts > 200) return reject(new Error(`timeout: ${label}`));
      setImmediate(poll);
    };
    poll();
  });
}

(async () => {
  require("./background.js");
  await waitUntil(() => storage.memeQuantBrowserV3?.slots?.D, "四窗口初始化");

  const slots = storage.memeQuantBrowserV3.slots;
  assert.equal(createCalls, 4, "旧版单窗口四 Tab 必须迁移成四次 windows.create");
  assert.equal(new Set(Object.values(slots).map((slot) => slot.windowId)).size, 4);
  for (const slot of Object.values(slots)) {
    assert.equal(windows.get(slot.windowId).type, "popup");
    assert.equal(windows.get(slot.windowId).tabs.length, 1, "每个 Slot Window 只能保留一个 Tab");
  }

  await waitUntil(() => sockets[0]?.sent.some((message) => message.type === "HELLO"), "HELLO");
  const callRuntime = message => new Promise(resolve => listeners.runtimeMessage[0](message, {}, resolve));
  const beforeMaintenanceUpdates = windowUpdateCalls;
  const beforeMaintenanceStorage = storageWriteCalls;
  for (let i = 0; i < 5; i++) await callRuntime({ type: "GET_STATUS" });
  await callRuntime({ type: "POLL_NOW" });
  for (const fn of listeners.alarm) fn({ name: "memeQuantReconnect" });
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.equal(windowUpdateCalls, beforeMaintenanceUpdates, "状态轮询/心跳不允许重新平铺窗口");
  assert.equal(storageWriteCalls, beforeMaintenanceStorage, "绑定未改变时不反复写 storage");
  const beforeWindowIds = [...windows.keys()].sort();
  const slotA = slots.A;
  const ca = `0x${"a".repeat(40)}`;
  sockets[0].onmessage({ data: JSON.stringify({ type: "OPEN", slotId: "A", commandId: "cmd-1", ca }) });
  await waitUntil(() => tabs.get(slotA.tabId)?.url.endsWith(ca), "OPEN 更新 URL");
  assert.equal(tabUpdateCalls, 1, "OPEN 必须只调用一次 tabs.update");
  assert.deepEqual([...windows.keys()].sort(), beforeWindowIds, "OPEN 不得新建或替换窗口");
  assert.equal(tabs.size, 5, "OPEN 不得新增 Tab，原有非 GMGN 页面应保留");
  assert.equal(tabs.get(9).url, "edge://newtab/");
  assert.ok(sockets[0].sent.some((message) => message.type === "ACK" && message.windowId === slotA.windowId));
  assert.ok(sockets[0].sent.some(m => m.type === "PROGRESS" && m.phase === "RECEIVED" && m.commandId === "cmd-1"));
  assert.ok(sockets[0].sent.some(m => m.type === "PROGRESS" && m.phase === "NAVIGATING" && m.commandId === "cmd-1"));
  assert.ok(sockets[0].sent.some(m => m.type === "PROGRESS" && m.phase === "URL_CONFIRMED" && m.commandId === "cmd-1"));
  sockets[0].onmessage({ data: JSON.stringify({ type:"OPEN", slotId:"A", commandId:"cmd-1", ca }) });
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.equal(tabUpdateCalls, 1, "重复 OPEN 不重新加载已导航的同一命令");

  sockets[0].onmessage({ data: JSON.stringify({ type: "RESET", resetId: 123 }) });
  await waitUntil(() => sockets[0].sent.some(m => m.type === "RESET_ACK" && m.resetId === 123), "清空后四窗口确认");
  assert.ok(Object.values(slots).every(s => tabs.get(s.tabId).url === HOME_URL));
  assert.deepEqual([...windows.keys()].sort(), beforeWindowIds, "RESET 复用四窗口");
  assert.equal(tabUpdateCalls, 5);
  sockets[0].onmessage({ data: JSON.stringify({ type: "OPEN", slotId: "A", commandId: "cmd-2", ca }) });
  await waitUntil(() => sockets[0].sent.some(m => m.type === "ACK" && m.commandId === "cmd-2"), "清空后新 OPEN");
  sockets[0].onmessage({ data: JSON.stringify({ type: "RESET", resetId: 123 }) });
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.equal(tabs.get(slotA.tabId).url, `https://gmgn.ai/bsc/token/${ca}`, "重复 RESET 不能覆盖新 OPEN");
  assert.equal(tabUpdateCalls, 6);

  const extraId = nextTabId++;
  const extra = { id: extraId, windowId: slotA.windowId, url: HOME_URL };
  tabs.set(extraId, extra);
  windows.get(slotA.windowId).tabs.push(extra);
  for (const fn of listeners.tabCreated) fn(extra);
  await waitUntil(() => !tabs.has(extraId), "清理 Slot 多余 Tab");
  assert.equal(windows.get(slotA.windowId).tabs.length, 1);

  const oldB = { ...storage.memeQuantBrowserV3.slots.B };
  await chrome.tabs.remove(oldB.tabId);
  await waitUntil(
    () => storage.memeQuantBrowserV3.slots.B?.windowId !== oldB.windowId && windows.size === 5,
    "意外关闭窗口后只重建对应 Slot",
  );
  assert.equal(createCalls, 5, "丢失一个 Slot 时只允许补建一个窗口");
  assert.equal(new Set(Object.values(storage.memeQuantBrowserV3.slots).map((slot) => slot.windowId)).size, 4);
  for (const slot of Object.values(storage.memeQuantBrowserV3.slots)) {
    assert.equal(windows.get(slot.windowId).tabs.length, 1);
  }

  failNextTabUpdate = true;
  sockets[0].onmessage({ data: JSON.stringify({ type:"OPEN", slotId:"A", commandId:"failed-3", ca }) });
  await waitUntil(() => sockets[0].sent.some(m => m.type === "COMMAND_ERROR" && m.commandId === "failed-3"), "立即上报明确跳转错误");
  const error = sockets[0].sent.find(m => m.type === "COMMAND_ERROR" && m.commandId === "failed-3");
  assert.equal(error.slotId, "A"); assert.equal(error.ca, ca);
  assert.equal(createCalls, 5, "跳转失败不能重建仍存在的窗口");

  const shutdownBindings = () => Object.entries(storage.memeQuantBrowserV3.slots).map(([slotId,s]) => ({
    slotId, browserWindowId: s.windowId, browserTabId: s.tabId,
  }));
  const wsCommand = message => sockets[0].onmessage({ data: JSON.stringify(message) });
  const shutdownAck = (id, phase) => sockets[0].sent.some(m => m.type === "SHUTDOWN_ACK" && m.requestId === id && m.phase === phase);
  const beforeCloseCount = createCalls;
  const beforeCloseWindows = [...windows.keys()];
  wsCommand({type:"PREPARE_CLOSE", requestId:"invalid-shutdown", slots:shutdownBindings().slice(0,3)});
  await waitUntil(() => shutdownAck("invalid-shutdown","FAILED"), "无效关闭绑定必须拒绝");
  assert.deepEqual([...windows.keys()], beforeCloseWindows);
  wsCommand({type:"CLOSE_WINDOWS", requestId:"invalid-shutdown"});
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.deepEqual([...windows.keys()], beforeCloseWindows, "未经 READY 的 CLOSE 不得关窗");

  // A personal extra tab must be protected, even if an old Slot window was modified.
  const safeA = storage.memeQuantBrowserV3.slots.A;
  const personal = {id:nextTabId++, windowId:safeA.windowId, url:"https://example.com/personal"};
  tabs.set(personal.id, personal); windows.get(safeA.windowId).tabs.push(personal);
  for (const fn of listeners.tabCreated) fn(personal);
  wsCommand({type:"PREPARE_CLOSE", requestId:"protect-personal", slots:shutdownBindings()});
  await waitUntil(() => shutdownAck("protect-personal","FAILED"), "拒绝含私人标签页的窗口");
  assert.ok(tabs.has(personal.id), "关闭状态下不能顺手清理私人标签页");
  assert.deepEqual([...windows.keys()], beforeCloseWindows);
  await chrome.tabs.remove(personal.id);

  const closeBindings = shutdownBindings();
  wsCommand({type:"PREPARE_CLOSE", requestId:"safe-shutdown", slots:closeBindings});
  await waitUntil(() => shutdownAck("safe-shutdown","READY"), "停用持久化后才确认 READY");
  assert.equal(storage.memeQuantBrowserV3.enabled, false);
  assert.equal(storage.memeQuantBrowserV3.stoppedByBackend, true);
  const beforeCloseUpdates = tabUpdateCalls;
  wsCommand({type:"OPEN", slotId:"A", commandId:"late-open", ca});
  wsCommand({type:"RESET", resetId:999});
  wsCommand({type:"CLOSE_WINDOWS", requestId:"stale-shutdown"});
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.equal(tabUpdateCalls, beforeCloseUpdates, "关闭后 OPEN/RESET 不再跳转");
  assert.deepEqual([...windows.keys()], beforeCloseWindows, "过期关闭 ID 不得关窗");
  wsCommand({type:"CLOSE_WINDOWS", requestId:"safe-shutdown"});
  await waitUntil(() => shutdownAck("safe-shutdown","CLOSED"), "确认四个窗口均关闭");
  await waitUntil(() => Object.keys(storage.memeQuantBrowserV3.slots).length === 0, "清除临时窗口绑定");
  assert.deepEqual([...windows.keys()], [1], "只关闭甲乙丙丁，不影响普通 Edge 窗口");
  assert.equal(tabs.get(9).url, "edge://newtab/");
  for (const fn of listeners.alarm) fn({name:"memeQuantReconnect"});
  for (const fn of listeners.installed) fn();
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.equal(createCalls, beforeCloseCount, "主动关闭不能自动补建、重连或插件重载后弹窗");
  wsCommand({type:"CLOSE_WINDOWS", requestId:"safe-shutdown"});
  await new Promise(resolve => setImmediate(() => setImmediate(resolve)));
  assert.deepEqual([...windows.keys()], [1], "重复关闭不影响非 Slot 窗口");
  // A new dedicated-browser startup is a deliberate resume of monitoring only.
  for (const fn of listeners.startup) fn();
  await waitUntil(() => storage.memeQuantBrowserV3.enabled && Object.keys(storage.memeQuantBrowserV3.slots).length === 4, "重新启动专用浏览器后恢复四窗口");
  assert.equal(createCalls, beforeCloseCount+4);

  console.log("background integration tests passed");
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
