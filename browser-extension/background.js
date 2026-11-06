importScripts("slot-layout.js");

// 纯执行端：Token 生命周期、候补排名和 Slot 分配全部由 Rust 后端决定。
// 核心不变量：A-D 各自绑定一个不同的顶层窗口，每个窗口只保留一个 GMGN Tab。
const { SLOT_IDS, tileBounds, hasFourDistinctWindows } = globalThis.MemeQuantSlotLayout;
const STORAGE_KEY = "memeQuantBrowserV3";
const LEGACY_STORAGE_KEY = "memeQuantBrowserV2";
const WS_URL = "ws://127.0.0.1:8787/ws/browser";
const HOME_URL = "https://gmgn.ai/?chain=bsc";
const TOKEN_PREFIX = "https://gmgn.ai/bsc/token/";
const RECONNECT_ALARM = "memeQuantReconnect";
const PROTOCOL_VERSION = "3.1.4";

let state = { enabled: true, slots: {} };
let socket = null;
let reconnectTimer = null;
let ensurePromise = null;
let initialized = false;
let commandQueue = Promise.resolve();
let resetting = 0;
let resetNavigating = false;
let layoutDirty = true;
let keepAliveTimer = null;
let closing = false;
let closeRequest = null;
const pending = new Map();
const navigationContexts = new Map();
const lastReportedUrls = new Map();
const recreatedSlots = new Set();
const intentionalTabRemovals = new Set();

function validCa(value) {
  const ca = String(value || "").toLowerCase();
  return /^0x[0-9a-f]{40}$/.test(ca) ? ca : null;
}

function tokenFromUrl(raw) {
  try {
    const u = new URL(raw);
    if (u.hostname !== "gmgn.ai") return null;
    const match = u.pathname.match(/^\/bsc\/token\/(0x[0-9a-f]{40})\/?$/i);
    return match ? match[1].toLowerCase() : null;
  } catch (_) {
    return null;
  }
}

function isGmgnUrl(raw) {
  try { return new URL(raw).hostname === "gmgn.ai"; } catch (_) { return false; }
}

async function load() {
  if (initialized) return;
  const saved = (await chrome.storage.local.get(STORAGE_KEY))[STORAGE_KEY];
  if (saved && typeof saved === "object") {
    state.enabled = saved.enabled !== false;
    state.stoppedByBackend = saved.stoppedByBackend === true;
    state.lastResetId = Number.isSafeInteger(saved.lastResetId) ? saved.lastResetId : 0;
    for (const slotId of SLOT_IDS) {
      const entry = saved.slots?.[slotId];
      if (Number.isInteger(entry?.windowId) && Number.isInteger(entry?.tabId)) {
        state.slots[slotId] = { windowId: entry.windowId, tabId: entry.tabId };
      }
    }
  }
  initialized = true;
}

async function save() {
  await chrome.storage.local.set({ [STORAGE_KEY]: state });
}

async function existingTab(tabId) {
  if (!Number.isInteger(tabId)) return null;
  try { return await chrome.tabs.get(tabId); } catch (_) { return null; }
}

async function existingWindow(windowId) {
  if (!Number.isInteger(windowId)) return null;
  try { return await chrome.windows.get(windowId, { populate: true }); } catch (_) { return null; }
}

async function primaryWorkArea() {
  try {
    const displays = await chrome.system.display.getInfo();
    const display = displays.find((item) => item.isPrimary && item.isEnabled !== false)
      || displays.find((item) => item.isEnabled !== false)
      || displays[0];
    if (display?.workArea?.width > 0 && display?.workArea?.height > 0) return display.workArea;
  } catch (_) {}
  try {
    const current = await chrome.windows.getLastFocused();
    if (current?.width > 0 && current?.height > 0) {
      return { left: current.left || 0, top: current.top || 0, width: current.width, height: current.height };
    }
  } catch (_) {}
  return { left: 0, top: 0, width: 1920, height: 1040 };
}

async function createSlotWindow(slotId, url, bounds) {
  const created = await chrome.windows.create({
    url: isGmgnUrl(url) ? url : HOME_URL,
    type: "popup",
    focused: false,
    ...bounds,
  });
  if (!Number.isInteger(created?.id)) throw new Error(`无法创建窗口 ${slotId}`);
  const populated = created.tabs?.length ? created : await chrome.windows.get(created.id, { populate: true });
  const tab = populated.tabs?.[0];
  if (!Number.isInteger(tab?.id)) throw new Error(`窗口 ${slotId} 没有可绑定标签页`);
  state.slots[slotId] = { windowId: created.id, tabId: tab.id };
  recreatedSlots.add(slotId);
}

async function preservedUrlsAndTabs() {
  const urls = {};
  const oldTabIds = new Set();
  const oldWindowIds = new Set();
  for (const slotId of SLOT_IDS) {
    const tabId = state.slots[slotId]?.tabId;
    const tab = await existingTab(tabId);
    if (tab) {
      oldTabIds.add(tab.id);
      oldWindowIds.add(tab.windowId);
      if (isGmgnUrl(tab.url)) urls[slotId] = tab.url;
    }
  }

  const legacy = (await chrome.storage.local.get(LEGACY_STORAGE_KEY))[LEGACY_STORAGE_KEY];
  const legacyIds = Array.isArray(legacy?.tabIds) ? legacy.tabIds.filter(Number.isInteger).slice(0, 4) : [];
  for (let i = 0; i < SLOT_IDS.length; i += 1) {
    const tab = await existingTab(legacyIds[i]);
    if (!tab) continue;
    oldTabIds.add(tab.id);
    oldWindowIds.add(tab.windowId);
    if (!urls[SLOT_IDS[i]] && isGmgnUrl(tab.url)) urls[SLOT_IDS[i]] = tab.url;
  }

  const windows = await chrome.windows.getAll({ populate: true, windowTypes: ["normal", "popup"] });
  const candidates = windows.flatMap((win) => win.tabs || []).filter((tab) => isGmgnUrl(tab.url));
  const usedTabIds = new Set(oldTabIds);
  for (const slotId of SLOT_IDS) {
    if (urls[slotId]) continue;
    const tab = candidates.find((item) => Number.isInteger(item.id) && !usedTabIds.has(item.id));
    if (tab) {
      urls[slotId] = tab.url;
      oldTabIds.add(tab.id);
      oldWindowIds.add(tab.windowId);
      usedTabIds.add(tab.id);
    }
  }
  return { urls, oldTabIds, oldWindowIds };
}

async function rebuildAllSlots() {
  const { urls, oldTabIds, oldWindowIds } = await preservedUrlsAndTabs();
  const layouts = tileBounds(await primaryWorkArea());
  state.slots = {};
  for (const slotId of SLOT_IDS) {
    await createSlotWindow(slotId, urls[slotId] || HOME_URL, layouts[slotId]);
  }
  await save();

  const newTabIds = new Set(SLOT_IDS.map((slotId) => state.slots[slotId].tabId));
  const removable = [...oldTabIds].filter((tabId) => !newTabIds.has(tabId));
  for (const tabId of removable) intentionalTabRemovals.add(tabId);
  if (removable.length) {
    try { await chrome.tabs.remove(removable); } catch (_) {}
  }
  const newWindowIds = new Set(SLOT_IDS.map((slotId) => state.slots[slotId].windowId));
  for (const windowId of oldWindowIds) {
    if (newWindowIds.has(windowId)) continue;
    // Never close unrelated tabs that happened to share an old slot window.
    const oldWindow = await existingWindow(windowId);
    if (oldWindow && !(oldWindow.tabs || []).length) {
      try { await chrome.windows.remove(windowId); } catch (_) {}
    }
  }
}

async function inspectSlot(slotId) {
  const entry = state.slots[slotId];
  if (!entry) return null;
  const tab = await existingTab(entry.tabId);
  if (tab && Number.isInteger(tab.windowId)) {
    const win = await existingWindow(tab.windowId);
    return { windowId: tab.windowId, tabId: tab.id, url: tab.url || HOME_URL, type: win?.type };
  }
  const win = await existingWindow(entry.windowId);
  const only = win?.tabs?.length === 1 ? win.tabs[0] : null;
  if (Number.isInteger(only?.id)) {
    return { windowId: win.id, tabId: only.id, url: only.url || HOME_URL, type: win.type };
  }
  return null;
}

async function enforceSingletonAndTile() {
  const layouts = tileBounds(await primaryWorkArea());
  for (const slotId of SLOT_IDS) {
    const entry = state.slots[slotId];
    const win = await existingWindow(entry?.windowId);
    if (!win) continue;
    const extras = (win.tabs || []).filter((tab) => tab.id !== entry.tabId && Number.isInteger(tab.id));
    for (const tab of extras) intentionalTabRemovals.add(tab.id);
    if (extras.length) {
      try { await chrome.tabs.remove(extras.map((tab) => tab.id)); } catch (_) {}
    }
    try {
      await chrome.windows.update(entry.windowId, { state: "normal" });
      await chrome.windows.update(entry.windowId, layouts[slotId]);
    } catch (_) {}
  }
}

async function ensureSlotsImpl() {
  await load();
  if (closing || !state.enabled) return;
  const hadSavedSlots = SLOT_IDS.some((slotId) => state.slots[slotId]);
  const inspected = {};
  const windowIds = new Set();
  let duplicateWindow = false;
  let bindingsChanged = false;
  for (const slotId of SLOT_IDS) {
    const found = await inspectSlot(slotId);
    if (!found) continue;
    if (windowIds.has(found.windowId)) duplicateWindow = true;
    windowIds.add(found.windowId);
    inspected[slotId] = found;
  }

  if (!hadSavedSlots || duplicateWindow || Object.values(inspected).some(slot => slot.type !== "popup")) {
    await rebuildAllSlots();
    layoutDirty = true;
  } else {
    const layouts = tileBounds(await primaryWorkArea());
    for (const slotId of SLOT_IDS) {
      const found = inspected[slotId];
      if (found) {
        const previous = state.slots[slotId];
        bindingsChanged ||= previous?.windowId !== found.windowId || previous?.tabId !== found.tabId;
        state.slots[slotId] = { windowId: found.windowId, tabId: found.tabId };
      } else {
        await createSlotWindow(slotId, HOME_URL, layouts[slotId]);
        bindingsChanged = true;
        layoutDirty = true;
      }
    }
    if (!hasFourDistinctWindows(state.slots)) await rebuildAllSlots();
    if (bindingsChanged) await save();
  }
  if (layoutDirty) {
    await enforceSingletonAndTile();
    layoutDirty = false;
  }
}

async function ensureSlots() {
  if (!ensurePromise) ensurePromise = ensureSlotsImpl().finally(() => { ensurePromise = null; });
  return ensurePromise;
}

function send(payload) {
  if (socket?.readyState !== WebSocket.OPEN) return false;
  socket.send(JSON.stringify(payload));
  return true;
}

async function reports() {
  return Promise.all(SLOT_IDS.map(async (slotId) => {
    const entry = state.slots[slotId];
    const tab = await existingTab(entry?.tabId);
    return {
      slotId,
      windowId: Number.isInteger(entry?.windowId) ? entry.windowId : null,
      tabId: Number.isInteger(tab?.id) ? tab.id : null,
      url: tab?.url || "",
      pendingUrl: tab?.pendingUrl || null,
      recreated: recreatedSlots.has(slotId),
    };
  }));
}

function progress(context, phase, pageClockMs = Date.now()) {
  if (!context) return;
  send({ type: "PROGRESS", slotId: context.slotId, commandId: context.commandId,
    ca: context.ca, windowId: context.windowId, tabId: context.tabId, phase, pageClockMs });
}

async function hello() {
  if (closing || !state.enabled) return;
  await ensureSlots();
  if (closing || !state.enabled) return;
  if (send({ type: "HELLO", version: PROTOCOL_VERSION, supportsCloseWindows: true, slots: await reports() })) recreatedSlots.clear();
}

async function handleOpen(message) {
  if (resetting || closing) return;
  const ca = validCa(message.ca);
  if (!SLOT_IDS.includes(message.slotId) || !ca || !message.commandId) return;
  // Normal OPEN only verifies its own binding; layout belongs to startup/recovery.
  await load();
  if (!state.enabled || resetting || closing) return;
  let entry = state.slots[message.slotId];
  const tab = await existingTab(entry?.tabId);
  if (!tab || tab.windowId !== entry.windowId) {
    delete state.slots[message.slotId];
    await save();
    await ensureSlots();
    entry = state.slots[message.slotId];
  }
  const context = { commandId: message.commandId, ca, slotId: message.slotId, ...entry };
  const previous = navigationContexts.get(message.slotId);
  if (previous?.commandId === message.commandId && previous.ca === ca) {
    await reportUrl(entry.tabId, tab?.url || "");
    return;
  }
  navigationContexts.set(message.slotId, context);
  pending.set(message.slotId, context);
  progress(context, "RECEIVED", message.receivedAtMs || Date.now());
  progress(context, "NAVIGATING");
  // 正常调度唯一允许的页面操作：复用该 Slot 已绑定的 tabId 修改 URL。
  if (!closing && state.enabled) await chrome.tabs.update(entry.tabId, { url: `${TOKEN_PREFIX}${ca}` });
}

// Two-phase close: validate ownership + persist pause BEFORE READY; only an
// explicit matching CLOSE_WINDOWS may remove the four dedicated popup windows.
async function validateCloseSlots(slots) {
  if (!Array.isArray(slots) || slots.length !== 4
      || new Set(slots.map(s => s.slotId)).size !== 4
      || new Set(slots.map(s => s.browserWindowId)).size !== 4
      || new Set(slots.map(s => s.browserTabId)).size !== 4) throw Error("关闭请求缺少四个唯一窗口绑定");
  for (const s of slots) {
    const owned = state.slots[s.slotId];
    if (!SLOT_IDS.includes(s.slotId) || !owned
        || !Number.isInteger(s.browserWindowId) || !Number.isInteger(s.browserTabId)
        || owned.windowId !== s.browserWindowId || owned.tabId !== s.browserTabId) throw Error("窗口绑定已变化，请重新同步后重试");
    const win = await existingWindow(owned.windowId);
    const tab = await existingTab(owned.tabId);
    if (!win && !tab) continue; // Already closed; never create a replacement here.
    if (!win || win.type !== "popup" || win.tabs?.length !== 1 || !tab
        || tab.windowId !== win.id || win.tabs[0].id !== tab.id
        || !isGmgnUrl(tab.url) || (tab.pendingUrl && !isGmgnUrl(tab.pendingUrl))) {
      throw Error(`窗口 ${s.slotId} 不再是单页 GMGN 专用窗口，为保护其他页面拒绝关闭`);
    }
  }
}

async function prepareClose(message) {
  try {
    await load();
    if (ensurePromise) await ensurePromise;
    closeRequest = null;
    // Persist pause even on validation failure. A service-worker reload must
    // not revive automatic window repair while the backend is quiescent.
    state.enabled = false;
    state.stoppedByBackend = true;
    await save();
    clearTimeout(reconnectTimer); clearInterval(keepAliveTimer);
    if (typeof message.requestId !== "string" || !message.requestId) throw Error("缺少关闭请求 ID");
    await validateCloseSlots(message.slots);
    closeRequest = { id: message.requestId, slots: message.slots };
    pending.clear(); navigationContexts.clear(); resetting = 0;
    send({ type: "SHUTDOWN_ACK", requestId: message.requestId, phase: "READY" });
  } catch (error) {
    // Remain quiescent, keep the connection for a safe retry; never auto-repair.
    send({ type: "SHUTDOWN_ACK", requestId: message.requestId, phase: "FAILED", error: String(error?.message || error) });
  }
}

async function closeWindows(message) {
  if (!closeRequest || closeRequest.id !== message.requestId || !closing) return;
  try {
    // Check again: the user may have moved/added a personal tab after READY.
    await validateCloseSlots(closeRequest.slots);
    for (const s of closeRequest.slots) {
      await validateCloseSlots([ ...closeRequest.slots ]);
      if (await existingWindow(s.browserWindowId)) await chrome.windows.remove(s.browserWindowId);
    }
    const remaining = await Promise.all(closeRequest.slots.map(s => existingWindow(s.browserWindowId)));
    if (remaining.some(Boolean)) throw Error("仍有专用窗口未关闭");
    send({ type: "SHUTDOWN_ACK", requestId: message.requestId, phase: "CLOSED" });
    state.slots = {};
    await save();
    socket?.close();
  } catch (error) {
    send({ type: "SHUTDOWN_ACK", requestId: message.requestId, phase: "FAILED", error: String(error?.message || error) });
  }
}

function isHome(raw) {
  try { const u = new URL(raw); return u.hostname === "gmgn.ai" && u.pathname === "/" && u.searchParams.get("chain") === "bsc"; } catch (_) { return false; }
}

async function ackReset() {
  const id = resetting;
  if (!id || resetNavigating) return;
  const slots = await reports();
  if (resetting !== id || !slots.every(s => Number.isInteger(s.tabId) && Number.isInteger(s.windowId) && isHome(s.url))) return;
  state.lastResetId = id;
  await save();
  if (resetting !== id) return;
  // Clear before sending: the next OPEN is valid only after this confirmation.
  resetting = 0;
  send({ type: "RESET_ACK", resetId: id, slots });
}

async function handleReset(message) {
  if (closing || !state.enabled) return;
  const id = message.resetId;
  if (!Number.isSafeInteger(id) || id <= 0) return;
  await load();
  if (id <= (state.lastResetId || 0)) {
    // Retried ACK must never navigate away from a newer OPEN.
    const slots = await reports();
    if (slots.every(s => isHome(s.url))) send({ type: "RESET_ACK", resetId: id, slots });
    return;
  }
  resetting = id;
  pending.clear();
  navigationContexts.clear();
  lastReportedUrls.clear();
  // A retry may arrive after navigation completed but before its URL event/ACK.
  // Verify first; do not repeatedly reload four already-correct pages.
  await ackReset();
  if (!resetting) return;
  resetNavigating = true;
  try {
    await ensureSlots();
    await Promise.all(SLOT_IDS.map(async slot => {
      const tab = await existingTab(state.slots[slot].tabId);
      if (!tab || !isHome(tab.url) || (tab.pendingUrl && !isHome(tab.pendingUrl))) {
        await chrome.tabs.update(state.slots[slot].tabId, { url: HOME_URL });
      }
    }));
  } finally { resetNavigating = false; }
  await ackReset();
}

async function reportUrl(tabId, url) {
  if (closing || !state.enabled) return;
  if (resetting) { await ackReset(); return; }
  const slotId = SLOT_IDS.find((id) => state.slots[id]?.tabId === tabId);
  if (!slotId || !url) return;
  const tab = await existingTab(tabId);
  if (!tab || tab.url !== url || resetting) return;
  const entry = state.slots[slotId];
  const waiting = pending.get(slotId);
  const actual = tokenFromUrl(url);
  if (waiting && !actual) return;
  if (waiting && actual === waiting.ca) {
    progress(waiting, "URL_CONFIRMED");
    const sent = send({
      type: "ACK", slotId, windowId: entry.windowId, tabId,
      commandId: waiting.commandId, ca: waiting.ca,
    });
    if (sent) pending.delete(slotId);
  }
  const identity = `${entry.windowId}:${url}`;
  if (lastReportedUrls.get(tabId) !== identity
      && send({ type: "URL", slotId, windowId: entry.windowId, tabId, url })) {
    lastReportedUrls.set(tabId, identity);
  }
}

function connect() {
  if (closing || !state.enabled || socket?.readyState === WebSocket.OPEN || socket?.readyState === WebSocket.CONNECTING) return;
  const connectedSocket = new WebSocket(WS_URL);
  socket = connectedSocket;
  connectedSocket.onopen = () => {
    clearTimeout(reconnectTimer);
    clearInterval(keepAliveTimer);
    keepAliveTimer = setInterval(() => send({ type: "PING" }), 15000);
    keepAliveTimer.unref?.();
    hello().catch(() => {});
  };
  connectedSocket.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data);
      if (message.type === "PREPARE_CLOSE") {
        // Gate queued OPENs and removal/display callbacks immediately, before
        // awaiting the existing command/ensure queue.
        closing = true;
        commandQueue = commandQueue.catch(() => {}).then(() => prepareClose(message));
      }
      if (message.type === "CLOSE_WINDOWS") commandQueue = commandQueue.then(() => closeWindows(message));
      if (message.type === "OPEN" || message.type === "RESET") {
        message.receivedAtMs = Date.now();
        commandQueue = commandQueue.then(() => message.type === "RESET" ? handleReset(message) : handleOpen(message)).catch(error => {
          console.warn("Browser command failed", error);
          if (pending.get(message.slotId)?.commandId === message.commandId) pending.delete(message.slotId);
          if (navigationContexts.get(message.slotId)?.commandId === message.commandId) navigationContexts.delete(message.slotId);
          send({ type: "COMMAND_ERROR", commandType: message.type, resetId: message.resetId || 0,
            slotId: message.slotId, commandId: message.commandId, ca: message.ca,
            error: String(error?.message || error) });
        });
      }
      if (message.type === "SYNC_REQUEST") hello().catch(() => {});
    } catch (_) {}
  };
  connectedSocket.onclose = () => {
    if (socket !== connectedSocket) return;
    socket = null;
    clearInterval(keepAliveTimer);
    lastReportedUrls.clear();
    if (state.enabled && !closing) reconnectTimer = setTimeout(connect, 500);
  };
  connectedSocket.onerror = () => connectedSocket.close();
}

async function status() {
  // Popup polls once per second: status must never resize/create windows.
  await load();
  const slotReports = await reports();
  return {
    ok: true,
    enabled: state.enabled,
    backendOk: socket?.readyState === WebSocket.OPEN,
    invariantOk: hasFourDistinctWindows(state.slots),
    slots: slotReports.map((slot, index) => ({
      index: index + 1,
      slotId: slot.slotId,
      windowId: slot.windowId,
      tabId: slot.tabId,
      token: tokenFromUrl(slot.url),
    })),
  };
}

chrome.runtime.onMessage.addListener((message, sender, reply) => {
  (async () => {
    await load();
    if (message?.type === "GMGN_HEARTBEAT" && sender.tab?.id) {
      await reportUrl(sender.tab.id, message.url || sender.tab.url || "");
      connect();
      return { ok: true };
    }
    if (message?.type === "GMGN_PAGE_PROGRESS" && sender.tab?.id) {
      const slotId = SLOT_IDS.find(id => state.slots[id]?.tabId === sender.tab.id);
      const context = navigationContexts.get(slotId);
      const tab = await existingTab(sender.tab.id);
      if (context && context.tabId === sender.tab.id && context.windowId === tab?.windowId
          && context.ca === validCa(message.token) && context.ca === tokenFromUrl(tab?.url)
          && ["DOCUMENT_READY", "CHART_CONTAINER"].includes(message.phase)
          && Number.isSafeInteger(message.pageClockMs)) progress(context, message.phase, message.pageClockMs);
      return { ok: true };
    }
    // 页面侧观测到的「K 线可见」时刻。原样转发给后端，由后端与该 Slot
    // 的 OPEN/ACK 时间拼接；这里不做任何换算，避免引入时钟假设。
    if (message?.type === "GMGN_CHART_READY" && sender.tab?.id) {
      const slotId = SLOT_IDS.find((id) => state.slots[id]?.tabId === sender.tab.id);
      if (slotId && Number.isFinite(message.readyAtMs)) {
        send({
          type: "CHART_READY",
          slotId,
          windowId: state.slots[slotId].windowId,
          tabId: sender.tab.id,
          ca: validCa(message.token) || "",
          pageClockMs: Math.round(message.readyAtMs),
        });
      }
      connect();
      return { ok: true };
    }
    if (message?.type === "INIT_AND_START" || message?.type === "SET_ENABLED") {
      if (closing && socket?.readyState === WebSocket.OPEN) return { ok: false, error: "后端正在关闭，请等待退出后再启动" };
      closing = false; closeRequest = null; state.stoppedByBackend = false;
      state.enabled = message.type === "INIT_AND_START" ? true : Boolean(message.enabled);
      await save();
      if (state.enabled) { layoutDirty = true; await ensureSlots(); connect(); } else socket?.close();
      return status();
    }
    if (message?.type === "POLL_NOW") { connect(); await hello(); return status(); }
    if (message?.type === "GET_STATUS") return status();
    return status();
  })().then(reply).catch((error) => reply({ ok: false, error: String(error?.message || error) }));
  return true;
});

chrome.tabs.onUpdated.addListener((tabId, change) => {
  if (change.url) reportUrl(tabId, change.url).catch(() => {});
  else if (change.status === "complete" && resetting) ackReset().catch(() => {});
});

chrome.tabs.onCreated.addListener((tab) => {
  if (closing || !state.enabled) return;
  const slotId = SLOT_IDS.find((id) => state.slots[id]?.windowId === tab.windowId);
  if (!slotId || tab.id === state.slots[slotId].tabId || !Number.isInteger(tab.id)) return;
  intentionalTabRemovals.add(tab.id);
  chrome.tabs.remove(tab.id).catch(() => {});
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (intentionalTabRemovals.delete(tabId)) return;
  if (closing || !state.enabled) return;
  const slotId = SLOT_IDS.find((id) => state.slots[id]?.tabId === tabId);
  if (!slotId) return;
  delete state.slots[slotId];
  layoutDirty = true;
  save().then(() => ensureSlots()).then(() => hello()).catch(() => {});
});

chrome.windows.onRemoved.addListener((windowId) => {
  if (closing || !state.enabled) return;
  const slotId = SLOT_IDS.find((id) => state.slots[id]?.windowId === windowId);
  if (!slotId) return;
  delete state.slots[slotId];
  layoutDirty = true;
  save().then(() => ensureSlots()).then(() => hello()).catch(() => {});
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name !== RECONNECT_ALARM) return;
  load().then(async () => {
    if (!state.enabled) return;
    connect();
    if (socket?.readyState === WebSocket.OPEN) await hello();
  }).catch(() => {});
});

chrome.system.display.onDisplayChanged?.addListener(() => {
  layoutDirty = true;
  if (state.enabled) ensureSlots().catch(() => {});
});

chrome.runtime.onStartup.addListener(() => load().then(async () => {
  // A deliberate fresh launch of the dedicated Edge profile resumes monitoring
  // windows only. Backend restart still keeps trading disabled and locked.
  if (state.stoppedByBackend) {
    closing = false; closeRequest = null; state.stoppedByBackend = false; state.enabled = true;
    await save();
  }
  if (state.enabled) { await ensureSlots(); connect(); }
}));
chrome.runtime.onInstalled.addListener(() => load().then(async () => {
  if (state.enabled) { await ensureSlots(); connect(); }
}));
chrome.alarms.create(RECONNECT_ALARM, { periodInMinutes: 0.5 });
load().then(async () => { if (state.enabled) { await ensureSlots(); connect(); } }).catch(() => {});
