# GMGN 四窗口执行端

这是 Manifest V3 Chrome/Edge 扩展。Rust 后端是唯一状态源；扩展不做候补排序、不计算 B/C 生命周期，也不决定淘汰。

扩展始终维护 A-D 四个不同的顶层 Edge 窗口，每个窗口只保留一个 GMGN Tab，并按主显示器可用区域 2×2 平铺。新币到达时只对该 Slot 已绑定的 `tabId` 执行 `chrome.tabs.update()`；不会新建 Tab，也不会改变四窗口结构。只有 Slot 窗口确实丢失时才会用 `chrome.windows.create()` 重建。

安装：

1. 先运行项目根目录的 `start-backend.ps1`。
2. 浏览器进入扩展管理页，启用开发人员模式。
3. “加载已解压的扩展”，选择本目录。
4. 点击扩展图标，再点“创建/修复四个窗口并启动”。

协议：

- 连接 `ws://127.0.0.1:8787/ws/browser`。
- 后端发 `OPEN { commandId, slotId, ca, url }`。
- 扩展只用 `tabs.update()` 导航 Slot 已绑定的唯一标签页；实际 URL 已匹配 CA 后才回 `ACK`。
- 标签页 URL 变化时回传 `URL`。主页判定由后端完成：主机 `gmgn.ai`、路径 `/`、查询参数 `chain=bsc`。
- 重连后发 `HELLO`，包含 A-D 四组互不重复的 `windowId`、`tabId` 与真实 URL；后端也会拒绝共享窗口的 Slot 报告。

不要同时安装旧版扩展；旧版会自行轮换页面，与后端调度冲突。
