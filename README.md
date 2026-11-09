# Meme Quant

BSC 多 Launchpad 新币监控、A/B/C 筛选、GMGN 四独立窗口调度，以及额外的监控网页。

这是经过脱敏整理的源码版本 `3.4.3-oss.1`。原始运行目录和个人数据不包含在这里；源码及离线测试不代表交易安全审计。

本次更新修复重新监控时的区块头查询、监控健康状态显示及 B 币 600 秒周期判定。具体行为见 [更新记录](docs/CHANGELOG-3.4.3.md)。

## 安全边界

- **默认只监控。** `cargo build --release --locked` 不启用 `live-trading`，没有实盘广播能力。
- 钱包默认锁定、自动买入默认关闭；重启不恢复买入，不读取私钥环境变量来自动解锁。
- 不提供用户钱包、监控地址名单、付费 RPC Key、控制口令、数据库或浏览器资料。
- 源码保留实验性买入执行器；未经独立安全审计，切勿直接用于大额资产。没有自动卖出，也不保证收益、延迟或不漏币。
- 不与 GMGN、任何 Launchpad、RPC 服务商存在官方关联。不复制 GMGN 的网站代码；四窗口直接访问其网站。使用者自行确认服务条款。
- 测试中的重复数字私钥、地址、交易哈希均为公开离线测试夹具；**绝不能给测试账户充值**。

## 系统规则

Flap、Four.meme、GeniusFun 适配器统一产生新币事件，按 CA 去重后进入同一状态机：

1. 买卖税均 <= 4% 为 A；确认超标永久淘汰，税率未知保持待确认，不猜成零税。
2. 创建后 60 秒内，完整的 3 秒 K 线出现三连阳，且本地监控名单中的至少一个实际买家发生有效买入，晋升 B。二者无先后要求。
3. K 线按 Unix 时间对齐。无成交空档不算一根、也不打断连阳；有成交阴线或平线打断。
4. 四个独立顶层窗口 A/B/C/D，每窗一个 GMGN Tab；分配只更新已有 Tab URL，实际 URL 确认后 ACK。
5. 首次主动返回 GMGN BSC 首页释放窗口时，B 进入 C，重新等待进入 C 后的三连阳，观察 240 秒。仅允许一次复活。
6. B 每 600 秒一个周期，周期市值峰值须至少达到 $10,000；C 不参与此规则。复活后重启 B 周期。
7. 槽位释放时按候补当前市值降序补位；网页排序不改变调度。人工删除和终态不可复活。

“当前市值”来自后端已经收到并验证的数据，不承诺是窗口释放那一毫秒的全链真值。公共 RPC 会限流、断线或落后；补扫及去重不能消除所有链重组、服务故障或平台升级风险。

## 环境

- Windows 10/11，Microsoft Edge 或 Chromium 浏览器。
- 当前 stable Rust（MSVC 工具链）；Windows C++ Build Tools 及 Windows SDK，用于编译原生依赖。
- Node.js 22 或更新版本，用于网页及扩展离线测试。
- Python 3.10 或更新版本，用于发布前扫描和测试。

## 快速开始：只监控

在**这个项目目录**打开 PowerShell：

```powershell
Copy-Item .\config\targets.example.json .\config\targets.local.json
```

编辑 `config/targets.local.json`，填入你自己的监控钱包地址数组。仓库示例为空；空名单、重复地址、零地址、格式错误都会在连接 RPC 前报错，不会悄悄换成作者名单。

```json
["0x1111111111111111111111111111111111111111"]
```

上面只是格式示例，并非推荐监控地址；请替换。地址数量不强制为 189。配置只在启动时加载，修改后需自行重启。

```powershell
cargo build --release --locked
.\target\release\meme-quant.exe --check-config
.\target\release\meme-quant.exe
```

`--check-config` 只检查监控名单、资源目录和监听配置，不连接 RPC、不启动服务、不读取钱包。缺失 `trading-config.json` 会退回禁用交易配置；监控不需要钱包或交易 RPC。

也可以使用 `start-backend.ps1`（前台）或 `start-system.ps1`（后台 + 专用 Edge）。两个脚本只构建默认监控版，不会自动打开实盘许可或导入钱包。

不要与另一套后端同时运行；8787 端口只应由一个实例占用。脚本发现占用会报错，不会杀掉已有进程。请遵守本机 PowerShell 执行策略；不要为了运行脚本关闭系统安全保护。

### 安装插件

1. 如使用 `start-system.ps1`，在它启动的专用 Edge 配置文件中操作；首次安装可以手动打开 `edge://extensions/`。
2. 开启开发人员模式，加载已解压的扩展，选择本项目的 `browser-extension/`。
3. 点击插件图标，再点“创建/修复四个窗口并启动”。
4. 本机监控网页为 <http://127.0.0.1:8787/>。

不要在私人浏览器配置中同时启用多份插件。Slot 是专用窗口，请勿放入私人标签页；原有单页约束会清理 Slot 内额外标签页。正常调度不创建新标签页。

监控网页的“清空并重新监控”会淘汰当前观察池，但保留终态、历史和配置；“关闭后端及甲乙丙丁窗口”是退出，不是清空。关闭流程会先停止买入和调度、保存状态，再核对并关闭专用窗口。旧版/断线插件不会被当成已确认；最后一个 Edge 窗口退出可能丢失最终 ACK，此时界面明确标为未确认。不要把断线一律理解为关窗成功。

## 额外监控网页

除了四个 GMGN 看盘窗口，系统还提供一个监控网页，用于查看候补币列表和系统状态，以及进行相关管理操作。直接在运行后端的同一台电脑上，用浏览器打开 <http://127.0.0.1:8787/> 即可使用。

### 可选：局域网访问

默认只监听 `127.0.0.1:8787`。如需从可信局域网中的其他设备访问，可用脚本的 `-Lan` 参数显式开放局域网：

```powershell
.\start-backend.ps1 -Lan
```

首次会生成只供本机保存的随机控制口令 `config/dashboard-control-token.local.txt`。在可信局域网用 `http://运行后端的电脑局域网IP:8787/` 访问，变更操作需要在网页输入该口令。口令不是钱包密码，不要贴到 Issue 或截图里。

手动启动也可设置 `MEME_QUANT_LISTEN_ADDR=0.0.0.0:8787` 和至少 24 字符的 `DASHBOARD_CONTROL_TOKEN`。非回环监听缺少口令会拒绝启动。读取状态的接口仍无需口令，因此只在可信局域网使用；不要做公网端口转发。HTTP 本身不加密，跨不可信网络需要额外的可信加密通道及访问控制。Windows 防火墙规则由使用者自行审查设置，脚本不修改防火墙。

## 本地配置与文件

| 项目 | 默认位置 / 覆盖方式 |
| --- | --- |
| 监控地址 | `config/targets.local.json` / `MEME_QUANT_TARGETS` |
| 运行资源目录 | 当前目录 / `MEME_QUANT_HOME` |
| 数据库 | `data/meme-quant.sqlite3` / `MEME_QUANT_DB` |
| 交易配置 | `trading-config.json` / `TRADING_CONFIG_FILE` |
| 监听地址 | `127.0.0.1:8787` / `MEME_QUANT_LISTEN_ADDR`，端口固定 8787 |
| 手动启动回填 | `SEED_BLOCKS`，默认 0；重连补扫是独立机制 |

监控的公共 RPC 候选在 `src/config.rs`；交易 RPC 只由使用者自己的本地交易配置提供。公共 RPC 不保证配额、可用性或低延迟，平台合约和 ABI 也可能变化。

## 买入代码

详细边界见 [TRADING.md](TRADING.md)。仅首次成为 B 且当场取得空闲窗口的币，才可能产生一次买入意图；四窗满时的候补、后续补位和 C 复活不买。预检与窗口跳转并行，不等待页面 ACK。

默认构建和示例配置均关闭实盘。任何开启实盘、钱包导入和资金操作都必须由使用者独立审查并在自己本机主动完成。暂停只取消尚未广播的意图；已广播交易不能撤回，窗口关闭或清空观察池不会卖出持仓。

## 测试与发布

发布扫描针对尚未运行的干净源代码副本；若已生成 `target/`、`data/` 或本地配置，扫描拒绝通过是预期行为。请从全新 clone/受控导出再次扫描，不要删除自己的运行数据来凑通过。CI 将编译产物放在仓库外。

```powershell
python scripts/check-release.py
python -m unittest discover -s scripts -p "test_*.py"
node scripts/test-js.cjs
cargo fmt --check
cargo test --locked
cargo test --locked --features live-trading
```

测试含离线 RPC/SQLite/签名夹具，不需要真实钱包。两种 Rust 构建测试不等同于主网实盘验证。CI 不配置交易密钥，也不启动监控或发送链上交易。

发布流程见 [PUBLISHING.md](PUBLISHING.md)；脱敏范围和本地验证结果见 [RELEASE-REVIEW.md](RELEASE-REVIEW.md)。不要把你的运行目录、数据库或浏览器资料整体上传。

## 许可与来源

已准备 MIT 许可证，版权署名暂用 `Meme Quant contributors`。**公开发布前，所有者仍须确认最终署名，以及原始压缩包/基础代码是否有权按 MIT 再分发。** 此准备动作不替任何未知上游授权，也不改变第三方许可。见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。

未使用的本地 Lightweight Charts 包未包含在此副本；看盘继续使用 GMGN 原站。第三方网站、标识、合约和 RPC 服务不因本项目开源而被授予使用权。
