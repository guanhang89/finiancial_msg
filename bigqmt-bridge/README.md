# 大 QMT Redis RPC 桥接方案（miniQMT 替代 · 2026-08 最新版）

> 整理自项目历史文档与 2026-08-08 ~ 08-11 的实际迁移与实盘验证记录，已脱敏（账号、券商、路径、密码均以占位符表示）。
> 历史原文：`docs/01-迁移方案.md`、`docs/02-切换操作手册.md`；RPC 设计与兼容层细节：`docs/03+`（converter 自带文档）。

## 0. 方案一句话

业务代码 `from xtquant import ...` **一字不改**：客户端 site-packages 里的 miniQMT xtquant 被替换为本包的 shim，所有行情/交易调用经 **Redis RPC** 转发到大 QMT 终端内运行的桥接策略（`BIGQMT_REDIS_DRYRUN`）执行；委托/成交事件经 Redis **pub/sub + capped stream** 实时推回客户端，实现事件驱动的订单确认。

```
┌──────────────────────────── 同一台 Windows 机器 ────────────────────────────┐
│                                                                            │
│  业务进程 (py3.11)                 Redis 服务               大 QMT 终端     │
│  ┌────────────────────┐      ┌──────────────────┐      ┌────────────────┐  │
│  │ 监控/选股/回测脚本  │ RPC  │ redis-server     │      │ 策略编辑器运行  │  │
│  │ from xtquant ...   │─────►│ 127.0.0.1:6379   │◄────►│ BIGQMT_REDIS_  │  │
│  │        │           │ 请求  │ db=5             │ 事件  │ DRYRUN 桥接策略 │  │
│  │        ▼           │ 响应  │ rpc req/rsp 队列  │      │ passorder /    │  │
│  │ site-packages\     │◄─────│ order/trade      │      │ get_trade_     │  │
│  │ xtquant (shim) ───►│ 事件  │ events pub/sub   │      │ detail_data /  │  │
│  │ bigqmt_signal_     │      │ + capped stream  │      │ ContextInfo    │  │
│  │ trader (RPC 客户端) │      │                  │      │                │  │
│  └────────────────────┘      └──────────────────┘      └────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
```

## 1. 组件清单（本包目录）

| 路径 | 部署位置 | 作用 |
|---|---|---|
| `qmt_side/BIGQMT_REDIS_DRYRUN.py` | `<QMT交易端>\python\` | 桥接策略入口（QMT 策略编辑器加载它） |
| `qmt_side/bigqmt_signal_trader/` | `<QMT交易端>\python\` | QMT 侧运行库：RPC 服务、事件发布、xtquant 兼容实现 |
| `qmt_side/bigqmt_signal_trader_strategy.py` / `..._redis_rpc_runtime.py` | `<QMT交易端>\python\` | 策略装配/运行时（入口会 import） |
| `qmt_side/bigqmt_signal_trader_local_config.example.py` | `<QMT交易端>\python\` | QMT 侧私有配置模板（复制为 `bigqmt_signal_trader_local_config.py`，**不入 git**） |
| `client/xtquant/` | 客户端 venv `site-packages\xtquant\` | shim 包：与 miniQMT xtquant 同接口，内部走 RPC |
| `client/bigqmt_signal_trader_client_config.example.py` | 客户端 venv `site-packages\` | 客户端私有配置模板（复制为 `bigqmt_signal_trader_client_config.py`，**不入 git**） |
| `tools/bigqmt_e2e_test.py` | 任意 | 事件驱动实盘 E2E：买入成交腿 + 挂单撤单腿（见第 5 节） |
| `tools/bigqmt_signal_trader_diagnostic.py` / `bench_*.py` | 任意 | 链路诊断 / 延迟基准 |
| `tests/` | 开发用 | converter 单元测试 |

## 2. 通道与事件约定

| 通道 | 形态 | 说明 |
|---|---|---|
| `bigqmt:rpc:req:{资金账号}` | list（rpush/blpop） | RPC 请求/响应，超时客户端默认 15s（冷启动首次重 RPC 可能 >6s） |
| `bigqmt:order_events:{资金账号}` | publish + xadd（capped stream, maxlen≈2000） | 委托事件（`on_stock_order`） |
| `bigqmt:trade_events:{资金账号}` | publish + xadd（同上） | 成交事件（`on_stock_trade`） |

**委托事件载荷**（JSON）关键字段：`order_id`（交易所 sysid，未分配时为空）、`user_order_id`（客户端传入的 `bq:` 前缀 ID，取自委托 remark）、`remark`、`status`、`traded_volume`、`order_volume`、`price`、`stock_code`、`ts`。

**订单状态语义**（交易所原始状态码原样透传）：

| status | 含义 | 终态 |
|---|---|---|
| 48 | 已报待报（本地缓存，尚未进交易所） | 否 |
| 50 | 已报（交易所已接受） | 否 |
| 54 | 已撤 | **是** |
| 55 | 部成已撤 | 是 |
| 56 | 已成 | **是** |
| 57 | 废单 | 是 |

> 客户端不得把「RPC 返回」当作委托确认；必须等 `status>=50` 的事件。RPC 返回仅代表信号/请求被策略接受。

## 3. 部署步骤

### 3.1 Redis 服务端（一次性）

1. 安装 tporadowski/redis 5.0.14.1 MSI（勾选 port 6379 / Add to PATH / Install as Windows Service）；或用 Memurai。
2. 编辑 `redis.windows-service.conf`：`bind 127.0.0.1`、取消注释 `requirepass <Redis密码>`。
3. `Restart-Service Redis`；验证：`redis-cli -h 127.0.0.1 -a <Redis密码> -n 5 ping` → PONG。
4. 防火墙**不要**放行 6379（仅本机）。

> **关键约束：客户端 redis-py 必须 < 6**（实测固定 `redis==5.2.1`）。redis-py 6+ 默认 RESP3 发 `HELLO` 握手，Redis 5.0 服务端不支持（报 `unknown command HELLO`）。**不要升级 redis 包。**

### 3.2 QMT 侧部署（正式端/模拟端相同，约 0.5 天）

```powershell
$SRC = "<本包>\qmt_side"
$QMT_PY = "C:\QMT交易端\python"        # 模拟端：C:\QMT交易端_模拟\python
Copy-Item -Recurse "$SRC\bigqmt_signal_trader" "$QMT_PY\bigqmt_signal_trader"
Copy-Item "$SRC\BIGQMT_REDIS_DRYRUN.py","$SRC\bigqmt_signal_trader_strategy.py","$SRC\bigqmt_signal_trader_redis_rpc_runtime.py" $QMT_PY
```

QMT 内置 Python 3.6.8 已自带 redis 3.5.3，QMT 侧零依赖。

1. 在 `$QMT_PY` 下按 `bigqmt_signal_trader_local_config.example.py` 创建私有配置 `bigqmt_signal_trader_local_config.py`：
   - `BIGQMT_ACCOUNT_ID = "<资金账号>"`（模拟端填模拟账号）
   - Redis 连接同上；`rpc_allow_order_methods` **先 False**（只读验证），上线下单时改 True
   - `exec_events_enabled: True`（事件推送必须开）；`download_jobs_enabled: False`（大 QMT 终端 SDK 不能连数据服务，历史数据靠终端自身下载，见 4.4）
2. QMT 终端（XtItClient.exe）**完整模式**登录（非极简模式；与 miniQMT 不要同时登同一账号）。
3. 「模型研究」→「+新建策略」（**坑：策略列表不扫描 python 目录，必须手工新建**）→ 类型 python → 名称纯 ASCII：`BIGQMT_REDIS_DRYRUN` → 删除模板，把 `python\BIGQMT_REDIS_DRYRUN.py`（GBK）全文粘贴进编辑器（内容全 ASCII 不会乱码；**不要另存为 UTF-8**）。
4. 右侧设置：运行周期 `1分钟`；标的 `000300.SH`；不勾「启动本地python」；不开自动交易 → 保存 → 编译 → 运行。
5. 输出面板出现 `started channel=bigqmt:rpc:req:{资金账号}` 即就绪。
6. 改配置后需**停止再运行**策略；reload 不生效就重启终端。
7. **运行模式**：策略交易面板可选「模拟 / 实盘」。
   - 模拟：RPC 下单仅生成「策略信号」，不进交易系统、**不触发任何委托/成交事件**——只用于验证链路通不通。
   - 实盘：真实委托，事件正常推送。事件链路验证必须切实盘（用小额 ETF 单，见第 5 节）。
8. 保持 QMT 终端常开（可设开机自启+自动登录）；QMT 关闭则桥接中断。

### 3.3 客户端部署（约 0.5 天）

```powershell
$PY = "<项目根目录>\.venv\python.exe"
$SP = "<项目根目录>\.venv\Lib\site-packages"
& $PY -m pip install redis==5.2.1
# 备份后移除 miniQMT 版 xtquant，装入 shim：
Copy-Item -Recurse "$SP\xtquant" "$SP\xtquant_miniqmt_backup"
Remove-Item -Recurse "$SP\xtquant"
Copy-Item -Recurse "<本包>\client\xtquant" "$SP\xtquant"
Copy-Item -Recurse "<本包>\qmt_side\bigqmt_signal_trader" "$SP\bigqmt_signal_trader"
```

在 `$SP` 下按模板创建 `bigqmt_signal_trader_client_config.py`（`BIGQMT_ACCOUNT_ID` 建议读环境变量，便于切模拟账号；`BIGQMT_RPC_TIMEOUT_SECONDS = 15.0`）。

业务代码**零改动**（仍 `from xtquant import xtdata, xttrader, xtconstant`）；如需显式指定 shim 来源，可在入口 `sys.path.insert(0, r"<本包>\client")` 后直接 import。

> 铁律：site-packages 下必须始终存在带 `__init__.py` 的 xtquant 包（miniQMT 版或 shim 版）。否则从含 `xtquant\` 参考目录的仓库根运行脚本会把该目录当命名空间包，`from xtquant import xtdata` 报迷惑性 ImportError。

## 4. 数据面注意事项（shim 已消化的差异，对业务透明）

1. **长表转置**：桥的 `get_market_data` 返回长表，shim 归一化为 miniQMT 原生 `{field: DataFrame(index=代码, columns=时间)}`，时间标签纯数字化（`20260807 14:56:00`→`20260807145600`）。
2. **多股批量拆分**：大 QMT 多股查询返回异常结构时，shim 自动逐股查询再按 field 拼接。
3. **`time` 字段 + 空起始时间**：大 QMT 不支持 `time` 字段 → RPC 前剥离、返回时按 epoch 毫秒合成；`start_time=''` 会被服务端拒绝（1m 数据权限限近一年）→ shim 按 `count`×周期推算起始时间（封顶 360 天）。
4. **数据下载分两层（2026-08-11 实测定论）**：终端原生 `download_history_data`（RPC 直达）**不可用**——终端内 SDK 报「无法连接数据服务」，与周期、年份无关；但 shim 的 `xtdata.download_history_data` **可用**，语义是「RPC 读取 + 客户端本地缓存」（cache-through），不触发终端下载，业务代码可正常调用。**可读范围取决于终端本地数据覆盖**（实测 1m 可读 2 年前的 2024-01，5302 根但不完整）——「1m 限近一年」是当时覆盖状态的表象，非固定权限；`start_time=''` 仍会被服务端拒绝，shim 按 `count` 推算起始时间的逻辑保留。历史补录只能走终端自身：QMT【系统→设置→行情/数据下载】开自动下载，或【数据管理】手动补。

## 5. 交易链路验证（标准操作流程）

### 5.1 只读自检（下单开关关闭状态下）

用客户端 venv 跑只读脚本（`docs/01-迁移方案.md` Phase 2.3 有完整脚本）：connect / 资产 / 持仓 / 当日委托 / `get_full_tick` / 前复权日线 / 交易日历 / 合约详情。验收：打印无异常，资产持仓与 QMT 终端【交易】页面一致。

### 5.2 开下单开关

改 QMT 侧 `bigqmt_signal_trader_local_config.py` → `"rpc_allow_order_methods": True`，策略编辑器**停止再运行**桥接策略。（关闭时 `order_stock` 会被服务端拒绝 `rpc method is not allowed`，这也是闸门验证方法。）

### 5.3 事件驱动 E2E（`tools/bigqmt_e2e_test.py`，实盘模式、盘中执行）

```powershell
python tools\bigqmt_e2e_test.py --account <资金账号> --code 515050.SH --volume 100
```

两腿全自动：

- **Leg A 买入成交腿**：以卖一价买 100 股 ETF（约百元级资金）。预期事件序列：委托事件 `status=50`（先无 sysid、`uid=bq:xxx`，随后带 sysid 的 50）→ 成交事件（`traded=100 @ 价`，remark 携带 `bq:` ID）→ 委托事件 `status=56`。随后 RPC 查询持仓核对 `+100` **真实成交**。
- **Leg B 挂单撤单腿**：以低于市价约 7% 的价格挂买单（不会成交）→ 等 `status=50` 学到 sysid → `cancel_order_stock(bq id)`（shim 自动解析成 sysid 撤单）→ 等 `status=54` 确认已撤。

全部 PASS 即证明 **下单 → 事件确认 → 撤单** 全链路真实可用。

> 注意：策略处于「模拟」运行模式时，本工具两腿都会因等不到事件而超时 FAIL——这是预期行为（信号单不触发事件），切换到「实盘」模式后重跑即可。

## 6. 事件驱动订单确认机制（2026-08-10 起的关键增强）

旧版桥的事件不带用户标识，RPC 委托事件无法归属到调用方（历史上曾因此出现同标的方向重复下 3 笔的事故）。现机制：

1. **用户委托 ID**：客户端下单前生成 `bq:<10位hash>` 作为 `order_remark` 透传（`order_stock_user(..., order_remark=uid)` → QMT 侧 `passorder` 的 remark → 交易所回报原样带回）。
2. **事件载荷**：委托/成交事件同时携带 `order_id`（sysid，可能为空）、`user_order_id`、`remark`。客户端按 `uid` 归属；`sysid` 到达后与同一订单快照归并，之后事件按 sysid 关联。
3. **确认语义**：`place_order_with_confirm` 等 `status>=50` 事件才返回「委托确认」；成交以 `status=56`（或成交事件）为准；撤单以 `status=54` 事件为准，超时未确认则告警并禁止盲目重试。
4. **防叠单**：下单前做在途检查——同标的同方向存在未达终态的委托时复用/跳过，不再重复发单。
5. **撤单解析**：`cancel_order_stock` 接受 `bq:` 用户 ID，shim 从事件快照解析出 sysid 后走原撤单 RPC。

## 7. 日常运维

| 事项 | 说明 |
|---|---|
| 每日启动顺序 | ① Redis（Windows 服务自动）→ ② 大 QMT 终端登录 → ③ 确认桥接策略在运行 → ④ 业务脚本照常 |
| QMT 崩溃/断线 | 重启 QMT 后在策略编辑器重新运行策略；客户端 RPC 会超时（15s）报错，业务重试逻辑兜底 |
| Redis 故障 | 所有调用立即超时失败；Redis 恢复后无需重启 QMT 策略即可恢复 |
| 历史数据缺口 | `get_market_data` 缺日子时，用 QMT【数据管理】手动补对应品种/区间 |
| 日志位置 | QMT 侧：策略编辑器输出面板；客户端：业务脚本自身日志 |
| 升级桥接包 | 重新执行 3.2/3.3 的拷贝（覆盖同名文件），QMT 侧停止再运行策略生效 |

## 8. 回滚方案

```powershell
$SP = "<项目根目录>\.venv\Lib\site-packages"
Remove-Item -Recurse "$SP\xtquant","$SP\bigqmt_signal_trader"
Rename-Item "$SP\xtquant_miniqmt_backup" "$SP\xtquant"
```

再停用 QMT 里的桥接策略，改用 XtMiniQmt 极简模式登录即可。大 QMT 桥与 miniQMT 互不干扰（不同进程不同连接），两套可并存。

## 9. 已知限制（接受现状）

| 缺口 | 影响 | 兜底 |
|---|---|---|
| `on_order_error` 不推送 | 废单不由回调触发 | 业务侧 60s 轮询终态处理 |
| `on_disconnected` 不触发 | 无断线推送 | 探活式重连（每轮 `query_stock_asset`，异常即重连） |
| `on_account_status` 不推送 | 仅少一条日志 | 无 |
| 终端原生下载 RPC 不可用 | 无法脚本化补历史数据 | 终端 GUI【数据管理】补录/自动下载；shim `download_history_data`（读取+本地缓存）业务可用 |
| 模拟运行模式不产生事件 | 事件链路只能在实盘验证 | 小额 ETF 单 E2E（第 5.3 节） |
| 首次重 RPC 冷启动 >6s | 偶发超时 | 客户端超时 15s，热身后 ~0.5s |
| RPC p50≈13ms | 对秒级轮询无影响 | — |

## 10. 验证记录（脱敏）

- **2026-08-09**：只读链路全通过（模拟端+正式端：connect/资产/持仓/委托/tick/1d 前复权/1m 240 根/多股合并/交易日历/合约详情）；下单闸门验证通过（关闭时 `rpc method is not allowed`）；确认 redis-py 8.x 与 Redis 5.0 不兼容 → 固定 `redis==5.2.1`。
- **2026-08-10**：事件链路增强上线（事件携带 `uid/sysid/remark`）；复盘确认旧版事件无用户标识导致 RPC 委托无法归属（曾致重复下单），已通过 `bq:` 用户委托 ID 机制修复。
- **2026-08-11**：实盘 E2E 全通过 —— ETF 100 股 @ 卖一价买入成交（事件 `50`→成交回报→`56`，持仓核对 +100 一致）；低价挂单撤单（`50`→`54`，撤单按 `bq:` ID 自动解析 sysid）。模拟运行模式验证：信号单不产生事件，符合设计。下载能力实测定论：终端原生 `download_history_data` RPC 不可用（「无法连接数据服务」，与年份无关）；shim 路径可用；1m 数据可读 2 年前（覆盖不完整），「1m 限近一年」为当时本地覆盖表象而非固定权限。

## 11. 参考文档索引

| 文件 | 内容 |
|---|---|
| `docs/01-迁移方案.md` | 分阶段迁移计划（Phase 0~5）、回滚、风险表 |
| `docs/02-切换操作手册.md` | 实际切换的操作记录与踩坑（策略列表不扫描目录、GBK、redis 版本等） |
| `docs/BIG_QMT_REDIS_RPC.md` | RPC 协议设计（请求/响应/错误/方法白名单） |
| `docs/RPC_API_REFERENCE.md` | 全部 RPC 方法签名与返回结构 |
| `docs/RPC_TRANSPORTS.md` | 传输层（redis/zmq/mysql/shm）对比与选择 |
| `docs/XTQUANT_COMPAT_REPLACEMENT.md` | shim 与 miniQMT xtquant 的兼容矩阵 |
| `docs/BIG_QMT_SIGNAL_TRADER_RUNBOOK.md` | converter 运行手册 |
| `docs/00-converter-README.md` | converter 项目原始 README |
