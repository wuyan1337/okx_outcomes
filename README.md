# OKX Outcomes Bot

这是一个面向 OKX Outcomes 预测市场的多账户交易控制台。它通过 FastAPI 提供网页和 API，把 OKX 下单、持仓、订单簿、Poly 真实 outcome 价格监控、策略风控、自动重部署和 Sniper 组合到同一个服务中。

> **实盘风险提示**
>
> 本项目会真实签名、下单、撤单和平仓。薄订单簿、网络延迟、假跳价、部分成交、API 超时、代理故障和服务重启都可能留下挂单或裸仓。`Stop` 不一定等于撤单，更不等于平仓；`Cancel All`、Poly Monitor 和 `Flash` 是账户级高影响操作。实盘前请使用独立账户和极小仓位完成完整演练。

本文描述的是当前新版代码，特别包括：

- Poly 价格固定显示两位 cent；
- OKX 按官方价格区间使用 1¢ 或 0.1¢ tick；
- Manual、Monitor 和 Sniper 全程区分真实 YES/NO；
- 新 Sniper 支持多条显式 `Poly outcome → OKX market + side` 映射；
- 一条候选触发后，全局锁住其他候选，直到该笔仓位完整退出；
- Sniper 配置和运行状态可以跨页面刷新、服务重启恢复；
- 根路径和 `/okx` 子路径均可部署。

## 1. 先理解单位和标识

### 1.1 cent、xp 和 share

- `¢` 或页面中的 `c` 表示 **cent**，不是美元。
- `xp` 是 OKX Outcomes 使用的基础记账/交易单位，不是美元。
- `share` 是 outcome token 数量。
- 订单名义额按 `OKX 原始价格 × share 数量` 计算；买单名义额不足 `1 xp` 时不会提交。

例如：

- 页面输入 `12.34¢`，对应原始价格 `0.1234`；
- `0.1¢` 对应原始价格 `0.001`；
- 原始价格 `0.01` 对应 `1¢`，不是 `0.01¢`。

### 1.2 三类 ID 不能混用

| 标识 | 所属平台 | 含义 | 当前用途 |
| --- | --- | --- | --- |
| Poly CLOB token ID | Polymarket | 某一个真实 outcome 的订单簿标识 | Poly 订单簿、Monitor、Sniper 信号 |
| OKX `marketId` | OKX | 一个预测市场的标识 | 市场查询、持仓归属、签名操作 |
| OKX `yesAssetId` / `noAssetId` | OKX | 可实际交易的 YES/NO outcome asset | 下单、撤单、持仓和订单识别 |

OKX 的 `pm-books` 查询和 WebSocket 订阅使用 `yesAssetId`；NO 订单簿由 YES 订单簿按真实买卖方向镜像。Poly 则必须选择对应 outcome 自己的真实 token，不能把 Poly NO 简单当成 `1 - YES`。

当 OKX Position API 没有直接返回 `assetId` 时，项目沿用当前 Outcomes 规则，从 `marketId` 和 `tokenIndex/tokenName` 识别：

- YES：`marketId + "000"`
- NO：`marketId + "100"`

这只是 OKX 内部 asset 的识别规则，不能用来生成 Poly token。

## 2. 价格精度规则

所有交易路径都应先把输入转换为 `Decimal`，再根据买卖方向对齐，避免二进制浮点和 `int(price)` 把 `0.1¢` 变成 `0`。

### 2.1 Poly 显示

Poly API 原价范围是 `0..1`。页面展示时乘以 100，并固定显示两位 cent：

| Poly 原价 | 页面显示 |
| ---: | ---: |
| `0.1234` | `12.34¢` |
| `0.0100` | `1.00¢` |
| `0.0010` | `0.10¢` |

Poly Order Book、Monitor 状态和 Sniper Poly 信号都使用这套格式。

### 2.2 OKX 可执行 tick

| OKX 原价范围 | 页面 cent 范围 | tick | 页面显示 |
| --- | --- | --- | --- |
| `0.04–0.96`，含边界 | `4–96¢` | `0.01`，即 `1¢` | 整数 cent |
| 小于 `0.04` 或大于 `0.96` | `0.1–3.9¢`、`96.1–99.9¢` | `0.001`，即 `0.1¢` | 一位小数 cent |

OKX 当前可执行原价范围为 `0.001–0.999`，也就是 `0.1–99.9¢`。因此：

- `0.1¢` 是合法边界，必须发送为 `0.001`；
- `0.01¢` 低于当前最小 tick，不能作为合法挂单价；
- `4¢` 和 `96¢` 使用整数 tick；
- `3.9¢`、`96.1¢` 使用 0.1¢ tick。

方向性对齐规则：

- BUY 向下对齐，避免实际买价高于用户上限；
- SELL 向上对齐，避免实际卖价低于用户目标；
- 只做显示时按对应市场 tick 格式化，不应先转整数。

OKX NO 镜像簿的方向是：

- 买 NO 的 best ask = `1 - YES best bid`；
- 卖 NO 的 best bid = `1 - YES best ask`。

镜像时必须交换 bid/ask，不能只对单个价格做 `1 - price` 后仍保留原方向。

## 3. 系统架构

```text
Browser
  ├─ HTML / JavaScript dashboard
  ├─ REST /api/...
  └─ WebSocket /ws/book/{account_id}
          │
          ▼
FastAPI backend/app.py
  ├─ Basic Auth 与页面路由
  ├─ base-path middleware
  ├─ 市场、订单、持仓、策略 API
  └─ AccountManager
          │
          ├─ AccountInstance A
          │    ├─ OKXOutcomesClient
          │    ├─ Manual-D / Manual-S
          │    ├─ Poly Monitor / Auto-Rebalance
          │    ├─ New Sniper / Old Sniper
          │    └─ TelegramNotifier
          │
          └─ AccountInstance B ...

SQLite + Fernet key + JSON runtime files
```

一次典型请求的调用链是：

```text
浏览器 → FastAPI route → AccountManager → 指定 AccountInstance
      → strategy/client → OKX REST 或 Poly REST/WS
```

核心边界：

- `backend/app.py` 负责 HTTP、认证、参数接收和页面服务，不承载具体策略循环；
- `AccountManager` 为每个账户创建独立 client 和策略对象，管理启动、关闭与恢复；
- `OKXOutcomesClient` 统一处理 REST 鉴权、写操作签名、SOCKS5、分页和心跳；
- `backend/ws_book.py` 只负责向浏览器汇总 Poly/OKX 订单簿；
- 策略服务自己负责状态机、partial fill、风控和持久化。

## 4. 项目目录和文件职责

### 4.1 根目录

| 路径 | 职责 |
| --- | --- |
| `README.md` | 当前项目手册。 |
| `requirements.txt` | Python 运行依赖。 |
| `config.yaml` | Web 登录、base path 和首次 Telegram 配置来源。不要提交真实凭据。 |
| `.gitignore` | 忽略 Python 缓存、`data/`、扩展名恰好为 `.bak` 的文件和 `.env`。仓库里已跟踪的 `.bak-*` 历史快照不会因此消失。 |
| `auto-push.sh` | Linux `inotify` 自动 add/commit/push 脚本；不是启动脚本，运行前必须审查。 |
| `ss.txt` | 本地 OKX Outcome API 文档副本，不是程序运行依赖，当前也不应作为凭据示例。 |
| `patch_fix.py` | 旧的一次性文本替换补丁，不在启动或测试流程中；重复执行可能误改现有代码。 |
| `layers` | 空占位文件，当前没有代码读取。 |
| `scripts/bootstrap-egress-node.sh` | 在新 Ubuntu 出口 VPS 创建受限 SSH forwarding 用户；只允许 Bot 单 IP、key auth 和 OKX 目标，不启动公网 SOCKS。失败会恢复原 SSH 配置/authorized key。 |
| `scripts/install-egress-tunnel.sh` | 在 Bot Ubuntu 主机核验 host key，安装 `127.0.0.1` dynamic SOCKS systemd 服务并做 Region 连通检查。 |
| `scripts/proxy_nodes.py` | 原子管理本机 node registry、列出 backend 实际节点并执行统一延迟 benchmark。 |
| `data/` | 本地运行数据，Git 已忽略；包含数据库、密钥和策略状态，不能随意清空。 |
| `*.bak-*`、`*.bak` | 人工历史快照，不会被 Python import；仅用于人工比对或回滚参考。 |

### 4.2 Backend

| 路径 | 职责 |
| --- | --- |
| `backend/__init__.py` | 将 `backend` 标记为 Python package。 |
| `backend/app.py` | FastAPI 入口；加载配置、Basic Auth、base-path middleware、生命周期、页面与全部 REST API；解析 Poly URL，并维护 Poly/OKX 映射。 |
| `backend/ws_book.py` | Poly 与 OKX 订单簿 relay；连接 OKX 官方 `/ws/v5/business` 的 `pm-books`，处理 snapshot/update、序列 gap 和断线重连。 |
| `backend/core/__init__.py` | `backend.core` package 标记。 |
| `backend/core/auth.py` | OKX HMAC REST 鉴权；写操作使用 msgpack、keccak、EIP-712/ECDSA 生成签名。 |
| `backend/core/client.py` | OKX Outcomes 异步 REST client；负责代理连接、读写 API、分页、`clientOrderId`、heartbeat 和 session 生命周期。 |
| `backend/core/database.py` | SQLite 表、账户增删改查、Fernet 加解密、全局配置和 Sniper 状态。 |
| `backend/core/proxy_nodes.py` | 内置节点、统一名称、`data/proxy_nodes.json` 本地扩展和节点校验。数据库仍保存稳定的旧 ID。 |
| `backend/core/price_utils.py` | 所有 cent/`0..1` 转换、OKX tick、方向性对齐、显示格式和二元价格镜像。 |
| `backend/services/__init__.py` | `backend.services` package 标记。 |
| `backend/services/account_manager.py` | 为每个账户组装 client、notifier 和全部策略；处理依赖连接、启动/关闭与新版 Sniper 恢复。 |
| `backend/services/manual_strategy_d.py` | Manual-D 双边挂单、增量 fill 检测、对应卖单和 pending sell 持久化。 |
| `backend/services/manual_strategy_s.py` | Manual-S 单边引擎；Bid-Sell 也复用此引擎。 |
| `backend/services/poly_monitor.py` | Poly best bid 监控、深度门槛、触发账户级撤单和 Auto-Rebalance。 |
| `backend/services/auto_rebalance.py` | Monitor 触发后的对账、稳定等待、成本层同步、卖单恢复和策略重启。 |
| `backend/services/poly_sniper.py` | 新版显式多候选 Sniper、全局锁、入场/退出、partial fill、状态保存和恢复。 |
| `backend/services/poly_sniper_old.py` | 遗留版 Sniper，仅用于兼容和对照。新功能不要继续加在这里。 |
| `backend/services/notifier.py` | Telegram 异步发送队列和配置刷新。 |
| `backend/services/size_utils.py` | 数量规范化；买单 share 取整数，卖单保留真实可用的小数数量。 |
| `backend/services/latency_probe.py` | OKX 各路由与 Poly Direct 的冷/热多次探测、统计和错误分类；不伪装成真实下单延迟。 |

### 4.3 Frontend

| 路径 | 职责 |
| --- | --- |
| `frontend/dashboard.html` | 单文件主控制台，包含 HTML、CSS 和 JavaScript；账户、策略、订单簿、订单、持仓、配置和日志 UI 都在此文件。 |
| `frontend/login.html` | 登录页。 |
| `frontend/sniper-help.html` | 新 Sniper 参数和操作帮助。 |
| `frontend/polytest.html` | 未被 `app.py` 注册的旧诊断页，含历史硬编码信息，不应部署或作为生产入口。 |

### 4.4 Tests

| 路径 | 覆盖重点 |
| --- | --- |
| `tests/test_price_precision.py` | Poly/OKX 显示、边界 tick、0.1¢ 和方向性对齐。 |
| `tests/test_poly_sniper.py` | 多候选、全局锁、NO 镜像、信号、partial fill 和退出行为。 |
| `tests/test_sniper_persistence.py` | Sniper SQLite 状态保存、刷新/重启恢复与锁定恢复。 |
| `tests/test_account_manager_sniper_shutdown.py` | AccountManager 关闭时保留 Sniper 恢复语义，不做意外账户级撤单。 |
| `tests/test_persistence_and_rebalance.py` | 辅助状态持久化和 Auto-Rebalance 成本层行为。 |
| `tests/test_ws_base_path.py` | OKX WS snapshot/序列处理、HTTP/WS 子路径和前端 JavaScript 语法。 |
| `tests/test_rebalance_hardening.py` | 成本层隔离、双资产对账、撤单终态、protective sell 和失败关闭。 |
| `tests/test_manual_strategy_safety.py` | D/S 单边失败、撤单确认、sell 重试、Bid-Sell spread 和 sell-all 例外。 |
| `tests/test_dashboard_sniper_active_target.py` | Sniper active target 切换、页面价格/数量格式和手工下单保护。 |
| `tests/test_app_cancel_all.py` | 页面 Cancel All/Flash 的撤单、reduce-only IOC 与最终订单/持仓核验。 |
| `tests/test_app_config_safety.py` | 凭据/region 变更前的本地状态、交易所敞口、agent key 和 client 原子切换。 |
| `tests/test_poly_monitor_empty_book.py` | Monitor 对 REST/WS 空 bid 的风控、初始空簿拒绝和异常响应忽略。 |
| `tests/test_latency_probe.py` | Region/真实 endpoint、冷/热采样、统计、失败分类、Poly shared route 和并发隔离。 |
| `tests/test_proxy_nodes.py` | 稳定节点 ID、canonical 名称、本地 registry、重复名和 tunnel loopback 约束。 |
| `tests/test_dashboard_latency_ui.py` | Health/Add Account 延迟展示、Region、请求取消、旧响应隔离和 Sniper 真实埋点展示。 |
| `tests/test_app_latency_api.py` | latency API 参数传递、节点 assignment metadata、账户真实 health code 和零余额。 |

## 5. 配置与启动

### 5.1 环境要求

- Python 3.10 或更新版本；
- 能访问 OKX Outcomes 和 Poly CLOB；
- 如果账户指定 SOCKS5 节点，本机相应端口必须可用；
- 生产部署建议由 Nginx/Caddy 提供 TLS。

### 5.2 安装

Windows PowerShell：

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Linux：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### 5.3 `config.yaml`

只使用占位符创建配置，不要复制仓库历史中的登录或 Telegram 值：

```yaml
server:
  host: 127.0.0.1
  port: 8899
  username: "change-me"
  password: "use-a-long-random-password"
  base_path: ""

telegram:
  bot_token: ""
  chat_ids: []
```

说明：

- `server.username` 和 `server.password` 用于页面/API Basic Auth；
- `server.base_path` 控制根路径或子路径部署；
- `server.host` / `server.port` 不会替你启动 Uvicorn，实际监听地址以 Uvicorn CLI 参数为准；
- YAML 中 Telegram 只在 SQLite 全局配置为空时首次迁移；之后以 Dashboard/SQLite 保存的值为准。

账户的 OKX API key、secret、passphrase、agent private key 和 proxy node 从 Dashboard 添加，保存到 `data/accounts.db`。敏感字段由 `data/.key` 对应的 Fernet key 加密。

Fernet 只是本机静态保护，不是专用密钥保险库。拿到 `accounts.db` 和 `.key` 的人可以解密账户凭据。

### 5.4 启动

```powershell
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8899
```

然后访问：

```text
http://127.0.0.1:8899/
```

不要运行 `auto-push.sh` 来启动服务；它只负责监控文件并自动提交、推送 Git。

**必须保持单进程、单实例。**不要添加 `--workers 2` 或更大的值，也不要让两套服务共享同一个 `data/`。`AccountManager`、策略任务、WebSocket 状态和账户生命周期锁都在进程内；SQLite 的 Sniper snapshot 只用于恢复，不是跨进程 leader lock。多 worker 或多实例可能各自恢复同一条 running Sniper、重复监听并重复下单，也会让 `.key` 和本地 JSON 状态产生写入竞态。

## 6. Dashboard 操作区

### 6.1 账户

每个账户拥有独立的：

- OKX client 和 SOCKS5 路由；
- Manual-D、Manual-S/Bid-Sell；
- Poly Monitor 与 Auto-Rebalance；
- 新版/旧版 Sniper；
- 带账户名上下文的策略运行日志。

Telegram token/chat 配置是全局共享的；各账户策略引用同一个 notifier，但通知正文会带账户上下文。

同一个 proxy node 当前只允许分配给一个账户。账户删除会移除数据库记录并关闭相应实例；删除前应先在 OKX 核对开放订单和持仓。

### 6.2 Order Book

- Poly Order Book 使用所选真实 Poly token；
- OKX Order Book 总是以 `yesAssetId` 查询/订阅；
- 当页面选择 OKX NO 时，后端/前端按 YES 簿镜像成真实 NO bid/ask；
- Poly 固定显示两位 cent；
- OKX 根据当前价格区间显示整数或一位小数 cent。

OKX WS 必须先收到 `snapshot` 才接受 `update`。如果 update 早于 snapshot，或 `prevSeqId/seqId` 出现断链，relay 会主动重连，避免继续展示失真的本地簿。

当前已知限制：`pm-books` 序列 gap 会被检测并重连，但还没有对 OKX checksum 做独立校验；REST fallback 与 WebSocket 更新之间仍可能出现很短的显示竞态，最终应以重新同步后的订单簿和 OKX 真实订单状态为准。

### 6.3 Active Orders、Cancel All 和 Flash

- 单条取消只针对选中的订单；
- `Cancel All` 会处理该账户的全部开放订单，不限于当前页面策略；
- `Flash` 会停止策略、撤账户开放订单，再按各持仓 best bid 提交 reduce-only IOC 卖单；
- `Flash` 只有在最终重新查询确认开放订单和开放持仓都为 0 时才返回完成；
- IOC 被 OKX 接受只表示请求已提交，不能保证全部成交；仍有 partial position 时页面会明确显示未完成。
- OKX 已确认清空后，Flash 会再调用 Sniper 的持久 ledger 对账；若订单终态仍无法证明，OKX 敞口虽已清空，接口仍会提示 Sniper 需要对账，而不会直接删除锁。

执行高影响操作后必须重新查询 Open Orders 和 Positions，不能只看按钮返回成功。

### 6.4 Manual 手工交易

Manual 页允许：

- 选择 OKX 市场和真实 YES/NO；
- Market 模式按当前 best ask/bid 提交 IOC；
- Limit 模式按输入 cent 价格提交 GTC；
- 买入数量使用整数 share；
- 卖出使用实际持仓可用数量，可保留小数；低于 `1 xp` 时只有“卖完全部仓位且没有冻结数量”才适用 sell-all 例外；
- 查看 position 和 closed-order 交易历史。

服务端会再次校验和方向性对齐价格。买单名义额低于 `1 xp` 时直接拒绝。API 超时不代表订单一定失败；遇到结果未知时应先按 `clientOrderId/orderId` 和持仓查询，不能盲目重下。

新版 Sniper 正在运行时，普通 Manual 下单会被服务端拒绝，防止手工仓位变化破坏 `position_before` baseline。Sniper 已停止但仍 locked 时禁止 Manual BUY；如确需人工对账，可在核对订单后使用 reduce-only Manual SELL 或账户级 Flash，随后再次执行 Sniper Stop/状态查询让本地 ledger 完成对账。不要在 Sniper holding 期间从其他页面、脚本或另一套服务修改同一 asset 的仓位。

API 凭据或 KYC region 会替换账户 client。只要任一策略、Monitor、Auto-Rebalance 或 Sniper 锁仍活动，服务端会拒绝保存这类变更。`Stop` 可能保留交易所挂单，所以本地任务停止还不够：服务端还必须成功查询并确认旧账户 Open Orders 和 Positions 都为 0，否则返回 409/503，不写数据库也不替换 client。新 agent private key 会在写库前解析验证；准备好新 client 后才执行数据库更新和内存切换。

## 7. Manual-D

Manual-D 是同一二元市场的双边策略。页面输入 `a`，单位为 cent，当前四个目标价为：

```text
YES buy  = a
NO  buy  = 99 - a
YES sell = a + 1
NO  sell = 100 - a
```

例如 `a = 30¢`：

```text
YES: buy 30¢ → sell 31¢
NO:  buy 69¢ → sell 70¢
```

运行过程：

1. 按账户可用余额和 Position % 计算相同 pairs 数量；
2. 同时提交 YES/NO GTC buy；
3. 轮询 closed orders 和仍开放订单中的 partial fill；
4. 只处理新增 fill 数量；
5. 为对应 asset 提交 GTC sell；
6. 将已处理数量写入 `pending_sells_d_{account_id}.json`，避免重启后重复处理。

所有价格最终仍经过 OKX tick 校验和方向性对齐，不能假定公式结果天然可执行。

双边部署采用失败保护：如果 NO buy 提交失败，会撤销本轮 YES buy，并轮询确认撤单终态。撤单结果未知或任一边已经成交时，策略会停止继续部署新一轮，但主循环仍管理已经观察到的 fill，避免用“启动失败”掩盖裸仓。对应 sell 只有在 OKX 明确接受后才推进 pending 进度；失败数量按每个 buy order 以 `2/4/8/.../60` 秒退避重试。低于 `1 xp` 的 sell 必须重新查询 position，只有请求数量等于 `availableSize` 且也等于总 `size` 时才使用 sell-all 例外。

**Manual-D 的 Stop 语义：**停止策略循环和继续部署，但保留已经提交到 OKX 的挂单；不会自动 Cancel All，也不会平仓。

Manual-D 的 Poly Monitor 固定要求 **真实 Poly YES token**。它不会让用户选择一个 Poly NO 后用 `100 - YES` 冒充监控价。

## 8. Manual-S

Manual-S 只交易选中的一个真实 OKX side：

```text
buy  = a
sell = a + 1¢
```

主要行为：

- 用户明确选择 OKX YES 或 NO；
- 按 Position % 计算并提交 GTC buy；
- 轮询完整成交和 partial fill；
- 每次只为新增 fill 提交对应 GTC sell；
- pending sell 进度写入 `pending_sells_s_{account_id}.json`；
- sell 被拒绝或查询失败时不推进 pending 进度，按 buy order 退避重试；
- 低于 `1 xp` 时，仅在 position 明确证明没有冻结数量且会卖完整仓位时提交 sell-all；
- Stop 只停止循环，保留 OKX 现有挂单。

如果交易 OKX NO：

- OKX 可执行簿来自 YES `pm-books` 的真实镜像；
- Poly Monitor 必须选择该市场的 **真实 Poly NO token**；
- Monitor 使用这个 NO token 的真实 best bid，不使用 `100 - Poly YES`。

## 9. Bid-Sell

Bid-Sell 复用 Manual-S 引擎，但页面输入的是目标 sell：

```text
sell = 页面输入
buy  = sell - 1¢
```

它仍需要明确选择 OKX YES 或 NO，并遵循相同的 partial fill、pending sell、Stop 和精度规则。

差异在 Poly Monitor 的参考价：

- Manual-S classic 使用 buy level；
- Bid-Sell 使用 sell level；
- Poly bid 下跌或满足深度要求的上涨都可能触发重新部署。

## 10. Poly Monitor 风控

Poly Monitor 同时使用 Poly REST 轮询和 WebSocket；REST 基准轮询约每 1.5 秒一次。比较前先把 Poly best bid 向下对齐到当前可执行 OKX tick，避免用无法实际挂出的价格触发错误比较。

### 10.1 触发条件

- **bid 下跌：**立即触发；
- **bid 上涨：**只有新 best bid 的 `price × size` 达到 Depth Threshold 才触发；
- 已有正基准后，如果一次成功的 REST/WS 订单簿明确返回空 bids，会按 bid `0`、depth `0` 处理并触发下跌风控；网络失败或畸形响应不会冒充空簿；
- 启动时若没有显式 reference 且初始 Poly bids 为空，Monitor 拒绝启动，不能把 `0` 固化成基准；
- Depth Threshold 单位是 `xp`，不是美元。

使用真实 token 的规则：

| 策略 | Poly Monitor 必须选择 |
| --- | --- |
| Manual-D | 真实 YES token |
| Manual-S YES | 真实 YES token |
| Manual-S NO | 真实 NO token |
| Bid-Sell YES | 真实 YES token |
| Bid-Sell NO | 真实 NO token |

### 10.2 触发顺序

1. 对当前 OKX **账户级 Cancel All**；
2. 停止当前策略；
3. 写日志并发送通知；
4. 如果启用 Auto-Rebalance，启动重部署流程。

> **重要：**Poly Monitor 的撤单范围可能覆盖该 OKX 账户所有开放订单，不只是当前 Manual 策略创建的订单。不要在同一个账户中混放不希望被风控撤掉的手工订单或其他机器人订单。

## 11. Auto-Rebalance

Auto-Rebalance 只在 Poly Monitor 已触发且账户启用该开关时运行：

1. 查询账户开放订单；如仍有订单，使用 batch cancel 清理，并以“成功查询且结果为 0”作为唯一撤单完成条件；不会再次调用限频约 `1 次/小时` 的账户级 `cancelAll([])`；
2. 捕获触发前后可能出现的 partial/complete BUY fill，并按 order 累计成交量幂等去重；
3. 按真实 `marketId + assetId` 查询 OKX positions；Manual-D 会分别对账 YES 和 NO，不把两边混成同一个成本层；
4. 每 5 秒采样一次 Poly 价格；`stabilize_secs` 会换算成连续样本数（至少 1 次），因此它不是严格连续计时 N 秒，最长观察约 3 分钟；
5. 用 `Decimal` 将新价对齐到 OKX 可执行 tick；
6. 即使稳定价与原价相同，也会把每个 asset 的实际持仓数量与本地成本层完整同步；
7. 按 `max(记录成本价, 新 sell 价)` 为每个 outcome 重挂 `reduceOnly` protective sell，避免低于记录成本卖出；低于 `1 xp` 只允许整仓 sell-all；
8. 先恢复 Monitor，再重启原 Manual-D、Manual-S 或 Bid-Sell；任一步失败都会停止新 BUY 并报告失败，不能把部分成功写成“完成”。

成本层保存在：

```text
data/cost_layers_{account_id}.json
```

当前文件格式是 version 2：

```json
{
  "version": 2,
  "enabled": true,
  "layers": {
    "market_id": {
      "asset_id": {
        "55": 12.5
      }
    }
  },
  "legacy_unassigned": {},
  "captured_buy_fills": {}
}
```

- `layers` 按 market、asset、成本 cent 三层隔离；同价但不同市场/不同 outcome 不会互相扣减；
- `captured_buy_fills` 记录每笔已计入的累计 BUY fill，避免下一次风控重复加层；
- 旧版 flat `{price: quantity}` 文件缺少资产归属，加载后只会进入 `legacy_unassigned`，绝不会自动套到当前 YES/NO；需要人工查清来源后处理；
- `enabled` 是该账户的 Auto-Rebalance 开关；UI 切换成功时会和成本层一起原子写入，普通服务重启后自动恢复；
- 老的 version 2 文件没有 `enabled` 时默认关闭；字段不是 JSON boolean 或文件损坏时也安全地保持关闭；
- 恢复 `enabled` 只恢复开关，不会凭空恢复已经停止的 Manual/Monitor 任务。Auto-Rebalance 仍然只会在 Poly Monitor 实际触发时运行。

清空成本层是高风险维护动作，它不会凭空消除 OKX 实际持仓，执行前必须先对账。账户仍有策略、Monitor、Sniper 锁或 Auto-Rebalance 活动时，clear-layers API 会拒绝执行；成本层文件写入失败时会恢复内存中的原数据并返回失败。只有撤单终态、position 查询、protective sell、Monitor 和策略重启全部成功，Auto-Rebalance 才会报告完成。

## 12. 新版 Sniper：显式多候选和全局锁

新版 Sniper 不再自动从一个市场同时派生 YES 和 NO。每一行都是用户明确选择的映射：

```text
真实 Poly outcome token → OKX market → OKX YES 或 NO
```

例如一个赛事可以配置：

| Poly signal | OKX market | OKX side |
| --- | --- | --- |
| Argentina 的真实 YES token | Argentina | YES |
| Australia 的真实 NO token | Australia | NO |
| Draw 的真实 YES token | Draw | YES |

每一行的 Poly outcome 和 OKX side 是独立选择；代码不会假定它们名称相同，也不会从 Argentina 自动生成另一边。

### 12.1 页面配置

1. 输入 Poly 市场 URL，让后端解析所有真实 outcomes；
2. 点击 `Add Candidate`；
3. 每行选择一个真实 Poly outcome；
4. 每行选择对应 OKX market；
5. 每行选择要买的 OKX YES 或 NO；
6. 设置 jump threshold、Buy Limit、Position %、timeout、settle 和 window；
7. 点击 `Start Sniper`。

同一 Poly token 不能重复出现在两行；重复信号会造成候选含义不确定，后端会拒绝启动。同一账户的所有策略 Start/Stop、Cancel All 和 Flash 操作由账户级生命周期锁串行执行，Sniper 自身还有独立 start/stop 锁，防止多个浏览器页同时启动出两套监听任务。

运行中或 locked 时所有配置输入会锁定，避免浏览器显示值和后端实际运行配置发生偏差。刷新页面后，页面从后端状态恢复 targets、参数、运行状态、active target 和 locked 状态。Poly URL 解析和状态轮询都带请求序号，旧请求晚返回时不会覆盖较新的 URL、target 或盘口绑定。

界面只保留简单的 Start/Stop 语义，没有 Pause New Entries、Stop Target、Close Target 或 Close All。

### 12.2 全局锁

空闲时所有候选共同监听。第一条满足条件的候选通过 `asyncio.Lock` 取得账户内 Sniper 全局锁：

```text
Monitoring all candidates
          │ first valid signal
          ▼
Locked to one target
  ├─ entry
  ├─ holding
  └─ exit + position reconciliation
          │ baseline restored
          ▼
Unlock and monitor all candidates again
```

因此 Argentina 已经触发、入场或持仓时：

- Australia 和 Draw 不能再触发入场；
- 它们的行情消息即使继续到达，也没有入场资格；
- 系统不会同时管理第二个 Sniper 仓位；
- 只有 active target 完整退出并与 position baseline 对账成功后才解锁。

这道锁是为了防止一笔持仓期间被其他噪音信号连续击中，不是为了让多个候选同时开仓。

### 12.3 信号

- 在 `window_ms` 时间窗内记录该 Poly token 的 best bid；
- 以窗口 low 到 current 的上涨判断 jump；
- jump 必须达到 threshold；
- `settle_ms > 0` 时，价格还要保持不变达到 settle 时间；
- 最终 entry 价必须同时低于 Poly 当前信号价和 Buy Limit；
- 每个候选使用自己的真实 Poly token，Poly NO 不从 YES 推导。
- Poly 订单簿变空时，该候选会立刻失去校准并清空 jump window；报价重新出现后的第一笔只用于重新校准，不会把断档前后的价格拼成一次假跳；WS 重连也采用相同规则。

### 12.4 入场、持仓和退出

- 入场提交短暂的 GTC marketable limit；
- 生成 `clientOrderId` 并成功保存下单前 SQLite snapshot 后才允许发单；任一步失败都会禁止本次下单；
- 约 0.3 秒后只按 Sniper 自己的 `clientOrderId` 撤未成交 entry，不做账户级撤单；
- 买入 share 规范为整数；
- 名义额不足 `1 xp` 不下单；
- partial fill 会保留并进入持仓管理，不能当成整单失败；
- 约持仓 1 秒时执行一次 Poly stoploss 检查；
- timeout 到达后，按 OKX 当前 best bid 提交 reduce-only IOC；
- 最多尝试 5 次退出；
- 每次退出使用独立 `clientOrderId`，累计真实 partial fill；
- 必须确认 position 回到入场前 `position_before` baseline，才视为完整退出和解锁。

OKX NO 的入场/退出价格来自正确镜像：

```text
NO entry ask = 1 - YES best bid
NO exit bid  = 1 - YES best ask
```

### 12.5 Stop 的准确语义

点击 Sniper Stop 会：

- 停止接收新信号；
- 撤销 Sniper 自己仍未完成的 entry；
- 保存最新配置和 runtime 状态；
- **不会替用户主动平掉已存在的 Sniper 持仓。**

如果 Stop 时仍有持仓、订单终态无法确认或 API 请求结果未知，Sniper 会保持 `locked/error`。锁没有解除前不能再次 Start；应先人工核对 OKX Open Orders、Closed Orders、Positions 和服务日志。

页面会把 `error` 或“stopped 但仍 locked”明确显示为需要对账，不会显示成 Idle。Stop API 也会返回真实的 `locked/needs_reconciliation`；只有后端确认已安全解锁，页面才提示普通的 stopped。

每次真实 entry/exit 前都会先写带 `clientOrderId` 的 snapshot，写入失败就不会发单。如果 holding 起点、订单结果不确定后的 error lock，或 stopped marker 这类关键状态写入失败，当前进程会标记 `persistence_error`、停止继续自动推进，并锁住 Sniper 配置、Manual 下单和其他策略启动；Stop 本身也会返回失败。此时不要直接重启服务，因为数据库里可能仍是旧的 `running=true` 或旧持仓计时；修复磁盘/数据库问题后再次点击 Stop，直到最新 stopped/locked 状态成功写入并完成 OKX 对账。

## 13. Sniper 持久化和服务重启恢复

新版 Sniper 在每次关键状态变化时写 SQLite `sniper_state`，包括：

- config 和所有 targets；
- 原先是否 running；
- active target 和 locked；
- buy/sold 数量；
- `position_before`；
- entry `clientOrderId`；
- 所有 exit `clientOrderId`；
- 当前状态和更新时间。

服务启动时，`AccountManager` 调用 `restore(resume=running)`：

- 没有风险敞口时，恢复原 targets 并继续监控；
- 存在锁时，查询 open/closed orders 和 positions；
- 能确认 entry/exit 与仓位归属时，恢复 holding 或安全解锁；
- 无法确认时保持 error/locked，禁止新入场；
- 正常服务关闭不会为了方便而执行账户级 Cancel All；
- 正常关闭会保留“关闭前原本 running”的恢复标记。

如果 `sniper_state` 的 config/runtime JSON 损坏，启动时会将该账户 Sniper 标为 `error/locked`，保留原数据库行并阻止自动 snapshot 覆盖。不要直接清掉这条记录；先备份数据库，再依据 OKX 的真实订单和 position 做人工恢复。

恢复失败分两类处理：JSON 可解析但 targets/config 语义非法时，同样保留原行并阻止后续写入；如果配置有效、只是 OKX 订单或持仓查询暂时失败，则保持 `error/locked`，但不永久封死 snapshot 写入，网络恢复后可以再次执行 Stop 触发对账。

页面刷新和服务重启是两件事：

- 页面刷新只重新读取后端当前状态，不中断后端策略；
- 服务重启会走订单和 position 对账，不能只相信重启前的内存数量。

Manual-D、Manual-S 和 Poly Monitor 的完整 running 状态当前不会像新版 Sniper 一样自动续跑。它们只持久化 pending fill、成本层或映射等辅助数据。

## 14. SQLite 和本地运行数据

主要文件：

```text
data/
├─ accounts.db
├─ .key
├─ accounts.db-wal / accounts.db-shm     # SQLite 运行时可能出现
├─ proxy_nodes.json                       # 本机节点扩展/覆盖，部署时必须保留
├─ pending_sells_d_{account_id}.json
├─ pending_sells_s_{account_id}.json
├─ cost_layers_{account_id}.json
└─ poly_asset_map.json
```

`accounts.db` 当前包含：

| 表 | 内容 |
| --- | --- |
| `accounts` | 账户名、加密后的 API 凭据、agent key、proxy node 等。 |
| `global_config` | Telegram 等全局配置和少量运行辅助状态。 |
| `sniper_state` | 新版 Sniper 配置、runtime snapshot、running 标记和更新时间。 |

备份要求：

- `accounts.db` 与 `.key` 必须一起备份；
- 在数据库正在写入时，还要正确处理 WAL/SHM，优先先停服务再做一致性备份；
- pending sells、cost layers 和 `poly_asset_map.json` 也属于恢复所需状态；
- 不要因为 `data/` 被 Git 忽略就把它当成可删除缓存。

## 15. 延迟检测、统一节点与新服接入

### 15.1 两类延迟不能混在一起

Dashboard 的 Add Account 和 System Health 使用的是**合成读请求探测**：

- OKX Region 为 AU 时探测账户真实使用的 `https://us.okx.com`；CN 时探测 `https://www.okx.com`；
- 已有同 Region 账户且能确定 active Sniper target（或只有一个完整 target）时，OKX 请求使用该账户逐次签名，并探测这一组映射的 `/api/v5/market/pm-books?instId=...&sz=10`；Poly 请求使用同一映射的真实 token `/book`。多 target 尚未触发、无法确定唯一映射时退回 connectivity-only，不会拼接两个无关市场；
- OKX `pm-books` 是鉴权接口：没有同 Region 账户时不能做真实 market read，只能退回 `/api/v5/public/time` 连通性探测，不能把后者标成行情延迟；
- 没有可用 asset/token 时只探测 `/api/v5/public/time` 和 Poly `/time`，页面明确标为 `Connectivity only`，不能把它当行情或下单速度；
- 每条路径使用一个 session：第一请求单列为 cold，随后默认 5 次 warm，返回原始样本、median、min、max、jitter、成功/失败/跳过数和错误分类；失败节点不会用少量偶然成功样本参与 `LOWEST RTT` 排名；
- Poly 从 Bot 主机 Direct 访问，对所有 OKX 出口节点是同一条共享路径，所以只显示一次；不会把同一个 Poly RTT 重复加到每个节点；
- 页面不再显示没有真实含义的 `jp2_ms` 或简单 `total_ms`，并会在切换 Region/关闭弹窗时取消旧请求，旧响应不能覆盖新结果。
- 排名徽标写作 `LOWEST RTT`，只表示本轮同类读探测最低，不宣称该节点的真实订单一定最快。

这类 GET 探测能比较 DNS、连接、代理和读路径，**不能测出真实订单 POST 的确认时间**。新版 Sniper 因此另外被动记录生产路径，不会为了测速主动下单：

```text
本机收到 Poly WS 价格事件
  → signal_to_lock
  → trigger persistence / optional settle
  → OKX book
  → positions
  → balance
  → notification queue
  → pre-order persistence
  → sign + order POST
  → OKX acknowledgement
```

Sniper 面板显示最后一次各分段、`Trigger→Ack`，以及最近窗口的 p50/min/max/jitter、成功订单响应数、失败率、实测 Region 和当时的 canonical 节点名。每条 trace 还记录稳定 proxy ID；rolling 只聚合同一 Region、同一 proxy route，切换线路后不会把旧线路样本混进新线路。本地 `TIMEOUT`/`ERROR` 不会伪装成 OKX acknowledgement；触发后在下单前被风控释放也单独计数。这里的起点是 Bot **收到** Poly 消息的本机单调时钟，无法凭空推算消息在 Poly 服务端产生后、到达 Bot 前已经消耗的网络时间。

### 15.2 节点名称规则

数据库和 API 内部继续使用已有稳定 ID，避免破坏已保存账户；所有用户界面统一显示 `国家-城市-序号 | 城市 | 路由`：

| 稳定 ID | 统一显示名 |
| --- | --- |
| `JP1`…`JP5` | `JP-TYO-01`…`JP-TYO-05` |
| `OSAKA` | `JP-OSA-01` |
| `SG1` | `SG-SIN-01` |
| `K1` | `KR-SEL-01` |
| `SYD1` / `SYD2` | `AU-SYD-01` / `AU-SYD-02` |

新节点名必须使用 `CC-CITY-NN`，例如 `JP-TYO-06`。`direct`、旧式 `socks5` 和新式 `tunnel` 是路由类型，不再混进节点名。内置旧节点仍保留以兼容现有账户；新节点只允许 Direct 或绑定 `127.0.0.1` 的 SSH tunnel，不允许通过注册工具新增公网裸 SOCKS5。

本机扩展写入 `data/proxy_nodes.json`，升级代码时不要覆盖它。服务在启动时读取 registry，所以注册或替换节点后必须重启这一套单进程 FastAPI 服务。先检查有效配置：

```bash
python3 scripts/proxy_nodes.py list
```

registry 损坏时 CLI 会拒绝覆盖；后端则记录错误并保留可用内置节点。canonical 名称、Direct route 和本机 tunnel port 都不能重复。

### 15.3 新 Ubuntu 出口 VPS 的统一安装流程

出口 VPS 和 Bot 主机是两台机器，所以安全安装分为两个脚本。示例增加 `JP-TYO-06`，Bot 本机 tunnel 使用 `9116`：

1. 在 **Bot 主机**生成这个节点专用、无 passphrase 的 Ed25519 key，并通过 VPS 控制台等可信通道把 `.pub` 放到出口 VPS：

   ```bash
   sudo ssh-keygen -t ed25519 -N '' -f /root/JP-TYO-06 -C okx-JP-TYO-06
   ```

2. 在**出口 VPS 控制台**运行 bootstrap；`--bot-cidr` 只允许 Bot 公网 IP 登录：

   ```bash
   sudo bash ./scripts/bootstrap-egress-node.sh \
     --node-id JP-TYO-06 \
     --bot-cidr 203.0.113.10/32 \
     --public-key-file /root/JP-TYO-06.pub
   ```

3. 仍从 VPS 控制台读取 `/etc/ssh/ssh_host_ed25519_key.pub` 和脚本打印的 `SHA256:...` host fingerprint，通过可信通道分别保存到 Bot 主机。不要在第一次网络连接时盲目接受 host key。

4. 在 **Bot 主机**安装 systemd 持久 tunnel：

   ```bash
   sudo bash ./scripts/install-egress-tunnel.sh \
     --node-id JP-TYO-06 \
     --host 203.0.113.20 \
     --region AU \
     --local-port 9116 \
     --private-key /root/JP-TYO-06 \
     --host-key-file /root/JP-TYO-06.host-ed25519.pub \
     --host-key-fingerprint SHA256:REPLACE_WITH_CONSOLE_VALUE
   ```

5. tunnel 通过对应 Region 的 OKX 连通检查后，在 Bot 项目目录注册并重启 Bot 服务：

   ```bash
   python3 scripts/proxy_nodes.py add \
     --id JP-TYO-06 \
     --location Tokyo \
     --port 9116

   python3 scripts/proxy_nodes.py list
   ```

脚本只让 SOCKS 监听 `127.0.0.1`，远端账户只允许 key auth 和指定的 OKX 443/8443 转发。云安全组还应把 SSH 入站限制到 Bot IP。不要开放公网 `9111`，也不要把专用私钥复制进项目、ZIP 或 Git。

节点替换必须显式加 `--replace`。先用 VPS 控制台确认新 host key 和连接信息，再替换；脚本不会在信息不完整时自动信任新主机。

### 15.4 怎样选“最好”的节点

不能只凭城市或一次 ping 下结论。当前代理只改变 **Bot → OKX** 路径，Poly 仍从 Bot Direct 访问。建议按这个顺序选择：

1. 对账户真实 Region 分开测 AU/CN，不跨 endpoint 比数字；
2. 有真实市场时带 asset/token，先剔除 DNS、TLS、proxy、HTTP 失败和成功率不稳定的节点；
3. 比较 warm median、max 和 jitter，不用 cold 值或简单相加的 `total_ms` 排名；
4. 最终以自然发生的 Sniper `Trigger→Ack` rolling 数据和失败率决定，因为它包含下单前查询、持久化、签名和真实 POST；
5. 至少跨高峰/低峰多个时段，最好观察 24 小时，再把账户固定到节点。

命令行可先做同条件比较：

```bash
python3 scripts/proxy_nodes.py benchmark \
  --region AU \
  --samples 5 \
  --yes-asset-id REAL_OKX_YES_ASSET_ID \
  --poly-token-id REAL_POLY_TOKEN_ID \
  --account-id LOCAL_ACCOUNT_ID
```

如果当前 Bot 主机确实位于 Tokyo，先把 `JP-TYO-02` Direct 当基线，再测 Tokyo 同城不同供应商；同城通常少一段 Bot→出口 RTT，但并不保证 OKX 的运营商路由最好。Seoul、Singapore、Sydney 只在同 Region、同时间窗口的 warm 和真实 order acknowledgement 数据更好时替换。没有从实际部署主机取得这些数据前，不能诚实地宣布某个现有 IP 永远最快。

## 16. 部署在根路径

配置：

```yaml
server:
  base_path: ""
```

启动：

```bash
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8899
```

这里故意不使用多 worker；任何部署方式都必须保证同一账户数据目录只有一个 bot 进程。

反向代理到根路径时，要同时支持普通 HTTP 和 WebSocket Upgrade。

## 17. 部署在 `https://xxxx.com/okx`

`base_path` 只填写 URL path，不填写域名：

```yaml
server:
  base_path: "/okx"
```

也可以在启动前设置环境变量，环境变量优先：

Windows PowerShell：

```powershell
$env:OKX_BASE_PATH = "/okx"
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8899
```

Linux：

```bash
export OKX_BASE_PATH=/okx
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8899
```

这里同样必须保持单 worker、单实例。

推荐 Nginx 使用“剥离前缀”配置：

```nginx
location = /okx {
    return 301 /okx/;
}

location /okx/ {
    proxy_pass http://127.0.0.1:8899/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /okx;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```

关键点：

- `location /okx/` 和 `proxy_pass .../` 尾部斜线决定 Nginx 会剥离 `/okx`；
- 页面 API URL、登录跳转和 WebSocket URL 都会使用同一个 base path；
- middleware 也兼容上游仍保留 `/okx/...` 的代理方式；
- WebSocket 没有 `Upgrade`/`Connection` 头时，页面可能能打开，但订单簿会持续断线；
- 外部访问地址是 `https://xxxx.com/okx/`，不要把 `xxxx.com` 写进 `base_path`。

如果页面可以打开但 API 404，依次检查：

1. `server.base_path` 或 `OKX_BASE_PATH` 是否为同一个值；
2. Nginx `proxy_pass` 是否带尾部斜线；
3. 浏览器 Network 中请求是否为 `/okx/api/...`；
4. WebSocket 是否连接 `/okx/ws/book/...`；
5. 上游收到的是剥离前缀后的 `/...`，还是保留前缀的 `/okx/...`。

## 18. 测试与静态检查

测试使用 Python 标准库 `unittest`，不需要额外安装 pytest：

```powershell
python -m unittest discover -s tests -v
```

本次本地审查结果为 `213 tests OK`；其中前端测试会提取各 HTML 的 inline JavaScript 并交给 Node.js 做语法检查。

Python 语法检查：

```powershell
python -m compileall backend scripts tests
```

Git whitespace 检查：

```powershell
git diff --check
```

部分测试可能因本机未安装可选环境或无法访问外部服务而 skip；任何涉及精度、Sniper 锁、恢复或 base path 的失败都不应在实盘前忽略。

## 19. 常见故障排查

### 页面上的 `0.1¢` 显示为 0

检查：

- 是否把 cent 输入错误地当成 `0..1` 原价再次除以 100；
- 是否经过了 `int(...)`；
- 是否使用二进制 float 后再按整数格式显示；
- OKX 原始发送值是否为 `0.001`；
- 当前代码路径是否统一调用 `price_utils.py`。

### OKX manual 买卖发不出去

优先检查：

- 价格是否符合 4–96¢ 的 1¢ tick，或区间外的 0.1¢ tick；
- BUY 名义额是否达到 `1 xp`；
- 买入 share 是否为整数；
- SELL 是否超过真实 available size；
- NO 是否使用正确 `noAssetId`，同时用 YES book 镜像报价；
- API 返回超时后，订单是否其实已经进入 open/closed 列表；
- 服务器时间是否准确，代理是否稳定，签名凭据是否对应同一账户。

### Poly Monitor 看起来选错 NO 价格

- Manual-D 必须是真实 Poly YES；
- Manual-S/Bid-Sell 的 NO 必须是真实 Poly NO；
- Poly side 不做 `100 - YES`；
- 只有 OKX `pm-books` 的 NO 报价使用 YES 簿镜像。

### Sniper 重启后显示 locked

这是保护状态，不应直接清库或再次启动。核对：

1. active target 的 entry/exit `clientOrderId`；
2. OKX open orders；
3. OKX closed orders 和实际 filled size；
4. 该 asset 当前 position；
5. `position_before` baseline；
6. 后端恢复日志。

只有确认没有 Sniper 可归属的剩余仓位和未决订单后，才能解除风险。

### Order Book 不更新

- 检查 Nginx WebSocket Upgrade；
- 检查 `/okx` 是否同时作用于 HTTP 和 WS；
- 检查 OKX business WS 是否可经该账户代理访问；
- 查看是否出现 update-before-snapshot 或 seq gap 重连；
- Poly token 和 OKX yesAssetId 是否被混用。

### Monitor 触发后还有仓位

Cancel All 只撤单，不会撤销已经发生的 fill。Auto-Rebalance 会尝试捕获 partial fill 并对账持仓，但仍须人工检查。`Flash` 的 IOC 也可能只成交一部分。

## 19. 遗留代码与生产边界

### `poly_sniper_old.py`

旧版会从一对 YES/NO 配置自动同时监控两边，缺少新版显式多候选全局锁和完整持久恢复。保留它是为了兼容和行为对照，不代表推荐继续使用。多赛事、多 outcome 和恢复相关功能只应维护在 `poly_sniper.py`。

### `.bak-*`

这些是历史人工快照，Python 不会 import 它们。它们可能包含已经修复的逻辑、旧 UI、旧配置或过期凭据，不能当成当前实现说明。

### `frontend/polytest.html`

这是旧独立诊断页面，`app.py` 没有提供对应路由，而且页面含历史硬编码信息。生产部署时不要额外暴露它。

### `patch_fix.py`

这是一次性旧补丁。当前启动和测试都不调用它；再次执行会对源码做文本替换，可能破坏已经更新的实现。

### `layers` 和 `scripts/`

`layers` 是空占位，不是运行入口。`scripts/` 现在包含出口 VPS bootstrap、Bot 主机 SSH tunnel 安装和节点 registry/benchmark 工具；应用不会自动执行它们，但新节点接入和生产排障需要保留，使用方法见第 15 章。

### `auto-push.sh`

该脚本会自动 `git add`、`commit` 和 `push`。它不属于交易服务，开发或审查期间不要误启动。

### `ss.txt`

这是本地 OKX Outcome API 文档副本，用于对照协议；程序不读取它。正式维护时应以当前官方文档和实际 API 响应为最终依据。

## 20. 上线前检查清单

- [ ] 使用独立、小资金 OKX Outcomes 账户；
- [ ] 修改默认 Web 登录密码并启用 HTTPS；
- [ ] 确认只有一个 bot 进程，Uvicorn 没有启用多 worker，也没有第二实例共享 `data/`；
- [ ] 确认服务器时间同步；
- [ ] 验证每个账户的代理和 API heartbeat；
- [ ] 用 4¢、96¢、3.9¢、96.1¢、0.1¢ 验证显示和下单格式；
- [ ] 分别验证真实 Poly YES 和真实 Poly NO token；
- [ ] 验证 Manual-S NO 的 OKX 镜像 bid/ask；
- [ ] 用小单演练 partial fill、Stop、Cancel All 和 Flash；
- [ ] 配置多个 Sniper candidates，确认第一条触发后其他候选被锁；
- [ ] 在 holding 状态刷新页面并重启服务，确认恢复和 position 对账；
- [ ] 验证 `/okx/api/...` 和 `/okx/ws/...` 都能通过反向代理；
- [ ] 一起备份 `data/accounts.db`、`data/.key` 和策略 JSON 状态；
- [ ] 检查 OKX Open Orders、Closed Orders、Positions 与本地日志一致。

本项目没有任何“返回成功就一定成交”或“进程停止就一定安全”的假设。所有自动化操作最终都应以 OKX 真实订单终态和真实 position 为准。
