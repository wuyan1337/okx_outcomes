# 策略执行逻辑与运营说明

本文按当前代码实际行为说明每一个交易入口。它不是概念介绍，而是部署、实盘演练和故障排查时使用的执行顺序说明。

项目中的 `c`、`cent`、`¢` 都表示预测市场价格的百分点单位。例如 `55¢` 对应接口原始价格 `0.55`；这里不讨论美元换算。

## 1. 先理解价格、YES/NO 和全局安全规则

### 1.1 价格精度

实现入口：`backend/core/price_utils.py` 中的 `okx_tick()`、`align_okx_price()`、`format_okx_price()`、`format_poly_cents()`。

1. Polymarket 接口返回 `0..1` 的原始价格，页面统一乘以 100，并显示两位 cent，例如 `0.5432 → 54.32¢`。
2. OKX 在 `4¢..96¢` 范围内使用 `1¢` tick，因此显示为 `54¢`、`55¢`。
3. OKX 在低于 `4¢` 或高于 `96¢` 的范围使用 `0.1¢` tick，因此显示为 `0.1¢`、`3.9¢`、`96.1¢`。`0.1¢` 发给 OKX 的原始价格是 `0.001`，不能显示或提交成 `0`。
4. BUY 价格向下对齐到可执行 tick，避免实际买价高于用户限制；SELL 价格向上对齐，避免实际卖价低于用户要求。
5. Poly Monitor 的风险比较使用“最接近 tick、正中间向上”的 `ROUND_HALF_UP`，不是一律向下取整。原始 Poly 价仍完整写入行情日志，只有风险比较值会对齐。

### 1.2 YES 和 NO 的真实含义

1. Polymarket 的 YES 与 NO 是两个真实 token。Monitor 或 Sniper 选择 NO 时，必须使用真实 Poly NO token，不能拿 `1 - YES` 冒充 Poly NO 行情。
2. OKX `pm-books` 读取使用 YES asset 的订单簿。
3. 交易 OKX NO 时，程序从 YES 簿构造可交易的 NO 视图：`NO bid = 1 - YES ask`，`NO ask = 1 - YES bid`。
4. 构造 NO 盘口只用于读价；真正下 NO 订单仍使用该市场真实的 NO asset ID。
5. 每次策略配置都必须保存 `marketId`、YES asset ID、NO asset ID，以及所选真实 Poly token，不能只靠页面名称猜对应关系。

### 1.3 所有策略共用的安全门

实现入口：`backend/app.py` 的 `_serialize_strategy_lifecycle()`、`_entry_safety_response()`、`_automated_trading_active()`；`backend/services/account_manager.py` 的 `AccountInstance.reconcile_account_safety()`。

1. 同一账户的 Start、Stop、Cancel All、Ask Sell、Flash 等关键动作通过账户生命周期锁串行执行，避免两个浏览器请求同时改变状态。
2. Manual-D、Manual-S/Bid-Sell、新 Sniper、Sniper Old、Monitor/Auto-Rebalance 互斥；已有策略或风险恢复流程未结束时，新策略不能启动。
3. Sniper 未决锁或 Auto-Rebalance recovery journal 时，相关策略会拒绝不安全的新入口。
4. 减少风险的动作仍保留：撤单、reduce-only SELL、Flash Close 和明确的 recovery reconcile。
5. 查询订单、仓位或关键状态写盘失败时，系统按“无法证明安全”处理，不会把未知状态当成成功。

## 2. Manual-D：同一二元市场双边循环

用途：同时在同一 OKX 市场挂 YES BUY 和 NO BUY；任一 BUY 成交后，为该成交量挂对应的保护 SELL，然后继续用可用余额部署下一组 BUY。

实现入口：

- API：`backend/app.py` 的 `account_manual_start_v2()`、`account_manual_stop_v2()`。
- 引擎：`backend/services/manual_strategy_d.py` 的 `ManualStrategyD`。
- 主要方法：`start()`、`_seed_then_run()`、`_main_loop()`、`_deploy_balance()`、`_check_fills_post_sells()`、`_check_sell_fills()`、`stop()`。
- 订单确认：`backend/services/order_lifecycle.py`。

### 2.1 输入和价格关系

1. 用户选择一个 OKX 二元市场，程序取得该市场的 `marketId`、YES asset ID 和 NO asset ID。
2. 用户输入 YES 买价目标 `a`，单位是 cent；原始双边目标关系为 `YES buy≈a`、`NO buy≈99-a`。
3. 程序先分别得到并向下对齐两个真实 executable BUY；不能把未对齐的理论价格直接当成 OKX 挂价。
4. 每边的真实 SELL 都从“该边已经对齐的真实 BUY + 1¢”计算，再按 SELL 方向向上对齐。一般区间等价于 `YES sell≈a+1`、`NO sell≈100-a`，跨越 4¢/96¢ tick 边界时以真实 BUY 为准。
5. 例如 `a = 55¢`：YES `B55/S56`，NO `B44/S45`。边界例子 `a = 3.9¢`：YES `B3.9/S5`，NO `B95/S96`；页面、状态、下单和 Rebalance 必须显示同一组实际价格。

### 2.2 启动顺序

1. API 先检查账户 recovery、其他策略、Sniper 锁和 Auto-Rebalance 状态；任一未安全结束则拒绝启动。
2. 后端验证 YES/NO asset 完整，验证输入价可对齐为真实 OKX 可执行价，并先测试 OKX 余额接口可用。
3. 如果启用 Poly Monitor，必须选择真实 Poly YES token；启动前检查 OKX YES best bid 与策略 YES buy 一致，并检查 Poly best bid 映射后与监控参考价一致。
4. `ManualStrategyD.start()` 保存本次 `run_id`、市场、四个价格、Position %、扫描间隔和稳定观察时间。
5. 状态先写入 `data/pending_sells_d_{account_id}.json`；状态文件无法读取或写入时禁止继续部署新 BUY。

### 2.3 部署 BUY 的顺序

1. `_seed_then_run()` 先读取已有 open/closed orders，建立本轮开始前的成交水位，避免把旧成交重新处理。
2. `_deploy_balance()` 查询 points 可用余额，并统计该市场 YES/NO outcome 上全部开放 BUY 已锁定的金额；这是账户资金上限的保守计算，但 fill 与撤单归属仍只认本策略自己的 `clientOrderId`。
3. 可部署上限为“可用余额 + 本策略已锁资金”的 `Position %`；超出上限时不继续挂新单。
4. 双边使用相同 share 数；share 数同时受 YES/NO 两边预算、深度和 OKX 每边至少 `1 xp` BUY 名义额约束。
5. 每个 BUY 在发送前生成并持久化 `clientOrderId`，发送后按该 ID 核对真实订单状态；不会把同 asset 的其他人工订单认作本策略订单。
6. 双边部署不是把两个 POST 当成天然原子操作：先发一边、再发另一边；若第二边失败，程序只撤本轮第一边并确认终态。
7. 如果回滚无法确认，或回滚后 YES/NO 的最终累计 fill 数量不相等，设置 `deployment_blocked`，停止部署新的 BUY；两边恰好等量成交不算 one-sided imbalance。主循环继续管理所有已确认成交和保护 SELL。

### 2.4 成交、保护 SELL 和再部署

1. `_main_loop()` 按配置间隔读取 closed orders，同时读取带 partial fill 的 open orders。
2. `_check_fills_post_sells()` 只匹配本策略持久化的 `clientOrderId`，计算“本次累计成交 - 已处理成交”的增量。
3. 每一笔新增 YES fill 挂 YES protective SELL；每一笔新增 NO fill 挂 NO protective SELL。SELL 价来自原 BUY 记录，不会因后来修改页面价格而改变旧仓的退出价。
4. 保护 SELL 使用真实 outcome asset、SELL 向上对齐、reduce-only GTC；只有确认订单已经进入可接受状态后，才推进该 BUY 的已保护数量。
5. SELL 提交失败时不把该 fill 标记为已保护；按约 `2/4/8/16/32/60` 秒退避重试，并明确记录裸仓风险。
6. 低于 `1 xp` 的 SELL 只有在重新查询仓位后证明“本次卖出量 = availableSize = 总 size”时，才使用 OKX sell-all 例外。
7. `_check_sell_fills()` 计算 SELL 的增量成交，更新统计和 Auto-Rebalance 成本层；随后 `_deploy_balance()` 再检查是否有余额可以部署下一组 BUY。

### 2.5 Stop、刷新、重启和失败锁

1. Stop 先停止新部署和扫描循环。
2. Stop 只处理 Manual-D 自己仍未完成的入场 BUY，并按自己的 `clientOrderId` 撤销、核对；不会执行账户级 Cancel All。
3. 撤 BUY 后若终态显示新增 fill，Stop 会先补齐缺失的保护 SELL；已经确认的保护 SELL 保留在 OKX，Stop 不主动平仓。
4. 页面刷新不会改变后端正在运行的任务；但服务进程重启后，Manual-D 不会像新版 Sniper 一样自动续跑。
5. `pending_sells`、订单归属和已处理成交水位写入账户专属 JSON，重启后作为订单归属与已处理水位证据；它不会自动授权新 entry。交易所状态探测只记录观察结果，不会锁住账户。
6. 状态损坏、订单终态未知或写盘失败时保持 `deployment_blocked`。Stop 无法完成安全核验时 API 返回 409。

## 3. Manual-S：只交易一个真实 outcome

用途：只选择 YES 或 NO 的一边循环挂 BUY；BUY 成交后挂同一真实 outcome 的保护 SELL。

实现入口：

- API：`backend/app.py` 的 `account_manual_s_start()`、`account_manual_s_stop()`。
- 引擎：`backend/services/manual_strategy_s.py` 的 `ManualStrategyS`。
- 主要方法：`start()`、`_seed_then_run()`、`_main_loop()`、`_deploy_balance()`、`_check_fills_post_sells()`、`_check_sell_fills()`、`stop()`。

### 3.1 启动和选边

1. 用户选择 OKX 市场，再明确选择 `YES` 或 `NO`。
2. 选择 YES 时，交易 YES asset；选择 NO 时，交易真实 NO asset，读盘口时才从 YES book 镜像 NO bid/ask。
3. classic 模式输入 BUY 价；默认 SELL 目标为 BUY `+1¢`，也可由调用方显式提供合法 SELL 价。
4. BUY 向下对齐，SELL 向上对齐；后端拒绝无效 side、无效 mode、缺失 NO asset 或不可执行价格。
5. 如果绑定 Monitor，YES 策略必须选择真实 Poly YES token，NO 策略必须选择真实 Poly NO token；Poly NO 不从 YES 反算。

### 3.2 运行循环

1. 启动前执行账户安全门和策略互斥检查，再保存市场、选边、买卖价、Position %、扫描间隔及 `run_id`。
2. 读取历史 open/closed orders 建立成交水位，只跟踪本策略持久化的 `clientOrderId`。
3. 根据可用余额、所选 outcome 上开放 BUY 占用资金、Position % 和 `1 xp` 最小 BUY 名义额计算整数 share 数；订单归属与 fill 处理仍只认本策略自己的 `clientOrderId`。
4. 发送 BUY 前持久化订单归属；发送后确认真实状态。未知状态不继续假设“没下出去”。
5. 扫描 partial/full fill 的增量，为新增成交挂同一 asset 的 reduce-only GTC SELL，并确认 SELL 状态。
6. SELL 失败按退避计划重试；低于 `1 xp` 时仅允许经过仓位核验的 sell-all 例外。
7. SELL 成交后更新成本层和统计，再用释放出的余额部署下一笔 BUY。

### 3.3 Stop 和恢复

1. Stop 停止扫描和新 BUY。
2. 只撤销并确认本策略未完成的入场 BUY；不撤其他策略或人工订单。
3. 撤 BUY 后若发现新增 fill，会先补齐缺失保护；已确认保护 SELL 保留，Stop 不主动平仓。
4. `data/pending_sells_s_{account_id}.json` 保存 pending fill、订单归属和处理水位；页面刷新保留后端状态，服务重启不自动恢复 running，也不自动授权新 entry。
5. 启动安全检查发现交易所仍有订单/仓位时只记录状态；JSON ledger 是归属证据，不是绕过账户对账的开关。
6. 状态文件失败、订单结果未知或连续操作失败会阻止新部署，现有已知成交仍优先进入保护处理；Stop 无法完成安全核验时 API 返回 409。

## 4. Bid-Sell：以 SELL 价为输入的 Manual-S

用途：复用 Manual-S 单边引擎，但用户先指定目标 SELL，系统把 BUY 固定在它下方 `1¢`。

实现入口：前端 `frontend/dashboard.html` 的 `tgBS()`；API 仍是 `account_manual_s_start()`；引擎仍是 `ManualStrategyS`，`mode="bid-sell"`。

1. 用户选择市场、YES/NO side，并输入目标 SELL 价。
2. 前端先计算 `BUY = SELL - 1¢`，后端再次独立校验，不能只信任浏览器。
3. BUY、SELL 分别按各自方向对齐后，两个真实可执行价格必须仍然正好相差 `1¢`；跨 tick 区间后不满足时拒绝启动。
4. 下单、fill 增量、保护 SELL、sell-all 例外、Stop 和 JSON 持久化全部与 Manual-S 相同。
5. 与 classic Manual-S 的关键差异是 Monitor 参考价：classic 参考 BUY，Bid-Sell 参考 SELL；Auto-Rebalance 稳定后也保持原 spread，再反推新 BUY。

## 5. Poly Monitor：Manual 的外部价格风控

用途：持续观察某一个真实 Poly outcome 的 best bid；当它相对策略参考价发生有效变化时，停止当前入场逻辑并进入撤单/恢复流程。

实现入口：

- API：`backend/app.py` 的 `account_monitor_start()`、`account_monitor_stop()`。
- 引擎：`backend/services/poly_monitor.py` 的 `PolyMonitor`。
- 主要方法：`start()`、`_monitor_loop()`、`_rest_poll_loop()`、`_ws_listen_loop()`、`_risk_bid_price()`、`_check_price()`、`_trigger()`、`_fast_cancel_all()`。

### 5.1 启动与数据源

1. Monitor 必须绑定一个真实 Poly token；Manual-D 固定使用 Poly YES，Manual-S/Bid-Sell 必须使用与所交易 side 相同的真实 token。
2. reference 可以由策略传入；没有 reference 时，先从 Poly `/book` 取得当前 best bid。初始 bids 为空或请求失败时拒绝启动，不能把 `0` 当基准。
3. 启动后并行运行两条数据链：Poly REST 大约每 `1.5s` 拉一次完整 book，Poly WebSocket 接收即时 book/price_change。
4. WS 收到 `price_change` 后会再拉完整 REST book，避免只凭一个增量字段推断完整 best bid 与深度。
5. 每次完整观察都记录原始 bids/asks、接收墙钟、单调钟、来源和 best bid 到 `market_data.jsonl`。

### 5.2 风险判断

1. 原始 Poly best bid 与 reference 都由 `_risk_bid_price()` 按当前 OKX tick 做 `ROUND_HALF_UP` 对齐。
2. 若风险价相等，不触发。
3. 若当前风险价低于 reference，立即触发；下跌不要求深度门槛，因为原参考 bid 已经从最优买盘消失。
4. 若当前风险价高于 reference，统计新 best-bid 这一档所有 size，并计算 `best bid price × size`。
5. 上涨只有在该档名义深度不低于 `depth_threshold` 时触发；不足则记录 `monitor.signal_ignored_depth` 并继续观察。
6. 有效的空 bid 视为 best bid `0`，因此相对正 reference 是明确下跌；格式错误、HTTP 失败或缺少 `bids` 字段则是数据不可用，不当成价格信号。

### 5.3 触发后的严格顺序

1. `_schedule_trigger()` 为本次触发生成 `trace_id`，同一 Monitor generation 只允许一个触发任务。
2. 在停止 Manual 之前，先把 `monitor_strategy_stop/before`、recovery ID、触发原因、策略参数和 token 原子写入 recovery journal；写盘失败时仍尝试冻结 Manual，但绝不执行账户级 Cancel All。
3. 调用账户回调停止 Manual-D 和 Manual-S；两边的 `stop()` 都必须在 `5s` 内明确返回 `True`。任一返回 `False`、抛异常、超时或缺少回调，都会写 `monitor_strategy_stop/failed`，随后停止流程。
4. 只有 Manual stop 已确认安全，才写 `monitor_strategy_stop/after` 和 `monitor_cancel_all/before`；任一阶段写盘失败都不进入账户级撤单。
5. `_fast_cancel_all()` 撤该 OKX 账户的所有开放订单，并反复查询；只有成功查询且 open orders 为 0 才写 `monitor_cancel_all/after`，否则写 `failed`。
6. stop 失败时只写日志/journal 并停止；stop 已确认且 cancel 结果成功写盘后，Monitor 才根据撤单结果发送通知。撤单失败、查询失败或 recovery after 阶段写盘失败都不会启动 Auto-Rebalance。
7. 仅当 Auto-Rebalance 已启用、Manual stop 明确成功、账户 open orders 明确为 0，才进入 `AutoRebalance.execute()`。
8. 未启用 Auto-Rebalance 时，Monitor 保持 triggered/stopped，Manual 不会自行重启。

### 5.4 Stop 和失败状态

1. 用户 Stop 会停止 REST、WS 和未完成 trigger task，并取消正在运行的 Auto-Rebalance task。
2. 普通 Stop 本身不主动撤 OKX 订单；只有已经进入有效 trigger 流程时才执行账户级撤单。
3. 未完成的 recovery journal 不会因 Stop 自动清除，下一次 Monitor Start 与 Auto-Rebalance enable 都会被拒绝。
4. 状态接口会返回 `triggered`、reason、reference、last bid、depth value 和 recovery 状态。
5. 关键日志使用 `run_id + trace_id`，主要事件前缀为 `monitor.*`。

## 6. Auto-Rebalance：触发后的对账、保护和重启

用途：不是独立信号策略。它只在 Poly Monitor 已触发并且账户开关为 ON 时，重新核对成交与持仓、重挂保护 SELL，再恢复 Monitor 和原 Manual 策略。

实现入口：`backend/services/auto_rebalance.py` 的 `AutoRebalance`；主要方法 `set_enabled()`、`prepare_recovery()`、`execute()`、`_do_rebalance()`、`_wait_for_stable_price()`、`_plan_sell_orders()`、`_post_layer_sells()`、`_restart()`、`recover()`。

### 6.1 开关与持久化

1. `enabled` 按账户保存在 `data/cost_layers_{account_id}.json`，与成本层、捕获成交水位和 recovery journal 一起写盘。
2. 服务重启后恢复该开关；老文件缺字段、字段不是 boolean 或文件损坏时默认关闭。
3. 开关写盘失败会回滚内存值，API 返回真实状态；Manual-D、Manual-S、Bid-Sell 三处 UI 显示的是同一个账户级开关。
4. 恢复开关不等于恢复 Manual/Monitor running；没有新的 Monitor trigger，Auto-Rebalance 不会自行执行。
5. 存在 outstanding recovery 时不能打开开关，也不能启动新的 Monitor。

### 6.2 正常 Rebalance 顺序

1. 读取触发时固化的策略参数，确认 strategy type、marketId 和真实 outcome assets 没有变化。
2. 再次查询账户 open orders；若出现残留，按精确 order/asset 批量撤销并继续核验，直到成功查询且结果为 0。这里不会再次调用账户级 `cancelAll([])`。
3. 从 owned BUY orders 捕获触发期间出现的 partial/full fills，按 `marketId + assetId + buy cost` 加入成本层，并用捕获水位防止以后重复计入。
4. 查询真实 positions，对 YES/NO 各自同步；Manual-D 两边永远是两个 asset 层，不混成一个总仓位。
5. 每 `5s` 采样一次 Poly best bid；需要按配置观察期得到连续稳定样本，样本之间允许的最大差为原始价格 `0.005`，最多尝试 36 次。
6. 稳定 Poly bid 映射为新的可执行参考价；新的 BUY 参考按买单方向保守向下对齐。
7. 再次按真实 positions 同步成本层，因为旧保护 SELL 已被撤掉，即使价格没变也必须重新保护现有仓位。
8. `_plan_sell_orders()` 为每层计算 `max(该层成本, 新目标 SELL)`，再向上对齐，确保不在记录成本以下卖出。
9. 小于 `1 xp` 的相邻层会合并；若只剩整仓的小额数量，可使用 reduce-only sell-all 例外。
10. 每笔 protective SELL 先生成 `clientOrderId`，再发 reduce-only GTC；随后轮询 open/closed orders，只有 `ACTIVE` 或 `FILLED` 才算确认。
11. 任一保护 SELL 变成 `PARTIALLY_FILLED/FAILED/PLACE_FAILED/CANCELLED/EXPIRED`，或在期限内无法确认，都使本次 Rebalance 失败并保留失败详情。
12. 所有持仓有保护后，先重启 Monitor，再重启原 Manual-D、Manual-S 或 Bid-Sell；Manual 重启前后都会检查 `deployment_blocked/block_reason`，只有未锁定、进入 running 且 entry barrier 成功释放才算成功。重启或 journal 失败后的回滚中，Manual stop 明确返回 `False` 也算清理失败，必须记录并保留 recovery，不能写成安全完成。
13. 最后才清 recovery journal。journal 清除失败时也停止新入场，不能把“交易动作完成但状态没写盘”报告为完成。

### 6.3 中断恢复

1. 每个有副作用的阶段都在执行前后写 `before/after/failed`，并使用同一个 `rebalance_id`。
2. 进程崩溃、任务取消、REST 未确认或写盘失败时，`recovery.required` 保留。
3. `recover()` 先调用严格 stop：Manual stop 必须明确返回 `True`，否则不进入撤单；随后停止 Monitor，再撤单核验、捕获 fills、对账 positions、同步成本层并重挂确认 protective SELL。
4. 恢复流程只恢复保护，不自动恢复新的 BUY entry，也不自动重启 Manual/Monitor。
5. 只有保护完成且 journal 清除成功后，Auto-Rebalance 自己的 recovery-required 状态才解除；账户安全探测只记录观察结果，不会锁住账户。
6. 主要结构化日志前缀为 `rebalance.*`，同一次流程用 `rebalance_id` 串联。

## 7. 新版 Poly Sniper：多候选监听、单仓全局锁

用途：监听多个用户明确配置的 Poly outcome；某一候选在短时间内快速上涨时，尝试在 OKX 盘口尚未追上前买入映射的 YES 或 NO，短暂持有后退出。

实现入口：

- API：`backend/app.py` 的 `sniper_start()`、`sniper_stop()`、`sniper_status()`。
- 引擎：`backend/services/poly_sniper.py` 的 `PolySniper`。
- Dashboard：`frontend/dashboard.html` 的 `viewSniperTarget()`、`syncSniperBookView()`、`syncSniperBookBinding()`。
- 主要方法：`start()`、`_process_bid()`、`_try_trigger()`、`_run_trigger()`、`_execute_entry()`、`_holding_loop()`、`_exit_position()`、`stop()`、`restore()`。

### 7.1 每一行 Target 表示什么

1. 每一行明确选择一个真实 Poly token，例如 Argentina YES、Australia NO 或 Draw YES。
2. 同一行再选择一个 OKX market 和要交易的 OKX YES/NO side。
3. 后端保存 `poly_token_id → marketId + yes_asset_id + no_asset_id + side` 的完整映射。
4. 同一个 Poly token 不能在两行重复；缺 market、缺 side asset、ID 重复或 budget 非正数都会拒绝启动。
5. 新版不会根据一个市场自动派生“同时狙击 YES 和 NO”；要监听哪个 outcome，必须单独加一行。
6. Dashboard 每行的 `View Book` 只选择当前显示的 Poly + OKX pair，不改变 target 配置、监听 token、active target 或交易判断。
7. Dashboard 只有一个 viewer WebSocket；`backend/ws_book.py` 只转发当前查看 pair，真实多 target 监听由 `PolySniper` 自己的一条 Poly WebSocket 完成。
8. monitoring 时可以查看任意 target；active target 出现后 viewer 每次状态同步都固定 active 行，其他 `View Book` 禁用。
9. 同一页面会话内完全退出后恢复触发前查看行；浏览器刷新后 active 仍从后端恢复，但纯前端查看偏好不写入 Sniper SQLite ledger。
10. Sniper idle 时不会从 Manual、Bid-Sell 或 Sniper Old 抢共享 Order Book；Sniper running 时其他页面的市场选择也不能覆盖 Sniper viewer。

### 7.2 行情检测顺序

1. 启动后用一个 Poly WebSocket 订阅所有 target token，每个 target 有独立的 last bid、rolling window 和校准状态。
2. 空 bids 会清空该 target 的校准和窗口；重新出现的第一帧只用于校准，不触发下单。
3. 后续每帧把当前 bid 加入 `window_ms` 滚动窗口，计算 `window_low` 和 `current_bid - window_low`。
4. 变化乘以 100 得到 cent jump；低于 `threshold_cents` 只记录 `below_threshold`，继续监听。
5. 达到阈值后执行 bounce filter：若当前价仍低于窗口高水位的 70%，判定为大跌后的噪音反弹，不下单。
6. 通过过滤的候选生成 `trace_id` 并尝试取得全局 trigger lock。
7. 判断使用原始 `Decimal` best bid，不先按 Dashboard 的两位 cent 显示做四舍五入。

### 7.3 多候选全局锁

1. 空闲时所有 target 同时监听。
2. 第一条有效信号取得 `_trigger_lock` 后，写入 `active_target_id`，Sniper 进入 locked/triggered。
3. Argentina 已触发、进入下单或持仓后，Australia、Draw 等其他 target 的非空报价只更新最新 `last_bid` 并记录 `observed_while_locked`；不更新 trigger window、不读取 OKX snapshot、不创建 trigger task，也不能入场。空盘口仍会立刻清空该 target 的校准、baseline 和 window。
4. 系统任何时刻只管理一个 Sniper 仓位，避免多个噪音信号同时消耗余额或互相破坏仓位基准。
5. 只有 active target 完全退出并完成 position 对账后，才清全局锁并恢复全部 target 的入场判断；恢复时清空所有旧 window，用锁定期间最新 `last_bid` 重新建立 baseline，避免锁内噪音立即触发。

### 7.4 触发到 BUY

1. 可选 `settle_ms`：Poly 价格必须连续不变达到该时长；settle 后若相对原 window low 已不足阈值，释放锁，不下单。`settle_ms=0` 时不会在 IOC 前再次读取 Poly；`settle_ms>0` 当前没有额外最大等待上限，等待期间保持全局锁。
2. settle 后读取新的 OKX REST depth=10 YES book；Dashboard viewer 和持续 trace WS 不参与下单决策。target 为 NO 时，从 YES bids 构造 NO asks。
3. 价格上限为 `min(当前 Poly bid, Buy Limit)`；只保留同时不高于上限、并且严格低于当前 Poly bid 的 OKX asks。
4. 如果没有合格 asks，释放锁，继续监听其他信号。
5. 下单前查询该 target 的真实 OKX position，保存 `position_before`；查询失败时不入场。
6. 查询 points 余额；使用 target 独立 budget，或 `余额 × Position %`，再受合格深度限制。
7. 规范化为可执行 share 数；少于 1 share 或 BUY 名义额低于 `1 xp` 时不入场。
8. 先生成 entry `clientOrderId`，把 entering 状态、active target、position baseline 和 cloid 写入 SQLite snapshot；写盘失败绝不发单。
9. 以最高合格 ask 作为限价，提交 IOC BUY。IOC 允许 partial fill，但未成交余量不会留在簿上。
10. API code=0 仍不直接当作完成；按 `clientOrderId` 查询 closed order，取得最终 filled size 和 filled amount。
11. 明确零成交时释放 trigger；有成交时进入 holding；TIMEOUT/ERROR 或终态无法确认时停止自动推进并保持全局锁，等待对账。

### 7.5 持仓和退出

1. holding 起点和买入均价先持久化。
2. `timeout_secs` 从 IOC 最终成交确认并且 holding 起点成功持久化后开始；settle、OKX snapshot、position/balance、下单和订单终态确认都不计入 timeout。
3. 持仓约满 `1s` 时读取一次真实 Poly target best bid；如果 OKX 买入均价高于 Poly bid，立即进入 stoploss exit。
4. 1 秒 stoploss 检查没有触发退出时，达到 `timeout_secs` 进入 timeout exit。
5. exit 查询当前 target 的真实 OKX best bid；NO target 从 YES asks 镜像得到 NO bid。
6. 每次退出先持久化新的 exit `clientOrderId`，再提交 reduce-only IOC SELL。
7. 最多进行 5 次退出尝试，每次按自己的 cloid 核对终态和增量成交；未知结果立即停止并锁住，不盲目重复卖出。
8. 订单账本显示已卖完后，还要查询真实 position；只有 position 回到 `position_before` 才算完全退出。
9. 完全退出后清 active target、买卖数量、订单 ID 和计时，解除全局锁，所有候选重新具备触发资格。

### 7.6 Stop、页面刷新和服务重启

1. Stop 停止接收新信号，并取消本进程的 trigger/holding 任务。
2. 若 entry 尚未完成，Stop 只撤 Sniper 自己的 entry cloid 并确认终态，不执行账户级 Cancel All。
3. Stop 不主动平掉已经存在的 Sniper 持仓；有剩余持仓、未决 entry/exit 或未知终态时保留 `locked`。
4. 运行配置、targets、active target、position baseline、entry/exit cloid、成交量和持仓起点保存在 SQLite `sniper_state`。
5. 页面刷新只重新读取状态，running 时配置输入锁定，不能边运行边修改。
6. `View Book` 是显示控件而不是交易配置；monitoring 时可切换，active 时强制固定，浏览器刷新不会改变后端 active/locked 状态。
7. 服务重启时先恢复 snapshot，再与 OKX open/closed orders 和 positions 对账；holding 使用持久化的 `hold_start_epoch_ms` 重建已持仓时间，timeout 不会从零重新计时；只有可证明安全才恢复 monitoring/holding。
8. snapshot 损坏、关键写盘失败或状态无法归属时保持 error/locked，不能启动新 Sniper 或其他增加敞口的策略。

### 7.7 Sniper 延迟日志

1. 每次 Poly 帧分配 `frame_id`，每个有效候选分配 `trace_id`，每次 Start 分配 `run_id`。
2. 生产路径记录 signal-to-lock、trigger persistence、settle、OKX book、positions、balance、notify queue、pre-order persistence、order submit 和 Trigger-to-Ack。
3. 同时记录 OKX REST/WS 盘口与 Poly 原始帧，便于按毫秒对齐“Poly 怎么跳、OKX 怎么跳、机器人在哪一步”。
4. 本地 TIMEOUT/ERROR 不算 OKX acknowledgement；未发单就释放的 trigger 单独标记。
5. 主要查询键是 `account_id + run_id + trace_id + frame_id + target_id + clientOrderId`。

## 8. Sniper Old：遗留兼容逻辑

用途：保留旧页面与旧操作习惯，不建议正式运营继续使用。

实现入口：`backend/services/poly_sniper_old.py` 的 `PolySniperOld`；API 为 `sniper_old_start()`、`sniper_old_stop()`。

1. 启动时固定接收一对 Poly YES/NO token，并同时监听这两个 token；YES 与 NO 共用一套单仓状态，不能像新版那样配置多条任意 outcome，也不能同时管理多个候选仓位。
2. 每个 token 维护 rolling window；窗口内使用原始 cent 变化与阈值比较，并执行 bounce filter，不会先 `round()` 再决定是否触发。
3. 任一边满足信号后锁定该边，快照 OKX ask 深度，生成唯一 `clientOrderId`，再按 `ask_levels`、可用余额和 Position % 提交 IOC BUY。
4. entry 提交后必须按自己的 `clientOrderId`/`orderId` 查询精确订单终态。TIMEOUT、无效响应或终态不明确时只撤自己的订单；仍无法确认就进入 `error/locked`，不会继续开新仓。
5. YES/NO 合成盘口、entry 和 exit 都使用 OKX 动态 tick：`4¢..96¢` 为 `1¢`，区间外为 `0.1¢`；BUY 向下、SELL 向上对齐，低价不会被格式化成 `0.00`。
6. settle 结束后，当前主流程按该 outcome 的 OKX best bid 提交 reduce-only IOC SELL；旧的 `_post_sell_at_price()` 不是当前默认退出路径。
7. 退出最多尝试 5 次；每次读取真实成交量并累计，只卖尚未退出的剩余数量。最后必须查询交易所 position 并确认该 asset 已归零。
8. Stop 先停止新信号，再对账未决 entry，撤销只属于本次 Old Sniper 的保护 SELL，然后对剩余仓位执行退出；无法证明订单终结且 position 为 0 时返回失败并保持锁定。
9. Sniper Old Stop 未完成时，API 返回 HTTP 409；明确的 reduce-only SELL 仍可用于减仓。
10. Sniper Old 的配置、running 标记、holding side、买卖数量、entry/exit `clientOrderId`、订单 ID、累计成交和 hold 起点写入 SQLite `sniper_old_state`；关键写盘失败会停止运行并进入 `error/locked`。
11. 服务重启采用两阶段恢复：先 `restore(..., resume=False)` 加载 ledger、逐个核对 entry/exit cloid 和真实 position，但不启动信号；第一阶段修正后的 ledger 必须先写回 SQLite，写盘失败立即 `error/locked`；然后才做账户级 open orders/positions 安全对账。
12. 只有账户安全对账通过、旧 snapshot 的 running 标记为真、且新版 Sniper 不需要恢复时，才执行 `restore(..., resume=True)`；有持仓则恢复 holding/timeout 管理，无持仓则恢复 monitoring。
13. snapshot JSON 损坏、市场/token 映射不完整、订单终态或 position 无法确认时，保留 SQLite 行并返回未完成状态，不能先恢复监听再补对账。

## 9. Manual Trade：一次性人工 BUY/SELL

用途：用户直接指定 asset、方向、cent 价格、share 数和 GTC/IOC，不运行循环策略。

实现入口：`backend/app.py` 的 `account_manual_place()`；页面逻辑在 `frontend/dashboard.html`。

1. 自动策略、Monitor/Rebalance、Sniper starting/running 或关键持久化失败时，人工下单被拒绝。
2. Sniper 或 Sniper Old 已停止但 locked 时禁止人工 BUY；reduce-only SELL 仍可用于减仓。
3. 输入 cent 先转换到 `0..1` 原始价格，再按 BUY 向下、SELL 向上对齐；`0.1¢` 必须变成 `0.001`，不会变成 0。
4. BUY 必须满足至少 `1 xp` 名义额。
5. SELL 先查询真实 position，不能超过 `availableSize`，并始终设置 `reduce_only=True`。
6. SELL 低于 `1 xp` 时，只有卖出全部可用仓位且没有冻结数量才允许使用 sell-all 例外。
7. 订单使用唯一 `clientOrderId` 发送并按该 ID 查询。提交结果未知时先尝试撤自己的订单并确认终态；仍无法确认时返回 HTTP 409，不能盲目重下。
8. IOC 必须进入明确终态；`FILLED` 或终态中 `filledSize > 0` 才算有成交，GTC 只有确认 `ACTIVE` 或成交后才返回成功。
9. Manual Trade 不自动补保护 SELL、不运行循环策略；用户仍应在 Active/Closed Orders 和 Positions 中检查最终状态。

## 10. Ask Sell：按真实 Poly ask1 重挂保护 SELL

用途：对账户当前每个 outcome position，读取该 outcome 的真实 Poly ask1，并在 OKX 挂同价方向的 reduce-only GTC SELL。

实现入口：`backend/app.py` 的 `account_ask_sell()`；映射由 `poly_asset_map` 提供。

1. 有仍在运行、可能继续提交订单的自动策略时拒绝执行。已经停止但保留 recovery/Sniper 锁时，Ask Sell 作为 reduce-only 减风险动作仍可使用；它不会因此自动清除恢复锁。
2. 查询并验证 OKX open positions 和 open orders。
3. 根据 position 的 marketId/tokenIndex 确定真实 OKX YES/NO asset，再从本地映射找到对应的真实 Poly token。
4. 没有 Poly 映射、Poly `/book` 失败、asks 为空或 ask1 无效时，该 position 被明确标记 skipped/failed，不猜价格。
5. 将 Poly ask1 按 SELL 方向对齐到 OKX tick。
6. 只撤该 asset 的已有 SELL，并重新查询确认这些 SELL 已消失；然后再次查询该 asset 的最新 position/`availableSize`，不能沿用撤单前仍被旧 SELL 冻结的数量。
7. 无法确认旧 SELL 已消失或无法取得最新可用仓位时不挂新单；新 protective SELL 使用 reduce-only GTC、唯一 `clientOrderId`，只有确认 `ACTIVE/FILLED` 才算保护成功。
8. 新单被拒绝或进入明确非保护终态时，先确认该新单已经安全终结；如果旧 SELL 曾存在，再按旧保护价格和最新 position 尝试恢复保护。新单状态仍未知时不会假装回滚完成。
9. 返回逐 asset 结果；任一 position skipped/failed 时整体 `ok=false`，结束前会重新运行账户状态探测。

## 11. Flash Close：账户级紧急退出

用途：停止账户全部本地交易活动，在拆除保护前先验证每个仓位存在可执行退出盘口，然后逐 asset 撤单并按当前 OKX best bid 用 reduce-only IOC 平仓。

实现入口：`backend/app.py` 的 `_stop_account_trading()`、`_cancel_open_orders_verified()`、`account_flash_close()`。

1. 停止 Auto-Rebalance、Manual-D、Manual-S/Bid-Sell、Monitor、新 Sniper 和 Sniper Old，并记录每个 stop 是否已安全完成。
2. 在撤任何保护单前，同时查询真实 open orders 和 open positions；为每个 position 预先解析 YES/NO asset 和可执行退出价。YES 用 YES best bid，NO 用 YES best ask 镜像出的 NO best bid。
3. 任一 position 缺 market/outcome、position size 无效、盘口请求失败或没有可执行 best bid 时立即失败；此时 `cancelled=0` 且保留现有保护单。
4. 预检全部通过后，逐 asset 只撤该 asset 的开放订单并确认终态；随后重新查询 position 和盘口，因为旧 SELL 撤掉后 `availableSize` 与 best bid 都可能变化。
5. 每个 asset 最多进行 5 次 reduce-only IOC SELL；每次生成唯一 `clientOrderId`、确认订单终态，再查询真实剩余 position。HTTP `code=0` 或 IOC 终态本身都不是空仓证明。
6. 若某 asset 退出不完整，在最终账户清理前尝试按被撤保护单恢复 confirmed reduce-only GTC SELL；仍有仓位时只撤非保护订单，并返回保护覆盖是否完整。
7. 只有 positions 已明确为 0，才执行最后的账户开放订单清理；之后最多轮询 10 次 open orders 和 open positions，两者都为 0 才进入本地 ledger 收尾。
8. 普通 Flash 和删除账户都要求 Manual、新 Sniper、Sniper Old 的本地 stop/reconcile 结果可证明安全；交易所已空但本地锁未解除时仍返回失败。
9. 任一订单未知、查询失败、仓位残留或保护恢复失败都会返回失败，并在结果中保留 submitted/cancelled/remaining/failures 等已确认进度。

### 11.1 删除账户

1. 删除 API 先在同一个账户生命周期锁内调用完整 Flash Close，不能先删数据库凭据。
2. 只有 Flash 确认交易所 open orders 与 open positions 都为 0，且要求的本地 Manual/Sniper/Sniper Old 收尾已完成，才调用 `AccountManager.remove_account()`。
3. Flash 清理或本地收尾任一步失败返回 HTTP 409，账户实例、SQLite 账户行和凭据继续保留，供后续对账和重试。
4. 删除获准后才关闭只读探测/client 并删除 `accounts` 行；SQLite 外键同时删除该账户的 `sniper_state` 和 `sniper_old_state`。数据库删除本身异常时接口返回 500，不会伪报成功。

## 12. Account Safety Probe：只读账户安全探测

用途：这是账户级安全任务，不是盈利策略。它周期性读取 OKX 真实开放订单和开放仓位，用于确认 API、代理和账户状态仍可被安全观察。

实现入口：`backend/services/account_manager.py` 的 `_account_read_safety_probe()`、`start_account_heartbeat()`、`_account_heartbeat_loop()`、`_stop_entry_activity_for_recovery()`。

1. 每个账户完成启动安全对账后，启动一个后台只读探测 task；普通 Manual Stop 不会关闭它。
2. 默认每 `120s` 查询一次完整 open orders 和 open positions；每轮最多尝试 3 次，失败重试间隔约 `2s`。
3. 探测成功时更新 `last_heartbeat_ok`，并记录开放订单数、开放仓位数和本轮耗时。
4. 探测失败时写入日志；未知交易所状态不会被当成空仓。
5. 后续探测恢复时执行完整账户安全对账并更新观察日志；它不会停止策略、锁定账户或阻止删除账户。
6. 当前运行链路不会调用 `backend/core/client.py::heartbeat()`，也不会安装“5 分钟后自动 Cancel All”的 OKX dead-man。原因是该签名到期会撤掉包括保护 SELL 在内的全部订单，与持仓保护逻辑冲突。
7. 从曾经启用 dead-man 的旧版本升级时，旧进程最后发送的签名仍可能在约 5 分钟内到期并撤单。首次升级必须先确认空仓且无挂单，停止旧服务，并等待最后一份旧签名失效后再部署和启动新版。

## 13. 启动安全观察与人工 Reconcile

实现入口：`AccountManager._load_account()`、`AccountManager._finalize_account_load()`、`AccountInstance.reconcile_account_safety()`；API 为：


1. 服务启动先加载账户和解密 client，再读取新版 `sniper_state`；存在 snapshot 时先调用 `restore(..., resume=False)` 恢复配置、订单归属和锁，但暂不恢复信号监听。
2. 随后读取 `sniper_old_state`；有效 snapshot 同样先调用 `restore(..., resume=False)`，逐个核对旧 Sniper 的 entry/exit cloid 和真实 position，不启动监听或新的 BUY。第一阶段清理后的账本先写回 SQLite，并保留原 running 标记。
3. 接着查询完整 OKX open orders 与 open positions，并识别哪些状态能由已恢复的新版或旧版 Sniper ledger 解释。
4. 只有账户安全对账通过，且对应 snapshot 在关闭前确实 running，才执行第二阶段 `resume=True`。同一账户不会同时恢复新版和 Sniper Old；新版恢复请求存在时，Sniper Old resume 被阻止。
5. 完成上述恢复步骤后，才启动第 12 节的只读账户安全探测。
6. 查询失败、出现无法归属的开放订单/仓位、任一 Sniper snapshot 损坏，或 Auto-Rebalance journal 未完成时，账户记录观察/失败状态。没有显式 `assetId` 的 position 只有 `tokenIndex=1 + tokenName=YES` 或 `2 + NO` 的完整一致组合才能派生 outcome；字段缺失、未知或互相冲突一律记入 `invalid_positions`，不能默认成 YES。
7. 账户不会因该观察状态自动清仓；运营人员可使用撤单、reduce-only SELL、Flash 或专用 recover/reconcile。
8. 人工 Reconcile 若发现 Auto-Rebalance journal，会先执行保守 `recover()`：停组件、撤单核验、对账并补保护 SELL，但不恢复新 entry；随后再查询真实交易所状态。
9. Reconcile 是对 Auto-Rebalance 自己的恢复 journal 做保守处理，不存在账户级“强制解锁”或 Recovery Lock。
10. 不要直接删除 `data/accounts.db`、`data/cost_layers_*.json` 或 `data/pending_sells_*.json` 来消除界面警告，这会删除归属证据而不是消除真实订单和仓位。

## 14. 日志文件与逐毫秒排查方法

实现入口：`backend/core/logging_config.py`、`backend/core/client.py`、各策略的 `audit_event()`/`market_event()`。

默认目录为项目根目录的 `logs/`，可用环境变量 `OKX_BOT_LOG_DIR` 指定其他本地目录。

| 文件 | 内容 | 第一用途 |
|---|---|---|
| `logs/bot.log` | 普通运行日志、策略摘要、重连、状态变化 | 先看整体发生了什么 |
| `logs/error.log` | WARNING/ERROR/CRITICAL 镜像 | 快速找失败与风险锁 |
| `logs/audit.jsonl` | HTTP、OKX REST、下单/撤单、订单确认、持久化、策略阶段 | 还原动作链 |
| `logs/market_data.jsonl` | Poly/OKX 原始 book、WS delta、top-of-book、接收时间 | 对齐每毫秒行情变化 |

1. 先用 `account_id` 定位账户。
2. Manual/Monitor/Sniper 用 `run_id` 定位一次启动；Sniper/Monitor 用 `trace_id` 定位一次触发；Rebalance 用 `rebalance_id` 定位一次恢复链。
3. Sniper 再用 `frame_id + target_id` 找触发来源，用 `clientOrderId` 连接 pre-persist、POST、open/closed order 和 position reconcile。
4. 对比 `received_wall_ns`、`received_monotonic_ns`、`duration_ms` 和各 latency stage，确认延迟发生在 Poly 收包、策略判断、OKX book、positions、balance、写盘、签名还是 POST ACK。
5. 检查 `GET /api/logging/status` 的路径、queue size、`dropped_records` 和轮转状态；队列丢日志时，缺失记录不能被解释为“该动作没有发生”。
6. 默认轮转上限：bot `32 MiB × 11`、error `16 MiB × 13`、audit `32 MiB × 13`、market `128 MiB × 17`（包含当前文件）；部署前确认磁盘容量足够。
7. JSONL 含账户、订单、市场、价格、数量和仓位摘要，仍是敏感运营数据；备份和传输时应按交易日志保护。

## 15. 正式运行前的顺序检查表

1. 确认只运行一个服务进程、一个 worker；不要让两套服务共享同一 `data/`。
2. 打开 Dashboard 后确认账户 recovery 为 unlocked、只读账户安全探测正常、日志队列没有持续 drop。
3. 检查 Poly outcome 与 OKX market/side 映射，尤其是 NO 必须选择真实 Poly NO token。
4. 用 `0.1¢`、`3.9¢`、`4¢`、`96¢`、`96.1¢` 验证页面显示和后端实际提交价格。
5. 用极小仓位分别演练 Manual-D、Manual-S YES、Manual-S NO、Bid-Sell、Monitor trigger、Auto-Rebalance recovery 和新版 Sniper。
6. 每次演练都检查 Active Orders、Closed Orders、Positions、`audit.jsonl` 和 `market_data.jsonl`，不能只看页面按钮文字。
7. 专门演练 partial fill、HTTP timeout、服务重启、状态文件不可写、空订单簿和 Flash 未完全成交。
8. 只有 Stop、恢复锁、protective SELL 和 Flash 的真实结果都与本文一致后，再逐步增加 Position %。
