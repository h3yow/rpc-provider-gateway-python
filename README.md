# RPC Provider Gateway

本地多上游 JSON-RPC 网关：在多个上游节点之间路由请求，提供故障转移、节点自愈、
运行时热变更、统一链头口径下的结果缓存与调用方配额，所有行为在并发下给出确定且
可解释的结论。

## 架构

```
调用方 ──> 网关 (FastAPI, :8080)
             ├─ QuotaManager     按调用方的速率/并发配额
             ├─ ResultCache      按请求语义区分的缓存（统一链头口径判定失效）
             ├─ UpstreamRegistry 状态机/优先级/权重选择 + 统一链头口径 + 热变更
             ├─ RecoveryProber   摘除节点的退避探活与自动回补
             └─ Router           编排：配额 → 缓存 → 选择 → 故障转移
                    │
             ┌──────┴──────┐
        mock node-a(:8545)  mock node-b(:8546)   ← 本地模拟上游
```

模块：`gateway/errors.py`（归一化错误）、`gateway/upstream.py`（状态机、选择、
统一链头口径、热变更）、`gateway/recovery.py`（自愈探活）、`gateway/cache.py`（缓存）、
`gateway/quota.py`（配额）、`gateway/router.py`（编排）、
`gateway/mock_node.py` / `gateway/mock_server.py`（模拟上游）、`gateway/server.py`（HTTP 入口）。

## 节点状态机与路由策略

每个上游节点处于三种状态之一（`GET /upstreams` 可见）：

| 状态 | 含义 | 是否参与选路 | 是否计入链头口径 |
|---|---|---|---|
| `active` | 正常服务 | 是 | 是 |
| `down` | 连续失败被摘除，等待恢复确认 | 否 | 否 |
| `joining` | 运行时热加入，等待对齐探测 | 否 | 否 |

- **选择**：仅在 `active` 节点中选择；先取最小 `priority` 的一层，层内按 `weight`
  加权随机。HEAD_BOUND 请求（见缓存语义）进一步只在**已追平统一链头口径**的节点中
  选择；若无同步节点则降级为全体 `active`（可用性优先，结果不会被当作最新缓存）。
- **摘除**：连续失败达到 `FAILURE_THRESHOLD`（3 次）转为 `down`，立即退出选路。
- **故障转移**：一次请求内已失败的节点加入排除集，重选其他节点重试，最多 `max_retries` 次。
  - 连接失败（`upstream_unavailable`，请求未到达上游）：所有方法都可安全重试。
  - 超时（`upstream_timeout`，结果不确定）：仅幂等读方法重试；**写方法不重试**，避免重复执行。
  - 上游业务错误（`upstream_error`）：不重试，直接返回。
- **错误归一化**：上游原始异常统一映射为 `GatewayError`，携带可区分的 `kind` 与
  JSON-RPC 错误码，原始异常内容不外泄：

  | kind | code | retryable |
  |---|---|---|
  | `upstream_timeout` | -32001 | 是（仅幂等） |
  | `upstream_unavailable` | -32002 | 是 |
  | `no_healthy_upstream` | -32003 | 是 |
  | `upstream_error` | -32004 | 否 |
  | `rate_limited` | -32005 | 是 |
  | `concurrency_limited` | -32006 | 是 |
  | `invalid_request` | -32600 | 否 |

## 节点自愈（自动恢复）

被摘除的节点不会永久排除：`RecoveryProber` 以后台循环对 `down` / `joining` 节点做
低开销探活（`eth_blockNumber`，独立超时），确认恢复后按**原有优先级与权重**放回选路。

- **退避**：首次探测前等待 `initial_interval`，每次失败按 `backoff_multiplier`
  指数退避，上限 `max_interval`——不会把本来就弱的节点打垮。
- **确认才放量**：`down` 节点需连续 `successes_needed` 次探测成功才恢复；
  中途失败重新计数并退避。恢复判定完成前正常流量**绝不**引向该节点。
- **独立通道**：探测不消耗任何调用方配额、不写缓存、不影响在途请求；
  正在处理的请求按各自进入时的结论收尾。
- 参数见 `gateway/recovery.py: RecoveryConfig`；`Router(recovery_config=...)`
  传入即启用，随服务生命周期自动启停。

## 运行时上游热变更

服务运行中可通过管理 API 变更上游集合，**对之后的选路立即生效**：

```bash
curl -X POST localhost:8080/upstreams -H 'content-type: application/json' \
  -d '{"name":"node-c","url":"http://127.0.0.1:8547/","priority":0,"weight":2}'  # 201, joining
curl -X PATCH localhost:8080/upstreams/node-c -H 'content-type: application/json' \
  -d '{"priority":1,"weight":5}'      # 调整优先级/权重/超时
curl -X DELETE localhost:8080/upstreams/node-c
```

- **热加入**：节点以 `joining` 状态进入，由自愈探活完成对齐后才放量；
  对齐前既不承接流量也不计入链头口径，不会污染整体判断。
- **热删除**：立即退出后续选路与链头口径；在途请求持有节点引用，按进入时的
  结论正常收尾，不重复执行、不丢失。
- **热调整**：`priority` / `weight` / `timeout` 修改即时生效，在途请求不受影响。

## 统一链头口径与缓存一致性

多节点链头对不齐时，网关对外只有一份**统一链头口径**（unified chain head）：

- **口径定义**：全体 `active` 节点中，被**过半数（法定人数）确认**的最高高度；
  未报告过链头的节点按 0 计（不确认任何高度）。
- **棘轮**：口径只增不减。个别节点单方面领先不会推高口径（需多数派确认），
  因此仍有效的缓存不会被反复判废、不会每次回源；落后节点也不会拉低口径。
- **落后节点**：链头低于口径的节点不会被选去服务 HEAD_BOUND 请求；极端降级场景
  （无任何同步节点）下其结果照常返回给调用方，但**不写入缓存**，不会被当成最新状态。
- **追平复用**：落后节点追平、口径相应前进后，盖章仍有效的缓存条目继续复用。
- **可观测**：`GET /chainhead` 返回当前口径、法定人数、各节点高度/状态/是否同步，
  便于定位“口径为什么是这个值”。

缓存按请求语义分三类（`gateway/cache.py: classify`）：

- **CACHEABLE**：结果已确定，永久有效。如 `eth_getBlockByHash`、`eth_getTransactionReceipt`，
  以及显式指定高度（`0x...`）的 `eth_getBlockByNumber` / `eth_getBalance` / `eth_call`。
- **HEAD_BOUND**：绑定链头。如 `latest` 余额、`eth_blockNumber`、`eth_gasPrice`。
  条目按“抓取开始时网关的统一链头口径”盖章；口径前进后，落后条目在读取时即被判
  过期剔除，**不会返回过期数据**。并发写缓存时，更旧链头的结果不得覆盖更新的条目。
- **NON_CACHEABLE**：`eth_sendRawTransaction`、`eth_subscribe` 等，每次直达上游。

链头感知：网关从 `eth_blockNumber` / `eth_getBlockByNumber` 响应中学习各节点链头，
`Router.refresh_chain_head()` 轮询全部 `active` 节点刷新口径（生产环境应作为周期
任务运行）。同一 key 的缓存值与节点无关，节点切换不会产生互相矛盾的结果。

## 配额语义

按调用方标识（HTTP 头 `x-caller-id`，缺省 `anonymous`）施加：

- **速率**：令牌桶（`rate_per_second` + `burst`）。令牌不足拒绝为 `rate_limited`。
- **并发**：在途请求上限 `max_concurrent`。超限拒绝为 `concurrency_limited`。
  先查速率再查并发，两种拒绝原因可区分。

原子性保证：

- 一次请求只 `acquire` 一次许可；**故障转移重试不重复扣减**。
- 令牌扣减与并发占用在同一临界区内完成；许可释放恰好一次（`Permit` 幂等释放），
  请求失败（含全部上游不可用）同样释放，不泄漏。
- 缓存命中与恢复探测均不消耗配额。
- **配置变更**（`QuotaManager.set_config`）即时生效：令牌余额重置为新容量；
  进行中的请求按原许可完成，不追溯取消。

## 与上一版的兼容范围

- 选路（健康/优先级/权重）、故障转移与重试语义、错误归一化、缓存分类与失效规则、
  配额语义全部保持不变，原有用例不回归。
- 行为差异仅在于“更保守、更可解释”：被摘除节点从“一次成功即恢复”改为“退避探活
  + 连续确认才恢复”；链头失效判据从“任一节点最高高度”改为“多数派确认的统一口径”
  （单节点部署下两者等价）。

## 本地启动与验证

### 启动模拟上游 + 网关

```bash
uv run python main.py
# 网关 :8080，模拟上游 node-a :8545（priority 0）、node-b :8546（priority 1）
# 自愈探活已启用：initial=2s，max=15s，连续 2 次成功才回补
```

### 基本调用

```bash
curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

### 验证故障转移与自愈

```bash
# 让 node-a 持续失败（503，模拟连接失败）；写请求转移到 node-b
curl -s -X POST "localhost:8545/admin/fail?count=10"
curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0xabc123"]}'
curl -s localhost:8080/upstreams   # 连续 3 次失败后 node-a 状态变为 down

# 停止注入故障，等待数秒：探活确认后 node-a 自动回到 active 并按原优先级参与选路
curl -s -X POST "localhost:8545/admin/fail?count=0"
sleep 5 && curl -s localhost:8080/upstreams
```

### 验证热变更

```bash
curl -s -X POST localhost:8080/upstreams -H 'content-type: application/json' \
  -d '{"name":"node-c","url":"http://127.0.0.1:8546/","priority":0,"weight":3}'
curl -s -X PATCH localhost:8080/upstreams/node-c -H 'content-type: application/json' \
  -d '{"weight":9}'
curl -s -X DELETE localhost:8080/upstreams/node-c
```

### 验证统一链头口径与缓存失效

```bash
curl -s -X POST localhost:8545/admin/advance?by=5   # 只推进 node-a
curl -s localhost:8080/chainhead                    # 口径不变：多数派未确认
curl -s -X POST localhost:8546/admin/advance?by=5   # node-b 追平
curl -s localhost:8080/chainhead                    # 口径前进，过期 HEAD_BOUND 缓存失效
```

### 观察状态

```bash
curl -s localhost:8080/upstreams      # 节点状态/失败计数/链头/是否同步
curl -s localhost:8080/chainhead      # 统一链头口径与判断依据
curl -s localhost:8080/quota/alice    # 某调用方配额用量
```

### 运行测试

```bash
uv run pytest tests/ -q
```

47 个测试覆盖：故障转移（含并发不丢失/不重复）、节点自愈（退避探活、连续确认、
回补原层级、在途请求不受影响、探测不占配额）、统一链头口径（单方面领先不判废、
多数派确认才前进、落后节点不污染最新状态、追平后复用）、运行时热变更（增删改
即时生效、与并发请求交织不丢失不重复）、缓存失效（含并发无过期读）、配额超限、
HTTP 端点错误归一化。测试日志（`log_cli`）打印节点选择、链头口径判断、恢复探测
与配置变更的经过，例如：

```
INFO gateway.upstream: upstream selected: a (priority=0, tier_size=2, excluded=[])
INFO gateway.upstream: unified chain head advanced: 10 -> 15 (quorum=2 of 2 active nodes)
INFO gateway.upstream: upstream marked down: a after 3 consecutive failures; recovery probing (with backoff) required before rejoining
INFO gateway.recovery: upstream recovered: a after 2 consecutive successful probes (chain_head=5)
INFO gateway.upstream: upstream rejoined routing: a (down -> active, priority=0, weight=1, chain_head=5)
INFO gateway.upstream: upstream reconfigured: b priority=-1 weight=1 timeout=5.0
INFO gateway.cache: cache invalidated on read: eth_blockNumber (entry_head=0 < current_head=5)
INFO gateway.quota: quota acquired: caller=c1 tokens=9.00 in_flight=1
```
