# RPC Provider Gateway

本地多上游 JSON-RPC 网关：在多个上游节点之间路由请求，提供故障转移、**节点摘除后的自动恢复（自愈）**、
**运行时上游热变更**、**统一链头口径下的缓存一致性**与调用方配额，所有行为在并发下给出确定且可解释的结论。

## 架构

```
调用方 ──> 网关 (FastAPI, :8080)
             ├─ QuotaManager    按调用方的速率/并发配额
             ├─ ResultCache     按请求语义区分、按统一链头口径盖章的缓存
             ├─ UpstreamRegistry 热变更/选路快照/健康与恢复/共识链头
             ├─ Router          编排：配额 → 缓存 → 快照选路 → 故障转移
             └─ HealthMonitor   后台：链头轮询 + 摘除节点退避探活
                    │
             ┌──────┼──────┬──────┐
        node-a(:8545) node-b(:8546) node-c(:8547)   ← 本地模拟上游
```

模块：`gateway/errors.py`（归一化错误）、`gateway/normalize.py`（异常归一化，路由与探测器共用）、
`gateway/upstream.py`（注册、热变更、选路快照、健康/恢复、共识链头）、
`gateway/health_monitor.py`（后台轮询与退避恢复探测）、
`gateway/cache.py`（缓存）、`gateway/quota.py`（配额）、`gateway/router.py`（编排）、
`gateway/mock_node.py` / `gateway/mock_server.py`（模拟上游）、`gateway/server.py`（HTTP 入口）。

## 路由与健康策略

- **选择**：仅在健康、未下线且**已与统一链头口径对齐**的节点中选择；先取最小 `priority` 的一层，
  层内按 `weight` 加权随机。冷启动链头口径尚未确立时，先在健康节点上引导首批请求以学习链头。
- **健康追踪**：连续失败达到 `FAILURE_THRESHOLD`（3 次）标记为不健康并**摘除**，不再被选中。
- **故障转移**：一次请求内已失败的节点加入排除集，重选其他节点重试，最多 `max_retries` 次。
  - 连接失败（`upstream_unavailable`，请求未到达上游）：所有方法都可安全重试。
  - 超时（`upstream_timeout`，结果不确定）：仅幂等读方法重试；**写方法不重试**，避免重复执行。
  - 上游业务错误（`upstream_error`）：不重试，直接返回。
- **错误归一化**：上游原始异常（连接错误、HTTP 503、JSON-RPC error、超时）统一映射为
  `GatewayError`，携带可区分的 `kind` 与 JSON-RPC 错误码，原始异常内容不外泄：

  | kind | code | retryable |
  |---|---|---|
  | `upstream_timeout` | -32001 | 是（仅幂等） |
  | `upstream_unavailable` | -32002 | 是 |
  | `no_healthy_upstream` | -32003 | 是 |
  | `upstream_error` | -32004 | 否 |
  | `rate_limited` | -32005 | 是 |
  | `concurrency_limited` | -32006 | 是 |
  | `invalid_request` | -32600 | 否 |

## 节点摘除后的自动恢复（自愈）

被摘掉的节点**不会被永久排除**，由 `HealthMonitor` 后台通道低开销地重新探活：

- **只读探活**：探测器只发 `eth_blockNumber`，恢复判定完成前绝不把正常流量引过去
  （`select` 的实时门控也会二次拦截）。
- **指数退避**：`base_backoff * 2**attempts`，封顶 `max_backoff`（演示默认 1s→10s），
  避免反复重试压垮本来就弱的节点；到点才发下一次探测。
- **恢复的两条件**：①探活请求成功（节点可达）；②节点上报链头 **不落后于统一口径**。
  节点可达但落后时继续摘除，只学习其高度、等待追平；追平后才正式放量。
- **放回原层**：恢复后保留摘除前的 `priority` / `weight`，回到原来那一层参与加权选路。
- **在途请求不受影响**：恢复放量不会改变已经在处理的请求结论；它们各自持有进入时的路由快照。

`HealthMonitor` 同时承担活跃节点的链头轮询，可通过 `Router.start()/stop()` 管理生命周期；
也可直接调用 `poll_once()`（单轮）或 `Router.refresh_chain_head()`（显式刷新，测试用）。

## 运行时上游热变更

服务运行期间可以**新增节点、下线节点、调整优先级/权重**，无需重启：

- `POST /admin/upstreams`：新增节点。新节点处于 **warming**：在首次上报链头并追平口径前，
  **不参与选路、不参与共识投票**，因此不会因为自身状态未对齐而污染整体判断。
- `DELETE /admin/upstreams/{name}`：下线节点。立即离开候选集（实时门控，当次故障转移也不会
  再选它）；已经在该节点上处理的请求仍引用旧节点对象正常收尾。
- `PATCH /admin/upstreams/{name}`：调整 `priority` / `weight`，对**之后进入**的请求立即生效。
- **快照隔离**：每个请求进入时冻结一份 `RoutingView`（含单调递增的 `revision`、候选集、
  优先级、权重、链头口径）。变更只推进 `revision` 并影响之后的请求；在途请求按各自进入时的
  视图完成故障转移，因此变更与并发请求交织时不会重复执行、不会结果错乱。

## 统一链头口径与缓存一致性

当上游链头对不齐时，网关对外给一份**统一、可解释、单调不减**的链头口径：

- **多数口径（quorum）**：在健康、未下线且已上报链头的节点中，取“至少多数节点都已达到”的
  最高高度作为共识链头（即第 `N//2+1` 高的上报值）。3 节点报告 `[10,10,7]` 时口径为 10。
- **落后节点不外用**：链头低于口径的节点不具选路资格，其返回内容不会被当成最新状态对外，
  也不会写入缓存。
- **单节点领先不判废**：只有一个节点领先时口径不前进（如 `[12,9,9]` 仍是 9），仍然有效的
  缓存条目继续命中，**不会每次都回源**；`eth_blockNumber` 对外也统一返回口径值，调用方不会
  看到彼此矛盾的“链头”。
- **口径单调不减**：已确立的口径作为下限，落后节点恢复投票或瞬时掉队都不会把口径往下拽，
  旧缓存条目不会因“口径回退”被反复误判。
- **追平后继续复用**：落后节点追平、多数达成新高度时口径才前进；前进使旧的 HEAD_BOUND 条目
  失效一次（正常回源），链头稳定后同口径结果继续命中缓存。
- **盖章时机**：HEAD_BOUND 条目按“**请求进入时**的口径”盖章；若抓取期间口径前进，该条目在
  下次读取时即被判过期，杜绝把旧状态按新高度盖章造成的过期读。

判断依据可在 `GET /upstreams` 中看到：`consensus_head`、`head_settled`、`voters`、`quorum`、
`head_basis`（各投票者上报高度的降序列表）以及每个节点的 `chain_head` / `aligned` /
`retired` / `probe_attempts`。

## 缓存语义

按请求语义分三类（`gateway/cache.py: classify`）：

- **CACHEABLE**：结果已确定，永久有效。如 `eth_getBlockByHash`、`eth_getTransactionReceipt`，
  以及显式指定高度（`0x...`）的 `eth_getBlockByNumber` / `eth_getBalance` / `eth_call`。
- **HEAD_BOUND**：绑定链头。如 `latest` 余额、`eth_blockNumber`、`eth_gasPrice`。
  条目按进入时统一口径盖章；口径前进后在读取时判过期剔除，**不返回过期数据**。
  并发写缓存时更旧链头的结果不得覆盖更新条目（防回退）。
- **NON_CACHEABLE**：`eth_sendRawTransaction`、`eth_subscribe` 等，每次直达上游。

## 配额语义

按调用方标识（HTTP 头 `x-caller-id`，缺省 `anonymous`）施加：

- **速率**：令牌桶（`rate_per_second` + `burst`）。令牌不足拒绝为 `rate_limited`。
- **并发**：在途请求上限 `max_concurrent`。超限拒绝为 `concurrency_limited`。

原子性保证：一次请求只 `acquire` 一次（故障转移重试不重复扣减）；令牌扣减与并发占用在同一
临界区完成；许可释放恰好一次；缓存命中不消耗配额；`set_config` 即时生效，在途请求不追溯取消。

## 本地启动与验证

### 启动模拟上游 + 网关（含后台自愈监控）

```bash
uv run python main.py
# 网关 :8080；模拟上游 node-a :8545、node-b :8546（priority 0）、node-c :8547（priority 1）
# 后台 HealthMonitor：每 2s 轮询链头；摘除节点退避 1s 起步、10s 封顶
```

### 基本调用 / 观察统一口径

```bash
curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
curl -s localhost:8080/upstreams     # consensus_head/quorum/head_basis/各节点 aligned
```

### 验证故障转移与自愈

```bash
# 让 node-a 连续失败（503，模拟连接失败），它会在 3 次失败后被摘除
curl -s -X POST "localhost:8545/admin/fail?count=20"
# 请求自动转移到 node-b/node-c；随后清除注入故障，HealthMonitor 会按退避探活，
# 确认可达且链头追平后自动把 node-a 放回 priority=0/weight=1 层
curl -s -X POST "localhost:8545/admin/fail?count=0"
# 观察 /upstreams 中 node-a 的 healthy 与 probe_attempts 变化
```

### 验证运行时热变更

```bash
# 新增节点（先启动一个新的模拟上游进程，监听 8548，再注册）
curl -s -X POST localhost:8080/admin/upstreams -H 'content-type: application/json' \
  -d '{"name":"node-d","url":"http://127.0.0.1:8548/","priority":0,"weight":1}'
# 调整优先级/权重（对之后的请求立即生效）
curl -s -X PATCH localhost:8080/admin/upstreams/node-c \
  -H 'content-type: application/json' -d '{"priority":-1,"weight":5}'
# 下线节点（在途请求仍按旧视图收尾）
curl -s -X DELETE localhost:8080/admin/upstreams/node-c
```

### 验证链头分歧时的缓存结论

```bash
# 只推进某个节点的链头：多数口径不变，HEAD_BOUND 缓存继续命中、不回源
curl -s -X POST "localhost:8545/admin/advance?by=3"
# 多数节点都推进后，口径前进，旧条目失效一次并回源；稳定后继续复用
curl -s -X POST "localhost:8546/admin/advance?by=3"
```

### 运行测试

```bash
uv run pytest tests/ -q
```

46 个测试覆盖：故障转移（含并发不丢失/不重复、写请求超时不重试）、配额（速率/并发可区分、
故障转移不重复扣减、配置即时生效、在途请求稳定）、**摘除节点的退避探测与回补（可达但落后
不放量、追平后回补、恢复前不引流、后台自动恢复生命周期）**、**统一链头口径（多数判定、单点
领先不判废、落后节点不外用、追平后缓存复用、链头分裂时对外口径一致、在途抓取按进入时盖章）**、
**运行时热变更（调权即时生效、下线后在途请求收尾、新节点 warming、落后新节点不污染、变更与
并发请求交织无重复无错乱）**、HTTP 端点错误归一化与管理接口。

测试日志（`log_cli`）打印节点选择、链头口径判断、配置变更与恢复探测经过，例如：

```
INFO gateway.upstream: upstream ejected after 3 failures: a kind=upstream_unavailable (recovery probe scheduled)
INFO gateway.monitor: recovery probe: a unreachable kind=upstream_unavailable (backoff 0.20s)
INFO gateway.upstream: recovery probe reachable but behind: a head=8 < consensus=10 (kept ejected)
INFO gateway.upstream: upstream restored to routing: a head=10 consensus=10 priority=0 weight=1
INFO gateway.upstream: chain head consensus: head=10 quorum=2/3 basis=[10, 10, 7] floor=10
INFO gateway.router: unify head response: node=a reported=12 consensus=9 (serving consensus)
INFO gateway.upstream: upstream updated at runtime: c priority 1->-1, weight 1->5 rev=4
INFO gateway.upstream: upstream removed at runtime: c rev=5 (in-flight requests finish on their snapshot; consensus_head=10 settled=True)
INFO gateway.router: cache decision: method=eth_getBalance stamped_head=10 served_node_head=11
```
