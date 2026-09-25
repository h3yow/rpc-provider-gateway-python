# 本地多上游 JSON-RPC 网关（Python）

一个可完全在本地启动、行为确定且结论可解释的多上游 JSON-RPC 网关。用 3 个**模拟链节点**充当上游，演示并测试三件事：

1. **多上游路由与故障转移**：按健康状态 / 优先级 / 权重选节点，超时或报错自动转移重试；
2. **头感知结果缓存**：区分可缓存 / 不可缓存，链头变化后确定性失效，不返回过期数据、不产生跨节点矛盾；
3. **调用方配额**：按调用方标识施加**速率**与**并发**两类配额，拒绝原因可区分，准入与上游调用一一对应。

> 设计目标是"并发下结论确定且可解释"：所有关键判定（选了哪个上游、缓存命中/失效依据、配额扣减）都打印结构化日志，测试直接断言这些不变量。

---

## 目录结构

```
rpc_gateway/
  errors.py        # 归一化错误类型（调用方永远拿不到上游原始异常）
  jsonrpc.py       # JSON-RPC 2.0 报文工具
  methods.py       # 方法语义登记：缓存类别 / 是否幂等 / 是否依赖链头
  router.py        # 节点注册 + 健康/新鲜度过滤 + 优先级/加权候选排序
  health.py        # 健康熔断状态机 CLOSED / OPEN / HALF_OPEN
  upstream.py      # 真实 HTTP 传输（httpx），异常归一化
  inprocess.py     # 进程内传输（测试/演示用，直调模拟节点，不经端口）
  cache.py         # 头感知缓存 + 同键单飞（single-flight）
  quota.py         # 固定窗口速率 + 在途并发槽，原子准入，热更新
  head_monitor.py  # canonical head 追踪：过半确认 + "先失效后提升"
  gateway.py       # 核心编排：配额 → 缓存单飞 → 路由 → 故障转移
  config.py        # 配置装载与异步组装工厂
  server.py        # FastAPI 入口与 /admin 管理接口
  localstack.py    # 一键启动 3 模拟节点 + 网关（真实 HTTP 端口）
mocknode/
  cluster.py       # 模拟集群共享账本、出块、幂等去重、故障注入
  app.py           # 单个模拟节点 FastAPI 应用 + /_control 控制端点
examples/
  demo_local_stack.py   # 自动演示三大能力的脚本（起栈 → 演示 → 退出）
tests/             # 33 个 pytest 用例（故障转移 / 缓存 / 配额 / 并发 / E2E）
```

---

## 快速开始

依赖：Python ≥ 3.14、[uv](https://docs.astral.sh/uv/)。

```bash
# 安装（fastapi/httpx + pytest 开发依赖）
uv sync

# 跑全部测试（关键判定会打印 INFO 日志）
uv run pytest -q

# 一键本地启动：3 个模拟节点 + 网关（前台运行，Ctrl-C 退出）
uv run python -m rpc_gateway.localstack
#   网关:      http://127.0.0.1:8080
#   模拟节点:  http://127.0.0.1:8101 (node-a, priority=50)
#              http://127.0.0.1:8102 (node-b, priority=100)
#              http://127.0.0.1:8103 (node-c, priority=100)

# 或运行自动演示（起栈 → 逐项演示 → 自动退出）
uv run python examples/demo_local_stack.py
```

### 手动验证（另开终端）

```bash
GW=http://127.0.0.1:8080

# 1) 普通读（X-Caller-Id 标识调用方，缺省为 anonymous）
curl -s $GW/ -X POST -H 'content-type: application/json' -H 'X-Caller-Id: demo' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}'

# 2) 让最高优先级节点 node-a 报 500，观察故障转移（自动落到 b/c）
curl -s http://127.0.0.1:8101/_control/behavior -X POST \
  -H 'content-type: application/json' -d '{"http500":true}'
curl -s $GW/ -X POST -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"web3_clientVersion"}'

# 3) 出块 -> 手动推进链头 -> 头依赖缓存失效，读到新高度
curl -s http://127.0.0.1:8101/_control/mine -X POST \
  -H 'content-type: application/json' -d '{"count":3}'
curl -s $GW/admin/heads/poll -X POST
curl -s $GW/ -X POST -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"eth_blockNumber"}'

# 4) 热更新配额：把 demo 限制为 1 req/s，第二次请求被区分性拒绝
curl -s $GW/admin/quota -X POST -H 'content-type: application/json' \
  -d '{"caller":"demo","rate_per_second":1,"max_concurrency":5}'

# 5) 查看节点健康与链头
curl -s $GW/admin/nodes
```

---

## 一、路由与健康策略

### 候选节点选择（`rpc_gateway/router.py`）
对每个请求按顺序过滤、排序，得到**故障转移尝试序列**：

1. **注册开关**：`enabled=false` 的节点摘除（`POST /admin/nodes/{name}/enable`）；
2. **健康熔断**：熔断 `OPEN` 的节点不放行；冷却到期后进入 `HALF_OPEN`，**全集群只放一个试探请求**，成功才恢复，避免恢复瞬间被并发打穿；
3. **链头新鲜度**（仅头依赖读）：要求 `node_head >= canonical_head`，**落后节点直接不参与**——这从路由层杜绝"在 b/c 读到旧世界、在 a 读到新世界"的跨节点矛盾；
4. **优先级**：`priority` 数值越小越优先；
5. **权重**：同一优先级内按 `weight` 做不放回加权抽样，决定该组内的尝试顺序。

### 健康熔断（`rpc_gateway/health.py`）
* 连续失败达到阈值（默认 2）→ `OPEN` 摘除并记录冷却起点；
* 冷却（默认 5s）到期后下一次请求把节点置为 `HALF_OPEN` 并独占试探权；
* 试探成功 → `CLOSED`；试探失败 → 重新 `OPEN` 计时；
* 所有状态转换在每节点锁内完成，并发故障不会重复摘除/重复计时。

### 故障转移与错误归一化（`rpc_gateway/gateway.py`、`upstream.py`）
* 网关按候选序列逐个尝试：
  * **超时 / 连接失败 / 5xx / 非法响应** → 记一次失败，换下一个节点重试；
  * **上游明确的 JSON-RPC error（业务错误）** → 视为该请求的确定业务结论，**不换节点重试**，直接归一化返回；
* 调用方看到的错误只有脱敏后的固定类型（见下表），**永远拿不到 httpx 异常、内部堆栈或上游原始 message**（仅保留数值型 `upstream_code`）；
* 所有候选都失败 → `no_healthy_node`，并在 `data.attempted` 给出尝试过的节点序列、`data.last_error_type` 给出最后一类失败，结论完全可解释。

| error_type                  | code    | 含义 |
|-----------------------------|---------|------|
| `upstream_timeout`          | -32002  | 所选上游在超时窗口内无响应 |
| `upstream_unavailable`      | -32001  | 连接失败 / 5xx / 非法响应 |
| `upstream_rpc_error`        | -32003  | 上游返回业务 JSON-RPC error（已脱敏） |
| `no_healthy_node`           | -32004  | 没有合格节点，或所有候选尝试失败 |
| `rate_limit_exceeded`       | -32010  | 超出速率配额 |
| `concurrency_limit_exceeded`| -32011  | 超出在途并发配额 |
| `invalid_request` / `method_not_allowed` | -32600 / -32601 | 报文非法 / 方法未登记 |

### 请求不丢失、不重复执行
* **读请求**天然幂等，故障转移重试安全；
* **写请求**（`eth_sendRawTransaction`，登记为非幂等）由网关对 `method+params` 派生**稳定幂等键**，在每次重试中携带；模拟集群以幂等键去重，**同一逻辑请求在集群侧只应用一次**，重放返回同一交易哈希。

---

## 二、缓存一致性规则（`methods.py`、`cache.py`、`head_monitor.py`）

### 请求按语义分三类（在 `methods.py` 显式登记，未登记方法直接拒绝）
| 类别 | 方法示例 | 规则 |
|------|----------|------|
| `finalized`（终局） | `eth_chainId` | 整条链一致、永不变，一次缓存长期有效 |
| `head`（头依赖） | `eth_blockNumber`、`eth_getBlockByNumber`、`eth_getBalance`、`eth_call`、`eth_getTransactionReceipt` 等 | 结果带 canonical head 标签；**链头推进即整组失效**；且只允许 `node_head >= canonical_head` 的节点服务 |
| `never`（不缓存） | `eth_sendRawTransaction`、`eth_estimateGas`、`eth_syncing`、`web3_clientVersion` | 写方法 / 结果随时变化 / 节点相关 |

### 链头推进：过半确认 + "先失效后提升"
1. 头监控周期性（`head_poll_interval`）向所有启用节点探 `eth_blockNumber`，更新各节点 `node_head`；业务响应头 `X-Mock-Head` 也会即时更新；
2. 仅当**过半节点**观测到同一新高度时才提升 canonical head——单个超前节点无法独自推动全局，避免抖动；
3. 提升顺序严格为：**先 `cache.invalidate_head_tagged(new_head)`，之后 canonical head 才变为新值**。因此任何读到新 head 的请求，看到的必然是失效后的缓存，杜绝过期读。

### 并发一致
* **同键单飞（single-flight）**：并发同键未命中时，有且仅有一个 leader 真正回源，其余请求 await 同一个 future，拿到**完全相同**的结果，上游对该逻辑请求只执行一次；
* 缓存读写、失效均在锁内；过期（标签 head < canonical）条目会被摘除并回源。

> 说明：网关不会"凭空知道"链已推进。链头推进由后台轮询或显式 `POST /admin/heads/poll` 触发（生产中即健康检查周期）。在确认推进之前返回旧 head 的缓存值是**有意的一致性选择**，而不是过期读。

---

## 三、配额语义（`rpc_gateway/quota.py`）

* 调用方由 HTTP 头 **`X-Caller-Id`** 标识，缺省为 `anonymous`；
* 两类限制：**固定窗口速率**（`rate_per_second`，默认 1 秒窗口）与**在途并发槽**（`max_concurrency`）；
* **原子准入**：在每调用方一把锁内，先判速率、后判并发，然后才同时"计一次速率 + 占一个并发槽"：
  * 速率已满 → `rate_limit_exceeded`（-32010）；并发已满 → `concurrency_limit_exceeded`（-32011），**拒绝原因可区分**；
  * **拒绝不扣任何东西**，且拒绝发生在路由 / 上游调用之前；
* **配额判定与上游调用一一对应**：准入在整个逻辑请求外围只发生一次。故障转移在内部多次尝试上游，**不重复扣减**；请求结束（成功或失败）释放并发槽，**不漏扣、不泄漏**；
* 缓存命中 / 单飞等待也只在准入之后发生，未准入请求不能搭车他人结果；
* **热更新**（`POST /admin/quota`）整体替换某调用方限额，对后续准入**即时生效**；**已在途的请求继续持有旧槽位直到结束，结论稳定**，缩小限额不会打断进行中的请求（但可能出现瞬时在途数 > 新上限，随请求完成收敛）。

### 管理接口
| 接口 | 作用 |
|------|------|
| `GET  /admin/nodes` | 节点注册信息、健康状态、各自链头、`_canonical_head` |
| `POST /admin/nodes/{name}/enable` | `{"enabled": false}` 摘除 / 恢复节点 |
| `POST /admin/quota` | `{"caller","rate_per_second","max_concurrency"}` 热更新 |
| `POST /admin/heads/poll` | 手动触发一轮链头探测（演示 / 测试确定性推进） |

### 模拟节点控制接口（仅本地）
| 接口 | 作用 |
|------|------|
| `POST /_control/mine` | `{"count": n}` 生产区块 |
| `POST /_control/behavior` | 注入故障：`fail`(业务error)、`http500`、`timeout`、`latency`、`head_offset` |
| `GET  /_control/state` | 节点视角链头、真实链头、交易实际执行次数 |

---

## 四、测试

```bash
uv run pytest -q                 # 全部 33 个用例
uv run pytest -s -k failover     # 故障转移（含日志）
uv run pytest -s -k quota        # 配额
uv run pytest -s -k cache        # 缓存一致性 / 单飞
uv run pytest -s -k stress       # 并发计数精确性
```

测试覆盖（均为并发场景）：
* **故障转移**：500/超时/业务错误的区分与转移、熔断摘除、候选耗尽的可解释错误、原始异常不外泄；
* **缓存**：链头推进后头标签条目失效、终局条目存活、落后节点不得服务头依赖读、推进中并发读不撕裂、同键 20~50 并发只回源 1 次；
* **配额**：速率/并发拒绝可区分、拒绝不触达上游、故障转移只扣一次、调用方隔离、热更新即时生效、缩容不影响在途请求、窗口恢复；
* **并发计数**：成功数 + 拒绝数 == 请求总数、在途不超上限、结束归零、不同交易各执行一次且重放不重复；
* **E2E**：FastAPI + 进程内 ASGI 传输验证 HTTP 状态码、批量请求、通知 204、管理接口与脱敏。

单测日志中可直接看到三类判定依据，例如：

```
rpc_gateway.router  route: require_head=True canonical=2 ordered=[node-a,node-c] rejected=[node-b:lagging(...)]
rpc_gateway.cache   cache decision: key=eth_blockNumber:[] HIT (class=head entry_head=2 canonical=2)
rpc_gateway.cache   cache decision: INVALIDATE head-tagged at new_head=3 (dropped=1)
rpc_gateway.quota   quota decision: caller=demo ADMIT rate_used=3/5 in_flight=2/4
rpc_gateway.quota   quota decision: caller=demo REJECT concurrency in_flight=4/4
rpc_gateway         upstream select: method=... attempt=1/3 node=node-a priority=50
rpc_gateway         failover: ... node=node-a failed (upstream_timeout) -> try next
```

---

## 五、实现说明与边界

* 传输有两套实现：`HttpTransport`（真实 httpx，用于 localstack / 生产形态）与 `InProcessTransport`（测试用，直调模拟集群，零端口、确定性强）；两者对外契约一致，都只抛归一化错误；
* 幂等键通过 JSON-RPC 信封扩展字段 `xIdempotencyKey` 传递，仅模拟节点识别；真实节点会忽略未知字段；
* 模拟节点的哈希为确定性 SHA-256（非真实 keccak），账本为最小可用模型，目标是验证网关语义而非链本身；
* `examples/demo_local_stack.py` 与 `localstack.py` 仅用于本地验证，管理/控制接口无鉴权，请勿直接暴露到公网。
