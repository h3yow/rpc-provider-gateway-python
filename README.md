# RPC Provider Gateway

本地多上游 JSON-RPC 网关：在多个上游节点之间路由请求，提供故障转移、结果缓存与调用方配额，所有行为在并发下给出确定且可解释的结论。

## 架构

```
调用方 ──> 网关 (FastAPI, :8080)
             ├─ QuotaManager   按调用方的速率/并发配额
             ├─ ResultCache    按请求语义区分的缓存
             ├─ UpstreamRegistry 健康/优先级/权重选择
             └─ Router         编排：配额 → 缓存 → 选择 → 故障转移
                    │
             ┌──────┴──────┐
        mock node-a(:8545)  mock node-b(:8546)   ← 本地模拟上游
```

模块：`gateway/errors.py`（归一化错误）、`gateway/upstream.py`（注册与选择）、
`gateway/cache.py`（缓存）、`gateway/quota.py`（配额）、`gateway/router.py`（编排）、
`gateway/mock_node.py` / `gateway/mock_server.py`（模拟上游）、`gateway/server.py`（HTTP 入口）。

## 路由与健康策略

- **选择**：仅在健康节点中选择；先取最小 `priority` 的一层，层内按 `weight` 加权随机。
- **健康追踪**：连续失败达到 `FAILURE_THRESHOLD`（3 次）标记为不健康，不再被选中；
  成功后立即恢复健康并重置计数。
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

## 缓存一致性规则

按请求语义分三类（`gateway/cache.py: classify`）：

- **CACHEABLE**：结果已确定，永久有效。如 `eth_getBlockByHash`、`eth_getTransactionReceipt`，
  以及显式指定高度（`0x...`）的 `eth_getBlockByNumber` / `eth_getBalance` / `eth_call`。
- **HEAD_BOUND**：绑定链头。如 `latest` 余额、`eth_blockNumber`、`eth_gasPrice`。
  条目按“抓取开始时网关已知链头”盖章；网关观测到更高链头后，条目在读取时即被判过期剔除，
  **不会返回过期数据**。并发写缓存时，更旧链头的结果不得覆盖更新的条目（防回退）。
- **NON_CACHEABLE**：`eth_sendRawTransaction`、`eth_subscribe` 等，每次直达上游。

链头感知：网关从 `eth_blockNumber` 响应中学习各节点链头，也可通过
`Router.refresh_chain_head()` 主动轮询（生产环境应作为周期任务运行）。
同一 key 的缓存值与节点无关，节点切换不会产生互相矛盾的结果。

## 配额语义

按调用方标识（HTTP 头 `x-caller-id`，缺省 `anonymous`）施加：

- **速率**：令牌桶（`rate_per_second` + `burst`）。令牌不足拒绝为 `rate_limited`。
- **并发**：在途请求上限 `max_concurrent`。超限拒绝为 `concurrency_limited`。
  先查速率再查并发，两种拒绝原因可区分。

原子性保证：

- 一次请求只 `acquire` 一次许可；**故障转移重试不重复扣减**。
- 令牌扣减与并发占用在同一临界区内完成；许可释放恰好一次（`Permit` 幂等释放），
  请求失败（含全部上游不可用）同样释放，不泄漏。
- 缓存命中的请求不消耗配额（未触及上游）。
- **配置变更**（`QuotaManager.set_config`）即时生效：令牌余额重置为新容量；
  进行中的请求按原许可完成，不追溯取消。

## 本地启动与验证

### 启动模拟上游 + 网关

```bash
uv run python main.py
# 网关 :8080，模拟上游 node-a :8545（priority 0）、node-b :8546（priority 1）
```

### 基本调用

```bash
curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

### 验证故障转移

```bash
# 让 node-a 接下来 3 次调用失败（503，模拟连接失败）
curl -s -X POST "localhost:8545/admin/fail?count=3"
# 写请求将转移到 node-b 并成功
curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0xabc123"]}'

# 注入超时（节点挂起，网关 2s 超时）；写请求不重试，返回 upstream_timeout
curl -s -X POST "localhost:8545/admin/fail?count=1&kind=timeout"
curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"eth_sendRawTransaction","params":["0xdef456"]}'
```

### 验证缓存失效

```bash
# 推进 node-a 链头
curl -s -X POST localhost:8545/admin/advance
# 网关观测链头后，HEAD_BOUND 缓存失效（单测覆盖自动轮询路径）
```

### 观察状态

```bash
curl -s localhost:8080/upstreams      # 节点健康/失败计数/链头
curl -s localhost:8080/quota/alice    # 某调用方配额用量
```

### 运行测试

```bash
uv run pytest tests/ -q
```

27 个测试覆盖：故障转移（含并发不丢失/不重复）、缓存失效（含并发无过期读、
结果不回退）、配额超限（速率/并发可区分、故障转移不重复扣减、配置即时生效、
在途请求稳定）、HTTP 端点错误归一化。测试日志（`log_cli`）打印上游选择、
缓存判定依据与配额变动，例如：

```
INFO gateway.upstream: upstream selected: a (priority=0, tier_size=2, excluded=[])
INFO gateway.cache: cache invalidated on read: eth_blockNumber (entry_head=0 < current_head=5)
INFO gateway.quota: quota acquired: caller=c1 tokens=9.00 in_flight=1
INFO gateway.router: failover decision: method=eth_sendRawTransaction node=a kind=upstream_timeout retry=False attempt=1
```
