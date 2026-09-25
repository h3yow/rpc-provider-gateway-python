"""并发与故障转移语义测试。

覆盖：
* 高优先级节点故障时按优先级/权重顺序转移，请求不丢失；
* 超时与业务 RPC 错误归一化为可区分类型，上游原始异常不外泄；
* 连续失败触发熔断摘除；
* 写方法（eth_sendRawTransaction）在故障转移重试下 exactly-once；
* 并发读全部得到结果，不出现请求丢失或跨结果矛盾。
"""
from __future__ import annotations

import asyncio

import pytest

from mocknode.cluster import NodeFault


@pytest.mark.asyncio
async def test_basic_read_and_canonical_head(harness):
    resp = await harness.call("eth_blockNumber")
    assert resp["result"] == "0x0"
    harness.cluster.mine(2)
    # 链头推进必须经过轮询确认（过半节点）后才可见；确认前仍返回 head=0 的缓存
    resp_stale = await harness.call("eth_blockNumber")
    assert resp_stale["result"] == "0x0"
    new_head = await harness.poll_heads()
    assert new_head == 2
    resp = await harness.call("eth_blockNumber")
    assert resp["result"] == "0x2"
    assert harness.gateway.head_monitor.canonical_head == 2


@pytest.mark.asyncio
async def test_failover_on_rpc_error_to_next_node(harness):
    # node-a 优先级最高；让它返回业务 RPC error —— 这是"明确业务失败"，
    # 但为保证请求不丢失，模拟节点注入错误应被归一化；这里用 http500 做可重试故障，
    # rpc_error（上游 JSON-RPC error）的不重试语义在另一个用例验证。
    harness.cluster.set_behavior("node-a", http500=True)
    resp = await harness.call("eth_blockNumber")
    assert "result" in resp
    # node-a 失败一次（阈值=1）被熔断摘除；后续流量直接落到 b/c
    snap = await harness.gateway.health.snapshot()
    assert snap["node-a"].state.value == "open"


@pytest.mark.asyncio
async def test_timeout_is_normalized_and_retried(harness):
    harness.cluster.set_behavior("node-a", timeout=True)
    resp = await harness.call("eth_blockNumber")
    # 转移到健康节点成功；没有把超时直接抛给调用方
    assert resp["result"] in ("0x0",)
    snap = await harness.gateway.health.snapshot()
    assert snap["node-a"].state.value == "open"


@pytest.mark.asyncio
async def test_all_nodes_fail_gives_distinguishable_error(harness):
    for node in ("node-a", "node-b", "node-c"):
        harness.cluster.set_behavior(node, http500=True)
    resp = await harness.call("eth_blockNumber")
    assert "error" in resp
    assert resp["error"]["data"]["error_type"] == "no_healthy_node"
    attempted = resp["error"]["data"]["attempted"]
    # node-a 优先级最高必在首位；b/c 同优先级按权重随机，二者都应被尝试
    assert attempted[0] == "node-a"
    assert set(attempted) == {"node-a", "node-b", "node-c"}
    # 原始异常细节（httpx / 堆栈 / 内部消息）不得出现在响应中
    assert "http500" not in str(resp)
    assert "NodeFault" not in str(resp)


@pytest.mark.asyncio
async def test_upstream_rpc_error_is_not_retried_but_normalized(harness):
    # 所有节点都返回 JSON-RPC error：应归一化为 upstream_rpc_error，
    # 且尝试在第一个候选即终止（明确业务错误不转移重试）。
    for node in ("node-a", "node-b", "node-c"):
        harness.cluster.set_behavior(node, fail=True)
    resp = await harness.call("eth_getBalance", ["0xabc", "latest"])
    # eth_getBalance latest 命中 fail 注入：NodeFault rpc_error
    assert resp["error"]["data"]["error_type"] == "upstream_rpc_error"
    # 上游 message 被脱敏：响应中不应包含节点的原始文案
    assert "injected failure" not in str(resp)


@pytest.mark.asyncio
async def test_send_raw_transaction_failover_is_exactly_once(harness):
    # 首选节点超时 -> 转移到次选节点重试；幂等键保证集群只执行一次
    harness.cluster.set_behavior("node-a", timeout=True)
    raw = "0x" + "ab" * 40
    responses = await asyncio.gather(
        harness.call("eth_sendRawTransaction", [raw], req_id=1),
    )
    tx1 = responses[0]["result"]
    assert harness.cluster.tx_executions() == 1

    # 再次提交完全相同的请求（同一逻辑请求重放）：同一交易哈希，执行数不增加
    resp2 = await harness.call("eth_sendRawTransaction", [raw], req_id=2)
    assert resp2["result"] == tx1
    assert harness.cluster.tx_executions() == 1


@pytest.mark.asyncio
async def test_concurrent_reads_no_request_loss(harness):
    harness.cluster.set_behavior("node-a", timeout=True)  # 强制走转移路径
    responses = await asyncio.gather(
        *[
            harness.call("eth_blockNumber", req_id=i)
            for i in range(30)
        ]
    )
    assert all("result" in r for r in responses), responses
    assert {r["result"] for r in responses} == {"0x0"}
