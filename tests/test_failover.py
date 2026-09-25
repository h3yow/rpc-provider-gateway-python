"""节点故障转移：请求不丢失、不重复执行，错误类型可区分且原始异常不外泄。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import ErrorKind, GatewayError
from gateway.mock_node import MockNode
from gateway.upstream import FAILURE_THRESHOLD


async def test_failover_to_healthy_node(harness_factory):
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1  # a 优先被选中
    h.mock("a").fail_next = 1  # a 第一次调用失败

    result = await h.router.call("eth_sendRawTransaction", ["0xdeadbeef"], caller="c1")

    assert result.startswith("0xtx")
    assert h.mock("a").call_count == 1
    assert h.mock("b").call_count == 1  # 请求转移到 b，未丢失


async def test_non_idempotent_not_duplicated_on_timeout(harness_factory):
    """写请求超时（结果不确定）不得重试，避免重复执行。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1  # a 优先被选中
    h.mock("a").fail_next = 1
    h.mock("a").fail_kind = "timeout"

    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_sendRawTransaction", ["0xdeadbeef"], caller="c1")

    assert ei.value.kind == ErrorKind.UPSTREAM_TIMEOUT
    assert h.mock("a").call_count == 1
    assert h.mock("b").call_count == 0  # 未在 b 上重复执行


async def test_connection_failure_retries_even_for_writes(harness_factory):
    """连接失败说明请求未到达，写请求也可安全转移。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1  # a 优先被选中
    h.mock("a").fail_next = 1
    h.mock("a").fail_kind = "unavailable"

    result = await h.router.call("eth_sendRawTransaction", ["0xdeadbeef"], caller="c1")

    assert result.startswith("0xtx")
    assert h.mock("a").call_count == 1
    assert h.mock("b").call_count == 1


async def test_node_marked_unhealthy_after_threshold(harness_factory):
    # a 优先级更高（数值小），每次先被选；持续失败达到阈值后应被标记不健康
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1
    h.mock("a").fail_next = 100

    for _ in range(FAILURE_THRESHOLD):
        await h.router.call("eth_sendRawTransaction", ["0xdeadbeef"], caller="c1")

    node_a = h.registry.get("a")
    assert not node_a.healthy
    assert h.mock("a").call_count == FAILURE_THRESHOLD
    calls_before = h.mock("b").call_count
    # 后续请求应直接路由到 b，不再尝试 a
    await h.router.call("eth_sendRawTransaction", ["0xdeadbeef"], caller="c1")
    assert h.mock("a").call_count == FAILURE_THRESHOLD
    assert h.mock("b").call_count == calls_before + 1


async def test_all_upstreams_down_raises_normalized_error(harness_factory):
    h = harness_factory([MockNode("a"), MockNode("b")], max_retries=4)
    h.mock("a").fail_next = 100
    h.mock("b").fail_next = 100

    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_blockNumber", [], caller="c1")

    # 错误类型可区分，且不是原始 MockUpstreamError
    assert ei.value.kind in (ErrorKind.UPSTREAM_UNAVAILABLE,
                             ErrorKind.NO_HEALTHY_UPSTREAM)
    assert "mock node" not in ei.value.message  # 原始异常内容不外泄


async def test_concurrent_failover_no_lost_no_duplicated(harness_factory):
    """并发写请求下节点故障：每个请求恰好执行一次，结论确定。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.mock("a").fail_next = 1000  # a 全程连接失败

    n = 20
    results = await asyncio.gather(*(
        h.router.call("eth_sendRawTransaction", [f"0x{i:016x}"], caller=f"c{i}")
        for i in range(n)
    ))

    assert len(set(results)) == n          # 每笔交易哈希唯一，无重复执行
    assert h.mock("b").call_count == n     # 全部转移到 b，无丢失
    total = h.mock("a").call_count + h.mock("b").call_count
    assert total == n + h.mock("a").call_count  # b 上无重复


async def test_error_serialization_distinguishable(harness_factory):
    h = harness_factory([MockNode("a")], max_retries=1)
    h.mock("a").fail_next = 1
    h.mock("a").fail_kind = "timeout"

    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_blockNumber", [], caller="c1")

    payload = ei.value.to_jsonrpc_error()
    assert payload["data"]["kind"] == ErrorKind.UPSTREAM_TIMEOUT.value
    assert payload["data"]["retryable"] is True
    assert payload["code"] == -32001
