"""节点自动恢复：摘除后退避探活、追平后回补原优先级/权重层。"""
from __future__ import annotations

import pytest

from gateway.health_monitor import HealthMonitor
from gateway.mock_node import MockNode
from gateway.upstream import FAILURE_THRESHOLD, UpstreamNode


def _monitor(harness, **kwargs):
    return HealthMonitor(harness.registry, harness.router.transport, **kwargs)


async def test_ejected_node_probed_with_backoff_and_restored(harness_factory):
    h = harness_factory([MockNode("a", chain_head=5), MockNode("b", chain_head=5)])
    h.registry.get("b").priority = 1
    mon = _monitor(h, poll_interval=0.01, base_backoff=0.01, max_backoff=0.05)
    # 先确立共识口径
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 5

    # a 持续失败直到被摘除
    h.mock("a").fail_next = 100
    for _ in range(FAILURE_THRESHOLD):
        await h.router.call("eth_sendRawTransaction", ["0xaa"], caller="c1")
    node_a = h.registry.get("a")
    assert not node_a.healthy
    calls_at_eject = h.mock("a").call_count

    # a 仍然挂着：探测失败，退避递增，且不接收正常流量
    stats = await mon.poll_once()
    assert stats["probed"] == 1 and stats["restored"] == 0
    assert node_a.probe_attempts == 1
    first_next = node_a.probe_next_at
    await mon.poll_once()  # 未到退避时刻：不应重复探测
    assert node_a.probe_attempts == 1
    assert h.mock("a").call_count == calls_at_eject + 1  # 只有一次探活请求

    # a 恢复（链头与共识一致）：下一轮探活后放回
    h.mock("a").fail_next = 0
    node_a.probe_next_at = 0.0  # 让探活立即到期
    stats = await mon.poll_once()
    assert stats["restored"] == 1
    assert node_a.healthy
    assert node_a.priority == 0 and node_a.weight == 1  # 回到原来的层和权重

    # 恢复后正常流量重新选到 a（其优先级更高）
    calls_b_before = h.mock("b").call_count
    await h.router.call("eth_blockNumber", [], caller="c1")
    assert h.mock("a").call_count > calls_at_eject + 1
    assert h.mock("b").call_count == calls_b_before


async def test_backoff_grows_until_cap(harness_factory):
    h = harness_factory([MockNode("a"), MockNode("b")])
    mon = _monitor(h, poll_interval=0.01, base_backoff=0.1, max_backoff=0.4)
    assert mon._backoff_for(0) == pytest.approx(0.1)
    assert mon._backoff_for(1) == pytest.approx(0.2)
    assert mon._backoff_for(2) == pytest.approx(0.4)
    assert mon._backoff_for(5) == pytest.approx(0.4)  # 封顶，不打垮弱节点


async def test_restored_behind_node_kept_ejected_until_caught_up(harness_factory):
    h = harness_factory([MockNode("a", chain_head=8), MockNode("b", chain_head=8)])
    h.registry.get("b").priority = 1
    mon = _monitor(h, poll_interval=0.01)
    await h.router.refresh_chain_head()
    node_a = h.registry.get("a")

    # 摘除 a
    h.mock("a").fail_next = 100
    for _ in range(FAILURE_THRESHOLD):
        await h.router.call("eth_sendRawTransaction", ["0xaa"], caller="c1")
    assert not node_a.healthy

    # 集群链头前进到 10（仅健康的 b 推进口径），a 恢复但停在 8：落后，不放量
    h.mock("b").advance_head(2)
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 10
    h.mock("a").fail_next = 0
    stats = await mon.poll_once()
    assert stats["behind"] == 1 and stats["restored"] == 0
    assert not node_a.healthy
    # 状态可见 a 未对齐
    a_status = next(n for n in h.registry.status()["nodes"] if n["name"] == "a")
    assert a_status["aligned"] is False

    # 追平共识后回补
    h.mock("a").advance_head(2)
    node_a.probe_next_at = 0.0
    stats = await mon.poll_once()
    assert stats["restored"] == 1
    assert node_a.healthy


async def test_traffic_not_routed_before_recovery_confirmed(harness_factory):
    """恢复判定完成前，在途的后续请求绝不能被引到摘除节点。"""
    h = harness_factory([MockNode("a", chain_head=3), MockNode("b", chain_head=3)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()
    h.mock("a").fail_next = 100
    for _ in range(FAILURE_THRESHOLD):
        await h.router.call("eth_sendRawTransaction", ["0xaa"], caller="c1")
    assert not h.registry.get("a").healthy

    # 节点 a 此刻恢复，但尚未经过任何探活：仍然不可选
    h.mock("a").fail_next = 0
    calls_b = h.mock("b").call_count
    await h.router.call("eth_blockNumber", [], caller="c1")
    assert h.mock("b").call_count == calls_b + 1
    # a 只承担了摘除前的失败尝试（含每次故障转移），恢复后未被引流
    ejected_calls = h.mock("a").call_count
    await h.router.call("eth_sendRawTransaction", ["0xbb"], caller="c1")
    assert h.mock("a").call_count == ejected_calls  # 未经探活确认不放量
