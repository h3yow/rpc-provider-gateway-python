"""节点自愈：摘除后自动探活、退避、确认恢复后按原层级回补，在途请求不受影响。"""
from __future__ import annotations

import asyncio

from gateway.errors import ErrorKind, GatewayError
from gateway.mock_node import MockNode
from gateway.recovery import RecoveryConfig
from gateway.upstream import FAILURE_THRESHOLD, NodeState


def _recovery_config(**overrides) -> RecoveryConfig:
    base = dict(initial_interval=0.01, max_interval=0.05, backoff_multiplier=2.0,
                successes_needed=2, probe_timeout=1.0, tick=0.01)
    base.update(overrides)
    return RecoveryConfig(**base)


async def _force_down(h, name: str) -> None:
    """通过连续失败把节点摘除（与真实流量路径一致）。"""
    h.mock(name).fail_next = 100
    for _ in range(FAILURE_THRESHOLD):
        try:
            await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
        except GatewayError:
            pass  # 无其他健康节点时调用本身会失败，此处只关心摘除
    assert h.registry.get(name).state == NodeState.DOWN


async def test_down_node_recovers_and_rejoins_original_tier(harness_factory):
    """摘除 -> 探测确认恢复 -> 按原有优先级/权重放回选路。"""
    h = harness_factory([MockNode("a"), MockNode("b")],
                        recovery_config=_recovery_config())
    h.registry.get("b").priority = 1  # a 原本是高优先级层
    await _force_down(h, "a")
    calls_a = h.mock("a").call_count

    h.mock("a").fail_next = 0  # 节点实际已恢复
    for _ in range(2):  # successes_needed=2
        assert await h.router.prober.probe_node("a") is True

    node_a = h.registry.get("a")
    assert node_a.state == NodeState.ACTIVE
    assert node_a.priority == 0 and node_a.weight == 1  # 原层级与权重保留
    # 恢复后重新承接流量（优先级 0 层只有 a）
    await h.router.call("eth_sendRawTransaction", ["0x02"], caller="c1")
    assert h.mock("a").call_count > calls_a


async def test_recovery_requires_consecutive_successes(harness_factory):
    """单次探测成功不足以放量；中途失败会重置连续成功计数。"""
    h = harness_factory([MockNode("a"), MockNode("b")],
                        recovery_config=_recovery_config(successes_needed=2))
    h.registry.get("b").priority = 1
    await _force_down(h, "a")
    h.mock("a").fail_next = 0

    assert await h.router.prober.probe_node("a") is True
    assert h.registry.get("a").state == NodeState.DOWN  # 1/2，未放量

    # 中途一次失败：连续计数清零，退避生效
    h.mock("a").fail_next = 1
    assert await h.router.prober.probe_node("a") is False
    assert await h.router.prober.probe_node("a") is True
    assert h.registry.get("a").state == NodeState.DOWN  # 重新计 1/2
    assert await h.router.prober.probe_node("a") is True
    assert h.registry.get("a").state == NodeState.ACTIVE  # 2/2，放量


async def test_probe_backoff_limits_pressure_on_weak_node(harness_factory):
    """探测失败按指数退避：未到期的节点不会被反复探测。"""
    h = harness_factory([MockNode("a")],
                        recovery_config=_recovery_config(initial_interval=100.0,
                                                         max_interval=400.0))
    await _force_down(h, "a")
    calls = h.mock("a").call_count

    # 首次调度：初始间隔 100s，未到期不探测
    assert await h.router.prober.probe_due() == []
    assert h.mock("a").call_count == calls

    # 强制探测一次（失败）：退避到 200s，随后 probe_due 仍不触发
    assert await h.router.prober.probe_node("a") is False
    assert h.mock("a").call_count == calls + 1
    assert await h.router.prober.probe_due() == []
    assert h.mock("a").call_count == calls + 1  # 没有额外探测压力


async def test_no_normal_traffic_before_recovery_confirmed(harness_factory):
    """恢复判定完成前，正常流量绝不引向 DOWN 节点（即使探测已成功一次）。"""
    h = harness_factory([MockNode("a"), MockNode("b")],
                        recovery_config=_recovery_config(successes_needed=3))
    h.registry.get("b").priority = 1
    await _force_down(h, "a")
    h.mock("a").fail_next = 0
    calls_a = h.mock("a").call_count

    await h.router.prober.probe_node("a")  # 1/3 次确认
    for _ in range(5):
        await h.router.call("eth_sendRawTransaction", ["0x03"], caller="c1")
    # a 只被探测触及，正常流量全部走 b
    assert h.mock("a").call_count == calls_a + 1


async def test_in_flight_request_conclusion_unaffected_by_recovery(harness_factory):
    """恢复放量发生在请求处理中途：该请求仍按进入时的选路结论收尾。"""
    h = harness_factory([MockNode("a"), MockNode("b")],
                        recovery_config=_recovery_config())
    h.registry.get("b").priority = 1
    await _force_down(h, "a")

    h.mock("b").latency = 0.2
    task = asyncio.create_task(
        h.router.call("eth_getBlockByHash", ["0xhash1"], caller="c1"))
    await asyncio.sleep(0.05)  # 请求已派发给 b，正在处理
    # a 在请求处理中途恢复并重新放量
    h.mock("a").fail_next = 0
    await h.router.prober.probe_node("a")
    await h.router.prober.probe_node("a")
    assert h.registry.get("a").state == NodeState.ACTIVE

    result = await task
    assert result["node"] == "b"  # 结论不被中途的恢复改变


async def test_background_prober_recovers_node_automatically(harness_factory):
    """后台循环自动完成探活与回补，无需人工触发。"""
    h = harness_factory([MockNode("a"), MockNode("b")],
                        recovery_config=_recovery_config())
    h.registry.get("b").priority = 1
    await _force_down(h, "a")
    h.mock("a").fail_next = 0

    await h.router.prober.start()
    try:
        for _ in range(200):
            if h.registry.get("a").state == NodeState.ACTIVE:
                break
            await asyncio.sleep(0.02)
        assert h.registry.get("a").state == NodeState.ACTIVE
    finally:
        await h.router.prober.stop()


async def test_probes_bypass_quota_and_cache(harness_factory):
    """恢复探测是独立通道：不消耗调用方配额、不写缓存。"""
    from gateway.quota import QuotaConfig
    h = harness_factory([MockNode("a")],
                        quota_config=QuotaConfig(rate_per_second=0.001, burst=5,
                                                 max_concurrent=10),
                        recovery_config=_recovery_config())
    node = h.registry.get("a")
    for _ in range(FAILURE_THRESHOLD):  # 直接上报失败，不经过正常流量
        await h.registry.report_failure(
            node, GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE, "down"))
    assert node.state == NodeState.DOWN

    await h.router.prober.probe_node("a")
    await h.router.prober.probe_node("a")
    assert node.state == NodeState.ACTIVE

    stats = await h.quota.stats("anonymous")
    assert stats["tokens"] == 5.0      # 探测未消耗任何调用方配额
    assert await h.cache.size() == 0   # 探测不写缓存
