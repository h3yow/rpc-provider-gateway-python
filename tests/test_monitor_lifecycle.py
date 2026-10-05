"""HealthMonitor 后台任务：start/stop 幂等，摘除节点能被自动探活并回补。"""
from __future__ import annotations

import asyncio

from gateway.health_monitor import HealthMonitor
from gateway.mock_node import MockNode
from gateway.upstream import FAILURE_THRESHOLD


async def test_monitor_auto_recovers_ejected_node(harness_factory):
    h = harness_factory([MockNode("a", chain_head=4), MockNode("b", chain_head=4)])
    h.registry.get("b").priority = 1
    mon = HealthMonitor(h.registry, h.router.transport,
                        poll_interval=0.02, base_backoff=0.01,
                        max_backoff=0.05, probe_timeout=1.0)
    h.router.monitor = mon
    await h.router.start()
    try:
        await h.router.refresh_chain_head()
        # 摘除 a
        h.mock("a").fail_next = 100
        for _ in range(FAILURE_THRESHOLD):
            await h.router.call("eth_sendRawTransaction", ["0xaa"], caller="c1")
        assert not h.registry.get("a").healthy

        # a 恢复：后台探测器应在若干周期内自动把它放回
        h.mock("a").fail_next = 0
        deadline = 3.0
        while deadline > 0 and not h.registry.get("a").healthy:
            await asyncio.sleep(0.02)
            deadline -= 0.02
        assert h.registry.get("a").healthy, "摘除节点未被自动恢复"
    finally:
        await h.router.stop()
    # 停止后任务退出
    assert mon._task is None


async def test_monitor_start_stop_idempotent(harness_factory):
    h = harness_factory([MockNode("a")])
    mon = HealthMonitor(h.registry, h.router.transport, poll_interval=10)
    await mon.start()
    await mon.start()  # 重复 start 不产生多个任务
    assert mon._task is not None
    await mon.stop()
    await mon.stop()  # 重复 stop 安全
    assert mon._task is None
