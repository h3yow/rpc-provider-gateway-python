"""运行时上游热变更：增删改立即生效、在途请求按进入时结论收尾、新节点对齐前不污染。"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from gateway.errors import GatewayError
from gateway.mock_node import MockNode
from gateway.recovery import RecoveryConfig
from gateway.server import create_app
from gateway.upstream import NodeState, UpstreamNode


def _recovery_config() -> RecoveryConfig:
    return RecoveryConfig(initial_interval=0.01, max_interval=0.05,
                          successes_needed=2, probe_timeout=1.0, tick=0.01)


async def test_hot_add_waits_for_alignment_then_serves(harness_factory):
    """热加入的节点先处于 JOINING：不承接流量、不计入链头口径；探测对齐后才放量。"""
    h = harness_factory([MockNode("a", chain_head=20)],
                        recovery_config=_recovery_config())
    await h.router.refresh_chain_head()
    assert h.registry.unified_chain_head() == 20

    mock_c = MockNode("c", chain_head=0)  # 新节点链头远远落后
    h.mocks["c"] = mock_c
    await h.registry.add_node(UpstreamNode(name="c", url="mock://c", priority=0))
    assert h.registry.get("c").state == NodeState.JOINING

    # JOINING 期间：正常流量不会打过去，链头口径不被拉低
    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    assert mock_c.call_count == 0
    assert h.registry.unified_chain_head() == 20

    # 对齐探测成功后放量；其落后的链头（0）不会拉低口径（棘轮 + 多数派）
    assert await h.router.prober.probe_node("c") is True
    assert h.registry.get("c").state == NodeState.ACTIVE
    assert h.registry.unified_chain_head() == 20

    # HEAD_BOUND 请求仍只去已同步的 a；落后但已激活的 c 被跳过
    r = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r == hex(10**18 + 20)
    assert mock_c.call_count == 1  # 仅有对齐探测那一次


async def test_hot_remove_takes_effect_immediately(harness_factory):
    """下线节点立即退出后续选路；重复下线报错。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1

    await h.registry.remove_node("a")
    for _ in range(3):
        await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    assert h.mock("a").call_count == 0
    assert h.mock("b").call_count == 3

    with pytest.raises(GatewayError):
        await h.registry.remove_node("a")


async def test_hot_update_priority_weight_immediate(harness_factory):
    """优先级/权重调整对之后的选路立即生效。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1

    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    assert h.mock("a").call_count == 1  # 调整前 a 优先

    await h.registry.update_node("b", priority=-1)  # b 升为最高优先级
    await h.router.call("eth_sendRawTransaction", ["0x02"], caller="c1")
    assert h.mock("b").call_count == 1
    assert h.mock("a").call_count == 1  # a 未再被选中

    await h.registry.update_node("a", priority=-1, weight=0)
    await h.registry.update_node("b", weight=5)
    await h.router.call("eth_sendRawTransaction", ["0x03"], caller="c1")
    # 同层 a 权重 0、b 权重 5：必然选 b
    assert h.mock("b").call_count == 2

    with pytest.raises(GatewayError):
        await h.registry.update_node("missing", priority=0)


async def test_in_flight_request_finishes_on_removed_node(harness_factory):
    """请求处理中途节点被下线：该请求仍按进入时的选路结论收尾。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.get("b").priority = 1
    h.mock("a").latency = 0.2

    task = asyncio.create_task(
        h.router.call("eth_getBlockByHash", ["0xhash9"], caller="c1"))
    await asyncio.sleep(0.05)  # 请求已派发给 a
    await h.registry.remove_node("a")

    result = await task
    assert result["node"] == "a"  # 在途请求结论不被变更影响
    # 之后的请求只去 b
    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    assert h.mock("b").call_count == 1


async def test_concurrent_requests_during_removal_no_loss_no_duplication(harness_factory):
    """并发写请求与节点下线交织：每个请求恰好执行一次，不丢失、不重复。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.mock("a").latency = 0.05
    h.mock("b").latency = 0.05

    n = 20
    tasks = [asyncio.create_task(
        h.router.call("eth_sendRawTransaction", [f"0x{i:016x}"], caller=f"c{i}"))
        for i in range(n)]
    await asyncio.sleep(0.01)  # 部分请求在途时下线 a
    await h.registry.remove_node("a")
    results = await asyncio.gather(*tasks)

    assert len(set(results)) == n  # 交易哈希唯一：无重复执行
    total = h.mock("a").call_count + h.mock("b").call_count
    assert total == n              # 每次请求恰好执行一次：无丢失


async def test_concurrent_requests_during_reconfig_all_succeed(harness_factory):
    """并发读请求与权重/优先级变更交织：全部成功且结果一致。"""
    h = harness_factory([MockNode("a", chain_head=7), MockNode("b", chain_head=7)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()

    async def reader():
        return await asyncio.gather(*(
            h.router.call("eth_getBalance", ["0xaddr", "latest"], caller=f"c{i}")
            for i in range(20)))

    async def reconfig():
        for _ in range(5):
            await h.registry.update_node("a", priority=1)
            await h.registry.update_node("b", priority=0)
            await h.registry.update_node("a", priority=0)
            await h.registry.update_node("b", priority=1)

    results, _ = await asyncio.gather(reader(), reconfig())
    assert len(set(results)) == 1  # 同链头下结论一致，与变更交织无关


async def test_admin_hot_change_endpoints(harness_factory):
    """管理端点：增/改/删节点与链头口径状态可见。"""
    h = harness_factory([MockNode("a", chain_head=5)],
                        recovery_config=_recovery_config())
    app = create_app(h.router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as client:
        # 热加入
        resp = await client.post("/upstreams", json={
            "name": "b", "url": "http://127.0.0.1:9999/", "priority": 1,
            "weight": 2})
        assert resp.status_code == 201
        assert resp.json()["state"] == "joining"
        # 重复加入冲突
        resp = await client.post("/upstreams", json={"name": "b", "url": "u"})
        assert resp.status_code == 409
        # 热调整
        resp = await client.patch("/upstreams/b", json={"priority": 0, "weight": 3})
        assert resp.status_code == 200
        assert resp.json()["priority"] == 0 and resp.json()["weight"] == 3
        # 链头口径状态可见
        resp = await client.get("/chainhead")
        body = resp.json()
        assert body["unified_head"] == 0  # 未报告过链头
        assert {n["name"] for n in body["nodes"]} == {"a", "b"}
        # 节点列表包含状态字段
        resp = await client.get("/upstreams")
        states = {u["name"]: u["state"] for u in resp.json()}
        assert states == {"a": "active", "b": "joining"}
        # 热删除
        resp = await client.delete("/upstreams/b")
        assert resp.status_code == 200
        resp = await client.delete("/upstreams/b")
        assert resp.status_code == 404
        resp = await client.get("/upstreams")
        assert [u["name"] for u in resp.json()] == ["a"]
