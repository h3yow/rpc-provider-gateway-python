"""运行时上游热变更：增/删/改即时生效，新节点先对齐，在途请求按快照收尾。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import ErrorKind, GatewayError
from gateway.mock_node import MockNode
from gateway.upstream import UpstreamNode


async def test_priority_change_takes_effect_immediately(harness_factory):
    h = harness_factory([MockNode("a"), MockNode("b")])
    # 初始 a 优先
    h.registry.get("b").priority = 1
    await h.router.call("eth_sendRawTransaction", ["0x1"], caller="c1")
    assert h.mock("a").call_count == 1

    # 运行时把 b 提到更高优先级（数值更小），对之后请求立即生效
    h.registry.update_node("b", priority=-1)
    for _ in range(3):
        await h.router.call("eth_sendRawTransaction", ["0x1"], caller="c1")
    assert h.mock("b").call_count == 3


async def test_weight_change_reflected_in_status(harness_factory):
    h = harness_factory([MockNode("a"), MockNode("b")])
    h.registry.update_node("a", weight=9)
    assert h.registry.get("a").weight == 9
    assert h.registry.revision == 3  # 注册两次 + 一次变更


async def test_remove_node_stops_routing_but_in_flight_finishes(harness_factory):
    h = harness_factory([MockNode("a")])
    await h.router.refresh_chain_head()

    # 让 a 的下一次调用阻塞，构造“在途”窗口
    gate = asyncio.Event()
    original = h.mock("a").handle

    async def slow_handle(method, params):
        await gate.wait()
        return await original(method, params)

    h.mock("a").handle = slow_handle
    in_flight = asyncio.create_task(
        h.router.call("eth_getBlockByHash", ["0xzzz"], caller="c1"))
    await asyncio.sleep(0)  # 确保请求已进入并选中 a

    # 运行时下线 a：之后的新请求无健康节点；在途请求不受影响
    assert h.registry.remove_node("a") is True
    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_blockNumber", [], caller="c2")
    assert ei.value.kind == ErrorKind.NO_HEALTHY_UPSTREAM

    gate.set()
    result = await in_flight  # 在途请求按进入时的快照正常收尾，不被取消
    assert result["node"] == "a"


async def test_new_node_warms_until_head_aligned(harness_factory):
    h = harness_factory([MockNode("a", chain_head=10)])
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 10

    # 运行时新增节点 c，初始未报告链头：不参与选路、不参与共识
    c_mock = MockNode("c", chain_head=10)
    h.mocks["c"] = c_mock
    h.registry.add_node(UpstreamNode("c", "mock://c"))
    status = h.registry.status()
    c_status = next(n for n in status["nodes"] if n["name"] == "c")
    assert c_status["head_reported"] is False and c_status["aligned"] is False

    # 仅 a、c 两个节点时，c 未对齐：流量仍全部由 a 承接
    for _ in range(5):
        await h.router.call("eth_sendRawTransaction", ["0x1"], caller="c1")
    assert c_mock.call_count == 0

    # c 报告链头并追平后才放量（通过后台轮询学习）
    await h.router.refresh_chain_head()
    c_status = next(n for n in h.registry.status()["nodes"] if n["name"] == "c")
    assert c_status["aligned"] is True


async def test_new_node_behind_consensus_not_polluting(harness_factory):
    h = harness_factory([MockNode("a", chain_head=20), MockNode("b", chain_head=20)])
    await h.router.refresh_chain_head()
    # 新增一个落后节点
    c_mock = MockNode("c", chain_head=15)
    h.mocks["c"] = c_mock
    h.registry.add_node(UpstreamNode("c", "mock://c"))
    await h.router.refresh_chain_head()

    # 口径仍是 20，不因新加入的落后者改变
    assert h.registry.consensus_chain_head() == 20
    c_status = next(n for n in h.registry.status()["nodes"] if n["name"] == "c")
    assert c_status["aligned"] is False


async def test_concurrent_requests_interleaved_with_config_change(harness_factory):
    """热变更与并发请求交织：每个请求结论按其进入时快照，无重复执行、无错乱。"""
    h = harness_factory([MockNode("a"), MockNode("b")])
    await h.router.refresh_chain_head()
    h.registry.get("b").priority = 1
    n = 40
    txs = [f"0x{i:016x}" for i in range(n)]

    async def send(tx: str) -> str:
        return await h.router.call("eth_sendRawTransaction", [tx], caller="c1")

    requests = [asyncio.create_task(send(tx)) for tx in txs]
    # 请求陆续进入的过程中做多次热变更
    await asyncio.sleep(0)
    h.registry.update_node("b", priority=0, weight=5)
    await asyncio.sleep(0)
    h.registry.update_node("a", weight=2)
    h.registry.remove_node("b")
    h.registry.add_node(UpstreamNode("b", "mock://b"))
    results = await asyncio.gather(*requests, return_exceptions=True)

    # 成功的结果各自唯一：没有重复执行；失败只能是“下线窗口无健康节点”的可重试错误
    succeeded = [r for r in results if not isinstance(r, BaseException)]
    for r in results:
        if isinstance(r, BaseException):
            assert isinstance(r, GatewayError)
            assert r.kind == ErrorKind.NO_HEALTHY_UPSTREAM
    # 同一笔交易不会在两个节点各执行一次：所有成功哈希唯一
    assert len(set(succeeded)) == len(succeeded)
    # mock 节点记录的交易集合与成功结果一致
    handled = set()
    for m in (h.mock("a"), h.mock("b")):
        handled |= set(m._receipts.keys())
    assert set(succeeded) <= handled


async def test_remove_unknown_node_is_idempotent(harness_factory):
    h = harness_factory([MockNode("a")])
    assert h.registry.remove_node("nope") is False
    assert h.registry.remove_node("a") is True
    assert h.registry.remove_node("a") is False
