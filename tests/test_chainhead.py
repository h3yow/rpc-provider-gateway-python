"""统一链头口径：多数派确认、棘轮单调、落后节点不污染最新状态与缓存。"""
from __future__ import annotations

from gateway.mock_node import MockNode


async def test_single_node_lead_does_not_invalidate_valid_cache(harness_factory):
    """个别节点单方面领先不推高统一口径，仍有效的缓存不被判废、不回源。"""
    h = harness_factory([MockNode("a", chain_head=10), MockNode("b", chain_head=10)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()  # 双方报告 10，统一口径 = 10
    assert h.registry.unified_chain_head() == 10

    r1 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")

    # a 单方面领先到 15：法定人数（2/2）未确认，口径保持 10
    h.mock("a").advance_head(5)
    await h.router.refresh_chain_head()
    assert h.registry.unified_chain_head() == 10
    calls_a = h.mock("a").call_count

    # 缓存条目（盖章 10）仍然有效：直接命中，不回源
    r2 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r2 == r1
    assert h.mock("a").call_count == calls_a


async def test_unified_head_advances_only_when_quorum_catches_up(harness_factory):
    """落后节点追平后口径才前进，此时旧条目才失效。"""
    h = harness_factory([MockNode("a", chain_head=10), MockNode("b", chain_head=10)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()
    r1 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")

    h.mock("a").advance_head(5)  # a=15, b=10：口径不动
    await h.router.refresh_chain_head()
    assert h.registry.unified_chain_head() == 10

    h.mock("b").advance_head(5)  # b 追平到 15：多数派确认，口径前进
    await h.router.refresh_chain_head()
    assert h.registry.unified_chain_head() == 15

    # 旧条目（盖章 10）此刻才失效，重新回源拿到新链头下的结果
    r2 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r2 != r1
    assert r2 == hex(10**18 + 15)


async def test_lagging_node_skipped_for_head_bound_requests(harness_factory):
    """HEAD_BOUND 请求只在已追平统一口径的节点中选择，落后节点被跳过。"""
    h = harness_factory([MockNode("a", chain_head=20), MockNode("b", chain_head=20),
                         MockNode("c", chain_head=10)])
    await h.router.refresh_chain_head()  # 口径 = 20（a、b 确认），c 落后
    assert h.registry.unified_chain_head() == 20
    calls_c = h.mock("c").call_count

    # 三个节点同优先级，若不看同步状态 c 有 1/3 概率被选中
    for i in range(10):
        r = await h.router.call("eth_getBalance", [f"0xaddr{i}", "latest"],
                                caller="c1")
        assert r == hex(10**18 + 20)  # 结果始终来自已同步节点
    assert h.mock("c").call_count == calls_c  # c 未被正常流量触及


async def test_lagging_node_result_not_cached_as_latest(harness_factory):
    """降级场景（无同步节点）：落后节点的结果可以返回，但不得当作最新状态缓存。"""
    h = harness_factory([MockNode("a", chain_head=20), MockNode("b", chain_head=20),
                         MockNode("c", chain_head=10)])
    await h.router.refresh_chain_head()
    assert h.registry.unified_chain_head() == 20
    # 摘除同步节点，只剩落后的 c；口径因棘轮保持 20
    await h.registry.remove_node("a")
    await h.registry.remove_node("b")
    assert h.registry.unified_chain_head() == 20

    calls_c = h.mock("c").call_count
    r1 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r1 == hex(10**18 + 10)  # 可用性：结果照常返回
    assert await h.cache.size() == 0  # 但未作为最新状态写入缓存
    # 同样的请求再次回源，而不是读到落后节点留下的“最新”缓存
    r2 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r2 == r1
    assert h.mock("c").call_count == calls_c + 2


async def test_valid_cache_survives_lagging_node_catch_up(harness_factory):
    """落后节点追平、口径前进后，盖章仍有效的条目继续复用，不重复回源。"""
    h = harness_factory([MockNode("a", chain_head=20), MockNode("b", chain_head=10)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()  # 口径 = 10（b 未确认 20）
    assert h.registry.unified_chain_head() == 10

    # a 回答 eth_blockNumber=0x14，条目按响应提取的高度盖章 20
    r1 = await h.router.call("eth_blockNumber", [], caller="c1")
    assert r1 == "0x14"

    # b 追平到 20：口径前进到 20；盖章 20 的条目仍然有效
    h.mock("b").advance_head(10)
    await h.router.refresh_chain_head()
    assert h.registry.unified_chain_head() == 20
    calls_a = h.mock("a").call_count

    r2 = await h.router.call("eth_blockNumber", [], caller="c1")
    assert r2 == "0x14"                       # 复用仍有效的结果
    assert h.mock("a").call_count == calls_a  # 没有重复回源


async def test_chain_head_status_exposes_basis(harness_factory):
    """口径与其判断依据（法定人数、各节点高度、同步状态）在状态里可见。"""
    h = harness_factory([MockNode("a", chain_head=20), MockNode("b", chain_head=20),
                         MockNode("c", chain_head=10)])
    await h.router.refresh_chain_head()

    status = h.registry.chain_head_status()
    assert status["unified_head"] == 20
    assert status["quorum"] == 2
    assert status["active_nodes"] == 3
    nodes = {n["name"]: n for n in status["nodes"]}
    assert nodes["a"]["synced"] is True and nodes["a"]["counted_in_quorum"] is True
    assert nodes["c"]["synced"] is False     # 落后节点在状态中可辨识
    assert nodes["c"]["chain_head"] == 10
