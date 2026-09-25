"""缓存：语义区分、链头失效、并发读写一致性。"""
from __future__ import annotations

import asyncio

from gateway.cache import CachePolicy, classify
from gateway.mock_node import MockNode


def test_classify_semantics():
    assert classify("eth_getBlockByHash", ["0xabc"]) is CachePolicy.CACHEABLE
    assert classify("eth_getBlockByNumber", ["0x5", False]) is CachePolicy.CACHEABLE
    assert classify("eth_getBlockByNumber", ["latest", False]) is CachePolicy.HEAD_BOUND
    assert classify("eth_getBalance", ["0xaddr", "latest"]) is CachePolicy.HEAD_BOUND
    assert classify("eth_getBalance", ["0xaddr", "0x9"]) is CachePolicy.CACHEABLE
    assert classify("eth_blockNumber", []) is CachePolicy.HEAD_BOUND
    assert classify("eth_sendRawTransaction", ["0x"]) is CachePolicy.NON_CACHEABLE
    assert classify("eth_subscribe", ["newHeads"]) is CachePolicy.NON_CACHEABLE


async def test_cacheable_method_served_from_cache(harness_factory):
    h = harness_factory([MockNode("a")])

    r1 = await h.router.call("eth_getBlockByHash", ["0xabc"], caller="c1")
    r2 = await h.router.call("eth_getBlockByHash", ["0xabc"], caller="c1")

    assert r1 == r2
    assert h.mock("a").call_count == 1  # 第二次命中缓存


async def test_head_bound_invalidated_on_head_advance(harness_factory):
    h = harness_factory([MockNode("a")])

    r1 = await h.router.call("eth_blockNumber", [], caller="c1")
    assert r1 == "0x0"
    # 命中缓存，不再访问上游
    await h.router.call("eth_blockNumber", [], caller="c1")
    assert h.mock("a").call_count == 1

    # 链头推进：网关观测到新链头后，旧结果必须失效，不得返回过期数据
    h.mock("a").advance_head(5)
    await h.router.refresh_chain_head()
    r2 = await h.router.call("eth_blockNumber", [], caller="c1")
    assert r2 == "0x5"
    assert h.mock("a").call_count == 3  # 首次查询 + 链头轮询 + 失效后重查


async def test_pinned_height_result_survives_head_advance(harness_factory):
    """显式高度的查询结果已确定，链头推进后仍应命中缓存。"""
    h = harness_factory([MockNode("a", chain_head=10)])

    r1 = await h.router.call("eth_getBlockByNumber", ["0x5", False], caller="c1")
    assert r1["number"] == "0x5"
    h.mock("a").advance_head(10)
    # 同步链头到注册表
    await h.router.call("eth_blockNumber", [], caller="c1")

    r2 = await h.router.call("eth_getBlockByNumber", ["0x5", False], caller="c1")
    assert r2 == r1
    assert h.mock("a").call_count == 2  # 仅首次查询 + 链头同步


async def test_non_cacheable_always_hits_upstream(harness_factory):
    h = harness_factory([MockNode("a")])

    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")

    assert h.mock("a").call_count == 2


async def test_concurrent_readers_consistent_results(harness_factory):
    """并发读同一 head-bound 方法：所有调用方得到一致结果，无过期读。"""
    h = harness_factory([MockNode("a", chain_head=7)])
    # 先建立链头认知
    await h.router.call("eth_blockNumber", [], caller="warmup")

    results = await asyncio.gather(*(
        h.router.call("eth_getBalance", ["0xaddr", "latest"], caller=f"c{i}")
        for i in range(30)
    ))

    assert len(set(results)) == 1  # 所有并发读结论一致
    assert results[0] == hex(10**18 + 7)


async def test_no_stale_read_under_concurrent_head_advance(harness_factory):
    """链头推进与并发读交错：任何成功返回都不得落后于已确认链头。"""
    h = harness_factory([MockNode("a", chain_head=0)])

    async def reader(observed: list[int]):
        for _ in range(10):
            r = await h.router.call("eth_blockNumber", [], caller="reader")
            observed.append(int(r, 16))

    async def advancer():
        for _ in range(5):
            h.mock("a").advance_head(1)
            await h.router.refresh_chain_head()  # 网关观测新链头
            await asyncio.sleep(0)  # 让出调度，制造交错

    observed: list[int] = []
    await asyncio.gather(reader(observed), advancer())

    # 单调性：观测到的链头序列不得回退（不允许读到比先前更旧的链头）
    assert observed == sorted(observed), f"stale read detected: {observed}"


async def test_cache_not_polluted_across_nodes(harness_factory):
    """节点切换后缓存结论不得互相矛盾：同链头下同 key 结果一致。"""
    h = harness_factory([MockNode("a", chain_head=3), MockNode("b", chain_head=3)])
    h.registry.get("b").priority = 1

    r1 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    # a 故障，转移到 b；同链头下结果必须一致
    h.mock("a").fail_next = 100
    h.cache._entries.clear()  # 强制重新查询（模拟缓存未命中路径）
    r2 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")

    assert r1 == r2
