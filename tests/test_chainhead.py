"""统一链头口径下的缓存一致性：落后不外用、领先不判废、追平可复用。"""
from __future__ import annotations

import asyncio

from gateway.mock_node import MockNode


async def test_consensus_is_majority_head(harness_factory):
    # 三节点：两个在 10，一个落后在 7 -> 多数口径 10
    h = harness_factory([MockNode("a", chain_head=10),
                         MockNode("b", chain_head=10),
                         MockNode("c", chain_head=7)])
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 10
    st = h.registry.status()
    assert st["quorum"] == 2 and st["head_basis"] == [10, 10, 7]
    # 落后节点不对齐，不参与选路
    c = next(n for n in st["nodes"] if n["name"] == "c")
    assert c["aligned"] is False


async def test_single_leading_node_does_not_advance_consensus(harness_factory):
    h = harness_factory([MockNode("a", chain_head=9), MockNode("b", chain_head=9)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 9

    # 先建立一份 head-bound 缓存（latest 余额）
    r1 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r1 == hex(10**18 + 9)
    a_calls = h.mock("a").call_count

    # a 单方面领先到 12，但多数仍在 9：口径不应前进
    h.mock("a").advance_head(3)
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 9
    a_calls = h.mock("a").call_count  # refresh 之后再取基准

    # 既有结果仍有效：不能因为单点领先就反复判废、回源
    r2 = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r2 == r1
    assert h.mock("a").call_count == a_calls  # 命中缓存，没有回源


async def test_lagging_node_not_served_and_cannot_write_stale_cache(harness_factory):
    h = harness_factory([MockNode("a", chain_head=10), MockNode("b", chain_head=10)])
    h.registry.get("b").priority = 1
    await h.router.refresh_chain_head()
    # 让 a 不可用并摘除（写请求每次真实选路）
    h.mock("a").fail_next = 100
    # 先把 a 摘掉，b 仍报告 10 但随后落后：构造落后需共识前进而 b 不动
    from gateway.upstream import FAILURE_THRESHOLD
    for _ in range(FAILURE_THRESHOLD):
        await h.router.call("eth_sendRawTransaction", ["0x1"], caller="c1")
    assert not h.registry.get("a").healthy

    # 只剩 b 健康，推进 b 到 12 以建立新口径，再加入一个落后节点视角：
    # 重新接回 a（恢复但停在 10 < 共识 12），a 不得被选
    h.mock("b").advance_head(2)
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 12
    node_a = h.registry.get("a")
    node_a.healthy = True            # 模拟探活发现可达
    node_a.consecutive_failures = 0
    node_a.probe_attempts = 0
    # a 仍报告旧高度 10（注册表记录的），未对齐
    status = h.registry.status()
    a_status = next(n for n in status["nodes"] if n["name"] == "a")
    assert a_status["aligned"] is False

    # 流量仍只能给 b；a 落后不引流，缓存也不会被 a 的旧内容污染
    r = await h.router.call("eth_blockNumber", [], caller="c1")
    assert int(r, 16) == 12
    assert h.mock("b").call_count >= 1


async def test_caught_up_node_allows_reuse_of_valid_cache(harness_factory):
    h = harness_factory([MockNode("a", chain_head=9), MockNode("b", chain_head=9)])
    await h.router.refresh_chain_head()
    await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert await h.cache.size() == 1

    # b 落后：口径保持 9（投票者 b=9, a=9 -> 若 b 掉下去则只有 a 投票）
    # 模拟 a 前进到 11 且 b 停 9：多数(2 节点需 2)未达成，口径仍是 9
    h.mock("a").advance_head(2)
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 9
    # b 追上 11：多数达成，口径推进到 11，旧 9 的条目失效一次（正常回源）
    h.mock("b").advance_head(2)
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 11
    r_new = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert r_new == hex(10**18 + 11)
    assert await h.cache.size() == 1

    # 之后链头稳定：之前仍然有效的结果（同口径 11）继续复用，不再回源
    a_calls = h.mock("a").call_count
    b_calls = h.mock("b").call_count
    for _ in range(5):
        r = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
        assert r == r_new
    assert h.mock("a").call_count == a_calls
    assert h.mock("b").call_count == b_calls


async def test_block_number_response_unified_under_split(harness_factory):
    """链头分裂时，对外 eth_blockNumber 必须给统一口径而非某节点的孤高值。"""
    h = harness_factory([MockNode("a", chain_head=15), MockNode("b", chain_head=15),
                         MockNode("c", chain_head=15)])
    await h.router.refresh_chain_head()
    # a 领先到 18（2/3 多数仍在 15）
    h.mock("a").advance_head(3)
    await h.router.refresh_chain_head()
    assert h.registry.consensus_chain_head() == 15

    results = await asyncio.gather(*(
        h.router.call("eth_blockNumber", [], caller=f"c{i}") for i in range(10)
    ))
    # 即使选到领先节点，对外口径也必须一致为 15
    assert set(results) == {"0xf"}


async def test_inflight_fetch_stamped_at_entry_head_not_advanced(harness_factory):
    """抓取在途期间链头前进：旧结果必须按进入时口径盖章，下次读取判过期。"""
    h = harness_factory([MockNode("a", chain_head=10)])
    await h.router.refresh_chain_head()

    gate = asyncio.Event()
    original = h.mock("a").handle
    stale_value = hex(10**18 + 10)  # 在途请求拿到的是 head=10 时的旧快照

    async def slow_handle(method, params):
        if method == "eth_getBalance":
            await gate.wait()
            return stale_value  # 即使链已前进，这个在途响应仍是旧快照
        return await original(method, params)

    h.mock("a").handle = slow_handle

    task = asyncio.create_task(
        h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1"))
    await asyncio.sleep(0.05)  # 请求已在途、按 head=10 进入
    # 链头在其抓取期间前进到 11
    h.mock("a").advance_head(1)
    await h.router.refresh_chain_head()
    gate.set()
    stale = await task
    assert stale == stale_value

    # 旧结果的盖章高度必须是进入时口径 10，而不是抓取期间前进到的 11：
    # 这样在当前口径下它一定被判过期，绝不会作为新状态对外。
    from gateway import cache as cache_mod
    key = cache_mod.make_key("eth_getBalance", ["0xaddr", "latest"])
    entry = h.cache._entries.get(key)
    assert entry is not None and entry.chain_head == 10
    hit, _ = await h.cache.get(key, 11)
    assert hit is False  # 按当前口径 11 判定为过期

    # 下一次正常读取得到新状态（失效后回源），且不会回退
    h.mock("a").handle = original  # 恢复真实 handler（链头已在 11）
    fresh = await h.router.call("eth_getBalance", ["0xaddr", "latest"], caller="c1")
    assert fresh == hex(10**18 + 11)
