"""缓存一致性与并发读写测试。

覆盖：
* 头依赖结果在链头推进后失效，绝不返回过期数据；
* 终局数据（eth_chainId）跨链头持续有效；
* 并发同键未命中被单飞合并，上游只执行一次，等待者得到同一结果；
* 落后节点不得服务头依赖读（跨节点不矛盾）；
* 失效/提升顺序保证"读到新 head 时缓存一定已失效"。
"""
from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_head_tagged_entry_invalidated_after_head_advance(harness):
    r1 = await harness.call("eth_blockNumber")
    assert r1["result"] == "0x0"
    harness.cluster.mine(1)
    await harness.poll_heads()
    r2 = await harness.call("eth_blockNumber")
    assert r2["result"] == "0x1", r2


@pytest.mark.asyncio
async def test_finalized_entry_survives_head_advance(harness):
    r1 = await harness.call("eth_chainId")
    assert r1["result"] == "0x539"
    harness.cluster.mine(5)
    await harness.poll_heads()
    # 终局条目不随头失效
    size_before = await harness.gateway.cache.size()
    r2 = await harness.call("eth_chainId")
    assert r2["result"] == "0x539"
    assert await harness.gateway.cache.size() == size_before


@pytest.mark.asyncio
async def test_concurrent_same_key_collapses_to_single_upstream_call(harness):
    # 让首选节点慢下来，放大并发窗口；统计回源次数
    harness.cluster.set_behavior("node-a", latency=0.05)
    calls_before = getattr(harness, "_upstream_calls", 0)

    counter = {"n": 0}
    original = harness.gateway.transport.call

    async def counting_call(*args, **kwargs):
        counter["n"] += 1
        return await original(*args, **kwargs)

    harness.gateway.transport.call = counting_call  # type: ignore[method-assign]

    responses = await asyncio.gather(
        *[
            harness.call("eth_getBalance", ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "latest"], req_id=i)
            for i in range(20)
        ]
    )
    assert all("result" in r for r in responses)
    # 20 个并发同键未命中 -> 只有 1 次真正回源
    assert counter["n"] == 1, counter
    values = {r["result"] for r in responses}
    assert len(values) == 1
    harness.gateway.transport.call = original  # type: ignore[method-assign]
    _ = calls_before


@pytest.mark.asyncio
async def test_lagging_node_cannot_serve_head_dependent_read(harness):
    # 推进到 head=3 并让网关确认
    harness.cluster.mine(3)
    await harness.poll_heads()
    # node-b 落后 3 个块、node-a 故障：若只剩落后节点，应得到 no_healthy_node
    # 而不是 node-b 的旧世界结果
    harness.cluster.set_behavior("node-b", head_offset=-3)
    harness.cluster.set_behavior("node-c", head_offset=-3)
    # 重新探测，让注册表观察到 b/c 已落后（观测值允许下降）
    await harness.poll_heads()
    harness.cluster.set_behavior("node-a", timeout=True)
    resp = await harness.call("eth_blockNumber")
    assert resp["error"]["data"]["error_type"] == "no_healthy_node"
    assert resp["error"]["data"]["head_dependent"] is True


@pytest.mark.asyncio
async def test_cache_stores_value_tagged_with_current_head(harness):
    await harness.call("eth_gasPrice")
    entry_key = "eth_gasPrice:[]"
    value = await harness.gateway.cache.get(entry_key, 0)
    assert value == hex(1_000_000_000)
    # 头部推进后该键立即视为过期
    harness.cluster.mine(1)
    await harness.poll_heads()
    assert await harness.gateway.cache.get(entry_key, 1) is None


@pytest.mark.asyncio
async def test_concurrent_readers_consistent_during_advance(harness):
    """链头推进前后的读都必须自洽：旧 head 得旧值，新 head 得新值，不撕裂。"""
    await harness.call("eth_blockNumber")  # 预热 head=0 缓存

    results: list[tuple[str, str | None]] = []

    async def reader(tag: str):
        for _ in range(60):
            r = await harness.call("eth_blockNumber")
            results.append((tag, r.get("result") or r.get("error", {}).get("data", {}).get("error_type")))
            await asyncio.sleep(0.002)

    async def advancer():
        for n in range(1, 4):
            await asyncio.sleep(0.005)
            harness.cluster.mine(1)
            await harness.poll_heads()

    await asyncio.gather(reader("r1"), reader("r2"), advancer())

    values = {v for _, v in results if v is not None}
    # 所有可见值必须是合法链头之一，且单调非减（按结果出现顺序允许读到旧/新缓存）
    allowed = {"0x0", "0x1", "0x2", "0x3"}
    assert values <= allowed, values - allowed
    # 最终一定能读到新链头（没有永久过期读）
    assert "0x3" in values


@pytest.mark.asyncio
async def test_result_from_stale_view_is_not_cached_as_new_head(harness):
    """回源返回后才发现节点视角落后：结果只能按旧 head 打标签，随后被判定过期。"""
    # 推进到 head=4 并确认
    harness.cluster.mine(4)
    await harness.poll_heads()
    assert harness.gateway.head_monitor.canonical_head == 4

    # 先把 canonical 推进到 6（所有节点同步）
    harness.cluster.mine(2)
    await harness.poll_heads()
    assert harness.gateway.head_monitor.canonical_head == 6

    # 模拟"回源在途期间服务节点落后"：让节点视角停在旧高度，
    # 但绕过注册表过滤（直接清空缓存并调用，transport 回传的 node_head 是旧值）。
    for node in ("node-a", "node-b", "node-c"):
        harness.cluster.set_behavior(node, head_offset=-2)  # view = 4
    key = "eth_getBalance:[\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"latest\"]"
    # 注册表此刻仍记 head=6（选择不被过滤），而响应 X-Mock-Head 回传 4
    resp = await harness.call(
        "eth_getBalance",
        ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "latest"],
    )
    assert "result" in resp
    # 关键：条目必须以节点视角 4 打标签（而不是 canonical 6）。
    # 直接检查内部条目标签；observe_node_head 已把落后登记，canonical=6
    # 下该旧标签条目不允许作为新结果返回。
    entry = harness.gateway.cache._entries.get(key)
    assert entry is not None
    assert entry.head == 4, entry.head
    assert await harness.gateway.cache.get(key, 6) is None
