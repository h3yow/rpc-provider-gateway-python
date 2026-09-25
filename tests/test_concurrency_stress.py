"""并发压力下的计数精确性：不重扣、不漏扣、回源不重复。

这些用例用计数器直接断言内部不变量，避免仅靠最终响应"看起来成功"。
"""
from __future__ import annotations

import asyncio

import pytest

from rpc_gateway.config import GatewayConfig, build_gateway_async
from rpc_gateway.inprocess import InProcessTransport


async def _gw(cluster, *, rate, concurrency, latency=0.0):
    if latency:
        for n in ("node-a", "node-b", "node-c"):
            cluster.set_behavior(n, latency=latency)
    config = GatewayConfig.from_dict(
        {
            "nodes": [
                {"name": "node-a", "url": "node-a", "priority": 50},
                {"name": "node-b", "url": "node-b", "priority": 100},
                {"name": "node-c", "url": "node-c", "priority": 100},
            ],
            "default_quota": {"rate_per_second": rate, "max_concurrency": concurrency},
            "request_timeout": 2.0,
            "health_failure_threshold": 100,
            "health_cool_down_seconds": 60,
            "head_poll_interval": 10,
        }
    )
    gateway, _ = await build_gateway_async(config, transport=InProcessTransport(cluster))
    return gateway


async def _rpc(gw, i, caller="stress"):
    return await gw.handle(
        caller,
        {"jsonrpc": "2.0", "id": i, "method": "eth_chainId", "params": []},
    )


@pytest.mark.asyncio
async def test_accepted_minus_rejected_matches_quota(cluster):
    """并发下：成功数 + 速率拒绝数 == 请求总数；速率计数精确不漂移。"""
    gw = await _gw(cluster, rate=20, concurrency=200)
    total = 200
    responses = await asyncio.gather(*[_rpc(gw, i) for i in range(total)])
    ok = sum(1 for r in responses if "result" in r)
    rejected = [r for r in responses if "error" in r]
    rate_rej = [r for r in rejected if r["error"]["data"]["error_type"] == "rate_limit_exceeded"]
    assert ok + len(rate_rej) == total
    # 一个窗口内成功准入数恰好等于速率上限
    assert ok == 20
    assert gw.quota.window_used("stress") == 20
    assert gw.quota.in_flight("stress") == 0  # 全部释放，无槽位泄漏


@pytest.mark.asyncio
async def test_concurrency_slot_accounting_under_load(cluster):
    """在途并发永远不超过上限，且结束后归零（无重复/遗漏释放）。"""
    gw = await _gw(cluster, rate=100000, concurrency=8, latency=0.02)
    max_seen = {"v": 0}
    original_acquire = gw.quota.acquire

    async def tracked(caller):
        token = await original_acquire(caller)
        max_seen["v"] = max(max_seen["v"], gw.quota.in_flight(caller))
        return token

    gw.quota.acquire = tracked  # type: ignore[method-assign]

    responses = await asyncio.gather(*[_rpc(gw, i) for i in range(60)])
    ok = [r for r in responses if "result" in r]
    rejected = [
        r for r in responses
        if r.get("error", {}).get("data", {}).get("error_type")
        == "concurrency_limit_exceeded"
    ]
    # 并发槽满的请求被区分性拒绝；总数守恒，没有请求丢失
    assert len(ok) + len(rejected) == 60
    assert len(ok) >= 8  # 至少占满过一次
    assert max_seen["v"] <= 8
    assert gw.quota.in_flight("stress") == 0


@pytest.mark.asyncio
async def test_singleflight_upstream_calls_exactly_once_per_key(cluster):
    """50 个并发同键头依赖读：回源恰好 1 次，其余全部拿到同一结果。"""
    gw = await _gw(cluster, rate=100000, concurrency=200, latency=0.05)
    counter = {"n": 0}
    original = gw.transport.call

    async def counting(*a, **k):
        counter["n"] += 1
        return await original(*a, **k)

    gw.transport.call = counting  # type: ignore[method-assign]
    responses = await asyncio.gather(
        *[
            gw.handle(
                "stress",
                {"jsonrpc": "2.0", "id": i, "method": "eth_gasPrice", "params": []},
            )
            for i in range(50)
        ]
    )
    assert counter["n"] == 1, counter["n"]
    assert len({r["result"] for r in responses}) == 1


@pytest.mark.asyncio
async def test_send_raw_transactions_distinct_keys_each_executed_once(cluster):
    """不同交易各自恰好执行一次；同一交易的并发重复提交不重复执行。"""
    gw = await _gw(cluster, rate=100000, concurrency=200)
    # node-a 持续超时：所有写都走故障转移，验证转移路径下的 exactly-once
    cluster.set_behavior("node-a", timeout=True)

    distinct = [f"0x{i:040x}" for i in range(10)]
    responses = await asyncio.gather(
        *[
            gw.handle(
                "writer",
                {"jsonrpc": "2.0", "id": i, "method": "eth_sendRawTransaction", "params": [tx]},
            )
            for i, tx in enumerate(distinct)
        ]
    )
    assert all("result" in r for r in responses), responses
    assert cluster.tx_executions() == 10

    # 同一批交易原样重放：幂等命中，执行数不增加
    replay = await asyncio.gather(
        *[
            gw.handle(
                "writer",
                {"jsonrpc": "2.0", "id": 100 + i, "method": "eth_sendRawTransaction", "params": [tx]},
            )
            for i, tx in enumerate(distinct)
        ]
    )
    assert all("result" in r for r in replay)
    assert cluster.tx_executions() == 10
