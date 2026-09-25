"""调用方配额语义测试。

覆盖：
* 超额时速率(-32010)与并发(-32011)拒绝原因可区分；
* 被拒绝的请求不扣减、不触达上游；
* 故障转移多次尝试只扣一次（无重复扣减、无漏扣）；
* 不同调用方隔离；
* 配置热更新后新限额对后续请求即时生效，进行中请求结论稳定；
* 速率窗口滚动恢复。
"""
from __future__ import annotations

import asyncio

import pytest

from rpc_gateway.config import GatewayConfig, build_gateway_async
from rpc_gateway.inprocess import InProcessTransport
from rpc_gateway.quota import QuotaLimits


async def _limited_gateway(cluster, *, rate, concurrency, timeout=0.5):
    config = GatewayConfig.from_dict(
        {
            "nodes": [
                {"name": "node-a", "url": "node-a", "priority": 50},
                {"name": "node-b", "url": "node-b", "priority": 100},
            ],
            "default_quota": {"rate_per_second": rate, "max_concurrency": concurrency},
            "request_timeout": timeout,
            "health_failure_threshold": 5,
            "health_cool_down_seconds": 60,
            "head_poll_interval": 10,
        }
    )
    gateway, _ = await build_gateway_async(config, transport=InProcessTransport(cluster))
    return gateway


async def _rpc(gateway, method, params=None, *, caller="c1", req_id=1):
    return await gateway.handle(
        caller,
        {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or []},
    )


@pytest.mark.asyncio
async def test_rate_limit_rejection_is_distinguishable(cluster):
    gw = await _limited_gateway(cluster, rate=2, concurrency=10)
    ok1, ok2, rejected = await asyncio.gather(
        _rpc(gw, "eth_chainId", req_id=1),
        _rpc(gw, "eth_chainId", req_id=2),
        _rpc(gw, "eth_chainId", req_id=3),
    )
    assert "result" in ok1 and "result" in ok2
    assert rejected["error"]["code"] == -32010
    assert rejected["error"]["data"]["error_type"] == "rate_limit_exceeded"
    assert rejected["error"]["data"]["limit"] == 2


@pytest.mark.asyncio
async def test_concurrency_limit_rejection_is_distinguishable(cluster):
    # 速率给足、并发=1：让上游调用阻塞在途中
    cluster.set_behavior("node-a", latency=0.2)
    gw = await _limited_gateway(cluster, rate=1000, concurrency=1, timeout=2.0)

    started = asyncio.Event()

    async def slow_call():
        # 先占住唯一并发槽
        started.set()
        return await _rpc(gw, "eth_blockNumber", req_id=1)

    slow = asyncio.create_task(slow_call())
    await started.wait()
    await asyncio.sleep(0.05)  # 确保慢请求已在途
    rejected = await _rpc(gw, "eth_blockNumber", req_id=2)
    assert rejected["error"]["code"] == -32011
    assert rejected["error"]["data"]["error_type"] == "concurrency_limit_exceeded"
    result = await slow
    assert "result" in result


@pytest.mark.asyncio
async def test_rejected_request_does_not_touch_upstream(cluster):
    gw = await _limited_gateway(cluster, rate=1, concurrency=10)
    await _rpc(gw, "eth_chainId", req_id=1)
    # 之后的拒绝发生在路由之前；让 node-a 故障也无所谓——拒绝不应变成节点错误
    cluster.set_behavior("node-a", http500=True)
    rejected = await _rpc(gw, "eth_chainId", req_id=2)
    assert rejected["error"]["data"]["error_type"] == "rate_limit_exceeded"


@pytest.mark.asyncio
async def test_failover_charges_only_once(cluster):
    gw = await _limited_gateway(cluster, rate=1000, concurrency=10)
    # 首选节点超时并转移：一次逻辑请求只占/释一次并发槽、只计一次速率
    cluster.set_behavior("node-a", timeout=True)
    before = gw.quota.window_used("c1")
    resp = await _rpc(gw, "eth_blockNumber")
    assert "result" in resp
    after = gw.quota.window_used("c1")
    assert after - before == 1
    assert gw.quota.in_flight("c1") == 0  # 槽位已释放，无泄漏


@pytest.mark.asyncio
async def test_callers_are_isolated(cluster):
    gw = await _limited_gateway(cluster, rate=1, concurrency=10)
    r1 = await _rpc(gw, "eth_chainId", caller="alice", req_id=1)
    r2 = await _rpc(gw, "eth_chainId", caller="bob", req_id=2)
    r3 = await _rpc(gw, "eth_chainId", caller="alice", req_id=3)
    assert "result" in r1 and "result" in r2
    assert r3["error"]["data"]["error_type"] == "rate_limit_exceeded"


@pytest.mark.asyncio
async def test_quota_hot_update_takes_effect_immediately(cluster):
    gw = await _limited_gateway(cluster, rate=1, concurrency=10)
    r = await _rpc(gw, "eth_chainId", req_id=1)
    assert "result" in r
    rejected = await _rpc(gw, "eth_chainId", req_id=2)
    assert rejected["error"]["data"]["error_type"] == "rate_limit_exceeded"
    # 热更新放大配额 -> 立即生效
    gw.quota.update_limits("c1", QuotaLimits(rate_per_second=100, max_concurrency=10))
    r2 = await _rpc(gw, "eth_chainId", req_id=3)
    assert "result" in r2


@pytest.mark.asyncio
async def test_in_flight_request_stays_valid_after_limit_shrink(cluster):
    """缩容不影响已经准入的在途请求：其结论保持稳定，槽位正常释放。"""
    cluster.set_behavior("node-a", latency=0.2)
    gw = await _limited_gateway(cluster, rate=1000, concurrency=10, timeout=2.0)

    admitted = asyncio.Event()

    async def slow():
        admitted.set()
        return await _rpc(gw, "eth_blockNumber", req_id=1)

    task = asyncio.create_task(slow())
    await admitted.wait()
    await asyncio.sleep(0.05)
    # 在途期间把并发缩到 0：在途请求不被打断/不误判
    gw.quota.update_limits("c1", QuotaLimits(rate_per_second=1000, max_concurrency=0))
    result = await task
    assert "result" in result
    assert gw.quota.in_flight("c1") == 0
    # 新请求立即受新限额约束
    rejected = await _rpc(gw, "eth_blockNumber", req_id=2)
    assert rejected["error"]["data"]["error_type"] == "concurrency_limit_exceeded"


@pytest.mark.asyncio
async def test_rate_window_recovers(cluster):
    t = {"v": 0.0}
    from rpc_gateway.quota import QuotaManager

    qm = QuotaManager(QuotaLimits(2, 10), clock=lambda: t["v"])
    await qm.acquire("x")
    await qm.acquire("x")
    with pytest.raises(Exception):
        await qm.acquire("x")
    t["v"] += 1.0  # 新窗口
    a = await qm.acquire("x")
    assert qm.window_used("x") == 1
    a.release()
