"""配额：速率与并发超限、原子扣减、配置即时生效。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import ErrorKind, GatewayError
from gateway.mock_node import MockNode
from gateway.quota import QuotaConfig


def _tight_quota(**kw) -> QuotaConfig:
    base = dict(rate_per_second=1000.0, burst=1000, max_concurrent=1000)
    base.update(kw)
    return QuotaConfig(**base)


async def test_rate_limit_rejected_distinguishably(harness_factory):
    h = harness_factory([MockNode("a")],
                        quota_config=_tight_quota(rate_per_second=0.001, burst=2))

    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    await h.router.call("eth_sendRawTransaction", ["0x02"], caller="c1")
    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_sendRawTransaction", ["0x03"], caller="c1")

    assert ei.value.kind == ErrorKind.RATE_LIMITED
    assert ei.value.code == -32005
    assert h.mock("a").call_count == 2  # 被拒请求未触及上游


async def test_concurrency_limit_rejected_distinguishably(harness_factory):
    h = harness_factory([MockNode("a")], quota_config=_tight_quota(max_concurrent=2))
    h.mock("a").latency = 0.05  # 让请求在途一段时间

    results = await asyncio.gather(*(
        h.router.call("eth_sendRawTransaction", [f"0x{i:02x}"], caller="c1")
        for i in range(5)
    ), return_exceptions=True)

    errors = [r for r in results if isinstance(r, GatewayError)]
    successes = [r for r in results if not isinstance(r, GatewayError)]
    assert len(successes) == 2
    assert len(errors) == 3
    assert all(e.kind == ErrorKind.CONCURRENCY_LIMITED for e in errors)
    assert all(e.code == -32006 for e in errors)
    # 在途占用最终归零：许可恰好释放一次
    stats = await h.quota.stats("c1")
    assert stats["in_flight"] == 0


async def test_failover_does_not_double_charge(harness_factory):
    """故障转移重试不得重复扣减配额：一次请求只消耗一个令牌、一个并发位。"""
    h = harness_factory([MockNode("a"), MockNode("b")],
                        quota_config=_tight_quota(rate_per_second=0.001, burst=10))
    h.registry.get("b").priority = 1
    h.mock("a").fail_next = 2  # 两次请求各转移一次

    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    await h.router.call("eth_sendRawTransaction", ["0x02"], caller="c1")

    stats = await h.quota.stats("c1")
    assert stats["tokens"] == pytest.approx(8.0, abs=0.5)  # 恰好扣 2 个，不重不漏
    assert stats["in_flight"] == 0
    assert h.mock("a").call_count == 2
    assert h.mock("b").call_count == 2


async def test_failed_request_releases_concurrency_slot(harness_factory):
    """请求失败（含全部上游不可用）也必须释放并发占用，不得泄漏。"""
    h = harness_factory([MockNode("a")],
                        quota_config=_tight_quota(max_concurrent=1), max_retries=1)
    h.mock("a").fail_next = 10

    for _ in range(3):
        with pytest.raises(GatewayError):
            await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")

    stats = await h.quota.stats("c1")
    assert stats["in_flight"] == 0  # 无泄漏


async def test_per_caller_isolation(harness_factory):
    h = harness_factory([MockNode("a")],
                        quota_config=_tight_quota(rate_per_second=0.001, burst=1))

    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_sendRawTransaction", ["0x02"], caller="c1")
    assert ei.value.kind == ErrorKind.RATE_LIMITED
    # 其他调用方不受影响
    await h.router.call("eth_sendRawTransaction", ["0x03"], caller="c2")


async def test_config_change_takes_effect_immediately(harness_factory):
    h = harness_factory([MockNode("a")],
                        quota_config=_tight_quota(rate_per_second=0.001, burst=1))

    await h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1")
    with pytest.raises(GatewayError):
        await h.router.call("eth_sendRawTransaction", ["0x02"], caller="c1")

    # 放宽配置：即时生效，无需重启
    h.quota.set_config("c1", _tight_quota())
    await h.router.call("eth_sendRawTransaction", ["0x03"], caller="c1")

    # 收紧配置：同样即时生效
    h.quota.set_config("c1", _tight_quota(rate_per_second=0.001, burst=1))
    await h.router.call("eth_sendRawTransaction", ["0x04"], caller="c1")
    with pytest.raises(GatewayError) as ei:
        await h.router.call("eth_sendRawTransaction", ["0x05"], caller="c1")
    assert ei.value.kind == ErrorKind.RATE_LIMITED


async def test_in_flight_request_stable_across_config_change(harness_factory):
    """配置变更不追溯：进行中的请求按原许可完成，结论稳定。"""
    h = harness_factory([MockNode("a")], quota_config=_tight_quota(max_concurrent=5))
    h.mock("a").latency = 0.05

    task = asyncio.create_task(
        h.router.call("eth_sendRawTransaction", ["0x01"], caller="c1"))
    await asyncio.sleep(0.01)  # 确保请求已在途
    # 进行中将并发上限收紧到 0：在途请求不应被取消
    h.quota.set_config("c1", _tight_quota(max_concurrent=0))

    result = await task
    assert result.startswith("0xtx")  # 在途请求正常完成
    stats = await h.quota.stats("c1")
    assert stats["in_flight"] == 0
