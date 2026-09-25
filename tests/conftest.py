"""pytest 公共夹具：确定性的本地模拟集群 + 进程内传输 + 网关。"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from mocknode.cluster import MockCluster
from rpc_gateway.cache import HeadAwareCache
from rpc_gateway.config import GatewayConfig
from rpc_gateway.config import build_gateway_async
from rpc_gateway.gateway import Gateway
from rpc_gateway.inprocess import InProcessTransport


@pytest.fixture
def cluster() -> MockCluster:
    return MockCluster()


async def _build(
    cluster: MockCluster,
    *,
    rate: int = 1000,
    concurrency: int = 200,
    timeout: float = 0.5,
    failure_threshold: int = 1,
    cool_down: float = 60.0,
    caller_quotas: dict | None = None,
) -> tuple[Gateway, GatewayConfig]:
    config = GatewayConfig.from_dict(
        {
            "nodes": [
                {"name": "node-a", "url": "node-a", "priority": 50},
                {"name": "node-b", "url": "node-b", "priority": 100},
                {"name": "node-c", "url": "node-c", "priority": 100, "weight": 2},
            ],
            "default_quota": {"rate_per_second": rate, "max_concurrency": concurrency},
            "caller_quotas": caller_quotas or {},
            "request_timeout": timeout,
            "health_failure_threshold": failure_threshold,
            "health_cool_down_seconds": cool_down,
            "head_poll_interval": 10.0,
        }
    )
    gateway, _ = await build_gateway_async(
        config, transport=InProcessTransport(cluster)
    )
    return gateway, config


@pytest.fixture
async def harness(cluster: MockCluster):
    gateway, config = await _build(cluster)
    return _Harness(gateway=gateway, config=config, cluster=cluster)


@dataclass
class _Harness:
    gateway: Gateway
    config: GatewayConfig
    cluster: MockCluster

    async def call(
        self,
        method: str,
        params: list | None = None,
        *,
        caller: str = "tester",
        req_id: int = 1,
    ) -> dict:
        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params or [],
        }
        return await self.gateway.handle(caller, payload)

    async def poll_heads(self) -> int:
        return await self.gateway.head_monitor.poll_once()


def pytest_configure(config: object) -> None:
    # 让单测日志中的关键判定可见（上游选择/缓存判定/配额变动）
    logging.basicConfig(level=logging.INFO)
