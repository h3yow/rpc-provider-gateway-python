"""测试基础设施：把网关路由接到进程内模拟节点上。"""
from __future__ import annotations

import pytest

from gateway.cache import ResultCache
from gateway.mock_node import MockNode
from gateway.quota import QuotaConfig, QuotaManager
from gateway.router import Router
from gateway.upstream import UpstreamNode, UpstreamRegistry


class GatewayHarness:
    """把 Router 与一组 MockNode 连接起来，便于断言调用次数与故障注入。"""

    def __init__(self, mocks: list[MockNode], quota_config: QuotaConfig | None = None,
                 max_retries: int = 3, **node_kwargs):
        self.mocks = {m.name: m for m in mocks}
        self.registry = UpstreamRegistry()
        for m in mocks:
            self.registry.register(UpstreamNode(name=m.name, url=f"mock://{m.name}",
                                                **node_kwargs))
        self.cache = ResultCache()
        self.quota = QuotaManager(quota_config or QuotaConfig())
        self.router = Router(self.registry, self.cache, self.quota,
                             transport=self._transport, max_retries=max_retries)

    async def _transport(self, node, method, params):
        return await self.mocks[node.name].handle(method, params)

    def mock(self, name: str) -> MockNode:
        return self.mocks[name]


@pytest.fixture
def harness_factory():
    def factory(mocks, **kwargs) -> GatewayHarness:
        return GatewayHarness(mocks, **kwargs)
    return factory
