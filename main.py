"""本地演示：启动 3 个模拟上游节点 + 1 个网关（含自愈、热变更、统一链头口径）。

用法：
    uv run python main.py
    curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

import httpx
import uvicorn

from gateway.cache import ResultCache
from gateway.health_monitor import HealthMonitor
from gateway.mock_node import MockNode
from gateway.mock_server import create_mock_app
from gateway.quota import QuotaConfig, QuotaManager
from gateway.router import Router
from gateway.server import create_app, make_http_transport
from gateway.upstream import UpstreamNode, UpstreamRegistry

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")

GATEWAY_PORT = 8080
# name, port, priority, weight
UPSTREAMS = [
    ("node-a", 8545, 0, 1),
    ("node-b", 8546, 0, 1),
    ("node-c", 8547, 1, 1),
]


async def serve() -> None:
    mocks = {name: MockNode(name) for name, _, _, _ in UPSTREAMS}

    registry = UpstreamRegistry()
    for name, port, priority, weight in UPSTREAMS:
        registry.register(UpstreamNode(
            name=name, url=f"http://127.0.0.1:{port}/",
            priority=priority, weight=weight, timeout=2.0))

    client = httpx.AsyncClient()
    transport = make_http_transport(client)
    monitor = HealthMonitor(
        registry, transport,
        poll_interval=2.0, base_backoff=1.0, max_backoff=10.0,
        probe_timeout=2.0,
    )
    router = Router(registry, ResultCache(),
                    QuotaManager(QuotaConfig(rate_per_second=50, burst=50,
                                             max_concurrent=20)),
                    transport=transport, max_retries=3, monitor=monitor)

    configs = [
        uvicorn.Config(create_mock_app(mocks[name]), host="127.0.0.1", port=port,
                       log_level="warning")
        for name, port, _, _ in UPSTREAMS
    ]
    configs.append(uvicorn.Config(create_app(router), host="127.0.0.1",
                                  port=GATEWAY_PORT, log_level="warning"))
    servers = [uvicorn.Server(c) for c in configs]
    logging.getLogger("main").info(
        "gateway on :%d, upstreams: %s", GATEWAY_PORT,
        ", ".join(f"{n}=:{p}" for n, p, _, _ in UPSTREAMS))

    await router.start()  # 启动后台链头轮询与退避恢复探测
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        await router.stop()
        await client.aclose()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve())
