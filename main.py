"""本地演示：启动 2 个模拟上游节点 + 1 个网关。

用法：
    uv run python main.py
    curl -s -X POST localhost:8080/rpc -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
"""
from __future__ import annotations

import asyncio
import logging

import httpx
import uvicorn

from gateway.cache import ResultCache
from gateway.mock_node import MockNode
from gateway.mock_server import create_mock_app
from gateway.quota import QuotaConfig, QuotaManager
from gateway.router import Router
from gateway.server import create_app, make_http_transport
from gateway.upstream import UpstreamNode, UpstreamRegistry

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")

GATEWAY_PORT = 8080
UPSTREAMS = [("node-a", 8545, 0), ("node-b", 8546, 1)]  # name, port, priority


async def serve() -> None:
    mocks = {name: MockNode(name) for name, _, _ in UPSTREAMS}

    registry = UpstreamRegistry()
    for name, port, priority in UPSTREAMS:
        registry.register(UpstreamNode(
            name=name, url=f"http://127.0.0.1:{port}/",
            priority=priority, weight=1, timeout=2.0))

    client = httpx.AsyncClient()
    router = Router(registry, ResultCache(),
                    QuotaManager(QuotaConfig(rate_per_second=50, burst=50,
                                             max_concurrent=20)),
                    transport=make_http_transport(client))

    configs = [
        uvicorn.Config(create_mock_app(mocks[name]), host="127.0.0.1", port=port,
                       log_level="warning")
        for name, port, _ in UPSTREAMS
    ]
    configs.append(uvicorn.Config(create_app(router), host="127.0.0.1",
                                  port=GATEWAY_PORT, log_level="warning"))
    servers = [uvicorn.Server(c) for c in configs]
    logging.getLogger("main").info(
        "gateway on :%d, upstreams: %s", GATEWAY_PORT,
        ", ".join(f"{n}=:{p}" for n, p, _ in UPSTREAMS))
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(serve())
