"""本地一键启动：3 个模拟节点 + 网关（全部真实 HTTP 端口，同一进程）。

用法：  uv run python -m rpc_gateway.localstack
默认：  网关 http://127.0.0.1:8080 ；节点 8101 / 8102 / 8103
"""
from __future__ import annotations

import asyncio
import logging
import signal

import uvicorn

from mocknode.app import create_node_app
from mocknode.cluster import MockCluster
from rpc_gateway.config import GatewayConfig, build_gateway_async
from rpc_gateway.server import create_app

GATEWAY_PORT = 8080
NODE_PORTS = (8101, 8102, 8103)
NODE_NAMES = ("node-a", "node-b", "node-c")


def _build_config() -> GatewayConfig:
    return GatewayConfig.from_dict(
        {
            "nodes": [
                {"name": name, "url": f"http://127.0.0.1:{port}", "priority": priority, "weight": 1}
                for name, port, priority in zip(
                    NODE_NAMES, NODE_PORTS, (50, 100, 100), strict=True
                )
            ],
            "default_quota": {"rate_per_second": 10, "max_concurrency": 4},
            "caller_quotas": {
                "premium": {"rate_per_second": 100, "max_concurrency": 16},
            },
            "request_timeout": 1.0,
            "head_poll_interval": 0.5,
        }
    )


async def _serve(app: uvicorn.types.ASGIApplication, port: int) -> uvicorn.Server:
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    # 等待端口就绪
    while not server.started:
        await asyncio.sleep(0.01)
    return server


async def run_local_stack() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    cluster = MockCluster()
    node_servers: list[uvicorn.Server] = []
    for name, port in zip(NODE_NAMES, NODE_PORTS, strict=True):
        node_servers.append(await _serve(create_node_app(name, cluster), port))
        logging.info("localstack: mock node %s on %d", name, port)

    gateway, owned_http = await build_gateway_async(_build_config())
    await gateway.head_monitor.start()
    gateway_server = await _serve(create_app(gateway), GATEWAY_PORT)
    logging.info("localstack: GATEWAY on http://127.0.0.1:%d", GATEWAY_PORT)
    logging.info(
        "localstack: try: curl -s http://127.0.0.1:%d/ -X POST -H 'content-type: application/json' "
        "-H 'X-Caller-Id: demo' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\"}'",
        GATEWAY_PORT,
    )

    stop_event = asyncio.Event()

    def _on_signal(*_a: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        await gateway.head_monitor.stop()
        gateway_server.should_exit = True
        for server in node_servers:
            server.should_exit = True
        if owned_http is not None:
            await owned_http.aclose()
        await asyncio.sleep(0.1)


def main() -> None:
    asyncio.run(run_local_stack())


if __name__ == "__main__":
    main()
