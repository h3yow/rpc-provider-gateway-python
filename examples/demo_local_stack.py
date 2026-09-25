#!/usr/bin/env python3
"""本地演示脚本：在同一进程启动 3 个模拟节点 + 网关，逐项演示三大能力后退出。

运行：  uv run python examples/demo_local_stack.py
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

# 允许从仓库根目录直接运行：python examples/demo_local_stack.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
import uvicorn

from mocknode.app import create_node_app
from mocknode.cluster import MockCluster
from rpc_gateway.config import GatewayConfig, build_gateway_async
from rpc_gateway.server import create_app

GATEWAY = 8090
NODES = {"node-a": 8201, "node-b": 8202, "node-c": 8203}


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


async def _serve(app, port: int) -> uvicorn.Server:
    cfg = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="off")
    server = uvicorn.Server(cfg)
    asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    return server


async def rpc(client: httpx.AsyncClient, method: str, params=None, *, caller="demo", id=1):
    resp = await client.post(
        f"http://127.0.0.1:{GATEWAY}/",
        json={"jsonrpc": "2.0", "id": id, "method": method, "params": params or []},
        headers={"X-Caller-Id": caller},
    )
    return resp.json()


async def main() -> None:
    cluster = MockCluster()
    servers = []
    for name, port in NODES.items():
        servers.append(await _serve(create_node_app(name, cluster), port))

    config = GatewayConfig.from_dict(
        {
            "nodes": [
                {"name": n, "url": f"http://127.0.0.1:{p}", "priority": pr}
                for (n, p), pr in zip(NODES.items(), (50, 100, 100))
            ],
            "default_quota": {"rate_per_second": 100, "max_concurrency": 16},
            "request_timeout": 1.0,
            "head_poll_interval": 0.5,
        }
    )
    gateway, owned = await build_gateway_async(config)
    await gateway.head_monitor.start()
    servers.append(await _serve(create_app(gateway), GATEWAY))

    async with httpx.AsyncClient() as client:
        banner("1) 正常读取（头依赖结果被缓存）")
        print(await rpc(client, "eth_blockNumber", id=1))

        banner("2) 节点故障转移：node-a 注入 500，流量转移到 node-b/c")
        await client.post(
            f"http://127.0.0.1:{NODES['node-a']}/_control/behavior",
            json={"http500": True},
        )
        for i in range(3):
            print(await rpc(client, "web3_clientVersion", id=10 + i))
        nodes = (await client.get(f"http://127.0.0.1:{GATEWAY}/admin/nodes")).json()
        print("node-a health after failures:", nodes["node-a"]["health"])

        banner("3) 链头推进与缓存失效：出块后轮询，旧缓存失效")
        cluster.mine(3)
        await client.post(f"http://127.0.0.1:{GATEWAY}/admin/heads/poll")
        print(await rpc(client, "eth_blockNumber", id=20))

        banner("4) 写请求幂等：node-a 超时 + 故障转移，集群只执行一次")
        await client.post(
            f"http://127.0.0.1:{NODES['node-a']}/_control/behavior",
            json={"http500": False, "timeout": True},
        )
        raw = "0x" + "cd" * 40
        r1 = await rpc(client, "eth_sendRawTransaction", [raw], caller="writer", id=30)
        r2 = await rpc(client, "eth_sendRawTransaction", [raw], caller="writer", id=31)
        print("tx hash #1:", r1.get("result"))
        print("tx hash #2:", r2.get("result"), "(相同 => 集群去重)")
        state = await client.get(f"http://127.0.0.1:{NODES['node-b']}/_control/state")
        print("cluster executions:", state.json()["tx_executions"], "(期望 1)")

        banner("5) 配额：为调用方 throttled 设置速率=2/s，第 3 个请求被区分性拒绝")
        await client.post(
            f"http://127.0.0.1:{GATEWAY}/admin/quota",
            json={"caller": "throttled", "rate_per_second": 2, "max_concurrency": 5},
        )
        for i in range(3):
            r = await rpc(client, "eth_chainId", id=40 + i, caller="throttled")
            payload = r.get("error") or r.get("result")
            print(json.dumps(payload, ensure_ascii=False))

        banner("6) 配额热更新即时生效：放宽 throttled 后立刻可请求")
        await client.post(
            f"http://127.0.0.1:{GATEWAY}/admin/quota",
            json={"caller": "throttled", "rate_per_second": 100, "max_concurrency": 10},
        )
        print("放宽后立即请求:", await rpc(client, "eth_chainId", id=50, caller="throttled"))

    await gateway.head_monitor.stop()
    for s in servers:
        s.should_exit = True
    if owned is not None:
        await owned.aclose()
    await asyncio.sleep(0.1)
    print("\ndemo finished.")


if __name__ == "__main__":
    asyncio.run(main())
