"""单个模拟节点的 FastAPI 应用。

业务端点 POST / 处理 JSON-RPC；故障注入后的行为与真实上游一致：
* timeout  -> 请求挂起（由调用方超时中断）；
* http500  -> 500 状态码；
* rpc_error -> 标准 JSON-RPC error 体；
响应统一带 ``X-Mock-Head``（该节点视角链头），供网关即时更新节点链头。

另提供控制端点（仅本地实验使用）：
* POST /_control/mine   {"count": n}
* POST /_control/behavior  {"fail": true, "latency": 0.2, ...}
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import FastAPI, Request, Response

from .cluster import MockCluster, NodeFault

logger = logging.getLogger("mocknode.app")


def create_node_app(node_name: str, cluster: MockCluster) -> FastAPI:
    app = FastAPI(title=f"mock-node-{node_name}")

    def _head_header() -> dict[str, str]:
        return {"X-Mock-Head": hex(cluster.view_head(node_name))}

    @app.post("/")
    async def rpc_entry(request: Request) -> Response:
        try:
            payload = await request.json()
        except Exception:
            return Response(
                content='{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}',
                media_type="application/json",
                status_code=200,
                headers=_head_header(),
            )

        req_id = payload.get("id") if isinstance(payload, dict) else None
        method = payload.get("method") if isinstance(payload, dict) else None
        params = payload.get("params", []) if isinstance(payload, dict) else []
        if isinstance(params, dict):
            params = tuple(params.values())
        else:
            params = tuple(params)
        idem = payload.get("xIdempotencyKey") if isinstance(payload, dict) else None

        try:
            result = await _invoke(cluster, node_name, method, params, idem)
        except NodeFault as fault:
            return await _translate_fault(fault, req_id, _head_header())
        except Exception:  # noqa: BLE001 - 模拟节点自身也不向外吐堆栈
            logger.exception("mocknode: node=%s internal failure", node_name)
            error = {"code": -32603, "message": "internal error"}
            return _json_response(
                {"jsonrpc": "2.0", "id": req_id, "error": error},
                headers=_head_header(),
            )

        return _json_response(
            {"jsonrpc": "2.0", "id": req_id, "result": result},
            headers=_head_header(),
        )

    @app.post("/_control/mine")
    async def mine(body: dict[str, Any]) -> dict[str, Any]:
        count = int(body.get("count", 1))
        return {"head": cluster.mine(count)}

    @app.post("/_control/behavior")
    async def set_behavior(body: dict[str, Any]) -> dict[str, Any]:
        b = cluster.set_behavior(node_name, **body)
        return {
            "node": node_name,
            "fail": b.fail,
            "http500": b.http500,
            "timeout": b.timeout,
            "latency": b.latency,
            "head_offset": b.head_offset,
        }

    @app.get("/_control/state")
    async def state() -> dict[str, Any]:
        return {
            "node": node_name,
            "view_head": cluster.view_head(node_name),
            "real_head": cluster.real_head(),
            "tx_executions": cluster.tx_executions(),
        }

    return app


async def _invoke(
    cluster: MockCluster,
    node: str,
    method: str | None,
    params: tuple[Any, ...],
    idem: str | None,
) -> Any:
    if method == "eth_sendRawTransaction":
        raw_tx = params[0] if params else ""
        key = idem or f"auto:{raw_tx}"
        outcome = await cluster.apply_transaction(key, raw_tx)
        return outcome["tx_hash"]
    return await cluster.call(node, method or "", params)


async def _translate_fault(
    fault: NodeFault, req_id: Any, headers: dict[str, str]
) -> Response:
    if fault.kind == "timeout":
        # 模拟真实超时：长时间挂起直到客户端断开/服务关停（可被取消），
        # 避免在关停时残留永不结束的任务。
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        raise RuntimeError("unreachable")  # pragma: no cover
    if fault.kind == "http_500":
        return Response(
            content="internal server error",
            status_code=fault.http_status or 500,
            headers=headers,
        )
    error = {"code": -32000, "message": "mock node injected failure"}
    return _json_response(
        {"jsonrpc": "2.0", "id": req_id, "error": error}, headers=headers
    )


def _json_response(body: dict[str, Any], headers: dict[str, str] | None = None) -> Response:
    import json

    return Response(
        content=json.dumps(body),
        media_type="application/json",
        headers=headers or {},
    )
