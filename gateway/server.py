"""FastAPI 入口：暴露 JSON-RPC 端点与管理接口。"""
from __future__ import annotations

import itertools
import logging
from typing import Any

import httpx
from fastapi import FastAPI, Request

from .errors import ErrorKind, GatewayError
from .router import Router
from .upstream import UpstreamNode

logger = logging.getLogger("gateway.server")

_request_ids = itertools.count(1)


def make_http_transport(client: httpx.AsyncClient):
    """构造基于 httpx 的真实 HTTP JSON-RPC 传输层。"""

    async def transport(node: UpstreamNode, method: str, params: list | None) -> Any:
        resp = await client.post(
            node.url,
            json={"jsonrpc": "2.0", "id": next(_request_ids),
                  "method": method, "params": params or []},
            timeout=node.timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        if "error" in payload:
            raise GatewayError(ErrorKind.UPSTREAM_ERROR,
                               "upstream returned an error", retryable=False)
        return payload.get("result")

    return transport


def create_app(router: Router) -> FastAPI:
    app = FastAPI(title="rpc-provider-gateway")

    @app.post("/rpc")
    async def rpc(payload: dict, request: Request) -> dict:
        req_id = payload.get("id")
        method = payload.get("method")
        params = payload.get("params")
        if not isinstance(method, str) or not method:
            err = GatewayError(ErrorKind.INVALID_REQUEST, "invalid JSON-RPC request")
            return {"jsonrpc": "2.0", "id": req_id, "error": err.to_jsonrpc_error()}
        caller = request.headers.get("x-caller-id", "anonymous")
        try:
            result = await router.call(method, params, caller)
        except GatewayError as exc:
            return {"jsonrpc": "2.0", "id": req_id, "error": exc.to_jsonrpc_error()}
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/upstreams")
    async def upstreams() -> list[dict]:
        return [
            {"name": n.name, "url": n.url, "priority": n.priority,
             "weight": n.weight, "healthy": n.healthy,
             "consecutive_failures": n.consecutive_failures,
             "chain_head": n.chain_head}
            for n in router.registry.all_nodes()
        ]

    @app.get("/quota/{caller}")
    async def quota_stats(caller: str) -> dict:
        return await router.quota.stats(caller)

    return app
