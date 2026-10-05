"""FastAPI 入口：暴露 JSON-RPC 端点与管理接口。"""
from __future__ import annotations

import itertools
import logging
from typing import Any

import httpx
from fastapi import FastAPI, Request
from pydantic import BaseModel

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


class UpstreamConfig(BaseModel):
    name: str
    url: str
    priority: int = 0
    weight: int = 1
    timeout: float = 5.0


class UpstreamPatch(BaseModel):
    priority: int | None = None
    weight: int | None = None


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
    async def upstreams() -> dict:
        # 统一链头口径、法定人数与判断依据均在此可见，便于定位节点分歧
        return router.registry.status()

    @app.post("/admin/upstreams")
    async def add_upstream(cfg: UpstreamConfig) -> dict:
        node = UpstreamNode(name=cfg.name, url=cfg.url, priority=cfg.priority,
                            weight=cfg.weight, timeout=cfg.timeout)
        try:
            router.registry.add_node(node)
        except ValueError as exc:
            raise GatewayError(ErrorKind.INVALID_REQUEST, str(exc)) from exc
        return {"added": cfg.name, "revision": router.registry.revision,
                "warming": True}

    @app.patch("/admin/upstreams/{name}")
    async def patch_upstream(name: str, patch: UpstreamPatch) -> dict:
        try:
            node = router.registry.update_node(
                name, priority=patch.priority, weight=patch.weight)
        except KeyError as exc:
            raise GatewayError(ErrorKind.INVALID_REQUEST, str(exc)) from exc
        return {"name": name, "priority": node.priority,
                "weight": node.weight, "revision": router.registry.revision}

    @app.delete("/admin/upstreams/{name}")
    async def remove_upstream(name: str) -> dict:
        removed = router.registry.remove_node(name)
        if not removed:
            raise GatewayError(ErrorKind.INVALID_REQUEST,
                               f"upstream not active: {name}")
        return {"removed": name, "revision": router.registry.revision}

    @app.get("/quota/{caller}")
    async def quota_stats(caller: str) -> dict:
        return await router.quota.stats(caller)

    return app
