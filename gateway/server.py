"""FastAPI 入口：暴露 JSON-RPC 端点与管理接口（含运行时上游热变更）。"""
from __future__ import annotations

import itertools
import logging
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request

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


def _node_dict(n: UpstreamNode, unified_head: int) -> dict:
    return {"name": n.name, "url": n.url, "priority": n.priority,
            "weight": n.weight, "state": n.state.value, "healthy": n.healthy,
            "consecutive_failures": n.consecutive_failures,
            "chain_head": n.chain_head, "head_known": n.head_known,
            "synced": n.chain_head >= unified_head}


def create_app(router: Router) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 自愈探测循环随服务启动；探测通道独立于正常流量
        if router.prober is not None:
            await router.prober.start()
        yield
        if router.prober is not None:
            await router.prober.stop()

    app = FastAPI(title="rpc-provider-gateway", lifespan=lifespan)

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
        unified = router.registry.unified_chain_head()
        return [_node_dict(n, unified) for n in router.registry.all_nodes()]

    @app.post("/upstreams", status_code=201)
    async def add_upstream(payload: dict) -> dict:
        """运行时热加入节点：以 JOINING 状态进入，探测对齐后才参与选路。"""
        try:
            node = UpstreamNode(
                name=str(payload["name"]), url=str(payload["url"]),
                priority=int(payload.get("priority", 0)),
                weight=int(payload.get("weight", 1)),
                timeout=float(payload.get("timeout", 5.0)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=400,
                                detail=f"invalid upstream spec: {exc}")
        try:
            await router.registry.add_node(node)
        except GatewayError as exc:
            raise HTTPException(status_code=409, detail=exc.message)
        logger.info("admin: upstream added via API: %s", node.name)
        return _node_dict(node, router.registry.unified_chain_head())

    @app.delete("/upstreams/{name}")
    async def remove_upstream(name: str) -> dict:
        """运行时下线节点：立即退出后续选路；在途请求按进入时的结论收尾。"""
        try:
            node = await router.registry.remove_node(name)
        except GatewayError as exc:
            raise HTTPException(status_code=404, detail=exc.message)
        if router.prober is not None:
            router.prober.forget(name)
        logger.info("admin: upstream removed via API: %s", name)
        return _node_dict(node, router.registry.unified_chain_head())

    @app.patch("/upstreams/{name}")
    async def update_upstream(name: str, payload: dict) -> dict:
        """运行时调整优先级/权重/超时，对之后的选路立即生效。"""
        try:
            node = await router.registry.update_node(
                name,
                priority=payload.get("priority"),
                weight=payload.get("weight"),
                timeout=payload.get("timeout"),
            )
        except GatewayError as exc:
            raise HTTPException(status_code=404, detail=exc.message)
        logger.info("admin: upstream updated via API: %s", name)
        return _node_dict(node, router.registry.unified_chain_head())

    @app.get("/chainhead")
    async def chainhead() -> dict:
        """统一链头口径及其判断依据（法定人数、各节点高度与同步状态）。"""
        return router.registry.chain_head_status()

    @app.get("/quota/{caller}")
    async def quota_stats(caller: str) -> dict:
        return await router.quota.stats(caller)

    return app
