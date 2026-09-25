"""FastAPI HTTP 入口与管理接口。

业务：
* POST /          —— JSON-RPC（单请求或批量），调用方由 ``X-Caller-Id`` 标识
管理（仅本地，无鉴权）：
* GET  /admin/nodes            —— 节点注册信息与健康快照
* POST /admin/nodes/{n}/enable —— 启用/摘除节点 {"enabled": false}
* POST /admin/quota            —— 热更新配额 {"caller": "...", "rate_per_second": n, "max_concurrency": m}
* POST /admin/heads/poll       —— 手动触发一轮链头探测（演示/测试确定性推进）
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import Body, FastAPI, Header
from fastapi.responses import JSONResponse

from .gateway import Gateway
from .quota import QuotaLimits

logger = logging.getLogger("rpc_gateway.server")

ANONYMOUS = "anonymous"


def create_app(gateway: Gateway) -> FastAPI:
    app = FastAPI(title="local multi-upstream JSON-RPC gateway")

    @app.post("/")
    async def rpc(
        payload: Any = Body(...),
        x_caller_id: str | None = Header(default=None),
    ) -> JSONResponse:
        caller = x_caller_id or ANONYMOUS
        result = await gateway.handle_message(caller, payload)
        if result is None:
            # 通知：204 无内容
            return JSONResponse(status_code=204, content=None)
        return JSONResponse(content=result)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/admin/nodes")
    async def nodes() -> dict[str, Any]:
        snap = await gateway.health.snapshot()
        out: dict[str, Any] = {}
        for name in gateway.registry.all_names():
            config = gateway.registry.get(name)
            health = snap.get(name)
            out[name] = {
                "url": config.url if config else None,
                "priority": config.priority if config else None,
                "weight": config.weight if config else None,
                "enabled": config.enabled if config else False,
                "node_head": config.node_head if config else 0,
                "health": health.state.value if health else "closed",
                "consecutive_failures": health.consecutive_failures if health else 0,
            }
        out["_canonical_head"] = gateway.head_monitor.canonical_head
        return out

    @app.post("/admin/nodes/{name}/enable")
    async def set_enabled(name: str, body: dict[str, Any]) -> dict[str, Any]:
        await gateway.registry.set_enabled(name, bool(body.get("enabled", True)))
        return {"node": name, "enabled": body.get("enabled", True)}

    @app.post("/admin/quota")
    async def update_quota(body: dict[str, Any]) -> dict[str, Any]:
        caller = body.get("caller", ANONYMOUS)
        limits = QuotaLimits(
            rate_per_second=int(body["rate_per_second"]),
            max_concurrency=int(body["max_concurrency"]),
        )
        gateway.quota.update_limits(caller, limits)
        return {
            "caller": caller,
            "rate_per_second": limits.rate_per_second,
            "max_concurrency": limits.max_concurrency,
        }

    @app.post("/admin/heads/poll")
    async def poll_heads() -> dict[str, Any]:
        head = await gateway.head_monitor.poll_once()
        return {"canonical_head": head}

    return app
