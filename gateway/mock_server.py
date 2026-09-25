"""把 MockNode 包装为独立 HTTP JSON-RPC 服务，作为本地模拟上游。"""
from __future__ import annotations

import asyncio

from fastapi import FastAPI, Response

from .mock_node import MockNode, MockUpstreamError

# 注入 timeout 故障时，模拟节点挂起的时长（应远大于网关侧节点超时）
_TIMEOUT_HANG_SECONDS = 10.0


def create_mock_app(mock: MockNode) -> FastAPI:
    app = FastAPI(title=f"mock-upstream-{mock.name}")

    @app.post("/")
    async def rpc(payload: dict):
        req_id = payload.get("id")
        try:
            result = await mock.handle(payload.get("method", ""),
                                       payload.get("params"))
        except MockUpstreamError as exc:
            if exc.kind == "timeout":
                # 模拟节点挂起：网关侧将先触发自身超时
                await asyncio.sleep(_TIMEOUT_HANG_SECONDS)
                return Response(status_code=503)
            if exc.kind == "unavailable":
                # 模拟连接/网关层失败：HTTP 503，而非 JSON-RPC 业务错误
                return Response(status_code=503)
            return {"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32000, "message": str(exc)}}
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    @app.post("/admin/advance")
    async def advance(by: int = 1) -> dict:
        return {"chain_head": mock.advance_head(by)}

    @app.post("/admin/fail")
    async def fail(count: int = 1, kind: str = "unavailable") -> dict:
        mock.fail_next = count
        mock.fail_kind = kind
        return {"fail_next": mock.fail_next, "kind": kind}

    return app
