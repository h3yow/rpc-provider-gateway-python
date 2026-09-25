"""上游 HTTP/JSON-RPC 传输层：把上游一切异常归一化，绝不裸传。

归一化出口只有三类（均可区分、面向调用方安全）：
* :class:`UpstreamTimeoutError`  —— 超时
* :class:`UpstreamUnavailableError` —— 连接失败 / 5xx / 非法响应体
* :class:`UpstreamRPCError` —— 上游返回 JSON-RPC error（仅保留其错误码）

调用方永远拿不到 httpx 异常对象、URL 之外的内部堆栈或原始错误文本。
节点自身链头通过约定响应头 ``X-Mock-Head`` 回传（模拟节点实现）。
"""
from __future__ import annotations

import itertools
import json as _json
import logging
from typing import Any, Protocol

import httpx

from .errors import (
    UpstreamRPCError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)

logger = logging.getLogger("rpc_gateway.upstream")

NODE_HEAD_HEADER = "x-mock-head"


class UpstreamTransport(Protocol):
    async def call(
        self,
        url: str,
        method: str,
        params: tuple[Any, ...],
        *,
        timeout: float,
        idempotency_key: str | None = None,
    ) -> tuple[Any, int | None]:
        """返回 ``(result, 节点链头|None)``；失败只抛 GatewayError 子类。"""
        ...


class HttpTransport:
    """基于 httpx 的真实 HTTP 传输。"""

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client
        self._owns_client = client is None
        self._ids = itertools.count(1)

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()

    async def call(
        self,
        url: str,
        method: str,
        params: tuple[Any, ...],
        *,
        timeout: float,
        idempotency_key: str | None = None,
    ) -> tuple[Any, int | None]:
        if self._client is None:
            self._client = httpx.AsyncClient()
        body: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": next(self._ids),
            "method": method,
            "params": list(params),
        }
        if idempotency_key is not None:
            # 与模拟节点约定的去重元数据；真实节点忽略未知字段也无副作用
            body["xIdempotencyKey"] = idempotency_key
        headers = {"content-type": "application/json"}
        try:
            resp = await self._client.post(
                url, content=_json.dumps(body), headers=headers, timeout=timeout
            )
        except httpx.TimeoutException:
            logger.info("upstream: url=%s method=%s TIMEOUT", url, method)
            raise UpstreamTimeoutError(
                "upstream request timed out",
                data={"method": method},
            ) from None
        except httpx.HTTPError:
            # 连接拒绝 / DNS / 连接重置等统一归一化，原始异常不外泄
            logger.info("upstream: url=%s method=%s CONNECT/HTTP ERROR", url, method)
            raise UpstreamUnavailableError(
                "upstream connection failed",
                data={"method": method},
            ) from None

        if resp.status_code >= 500:
            logger.info(
                "upstream: url=%s method=%s SERVER_ERROR status=%d",
                url, method, resp.status_code,
            )
            raise UpstreamUnavailableError(
                "upstream server error",
                data={"method": method, "http_status": resp.status_code},
            )

        try:
            payload = resp.json()
        except ValueError:
            logger.info("upstream: url=%s method=%s BAD_JSON", url, method)
            raise UpstreamUnavailableError(
                "upstream returned a non-JSON response",
                data={"method": method},
            ) from None

        if not isinstance(payload, dict) or "jsonrpc" not in payload:
            logger.info("upstream: url=%s method=%s BAD_PAYLOAD", url, method)
            raise UpstreamUnavailableError(
                "upstream returned a malformed JSON-RPC response",
                data={"method": method},
            )

        node_head = _parse_head(resp.headers.get(NODE_HEAD_HEADER))

        if "error" in payload:
            error = payload["error"]
            if not isinstance(error, dict):
                raise UpstreamUnavailableError(
                    "upstream returned a malformed error object",
                    data={"method": method},
                )
            upstream_code = error.get("code") if isinstance(error.get("code"), int) else None
            logger.info(
                "upstream: url=%s method=%s RPC_ERROR upstream_code=%s",
                url, method, upstream_code,
            )
            # 不透传上游 message；只保留数值错误码，文本使用统一安全措辞
            raise UpstreamRPCError(
                "upstream rejected the request",
                data={"method": method, "upstream_code": upstream_code},
            )

        if "result" not in payload:
            raise UpstreamUnavailableError(
                "upstream response missing result",
                data={"method": method},
            )

        logger.info(
            "upstream: url=%s method=%s OK node_head=%s", url, method, node_head
        )
        return payload["result"], node_head


def _parse_head(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw, 16) if raw.startswith("0x") else int(raw)
    except (TypeError, ValueError):
        return None
