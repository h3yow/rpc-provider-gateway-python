"""上游异常归一化：router 与健康探测器共用，避免循环导入。"""
from __future__ import annotations

import asyncio
import logging

import httpx

from .errors import ErrorKind, GatewayError
from .mock_node import MockUpstreamError

logger = logging.getLogger("gateway.router")


def normalize(exc: BaseException) -> GatewayError:
    """把上游原始异常归一化为 GatewayError，绝不向调用方透传原始异常。"""
    if isinstance(exc, GatewayError):
        return exc
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return GatewayError(ErrorKind.UPSTREAM_TIMEOUT,
                            "upstream request timed out", retryable=True)
    if isinstance(exc, MockUpstreamError):
        if exc.kind == "timeout":
            return GatewayError(ErrorKind.UPSTREAM_TIMEOUT,
                                "upstream request timed out", retryable=True)
        if exc.kind == "unavailable":
            return GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE,
                                "upstream connection failed", retryable=True)
        return GatewayError(ErrorKind.UPSTREAM_ERROR,
                            "upstream returned an error", retryable=False)
    if httpx is not None and isinstance(exc, httpx.TimeoutException):
        return GatewayError(ErrorKind.UPSTREAM_TIMEOUT,
                            "upstream request timed out", retryable=True)
    if httpx is not None and isinstance(exc, httpx.HTTPError):
        return GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE,
                            "upstream connection failed", retryable=True)
    # 未知异常：归为不可用，消息中不携带原始异常内容
    logger.warning("unknown upstream exception type: %s", type(exc).__name__)
    return GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE,
                        "upstream request failed", retryable=True)
