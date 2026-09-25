"""归一化错误类型：上游原始异常不得直接暴露给调用方。"""
from __future__ import annotations

from enum import Enum


class ErrorKind(str, Enum):
    """可区分的错误类别。"""

    UPSTREAM_TIMEOUT = "upstream_timeout"        # 上游超时
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"  # 上游连接失败/不可用
    UPSTREAM_ERROR = "upstream_error"            # 上游返回 JSON-RPC 错误
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"  # 无健康上游可路由
    RATE_LIMITED = "rate_limited"                # 速率配额超限
    CONCURRENCY_LIMITED = "concurrency_limited"  # 并发配额超限
    INVALID_REQUEST = "invalid_request"          # 请求本身非法
    INTERNAL = "internal"                        # 网关内部错误


# 映射到 JSON-RPC 错误码（避开实现自定义保留段 -32099..-32000 之外的冲突）
_ERROR_CODES: dict[ErrorKind, int] = {
    ErrorKind.UPSTREAM_TIMEOUT: -32001,
    ErrorKind.UPSTREAM_UNAVAILABLE: -32002,
    ErrorKind.NO_HEALTHY_UPSTREAM: -32003,
    ErrorKind.UPSTREAM_ERROR: -32004,
    ErrorKind.RATE_LIMITED: -32005,
    ErrorKind.CONCURRENCY_LIMITED: -32006,
    ErrorKind.INVALID_REQUEST: -32600,
    ErrorKind.INTERNAL: -32603,
}


class GatewayError(Exception):
    """网关对调用方暴露的唯一异常类型。"""

    def __init__(self, kind: ErrorKind, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.retryable = retryable

    @property
    def code(self) -> int:
        return _ERROR_CODES[self.kind]

    def to_jsonrpc_error(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "data": {"kind": self.kind.value, "retryable": self.retryable},
        }
