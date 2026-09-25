"""归一化错误类型。

上游（模拟节点 / HTTP 传输）抛出的任何异常都必须在网关内被转换为
:class:`GatewayError`，调用方永远拿不到上游原始异常类型或堆栈。
"""
from __future__ import annotations

from typing import Any


class GatewayError(Exception):
    """所有对调用方可见错误的基类。

    :param code: 自定义 JSON-RPC 错误码
    :param message: 面向调用方的安全消息（不含上游内部细节）
    :param error_type: 可区分的错误类型标识
    :param data: 附加的结构化数据（同样不得包含上游原始异常）
    """

    code = -32000
    error_type = "gateway_error"

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        error_type: str | None = None,
        data: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if error_type is not None:
            self.error_type = error_type
        # error_type 始终进入结构化 data，调用方在响应与异常对象两处
        # 都能用同一字段区分失败原因；不附带任何上游原始信息。
        payload = dict(data) if isinstance(data, dict) else {}
        payload["error_type"] = self.error_type
        if data is not None and not isinstance(data, dict):
            payload["detail"] = data
        self.data = payload

    def to_json_rpc(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "data": self.data}


class UpstreamUnavailableError(GatewayError):
    """所有上游均不可用（健康熔断 / 全部调用失败）。"""

    code = -32001
    error_type = "upstream_unavailable"


class UpstreamTimeoutError(GatewayError):
    """所选上游在超时窗口内没有响应。"""

    code = -32002
    error_type = "upstream_timeout"


class UpstreamRPCError(GatewayError):
    """上游返回了 JSON-RPC error（已脱敏归一化）。"""

    code = -32003
    error_type = "upstream_rpc_error"


class NoHealthyNodeError(GatewayError):
    """当前没有通过健康检查、且满足读取要求的节点。"""

    code = -32004
    error_type = "no_healthy_node"


class RateLimitExceededError(GatewayError):
    """调用方超出单位时间速率配额。"""

    code = -32010
    error_type = "rate_limit_exceeded"


class ConcurrencyLimitExceededError(GatewayError):
    """调用方超出在途并发配额。"""

    code = -32011
    error_type = "concurrency_limit_exceeded"


class InvalidRequestError(GatewayError):
    """调用方请求格式非法。"""

    code = -32600
    error_type = "invalid_request"


class MethodNotAllowedError(GatewayError):
    """方法未被网关登记 / 不允许。"""

    code = -32601
    error_type = "method_not_allowed"
