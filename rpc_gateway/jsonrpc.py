"""JSON-RPC 2.0 报文与批处理工具。"""
from __future__ import annotations

from typing import Any

JSONRPC_VERSION = "2.0"

# JSON-RPC 2.0 标准/预留错误码
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603


def make_result(id_: Any, result: Any) -> dict[str, Any]:
    """构造成功响应。"""
    return {"jsonrpc": JSONRPC_VERSION, "id": id_, "result": result}


def make_error(id_: Any, error: dict[str, Any]) -> dict[str, Any]:
    """构造错误响应。"""
    return {"jsonrpc": JSONRPC_VERSION, "id": id_, "error": error}


def is_notification(payload: dict[str, Any]) -> bool:
    """id 字段缺失的 JSON-RPC 请求视为通知，不产生响应。

    注意：按规范，id 显式为 null 仍然要响应（id=null）。
    """
    return "id" not in payload
