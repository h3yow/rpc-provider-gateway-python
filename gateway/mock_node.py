"""本地模拟上游节点：可编程故障、延迟与链头推进。"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("gateway.mock")


class MockUpstreamError(Exception):
    """模拟节点抛出的“原始”异常，网关必须将其归一化，不得透传。"""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind  # "timeout" | "unavailable" | "rpc_error"


class MockNode:
    """进程内模拟 JSON-RPC 节点，供测试与本地验证使用。

    维护一条简化的模拟链：区块按高度编号，交易收据在提交后立即可查。
    """

    def __init__(self, name: str, chain_head: int = 0) -> None:
        self.name = name
        self.chain_head = chain_head
        self.fail_next: int = 0        # 接下来 N 次调用强制失败
        self.fail_kind: str = "unavailable"
        self.latency: float = 0.0      # 每次调用的人为延迟（秒）
        self.call_count: int = 0
        self._receipts: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    def advance_head(self, by: int = 1) -> int:
        self.chain_head += by
        logger.info("mock node %s advanced head to %d", self.name, self.chain_head)
        return self.chain_head

    async def handle(self, method: str, params: list | None) -> Any:
        """处理一次 JSON-RPC 调用，返回 result 或抛出 MockUpstreamError。"""
        async with self._lock:
            self.call_count += 1
            if self.fail_next > 0:
                self.fail_next -= 1
                kind = self.fail_kind
            else:
                kind = ""
        if self.latency > 0:
            await asyncio.sleep(self.latency)
        if kind:
            logger.warning("mock node %s failing call %s as %s", self.name, method, kind)
            raise MockUpstreamError(kind, f"mock node {self.name} injected {kind}")
        return self._dispatch(method, params or [])

    def _dispatch(self, method: str, params: list) -> Any:
        if method == "eth_blockNumber":
            return hex(self.chain_head)
        if method == "eth_getBlockByNumber":
            tag = params[0] if params else "latest"
            height = self.chain_head if tag in ("latest", "pending") else int(tag, 16)
            return {"number": hex(height), "hash": f"0xblock{height}",
                    "node": self.name}
        if method == "eth_getBlockByHash":
            return {"number": "0x1", "hash": params[0], "node": self.name}
        if method == "eth_getBalance":
            return hex(10**18 + self.chain_head)
        if method == "eth_getTransactionCount":
            return hex(self.chain_head)
        if method == "eth_call":
            return "0x" + f"{self.chain_head:064x}"
        if method == "eth_gasPrice":
            return hex(10**9)
        if method == "eth_sendRawTransaction":
            tx_hash = "0xtx" + params[0][2:]
            self._receipts[tx_hash] = {"transactionHash": tx_hash, "status": "0x1"}
            return tx_hash
        if method == "eth_getTransactionReceipt":
            return self._receipts.get(params[0])
        if method == "eth_getTransactionByHash":
            return {"hash": params[0], "node": self.name}
        raise MockUpstreamError("rpc_error", f"method not found: {method}")
