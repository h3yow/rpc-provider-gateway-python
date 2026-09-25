"""进程内上游传输：直接通过 ASGI 调用模拟节点，不经过真实端口。

* 供测试与本地确定性演示使用，行为与 HTTP 传输一致（含故障翻译）；
* 把模拟节点的 NodeFault 翻译为网关的归一化 GatewayError；
* 节点"宕机/超时"同样只产生 UpstreamTimeoutError / UpstreamUnavailableError。
"""
from __future__ import annotations

import asyncio
import itertools
import json as _json
import logging
from typing import Any

import httpx

from mocknode.cluster import MockCluster, NodeFault

from .errors import (
    UpstreamRPCError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)

logger = logging.getLogger("rpc_gateway.upstream")


class InProcessTransport:
    """:param urls: 节点名 -> ASGI app；可调用对象 cluster.call 亦可。"""

    def __init__(self, cluster: MockCluster) -> None:
        self._cluster = cluster
        self._ids = itertools.count(1)

    async def call(
        self,
        url: str,
        method: str,
        params: tuple[Any, ...],
        *,
        timeout: float,
        idempotency_key: str | None = None,
    ) -> tuple[Any, int | None]:
        node = url  # 进程内传输以节点名作为 url
        try:
            return await asyncio.wait_for(
                self._invoke(node, method, params, idempotency_key),
                timeout=timeout,
            )
        except TimeoutError:
            logger.info("inprocess: node=%s method=%s TIMEOUT", node, method)
            raise UpstreamTimeoutError(
                "upstream request timed out", data={"method": method}
            ) from None

    async def _invoke(
        self,
        node: str,
        method: str,
        params: tuple[Any, ...],
        idem: str | None,
    ) -> tuple[Any, int | None]:
        try:
            if method == "eth_sendRawTransaction":
                raw_tx = params[0] if params else ""
                key = idem or f"auto:{raw_tx}"
                outcome = await self._cluster.apply_transaction(key, raw_tx)
                result: Any = outcome["tx_hash"]
            else:
                result = await self._cluster.call(node, method, params)
        except NodeFault as fault:
            if fault.kind in ("timeout",):
                # 与 HTTP 挂起等价：长时间等待（由外层 wait_for 超时/任务取消中断）
                await asyncio.sleep(3600)
                raise RuntimeError("unreachable")  # pragma: no cover
            if fault.kind == "http_500":
                raise UpstreamUnavailableError(
                    "upstream server error",
                    data={"method": method, "http_status": 500},
                ) from None
            raise UpstreamRPCError(
                "upstream rejected the request",
                data={"method": method, "upstream_code": -32000},
            ) from None

        node_head = self._cluster.view_head(node)
        return result, node_head
