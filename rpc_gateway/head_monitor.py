"""canonical head 追踪与缓存失效编排。

推进模型（保持确定性与可解释性）：
* 周期探测所有 enabled 节点的 blockNumber，更新各自 node_head；
* 只有在"过半节点观测到同一新高度"时才把 canonical head 提升到该高度——
  单个超前节点无法独自推动全局，避免抖动；
* 任意业务响应带回的节点链头先记入注册表；若它超过 canonical head，
  同样走"过半确认"流程；
* 推进顺序严格为：先 ``cache.invalidate_head_tagged(new_head)``，
  之后才提升 canonical head。因此读到新 head 的请求必然看到的是失效后的缓存。
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .errors import GatewayError

if TYPE_CHECKING:
    from .cache import HeadAwareCache
    from .router import NodeRegistry, NodeConfig
    from .upstream import UpstreamTransport

logger = logging.getLogger("rpc_gateway.head")


class HeadMonitor:
    def __init__(
        self,
        registry: "NodeRegistry",
        cache: "HeadAwareCache",
        transport: "UpstreamTransport",
        *,
        poll_interval: float = 1.0,
        timeout: float = 1.0,
        initial_head: int = 0,
    ) -> None:
        self._registry = registry
        self._cache = cache
        self._transport = transport
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._head = initial_head
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()

    @property
    def canonical_head(self) -> int:
        return self._head

    async def observe_node_head(self, node: str, head: int) -> None:
        """业务响应带回节点链头时调用；必要时尝试推进。"""
        await self._registry.update_node_head(node, head)
        await self._maybe_advance(reason=f"observe:{node}")

    async def poll_once(self) -> int:
        """探测一轮所有 enabled 节点并按需推进，返回当前 canonical head。"""
        names = list(self._registry.all_names())
        results: dict[str, int] = {}
        for name in names:
            config = self._registry.get(name)
            if config is None or not config.enabled:
                continue
            try:
                result, _ = await self._transport.call(
                    config.url, "eth_blockNumber", (), timeout=self._timeout
                )
                value = int(result, 16) if isinstance(result, str) else int(result)
            except (GatewayError, ValueError, TypeError):
                # 探测失败不致命：跳过该节点，不触发熔断（业务调用才计失败）
                logger.info("head poll: node=%s unreachable, skipped", name)
                continue
            results[name] = value
            await self._registry.update_node_head(name, value)
            logger.info("head poll: node=%s reports head=%d", name, value)

        await self._maybe_advance(reason="poll", observations=results)
        return self._head

    async def _maybe_advance(
        self,
        *,
        reason: str,
        observations: dict[str, int] | None = None,
    ) -> None:
        async with self._lock:
            enabled_heads = [
                self._registry.get(n).node_head
                for n in self._registry.all_names()
                if (cfg := self._registry.get(n)) is not None and cfg.enabled
            ]
            if not enabled_heads:
                return
            total = len(enabled_heads)
            majority = total // 2 + 1

            target = self._head
            # 从高到低找第一个被过半节点达到的高度（单调递增）
            for candidate in sorted(set(enabled_heads), reverse=True):
                if candidate <= self._head:
                    break
                if sum(1 for h in enabled_heads if h >= candidate) >= majority:
                    target = candidate
                    break

            if target > self._head:
                # 关键顺序：先失效，后提升
                await self._cache.invalidate_head_tagged(target)
                logger.info(
                    "head advance: canonical %d -> %d (reason=%s, voters=%d/%d)",
                    self._head, target, reason,
                    sum(1 for h in enabled_heads if h >= target), total,
                )
                self._head = target
            else:
                logger.info(
                    "head no-advance: canonical=%s reason=%s heads=%s (need %d voters)",
                    self._head, reason, enabled_heads, majority,
                )

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="head-monitor")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.poll_once()
            except Exception:  # noqa: BLE001
                logger.exception("head monitor poll failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)
            except asyncio.TimeoutError:
                pass
