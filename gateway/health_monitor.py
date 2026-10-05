"""后台健康维护：链头轮询与摘除节点的退避恢复探测。"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from .errors import GatewayError
from .normalize import normalize
from .upstream import UpstreamNode, UpstreamRegistry

logger = logging.getLogger("gateway.monitor")

# 传输层签名：给定节点与方法参数，返回 result 或抛异常
Transport = Callable[[UpstreamNode, str, list | None], Awaitable[Any]]


class HealthMonitor:
    """周期性维护上游健康，不参与正常流量路径。

    - 对活跃健康节点轮询 eth_blockNumber，学习链头并推进统一口径；
    - 对被摘除节点按指数退避做只读探活，确认“可达且链头已追平共识”后
      才放回原优先级/权重层；恢复判定完成前不引流。

    退避：base_backoff * 2**attempts，封顶 max_backoff，避免反复重试压垮弱节点。
    """

    def __init__(self, registry: UpstreamRegistry, transport: Transport, *,
                 poll_interval: float = 1.0,
                 base_backoff: float = 0.2,
                 max_backoff: float = 5.0,
                 probe_timeout: float = 2.0) -> None:
        self.registry = registry
        self.transport = transport
        self.poll_interval = poll_interval
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.probe_timeout = probe_timeout
        self._task: asyncio.Task | None = None
        self._stop: asyncio.Event | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name="gateway-health-monitor")
        logger.info("health monitor started: poll_interval=%.2fs backoff=%.2f..%.2fs",
                    self.poll_interval, self.base_backoff, self.max_backoff)

    async def stop(self) -> None:
        if self._task is None:
            return
        assert self._stop is not None
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info("health monitor stopped")

    async def _run(self) -> None:
        assert self._stop is not None
        while not self._stop.is_set():
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - 后台任务永不因单轮异常退出
                logger.exception("health monitor poll error")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    def _backoff_for(self, attempts: int) -> float:
        backoff = min(self.max_backoff, self.base_backoff * (2 ** max(attempts, 0)))
        return backoff

    async def _read_head(self, node: UpstreamNode) -> int:
        result = await asyncio.wait_for(
            self.transport(node, "eth_blockNumber", []),
            timeout=self.probe_timeout,
        )
        return int(result, 16) if isinstance(result, str) else 0

    async def poll_once(self) -> dict:
        """执行一轮：先恢复探测（退避），再轮询活跃节点链头。返回本轮统计。"""
        import time
        now = time.monotonic()
        stats = {"probed": 0, "restored": 0, "still_down": 0, "behind": 0,
                 "polled": 0, "poll_failed": 0}

        # 1) 到点的摘除节点：只读探活，判定恢复前绝不引流
        for node in await self.registry.due_probes(now):
            stats["probed"] += 1
            backoff = self._backoff_for(node.probe_attempts)
            try:
                head = await self._read_head(node)
            except Exception as exc:  # noqa: BLE001 - 探活失败统一归一化
                error = normalize(exc)
                await self.registry.probe_failed(
                    node, time.monotonic() + backoff)
                stats["still_down"] += 1
                logger.info("recovery probe: %s unreachable kind=%s "
                            "(backoff %.2fs)", node.name, error.kind.value, backoff)
                continue
            consensus = self.registry.consensus_chain_head()
            if head < consensus:
                # 可达但落后：学习其高度（口径需要知道落后者在追），继续摘除
                await self.registry.probe_behind(
                    node, head, time.monotonic() + self.poll_interval)
                stats["behind"] += 1
                continue
            restored = await self.registry.probe_restored(node, head)
            if restored:
                stats["restored"] += 1

        # 2) 活跃健康节点：轮询链头，推进统一口径（并发、低开销）。
        # warming（未上报）节点也由这里轮询，完成首次链头对齐后才进入选路。
        active = [n for n in self.registry.all_nodes() if n.healthy]
        if active:
            results = await asyncio.gather(
                *(self._read_head(n) for n in active),
                return_exceptions=True,
            )
            for node, result in zip(active, results):
                if isinstance(result, BaseException):
                    stats["poll_failed"] += 1
                    error = _normalize(result)
                    await self.registry.report_failure(node, error)
                else:
                    stats["polled"] += 1
                    await self.registry.report_head(node, result)

        logger.info(
            "health poll: probed=%d restored=%d still_down=%d behind=%d "
            "polled=%d poll_failed=%d consensus=%d",
            stats["probed"], stats["restored"], stats["still_down"],
            stats["behind"], stats["polled"], stats["poll_failed"],
            self.registry.consensus_chain_head())
        return stats
