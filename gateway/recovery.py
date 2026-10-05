"""节点自愈：被摘除的上游按指数退避自动探活，确认恢复后才放回选路。"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from .upstream import NodeState, UpstreamNode, UpstreamRegistry

logger = logging.getLogger("gateway.recovery")


@dataclass
class RecoveryConfig:
    """恢复探测参数。"""

    initial_interval: float = 1.0     # 首次探测前等待（秒）
    max_interval: float = 30.0        # 退避上限（秒）
    backoff_multiplier: float = 2.0   # 连续失败时的退避倍率
    successes_needed: int = 2         # DOWN 节点重新放量前需要的连续成功探测次数
    probe_timeout: float = 2.0        # 单次探测超时（秒）
    probe_method: str = "eth_blockNumber"  # 轻量探活方法
    tick: float = 0.2                 # 后台循环调度粒度（秒）


@dataclass
class _ProbeState:
    next_probe_at: float = 0.0
    interval: float = 0.0
    consecutive_successes: int = 0


class RecoveryProber:
    """对 DOWN/JOINING 节点做低开销探活。

    - 探测走独立通道：不占用调用方配额、不经过缓存、不影响在途请求；
    - 恢复判定完成（连续 successes_needed 次成功）前，节点保持 DOWN/JOINING，
      正常流量不会被引过去；
    - 探测失败按指数退避，避免把本来就弱的节点打垮；
    - 确认恢复后按节点原有优先级与权重放回选路。
    """

    def __init__(self, registry: UpstreamRegistry, transport,
                 config: RecoveryConfig | None = None) -> None:
        self.registry = registry
        self.transport = transport
        self.config = config or RecoveryConfig()
        self._states: dict[str, _ProbeState] = {}
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        """启动后台探测循环（幂等）。"""
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())
        logger.info("recovery prober started: initial=%.2fs max=%.2fs "
                    "successes_needed=%d", self.config.initial_interval,
                    self.config.max_interval, self.config.successes_needed)

    async def stop(self) -> None:
        """停止后台循环并等待退出。"""
        self._stopped.set()
        if self._task is not None:
            await self._task
            self._task = None
            logger.info("recovery prober stopped")

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.probe_due()
            except Exception:  # noqa: BLE001 - 探测循环不得因单次异常退出
                logger.exception("recovery probe tick failed")
            try:
                await asyncio.wait_for(self._stopped.wait(),
                                       timeout=self.config.tick)
            except asyncio.TimeoutError:
                pass

    async def probe_due(self) -> list[str]:
        """探测所有已到期的节点，返回本轮被探测的节点名。

        测试可不启动后台循环、直接调用本方法确定性地推进恢复流程。
        """
        now = time.monotonic()
        probed: list[str] = []
        awaiting = self.registry.nodes_awaiting_probe()
        for node in awaiting:
            state = self._states.get(node.name)
            if state is None:
                state = self._states[node.name] = _ProbeState(
                    next_probe_at=now + self.config.initial_interval,
                    interval=self.config.initial_interval,
                )
                logger.info("recovery probe scheduled: %s state=%s first_probe_in=%.2fs",
                            node.name, node.state.value, self.config.initial_interval)
            if state.next_probe_at > now:
                continue
            await self._probe(node, state)
            probed.append(node.name)
        # 清理已不再需要探测的节点状态（被移除或已恢复）
        awaiting_names = {n.name for n in self.registry.nodes_awaiting_probe()}
        for name in list(self._states):
            if name not in awaiting_names:
                del self._states[name]
        return probed

    async def probe_node(self, name: str) -> bool:
        """立即探测指定节点（忽略退避调度），返回本次探测是否成功。"""
        node = self.registry.get(name)
        state = self._states.setdefault(
            name, _ProbeState(interval=self.config.initial_interval))
        return await self._probe(node, state)

    def forget(self, name: str) -> None:
        """节点被移除时丢弃其探测状态。"""
        self._states.pop(name, None)

    async def _probe(self, node: UpstreamNode, state: _ProbeState) -> bool:
        try:
            result = await asyncio.wait_for(
                self.transport(node, self.config.probe_method, []),
                timeout=self.config.probe_timeout,
            )
            head = int(result, 16) if isinstance(result, str) else None
        except Exception as exc:  # noqa: BLE001 - 任何失败都按退避处理
            state.consecutive_successes = 0
            state.interval = min(
                max(state.interval, self.config.initial_interval)
                * self.config.backoff_multiplier,
                self.config.max_interval,
            )
            state.next_probe_at = time.monotonic() + state.interval
            logger.warning("recovery probe failed: %s state=%s error=%s; "
                           "backing off, next probe in %.2fs",
                           node.name, node.state.value, type(exc).__name__,
                           state.interval)
            return False

        state.consecutive_successes += 1
        needed = 1 if node.state == NodeState.JOINING else self.config.successes_needed
        if state.consecutive_successes >= needed:
            await self.registry.mark_active(node, head)
            self._states.pop(node.name, None)
            logger.info("upstream recovered: %s after %d consecutive successful "
                        "probes (chain_head=%s)", node.name,
                        state.consecutive_successes, head)
            return True
        # 尚未达到连续成功次数：尽快安排下一次确认探测（不使用失败退避）
        state.next_probe_at = time.monotonic() + self.config.initial_interval
        logger.info("recovery probe succeeded: %s (%d/%d confirmations)",
                    node.name, state.consecutive_successes, needed)
        return True
