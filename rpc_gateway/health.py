"""节点健康状态与熔断。

* 连续失败达到阈值 -> OPEN（摘除）；OPEN 持续冷却后，下一次询问转入
  HALF_OPEN 且只放行一个试探请求；
* 试探成功 -> CLOSED；试探失败（或冷却内再来的失败）-> OPEN 重新计时；
* 所有状态转换在每节点一把锁内完成；并发故障只各自记录一次失败，
  OPEN 摘除与冷却计时只发生一次。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Callable

logger = logging.getLogger("rpc_gateway.health")


class HealthState(str, Enum):
    CLOSED = "closed"        # 正常，可承载流量
    OPEN = "open"            # 熔断摘除
    HALF_OPEN = "half_open"  # 冷却后试探中（仅一个在途请求）


@dataclass
class NodeHealth:
    state: HealthState
    consecutive_failures: int
    opened_at: float | None


class _State:
    __slots__ = ("state", "failures", "opened_at", "trial_in_flight", "lock")

    def __init__(self) -> None:
        self.state = HealthState.CLOSED
        self.failures = 0
        self.opened_at: float | None = None
        self.trial_in_flight = False
        self.lock = asyncio.Lock()


class HealthMonitor:
    def __init__(
        self,
        failure_threshold: int = 2,
        cool_down_seconds: float = 5.0,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._threshold = failure_threshold
        self._cool_down = cool_down_seconds
        self._clock = clock or _default_clock
        self._states: dict[str, _State] = {}
        self._guard = asyncio.Lock()

    async def _get(self, node: str) -> _State:
        state = self._states.get(node)
        if state is None:
            async with self._guard:
                state = self._states.get(node)
                if state is None:
                    state = _State()
                    self._states[node] = state
        return state

    async def allow_request(self, node: str, now: float | None = None) -> bool:
        """节点当前是否可承载流量。

        OPEN 冷却到期时，第一个询问把节点置为 HALF_OPEN 并独占试探权，
        同期其他请求仍被拒绝——这保证故障节点恢复时不会被并发瞬间打穿。
        """
        now = self._clock() if now is None else now
        state = await self._get(node)
        async with state.lock:
            if state.state is HealthState.CLOSED:
                return True
            if state.state is HealthState.OPEN:
                if state.opened_at is not None and now - state.opened_at >= self._cool_down:
                    state.state = HealthState.HALF_OPEN
                    state.trial_in_flight = True
                    logger.info(
                        "health: node=%s OPEN -> HALF_OPEN (trial granted)", node
                    )
                    return True
                return False
            # HALF_OPEN：同一时刻只允许一个试探
            if state.trial_in_flight:
                return False
            state.trial_in_flight = True
            return True

    async def record_success(self, node: str) -> None:
        state = await self._get(node)
        async with state.lock:
            prior = state.state
            state.state = HealthState.CLOSED
            state.failures = 0
            state.opened_at = None
            state.trial_in_flight = False
            if prior is not HealthState.CLOSED:
                logger.info("health: node=%s %s -> CLOSED (recovered)", node, prior.value)

    async def record_failure(self, node: str, now: float | None = None) -> None:
        now = self._clock() if now is None else now
        state = await self._get(node)
        async with state.lock:
            was_trial = state.state is HealthState.HALF_OPEN
            state.trial_in_flight = False
            state.failures += 1
            if was_trial or state.failures >= self._threshold:
                state.state = HealthState.OPEN
                state.opened_at = now
                logger.info(
                    "health: node=%s -> OPEN (failures=%d trial_failed=%s)",
                    node, state.failures, was_trial,
                )
            else:
                logger.info(
                    "health: node=%s failure recorded (failures=%d/%d)",
                    node, state.failures, self._threshold,
                )

    async def reset(self, node: str) -> None:
        state = await self._get(node)
        async with state.lock:
            state.state = HealthState.CLOSED
            state.failures = 0
            state.opened_at = None
            state.trial_in_flight = False

    async def snapshot(self) -> dict[str, NodeHealth]:
        out: dict[str, NodeHealth] = {}
        for node, state in self._states.items():
            async with state.lock:
                out[node] = NodeHealth(
                    state=state.state,
                    consecutive_failures=state.failures,
                    opened_at=state.opened_at,
                )
        return out


def _default_clock() -> float:
    import asyncio

    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:
        import time

        return time.monotonic()
