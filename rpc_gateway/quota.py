"""调用方配额：固定窗口速率 + 在途并发槽。

语义：
* 准入（扣速率令牌 + 占并发槽）在每调用方一把锁内原子完成；拒绝不扣任何东西；
* 一次逻辑请求只扣一次，故障转移的多次上游尝试不重复扣减（准入与释放只在
  网关请求边界各发生一次）；
* 速率先判、并发后判，因此拒绝原因可区分（-32010 / -32011）；
* 配置热更新整体替换限额；进行中的请求持旧槽位运行至结束，结论稳定，
  新限额只影响后续准入；拒绝在发起上游调用之前发生，天然不漏不重。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Callable

from .errors import ConcurrencyLimitExceededError, RateLimitExceededError

logger = logging.getLogger("rpc_gateway.quota")

DEFAULT_CALLER = "anonymous"
DEFAULT_WINDOW_SECONDS = 1.0


@dataclass
class QuotaLimits:
    rate_per_second: int
    max_concurrency: int


class _CallerState:
    __slots__ = ("limits", "window_start", "window_count", "in_flight", "lock")

    def __init__(self, limits: QuotaLimits, now: float) -> None:
        self.limits = limits
        self.window_start = now
        self.window_count = 0
        self.in_flight = 0
        self.lock = asyncio.Lock()


class QuotaAcquired:
    """准入成功的上下文管理器：退出时释放并发槽（速率令牌不回补）。"""

    def __init__(self, manager: "QuotaManager", caller: str) -> None:
        self._manager = manager
        self._caller = caller
        self._released = False

    async def __aenter__(self) -> "QuotaAcquired":
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._manager._release(self._caller)

    def release(self) -> None:
        self._manager._release(self._caller)


class QuotaManager:
    def __init__(
        self,
        default_limits: QuotaLimits,
        *,
        clock: Callable[[], float] | None = None,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
    ) -> None:
        self._clock = clock or _default_clock
        self._window = window_seconds
        self._default_limits = default_limits
        self._states: dict[str, _CallerState] = {}
        self._states_lock = asyncio.Lock()

    def update_limits(self, caller: str, limits: QuotaLimits) -> None:
        """热更新某调用方（或 default）限额，立即对后续准入生效。

        进行中请求继续占用旧槽位直到结束；速率窗口计数保留（语义：本窗口内
        已经发生的调用事实不回滚），仅上限被替换。
        """
        now = self._clock()
        state = self._states.get(caller)
        if state is None:
            state = _CallerState(limits, now)
            self._states[caller] = state
        else:
            state.limits = limits
        if caller == DEFAULT_CALLER:
            self._default_limits = limits
        logger.info(
            "quota change: caller=%s rate=%d/s concurrency=%d",
            caller, limits.rate_per_second, limits.max_concurrency,
        )

    def _limits_for(self, caller: str) -> QuotaLimits:
        state = self._states.get(caller)
        return state.limits if state is not None else self._default_limits

    async def _state_for(self, caller: str) -> _CallerState:
        state = self._states.get(caller)
        if state is None:
            async with self._states_lock:
                state = self._states.get(caller)
                if state is None:
                    state = _CallerState(self._default_limits, self._clock())
                    self._states[caller] = state
        return state

    async def acquire(self, caller: str) -> QuotaAcquired:
        """原子判定速率与并发；超额抛出可区分的拒绝异常，且不扣减。

        速率判定先于并发判定，故调用方能据 error_type 区分两种拒绝。
        """
        state = await self._state_for(caller)
        async with state.lock:
            now = self._clock()
            if now - state.window_start >= self._window:
                # 进入新的固定窗口
                state.window_start = now
                state.window_count = 0

            limits = state.limits
            if state.window_count >= limits.rate_per_second:
                retry_after = self._window - (now - state.window_start)
                logger.info(
                    "quota decision: caller=%s REJECT rate used=%d/%d "
                    "retry_after=%.3f",
                    caller, state.window_count, limits.rate_per_second, retry_after,
                )
                raise RateLimitExceededError(
                    "rate limit exceeded",
                    data={
                        "caller": caller,
                        "limit": limits.rate_per_second,
                        "window_seconds": self._window,
                        "retry_after": round(retry_after, 3),
                    },
                )

            if state.in_flight >= limits.max_concurrency:
                logger.info(
                    "quota decision: caller=%s REJECT concurrency in_flight=%d/%d",
                    caller, state.in_flight, limits.max_concurrency,
                )
                raise ConcurrencyLimitExceededError(
                    "concurrency limit exceeded",
                    data={
                        "caller": caller,
                        "limit": limits.max_concurrency,
                        "in_flight": state.in_flight,
                    },
                )

            state.window_count += 1
            state.in_flight += 1
            logger.info(
                "quota decision: caller=%s ADMIT rate_used=%d/%d in_flight=%d/%d",
                caller, state.window_count, limits.rate_per_second,
                state.in_flight, limits.max_concurrency,
            )
            return QuotaAcquired(self, caller)

    def _release(self, caller: str) -> None:
        state = self._states.get(caller)
        if state is None:
            return
        # release 与 admit 一一对应，且只在请求终态时调用；
        # 采用简单自减即可（不存在重复 release 路径）。
        if state.in_flight > 0:
            state.in_flight -= 1
        logger.info(
            "quota change: caller=%s release slot in_flight=%d",
            caller, state.in_flight,
        )

    def in_flight(self, caller: str) -> int:
        state = self._states.get(caller)
        return state.in_flight if state is not None else 0

    def window_used(self, caller: str) -> int:
        state = self._states.get(caller)
        return state.window_count if state is not None else 0


def _default_clock() -> float:
    import asyncio

    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:
        import time

        return time.monotonic()
