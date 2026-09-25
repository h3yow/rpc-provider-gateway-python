"""按调用方标识的速率与并发配额。"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from .errors import ErrorKind, GatewayError

logger = logging.getLogger("gateway.quota")


@dataclass
class QuotaConfig:
    rate_per_second: float = 100.0   # 令牌桶速率
    burst: int = 100                 # 令牌桶容量
    max_concurrent: int = 50         # 最大并发在途请求


@dataclass
class _CallerState:
    tokens: float
    last_refill: float
    in_flight: int = 0


class QuotaManager:
    """配额判定与上游调用保持原子对应。

    一次请求只 acquire 一次（故障转移重试不重复扣减）；
    acquire 成功后在同一把锁内完成令牌扣减与并发占用，保证不重复扣减、不漏扣。
    """

    def __init__(self, default: QuotaConfig | None = None) -> None:
        self._default = default or QuotaConfig()
        self._configs: dict[str, QuotaConfig] = {}
        self._states: dict[str, _CallerState] = {}
        self._lock = asyncio.Lock()

    def _config_for(self, caller: str) -> QuotaConfig:
        return self._configs.get(caller, self._default)

    def set_config(self, caller: str, config: QuotaConfig) -> None:
        """配置变更即时生效；进行中的请求结论稳定（不追溯取消）。

        令牌余额重置为新容量（变更即新的预算周期），并发占用不变。
        """
        self._configs[caller] = config
        state = self._states.get(caller)
        if state is not None:
            state.tokens = float(config.burst)
            state.last_refill = time.monotonic()
        logger.info("quota config updated: caller=%s rate=%.1f burst=%d max_concurrent=%d",
                    caller, config.rate_per_second, config.burst, config.max_concurrent)

    async def acquire(self, caller: str) -> "Permit":
        """获取一次调用许可；超额时抛 RATE_LIMITED / CONCURRENCY_LIMITED。"""
        async with self._lock:
            config = self._config_for(caller)
            state = self._states.get(caller)
            if state is None:
                state = _CallerState(tokens=float(config.burst),
                                     last_refill=time.monotonic())
                self._states[caller] = state
            # 令牌桶补充
            now = time.monotonic()
            elapsed = now - state.last_refill
            state.tokens = min(float(config.burst),
                               state.tokens + elapsed * config.rate_per_second)
            state.last_refill = now
            # 先查速率再查并发，拒绝原因可区分
            if state.tokens < 1.0:
                logger.info("quota denied: caller=%s reason=rate_limited tokens=%.2f",
                            caller, state.tokens)
                raise GatewayError(ErrorKind.RATE_LIMITED,
                                   f"caller '{caller}' rate limit exceeded",
                                   retryable=True)
            if state.in_flight >= config.max_concurrent:
                logger.info("quota denied: caller=%s reason=concurrency_limited in_flight=%d",
                            caller, state.in_flight)
                raise GatewayError(ErrorKind.CONCURRENCY_LIMITED,
                                   f"caller '{caller}' concurrency limit exceeded",
                                   retryable=True)
            # 原子扣减：令牌与并发占用在同一临界区内完成
            state.tokens -= 1.0
            state.in_flight += 1
            logger.info("quota acquired: caller=%s tokens=%.2f in_flight=%d",
                        caller, state.tokens, state.in_flight)
        return Permit(self, caller)

    async def _release(self, caller: str) -> None:
        async with self._lock:
            state = self._states.get(caller)
            if state is not None and state.in_flight > 0:
                state.in_flight -= 1
                logger.info("quota released: caller=%s in_flight=%d",
                            caller, state.in_flight)

    async def stats(self, caller: str) -> dict:
        async with self._lock:
            state = self._states.get(caller)
            config = self._config_for(caller)
            return {
                "tokens": state.tokens if state else float(config.burst),
                "in_flight": state.in_flight if state else 0,
                "rate_per_second": config.rate_per_second,
                "burst": config.burst,
                "max_concurrent": config.max_concurrent,
            }


class Permit:
    """一次调用的许可；异步上下文管理器，退出时释放并发占用（恰好一次）。"""

    def __init__(self, manager: QuotaManager, caller: str) -> None:
        self._manager = manager
        self._caller = caller
        self._released = False

    async def __aenter__(self) -> "Permit":
        return self

    async def __aexit__(self, *exc: object) -> None:
        if not self._released:
            self._released = True
            await self._manager._release(self._caller)
