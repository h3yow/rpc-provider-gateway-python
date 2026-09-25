"""路由引擎：组合配额、缓存、上游选择与故障转移。"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import httpx

from . import cache as cache_mod
from .cache import CachePolicy, ResultCache
from .errors import ErrorKind, GatewayError
from .mock_node import MockUpstreamError
from .quota import QuotaManager
from .upstream import UpstreamNode, UpstreamRegistry

logger = logging.getLogger("gateway.router")

# 传输层签名：给定节点与方法参数，返回 result 或抛异常
Transport = Callable[[UpstreamNode, str, list | None], Awaitable[Any]]


def _normalize(exc: BaseException) -> GatewayError:
    """把上游原始异常归一化为 GatewayError，绝不向调用方透传原始异常。"""
    if isinstance(exc, GatewayError):
        return exc
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return GatewayError(ErrorKind.UPSTREAM_TIMEOUT,
                            "upstream request timed out", retryable=True)
    if isinstance(exc, MockUpstreamError):
        if exc.kind == "timeout":
            return GatewayError(ErrorKind.UPSTREAM_TIMEOUT,
                                "upstream request timed out", retryable=True)
        if exc.kind == "unavailable":
            return GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE,
                                "upstream connection failed", retryable=True)
        return GatewayError(ErrorKind.UPSTREAM_ERROR,
                            "upstream returned an error", retryable=False)
    if httpx is not None and isinstance(exc, httpx.TimeoutException):
        return GatewayError(ErrorKind.UPSTREAM_TIMEOUT,
                            "upstream request timed out", retryable=True)
    if httpx is not None and isinstance(exc, httpx.HTTPError):
        return GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE,
                            "upstream connection failed", retryable=True)
    # 未知异常：归为不可用，消息中不携带原始异常内容
    logger.warning("unknown upstream exception type: %s", type(exc).__name__)
    return GatewayError(ErrorKind.UPSTREAM_UNAVAILABLE,
                        "upstream request failed", retryable=True)


class Router:
    def __init__(
        self,
        registry: UpstreamRegistry,
        cache: ResultCache,
        quota: QuotaManager,
        transport: Transport,
        max_retries: int = 3,
    ) -> None:
        self.registry = registry
        self.cache = cache
        self.quota = quota
        self.transport = transport
        self.max_retries = max_retries

    async def call(
        self, method: str, params: list | None, caller: str = "anonymous"
    ) -> Any:
        """执行一次 JSON-RPC 调用：配额 -> 缓存 -> 选择上游 -> 失败转移重试。"""
        policy = cache_mod.classify(method, params)
        key = cache_mod.make_key(method, params)

        if policy != CachePolicy.NON_CACHEABLE:
            hit, value = await self.cache.get(key, self.registry.max_chain_head())
            if hit:
                logger.info("served from cache: method=%s caller=%s", method, caller)
                return value

        # 配额一次请求只获取一次：故障转移重试不会重复扣减
        head_before = self.registry.max_chain_head()
        permit = await self.quota.acquire(caller)
        async with permit:
            result = await self._dispatch_with_failover(method, params, policy)

        if policy != CachePolicy.NON_CACHEABLE:
            # 条目按“抓取开始时网关已知链头”盖章；若抓取期间链头已前进，
            # 该条目会在下次读取时被判过期，从而杜绝过期读。
            stamp = head_before
            extracted = self._extract_chain_head(method, result)
            if extracted is not None:
                stamp = max(stamp, extracted)
            await self.cache.put(key, result, stamp, policy)
        return result

    async def refresh_chain_head(self) -> int:
        """主动向上游轮询链头并失效过期缓存。

        网关通过此方法（周期任务或测试显式触发）感知链头前进；
        HEAD_BOUND 条目在网关观测到更高链头后立即失效。
        """
        node = await self.registry.select()
        result = await self.transport(node, "eth_blockNumber", [])
        head = int(result, 16) if isinstance(result, str) else 0
        await self.registry.report_success(node, head)
        invalidated = await self.cache.invalidate_below(head)
        logger.info("chain head refreshed: head=%d invalidated=%d", head, invalidated)
        return head

    async def _dispatch_with_failover(
        self, method: str, params: list | None, policy: CachePolicy
    ) -> Any:
        tried: set[str] = set()
        last_error: GatewayError | None = None
        idempotent = policy != CachePolicy.NON_CACHEABLE

        for attempt in range(self.max_retries):
            try:
                node = await self.registry.select(exclude=tried)
            except GatewayError as exc:
                # 无更多健康节点可转移
                raise last_error if last_error is not None else exc
            try:
                result = await asyncio.wait_for(
                    self.transport(node, method, params), timeout=node.timeout
                )
            except Exception as exc:  # noqa: BLE001 - 统一归一化
                error = _normalize(exc)
                await self.registry.report_failure(node, error)
                tried.add(node.name)
                last_error = error
                # 非幂等请求在超时（结果不确定）时不得重试，避免重复执行；
                # 连接失败（请求未到达）对所有方法都可安全重试。
                can_retry = error.kind == ErrorKind.UPSTREAM_UNAVAILABLE or (
                    error.retryable and idempotent
                )
                logger.info(
                    "failover decision: method=%s node=%s kind=%s retry=%s attempt=%d",
                    method, node.name, error.kind.value, can_retry, attempt + 1,
                )
                if not can_retry:
                    raise error
                continue
            # 成功：上报健康并同步链头，随后主动失效过期缓存
            head = self._extract_chain_head(method, result)
            await self.registry.report_success(node, head)
            new_head = self.registry.max_chain_head()
            if new_head > 0:
                await self.cache.invalidate_below(new_head)
            logger.info("request served: method=%s node=%s attempt=%d",
                        method, node.name, attempt + 1)
            return result
        raise last_error if last_error is not None else GatewayError(
            ErrorKind.NO_HEALTHY_UPSTREAM, "no healthy upstream available",
            retryable=True,
        )

    @staticmethod
    def _extract_chain_head(method: str, result: Any) -> int | None:
        """从响应中提取链头高度（仅 eth_blockNumber 直接给出）。"""
        if method == "eth_blockNumber" and isinstance(result, str):
            try:
                return int(result, 16)
            except ValueError:
                return None
        return None
