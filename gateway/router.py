"""路由引擎：组合配额、缓存、上游选择、故障转移、统一链头口径与自愈。"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import httpx

from . import cache as cache_mod
from .cache import CachePolicy, ResultCache
from .errors import ErrorKind, GatewayError
from .health_monitor import HealthMonitor
from .normalize import normalize
from .mock_node import MockUpstreamError
from .quota import QuotaManager
from .upstream import RoutingView, UpstreamNode, UpstreamRegistry

logger = logging.getLogger("gateway.router")

# 传输层签名：给定节点与方法参数，返回 result 或抛异常
Transport = Callable[[UpstreamNode, str, list | None], Awaitable[Any]]


class Router:
    def __init__(
        self,
        registry: UpstreamRegistry,
        cache: ResultCache,
        quota: QuotaManager,
        transport: Transport,
        max_retries: int = 3,
        monitor: HealthMonitor | None = None,
    ) -> None:
        self.registry = registry
        self.cache = cache
        self.quota = quota
        self.transport = transport
        self.max_retries = max_retries
        self.monitor = monitor

    async def start(self) -> None:
        """启动后台健康维护（链头轮询 + 恢复探测）。"""
        if self.monitor is not None:
            await self.monitor.start()

    async def stop(self) -> None:
        if self.monitor is not None:
            await self.monitor.stop()

    async def call(
        self, method: str, params: list | None, caller: str = "anonymous"
    ) -> Any:
        """执行一次 JSON-RPC 调用：配额 -> 缓存 -> 选择上游 -> 失败转移重试。

        请求进入时冻结一份路由视图；热变更只影响之后进入的请求，本请求在
        视图内按原优先级/权重/候选集完成故障转移，结论不被中途变更改写。
        """
        policy = cache_mod.classify(method, params)
        key = cache_mod.make_key(method, params)
        # 进入时刻快照：候选集/优先级/权重/链头口径在本请求内固定
        view = await self.registry.snapshot()

        if policy != CachePolicy.NON_CACHEABLE:
            hit, value = await self.cache.get(key, view.consensus_head)
            if hit:
                logger.info("served from cache: method=%s caller=%s head=%d",
                            method, caller, view.consensus_head)
                return value

        # 配额一次请求只获取一次：故障转移重试不会重复扣减
        permit = await self.quota.acquire(caller)
        async with permit:
            result, served_head = await self._dispatch_with_failover(
                method, params, policy, view)

        if policy != CachePolicy.NON_CACHEABLE:
            # 条目按“请求进入时”的统一链头口径盖章：若抓取期间口径已前进，
            # 该条目会在下次读取时按新口径判过期，杜绝把旧状态按新高度盖章
            # 造成的过期读。盖章不采用任何节点单方面的上报值。
            stamp = view.consensus_head
            await self.cache.put(key, result, stamp, policy)
            logger.info("cache decision: method=%s stamped_head=%d served_node_head=%s",
                        method, stamp, served_head)
        return result

    async def refresh_chain_head(self) -> int:
        """向全部活跃健康节点轮询链头，推进统一口径并失效过期缓存。

        生产环境由 HealthMonitor 周期执行；测试可显式触发。被摘除节点不走
        这里（它们由恢复探测通道处理，带退避且在恢复前不引流）。
        """
        nodes = [n for n in self.registry.all_nodes() if n.healthy]
        if not nodes:
            raise GatewayError(ErrorKind.NO_HEALTHY_UPSTREAM,
                               "no healthy upstream for chain head poll",
                               retryable=True)

        async def poll(node: UpstreamNode) -> int | None:
            try:
                result = await asyncio.wait_for(
                    self.transport(node, "eth_blockNumber", []),
                    timeout=node.timeout)
            except Exception as exc:  # noqa: BLE001 - 归一化后上报
                await self.registry.report_failure(node, normalize(exc))
                return None
            head = int(result, 16) if isinstance(result, str) else 0
            return head

        results = await asyncio.gather(*(poll(n) for n in nodes))
        heads = [(n, h) for n, h in zip(nodes, results) if h is not None]
        for node, head in heads:
            await self.registry.report_head(node, head)  # type: ignore[arg-type]
        consensus = self.registry.consensus_chain_head()
        invalidated = await self.cache.invalidate_below(consensus)
        logger.info("chain head refreshed: consensus=%d reports=%d invalidated=%d",
                    consensus, len(heads), invalidated)
        return consensus

    async def _dispatch_with_failover(
        self, method: str, params: list | None, policy: CachePolicy,
        view: RoutingView,
    ) -> tuple[Any, int | None]:
        tried: set[str] = set()
        last_error: GatewayError | None = None
        idempotent = policy != CachePolicy.NON_CACHEABLE

        for attempt in range(self.max_retries):
            try:
                node = await self.registry.select(view=view, exclude=tried)
            except GatewayError as exc:
                # 无更多健康节点可转移
                raise last_error if last_error is not None else exc
            try:
                result = await asyncio.wait_for(
                    self.transport(node, method, params), timeout=node.timeout
                )
            except Exception as exc:  # noqa: BLE001 - 统一归一化
                error = normalize(exc)
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
            # 成功：上报健康并学习链头（统一口径由注册表按多数重算）
            extracted = self._extract_chain_head(method, result)
            await self.registry.report_success(node, extracted)
            consensus = self.registry.consensus_chain_head()
            served_head = node.chain_head if node.head_reported else None
            if consensus > 0:
                await self.cache.invalidate_below(consensus)
            # eth_blockNumber 是网关对外的链头口径：单个节点领先时对外仍给
            # 统一口径值，避免调用方看到彼此矛盾的“链头”。
            if (method == "eth_blockNumber" and self.registry.head_settled()
                    and isinstance(result, str)):
                try:
                    reported = int(result, 16)
                except ValueError:
                    reported = consensus
                if reported > consensus:
                    logger.info(
                        "unify head response: node=%s reported=%d consensus=%d "
                        "(serving consensus)", node.name, reported, consensus)
                    result = hex(consensus)
            logger.info("request served: method=%s node=%s attempt=%d head=%s",
                        method, node.name, attempt + 1, served_head)
            return result, served_head
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
