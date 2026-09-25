"""上游节点注册、健康追踪与按优先级/权重的选择。"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field

from .errors import ErrorKind, GatewayError

logger = logging.getLogger("gateway.upstream")

# 连续失败达到该次数后标记为不健康
FAILURE_THRESHOLD = 3


@dataclass
class UpstreamNode:
    """一个上游 JSON-RPC 节点。"""

    name: str
    url: str
    priority: int = 0          # 数值越小优先级越高
    weight: int = 1            # 同优先级内按权重随机选择
    timeout: float = 5.0
    healthy: bool = True
    consecutive_failures: int = 0
    chain_head: int = 0          # 该节点报告的链头高度
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class UpstreamRegistry:
    """上游注册表：负责节点选择、健康标记与故障转移候选枚举。"""

    def __init__(self) -> None:
        self._nodes: dict[str, UpstreamNode] = {}

    def register(self, node: UpstreamNode) -> None:
        self._nodes[node.name] = node
        logger.info("upstream registered: %s url=%s priority=%d weight=%d",
                    node.name, node.url, node.priority, node.weight)

    def get(self, name: str) -> UpstreamNode:
        return self._nodes[name]

    def all_nodes(self) -> list[UpstreamNode]:
        return list(self._nodes.values())

    async def select(self, exclude: set[str] | None = None) -> UpstreamNode:
        """按健康状态、优先级与权重选择一个节点；无健康节点时抛 NO_HEALTHY_UPSTREAM。

        exclude 用于故障转移：同一请求内已失败过的节点不再被选。
        """
        exclude = exclude or set()
        candidates = [
            n for n in self._nodes.values() if n.healthy and n.name not in exclude
        ]
        if not candidates:
            raise GatewayError(
                ErrorKind.NO_HEALTHY_UPSTREAM,
                f"no healthy upstream available (excluded: {sorted(exclude)})",
                retryable=True,
            )
        best_priority = min(n.priority for n in candidates)
        tier = [n for n in candidates if n.priority == best_priority]
        weights = [max(n.weight, 0) for n in tier]
        if sum(weights) == 0:
            chosen = random.choice(tier)
        else:
            chosen = random.choices(tier, weights=weights, k=1)[0]
        logger.info("upstream selected: %s (priority=%d, tier_size=%d, excluded=%s)",
                    chosen.name, chosen.priority, len(tier), sorted(exclude))
        return chosen

    async def report_success(self, node: UpstreamNode, chain_head: int | None = None) -> None:
        async with node._lock:
            was_unhealthy = not node.healthy
            node.healthy = True
            node.consecutive_failures = 0
            if chain_head is not None and chain_head > node.chain_head:
                node.chain_head = chain_head
        if was_unhealthy:
            logger.info("upstream recovered: %s", node.name)

    async def report_failure(self, node: UpstreamNode, error: GatewayError) -> None:
        async with node._lock:
            node.consecutive_failures += 1
            failures = node.consecutive_failures
            if failures >= FAILURE_THRESHOLD:
                node.healthy = False
        logger.warning("upstream failure: %s kind=%s consecutive=%d healthy=%s",
                       node.name, error.kind.value, failures, node.healthy)

    def max_chain_head(self) -> int:
        """所有节点报告的最高链头（用于缓存失效判定）。"""
        if not self._nodes:
            return 0
        return max(n.chain_head for n in self._nodes.values())
