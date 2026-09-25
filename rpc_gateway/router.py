"""节点注册与请求路由：健康过滤 + 链头新鲜度过滤 + 优先级/加权选择。

候选顺序（``select_candidates``）：
1. enabled 且健康熔断允许（CLOSED，或冷却到期的 HALF_OPEN 唯一试探）；
2. 头依赖读额外要求 ``node_head >= canonical_head``——落后节点不参与，
   从路由层杜绝跨节点"读到旧世界"的矛盾结果；
3. 按 priority 升序（数值小优先），同一优先级内按 weight 加权随机排序。
故障转移按返回列表顺序逐个尝试，因此优先级在重试中保持稳定。
"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Sequence

from .health import HealthMonitor

logger = logging.getLogger("rpc_gateway.router")


@dataclass
class NodeConfig:
    name: str
    url: str
    priority: int = 100          # 数值越小优先级越高
    weight: int = 1              # 同优先级内加权（>=1）
    enabled: bool = True
    node_head: int = 0           # 最近观测到的该节点链头


class NodeRegistry:
    def __init__(self, health: HealthMonitor) -> None:
        self._health = health
        self._nodes: dict[str, NodeConfig] = {}
        self._order: list[str] = []
        self._lock = asyncio.Lock()

    async def register(self, config: NodeConfig) -> None:
        async with self._lock:
            if config.name not in self._nodes:
                self._order.append(config.name)
            self._nodes[config.name] = config
            logger.info(
                "registry: node=%s url=%s priority=%d weight=%d registered",
                config.name, config.url, config.priority, config.weight,
            )

    async def set_enabled(self, name: str, enabled: bool) -> None:
        async with self._lock:
            config = self._nodes.get(name)
            if config is not None:
                config.enabled = enabled
                logger.info("registry: node=%s enabled=%s", name, enabled)

    async def update_node_head(self, name: str, head: int) -> None:
        async with self._lock:
            config = self._nodes.get(name)
            if config is not None and config.node_head != head:
                # 以最近观测为准：节点可能因同步延迟而落后（head 变小），
                # 单调取大将无法在路由层摘除落后节点。
                config.node_head = head

    def get(self, name: str) -> NodeConfig | None:
        return self._nodes.get(name)

    def all_names(self) -> Sequence[str]:
        return tuple(self._order)

    async def select_candidates(
        self,
        *,
        require_canonical_head: bool,
        canonical_head: int,
        rng: random.Random | None = None,
    ) -> list[NodeConfig]:
        """返回有序候选列表：健康 -> 新鲜度 -> priority -> 同组加权洗牌。"""
        rng = rng or random
        candidates: list[NodeConfig] = []
        rejected: list[tuple[str, str]] = []
        for name in self._order:
            config = self._nodes[name]
            if not config.enabled:
                rejected.append((name, "disabled"))
                continue
            if not await self._health.allow_request(name):
                rejected.append((name, "circuit_open"))
                continue
            if require_canonical_head and config.node_head < canonical_head:
                rejected.append(
                    (name, f"lagging(node_head={config.node_head}<{canonical_head})")
                )
                continue
            candidates.append(config)

        candidates.sort(key=lambda c: c.priority)
        grouped: list[NodeConfig] = []
        i = 0
        while i < len(candidates):
            j = i + 1
            while j < len(candidates) and candidates[j].priority == candidates[i].priority:
                j += 1
            tier = candidates[i:j]
            weights = [max(1, c.weight) for c in tier]
            # 不放回加权抽样，得到该优先级组内的尝试顺序
            pool = list(tier)
            pool_weights = list(weights)
            ordered: list[NodeConfig] = []
            while pool:
                idx = rng.choices(range(len(pool)), weights=pool_weights, k=1)[0]
                ordered.append(pool.pop(idx))
                pool_weights.pop(idx)
            grouped.extend(ordered)
            i = j

        logger.info(
            "route: require_head=%s canonical=%s ordered=[%s] rejected=[%s]",
            require_canonical_head, canonical_head,
            ", ".join(c.name for c in grouped),
            ", ".join(f"{n}:{r}" for n, r in rejected),
        )
        return grouped
