"""上游节点注册、健康追踪、统一链头口径与按优先级/权重的选择。"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from enum import Enum

from .errors import ErrorKind, GatewayError

logger = logging.getLogger("gateway.upstream")

# 连续失败达到该次数后标记为 DOWN，等待恢复探测
FAILURE_THRESHOLD = 3


class NodeState(str, Enum):
    """节点生命周期状态。"""

    ACTIVE = "active"    # 参与正常选路
    DOWN = "down"        # 连续失败被摘除，等待恢复探测确认后才可放回
    JOINING = "joining"  # 运行时热加入，等待首次对齐探测，暂不参与选路与链头口径


@dataclass
class UpstreamNode:
    """一个上游 JSON-RPC 节点。"""

    name: str
    url: str
    priority: int = 0          # 数值越小优先级越高
    weight: int = 1            # 同优先级内按权重随机选择
    timeout: float = 5.0
    state: NodeState = NodeState.ACTIVE
    consecutive_failures: int = 0
    chain_head: int = 0          # 该节点最近一次报告的链头高度
    head_known: bool = False     # 是否至少报告过一次链头（未报告的节点不计入链头口径）
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def healthy(self) -> bool:
        """兼容视角：只有 ACTIVE 节点是健康的、可选路的。"""
        return self.state == NodeState.ACTIVE


class UpstreamRegistry:
    """上游注册表：节点选择、健康标记、运行时热变更与统一链头口径。

    统一链头口径（unified chain head）：全体 ACTIVE 节点中，被过半数（法定人数）
    确认的最高高度；尚未报告过链头的节点按 0 计（不确认任何高度）；口径只增不减
    （棘轮），因此：
    - 个别节点单方面领先不会推高口径、不会判废仍有效的缓存；
    - 落后或未对齐的节点不会拉低口径，其返回的内容不会被当成最新状态；
    - 落后节点追平后口径自然前进，此前仍有效的条目全程可复用。
    """

    def __init__(self) -> None:
        self._nodes: dict[str, UpstreamNode] = {}
        self._lock = asyncio.Lock()
        self._unified_head = 0

    # ---- 注册与运行时热变更 ----

    def register(self, node: UpstreamNode) -> None:
        """启动期注册：节点按给定状态（默认 ACTIVE）直接加入。"""
        self._nodes[node.name] = node
        logger.info("upstream registered: %s url=%s priority=%d weight=%d state=%s",
                    node.name, node.url, node.priority, node.weight, node.state.value)

    async def add_node(self, node: UpstreamNode) -> None:
        """运行时热加入：节点以 JOINING 状态进入，待探测对齐后才参与选路。

        新节点状态未对齐前既不承接流量，也不计入链头口径，避免污染整体判断。
        """
        async with self._lock:
            if node.name in self._nodes:
                raise GatewayError(ErrorKind.INVALID_REQUEST,
                                   f"upstream '{node.name}' already exists")
            node.state = NodeState.JOINING
            self._nodes[node.name] = node
        logger.info("upstream added at runtime: %s url=%s priority=%d weight=%d "
                    "(joining, awaiting alignment probe)",
                    node.name, node.url, node.priority, node.weight)

    async def remove_node(self, name: str) -> UpstreamNode:
        """运行时下线节点：立即从后续选路与链头口径中移除。

        已在处理中的请求持有节点引用，按各自进入时的结论收尾，不受移除影响。
        """
        async with self._lock:
            node = self._nodes.pop(name, None)
            if node is None:
                raise GatewayError(ErrorKind.INVALID_REQUEST,
                                   f"upstream '{name}' not found")
            self._refresh_unified_head_locked()
        logger.info("upstream removed at runtime: %s", name)
        return node

    async def update_node(self, name: str, *, priority: int | None = None,
                          weight: int | None = None,
                          timeout: float | None = None) -> UpstreamNode:
        """运行时调整优先级/权重/超时，对之后的选路立即生效。"""
        async with self._lock:
            node = self._nodes.get(name)
            if node is None:
                raise GatewayError(ErrorKind.INVALID_REQUEST,
                                   f"upstream '{name}' not found")
            if priority is not None:
                node.priority = priority
            if weight is not None:
                node.weight = weight
            if timeout is not None:
                node.timeout = timeout
        logger.info("upstream reconfigured: %s priority=%d weight=%d timeout=%.1f",
                    node.name, node.priority, node.weight, node.timeout)
        return node

    def get(self, name: str) -> UpstreamNode:
        return self._nodes[name]

    def all_nodes(self) -> list[UpstreamNode]:
        return list(self._nodes.values())

    def active_nodes(self) -> list[UpstreamNode]:
        return [n for n in self._nodes.values() if n.state == NodeState.ACTIVE]

    def nodes_awaiting_probe(self) -> list[UpstreamNode]:
        """需要恢复/对齐探测的节点（DOWN 或 JOINING）。"""
        return [n for n in self._nodes.values()
                if n.state in (NodeState.DOWN, NodeState.JOINING)]

    # ---- 选路 ----

    async def select(self, exclude: set[str] | None = None,
                     require_synced: bool = False) -> UpstreamNode:
        """按状态、优先级与权重选择节点；无 ACTIVE 节点时抛 NO_HEALTHY_UPSTREAM。

        exclude 用于故障转移：同一请求内已失败过的节点不再被选。
        require_synced 用于 HEAD_BOUND 请求：优先在链头已追平统一口径的节点中
        选择，避免把落后节点返回的内容当成最新状态；若无同步节点则降级为全体
        ACTIVE 节点（可用性优先，结果不会被当作最新缓存，由路由层负责）。
        """
        exclude = exclude or set()
        async with self._lock:
            candidates = [
                n for n in self._nodes.values()
                if n.state == NodeState.ACTIVE and n.name not in exclude
            ]
            if require_synced:
                synced = [n for n in candidates if n.chain_head >= self._unified_head]
                if synced:
                    if len(synced) < len(candidates):
                        logger.info("head-bound selection: %d/%d active nodes synced "
                                    "at unified head %d, lagging nodes skipped",
                                    len(synced), len(candidates), self._unified_head)
                    candidates = synced
                else:
                    logger.warning("head-bound selection degraded: no node synced at "
                                   "unified head %d, falling back to all active nodes",
                                   self._unified_head)
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

    # ---- 健康与链头上报 ----

    def _is_registered(self, node: UpstreamNode) -> bool:
        return self._nodes.get(node.name) is node

    async def report_success(self, node: UpstreamNode,
                             chain_head: int | None = None) -> None:
        """正常流量成功：重置失败计数并吸收链头信息。

        非 ACTIVE 节点（如处理中途被摘除）的迟到成功只记录链头证据，
        不改变其状态——重新放量只能由恢复探测确认，保证结论可解释。
        """
        if not self._is_registered(node):
            logger.info("late success ignored (node removed): %s", node.name)
            return
        async with node._lock:
            node.consecutive_failures = 0
            if chain_head is not None:
                if chain_head > node.chain_head:
                    node.chain_head = chain_head
                node.head_known = True
            state = node.state
        if state != NodeState.ACTIVE:
            logger.info("late success on %s node: %s (state unchanged, "
                        "reactivation requires recovery probes)", state.value, node.name)
        async with self._lock:
            self._refresh_unified_head_locked()

    async def report_failure(self, node: UpstreamNode, error: GatewayError) -> None:
        """正常流量失败：连续失败达阈值后摘除（ACTIVE -> DOWN）。"""
        if not self._is_registered(node):
            logger.info("late failure ignored (node removed): %s", node.name)
            return
        async with node._lock:
            node.consecutive_failures += 1
            failures = node.consecutive_failures
            if failures >= FAILURE_THRESHOLD and node.state == NodeState.ACTIVE:
                node.state = NodeState.DOWN
                logger.warning("upstream marked down: %s after %d consecutive "
                               "failures; recovery probing (with backoff) required "
                               "before rejoining", node.name, failures)
        logger.warning("upstream failure: %s kind=%s consecutive=%d state=%s",
                       node.name, error.kind.value, failures, node.state.value)
        async with self._lock:
            self._refresh_unified_head_locked()

    async def mark_active(self, node: UpstreamNode,
                          chain_head: int | None = None) -> None:
        """恢复探测确认后，把节点按原有优先级与权重放回选路。"""
        if not self._is_registered(node):
            return
        async with node._lock:
            previous = node.state
            node.state = NodeState.ACTIVE
            node.consecutive_failures = 0
            if chain_head is not None:
                if chain_head > node.chain_head:
                    node.chain_head = chain_head
                node.head_known = True
        logger.info("upstream rejoined routing: %s (%s -> active, priority=%d, "
                    "weight=%d, chain_head=%d)", node.name, previous.value,
                    node.priority, node.weight, node.chain_head)
        async with self._lock:
            self._refresh_unified_head_locked()

    # ---- 统一链头口径 ----

    def unified_chain_head(self) -> int:
        """对外统一链头口径：过半数 ACTIVE 节点确认的最高高度，只增不减。"""
        return self._unified_head

    def _refresh_unified_head_locked(self) -> None:
        """重算统一链头口径（调用方须持有 registry 锁）。棘轮：只前进不后退。

        法定人数按全体 ACTIVE 节点计算；尚未报告过链头的节点按 0 计
        （不确认任何高度），因此状态未对齐的节点只会推迟口径前进，
        绝不会单方面推高或拉低口径。
        """
        active = [n for n in self._nodes.values() if n.state == NodeState.ACTIVE]
        if not active:
            return
        quorum = len(active) // 2 + 1
        heads = sorted((n.chain_head if n.head_known else 0 for n in active),
                       reverse=True)
        confirmed = heads[quorum - 1]  # 至少 quorum 个节点达到的最高高度
        if confirmed > self._unified_head:
            logger.info("unified chain head advanced: %d -> %d "
                        "(quorum=%d of %d active nodes)",
                        self._unified_head, confirmed, quorum, len(active))
            self._unified_head = confirmed

    def chain_head_status(self) -> dict:
        """链头口径及其判断依据，供状态端点暴露，便于定位问题。"""
        active = [n for n in self._nodes.values() if n.state == NodeState.ACTIVE]
        quorum = len(active) // 2 + 1 if active else 0
        return {
            "unified_head": self._unified_head,
            "quorum": quorum,
            "active_nodes": len(active),
            "nodes": [
                {"name": n.name, "state": n.state.value,
                 "chain_head": n.chain_head, "head_known": n.head_known,
                 "synced": n.chain_head >= self._unified_head,
                 "counted_in_quorum": n.state == NodeState.ACTIVE}
                for n in self._nodes.values()
            ],
        }
