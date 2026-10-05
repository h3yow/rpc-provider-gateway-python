"""上游节点注册、健康追踪、热变更与统一链头口径。"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass

from .errors import ErrorKind, GatewayError

logger = logging.getLogger("gateway.upstream")

# 连续失败达到该次数后标记为不健康并摘除
FAILURE_THRESHOLD = 3


@dataclass(frozen=True)
class NodeRoute:
    """选路时刻节点的不可变快照：请求在整个故障转移过程中按此视图收尾。"""

    name: str
    priority: int
    weight: int


@dataclass(frozen=True)
class RoutingView:
    """一次请求进入时的注册表路由视图（冻结）。

    revision 单调递增；热变更后 revision 前进，旧视图仍可让在途请求按原结论收尾。
    """

    revision: int
    routes: dict[str, NodeRoute]
    consensus_head: int
    head_settled: bool


@dataclass
class UpstreamNode:
    """一个上游 JSON-RPC 节点及其运行时状态。"""

    name: str
    url: str
    priority: int = 0           # 数值越小优先级越高
    weight: int = 1             # 同优先级内按权重随机选择
    timeout: float = 5.0
    healthy: bool = True
    consecutive_failures: int = 0
    chain_head: int = 0           # 该节点最近报告的链头高度
    head_reported: bool = False   # 是否曾报告链头（报告前不参与选路门控与共识）
    retired: bool = False         # 运行时下线：永不再参与选路/探测
    # 恢复探测退避状态
    probe_attempts: int = 0
    probe_next_at: float = 0.0


class UpstreamRegistry:
    """上游注册表：热变更、选路快照、健康/恢复追踪与共识链头。

    统一链头口径：在健康、未下线且报告过链头的节点中，取“至少多数（quorum）
    节点都已达到”的最高高度作为共识链头。落后节点不参与对外服务；单个节点
    单方面领先不会顶高口径；新加入/刚恢复的节点在首次链头对齐前不参与选路
    与共识投票。链头口径尚未确立（冷启动、尚无任何链头上报）时不做对齐门控，
    允许在健康节点上引导首批请求。
    """

    def __init__(self) -> None:
        self._nodes: dict[str, UpstreamNode] = {}
        self._lock = asyncio.Lock()
        self._revision = 0
        self._consensus_head = 0
        self._head_settled = False

    # ---- 配置：启动注册与运行时热变更走同一套临界区语义 ----

    def register(self, node: UpstreamNode) -> None:
        if node.name in self._nodes:
            raise ValueError(f"upstream already registered: {node.name}")
        self._nodes[node.name] = node
        self._revision += 1
        logger.info("upstream registered: %s url=%s priority=%d weight=%d rev=%d",
                    node.name, node.url, node.priority, node.weight, self._revision)

    def add_node(self, node: UpstreamNode) -> None:
        """运行时新增节点：首次链头对齐前处于 warming，不参与选路与共识。"""
        existing = self._nodes.get(node.name)
        if existing is not None and not existing.retired:
            raise ValueError(f"upstream already active: {node.name}")
        node.head_reported = False
        node.healthy = True
        node.retired = False
        node.consecutive_failures = 0
        node.probe_attempts = 0
        self._nodes[node.name] = node
        self._revision += 1
        logger.info("upstream added at runtime: %s priority=%d weight=%d rev=%d "
                    "(warming until chain head aligns)",
                    node.name, node.priority, node.weight, self._revision)

    def remove_node(self, name: str) -> bool:
        """运行时下线节点：立即离开候选集；在途请求仍可引用该节点对象收尾。"""
        node = self._nodes.get(name)
        if node is None or node.retired:
            return False
        node.retired = True
        node.healthy = False
        self._revision += 1
        self._recompute_consensus_locked()
        logger.info("upstream removed at runtime: %s rev=%d (in-flight requests "
                    "finish on their snapshot; consensus_head=%d settled=%s)",
                    name, self._revision, self._consensus_head, self._head_settled)
        return True

    def update_node(self, name: str, *, priority: int | None = None,
                    weight: int | None = None) -> UpstreamNode:
        """运行时调整优先级/权重：对之后的选路立即生效；在途请求沿用旧视图。"""
        node = self._nodes.get(name)
        if node is None or node.retired:
            raise KeyError(f"upstream not active: {name}")
        changes = []
        if priority is not None and priority != node.priority:
            changes.append(f"priority {node.priority}->{priority}")
            node.priority = priority
        if weight is not None and weight != node.weight:
            changes.append(f"weight {node.weight}->{weight}")
            node.weight = weight
        if changes:
            self._revision += 1
            logger.info("upstream updated at runtime: %s %s rev=%d",
                        name, ", ".join(changes), self._revision)
        return node

    def get(self, name: str) -> UpstreamNode:
        return self._nodes[name]

    def all_nodes(self) -> list[UpstreamNode]:
        return [n for n in self._nodes.values() if not n.retired]

    @property
    def revision(self) -> int:
        return self._revision

    # ---- 快照与选路 ----

    async def snapshot(self) -> RoutingView:
        async with self._lock:
            routes = {
                n.name: NodeRoute(n.name, n.priority, n.weight)
                for n in self._nodes.values() if not n.retired
            }
            return RoutingView(self._revision, dict(routes),
                               self._consensus_head, self._head_settled)

    def _eligible_live(self, node: UpstreamNode, view: RoutingView) -> bool:
        """实时门控：下线/摘除/未对齐的节点不引流；口径未确立时仅做健康门控。"""
        if node.retired or not node.healthy:
            return False
        if not view.head_settled:
            return True  # 冷启动引导：尚无链头口径，允许健康节点承接首批请求
        if not node.head_reported or node.chain_head < view.consensus_head:
            return False
        return True

    async def select(self, view: RoutingView | None = None,
                     exclude: set[str] | None = None) -> UpstreamNode:
        """在请求进入时的视图内按优先级/权重选择一个已对齐的健康节点。

        - view：请求进入时冻结的路由视图（候选集合、优先级、权重）；
          缺省时即时快照（向后兼容）。
        - exclude：同一请求内已失败过的节点不再被选。
        健康/对齐为实时门控：摘除与下线对当次故障转移也立即安全。
        """
        if view is None:
            view = await self.snapshot()
        exclude = exclude or set()
        candidates: list[UpstreamNode] = []
        for name in view.routes:
            if name in exclude:
                continue
            node = self._nodes.get(name)
            if node is not None and self._eligible_live(node, view):
                candidates.append(node)
        if not candidates:
            raise GatewayError(
                ErrorKind.NO_HEALTHY_UPSTREAM,
                f"no healthy upstream available (rev={view.revision}, "
                f"settled={view.head_settled}, head={view.consensus_head}, "
                f"excluded={sorted(exclude)})",
                retryable=True,
            )
        best_priority = min(n.priority for n in candidates)
        tier = [n for n in candidates if n.priority == best_priority]
        weights = [max(n.weight, 0) for n in tier]
        if sum(weights) == 0:
            chosen = random.choice(tier)
        else:
            chosen = random.choices(tier, weights=weights, k=1)[0]
        logger.info(
            "upstream selected: %s (rev=%d priority=%d tier_size=%d excluded=%s)",
            chosen.name, view.revision, best_priority, len(tier), sorted(exclude))
        return chosen

    # ---- 健康、恢复探测登记 ----

    async def report_head(self, node: UpstreamNode, head: int) -> int:
        """登记节点报告的链头（单调），重算共识，返回当前共识链头。"""
        async with self._lock:
            self._record_head_locked(node, head)
            self._recompute_consensus_locked()
            return self._consensus_head

    async def report_success(self, node: UpstreamNode,
                             chain_head: int | None = None) -> int:
        """正常流量成功：恢复健康计数并（如有）学习链头；返回共识链头。"""
        async with self._lock:
            was_unhealthy = not node.healthy
            node.healthy = True
            node.consecutive_failures = 0
            if chain_head is not None:
                self._record_head_locked(node, chain_head)
            self._recompute_consensus_locked()
            if was_unhealthy:
                logger.info("upstream recovered via traffic: %s", node.name)
            return self._consensus_head

    async def report_failure(self, node: UpstreamNode, error: GatewayError) -> None:
        async with self._lock:
            node.consecutive_failures += 1
            failures = node.consecutive_failures
            if failures >= FAILURE_THRESHOLD and node.healthy:
                node.healthy = False
                node.probe_attempts = 0
                node.probe_next_at = time.monotonic()  # 下个探测周期即可试
                logger.warning(
                    "upstream ejected after %d failures: %s kind=%s "
                    "(recovery probe scheduled)", failures, node.name,
                    error.kind.value)
            self._recompute_consensus_locked()
        logger.warning("upstream failure: %s kind=%s consecutive=%d healthy=%s",
                       node.name, error.kind.value, failures, node.healthy)

    async def due_probes(self, now: float | None = None) -> list[UpstreamNode]:
        """返回到达退避时刻、可进行恢复探测的已摘除节点。"""
        now = time.monotonic() if now is None else now
        async with self._lock:
            return [
                n for n in self._nodes.values()
                if not n.healthy and not n.retired and n.probe_next_at <= now
            ]

    async def probe_failed(self, node: UpstreamNode, next_at: float) -> None:
        """探测仍失败：推进退避。节点在恢复前不会收到任何正常流量。"""
        async with self._lock:
            node.probe_attempts += 1
            node.probe_next_at = next_at
            logger.info("recovery probe failed: %s attempts=%d next_after=%.3fs",
                        node.name, node.probe_attempts,
                        max(0.0, next_at - time.monotonic()))

    async def probe_behind(self, node: UpstreamNode, head: int,
                           next_at: float) -> int:
        """探测可达但链头落后：学习其高度但继续摘除，等待追平。返回共识链头。"""
        async with self._lock:
            node.probe_attempts = 0  # 节点本身可达，退避不应继续累积
            node.probe_next_at = next_at
            self._record_head_locked(node, head)
            self._recompute_consensus_locked()
            logger.info("recovery probe reachable but behind: %s head=%d < "
                        "consensus=%d (kept ejected)",
                        node.name, head, self._consensus_head)
            return self._consensus_head

    async def probe_restored(self, node: UpstreamNode, head: int) -> bool:
        """探测成功且链头不落后于共识：正式放回原优先级/权重层。返回是否放量。"""
        async with self._lock:
            self._record_head_locked(node, head)
            self._recompute_consensus_locked()
            if node.chain_head < self._consensus_head:
                return False
            node.healthy = True
            node.consecutive_failures = 0
            node.probe_attempts = 0
            node.probe_next_at = 0.0
            logger.info("upstream restored to routing: %s head=%d consensus=%d "
                        "priority=%d weight=%d",
                        node.name, node.chain_head, self._consensus_head,
                        node.priority, node.weight)
            return True

    def consensus_chain_head(self) -> int:
        return self._consensus_head

    def head_settled(self) -> bool:
        return self._head_settled

    def max_chain_head(self) -> int:
        """兼容旧接口：统一口径下“网关对外链头”即共识链头。"""
        return self._consensus_head

    # ---- 状态可观测 ----

    def status(self) -> dict:
        """返回统一链头口径及其判断依据，便于定位节点分歧。"""
        voters = [
            n for n in self._nodes.values()
            if not n.retired and n.healthy and n.head_reported
        ]
        quorum = len(voters) // 2 + 1 if voters else 0
        basis = sorted((n.chain_head for n in voters), reverse=True)
        return {
            "revision": self._revision,
            "consensus_head": self._consensus_head,
            "head_settled": self._head_settled,
            "voters": len(voters),
            "quorum": quorum,
            "head_basis": basis,
            "nodes": [
                {
                    "name": n.name,
                    "url": n.url,
                    "priority": n.priority,
                    "weight": n.weight,
                    "healthy": n.healthy,
                    "retired": n.retired,
                    "chain_head": n.chain_head,
                    "head_reported": n.head_reported,
                    "aligned": (n.head_reported
                                and n.chain_head >= self._consensus_head),
                    "consecutive_failures": n.consecutive_failures,
                    "probe_attempts": n.probe_attempts,
                }
                for n in self._nodes.values()
            ],
        }

    # ---- 内部 ----

    def _record_head_locked(self, node: UpstreamNode, head: int) -> None:
        if not node.head_reported:
            node.head_reported = True
            node.chain_head = head
            logger.info("node first head report: %s head=%d", node.name, head)
        elif head > node.chain_head:
            node.chain_head = head

    def _recompute_consensus_locked(self) -> None:
        """多数口径：quorum 个健康投票者都达到的最高高度 = 第 quorum 高的上报值。

        口径一旦确立即单调不减（链头不会倒退）：恢复中的落后节点、剩余健康
        节点的瞬时落后都不能把已确认口径往下拽；这也避免旧条目被“口径回退”
        反复误判。投票者本身要求其上报高度不低于现行口径，防止落后者稀释多数。
        """
        prev = self._consensus_head if self._head_settled else 0
        voters = [
            n for n in self._nodes.values()
            if (not n.retired and n.healthy and n.head_reported
                and n.chain_head >= prev)
        ]
        if not voters:
            # 没有不落后于现行口径的投票者：维持原口径（冷启动时仍未确立）
            self._head_settled = self._head_settled
            return
        heads = sorted((n.chain_head for n in voters), reverse=True)
        quorum = len(voters) // 2 + 1
        candidate = heads[quorum - 1]
        self._consensus_head = max(prev, candidate)
        self._head_settled = True
        logger.info("chain head consensus: head=%d quorum=%d/%d basis=%s floor=%d",
                    self._consensus_head, quorum, len(voters), heads, prev)
