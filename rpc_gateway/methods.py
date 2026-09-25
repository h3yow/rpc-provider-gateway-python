"""RPC 方法的语义分类：缓存策略、是否幂等、是否依赖链头。

只登记被网关允许的方法；未登记方法在网关层直接拒绝（method_not_allowed），
不会被转发到上游。
"""
from __future__ import annotations

from enum import Enum


class CacheClass(str, Enum):
    NEVER = "never"          # 写方法 / 结果随时变化，绝不缓存
    HEAD_TAGGED = "head"     # 依赖链头：链头推进即失效
    FINALIZED = "finalized"  # 终局数据：一次确定长期有效


class MethodSpec:
    __slots__ = ("cache_class", "idempotent", "head_dependent", "needs_canonical_head")

    def __init__(
        self,
        cache_class: CacheClass,
        *,
        idempotent: bool,
        head_dependent: bool,
        needs_canonical_head: bool = False,
    ) -> None:
        self.cache_class = cache_class
        self.idempotent = idempotent
        self.head_dependent = head_dependent
        # 读取时要求节点链头不落后于网关已知 canonical head，
        # 否则不允许服务头依赖读（避免跨节点结果矛盾）。
        self.needs_canonical_head = needs_canonical_head


def _h(**kw: bool) -> MethodSpec:
    """头依赖读方法的简写：HEAD_TAGGED + 要求节点不落后。"""
    return MethodSpec(
        CacheClass.HEAD_TAGGED,
        idempotent=True,
        head_dependent=True,
        needs_canonical_head=True,
        **kw,
    )


METHOD_REGISTRY: dict[str, MethodSpec] = {
    # —— 终局数据：整条链一致且永不变，一次缓存长期有效 ——
    "eth_chainId": MethodSpec(
        CacheClass.FINALIZED, idempotent=True, head_dependent=False
    ),
    # —— 头依赖读：链头推进即失效，且只允许不落后的节点服务 ——
    "eth_blockNumber": _h(),
    "eth_getBlockByNumber": _h(),
    "eth_getBlockTransactionCountByNumber": _h(),
    "eth_getBalance": _h(),
    "eth_getTransactionCount": _h(),
    "eth_getTransactionReceipt": _h(),
    "eth_call": _h(),
    "eth_gasPrice": _h(),
    # —— 结果随节点/时间变化，不做缓存 ——
    "eth_estimateGas": MethodSpec(
        CacheClass.NEVER, idempotent=True, head_dependent=True,
        needs_canonical_head=True,
    ),
    "eth_syncing": MethodSpec(
        CacheClass.NEVER, idempotent=True, head_dependent=False
    ),
    "web3_clientVersion": MethodSpec(
        CacheClass.NEVER, idempotent=True, head_dependent=False
    ),
    # —— 写方法：不缓存、非幂等；网关派生幂等键后才允许故障转移重试 ——
    "eth_sendRawTransaction": MethodSpec(
        CacheClass.NEVER, idempotent=False, head_dependent=False
    ),
}


def get_method_spec(method: str) -> MethodSpec | None:
    return METHOD_REGISTRY.get(method)
