"""按请求语义区分的缓存，链头变化后未确定结果必须失效。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger("gateway.cache")


class CachePolicy(str, Enum):
    CACHEABLE = "cacheable"          # 结果已确定，可长期缓存（如按哈希/固定高度查询）
    HEAD_BOUND = "head_bound"        # 绑定链头，链头变化即失效（如 latest 余额、链头高度）
    NON_CACHEABLE = "non_cacheable"  # 不可缓存（如发交易、订阅）


@dataclass(frozen=True)
class CacheKey:
    method: str
    params_fingerprint: str


@dataclass
class CacheEntry:
    value: Any
    chain_head: int          # 写入时网关已知的链头高度
    policy: CachePolicy      # 仅 HEAD_BOUND 条目参与链头失效


# 结果永久确定的方法（按不可变标识查询）
_CACHEABLE_METHODS = {
    "eth_getBlockByHash",
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
}

# 绑定链头的方法：参数中含 latest/pending 或缺省高度
_HEAD_BOUND_METHODS = {
    "eth_getBlockByNumber",
    "eth_getBalance",
    "eth_getTransactionCount",
    "eth_call",
    "eth_blockNumber",
    "eth_gasPrice",
}


def classify(method: str, params: list | None) -> CachePolicy:
    """按 JSON-RPC 方法语义判定缓存策略。"""
    if method in _CACHEABLE_METHODS:
        return CachePolicy.CACHEABLE
    if method in _HEAD_BOUND_METHODS:
        # 显式指定了具体高度（非 latest/pending）的查询结果已确定，可长期缓存
        if method in {"eth_getBlockByNumber", "eth_getBalance", "eth_call"} and params:
            tag = params[0] if method == "eth_getBlockByNumber" else (
                params[1] if len(params) > 1 else None
            )
            if isinstance(tag, str) and tag.startswith("0x"):
                return CachePolicy.CACHEABLE
        return CachePolicy.HEAD_BOUND
    return CachePolicy.NON_CACHEABLE


def make_key(method: str, params: list | None) -> CacheKey:
    canonical = json.dumps(params or [], sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
    return CacheKey(method=method, params_fingerprint=fingerprint)


class ResultCache:
    """并发安全的缓存；仅 HEAD_BOUND 条目随链头前进而失效。"""

    def __init__(self) -> None:
        self._entries: dict[CacheKey, CacheEntry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: CacheKey, current_head: int) -> tuple[bool, Any]:
        """命中返回 (True, value)；未命中或已失效返回 (False, None)。

        HEAD_BOUND 语义：条目写入时的链头若落后于网关已知链头，视为过期，
        读取时即剔除，保证不会返回过期数据。CACHEABLE 条目不受链头影响。
        """
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                logger.info("cache miss: %s", key.method)
                return False, None
            if (entry.policy == CachePolicy.HEAD_BOUND
                    and entry.chain_head < current_head):
                del self._entries[key]
                logger.info("cache invalidated on read: %s (entry_head=%d < current_head=%d)",
                            key.method, entry.chain_head, current_head)
                return False, None
            logger.info("cache hit: %s policy=%s (head=%d)",
                        key.method, entry.policy.value, entry.chain_head)
            return True, entry.value

    async def put(self, key: CacheKey, value: Any, chain_head: int,
                  policy: CachePolicy) -> None:
        async with self._lock:
            existing = self._entries.get(key)
            # 不用更旧链头的结果覆盖更新的条目，避免并发读写下结果回退
            if (existing is not None and existing.policy == CachePolicy.HEAD_BOUND
                    and existing.chain_head > chain_head):
                logger.info("cache put skipped (stale): %s existing_head=%d new_head=%d",
                            key.method, existing.chain_head, chain_head)
                return
            self._entries[key] = CacheEntry(value=value, chain_head=chain_head,
                                            policy=policy)
            logger.info("cache stored: %s policy=%s (head=%d)",
                        key.method, policy.value, chain_head)

    async def invalidate_below(self, chain_head: int) -> int:
        """使所有 HEAD_BOUND 且 chain_head 低于给定值的条目失效，返回失效条数。"""
        async with self._lock:
            stale = [
                k for k, e in self._entries.items()
                if e.policy == CachePolicy.HEAD_BOUND and e.chain_head < chain_head
            ]
            for k in stale:
                del self._entries[k]
            if stale:
                logger.info("cache invalidated %d head-bound entries below head %d",
                            len(stale), chain_head)
            return len(stale)

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)
