"""头感知结果缓存 + 同键单飞（single-flight）。

一致性规则：
* HEAD_TAGGED 条目带 canonical head 标签；canonical head 推进时整组失效；
* FINALIZED 条目不随链头失效（终局数据）；
* 失效先于新 head 下的填充发生（canonical head 提升在缓存失效之后才可见）；
* 同键并发未命中只允许一个请求真正打到上游，其余等待同一结果；
* 等待者拿到的结果与发起者一致，且该逻辑请求在上游只执行一次。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("rpc_gateway.cache")


@dataclass
class CacheEntry:
    __slots__ = ("value", "head", "created_at", "cache_class")
    value: Any
    head: int
    created_at: float
    cache_class: str


class HeadAwareCache:
    def __init__(self) -> None:
        self._entries: dict[str, CacheEntry] = {}
        self._inflight: dict[str, asyncio.Future[Any]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str, canonical_head: int) -> Any | None:
        """命中且未过期返回值；HEAD_TAGGED 条目 head 落后即视为未命中。"""
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                logger.info("cache decision: key=%s MISS (no entry)", key)
                return None
            if entry.cache_class == "head" and entry.head < canonical_head:
                logger.info(
                    "cache decision: key=%s STALE (entry_head=%s < canonical=%s)",
                    key, entry.head, canonical_head,
                )
                # 过期条目立即摘除，避免后续重复判定
                self._entries.pop(key, None)
                return None
            logger.info(
                "cache decision: key=%s HIT (class=%s entry_head=%s canonical=%s)",
                key, entry.cache_class, entry.head, canonical_head,
            )
            return entry.value

    async def put(
        self, key: str, value: Any, canonical_head: int, cache_class: str
    ) -> None:
        async with self._lock:
            self._entries[key] = CacheEntry(
                value=value,
                head=canonical_head,
                created_at=asyncio.get_running_loop().time(),
                cache_class=cache_class,
            )
            logger.info(
                "cache decision: key=%s STORE (class=%s head=%s)",
                key, cache_class, canonical_head,
            )

    async def invalidate_head_tagged(self, new_head: int) -> None:
        """canonical head 推进时调用：丢弃全部 HEAD_TAGGED 条目。

        与 head 提升在同一把协调顺序内执行：先失效，head 才对外可见。
        """
        async with self._lock:
            stale = [
                k for k, e in self._entries.items() if e.cache_class == "head"
            ]
            for k in stale:
                self._entries.pop(k, None)
            logger.info(
                "cache decision: INVALIDATE head-tagged at new_head=%s "
                "(dropped=%d remaining=%d)",
                new_head, len(stale), len(self._entries),
            )

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)

    async def begin_singleflight(
        self, key: str
    ) -> tuple[bool, "asyncio.Future[Any] | None"]:
        """登记同键单飞。

        返回 ``(leader, waiter)``：
        * ``(True, None)``：本次调用负责回源，并应在结束后 :meth:`finish_singleflight`；
        * ``(False, future)``：已有同键回源在途，await 该 future 取同一结果。

        判定与登记在锁内一次完成，并发下有且仅有一个 leader。
        """
        async with self._lock:
            existing = self._inflight.get(key)
            if existing is not None:
                logger.info("cache decision: key=%s JOIN in-flight single-flight", key)
                return False, existing
            future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            self._inflight[key] = future
            logger.info("cache decision: key=%s LEADER of single-flight", key)
            return True, None

    def finish_singleflight(self, key: str, value: Any, error: BaseException | None) -> None:
        """回源结束：把结果/异常广播给等待者并释放槽位。"""
        future = self._inflight.pop(key, None)
        if future is None or future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(value)
        logger.info(
            "cache decision: key=%s RESOLVE single-flight (%s)",
            key, "error" if error is not None else "value",
        )
