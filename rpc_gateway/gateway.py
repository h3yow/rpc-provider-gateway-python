"""网关核心编排：配额 -> 缓存（单飞） -> 路由 -> 故障转移 -> 结果归一化。

关键不变量：
* 配额准入是请求级一次性事件：故障转移多次尝试上游不会重复扣减，
  被拒绝的请求根本不会触达路由/上游（不漏扣、不重扣）；
* 可缓存方法的并发未命中由同键单飞合并，上游对该逻辑请求只执行一次；
* 头依赖读只发给 node_head >= canonical_head 的节点，且结果带当前 head 标签；
  canonical head 推进时先失效缓存再提升 head，故无过期读；
* 非幂等写（eth_sendRawTransaction）携带网关派生的幂等键，故障转移重试
  在集群侧 exactly-once；模糊故障（超时/连接错误）若候选耗尽则归一化为
  upstream_unavailable，不向调用方暴露原始异常，也不会静默重放到不同集群。
"""
from __future__ import annotations

import hashlib
import json as _json
import logging
from typing import Any

from .cache import HeadAwareCache
from .errors import (
    GatewayError,
    InvalidRequestError,
    MethodNotAllowedError,
    NoHealthyNodeError,
    UpstreamRPCError,
)
from .head_monitor import HeadMonitor
from .health import HealthMonitor
from .jsonrpc import make_error, make_result
from .methods import CacheClass, get_method_spec
from .quota import QuotaManager
from .router import NodeRegistry
from .upstream import UpstreamTransport

logger = logging.getLogger("rpc_gateway")

# 被视为"节点侧问题、值得换节点重试"的错误由各错误类型的 error_type 标识
RETRIABLE_TYPES = {"upstream_timeout", "upstream_unavailable"}


class Gateway:
    def __init__(
        self,
        registry: NodeRegistry,
        health: HealthMonitor,
        cache: HeadAwareCache,
        quota: QuotaManager,
        head_monitor: HeadMonitor,
        transport: UpstreamTransport,
        *,
        request_timeout: float = 2.0,
    ) -> None:
        self.registry = registry
        self.health = health
        self.cache = cache
        self.quota = quota
        self.head_monitor = head_monitor
        self.transport = transport
        self.request_timeout = request_timeout

    # —— 报文入口 ——
    async def handle_message(
        self, caller: str, payload: Any
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        if isinstance(payload, list):
            if not payload:
                return make_error(
                    None,
                    InvalidRequestError("empty batch").to_json_rpc(),
                )
            responses: list[dict[str, Any]] = []
            for item in payload:
                resp = await self.handle(caller, item)
                if resp is not None:
                    responses.append(resp)
            return responses
        return await self.handle(caller, payload)

    async def handle(self, caller: str, payload: Any) -> dict[str, Any] | None:
        if not isinstance(payload, dict) or "method" not in payload:
            return make_error(
                payload.get("id") if isinstance(payload, dict) else None,
                InvalidRequestError("invalid JSON-RPC request").to_json_rpc(),
            )

        req_id = payload.get("id")
        notification = "id" not in payload
        method = payload.get("method")
        params = _normalize_params(payload.get("params", []))

        if not isinstance(method, str) or params is None:
            resp = make_error(
                req_id, InvalidRequestError("invalid method/params").to_json_rpc()
            )
            return None if notification else resp

        spec = get_method_spec(method)
        if spec is None:
            resp = make_error(
                req_id, MethodNotAllowedError(
                    "method not allowed", data={"method": method}
                ).to_json_rpc()
            )
            return None if notification else resp

        try:
            result = await self._dispatch(caller, method, params)
        except GatewayError as exc:
            logger.info(
                "gateway: caller=%s method=%s FAILURE error_type=%s code=%s",
                caller, method, exc.error_type, exc.code,
            )
            return None if notification else make_error(req_id, exc.to_json_rpc())

        return None if notification else make_result(req_id, result)

    # —— 配额 + 缓存 + 路由的单次逻辑请求 ——
    async def _dispatch(
        self, caller: str, method: str, params: tuple[Any, ...]
    ) -> Any:
        spec = get_method_spec(method)
        assert spec is not None
        head = self.head_monitor.canonical_head

        # 1) 缓存读（含单飞判定）必须在配额准入之后：未准入请求不消耗任何
        #    共享资源，也不允许搭车别人的回源结果。
        admission = await self.quota.acquire(caller)
        try:
            cache_key = _cache_key(method, params)
            if spec.cache_class is not CacheClass.NEVER:
                cached = await self.cache.get(cache_key, head)
                if cached is not None:
                    logger.info(
                        "gateway: caller=%s method=%s served from CACHE key=%s",
                        caller, method, cache_key,
                    )
                    return cached

                leader, waiter = await self.cache.begin_singleflight(cache_key)
                if not leader:
                    logger.info(
                        "gateway: caller=%s method=%s awaiting single-flight key=%s",
                        caller, method, cache_key,
                    )
                    return await waiter  # 同一结果；上游只被 leader 调用一次

                try:
                    result, served_head = await self._route_and_call(method, params)
                    # 回源期间 canonical head 可能已被推进。给结果打标签时取
                    # "响应节点视角链头"与"当前 canonical"的较小者：
                    # 若服务节点其实落后于新 canonical，该条目会被立即判为过期，
                    # 绝不把旧世界结果当作新世界缓存。
                    tag_head = self.head_monitor.canonical_head
                    if served_head is not None and served_head < tag_head:
                        tag_head = served_head
                    await self.cache.put(
                        cache_key, result, tag_head, spec.cache_class.value
                    )
                    self.cache.finish_singleflight(cache_key, result, None)
                    return result
                except BaseException as exc:
                    self.cache.finish_singleflight(cache_key, None, exc)
                    raise

            result, _ = await self._route_and_call(method, params)
            return result
        finally:
            admission.release()

    # —— 路由与故障转移 ——
    async def _route_and_call(
        self, method: str, params: tuple[Any, ...]
    ) -> tuple[Any, int | None]:
        """返回 ``(结果, 实际服务节点视角链头)``。"""
        spec = get_method_spec(method)
        assert spec is not None
        head = self.head_monitor.canonical_head
        candidates = await self.registry.select_candidates(
            require_canonical_head=spec.needs_canonical_head,
            canonical_head=head,
        )
        if not candidates:
            raise NoHealthyNodeError(
                "no eligible upstream node",
                data={
                    "method": method,
                    "head_dependent": spec.needs_canonical_head,
                    "canonical_head": head,
                },
            )

        idem = None
        if not spec.idempotent:
            # 网关派生稳定幂等键：同一逻辑请求在重试间保持一致
            idem = "idem-" + hashlib.sha256(
                _json.dumps([method, list(params)], sort_keys=True).encode()
            ).hexdigest()[:32]

        last_error: GatewayError | None = None
        for index, node in enumerate(candidates):
            logger.info(
                "upstream select: method=%s attempt=%d/%d node=%s priority=%s",
                method, index + 1, len(candidates), node.name, node.priority,
            )
            try:
                result, node_head = await self.transport.call(
                    node.url, method, params,
                    timeout=self.request_timeout, idempotency_key=idem,
                )
            except UpstreamRPCError as exc:
                # 上游明确业务错误：不是节点故障，不换节点重试，直接归一化返回
                logger.info(
                    "failover: method=%s node=%s upstream RPC error (no retry)",
                    method, node.name,
                )
                last_error = exc
                break
            except GatewayError as exc:
                last_error = exc
                logger.info(
                    "failover: method=%s node=%s failed (%s) -> try next",
                    method, node.name, exc.error_type,
                )
                await self.health.record_failure(node.name)
                continue

            await self.health.record_success(node.name)
            if node_head is not None:
                await self.head_monitor.observe_node_head(node.name, node_head)
            logger.info(
                "failover: method=%s served by node=%s", method, node.name
            )
            return result, node_head

        assert last_error is not None
        # 所有候选耗尽：给出可区分的归一化结论
        if isinstance(last_error, UpstreamRPCError):
            raise last_error
        raise NoHealthyNodeError(
            "all upstream attempts failed",
            data={
                "method": method,
                "head_dependent": spec.needs_canonical_head,
                "canonical_head": head,
                "attempted": [n.name for n in candidates],
                "last_error_type": last_error.error_type,
            },
        ) from last_error


def _normalize_params(params: Any) -> tuple[Any, ...] | None:
    if params is None or params == []:
        return ()
    if isinstance(params, list):
        return tuple(params)
    if isinstance(params, dict):
        return tuple(params.values())
    return None


def _cache_key(method: str, params: tuple[Any, ...]) -> str:
    payload = _json.dumps(params, sort_keys=True, separators=(",", ":"), default=str)
    return f"{method}:{payload}"
