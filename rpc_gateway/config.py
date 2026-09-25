"""静态配置装载与网关组装。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .cache import HeadAwareCache
from .gateway import Gateway
from .head_monitor import HeadMonitor
from .health import HealthMonitor
from .quota import QuotaLimits, QuotaManager
from .router import NodeConfig, NodeRegistry
from .upstream import HttpTransport, UpstreamTransport


@dataclass
class GatewayConfig:
    nodes: list[NodeConfig] = field(default_factory=list)
    default_quota: QuotaLimits = field(default_factory=lambda: QuotaLimits(5, 4))
    caller_quotas: dict[str, QuotaLimits] = field(default_factory=dict)
    request_timeout: float = 2.0
    health_failure_threshold: int = 2
    health_cool_down_seconds: float = 5.0
    head_poll_interval: float = 1.0

    @staticmethod
    def load(path: str | Path) -> "GatewayConfig":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return GatewayConfig.from_dict(data)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "GatewayConfig":
        nodes = [
            NodeConfig(
                name=item["name"],
                url=item["url"],
                priority=int(item.get("priority", 100)),
                weight=int(item.get("weight", 1)),
                enabled=bool(item.get("enabled", True)),
                node_head=int(item.get("initial_head", 0)),
            )
            for item in data.get("nodes", [])
        ]
        q = data.get("default_quota", {})
        default_quota = QuotaLimits(
            rate_per_second=int(q.get("rate_per_second", 5)),
            max_concurrency=int(q.get("max_concurrency", 4)),
        )
        caller_quotas = {
            name: QuotaLimits(
                rate_per_second=int(item.get("rate_per_second", 5)),
                max_concurrency=int(item.get("max_concurrency", 4)),
            )
            for name, item in data.get("caller_quotas", {}).items()
        }
        return GatewayConfig(
            nodes=nodes,
            default_quota=default_quota,
            caller_quotas=caller_quotas,
            request_timeout=float(data.get("request_timeout", 2.0)),
            health_failure_threshold=int(data.get("health_failure_threshold", 2)),
            health_cool_down_seconds=float(data.get("health_cool_down_seconds", 5.0)),
            head_poll_interval=float(data.get("head_poll_interval", 1.0)),
        )


async def build_gateway_async(
    config: GatewayConfig,
    transport: UpstreamTransport | None = None,
) -> tuple[Gateway, HttpTransport | None]:
    """在运行中的事件循环内组装网关。

    返回 ``(gateway, owned_http_transport)``：使用自定义传输（如进程内测试
    传输）时第二个元素为 None；使用默认 HTTP 传输时返回它以便关闭连接池。
    """
    health = HealthMonitor(
        failure_threshold=config.health_failure_threshold,
        cool_down_seconds=config.health_cool_down_seconds,
    )
    registry = NodeRegistry(health)
    for node in config.nodes:
        await registry.register(node)

    cache = HeadAwareCache()
    quota = QuotaManager(config.default_quota)
    for caller, limits in config.caller_quotas.items():
        quota.update_limits(caller, limits)

    owned: HttpTransport | None = None
    http_transport: UpstreamTransport
    if transport is None:
        owned = HttpTransport()
        http_transport = owned
    else:
        http_transport = transport

    head_monitor = HeadMonitor(
        registry, cache, http_transport,
        poll_interval=config.head_poll_interval,
        timeout=config.request_timeout,
    )
    gateway = Gateway(
        registry, health, cache, quota, head_monitor, http_transport,
        request_timeout=config.request_timeout,
    )
    return gateway, owned
