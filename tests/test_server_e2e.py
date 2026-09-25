"""HTTP 端到端：FastAPI + 进程内传输（ASGI），不监听真实端口。"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from rpc_gateway.quota import QuotaLimits
from rpc_gateway.server import create_app


@pytest.fixture
async def client(harness):
    app = create_app(harness.gateway)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://gw") as ac:
        yield ac, harness


async def _rpc(ac, method, params=None, *, caller="demo", req_id=1):
    headers = {"X-Caller-Id": caller}
    return await ac.post(
        "/",
        json={"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or []},
        headers=headers,
    )


@pytest.mark.asyncio
async def test_single_request_over_http(client):
    ac, harness = client
    resp = await _rpc(ac, "eth_chainId")
    assert resp.status_code == 200
    assert resp.json()["result"] == "0x539"


@pytest.mark.asyncio
async def test_batch_request(client):
    ac, _ = client
    resp = await ac.post(
        "/",
        json=[
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId"},
            {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber"},
        ],
    )
    body = resp.json()
    assert isinstance(body, list) and len(body) == 2
    assert body[0]["result"] == "0x539"


@pytest.mark.asyncio
async def test_notification_returns_204(client):
    ac, _ = client
    resp = await ac.post("/", json={"jsonrpc": "2.0", "method": "eth_blockNumber"})
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_unknown_method_rejected(client):
    ac, _ = client
    resp = await _rpc(ac, "eth_dangerousMethod")
    body = resp.json()
    assert body["error"]["data"]["error_type"] == "method_not_allowed"


@pytest.mark.asyncio
async def test_caller_quota_and_hot_update(client):
    ac, harness = client
    # 把 demo 调用方速率调到 1
    r = await ac.post(
        "/admin/quota",
        json={"caller": "demo", "rate_per_second": 1, "max_concurrency": 5},
    )
    assert r.status_code == 200
    ok = await _rpc(ac, "eth_chainId", caller="demo", req_id=1)
    rejected = await _rpc(ac, "eth_chainId", caller="demo", req_id=2)
    assert ok.json().get("result") == "0x539"
    assert rejected.json()["error"]["data"]["error_type"] == "rate_limit_exceeded"
    # 其他调用方不受影响
    other = await _rpc(ac, "eth_chainId", caller="other", req_id=3)
    assert other.json().get("result") == "0x539"


@pytest.mark.asyncio
async def test_admin_nodes_reports_health_and_head(client):
    ac, harness = client
    harness.cluster.mine(2)
    await harness.poll_heads()
    resp = await ac.get("/admin/nodes")
    body = resp.json()
    assert body["_canonical_head"] == 2
    assert body["node-a"]["node_head"] == 2
    assert body["node-a"]["health"] == "closed"


@pytest.mark.asyncio
async def test_disable_node_via_admin(client):
    ac, harness = client
    for node in ("node-a", "node-b", "node-c"):
        await ac.post(f"/admin/nodes/{node}/enable", json={"enabled": False})
    resp = await _rpc(ac, "eth_blockNumber")
    assert resp.json()["error"]["data"]["error_type"] == "no_healthy_node"


@pytest.mark.asyncio
async def test_raw_upstream_exception_never_serialized(client):
    ac, harness = client
    # 触发一次真实故障转移失败路径，响应里只能出现安全字段
    for node in ("node-a", "node-b", "node-c"):
        harness.cluster.set_behavior(node, timeout=True)
    resp = await _rpc(ac, "eth_blockNumber")
    text = resp.text
    body = resp.json()
    assert body["error"]["data"]["error_type"] == "no_healthy_node"
    for leaked in ("Traceback", "NodeFault", "httpx", "asyncio.exceptions"):
        assert leaked not in text
