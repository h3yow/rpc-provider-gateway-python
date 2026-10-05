"""HTTP 端点：JSON-RPC 响应格式、错误归一化、调用方标识。"""
from __future__ import annotations

import httpx
import pytest

from gateway.mock_node import MockNode
from gateway.quota import QuotaConfig
from gateway.server import create_app


def _client(harness):
    app = create_app(harness.router)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_rpc_success(harness_factory):
    h = harness_factory([MockNode("a")])
    async with _client(h) as client:
        resp = await client.post("/rpc", json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []})
    body = resp.json()
    assert body["id"] == 1
    assert body["result"] == "0x0"
    assert "error" not in body


async def test_rpc_upstream_error_normalized(harness_factory):
    h = harness_factory([MockNode("a")], max_retries=1)
    h.mock("a").fail_next = 1
    h.mock("a").fail_kind = "timeout"
    async with _client(h) as client:
        resp = await client.post("/rpc", json={
            "jsonrpc": "2.0", "id": 7, "method": "eth_blockNumber", "params": []})
    body = resp.json()
    assert body["error"]["code"] == -32001
    assert body["error"]["data"]["kind"] == "upstream_timeout"
    assert "mock node" not in body["error"]["message"]  # 原始异常不外泄


async def test_rpc_quota_rejection_per_caller(harness_factory):
    h = harness_factory([MockNode("a")],
                        quota_config=QuotaConfig(rate_per_second=0.001, burst=1,
                                                 max_concurrent=10))
    async with _client(h) as client:
        ok = await client.post("/rpc", headers={"x-caller-id": "alice"}, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction",
            "params": ["0x01"]})
        assert "result" in ok.json()
        denied = await client.post("/rpc", headers={"x-caller-id": "alice"}, json={
            "jsonrpc": "2.0", "id": 2, "method": "eth_sendRawTransaction",
            "params": ["0x02"]})
        assert denied.json()["error"]["data"]["kind"] == "rate_limited"
        # 其他调用方不受影响
        other = await client.post("/rpc", headers={"x-caller-id": "bob"}, json={
            "jsonrpc": "2.0", "id": 3, "method": "eth_sendRawTransaction",
            "params": ["0x03"]})
        assert "result" in other.json()


async def test_rpc_invalid_request(harness_factory):
    h = harness_factory([MockNode("a")])
    async with _client(h) as client:
        resp = await client.post("/rpc", json={"jsonrpc": "2.0", "id": 1})
    assert resp.json()["error"]["code"] == -32600


async def test_management_endpoints(harness_factory):
    h = harness_factory([MockNode("a"), MockNode("b")])
    async with _client(h) as client:
        assert (await client.get("/healthz")).json()["status"] == "ok"
        upstreams = (await client.get("/upstreams")).json()
        assert {u["name"] for u in upstreams["nodes"]} == {"a", "b"}
        # 统一链头口径及其判断依据在状态中可见
        assert upstreams["consensus_head"] == 0
        assert "quorum" in upstreams and "head_basis" in upstreams
        stats = (await client.get("/quota/alice")).json()
        assert stats["in_flight"] == 0
