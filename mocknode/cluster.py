"""模拟集群共享账本与故障注入控制。

* 所有节点共享同一份链式区块与账号状态，保证"同一链"语义；
* sendRawTransaction 以调用方提供/网关派生的幂等键去重，
  集群只应用一次，重复提交返回同一交易哈希（exactly-once 可验证）；
* 每个节点可独立配置延迟 / 报错 / 链头偏移 / 宕机；
* 故障用 :class:`NodeFault` 表达，由 HTTP 应用（mocknode.app）或
  进程内传输（测试）翻译成各自的失败形态。
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("mocknode.cluster")

GENESIS_ACCOUNT = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


class NodeFault(Exception):
    """模拟节点主动注入的故障（非上游原始异常，仅供测试传输翻译）。"""

    def __init__(self, kind: str, http_status: int | None = None) -> None:
        super().__init__(kind)
        self.kind = kind
        self.http_status = http_status


@dataclass
class NodeBehavior:
    fail: bool = False        # 对所有业务调用返回 JSON-RPC error
    http500: bool = False     # 返回 500（服务端错误）
    timeout: bool = False     # 挂起不响应（配合客户端超时）
    latency: float = 0.0      # 额外延迟（秒）
    head_offset: int = 0      # 该节点视角相对真实链头的偏移（可为负）


def _hex(value: int) -> str:
    return hex(value)


def _keccakish(data: str) -> str:
    """模拟交易/区块哈希：无需真实 keccak，确定性 32 字节摘要即可。"""
    digest = hashlib.sha256(data.encode()).hexdigest()
    return "0x" + digest


@dataclass
class _Block:
    number: int
    hash: str
    parent_hash: str
    timestamp: int
    transactions: list[str] = field(default_factory=list)


class MockCluster:
    def __init__(self, genesis_balance: int = 10**21) -> None:
        self._lock = asyncio.Lock()
        self._behaviors: dict[str, NodeBehavior] = {}
        self._balances: dict[str, int] = {GENESIS_ACCOUNT: genesis_balance}
        self._nonces: dict[str, int] = {}
        self._blocks: list[_Block] = []
        self._receipts: dict[str, dict[str, Any]] = {}
        # 幂等键 -> 交易哈希：保证同一笔交易在集群内只应用一次
        self._idem: dict[str, str] = {}
        self._executions = 0
        # 创世区块
        self._blocks.append(
            _Block(number=0, hash=_keccakish("block-0"), parent_hash="0x" + "00" * 32, timestamp=0)
        )

    # —— 故障注入 ——
    def behavior(self, node: str) -> NodeBehavior:
        b = self._behaviors.get(node)
        if b is None:
            b = NodeBehavior()
            self._behaviors[node] = b
        return b

    def set_behavior(self, node: str, **changes: Any) -> NodeBehavior:
        b = self.behavior(node)
        for key, value in changes.items():
            if not hasattr(b, key):
                raise AttributeError(f"unknown behavior field: {key}")
            setattr(b, key, value)
        logger.info("mocknode: node=%s behavior -> %s", node, changes)
        return b

    # —— 链状态 ——
    def real_head(self) -> int:
        return self._blocks[-1].number

    def view_head(self, node: str) -> int:
        offset = self.behavior(node).head_offset
        return max(0, self.real_head() + offset)

    def mine(self, count: int = 1) -> int:
        """生产区块并返回新链头。"""
        for i in range(count):
            n = self.real_head() + 1
            self._blocks.append(
                _Block(
                    number=n,
                    hash=_keccakish(f"block-{n}"),
                    parent_hash=self._blocks[-1].hash,
                    timestamp=1_700_000_000 + n,
                )
            )
        logger.info("mocknode: mined %d block(s), real_head=%d", count, self.real_head())
        return self.real_head()

    def tx_executions(self) -> int:
        return self._executions

    async def apply_transaction(self, idempotency_key: str, raw_tx: str) -> dict[str, Any]:
        async with self._lock:
            existing = self._idem.get(idempotency_key)
            if existing is not None:
                logger.info(
                    "mocknode: duplicate sendRawTransaction idem=%s -> same txhash, "
                    "executions stays %d",
                    idempotency_key, self._executions,
                )
                return {"tx_hash": existing, "duplicated": True, "executions": self._executions}
            tx_hash = _keccakish(f"tx:{idempotency_key}:{raw_tx}")
            sender = _sender_of(raw_tx)
            self._nonces[sender] = self._nonces.get(sender, 0) + 1
            self._balances.setdefault(sender, 0)
            block_number = self.real_head()
            self._blocks[-1].transactions.append(tx_hash)
            self._receipts[tx_hash] = {
                "transactionHash": tx_hash,
                "blockNumber": _hex(block_number),
                "blockHash": self._blocks[-1].hash,
                "status": "0x1",
            }
            self._idem[idempotency_key] = tx_hash
            self._executions += 1
            logger.info(
                "mocknode: apply tx idem=%s txhash=%s executions=%d",
                idempotency_key, tx_hash, self._executions,
            )
            return {"tx_hash": tx_hash, "duplicated": False, "executions": self._executions}

    # —— 节点视角的统一入口 ——
    async def call(self, node: str, method: str, params: tuple[Any, ...]) -> Any:
        behavior = self.behavior(node)
        if behavior.timeout:
            logger.info("mocknode: node=%s inject TIMEOUT on %s", node, method)
            raise NodeFault("timeout")
        if behavior.fail:
            logger.info("mocknode: node=%s inject RPC_ERROR on %s", node, method)
            raise NodeFault("rpc_error")
        if behavior.http500:
            logger.info("mocknode: node=%s inject HTTP500 on %s", node, method)
            raise NodeFault("http_500", http_status=500)
        if behavior.latency:
            await asyncio.sleep(behavior.latency)
        async with self._lock:
            return self._dispatch(node, method, params)

    def _dispatch(self, node: str, method: str, params: tuple[Any, ...]) -> Any:
        head = self.view_head(node)

        def block_by_number(tag_or_number: Any, full: bool = False) -> dict[str, Any] | None:
            number = _resolve_tag(tag_or_number, head)
            if number is None or number < 0 or number >= len(self._blocks):
                return None
            blk = self._blocks[number]
            return {
                "number": _hex(blk.number),
                "hash": blk.hash,
                "parentHash": blk.parent_hash,
                "timestamp": _hex(blk.timestamp),
                "transactions": list(blk.transactions) if full else list(blk.transactions),
            }

        if method == "eth_chainId":
            return "0x539"  # 1337
        if method == "web3_clientVersion":
            return f"MockNode/{node}/1.0"
        if method == "eth_blockNumber":
            return _hex(head)
        if method == "eth_syncing":
            return False
        if method == "eth_gasPrice":
            return _hex(1_000_000_000)
        if method == "eth_getBlockByNumber":
            return block_by_number(params[0], bool(params[1]) if len(params) > 1 else False)
        if method == "eth_getBlockTransactionCountByNumber":
            blk = block_by_number(params[0])
            return _hex(len(blk["transactions"])) if blk else None
        if method == "eth_getBalance":
            address = _addr(params[0])
            block = _resolve_tag(params[1] if len(params) > 1 else "latest", head)
            if block is None or block < 0 or block > self.real_head():
                raise NodeFault("rpc_error")
            return _hex(self._balances.get(address.lower(), 0))
        if method == "eth_getTransactionCount":
            address = _addr(params[0])
            return _hex(self._nonces.get(address.lower(), 0))
        if method == "eth_getTransactionReceipt":
            return self._receipts.get(params[0])
        if method == "eth_call":
            # 模拟合约只读：返回固定值（真实链会按 to/data 变化）
            return "0x"
        if method == "eth_estimateGas":
            return _hex(21_000)
        if method == "eth_sendRawTransaction":
            raise RuntimeError("sendRawTransaction must go through apply_transaction")
        raise NodeFault("rpc_error")


def _addr(value: Any) -> str:
    return str(value).lower()


def _sender_of(raw_tx: str) -> str:
    # 模拟：用 raw_tx 前 42 字符当发送者，缺省回创世账号
    if isinstance(raw_tx, str) and raw_tx.startswith("0x") and len(raw_tx) >= 42:
        return raw_tx[:42].lower()
    return GENESIS_ACCOUNT


def _resolve_tag(tag_or_number: Any, head: int) -> int | None:
    if isinstance(tag_or_number, int):
        return tag_or_number
    text = str(tag_or_number).lower()
    if text in ("latest", "pending"):
        return head
    if text == "earliest":
        return 0
    if text.startswith("0x"):
        try:
            return int(text, 16)
        except ValueError:
            return None
    try:
        return int(text)
    except ValueError:
        return None
