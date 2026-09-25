"""便捷入口：等价于 `python -m rpc_gateway.localstack`。

启动本地多上游 JSON-RPC 网关与 3 个模拟节点。
"""
from rpc_gateway.localstack import main

if __name__ == "__main__":
    main()
