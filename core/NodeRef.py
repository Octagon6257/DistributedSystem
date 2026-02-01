from typing import Optional, List, Dict, Any
from network.RpcClient import RPCClient
from config.LoggingConfig import get_logger

logger = get_logger("NodeRef")


class NodeRef:

    def __init__(self, node_id: int, ip: str, port: int):
        self.id = node_id
        self.ip = ip
        self.port = port

    def as_dict(self) -> Dict[str, Any]:
        return {'id': self.id,'ip': self.ip, 'port': self.port
    }