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

class RemoteNode(NodeRef):

    def __init__(self, node_id: int, ip: str, port: int, local_ip: str = "0.0.0.0", local_port: int = 0):
            super().__init__(node_id, ip, port)
            self._local_ip = local_ip
            self._local_port = local_port
            self.rpc = RPCClient(local_ip, local_port)

    def _create_remote(self, node_id: int, ip: str, port: int) -> 'RemoteNode':
            return RemoteNode(node_id, ip, port, self._local_ip, self._local_port)

    async def find_successor(self, key_id: int) -> Optional['RemoteNode']:
            result = await self.rpc.send_request(
                self.ip, self.port, "FIND_SUCCESSOR", {'id': key_id}
            )
            if result and result.get('id') is not None:
                return self._create_remote(result['id'], result['ip'], result['port'])
            return None

    async def get_predecessor(self) -> Optional['RemoteNode']:
            result = await self.rpc.send_request(
                self.ip, self.port, "GET_PREDECESSOR"
            )
            if result and result.get('id') is not None:
                return self._create_remote(result['id'], result['ip'], result['port'])
            return None

    async def get_successor(self) -> Optional['RemoteNode']:
            result = await self.rpc.send_request(
                self.ip, self.port, "GET_SUCCESSOR"
            )
            if result and result.get('id') is not None:
                return self._create_remote(result['id'], result['ip'], result['port'])
            return None

    async def notify(self, node_ref: NodeRef) -> bool:
            payload = node_ref.as_dict()
            result = await self.rpc.send_request(
                self.ip, self.port, "NOTIFY", payload
            )
            return result is not None

    async def closest_preceding_node(self, key_id: int) -> Optional['RemoteNode']:
            result = await self.rpc.send_request(
                self.ip, self.port, "CLOSEST_PRECEDING_NODE", {'id': key_id}
            )
            if result and result.get('id') is not None:
                return self._create_remote(result['id'], result['ip'], result['port'])
            return None

    async def get_keys_in_range(self, start: int, end: int) -> List[str]:
            result = await self.rpc.send_request(
                self.ip, self.port, "GET_KEYS_IN_RANGE", {'start': start, 'end': end}
            )
            if result:
                return result.get('keys', [])
            return []

    async def transfer_keys(self, keys: List[str]) -> Dict[str, Any]:
            result = await self.rpc.send_request(
                self.ip, self.port, "TRANSFER_KEYS", {'keys': keys}
            )
            if result:
                return result.get('data', {})
            return {}

    async def receive_keys(self, key_value_dict: Dict[str, Any]) -> bool:
            result = await self.rpc.send_request(
                self.ip, self.port, "RECEIVE_KEYS", {'data': key_value_dict}
            )
            return result is not None

    async def store_key(self, key: str, value: Any) -> bool:
            result = await self.rpc.send_request(
                self.ip, self.port, "STORE_KEY", {'key': key, 'value': value}
            )
            return result is not None and result.get('status') == 'ok'

    async def get_key(self, key: str) -> Optional[Any]:
            result = await self.rpc.send_request(
                self.ip, self.port, "GET_KEY", {'key': key}
            )
            if result:
                return result.get('value')
            return None

    async def ping(self) -> bool:
            return await self.rpc.ping(self.ip, self.port)

    async def store_replica(self, key: str, value: Any) -> bool:
            result = await self.rpc.send_request(
                self.ip, self.port, "STORE_REPLICA", {'key': key, 'value': value}
            )
            return result is not None and result.get('status') == 'ok'