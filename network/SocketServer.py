import asyncio
from typing import Optional
from core.ChordNode import ChordNode
from core.NodeRef import RemoteNode
from network.MessageProtocol import MessageProtocol, ChordMessage
from config.LoggingConfig import get_logger

logger = get_logger("SocketServer")


class SocketServer:
    def __init__(self, host: str, port: int, protocol: MessageProtocol):
        self.host = host
        self.port = port
        self._protocol = protocol
        self._node: Optional[ChordNode] = None
        self._server: Optional[asyncio.Server] = None

    def set_node(self, node: ChordNode) -> None:
        self._node = node

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client,self.host,self.port)
        logger.info(f"Chord Server listening at {self.host}:{self.port}")
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Server stopped")

    async def _handle_client(self,reader: asyncio.StreamReader,writer: asyncio.StreamWriter) -> None:
        try:
            request = await self._protocol.read_message(reader)
            if request:
                response_payload = await self._dispatch_request(request)
                response = ChordMessage(type=f"{request.type}_RESPONSE",payload=response_payload,sender_ip=self.host,sender_port=self.port)
                await self._protocol.send_message(writer, response)
        except Exception as e:
            logger.error(f"Error in client management: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except(OSError, asyncio.TimeoutError):
                pass

    async def _dispatch_request(self, request: ChordMessage) -> dict:
        cmd = request.type
        payload = request.payload
        try:
            if cmd == "FIND_SUCCESSOR":
                if 'id' not in payload:
                    return {'error': 'missing_id'}
                result_node = await self._node.topology_manager.find_successor(payload['id'])
                return result_node.as_dict() if result_node else {'id': None}

            elif cmd == "GET_PREDECESSOR":
                pred = await self._node.topology_manager.get_predecessor()
                return pred.as_dict() if pred else {'id': None}

            elif cmd == "GET_SUCCESSOR":
                successor = await self._node.topology_manager.get_successor()
                return successor.as_dict() if successor else {'id': None}

            elif cmd == "CLOSEST_PRECEDING_NODE":
                node = await self._node.topology_manager.closest_preceding_node(payload['id'])
                return node.as_dict() if node else {'id': None}

            elif cmd == "NOTIFY":
                notifier = RemoteNode(payload['id'], payload['ip'], payload['port'], self.host, self.port)
                await self._node.topology_manager.notify(notifier)
                return {'status': 'ok'}

            elif cmd == "STORE_KEY":
                if 'key' not in payload or 'value' not in payload:
                    return {'error': 'missing_key_or_value'}
                result = await self._node.store(payload['key'], payload['value'])
                return {'status': 'ok' if result else 'error'}

            elif cmd == "STORE_REPLICA":
                if 'key' not in payload or 'value' not in payload:
                    return {'error': 'missing_key_or_value'}
                result = await self._node.store_replica(payload['key'], payload['value'])
                return {'status': 'ok' if result else 'error'}

            elif cmd == "GET_KEY":
                if 'key' not in payload:
                    return {'error': 'missing_key'}
                val = await self._node.get(payload['key'])
                return {'value': val}

            elif cmd == "GET_KEYS_IN_RANGE":
                keys = self._node.data_transfer_manager.get_keys_in_range_local(payload['start'], payload['end'])
                return {'keys': keys}

            elif cmd == "TRANSFER_KEYS":
                data = self._node.data_transfer_manager.transfer_keys_local(payload['keys'])
                return {'data': data}

            elif cmd == "RECEIVE_KEYS":
                for key, value in payload['data'].items():
                    await self._node.store(key, value)
                return {'status': 'ok'}

            elif cmd == "PING":
                return {'status': 'alive', 'id': self._node.id}

            elif cmd == "GET_STATUS":
                return self._node.get_status()

            else:
                logger.warning(f"Unknown command: {cmd}")
                return {'error': 'unknown_command'}

        except Exception as e:
            logger.error(f"Error while executing command {cmd}: {e}")
            return {'error': str(e)}