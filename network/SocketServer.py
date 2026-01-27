import asyncio

from core import ChordNode
from network.MessageProtocol import MessageProtocol, ChordMessage


class SocketServer:

    def __init__(self, host: str, port: int, protocol: MessageProtocol):
        self.host = host
        self.port = port
        self._protocol = protocol
        self._node = None
        self._server = None

    def set_node(self, node: ChordNode) -> None:
        self._node = node

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        await self._server.serve_forever()

    async def stop(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_addr = writer.get_extra_info('peername')
        try:
            request = await self._protocol.read_message(reader)

            if request is not None:
                response = await self._dispatch_request(request)
                await self._protocol.send_message(writer, response)
            else:
                print(f"No request received from {peer_addr}.")
        except (OSError, asyncio.IncompleteReadError) as e:
            # Gestisce solo errori di rete o di protocollo previsti
            print(f"Network error handling client {peer_addr}: {e}")
        except Exception as e:
            print(f"Application error handling client {peer_addr}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass


    async def _dispatch_request(self, request: ChordMessage) -> ChordMessage:
        #TODO IF FOR REQUESTS
        pass