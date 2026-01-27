import json
import struct
import asyncio
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class ChordMessage:
    type: str
    payload: dict
    sender_ip: str
    sender_port: int


class MessageProtocol:

    def __init__(self) -> None:
        self.encoding: str = 'utf-8'

    def _pack(self, message: ChordMessage) -> bytes:
        msg_dict = asdict(message)
        json_str = json.dumps(msg_dict)
        return json_str.encode(self.encoding)

    def _unpack(self, data: bytes) -> ChordMessage:
        json_str = data.decode(self.encoding)
        msg_dict = json.loads(json_str)
        return ChordMessage(**msg_dict)

    async def send_message(self, writer: asyncio.StreamWriter, message: ChordMessage) -> None:
        body_bytes = self._pack(message)
        length = len(body_bytes)
        header = struct.pack('!I', length)
        writer.write(header + body_bytes)
        await writer.drain()

    async def read_message(self, reader: asyncio.StreamReader) -> Optional[ChordMessage]:
        try:
            header = await reader.readexactly(4)
            length = struct.unpack('!I', header)[0]
            body_bytes = await reader.readexactly(length)
            return self._unpack(body_bytes)
        except asyncio.IncompleteReadError:
            return None
        except Exception as e:
            print(f"Protocol Error: {e}")
            return None