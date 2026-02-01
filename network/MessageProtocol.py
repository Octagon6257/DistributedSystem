import asyncio
import json
import struct
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from config.LoggingConfig import get_logger
from security.Encryption import MessageSecurity

logger = get_logger("MessageProtocol")


@dataclass
class ChordMessage:
    type: str
    payload: Dict[str, Any]
    sender_ip: str
    sender_port: int

    def to_dict(self) -> dict:
        return asdict(self)


class MessageProtocol:

    def __init__(self, encoding: str = 'utf-8', encryption_key: str = None):
        self.encoding = encoding
        self.enable_encryption = encryption_key is not None
        if self.enable_encryption:
            self.security = MessageSecurity(encryption_key)
        else:
            self.security = None

    def _pack(self, message: ChordMessage) -> bytes:
        message_dict = message.to_dict()
        if self.enable_encryption and self.security:
            encrypted_data, signature = self.security.encrypt_message(message_dict)
            secure_wrapper = {'encrypted': True,'data': encrypted_data.hex(),'signature': signature}
            json_str = json.dumps(secure_wrapper)
        else:
            json_str = json.dumps(message_dict)
        return json_str.encode(self.encoding)

    def _unpack(self, data: bytes) -> ChordMessage:
        json_str = data.decode(self.encoding)
        received_dict = json.loads(json_str)
        if self.enable_encryption and self.security:
            if received_dict.get('encrypted'):
                encrypted_data = bytes.fromhex(received_dict['data'])
                signature = received_dict['signature']
                try:
                    message_dict = self.security.decrypt_message(encrypted_data, signature)
                except ValueError as e:
                    logger.error(f"Message not valid: {e}")
                    raise
            else:
                message_dict = received_dict
        else:
            if received_dict.get('encrypted'):
                raise ValueError("Received encrypted message but encryption is disabled")
            message_dict = received_dict
        return ChordMessage(**message_dict)

    async def send_message(self, writer: asyncio.StreamWriter, message: ChordMessage) -> None:
        try:
            body_bytes = self._pack(message)
            length = len(body_bytes)
            header = struct.pack('!I', length)
            writer.write(header + body_bytes)
            await writer.drain()
            logger.debug(f"Message sent {message.type} ({length} bytes)")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    async def read_message(self, reader: asyncio.StreamReader) -> Optional[ChordMessage]:
        try:
            header = await reader.readexactly(4)
            length = struct.unpack('!I', header)[0]
            body_bytes = await reader.readexactly(length)
            message = self._unpack(body_bytes)
            logger.debug(f"Message received {message.type}")
            return message
        except asyncio.IncompleteReadError:
            logger.debug("Connessione chiusa dal peer")
            return None
        except ValueError as e:
            logger.error(f"Connection closed by peer: {e}")
            return None
        except Exception as e:
            logger.error(f"Error reading message: {e}")
            return None