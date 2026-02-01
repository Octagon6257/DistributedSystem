import asyncio
from typing import Optional, Dict, Any
from .MessageProtocol import MessageProtocol, ChordMessage
from config.LoggingConfig import get_logger
from config.Settings import NetworkSettings, SecuritySettings

logger = get_logger("RPCClient")


class RPCClient:

    def __init__(self, local_ip: str = "0.0.0.0", local_port: int = 0):
        self.local_ip = local_ip
        self.local_port = local_port
        encryption_key = SecuritySettings.SECRET_KEY if SecuritySettings.ENCRYPTION_ENABLED else None
        self.protocol = MessageProtocol(encryption_key=encryption_key)

    async def send_request(self, target_ip: str, target_port: int, method: str, payload: Optional[Dict[str, Any]] = None, timeout: float = NetworkSettings.TIMEOUT) -> Optional[Dict[str, Any]]:
        if payload is None:
            payload = {}
        writer = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(target_ip, target_port), timeout=timeout)
            message = ChordMessage(type=method, payload=payload, sender_ip=self.local_ip, sender_port=self.local_port)
            await self.protocol.send_message(writer, message)
            response = await asyncio.wait_for(self.protocol.read_message(reader), timeout=timeout)
            if response:
                return response.payload
            return None
        except asyncio.TimeoutError:
            logger.warning(f"Connection timeout to {target_ip}:{target_port}")
            return None
        except ConnectionRefusedError:
            logger.warning(f"Refused connection by {target_ip}:{target_port}")
            return None
        except Exception as e:
            logger.error(f"RPC error to {target_ip}:{target_port}: {e}")
            return None
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except (OSError, asyncio.TimeoutError):
                    pass

    async def ping(self, target_ip: str, target_port: int, timeout: float = 1.0) -> bool:
        result = await self.send_request(target_ip, target_port, "PING", {}, timeout=timeout)
        return result is not None