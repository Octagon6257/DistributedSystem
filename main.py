import asyncio
import random

from core.ChordNode import ChordNode
from core.NodeRef import RemoteNode
from network.SocketServer import SocketServer
from network.MessageProtocol import MessageProtocol
from config.LoggingConfig import setup_logging
from config.Settings import ChordSettings, SecuritySettings

logger = setup_logging()


async def maintenance_loop(node: ChordNode) -> None:
    while True:
        try:
            await asyncio.sleep(random.uniform(1, ChordSettings.STABILIZE_INTERVAL))
            await node.topology_manager.stabilize()
            await node.finger_table.fix_fingers()
            await node.topology_manager.check_predecessor()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Maintenance loop error: {e}")


async def status_loop(node: ChordNode) -> None:
    while True:
        try:
            await asyncio.sleep(10)
            status = node.get_status()
            logger.info(
                f"STATUS [:{node.port}] ID:{node.id % 1000} | "
                f"Suc:{status['successor'] % 1000} | Pred:{status['predecessor'] % 1000} | Keys:{status['keys_count']}"
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Status loop error: {e}")


async def run_node(host: str, port: int, bootstrap_ip=None, bootstrap_port=None) -> None:
    encryption_key = SecuritySettings.SECRET_KEY if SecuritySettings.ENCRYPTION_ENABLED else None
    protocol = MessageProtocol(encryption_key=encryption_key)
    server = SocketServer(host, port, protocol)
    node = ChordNode(host, port)
    server.set_node(node)

    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)

    if bootstrap_ip and bootstrap_port:
        bootstrap_node = RemoteNode(0, bootstrap_ip, bootstrap_port, host, port)
        await node.join(bootstrap_node)
    else:
        await node.join(None)

    maintenance_task = asyncio.create_task(maintenance_loop(node))
    status_task = asyncio.create_task(status_loop(node))
    logger.info(f"Chord node {node.id} running on {host}:{port}")

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        maintenance_task.cancel()
        status_task.cancel()
        server_task.cancel()
        await server.stop()