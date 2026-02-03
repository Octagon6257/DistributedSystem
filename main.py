import asyncio
import random

from core.ChordNode import ChordNode
from network.SocketServer import SocketServer
from network.MessageProtocol import MessageProtocol
from config.LoggingConfig import setup_logging
from config.Settings import ChordSettings, SecuritySettings
from fault_tolerance.FailureDetector import FailureDetector

logger = setup_logging()


async def maintenance_loop(node: ChordNode) -> None:
    logger.info("Maintenance loop started")
    try:
        while True:
            if not node.running:
                break

            await asyncio.sleep(random.uniform(1, ChordSettings.STABILIZE_INTERVAL))
            await node.stabilize()

            await asyncio.sleep(ChordSettings.FIX_FINGERS_INTERVAL)
            await node.fix_fingers()

            await asyncio.sleep(ChordSettings.CHECK_PREDECESSOR_INTERVAL)
            await node.check_predecessor()

    except asyncio.CancelledError:
        logger.info("Maintenance loop cancelled")
        return
    except Exception as e:
        if node.running:
            logger.error(f"Maintenance loop error: {e}")


async def status_loop(node: ChordNode) -> None:
    try:
        while True:
            if not node.running: break
            await asyncio.sleep(10)
            status = node.get_status()
            successor = status['successor'] % 1000 if status['successor'] else None
            pred = status['predecessor'] % 1000 if status['predecessor'] else None
            logger.info(
                f"STATUS [:{node.port}] ID:{node.id % 1000} | "
                f"Successor:{successor} | Pred:{pred} | Keys:{status['keys_count']}"
            )
    except asyncio.CancelledError:
        return
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
        await node.join(bootstrap_ip, bootstrap_port)
    else:
        await node.create_ring()

    failure_detector = FailureDetector(node)
    await failure_detector.start()

    maintenance_task = asyncio.create_task(maintenance_loop(node))
    status_task = asyncio.create_task(status_loop(node))

    logger.info(f"Chord node {node.id % 1000 if node.id is not None else None} running on {host}:{port}")

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info(f"Stopping node {port}...")
    finally:
        await node.stop()
        await failure_detector.stop()
        maintenance_task.cancel()
        status_task.cancel()
        await server.stop()
        await asyncio.gather(maintenance_task, status_task, return_exceptions=True)

        if not server_task.done():
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass