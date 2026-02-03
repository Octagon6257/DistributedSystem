import asyncio
from typing import Optional, Any
from core.NodeRef import NodeRef, RemoteNode
from core.DataStore import DataStore
from core.FingerTable import FingerTable
from core.TopologyManager import TopologyManager
from core.DataTransferManager import DataTransferManager
from utils.ChordMath import ChordMath
from config.LoggingConfig import get_logger
from config.Settings import ChordSettings, NetworkSettings

logger = get_logger("ChordNode")


class ChordNode(NodeRef):

    def __init__(self, ip: str, port: int):
        node_id = ChordMath.compute_hash(f"{ip}:{port}")
        super().__init__(node_id, ip, port)
        self.data_store = DataStore()
        self.finger_table = FingerTable(self)
        self.topology_manager = TopologyManager(self)
        self.data_transfer_manager = DataTransferManager(self.data_store, self.id)
        self.replication_factor = ChordSettings.REPLICATION_FACTOR
        self.running = True
        logger.info(f"Chord Node created: ID={self.id % 1000 if self.id is not None else None}, {ip}:{port}")

    async def stop(self):
        self.running = False

    async def create_ring(self):
        if not self.running: return
        logger.info("Creating new Chord ring (First Node)")

    async def join(self, bootstrap_ip: str, bootstrap_port: int):
        if not self.running: return
        try:
            bootstrap_node = RemoteNode(0, bootstrap_ip, bootstrap_port, self.ip, self.port)
            logger.info(f"Join to ring through bootstrap node {bootstrap_ip}:{bootstrap_port}")
            successor = await bootstrap_node.find_successor(self.id)
            if successor:
                logger.info(f"Successor found: {successor.id % 1000 if successor.id is not None else None}")
                self.topology_manager.successor = RemoteNode(
                    successor.id, successor.ip, successor.port, self.ip, self.port
                )
                await self.data_transfer_manager.acquire_keys_from_successor(self.topology_manager.successor)
                await self.finger_table.initialize()
                logger.info("Join completed successfully")
            else:
                logger.error("Failed to find successor during join")
                raise ConnectionError("Could not find successor")
        except (OSError, asyncio.TimeoutError) as e:
            logger.error(f"Network error during join: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during join: {e}")
            raise

    async def stabilize(self):
        if not self.running: return
        try:
            await self.topology_manager.stabilize()
        except (OSError, asyncio.TimeoutError):
            pass
        except Exception as e:
            logger.error(f"Error during stabilize: {e}")

    async def fix_fingers(self):
        if not self.running: return
        try:
            await self.finger_table.fix_fingers()
        except (OSError, asyncio.TimeoutError):
            pass
        except Exception as e:
            logger.error(f"Error during fix_fingers: {e}")

    async def check_predecessor(self):
        if not self.running: return
        try:
            await self.topology_manager.check_predecessor()
        except (OSError, asyncio.TimeoutError):
            pass
        except Exception as e:
            logger.error(f"Error during check_predecessor: {e}")

    async def store(self, key: str, value: Any) -> bool:
        if not self.running: return False
        try:
            key_hash = ChordMath.compute_hash(key)
            responsible_node = await self.topology_manager.find_successor(key_hash)

            if responsible_node:
                if responsible_node.id == self.id:
                    self.data_store.store(key, value)
                    logger.info(f"Key stored '{key}' locally")
                    await self._replicate_to_successors(key, value)
                    return True
                else:
                    result = await responsible_node.store_key(key, value)
                    if result:
                        logger.info(f"Key stored '{key}' to node {responsible_node.port}")
                    return result
            return False
        except (OSError, asyncio.TimeoutError) as e:
            logger.warning(f"Network error during store of '{key}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during store of '{key}': {e}")
            return False

    async def _replicate_to_successors(self, key: str, value: Any) -> None:
        if not self.running: return
        try:
            successors = await self.topology_manager.get_successor_list(self.replication_factor - 1)
            logger.info(f"Replicating '{key}' to {len(successors)} successors (factor={self.replication_factor})")
            for i, successor in enumerate(successors):
                if not self.running: break
                if successor and successor.id != self.id:
                    try:
                        result = await successor.store_replica(key, value)
                        if result:
                            logger.info(f"Replicated '{key}' to successor {i + 1}: port {successor.port}")
                    except (OSError, asyncio.TimeoutError):
                        logger.debug(f"Failed to replicate '{key}' to successor {successor.port} (unreachable)")
        except Exception as e:
            logger.error(f"Error gathering successors for replication: {e}")

    async def store_replica(self, key: str, value: Any) -> bool:
        if not self.running: return False
        try:
            self.data_store.store(key, value)
            logger.info(f"Replica stored '{key}' locally")
            return True
        except Exception as e:
            logger.error(f"Error storing replica '{key}': {e}")
            return False

    async def get(self, key: str) -> Optional[Any]:
        if not self.running: return None
        try:
            local_value = self.data_store.get(key)
            if local_value is not None:
                return local_value

            key_hash = ChordMath.compute_hash(key)
            try:
                responsible_node = await asyncio.wait_for(self.topology_manager.find_successor(key_hash),timeout=NetworkSettings.TIMEOUT)

                if responsible_node and responsible_node.id == self.id:
                    return None

                if responsible_node:
                    value = await asyncio.wait_for(responsible_node.get_key(key), timeout=NetworkSettings.TIMEOUT)
                    if value is not None:
                        return value
            except (asyncio.TimeoutError, OSError):
                pass
            return await self._get_from_replicas(key)

        except Exception as e:
            logger.error(f"Error during get of '{key}': {e}")
            return None

    async def _get_from_replicas(self, key: str) -> Optional[Any]:
        if not self.running: return None
        try:
            potential_nodes = await self.topology_manager.get_successor_list(self.replication_factor)

            if len(potential_nodes) < self.replication_factor:
                for finger in self.finger_table.fingers:
                    if finger and finger.id != self.id:
                        potential_nodes.append(finger)

            unique_nodes = []
            seen = {self.id}
            for n in potential_nodes:
                if n and n.id not in seen:
                    unique_nodes.append(n)
                    seen.add(n.id)

            for node in unique_nodes:
                if not self.running: return None
                try:
                    value = await asyncio.wait_for(node.get_key(key), timeout=NetworkSettings.TIMEOUT)
                    if value is not None:
                        logger.info(f"Key '{key}' recovered from replica/finger at port {node.port}")
                        return value
                except (OSError, asyncio.TimeoutError):
                    continue
        except Exception as e:
            logger.debug(f"Error in replica lookup logic: {e}")
        return None

    def get_status(self) -> dict:
        successor = self.topology_manager.successor
        pred = self.topology_manager.predecessor
        return {
            'id': self.id,
            'ip': self.ip,
            'port': self.port,
            'successor': successor.id if successor else None,
            'predecessor': pred.id if pred else None,
            'keys_count': len(self.data_store.data),
            'keys': list(self.data_store.data.keys())
        }