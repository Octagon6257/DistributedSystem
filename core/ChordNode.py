from typing import Optional, Any
from utils.ChordMath import ChordMath
from .TopologyManager import TopologyManager
from .DataStore import DataStore
from .DataTransferManager import DataTransferManager
from .FingerTable import FingerTable
from .NodeRef import NodeRef, RemoteNode
from config.LoggingConfig import get_logger

logger = get_logger("ChordNode")


class ChordNode(NodeRef):

    def __init__(self, ip: str, port: int):
        node_id = ChordMath.compute_hash(f"{ip}:{port}")
        super().__init__(node_id, ip, port)
        self.topology_manager = TopologyManager(self)
        self.data_store = DataStore()
        self.data_transfer_manager = DataTransferManager(self.data_store, self.id)
        self.finger_table = FingerTable(self)
        self.joined = False

        logger.info(f"Chord Node created: ID={self.id}, {ip}:{port}")

    async def join(self, bootstrap_node: Optional[RemoteNode]) -> None:
        if bootstrap_node:
            logger.info(f"Join to ring through bootstrap node {bootstrap_node.ip}:{bootstrap_node.port}")
            try:
                successor = await bootstrap_node.find_successor(self.id)
                if successor:
                    logger.info(f"Successor found: {successor.id}")
                    self.topology_manager.successor = successor
                    await self.data_transfer_manager.acquire_keys_from_successor(successor)
                    await self.finger_table.initialize()
                    self.joined = True
                    logger.info("Join completed successfully")
                else:
                    logger.error("Unable to find successor")
            except Exception as e:
                logger.error(f"Error during join: {e}")
        else:
            logger.info("Creating new Chord ring (First Node)")
            self.joined = True

    async def leave(self) -> None:
        logger.info(f"Node {self.id} is leaving the ring")
        try:
            if self.topology_manager.successor and self.topology_manager.successor.id != self.id:
                all_data = self.data_store.get_all_data()
                if all_data:
                    await self.topology_manager.successor.receive_keys(all_data)
                    logger.info(f"Transferred {len(all_data)} keys to successor")
            self.joined = False
            logger.info("Leave completed successfully")
        except Exception as e:
            logger.error(f"Error during leave: {e}")

    async def store(self, key: str, value: Any) -> bool:
        try:
            key_hash = ChordMath.compute_hash(key)
            responsible_node = await self.topology_manager.find_successor(key_hash)
            if responsible_node:
                if responsible_node.id == self.id:
                    self.data_store.store(key, value)
                    logger.info(f"Key memorized '{key}' locally")
                    return True
                else:
                    result = await responsible_node.store_key(key, value)
                    if result:
                        logger.info(f"Key memorized '{key}' to node {responsible_node.id}")
                    return result
            return False
        except Exception as e:
            logger.error(f"Error during store of '{key}': {e}")
            return False

    async def get(self, key: str) -> Optional[Any]:
        try:
            key_hash = ChordMath.compute_hash(key)
            responsible_node = await self.topology_manager.find_successor(key_hash)
            if responsible_node:
                if responsible_node.id == self.id:
                    value = self.data_store.get(key)
                    logger.debug(f"Key found '{key}' locally")
                    return value
                else:
                    value = await responsible_node.get_key(key)
                    if value is not None:
                        logger.debug(f"Key found '{key}' into node {responsible_node.id}")
                    return value
            return None
        except Exception as e:
            logger.error(f"Error during get of '{key}': {e}")
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
            'joined': self.joined
        }
