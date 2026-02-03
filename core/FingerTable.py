from typing import List, Optional, TYPE_CHECKING
from config.Settings import ChordSettings
from utils.ChordMath import ChordMath
from config.LoggingConfig import get_logger
from .NodeRef import RemoteNode

if TYPE_CHECKING:
    from .ChordNode import ChordNode

logger = get_logger("FingerTable")


class FingerTable:

    def __init__(self, node: 'ChordNode'):
        self.node = node
        self.fingers: List[Optional['RemoteNode']] = [None] * ChordSettings.M_BIT
        self.next_finger = 0

    def _create_remote(self, node_id: int, ip: str, port: int) -> 'RemoteNode':
        return RemoteNode(node_id, ip, port, self.node.ip, self.node.port)

    async def initialize(self) -> None:
        logger.info(f"Finger Table initialization for node: {self.node.id % 1000 if self.node.id is not None else None}")
        for i in range(ChordSettings.M_BIT):
            start = (self.node.id + (2 ** i)) % ChordSettings.MODULUS
            finger = await self.node.topology_manager.find_successor(start)
            self.fingers[i] = finger
        logger.info("Finger table initialized")

    async def fix_fingers(self) -> None:
        self.next_finger = (self.next_finger + 1) % ChordSettings.M_BIT
        start = (self.node.id + (2 ** self.next_finger)) % ChordSettings.MODULUS
        try:
            finger = await self.node.topology_manager.find_successor(start)
            self.fingers[self.next_finger] = finger
            logger.debug(f"Updated finger[{self.next_finger}] = {finger.id if finger else None}")
        except Exception as e:
            logger.error(f"Error while updating finger[{self.next_finger}]: {e}")

    def closest_preceding_node(self, key_id: int) -> 'RemoteNode':
        for i in range(ChordSettings.M_BIT - 1, -1, -1):
            finger = self.fingers[i]
            if finger and ChordMath.in_interval(self.node.id, finger.id, key_id, inclusive=False):
                return finger
        return self._create_remote(self.node.id, self.node.ip, self.node.port)

    def get_fingers(self) -> List[Optional[int]]:
        ids = []
        for finger in self.fingers:
            if finger is not None:
                ids.append(finger.id)
            else:
                ids.append(None)
        return ids

    def clear(self) -> None:
        self.fingers = [None] * ChordSettings.M_BIT
        self.next_finger = 0