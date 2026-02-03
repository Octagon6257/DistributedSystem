import asyncio
from typing import Optional, List, TYPE_CHECKING
from config.LoggingConfig import get_logger
from utils.ChordMath import ChordMath
from .NodeRef import RemoteNode, NodeRef

if TYPE_CHECKING:
    from .ChordNode import ChordNode

logger = get_logger("TopologyManager")


class TopologyManager:
    def __init__(self, node: 'ChordNode'):
        self.node = node
        self.successor: Optional['RemoteNode'] = None
        self.predecessor: Optional['RemoteNode'] = None
        self.successor = self._create_remote(node.id, node.ip, node.port)

    def _create_remote(self, node_id: int, ip: str, port: int) -> 'RemoteNode':
        return RemoteNode(node_id, ip, port, self.node.ip, self.node.port)

    async def find_successor(self, key_id: int) -> Optional['RemoteNode']:
        if self.successor and self.successor.id == self.node.id:
            return self.successor

        if self.successor and ChordMath.in_interval(self.node.id, key_id, self.successor.id):
            return self.successor
            
        closest = await self.closest_preceding_node(key_id)
        if closest.id == self.node.id:
            return self.successor
        try:
            return await closest.find_successor(key_id)
        except Exception as e:
            logger.error(f"Error in find_successor for {key_id}: {e}")
            return self.successor

    async def closest_preceding_node(self, key_id: int) -> 'RemoteNode':
        closest = self.node.finger_table.closest_preceding_node(key_id)
        if closest.id != self.node.id:
            try:
                if await closest.ping():
                    return closest
            except OSError:
                pass
        if self.successor and self.successor.id != self.node.id:
            if ChordMath.in_interval(self.node.id, self.successor.id, key_id, inclusive=False):
                return self.successor
        return self._create_remote(self.node.id, self.node.ip, self.node.port)

    async def stabilize(self) -> None:
        if not self.successor:
            logger.warning("No successor during stabilize")
            return
        try:
            x = await self.successor.get_predecessor()
            logger.debug(f"stabilize: my successor {self.successor.port} has predecessor {x.port if x else None}")
            if x and x.id != self.node.id:
                should_update = (
                    self.successor.id == self.node.id or
                    ChordMath.in_interval(self.node.id, x.id, self.successor.id, inclusive=False)
                )
                if should_update:
                    logger.info(f"Updating successor: {self.successor.port} -> {x.port}")
                    self.successor = self._create_remote(x.id, x.ip, x.port)
            self_ref = self._create_remote(self.node.id, self.node.ip, self.node.port)
            await self.successor.notify(self_ref)

        except Exception as e:
            logger.error(f"Error during stabilize: {e}")
            await self._handle_successor_failure()

    async def notify(self, node_ref: 'NodeRef') -> None:
        should_update = (
            not self.predecessor or
            self.predecessor.id == self.node.id or
            ChordMath.in_interval(self.predecessor.id, node_ref.id, self.node.id, inclusive=False)
        )
        if should_update and node_ref.id != self.node.id:
            old_pred = self.predecessor.id if self.predecessor else None
            self.predecessor = self._create_remote(node_ref.id, node_ref.ip, node_ref.port)
            logger.info(f"Updating predecessor: {old_pred} -> {node_ref.id}")

    async def check_predecessor(self) -> None:
        if self.predecessor:
            try:
                if not await self.predecessor.ping():
                    logger.warning(f"Predecessor {self.predecessor.id} unreachable")
                    self.predecessor = None
            except Exception as e:
                logger.error(f"Error during predecessor check: {e}")
                self.predecessor = None

    async def _handle_successor_failure(self) -> None:
        logger.warning(f"Successor node failure {self.successor.id}")
        for finger in self.node.finger_table.fingers:
            if finger and finger.id != self.node.id:
                try:
                    if await finger.ping():
                        logger.info(f"New successor found: {finger.id}")
                        self.successor = finger
                        return
                except OSError:
                    continue
        logger.warning("No successor detected, falling back to self")
        self.successor = self._create_remote(self.node.id, self.node.ip, self.node.port)

    async def get_successor(self) -> Optional['RemoteNode']:
        return self.successor

    async def get_predecessor(self) -> Optional['RemoteNode']:
        return self.predecessor

    async def get_successor_list(self, count: int) -> List['RemoteNode']:
        successors = []
        seen_ids = {self.node.id}
        current = self.successor

        while len(successors) < count and current and current.id not in seen_ids:
            if not self.node.running: break

            seen_ids.add(current.id)
            successors.append(current)

            try:
                next_successor = await current.get_successor()
                if next_successor and next_successor.id not in seen_ids:
                    current = self._create_remote(next_successor.id, next_successor.ip, next_successor.port)
                else:
                    break
            except (OSError, asyncio.TimeoutError):
                break
            except Exception as e:
                logger.error(f"Unexpected error in get_successor_list: {e}")
                break

        return successors