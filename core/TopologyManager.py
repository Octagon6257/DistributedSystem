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
        self.successor_list: List['RemoteNode'] = []
        self.successor = self._create_remote(node.id, node.ip, node.port)
        self._successor_recovery_lock = asyncio.Lock()

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
            if x and x.id != self.node.id and x.id != self.successor.id:
                should_update = (
                        self.successor.id == self.node.id or
                        ChordMath.in_interval(self.node.id, x.id, self.successor.id, inclusive=False)
                )
                if should_update:
                    logger.info(f"[Node {self.node.id % 1000}] Updating successor: {self.successor.id % 1000} -> {x.id % 1000}")
                    self.successor = self._create_remote(x.id, x.ip, x.port)
            self_ref = self._create_remote(self.node.id, self.node.ip, self.node.port)
            await self.successor.notify(self_ref)

            await self._update_successor_list()

        except Exception as e:
            logger.error(f"Error during stabilize: {e}")
            await self.handle_successor_failure()

    async def _update_successor_list(self) -> None:
        try:
            max_successors = self.node.replication_factor
            new_list = []
            seen_ids = {self.node.id}
            current = self.successor

            while len(new_list) < max_successors and current and current.id not in seen_ids:
                seen_ids.add(current.id)
                new_list.append(current)
                try:
                    next_suc = await current.get_successor()
                    if next_suc and next_suc.id not in seen_ids:
                        current = self._create_remote(next_suc.id, next_suc.ip, next_suc.port)
                    else:
                        break
                except (OSError, asyncio.TimeoutError):
                    break

            self.successor_list = new_list
            logger.debug(f"Successor list updated: {len(self.successor_list)} nodes")
        except Exception as e:
            logger.error(f"Error updating successor list: {e}")

    async def notify(self, node_ref: 'NodeRef') -> None:
        should_update = (
                not self.predecessor or
                self.predecessor.id == self.node.id or
                ChordMath.in_interval(self.predecessor.id, node_ref.id, self.node.id, inclusive=False)
        )
        if should_update and node_ref.id != self.node.id:
            old_pred = self.predecessor.id if self.predecessor else None
            self.predecessor = self._create_remote(node_ref.id, node_ref.ip, node_ref.port)
            logger.info(f"Updating predecessor: {old_pred % 1000 if old_pred is not None else None} -> {node_ref.id % 1000 if node_ref is not None else None} ")

    async def check_predecessor(self) -> None:
        if self.predecessor:
            try:
                if not await self.predecessor.ping():
                    logger.warning(f"Predecessor {self.predecessor.id % 1000 if self.predecessor.id is not None else None} unreachable")
                    self.predecessor = None
            except Exception as e:
                logger.error(f"Error during predecessor check: {e}")
                self.predecessor = None

    async def handle_successor_failure(self) -> None:
        async with self._successor_recovery_lock:
            if not self.successor:
                return

            old_successor_id = self.successor.id
            logger.warning(f"Handling successor failure for node {old_successor_id % 1000 if old_successor_id is not None else None}")
            for suc in self.successor_list[1:]:
                if suc and suc.id != self.node.id:
                    try:
                        if await suc.ping():
                            logger.info(f"New successor from successor_list: {suc.id % 1000 if suc.id is not None else None}")
                            self.successor = suc
                            self.successor_list = [s for s in self.successor_list if s.id != old_successor_id]
                            return
                    except OSError:
                        continue
            for finger in self.node.finger_table.fingers:
                if finger and finger.id != self.node.id and finger.id != old_successor_id:
                    try:
                        if await finger.ping():
                            logger.info(f"New successor from finger table: {finger.id % 1000 if finger.id is not None else None}")
                            self.successor = finger
                            self.successor_list = []
                            return
                    except OSError:
                        continue
            logger.warning("No successor detected, falling back to self")
            self.successor = self._create_remote(self.node.id, self.node.ip, self.node.port)
            self.successor_list = []

    async def get_successor(self) -> Optional['RemoteNode']:
        return self.successor

    async def get_predecessor(self) -> Optional['RemoteNode']:
        return self.predecessor

    async def get_successor_list(self, count: int) -> List['RemoteNode']:
        if self.successor_list and len(self.successor_list) >= count:
            return self.successor_list[:count]
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