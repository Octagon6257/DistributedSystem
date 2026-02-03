import asyncio
from typing import Optional, TYPE_CHECKING
from config.Settings import FailureDetectorSettings
from config.LoggingConfig import get_logger

if TYPE_CHECKING:
    from core.ChordNode import ChordNode

logger = get_logger("FailureDetector")


class FailureDetector:

    def __init__(self, node: 'ChordNode'):
        self.node = node
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self.successor_failures = 0
        self.predecessor_failures = 0

    async def start(self) -> None:
        if not self.running:
            self.running = True
            self._task = asyncio.create_task(self._monitor_loop())
            logger.info("Failure detector started")

    async def stop(self) -> None:
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Failure detector stopped")

    async def _monitor_loop(self) -> None:
        while self.running:
            try:
                await asyncio.sleep(FailureDetectorSettings.PING_INTERVAL)
                await self._check_successor()
                await self._check_predecessor()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring error: {e}")

    async def _check_successor(self) -> None:
        successor = self.node.topology_manager.successor

        if not successor or successor.id == self.node.id:
            self.successor_failures = 0
            return
        try:
            alive = await asyncio.wait_for(successor.ping(), timeout=FailureDetectorSettings.TIMEOUT)
            if alive:
                if self.successor_failures > 0:
                    logger.info(f"Successor {successor.id % 1000 if successor.id is not None else None} back online")
                self.successor_failures = 0
            else:
                self._handle_successor_failure()
        except asyncio.TimeoutError:
            self._handle_successor_failure()
        except Exception as e:
            logger.error(f"Error in successor check: {e}")
            self._handle_successor_failure()

    def _handle_successor_failure(self) -> None:
        self.successor_failures += 1
        successor = self.node.topology_manager.successor
        logger.warning(
            f"Successor {successor.id % 1000 if successor.id is not None else None} not responding "
            f"(Attempt {self.successor_failures}/{FailureDetectorSettings.FAILURE_THRESHOLD})"
        )

        if self.successor_failures >= FailureDetectorSettings.FAILURE_THRESHOLD:
            logger.error(f"Successor {successor.id % 1000 if successor.id is not None else None} declared dead")
            asyncio.create_task(self._trigger_successor_recovery())
            self.successor_failures = 0

    async def _trigger_successor_recovery(self) -> None:
        try:
            await self.node.topology_manager.handle_successor_failure()
            logger.info("Successor recovery completed")
        except Exception as e:
            logger.error(f"Error during successor recovery: {e}")

    async def _check_predecessor(self) -> None:
        predecessor = self.node.topology_manager.predecessor
        if not predecessor:
            self.predecessor_failures = 0
            return
        try:
            alive = await asyncio.wait_for(predecessor.ping(), timeout=FailureDetectorSettings.TIMEOUT)
            if alive:
                if self.predecessor_failures > 0:
                    logger.info(f"Predecessor {predecessor.id % 1000 if predecessor.id is not None else None} back online")
                self.predecessor_failures = 0
            else:
                self._handle_predecessor_failure()
        except asyncio.TimeoutError:
            self._handle_predecessor_failure()
        except Exception as e:
            logger.error(f"Error in predecessor check: {e}")
            self._handle_predecessor_failure()

    def _handle_predecessor_failure(self) -> None:
        self.predecessor_failures += 1
        predecessor = self.node.topology_manager.predecessor
        logger.warning(f"Predecessor {predecessor.id % 1000 if predecessor.id is not None else None} not responding \n" f"(Attempt {self.predecessor_failures}/{FailureDetectorSettings.FAILURE_THRESHOLD})")
        if self.predecessor_failures >= FailureDetectorSettings.FAILURE_THRESHOLD:
            logger.error(f"Predecessor {predecessor.id % 1000 if predecessor.id is not None else None} declared dead")
            self.node.topology_manager.predecessor = None
            self.predecessor_failures = 0