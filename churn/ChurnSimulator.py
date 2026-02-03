import asyncio
import random
from typing import Callable, Optional
from config.LoggingConfig import get_logger

logger = get_logger("ChurnSimulator")


class ChurnSimulator:

    def __init__(self, on_node_join: Optional[Callable] = None, on_node_leave: Optional[Callable] = None):
        self.on_node_join = on_node_join
        self.on_node_leave = on_node_leave
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self.total_joins = 0
        self.total_leaves = 0

    async def start_random_churn(self, interval_min: float = 5.0, interval_max: float = 15.0, join_probability: float = 0.5, max_actions: int = 50) -> None:
        self.running = True
        action_count = 0
        logger.info("Starting casual churn simulation")
        while self.running:
            if action_count >= max_actions:
                logger.info(f"Reached max_actions limit: {max_actions}")
                break
            try:
                interval = random.uniform(interval_min, interval_max)
                await asyncio.sleep(interval)
                if random.random() < join_probability:
                    action = 'join'
                    if self.on_node_join:
                        await self.on_node_join()
                        self.total_joins += 1
                else:
                    action = 'leave'
                    if self.on_node_leave:
                        await self.on_node_leave()
                        self.total_leaves += 1
                action_count += 1
                logger.info(
                    f"Churn event: {action} "
                    f"(total: {self.total_joins} joins, {self.total_leaves} leaves)"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in churn simulation: {e}")

    async def start_periodic_churn(self, join_interval: float = 10.0, leave_interval: float = 15.0) -> None:
        self.running = True
        logger.info("Starting periodic churn simulation")
        join_task = asyncio.create_task(self._periodic_joins(join_interval))
        leave_task = asyncio.create_task(self._periodic_leaves(leave_interval))
        try:
            await asyncio.gather(join_task, leave_task)
        except asyncio.CancelledError:
            join_task.cancel()
            leave_task.cancel()

    async def _periodic_joins(self, interval: float) -> None:
        while self.running:
            try:
                await asyncio.sleep(interval)
                if self.on_node_join:
                    await self.on_node_join()
                    self.total_joins += 1
                    logger.info(f"Churn: JOIN (total joins: {self.total_joins})")
            except asyncio.CancelledError:
                break

    async def _periodic_leaves(self, interval: float) -> None:
        while self.running:
            try:
                await asyncio.sleep(interval)
                if self.on_node_leave:
                    await self.on_node_leave()
                    self.total_leaves += 1
                    logger.info(f"Churn: LEAVE (total leaves: {self.total_leaves})")
            except asyncio.CancelledError:
                break

    async def stop(self) -> None:
        logger.info("Stopping churn simulation")
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def get_stats(self) -> dict:
        return {
            'total_joins': self.total_joins,
            'total_leaves': self.total_leaves,
            'net_change': self.total_joins - self.total_leaves
        }