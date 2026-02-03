import asyncio
import logging
import random
import sys

sys.path.insert(0, "..")

from main import run_node
from churn.ChurnSimulator import ChurnSimulator
from config.Settings import NetworkSettings

NUM_NODES = 5
BASE_PORT = NetworkSettings.STARTING_PORT
HOST = NetworkSettings.IP_ADDRESS
MAX_ACTIONS = 50

logging.getLogger("RPCClient")


class ChurnNetwork:

    def __init__(self):
        self.tasks = {}
        self.next_node_id = 0
        self.available_node_ids = []

    def start_node(self, node_id, bootstrap=False):
        port = BASE_PORT + node_id
        if bootstrap:
            print(f"Bootstrap node (port {port})")
            task = asyncio.create_task(run_node(HOST, port))
        else:
            print(f"JOIN node {node_id} (port {port})")
            task = asyncio.create_task(run_node(HOST, port, HOST, BASE_PORT))
        self.tasks[node_id] = task

    def kill_node(self, node_id):
        if node_id in self.tasks and node_id != 0:
            print(f"LEAVE node {node_id}")
            self.tasks[node_id].cancel()
            del self.tasks[node_id]
            self.available_node_ids.append(node_id)

    async def on_join(self):
        if self.available_node_ids:
            node_id = self.available_node_ids.pop(0)
        else:
            self.next_node_id += 1
            node_id = self.next_node_id
        self.start_node(node_id)
        await asyncio.sleep(1)

    async def on_leave(self):
        killable = [n for n in self.tasks if n != 0]
        if killable:
            node_id = random.choice(killable)
            self.kill_node(node_id)
            await asyncio.sleep(0.5)

    def stop_all(self):
        for task in self.tasks.values():
            task.cancel()


async def run_churn_test():
    print("=" * 70)
    print("CHORD CHURN TEST - 50 ACTIONS")
    print("=" * 70)

    network = ChurnNetwork()

    try:
        print(f"\nPHASE 1: Starting initial network ({NUM_NODES} nodes)")
        network.start_node(0, bootstrap=True)
        await asyncio.sleep(2)

        for i in range(1, NUM_NODES):
            network.start_node(i)
            network.next_node_id = i
            await asyncio.sleep(1)

        await asyncio.sleep(5)

        print(f"\nPHASE 2: Dynamic churn ({MAX_ACTIONS} actions)")
        churn = ChurnSimulator(on_node_join=network.on_join, on_node_leave=network.on_leave)

        await churn.start_random_churn(interval_min=1.0,interval_max=3.0,join_probability=0.5,max_actions=MAX_ACTIONS)

        stats = churn.get_stats()
        print(f"\nChurn stats: {stats['total_joins']} joins, {stats['total_leaves']} leaves")
        print(f"Active nodes: {len(network.tasks)}")
        print("\nCHURN TEST COMPLETED")

    except KeyboardInterrupt:
        pass
    finally:
        network.stop_all()


if __name__ == "__main__":
    try:
        asyncio.run(run_churn_test())
    except KeyboardInterrupt:
        pass