import asyncio
import random
import sys

sys.path.insert(0, "..")

from main import run_node
from network.RpcClient import RPCClient
from config.Settings import NetworkSettings

NUM_NODES = 10
NUM_CRASHES = 4
BASE_PORT = NetworkSettings.STARTING_PORT
HOST = NetworkSettings.IP_ADDRESS


class CrashNetwork:

    def __init__(self):
        self.tasks = {}
        self.rpc = RPCClient()

    def start_node(self, node_id, bootstrap=False):
        port = BASE_PORT + node_id
        if bootstrap:
            print(f"Bootstrap node (port {port})")
            task = asyncio.create_task(run_node(HOST, port))
        else:
            print(f"JOIN node {node_id} (port {port})")
            task = asyncio.create_task(run_node(HOST, port, HOST, BASE_PORT))
        self.tasks[node_id] = task

    def crash_node(self, node_id):
        if node_id in self.tasks and node_id != 0:
            print(f"   CRASH node {node_id} (port {BASE_PORT + node_id})")
            self.tasks[node_id].cancel()
            del self.tasks[node_id]
            return True
        return False

    def get_crashable_nodes(self):
        return [n for n in self.tasks if n != 0]

    def get_active_port(self):
        if self.tasks:
            return BASE_PORT + list(self.tasks.keys())[0]
        return BASE_PORT

    def stop_all(self):
        for task in self.tasks.values():
            task.cancel()


async def run_crash_test():
    print("=" * 70)
    print("CHORD CRASH TEST - NODE FAILURE SIMULATION")
    print("=" * 70)

    network = CrashNetwork()
    rpc = RPCClient()

    try:
        print(f"\nPHASE 1: Starting initial network ({NUM_NODES} nodes)")
        network.start_node(0, bootstrap=True)
        await asyncio.sleep(2)

        for i in range(1, NUM_NODES):
            network.start_node(i)
            await asyncio.sleep(1)

        print("\n   Waiting for ring stabilization (15 seconds)...")
        await asyncio.sleep(15)
        
        print("\n   Checking node status...")
        for node_id in [0, 1, 2]:
            port = BASE_PORT + node_id
            status = await rpc.send_request(HOST, port, "GET_STATUS", {})
            if status:
                print(f"   Node {port}: successor={status.get('successor', 'N/A')}, keys={status.get('keys_count', 0)}")

        print(f"\nPHASE 2: Storing data")
        test_data = {
            "user:alice": "Alice Smith",
            "user:bob": "Bob Jones",
            "user:charlie": "Charlie Brown",
            "config:timeout": "30s",
            "config:retries": "3",
        }

        for key, value in test_data.items():
            await rpc.send_request(HOST, BASE_PORT, "STORE_KEY",{"key": key, "value": value})
            print(f"   Stored: {key}")

        await asyncio.sleep(5)

        print(f"\nPHASE 3: Simulating {NUM_CRASHES} random CRASHES")
        crashable = network.get_crashable_nodes()
        crash_targets = random.sample(crashable, min(NUM_CRASHES, len(crashable)))

        for node_id in crash_targets:
            network.crash_node(node_id)
            await asyncio.sleep(0.5)

        print(f"\nPHASE 4: Recovery")
        print("   Waiting for recovery (15 seconds)...")
        await asyncio.sleep(15)

        print(f"\nPHASE 5: Verifying data retrieval")
        success_count = 0
        lost_count = 0
        
        active_port = network.get_active_port()
        
        for key, expected in test_data.items():
            try:
                response = await rpc.send_request(HOST, active_port, "GET_KEY", {"key": key})
                if response and response.get("value") == expected:
                    print(f"   OK: {key} -> {expected}")
                    success_count += 1
                elif response and response.get("value"):
                    print(f"   WRONG: {key} (expected '{expected}', got '{response.get('value')}')")
                    lost_count += 1
                else:
                    print(f"   LOST: {key}")
                    lost_count += 1
            except Exception as e:
                print(f"   ERROR: {key} ({e})")
                lost_count += 1

        print(f"\nActive nodes: {len(network.tasks)}")
        print(f"Data recovery: {success_count}/{len(test_data)} ({lost_count} lost)")
        print("\nCRASH TEST COMPLETED")

    except KeyboardInterrupt:
        pass
    finally:
        network.stop_all()


if __name__ == "__main__":
    try:
        asyncio.run(run_crash_test())
    except KeyboardInterrupt:
        pass
