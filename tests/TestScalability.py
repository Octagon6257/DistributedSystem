import asyncio
import random
import time
import sys

sys.path.insert(0, "..")

from main import run_node
from network.RpcClient import RPCClient
from config.Settings import NetworkSettings

BASE_PORT = NetworkSettings.STARTING_PORT
HOST = NetworkSettings.IP_ADDRESS
NODE_COUNTS = [10, 20, 30, 40, 50]
NUM_KEYS = 20


class ScalabilityNetwork:

    def __init__(self):
        self.tasks = {}
        self.rpc = RPCClient()
        self.num_nodes = 0

    def start_node(self, node_id, bootstrap=False):
        port = BASE_PORT + node_id
        if bootstrap:
            task = asyncio.create_task(run_node(HOST, port))
        else:
            task = asyncio.create_task(run_node(HOST, port, HOST, BASE_PORT))
        self.tasks[node_id] = task

    def get_random_port(self):
        return BASE_PORT + random.randint(0, self.num_nodes - 1)

    def stop_all(self):
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()


async def run_scalability_test():
    all_results = []

    for num_nodes in NODE_COUNTS:
        network = ScalabilityNetwork()
        network.num_nodes = num_nodes
        rpc = RPCClient()

        try:
            print(f"\nPHASE 1: Starting {num_nodes} nodes")
            start_time = time.time()
            network.start_node(0, bootstrap=True)
            await asyncio.sleep(2)

            for i in range(1, num_nodes):
                network.start_node(i)
                await asyncio.sleep(0.3)

            startup_time = time.time() - start_time
            print(f"\nPHASE 2: Waiting for stabilization")
            stabilization_wait = max(10, num_nodes // 2)
            await asyncio.sleep(stabilization_wait)
            print(f"\nPHASE 3: Storing {NUM_KEYS} keys")
            store_times = []
            store_failures = 0
            for i in range(NUM_KEYS):
                key = f"scalability_key_{i}"
                value = f"value_{i}"
                port = network.get_random_port()
                start = time.time()
                try:
                    await rpc.send_request(HOST, port, "STORE_KEY", {"key": key, "value": value})
                    store_times.append(time.time() - start)
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    store_failures += 1

            avg_store = sum(store_times) / len(store_times) if store_times else 0

            await asyncio.sleep(3)
            print(f"\nPHASE 4: Retrieving {NUM_KEYS} keys")
            get_times = []
            success_count = 0
            not_found_count = 0
            wrong_value_count = 0
            network_failures = 0
            for i in range(NUM_KEYS):
                key = f"scalability_key_{i}"
                expected = f"value_{i}"
                port = network.get_random_port()
                start = time.time()
                try:
                    result = await rpc.send_request(HOST, port, "GET_KEY", {"key": key})
                    get_times.append(time.time() - start)
                    if result and result.get("value") == expected:
                        success_count += 1
                    elif result and result.get("value") is None:
                        not_found_count += 1
                    else:
                        wrong_value_count += 1
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    network_failures += 1

            avg_get = sum(get_times) / len(get_times) if get_times else 0
            success_rate = (success_count / NUM_KEYS) * 100

            all_results.append({
                "nodes": num_nodes,
                "startup_time": startup_time,
                "avg_store_ms": avg_store * 1000,
                "avg_get_ms": avg_get * 1000,
                "success_rate": success_rate,
                "store_failures": store_failures,
                "not_found": not_found_count,
                "wrong_value": wrong_value_count,
                "network_failures": network_failures
            })

        except KeyboardInterrupt:
            break
        finally:
            network.stop_all()
            await asyncio.sleep(2)

    print("\n" + "=" * 70)
    print("CHORD SCALABILITY TEST REPORT")
    print("=" * 70)
    print(f"{'Nodes':<10}{'Startup(s)':<12}{'Store(ms)':<12}{'Get(ms)':<12}{'Success%':<10}")
    print("-" * 56)
    for r in all_results:
        print(f"{r['nodes']:<10}{r['startup_time']:<12.2f}{r['avg_store_ms']:<12.1f}"
              f"{r['avg_get_ms']:<12.1f}{r['success_rate']:<10.1f}")

    print("\n" + "=" * 70)
    print("DETAILED RESULTS")
    print("=" * 70)
    for r in all_results:
        print(f"\n[{r['nodes']} nodes]")
        print(f"   Startup time: {r['startup_time']:.2f}s")
        print(f"   Avg STORE time: {r['avg_store_ms']:.1f}ms")
        print(f"   Avg GET time: {r['avg_get_ms']:.1f}ms")
        print(f"   Success rate: {r['success_rate']:.1f}% ({int(r['success_rate'] * NUM_KEYS / 100)}/{NUM_KEYS})")
        if r['store_failures'] > 0:
            print(f"   Store failures: {r['store_failures']}")
        if r['not_found'] > 0:
            print(f"   Not found: {r['not_found']}")
        if r['wrong_value'] > 0:
            print(f"   Wrong value: {r['wrong_value']}")
        if r['network_failures'] > 0:
            print(f"   Network failures: {r['network_failures']}")


if __name__ == "__main__":
    try:
        asyncio.run(run_scalability_test())
    except KeyboardInterrupt:
        pass