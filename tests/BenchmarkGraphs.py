import asyncio
import random
import time
import sys
from dataclasses import dataclass, field
from typing import List, Dict

import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, "..")

from main import run_node
from network.RpcClient import RPCClient
from config.Settings import NetworkSettings

BASE_PORT = NetworkSettings.STARTING_PORT
HOST = NetworkSettings.IP_ADDRESS

SCALABILITY_NODE_COUNTS = [5, 10, 15, 20, 25]
SCALABILITY_NUM_KEYS = 15

CRASH_TOTAL_NODES = 10
CRASH_TEST_KEYS = 20
CRASH_COUNTS_TO_TEST = [0, 1, 2, 3, 4, 5, 6, 7, 8]

CHURN_INITIAL_NODES = 10
CHURN_MAX_ACTIONS = 40
CHURN_JOIN_PROBABILITY = 0.55
CHURN_MIN_NODES = 3


@dataclass
class BenchmarkResults:
    node_counts: List[int] = field(default_factory=list)
    avg_lookup_times_ms: List[float] = field(default_factory=list)
    avg_store_times_ms: List[float] = field(default_factory=list)
    success_rates: List[float] = field(default_factory=list)
    crash_counts: List[int] = field(default_factory=list)
    recovery_rates: List[float] = field(default_factory=list)
    churn_timestamps: List[float] = field(default_factory=list)
    active_nodes_over_time: List[int] = field(default_factory=list)


class TestNetwork:
    def __init__(self):
        self.tasks: Dict[int, asyncio.Task] = {}
        self.rpc = RPCClient()
        self.next_id = 0

    def start_node(self, node_id: int, bootstrap: bool = False):
        port = BASE_PORT + node_id
        if bootstrap:
            task = asyncio.create_task(run_node(HOST, port))
        else:
            task = asyncio.create_task(run_node(HOST, port, HOST, BASE_PORT))
        self.tasks[node_id] = task
        self.next_id = max(self.next_id, node_id + 1)

    def stop_node(self, node_id: int):
        if node_id in self.tasks:
            self.tasks[node_id].cancel()
            del self.tasks[node_id]

    def stop_all(self):
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()

    def get_random_port(self) -> int:
        if self.tasks:
            return BASE_PORT + random.choice(list(self.tasks.keys()))
        return BASE_PORT

    def get_active_port(self) -> int:
        if self.tasks:
            node_id = random.choice(list(self.tasks.keys()))
            return BASE_PORT + node_id
        return BASE_PORT

    @property
    def num_active(self) -> int:
        return len(self.tasks)


async def run_scalability_test(results: BenchmarkResults):
    print("\n" + "=" * 60)
    print("TEST 1: SCALABILITY")
    print("=" * 60)

    rpc = RPCClient()

    for num_nodes in SCALABILITY_NODE_COUNTS:
        network = TestNetwork()

        try:
            print(f"\n[{num_nodes} nodes] Starting network...")

            network.start_node(0, bootstrap=True)
            await asyncio.sleep(2)

            for i in range(1, num_nodes):
                network.start_node(i)
                await asyncio.sleep(0.3)

            stabilization_time = max(8, num_nodes // 2)
            print(f"[{num_nodes} nodes] Stabilization ({stabilization_time}s)...")
            await asyncio.sleep(stabilization_time)

            print(f"[{num_nodes} nodes] STORE test...")
            store_times = []
            for i in range(SCALABILITY_NUM_KEYS):
                key = f"bench_key_{i}"
                value = f"value_{i}"
                port = network.get_random_port()

                start = time.time()
                try:
                    await rpc.send_request(HOST, port, "STORE_KEY", {"key": key, "value": value})
                    store_times.append((time.time() - start) * 1000)
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    pass

            await asyncio.sleep(2)

            print(f"[{num_nodes} nodes] GET test...")
            get_times = []
            successes = 0
            for i in range(SCALABILITY_NUM_KEYS):
                key = f"bench_key_{i}"
                expected = f"value_{i}"
                port = network.get_random_port()

                start = time.time()
                try:
                    result = await rpc.send_request(HOST, port, "GET_KEY", {"key": key})
                    get_times.append((time.time() - start) * 1000)
                    if result and result.get("value") == expected:
                        successes += 1
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    pass

            avg_store = sum(store_times) / len(store_times) if store_times else 0
            avg_get = sum(get_times) / len(get_times) if get_times else 0
            success_rate = (successes / SCALABILITY_NUM_KEYS) * 100

            results.node_counts.append(num_nodes)
            results.avg_store_times_ms.append(avg_store)
            results.avg_lookup_times_ms.append(avg_get)
            results.success_rates.append(success_rate)

            print(f"[{num_nodes} nodes] STORE: {avg_store:.1f}ms | GET: {avg_get:.1f}ms | Success: {success_rate:.0f}%")

        finally:
            network.stop_all()
            await asyncio.sleep(2)


async def run_crash_test(results: BenchmarkResults):
    print("\n" + "=" * 60)
    print("TEST 2: CRASH RECOVERY")
    print("=" * 60)

    rpc = RPCClient()

    for num_crashes in CRASH_COUNTS_TO_TEST:
        network = TestNetwork()

        try:
            print(f"\n[{num_crashes} crashes] Starting network ({CRASH_TOTAL_NODES} nodes)...")

            network.start_node(0, bootstrap=True)
            await asyncio.sleep(2)

            for i in range(1, CRASH_TOTAL_NODES):
                network.start_node(i)
                await asyncio.sleep(0.5)

            print(f"[{num_crashes} crashes] Stabilization...")
            await asyncio.sleep(10)

            test_keys = {}
            for i in range(CRASH_TEST_KEYS):
                key = f"crash_test_{num_crashes}_{i}"
                value = f"data_{i}"
                test_keys[key] = value
                await rpc.send_request(HOST, BASE_PORT, "STORE_KEY", {"key": key, "value": value})

            await asyncio.sleep(3)

            if num_crashes > 0:
                print(f"[{num_crashes} crashes] Simulating crashes...")
                crashable = list(network.tasks.keys())
                to_crash = random.sample(crashable, min(num_crashes, len(crashable)))
                for node_id in to_crash:
                    network.stop_node(node_id)
                    await asyncio.sleep(0.3)

            print(f"[{num_crashes} crashes] Waiting for recovery...")
            await asyncio.sleep(10)

            recovered = 0
            active_port = network.get_active_port()
            for key, expected in test_keys.items():
                try:
                    result = await rpc.send_request(HOST, active_port, "GET_KEY", {"key": key})
                    if result and result.get("value") == expected:
                        recovered += 1
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    pass

            recovery_rate = (recovered / CRASH_TEST_KEYS) * 100
            results.crash_counts.append(num_crashes)
            results.recovery_rates.append(recovery_rate)

            print(f"[{num_crashes} crashes] Recovery: {recovered}/{CRASH_TEST_KEYS} ({recovery_rate:.0f}%)")

        finally:
            network.stop_all()
            await asyncio.sleep(2)


async def run_churn_test(results: BenchmarkResults):
    print("\n" + "=" * 60)
    print("TEST 3: CHURN STABILITY")
    print("=" * 60)

    network = TestNetwork()
    available_ids = []
    start_time = time.time()

    def record_snapshot():
        elapsed = time.time() - start_time
        results.churn_timestamps.append(elapsed)
        results.active_nodes_over_time.append(network.num_active)

    try:
        print(f"\nStarting initial network ({CHURN_INITIAL_NODES} nodes)...")

        network.start_node(0, bootstrap=True)
        await asyncio.sleep(2)
        record_snapshot()

        for i in range(1, CHURN_INITIAL_NODES):
            network.start_node(i)
            await asyncio.sleep(0.8)
            record_snapshot()

        await asyncio.sleep(3)
        record_snapshot()

        print(f"Starting churn ({CHURN_MAX_ACTIONS} actions, {CHURN_JOIN_PROBABILITY*100:.0f}% join)...")

        for action in range(CHURN_MAX_ACTIONS):
            await asyncio.sleep(random.uniform(0.5, 1.2))

            current_nodes = network.num_active

            if current_nodes <= CHURN_MIN_NODES:
                do_join = True
            elif current_nodes >= CHURN_INITIAL_NODES + 5:
                do_join = random.random() < 0.3
            else:
                do_join = random.random() < CHURN_JOIN_PROBABILITY

            if do_join:
                if available_ids:
                    new_id = available_ids.pop(0)
                else:
                    new_id = network.next_id
                network.start_node(new_id)
            else:
                leavable = [n for n in network.tasks.keys() if n != 0]
                if leavable and network.num_active > CHURN_MIN_NODES:
                    leave_id = random.choice(leavable)
                    network.stop_node(leave_id)
                    available_ids.append(leave_id)

            record_snapshot()

            if (action + 1) % 10 == 0:
                print(f"  Action {action + 1}/{CHURN_MAX_ACTIONS} - Active nodes: {network.num_active}")

        print(f"\nChurn completed. Final nodes: {network.num_active}")

    finally:
        network.stop_all()
        await asyncio.sleep(1)


def generate_graphs(results: BenchmarkResults, output_dir: str = "."):
    print("\n" + "=" * 60)
    print("GENERATING GRAPHS")
    print("=" * 60)

    if 'seaborn-v0_8-whitegrid' in plt.style.available:
        plt.style.use('seaborn-v0_8-whitegrid')

    if results.node_counts:
        fig, ax = plt.subplots(figsize=(10, 6))

        ax.plot(results.node_counts, results.avg_lookup_times_ms,
                'o-', color='#2E86AB', linewidth=2, markersize=8, label='GET (Lookup)')
        ax.plot(results.node_counts, results.avg_store_times_ms,
                's-', color='#A23B72', linewidth=2, markersize=8, label='STORE')

        if len(results.node_counts) > 1:
            base = min(results.avg_lookup_times_ms) * 0.8
            theoretical = [base + 3 * np.log2(n) for n in results.node_counts]
            ax.plot(results.node_counts, theoretical,
                    '--', color='gray', linewidth=1.5, alpha=0.7, label='O(log N) theoretical')

        ax.set_xlabel('Number of Nodes', fontsize=12)
        ax.set_ylabel('Average Latency (ms)', fontsize=12)
        ax.set_title('Chord DHT Scalability: Latency vs Network Size', fontsize=14, fontweight='bold')
        ax.legend(loc='upper left', fontsize=10)
        ax.set_ylim(bottom=0)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        path1 = f"{output_dir}/graph_scalability.png"
        plt.savefig(path1, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  {path1}")

    if results.crash_counts:
        fig, ax = plt.subplots(figsize=(10, 6))

        colors = ['#28A745' if r >= 80 else '#FFC107' if r >= 50 else '#DC3545'
                  for r in results.recovery_rates]

        bars = ax.bar(results.crash_counts, results.recovery_rates,
                      color=colors, edgecolor='black', linewidth=1.2)

        for bar, rate in zip(bars, results.recovery_rates):
            ax.annotate(f'{rate:.0f}%',
                        xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=11, fontweight='bold')

        ax.axvline(x=2.5, color='red', linestyle='--', linewidth=2, alpha=0.7)
        ax.annotate('Replication\nFactor (3)', xy=(2.7, 50), fontsize=10, color='red')

        ax.set_xlabel('Number of Crashed Nodes', fontsize=12)
        ax.set_ylabel('Data Recovery Rate (%)', fontsize=12)
        ax.set_title(f'Fault Tolerance: Data Recovery After Crash\n({CRASH_TOTAL_NODES} nodes, {CRASH_TEST_KEYS} keys)',
                     fontsize=14, fontweight='bold')
        ax.set_ylim(0, 110)
        ax.set_xticks(results.crash_counts)
        ax.grid(True, axis='y', alpha=0.3)

        plt.tight_layout()
        path2 = f"{output_dir}/graph_fault_tolerance.png"
        plt.savefig(path2, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  {path2}")

    if results.churn_timestamps:
        fig, ax = plt.subplots(figsize=(12, 5))

        ax.fill_between(results.churn_timestamps, results.active_nodes_over_time,
                        alpha=0.3, color='#2E86AB')
        ax.plot(results.churn_timestamps, results.active_nodes_over_time,
                '-', color='#2E86AB', linewidth=2)

        avg_nodes = float(np.mean(results.active_nodes_over_time))
        ax.axhline(y=avg_nodes, color='orange', linestyle=':', linewidth=2, alpha=0.8)
        ax.annotate(f'Average: {avg_nodes:.1f} nodes',
                    xy=(max(results.churn_timestamps) * 0.7, avg_nodes + 0.5),
                    fontsize=10, color='orange')

        ax.set_xlabel('Time (seconds)', fontsize=12)
        ax.set_ylabel('Active Nodes', fontsize=12)
        ax.set_title(f'Network Stability During Churn ({CHURN_MAX_ACTIONS} join/leave events)',
                     fontsize=14, fontweight='bold')
        ax.set_ylim(bottom=0, top=max(results.active_nodes_over_time) + 3)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        path3 = f"{output_dir}/graph_churn.png"
        plt.savefig(path3, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  {path3}")

    if results.node_counts and results.crash_counts and results.churn_timestamps:
        fig, axes = plt.subplots(1, 3, figsize=(16, 5))

        ax1 = axes[0]
        ax1.plot(results.node_counts, results.avg_lookup_times_ms, 'o-', color='#2E86AB', linewidth=2, markersize=6, label='GET')
        ax1.plot(results.node_counts, results.avg_store_times_ms, 's-', color='#A23B72', linewidth=2, markersize=6, label='STORE')
        ax1.set_xlabel('Nodes')
        ax1.set_ylabel('Latency (ms)')
        ax1.set_title('Scalability', fontweight='bold')
        ax1.legend(fontsize=8)
        ax1.grid(True, alpha=0.3)
        ax1.set_ylim(bottom=0)

        ax2 = axes[1]
        colors = ['#28A745' if r >= 80 else '#FFC107' if r >= 50 else '#DC3545' for r in results.recovery_rates]
        ax2.bar(results.crash_counts, results.recovery_rates, color=colors, edgecolor='black')
        ax2.axvline(x=2.5, color='red', linestyle='--', alpha=0.7)
        ax2.set_xlabel('Crashed Nodes')
        ax2.set_ylabel('Recovery (%)')
        ax2.set_title('Fault Tolerance', fontweight='bold')
        ax2.set_ylim(0, 110)
        ax2.grid(True, axis='y', alpha=0.3)

        ax3 = axes[2]
        ax3.fill_between(results.churn_timestamps, results.active_nodes_over_time, alpha=0.3, color='#2E86AB')
        ax3.plot(results.churn_timestamps, results.active_nodes_over_time, '-', color='#2E86AB', linewidth=1.5)
        ax3.axhline(y=float(np.mean(results.active_nodes_over_time)), color='orange', linestyle=':', linewidth=2)
        ax3.set_xlabel('Time (s)')
        ax3.set_ylabel('Active Nodes')
        ax3.set_title('Churn Stability', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        ax3.set_ylim(bottom=0)

        plt.suptitle('Chord DHT - Benchmark Results', fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        path4 = f"{output_dir}/graphs_combined.png"
        plt.savefig(path4, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  {path4}")


async def run_all_tests():
    print("=" * 60)
    print("CHORD DHT - COMPLETE BENCHMARK")
    print("=" * 60)
    print(f"Host: {HOST}")
    print(f"Base Port: {BASE_PORT}")
    print("=" * 60)

    results = BenchmarkResults()

    await run_scalability_test(results)
    await run_crash_test(results)
    await run_churn_test(results)

    generate_graphs(results, ".")

    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETED!")
    print("=" * 60)
    print("\nGenerated files:")
    print("  - graph_scalability.png")
    print("  - graph_fault_tolerance.png")
    print("  - graph_churn.png")
    print("  - graphs_combined.png")


if __name__ == "__main__":
    try:
        asyncio.run(run_all_tests())
    except KeyboardInterrupt:
        print("\nInterrupted by user")