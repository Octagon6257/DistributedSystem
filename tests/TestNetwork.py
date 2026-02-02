import asyncio
import sys

sys.path.insert(0, "..")
from main import run_node
from config.Settings import NetworkSettings

NUM_NODES = 5
BASE_PORT = NetworkSettings.STARTING_PORT


async def start_network():
    tasks = [asyncio.create_task(
        run_node(NetworkSettings.IP_ADDRESS, BASE_PORT)
    )]

    await asyncio.sleep(2)

    for i in range(1, NUM_NODES):
        tasks.append(asyncio.create_task(
            run_node(NetworkSettings.IP_ADDRESS, BASE_PORT + i, NetworkSettings.IP_ADDRESS, BASE_PORT)))
        await asyncio.sleep(1)

    print(f"Network started: {NUM_NODES} nodes on ports {BASE_PORT}-{BASE_PORT + NUM_NODES - 1}")
    return tasks


async def main():
    tasks = await start_network()
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass