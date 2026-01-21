from utils.ChordMath import ChordMath
from .TopologyManager import TopologyManager
from .DataStore import DataStore
from .DataTransferManager import DataTransferManager
from .FingerTable import FingerTable


class ChordNode:
    def __init__(self, ip, port):
        self.id = ChordMath.compute_hash(f"{ip}:{port}")
        self.topology_manager = TopologyManager(self)
        self.data_store = DataStore()
        self.data_transfer_manager = DataTransferManager(self.data_store, self.id)
        self.finger_table = FingerTable(self)

    def join(self, bootstrap_node):
        self.topology_manager.join(bootstrap_node)
        self.data_transfer_manager.acquire_keys_from_successor(
            self.topology_manager.get_successor()
        )
        self.finger_table.initialize()

    def __repr__(self):
        return f"<Node Hash: {self.id})>"