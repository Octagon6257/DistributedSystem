class DataTransferManager:

    def __init__(self, data_store, node_id):
        self.data_store = data_store
        self.node_id = node_id

    def acquire_keys_from_successor(self, successor_node):
        predecessor = successor_node.topology_manager.get_predecessor()
        start_id = predecessor.id if predecessor else successor_node.id
        end_id = self.node_id
        keys_to_transfer = successor_node.data_transfer_manager.get_keys_in_range(start_id, end_id)
        if keys_to_transfer:
            data = successor_node.data_transfer_manager.transfer_keys(keys_to_transfer)
            self.receive_keys(data)
            return len(data)
        return 0

    def get_keys_in_range(self, start, end):
        return self.data_store.get_keys_in_range(start, end)

    def transfer_keys(self, keys):
        return self.data_store.transfer_keys(keys)

    def receive_keys(self, key_value_dict):
        self.data_store.receive_keys(key_value_dict)