from utils.ChordMath import ChordMath

class DataStore:

    def __init__(self):
        self.data = {}
        self.node_id = None

    def store(self, key, value):
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        return self.data.pop(key, None)

    def get_keys_in_range(self, start, end):
        keys = []
        for key in self.data.keys():
            key_hash = ChordMath.compute_hash(key)
            if ChordMath.in_interval(start, key_hash, end):
                keys.append(key)
        return keys

    def transfer_keys(self, keys):
        transferred = {}
        for key in keys:
            if key in self.data:
                transferred[key] = self.data.pop(key)
        return transferred

    def receive_keys(self, key_value_dict):
        self.data.update(key_value_dict)
