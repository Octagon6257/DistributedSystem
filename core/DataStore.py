from utils.ChordMath import ChordMath

class DataStore:

    def __init__(self):
        self.data = {}
        self.key_hashes = {}

    def store(self, key, value):
        key_hash = ChordMath.compute_hash(key)
        self.data[key] = value
        self.key_hashes[key] = key_hash
        return True

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        self.key_hashes.pop(key, None)
        return self.data.pop(key, None)

    def get_keys_in_range(self, start, end):
        keys = []
        for orig_key, key_hash in self.key_hashes.items():
            if ChordMath.in_interval(start, key_hash, end):
                keys.append(orig_key)
        return keys

    def transfer_keys(self, keys):
        transferred = {}
        for key in keys:
            if key in self.data:
                transferred[key] = self.data.pop(key)
                self.key_hashes.pop(key, None)
        return transferred

    def receive_keys(self, key_value_dict):
        for key, value in key_value_dict.items():
            self.store(key, value)