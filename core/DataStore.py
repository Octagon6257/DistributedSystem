from typing import Dict, List, Any
from utils.ChordMath import ChordMath
from config.LoggingConfig import get_logger

logger = get_logger("DataStore")


class DataStore:

    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.key_hashes: Dict[str, int] = {}

    def store(self, key: str, value: Any) -> bool:
        key_hash = ChordMath.compute_hash(key)
        self.data[key] = value
        self.key_hashes[key] = key_hash
        logger.debug(f"Memorized key '{key}' with hash {key_hash}")
        return True

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def delete(self, key: str) -> Any:
        self.key_hashes.pop(key, None)
        value = self.data.pop(key, None)
        if value is not None:
            logger.debug(f"Removed key '{key}'")
        return value

    def get_keys_in_range(self, start: int, end: int) -> List[str]:
        keys = []
        for orig_key, key_hash in self.key_hashes.items():
            if ChordMath.in_interval(start, key_hash, end):
                keys.append(orig_key)
        return keys

    def transfer_keys(self, keys: List[str]) -> Dict[str, Any]:
        transferred = {}
        for key in keys:
            if key in self.data:
                transferred[key] = self.data.pop(key)
                self.key_hashes.pop(key, None)
        if transferred:
            logger.info(f"Transferred {len(transferred)} keys")
        return transferred

    def receive_keys(self, key_value_dict: Dict[str, Any]) -> None:
        for key, value in key_value_dict.items():
            self.store(key, value)
        if key_value_dict:
            logger.info(f"Received {len(key_value_dict)} keys")

    def get_all_data(self) -> Dict[str, Any]:
        return self.data.copy()

    def clear(self) -> None:
        self.data.clear()
        self.key_hashes.clear()
        logger.info("Storage cleared out")
