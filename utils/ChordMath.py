import hashlib
from config.Settings import ChordSettings

class ChordMath:

    @staticmethod
    def in_interval(start, key, end, inclusive=True):
        if start < end:
            return (start < key <= end) if inclusive else (start < key < end)
        else:
            return (key > start or key <= end) if inclusive else (key > start or key < end)

    @staticmethod
    def compute_hash(value):
        return int(hashlib.sha256(str(value).encode()).hexdigest(), 16) % ChordSettings.MODULUS