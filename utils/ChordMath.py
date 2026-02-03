import hashlib
from config.Settings import ChordSettings


class ChordMath:

    @staticmethod
    def in_interval(start: int, key: int, end: int, inclusive: bool = True) -> bool:
        if start == end:
            if inclusive:
                return key == start
            else:
                return False
        if start < end:
            if inclusive:
                return start < key <= end
            else:
                return start < key < end
        else:
            if inclusive:
                return key > start or key <= end
            else:
                return key > start or key < end

    @staticmethod
    def compute_hash(value: str) -> int:
        hash_digest = hashlib.sha256(str(value).encode()).hexdigest()
        return int(hash_digest, 16) % ChordSettings.MODULUS