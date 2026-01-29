class NetworkSettings:
    IP_ADDRESS = '127.0.0.1'
    STARTING_PORT = 5000
    TIMEOUT = 2.0
    MAX_RETRIES = 3


class ChordSettings:
    M_BIT = 256
    MODULUS = 2 ** M_BIT
    STABILIZE_INTERVAL = 2
    FIX_FINGERS_INTERVAL = 2
    CHECK_PREDECESSOR_INTERVAL = 2
    REPLICATION_FACTOR = 3


class FailureDetectorSettings:
    PING_INTERVAL = 1
    FAILURE_THRESHOLD = 3
    TIMEOUT = 1.0

class SecuritySettings:
    SECRET_KEY = "chord_dht_secret_key_2026"
    ENCRYPTION_ENABLED = True
    SIGNATURE_ENABLED = True
