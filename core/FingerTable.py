from typing import Any

from config.Settings import ChordSettings
from utils.ChordMath import ChordMath

class FingerTable:

    def __init__(self, node):
        self.node = node
        self.fingers: list[Any] = [None] * ChordSettings.M_BIT
        self.next_finger = -1

    def initialize(self):
        for i in range(ChordSettings.M_BIT):
            start = (self.node.id + (2 ** i)) % ChordSettings.MODULUS
            self.fingers[i] = self.node.topology_manager.find_successor(start)

    def fix_fingers(self):
        self.next_finger = (self.next_finger + 1) % ChordSettings.M_BIT
        start = (self.node.id + (2 ** self.next_finger)) % ChordSettings.MODULUS
        self.fingers[self.next_finger] = self.node.topology_manager.find_successor(start)

    def closest_preceding_node(self, node_id):
        for finger in reversed(self.fingers):
            if finger and ChordMath.in_interval(self.node.id, finger.id, node_id, inclusive=False):
                return finger
        return self.node