from utils.ChordMath import ChordMath

class TopologyManager:

    def __init__(self, node):
        self.node = node
        self.successor = node
        self.predecessor = None

    def join(self, bootstrap_node):
        if bootstrap_node is self.node:
            self.successor = self.node
            self.predecessor = self.node
        else:
            self.successor = bootstrap_node.topology_manager.find_successor(self.node.id)

    def find_successor(self, node_id):
        current_node = self.node
        count = 0
        while True:
            if ChordMath.in_interval(current_node.id, node_id, current_node.topology_manager.successor.id):
                return current_node.topology_manager.successor
            n0 = current_node.topology_manager.closest_preceding_node(node_id)
            if n0 is current_node:
                return current_node.topology_manager.successor
            current_node = n0
            count += 1
            if count > 100:
                raise Exception("Hop limit exceeded")

    def closest_preceding_node(self, node_id):
        return self.node.finger_table.closest_preceding_node(node_id)

    def notify(self, potential_predecessor):
        if (self.predecessor is None or
                ChordMath.in_interval(self.predecessor.id, potential_predecessor.id, self.node.id)):
            self.predecessor = potential_predecessor

    def stabilize(self):
        x = self.successor.topology_manager.predecessor
        if x and ChordMath.in_interval(self.node.id, x.id, self.successor.id):
            self.successor = x
        self.successor.topology_manager.notify(self.node)

    def get_successor(self):
        return self.successor

    def get_predecessor(self):
        return self.predecessor