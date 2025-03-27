import logging

logger = logging.getLogger(__name__)

class NodeManager:
    def __init__(self, total_nodes, hostfile_path):
        self.total_nodes = total_nodes
        self.hostfile_path = hostfile_path

    def read_hostfile(self):
        """Read available nodes from the hostfile."""
        with open(self.hostfile_path, 'r') as file:
            return [line.strip() for line in file if line.strip()]

    def write_hostfile(self, nodes):
        """Write updated node list to the hostfile."""
        with open(self.hostfile_path, 'w') as file:
            file.writelines(f"{node}\n" for node in nodes)

    def allocate_nodes(self, num_nodes):
        """Allocate nodes for a job if available."""
        available_nodes = self.read_hostfile()
        if len(available_nodes) >= num_nodes:
            allocated_nodes = available_nodes[:num_nodes]
            remaining_nodes = available_nodes[num_nodes:]
            self.write_hostfile(remaining_nodes)
            logger.info(f"Nodes Allocated: {allocated_nodes}")
            return allocated_nodes
        return None

    def free_nodes(self, allocated_nodes):
        """Free nodes after job completion and add them back to the hostfile."""
        available_nodes = self.read_hostfile()
        available_nodes.extend(allocated_nodes)
        self.write_hostfile(available_nodes)
        logger.info(f"Nodes Freed: {allocated_nodes}")
        print(f"Freed nodes: {allocated_nodes}")