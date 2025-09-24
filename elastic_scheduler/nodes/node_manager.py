
import logging
import os
import time
import subprocess
from typing import List, Optional

logger = logging.getLogger(__name__)


class NodeManager:
    def __init__(self, total_nodes: int, hostfile_path: str, name_pm: str):
        """Initialize NodeManager."""
        self.total_nodes = total_nodes
        self.hostfile_path = hostfile_path
        self.name_pm = name_pm

    def read_hostfile(self) -> List[str]:
        """Read available nodes from the hostfile."""
        try:
            with open(self.hostfile_path, 'r') as file:
                return [line.strip() for line in file if line.strip()]
        except FileNotFoundError:
            logger.warning(f"Hostfile {self.hostfile_path} not found.")
            return []

    def write_hostfile(self, nodes: List[str]) -> None:
        """Write updated node list to the hostfile."""
        with open(self.hostfile_path, 'w') as file:
            for node in nodes:
                file.write(f"{node}\n")

    def allocate_nodes(self, num_nodes: int) -> Optional[List[str]]:
        """Allocate nodes for a job if available."""
        available_nodes = self.read_hostfile()
        if len(available_nodes) >= num_nodes:
            allocated_nodes = available_nodes[:num_nodes]
            remaining_nodes = available_nodes[num_nodes:]
            self.write_hostfile(remaining_nodes)
            logger.info(f"Nodes Allocated: {allocated_nodes}")
            return allocated_nodes
        logger.info(f"Not enough nodes available to allocate {num_nodes} nodes.")
        return None

    def free_nodes(self, allocated_nodes: List[str]) -> None:
        """Free nodes after job completion and add them back to the hostfile."""
        available_nodes = self.read_hostfile()
        available_nodes.extend(allocated_nodes)
        # Remove duplicates and sort for consistency
        unique_nodes = sorted(set(available_nodes))
        self.write_hostfile(unique_nodes)
        logger.info(f"Nodes Freed: {allocated_nodes}")

    def start_process_manager(self, dvm_file_path: str, timeout: int = 1) -> int:
        """Start the process manager (PRRTE) and return its PID."""
        pid = -1
        if self.name_pm == "PRRTE":
            filename = "pid"
            cmd = [
                "prte",
                "--report-pid", filename,
                "--report-uri", dvm_file_path,
                "--daemonize",
                "--hostfile", self.hostfile_path
            ]
            logger.info(f"Starting PRRTE: {' '.join(cmd)}")
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to start PRRTE: {e}")
                raise
            start = time.time()
            while pid < 0:
                if time.time() - start > timeout:
                    logger.error("PRRTE startup timed out!")
                    raise TimeoutError("PRRTE startup timed out!")
                try:
                    with open(filename, 'r') as f:
                        pid = int(f.readline().strip())
                except FileNotFoundError:
                    time.sleep(0.1)
                except ValueError:
                    time.sleep(0.1)
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except Exception as e:
                logger.warning(f"Could not remove pid file: {e}")
        else:
            logger.error(f"Process Manager '{self.name_pm}' not supported!")
            raise NotImplementedError("Process Manager not supported!")

        logger.info("PRRTE DVM Started")
        return pid

    def stop_process_manager(self, pid: int) -> None:
        """Stop the process manager (PRRTE) using its PID."""
        if self.name_pm == "PRRTE":
            cmd = ["pterm", "--pid", str(pid)]
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to stop PRRTE: {e}")
        logger.info("PRRTE DVM Terminated")