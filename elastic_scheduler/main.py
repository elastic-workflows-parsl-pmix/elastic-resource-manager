import time
import os
from datetime import datetime

from elastic_scheduler.nodes.node_manager import NodeManager
from elastic_scheduler.core.hpc_scheduler import HPCScheduler
from elastic_scheduler.jobs.job_monitor import JobMonitor

import logging
import argparse

logs_dir = os.getcwd()  # Use the directory where the program is run
os.makedirs(logs_dir, exist_ok=True)
log_filename = f"scheduler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(logs_dir, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s)",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info("Starting the Elastic Resource Manager")

def main() -> None:
    """Main entry point for the Elastic Resource Manager."""
    parser = argparse.ArgumentParser(description="Elastic Resource Manager")
    parser.add_argument("-H", "--hostfile", default="hostfile", help="File path to the hostfile")
    parser.add_argument("-j", "--jobfile", default="jobs.json", help="File path to the job file")
    parser.add_argument("-N", "--nnodes", type=int, default=10, help="Number of nodes")
    parser.add_argument("-p", "--policyfile", default="policy.json", help="File path to the policy file")
    parser.add_argument("-s", "--schedule_type", type=int, default=0, help="Schedule Type")
    args = parser.parse_args()

    hostfile_path = args.hostfile
    job_file_path = args.jobfile
    policy_file_path = args.policyfile
    total_nodes = args.nnodes
    s_type = args.schedule_type

    test_directory = os.path.dirname(os.path.abspath(job_file_path))
    dvm_file_path = os.path.join(test_directory, "dvm.uri")

    logger.info(f"Hostfile: {hostfile_path}")
    logger.info(f"Job file: {job_file_path}")
    logger.info(f"Policy file: {policy_file_path}")
    logger.info(f"Total nodes: {total_nodes}")
    logger.info(f"Schedule type: {s_type}")

    try:
        node_manager = NodeManager(total_nodes=total_nodes, hostfile_path=hostfile_path, name_pm="PRRTE")
        pid = node_manager.start_process_manager(dvm_file_path)
    except Exception as e:
        logger.error(f"Failed to start process manager: {e}")
        return

    scheduler = HPCScheduler(node_manager, job_file_path, policy_file_path, dvm_file_path, s_type)
    job_monitor = JobMonitor(scheduler, job_file_path)

    # Start the scheduler and job monitoring in separate threads
    scheduler.start()
    job_monitor.start_monitoring()

    logger.info("Scheduler and Job Monitor started.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Stopping process manager.")
        try:
            node_manager.stop_process_manager(pid)
        except Exception as e:
            logger.error(f"Error stopping process manager: {e}")
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
