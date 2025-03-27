import time
import json
import threading
from collections import deque
import os
import subprocess

from node_manager import NodeManager
from hpc_scheduler import HPCScheduler
from job_monitor import JobMonitor

import logging

import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set logging level
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../tests/scheduler.log"),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)

logger = logging.getLogger(__name__)

logger.info("Starting the Elastic Resource Manager")

def start_process_manager(name, hostfile, timeout=1):
    pid = -1
    if name == "PRRTE":
        filename = "pid"
        # Start PRRTE
        os.system("prte --report-pid "+filename+" --report-uri /home/rbhattara/europar25/tests/dvm.uri --daemonize --hostfile "+hostfile)
        start = time.time()
        # get the pid of the PRRTE Master
        while pid < 0:
            if time.time() - start > timeout:
                raise Exception("PRRTE startup timed out!") 
            
            try:
                pid = int(open(filename, 'r').readlines()[0])
            except FileNotFoundError:
                time.sleep(0.1)
        try: 
            if os.path.exists(filename):
                os.remove(filename)
        except Exception:
            pass

    else:
        raise Exception("Process Manager not supported!")

    logger.info("PRRTE DVM Started")
    return pid

# Stop the Process Manager
def stop_process_manager(name, pid):
    if name == "PRRTE":
        os.system("pterm --pid "+str(pid))
    logger.info("PRRTE DVM Terminated")

if __name__ == "__main__":
    parser   = argparse.ArgumentParser()
    parser.add_argument("-H", "--hostfile", default='hostfile', help="File path to the hostfile")
    parser.add_argument("-j", "--jobfile", default='jobs.json', help="File path to the job file")
    parser.add_argument("-N", "--nnodes", default='10', help="Number of nodes")
    parser.add_argument("-p", "--policyfile", default='policy.json', help="File path to the policy file")
    parser.add_argument("-s", "--schedule_type", default=0, help="Schedule Type")
    args   = parser.parse_args()

    hostfile_path = args.hostfile
    job_file_path = args.jobfile
    policy_file_path = args.policyfile
    total_nodes = int(args.nnodes)
    s_type = int(args.schedule_type)

    pid=start_process_manager("PRRTE", hostfile_path)

    node_manager = NodeManager(total_nodes=total_nodes, hostfile_path=hostfile_path)
    scheduler = HPCScheduler(node_manager, job_file_path, policy_file_path, s_type)
    job_monitor = JobMonitor(scheduler, job_file_path)

    # Start the scheduler and job monitoring in separate threads
    scheduler.start()
    job_monitor.start_monitoring()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_process_manager("PRRTE", pid) 
        print("Scheduler stopped.")
