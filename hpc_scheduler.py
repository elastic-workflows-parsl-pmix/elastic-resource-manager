import threading
from collections import deque
import json
import subprocess
import time
import os
import logging

logger = logging.getLogger(__name__)

def split_list(lst, n):
    if n > len(lst):
        n = len(lst)  # Avoid index errors
    removed_elements = lst[-n:]  # Get the last N elements
    del lst[-n:]  # Modify original list in-place
    return removed_elements  # Return removed elements

class HPCScheduler:
    def __init__(self, node_manager, job_file, policy_file, schedule_type):
        self.node_manager = node_manager
        self.job_queue = deque()
        self.running_jobs = []
        self.job_file = job_file
        self.policy_file = policy_file
        self.lock = threading.Lock()
        self.schedule_type = schedule_type

    def submit_job(self, job):
        """Submit a job to the scheduler queue."""
        with self.lock:
            self.job_queue.append(job)

    # def expand_elastic_jobs(self, available_nodes, job_id = None):
    #     # Find running elastic jobs that are currently at min_nodes and try to expand
    #     if job_id is None:
    #         elastic_jobs_running_on_min_allocation = [(job, allocated_nodes) for job, allocated_nodes in self.running_jobs if job.type == 'elastic' and len(allocated_nodes) < job.max_nodes]
    #         if not elastic_jobs_running_on_min_allocation:
    #             return  # No elastic jobs to expand
    #     else:
    #         elastic_jobs_running_on_min_allocation = [(job, allocated_nodes) for job, allocated_nodes in self.running_jobs if job.id == job_id]

    #     logger.info(f"[Elastic Scaling] {len(elastic_jobs_running_on_min_allocation)} elastic jobs found. Available nodes: {available_nodes}")
    #     for job, allocated_nodes in elastic_jobs_running_on_min_allocation:
    #         needed_nodes = job.max_nodes - len(allocated_nodes)
    #         if needed_nodes <= 0:
    #             continue  # Job already at max capacity
    #         expandable_nodes = min(needed_nodes, available_nodes)  # Ensure we don't exceed available resources
    #         if expandable_nodes > 0:
    #             new_nodes = self.node_manager.allocate_nodes(expandable_nodes)
    #             if new_nodes:
    #                 allocated_nodes.extend(new_nodes)
    #                 self.running_jobs = [(j, nodes) if j != job else (j, allocated_nodes) for j, nodes in self.running_jobs]
    #                 new_entry = {
    #                         "id": str(job.id),
    #                         "scale": "expand",
    #                         "num_nodes": expandable_nodes,
    #                         "nodes": str(",".join(new_nodes)),
    #                         "start_after": 0
    #                     }
    #                 if os.path.exists(self.policy_file):
    #                     with open(self.policy_file, "r+") as file:
    #                         try:
    #                             policy_data = json.load(file)
    #                             if not any(job_entry["id"] == new_entry["id"] for job_entry in policy_data["jobs"]):
    #                                 policy_data["jobs"].append(new_entry)
    #                                 file.seek(0)
    #                                 file.truncate()
    #                                 json.dump(policy_data, file, indent=4)
    #                                 logger.info(f"[Policy] New job entry added for Job {job.id}.")
    #                                 logger.info(policy_data)
    #                             else:
    #                                 logger.info(f"[Policy] Job {job.id} entry already exists.")
    #                         except json.JSONDecodeError:
    #                             print("[Policy] Invalid JSON format. Resetting policy file.")
    #                             policy_data = {"jobs": [new_entry]}
    #                             with open(self.policy_file, "w") as new_file:
    #                                 json.dump(policy_data, new_file, indent=4)
    #                 available_nodes -= len(new_nodes)
    #                 print(f"[Elastic Scaling] Job {job.id} expanded by {len(new_nodes)} nodes (Now: {len(allocated_nodes)} nodes).")
    #         # If no more nodes available, stop further expansion
    #         if available_nodes <= 0:
    #             break

    def expand_elastic_jobs(self, available_nodes, job_id=None):
        if job_id is None:
            elastic_jobs_running_on_min_allocation = [
                (job, allocated_nodes)
                for job, allocated_nodes in self.running_jobs
                if job.type == "elastic" and len(allocated_nodes) < job.max_nodes
            ]
            if not elastic_jobs_running_on_min_allocation:
                return  # No elastic jobs to expand
        else:
            elastic_jobs_running_on_min_allocation = [
                (job, allocated_nodes)
                for job, allocated_nodes in self.running_jobs
                if job.id == job_id
            ]

        logger.info(
            f"[Elastic Scaling] {len(elastic_jobs_running_on_min_allocation)} elastic jobs found. Available nodes: {available_nodes}"
        )

        for job, allocated_nodes in elastic_jobs_running_on_min_allocation:
            needed_nodes = job.max_nodes - len(allocated_nodes)
            if needed_nodes <= 0:
                continue  # Job already at max capacity
            
            expandable_nodes = min(needed_nodes, available_nodes)  # Ensure we don't exceed available resources
            
            if expandable_nodes > 0:
                new_nodes = self.node_manager.allocate_nodes(expandable_nodes)
                if new_nodes:
                    allocated_nodes.extend(new_nodes)
                    self.running_jobs = [
                        (j, nodes) if j != job else (j, allocated_nodes)
                        for j, nodes in self.running_jobs
                    ]

                    new_entry = {
                        "id": str(job.id),
                        "scale": "expand",
                        "num_nodes": expandable_nodes,
                        "nodes": ",".join(new_nodes),
                        "start_after": 0,
                    }

                    # Ensure the policy file exists or is properly formatted
                    if not os.path.exists(self.policy_file) or os.stat(self.policy_file).st_size == 0:
                        logger.warning("[Policy] Policy file does not exist or is empty. Creating a new policy file.")
                        policy_data = {"jobs": [new_entry]}
                        with open(self.policy_file, "w") as new_file:
                            json.dump(policy_data, new_file, indent=4)
                    else:
                        # Read and update the policy file
                        with open(self.policy_file, "r+") as file:
                            try:
                                policy_data = json.load(file)
                            except json.JSONDecodeError:
                                logger.error("[Policy] Invalid JSON format. Resetting policy file.")
                                policy_data = {"jobs": [new_entry]}
                                with open(self.policy_file, "w") as new_file:
                                    json.dump(policy_data, new_file, indent=4)
                                continue

                            if not any(job_entry["id"] == new_entry["id"] for job_entry in policy_data.get("jobs", [])):
                                policy_data["jobs"].append(new_entry)
                                file.seek(0)
                                file.truncate()  # Clear the file before writing
                                json.dump(policy_data, file, indent=4)
                                logger.info(f"[Policy] New job entry added for Job {job.id}.")
                            else:
                                logger.info(f"[Policy] Job {job.id} entry already exists.")

                    available_nodes -= len(new_nodes)
                    logger.info(
                        f"[Elastic Scaling] Job {job.id} expanded by {len(new_nodes)} nodes (Now: {len(allocated_nodes)} nodes)."
                    )

            # If no more nodes available, stop further expansion
            if available_nodes <= 0:
                break

    def shrink_elastic_jobs(self, required_nodes):
        # Find running elastic jobs that are currently at nodes > min_nodes and try to shrink
        elastic_jobs_running_on_higher_allocation = [(job, allocated_nodes) for job, allocated_nodes in self.running_jobs if job.type == 'elastic' and len(allocated_nodes) > job.min_nodes]
        if not elastic_jobs_running_on_higher_allocation:
            return  # No elastic jobs to shrink

        logger.info(f"[Elastic Scaling] {len(elastic_jobs_running_on_higher_allocation)} elastic jobs found. Required nodes: {required_nodes}")
        for job, allocated_nodes in elastic_jobs_running_on_higher_allocation:
            extra_nodes_for_job = len(allocated_nodes) - job.min_nodes
            if extra_nodes_for_job == 0:
                continue  # Job already at min capacity
            shrinkable_nodes_num = min(extra_nodes_for_job, required_nodes)  # Ensure we don't exceed than required resources
            if shrinkable_nodes_num > 0:

                if os.path.exists(self.policy_file):
                    with open(self.policy_file, "r") as file:
                        policy_data = json.load(file)
                        if any(job_entry["id"] == str(job.id) for job_entry in policy_data["jobs"]):
                            logger.info(f"[Policy] Job {job.id} entry already exists.")
                            time.sleep(120)
                            return

                shrinkable_nodes = split_list(allocated_nodes, shrinkable_nodes_num)
                if shrinkable_nodes:
                    self.running_jobs = [(j, nodes) if j != job else (j, allocated_nodes) for j, nodes in self.running_jobs]
                    new_entry = {
                            "id": str(job.id),
                            "scale": "shrink",
                            "num_nodes": shrinkable_nodes_num,
                            "nodes": str(",".join(shrinkable_nodes)),
                            "start_after": 0
                        }
                    if os.path.exists(self.policy_file):
                        with open(self.policy_file, "r+") as file:
                            try:
                                policy_data = json.load(file)
                                # if "jobs" not in policy_data:
                                #     policy_data["jobs"] = []  # Ensure "jobs" exists
                                if any(job_entry["id"] == new_entry["id"] for job_entry in policy_data["jobs"]):
                                    logger.info(f"[Policy] Job {job.id} entry already exists.")
                                policy_data["jobs"].append(new_entry)
                                file.seek(0)
                                file.truncate()
                                json.dump(policy_data, file, indent=4)
                                logger.info(f"[Policy] New job entry added for Job {job.id}.")
                            except json.JSONDecodeError:
                                print("[Policy] Invalid JSON format. Resetting policy file.")
                                policy_data = {"jobs": [new_entry]}
                                with open(self.policy_file, "w") as new_file:
                                    json.dump(policy_data, new_file, indent=4)
                    required_nodes -= len(shrinkable_nodes)
                    self.node_manager.free_nodes(shrinkable_nodes)
                    time.sleep(120)
                    print(f"[Elastic Scaling] Job {job.id} shrunk by {len(shrinkable_nodes)} nodes (Now: {len(allocated_nodes)} nodes).")
            # If no more nodes available, stop further expansion
            if required_nodes <= 0:
                break

    def schedule_jobs(self):
        """Continuously checks for available jobs to schedule."""
        while True:
            time.sleep(1)
            with self.lock:
                if self.job_queue:
                    job = self.job_queue[0]
                    allocated_nodes = self.node_manager.allocate_nodes(job.min_nodes)
                    if allocated_nodes:
                        self.job_queue.popleft()
                        self.running_jobs.append((job, allocated_nodes))
                        threading.Thread(target=self.execute_job, args=(job, allocated_nodes)).start()

                        if self.schedule_type == 1 and job.type=="elastic" and len(self.node_manager.read_hostfile()) > 0:
                            nodes_to_expand_for_job = job.max_nodes - job.min_nodes
                            nodes_for_expansion = min(nodes_to_expand_for_job, len(self.node_manager.read_hostfile()))
                            self.expand_elastic_jobs(nodes_for_expansion, job.id)
                    else:
                        # check for resource fragmentation
                        available_nodes = len(self.node_manager.read_hostfile())
                        if available_nodes > 0:
                            minimum_required_for_next_job = int(job.min_nodes)
                            if available_nodes < minimum_required_for_next_job and len(self.job_queue)!=0:
                                logger.info(f"[Fragmentation] Available nodes: {available_nodes}, "
                                        f"but smallest pending job requires: {minimum_required_for_next_job}. "
                                        f"Potential resource fragmentation detected!")
                                if self.schedule_type == 0:
                                    # create elastic schedule if possible
                                    self.expand_elastic_jobs(available_nodes)
                                else:
                                    self.shrink_elastic_jobs(minimum_required_for_next_job - available_nodes)
                else:
                    available_nodes = len(self.node_manager.read_hostfile())
                    if available_nodes > 0 and len(self.running_jobs)!=0:
                        logger.info("Try expanding if free nodes detected.")
                        self.expand_elastic_jobs(available_nodes)


    def execute_job(self, job, allocated_nodes):
        """Simulate job execution and free nodes after completion."""
        allocated_nodes_string_without_slots = ",".join(allocated_nodes)
        allocated_nodes_string = ",".join(f"{node}:64" for node in allocated_nodes)
        print(job.command)

        print(job)
        if "prun " in job.command:
            command = job.command.replace("prun ", "prun --dvm-uri file:/home/rbhattara/europar25/tests/dvm.uri --host {} --map-by node --bind-to none -n {} ".format(allocated_nodes_string, len(allocated_nodes)))
        if "--nodelist " in job.command:
            command = job.command.replace("--nodelist ", "--nodelist {} ".format(allocated_nodes_string_without_slots))
            command = command.replace("--job_id ", "--job_id {} ".format(job.id))
            command = command.replace("--nnodes ", "--nnodes {} ".format(len(allocated_nodes)))
            command = command.replace("--wt", "--wt {}".format(job.execution_time))
        if "-L" in job.command:
            command = job.command.replace("-L", "-L {} -N {} -J {} ".format(allocated_nodes_string_without_slots, len(allocated_nodes), job.id))
        if "NUM_NODES" in job.command:
            command = job.command.replace("NUM_NODES_EXAMOL ", "NUM_NODES_EXAMOL={} NODES_EXAMOL={} JOB_ID_EXAMOL={} WALLTIME_EXAMOL={} ".format(len(allocated_nodes), allocated_nodes_string_without_slots, job.id, job.execution_time))       
        logger.info(f"Job {job.id} started with command: {command} on nodes: {allocated_nodes}")
        job_start_time = time.time()
        try:
            # Run the command using subprocess
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            # Capture and print real-time output
            for line in iter(process.stdout.readline, ''):
                print(f"[Job {job.id}] {line.strip()}")
            process.stdout.close()
            process.communicate()  # Wait for process to complete
            if process.returncode == 0:
                self.update_job_status(job.command, "completed")
                print(f"Job {job.id} completed successfully.")
                logger.info(f"Job {job.id} completed successfully.")
            else:
                print(f"Job {job.id} failed with return code {process.returncode}. Error: {process.stderr.read()}")
                logger.info(f"Job {job.id} failed with return code {process.returncode}. Error: {process.stderr.read()}")
                self.update_job_status(job.command, "failed")

        except Exception as e:
            print(f"Error executing job {job.id}: {e}")
            logger.error(f"Error executing job {job.id}: {e}")
        with self.lock:
            self.running_jobs = [j for j in self.running_jobs if j[0] != job]
            self.node_manager.free_nodes(allocated_nodes)
        print(f"Job {job.id} completed.")
        job_actual_duration = time.time() - job_start_time

        logger.info(f"Job {job.id} completed and took {job_actual_duration}")

    def update_job_status(self, command, status):
        """Update job status in JSON file."""
        if not os.path.exists(self.job_file):
            return

        with open(self.job_file, "r+") as file:
            try:
                jobs = json.load(file)
                for job in jobs:
                    if job.get("job_command") == command:
                        job["status"] = status
                        break

                # Write back the updated job list
                file.seek(0)
                json.dump(jobs, file, indent=4)
                file.truncate()

            except json.JSONDecodeError:
                print("Invalid JSON format in job file.")

    def start(self):
        """Start the scheduler in a separate thread."""
        threading.Thread(target=self.schedule_jobs, daemon=True).start()