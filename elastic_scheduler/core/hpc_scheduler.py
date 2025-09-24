from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Any

from elastic_scheduler.jobs.job import JobRecord, JobStatus
from elastic_scheduler.core.elastic_scheduler import (
    expand_elastic_jobs as elastic_expand,
    shrink_elastic_jobs as elastic_shrink,
)

logger = logging.getLogger(__name__)


class HPCScheduler:
    """
    Coordinates job scheduling, expansion, shrinking, and execution.
    """

    def __init__(self, node_manager: Any, job_file: str, policy_file: str, dvm_file: str, schedule_type: int) -> None:
        self.node_manager = node_manager
        self.job_queue: Deque[JobRecord] = deque()
        self.running_jobs: List[JobRecord] = []
        self.completed_jobs: List[JobRecord] = []
        self.job_file = job_file
        self.policy_file = policy_file
        self.completed_log_file = os.path.join(os.path.dirname(self.job_file) or ".", "completed_jobs.jsonl")
        self.schedule_type = schedule_type
        self.dvm_file = dvm_file

        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)

    # ------------ Public API ------------

    def submit_job(self, job: JobRecord) -> str:
        """Submit a job, stamp arrival_time, enqueue, notify, and return job_id."""
        with self.cv:
            job.mark_queued()
            self.job_queue.append(job)
            self.cv.notify()
            logger.info(f"Job {job.id} submitted and queued.")
            return job.id

    def start(self) -> None:
        """Start the scheduler in a separate thread."""
        t = threading.Thread(target=self._run_loop, name="HPCScheduler", daemon=True)
        t.start()

    # ------------ Main loop ------------

    def _run_loop(self) -> None:
        """
        Event-driven scheduler loop.
        Wakes on job submission or when resources are freed (notify from execute_job).
        """
        while True:
            with self.cv:
                qsize = len(self.job_queue)
                rsize = len(self.running_jobs)
                if qsize == 0:
                    logger.info(f"Scheduler waiting. Queue size: {qsize}, Running: {rsize}")
                    self.cv.wait(timeout=5.0)
                    logger.info(f"Scheduler woke up. Queue size: {len(self.job_queue)}")
                # If queue > 0, skip waiting and try scheduling immediately
                else:
                    self.cv.wait(timeout=1.0)

            try:
                self._attempt_scheduling()
            except Exception as e:
                logger.exception(f"Scheduling iteration failed: {e}")

    def _attempt_scheduling(self) -> None:
        """Try to schedule jobs, expand elastic jobs, or shrink when fragmented."""
        with self.cv:
            # prune finished (job threads only set status; scheduler owns the list)
            self.running_jobs = [j for j in self.running_jobs if j.status == JobStatus.RUNNING]
            if self.job_queue:
                job = self.job_queue[0]
                req = job.spec.min_nodes if self.schedule_type == 0 else job.spec.max_nodes
            else:
                job = None
                req = 0
            logger.info(f"Scheduling attempt: {len(self.job_queue)} jobs in queue")

        allocated = None
        if job:
            allocated = self.node_manager.allocate_nodes(req)

        with self.cv:
            if job and allocated and self.job_queue and self.job_queue[0] is job:
                self.job_queue.popleft()
                job.start(allocated)
                self.running_jobs.append(job)
                threading.Thread(
                    target=self.execute_job,
                    args=(job,),
                    name=f"Job-{job.spec.id}",
                    daemon=True,
                ).start()
                return

        available = len(self.node_manager.read_hostfile())
        if not allocated and available > 0 and len(self.running_jobs) > 0:  # fragmented
            if self.schedule_type == 0:
                logger.info("Free nodes detected with running jobs. Attempting expansion.")
                expanded = elastic_expand(self.running_jobs, available, self.node_manager, self.policy_file)
                if expanded > 0:
                    self._notify()
            else:
                min_allocation_job = job.spec.min_nodes if job else 0
                logger.info("Free nodes detected with running jobs. Attempting shrinking.")
                shrunk = elastic_shrink(self.running_jobs, min_allocation_job, self.node_manager, self.policy_file)
                if shrunk > 0:
                    self._notify()
    
    # ------------ Job execution ------------

    def get_launch_command(self, rj: JobRecord) -> str:
        """Construct the command to launch the job (some jobs may require specific commands) on its allocated nodes."""
        nodes_str = ",".join(rj.runtime.nodes)
        nodes_with_slots = ",".join(f"{n}:64" for n in rj.runtime.nodes)
        command = rj.command
        if "prun " in command:
            command = command.replace(
                "prun ",
                f"prun --dvm-uri file:{self.dvm_file} --host {nodes_with_slots} "
                f"--map-by node --bind-to none -n {len(rj.nodes)} "
            )
        if "--nodelist " in command:
            command = command.replace("--nodelist ", f"--nodelist {nodes_str} ")
            command = command.replace("--job_id ", f"--job_id {rj.id} ")
            command = command.replace("--nnodes ", f"--nnodes {len(rj.nodes)} ")
            command = command.replace("--wt", f"--wt {rj.walltime}")  # fixed
        if "-L" in command:
            command = command.replace(
                "-L", f"-L {nodes_str} -N {len(rj.nodes)} -J {rj.id} "
            )
        if "NUM_NODES" in command:
            command = command.replace(
                "NUM_NODES_EXAMOL ",
                f"NUM_NODES_EXAMOL={len(rj.nodes)} "
                f"NODES_EXAMOL={nodes_str} "
                f"JOB_ID_EXAMOL={rj.id} "
                f"WALLTIME_EXAMOL={rj.walltime} "
            )
        return command
    
    def execute_job(self, rj: JobRecord) -> None:
        """
        Execute a job and free nodes after completion. Updates job status in job_file.
        """
        command = self.get_launch_command(rj)

        try:
            logger.info(f"Job {rj.id} starting: {command} | nodes={rj.nodes}")
            start = time.time()

            proc = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Read both streams concurrently to avoid deadlocks
            def _pump_stream(stream, log_fn):
                try:
                    for line in iter(stream.readline, ""):
                        if line:
                            log_fn(line.rstrip())
                except Exception:
                    pass
                finally:
                    try:
                        stream.close()
                    except Exception:
                        pass

            threads: List[threading.Thread] = []
            if proc.stdout:
                t_out = threading.Thread(
                    target=_pump_stream,
                    args=(proc.stdout, lambda l: logger.info(f"[Job {rj.id}] {l}")),
                    daemon=True,
                )
                t_out.start()
                threads.append(t_out)
            if proc.stderr:
                t_err = threading.Thread(
                    target=_pump_stream,
                    args=(proc.stderr, lambda l: logger.error(f"[Job {rj.id}][stderr] {l}")),
                    daemon=True,
                )
                t_err.start()
                threads.append(t_err)

            rc = proc.wait()
            for t in threads:
                t.join(timeout=2.0)

            if rc == 0:
                self.update_job_status(rj.command, "completed")
                logger.info(f"Job {rj.id} completed successfully.")
                rj.complete(True)
            else:
                logger.error(f"Job {rj.id} failed with code {rc}.")
                self.update_job_status(rj.command, "failed")
                rj.complete(False)

        except Exception as e:
            logger.exception(f"Error executing job {rj.id}: {e}")

        nodes_to_free = list(rj.nodes)  # snapshot
        self.node_manager.free_nodes(nodes_to_free)

        duration = time.time() - start if 'start' in locals() else 0.0
        last_el = (
            time.strftime('%H:%M:%S', time.localtime(rj.runtime.last_elastic_time))
            if rj.runtime.last_elastic_time else 'n/a'
        )
        logger.info(
            f"Job {rj.id} finished. Duration: {duration:.2f}s. "
            f"Elastic events: {rj.runtime.elastic_events}, last elastic: {last_el}"
        )
        try:
            entry = self._serialize_completed_job(rj)
            self._append_completed_entry(entry)
        except Exception as e:
            logger.error(f"Failed to append completed job: {e}")
        
        self._notify()

    def _serialize_completed_job(self, rj: JobRecord) -> Dict[str, Any]:
        return {
            "id": rj.id,
            "status": rj.status.value,
            "min_nodes": rj.min_nodes,
            "max_nodes": rj.max_nodes,
            "walltime": rj.walltime,  # fixed
            "command": rj.command,
            "arrival_time": rj.runtime.arrival_time,
            "start_time": rj.runtime.start_time,
            "completion_time": rj.runtime.completion_time,
            "elastic_events": rj.runtime.elastic_events,
            "last_elastic_time": rj.runtime.last_elastic_time,
            "final_node_count": len(rj.nodes),
            "final_nodes": list(rj.nodes),
        }

    # ------------ Helpers ------------

    def update_job_status(self, command: str, status: str) -> None:
        """Update job status in the job JSON file."""
        path = Path(self.job_file)
        if not path.exists():
            return

        try:
            with path.open("r+", encoding="utf-8") as f:
                jobs = json.load(f)
                for j in jobs:
                    if j.get("job_command") == command:
                        j["status"] = status
                        break
                f.seek(0)
                json.dump(jobs, f, indent=4)
                f.truncate()
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in job file when updating status.")
        except Exception as e:
            logger.error(f"Failed to update job status: {e}")

    def _append_completed_entry(self, entry: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.completed_log_file) or ".", exist_ok=True)
        with open(self.completed_log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _available_nodes(self) -> int:
        try:
            return len(self.node_manager.read_hostfile())
        except Exception as e:
            logger.error(f"Failed to read hostfile: {e}")
            return 0

    def _notify(self) -> None:
        """Notify the scheduler loop of new work/resources."""
        with self.cv:
            logger.info(f"Notifying scheduler - queue:{len(self.job_queue)}, nodes:{self._available_nodes()}")
            self.cv.notify_all()