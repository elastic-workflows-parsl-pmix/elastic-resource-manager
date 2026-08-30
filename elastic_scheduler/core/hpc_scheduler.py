from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Any, Optional

from elastic_scheduler.jobs.job import JobRecord, JobStatus, JobRequest
from elastic_scheduler.core.elastic_scheduler import (
    expand_elastic_jobs as elastic_expand,
    mark_incomplete_phases_on_completion,
    shrink_elastic_jobs as elastic_shrink,
    handle_evolving_job_requests as handle_evolving
)

logger = logging.getLogger(__name__)

@dataclass
class Reservation:
    """A reservation of nodes for a pending job."""
    job_id: str
    nodes_needed: int
    nodes_reserved: int  # nodes we expect from shrinking
    created_at: float = field(default_factory=time.time)
    shrink_issued: bool = False
    expires_at: float = field(default_factory=lambda: time.time() + 300)  # 5 min timeout
    
    def is_expired(self) -> bool:
        return time.time() > self.expires_at

class HPCScheduler:
    """
    Coordinates job scheduling, expansion, shrinking, and execution.
    """

    def __init__(self, node_manager: Any, job_file: str, policy_file: str, dvm_file: str, evolving_job_requests_file: str, schedule_type: int, expand_strategy: str = "fcfs", shrink_strategy: str = "fcfs", scheduling_mode: str = "rigid") -> None:
        self.node_manager = node_manager
        self.job_queue: Deque[JobRecord] = deque()
        self.running_jobs: List[JobRecord] = []
        self.completed_jobs: List[JobRecord] = []
        self.evolving_job_requests_file = evolving_job_requests_file
        self.job_file = job_file
        self.policy_file = policy_file
        self.completed_log_file = os.path.join(os.path.dirname(self.job_file) or ".", "completed_jobs.jsonl")
        self.pending_job_requests: Deque[JobRequest] = deque()
        self.schedule_type = schedule_type
        self.dvm_file = dvm_file
        self.expand_strategy = expand_strategy
        self.shrink_strategy = shrink_strategy
        # Scheduling mode: "rigid", "evolving", "scheduler_driven"
        self.scheduling_mode = scheduling_mode

        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)

        self.reservations: Dict[str, Reservation] = {}  # job_id -> Reservation


    # ------------ Public API ------------
    
    def available_nodes(self) -> int:
        """Get available nodes from node manager."""
        return len(self.node_manager.read_hostfile())

    def submit_job(self, job: JobRecord) -> str:
        """Submit a job, stamp arrival_time, enqueue, notify, and return job_id."""
        with self.cv:
            job.mark_queued()
            self.job_queue.append(job)
            self.cv.notify()
            logger.info(f"Job {job.id} submitted and queued.")
            return job.id

    def submit_evolving_job_requests(self,  job_request: JobRequest) -> None:
        """Submit a evolving job request to the scheduler queue."""
        with self.lock:
            self.pending_job_requests.append(job_request)
            logger.info(f"Evolving job request for job {job_request.job_id} submitted and queued.")
            self.cv.notify()

    def start(self) -> None:
        """Start the scheduler in a separate thread."""
        t = threading.Thread(target=self._run_loop, name="HPCScheduler", daemon=True)
        t.start()

    # ------------ Reservation helpers ------------

    def _get_reservation(self, job_id: str) -> Optional[Reservation]:
        """Get existing reservation for a job, removing if expired."""
        res = self.reservations.get(job_id)
        if res and res.is_expired():
            logger.info(f"[Reservation] Reservation for job {job_id} expired, removing.")
            del self.reservations[job_id]
            return None
        return res

    def _create_reservation(self, job: JobRecord, nodes_needed: int, nodes_from_shrink: int) -> Reservation:
        """Create a new reservation for a job waiting on shrink."""
        res = Reservation(
            job_id=job.id,
            nodes_needed=nodes_needed,
            nodes_reserved=nodes_from_shrink,
            shrink_issued=True
        )
        self.reservations[job.id] = res
        logger.info(f"[Reservation] Created reservation for job {job.id}: "
                   f"needs={nodes_needed}, expecting={nodes_from_shrink} from shrink")
        return res

    def _clear_reservation(self, job_id: str) -> None:
        """Clear reservation when job starts or is cancelled."""
        if job_id in self.reservations:
            del self.reservations[job_id]
            logger.info(f"[Reservation] Cleared reservation for job {job_id}")

    def _cleanup_expired_reservations(self) -> None:
        """Remove any expired reservations."""
        expired = [jid for jid, res in self.reservations.items() if res.is_expired()]
        for jid in expired:
            del self.reservations[jid]
            logger.info(f"[Reservation] Expired reservation for job {jid}")

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
                has_req = bool(self.pending_job_requests)
                if qsize == 0 and not has_req:
                    logger.info(f"Scheduler waiting. Queue size: {qsize}, Running: {rsize}")
                    self.cv.wait(timeout=5.0)
                    logger.info(f"Scheduler woke up. Queue size: {len(self.job_queue)}")
                # If queue > 0, skip waiting and try scheduling immediately
                else:
                    self.cv.wait(timeout=1.0)

            try:
                self._cleanup_expired_reservations()
                self._attempt_scheduling()
            except Exception as e:
                logger.exception(f"Scheduling iteration failed: {e}")

    def schedule_pending_jobs(self, job: JobRecord) -> bool:
        """Try to schedule pending jobs from the queue."""
        logger.info(f"Scheduling attempt: {len(self.job_queue)} jobs in queue")
        logger.info(f"Attempting to schedule Job {job.id} (min_nodes={job.min_nodes}, max_nodes={job.max_nodes})")
        
        # First try with preferred allocation size based on schedule type
        req = job.spec.min_nodes if self.schedule_type == 0 else job.spec.max_nodes
        logger.info(f"First attempt with {req} nodes (schedule_type={self.schedule_type})")
        allocated = self.node_manager.allocate_nodes(req)
        
        # If schedule_type is 1 and max_nodes allocation failed, try with min_nodes as fallback
        if not allocated and self.schedule_type == 1 and job.spec.min_nodes < job.spec.max_nodes:
            logger.info(f"Max nodes allocation failed, trying fallback with min_nodes={job.spec.min_nodes}")
            req = job.spec.min_nodes
            allocated = self.node_manager.allocate_nodes(req)
        
        with self.cv:
            if allocated and self.job_queue and self.job_queue[0] is job:
                self.job_queue.popleft()
                # Clear any reservation since job is starting
                self._clear_reservation(job.id)
                job.start(allocated)
                self.running_jobs.append(job)
                threading.Thread(
                    target=self.execute_job,
                    args=(job,),
                    name=f"Job-{job.spec.id}",
                    daemon=True,
                ).start()
                logger.info(f"Successfully scheduled Job {job.id} with {len(allocated)} nodes")
                return True

        return False

    def schedule_job_request(self, job_request: JobRequest) -> bool:
        with self.cv:
            if not self.pending_job_requests or self.pending_job_requests[0] is not job_request:
                return False
            self.pending_job_requests.popleft()

        job_id = job_request.job_id
        job_record = next((j for j in self.running_jobs if str(j.id) == str(job_id)), None)
        if not job_record:
            logger.info(f"Evolving request for unknown/finished job {job_id}; discarding.")
            return False
        handle_evolving(job_request, job_record, self.available_nodes(), self.node_manager, self.policy_file, self.evolving_job_requests_file)
        return True

    def _attempt_scheduling(self) -> None:
        """Try to schedule jobs, expand elastic jobs, or shrink when fragmented."""
        with self.cv:
            self.running_jobs = [j for j in self.running_jobs if j.status == JobStatus.RUNNING]
            job = self.job_queue[0] if self.job_queue else None
            job_request = self.pending_job_requests[0] if self.pending_job_requests else None

        # === MODE: RIGID ===
        # Only schedule jobs, no elastic operations
        if self.scheduling_mode == "rigid":
            if job:
                if self.schedule_pending_jobs(job):
                    logger.info(f"[Rigid] Scheduled Job {job.id} from queue.")
                    return
                # Try backfill if head job couldn't start
                if len(self.job_queue) > 0:
                    self.backfill_jobs()
            return

        # === MODE: EVOLVING ===
        # Schedule jobs + only honor application-driven requests (no scheduler-initiated scaling)
        if self.scheduling_mode == "evolving":
            # Process evolving job requests first (application-driven)
            if job_request:
                if self.schedule_job_request(job_request):
                    logger.info(f"[Evolving] Processed evolving request {job_request.id}.")
                    return

            # Then try to schedule pending jobs
            if job:
                if self.schedule_pending_jobs(job):
                    logger.info(f"[Evolving] Scheduled Job {job.id} from queue.")
                    return
                # Try backfill if head job couldn't start
                if len(self.job_queue) > 0:
                    self.backfill_jobs()
            return

        # === MODE: SCHEDULER_DRIVEN ===
        # Full scheduler control: shrink victims to fit jobs, expand when idle nodes exist
        if self.scheduling_mode == "scheduler_driven":
            # 1) Process evolving requests if any (still honor app requests)
            if job_request:
                if self.schedule_job_request(job_request):
                    logger.info(f"[Scheduler-Driven] Processed evolving request {job_request.id}.")
                    return

            # 2) Try to schedule pending job directly
            if job:
                if self.schedule_pending_jobs(job):
                    logger.info(f"[Scheduler-Driven] Scheduled Job {job.id} from queue.")
                    return

                # 3) If job couldn't start, try shrinking running jobs to make room
                available = self.available_nodes()
                required = (job.spec.min_nodes if self.schedule_type == 0 else job.spec.max_nodes)
                shortfall = required - available

                # if shortfall > 0 and available > 0:
                if shortfall > 0:
                    # Check if we already have a pending shrink for this job
                    existing_res = self._get_reservation(job.id)

                    if existing_res and existing_res.shrink_issued:
                        logger.info(f"[Scheduler-Driven] Existing shrink reservation for Job {job.id} in place; waiting.")
                        return
                    
                    logger.info(f"[Scheduler-Driven] Job {job.id} needs {required} nodes, "
                               f"available={available}, shortfall={shortfall}. Attempting shrink.")
                    
                    shrunk = elastic_shrink(
                        self.running_jobs, shortfall, self.node_manager, 
                        self.policy_file, strategy=self.shrink_strategy
                    )
                    if shrunk > 0:
                        self._create_reservation(job, required, shrunk)
                        logger.info(f"[Scheduler-Driven] Issued shrink for {shrunk} nodes. "
                                       f"Job {job.id} will start when nodes are released.")
                        return
                    else:
                        # 3b) Could not shrink enough, but if there are idle nodes, 
                        # expand running jobs to reduce fragmentation
                        logger.info(f"[Scheduler-Driven] Could not shrink enough nodes for Job {job.id}.")
                        if available > 0 and len(self.running_jobs) > 0:
                            logger.info(f"[Scheduler-Driven] {available} idle nodes exist but can't fit pending job. "
                                       f"Expanding running jobs to reduce fragmentation.")
                            expanded = elastic_expand(
                                self.running_jobs, available, self.node_manager, 
                                self.policy_file, strategy=self.expand_strategy
                            )
                            if expanded > 0:
                                logger.info(f"[Scheduler-Driven] Expanded running jobs by {expanded} nodes to reduce fragmentation.")
                                return
                # 4) Try backfill if head job still can't start
                if len(self.job_queue) > 0 and available > 0:
                    self.backfill_jobs()

            # 5) If no pending jobs or after scheduling, expand running jobs with idle nodes
            available = self.available_nodes()
            has_pending_reservation = any(
                res.shrink_issued and not res.is_expired() 
                for res in self.reservations.values()
            )
            if available > 0 and len(self.running_jobs) > 0 and not has_pending_reservation:
                logger.info(f"[Scheduler-Driven] {available} idle nodes available. Attempting expansion.")
                expanded = elastic_expand(
                    self.running_jobs, available, self.node_manager, 
                    self.policy_file, strategy=self.expand_strategy
                )
                if expanded > 0:
                    logger.info(f"[Scheduler-Driven] Expanded running jobs by {expanded} nodes.")
            elif has_pending_reservation:
                logger.info(f"[Scheduler-Driven] {available} idle nodes available but reserved for pending job.")
            return

        logger.warning(f"Unknown scheduling_mode '{self.scheduling_mode}', using default behavior.")

    def backfill_jobs(self) -> None:
        # --- Backfilling: try to run a smaller job without delaying the head job ---
        # Snapshot state for safe computations outside the lock
        with self.cv:
            queue_snapshot = list(self.job_queue)
            running_snapshot = list(self.running_jobs)
        if queue_snapshot:
            head = queue_snapshot[0]
            available_now = self.available_nodes()
            if available_now > 0:
                t_res = self._reserve_time_for_head(head, available_now, running_snapshot)
                now = time.time()
                backfill = None
                for cand in queue_snapshot[1:]:
                    cand_req = cand.spec.min_nodes if self.schedule_type == 0 else cand.spec.max_nodes
                    if cand_req <= available_now:
                        cand_wt = self._walltime_seconds(cand.walltime)
                        if cand_wt > 0 and (now + cand_wt) <= t_res:
                            backfill = (cand, cand_req)
                            break

                if backfill:
                    cand, cand_req = backfill
                    bf_nodes = self.node_manager.allocate_nodes(cand_req)
                    if bf_nodes:
                        committed = False
                        with self.cv:
                            # Ensure the candidate is still queued; remove by identity
                            try:
                                self.job_queue.remove(cand)
                                cand.start(bf_nodes)
                                self.running_jobs.append(cand)
                                threading.Thread(
                                    target=self.execute_job,
                                    args=(cand,),
                                    name=f"Job-{cand.spec.id}",
                                    daemon=True,
                                ).start()
                                logger.info(f"Backfilled Job {cand.id} (req={cand_req}) before head Job {head.id}")
                                committed = True
                            except ValueError:
                                committed = False
                        if committed:
                            return
                        else:
                            # Candidate disappeared; return nodes
                            self.node_manager.free_nodes(bf_nodes)
    
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
        if "--nodelist " in rj.command:
            command = rj.command.replace("--nodelist ", "--nodelist {} ".format(nodes_str))
            command = command.replace("--job_id ", "--job_id {} ".format(rj.id))
            command = command.replace("--nnodes ", "--nnodes {} --minnodes {} --maxnodes {} ".format(len(rj.nodes), rj.min_nodes, rj.max_nodes))
            command = command.replace("--wt", "--wt {}".format(rj.walltime))
        if "-L" in command:
            command = command.replace("-L", "-L {} -N {} -J {} -mi {} -ma {}".format(nodes_str, len(rj.nodes), rj.id, rj.min_nodes, rj.max_nodes))
        if "NUM_NODES" in command:
            command = command.replace("NUM_NODES_EXAMOL ", "NUM_NODES_EXAMOL={} NODES_EXAMOL={} JOB_ID_EXAMOL={} MIN_NODES_EXAMOL={} MAX_NODES_EXAMOL={} WALLTIME_EXAMOL={} ".format(len(rj.nodes), nodes_str, rj.id, rj.min_nodes, rj.max_nodes, rj.walltime))

        return command
    
    def execute_job(self, rj: JobRecord, log_stderr: bool = False) -> None:
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
            if proc.stderr and log_stderr:  # Only log stderr if the flag is True
                t_err = threading.Thread(
                    target=_pump_stream,
                    args=(proc.stderr, lambda l: logger.error(f"[Job {rj.id}][stderr] {l}")),
                    daemon=True,
                )
                t_err.start()
                threads.append(t_err)
            elif proc.stderr:  # Still read stderr to prevent subprocess from hanging
                t_err = threading.Thread(
                    target=_pump_stream,
                    args=(proc.stderr, lambda l: None),  # Do nothing with the line
                    daemon=True,
                )
                t_err.start()
                threads.append(t_err)

            rc = proc.wait()
            for t in threads:
                t.join(timeout=2.0)

            if rc == 0:
                self.update_job_status(rj.command, "completed")
                mark_incomplete_phases_on_completion(rj)
                logger.info(f"Job {rj.id} completed successfully.")
                rj.complete(True)
            else:
                logger.error(f"Job {rj.id} failed with code {rc}.")
                mark_incomplete_phases_on_completion(rj, success=False)
                self.update_job_status(rj.command, "failed")
                rj.complete(False)

        except Exception as e:
            logger.exception(f"Error executing job {rj.id}: {e}")
            rj.complete(False)

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
        
        nodes_to_free = list(rj.nodes)  # snapshot
        self.node_manager.free_nodes(nodes_to_free)
        self._notify()

    def _serialize_completed_job(self, rj: JobRecord) -> Dict[str, Any]:
        """Serialize completed job with full elastic phase details."""
        # Get phase summary and detailed phase data
        phase_summary = rj.runtime.get_phase_summary()
        phases = [p.to_dict() for p in rj.runtime.elastic_phases]
        
        # Calculate total scaling overhead (time spent in scaling operations)
        total_scaling_overhead = sum(
            p.scaling_duration or 0 for p in rj.runtime.elastic_phases 
            if p.operation != "initial"
        )
        
        # Calculate node-hours for analysis
        total_node_seconds = 0
        for phase in rj.runtime.elastic_phases:
            if phase.phase_duration and phase.nodes_after:
                total_node_seconds += phase.phase_duration * phase.nodes_after
        
        return {
            # Basic job info
            "id": rj.id,
            "status": rj.status.value,
            "min_nodes": rj.min_nodes,
            "max_nodes": rj.max_nodes,
            "walltime": rj.walltime,
            "command": rj.command,
            
            # Timing info
            "arrival_time": rj.runtime.arrival_time,
            "start_time": rj.runtime.start_time,
            "completion_time": rj.runtime.completion_time,
            "total_runtime": (
                rj.runtime.completion_time - rj.runtime.start_time
                if rj.runtime.completion_time and rj.runtime.start_time else None
            ),
            "wait_time": (
                rj.runtime.start_time - rj.runtime.arrival_time
                if rj.runtime.start_time and rj.runtime.arrival_time else None
            ),
            
            # Node info
            "initial_nodes": phases[0]["nodes_after"] if phases else len(rj.nodes),
            "final_node_count": len(rj.nodes),
            "final_nodes": list(rj.nodes),
            
            # Elastic scaling summary
            "elastic_events": rj.runtime.elastic_events,
            "last_elastic_time": rj.runtime.last_elastic_time,
            "total_scaling_overhead": total_scaling_overhead,
            "total_node_seconds": total_node_seconds,
            
            # Phase summary statistics
            "phase_summary": phase_summary,
            
            # Detailed phase history
            "phases": phases,
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


    def _notify(self) -> None:
        """Notify the scheduler loop of new work/resources."""
        with self.cv:
            logger.info(f"Notifying scheduler - queue:{len(self.job_queue)}, nodes:{self.available_nodes()}")
            self.cv.notify_all()

    def _walltime_seconds(self, wt: str | None) -> int:
        """Parse walltime 'HH:MM:SS' (or 'MM:SS', or seconds) to seconds; 0 if unknown."""
        if not wt:
            return 0
        try:
            parts = [int(p) for p in str(wt).split(":")]
            if len(parts) == 3:
                h, m, s = parts
            elif len(parts) == 2:
                h, m, s = 0, parts[0], parts[1]
            elif len(parts) == 1:
                h, m, s = 0, 0, parts[0]
            else:
                return 0
            return h * 3600 + m * 60 + s
        except Exception:
            return 0

    def _reserve_time_for_head(self, head: JobRecord, idle_now: int, running_snapshot: List[JobRecord]) -> float:
        """
        Compute earliest time when 'head' can start (EASY reservation).
        Returns epoch seconds T* such that at least head.req nodes are available by T*.
        """
        now = time.time()
        if not head:
            return now
        req = head.spec.min_nodes if self.schedule_type == 0 else head.spec.max_nodes
        if idle_now >= req:
            return now

        # Build release timeline from running jobs
        events: List[tuple[float, int]] = []
        for r in running_snapshot:
            # need start_time and walltime for ETA; if unknown, be conservative (far future)
            st = getattr(r.runtime, "start_time", None)
            wt = self._walltime_seconds(getattr(r, "walltime", None))
            if st is None or wt <= 0:
                # push very far so we don't accidentally delay the head
                eta = now + 365*24*3600
            else:
                eta = st + wt
            nodes_release = len(getattr(r, "nodes", []) or getattr(r.runtime, "nodes", []))
            if nodes_release > 0:
                events.append((eta, nodes_release))

        events.sort(key=lambda x: x[0])
        have = idle_now
        for t, rel in events:
            have += rel
            if have >= req:
                return t

        # If still not enough, head cannot be guaranteed; return far future
        return now + 365*24*3600