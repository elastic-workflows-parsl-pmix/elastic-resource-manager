from __future__ import annotations

import fcntl
import json
import logging
import os
import time
import threading
from pathlib import Path
from typing import List, Any, Dict

from elastic_scheduler.jobs.job import JobRecord, JobRequest

logger = logging.getLogger(__name__)

def _wait_and_allocate_nodes(policy_file: str, entry: Dict[str, Any], expand_nodes: List[str], 
                             job_id: Any, runtime_nodes: Any, elastic_events: int, 
                             timeout: float = 600.0, expand_start_time: float = None):
    """
    Wait in a background thread for policy to be applied during expansion.
    This function runs in its own thread to avoid blocking the main scheduler.
    """
    try:
        # Track when this specific job's expansion starts waiting
        wait_start_time = time.time()
        last_log_time = wait_start_time
        
        # Wait until policy is applied or timeout occurs
        cancel_event = threading.Event()
        
        while not cancel_event.is_set():
            if not policy_entry_exists(policy_file, entry):
                # Policy applied
                wait_duration = time.time() - wait_start_time
                total_duration = time.time() - expand_start_time if expand_start_time else wait_duration
                
                logger.info(
                    f"[Elastic Scaling] Job {job_id} +{len(expand_nodes)} nodes "
                    f"(Now: {len(runtime_nodes)}). Events={elastic_events} "
                    f"[Wait: {wait_duration:.2f}s, Total: {total_duration:.2f}s]"
                )
                return
                
            # Periodically log that we're still waiting
            current_time = time.time()
            if current_time - last_log_time >= 30.0:
                elapsed = current_time - wait_start_time
                logger.info(f"[Policy] Still waiting for expand to apply for Job {job_id} "
                          f"(nodes: {len(expand_nodes)}, waiting: {elapsed:.1f}s)")
                last_log_time = current_time
                
            # Check for timeout
            if timeout and (current_time - wait_start_time) > timeout:
                logger.warning(
                    f"[Policy] Expand for Job {job_id} not applied within {timeout}s timeout; "
                    f"nodes may not be properly allocated: {entry['nodes']}."
                )
                return
                
            # Sleep briefly to avoid CPU spin
            time.sleep(1.0)
            
    except Exception as e:
        logger.error(f"[Policy] Error in background thread waiting for Job {job_id} expand: {e}")

def _wait_and_free_nodes(policy_file: str, entry: Dict[str, Any], shrink_nodes: List[str], 
                        node_manager: Any, job_id: Any, runtime_nodes: Any, 
                        elastic_events: int, timeout: float = 600.0, shrink_start_time: float = None):
    """
    Wait in a background thread for policy to be applied, then free nodes.
    This function runs in its own thread to avoid blocking the main scheduler.
    """
    try:
        # Track when this specific job's shrink starts waiting
        wait_start_time = time.time()
        last_log_time = wait_start_time
        
        # Wait until policy is applied or timeout occurs
        cancel_event = threading.Event()
        
        while not cancel_event.is_set():
            if not policy_entry_exists(policy_file, entry):
                # Policy applied - free the nodes
                node_manager.free_nodes(shrink_nodes)
                
                # Calculate timings
                wait_duration = time.time() - wait_start_time
                total_duration = time.time() - shrink_start_time if shrink_start_time else wait_duration
                
                logger.info(
                    f"[Elastic Scaling] Job {job_id} -{len(shrink_nodes)} nodes "
                    f"(Now: {len(runtime_nodes)}). Events={elastic_events} "
                    f"[Wait: {wait_duration:.2f}s, Total: {total_duration:.2f}s]"
                )
                return
                
            # Periodically log that we're still waiting
            current_time = time.time()
            if current_time - last_log_time >= 30.0:
                elapsed = current_time - wait_start_time
                logger.info(f"[Policy] Still waiting for shrink to apply for Job {job_id} "
                          f"(nodes: {len(shrink_nodes)}, waiting: {elapsed:.1f}s)")
                last_log_time = current_time
                
            # Check for timeout
            if timeout and (current_time - wait_start_time) > timeout:
                logger.warning(
                    f"[Policy] Shrink for Job {job_id} not applied within {timeout}s timeout; "
                    f"deferring free of nodes {entry['nodes']}."
                )
                return
                
            # Sleep briefly to avoid CPU spin
            time.sleep(1.0)
            
    except Exception as e:
        logger.error(f"[Policy] Error in background thread waiting for Job {job_id} shrink: {e}")


def _is_elastic_capable(job: Any) -> bool:
    try:
        job_type = getattr(job, "type", None)
        return job_type in ("elastic") and int(job.max_nodes) > int(job.min_nodes)
    except Exception:
        job_type = getattr(job, "type", None)
        return job_type in ("elastic")


def _split_list(items: List[str], n: int) -> List[str]:
    if n <= 0:
        return []
    n = min(n, len(items))
    removed = items[-n:]
    del items[-n:]
    return removed

def _policy_entry_matches(e: Dict[str, Any], entry: Dict[str, Any]) -> bool:
    try:
        return (
            str(e.get("id")) == str(entry.get("id")) and
            str(e.get("scale")) == str(entry.get("scale")) and
            int(e.get("num_nodes", -1)) == int(entry.get("num_nodes", -1)) and
            str(e.get("nodes", "")) == str(entry.get("nodes", ""))
        )
    except Exception:
        return False

def policy_entry_exists(policy_file: str, entry: Dict[str, Any]) -> bool:
    """True if the exact entry is present in policy_file."""
    path = Path(policy_file)
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return any(_policy_entry_matches(e, entry) for e in data.get("jobs", []))
    except Exception as e:
        logger.warning(f"[Policy] Failed to read policy file {path}: {e}")
        return False

def check_complete(policy_file: str, entry: Dict[str, Any], timeout: float = 60.0, interval: float = 1.0) -> bool:
    """Wait until this exact entry is removed (applied)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not policy_entry_exists(policy_file, entry):
            return True
        time.sleep(interval)
    return False

def policy_has_job(policy_file: str, job_id: Any) -> bool:
    path = Path(policy_file)
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return any(str(job_id) == str(entry.get("id")) for entry in data.get("jobs", []))
    except Exception as e:
        logger.warning(f"Failed to read policy file {path}: {e}")
        return False


def update_policy_file(policy_file: str, entry: Dict[str, Any]) -> None:
    """
    Ensure policy file exists/valid, then append the entry if job id not present.
    Matches the simpler working pattern you described.
    """
    new_entry = dict(entry)
    job_id_str = str(new_entry.get("id"))

    try:
        # Create or reset if missing/empty
        if not os.path.exists(policy_file) or os.stat(policy_file).st_size == 0:
            logger.warning("[Policy] Policy file does not exist or is empty. Creating a new policy file.")
            policy_data = {"jobs": [new_entry]}
            with open(policy_file, "w", encoding="utf-8") as new_file:
                json.dump(policy_data, new_file, indent=4)
            logger.info(f"[Policy] New job entry added for Job {job_id_str}.")
            return

        # Read and update
        with open(policy_file, "r+", encoding="utf-8") as file:
            try:
                policy_data = json.load(file)
            except json.JSONDecodeError:
                logger.error("[Policy] Invalid JSON format. Resetting policy file.")
                policy_data = {"jobs": [new_entry]}
                # Rewrite file
                file.seek(0)
                file.truncate()
                json.dump(policy_data, file, indent=4)
                logger.info(f"[Policy] New job entry added for Job {job_id_str}.")
                return
            jobs = policy_data.get("jobs", [])
            jobs.append(new_entry)
            policy_data["jobs"] = jobs
            file.seek(0)
            file.truncate()
            json.dump(policy_data, file, indent=4)
            logger.info(f"[Policy] New job entry added for Job {job_id_str}.")

    except Exception as e:
        logger.error(f"Failed to update policy file {policy_file}: {e}")


# ---------- Candidate Selection Strategies ----------

def _parse_walltime(wt_str: str) -> int:
    """Parse walltime string to seconds."""
    try:
        if not wt_str:
            return 0
        parts = wt_str.split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        else:
            return int(wt_str)
    except (ValueError, TypeError):
        return 0

def _get_cooldown_period(job: JobRecord) -> float:
    walltime_secs = _parse_walltime(job.walltime)
    cooldown_pct = 0.25  # 25% of walltime
    cooldown_min = 60  # minimum 60 seconds
    cooldown_max = 300  # maximum 5 minutes
    cooldown = max(cooldown_min, min(cooldown_max, walltime_secs * cooldown_pct))
    return cooldown

def _get_remaining_time(job: JobRecord) -> float:
    """Return estimated remaining time for a job in seconds."""
    try:
        start_time = getattr(job.runtime, "start_time", 0)
        if not start_time:
            return float('inf')
        walltime_secs = _parse_walltime(job.walltime)
        if not walltime_secs:
            return float('inf')
        elapsed = max(0, time.time() - start_time)
        return max(0, walltime_secs - elapsed)
    except Exception:
        return float('inf')

def _get_job_priority(job: JobRecord) -> int:
    """Get job priority (higher is more important)."""
    try:
        return int(getattr(job.spec, "priority", 0))
    except (ValueError, TypeError, AttributeError):
        return 0

# ---------- Expansion Strategies ----------

def _expand_fcfs(candidates: List[JobRecord]) -> List[JobRecord]:
    """First-come-first-served expansion (default)."""
    return candidates  # already in submission order

def _expand_longest_first(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by longest walltime first."""
    return sorted(candidates, key=lambda j: _parse_walltime(j.walltime), reverse=True)

def _expand_most_remaining(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by most remaining time first."""
    return sorted(candidates, key=_get_remaining_time, reverse=True)

def _expand_highest_priority(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by highest priority first."""
    return sorted(candidates, key=_get_job_priority, reverse=True)

def _expand_most_scalable(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by most scalable (largest headroom) first."""
    return sorted(candidates, key=lambda j: j.max_nodes - len(j.nodes), reverse=True)

def _expand_all_equal(candidates: List[JobRecord]) -> List[JobRecord]:
    """Return all candidates for equal sharing."""
    # The expand function will handle dividing nodes equally
    return candidates

# ---------- Shrinking Strategies ----------

def _shrink_fcfs(candidates: List[JobRecord]) -> List[JobRecord]:
    """First-come-first-served shrinking (reverse of submission order)."""
    return list(reversed(candidates))  # newest jobs shrink first

def _shrink_shortest_first(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by shortest walltime first."""
    return sorted(candidates, key=lambda j: _parse_walltime(j.walltime))

def _shrink_least_remaining(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by least remaining time first."""
    return sorted(candidates, key=_get_remaining_time)

def _shrink_lowest_priority(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by lowest priority first."""
    return sorted(candidates, key=_get_job_priority)

def _shrink_most_nodes(candidates: List[JobRecord]) -> List[JobRecord]:
    """Sort by jobs with most extra nodes above minimum."""
    return sorted(candidates, key=lambda j: len(j.nodes) - j.min_nodes, reverse=True)

def _shrink_all_equal(candidates: List[JobRecord]) -> List[JobRecord]:
    """Return all candidates for equal shrinking."""
    # The shrink function will handle removing nodes proportionally
    return candidates

# ---------- Strategy Registry ----------

EXPAND_STRATEGIES = {
    "fcfs": _expand_fcfs,
    "longest": _expand_longest_first,
    "most_remaining": _expand_most_remaining, 
    "priority": _expand_highest_priority,
    "most_scalable": _expand_most_scalable,
    "equal": _expand_all_equal,
}

SHRINK_STRATEGIES = {
    "fcfs": _shrink_fcfs,
    "shortest": _shrink_shortest_first,
    "least_remaining": _shrink_least_remaining,
    "priority": _shrink_lowest_priority,
    "most_nodes": _shrink_most_nodes,
    "equal": _shrink_all_equal,
}

# ---------- Updated Main Functions ----------

def expand_elastic_jobs(
    running_jobs: List[JobRecord],
    available_nodes: int,
    node_manager: Any,
    policy_file: str,
    evolving_request: bool = False,
    strategy: str = "fcfs"
) -> int:
    """
    Expand elastic-capable jobs up to their max_nodes, constrained by available_nodes.
    Returns total number of nodes newly allocated.

    Strategies:
    - fcfs: First-Come-First-Served (default)
    - longest: Longest walltime first
    - most_remaining: Most remaining time first
    - priority: Highest priority first
    - most_scalable: Jobs with most expansion headroom first
    - equal: Try to expand all jobs equally
    """
    if available_nodes <= 0:
        return 0

    # Handle evolving jobs or find elastic candidates
    if len(running_jobs) == 1 and running_jobs[0].spec.type == "evolving" and evolving_request:
        candidates = [running_jobs[0]]
    else:
        candidates = [
            rj for rj in running_jobs
            if _is_elastic_capable(rj) and len(rj.nodes) < rj.max_nodes
        ]

    if not candidates:
        logger.info("[Elastic Scaling] No elastic-capable candidates to expand.")
        return 0

    if not evolving_request:
    # Apply the selected strategy
        strategy_fn = EXPAND_STRATEGIES.get(strategy, _expand_fcfs)
        candidates = strategy_fn(candidates)
        
        logger.info(f"[Elastic Scaling] {len(candidates)} elastic-capable jobs using '{strategy}' strategy. Available: {available_nodes}")
    else:
        logger.info(f"[Elastic Scaling] Evolving job request for Job {candidates[0].id}. Expand by: {available_nodes}")
    
    if strategy == "equal" and len(candidates) > 0:
        return _expand_equal_share(candidates, available_nodes, node_manager, policy_file)
    else:
        return _expand_sequential(candidates, available_nodes, node_manager, policy_file)

# Update _expand_sequential to include timing and background threads
def _expand_sequential(candidates: List[JobRecord], available_nodes: int, node_manager: Any, policy_file: str) -> int:
    """Expand jobs sequentially according to the provided order with timing."""
    expand_start = time.time()
    expanded_total = 0
    threads = []  # Track all threads

    for rj in candidates:
        now = time.time()
        cooldown = _get_cooldown_period(rj)
        
        # Avoid rapid repeated expand attempts
        if rj.runtime.elastic_events and (now - rj.runtime.last_elastic_time < cooldown):
            logger.info(f"[Elastic Scaling] Skipping rapid expand attempt for Job {rj.id}. "
                        f"Cooldown: {cooldown:.1f}s")
            continue

        need = rj.max_nodes - len(rj.nodes)
        if need <= 0:
            continue

        expandable = min(need, available_nodes - expanded_total)
        if expandable <= 0:
            break

        if policy_has_job(policy_file, rj.id):
            logger.info(f"[Policy] Job {rj.id} entry already exists. Skipping expand request.")
            continue

        new_nodes = node_manager.allocate_nodes(expandable)
        if not new_nodes:
            continue

        rj.record_expand(new_nodes)

        entry = {
            "id": str(rj.id),
            "scale": "expand",
            "num_nodes": len(new_nodes),
            "nodes": ",".join(new_nodes),
            "start_after": 0,
        }
        update_policy_file(policy_file, entry)

        # Launch background thread to wait for policy application
        thread = threading.Thread(
            target=_wait_and_allocate_nodes,
            args=(policy_file, entry, new_nodes, rj.id, 
                  rj.runtime.nodes, rj.runtime.elastic_events),
            kwargs={"expand_start_time": expand_start},
            daemon=True,
            name=f"ExpandWaiter-{rj.id}"
        )
        thread.start()
        threads.append((thread, rj.id, len(new_nodes)))

        expanded_total += len(new_nodes)
        logger.info(
            f"[Elastic Scaling] Job {rj.id} +{len(new_nodes)} "
            f"(Now: {len(rj.runtime.nodes)}). Events={rj.runtime.elastic_events} "
            f"[Thread started]"
        )

        if expanded_total >= available_nodes:
            break

    expand_duration = time.time() - expand_start
    
    logger.info(
        f"[Elastic Scaling] Expand complete. "
        f"Allocated {expanded_total}/{available_nodes} nodes. "
        f"Execution: {expand_duration:.3f}s. "
        f"{len(threads)} background threads active."
    )

    return expanded_total


def _expand_equal_share(candidates: List[JobRecord], available_nodes: int, node_manager: Any, policy_file: str) -> int:
    """Expand all jobs with equal distribution of nodes and timing."""
    if not candidates or available_nodes <= 0:
        return 0

    expand_start = time.time()
    
    cooldown = _get_cooldown_period(candidates[0])
    if candidates[0].runtime.elastic_events and (time.time() - candidates[0].runtime.last_elastic_time < cooldown):
        logger.info(f"[Elastic Scaling] Skipping rapid expand attempt for Jobs {', '.join(str(j.id) for j in candidates)}.")
        return 0
    
    # Calculate basic fair share
    jobs_to_expand = [j for j in candidates if not policy_has_job(policy_file, j.id)]
    if not jobs_to_expand:
        return 0

    expanded_total = 0
    threads = []
    
    # First pass: calculate fair share
    fair_shares = {}
    remaining = available_nodes
    active_jobs = len(jobs_to_expand)
    
    for job in jobs_to_expand:
        headroom = job.max_nodes - len(job.nodes)
        if headroom <= 0:
            active_jobs -= 1
            continue
            
        fair_share = min(headroom, remaining // max(1, active_jobs))
        if fair_share <= 0:
            active_jobs -= 1
            continue
            
        fair_shares[job.id] = fair_share
        remaining -= fair_share
        active_jobs -= 1
    
    # Second pass: apply fair shares
    for job in jobs_to_expand:
        share = fair_shares.get(job.id, 0)
        if share <= 0:
            continue
            
        new_nodes = node_manager.allocate_nodes(share)
        if not new_nodes:
            continue

        job.record_expand(new_nodes)

        entry = {
            "id": str(job.id),
            "scale": "expand",
            "num_nodes": len(new_nodes),
            "nodes": ",".join(new_nodes),
            "start_after": 0,
        }
        update_policy_file(policy_file, entry)

        # Launch background thread
        thread = threading.Thread(
            target=_wait_and_allocate_nodes,
            args=(policy_file, entry, new_nodes, job.id, 
                  job.runtime.nodes, job.runtime.elastic_events),
            kwargs={"expand_start_time": expand_start},
            daemon=True,
            name=f"ExpandWaiter-{job.id}"
        )
        thread.start()
        threads.append((thread, job.id, len(new_nodes)))

        expanded_total += len(new_nodes)
        logger.info(
            f"[Elastic Scaling] Job {job.id} +{len(new_nodes)} "
            f"(Now: {len(job.runtime.nodes)}, fair share). Events={job.runtime.elastic_events} "
            f"[Thread started]"
        )

    expand_duration = time.time() - expand_start
    
    logger.info(
        f"[Elastic Scaling] Equal-share expand complete. "
        f"Allocated {expanded_total}/{available_nodes} nodes. "
        f"Execution: {expand_duration:.3f}s. "
        f"{len(threads)} background threads active."
    )

    return expanded_total

def shrink_elastic_jobs(
    running_jobs: List[JobRecord],
    required_nodes: int,
    node_manager: Any,
    policy_file: str,
    evolving_request: bool = False,
    strategy: str = "fcfs"
) -> int:
    """
    Request shrinking of elastic-capable jobs to free required_nodes.
    Writes 'shrink' directives to policy and frees nodes.
    Returns total nodes freed.

    Strategies:
    - fcfs: First-Come-First-Served (newest jobs first)
    - shortest: Shortest walltime first
    - least_remaining: Least remaining time first
    - priority: Lowest priority first
    - most_nodes: Jobs with most nodes above minimum first
    - equal: Try to shrink all jobs proportionally
    """
    if required_nodes <= 0:
        return 0

    # Handle evolving jobs or find elastic candidates
    if len(running_jobs) == 1 and running_jobs[0].spec.type == "evolving" and evolving_request:
        candidates = [running_jobs[0]]
    else:
        candidates: List[JobRecord] = [
            rj for rj in running_jobs
            if _is_elastic_capable(rj) and len(rj.nodes) > rj.min_nodes
        ]

    if not candidates:
        logger.info("[Elastic Scaling] No elastic-capable candidates to shrink.")
        return 0

    if not evolving_request:
        # Apply the selected strategy
        strategy_fn = SHRINK_STRATEGIES.get(strategy, _shrink_fcfs)
        candidates = strategy_fn(candidates)
        logger.info(f"[Elastic Scaling] {len(candidates)} elastic-capable jobs using '{strategy}' strategy. Need to free: {required_nodes}")
    else:
        logger.info(f"[Elastic Scaling] Evolving job request for Job {candidates[0].id}. Shrink by: {required_nodes}")
    
    if strategy == "equal" and len(candidates) > 0:
        return _shrink_equal_share(candidates, required_nodes, node_manager, policy_file)
    else:
        return _shrink_sequential(candidates, required_nodes, node_manager, policy_file)

def _shrink_sequential(candidates: List[JobRecord], required_nodes: int, node_manager: Any, policy_file: str) -> int:
    """Shrink jobs sequentially according to the provided order, with validation phase and timing."""
    
    # Phase 1: Validation - Check if we can meet requirements without actually shrinking
    validation_start = time.time()
    freed_total = 0
    shrink_plan = []  # List of (job, shrink_nodes, shrink_num) tuples
    
    for rj in candidates:
        if freed_total >= required_nodes:
            break
            
        now = time.time()
        cooldown = _get_cooldown_period(rj)
        
        # Skip if in cooldown
        if rj.runtime.elastic_events and (now - rj.runtime.last_elastic_time < cooldown):
            logger.info(f"[Elastic Scaling][Validation] Skipping Job {rj.id} - in cooldown ({cooldown:.1f}s)")
            continue

        # Skip if already has pending policy entry
        if policy_has_job(policy_file, rj.id):
            logger.info(f"[Elastic Scaling][Validation] Skipping Job {rj.id} - policy entry exists")
            continue

        extra = len(rj.nodes) - rj.min_nodes
        if extra <= 0:
            continue

        shrink_num = min(extra, required_nodes - freed_total)
        if shrink_num <= 0:
            continue

        # Simulate the split (don't actually modify the job yet)
        nodes_copy = list(rj.nodes)
        potential_shrink = nodes_copy[-shrink_num:] if shrink_num <= len(nodes_copy) else []
        
        if not potential_shrink:
            continue

        shrink_plan.append((rj, potential_shrink, shrink_num))
        freed_total += shrink_num
        
        logger.info(f"[Elastic Scaling][Validation] Job {rj.id} can provide {shrink_num} nodes")

    validation_duration = time.time() - validation_start

    # Check if we can meet the requirement
    if freed_total < required_nodes:
        logger.warning(
            f"[Elastic Scaling][Validation] Cannot meet requirement. "
            f"Need {required_nodes} nodes, can only free {freed_total} nodes. "
            f"Validation took {validation_duration:.3f}s. Aborting shrink operation."
        )
        return 0
    
    logger.info(
        f"[Elastic Scaling][Validation] Validation passed in {validation_duration:.3f}s. "
        f"Can free {freed_total}/{required_nodes} nodes from {len(shrink_plan)} jobs. "
        f"Proceeding with shrink."
    )
    
    # Phase 2: Execution - Actually perform the shrinking
    execution_start = time.time()
    actual_freed = 0
    threads = []  # Track all threads
    
    for rj, potential_shrink, shrink_num in shrink_plan:
        # Double-check conditions haven't changed
        if policy_has_job(policy_file, rj.id):
            logger.warning(f"[Elastic Scaling][Execution] Job {rj.id} now has policy entry, skipping")
            continue
            
        # Actually remove nodes from the job
        shrink_nodes = _split_list(rj.nodes, shrink_num)
        if not shrink_nodes:
            logger.warning(f"[Elastic Scaling][Execution] Failed to split nodes for Job {rj.id}")
            continue

        rj.record_shrink(shrink_nodes)

        entry = {
            "id": str(rj.id),
            "scale": "shrink",
            "num_nodes": len(shrink_nodes),
            "nodes": ",".join(shrink_nodes),
            "start_after": 0,
        }
        update_policy_file(policy_file, entry)

        # Launch background thread to wait for policy application and free nodes
        thread = threading.Thread(
            target=_wait_and_free_nodes,
            args=(policy_file, entry, shrink_nodes, node_manager, rj.id, 
                  rj.runtime.nodes, rj.runtime.elastic_events),
            kwargs={"shrink_start_time": execution_start},
            daemon=True,
            name=f"ShrinkWaiter-{rj.id}"
        )
        thread.start()
        threads.append((thread, rj.id, len(shrink_nodes)))
        
        actual_freed += len(shrink_nodes)
        
        logger.info(
            f"[Elastic Scaling][Execution] Job {rj.id} -{len(shrink_nodes)} nodes "
            f"(Now: {len(rj.runtime.nodes)}). Events={rj.runtime.elastic_events} "
            f"[Thread started]"
        )
        
        if actual_freed >= required_nodes:
            break

    execution_duration = time.time() - execution_start
    
    # Log summary with timing breakdown
    logger.info(
        f"[Elastic Scaling] Shrink complete. "
        f"Freed {actual_freed}/{required_nodes} nodes. "
        f"Validation: {validation_duration:.3f}s, Execution: {execution_duration:.3f}s, "
        f"Total: {validation_duration + execution_duration:.3f}s. "
        f"{len(threads)} background threads active."
    )
    
    return actual_freed


# Update _shrink_equal_share to include validation and execution timing
def _shrink_equal_share(candidates: List[JobRecord], required_nodes: int, node_manager: Any, policy_file: str) -> int:
    """Shrink all jobs proportionally with validation phase and timing."""
    if not candidates or required_nodes <= 0:
        return 0

    validation_start = time.time()
    
    cooldown = _get_cooldown_period(candidates[0])
    if candidates[0].runtime.elastic_events and (time.time() - candidates[0].runtime.last_elastic_time < cooldown):
        logger.info(f"[Elastic Scaling] Skipping rapid shrink attempt - in cooldown ({cooldown:.1f}s)")
        return 0

    # Filter out jobs with policy entries
    jobs_to_shrink = [j for j in candidates if not policy_has_job(policy_file, j.id)]
    if not jobs_to_shrink:
        logger.info("[Elastic Scaling][Validation] No jobs available for shrinking (all have policy entries)")
        return 0

    # Phase 1: Validation - Calculate what we can shrink
    total_extra_nodes = sum(len(j.nodes) - j.min_nodes for j in jobs_to_shrink)
    if total_extra_nodes <= 0:
        logger.info("[Elastic Scaling][Validation] No shrinkable nodes available")
        return 0
    
    if total_extra_nodes < required_nodes:
        validation_duration = time.time() - validation_start
        logger.warning(
            f"[Elastic Scaling][Validation] Cannot meet requirement. "
            f"Need {required_nodes} nodes, only {total_extra_nodes} shrinkable nodes available. "
            f"Validation took {validation_duration:.3f}s. Aborting shrink operation."
        )
        return 0
    
    # Calculate proportional shrink amounts
    shrink_plan = []
    total_planned = 0
    
    for job in jobs_to_shrink:
        job_extra = len(job.nodes) - job.min_nodes
        if job_extra <= 0:
            continue
            
        # Proportional shrinking based on extra nodes
        job_ratio = job_extra / total_extra_nodes
        job_shrink = min(job_extra, max(1, int(required_nodes * job_ratio)))
        
        # Simulate node split
        nodes_copy = list(job.nodes)
        potential_shrink = nodes_copy[-job_shrink:] if job_shrink <= len(nodes_copy) else []
        
        if potential_shrink:
            shrink_plan.append((job, potential_shrink, job_shrink))
            total_planned += job_shrink
            logger.info(f"[Elastic Scaling][Validation] Job {job.id} can provide {job_shrink} nodes (proportional)")
    
    validation_duration = time.time() - validation_start
    
    if total_planned < required_nodes:
        logger.warning(
            f"[Elastic Scaling][Validation] Proportional distribution insufficient. "
            f"Need {required_nodes} nodes, can only free {total_planned} nodes. "
            f"Validation took {validation_duration:.3f}s. Aborting shrink operation."
        )
        return 0
    
    logger.info(
        f"[Elastic Scaling][Validation] Validation passed in {validation_duration:.3f}s. "
        f"Can free {total_planned}/{required_nodes} nodes from {len(shrink_plan)} jobs proportionally. "
        f"Proceeding with shrink."
    )
    
    # Phase 2: Execution - Actually perform the shrinking
    execution_start = time.time()
    actual_freed = 0
    threads = []
    
    for job, potential_shrink, shrink_num in shrink_plan:
        # Double-check conditions
        if policy_has_job(policy_file, job.id):
            logger.warning(f"[Elastic Scaling][Execution] Job {job.id} now has policy entry, skipping")
            continue
            
        # Actually remove nodes
        shrink_nodes = _split_list(job.nodes, shrink_num)
        if not shrink_nodes:
            logger.warning(f"[Elastic Scaling][Execution] Failed to split nodes for Job {job.id}")
            continue

        job.record_shrink(shrink_nodes)

        entry = {
            "id": str(job.id),
            "scale": "shrink",
            "num_nodes": len(shrink_nodes),
            "nodes": ",".join(shrink_nodes),
            "start_after": 0,
        }
        update_policy_file(policy_file, entry)

        # Launch background thread
        thread = threading.Thread(
            target=_wait_and_free_nodes,
            args=(policy_file, entry, shrink_nodes, node_manager, job.id, 
                  job.runtime.nodes, job.runtime.elastic_events),
            kwargs={"shrink_start_time": execution_start},
            daemon=True,
            name=f"ShrinkWaiter-{job.id}"
        )
        thread.start()
        threads.append((thread, job.id, len(shrink_nodes)))

        actual_freed += len(shrink_nodes)
        
        logger.info(
            f"[Elastic Scaling][Execution] Job {job.id} -{len(shrink_nodes)} nodes "
            f"(Now: {len(job.runtime.nodes)}, proportional). Events={job.runtime.elastic_events} "
            f"[Thread started]"
        )

        if actual_freed >= required_nodes:
            break

    execution_duration = time.time() - execution_start

    logger.info(
        f"[Elastic Scaling] Proportional shrink complete. "
        f"Freed {actual_freed}/{required_nodes} nodes. "
        f"Validation: {validation_duration:.3f}s, Execution: {execution_duration:.3f}s, "
        f"Total: {validation_duration + execution_duration:.3f}s. "
        f"{len(threads)} background threads active."
    )
    
    return actual_freed


def handle_evolving_job_requests(
     job_request: JobRequest,
     job_record: JobRecord,
     available_nodes: int,
     node_manager: Any,
     policy_file: str,
     evolving_job_requests_file: str = "jobrequests.json",
 ) -> None:
    """
    Handle evolving job requests.
    """
    if job_record.spec.type != "evolving":
        logger.error(f"Job {job_request.job_id} is not of type 'evolving'. Cannot process request.")
        update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
        remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        return
    
    jid = str(job_request.job_id)
    logger.info(f"Handling evolving job request for Job ID: {jid}")

    scale = str(job_request.scale).lower()
    try:
        req_nodes = int(job_request.num_nodes)
    except Exception:
        logger.error(f"[Evolving] Invalid num_nodes in request for Job {jid}: {job_request.num_nodes}")
        update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
        remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        return

    if req_nodes <= 0:
        logger.info(f"[Evolving] Rejecting request for Job {jid}: num_nodes must be > 0")
        update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
        remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        return

    cur = len(job_record.nodes)
    min_nodes = int(getattr(job_record, "min_nodes"))
    max_nodes = int(getattr(job_record, "max_nodes"))

    if scale == "expand":
        # Limit by job headroom and currently available cluster nodes
        headroom = max(0, max_nodes - cur)
        expand_by = min(req_nodes, headroom, max(0, int(available_nodes)))
        if expand_by <= 0:
            logger.info(f"[Evolving] Expand rejected for Job {jid}: requested={req_nodes}, headroom={headroom}, available={available_nodes}")
            update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
            remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
            return

        # Expand only this job by passing [job_record] and limiting available_nodes
        gained = expand_elastic_jobs(
            running_jobs=[job_record],
            available_nodes=expand_by,
            node_manager=node_manager,
            policy_file=policy_file,
            evolving_request=True
        )
        if gained > 0:
            logger.info(f"[Evolving] Expand accepted for Job {jid}: +{gained} nodes")
            # Mark applied and remove the request (prevents resubmission)
            update_job_requests_status(job_request.job_id, "applied", evolving_job_requests_file)
            remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        else:
            logger.info(f"[Evolving] Expand could not be applied for Job {jid}")
            update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
            remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        return

    if scale == "shrink":
        # Limit by how many nodes the job can release without violating min_nodes
        shrinkable = max(0, cur - min_nodes)
        shrink_by = min(req_nodes, shrinkable)
        if shrink_by <= 0:
            logger.info(f"[Evolving] Shrink rejected for Job {jid}: requested={req_nodes}, shrinkable={shrinkable}")
            update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
            remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
            return

        freed = shrink_elastic_jobs(
            running_jobs=[job_record],
            required_nodes=shrink_by,
            node_manager=node_manager,
            policy_file=policy_file,
            evolving_request=True
        )
        if freed > 0:
            logger.info(f"[Evolving] Shrink accepted for Job {jid}: -{freed} nodes")
            update_job_requests_status(job_request.job_id, "applied", evolving_job_requests_file)
            remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        else:
            logger.info(f"[Evolving] Shrink could not be applied for Job {jid}")
            update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
            remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)
        return

    logger.error(f"[Evolving] Unknown scale '{scale}' for Job {jid}")
    update_job_requests_status(job_request.job_id, "rejected", evolving_job_requests_file)
    remove_job_requests_by_id(job_request.job_id, evolving_job_requests_file)

def remove_job_requests_by_id(job_id, evolving_job_requests_file: str = "jobrequests.json") -> bool:
    try:
        with open(evolving_job_requests_file, 'r+') as file:
            fcntl.flock(file, fcntl.LOCK_EX)
            try:
                data = json.load(file)
                job_requests = data.get("job_requests", [])
                new_job_requests = [job_request for job_request in job_requests if job_request["job_id"] != str(job_id)]

                if len(job_requests) == len(new_job_requests):
                    logger.info(f"No job request with ID {job_id} found to remove.")
                    return False

                data["job_requests"] = new_job_requests
                file.seek(0)
                file.truncate()
                json.dump(data, file, indent=4)
                logger.info(f"Job request from job id {job_id} removed.")
                return True
            finally:
                fcntl.flock(file, fcntl.LOCK_UN)

    except (FileNotFoundError, json.JSONDecodeError):
        logger.error("File not found or contains invalid JSON.")
        return False

def update_job_requests_status(job_id, status, evolving_job_requests_file: str = "jobrequests.json") -> None:
    """Update job status in JSON file."""
    if not os.path.exists(evolving_job_requests_file):
        logger.info("No job requests file found")
        return

    with open(evolving_job_requests_file, "r+") as file:
        try:
            job_requests = json.load(file)
            for job_request in job_requests.get("job_requests", []):
                if int(job_request.get("job_id")) == int(job_id):
                    job_request["status"] = status
                    break

            # Write back the updated job list
            file.seek(0)
            json.dump(job_requests, file, indent=4)
            file.truncate()
            logger.info("Updated Job Request File")
        except json.JSONDecodeError:
            print("Invalid JSON format in job file.")