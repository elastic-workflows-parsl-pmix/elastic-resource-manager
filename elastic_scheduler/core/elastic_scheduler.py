from __future__ import annotations

import fcntl
import json
import logging
import os
from pathlib import Path
from typing import List, Any, Dict

from elastic_scheduler.jobs.job import JobRecord, JobRequest

logger = logging.getLogger(__name__)


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


def expand_elastic_jobs(
    running_jobs: List[JobRecord],
    available_nodes: int,
    node_manager: Any,
    policy_file: str,
    evolving_request: bool = False
) -> int:
    """
    Expand elastic-capable jobs up to their max_nodes, constrained by available_nodes.
    Returns total number of nodes newly allocated.
    """
    if available_nodes <= 0:
        return 0

    if len(running_jobs) == 1 and running_jobs[0].spec.type == "evolving" and evolving_request:
        # Special case: single evolving job, expand directly without policy
        candidates = [running_jobs[0]]
    else:
        candidates = [
            rj for rj in running_jobs
            if _is_elastic_capable(rj) and len(rj.nodes) < rj.max_nodes
        ]

    if not candidates:
        logger.info("[Elastic Scaling] No elastic-capable candidates to expand.")
        return 0

    logger.info(f"[Elastic Scaling] {len(candidates)} elastic-capable jobs. Available nodes: {available_nodes}")
    expanded_total = 0

    for rj in candidates:
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

        expanded_total += len(new_nodes)
        logger.info(
            f"[Elastic Scaling] Job {rj.id} +{len(new_nodes)} "
            f"(Now: {len(rj.runtime.nodes)}). Events={rj.runtime.elastic_events}"
        )

        if expanded_total >= available_nodes:
            break

    return expanded_total


def shrink_elastic_jobs(
    running_jobs: List[JobRecord],
    required_nodes: int,
    node_manager: Any,
    policy_file: str,
    evolving_request: bool = False
) -> int:
    """
    Request shrinking of elastic-capable jobs to free required_nodes.
    Writes 'shrink' directives to policy and frees nodes.
    Returns total nodes freed.
    """
    if required_nodes <= 0:
        return 0

    if len(running_jobs) == 1 and running_jobs[0].spec.type == "evolving" and evolving_request:
        # Special case: single evolving job, expand directly without policy
        candidates = [running_jobs[0]]
    else:
        candidates: List[JobRecord] = [
            rj for rj in running_jobs
            if _is_elastic_capable(rj) and len(rj.nodes) > rj.min_nodes
        ]

    if not candidates:
        logger.info("[Elastic Scaling] No elastic-capable candidates to shrink.")
        return 0

    logger.info(f"[Elastic Scaling] {len(candidates)} elastic-capable jobs. Need to free: {required_nodes}")
    freed_total = 0

    for rj in list(candidates):
        extra = len(rj.nodes) - rj.min_nodes
        if extra <= 0:
            continue

        shrink_num = min(extra, required_nodes - freed_total)
        if shrink_num <= 0:
            break

        # Avoid duplicate shrink requests if policy already contains this job
        if policy_has_job(policy_file, rj.id):
            logger.info(f"[Policy] Job {rj.id} entry already exists. Skipping shrink request.")
            continue

        shrink_nodes = _split_list(rj.nodes, shrink_num)
        if not shrink_nodes:
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

        node_manager.free_nodes(shrink_nodes)
        freed_total += len(shrink_nodes)

        logger.info(
            f"[Elastic Scaling] Job {rj.id} -{len(shrink_nodes)} "
            f"(Now: {len(rj.runtime.nodes)}). Events={rj.runtime.elastic_events}"
        )

        if freed_total >= required_nodes:
            break

    return freed_total

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