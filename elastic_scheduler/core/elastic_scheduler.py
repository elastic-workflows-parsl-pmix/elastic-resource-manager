from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import List, Optional, Any, Dict

from elastic_scheduler.jobs.job import JobRecord

logger = logging.getLogger(__name__)


def _is_elastic_capable(job: Any) -> bool:
    try:
        return getattr(job, "type", None) == "elastic" and int(job.max_nodes) > int(job.min_nodes)
    except Exception:
        return getattr(job, "type", None) == "elastic"


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
    job_id: Optional[str] = None,
) -> int:
    """
    Expand elastic-capable jobs up to their max_nodes, constrained by available_nodes.
    Returns total number of nodes newly allocated.
    """
    if available_nodes <= 0:
        return 0

    if job_id is None:
        candidates: List[JobRecord] = [
            rj for rj in running_jobs
            if _is_elastic_capable(rj) and len(rj.nodes) < rj.max_nodes
        ]
    else:
        candidates = [
            rj for rj in running_jobs
            if str(rj.id) == str(job_id) and _is_elastic_capable(rj) and len(rj.nodes) < rj.max_nodes
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
) -> int:
    """
    Request shrinking of elastic-capable jobs to free required_nodes.
    Writes 'shrink' directives to policy and frees nodes.
    Returns total nodes freed.
    """
    if required_nodes <= 0:
        return 0

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