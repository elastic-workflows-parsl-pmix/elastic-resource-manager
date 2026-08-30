from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Any, Dict, List, Optional


class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class ElasticPhase:
    """Represents a single elastic scaling phase."""
    phase_id: int
    operation: str  # "expand" or "shrink" or "initial"
    request_time: float  # When scaling was requested
    applied_time: Optional[float] = None  # When scaling was applied (policy complete)
    end_time: Optional[float] = None  # When phase ended (next phase started or job completed)
    nodes_before: int = 0
    nodes_after: int = 0
    nodes_added: List[str] = field(default_factory=list)
    nodes_removed: List[str] = field(default_factory=list)
    success: bool = True
    trigger: str = "scheduler"  # "scheduler", "evolving_request", "backfill"
    
    @property
    def scaling_duration(self) -> Optional[float]:
        """Time taken to apply the scaling operation (request to applied)."""
        if self.applied_time is None:
            return None
        return self.applied_time - self.request_time
    
    @property
    def phase_duration(self) -> Optional[float]:
        """Total time this phase was active (applied to end)."""
        if self.applied_time is None or self.end_time is None:
            return None
        return self.end_time - self.applied_time
    
    @property
    def total_duration(self) -> Optional[float]:
        """Total time from request to phase end."""
        if self.end_time is None:
            return None
        return self.end_time - self.request_time
    
    def mark_applied(self, success: bool = True) -> None:
        """Mark scaling operation as applied (policy complete)."""
        self.applied_time = time.time()
        self.success = success
    
    def mark_ended(self) -> None:
        """Mark phase as ended (next phase started or job completed)."""
        self.end_time = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for analysis export."""
        return {
            "phase_id": self.phase_id,
            "operation": self.operation,
            "request_time": self.request_time,
            "applied_time": self.applied_time,
            "end_time": self.end_time,
            "nodes_before": self.nodes_before,
            "nodes_after": self.nodes_after,
            "nodes_added": self.nodes_added,
            "nodes_removed": self.nodes_removed,
            "success": self.success,
            "trigger": self.trigger,
        }


@dataclass(frozen=True)
class JobSpec:
    id: str
    min_nodes: int
    max_nodes: int
    walltime: str
    command: str
    type: Optional[str] = None
    default_nodes: Optional[int] = None  # optional, defaults to min_nodes if not provided
    serial_fraction: Optional[float] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "JobSpec":
        return JobSpec(
            id=str(d["id"]),
            min_nodes=int(d["min_nodes"]),
            max_nodes=int(d["max_nodes"]),
            walltime=str(d["walltime"]),
            command=str(d["job_command"]),
            type=d.get("type"),
            default_nodes=int(d.get("default_nodes", d["min_nodes"])),
            serial_fraction=float(d["serial_fraction"]) if d.get("serial_fraction") is not None else None
        )


@dataclass
class JobRuntime:
    nodes: List[str] = field(default_factory=list)
    start_time: Optional[float] = None
    elastic_events: int = 0
    last_elastic_time: Optional[float] = None
    arrival_time: Optional[float] = None
    completion_time: Optional[float] = None

    # NEW: Structured elastic phase tracking
    elastic_phases: List[ElasticPhase] = field(default_factory=list)
    _current_phase: Optional[ElasticPhase] = field(default=None, repr=False)
    
    def start_phase(self, operation: str, trigger: str = "scheduler", removed: Optional[List[str]] = None) -> ElasticPhase:
        """Begin tracking a new elastic phase."""
        # End the previous phase if one exists
        if self._current_phase is not None:
            self._current_phase.mark_ended()
            self.elastic_phases.append(self._current_phase)
        
        # Calculate nodes_before based on operation type
        # For initial: no nodes yet
        if operation == "shrink" and removed is not None:
            nodes_before = len(self.nodes) + len(removed)
        else:
            nodes_before = len(self.nodes)
        
        phase = ElasticPhase(
            phase_id=len(self.elastic_phases) + 1,
            operation=operation,
            request_time=time.time(),
            nodes_before=nodes_before,
            trigger=trigger,
        )
        self._current_phase = phase
        return phase
    
    def mark_phase_applied(self, success: bool = True) -> Optional[ElasticPhase]:
        """Mark the current phase as applied (scaling operation complete)."""
        if self._current_phase is None:
            return None
        
        self._current_phase.nodes_after = len(self.nodes)
        self._current_phase.mark_applied(success)
        return self._current_phase
    
    def finalize_phases(self) -> None:
        """Finalize all phases when job completes."""
        if self._current_phase is not None:
            self._current_phase.mark_ended()
            self.elastic_phases.append(self._current_phase)
            self._current_phase = None
    
    def get_phase_summary(self) -> Dict[str, Any]:
        """Get summary statistics for all phases."""
        if not self.elastic_phases:
            return {"total_phases": 0}
        
        expand_phases = [p for p in self.elastic_phases if p.operation == "expand"]
        shrink_phases = [p for p in self.elastic_phases if p.operation == "shrink"]
        
        # Calculate scaling times (time to apply operation)
        expand_scaling_times = [p.scaling_duration for p in expand_phases if p.scaling_duration is not None]
        shrink_scaling_times = [p.scaling_duration for p in shrink_phases if p.scaling_duration is not None]
        
        # Calculate phase durations (time running at that node count)
        expand_phase_durations = [p.phase_duration for p in expand_phases if p.phase_duration is not None]
        shrink_phase_durations = [p.phase_duration for p in shrink_phases if p.phase_duration is not None]
        
        return {
            "total_phases": len(self.elastic_phases),
            "expand_count": len(expand_phases),
            "shrink_count": len(shrink_phases),
            # Scaling operation times
            "total_expand_scaling_time": sum(expand_scaling_times),
            "total_shrink_scaling_time": sum(shrink_scaling_times),
            "avg_expand_scaling_time": (sum(expand_scaling_times) / len(expand_scaling_times)) if expand_scaling_times else 0,
            "avg_shrink_scaling_time": (sum(shrink_scaling_times) / len(shrink_scaling_times)) if shrink_scaling_times else 0,
            # Phase durations (time running at node count)
            "total_expand_phase_duration": sum(expand_phase_durations),
            "total_shrink_phase_duration": sum(shrink_phase_durations),
            "avg_expand_phase_duration": (sum(expand_phase_durations) / len(expand_phase_durations)) if expand_phase_durations else 0,
            "avg_shrink_phase_duration": (sum(shrink_phase_durations) / len(shrink_phase_durations)) if shrink_phase_durations else 0,
            "success_rate": sum(1 for p in self.elastic_phases if p.success) / len(self.elastic_phases),
        }


@dataclass
class JobRecord:
    spec: JobSpec
    status: JobStatus = JobStatus.PENDING
    runtime: JobRuntime = field(default_factory=JobRuntime)

    # convenience properties
    @property
    def id(self) -> str: return self.spec.id
    @property
    def nodes(self) -> List[str]: return self.runtime.nodes
    @property
    def min_nodes(self) -> int: return self.spec.min_nodes
    @property
    def max_nodes(self) -> int: return self.spec.max_nodes
    @property
    def command(self) -> str: return self.spec.command
    @property
    def walltime(self) -> str: return self.spec.walltime
    @property
    def type(self) -> Optional[str]: return self.spec.type

    @classmethod
    def from_spec(cls, spec: JobSpec) -> "JobRecord":
        return cls(spec=spec)

    def mark_queued(self) -> None:
        self.runtime.arrival_time = time.time()
        self.status = JobStatus.QUEUED

    def start(self, nodes: List[str]) -> None:
        self.runtime.nodes = list(nodes)
        self.runtime.start_time = time.time()
        self.status = JobStatus.RUNNING

        # Record initial allocation as first phase
        phase = self.runtime.start_phase("initial", trigger="scheduler")
        phase.nodes_added = list(nodes)
        phase.nodes_after = len(nodes)
        phase.mark_applied(success=True)  # Initial allocation is immediately applied

    def record_expand(self, added: List[str], trigger: str = "scheduler") -> ElasticPhase:
        if not added: return

        # Start phase tracking
        phase = self.runtime.start_phase("expand", trigger=trigger)
        phase.nodes_added = list(added)

        self.runtime.nodes.extend(added)
        self.runtime.elastic_events += 1
        self.runtime.last_elastic_time = time.time()
        return phase

    def complete_expand(self, success: bool = True) -> Optional[ElasticPhase]:
        """Complete the current expand phase."""
        return self.runtime.mark_phase_applied(success)
    
    def record_shrink(self, removed: List[str], trigger: str = "scheduler") -> ElasticPhase:
        """Start a shrink operation (returns phase, call complete_shrink when done)."""
        if not removed: return
        # Start phase tracking
        phase = self.runtime.start_phase("shrink", trigger=trigger, removed=removed)
        phase.nodes_removed = list(removed)

        for n in removed:
            try:
                self.runtime.nodes.remove(n)
            except ValueError:
                pass
        self.runtime.elastic_events += 1
        self.runtime.last_elastic_time = time.time()
        
        return phase

    def complete_shrink(self, success: bool = True) -> Optional[ElasticPhase]:
        """Complete the current shrink phase."""
        return self.runtime.mark_phase_applied(success)

    def complete(self, success: bool) -> None:
        self.runtime.completion_time = time.time()
        self.runtime.finalize_phases()  # End the last phase
        self.status = JobStatus.COMPLETED if success else JobStatus.FAILED

    def get_elastic_analysis(self) -> Dict[str, Any]:
        """Get comprehensive elastic scaling analysis data."""
        return {
            "job_id": self.id,
            "total_runtime": (self.runtime.completion_time - self.runtime.start_time) if self.runtime.completion_time else None,
            "wait_time": (self.runtime.start_time - self.runtime.arrival_time) if self.runtime.start_time and self.runtime.arrival_time else None,
            "initial_nodes": self.runtime.elastic_phases[0].nodes_after if self.runtime.elastic_phases else len(self.runtime.nodes),
            "final_nodes": len(self.runtime.nodes),
            "min_nodes": self.min_nodes,
            "max_nodes": self.max_nodes,
            "phase_summary": self.runtime.get_phase_summary(),
            "phases": [p.to_dict() for p in self.runtime.elastic_phases],
        }

class JobRequest:
    job_requests_counter = 1
    def __init__(self, job_id, scale, num_nodes, status):
        self.id = JobRequest.job_requests_counter
        self.job_id = job_id
        self.scale = scale
        self.num_nodes = num_nodes
        self.status = status
        JobRequest.job_requests_counter += 1

    @staticmethod
    def from_dict(job_dict):
        """Create a Job object from a dictionary."""
        return JobRequest(
            job_id=job_dict["job_id"],
            scale=job_dict["scale"],
            num_nodes=job_dict["num_nodes"],
            status=job_dict["status"]
        )