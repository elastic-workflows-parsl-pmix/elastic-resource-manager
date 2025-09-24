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


@dataclass(frozen=True)
class JobSpec:
    id: str
    min_nodes: int
    max_nodes: int
    walltime: str
    command: str
    type: Optional[str] = None
    default_nodes: Optional[int] = None  # optional, defaults to min_nodes if not provided

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
        )


@dataclass
class JobRuntime:
    nodes: List[str] = field(default_factory=list)
    start_time: Optional[float] = None
    elastic_events: int = 0
    last_elastic_time: Optional[float] = None
    arrival_time: Optional[float] = None
    completion_time: Optional[float] = None


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

    def record_expand(self, added: List[str]) -> None:
        if not added: return
        self.runtime.nodes.extend(added)
        self.runtime.elastic_events += 1
        self.runtime.last_elastic_time = time.time()

    def record_shrink(self, removed: List[str]) -> None:
        if not removed: return
        for n in removed:
            try:
                self.runtime.nodes.remove(n)
            except ValueError:
                pass
        self.runtime.elastic_events += 1
        self.runtime.last_elastic_time = time.time()

    def complete(self, success: bool) -> None:
        self.runtime.completion_time = time.time()
        self.status = JobStatus.COMPLETED if success else JobStatus.FAILED