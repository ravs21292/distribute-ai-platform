"""Deterministic resource matching. Not a Kubernetes scheduler."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResourceRequirements:
    cpu: int = 1
    memory_mb: int = 256
    gpu: int = 0
    gpu_memory_mb: int = 0
    gpu_preferred: bool = False

    @property
    def gpu_required(self) -> bool:
        return self.gpu > 0

    @property
    def resource_class(self) -> str:
        return "gpu" if self.gpu_required else "cpu"


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    worker_id: str
    capabilities: frozenset[str]
    cpu_count: int
    memory_mb: int
    gpu_count: int
    gpu_memory_mb: int
    healthy: bool = True


def is_eligible(worker: WorkerSnapshot, task_type: str, req: ResourceRequirements) -> bool:
    if not worker.healthy:
        return False
    if task_type not in worker.capabilities:
        return False
    if worker.cpu_count < req.cpu:
        return False
    if worker.memory_mb < req.memory_mb:
        return False
    if worker.gpu_count < req.gpu:
        return False
    if req.gpu > 0 and worker.gpu_memory_mb < req.gpu_memory_mb:
        return False
    return True


def publish_resource_class(
    req: ResourceRequirements, *, healthy_gpu_worker_exists: bool
) -> str:
    """Which stream class the scheduler should XADD to."""
    if req.gpu_required:
        return "gpu"
    if req.gpu_preferred and healthy_gpu_worker_exists:
        return "gpu"
    return "cpu"


def preference_sort_key(worker: WorkerSnapshot, req: ResourceRequirements) -> tuple[int, int]:
    """Lower is better. Used when explaining assignment; pull-based delivery still applies."""
    if req.gpu_preferred and not req.gpu_required:
        gpu_rank = 0 if worker.gpu_count > 0 else 1
    elif not req.gpu_required:
        gpu_rank = 0 if worker.gpu_count == 0 else 1
    else:
        gpu_rank = 0
    return (gpu_rank, -worker.gpu_memory_mb)


def worker_streams(
    capabilities: list[str],
    *,
    gpu_count: int,
    prefix: str = "dml.tasks",
) -> list[str]:
    """GPU workers read gpu streams first, then cpu overflow."""
    classes = ("gpu", "cpu") if gpu_count > 0 else ("cpu",)
    streams: list[str] = []
    for resource_class in classes:
        for cap in capabilities:
            streams.append(f"{prefix}.{cap}.{resource_class}")
    return streams


def stream_name(task_type: str, resource_class: str, *, prefix: str = "dml.tasks") -> str:
    return f"{prefix}.{task_type}.{resource_class}"
