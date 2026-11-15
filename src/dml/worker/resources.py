"""Hardware advertisement for worker registration."""

from __future__ import annotations

import os
import socket
import subprocess
from dataclasses import dataclass

import psutil


@dataclass(frozen=True, slots=True)
class DetectedResources:
    hostname: str
    process_id: int
    cpu_count: int
    memory_mb: int
    gpu_count: int
    gpu_model: str | None
    gpu_memory_mb: int
    device: str


def detect_resources(*, device: str = "cpu") -> DetectedResources:
    gpu_count, gpu_model, gpu_memory_mb = _read_nvidia()
    chosen = device
    if device == "auto":
        chosen = "cuda" if gpu_count > 0 else "cpu"
    if chosen == "cpu":
        # Advertise no GPU even if the box has one when the process opted into CPU.
        if device == "cpu":
            gpu_count, gpu_model, gpu_memory_mb = 0, None, 0
    return DetectedResources(
        hostname=socket.gethostname(),
        process_id=os.getpid(),
        cpu_count=os.cpu_count() or 1,
        memory_mb=int(psutil.virtual_memory().total / (1024 * 1024)),
        gpu_count=gpu_count,
        gpu_model=gpu_model,
        gpu_memory_mb=gpu_memory_mb,
        device=chosen,
    )


def _read_nvidia() -> tuple[int, str | None, int]:
    try:
        completed = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total",
                "--format=csv,noheader,nounits",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return 0, None, 0
    if completed.returncode != 0 or not completed.stdout.strip():
        return 0, None, 0
    lines = [ln.strip() for ln in completed.stdout.splitlines() if ln.strip()]
    if not lines:
        return 0, None, 0
    name, _, mem = lines[0].partition(",")
    try:
        memory_mb = int(float(mem.strip()))
    except ValueError:
        memory_mb = 0
    return len(lines), name.strip() or None, memory_mb
