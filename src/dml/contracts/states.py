"""Job / task / worker / attempt state machines."""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"


class WorkerStatus(StrEnum):
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    DRAINING = "draining"
    UNHEALTHY = "unhealthy"
    STOPPED = "stopped"


TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.CANCELLED}
    ),
    TaskStatus.QUEUED: frozenset(
        {TaskStatus.RUNNING, TaskStatus.PENDING, TaskStatus.CANCELLED}
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.PENDING,
            TaskStatus.DEAD_LETTER,
        }
    ),
    TaskStatus.FAILED: frozenset(
        {
            TaskStatus.PENDING,
            TaskStatus.QUEUED,
            TaskStatus.DEAD_LETTER,
            TaskStatus.CANCELLED,
            TaskStatus.RUNNING,
        }
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.DEAD_LETTER: frozenset({TaskStatus.PENDING}),
}

TERMINAL_TASK_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.CANCELLED, TaskStatus.DEAD_LETTER}
)

IN_FLIGHT_TASK_STATUSES = frozenset(
    {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.FAILED}
)


class InvalidStateTransition(ValueError):
    pass


def assert_task_transition(current: TaskStatus, target: TaskStatus) -> None:
    if current == target:
        return
    allowed = TASK_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise InvalidStateTransition(f"task cannot move {current} -> {target}")


def backoff_seconds(
    failed_attempt_number: int,
    *,
    base: float = 1.0,
    cap: float = 16.0,
    jitter: float = 0.2,
    rng: object | None = None,
) -> float:
    """Exponential backoff after a failed attempt.

    failed_attempt_number is the attempt that just failed (1-based).
    Delay sequence without jitter: 1, 2, 4, 8, 16, 16, ...
    """
    import random

    exponent = max(failed_attempt_number - 1, 0)
    delay = min(cap, base * (2**exponent))
    if jitter <= 0:
        return delay
    generator = rng if rng is not None else random
    spread = delay * jitter
    return max(0.0, delay + generator.uniform(-spread, spread))  # type: ignore[union-attr]
