"""Queue DTOs. Redis stores a pointer; PostgreSQL stores the payload."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class TaskMessage:
    task_id: UUID
    job_id: UUID
    task_type: str
    resource_class: str
    priority: int = 0

    def to_fields(self) -> dict[str, str]:
        return {
            "task_id": str(self.task_id),
            "job_id": str(self.job_id),
            "task_type": self.task_type,
            "resource_class": self.resource_class,
            "priority": str(self.priority),
        }

    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> TaskMessage:
        return cls(
            task_id=UUID(fields["task_id"]),
            job_id=UUID(fields["job_id"]),
            task_type=fields["task_type"],
            resource_class=fields["resource_class"],
            priority=int(fields.get("priority", "0")),
        )


@dataclass(frozen=True, slots=True)
class ClaimedTask:
    stream: str
    message_id: str
    message: TaskMessage
