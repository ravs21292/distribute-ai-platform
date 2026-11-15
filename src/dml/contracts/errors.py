"""Typed task failures. The worker boundary maps unexpected exceptions to retryable."""

from __future__ import annotations


class TaskError(Exception):
    retryable: bool = True

    def __init__(self, message: str, *, error_type: str | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type or self.__class__.__name__


class RetryableTaskError(TaskError):
    retryable = True


class PermanentTaskError(TaskError):
    retryable = False


def classify_exception(exc: BaseException) -> tuple[bool, str]:
    """Return (retryable, error_type)."""
    if isinstance(exc, TaskError):
        return exc.retryable, exc.error_type
    name = exc.__class__.__name__
    text = str(exc).lower()
    transient_names = (
        "TimeoutError",
        "OperationalError",
        "InterfaceError",
        "ConnectionError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "TemporaryError",
        "BusyLoadingError",
        "ReadTimeout",
        "ConnectTimeout",
    )
    if name in transient_names:
        return True, name
    if "too many requests" in text or "429" in text:
        return True, "RateLimited"
    if "connection" in text or "timeout" in text or "temporarily" in text:
        return True, name
    # Unknown faults are retryable so a bug deploy does not immediately DLQ every task.
    # PermanentTaskError is the explicit "do not retry" path.
    return True, name
