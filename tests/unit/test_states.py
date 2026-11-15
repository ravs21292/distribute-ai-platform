from dml.contracts.states import InvalidStateTransition, TaskStatus, assert_task_transition


def test_allowed_happy_path() -> None:
    assert_task_transition(TaskStatus.PENDING, TaskStatus.QUEUED)
    assert_task_transition(TaskStatus.QUEUED, TaskStatus.RUNNING)
    assert_task_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)


def test_retry_path() -> None:
    assert_task_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
    assert_task_transition(TaskStatus.FAILED, TaskStatus.PENDING)
    assert_task_transition(TaskStatus.RUNNING, TaskStatus.DEAD_LETTER)


def test_manual_retry_from_dlq() -> None:
    assert_task_transition(TaskStatus.DEAD_LETTER, TaskStatus.PENDING)


def test_cannot_revive_success() -> None:
    try:
        assert_task_transition(TaskStatus.SUCCEEDED, TaskStatus.PENDING)
    except InvalidStateTransition:
        return
    raise AssertionError("succeeded must be terminal")


def test_idempotent_same_status() -> None:
    assert_task_transition(TaskStatus.QUEUED, TaskStatus.QUEUED)
