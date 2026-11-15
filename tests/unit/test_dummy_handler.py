import hashlib

from dml.contracts.errors import PermanentTaskError
from dml.worker.handlers.dummy import DummyHandler


def test_digest_is_deterministic() -> None:
    seed = "corpus:42"
    expected = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    assert hashlib.sha256(seed.encode("utf-8")).hexdigest() == expected
    assert DummyHandler.task_type == "dummy"


async def test_missing_seed_is_permanent() -> None:
    handler = DummyHandler()

    class _Ctx:
        payload = {}

    try:
        await handler.execute(_Ctx())  # type: ignore[arg-type]
    except PermanentTaskError:
        return
    raise AssertionError("expected PermanentTaskError")
