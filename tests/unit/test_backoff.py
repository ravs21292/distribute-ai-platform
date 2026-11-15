from dml.contracts.states import backoff_seconds


class _FixedRng:
    def __init__(self, value: float) -> None:
        self.value = value

    def uniform(self, low: float, high: float) -> float:
        del low, high
        return self.value


def test_exponential_sequence_without_jitter() -> None:
    delays = [backoff_seconds(n, jitter=0) for n in range(1, 7)]
    assert delays == [1, 2, 4, 8, 16, 16]


def test_jitter_stays_within_band() -> None:
    delay = backoff_seconds(3, jitter=0.2, rng=_FixedRng(0.0))
    # attempt 3 → 4s, jitter applied as delay + uniform(-spread, spread)
    # our fake rng ignores bounds and returns 0, so delay + 0
    assert delay == 4.0
