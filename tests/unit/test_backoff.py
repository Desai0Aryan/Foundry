import pytest

from foundry.backoff import backoff_delay


def test_ceiling_doubles_and_delay_stays_in_upper_half():
    for attempt, ceiling in [(1, 2), (2, 4), (3, 8), (4, 16)]:
        assert backoff_delay(attempt, 2, 1000, rng=lambda: 0.0) == pytest.approx(ceiling / 2)
        assert backoff_delay(attempt, 2, 1000, rng=lambda: 1.0) == pytest.approx(ceiling)


def test_cap_is_respected_even_for_huge_attempt_counts():
    assert backoff_delay(500, 2, 60, rng=lambda: 1.0) == 60
    assert backoff_delay(500, 2, 60, rng=lambda: 0.0) == 30


def test_delays_are_jittered():
    assert len({backoff_delay(5, 1, 100) for _ in range(50)}) > 40


def test_random_delay_never_leaves_bounds():
    for attempt in range(1, 12):
        d = backoff_delay(attempt, 1.5, 40)
        ceiling = min(40, 1.5 * 2 ** (attempt - 1))
        assert ceiling / 2 <= d <= ceiling


def test_attempt_is_one_based():
    with pytest.raises(ValueError):
        backoff_delay(0, 1, 10)
