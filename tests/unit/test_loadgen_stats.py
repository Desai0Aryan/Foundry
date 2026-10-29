import pytest

from foundry.tools.loadgen import percentile, summarize


def test_percentile_interpolates_like_numpy():
    xs = [1, 2, 3, 4, 5]
    assert percentile(xs, 0) == 1 and percentile(xs, 100) == 5
    assert percentile(xs, 50) == 3
    assert percentile(xs, 25) == 2
    assert percentile([10, 20], 50) == pytest.approx(15)
    assert percentile(list(range(1, 101)), 95) == pytest.approx(95.05)


def test_percentile_handles_edges_and_unsorted_input():
    assert percentile([], 95) == 0.0
    assert percentile([7], 99) == 7
    assert percentile([5, 1, 3], 50) == 3


def test_summarize():
    s = summarize([1.0, 2.0, 3.0, 4.0])
    assert (s["count"], s["min"], s["max"], s["mean"]) == (4, 1.0, 4.0, 2.5)
    assert summarize([]) == {
        "count": 0,
        "min": 0.0,
        "p50": 0.0,
        "p95": 0.0,
        "p99": 0.0,
        "max": 0.0,
        "mean": 0.0,
    }
