import pytest

from foundry.domain import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    TRANSITIONS,
    JobStatus,
    can_transition,
    canonical_json,
    sha256_hex,
)


def test_every_status_has_a_transition_entry():
    assert set(TRANSITIONS) == set(JobStatus)


def test_active_and_terminal_partition_all_statuses():
    assert set(JobStatus) == ACTIVE_STATUSES | TERMINAL_STATUSES
    assert not ACTIVE_STATUSES & TERMINAL_STATUSES


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED),
        (JobStatus.RUNNING, JobStatus.DEAD),
        (JobStatus.RETRY_SCHEDULED, JobStatus.QUEUED),
        (JobStatus.DEAD, JobStatus.QUEUED),  # DLQ replay
        (JobStatus.RUNNING, JobStatus.QUEUED),  # graceful worker shutdown hands the job back
    ],
)
def test_legal_transitions(src, dst):
    assert can_transition(src, dst)


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (JobStatus.SUCCEEDED, JobStatus.RUNNING),
        (JobStatus.SUCCEEDED, JobStatus.QUEUED),
        (JobStatus.CANCELLED, JobStatus.QUEUED),
        (JobStatus.QUEUED, JobStatus.SUCCEEDED),  # must run first
        (JobStatus.QUEUED, JobStatus.DEAD),
    ],
)
def test_illegal_transitions(src, dst):
    assert not can_transition(src, dst)


def test_canonical_json_ignores_key_order():
    assert canonical_json({"a": 1, "b": {"y": 2, "x": 3}}) == canonical_json(
        {"b": {"x": 3, "y": 2}, "a": 1}
    )
    assert sha256_hex(canonical_json({"a": 1})) != sha256_hex(canonical_json({"a": 2}))
