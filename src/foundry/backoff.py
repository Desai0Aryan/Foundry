"""Exponential backoff with jitter."""

from __future__ import annotations

import random
from collections.abc import Callable


def backoff_delay(
    attempt: int,
    base: float,
    cap: float,
    rng: Callable[[], float] = random.random,
) -> float:
    """Delay before retrying after the given (1-based) failed attempt.

    "Equal jitter": the ceiling doubles each attempt (base, 2*base, 4*base ... up to cap),
    and the actual delay is drawn uniformly from [ceiling/2, ceiling]. The floor keeps
    delays growing (no near-zero retry storms); the randomness stops a fleet of jobs that
    failed together from retrying in lock-step (thundering herd).
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    ceiling = min(cap, base * (2 ** min(attempt - 1, 30)))
    return ceiling / 2 + rng() * ceiling / 2
