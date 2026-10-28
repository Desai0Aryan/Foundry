"""Exceptions job handlers raise to steer the retry policy."""

from __future__ import annotations


class PermanentError(Exception):
    """Retrying cannot help (bad input, corrupt file). Goes straight to the dead-letter queue."""


class TransientError(Exception):
    """A temporary failure (network blip, overloaded dependency). Retried with backoff."""
