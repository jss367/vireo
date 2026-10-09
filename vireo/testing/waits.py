"""Bounded synchronization budgets shared by both Python test suites."""

import os
import sys


def synchronization_timeout(seconds: float = 5.0) -> float:
    """Allow slow Windows CI workers to reach a checkpoint before failing.

    This is a maximum wait, not a sleep: a signalled event or finished thread
    still returns immediately. Keep subsecond probes and explicit zero
    timeouts unchanged so tests of nonblocking behavior retain their meaning.
    Read the environment on each call so tests can exercise both policies.
    """
    if sys.platform == "win32" and os.environ.get("CI", "").lower() == "true" and seconds >= 1:
        return max(30.0, seconds * 3)
    return seconds
