"""Stable progress and failure shapes shared by long-running workflows."""


def progress_event(phase, current, total, current_file="", **extra):
    event = {
        "phase": phase,
        "current": current,
        "total": total,
        "current_file": current_file,
    }
    event.update(extra)
    return event


def failure_event(error, *, phase=None, retryable=False):
    return {
        "message": str(error),
        "phase": phase,
        "retryable": bool(retryable),
    }
