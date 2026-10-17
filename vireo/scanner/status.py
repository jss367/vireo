"""Calling a scan's status callback with or without the phase keywords."""

import inspect


def _status_callback_supports_phase(status_callback):
    """Return whether a callback binds the scanner's phase keyword fields."""
    if status_callback is None:
        return False
    try:
        signature = inspect.signature(status_callback)
    except (TypeError, ValueError):
        # Opaque extension callables cannot be checked without invoking them.
        # Prefer the current rich contract; any callback error must propagate
        # rather than being mistaken for a legacy signature and retried.
        return True
    try:
        signature.bind(
            "",
            phase_current=0,
            phase_total=1,
            phase_label="phase",
        )
    except TypeError:
        return False
    return True


def _call_status_callback(
    status_callback, message, *, phase_current=None, phase_total=None,
    phase_label=None, supports_phase=None,
):
    """Invoke a status callback once using its prevalidated argument shape."""
    has_phase = (
        phase_current is not None
        or phase_total is not None
        or phase_label is not None
    )
    if not has_phase:
        status_callback(message)
        return
    if supports_phase is None:
        supports_phase = _status_callback_supports_phase(status_callback)
    if supports_phase:
        status_callback(
            message,
            phase_current=phase_current,
            phase_total=phase_total,
            phase_label=phase_label,
        )
    else:
        status_callback(message)
