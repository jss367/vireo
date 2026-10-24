"""JSON-safety check for the job dicts ``JobRunner`` publishes.

``GET /api/jobs`` and ``GET /api/jobs/<id>`` jsonify the runner's snapshots
of its job dicts, so one value JSON cannot encode (a ``set`` of photo ids a
worker parked on its job, say) answers every poll with a 500 for as long as
the job stays in memory. The autouse ``_shutdown_created_job_runners``
fixture in ``conftest.py`` runs every snapshot a test takes, and every job
left in a runner at teardown, through ``record_unsafe_snapshot`` and fails
the test on any entry in ``VIOLATIONS``.
"""

import json

# What the running test's snapshots could not encode, one line per job.
# A test that provokes a violation on purpose clears this before it ends.
VIOLATIONS = []


def unsafe_paths(value, path="job"):
    """Paths under ``value`` that ``json.dumps`` cannot encode.

    Keys are sorted, as Flask's ``jsonify`` sorts them, so a dict mixing
    ``int`` and ``str`` keys is reported too.
    """
    try:
        json.dumps(value, sort_keys=True)
        return []
    except (TypeError, ValueError):
        pass
    if isinstance(value, dict):
        paths = [
            found
            for key, item in value.items()
            for found in unsafe_paths(item, f"{path}[{key!r}]")
        ]
        return paths or [f"{path} (keys)"]
    if isinstance(value, (list, tuple)):
        paths = [
            found
            for index, item in enumerate(value)
            for found in unsafe_paths(item, f"{path}[{index}]")
        ]
        return paths or [path]
    return [f"{path} ({type(value).__name__})"]


def record_unsafe_snapshot(snap):
    """Add a line to ``VIOLATIONS`` when ``snap`` does not encode."""
    paths = unsafe_paths(snap)
    if paths:
        VIOLATIONS.append(
            f"{snap.get('type')} job {snap.get('id')}: " + ", ".join(paths)
        )
