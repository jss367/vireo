"""Shared polling helpers for tests that wait on background-job completion.

Many tests spawn a job (scan, classify, import, ...) and need to block until
it reaches a terminal state. The historical pattern was to inline a poll
loop:

    for _ in range(50):
        if client.get(f'/api/jobs/{job_id}').get_json()['status'] in (
            'completed', 'failed'):
            break
        time.sleep(0.1)

That gives a 5s budget, which is fine on an unloaded developer machine but
flakes under `pytest -n auto` where xdist workers contend for I/O. These
helpers replace that pattern with a generous default timeout (30s) and a
clear failure message that surfaces the last observed job state, so a real
hang is diagnosable instead of just a silent assertion failure on
"completed" later in the test.
"""

import time

import pytest

_DEFAULT_TERMINAL = ("completed", "failed", "cancelled")


def wait_for_job(fetch, *, timeout=30.0, poll=0.05,
                 terminal=_DEFAULT_TERMINAL, description=None):
    """Block until ``fetch()`` returns a job dict with a terminal ``status``.

    Args:
        fetch: zero-arg callable returning a job dict (or ``None`` if the
            job is not yet visible). Typically wraps a Flask client GET or a
            direct ``JobRunner.get`` call.
        timeout: max seconds to poll. Default 30s — generous enough for
            xdist parallel runs where workers contend for I/O.
        poll: sleep between polls.
        terminal: iterable of terminal status strings.
        description: human-readable label for the timeout message.

    Returns:
        The terminal job dict.

    Raises:
        pytest.fail.Exception when the job does not reach a terminal state
        before ``timeout``. The message includes the last observed state so
        a real hang is diagnosable.
    """
    deadline = time.monotonic() + timeout
    last = None
    while True:
        last = fetch()
        if last is not None and last.get("status") in terminal:
            return last
        if time.monotonic() >= deadline:
            label = description or "job"
            pytest.fail(
                f"{label} did not reach a terminal state within {timeout}s; "
                f"last={last!r}"
            )
        time.sleep(poll)


def wait_for_job_via_client(client, job_id, *, wait_for_history=False, **kwargs):
    """Wait for a job to terminate by polling the HTTP /api/jobs/<id> route.

    JobRunner sets ``job["status"]`` to a terminal value before persisting to
    the ``job_history`` table. Callers that immediately read history-backed
    endpoints (e.g. ``/api/duplicates/last-scan``, ``/api/jobs/history``)
    must pass ``wait_for_history=True`` to also block until the worker
    thread has flushed the row to SQLite — otherwise the next query may
    return stale or empty data.
    """
    def _fetch():
        return client.get(f"/api/jobs/{job_id}").get_json()
    job = wait_for_job(_fetch, description=f"job {job_id}", **kwargs)
    if wait_for_history:
        def _persisted():
            data = client.get(f"/api/jobs/{job_id}").get_json()
            return data if data and data.get("_persisted") else None
        job = wait_for_job(_persisted,
                           description=f"job {job_id} persistence",
                           **kwargs)
    return job


def wait_for_job_via_runner(runner, job_id, *, wait_for_history=False, **kwargs):
    """Wait for a job to terminate by polling JobRunner.get directly.

    See :func:`wait_for_job_via_client` for the meaning of ``wait_for_history``.
    """
    def _fetch():
        return runner.get(job_id)
    job = wait_for_job(_fetch, description=f"job {job_id}", **kwargs)
    if wait_for_history:
        def _persisted():
            data = runner.get(job_id)
            return data if data and data.get("_persisted") else None
        job = wait_for_job(_persisted,
                           description=f"job {job_id} persistence",
                           **kwargs)
    return job
