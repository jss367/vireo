"""Shared launch prologue for background-job routes.

Every route that starts a background job used to repeat the same prologue
by hand: grab the runner off the app, capture the active workspace id from
the request database, open a worker-thread ``Database`` bound to that
workspace inside the work closure, call ``runner.start(...)`` with the
workspace id, and return ``{"job_id": ...}``. ``background_job`` performs
that prologue once per request and hands the route a :class:`JobLaunch`::

    @app.route("/api/jobs/thumbnails", methods=["POST"])
    @background_job
    def api_job_thumbnails(ctx):
        def work(job):
            thread_db = ctx.thread_db()
            ...
        return ctx.start("thumbnails", work)

``@background_job`` must sit *inside* ``@app.route`` (closest to the
function) so Flask registers the wrapped view. Routes keep full control of
request validation: returning any Flask response before calling
``ctx.start`` short-circuits without starting a job.
"""

import functools

from flask import jsonify


class JobCancelled(Exception):
    """Exit a worker at a safe checkpoint after cancellation."""


class JobLaunch:
    """Per-request context for launching one background job.

    ``runner`` is the app's :class:`jobs.JobRunner`; ``workspace_id`` is the
    workspace that was active when the request arrived. Both are captured on
    the request thread so worker closures never touch Flask's ``g``.
    """

    __slots__ = ("runner", "workspace_id", "db_path", "_db_factory")

    def __init__(self, runner, workspace_id, db_path, db_factory):
        self.runner = runner
        self.workspace_id = workspace_id
        self.db_path = db_path
        self._db_factory = db_factory

    def thread_db(self):
        """Open a worker-thread database bound to the request's workspace.

        Call this *inside* the work closure: SQLite connections are not
        shared across threads, and the request connection in ``g`` is closed
        when the request ends.
        """
        db = self._db_factory(self.db_path)
        if self.workspace_id is not None:
            db.set_active_workspace(self.workspace_id)
        return db

    def checkpoint(self, job):
        """Pause or exit between completed units of work.

        Call only after releasing transactions, shared locks, and active
        subprocesses. The runner records the cancellation when this unwinds.
        """
        if self.runner.is_cancelled(job["id"]):
            raise JobCancelled("Job cancelled")

    def start_job(self, job_type, work, config=None, **kwargs):
        """Register ``work`` with the runner and return the new job id.

        ``workspace_id`` defaults to the request's active workspace; any
        other keyword is forwarded to :meth:`jobs.JobRunner.start` unchanged
        (``pausable``, ``runtime_warning``, ``ephemeral``, ...).
        """
        kwargs.setdefault("workspace_id", self.workspace_id)
        return self.runner.start(job_type, work, config=config, **kwargs)

    def start(self, job_type, work, config=None, *, extra=None, **kwargs):
        """Start the job and return the standard ``{"job_id": ...}`` response.

        ``extra`` adds keys alongside ``job_id`` for routes whose clients
        expect more than the id (for example a ``total`` for progress UIs).
        """
        job_id = self.start_job(job_type, work, config=config, **kwargs)
        payload = {"job_id": job_id}
        if extra:
            payload.update(extra)
        return jsonify(payload)


def make_background_job(get_runner, get_db, db_path, db_factory):
    """Build the ``@background_job`` decorator for one application.

    ``get_runner`` and ``get_db`` are called per request (the runner is
    looked up lazily so tests may swap ``app._job_runner``). ``db_factory``
    is the class used for worker-thread connections, normally ``Database``.
    """

    def background_job(view):
        @functools.wraps(view)
        def wrapper(*args, **kwargs):
            ctx = JobLaunch(
                get_runner(),
                get_db().active_workspace_id,
                db_path,
                db_factory,
            )
            return view(ctx, *args, **kwargs)

        return wrapper

    return background_job
