"""Collaborators of ``jobs.JobRunner``, each owning one slice of its state.

``jobs.JobRunner`` is the coordinator and the public API; nothing outside
``jobs.py`` (and tests) should construct these. It keeps the job registry
(``_jobs``), the one runner lock, id allocation, registration, the run loop,
and the steps that cross collaborators (pipeline promotion and queued
cancellation). The modules, by responsibility:

- ``workers``: the worker threads and the shutting-down flag.
- ``events``: each job's buffered events and SSE subscriber queues.
- ``steps``: writes to a job's step tree.
- ``control``: the cancel, pause and uncancellable flags, the waits
  workers park in, and the terminal status a returned or raised work
  function settles to.
- ``pipeline_queue``: queued pipeline contexts, in-flight enqueue
  admissions, the slot cap and the promotion retry.
- ``reservations``: synchronous workspace reservations and transfer
  admission, plus the busy-message helpers.
- ``history``: every ``job_history`` read and write (startup sweep, running
  and queued rows, checkpoint writes, final rows), each on its own SQLite
  connection.
- ``checkpoints``: the periodic checkpoint thread and the write lock that
  serializes snapshot-and-write.
- ``outcome``: the final-status rollup for a returned work function, the
  error count, and the one-line summary.
- ``views``: a fresh job dict, the snapshot copies callers get, and the
  job-shaped view of a queued pipeline.

Every collaborator that touches in-memory runner state shares the runner's
one ``RLock`` (``JobRunner._lock``, with ``_pause_condition`` built on it);
none adds a lock of its own except ``JobCheckpointer.write_lock``, which is
taken before the runner lock.
"""
