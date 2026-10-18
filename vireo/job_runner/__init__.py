"""Collaborators of ``jobs.JobRunner``, each owning one slice of its state.

``jobs.JobRunner`` is the coordinator and the public API; nothing outside
``jobs.py`` (and tests) should construct these. The modules, by
responsibility:

- ``workers``: the worker threads and the shutting-down flag.
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
one ``RLock`` (``JobRunner._lock``); none adds a lock of its own except
``JobCheckpointer.write_lock``, which is taken before the runner lock.
"""
