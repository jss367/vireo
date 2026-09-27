"""Processing stages used by :mod:`pipeline_job`.

The entry module owns scheduling, pause participation, and the regroup lock.
Stage functions own their work and terminal status. They receive a per-run
context and explicit queues, events, outputs, and helper dependencies; no
stage imports the orchestrator or looks up its globals.
"""
