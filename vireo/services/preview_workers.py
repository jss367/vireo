"""Bounded, latest-request-wins rendering in disposable child processes.

Only small control messages cross a pipe. Payloads and encoded images live in
private temporary files, so a stalled native decoder or partial large IPC write
cannot strand the supervising thread. Healthy workers retain decode caches.
"""

from __future__ import annotations

import atexit
import contextlib
import json
import logging
import multiprocessing
import os
import re
import tempfile
import threading
import time
import weakref
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from multiprocessing.connection import _ConnectionBase as Connection  # or Windows' PipeConnection
    from multiprocessing.process import BaseProcess

    from services.service_types import JsonDict

    # The child-side render: ``(payload, output_path) -> (status, message,
    # source)``; the image itself is written to ``output_path``.
    PreviewHandler = Callable[[JsonDict, str], tuple[int, str, str]]
    # What ``render`` answers: ``(status, body, source)``.
    PreviewResult = tuple[int, bytes, str]
    # A parent-side source guard, entered around one render with the job's
    # cancellation event.
    PreviewGuard = Callable[[threading.Event], AbstractContextManager[object]]

log = logging.getLogger(__name__)
_POOLS: weakref.WeakSet[PreviewWorkers] = weakref.WeakSet()

# BLAS/OpenMP runtimes read these only at import time, and the child imports
# the handler module (and its NumPy chain) during spawn bootstrap before any
# code we author runs. Setting them in the parent's os.environ around
# ``Process.start()`` is the only point where the child can inherit them.
_BLAS_THREADS_ENV = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS')
_SPAWN_LOCK = threading.Lock()


def _shutdown() -> None:
    for pool in list(_POOLS):
        pool.close()


atexit.register(_shutdown)


def parse_request(session: object, sequence: object) -> tuple[str, int]:
    if not isinstance(session, str) or not re.fullmatch(r'[a-f0-9]{32}', session):
        raise ValueError('Invalid preview session')
    if not isinstance(sequence, str) or not re.fullmatch(r'[0-9]{1,15}', sequence):
        raise ValueError('Invalid preview sequence')
    return session, int(sequence)


def _worker(connection: Connection, handler: PreviewHandler) -> None:
    try:
        while True:
            payload_path, output_path = connection.recv()
            try:
                payload = json.loads(Path(payload_path).read_text())
                result = handler(payload, output_path)
            except Exception:
                log.exception('Preview worker failed')
                result = (500, 'Could not render preview', '')
            connection.send(result)
    except (EOFError, BrokenPipeError):
        return
    finally:
        connection.close()


@dataclass(eq=False)
class _Job:
    session: str
    sequence: int
    payload: JsonDict
    deadline: float
    guard: PreviewGuard | None
    slot: int = 0
    done: threading.Event = field(default_factory=threading.Event)
    cancelled: threading.Event = field(default_factory=threading.Event)
    result: PreviewResult = (503, b'Preview unavailable', '')


class PreviewWorkers:
    def __init__(
        self, handler: PreviewHandler, *, workers: int = 2, pending: int = 8, timeout: float = 45,
        idle_timeout: float = 60, sessions: int = 1024, threads: int = 2,
    ) -> None:
        self.handler = handler
        self.workers = workers
        self.pending_limit = pending
        self.timeout = timeout
        self.idle_timeout = idle_timeout
        self.session_limit = sessions
        self.threads = threads
        self._condition = threading.Condition()
        self._latest: OrderedDict[str, int] = OrderedDict()
        self._jobs: dict[str, _Job] = {}
        self._affinity: dict[str, int] = {}
        self._active: list[_Job | None] = [None] * workers
        self._queue: deque[_Job] = deque()
        self._threads: list[threading.Thread] = []
        self._closed = False
        self._context = multiprocessing.get_context('spawn')
        _POOLS.add(self)

    def _claim(self, session: str, sequence: int) -> bool:
        # Retain cancellation barriers even if the corresponding GET has not
        # arrived. Never evict live jobs to make room for another tab.
        previous = self._latest.get(session)
        if previous is not None and sequence <= previous:
            return False
        if session not in self._latest and len(self._latest) >= self.session_limit:
            disposable = next((key for key in self._latest if key not in self._jobs), None)
            if disposable is None:
                return False
            del self._latest[disposable]
            self._affinity.pop(disposable, None)
        self._latest[session] = sequence
        self._latest.move_to_end(session)
        old = self._jobs.get(session)
        if old:
            old.cancelled.set()
            if old in self._queue:
                self._queue.remove(old)
            self._finish(old, (409, b'Preview superseded', ''))
        return True

    def _finish(self, job: _Job, result: PreviewResult) -> None:
        # Caller holds the condition lock. A superseded result cannot displace
        # a newer job on the same session, or overwrite its own cancellation.
        if not job.done.is_set():
            job.result = result
            job.done.set()
        if self._jobs.get(job.session) is job:
            del self._jobs[job.session]

    def cancel(self, session: str, sequence: int) -> None:
        with self._condition:
            if not self._closed:
                self._claim(session, sequence)
                self._condition.notify_all()

    def render(
        self, payload: JsonDict, session: str, sequence: int, *, guard: PreviewGuard | None = None,
    ) -> PreviewResult:
        job = _Job(session, sequence, payload, time.monotonic() + self.timeout, guard)
        with self._condition:
            if self._closed:
                return 503, b'Preview service stopped', ''
            if not self._claim(session, sequence):
                return 409, b'Preview superseded', ''
            if len(self._queue) >= self.pending_limit:
                return 503, b'Preview queue is full. Retry shortly.', ''
            slot = self._affinity.get(session)
            if slot is None or ((active := self._active[slot]) is not None and active.session != session):
                slot = min(range(self.workers), key=lambda index:
                           int(self._active[index] is not None) + sum(item.slot == index for item in self._queue))
            job.slot = self._affinity[session] = slot
            self._jobs[session] = job
            self._queue.append(job)
            if not self._threads:
                for index in range(self.workers):
                    thread = threading.Thread(target=self._serve, args=(index,), name=f'preview-{index}', daemon=True)
                    self._threads.append(thread)
                    thread.start()
            self._condition.notify_all()
        # Also bounds waiting for a parent-side source guard. Cancellation and
        # navigation endpoints remain available while a volume/cache is busy.
        if not job.done.wait(max(0, job.deadline - time.monotonic())):
            with self._condition:
                job.cancelled.set()
                if job in self._queue:
                    self._queue.remove(job)
                self._finish(job, (504, b'Preview timed out. Retry or keep editing.', ''))
                self._condition.notify_all()
        return job.result

    def _spawn_bounded(self, process: BaseProcess) -> None:
        # Spawn copies ``os.environ`` into the child before any Python-level
        # code runs there, and the child imports the handler (and its NumPy
        # chain) during bootstrap, so BLAS/OMP read these variables at that
        # point. The parent lock serializes concurrent spawns past the brief
        # os.environ mutation; the restore keeps the parent's own BLAS
        # settings unchanged.
        with _SPAWN_LOCK:
            preserved = {name: os.environ.get(name) for name in _BLAS_THREADS_ENV}
            try:
                for name in _BLAS_THREADS_ENV:
                    os.environ[name] = str(self.threads)
                process.start()
            finally:
                for name, value in preserved.items():
                    if value is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = value

    @staticmethod
    def _stop(process: BaseProcess | None, connection: Connection | None) -> None:
        if connection is not None:
            connection.close()
        if process is not None:
            if process.is_alive():
                process.terminate()
            process.join(0.5)
            if process.is_alive():
                process.kill()
                process.join(0.5)
            if process.is_alive():
                raise RuntimeError('Preview worker could not be reaped')
            process.close()

    def _serve(self, index: int) -> None:
        process: BaseProcess | None = None
        connection: Connection | None = None

        def eligible(job: _Job) -> bool:
            # Preserve warm cache affinity when its worker is available, but
            # let a free worker help tabs queued behind someone else's render.
            # Replacements wait for their own old worker to be reaped first.
            preferred = self._active[job.slot]
            return job.slot == index or (preferred is not None and preferred.session != job.session)

        try:
            with tempfile.TemporaryDirectory(prefix='vireo-preview-') as directory:
                payload_path = str(Path(directory) / 'request.json')
                output_path = str(Path(directory) / 'preview.jpg')
                while True:
                    with self._condition:
                        self._condition.wait_for(
                            lambda: self._closed or any(eligible(item) for item in self._queue),
                            timeout=self.idle_timeout,
                        )
                        if self._closed:
                            return
                        job = next((item for item in self._queue if eligible(item)), None)
                        if job is not None:
                            self._queue.remove(job)
                            job.slot = self._affinity[job.session] = index
                            self._active[index] = job
                    if job is None:
                        self._stop(process, connection)
                        process = connection = None
                        continue
                    if job.cancelled.is_set():
                        with self._condition:
                            self._active[index] = None
                        continue
                    result = (500, b'Preview worker stopped unexpectedly', '')
                    try:
                        with job.guard(job.cancelled) if job.guard is not None else contextlib.nullcontext():
                            if job.cancelled.is_set():
                                continue
                            if process is None:
                                connection, child = self._context.Pipe()
                                process = self._context.Process(target=_worker, args=(child, self.handler), daemon=True)
                                self._spawn_bounded(process)
                                child.close()
                            Path(payload_path).write_text(json.dumps(job.payload))
                            # The pipe is opened and closed together with the
                            # process, so it is open whenever a worker is.
                            connection = cast("Connection", connection)
                            connection.send((payload_path, output_path))
                            while not job.cancelled.is_set() and time.monotonic() < job.deadline:
                                if connection.poll(0.02):
                                    status, message, source = connection.recv()
                                    body = Path(output_path).read_bytes() if status == 200 else message.encode()
                                    result = status, body, source
                                    break
                                if not process.is_alive():
                                    raise EOFError('Preview worker exited')
                            else:
                                self._stop(process, connection)
                                process = connection = None
                                result = (504, b'Preview timed out', '')
                    except InterruptedError:
                        result = (409, b'Preview superseded', '')
                    except Exception:
                        log.exception('Could not supervise preview worker')
                        self._stop(process, connection)
                        process = connection = None
                    finally:
                        Path(payload_path).unlink(missing_ok=True)
                        Path(output_path).unlink(missing_ok=True)
                        with self._condition:
                            self._active[index] = None
                            self._finish(job, result)
        finally:
            self._stop(process, connection)

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for job in list(self._jobs.values()):
                job.cancelled.set()
                self._finish(job, (503, b'Preview service stopped', ''))
            self._queue.clear()
            self._condition.notify_all()
        for thread in self._threads:
            thread.join(1.5)
