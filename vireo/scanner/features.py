"""Content features (perceptual and file hash) for the files a scan processes.

``_FeatureHasher`` streams ``(image_path, (phash, file_hash))`` in queue
order, on a process pool or inline, under a CPU lease it drops whenever the
caller pauses.
"""

from __future__ import annotations

import contextlib
import logging
import multiprocessing
import sys
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from typing import TYPE_CHECKING

from image_loader import ScanCancelled
from scanner.hashing import (
    _SCAN_MP_METHOD,
    _claim_worker_count,
    _compute_file_features,
    _ScanPauseRequested,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator
    from concurrent.futures import Future
    from pathlib import Path

    from scanner.hashing import FileFeatures
    from scanner.run_state import _ScanSignals
    from scanner.scan_types import CheckCallback

    # A hashed work-queue file: its path and ``(phash, file_hash)``.
    _HashedFile = tuple[Path, FileFeatures]

log = logging.getLogger(__name__)


class _FeatureHasher:
    """Hashes the work queue; owns the pause and lease handling around it."""

    def __init__(
        self, *, signals: _ScanSignals, pause_check: CheckCallback | None,
        cancel_only_check: CheckCallback | None,
    ) -> None:
        self.signals = signals
        self.pause_check = pause_check
        self.cancel_only_check = cancel_only_check

    def _pause_pending(self) -> bool:
        return self.pause_check is not None and self.pause_check()

    def _check_cancelled_no_park(self) -> None:
        """Cancel probe safe to call INSIDE ``_claim_worker_count``.

        Never parks. When ``cancel_only_check`` was supplied the
        pause/cancel probes are strictly non-parking, so a Pause
        arriving between the two calls raises ``_ScanPauseRequested``
        instead of triggering the caller's parking ``cancel_check``.
        That lets the enclosing frame drain workers and release CPU
        permits before parking on the outer ``check_cancelled`` call.

        When ``cancel_only_check`` is not supplied this falls back to
        the pause-then-parking-cancel sequence, which has a race: a pause
        set between the two calls parks the job while it still holds the
        lease. Every pipeline caller wires the non-parking probe.
        """
        if self._pause_pending():
            raise _ScanPauseRequested()
        if self.cancel_only_check is not None:
            if self.cancel_only_check():
                raise ScanCancelled("scan cancelled")
            return
        self.signals.check_cancelled()

    def iter_features(self, files_to_process: list[Path], paths_to_extract: list[str]) -> Iterator[_HashedFile]:
        """Yield ``(image_path, (phash, file_hash))`` in queue order.

        Computes phash + file_hash in parallel across all files that need
        processing. These are the two per-file operations that actually read
        every byte of the image; everything else in the loop is cheap DB or
        dict work. Results stream in order, so workers keep computing the tail
        while the main thread commits the head — no O(n) buffer of features.
        """
        if not files_to_process:
            return
        # Track remaining work outside the ``_claim_worker_count`` context so
        # a pause can drop the lease, park at the caller's pause-aware
        # ``cancel_check``, then reacquire a fresh lease for whatever is
        # left. Without this, ``check_cancelled`` parking inside the pool
        # loop would hold every CPU permit for the entire pause — blocking
        # replacement scans, CPU inference, and model loads even though the
        # user asked this job to stop competing for resources.
        remaining = deque(
            zip(files_to_process, paths_to_extract, strict=True),
        )
        while remaining:
            # A standalone caller may provide a non-parking cancel probe.
            # Do not repeatedly claim permits and construct a process pool
            # while its independent pause probe remains true.
            if self._pause_pending():
                self.signals.check_cancelled()
                if self._pause_pending():
                    time.sleep(0.05)
                continue
            with _claim_worker_count(
                [ip for ip, _ in remaining],
                cancel_check=(
                    self.signals.check_cancelled
                    if self.signals.cancel_check is not None else None
                ),
            ) as workers:
                if self.signals.status_callback:
                    self.signals.emit_status(
                        f"Hashing {len(remaining)} files "
                        f"({workers} worker{'s' if workers != 1 else ''})..."
                    )
                log.info(
                    "Scanner hashing granted %d worker(s) for %d files",
                    workers, len(remaining),
                )
                if workers > 1:
                    paused = yield from self._pool_features(workers, remaining)
                else:
                    paused = yield from self._inline_features(remaining)
            # Lease is released here (``_claim_worker_count`` exited).
            # Park until the caller resumes (or cancels) before rebuilding
            # the pool with the remaining files. ``check_cancelled`` is
            # pause-aware: when the pipeline is pausing, ``cancel_check``
            # blocks inside it until resume, and raises ``ScanCancelled``
            # if the pause turns into a cancellation.
            if paused:
                self.signals.check_cancelled()
        # The final result is consumed while this generator is suspended.
        # Check once more only after the last lease has unwound so a pause or
        # cancellation arriving during that consumer work cannot strand the
        # pool and its CPU permits.
        self.signals.check_cancelled()

    def _pool_features(self, workers: int, remaining: deque[tuple[Path, str]]) -> Generator[_HashedFile, None, bool]:
        """Hash on a process pool; returns ``True`` when a pause stopped it."""
        mp_ctx = multiprocessing.get_context(_SCAN_MP_METHOD)
        pool = ProcessPoolExecutor(max_workers=workers, mp_context=mp_ctx)
        paused = False
        pending: deque[tuple[Path, str, Future[FileFeatures]]] = deque()
        try:
            # Bounded in-flight window instead of pool.map(): on Python
            # 3.11 Executor.map eagerly submits every input, so on a
            # 200k-file scan we would hold 200k queued futures in RAM.
            # A few submissions per worker is enough to keep them fed
            # while the main thread drains results in order.
            #
            # Pause is checked BEFORE ``check_cancelled`` at every
            # boundary. The pipeline's ``cancel_check`` parks on a
            # pending pause, so calling it first would trap the
            # scanner inside the lease context; raising
            # ``_ScanPauseRequested`` while the pool is still
            # running lets the enclosing frame drain workers and
            # release CPU permits before we park.
            max_in_flight = workers * 4
            while remaining:
                self._check_cancelled_no_park()
                image_path, path_str = remaining[0]
                pending.append((
                    image_path, path_str,
                    pool.submit(_compute_file_features, path_str),
                ))
                remaining.popleft()
                if len(pending) >= max_in_flight:
                    done_path, _done_str, done_fut = pending[0]
                    self._check_cancelled_no_park()
                    result = self._feature_result(done_fut)
                    if self._pause_pending():
                        raise _ScanPauseRequested()
                    pending.popleft()
                    yield done_path, result
            while pending:
                done_path, _done_str, done_fut = pending[0]
                self._check_cancelled_no_park()
                result = self._feature_result(done_fut)
                if self._pause_pending():
                    raise _ScanPauseRequested()
                pending.popleft()
                yield done_path, result
        except _ScanPauseRequested:
            # Requeue every submitted-but-undrained item so the
            # next pass reruns them under a fresh lease. Their
            # workers are torn down below.
            paused = True
            for image_path, path_str, _fut in reversed(pending):
                remaining.appendleft((image_path, path_str))
            # Terminate now, wait below with the lease still
            # held so a replacement scan cannot claim the same
            # permits while old workers are still consuming CPU.
            for proc in list(getattr(pool, "_processes", {}).values()):
                terminated = False
                # broad: termination and its diagnostics must not prevent worker shutdown
                with contextlib.suppress(Exception):
                    try:
                        proc.terminate()
                        terminated = True
                    finally:
                        if not terminated and isinstance(sys.exception(), Exception):
                            log.debug("Could not terminate a feature worker", exc_info=True)
            pool.shutdown(wait=True, cancel_futures=True)
        except BaseException:
            # broad: stops the workers on any exit, cancel and interrupt included, then re-raises.
            # Actively terminate the worker processes so their CPU
            # work stops now, then wait for them to actually exit
            # before letting ``_claim_worker_count`` release its
            # CPU permits. ``shutdown(wait=False)`` alone would
            # unwind the lease while old workers were still
            # hashing — a replacement scan or CPU inference could
            # then receive the same permits and defeat the
            # process-wide budget, and ``JobRunner.shutdown()``
            # could report completion while hashing continued.
            for proc in list(getattr(pool, "_processes", {}).values()):
                terminated = False
                # broad: termination and its diagnostics must not prevent worker shutdown
                with contextlib.suppress(Exception):
                    try:
                        proc.terminate()
                        terminated = True
                    finally:
                        if not terminated and isinstance(sys.exception(), Exception):
                            log.debug("Could not terminate a feature worker", exc_info=True)
            pool.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            pool.shutdown(wait=True)
        return paused

    def _feature_result(self, fut: Future[FileFeatures]) -> FileFeatures:
        while True:
            # ``_check_cancelled_no_park`` raises
            # ``_ScanPauseRequested`` on pending pause
            # and only calls the parking probe if it
            # cannot be avoided (no ``cancel_only_check``
            # supplied). That closes the race where
            # ``_pause_pending`` returns False, Pause
            # fires, and a parking ``check_cancelled``
            # parks while still inside the lease and
            # pool.
            self._check_cancelled_no_park()
            try:
                return fut.result(timeout=0.2)
            except TimeoutError:
                continue

    def _inline_features(self, remaining: deque[tuple[Path, str]]) -> Generator[_HashedFile, None, bool]:
        """Hash in this process; returns ``True`` when a pause stopped it."""
        paused = False
        while remaining:
            # ``_check_cancelled_no_park`` raises
            # ``_ScanPauseRequested`` on pending pause and
            # only invokes the parking probe if no
            # ``cancel_only_check`` was supplied. Caught and
            # translated to ``paused = True`` so this branch
            # keeps its "drop the lease, then park outside"
            # symmetry with the pool branch above.
            try:
                self._check_cancelled_no_park()
            except _ScanPauseRequested:
                paused = True
                break
            image_path, path_str = remaining[0]
            result = _compute_file_features(path_str)
            if self._pause_pending():
                paused = True
                break
            remaining.popleft()
            yield image_path, result
        return paused
