"""Content hashing and the CPU lease the scanner's hashing pool runs under."""

import contextlib
import hashlib
import logging
import multiprocessing
import os
import sys

import imagehash
from PIL import Image
from resource_ledger import (
    ResourceRequest,
    cpu_inference_request,
    cpu_phase_request,
    get_resource_ledger,
)

log = logging.getLogger(__name__)


EMPTY_FILE_SHA256 = hashlib.sha256(b"").hexdigest()


class _ScanPauseRequested(RuntimeError):
    """Internal signal that the caller wants the scanner to release its
    process-wide CPU permits and park before resuming.

    Raised only inside ``_iter_features`` when a ``pause_check`` was
    supplied. It never escapes the scanner — the enclosing frame catches
    it, drains the pool, releases the CPU lease by exiting
    ``_claim_worker_count``, and then invokes the pause-aware
    ``cancel_check`` (which parks). Once the pause resolves, hashing
    reacquires a fresh lease and continues with the remaining files.
    """


# scan() runs inside JobRunner/pipeline_job background threads, so the
# default POSIX "fork" start method is unsafe here: forking a
# multithreaded process can deadlock. In a PyInstaller bundle, forkserver
# also fails on macOS — workers fork from a parent that has already
# loaded PIL/Foundation, and the worker's first Cocoa-touching call
# crashes the child, surfacing as EOFError on the forkserver handshake
# in the parent. spawn does fork+exec for each worker, giving a clean
# process; paired with multiprocessing.freeze_support() in app.py's
# entry point it works inside the frozen sidecar. Dev runs keep
# forkserver for its cheap warmup.
_SCAN_MP_METHOD = (
    "spawn"
    if getattr(sys, "frozen", False)
    else "forkserver"
    if "forkserver" in multiprocessing.get_all_start_methods()
    else "spawn"
)


# Windows' ProcessPoolExecutor raises ValueError when max_workers > 61
# (the WaitForMultipleObjects handle limit). Clamp on Windows so scans
# don't fail on high-core-count machines or misconfigured scan_workers.
_WINDOWS_MAX_WORKERS = 61


def compute_file_hash(file_path, chunk_size=65536):
    """Compute SHA-256 hash of a file. Returns hex digest string."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute_fd_hash(fd, chunk_size=65536):
    """Compute SHA-256 hash of the object referred to by ``fd``.

    Reading from the descriptor rather than re-opening the pathname keeps
    a concurrent rename from substituting a FIFO or other blocking object
    behind us — the caller pins the object with ``os.open`` + ``fstat``,
    and this function only ever hashes whatever that descriptor already
    refers to. Does NOT seek: pass a freshly-opened descriptor, or the
    hash covers only the bytes past the current file offset.
    """
    h = hashlib.sha256()
    while True:
        chunk = os.read(fd, chunk_size)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


def _compute_file_features(path_str):
    """Compute (phash, file_hash) for one image.

    Module-level so ProcessPoolExecutor can pickle it. Mirrors the
    best-effort behavior the main scan loop used to have inline: any
    failure yields None for that field rather than raising.
    """
    phash = None
    # PIL raises a wide set on bad files (OSError, SyntaxError,
    # DecompressionBombError); a missing pHash only disables near-duplicate
    # grouping for this photo. This runs in a pool worker, whose log
    # records never reach the app log, so nothing is logged here.
    with contextlib.suppress(Exception), Image.open(path_str) as img:
        phash = str(imagehash.phash(img))
    file_hash = None
    # Unreadable file: the caller logs the ``None`` hash and leaves the stored
    # mtime stale so the next scan retries the file.
    with contextlib.suppress(OSError):
        file_hash = compute_file_hash(path_str)
    return phash, file_hash


def _resolve_worker_count(files_to_process):
    """Decide how many workers to use for feature computation.

    Returns 1 (sequential) when the batch is tiny or config disables
    parallelism; otherwise honors ``scan_workers`` (0 = auto, cap at
    cpu_count and batch size).
    """
    n = len(files_to_process)
    if n < 8:
        return 1
    try:
        import config as cfg
        configured = int(cfg.get("scan_workers") or 0)
    except (TypeError, ValueError):
        log.warning("Ignoring non-integer scan_workers setting", exc_info=True)
        configured = 0
    if configured == 1:
        return 1
    cpu = os.cpu_count() or 1
    if configured <= 0:
        workers = cpu
    else:
        workers = min(configured, cpu)
    if sys.platform == "win32":
        workers = min(workers, _WINDOWS_MAX_WORKERS)
    return max(1, min(workers, n))


@contextlib.contextmanager
def _claim_worker_count(files_to_process, cancel_check=None):
    """Yield the scanner worker grant from the process-wide CPU budget."""
    desired = _resolve_worker_count(files_to_process)
    ledger = get_resource_ledger()
    inference_threads = cpu_inference_request(ledger.cpu_capacity).preferred
    spare_after_inference = ledger.cpu_capacity - inference_threads
    cpu_reserve = inference_threads if spare_after_inference > 0 else 0
    request = ResourceRequest(
        cpu=cpu_phase_request(
            ledger.cpu_capacity,
            minimum=1,
            preferred=desired,
            maximum=desired,
        ),
        # The ledger applies this reserve to the aggregate allocation, so two
        # concurrent flexible scans cannot each consume the same nominal
        # spare capacity and collectively block an exact inference claim.
        cpu_reserve=cpu_reserve,
        label="scanner hashing",
    )
    with ledger.acquire(request, cancel_check=cancel_check) as lease:
        yield lease.cpu_permits
