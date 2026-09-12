"""One shared source-folder traversal streamed to the import page.

A single walk feeds both the per-folder counters and the preview grid, so
the import page never scans the same folder twice for one selection.  The
generator yields SSE frames; a client disconnect surfaces as an exception on
the next write, the ``finally`` sets the cancel event, and the walker
threads stop inside ``discover_source_files`` — a superseded preview stops
consuming the disk instead of walking on behind an aborted fetch.

Folder scheduling honors the per-volume policy from ``source_scan_policy``:
bounded parallelism on known-local volumes, one scan at a time per network
or removable volume, independent volumes overlapping under a global cap.
"""

import json
import os
import queue
import threading
import time
from pathlib import Path
from urllib.parse import quote

import source_scan_policy
from image_loader import ScanCancelled
from import_dedup import source_capture_timestamps
from ingest import discover_source_files

GLOBAL_SCAN_LIMIT = 4
# Progress frames are throttled per folder; heartbeats double as the
# disconnect probe while every walker is between reportable milestones.
PROGRESS_MIN_INTERVAL_SECONDS = 0.25
HEARTBEAT_SECONDS = 0.5

# A stream-local scheduler is insufficient when a disconnected walker is
# blocked inside the filesystem: another request could otherwise immediately
# start a replacement walk on the same slow volume. Keep lane occupancy until
# the worker itself exits, across every preview stream in this process.
_VOLUME_LANES_LOCK = threading.Lock()
_ACTIVE_VOLUME_LANES = {}
_ACTIVE_SCAN_COUNT = 0


def _try_acquire_volume_lane(volume_key, max_parallel):
    global _ACTIVE_SCAN_COUNT
    with _VOLUME_LANES_LOCK:
        active = _ACTIVE_VOLUME_LANES.get(volume_key, 0)
        if active >= max_parallel or _ACTIVE_SCAN_COUNT >= GLOBAL_SCAN_LIMIT:
            return False
        _ACTIVE_VOLUME_LANES[volume_key] = active + 1
        _ACTIVE_SCAN_COUNT += 1
        return True


def _release_volume_lane(volume_key):
    global _ACTIVE_SCAN_COUNT
    with _VOLUME_LANES_LOCK:
        active = _ACTIVE_VOLUME_LANES.get(volume_key, 0)
        if active <= 0:
            return
        if active <= 1:
            _ACTIVE_VOLUME_LANES.pop(volume_key, None)
        else:
            _ACTIVE_VOLUME_LANES[volume_key] = active - 1
        _ACTIVE_SCAN_COUNT -= 1


def unique_root_names(folders):
    """Shortest trailing path segments that are unique across sources.

    E.g. /mnt/cardA/DCIM and /mnt/cardB/DCIM become cardA/DCIM and
    cardB/DCIM.  Returns {} for a single source (its basename is used).
    """
    if len(folders) < 2:
        return {}
    root_names = {}
    parts = [Path(f).parts for f in folders]
    for depth in range(1, max(len(p) for p in parts) + 1):
        # These names are browser-facing labels, not paths passed back to the
        # filesystem. Keep their separator stable across server platforms so
        # POSIX-shaped inputs do not render with backslashes on Windows.
        suffixes = ["/".join(p[-depth:]) for p in parts]
        if len(set(suffixes)) == len(suffixes):
            for folder_path, suffix in zip(folders, suffixes, strict=True):
                root_names[folder_path] = suffix
            break
    else:
        for folder_path in folders:
            root_names[folder_path] = folder_path
    return root_names


def _empty_result(folder, error=False):
    return {
        "path": folder,
        "files": [],
        "type_breakdown": {},
        "total_size": 0,
        "count": 0,
        "error": error,
    }


def _walk_folder(folder, root_name, multi_source, file_types, recursive,
                 cancel, emit, include_capture_dates=False):
    """Walk one folder; returns a per-folder result or None when cancelled."""
    errors = []
    last_emit = [0.0]

    def progress(checked, found):
        now = time.monotonic()
        if now - last_emit[0] >= PROGRESS_MIN_INTERVAL_SECONDS:
            last_emit[0] = now
            emit({
                "type": "folder_progress",
                "path": folder,
                "stage": "walk",
                "checked": checked,
                "found": found,
            })

    try:
        discovered = discover_source_files(
            folder,
            file_types=file_types,
            recursive=recursive,
            onerror=errors.append,
            cancel_check=cancel.is_set,
            progress_callback=progress,
        )
    except ScanCancelled:
        return None

    files = []
    type_breakdown = {}
    total_size = 0
    last_metadata_emit = 0.0
    for index, f in enumerate(discovered, start=1):
        if cancel.is_set():
            return None
        try:
            stat = f.stat()
        except OSError:
            # Deleted or unreadable between walk and stat — drop the row
            # rather than kill the whole preview.
            continue
        ext = f.suffix.lower()
        type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
        total_size += stat.st_size
        # Determine subfolder relative to the source root
        try:
            rel = f.parent.relative_to(folder)
            subfolder = str(rel) if str(rel) != "." else root_name
        except ValueError:
            subfolder = root_name
        # Prefix with source root name when multiple sources to prevent
        # collisions (e.g. two cards with DCIM/100CANON)
        if multi_source and subfolder != root_name:
            subfolder = os.path.join(root_name, subfolder)
        files.append({
            "path": str(f),
            "filename": f.name,
            "subfolder": subfolder,
            "size": stat.st_size,
            "extension": ext,
            "mtime": stat.st_mtime,
            "thumb_url": "/api/import/folder-preview/thumbnail?path="
            + quote(str(f)),
        })
        now = time.monotonic()
        if (index % 500 == 0
                or now - last_metadata_emit >= PROGRESS_MIN_INTERVAL_SECONDS):
            last_metadata_emit = now
            emit({
                "type": "folder_progress",
                "path": folder,
                "stage": "metadata",
                "checked": index,
                "found": len(discovered),
            })
    if include_capture_dates:
        # Bound metadata reads so a disconnected preview stops between
        # batches. Capture dates deliberately never fall back to file mtime.
        for start in range(0, len(files), 128):
            if cancel.is_set():
                return None
            batch = files[start:start + 128]
            timestamps = source_capture_timestamps([Path(f["path"]) for f in batch])
            for f in batch:
                timestamp = timestamps.get(Path(f["path"]))
                f["capture_date"] = timestamp.date().isoformat() if timestamp else None
            emit({
                "type": "folder_progress", "path": folder,
                "stage": "capture_dates", "checked": start + len(batch),
                "found": len(files),
            })
    return {
        "path": folder,
        "files": files,
        "type_breakdown": type_breakdown,
        "total_size": total_size,
        "count": len(files),
        # Root-level failure with nothing found is the "unavailable"
        # signal; a partial subtree error with files still discovered is
        # a normal (if degraded) result, matching the old endpoint.
        "error": bool(errors) and not files,
    }


def stream_folder_preview(folders, file_types="both", recursive=True,
                          classify=None, include_capture_dates=False):
    """Yield SSE frames for a storage-aware multi-folder discovery walk.

    Frame sequence: one ``policy`` frame, then interleaved
    ``folder_started`` / ``folder_progress`` / ``folder_done`` frames as
    walkers run, then a final ``done`` frame whose payload matches the old
    synchronous folder-preview response.
    """
    classify = classify or source_scan_policy.classify_sources
    # The UI can't submit duplicate paths; drop exact repeats defensively
    # so the running-walker registry stays keyed by path.
    seen = set()
    folders = [f for f in folders if not (f in seen or seen.add(f))]
    policies = {p["path"]: p for p in classify(folders)}
    for folder in folders:
        policies.setdefault(folder, {
            "path": folder,
            "volume_key": "unknown-volume",
            "storage": "unknown",
            "max_parallel": 1,
        })

    root_names = unique_root_names(folders)
    multi_source = len(folders) > 1
    cancel = threading.Event()
    events = queue.Queue()
    results = {}
    pending = list(folders)
    running = {}

    def frame(payload):
        return f"data: {json.dumps(payload)}\n\n"

    def worker(folder):
        policy = policies[folder]
        try:
            try:
                result = _walk_folder(
                    folder,
                    root_names.get(folder, os.path.basename(folder.rstrip("/"))),
                    multi_source,
                    file_types,
                    recursive,
                    cancel,
                    events.put,
                    include_capture_dates=include_capture_dates,
                )
            except Exception:
                # A walker must never die silently: the scheduler would wait
                # on its completion forever. Surface the folder as unavailable.
                result = _empty_result(folder, error=True)
            events.put({"_completed": folder, "_result": result})
        finally:
            _release_volume_lane(
                policy["volume_key"],
            )

    try:
        yield frame({
            "type": "policy",
            "sources": [
                {
                    "path": folder,
                    "storage": policies[folder]["storage"],
                    "position": index + 1,
                    "total": len(folders),
                }
                for index, folder in enumerate(folders)
            ],
        })
        while pending or running:
            launched = True
            while launched and pending and len(running) < GLOBAL_SCAN_LIMIT:
                launched = False
                for folder in pending:
                    policy = policies[folder]
                    active_on_volume = sum(
                        1 for other in running
                        if policies[other]["volume_key"]
                        == policy["volume_key"]
                    )
                    if active_on_volume >= policy["max_parallel"]:
                        continue
                    if not _try_acquire_volume_lane(
                            policy["volume_key"], policy["max_parallel"]):
                        continue
                    pending.remove(folder)
                    thread = threading.Thread(
                        target=worker, args=(folder,), daemon=True,
                    )
                    running[folder] = thread
                    # Yield started before the thread runs so the client
                    # never sees progress for a folder it wasn't told about.
                    try:
                        yield frame({
                            "type": "folder_started",
                            "path": folder,
                            "storage": policy["storage"],
                        })
                        thread.start()
                    except BaseException:
                        running.pop(folder, None)
                        _release_volume_lane(policy["volume_key"])
                        raise
                    launched = True
                    break
            try:
                event = events.get(timeout=HEARTBEAT_SECONDS)
            except queue.Empty:
                # Comment frame: ignored by the client, but writing it is
                # how a disconnected socket surfaces while every walker is
                # deep in a quiet stretch of the tree.
                yield ": ping\n\n"
                continue
            if "_completed" in event:
                folder = event["_completed"]
                running.pop(folder, None)
                result = event["_result"]
                if result is None:
                    continue
                results[folder] = result
                yield frame({
                    "type": "folder_done",
                    "path": folder,
                    "count": result["count"],
                    "error": result["error"],
                })
            else:
                yield frame(event)

        all_files = []
        type_breakdown = {}
        total_size = 0
        source_counts = {}
        for folder in folders:
            result = results.get(folder) or _empty_result(folder)
            all_files.extend(result["files"])
            for ext, count in result["type_breakdown"].items():
                type_breakdown[ext] = type_breakdown.get(ext, 0) + count
            total_size += result["total_size"]
            source_counts[folder] = result["count"]
        yield frame({
            "type": "done",
            "total_count": len(all_files),
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": all_files,
            "source_counts": source_counts,
        })
    finally:
        cancel.set()
