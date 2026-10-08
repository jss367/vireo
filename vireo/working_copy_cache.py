"""Disk accounting and quota enforcement for generated working copies.

Working copies are edit-quality JPEG renditions generated from RAW files (and
oversized JPEGs).  Unlike preview-cache entries they do not have a dedicated
LRU table, so quota eviction uses the files' modification times: the oldest
generated files leave first.

An evicted row keeps ``working_copy_path`` NULL and records the source mtime in
``working_copy_evicted_mtime``.  The scanner uses that marker to distinguish a
deliberately evicted copy from a missing copy eligible for extraction.
A source-file change makes the marker stale and allows extraction again.
"""

import logging
import os
import threading
import time
from contextlib import contextmanager

from db import commit_with_retry

log = logging.getLogger(__name__)

DEFAULT_QUOTA_MB = 20 * 1024
_UNTRACKED_WRITE_GRACE_SECONDS = 60
# Sweep abandoned render tempfiles after this many seconds. On-demand
# extraction takes seconds; anything older is orphaned by a crash/kill.
# Excluding the tempfile from quota accounting stops it from displacing
# valid working copies, but the bytes still consume real disk — sweep so
# they cannot accumulate indefinitely.
_RENDER_TEMP_SWEEP_SECONDS = 60 * 60
# Cap snapshot-invalidation retries inside a single eviction pass. Concurrent
# catalog writers can bump ``PRAGMA data_version`` between our scan and the
# transaction, so returning after one deferral lets a lowered quota persist
# stale files indefinitely on a busy catalog. Retry with a fresh snapshot up
# to this many times before giving up so the caller does not have to loop.
_EVICTION_SNAPSHOT_RETRIES = 4
# When ``evict_if_over_quota`` exhausts its in-line snapshot retries under
# heavy catalog contention, ``arrange_deferred_over_quota_retry`` schedules a
# bounded background pass so the cache does not silently sit above a lowered
# quota until an unrelated cache write happens by. Delays back off from
# ``_DEFERRED_RETRY_INITIAL_DELAY`` up to ``_DEFERRED_RETRY_MAX_DELAY`` for at
# most ``_DEFERRED_RETRY_MAX_ATTEMPTS`` passes; each attempt is idempotent so
# an unrelated success (from a scanner or on-demand cache writer) ends the
# retry loop immediately.
_DEFERRED_RETRY_INITIAL_DELAY = 1.0
_DEFERRED_RETRY_MAX_DELAY = 30.0
_DEFERRED_RETRY_MAX_ATTEMPTS = 6
# Publication paths may need to run quota enforcement before releasing the
# guard (for example, when a settings write lowers the ceiling while a slow
# RAW decode is in flight). Keep that nested enforcement serialized with
# other publishers without deadlocking the current thread.
_eviction_lock = threading.RLock()
# One background retry at a time is enough — overlapping ``deferred=True``
# returns coalesce onto the pending pass, and that pass rereads state each
# attempt so it also catches later drops. Without this coalescing, every
# unrelated settings write during contention would spawn a new daemon thread.
_deferred_retry_lock = threading.Lock()
_deferred_retry_pending = False


@contextmanager
def working_copy_publication_guard(*, cancelled=None):
    """Serialize canonical publication with quota scan/delete passes.

    Preview supervisors can abandon a queued source lease on cancellation;
    ordinary publishers retain the existing blocking, reentrant behavior.
    """
    if cancelled is None:
        with _eviction_lock:
            yield
        return
    while not _eviction_lock.acquire(timeout=0.05):
        if cancelled():
            raise InterruptedError('Preview source lease cancelled')
    try:
        if cancelled():
            raise InterruptedError('Preview source lease cancelled')
        yield
    finally:
        _eviction_lock.release()


# Working copies carry no access record of their own, so quota eviction
# orders candidates by mtime — which on a generated file means "written
# longest ago", not "least recently used". Left alone that inverts the
# intent: the copies for the photos imported and reviewed most recently
# are exactly the ones a full cache reclaims first, while a decade-old
# archive that nobody opens survives because its copies happen to be
# newer on disk. Stamping the serve time on a cache hit turns mtime into
# a real recency signal without adding a second LRU table.
#
# Throttled: a photographer pixel-peeping one frame at 1:1 issues a burst
# of requests for the same file, and one utime per request buys nothing
# over one per session.
_ACCESS_TOUCH_INTERVAL_SECONDS = 6 * 60 * 60


def touch_working_copy_access(path):
    """Record a working-copy cache hit for eviction ordering.

    Advances the file's ATIME only, leaving its mtime stable. Callers
    that key off the working copy's mtime treat it as a content-version
    marker — ``_external_edit_handoff_path`` stamps the WC's mtime into
    the handoff meta and regenerates ``external-edits/<id>.jpg`` (via
    ``os.replace``) whenever it changes, which would overwrite anything
    the external editor saved into that file. Advancing atime instead
    keeps the content-version key stable while still giving
    ``_evict_once`` an LRU signal — it sorts by ``max(mtime_ns,
    atime_ns)`` for that reason.

    Display-cache freshness (``/photos/<id>/original``) also compares
    the render's mtime against its sources' mtimes; keeping the WC's
    mtime stable avoids spuriously invalidating that cache on serve.

    Throttled to ``_ACCESS_TOUCH_INTERVAL_SECONDS`` (using
    ``max(atime, mtime)`` as the current recency) so a burst of 1:1
    zoom requests costs one ``utime``. Best-effort: callers are
    serving bytes, so a failed stat/utime costs only eviction-ordering
    accuracy and must never fail the request. Returns True when the
    stamp moved.
    """
    if not path:
        return False
    now = time.time()
    try:
        stat_result = os.stat(path)
    except OSError:
        return False
    current_recency = max(stat_result.st_atime, stat_result.st_mtime)
    if now - current_recency < _ACCESS_TOUCH_INTERVAL_SECONDS:
        return False
    try:
        # Advance atime only; keep mtime as the content-version key.
        os.utime(path, (now, stat_result.st_mtime))
    except OSError:
        return False
    return True


def _file_identity(st):
    """Return the fields that distinguish an atomically replaced file."""
    if os.name == "nt":
        # Windows may expose different st_dev/st_ino values for the same file
        # through DirEntry.stat() and os.stat() (notably on Python 3.14),
        # which would make every candidate look replaced and disable quota
        # eviction. An atomic replacement changes creation time and normally
        # mtime as well; pair both with size for a stable Windows fingerprint.
        return (st.st_size, st.st_mtime_ns, st.st_ctime_ns)
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def _begin_stable_eviction_transaction(db, expected_data_version):
    """Lock catalog writers and confirm the eviction snapshot is current.

    SQLite can reuse the highest ``INTEGER PRIMARY KEY`` after a delete. If a
    photo is deleted and re-imported between our candidate scan and catalog
    UPDATE, an id-only predicate can stamp the old photo's eviction marker on
    the new row. ``BEGIN IMMEDIATE`` excludes row lifecycle writes through the
    unlink/update window; ``data_version`` detects any commit that won the
    race before the lock was acquired so this pass can leave the new row and
    its file untouched.
    """
    db.conn.execute("BEGIN IMMEDIATE")
    current_data_version = db.conn.execute(
        "PRAGMA data_version"
    ).fetchone()[0]
    if current_data_version == expected_data_version:
        return True
    db.conn.rollback()
    return False


def _is_private_working_tempfile(name):
    """Return True for known scanner/on-demand private tempfiles.

    ``vireo/app.py`` writes each on-demand working copy into a private
    ``.<photo_id>.render.*.jpg.tmp`` file before atomically publishing it to
    the canonical path. A process kill during that window leaves the tempfile
    behind; counting it toward quota usage — while eviction is unable to
    remove it because it never matches a catalog row — lets an orphan
    permanently consume the working-copy budget.
    """
    if not name.startswith(".") or not name.endswith(".jpg.tmp"):
        return False
    body = name[1:-len(".jpg.tmp")].lstrip(".")
    for marker in (".render.", ".jpg."):
        photo_id, separator, nonce = body.partition(marker)
        if separator and photo_id.isdigit() and nonce:
            return True
    return False


def sweep_abandoned_render_tempfiles(vireo_dir):
    """Reclaim ``.<id>.render.*.jpg.tmp`` orphans in ``working/``.

    ``_extract_original_copy`` writes each on-demand rendition into a private
    tempfile before atomically publishing it to ``working/<id>.jpg``. A
    process kill or crash during that window leaves the tempfile behind.
    Quota accounting deliberately skips these files (eviction has no catalog
    row to key off their names, so they would otherwise force real working
    copies out to stay under quota), which means without a dedicated sweep
    the orphan can consume disk indefinitely outside the configured ceiling.
    """
    working_dir = os.path.join(vireo_dir, "working")
    if not os.path.isdir(working_dir):
        return
    try:
        with os.scandir(working_dir) as entries:
            for entry in entries:
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                if not _is_private_working_tempfile(entry.name):
                    continue
                try:
                    os.remove(entry.path)
                except OSError as exc:
                    log.warning(
                        "Could not remove abandoned render tempfile %s: %s",
                        entry.path, exc,
                    )
    except OSError as exc:
        log.warning(
            "Could not scan working directory for render tempfiles %s: %s",
            working_dir, exc,
        )


def working_copy_quota_bytes(quota_mb=None):
    """Return the configured working-copy budget in bytes."""
    if quota_mb is None:
        import config as cfg

        quota_mb = cfg.load().get(
            "working_copy_cache_max_mb", DEFAULT_QUOTA_MB,
        )
    try:
        quota_mb = int(quota_mb)
    except (TypeError, ValueError):
        quota_mb = DEFAULT_QUOTA_MB
    return max(0, quota_mb) * 1024 * 1024


def working_copy_stats(vireo_dir, quota_mb=None):
    """Return direct-file count/bytes and the configured quota.

    Skips the private ``.<id>.render.*.jpg.tmp`` files the on-demand
    extractor uses as a staging area — those are not user-visible cache
    entries and eviction cannot reclaim them.
    """
    working_dir = os.path.join(vireo_dir, "working")
    count = 0
    total = 0
    if os.path.isdir(working_dir):
        try:
            with os.scandir(working_dir) as entries:
                for entry in entries:
                    try:
                        if not entry.is_file():
                            continue
                        if _is_private_working_tempfile(entry.name):
                            continue
                        total += entry.stat().st_size
                        count += 1
                    except OSError as exc:
                        log.warning(
                            "Could not stat working-copy file %s: %s",
                            entry.path, exc,
                        )
        except OSError as exc:
            log.warning("Could not inspect working-copy cache %s: %s", working_dir, exc)
    return {
        "count": count,
        "size": total,
        "path": working_dir,
        "quota_bytes": working_copy_quota_bytes(quota_mb),
    }


def evict_if_over_quota(
    db, vireo_dir, quota_mb=None, *, startup=False, protect_paths=None,
):
    """Delete oldest canonical working copies until usage is within quota.

    Legacy Vireo versions wrote ``working/<photo_id>.jpg`` without recording
    ``working_copy_path``. Those canonical files participate too, otherwise a
    zero quota can never reclaim them. Recently modified untracked files get a
    short grace period so a concurrent writer can finish and commit its row.
    Unknown files are counted toward usage but never deleted. Files that
    cannot be removed keep their database references so accounting remains
    honest and a later pass can retry.

    ``startup=True`` bypasses the untracked-writer grace: at process start no
    cache writer can be active yet, so an upgraded install with a legacy
    ``working/<id>.jpg`` whose mtime is within the grace window (recent modify
    time, or a future-dated timestamp copied from an archive) can still be
    reclaimed on the very first quota pass instead of waiting for another
    restart after the window closes.

    ``protect_paths`` is an optional iterable of absolute working-copy paths
    that this pass must not evict — used by the scanner's mid-batch quota
    enforcement to keep its own newly-written files from being reclaimed as
    the batch's oldest-mtime entries. Without it, DESC-id backfill would
    watch eviction pick off its highest-priority files first (the newest
    imports, born with the oldest wall-clock mtime) and defer the older
    imports the rotation was meant to preserve. Protected files still count
    toward usage, so a batch that fills the cache with protected content
    triggers the ``remaining_bytes > max_bytes`` stop signal without losing
    coverage of the very rows the ordering prioritizes.

    Returns a small result payload useful to startup/config callers and tests.
    """
    max_bytes = working_copy_quota_bytes(quota_mb)
    working_dir = os.path.join(vireo_dir, "working")
    if not os.path.isdir(working_dir):
        return {
            "evicted": 0, "freed_bytes": 0, "remaining_bytes": 0,
            "quota_bytes": max_bytes, "evicted_paths": [],
        }

    protected = frozenset(protect_paths or ())

    with _eviction_lock:
        for _snapshot_attempt in range(_EVICTION_SNAPSHOT_RETRIES):
            result = _evict_once(
                db, working_dir, max_bytes,
                startup=startup, protect_paths=protected,
            )
            if result is not None:
                return result
        # A concurrent catalog writer kept invalidating our snapshot. Report
        # deferral honestly so callers can log or retry later; another
        # publication/startup pass will still catch it, but at least this
        # settings save does not silently leave the cache over the new quota.
        log.warning(
            "Working-copy quota eviction deferred after %d snapshot retries; "
            "usage may remain above the configured limit until another pass",
            _EVICTION_SNAPSHOT_RETRIES,
        )
        return {
            "evicted": 0,
            "freed_bytes": 0,
            "remaining_bytes": None,
            "quota_bytes": max_bytes,
            "evicted_paths": [],
            "deferred": True,
        }


def _evict_once(
    db, working_dir, max_bytes, *, startup, protect_paths=frozenset(),
):
    """One snapshot-consistent eviction pass; ``None`` on data_version change.

    Returns the same result payload as ``evict_if_over_quota`` when the pass
    completed against a stable catalog snapshot. Returns ``None`` when the
    catalog changed between our scan and the writer lock so the caller can
    retake the snapshot and retry.

    ``_eviction_lock`` must already be held by the caller so this pass stays
    serialized with canonical publication.

    ``protect_paths`` — see ``evict_if_over_quota``.
    """
    catalog_data_version = db.conn.execute(
        "PRAGMA data_version"
    ).fetchone()[0]
    rows = db.conn.execute(
        "SELECT id, working_copy_path, file_mtime FROM photos "
    ).fetchall()
    files = {}
    total = 0
    stale_temp_cutoff_ns = (
        time.time_ns() - _RENDER_TEMP_SWEEP_SECONDS * 1_000_000_000
    )
    try:
        with os.scandir(working_dir) as directory_entries:
            for entry in directory_entries:
                try:
                    if not entry.is_file():
                        continue
                    if _is_private_working_tempfile(entry.name):
                        # A leftover on-demand extractor tempfile that
                        # eviction cannot reclaim (no catalog row keys off
                        # this name). Counting it toward ``total`` would
                        # force eviction to churn real working copies to
                        # stay under quota while the orphan lingered.
                        # Old enough to be a crash orphan? Reclaim the
                        # disk bytes so an interrupted producer cannot
                        # leak indefinitely; recent ones may still hold
                        # an active writer fd.
                        try:
                            st = entry.stat()
                        except OSError:
                            continue
                        if st.st_mtime_ns <= stale_temp_cutoff_ns:
                            try:
                                os.remove(entry.path)
                            except FileNotFoundError:
                                pass
                            except OSError as exc:
                                log.warning(
                                    "Failed to remove stale render "
                                    "tempfile %s: %s", entry.path, exc,
                                )
                        continue
                    st = entry.stat()
                except OSError as exc:
                    log.warning(
                        "Could not stat working-copy file %s: %s",
                        entry.path, exc,
                    )
                    continue
                files[entry.name] = (entry.path, st)
                total += st.st_size
    except OSError as exc:
        log.warning("Could not inspect working-copy cache %s: %s", working_dir, exc)

    entries = []
    known_canonical_names = {f"{row['id']}.jpg" for row in rows}
    untracked_cutoff_ns = (
        time.time_ns() - _UNTRACKED_WRITE_GRACE_SECONDS * 1_000_000_000
    )
    stale_tracked_ids = []
    for row in rows:
        expected_rel = f"working/{row['id']}.jpg"
        tracked = row["working_copy_path"] == expected_rel
        if row["working_copy_path"] is not None and not tracked:
            # Do not let a malformed or legacy catalog path expand the
            # deletion scope outside Vireo's managed working directory.
            continue
        file_entry = files.get(f"{row['id']}.jpg")
        if file_entry is None:
            if tracked:
                # A prior eviction unlinked the file but lost its DB
                # transition (typically a commit failure after the
                # unlinks). Reconcile so scanner backfill can regenerate
                # this row instead of skipping it forever because
                # working_copy_path is non-NULL.
                stale_tracked_ids.append(
                    (row["id"], os.path.join(working_dir, f"{row['id']}.jpg"))
                )
            continue
        path, st = file_entry
        if (
            not tracked
            and not startup
            and st.st_mtime_ns > untracked_cutoff_ns
        ):
            # This may be an extraction that has published its final path
            # but has not committed working_copy_path yet. Skipped at
            # runtime; startup bypasses the grace because no cache writer
            # can be active yet — the file is safe to reclaim.
            continue
        # LRU key is ``max(mtime_ns, atime_ns)``: mtime is the content-
        # version stamp (only advances on write), atime is the access-
        # recency stamp advanced by ``touch_working_copy_access``. Taking
        # the max means a freshly published file (whose atime tracks its
        # own write) still reads as fresh, and a served-recently file
        # (whose atime we advanced) also reads as fresh, without either
        # side lying about the other.
        entries.append(
            (
                max(st.st_mtime_ns, st.st_atime_ns),
                row["id"], st.st_size, path,
                expected_rel, _file_identity(st),
            )
        )

    # A deleted photo can leave its canonical file behind when Windows
    # temporarily refuses an unlink while a response handle is open. No
    # catalog row remains to contribute an ordinary eviction candidate,
    # but Vireo still owns numeric ``working/<id>.jpg`` names.
    for name, (path, st) in files.items():
        stem, extension = os.path.splitext(name)
        if (
            name not in known_canonical_names
            and extension.lower() == ".jpg"
            and stem.isdigit()
        ):
            photo_id = int(stem)
            entries.append(
                (
                    max(st.st_mtime_ns, st.st_atime_ns),
                    photo_id, st.st_size, path,
                    f"working/{photo_id}.jpg", _file_identity(st),
                )
            )

    if stale_tracked_ids:
        # A publisher may have replaced a file after the directory scan
        # observed it missing. Revalidate while holding the same lock
        # used by canonical publication before clearing the catalog row.
        still_missing = [
            (pid, f"working/{pid}.jpg")
            for pid, path in stale_tracked_ids
            if not os.path.exists(path)
        ]
        db.conn.executemany(
            "UPDATE photos SET working_copy_path=NULL "
            "WHERE id=? AND working_copy_path=?",
            still_missing,
        )
        if still_missing:
            commit_with_retry(db.conn)

    if total <= max_bytes:
        return {
            "evicted": 0, "freed_bytes": 0, "remaining_bytes": total,
            "quota_bytes": max_bytes, "evicted_paths": [],
        }

    if not _begin_stable_eviction_transaction(
        db, catalog_data_version,
    ):
        log.info(
            "Working-copy quota eviction snapshot invalidated by concurrent "
            "catalog writer; retaking snapshot"
        )
        # Signal the caller to retake the snapshot and retry. Leaving stale
        # files here (as an earlier revision did) let a lowered quota persist
        # them indefinitely because the settings handler only called us once.
        return None

    evicted = []
    freed_bytes = 0
    for (
        _mtime_ns, photo_id, size, path, expected_rel, sampled_identity,
    ) in sorted(entries):
        if total <= max_bytes:
            break
        if path in protect_paths:
            # Caller is the scanner's mid-batch enforcement asking us to
            # leave its just-written newest-import file in place. It has
            # the oldest wall-clock mtime in the batch (born first, DESC
            # id) and would otherwise be the first thing evicted, so the
            # backfill would lose exactly the coverage the ordering was
            # meant to buy. Continuing here bounds the transient overshoot
            # to the caller's own writes; the caller detects this via the
            # ``remaining_bytes > max_bytes`` return and stops the batch.
            continue
        try:
            if _file_identity(os.stat(path)) != sampled_identity:
                # Atomic publication replaced the sampled candidate after
                # the scan. Never unlink or account that new rendition as
                # though it were the older file selected for eviction.
                continue
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning("Failed to remove working-copy file %s: %s", path, exc)
            continue
        evicted.append((photo_id, expected_rel, path))
        total -= size
        freed_bytes += size

    for photo_id, expected_rel, path in evicted:
        # Between the ``os.remove`` above and this UPDATE, an on-demand
        # ``/photos/<id>/original`` request can regenerate the same
        # ``working/<id>.jpg`` and commit ``working_copy_path`` to the
        # same relative path. Clearing the row now would mark that
        # freshly written replacement as evicted while its bytes remain
        # on disk — the file becomes untracked and eviction skips it
        # forever after the writer-grace window closes. Only clear the
        # row if the file is still missing.
        if os.path.exists(path):
            continue
        db.conn.execute(
            "UPDATE photos SET working_copy_path=NULL, "
            "working_copy_evicted_mtime=COALESCE(file_mtime, -1) "
            "WHERE id=? AND (working_copy_path IS NULL "
            "OR working_copy_path=?)",
            (photo_id, expected_rel),
        )
    commit_with_retry(db.conn)
    if evicted:
        log.info(
            "Working-copy quota eviction: removed %d files, freed %.1f MB",
            len(evicted), freed_bytes / 1024 / 1024,
        )

    return {
        "evicted": len(evicted),
        "freed_bytes": freed_bytes,
        "remaining_bytes": total,
        "quota_bytes": max_bytes,
        # Which files went, not just how many: a batch caller (the scanner's
        # backfill) has to tell "reclaimed bytes I just wrote" from "reclaimed
        # coverage that already existed" to decide whether it is still growing
        # the cache or merely rotating it.
        "evicted_paths": [path for _, _, path in evicted],
    }


def arrange_deferred_over_quota_retry(
    db_path, vireo_dir, quota_mb=None,
    *, _sleep=time.sleep, _thread_starter=None,
):
    """Schedule a bounded background pass after ``deferred=True`` deferral.

    ``_settings_post_save_side_effects`` calls ``evict_if_over_quota`` once
    when the working-copy quota drops. If snapshot retries exhaust under a
    busy catalog, the settings request returns while the cache still sits
    above its new ceiling — every subsequent enforcement point (cache writer,
    scanner backfill, restart) still runs, but there is no guarantee any of
    those fires soon. This helper closes that window: it spawns a daemon
    thread that retries with backoff, using its own SQLite connection because
    ``Database`` handles are thread-affine.

    Idempotent by design: overlapping deferrals coalesce onto one background
    pass. Each attempt calls ``evict_if_over_quota`` which reads current
    state, so a concurrent success (from a scanner run or an on-demand
    writer) ends the loop naturally on the next attempt.

    ``_sleep`` and ``_thread_starter`` are injected for tests; production
    callers rely on the defaults.
    """
    global _deferred_retry_pending
    with _deferred_retry_lock:
        if _deferred_retry_pending:
            return False
        _deferred_retry_pending = True

    def _run():
        global _deferred_retry_pending
        try:
            from db import Database

            delay = _DEFERRED_RETRY_INITIAL_DELAY
            for _attempt in range(_DEFERRED_RETRY_MAX_ATTEMPTS):
                _sleep(delay)
                try:
                    retry_db = Database(db_path, initialize_schema=False)
                except Exception:
                    log.exception(
                        "Deferred working-copy quota retry could not open "
                        "a database connection at %s", db_path,
                    )
                    return
                try:
                    result = evict_if_over_quota(
                        retry_db, vireo_dir, quota_mb=quota_mb,
                    )
                except Exception:
                    log.exception(
                        "Deferred working-copy quota retry failed"
                    )
                    return
                finally:
                    retry_db.close()
                if not result.get("deferred"):
                    return
                delay = min(delay * 2, _DEFERRED_RETRY_MAX_DELAY)
            log.warning(
                "Working-copy quota deferred retry gave up after %d attempts; "
                "the next cache writer or restart will re-enforce the quota",
                _DEFERRED_RETRY_MAX_ATTEMPTS,
            )
        finally:
            with _deferred_retry_lock:
                _deferred_retry_pending = False

    if _thread_starter is None:
        thread = threading.Thread(
            target=_run, name="wc-quota-deferred-retry", daemon=True,
        )
        thread.start()
    else:
        _thread_starter(_run)
    return True
