"""One working-copy extraction pass over a snapshot of candidate rows.

``_WorkingCopyBatch`` extracts, publishes and records each row's working
copy while keeping the cache under its quota. The SQL lives in
``repositories/working_copies.py`` (``db.working_copies``).
"""

import functools
import logging
import os
import sqlite3
from dataclasses import dataclass, field

from image_loader import (
    RAW_EXTENSIONS,
    extract_working_copy,
    is_excluded_scan_path,
)
from PIL import Image
from render_source import is_undersized
from scanner.exif_fields import _oriented_dimensions, _scaled_dimensions
from scanner.status import _call_status_callback, _status_callback_supports_phase

log = logging.getLogger(__name__)


# How long a recorded extraction failure suppresses retries when the file's
# mtime hasn't changed. Without an upper bound, a transient failure (e.g. an
# external drive briefly unavailable at startup) would block backfill forever
# for unchanged files — undermining the self-healing intent. With it, a truly
# broken file is retried at most once per ``_FAILURE_RETRY_AFTER`` hours
# instead of every restart, and a recovered environment heals on its own.
_FAILURE_RETRY_AFTER = "-24 hours"


def _override_identity_matches(
        override_path, expected_size, expected_mtime_ns):
    """True when ``source_paths`` override is safe to substitute for the archive.

    The card-side override is trusted iff BOTH size and mtime match the
    identity captured when the file was copied. Size alone is not enough:
    a reused card mount or a rewritten source file can present the SAME
    byte length at the same path but with different content, and the
    extractor would then cache a working copy for the wrong bytes and set
    ``working_copy_path`` — normal backfill would never regenerate it
    from the verified archive copy. mtime narrows the trust window from
    "any file with the same size" to "the exact file we just copied":
    a rewrite by the camera or an OS-level file replacement bumps mtime;
    a remount of a different card presents mtimes from an unrelated
    session. Callers still need to guard against the (extremely rare)
    same-size-same-mtime coincidence by preferring the archive copy on
    any extraction failure — that retry lives in ``_extract_working_copies``.

    Any missing input (unknown expected size, unknown expected mtime,
    OSError from an unmounted card, or the file itself is gone) makes
    the override untrusted, so extraction falls back to the verified
    archive path.
    """
    if expected_size is None or expected_mtime_ns is None:
        return False
    try:
        st = os.stat(override_path)
    except OSError:
        return False
    return (
        st.st_size == int(expected_size)
        and st.st_mtime_ns == int(expected_mtime_ns)
    )


def _snapshot_row_still_current(db, row):
    """Return True if the candidate snapshot's identity still matches the DB.

    ``_extract_working_copies`` reads its rows up front and then processes
    them for minutes to days. During a pause the worker parks between rows;
    ``photos.id`` is not AUTOINCREMENT, so a row can be deleted and its id
    reused by a new import with different bytes while the loop is stopped.
    Callers use this to skip the row instead of extracting the deleted
    photo's source and attaching that JPEG to the replacement.

    The identity columns are not the whole snapshot the loop dereferences,
    so this re-reads two more values:

    * ``folders.path`` — the source is ``folder_path + filename``, and a
      relocated folder keeps its ``folder_id``. A stale path makes extraction
      fail against a file that has merely moved, and the identity-guarded
      failure UPDATE would then land (its four identity columns all still
      match) and stamp a spurious failure marker that suppresses this photo's
      backfill until its mtime changes or the retry grace expires.
    * ``companion_path`` — the RAW+JPEG companion is the second extraction
      source, so a row re-paired during the pause would be handed the
      previous companion's pixels.

    ``working_copy_path`` must also still be NULL. That is part of the
    candidate predicate, so a non-NULL value means another writer already
    published one and the multi-second decode would be pure waste.

    ``file_size``, ``file_mtime`` and ``companion_path`` may be NULL, so the
    SQL guard built from the identity columns uses ``IS`` (not ``=``).
    """
    current = db.working_copies.snapshot_row(row["id"])
    if current is None:
        return False
    return (
        current["working_copy_path"] is None
        and current["folder_id"] == row["folder_id"]
        and current["folder_path"] == row["folder_path"]
        and current["filename"] == row["filename"]
        and current["companion_path"] == row["companion_path"]
        and current["file_size"] == row["file_size"]
        and current["file_mtime"] == row["file_mtime"]
    )


def _working_copy_quota_mb(load_config):
    """The working-copy cache ceiling in MB from ``load_config()``."""
    try:
        return int(
            load_config().get("working_copy_cache_max_mb", 20480)
        )
    except (TypeError, ValueError):
        return 20480


def _configured_working_copy_quota_mb():
    """Re-read the saved ceiling; a settings write can change it mid-batch."""
    import config as cfg

    return _working_copy_quota_mb(cfg.load)


def _working_copy_enforce_threshold(limit_bytes):
    """New bytes a batch may write between mid-batch quota enforcements.

    Enforce roughly every quarter of the configured quota, capped at 512 MB
    so it doesn't run for every file on a small budget and doesn't defer
    eviction past ~2% of the ceiling on a large one. The 1 KB floor is only
    a safety net for tests with a near-zero quota.
    """
    return max(
        1024, min(limit_bytes // 4, 512 * 1024 * 1024),
    )


def _working_copy_id_from_path(path):
    """The photo id in a ``working/<id>.jpg`` path, or -1."""
    try:
        stem, _ = os.path.splitext(os.path.basename(path))
        return int(stem)
    except (ValueError, TypeError):
        return -1


def _record_published_identity(sink, published_path):
    """``on_publish`` hook: fingerprint the file extract just published.

    Runs WHILE ``extract_working_copy`` still holds the publication guard;
    a post-return stat is racy (see ``_WorkingCopyBatch._extract_row``).
    A failed stat records ``None`` — no proof of ownership.
    """
    from working_copy_cache import _file_identity

    try:
        sink.append(_file_identity(os.stat(published_path)))
    except OSError:
        sink.append(None)


@dataclass
class _RowSources:
    """Where one row's working copy can be read from."""

    catalog_primary: str
    primary_path: str
    primary_override_used: bool
    primary_is_raw: bool
    catalog_companion: str | None
    # The companion only when it exists on disk.
    companion_path: str | None
    companion_override_used: bool


@dataclass
class _RowExtraction:
    ok: bool
    # The file the last attempt read, for the failure log.
    source: str
    # ``working_copy_failed_source`` to record if every attempt failed.
    failure_source: str
    # The RAW failed and the companion JPEG produced the working copy.
    raw_failed_then_companion: bool
    # Fingerprints ``_record_published_identity`` captured under extract's
    # guard, one per publish.
    published_identities: list


@dataclass
class _RowPublication:
    # The published file is gone or no longer ours; nothing was committed.
    lost: bool = False
    fingerprint_mismatched: bool = False
    # The identity-guarded UPDATE matched no row; the output was discarded.
    orphaned: bool = False
    quota_lowered: bool = False
    retained_before_quota_lowering: set = field(default_factory=set)


class _WorkingCopyBatch:
    """One extraction pass over a snapshot of candidate rows.

    Bounds the cache while it writes: enforces the quota every
    ``incremental_threshold`` new bytes, stops once its own output fills
    the ceiling, marks the rows it never reached capacity-deferred, and
    trims any final overshoot from its lowest-priority output.
    """

    def __init__(
        self, db, vireo_dir, rows, *,
        wc_max_size, wc_quality, quota_mb, scope, source_paths,
        cancel_check, progress_callback, status_callback,
    ):
        self.db = db
        self.vireo_dir = vireo_dir
        self.rows = rows
        self.total = len(rows)
        self.wc_max_size = wc_max_size
        self.wc_quality = wc_quality
        self.scope = scope
        self.source_paths = source_paths
        self.cancel_check = cancel_check
        self.progress_callback = progress_callback
        self.status_callback = status_callback
        self.status_supports_phase = _status_callback_supports_phase(
            status_callback,
        )
        self._adopt_quota(quota_mb)

        self.new_bytes_since_enforce = 0
        # {abs_path: size} of the files this batch published and still holds.
        self.retained_new_files = {}
        # Parallel to ``retained_new_files``: {abs_path: _file_identity} of the
        # bytes THIS batch published. The post-loop DESC-priority trim verifies
        # each candidate against this fingerprint before unlinking so a
        # competing publisher (on-demand render, id reuse) that atomically
        # replaced our file between record time and guard acquisition survives
        # — the same identity check ``_evict_once`` and the orphan cleanup use.
        self.retained_new_identities = {}
        # Parallel to ``retained_new_files``: {abs_path: source-identity dict}
        # from the row snapshot at publish time. The post-loop trim's
        # deferred-marker UPDATE uses this identity in its WHERE so a
        # mid-batch companion re-pair or folder relocation cannot stamp
        # ``working_copy_evicted_mtime`` on a row whose extraction inputs
        # have since changed — the marker on the row's *previous* source
        # identity is what we want, since the deleted rendition was
        # generated from the previous state.
        self.retained_new_snapshots = {}
        self.stop_after_current = False

    def _adopt_quota(self, quota_mb):
        self.quota_mb = quota_mb
        self.quota_bytes = max(0, quota_mb) * 1024 * 1024
        self.incremental_threshold = _working_copy_enforce_threshold(
            self.quota_bytes
        )

    def _emit_progress(self, current):
        """Publish the secondary working-copy phase without replacing scan progress."""
        total = self.total
        unit = "working copy" if total == 1 else "working copies"
        message = (
            f"Generating working copies: {current:,} of {total:,}"
            if current
            else f"Generating {total:,} {unit}..."
        )
        if self.status_callback:
            _call_status_callback(
                self.status_callback,
                message,
                phase_current=current,
                phase_total=total,
                phase_label="Generating working copies",
                supports_phase=self.status_supports_phase,
            )
        if self.progress_callback is not None and current:
            self.progress_callback(current, total)

    def run(self):
        self._emit_progress(0)

        # Bound the transient overshoot while a large batch is running. A
        # library much larger than the configured quota would otherwise
        # generate the entire set before the post-loop enforce and fill the
        # disk by hundreds of gigabytes.
        from working_copy_cache import (
            _file_identity,
            evict_if_over_quota,
            working_copy_publication_guard,
            working_copy_stats,
        )

        self.file_identity = _file_identity
        self.evict_if_over_quota = evict_if_over_quota
        self.publication_guard = working_copy_publication_guard
        self.working_copy_stats = working_copy_stats

        # Commit per row so the writer lock is released between iterations.
        # Otherwise the first UPDATE in a batch auto-opens a transaction and
        # subsequent slow extract_working_copy() calls (RAW decode → JPEG
        # encode, multi-second per file on a slow disk) hold the writer lock
        # for the whole batch, starving any concurrent writer (e.g. a parallel
        # pipeline's add_photo INSERT past the 30s busy_timeout).
        for i, row in enumerate(self.rows, 1):
            if self.cancel_check is not None and self.cancel_check():
                log.info(
                    "Working-copy extraction cancelled after %d/%d rows",
                    i - 1, self.total,
                )
                break
            if not self._process_row(i, row):
                continue
            if self.stop_after_current and not self._resume_after_stop(i):
                break

        self._trim_overshoot()
        self._enforce_final_quota()

    # -- one row -----------------------------------------------------

    def _process_row(self, i, row):
        """Extract and publish one row; ``False`` when it ended early."""
        # ``cancel_check`` parks the worker on a pause request via
        # ``wait_if_paused``, so this snapshot row may have been picked up
        # hours ago. ``photos.id`` is not AUTOINCREMENT, so during the park
        # the row could have been deleted and its id reused by a new import
        # with entirely different bytes. Refetch the identity columns and
        # skip when the snapshot no longer matches — otherwise we would
        # extract the deleted photo's source path and attach that JPEG to
        # the replacement row's ``working_copy_path``.
        if not _snapshot_row_still_current(self.db, row):
            log.info(
                "Skipping working-copy extraction for photo %s: snapshot "
                "no longer matches the current row (deleted or replaced "
                "since candidate selection)",
                row["id"],
            )
            self._emit_progress(i)
            return False

        # Close the remaining gap between candidate selection and this file
        # read in case an alias is swapped while earlier rows are processed.
        if is_excluded_scan_path(row["folder_path"]):
            log.warning(
                "Skipping working-copy extraction inside excluded bundle: %s",
                row["folder_path"],
            )
            self._emit_progress(i)
            return False

        wc_rel = f"working/{row['id']}.jpg"
        wc_abs = os.path.join(self.vireo_dir, "working", f"{row['id']}.jpg")

        extraction = self._extract_row(row, wc_abs)
        publication = _RowPublication()
        if extraction.ok:
            publication = self._publish_row(row, wc_rel, wc_abs, extraction)
        else:
            self._mark_extraction_failed(row, extraction)

        if publication.lost:
            if publication.fingerprint_mismatched:
                log.info(
                    "Working copy for photo %s was atomically replaced by "
                    "a stale extractor (or a competing publisher) between "
                    "our publish and this catalog commit; the bytes at "
                    "%s do not match the fingerprint captured under "
                    "extract's guard. Leaving their bytes intact and "
                    "skipping this row's UPDATE",
                    row["id"], wc_abs,
                )
            else:
                log.info(
                    "Working copy for photo %s was evicted before its "
                    "catalog commit; leaving the eviction marker intact",
                    row["id"],
                )
            self._emit_progress(i)
            return False

        if publication.orphaned:
            # The bytes were removed under the guard; skip quota tracking
            # (nothing published) and move on. No eviction marker to leave
            # — the row that owned this id is gone, and the reused-id
            # replacement (if any) starts clean.
            self._emit_progress(i)
            return False

        if publication.quota_lowered:
            self._after_quota_lowered(
                publication.retained_before_quota_lowering,
            )

        self._emit_progress(i)

        if extraction.ok:
            self._track_published_bytes(
                row, wc_abs, extraction.published_identities,
            )
        return True

    def _resolve_sources(self, row):
        """Find the row's primary and companion files, preferring verified card copies.

        Working copies are the edit-quality source. For non-RAW primaries
        we still prefer the companion JPEG outright (fast path). For RAW
        primaries we decode the RAW with image_loader's highlight-preserving
        settings instead of baking in a camera JPEG that may already have
        clipped highlights — but if libraw cannot decode this RAW variant
        (and the embedded thumb is unusable too) we still fall back to the
        companion so an extractable JPEG copy isn't refused outright.
        """
        catalog_primary = os.path.join(row["folder_path"], row["filename"])
        primary_path, primary_override_used = self._card_override(
            catalog_primary,
        )
        primary_is_raw = (
            os.path.splitext(row["filename"])[1].lower() in RAW_EXTENSIONS
        )
        companion_path = None
        catalog_companion = None
        companion_override_used = False
        if row["companion_path"]:
            catalog_companion = os.path.join(
                row["folder_path"], row["companion_path"],
            )
            # Companion size is not on the RAW's row (pairing merged the
            # JPEG's photos row into the RAW's), but the import ledger
            # recorded the companion's card-side size and mtime at copy
            # time — use those to identity-check the override the same way
            # the primary does. Without this, a rewritten card-side JPEG
            # (or a remounted different card) would silently poison the
            # RAW's working copy via the companion-fallback path.
            candidate, companion_override_used = self._card_override(
                catalog_companion,
            )
            if os.path.isfile(candidate):
                companion_path = candidate
        return _RowSources(
            catalog_primary=catalog_primary,
            primary_path=primary_path,
            primary_override_used=primary_override_used,
            primary_is_raw=primary_is_raw,
            catalog_companion=catalog_companion,
            companion_path=companion_path,
            companion_override_used=companion_override_used,
        )

    def _card_override(self, catalog_path):
        """Return ``(path, used_override)`` for reading ``catalog_path``.

        ``source_paths`` overrides the catalog path with the card-side
        source so extraction reads local card bytes instead of the
        just-written archive copy over a slow NAS. Existence alone is not
        enough: if the card was unmounted, its mount point was reused for
        a different card, or the source file itself was rewritten between
        copy and this extraction pass, the override points at bytes that
        no longer match what the archive holds. Reading them here would
        cache a working copy for the wrong image and — because
        ``working_copy_path`` gets set — normal backfill would never
        regenerate it from the archive. Verify the override's size AND
        mtime against the identity captured by the import job at copy
        time (a rewritten source bumps mtime; a same-size collision on a
        reused card mount has an unrelated mtime); anything else (or a
        missing entry) falls back to the verified archive copy.
        """
        if self.source_paths:
            override_entry = self.source_paths.get(catalog_path)
            if override_entry:
                override_path, exp_size, exp_mtime_ns = override_entry
                if _override_identity_matches(
                    override_path, exp_size, exp_mtime_ns,
                ):
                    return override_path, True
        return catalog_path, False

    def _extract_row(self, row, wc_abs):
        """Write the row's working copy, falling back across its sources."""
        src = self._resolve_sources(row)
        if not src.primary_is_raw and src.companion_path:
            source = src.companion_path
            failure_source = "companion"
        else:
            source = src.primary_path
            failure_source = "source"

        # Fingerprint the file each successful ``extract_working_copy``
        # atomically publishes, so the orphan cleanup in
        # ``_discard_orphaned_publication`` can distinguish "our bytes
        # stayed put" from "a replacement publisher atomically replaced
        # ``wc_abs`` in the gap between extract's guard release and our
        # reacquire." The capture runs WHILE extract still holds the
        # publication guard (via ``on_publish``); a post-return stat is
        # racy — a competing publisher for a recycled id can atomically
        # replace ``wc_abs`` before we can stat, so we sample their
        # identity as ours, the cleanup sees no catalog owner yet (their
        # UPDATE is still waiting on our guard), and deletes their valid
        # bytes. Their extract then observes ``publication_lost`` and
        # cannot commit its otherwise-valid output.
        published_identities = []
        on_publish = functools.partial(
            _record_published_identity, published_identities,
        )

        # extract_working_copy is slow (RAW decode + JPEG encode); run it
        # before any DB write so no transaction is open while it runs.
        def extract(path):
            return extract_working_copy(
                path, wc_abs, max_size=self.wc_max_size,
                quality=self.wc_quality,
                publication_guard=self.publication_guard,
                on_publish=on_publish,
            )

        ok = extract(source)
        # Card-side override failed but the verified archive copy exists
        # — retry from the archive before falling through to the RAW→
        # companion fallback or recording a failure marker. Without this,
        # a transient card I/O error (or a card that was unmounted just
        # after the size check) would leave the row marked failed even
        # though the archive copy has the correct bytes.
        if not ok:
            source, ok = self._retry_from_archive(row, src, source, extract)

        raw_failed_then_companion = False
        if (
            ok
            and src.primary_is_raw
            and src.companion_path
            and row["width"]
            and row["height"]
        ):
            ok = self._raw_result_is_full_size(row, wc_abs, src.companion_path)
        if not ok and src.primary_is_raw and src.companion_path:
            log.info(
                "RAW working-copy extraction failed for photo %s (%s); "
                "falling back to companion JPEG %s",
                row["id"], src.primary_path, src.companion_path,
            )
            source = src.companion_path
            # Keep failure_source = "source": the RAW already failed, and
            # _has_current_working_copy_failure ignores "companion" markers
            # while both files exist — overwriting here would silently
            # un-shield request paths from the known RAW failure if the
            # companion extraction also fails.
            ok = extract(source)
            # Companion attempt used a card override and failed — retry
            # from the verified archive companion before recording failure.
            # Same rationale as the primary retry above: a transient card
            # I/O error or an unmounted card must not force this row into
            # a persistent failure marker when the archive copy is available.
            if (
                not ok
                and src.companion_override_used
                and src.catalog_companion
                and os.path.isfile(src.catalog_companion)
            ):
                log.info(
                    "Card-side companion extraction failed for photo %s "
                    "(%s); retrying from archive companion %s",
                    row["id"], source, src.catalog_companion,
                )
                source = src.catalog_companion
                ok = extract(source)
            raw_failed_then_companion = ok
        return _RowExtraction(
            ok=ok,
            source=source,
            failure_source=failure_source,
            raw_failed_then_companion=raw_failed_then_companion,
            published_identities=published_identities,
        )

    def _retry_from_archive(self, row, src, source, extract):
        """Retry a failed card-side read from the archive; ``(source, ok)``.

        Points ``src`` at the archive copy it switches to. Without an
        archive copy to retry from, returns ``source`` unchanged and False.
        """
        retry_source = None
        if source == src.primary_path and src.primary_override_used:
            retry_source = src.catalog_primary
        elif source == src.companion_path and src.companion_override_used:
            retry_source = src.catalog_companion
        if retry_source and os.path.isfile(retry_source):
            log.info(
                "Card-side working-copy extraction failed for photo "
                "%s (%s); retrying from archive %s",
                row["id"], source, retry_source,
            )
            source = retry_source
            if retry_source == src.catalog_primary:
                src.primary_path = src.catalog_primary
                src.primary_override_used = False
            else:
                src.companion_path = src.catalog_companion
                src.companion_override_used = False
            return source, extract(source)
        return source, False

    def _raw_result_is_full_size(self, row, wc_abs, companion_path):
        """Whether a RAW extraction is worth keeping over the companion.

        libraw returns an embedded JPEG when it can't demosaic a RAW; that
        preview is often a fraction of sensor resolution, so a successful
        extraction can still produce an undersized working copy. Treat a
        substantially undersized RAW extraction the same as a hard failure
        so the companion fallback replaces it instead of caching the
        downscaled preview.
        """
        # Stored width/height are the unrotated sensor axes; swap them
        # before scaling so portrait files (e.g. 6000x4000 + Orientation 6)
        # produce the same expected dimensions as the orientation-normalized
        # JPEG that extract_working_copy actually writes.
        oriented_w, oriented_h = _oriented_dimensions(
            row["width"], row["height"], row["exif_data"],
        )
        expected_w, expected_h = _scaled_dimensions(
            oriented_w, oriented_h, self.wc_max_size,
        )
        try:
            with Image.open(wc_abs) as _wc:
                wc_w, wc_h = _wc.size
        except Exception:
            # PIL couldn't open the file: corrupt / truncated /
            # unsupported. Treat as a failed extraction so the
            # companion fallback runs instead of caching an
            # unreadable working copy.
            log.info(
                "RAW working-copy extraction for photo %s produced "
                "an unreadable file %s; retrying from companion "
                "JPEG %s",
                row["id"], wc_abs, companion_path,
            )
            return False
        # Use a 1% relative tolerance (not the request paths' 1px slack):
        # libraw can emit the active image area a few pixels narrower than
        # the full sensor, and a strict check would mark a valid extraction
        # as failed and route every edited render away from the RAW.
        if is_undersized(
            wc_w, wc_h, expected_w, expected_h, abs_slack=0, rel_slack=0.01,
        ):
            log.info(
                "RAW working-copy extraction for photo %s produced "
                "undersized result (%dx%d, expected %dx%d); "
                "retrying from companion JPEG %s",
                row["id"], wc_w, wc_h, expected_w, expected_h,
                companion_path,
            )
            return False
        return True

    def _publish_row(self, row, wc_rel, wc_abs, extraction):
        """Commit a successful extraction to the catalog under the guard.

        ``extract_working_copy`` guards its atomic replace, but its guard
        ends before returning. A competing job may already have committed
        the same canonical path, allowing quota eviction to delete our
        replacement in that narrow gap. Reacquire the same guard,
        revalidate the published file, and hold it through the catalog
        commit so eviction can never leave a non-NULL path pointing at
        bytes it removed before this job committed.
        """
        publication = _RowPublication()
        with self.publication_guard():
            publication.lost, publication.fingerprint_mismatched = (
                self._publication_lost(
                    wc_abs, extraction.published_identities,
                )
            )
            if not publication.lost:
                # A settings request can lower the quota and finish its
                # one-time eviction while this slow extraction is still
                # encoding. Re-read the saved ceiling under the same
                # publication/eviction guard before exposing the row. If
                # it fell, enforce it before releasing the guard so this
                # stale batch cannot refill the cache after that request
                # returns.
                latest_quota_mb = _configured_working_copy_quota_mb()
                publication.quota_lowered = latest_quota_mb < self.quota_mb
                if publication.quota_lowered:
                    self._adopt_quota(latest_quota_mb)
                    publication.retained_before_quota_lowering = set(
                        self.retained_new_files
                    )

            update_cursor = None
            if not publication.lost:
                update_cursor = self._set_row_working_copy(
                    row, wc_rel, extraction.raw_failed_then_companion,
                )
            # Identity-guarded UPDATE matched no row: the snapshot's
            # source identity no longer matches the row at ``id``. Either
            # the row was deleted (id possibly reused by a new import) or
            # the file was rewritten during the slow decode.
            if update_cursor is not None and update_cursor.rowcount == 0:
                publication.orphaned = True
                self._discard_orphaned_publication(
                    row, wc_rel, wc_abs, extraction.published_identities,
                )
            if not publication.lost and not publication.orphaned:
                self.db.commit_with_retry()
                if publication.quota_lowered:
                    # Protect the batch's own writes here too — same
                    # reason as the mid-batch enforce. Without it, a
                    # settings drop mid-decode would sort by ASC
                    # mtime and reclaim the first-generated (highest-
                    # ID, newest-import) copies before the batch's
                    # ``removed_batch_files`` check stops the loop,
                    # leaving the currently-generated lower-ID photo
                    # covered instead. Leave ``wc_abs`` unprotected
                    # for the individually-oversized case, matching
                    # the mid-batch call.
                    self.evict_if_over_quota(
                        self.db, self.vireo_dir, quota_mb=self.quota_mb,
                        protect_paths=(
                            set(self.retained_new_files) - {wc_abs}
                        ),
                    )
        return publication

    def _publication_lost(self, wc_abs, published_identities):
        """Return ``(lost, fingerprint_mismatched)`` for the published file.

        Existence check first, then a fingerprint check against the
        identity captured INSIDE extract's guard (via ``on_publish``).
        ``extract_working_copy`` releases its guard before returning, so a
        stale extractor for a deleted-and-recycled id can atomically
        overwrite ``wc_abs`` between that release and our reacquire: the
        file exists, but its bytes are theirs, not ours. Committing
        ``working_copy_path=wc_rel`` would attach the stale extractor's
        pixels to this (current) row — the same class of harm the orphan
        cleanup guards for the rowcount==0 path, applied to the success
        path too.

        When the sink's last entry is ``None`` (the guarded stat itself
        failed inside extract's guard) we have no proof of ownership
        either — same treatment as the trim revalidation for
        ``expected_identity is None``: don't trust what we can't prove.
        An empty sink (extract never called ``on_publish``) skips the
        fingerprint check.
        """
        if not os.path.isfile(wc_abs):
            return True, False
        if published_identities:
            published_identity = published_identities[-1]
            try:
                current_identity = self.file_identity(os.stat(wc_abs))
            except OSError:
                current_identity = None
            if (
                published_identity is None
                or current_identity is None
                or current_identity != published_identity
            ):
                return True, True
        return False, False

    def _set_row_working_copy(self, row, wc_rel, raw_failed_then_companion):
        """Point the row at its new working copy; returns the UPDATE cursor.

        Extraction takes seconds per RAW; carry the snapshot's identity
        through the WHERE so an id reuse that happens between the
        pre-extract revalidate and this commit still cannot stamp the
        deleted row's working_copy_path onto the replacement. Guard on
        every input the extraction dereferences: (folder_id, filename,
        file_size, file_mtime) is not enough because a companion swap or a
        folder relocation during the multi-second decode leaves those four
        columns intact while ``companion_path`` and ``folders.path`` change
        under us — and ``companion_path`` is the second extraction source,
        so committing under a stale value can attach the previous
        companion's pixels to a re-paired row. Same NULL-safe guard used by
        the deferred-row and failure markers and by the pre-extraction
        revalidation in ``_snapshot_row_still_current``.
        ``working_copy_path IS NULL`` matches the candidate predicate so a
        concurrent on-demand writer that already committed for this id is
        not overwritten.
        """
        return self.db.working_copies.set_working_copy(
            row, wc_rel, raw_failed_then_companion=raw_failed_then_companion,
        )

    def _discard_orphaned_publication(
        self, row, wc_rel, wc_abs, published_identities,
    ):
        """Remove output whose row changed identity during extraction.

        The just-published bytes at ``working/<id>.jpg`` don't belong to
        whatever row now owns that id, so a later quota pass would treat
        them as the replacement's rendition — and if ``_evict_once`` then
        reclaims the file, it would stamp ``working_copy_evicted_mtime`` on
        the replacement, suppressing its own backfill until its source
        mtime or the quota changes. Remove the orphan under the same
        publication guard that would run eviction, and roll back the empty
        UPDATE so it doesn't hold an implicit transaction open.

        The publication guard released between our extract's atomic
        replace and this reacquire, so a publisher for a replacement row
        (id reuse, on-demand render) could have already atomically
        replaced ``wc_abs`` with its own bytes — and its own extract may
        still be running (its post-extract UPDATE waiting on our guard),
        so no catalog row yet claims ``wc_rel``. Unlinking unconditionally
        would delete that valid replacement, and the
        ``row_owned_by_replacement`` SELECT alone cannot see it either.
        Compare the current file's identity against the one captured
        INSIDE the extract's guard (via the ``on_publish`` callback) so the
        replacement's bytes survive whether it has committed yet or not.
        """
        file_still_ours = self._still_holds_published_bytes(
            wc_abs, published_identities,
        )
        row_owned_by_replacement = self.db.working_copies.path_is_claimed(wc_rel)
        if file_still_ours:
            # Our captured fingerprint matches the current bytes on disk.
            # Two cases collapse into the same action:
            #
            # * No row claims ``wc_rel`` — the bytes are an orphan from a
            #   deleted photo; unlink cleanly.
            # * A replacement row DID commit ``wc_rel``
            #   (``row_owned_by_replacement``) — that means WE atomically
            #   overwrote THEIR valid file with our stale bytes (we lost
            #   the id race but our extract kept running and published
            #   anyway). Preserving the file here would leave the
            #   replacement row serving OUR stale pixels indefinitely.
            #   Unlink so ``_evict_once``'s ``stale_tracked_ids``
            #   reconciles the replacement's row on its next pass and its
            #   own next request re-backfills.
            try:
                os.remove(wc_abs)
            except FileNotFoundError:
                pass
            except OSError:
                # Quota eviction will reclaim the bytes on a later pass;
                # log so a persistently unremovable file (a locked or
                # read-only cache dir) is visible rather than silently
                # accumulating.
                log.warning(
                    "Could not remove the orphaned working "
                    "copy at %s",
                    wc_abs, exc_info=True,
                )
            log.info(
                "Discarding orphaned working-copy output "
                "for photo %s: row identity changed during "
                "extraction; removed %s "
                "(row_owned_by_replacement=%s — replacement "
                "row will re-backfill after next quota "
                "reconciliation)",
                row["id"], wc_abs, row_owned_by_replacement,
            )
        else:
            log.info(
                "Discarding orphaned working-copy UPDATE "
                "for photo %s: row identity changed during "
                "extraction, and the canonical file at %s "
                "was atomically replaced by another "
                "publisher (file_still_ours=%s, "
                "row_owned_by_replacement=%s); leaving "
                "their bytes intact",
                row["id"], wc_abs, file_still_ours,
                row_owned_by_replacement,
            )
        try:
            self.db.rollback()
        except sqlite3.Error:
            # Nothing was written (rowcount 0), so a failed rollback costs
            # correctness nothing — the next commit_with_retry closes the
            # transaction.
            log.warning(
                "Rollback after an orphaned working-copy publish "
                "failed", exc_info=True,
            )

    def _still_holds_published_bytes(self, wc_abs, published_identities):
        """Whether ``wc_abs`` still holds the bytes this batch last published.

        False without a fingerprint captured under extract's guard, or when
        the file cannot be stat'd.
        """
        published_identity = (
            published_identities[-1] if published_identities else None
        )
        if published_identity is None:
            return False
        try:
            current_identity = self.file_identity(os.stat(wc_abs))
        except OSError:
            current_identity = None
        return current_identity == published_identity

    def _mark_extraction_failed(self, row, extraction):
        """Record the failure so the row is not retried every pass.

        Gated on current file_mtime so a future content change (mtime bump)
        clears the gate and we retry. The ``working_copy_failed_at``
        timestamp also expires the gate after ``_FAILURE_RETRY_AFTER`` so
        transient I/O / environment failures recover even if the file
        itself is unchanged. Logged at warning so the user sees that a
        specific file is the cause.
        """
        log.warning(
            "Working copy extraction failed for photo %s (%s); "
            "marked as failed and will retry on file change or after %s",
            row["id"], extraction.source, _FAILURE_RETRY_AFTER.lstrip("-"),
        )
        # Same identity guard as the success path: don't stamp a failure
        # marker on a row whose id was reused for a new photo while this
        # extraction was running, or whose companion or folder path changed
        # (relocation, re-pairing) — in either case the row that now owns
        # this id has different extraction inputs and the failure we saw
        # is not its failure. ``working_copy_path IS NULL`` so a concurrent
        # on-demand writer's successful publication is not clobbered with
        # a failure marker.
        self.db.working_copies.mark_failed(row, extraction.failure_source)
        self.db.commit_with_retry()

    # -- staying under the quota -------------------------------------

    def _drop_unverifiable_retained(self):
        """Forget retained files that are gone or have no fingerprint.

        Without a fingerprint we cannot prove the bytes are ours, so we
        must NOT keep the path in the retained set: keeping it would also
        keep it in ``protect_paths`` on the next ``evict_if_over_quota``,
        which would then reclaim pre-existing coverage to stay under the
        ceiling rather than the untrusted path.
        """
        self.retained_new_files = {
            path: size
            for path, size in self.retained_new_files.items()
            if os.path.isfile(path)
            and self.retained_new_identities.get(path) is not None
        }
        self.retained_new_identities = {
            path: identity
            for path, identity in self.retained_new_identities.items()
            if path in self.retained_new_files
        }
        self.retained_new_snapshots = {
            path: snapshot
            for path, snapshot in self.retained_new_snapshots.items()
            if path in self.retained_new_files
        }

    def _after_quota_lowered(self, retained_before_quota_lowering):
        self.new_bytes_since_enforce = 0
        self._drop_unverifiable_retained()
        removed_batch_files = (
            retained_before_quota_lowering - set(self.retained_new_files)
        )
        if self.quota_bytes <= 0 or removed_batch_files:
            self.stop_after_current = True

    def _track_published_bytes(self, row, wc_abs, published_identities):
        """Account for a published file and stop before overshooting.

        Track newly-generated bytes so the loop doesn't overshoot the
        quota by hundreds of gigabytes on a large backfill. A tiny wc_abs
        stat is cheap next to the RAW decode / JPEG encode that just ran.
        """
        try:
            new_bytes = os.path.getsize(wc_abs)
        except OSError:
            new_bytes = 0
        self.new_bytes_since_enforce += new_bytes
        if new_bytes:
            self.retained_new_files[wc_abs] = new_bytes
            # Record the identity captured under extract's guard (via
            # ``on_publish``) so the post-loop trim can tell our bytes
            # apart from a competing publisher's later atomic replace.
            # Falls back to a post-return stat if the callback didn't
            # fire — a minor regression window but never worse than the
            # pre-fingerprint behavior.
            if published_identities:
                self.retained_new_identities[wc_abs] = published_identities[-1]
            else:
                try:
                    self.retained_new_identities[wc_abs] = (
                        self.file_identity(os.stat(wc_abs))
                    )
                except OSError:
                    self.retained_new_identities[wc_abs] = None
            # Snapshot the row's source identity too. The trim's
            # deferred-marker UPDATE uses this identity in its WHERE so a
            # mid-batch companion re-pair or folder relocation doesn't
            # stamp ``working_copy_evicted_mtime`` from the row's NEW
            # source state (the deleted rendition was generated from the
            # OLD state, so the marker would suppress regeneration until
            # the primary mtime or the quota changes — a companion swap
            # alone may never do so).
            self.retained_new_snapshots[wc_abs] = {
                "folder_id": row["folder_id"],
                "filename": row["filename"],
                "companion_path": row["companion_path"],
                "file_size": row["file_size"],
                "file_mtime": row["file_mtime"],
                "folder_path": row["folder_path"],
            }

        if self.new_bytes_since_enforce >= self.incremental_threshold:
            self._enforce_mid_batch(wc_abs)

        # Once this batch alone has produced a full quota's worth of new
        # working copies, further generation would only displace files we
        # just wrote. Stop cleanly instead of churning disk.
        if (
            not self.stop_after_current
            and self.quota_bytes > 0
            and sum(self.retained_new_files.values()) >= self.quota_bytes
        ):
            self.stop_after_current = True

    def _enforce_mid_batch(self, wc_abs):
        """Enforce the quota mid-batch; stop the batch if it starts rotating."""
        retained_before_enforce = set(self.retained_new_files)
        enforcement = None
        try:
            # Protect this batch's own newly-written files from eviction —
            # but not the file we just wrote. Rows are processed DESC by id,
            # so the newest imports get the OLDEST wall-clock mtime in the
            # batch and would otherwise be first in line for
            # ``_evict_once``'s ascending-mtime pass — precisely the rows
            # the ordering was meant to prioritize. Leaving ``wc_abs``
            # unprotected preserves the individually-oversized case (a
            # single JPEG larger than the whole budget is still reclaimed
            # and the batch continues with later fitting candidates, per
            # ``removed_batch_files == {wc_abs}`` below). With this
            # protection, eviction reclaims pre-existing copies first
            # (caught by ``displaced_existing`` below) or, on an
            # empty/all-batch cache, reports overshoot via
            # ``remaining_bytes > max_bytes`` (caught below) so the batch
            # still stops cleanly without dropping its own
            # highest-priority coverage.
            enforcement = self.evict_if_over_quota(
                self.db, self.vireo_dir,
                protect_paths=set(self.retained_new_files) - {wc_abs},
            )
        except Exception:
            log.exception(
                "Working-copy quota enforcement failed mid-batch"
            )
        self.new_bytes_since_enforce = 0
        # Quota enforcement may have immediately reclaimed this rendition
        # (for example, when a single JPEG is larger than the whole budget)
        # or an earlier file from this batch. Only retained bytes should
        # move the batch toward its stop point; otherwise one oversized
        # file defers every later candidate, including copies that would
        # fit in the cache.
        self._drop_unverifiable_retained()
        # Once enforcement removes an earlier fitting rendition from this
        # same batch to keep the current one, retained capacity has started
        # rotating rather than growing. Continuing would decode every
        # remaining RAW only to evict another result. Do not treat removal
        # of only the current file as rotation: that is the
        # individually-oversized case, where later candidates may still fit
        # and must remain eligible.
        #
        # With ``protect_paths`` passed above, ``wc_abs`` is the only batch
        # file eligible for eviction; the ``any path != wc_abs`` check
        # therefore only fires when a batch write from before
        # ``protect_paths`` was introduced snuck through (kept for defence
        # in depth). The rotation signal that matters under protection is
        # below.
        removed_batch_files = (
            retained_before_enforce - set(self.retained_new_files)
        )
        if any(path != wc_abs for path in removed_batch_files):
            self.stop_after_current = True

        # DESC-ordering rotation under ``protect_paths``: eviction can only
        # reclaim ``wc_abs`` from the batch, so watching for
        # ``removed_batch_files != {wc_abs}`` (the old signal above) never
        # catches "batch has filled the cache with its own newer files and
        # is now rotating the just-written tail." When ``wc_abs`` was
        # reclaimed AND other batch files remain retained, every future
        # write will trip the same enforcement and land the same eviction —
        # pure decode waste. When ``retained_new_files`` is empty the only
        # removed file was an individually-oversized wc_abs (a single JPEG
        # larger than the whole budget); later smaller candidates may still
        # fit and the batch must remain eligible.
        if removed_batch_files == {wc_abs} and self.retained_new_files:
            log.info(
                "Working-copy batch rotated its own just-written "
                "file out to stay under the %.1f MB ceiling; "
                "deferring the rest rather than churning the tail",
                self.quota_bytes / 1024 / 1024,
            )
            self.stop_after_current = True

        self._stop_if_sweep_stalls(enforcement, retained_before_enforce)

    def _stop_if_sweep_stalls(self, enforcement, retained_before_enforce):
        """Stop the library-wide sweep once enforcement stops adding coverage."""
        # The same rotation, one step earlier: enforcement can stay under
        # the ceiling by reclaiming working copies this batch did not write
        # — the ones a previous run or an on-demand render already
        # published. Those deletions never shrink ``retained_new_files``,
        # so ``_enforce_mid_batch``'s rotation checks cannot see them, and
        # the batch would happily trade existing coverage for new coverage
        # file by file. That is not progress: total coverage is flat, and
        # because eviction sheds
        # the oldest stamps first it is spending the user's most recently
        # used copies to backfill photos they have not asked for. Defer the
        # rest.
        #
        # Only the library-wide pass. A scoped run is a scan or an import
        # of specific folders — the shoot the user just put on disk and is
        # about to cull — and displacing the least recently used old copy
        # to cover it is exactly what a cache is for. It is the unscoped
        # sweep, which has no such claim on being more important than what
        # is already cached, that must not churn.
        displaced_existing = self.scope is None and {
            path
            for path in (enforcement or {}).get("evicted_paths", ())
            if path not in retained_before_enforce
        }
        if displaced_existing:
            log.info(
                "Working-copy batch reclaimed %d pre-existing "
                "copies to stay under quota; deferring the rest "
                "rather than rotating the cache",
                len(displaced_existing),
            )
            self.stop_after_current = True

        # Protection can leave the cache above the ceiling when this batch
        # has already filled it with its own newest-import files: eviction
        # refused to reclaim them (as instructed), and there are no
        # pre-existing copies left to shed. That is the natural stop point
        # for a DESC-id backfill — the cache holds as much of the newest
        # tail as it can, and decoding more would only overshoot the
        # ceiling without displacing anything. A missing
        # ``remaining_bytes`` (snapshot deferral) is treated as "unknown,
        # do not force stop"; the mtime-rotation checks above still catch
        # that case.
        if enforcement is not None:
            remaining_bytes = enforcement.get("remaining_bytes")
            quota_bytes_from_enforcement = enforcement.get("quota_bytes")
            if (
                self.scope is None
                and remaining_bytes is not None
                and quota_bytes_from_enforcement is not None
                and remaining_bytes > quota_bytes_from_enforcement
            ):
                log.info(
                    "Working-copy batch has filled the cache with "
                    "its own newest-import files (%.1f MB over the "
                    "%.1f MB ceiling); deferring the rest rather "
                    "than overshooting further",
                    (
                        remaining_bytes
                        - quota_bytes_from_enforcement
                    ) / 1024 / 1024,
                    quota_bytes_from_enforcement / 1024 / 1024,
                )
                self.stop_after_current = True

    def _resume_after_stop(self, i):
        """Defer the rows the batch will not reach; ``True`` to keep going instead."""
        # A settings write may raise the ceiling while this slow batch is
        # decoding. Adopt the latest value before marking the remaining
        # rows as capacity-deferred; otherwise the settings handler can
        # clear old eviction markers and this stale batch can recreate
        # them afterward, suppressing future extraction indefinitely.
        latest_quota_mb = _configured_working_copy_quota_mb()
        if latest_quota_mb != self.quota_mb:
            self._adopt_quota(latest_quota_mb)
            self.stop_after_current = (
                self.quota_bytes <= 0
                or sum(self.retained_new_files.values()) >= self.quota_bytes
            )
            if not self.stop_after_current:
                return True

        log.info(
            "Working-copy batch reached quota (%d MB) after %d of %d "
            "candidates; deferring the rest to a later pass",
            self.quota_mb, i, self.total,
        )
        # Mark the remaining candidates as capacity-deferred so the next
        # explicit pass skips them while the quota is unchanged. Without
        # this, the next pass would decode and write another quota-sized
        # batch only to evict the batch we just committed — and repeat
        # indefinitely. Reuse ``working_copy_evicted_mtime``: the settings
        # write path already clears this marker when the user raises the
        # ceiling, and the candidate predicate treats a file_mtime change
        # as an escape hatch so a rewritten source retries automatically.
        # ``photos.id`` is not AUTOINCREMENT, so deleting the highest-ID
        # pending row while this long-running batch is decoding can let a
        # re-import reuse that id. Carry the full source identity captured
        # by the candidate snapshot through both writes; an id-only UPDATE
        # would otherwise stamp the replacement row's current mtime as
        # evicted and suppress its future extraction. A row that was
        # re-paired to a different companion or whose folder was relocated
        # while this batch ran has different extraction inputs; deferring
        # under the old snapshot would stamp ``working_copy_evicted_mtime``
        # on those new inputs and suppress backfill of the row's current
        # source until the quota or the primary mtime changes.
        remaining = self.rows[i:]
        if not remaining:
            return False
        self.db.working_copies.defer(remaining)
        self.db.commit_with_retry()

        return self._undo_deferral_if_quota_raised(remaining)

    def _undo_deferral_if_quota_raised(self, remaining):
        """Clear this pass's deferred markers if the ceiling rose meanwhile.

        ``True`` when it did and the batch keeps going.
        """
        # Close the remaining save/side-effect race: if the config
        # increased after ``_resume_after_stop``'s check but before its
        # commit, the settings handler may already have performed its
        # one-time marker clear. Re-read after committing and undo only this pass's
        # deferred markers when the new capacity can hold more of the
        # batch. If the save happens after this read, its own side effect
        # clears them instead.
        refreshed_quota_mb = _configured_working_copy_quota_mb()
        refreshed_quota_bytes = max(0, refreshed_quota_mb) * 1024 * 1024
        if not (
            refreshed_quota_mb > self.quota_mb
            and sum(self.retained_new_files.values()) < refreshed_quota_bytes
        ):
            return False
        # Same complete identity guard as ``_resume_after_stop``'s deferred
        # write: a companion re-pair or folder relocation between the
        # deferred write and this undo must not cause the undo to match — the marker on
        # the row's *previous* source identity is what we wrote, and the
        # row now has different extraction inputs whose backfill the marker
        # never suppressed.
        self.db.working_copies.undo_deferral(remaining)
        self.db.commit_with_retry()
        self._adopt_quota(refreshed_quota_mb)
        self.stop_after_current = False
        return True

    # -- after the loop ----------------------------------------------

    def _trim_overshoot(self):
        """Trim the batch's own overshoot, lowest-priority output first.

        The ``sum(retained_new_files.values()) >= quota_bytes`` stop fires
        as soon as the batch's own footprint hits the ceiling, but can
        leave a one-file overshoot when generated sizes don't divide the
        ceiling evenly (ten ~110 KB files under a 1 MB quota, say). If we
        defer to the unconditional ``evict_if_over_quota`` afterwards, it
        sorts by ascending mtime and reclaims the first-generated,
        highest-ID copy — the newest import — undoing the DESC-ID priority
        the batch was built around. Trim the overshoot ourselves in
        ascending-ID order (lowest-priority batch files first) so the
        newest imports survive. ``evict_if_over_quota`` reconciles the
        now-missing ``working_copy_path`` rows on its next pass.
        """
        if not (self.retained_new_files and self.quota_bytes > 0):
            return
        # Take the retained-files revalidation, the usage snapshot, the
        # quota re-read, and the trim decision all under
        # ``working_copy_publication_guard``. Every concurrent path that
        # touches the cache — on-demand publishers, quota eviction,
        # ``_settings_post_save_side_effects`` — takes that same lock, so
        # holding it here makes the (retained, existing_bytes,
        # quota_bytes, batch_over) tuple atomic with those concurrent
        # mutations.
        #
        # Without this: an on-demand render publishing after an unguarded
        # ``working_copy_stats`` but before the guard acquisition would
        # leave ``existing_bytes`` short, ``batch_over`` could go negative,
        # the trim declines to act, and the final ``evict_if_over_quota``
        # — which protects the batch — then reclaims the unaccounted bytes
        # from pre-existing coverage. That is the exact cannibalization
        # this path is meant to prevent.
        with self.publication_guard():
            self._revalidate_retained()
            batch_bytes = sum(self.retained_new_files.values())
            existing_bytes = 0
            if self.scope is None:
                existing_bytes = self._cache_bytes_outside_batch(batch_bytes)
            # Re-read the ceiling under the same guard. A settings save
            # that *raises* the quota runs ``_settings_post_save
            # _side_effects``, which clears every capacity marker; a read
            # outside the guard could see the stale lower value if the
            # raise lands between our read and our guard acquisition — the
            # settings-side clear would then complete first and this trim
            # would delete files and re-stamp markers on rows the user's
            # raise was meant to cover. The mid-batch stop path already
            # re-reads for exactly this reason.
            refreshed_quota_mb = _configured_working_copy_quota_mb()
            if refreshed_quota_mb > 0:
                self.quota_bytes = max(0, refreshed_quota_mb) * 1024 * 1024
            batch_over = existing_bytes + batch_bytes - self.quota_bytes
            if batch_over > 0:
                trimmed = self._remove_lowest_priority(batch_over)
                if trimmed:
                    self._mark_trimmed_deferred(trimmed)

    def _revalidate_retained(self):
        """Drop retained entries whose file is gone, replaced or unproven.

        Between publish and this trim, a competing publisher (on-demand
        render, id reuse, ``_evict_once``) can remove or atomically replace
        a canonical path we recorded. Counting those stale bytes inflates
        ``batch_over`` — e.g. ten 110 KB files against a 1 MB quota, one
        already reclaimed elsewhere: the stale sum still reads 1.1 MB,
        ``existing_bytes`` (measured under the same guard) has shed the
        110 KB, so ``batch_over`` reports a phantom 100 KB and the trim
        deletes a second surviving output and marks its row
        capacity-deferred for no reason. Dropping stale entries first
        keeps ``batch_bytes`` in sync with the same atomic filesystem
        snapshot ``working_copy_stats`` is about to take.

        Also drop entries with no captured publisher fingerprint.
        ``_record_published_identity`` records ``None`` on a stat failure
        (and the fallback stat in ``_track_published_bytes`` can fail the
        same way) — without a fingerprint we cannot tell our
        just-published bytes apart from a competing publisher that
        atomically replaced the same canonical path between publish and
        this trim. Trusting the path unconditionally would let the trim
        unlink the replacement's valid bytes. A later
        ``evict_if_over_quota`` pass will reclaim them by sampled identity
        if quota still needs the space.
        """
        for stale_path in list(self.retained_new_files):
            expected_identity = self.retained_new_identities.get(stale_path)
            try:
                current_identity = self.file_identity(os.stat(stale_path))
            except OSError:
                current_identity = None
            if (
                expected_identity is None
                or current_identity is None
                or current_identity != expected_identity
            ):
                self.retained_new_files.pop(stale_path, None)
                self.retained_new_identities.pop(stale_path, None)
                self.retained_new_snapshots.pop(stale_path, None)

    def _cache_bytes_outside_batch(self, batch_bytes):
        """Measure what was already cached before this batch.

        Comparing only the batch against the whole ceiling reports no
        overshoot whenever the batch alone fits — but a sweep landing on a
        cache that is already near the ceiling still pushes total usage
        over it, and the final pass then pays for the batch's new coverage
        with somebody's pre-existing coverage. That is the exact trade
        ``displaced_existing`` refuses mid-batch, and it must not sneak
        back in after the loop. A batch smaller than
        ``incremental_threshold`` never runs the guarded mid-batch
        enforcement at all, so this is the only place the guarantee can be
        honoured for small sweeps.

        Protecting the batch on the final pass does not achieve this on
        its own: it makes pre-existing files the *only* eviction
        candidates. The batch has to give up its own lowest-priority
        output instead.
        """
        try:
            return max(
                0,
                self.working_copy_stats(self.vireo_dir)["size"] - batch_bytes,
            )
        except Exception:
            # Accounting failure must not strand the batch; fall back to
            # the batch-only comparison the trim used before.
            log.exception(
                "Working-copy usage accounting failed; trimming "
                "against the batch footprint alone"
            )
            return 0

    def _remove_lowest_priority(self, batch_over):
        """Unlink batch output in ascending id order until under the ceiling.

        Returns ``[(photo_id, snapshot)]`` for the files removed.
        """
        trimmed = []
        for path, size in sorted(
            list(self.retained_new_files.items()),
            key=lambda kv: _working_copy_id_from_path(kv[0]),
        ):
            if batch_over <= 0:
                break
            # Verify the file on disk is STILL the one this batch
            # published. Between the moment we recorded it in
            # ``retained_new_files`` and this point, another publisher
            # (on-demand render, id reuse) could have atomically replaced
            # ``path`` and committed its own ``working_copy_path``.
            # ``retained_new_files`` alone stored only the size, so even
            # with the publication guard held we would happily unlink their
            # newer rendition and then clear its valid catalog entry via
            # the id+path UPDATE in ``_mark_trimmed_deferred``. Compare
            # identity the same way ``_evict_once`` and the orphan cleanup
            # do; a mismatch means our bytes are gone and the file now
            # belongs to someone else, so skip.
            expected_identity = self.retained_new_identities.get(path)
            try:
                current_identity = self.file_identity(os.stat(path))
            except OSError:
                current_identity = None
            if (
                expected_identity is not None
                and current_identity is not None
                and current_identity != expected_identity
            ):
                log.info(
                    "Post-loop DESC-priority trim skipping %s: "
                    "identity changed since publish (a competing "
                    "publisher has replaced this canonical path)",
                    path,
                )
                self.retained_new_files.pop(path, None)
                self.retained_new_identities.pop(path, None)
                continue
            try:
                os.remove(path)
            except FileNotFoundError:
                self.retained_new_files.pop(path, None)
                self.retained_new_identities.pop(path, None)
                continue
            except OSError:
                log.warning(
                    "Post-loop DESC-priority trim could not remove %s",
                    path, exc_info=True,
                )
                continue
            # Take the row snapshot BEFORE popping — used for the full
            # source-identity guard on the deferred-marker UPDATE.
            trimmed_snapshot = self.retained_new_snapshots.get(path)
            self.retained_new_files.pop(path, None)
            self.retained_new_identities.pop(path, None)
            self.retained_new_snapshots.pop(path, None)
            batch_over -= size
            trimmed.append(
                (_working_copy_id_from_path(path), trimmed_snapshot),
            )
        return trimmed

    def _mark_trimmed_deferred(self, trimmed):
        """Mark trimmed rows capacity-deferred, like rows the batch never reached.

        Without this the rows come back as ``working_copy_path IS NULL``
        with no marker — ``_evict_once``'s stale-tracked reconciliation
        deliberately clears the path so a lost DB transition can
        regenerate. A later explicit pass would select those same
        candidates again, re-decode the same lowest-priority rows, and
        trim them again: the exact "decode a quota-sized batch only to
        evict it, and repeat indefinitely" loop the deferred marker exists
        to prevent.

        The full snapshot-identity guard (folder_id, filename,
        companion_path, file_size, file_mtime, folders.path) matches the
        success/deferred UPDATEs elsewhere in this batch: if a concurrent
        scan changed the row's companion, folder path, size, or source
        mtime between publish and this UPDATE, the row's extraction inputs
        no longer match what the deleted rendition was generated from, so
        the marker (which is tied to the previous state) must not land —
        otherwise the candidate predicate suppresses regeneration for the
        row's NEW inputs until quota or primary mtime changes (a companion
        swap alone may never do so). ``working_copy_path IS ?`` also
        matches the concurrent-publisher case already covered by the
        identity skip in ``_remove_lowest_priority``.

        ``working_copy_evicted_mtime`` records the snapshot's
        ``file_mtime`` — the mtime the deleted rendition was generated
        from — so a future source rewrite (a real content change) clears
        the gate. Using the row's *current* ``file_mtime`` would tie the
        marker to the new state and a future ``file_mtime`` bump to the
        snapshot value would fail to clear it.
        """
        self.db.working_copies.defer_trimmed(trimmed)
        self.db.commit_with_retry()

    def _enforce_final_quota(self):
        """Enforce the quota once after the batch.

        A scan/import may add a large batch at once. Enforce once after the
        batch so the quota stays a hard steady-state ceiling without
        rescanning the working directory after every generated file.

        Protect the batch's own retained files for unscoped runs so
        pre-existing coverage gets reclaimed before the newest imports we
        just wrote. Without this, a batch whose own footprint stays under
        ``incremental_threshold`` never runs the mid-batch enforcement (and
        never trips ``displaced_existing``), the DESC-priority trim only
        fires when the batch alone overshoots, and this final unprotected
        pass would then sort by ASC mtime and reclaim hundreds of MB of
        pre-existing coverage — defeating the batch's non-cannibalization
        behavior. Scoped runs keep the old semantics (import shoots are
        explicitly allowed to displace older cache).
        """
        try:
            if self.scope is None and self.retained_new_files:
                self.evict_if_over_quota(
                    self.db, self.vireo_dir,
                    protect_paths=set(self.retained_new_files),
                )
            else:
                self.evict_if_over_quota(self.db, self.vireo_dir)
        except Exception:
            # The working copies themselves are valid even if quota
            # maintenance hits a transient filesystem/database error. Keep
            # the scan result and retry enforcement at startup or on the
            # next config/cache write.
            log.exception("Working-copy quota enforcement failed")
