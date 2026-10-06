"""Shared helpers for the preview_cache LRU.

The eviction pass is called from three places: the Flask request path
(``_serve_preview``), the startup migration in ``create_app``, and the
pipeline job's preview stage. Keeping the logic here avoids having the
pipeline import from ``app`` (which would be circular) or duplicating
the loop in two modules.
"""

import glob as _glob
import logging
import os
import shutil

log = logging.getLogger(__name__)

# The fixed ``/photos/<id>/preview?size=N`` tiers the server allows at all.
# ``allowed_preview_sizes`` serves this plus ``preview_max_size`` so UI
# surfaces besides the lightbox (pipeline review's resolution slider,
# browse's preview-size picker) can request these explicitly.
PREVIEW_TIER_SIZES = (1920, 2560, 3840)

# The tiers the lightbox actually steps through at fit. ``_lbPickSourceKey``
# in ``static/lightbox/source-loading.js`` only has explicit 2560 and 3840
# choices — below the first it serves ``/full`` (``preview_max_size``),
# past 3840 it falls back to ``/original``. 1920 never appears, so a
# ``preview_max_size`` under 1920 jumps straight from ``/full`` to 2560.
LIGHTBOX_FIT_TIER_SIZES = (2560, 3840)


def lightbox_fit_preview_sizes(preview_max_size):
    """Preview sizes the lightbox can request before falling back to /original.

    ``preview_max_size == 0`` makes ``/full`` redirect to ``/original``, so
    no preview size is in play. Otherwise ``/full`` serves
    ``preview_max_size`` and the lightbox only picks a fixed tier larger
    than that. The 1920 tier is omitted because ``_lbPickSourceKey`` never
    requests it: warming it just wastes an extra decode and cache slot.
    """
    if preview_max_size == 0:
        return []
    full = int(preview_max_size or 1920)
    return [full] + [size for size in LIGHTBOX_FIT_TIER_SIZES if size > full]


def cleanup_cached_files_for_deleted_photos(
    thumb_cache_dir, files, progress_callback=None, vireo_dir=None,
):
    """Remove thumbnail, preview, working-copy, and display files for deleted photos.

    ``files`` is the list returned by ``db.delete_photos`` /
    ``db.delete_folder``. The FK cascade drops preview_cache rows when
    photos are deleted, but the on-disk files stay unless we unlink
    them here — otherwise they leak into untracked bytes that eviction
    can't see, and on SQLite a retry that reuses one of the just-freed
    photo IDs would treat the stale ``{photo_id}.jpg`` as a valid
    thumbnail and skip regenerating it.

    Note: if an unlink fails (e.g. file locked on Windows), the file
    remains on disk as an orphan because the cascade has already removed
    the preview_cache row. "Clear cache" in Settings recovers by globbing
    the directory.

    ``--db`` and ``--thumb-dir`` are independently configurable, so
    ``thumb_cache_dir`` is not necessarily ``<vireo_dir>/thumbnails`` —
    ``audit.import_untracked`` takes both separately for exactly this
    reason. Callers that know the real cache root must pass ``vireo_dir``;
    every other family (previews, working copies, masks, offline, DNG)
    lives under it, and guessing ``dirname(thumb_cache_dir)`` would look
    for them beside the thumbnails instead. The fallback keeps the
    conventional layout working for callers that only have one path.
    """
    vireo_dir = vireo_dir or os.path.dirname(thumb_cache_dir)
    preview_dir = os.path.join(vireo_dir, "previews")
    working_dir = os.path.join(vireo_dir, "working")
    originals_dir = os.path.join(vireo_dir, "originals")
    masks_dir = os.path.join(vireo_dir, "masks")
    external_dng_dir = os.path.join(vireo_dir, "external-dng")
    external_edits_dir = os.path.join(vireo_dir, "external-edits")
    inat_uploads_dir = os.path.join(vireo_dir, "inat-uploads")
    # Offline-cache layout: offline/{originals,xmp,companions}/{pid}{ext}.
    # The FK cascade drops the offline_originals row when the photo is
    # deleted, so we lose the exact stored paths — glob by photo id to
    # cover any source extension and any sidecar/companion that was
    # copied alongside it.
    offline_dirs = [
        os.path.join(vireo_dir, "offline", "originals"),
        os.path.join(vireo_dir, "offline", "xmp"),
        os.path.join(vireo_dir, "offline", "companions"),
    ]
    total = len(files)
    for idx, f in enumerate(files, start=1):
        pid = f["photo_id"]
        # {id}.jpg lives in these dirs as a legacy full preview, thumbnail,
        # working copy, or prepared full-resolution render. {id}_{size}.jpg
        # is used for sized preview variants.
        for d in [thumb_cache_dir, preview_dir, working_dir, originals_dir]:
            cached = os.path.join(d, f"{pid}.jpg")
            if os.path.isfile(cached):
                try:
                    os.remove(cached)
                except OSError as e:
                    log.warning(
                        "Failed to remove cached file %s after photo "
                        "delete — will be reclaimed by Clear Cache: %s",
                        cached, e,
                    )
        # Paired RAW+JPEG views keep source-specific thumbnail variants next
        # to the legacy/default thumbnail. They are disposable derivatives
        # and must follow the photo out of the cache on delete as well.
        for variant in _glob.glob(os.path.join(thumb_cache_dir, f"{pid}_*.jpg")):
            try:
                os.remove(variant)
            except OSError as e:
                log.warning(
                    "Failed to remove thumbnail variant %s after photo "
                    "delete — will be reclaimed by Clear Cache: %s",
                    variant, e,
                )
        for prepared_render in _glob.glob(
            os.path.join(originals_dir, f"{pid}_*.jpg")
        ):
            try:
                os.remove(prepared_render)
            except OSError as e:
                log.warning(
                    "Failed to remove cached file %s after photo delete — "
                    "will be reclaimed by Clear Cache: %s",
                    prepared_render, e,
                )
        for name in (f"{pid}.display.jpg",):
            cached = os.path.join(originals_dir, name)
            if os.path.isfile(cached):
                try:
                    os.remove(cached)
                except OSError as e:
                    log.warning(
                        "Failed to remove cached original rendition %s after "
                        "photo delete — will be reclaimed by Clear Cache: %s",
                        cached, e,
                    )
        for variant in _glob.glob(os.path.join(preview_dir, f"{pid}_*.jpg")):
            try:
                os.remove(variant)
            except OSError as e:
                log.warning(
                    "Failed to remove preview variant %s after photo "
                    "delete — will be reclaimed by Clear Cache: %s",
                    variant, e,
                )
        for d in offline_dirs:
            for orphan in _glob.glob(os.path.join(d, f"{pid}.*")):
                try:
                    os.remove(orphan)
                except OSError as e:
                    log.warning(
                        "Failed to remove offline cache file %s after "
                        "photo delete — will be reclaimed by Clear "
                        "Cache: %s",
                        orphan, e,
                    )
        # Subject masks (``masks/{pid}.png``, ``masks/{pid}.{variant}.png``)
        # are id-keyed like everything above. photos.mask_path goes away
        # with the row, so a leftover file is invisible to the DB but is
        # picked back up by any photo that later inherits the id — the
        # pipeline would score the new photo against another photo's mask.
        #
        # ``edit-masks/{pid}.{ref}.png`` is deliberately NOT purged here.
        # Those snapshots are content-addressed: the filename carries a
        # photo id, but ``load_snapshot`` only reads one when the photo's
        # own recipe names that ``ref``, and a ref is a hash of the mask
        # bytes. A recycled id therefore can't surface a previous owner's
        # snapshot — a brand-new photo has no local section at all, and a
        # ref match would mean byte-identical masks. Deleting them by id
        # only creates a data-loss window: ``transfer_snapshots`` swallows
        # OSError when a rename fails (locked destination on Windows), so
        # an id-keyed purge in the companion-merge path would destroy the
        # sole remaining copy of a snapshot whose recipe has already been
        # reassigned to the primary, silently disabling that local
        # adjustment forever. ``local_masks.gc_edit_masks`` owns this
        # directory instead and reaps by ref — the correct key — with a
        # grace period and a re-stat guard.
        for orphan in _glob.glob(
            os.path.join(masks_dir, f"{pid}.png")
        ) + _glob.glob(os.path.join(masks_dir, f"{pid}.*.png")):
            try:
                os.remove(orphan)
            except OSError as e:
                log.warning(
                    "Failed to remove mask file %s after photo delete "
                    "— will be reclaimed by Clear Cache: %s",
                    orphan, e,
                )
        # ``external-edits/<pid>.jpg`` is the render handed to an external
        # editor, and ``<pid>.json`` is its cache key — recipe, source path,
        # source mtime, edit-math version. No photo id or content hash, so a
        # recycled id re-imported at the same path with a preserved mtime and
        # the same recipe matches the previous owner's metadata and the old
        # render gets handed to the editor.
        for name in (f"{pid}.jpg", f"{pid}.json"):
            handoff = os.path.join(external_edits_dir, name)
            if os.path.isfile(handoff):
                try:
                    os.remove(handoff)
                except OSError as e:
                    log.warning(
                        "Failed to remove external-edit handoff %s after "
                        "photo delete — will be reclaimed by Clear Cache: %s",
                        handoff, e,
                    )
        # ``inat-uploads/<pid>.jpg`` is the analogous handoff to iNaturalist,
        # and ``<pid>.json`` caches the same {recipe, source_path,
        # source_mtime, edit_math_version} envelope. Same failure mode:
        # ``_inat_upload_photo_path`` never checks the file mtime, so a
        # recycled id imported at the same path/mtime with the same recipe
        # would ship the previous owner's pixels to iNaturalist.
        for name in (f"{pid}.jpg", f"{pid}.json"):
            handoff = os.path.join(inat_uploads_dir, name)
            if os.path.isfile(handoff):
                try:
                    os.remove(handoff)
                except OSError as e:
                    log.warning(
                        "Failed to remove iNat upload handoff %s after "
                        "photo delete — will be reclaimed by Clear Cache: %s",
                        handoff, e,
                    )
        # ``external-dng/<pid>/<stem>.dng`` caches DNG conversions per
        # photo id for the Nikon-HE-NEF external-editor path. The
        # freshness check there is source-mtime-based, so a recycled id
        # with the same basename and older source would silently reuse
        # the previous owner's DNG. Wipe the whole per-id directory.
        pid_external_dng = os.path.join(external_dng_dir, str(pid))
        if os.path.isdir(pid_external_dng):
            try:
                shutil.rmtree(pid_external_dng)
            except OSError as e:
                log.warning(
                    "Failed to remove external-dng cache directory %s "
                    "after photo delete — will be reclaimed by Clear "
                    "Cache: %s",
                    pid_external_dng, e,
                )
        if progress_callback:
            progress_callback(idx, total, f.get("filename") or str(pid))


def _recycled_id_probe_paths(thumb_cache_dir, photo_id, vireo_dir=None):
    """One exact (non-globbed) path per id-keyed derivative family.

    Used as a cheap existence probe before the full globbing purge — see
    ``purge_cached_files_for_recycled_id``. These are the "legacy" (bare
    id, no variant suffix) names — the variant patterns are covered by
    ``_recycled_id_probe_patterns``.
    """
    vireo_dir = vireo_dir or os.path.dirname(thumb_cache_dir)
    return (
        os.path.join(thumb_cache_dir, f"{photo_id}.jpg"),
        os.path.join(vireo_dir, "working", f"{photo_id}.jpg"),
        os.path.join(vireo_dir, "previews", f"{photo_id}.jpg"),
        os.path.join(vireo_dir, "originals", f"{photo_id}.display.jpg"),
        os.path.join(vireo_dir, "masks", f"{photo_id}.png"),
        os.path.join(vireo_dir, "external-edits", f"{photo_id}.jpg"),
        os.path.join(vireo_dir, "inat-uploads", f"{photo_id}.jpg"),
        # ``external-dng/<pid>/`` is a per-photo-id *directory* rather than
        # a file — ``os.path.exists`` returns True for directories, so
        # the same machinery covers it. The batch index's directory sweep
        # (``RecycledIdIndex._build``) also enumerates its subdirectories.
        os.path.join(vireo_dir, "external-dng", str(photo_id)),
    )


def _recycled_id_probe_patterns(thumb_cache_dir, photo_id, vireo_dir=None):
    """Glob patterns covering the id-keyed derivative *variants*.

    The exact-path probe above misses the common case where a family
    exists only in a variant form: sized previews (``previews/7_1920.jpg``,
    the standard shape written by ``scanner._extract_previews``), source-
    specific thumbnails (``thumbnails/7_raw.jpg`` / ``7_jpeg.jpg``),
    prepared full-res renders (``originals/7_1920.jpg``), model-scoped
    subject masks (``masks/7.sam2-large.png``), and offline originals
    (``offline/originals/7.NEF``). Every one of these is unlinked by
    ``cleanup_cached_files_for_deleted_photos`` on the delete side, so
    the *probe* must also see them or the purge silently returns without
    firing and the new photo serves the previous owner's pixels through
    the request paths' lazy-adoption shortcuts.

    Callers use ``glob.iglob(pattern)`` with ``next(..., None)`` so each
    pattern short-circuits at the first hit — a fresh library pays only
    the ``os.scandir`` open on each empty derivative dir.
    """
    vireo_dir = vireo_dir or os.path.dirname(thumb_cache_dir)
    return (
        os.path.join(thumb_cache_dir, f"{photo_id}_*.jpg"),
        os.path.join(vireo_dir, "previews", f"{photo_id}_*.jpg"),
        os.path.join(vireo_dir, "originals", f"{photo_id}_*.jpg"),
        os.path.join(vireo_dir, "masks", f"{photo_id}.*.png"),
        os.path.join(vireo_dir, "external-edits", f"{photo_id}.json"),
        os.path.join(vireo_dir, "inat-uploads", f"{photo_id}.json"),
        os.path.join(vireo_dir, "offline", "originals", f"{photo_id}.*"),
        os.path.join(vireo_dir, "offline", "xmp", f"{photo_id}.*"),
        os.path.join(vireo_dir, "offline", "companions", f"{photo_id}.*"),
    )


def _recycled_id_has_stale_derivative(
    thumb_cache_dir, photo_id, vireo_dir=None, unreadable_dirs=None,
):
    """True when *any* id-keyed derivative for ``photo_id`` still exists.

    Exact paths (cheap ``stat``) first; falls through to variant globs
    only if none of the legacy names matched.

    Single-id probe — for the *batch* case (ingest, which asks this once
    per inserted photo) use :class:`RecycledIdIndex` instead. The variant
    globs enumerate a whole directory when they don't match, so asking
    per-photo across a populated cache is O(new photos × cached files):
    measured at 117 ms per miss against a 76k-thumbnail library, i.e.
    ~10 minutes of pure ``scandir`` for a 5000-photo import.

    ``unreadable_dirs`` lets ``RecycledIdIndex`` pass through the set of
    directories its own sweep couldn't enumerate. Variant patterns
    targeting those dirs would silently miss under ``glob.iglob`` (same
    ``scandir`` that already failed), so treat their presence as
    "unknown, assume a derivative exists" and return True — the purge is
    the conservative move; a spurious purge is cheap, a skipped one
    silently serves the previous owner's pixels.
    """
    for p in _recycled_id_probe_paths(thumb_cache_dir, photo_id, vireo_dir):
        if os.path.exists(p):
            return True
    unreadable = set()
    if unreadable_dirs:
        unreadable = {os.path.normpath(d) for d in unreadable_dirs}
    for pattern in _recycled_id_probe_patterns(
        thumb_cache_dir, photo_id, vireo_dir,
    ):
        if unreadable and os.path.normpath(os.path.dirname(pattern)) in unreadable:
            # The batch sweep couldn't enumerate this directory, so
            # ``glob.iglob`` would fail too. Assume a variant may be
            # hiding there and force the purge.
            return True
        if next(_glob.iglob(pattern), None) is not None:
            return True
    return False


def _derivative_dirs(thumb_cache_dir, vireo_dir=None):
    """Every directory that stores files keyed by bare photo id."""
    vireo_dir = vireo_dir or os.path.dirname(thumb_cache_dir)
    return (
        thumb_cache_dir,
        os.path.join(vireo_dir, "previews"),
        os.path.join(vireo_dir, "working"),
        os.path.join(vireo_dir, "originals"),
        os.path.join(vireo_dir, "masks"),
        os.path.join(vireo_dir, "external-edits"),
        os.path.join(vireo_dir, "inat-uploads"),
        os.path.join(vireo_dir, "offline", "originals"),
        os.path.join(vireo_dir, "offline", "xmp"),
        os.path.join(vireo_dir, "offline", "companions"),
    )


def _leading_photo_id(name):
    """Parse the photo id a derivative filename is keyed by, or ``None``.

    Every derivative name is the bare id followed by ``.`` or ``_`` and
    then a suffix / variant: ``4.jpg``, ``4_1920.jpg``, ``4.display.jpg``,
    ``4.sam2-large.png``, ``4.NEF``. Requiring that separator is what
    stops ``40.jpg`` from registering as id 4.
    """
    digits = 0
    while digits < len(name) and name[digits].isdigit():
        digits += 1
    if not digits or digits == len(name) or name[digits] not in "._":
        return None
    return int(name[:digits])


class RecycledIdIndex:
    """The set of photo ids that already have a cached derivative on disk.

    Ingest needs the same question answered once per inserted photo, and
    the per-id probe answers it by enumerating directories — see
    :func:`_recycled_id_has_stale_derivative` for the measured cost. One
    ``scandir`` per derivative directory answers it for every id at once,
    so a scan pays O(cached files) total instead of per photo. Mirrors
    the batching in ``scanner._sweep_untracked_previews_for_photos``,
    which exists for exactly this reason.

    Built lazily on first query, so a scan that inserts nothing never
    touches the filesystem. The snapshot is deliberately taken before
    ingest generates any derivative of its own: working copies and
    previews are extracted at the end of ``scan()``, and those belong to
    the new rows — a live re-read would see them and wrongly conclude the
    ids collided.

    If any derivative directory can't be enumerated (execute-only but
    unreadable, transient EIO, ACL quirk), the index remembers that and
    falls back to the exact-path per-id probe for every lookup. Treating
    ``scandir`` failure as "directory is empty" would silently teach the
    purge to skip recycled ids whose only surviving derivative lives
    there — and the exact-path probe (``os.path.exists``) still works on
    execute-only directories, so the fallback recovers that case.
    """

    def __init__(self, thumb_cache_dir, vireo_dir=None):
        self._thumb_cache_dir = thumb_cache_dir
        # --db and --thumb-dir are independent, so the other derivative
        # dirs are not necessarily siblings of the thumbnails. Callers
        # that know the real cache root pass it explicitly.
        self._vireo_dir = vireo_dir or os.path.dirname(thumb_cache_dir)
        self._ids = None
        self._incomplete = False
        self._unreadable_dirs = set()

    def _sweep(self, directory):
        """Return the ids seen in ``directory`` or ``None`` if unreadable.

        ``FileNotFoundError`` is genuinely empty (fresh install, no
        derivatives yet). Any other ``OSError`` — permissions, EIO — is
        an *unknown*, not an empty; the caller records the incomplete
        sweep and the per-id probe covers it later.

        The iterator itself can also raise (a network-backed cache
        losing the mount mid-walk, an ACL change between ``scandir`` and
        the first ``__next__``). Catch that too so an unrelated
        directory failure doesn't abort the whole index build.
        """
        try:
            entries = os.scandir(directory)
        except FileNotFoundError:
            return set()
        except OSError as e:
            log.warning(
                "Could not enumerate derivative directory %s for "
                "recycled-rowid detection (%s); the batch index will "
                "fall back to the exact-path probe for every lookup so "
                "recycled ids whose only surviving cached file lives "
                "here are still purged",
                directory, e,
            )
            return None
        found = set()
        try:
            with entries:
                for entry in entries:
                    photo_id = _leading_photo_id(entry.name)
                    if photo_id is not None:
                        found.add(photo_id)
        except OSError as e:
            log.warning(
                "Iterating derivative directory %s raised %s partway "
                "through recycled-rowid detection; treating as unreadable "
                "so the batch index falls back to the exact-path probe",
                directory, e,
            )
            return None
        return found

    def _build(self):
        ids = set()
        incomplete = False
        unreadable = set()
        for directory in _derivative_dirs(
            self._thumb_cache_dir, self._vireo_dir,
        ):
            seen = self._sweep(directory)
            if seen is None:
                incomplete = True
                unreadable.add(os.path.normpath(directory))
            else:
                ids.update(seen)
        # ``external-dng/`` isn't in ``_derivative_dirs`` because its
        # entries are per-id *subdirectories* named with bare digits
        # (``external-dng/42/``) — ``_leading_photo_id`` intentionally
        # requires a ``.``/``_`` terminator to keep ``40.jpg`` from
        # registering as id 4, and loosening it for the file-based
        # families to accept EOL would break that guard. Handle
        # external-dng in its own sweep instead.
        external_dng = os.path.join(self._vireo_dir, "external-dng")
        try:
            entries = os.scandir(external_dng)
        except FileNotFoundError:
            entries = None
        except OSError as e:
            log.warning(
                "Could not enumerate %s for recycled-rowid detection "
                "(%s); the batch index will fall back to the exact-path "
                "probe for every lookup", external_dng, e,
            )
            entries = None
            incomplete = True
            unreadable.add(os.path.normpath(external_dng))
        if entries is not None:
            try:
                with entries:
                    for entry in entries:
                        if entry.name.isdigit():
                            ids.add(int(entry.name))
            except OSError as e:
                log.warning(
                    "Iterating %s raised %s partway through recycled-"
                    "rowid detection; treating as unreadable so the "
                    "batch index falls back to the exact-path probe",
                    external_dng, e,
                )
                incomplete = True
                unreadable.add(os.path.normpath(external_dng))
        self._incomplete = incomplete
        self._unreadable_dirs = unreadable
        log.info(
            "Indexed %d photo ids with cached derivatives for "
            "recycled-rowid detection%s", len(ids),
            " (incomplete — some directories were unreadable)"
            if incomplete else "",
        )
        return ids

    def __contains__(self, photo_id):
        if self._ids is None:
            self._ids = self._build()
        if photo_id in self._ids:
            return True
        if self._incomplete:
            # One of the derivative dirs couldn't be enumerated, so its
            # absence from ``self._ids`` proves nothing. Fall back to the
            # exact-path probe, which uses ``os.path.exists`` on specific
            # files and works even when the containing directory is
            # execute-only but not readable — the shape of a permissions
            # gap that trips ``scandir`` but not ``stat``. Variant globs
            # rely on ``scandir`` too, so pass the unreadable-directory
            # set through: the probe treats variants in those dirs as
            # "unknown, assume present" and forces the purge rather than
            # silently serving another photo's pixels.
            return _recycled_id_has_stale_derivative(
                self._thumb_cache_dir, photo_id, self._vireo_dir,
                unreadable_dirs=self._unreadable_dirs,
            )
        return False


def ensure_preview_cache_invalidations_table(db):
    """Create the durable "do not adopt this preview" marker table.

    Shared home for the marker `app.py`'s ``_serve_preview`` consults.
    Backdating a preview file does nothing: the lazy-adoption branch there
    tests ``os.path.exists(cache_path)`` with no mtime comparison, so an
    on-disk preview that outlived its owner gets adopted and re-registered
    verbatim. Only this row keeps it out.
    """
    db.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS preview_cache_invalidations (
            photo_id INTEGER NOT NULL,
            size INTEGER NOT NULL,
            PRIMARY KEY (photo_id, size),
            FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
        )
        """,
    )


def mark_preview_cache_invalid(db, photo_id, size, *, commit=True):
    """Record that ``(photo_id, size)``'s on-disk preview must not be adopted."""
    ensure_preview_cache_invalidations_table(db)
    db.conn.execute(
        "INSERT OR IGNORE INTO preview_cache_invalidations (photo_id, size) "
        "VALUES (?, ?)",
        (photo_id, size),
    )
    if commit:
        db.conn.commit()


def _preview_size_from_path(path, photo_id, preview_dir=None):
    """Parse ``previews/<id>_<size>.jpg`` -> ``size``, else ``None``.

    ``preview_dir`` gates the match on the file actually living in
    ``previews/``. The name shape alone is ambiguous: a prepared render at
    ``originals/<id>_2048.jpg`` parses identically and would earn a durable
    ``preview_cache_invalidations`` row for a preview size that was never
    cached, permanently suppressing adoption of a preview that does get
    written later.
    """
    if preview_dir is not None and os.path.normpath(
        os.path.dirname(path)
    ) != os.path.normpath(preview_dir):
        return None
    name = os.path.basename(path)
    stem, ext = os.path.splitext(name)
    if ext.lower() != ".jpg":
        return None
    prefix = f"{photo_id}_"
    if not stem.startswith(prefix):
        return None
    suffix = stem[len(prefix):]
    return int(suffix) if suffix.isdigit() else None


def _surviving_derivative_paths(thumb_cache_dir, photo_id, vireo_dir=None):
    """Concrete surviving derivative **files** for ``photo_id``.

    Post-purge audit for :func:`purge_cached_files_for_recycled_id` — the
    probe helpers answer "does anything exist?" and short-circuit, but
    here we need the full list so each survivor can be dealt with.

    Directories are expanded into the files they contain rather than
    returned as-is. ``external-dng/<id>/`` is a per-id directory, and the
    freshness check that consumes it (``app.py``'s external-editor path)
    stats the *DNG file*, not its parent — so backdating the directory
    would leave the child's recent mtime intact and the caller would
    report a successful invalidation that invalidates nothing.
    """
    paths = [
        p for p in _recycled_id_probe_paths(
            thumb_cache_dir, photo_id, vireo_dir,
        )
        if os.path.exists(p)
    ]
    for pattern in _recycled_id_probe_patterns(
        thumb_cache_dir, photo_id, vireo_dir,
    ):
        paths.extend(_glob.glob(pattern))

    files = []
    for path in paths:
        if not os.path.isdir(path):
            files.append(path)
            continue
        for root, _dirs, names in os.walk(path):
            files.extend(os.path.join(root, name) for name in names)
    # dict.fromkeys: de-dupe (a bare name can match a variant pattern too)
    # while keeping a stable order for the log message.
    return list(dict.fromkeys(files))


def purge_cached_files_for_recycled_id(
    thumb_cache_dir, photo_id, id_index=None, vireo_dir=None, db=None,
    file_mtime=None,
):
    """Drop any cached derivative left over from a previous owner of ``photo_id``.

    ``photos.id`` is ``INTEGER PRIMARY KEY`` *without* ``AUTOINCREMENT``,
    so SQLite hands the next insert ``max(rowid) + 1`` — the ids freed by
    deleting the highest-numbered rows get handed straight back out. Every
    derivative in ``~/.vireo`` is keyed by bare photo id
    (``thumbnails/<id>.jpg``, ``working/<id>.jpg``, ``masks/<id>.png``, …),
    so a new photo that inherits a freed id silently adopts the *previous*
    photo's pixels: the Life List card for a gull renders whatever bird
    used to own that row.

    Delete paths are supposed to unlink these files, but the surface
    area is wide — ``Database._merge_into_existing``,
    ``scanner._pair_raw_jpeg_companions``, ``audit.remove_orphans`` and
    other raw-SQL drops each need to remember every id-keyed derivative
    family, and any unlink that fails leaves an orphan behind. Rather
    than trusting every delete site, ingest re-checks at the one moment
    the collision can actually happen — when a brand-new row claims a
    rowid.

    Returns ``True`` when something was purged. The caller uses that to
    schedule the batched untracked-preview sweep, which is too expensive
    to run per-photo.

    Pass a :class:`RecycledIdIndex` as ``id_index`` when calling this in a
    loop (ingest does): the probe becomes an O(1) set lookup against one
    ``scandir`` per derivative directory instead of a per-photo directory
    enumeration. Without it, falls back to the single-id probe.

    ``file_mtime`` is the new photo's source ``photos.file_mtime``. It
    picks the timestamp we backdate undeletable survivors to: the
    ``serve_thumbnail`` / prepared-render / external-DNG freshness gates
    all compare ``cached_mtime >= source_mtime``, so a survivor pinned to
    ``file_mtime - 1`` is provably rejected regardless of what the source
    mtime happens to be. When ``file_mtime`` is unavailable (older
    single-id callers) we fall back to ``0``, which works for any source
    with a positive mtime — the case the reviewer specifically flagged is
    a source at or below the Unix epoch, and callers on the ingest path
    do have ``file_mtime`` in hand.
    """
    if not thumb_cache_dir:
        return False
    if id_index is not None:
        if photo_id not in id_index:
            return False
    elif not _recycled_id_has_stale_derivative(
        thumb_cache_dir, photo_id, vireo_dir,
    ):
        return False
    log.info(
        "Photo %s reused a freed rowid with cached derivatives still on "
        "disk — purging them so the new photo renders its own pixels",
        photo_id,
    )
    cleanup_cached_files_for_deleted_photos(
        thumb_cache_dir, [{"photo_id": photo_id}], vireo_dir=vireo_dir,
    )
    # ``cleanup_cached_files_for_deleted_photos`` logs and continues when an
    # unlink fails (locked file on Windows, a momentarily unwritable
    # directory), so "we ran the purge" is not the same as "the stale
    # pixels are gone". Returning True unconditionally would let the scan
    # commit a recycled row while the previous owner's thumbnail is still
    # servable — and the request path's mtime guard can't catch it, since
    # a recently generated thumbnail beats an old capture's file_mtime.
    # Re-probe, and for anything left behind, backdate its mtime so that
    # guard *does* reject it. Timestamps are usually still writable when
    # deletion isn't (the lock blocks unlink, not utime), so this recovers
    # the common case; when even that fails we say so instead of leaving a
    # silent wrong-pixels bug.
    #
    # Backdating is not enough for *previews*, though: ``_serve_preview``'s
    # lazy-adoption branch tests ``os.path.exists(cache_path)`` with no
    # mtime comparison, so a surviving ``previews/<id>_<size>.jpg`` would
    # be adopted and re-registered verbatim. Those need the durable
    # ``preview_cache_invalidations`` marker, which that branch does
    # honour. Requires a ``db``; callers in the ingest path have one.
    survivors = _surviving_derivative_paths(
        thumb_cache_dir, photo_id, vireo_dir,
    )
    if survivors:
        base_dir = vireo_dir or os.path.dirname(thumb_cache_dir)
        preview_dir = os.path.join(base_dir, "previews")
        external_edits_dir = os.path.join(base_dir, "external-edits")
        inat_uploads_dir = os.path.join(base_dir, "inat-uploads")
        # ``serve_thumbnail`` / ``_prepared_full_resolution_render`` / the
        # external-DNG gate all treat ``cached_mtime >= source_mtime`` as
        # fresh. Backdating to a hard-coded ``0`` fails when the new
        # photo's source is itself at or below the Unix epoch (an archive
        # that preserved an epoch or negative filesystem timestamp) — the
        # survivor's mtime then still matches or exceeds ``file_mtime`` and
        # the guard waves the previous owner's pixels through. Anchor the
        # sentinel to the new photo's source instead so the comparison is
        # provably false regardless of what value ``file_mtime`` takes.
        if file_mtime is not None:
            try:
                backdate_target = float(file_mtime) - 1.0
            except (TypeError, ValueError):
                backdate_target = 0.0
        else:
            backdate_target = 0.0
        unfixable = []
        for path in survivors:
            backdated = True
            try:
                os.utime(path, (backdate_target, backdate_target))
            except OSError:
                backdated = False

            size = _preview_size_from_path(path, photo_id, preview_dir)
            if size is not None:
                # Previews are the one family where the marker, not the
                # mtime, is what keeps the adoption branch off the file —
                # so attempt it even when the backdate failed. Doing this
                # only on the backdate's success path (as an earlier
                # revision did) left a readable survivor adoptable.
                if db is None:
                    unfixable.append(path)
                    continue
                try:
                    mark_preview_cache_invalid(db, photo_id, size)
                except Exception:
                    log.warning(
                        "Could not mark surviving preview %s invalid", path,
                        exc_info=True,
                    )
                    unfixable.append(path)
                continue

            parent = os.path.normpath(os.path.dirname(path))
            if parent == os.path.normpath(external_edits_dir) or parent == (
                os.path.normpath(inat_uploads_dir)
            ):
                # ``_external_edit_handoff_path`` and
                # ``_inat_upload_photo_path`` compare the JSON's
                # {recipe, source_path, source_mtime, edit_math_version}
                # and never stat either file, so a backdate here changes
                # nothing. Nothing short of removing them helps.
                unfixable.append(path)
                continue

            if not backdated:
                unfixable.append(path)
        if unfixable:
            log.error(
                "Photo %s reused a freed rowid but %d cached derivative(s) "
                "could not be removed or invalidated: %s. Until they are "
                "cleared (Settings > Storage > Clear cache), this photo may "
                "render the previous owner's pixels.",
                photo_id, len(unfixable), ", ".join(unfixable),
            )
        else:
            log.warning(
                "Photo %s reused a freed rowid; %d cached derivative(s) "
                "survived deletion but were backdated so the freshness "
                "guard regenerates them: %s",
                photo_id, len(survivors), ", ".join(survivors),
            )
    return True


def evict_if_over_quota(db, vireo_dir):
    """Evict oldest preview_cache entries until under preview_cache_max_mb.

    Walks rows in ascending ``last_access_at`` order, removes files and
    rows, and stops as soon as total <= quota. Self-healing: if a file
    is already missing, the ghost row is still deleted. If ``unlink``
    fails for any other OS reason the row is *left in place* so the
    bytes stay accounted for and a future pass can retry; otherwise the
    accounting under-reports and eviction stops targeting the leaked
    bytes.

    Deletes are batched into one transaction to avoid hundreds of
    fsyncs when the quota is shrunk dramatically.
    """
    import config as cfg

    quota_mb = cfg.load().get("preview_cache_max_mb", 20480)
    max_bytes = int(quota_mb) * 1024 * 1024
    total = db.preview_cache_total_bytes()
    if total <= max_bytes:
        return

    preview_dir = os.path.join(vireo_dir, "previews")
    to_delete = []
    freed_bytes = 0
    for row in db.preview_cache_oldest_first():
        if total <= max_bytes:
            break
        path = os.path.join(
            preview_dir, f"{row['photo_id']}_{row['size']}.jpg"
        )
        removed = True
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning("Failed to remove preview cache file %s: %s", path, e)
            removed = False
        if removed:
            to_delete.append((row["photo_id"], row["size"]))
            total -= row["bytes"]
            freed_bytes += row["bytes"]

    if to_delete:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            to_delete,
        )
        db.conn.commit()
        log.info(
            "Preview cache eviction: removed %d entries, freed %.1f MB",
            len(to_delete), freed_bytes / 1024 / 1024,
        )


def reconcile_preview_cache(db, vireo_dir):
    """Drop preview_cache rows whose on-disk file is missing.

    Counterpart to ``evict_if_over_quota``'s self-heal: that path only
    cleans up ghost rows when the cache is *over* quota. If the cache
    accounting drifts while *under* quota — e.g. files deleted by an
    external process, or a previous eviction pass that removed files
    after the row's ``last_access_at`` was recently touched — the
    table keeps reporting ``total_bytes`` for files that no longer
    exist, and eviction stays asleep. That's invisible to the user
    until the next pipeline run regenerates everything from RAW
    because none of the cache files actually exist.

    Run at startup so a stale table can't poison the rest of the
    session. Returns the number of rows dropped.
    """
    preview_dir = os.path.join(vireo_dir, "previews")
    to_delete = []
    for row in db.preview_cache_oldest_first():
        path = os.path.join(
            preview_dir, f"{row['photo_id']}_{row['size']}.jpg"
        )
        if not os.path.exists(path):
            to_delete.append((row["photo_id"], row["size"]))

    if to_delete:
        db.conn.executemany(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            to_delete,
        )
        db.conn.commit()
        log.info(
            "Preview cache reconcile: dropped %d ghost rows (files missing on disk)",
            len(to_delete),
        )
    return len(to_delete)
