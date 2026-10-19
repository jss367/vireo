"""Moving individual photos (``move_photos``).

``move_photos`` runs its batch-wide refusals, then hands the batch to
``_PhotoMove``, which moves each photo in turn through its collaborators:
the stem ledger, the preflight checks, the file transfer, the catalog
rewrite and the developed-render relocation.
"""

import logging
import os
from dataclasses import dataclass, field

from .paths import (
    _is_case_insensitive_path,
    _samefile_or_false,
    _tracked_destination_overlap,
)
from .transfer import _copy_and_verify

# The package's one logger, named as before the split.
log = logging.getLogger("move")


def _xmp_path(filepath):
    """Return the XMP sidecar path for a file, or None if it doesn't exist.

    Matches ``.XMP`` too (as ``offline_cache._xmp_source_for`` does): on a
    case-sensitive volume an uppercase sidecar would otherwise be left
    behind at the source when its photo moves.
    """
    stem = os.path.splitext(filepath)[0]
    for xmp in (stem + ".xmp", stem + ".XMP"):
        if os.path.isfile(xmp):
            return xmp
    return None


def _companion_files(photo, src_dir):
    """Return list of extra files to move alongside a photo (XMP + companion RAW/JPEG)."""
    extras = []
    xmp = _xmp_path(os.path.join(src_dir, photo["filename"]))
    if xmp:
        extras.append(os.path.basename(xmp))
    if photo["companion_path"]:
        comp = os.path.join(src_dir, photo["companion_path"])
        if os.path.isfile(comp):
            extras.append(photo["companion_path"])
    return extras


def _has_untracked_destination_developed(
    destination, stem, developed_dir, case_insensitive=False,
    source_file=None,
):
    """Return True when the destination developed dir(s) already contain
    a same-stem file that no catalog row owns.

    ``move_photos`` guards the catalog side (rejecting two different
    source folders that both want the same destination stem), but the
    developed-render lookup in ``export._iter_developed_outputs``
    resolves by destination folder + stem alone. A leftover file at
    ``<destination>/developed/<stem>.*`` or
    ``<developed_dir>/<developed_folder_key(destination)>/<stem>.*``
    -- from a previously deleted photo, a manually-placed render, or a
    partial copy from a prior aborted move -- would silently become the
    moved photo's developed output. Detect that case so ``move_photos``
    can refuse the move rather than repoint the row against a mismatched
    render.
    """
    try:
        from ..export import developed_folder_key
    except ImportError:
        from export import developed_folder_key
    if not destination or not stem:
        return False
    subdirs = [os.path.join(destination, "developed")]
    if developed_dir:
        subdirs.append(
            os.path.join(developed_dir, developed_folder_key(destination))
        )
    for subdir in subdirs:
        if not subdir or not os.path.isdir(subdir):
            continue
        try:
            names = os.listdir(subdir)
        except OSError:
            continue
        expected_stem = stem.casefold() if case_insensitive else stem
        for name in names:
            candidate_stem = os.path.splitext(name)[0]
            if case_insensitive:
                candidate_stem = candidate_stem.casefold()
            if candidate_stem != expected_stem:
                continue
            candidate = os.path.join(subdir, name)
            # A tracked source folder may itself be named ``developed`` and
            # be moved into its parent. In that shape, the default-layout
            # probe points straight back at the source original; it is not an
            # untracked render and will be removed after the verified move.
            if source_file and _samefile_or_false(candidate, source_file):
                continue
            if os.path.isfile(candidate):
                return True
    return False


def _blocked_destination_developed_path(destination, developed_dir):
    """Return a non-directory entry that would block render relocation."""
    try:
        from ..export import developed_folder_key
    except ImportError:
        from export import developed_folder_key

    targets = [os.path.join(destination, "developed")]
    if developed_dir:
        targets.append(
            os.path.join(developed_dir, developed_folder_key(destination))
        )
    for target in targets:
        if os.path.lexists(target) and not os.path.isdir(target):
            return target
    return None


def _refuse_every_photo(photo_ids, conflict_msg, progress_cb):
    """Return the ``move_photos`` result that refuses the whole batch.

    Every photo is settled (with an error), so the bar reaches its total.
    """
    log.warning("Move refused: %s", conflict_msg)
    if progress_cb and photo_ids:
        progress_cb(len(photo_ids), len(photo_ids), "")
    return {
        "moved": 0,
        "errors": [
            f"{pid}: {conflict_msg}" for pid in photo_ids
        ] or [conflict_msg],
        "destination_folder_id": None,
    }


def _is_same_directory(folder_id, folder_path, dest_folder_id, destination):
    """Whether a photo's folder already is the move destination.

    ``destination`` has been through ``catalog_folder_path``, so a case or
    alias spelling of a cataloged folder resolves to that folder's row and
    the ids match. ``samefile`` covers two catalog rows naming one
    directory (e.g. both spellings on a case-insensitive volume).
    """
    if folder_id == dest_folder_id:
        return True
    try:
        return os.path.samefile(folder_path, destination)
    except OSError:
        return False


def move_photos(db, photo_ids, destination, progress_cb=None,
                developed_dir="", developed_listing_cache=None, cancel_check=None,
                pause_requested=None, pause_callback=None, keep_visible=True):
    """Move individual photos to a destination directory.

    Args:
        db: Database instance
        photo_ids: list of photo IDs to move
        destination: absolute path to target directory
        progress_cb: optional callback(current, total, filename)
        developed_dir: optional path to the configured
            ``darktable_output_dir``. When set, each moved photo's
            developed-output file — nested under a hash of its folder
            path via ``export.developed_folder_key`` — is rebased to
            match the new folder key so exports/full-resolution lookups
            still find the render instead of falling back to RAW.
        developed_listing_cache: optional dict shared across successive
            ``move_photos`` calls (e.g. from ``move_folder_by_date``) to
            amortize ``os.listdir`` on the per-source-folder developed
            subdirs. Without this, fanning N photos from one source
            folder across many destination groups relists the same
            developed subdir N times.

    With separate pause hooks, cancel_check must be cancellation-only.
    pause_requested is a non-parking probe; pause_callback runs after counts
    are reconciled, so a paused move leaves the folder tree consistent.

    Returns dict with keys: moved (int), already_in_place (int: photos
    already in the destination and left untouched), errors (list of str)
    """
    if developed_listing_cache is None:
        developed_listing_cache = {}
    from file_identity import catalog_folder_path

    # ``os.makedirs(..., exist_ok=True)`` raises ``FileExistsError`` when the
    # path exists but is a regular file. That would abort the whole batch
    # with an opaque exception, and for date-organized moves the preflight
    # can't rely on ``os.path.isdir`` alone to catch it. Detect that case
    # here and return a structured error so the caller reports every
    # affected photo instead of a mid-run crash.
    if os.path.lexists(destination) and not os.path.isdir(destination):
        return _refuse_every_photo(
            photo_ids, f"destination path is not a directory: {destination}",
            progress_cb,
        )
    os.makedirs(destination, exist_ok=True)
    destination = catalog_folder_path(db, destination)
    blocked_developed = _blocked_destination_developed_path(
        destination, developed_dir,
    )
    if blocked_developed:
        return _refuse_every_photo(
            photo_ids,
            "developed output path is not a directory: "
            f"{blocked_developed}",
            progress_cb,
        )
    move = _PhotoMove(db, destination, developed_dir, developed_listing_cache,
                      keep_visible)
    move.prepare(photo_ids)
    return move.run(photo_ids, progress_cb, cancel_check, pause_requested,
                    pause_callback)


# ``destination_stem_origins.get`` default for a stem no destination row holds.
_NO_DESTINATION_STEM = object()


@dataclass
class _PhotoToMove:
    """One photo that passed the source lookup in ``move_photos``."""

    pid: object
    photo: dict
    src_dir: str
    src_file: str
    stem: str
    stem_key: str
    dst_file: str = ""
    companions: list = field(default_factory=list)
    xmp_companion: str = ""
    preserve_source_render: bool = False


class _PhotoMove:
    """Coordinate one ``move_photos`` call.

    The per-photo phases live on collaborators, each owning only the state
    it writes:

      stems     (``_StemLedger``) — the destination render-stem provenance
        and the per-source-folder stem counts;
      catalog   (``_PhotoCatalogRewrite``) — the destination folder row, the
        workspace link and repointing each photo's row;
      preflight (``_PhotoPreflight``) — the photo lookup and every pre-copy
        refusal; built by ``prepare`` once the destination row exists;
      files     (``_PhotoFileTransfer``) — copying and verifying each photo
        and its companions, and deleting the originals;
      renders   (``_DevelopedRenders``) — rebasing developed outputs.

    Each pre-copy check records its error and returns None/False so ``run``
    skips to the next photo. The phases from the catalog update on never
    skip.
    """

    def __init__(self, db, destination, developed_dir, developed_listing_cache,
                 keep_visible):
        self.db = db
        self.destination = destination
        self.developed_dir = developed_dir
        self.moved = 0
        self.errors = []
        self.stems = _StemLedger(db, destination, developed_dir)
        self.catalog = _PhotoCatalogRewrite(db, destination, self.stems,
                                            keep_visible)
        self.files = _PhotoFileTransfer(destination, self.errors)
        self.renders = _DevelopedRenders(
            db, destination, developed_dir, developed_listing_cache, self.stems,
        )
        self.preflight = None

    def prepare(self, photo_ids):
        """Ensure the destination row, then load the stems and the photos."""
        db = self.db
        catalog = self.catalog
        catalog.ensure_destination_folder()
        self.stems.load_destination_stem_origins(catalog.dest_folder_id)
        photos_map = db.photos.get_by_ids(photo_ids)
        self.stems.load_source_stem_counts(photos_map)
        self.preflight = _PhotoPreflight(
            db, self.destination, self.developed_dir, catalog.dest_folder_id,
            photos_map, self.stems, self.files, self.errors,
        )

    def move_one(self, item):
        """Copy, repoint, rebase renders and delete originals for one photo.

        A photo already in the destination, or whose copy fails, stops here;
        the copy failure has recorded its error.
        """
        if self.preflight.is_already_in_place(item):
            return
        if not self.files.copy_files(item):
            return
        self.catalog.update_catalog(item)
        self.renders.relocate_developed(item)
        self.files.remove_originals(item)
        self.moved += 1

    def run(self, photo_ids, progress_cb, cancel_check, pause_requested,
            pause_callback):
        """Move each photo in order; return ``move_photos``' result."""
        db = self.db
        total = len(photo_ids)
        preflight, catalog = self.preflight, self.catalog
        try:
            for i, pid in enumerate(photo_ids):
                if pause_requested and pause_requested():
                    db.folders.update_counts()
                    if pause_callback:
                        pause_callback()
                if cancel_check and cancel_check():
                    break
                item = preflight.check_photo(pid)
                if item is not None:
                    self.move_one(item)
                # Every settled photo advances the bar, including one that
                # was skipped or failed (its reason is already in errors);
                # otherwise a batch with failures never reaches its total.
                if progress_cb:
                    photo = preflight.photos_map.get(pid)
                    progress_cb(i + 1, total, photo["filename"] if photo else "")
        finally:
            # Always update folder counts so they stay consistent even if an
            # exception interrupts the move loop after some photos were committed.
            if self.moved > 0:
                db.folders.update_counts()

        return {"moved": self.moved,
                "already_in_place": preflight.already_in_place,
                "errors": self.errors,
                "destination_folder_id": catalog.dest_folder_id}


class _StemLedger:
    """Which stems the destination holds, and from where; and which remain at
    each source.

    Owns the render-volume case folding (``render_case_insensitive``), the
    destination stem provenance (``destination_stem_origins``,
    ``destination_stem_exact``) and the per-source-folder stem counts
    (``source_stem_counts``). Every write to them happens here; the
    preflight only reads them.
    """

    def __init__(self, db, destination, developed_dir):
        self.db = db
        # Both the photo destination and a separately configured developed-output
        # directory can impose case-folded render names. For example, originals
        # may land on case-sensitive ext4 while renders land on a default macOS
        # APFS volume; ``IMG.CR3`` and ``img.NEF`` coexist in the former but their
        # ``*.jpg`` renders collide in the latter. Fold whenever either output
        # volume is case-insensitive before every lookup/write into
        # ``destination_stem_origins``.
        self.render_case_insensitive = _is_case_insensitive_path(destination)
        if developed_dir:
            self.render_case_insensitive = self.render_case_insensitive or \
                _is_case_insensitive_path(developed_dir)
        self.destination_stem_origins = {}
        self.destination_stem_exact = {}
        self.source_stem_counts = {}

    def stem_key(self, raw_stem):
        return raw_stem.casefold() if self.render_case_insensitive else raw_stem

    def load_destination_stem_origins(self, dest_folder_id):
        """Record which source folder each destination stem came from.

        Provenance is keyed by the source folder's **path** (not folders.id).
        SQLite ``INTEGER PRIMARY KEY`` without AUTOINCREMENT can reuse a freed
        rowid after ``Database.delete_folder``; storing the reusable id would
        let a new unrelated folder that lands on the same rowid compare equal
        to a stale reference and bypass the collision guard in
        ``refuse_render_collision``.
        """
        destination_stem_origins = self.destination_stem_origins
        destination_stem_exact = self.destination_stem_exact
        for row in self.db.moves_merge.move_provenance_in_folder(
            dest_folder_id,
        ):
            exact_stem = os.path.splitext(row["filename"])[0]
            stem = self.stem_key(exact_stem)
            origin = row["last_move_source_folder_path"]
            known_origin = destination_stem_origins.get(
                stem, _NO_DESTINATION_STEM,
            )
            if known_origin is _NO_DESTINATION_STEM:
                destination_stem_origins[stem] = origin
                destination_stem_exact[stem] = exact_stem
            elif known_origin != origin or \
                    destination_stem_exact[stem] != exact_stem:
                # Conflicting or partly unknown provenance cannot prove that a
                # new same-stem photo shares the existing developed render. On a
                # folding render volume, case-only stems from the same source are
                # distinct source renders too, so exact spelling is part of the
                # proof even though the destination lookup key is folded.
                destination_stem_origins[stem] = None

    def load_source_stem_counts(self, photos_map):
        """Count same-stem rows in each source folder of ``photos_map``."""
        db = self.db
        source_stem_counts = self.source_stem_counts
        for source_folder_id in {
            photo["folder_id"] for photo in photos_map.values()
        }:
            for row in db.moves_merge.filenames_in_folder(source_folder_id):
                source_stem = os.path.splitext(row["filename"])[0]
                key = (source_folder_id, source_stem)
                source_stem_counts[key] = source_stem_counts.get(key, 0) + 1

    def pin_destination_stem(self, item):
        """Record that ``item``'s stem now sits at the destination."""
        # Pin the stem to the proven source folder path so a same-source
        # sibling can follow in this call while a distinct source is
        # still rejected. Using the path (not folders.id) survives a
        # later delete/re-create of the source folder that would reuse
        # the same rowid.
        self.destination_stem_origins[item.stem_key] = item.src_dir
        self.destination_stem_exact[item.stem_key] = item.stem

    def release_source_stem(self, item):
        """Count the moved row out of its source stem.

        Sets ``item.preserve_source_render``: True while a same-stem sibling
        is still in the source folder.
        """
        db = self.db
        src_dir = item.src_dir
        stem = item.stem
        source_stem_counts = self.source_stem_counts
        source_stem_key = (item.photo["folder_id"], stem)
        source_stem_counts[source_stem_key] = max(
            0, source_stem_counts.get(source_stem_key, 1) - 1,
        )
        item.preserve_source_render = source_stem_counts[source_stem_key] > 0
        if not item.preserve_source_render:
            # No same-stem sibling remains in the source folder now
            # that this row moved out. Expire the destination
            # provenance for the stem so a later rescan/import of an
            # unrelated ``IMG.*`` back into the same source path
            # can't slip past the same-stem developed-render
            # collision guard by matching this row's stale origin.
            # Developed-output lookup at the destination is keyed by
            # folder + stem only, so a spoofed provenance would let
            # the new photo display/export this row's edit. Clear
            # every destination row (across all destinations, in
            # case fanout by date sent same-source siblings to
            # different folders) whose stored provenance is this
            # drained source; the ``os.path.splitext`` filter in
            # Python keeps ``foo.bar`` and ``foo`` from
            # collapsing under a naive ``LIKE 'foo.%'``.
            stale_ids = [
                row["id"] for row in db.moves_merge.photos_moved_from(src_dir)
                if os.path.splitext(row["filename"])[0] == stem
            ]
            if stale_ids:
                db.moves_merge.clear_move_source(stale_ids)
                db.commit()
            self.destination_stem_origins[item.stem_key] = None


class _PhotoCatalogRewrite:
    """The destination's folder row and repointing each moved photo at it.

    Owns ``dest_folder_id``, whether this call has linked the destination
    into the active workspace yet, and ``keep_visible``. ``update_catalog``
    is each photo's commit point.
    """

    def __init__(self, db, destination, stems, keep_visible):
        self.db = db
        self.destination = destination
        self.stems = stems
        self.keep_visible = keep_visible
        self.dest_folder_id = None
        self.workspace_linked = False

    def ensure_destination_folder(self):
        """Ensure destination folder record exists (workspace link deferred until first successful move)."""
        db = self.db
        destination = self.destination
        dest_row = db.moves_merge.folder_id_row(destination)
        if dest_row:
            self.dest_folder_id = dest_row["id"]
        else:
            # Insert folder record without auto-linking to workspace (add_folder would auto-link).
            # Set parent_id from the nearest existing ancestor so the destination
            # nests correctly in the browse tree instead of floating as a root.
            cur = db.moves_merge.insert_folder_if_absent(
                destination, os.path.basename(destination),
                db.folders.nearest_ancestor_id(destination),
            )
            db.commit()
            if cur.rowcount > 0:
                self.dest_folder_id = cur.lastrowid
            else:
                self.dest_folder_id = db.moves_merge.folder_id_row(
                    destination
                )["id"]

    def update_catalog(self, item):
        """Repoint the verified photo's row at the destination folder."""
        db = self.db
        link_destination = not self.workspace_linked and db.active_workspace_id is not None
        # Link destination and repoint the photo together, before deleting
        # originals. A failed visibility write must not leave a folder-wide
        # link exposing unrelated destination siblings after a failed move.
        # The savepoint releases and commits on success, and rolls back to
        # itself on any exception (including one from the release or commit).
        moves = db.moves_merge
        with moves.photo_move_savepoint():
            if link_destination:
                db._add_workspace_folder_no_commit(
                    db.active_workspace_id, self.dest_folder_id, restore_removed=True,
                )
            db.photo_visibility.preserve_for_move(item.pid, self.keep_visible)
            moves.repoint_moved_photo(item.pid, self.dest_folder_id, item.src_dir)
        if link_destination:
            self.workspace_linked = True
            db._new_images_cache.invalidate_workspaces(db._db_path, [db.active_workspace_id])
        self.stems.pin_destination_stem(item)


class _PhotoPreflight:
    """Look up each photo and run every pre-copy refusal.

    Owns the loaded photo rows (``photos_map``), the per-folder "already the
    destination" answers and the already-in-place count. Reads the stem
    ledger and the XMP sidecars this call already copied; writes nothing
    but ``errors``. Touches no file.
    """

    def __init__(self, db, destination, developed_dir, dest_folder_id,
                 photos_map, stems, files, errors):
        self.db = db
        self.destination = destination
        self.developed_dir = developed_dir
        self.dest_folder_id = dest_folder_id
        self.photos_map = photos_map
        self.stems = stems
        self.files = files
        self.errors = errors
        self.already_in_place = 0
        self.in_place_folders = {}

    def check_photo(self, pid):
        """Look up one photo and run every pre-copy refusal.

        Returns the photo to move, or None once the reason it is skipped has
        been recorded.
        """
        photo = self.photos_map.get(pid)
        if not photo:
            self.errors.append(f"Photo {pid} not found in database")
            return None

        folder_row = self.db.moves_merge.folder_path_row(photo["folder_id"])
        src_dir = folder_row["path"]
        src_file = os.path.join(src_dir, photo["filename"])
        stem = os.path.splitext(photo["filename"])[0]

        if not os.path.isfile(src_file):
            log.warning("Move skipped for %s: source file missing", photo["filename"])
            self.errors.append(f"{photo['filename']}: source file missing")
            return None

        if self.folder_is_destination(photo["folder_id"], src_dir):
            return _PhotoToMove(
                pid=pid, photo=photo, src_dir=src_dir, src_file=src_file,
                stem=stem, stem_key=self.stems.stem_key(stem),
            )
        item = _PhotoToMove(
            pid=pid, photo=photo, src_dir=src_dir, src_file=src_file,
            stem=stem, stem_key=self.stems.stem_key(stem),
        )
        if self.refuse_render_collision(item):
            return None
        if self.refuse_destination_collision(item):
            return None
        return item

    def folder_is_destination(self, folder_id, src_dir):
        if folder_id not in self.in_place_folders:
            self.in_place_folders[folder_id] = _is_same_directory(
                folder_id, src_dir, self.dest_folder_id, self.destination,
            )
        return self.in_place_folders[folder_id]

    def is_already_in_place(self, item):
        """Count photos already in the destination without touching their files."""
        if self.folder_is_destination(item.photo["folder_id"], item.src_dir):
            self.already_in_place += 1
            return True
        return False

    def refuse_render_collision(self, item):
        """Refuse a photo whose developed render would collide at the destination.

        Developed outputs are addressed by destination folder + stem,
        not by the original extension. Same-stem photos from one source
        folder intentionally share a render (RAW+JPEG), but two source
        folders can hold distinct renders with the same filename. Do not
        merge the latter into one destination and silently make one row
        display/export the other's edit.
        """
        photo = item.photo
        existing_origin = self.stems.destination_stem_origins.get(
            item.stem_key, _NO_DESTINATION_STEM,
        )
        if existing_origin is not _NO_DESTINATION_STEM \
                and (existing_origin != item.src_dir or
                     self.stems.destination_stem_exact.get(item.stem_key) != item.stem):
            log.warning(
                "Move skipped for %s: developed render stem collides at "
                "destination", photo["filename"],
            )
            self.errors.append(
                f"{photo['filename']}: developed render stem already "
                "exists at destination"
            )
            return True

        # The catalog-only check above can't see files on disk that no
        # tracked photo owns: a leftover render from a previously
        # deleted photo, a manually-placed file, or a partial copy from
        # an aborted earlier move. ``_iter_developed_outputs`` resolves
        # developed renders by destination folder + stem alone, so an
        # untracked ``<destination>/developed/<stem>.*`` or
        # ``<developed_dir>/<developed_folder_key(destination)>/<stem>.*``
        # would be silently served as this photo's developed output
        # after the row is repointed. Treat it as a move collision
        # before touching the row.
        if existing_origin is _NO_DESTINATION_STEM and \
                _has_untracked_destination_developed(
                    self.destination, item.stem, self.developed_dir,
                    case_insensitive=self.stems.render_case_insensitive,
                    source_file=item.src_file,
                ):
            log.warning(
                "Move skipped for %s: developed render already exists "
                "at destination", photo["filename"],
            )
            self.errors.append(
                f"{photo['filename']}: developed render already exists "
                "at destination"
            )
            return True
        return False

    def refuse_destination_collision(self, item):
        """Refuse a photo whose file or a companion already exists at the destination."""
        photo = item.photo
        src_dir = item.src_dir
        item.dst_file = os.path.join(self.destination, photo["filename"])
        if os.path.exists(item.dst_file):
            log.warning("Move skipped for %s: already exists at destination", photo["filename"])
            self.errors.append(f"{photo['filename']}: already exists at destination")
            return True

        # Gather companion files
        item.companions = _companion_files(photo, src_dir)
        _src_xmp = _xmp_path(item.src_file)
        item.xmp_companion = (
            os.path.basename(_src_xmp) if _src_xmp
            else os.path.splitext(photo["filename"])[0] + ".xmp"
        )

        # Check companion collisions
        for comp in item.companions:
            if os.path.exists(os.path.join(self.destination, comp)):
                # Same-stem photos intentionally share one XMP. If an
                # earlier row in this batch already copied that exact
                # source sidecar into this destination, reuse the verified
                # copy instead of treating the later sibling as a
                # collision. A pre-existing untracked XMP still blocks.
                if comp == item.xmp_companion and \
                        (src_dir, comp) in self.files.copied_xmp_companions:
                    continue
                self.errors.append(f"{comp}: companion file already exists at destination")
                return True
        return False


class _PhotoFileTransfer:
    """Every file operation on a moved photo's original and companions.

    ``copy_files`` copies and verifies them (removing this photo's partial
    copies if a companion fails); ``remove_originals`` deletes the source
    files once the catalog points at the destination. Owns the set of XMP
    sidecars this call already copied, which the preflight reads.
    """

    def __init__(self, destination, errors):
        self.destination = destination
        self.errors = errors
        self.copied_xmp_companions = set()

    def copy_files(self, item):
        """Copy and verify the photo and its companions; False skips the photo."""
        photo = item.photo
        src_dir = item.src_dir
        destination = self.destination
        copied_xmp_companions = self.copied_xmp_companions
        # Copy main file. A copy error (disk full, share dropped) fails
        # this photo only; the rest of the batch still moves.
        try:
            copied_ok = _copy_and_verify(item.src_file, item.dst_file)
        except OSError as e:
            log.warning("Move skipped for %s: copy failed: %s", photo["filename"], e)
            self.errors.append(f"{photo['filename']}: copy failed: {e}")
            return False
        if not copied_ok:
            log.warning("Move skipped for %s: verification failed after copy", photo["filename"])
            self.errors.append(f"{photo['filename']}: verification failed after copy")
            return False

        # Copy companions
        copied_companions = []
        for comp in item.companions:
            comp_src = os.path.join(src_dir, comp)
            comp_dst = os.path.join(destination, comp)
            if comp == item.xmp_companion and \
                    (src_dir, comp) in copied_xmp_companions:
                continue
            try:
                comp_copied = _copy_and_verify(comp_src, comp_dst)
                comp_error = "companion verification failed"
            except OSError as e:
                comp_copied = False
                comp_error = f"companion copy failed: {e}"
            if not comp_copied:
                log.warning("Move skipped for %s: %s: %s", photo["filename"],
                            comp, comp_error)
                self.errors.append(f"{comp}: {comp_error}")
                self.discard_partial_copies(item, copied_companions)
                return False
            copied_companions.append(comp)
            if comp == item.xmp_companion:
                copied_xmp_companions.add((src_dir, comp))
        return True

    def discard_partial_copies(self, item, copied_companions):
        """Remove the destination copies of a photo whose move failed.

        Only destination copies are removed; the originals were never
        touched. A copy that cannot be removed (locked by antivirus or an
        indexer on Windows, the share dropped) is reported against the photo
        and left in place, and the batch carries on: raising here would
        abort every remaining photo and drop the errors already collected.
        """
        photo = item.photo
        leftovers = []
        for name in [photo["filename"], *copied_companions]:
            path = os.path.join(self.destination, name)
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except OSError as exc:
                leftovers.append(f"{path} ({exc})")
                # The leftover sidecar is still a verified copy of this
                # source XMP, so a same-stem sibling may keep sharing it.
                continue
            if name == item.xmp_companion:
                self.copied_xmp_companions.discard((item.src_dir, name))
        if leftovers:
            log.warning("Could not remove partial copies of %s: %s",
                        photo["filename"], "; ".join(leftovers))
            self.errors.append(
                f"{photo['filename']}: not moved, and its partial copy could "
                f"not be removed from the destination: {'; '.join(leftovers)}. "
                "The original is unchanged at the source."
            )

    def remove_originals(self, item):
        """Delete the source original and companions, now that it is safe.

        The catalog and developed outputs are already at the new location,
        so a cleanup failure here is post-commit: report it as a per-photo
        error so the caller can surface the leftover files, but keep the
        batch moving. Without this catch a single OSError (read-only source
        directory, locked file on Windows) would abort every remaining photo
        in the batch with the catalog already repointed for the ones
        processed so far.

        The error says which files were left: the original (and with it the
        companions, which stay beside it), or only companions once the
        original is gone.
        """
        filename = item.photo["filename"]
        src_dir = item.src_dir
        # Keep a shared XMP at the source until the final same-stem catalog
        # row leaves. Date-organized moves call move_photos once per
        # destination group; preserving the sidecar after an early group
        # lets each later group copy the metadata before the final sibling
        # removes it.
        companions = [
            comp for comp in item.companions
            if not (comp == item.xmp_companion and item.preserve_source_render)
        ]
        try:
            os.remove(item.src_file)
        except OSError as exc:
            log.warning("Post-commit cleanup of %s failed: %s",
                        item.src_file, exc)
            # No companion was attempted on this branch, so every one in
            # ``companions`` is still beside the original. Report them
            # directly rather than probing with ``isfile``: when the source
            # directory or mount is inaccessible (a likely reason the
            # original delete failed), ``isfile`` silently returns False
            # for files that are actually still there.
            with_companions = (
                f"; its companions were left with it: {', '.join(companions)}"
                if companions else ""
            )
            self.errors.append(
                f"{filename}: moved to {self.destination}, but the original "
                f"could not be deleted from {src_dir} ({exc})"
                f"{with_companions}"
            )
            return
        leftovers = []
        for comp in companions:
            comp_src = os.path.join(src_dir, comp)
            if not os.path.isfile(comp_src):
                continue
            try:
                os.remove(comp_src)
            except OSError as exc:
                log.warning("Post-commit cleanup of %s failed: %s",
                            comp_src, exc)
                leftovers.append(f"{comp} ({exc})")
        if leftovers:
            self.errors.append(
                f"{filename}: moved to {self.destination} and the original "
                f"deleted, but companion files could not be deleted from "
                f"{src_dir}: {', '.join(leftovers)}"
            )


class _DevelopedRenders:
    """Rebase each moved photo's developed outputs to the destination.

    Owns which source folders have a cataloged ``developed`` subfolder (so
    their default-layout renders are left alone). Releases the photo's
    source stem in the ledger first, which decides whether a same-stem
    sibling still needs the source render.
    """

    def __init__(self, db, destination, developed_dir, developed_listing_cache,
                 stems):
        self.db = db
        self.destination = destination
        self.developed_dir = developed_dir
        self.developed_listing_cache = developed_listing_cache
        self.stems = stems
        self.managed_default_developed = {}

    def relocate_developed(self, item):
        """Rebase this photo's developed-output file(s) for the new folder.

        Runs BEFORE removing originals. Both develop-job layouts need to
        move — the configured ``darktable_output_dir`` (hashed under
        ``developed_folder_key``) and the default ``<folder>/developed/``
        subdir the job writes to when no output dir is configured. The
        folder_id update in ``update_catalog`` just invalidated both lookups;
        without this rebase, ``_iter_developed_outputs`` probes the destination
        folder and misses renders left under the old source folder, so
        exports/full-resolution fall back to the RAW. Doing it before
        cleanup means a subsequent os.remove failure (read-only source
        dir, locked file on Windows) still leaves catalog and developed
        renders in agreement at the new location.
        """
        try:
            from ..export import (
                relocate_default_developed_file,
                relocate_developed_file,
            )
        except ImportError:
            from export import (
                relocate_default_developed_file,
                relocate_developed_file,
            )
        src_dir = item.src_dir
        self.stems.release_source_stem(item)
        if self.developed_dir:
            relocate_developed_file(
                self.developed_dir, src_dir, self.destination, item.stem,
                self.developed_listing_cache, item.preserve_source_render,
            )
        default_developed_path = os.path.join(src_dir, "developed")
        if src_dir not in self.managed_default_developed:
            # A real catalog folder may legitimately be named
            # ``developed``. Treating its matching-stem originals as
            # generated renders would move them before their own folder
            # group is processed. Skip the default-layout relocation
            # whenever that directory is managed (or contains another
            # managed folder); its tracked photos move through the normal
            # catalog path instead.
            self.managed_default_developed[src_dir] = bool(
                _tracked_destination_overlap(
                    self.db, item.photo["folder_id"], default_developed_path,
                )
            )
        if not self.managed_default_developed[src_dir]:
            relocate_default_developed_file(
                src_dir, self.destination, item.stem,
                self.developed_listing_cache, item.preserve_source_render,
            )
