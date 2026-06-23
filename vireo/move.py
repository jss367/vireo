"""Photo and folder move operations with copy-verify-delete safety."""

import contextlib
import filecmp
import logging
import os
import shutil
import subprocess
import tempfile

log = logging.getLogger(__name__)


def _xmp_path(filepath):
    """Return the XMP sidecar path for a file, or None if it doesn't exist."""
    xmp = os.path.splitext(filepath)[0] + ".xmp"
    return xmp if os.path.isfile(xmp) else None


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


def _copy_and_verify(src, dst):
    """Copy a single file and verify size matches. Returns True on success."""
    shutil.copy2(src, dst)
    if os.path.getsize(src) != os.path.getsize(dst):
        os.remove(dst)
        return False
    return True


def resolve_folder_dest(folder_path, folder_name, destination):
    """Compute the final landing path for a folder move.

    The source folder is placed *inside* destination, keeping its name —
    e.g. moving /local/birds to /nas/photos yields /nas/photos/birds.
    Shared by move_folder() and the preflight route so the resolved path
    is computed in exactly one place.
    """
    name = folder_name or os.path.basename(folder_path.rstrip("/\\"))
    return os.path.join(destination, name)


def _copy_missing(src_path, dest_path):
    """Recursively copy only files that don't already exist at dest_path,
    never overwriting an existing destination file. shutil fallback for a
    merge when rsync is unavailable; mirrors rsync --ignore-existing."""
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        target_dir = dest_path if rel == "." else os.path.join(dest_path, rel)
        os.makedirs(target_dir, exist_ok=True)
        for fn in files:
            dst_file = os.path.join(target_dir, fn)
            if not os.path.exists(dst_file):
                shutil.copy2(os.path.join(root, fn), dst_file)


def _find_content_conflict(src_path, dest_path):
    """Return the relative path of the first source file that ALSO exists at
    dest_path but with different content, or None. Run before a merge copies
    anything: a same-name destination file is only safe to treat as "already
    there" if its bytes match the source. A size match alone is not enough —
    filecmp with shallow=False compares contents — so we never overwrite or
    later delete the source over a genuinely different destination file."""
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            dst_file = os.path.join(dest_path, rel_name)
            if os.path.isfile(dst_file) and \
                    not filecmp.cmp(src_file, dst_file, shallow=False):
                return rel_name
    return None


def _first_missing_source_file(src_path, dest_path):
    """Return the relative path of the first source file absent (or
    size-mismatched, or a symlink) at dest_path, or None if every source
    file is present and matches. Used to verify a merge before deleting
    originals.

    A symlinked destination entry is treated as missing: `os.path.isfile` /
    `os.path.getsize` follow the link, so a symlink pointing back into the
    source tree (directly, or via a symlinked parent directory) would pass
    a size compare even though no independent copy exists at the
    destination — and the post-copy `shutil.rmtree(src_path)` would then
    destroy the only copy. `lexists` lets a broken symlink count as
    missing instead of crashing later checks; `islink` catches the direct
    case; `samefile` catches the symlinked-parent case where `src_file`
    and `dst_file` resolve to the same inode.
    """
    for root, _, files in os.walk(src_path):
        rel = os.path.relpath(root, src_path)
        for fn in files:
            src_file = os.path.join(root, fn)
            rel_name = fn if rel == "." else os.path.join(rel, fn)
            dst_file = os.path.join(dest_path, rel_name)
            if not os.path.lexists(dst_file) or os.path.islink(dst_file):
                return rel_name
            try:
                if os.path.samefile(src_file, dst_file):
                    return rel_name
            except OSError:
                return rel_name
            if not os.path.isfile(dst_file) or \
                    os.path.getsize(src_file) != os.path.getsize(dst_file):
                return rel_name
    return None


def _samefile_or_false(a, b):
    try:
        return os.path.samefile(a, b)
    except OSError:
        return False


def _walk_up_paths(p):
    prev = None
    while p and p != prev:
        yield p
        prev = p
        p = os.path.dirname(p)


def _is_case_insensitive_path(path):
    """Whether the filesystem holding `path` treats two case variants as the
    same name (default macOS APFS, Windows).

    Walks up to the deepest existing ancestor of `path`, then probes the FS
    *at* that ancestor by case-flipping one of its child entries: if both
    spellings resolve (samefile) to the same inode, the FS at the ancestor
    folds case. Probing via a child name rather than the ancestor's own
    basename matters when the deepest existing ancestor is a mount point —
    a case-sensitive APFS volume mounted at `/Volumes/Photos` under default
    case-insensitive macOS HFS+ would otherwise be misread as
    case-insensitive (the basename probe tests `/Volumes`'s handling of
    `Photos`, not what the mounted volume does below it), causing valid
    moves into the mount to be refused as "already managed".

    When the ancestor has no letter-bearing existing child to flip (empty,
    or only digit/symbol-named entries), creates a short-lived temp dir
    *inside* the ancestor with a case-flippable suffix and probes via that
    instead — otherwise a fresh destination tree on case-insensitive POSIX
    (default macOS APFS at `/Volumes/Photos` with nothing in it yet) would
    fall through as "case-sensitive" and let a stale tracked row like
    `/Photos/Dst/src` slip past the case-folded overlap check against a
    move landing at `/Photos/dst/src`.

    Returns False on case-sensitive POSIX (Linux ext4/btrfs, opt-in APFS)
    and when no probe is possible at all (ancestor unreadable, not a
    directory, or read-only so the temp-probe fallback can't write) — the
    safe default, since spuriously folding case could merge two genuinely
    distinct paths.
    """
    if os.name == "nt":
        return True
    cur = path
    while cur and not os.path.exists(cur):
        parent = os.path.dirname(cur)
        if parent == cur:
            return False
        cur = parent
    if not cur or not os.path.isdir(cur):
        return False
    try:
        entries = os.listdir(cur)
    except OSError:
        return False
    for entry in entries:
        flipped = entry.swapcase()
        if flipped == entry:
            continue
        return _samefile_or_false(
            os.path.join(cur, entry),
            os.path.join(cur, flipped),
        )
    # No letter-bearing existing child to flip. Probe by creating our own
    # temp dir inside `cur` with a known case-flippable suffix and asking
    # samefile whether the flipped spelling resolves to the same inode.
    try:
        probe = tempfile.mkdtemp(prefix=".vireo_case_probe_", suffix="A",
                                 dir=cur)
    except OSError:
        return False
    try:
        flipped = probe[:-1] + probe[-1].swapcase()
        return _samefile_or_false(probe, flipped)
    finally:
        with contextlib.suppress(OSError):
            os.rmdir(probe)


def _path_equal_or_descends(candidate, ancestor, ancestor_case_insensitive=None):
    """True if `candidate` resolves to the same directory as `ancestor`, or is
    a descendant of it.

    Folds together every directory-alias surface the move guards need:
      - Symlinks: os.path.realpath.
      - Windows case folding: os.path.normcase.
      - Case-insensitive POSIX (default macOS APFS), where the above two are
        not enough — os.path.realpath does not fold case and os.path.normcase
        is a no-op on POSIX, so paths differing only by case string-compare
        unequal even though they resolve to the same inode: os.path.samefile
        (device + inode) is FS-truth on every platform and is used as a
        fallback, including a walk-up ancestor check for the descendant case
        where `candidate` itself doesn't exist yet.
      - Two missing leaves on case-insensitive POSIX, where neither path
        exists yet so samefile has nothing to compare: probe the FS for
        case-insensitivity via the deepest existing ancestor and, if so,
        redo the string compare case-folded.

    `ancestor_case_insensitive`: pass the result of `_is_case_insensitive_path(
    ancestor)` if you've already computed it (e.g. inside a loop with a fixed
    ancestor) so the probe doesn't re-run per call. None means probe lazily.
    """
    real_c = os.path.normcase(os.path.realpath(candidate))
    real_a = os.path.normcase(os.path.realpath(ancestor))
    if real_c == real_a or real_c.startswith(real_a + os.sep):
        return True

    if os.path.exists(candidate) and os.path.exists(ancestor) \
            and _samefile_or_false(candidate, ancestor):
        return True

    # Containment via case-only alias: walk up candidate's existing ancestors
    # looking for one whose inode matches `ancestor`. Handles the case where
    # the candidate leaf doesn't exist yet but its parent (or an ancestor) is
    # a case-only alias of `ancestor` on a case-insensitive POSIX filesystem.
    if os.path.exists(ancestor):
        for anc in _walk_up_paths(os.path.dirname(candidate)):
            if os.path.exists(anc) and _samefile_or_false(anc, ancestor):
                return True

    # Both leaves missing on a case-insensitive POSIX volume: e.g., a stale
    # folders.path row that differs from the resolved destination only by
    # case before either path has been created on disk. samefile can't fold
    # case for paths that don't exist; probe the FS for case-insensitivity
    # and redo the string compare case-folded so the stale row is still
    # caught before any copy.
    if os.name != "nt":
        if ancestor_case_insensitive is None:
            ancestor_case_insensitive = _is_case_insensitive_path(ancestor)
        if ancestor_case_insensitive:
            lower_c = real_c.lower()
            lower_a = real_a.lower()
            if lower_c == lower_a or lower_c.startswith(lower_a + os.sep):
                return True
    return False


def _destination_overlaps_source(src_path, dest_path):
    """True if dest_path equals src_path or one is a descendant of the other.

    The post-copy rmtree(src_path) would delete the only copy of the files if
    dest and src refer to the same on-disk directory, so this is checked
    before any copy. See `_path_equal_or_descends` for the alias surface.
    """
    return (_path_equal_or_descends(dest_path, src_path)
            or _path_equal_or_descends(src_path, dest_path))


def _tracked_destination_overlap(db, folder_id, dest_path):
    """Return another tracked folder at or below dest_path, if one exists.

    The FS case-insensitivity of `dest_path` is probed once and reused for
    every row — otherwise the probe (an os.listdir of the deepest existing
    ancestor, plus samefile of two child paths) re-runs per non-matching row,
    making this O(tracked_folders × destination_entries) before any copy on
    large catalogs or network-backed destinations.
    """
    dest_case_insensitive = (
        True if os.name == "nt" else _is_case_insensitive_path(dest_path)
    )
    for row in db.conn.execute(
        "SELECT id, path FROM folders WHERE id != ?", (folder_id,)
    ):
        if _path_equal_or_descends(
            row["path"], dest_path,
            ancestor_case_insensitive=dest_case_insensitive,
        ):
            return row
    return None


def move_photos(db, photo_ids, destination, progress_cb=None):
    """Move individual photos to a destination directory.

    Args:
        db: Database instance
        photo_ids: list of photo IDs to move
        destination: absolute path to target directory
        progress_cb: optional callback(current, total, filename)

    Returns dict with keys: moved (int), errors (list of str)
    """
    os.makedirs(destination, exist_ok=True)
    total = len(photo_ids)
    moved = 0
    errors = []

    # Ensure destination folder record exists (workspace link deferred until first successful move)
    dest_row = db.conn.execute("SELECT id FROM folders WHERE path = ?", (destination,)).fetchone()
    if dest_row:
        dest_folder_id = dest_row["id"]
    else:
        # Insert folder record without auto-linking to workspace (add_folder would auto-link)
        cur = db.conn.execute(
            "INSERT OR IGNORE INTO folders (path, name) VALUES (?, ?)",
            (destination, os.path.basename(destination)),
        )
        db.conn.commit()
        if cur.rowcount > 0:
            dest_folder_id = cur.lastrowid
        else:
            dest_folder_id = db.conn.execute(
                "SELECT id FROM folders WHERE path = ?", (destination,)
            ).fetchone()["id"]
    workspace_linked = False

    photos_map = db.get_photos_by_ids(photo_ids)

    try:
        for i, pid in enumerate(photo_ids):
            photo = photos_map.get(pid)
            if not photo:
                errors.append(f"Photo {pid} not found in database")
                continue

            folder_row = db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
            ).fetchone()
            src_dir = folder_row["path"]
            src_file = os.path.join(src_dir, photo["filename"])

            if not os.path.isfile(src_file):
                log.warning("Move skipped for %s: source file missing", photo["filename"])
                errors.append(f"{photo['filename']}: source file missing")
                continue

            dst_file = os.path.join(destination, photo["filename"])
            if os.path.exists(dst_file):
                log.warning("Move skipped for %s: already exists at destination", photo["filename"])
                errors.append(f"{photo['filename']}: already exists at destination")
                continue

            # Gather companion files
            companions = _companion_files(photo, src_dir)

            # Check companion collisions
            comp_collision = False
            for comp in companions:
                if os.path.exists(os.path.join(destination, comp)):
                    errors.append(f"{comp}: companion file already exists at destination")
                    comp_collision = True
                    break
            if comp_collision:
                continue

            # Copy main file
            if not _copy_and_verify(src_file, dst_file):
                log.warning("Move skipped for %s: verification failed after copy", photo["filename"])
                errors.append(f"{photo['filename']}: verification failed after copy")
                continue

            # Copy companions
            comp_ok = True
            copied_companions = []
            for comp in companions:
                comp_src = os.path.join(src_dir, comp)
                comp_dst = os.path.join(destination, comp)
                if not _copy_and_verify(comp_src, comp_dst):
                    errors.append(f"{comp}: companion verification failed")
                    # Clean up what we copied
                    os.remove(dst_file)
                    for cc in copied_companions:
                        os.remove(os.path.join(destination, cc))
                    comp_ok = False
                    break
                copied_companions.append(comp)

            if not comp_ok:
                continue

            # Verification passed — link destination folder to workspace on first success
            if not workspace_linked and db._active_workspace_id is not None:
                db.add_workspace_folder(db._active_workspace_id, dest_folder_id)
                workspace_linked = True

            # Update DB before deleting originals
            # This ensures a crash leaves duplicates (safe) rather than orphans
            db.conn.execute(
                "UPDATE photos SET folder_id = ? WHERE id = ?",
                (dest_folder_id, pid),
            )
            db.conn.commit()

            # Now safe to delete originals
            os.remove(src_file)
            for comp in companions:
                comp_src = os.path.join(src_dir, comp)
                if os.path.isfile(comp_src):
                    os.remove(comp_src)

            moved += 1

            if progress_cb:
                progress_cb(i + 1, total, photo["filename"])
    finally:
        # Always update folder counts so they stay consistent even if an
        # exception interrupts the move loop after some photos were committed.
        if moved > 0:
            db.update_folder_counts()

    return {"moved": moved, "errors": errors, "destination_folder_id": dest_folder_id}


def move_folder(db, folder_id, destination, progress_cb=None, developed_dir="",
                merge=False):
    """Move an entire folder (and subfolders) to a destination.

    The folder is placed inside the destination, preserving its name.
    E.g., moving /local/birds to /nas/photos creates /nas/photos/birds.

    Args:
        db: Database instance
        folder_id: ID of the source folder
        destination: absolute path to parent destination directory
        progress_cb: optional callback(current, total, filename)
        merge: when False (default), refuse to write into a destination
            that already exists — the safe all-or-nothing behavior. When
            True, merge/resume into the existing destination: rsync skips
            files already present with a matching checksum and copies only
            what is missing (this is how an interrupted move is resumed).
            Originals are deleted only after every source file is verified
            present at the destination. A failed merge never removes the
            destination, since it may hold the user's pre-existing files.
        developed_dir: optional path to the configured
            `darktable_output_dir`. When set, the folder's developed
            subdirectory — nested under a hash of its source path, see
            `export.developed_folder_key` — is rebased to match the new
            path after the move. Without this, exports silently fall
            back to RAW for every previously-developed photo in the
            moved folder.

    Returns dict with keys: moved (int), errors (list of str)
    """
    folder = db.conn.execute(
        "SELECT id, path, name FROM folders WHERE id = ?", (folder_id,)
    ).fetchone()
    if not folder:
        return {"moved": 0, "errors": ["Folder not found"]}

    src_path = folder["path"]
    folder_name = folder["name"] or os.path.basename(src_path)
    dest_path = resolve_folder_dest(src_path, folder["name"], destination)

    # Refuse a destination that overlaps the source. Moving a folder into
    # itself (or into one of its own descendants) would make the post-copy
    # rmtree(src) delete the only copy of the files. This is especially
    # dangerous for a merge, where a destination equal to the source passes
    # verification trivially (every source file is already "there") before the
    # delete wipes everything. See _destination_overlaps_source for the alias
    # surface (symlinks, Windows case folding, case-insensitive POSIX).
    if _destination_overlaps_source(src_path, dest_path):
        return {"moved": 0, "errors": [
            f"Destination overlaps the source folder: {dest_path}"
        ]}

    # Refuse moving into — or around — a destination Vireo already tracks as a
    # folder, regardless of whether that path currently exists on disk. A
    # correct tracked-tree merge needs recursive folder/photo reconciliation we
    # don't do here; a partial attempt would leave folders pointing at the
    # deleted source path, or collide on the folders.path UNIQUE constraint when
    # the source's children cascade onto a tracked descendant. Match the
    # destination itself and anything below it. The cases this feature exists
    # for — resuming an interrupted move, or moving into an untracked folder —
    # never hit this.
    #
    # Comparison goes through _path_equal_or_descends so symlink aliases, Windows
    # case folding, AND case-only aliases on case-insensitive POSIX (default
    # macOS APFS) all collapse to the same tracked row — otherwise a destination
    # reached via any of those would slip past and leave two folder rows managing
    # the same on-disk tree.
    tracked = _tracked_destination_overlap(db, folder_id, dest_path)
    if tracked:
        return {"moved": 0, "errors": [
            f"Destination overlaps a folder Vireo already manages "
            f"({tracked['path']}). Merging into or around a tracked folder "
            f"isn't supported."
        ]}

    dest_exists = os.path.exists(dest_path)
    if dest_exists and not merge:
        return {
            "moved": 0,
            "errors": [f"Destination already exists: {dest_path}"],
            "needs_merge": True,
        }

    if dest_exists:

        # Refuse if any same-name file already at the destination differs in
        # content. Never overwrite or later delete the user's data over a real
        # collision — only files that are byte-identical (a genuine resume) may
        # be treated as already-moved.
        conflict = _find_content_conflict(src_path, dest_path)
        if conflict is not None:
            return {"moved": 0, "errors": [
                f"Conflict: '{conflict}' already exists at the destination with "
                f"different content. Nothing was copied or deleted."
            ]}

    log.info("%s folder %s -> %s",
             "Merging" if dest_exists else "Moving", src_path, dest_path)

    # Use rsync for a robust copy. A merge/resume uses --ignore-existing so
    # rsync only creates files absent at the destination and NEVER overwrites
    # a file already there: this resumes an interrupted move (missing files get
    # copied, already-copied ones are left alone) while guaranteeing a merge
    # cannot destroy pre-existing destination data. A fresh move uses --checksum
    # for integrity. Any genuine same-name collision (an existing dest file that
    # differs from the source) is left untouched here and caught by the
    # post-copy verification below, which then refuses to delete the originals.
    rsync_flag = "--ignore-existing" if dest_exists else "--checksum"
    try:
        result = subprocess.run(
            ["rsync", "-a", rsync_flag, src_path + "/", dest_path + "/"],
            capture_output=True, text=True, timeout=3600,
        )
        if result.returncode != 0:
            return {"moved": 0, "errors": [f"rsync failed: {result.stderr.strip()}"]}
    except FileNotFoundError:
        # rsync not available, fall back to shutil
        try:
            if dest_exists:
                _copy_missing(src_path, dest_path)  # never overwrites
            else:
                shutil.copytree(src_path, dest_path)
        except Exception as exc:
            # Only remove a destination we created — never one that
            # pre-existed (a merge target may hold the user's own files).
            if not dest_exists:
                shutil.rmtree(dest_path, ignore_errors=True)
            return {"moved": 0, "errors": [f"Copy failed: {exc}"]}
    except subprocess.TimeoutExpired:
        return {"moved": 0, "errors": ["rsync timed out after 1 hour"]}

    # Verify before deleting originals.
    if dest_exists:
        # Merge: the destination may legitimately hold extra unrelated
        # files (and leftover temp files from an interrupted run), so a
        # count comparison is meaningless. Instead require that every
        # source file is present at the destination with a matching size.
        missing = _first_missing_source_file(src_path, dest_path)
        if missing is not None:
            return {"moved": 0, "errors": [
                f"Verification failed: '{missing}' missing, size mismatch, "
                f"or symlinked at destination. Originals preserved."
            ]}
    else:
        # Fresh move into a destination we created: a whole-tree file
        # count is a cheap, sufficient integrity check.
        src_count = sum(1 for _, _, files in os.walk(src_path) for _ in files)
        dst_count = sum(1 for _, _, files in os.walk(dest_path) for _ in files)
        if src_count != dst_count:
            shutil.rmtree(dest_path, ignore_errors=True)
            return {"moved": 0, "errors": [
                f"File count mismatch: source={src_count}, dest={dst_count}. Originals preserved."
            ]}

    # Count photos for progress
    all_photos = db.conn.execute(
        """SELECT p.id FROM photos p
           JOIN folders f ON f.id = p.folder_id
           WHERE f.path = ? OR f.path LIKE ?""",
        (src_path, src_path + "/%"),
    ).fetchall()
    total_photos = len(all_photos)

    # Update DB first: cascade folder paths (safer — if rmtree fails, the old
    # folder becomes an orphan on disk rather than the DB pointing to deleted
    # paths). A merge into an already-tracked destination is refused above, so
    # dest_path is never a different existing folder row here and this cascade
    # (root + all descendants) cannot collide with folders.path UNIQUE.
    db.move_folder_path(folder_id, dest_path)
    db.update_folder_counts()

    # Rebase any developed-output subdirs nested under the configured
    # darktable_output_dir. `developed_folder_key` hashes the folder's
    # path, so the DB update above just invalidated the old subdir's
    # implicit key — rename it on disk to match the new path, and cascade
    # to any descendant folders whose paths also shifted.
    if developed_dir:
        from export import relocate_developed_dir
        relocate_developed_dir(developed_dir, src_path, dest_path)
        # SQL LIKE treats `_` and `%` (and the escape char) as wildcards,
        # all of which are valid POSIX path characters. Without a strict
        # prefix guard, an unrelated folder like `/dXst/birds/fake` would
        # match a pattern like `/d_st/birds/%` and feed a bogus computed
        # old_path into relocate_developed_dir, mis-rebasing the wrong
        # developed subdir. Filter results by a literal prefix check.
        descendant_rows = db.conn.execute(
            "SELECT path FROM folders WHERE path LIKE ?",
            (dest_path + "/%",),
        ).fetchall()
        prefix = dest_path + "/"
        for row in descendant_rows:
            new_child = row["path"]
            if not new_child.startswith(prefix):
                continue
            old_child = src_path + new_child[len(dest_path):]
            relocate_developed_dir(developed_dir, old_child, new_child)

    # Delete originals
    log.info("Verification passed, deleting originals: %s", src_path)
    shutil.rmtree(src_path)

    if progress_cb:
        progress_cb(total_photos, total_photos, folder_name)

    return {"moved": total_photos, "errors": []}
