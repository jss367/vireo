"""Photo and folder move operations with copy-verify-delete safety."""

import logging
import os
import shutil
import subprocess

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


def move_folder(db, folder_id, destination, progress_cb=None, developed_dir=""):
    """Move an entire folder (and subfolders) to a destination.

    The folder is placed inside the destination, preserving its name.
    E.g., moving /local/birds to /nas/photos creates /nas/photos/birds.

    Args:
        db: Database instance
        folder_id: ID of the source folder
        destination: absolute path to parent destination directory
        progress_cb: optional callback(current, total, filename)
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
    dest_path = os.path.join(destination, folder_name)

    if os.path.exists(dest_path):
        return {"moved": 0, "errors": [f"Destination already exists: {dest_path}"]}

    log.info("Moving folder %s -> %s", src_path, dest_path)

    # Use rsync for robust copy with checksums
    try:
        result = subprocess.run(
            ["rsync", "-a", "--checksum", src_path + "/", dest_path + "/"],
            capture_output=True, text=True, timeout=3600,
        )
        if result.returncode != 0:
            return {"moved": 0, "errors": [f"rsync failed: {result.stderr.strip()}"]}
    except FileNotFoundError:
        # rsync not available, fall back to shutil
        try:
            shutil.copytree(src_path, dest_path)
        except Exception as exc:
            shutil.rmtree(dest_path, ignore_errors=True)
            return {"moved": 0, "errors": [f"Copy failed: {exc}"]}
    except subprocess.TimeoutExpired:
        return {"moved": 0, "errors": ["rsync timed out after 1 hour"]}

    # Verify file count
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

    # Update DB first: cascade folder paths (safer — if rmtree fails,
    # old folder becomes orphan on disk rather than DB pointing to deleted paths)
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
