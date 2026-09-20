"""Review and remove files left in the source of a completed folder move."""

import hashlib
import json
import os
import sqlite3
import stat


def _identity(path):
    info = os.lstat(path)
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns]


def review_source(db, source, expected_device=None, expected_inode=None):
    """Inventory without following links; protect catalog photos in all workspaces."""
    source = os.path.abspath(source)
    resolved = os.path.realpath(source)
    _protect_local_sources(db, source)
    if os.path.islink(source) or os.path.ismount(source):
        raise ValueError("Cannot clean up a linked folder or volume root")
    try:
        source_stat = os.lstat(source)
    except FileNotFoundError:
        # A disconnected volume or inaccessible parent is not evidence that
        # cleanup succeeded. Surface that error instead of claiming removal.
        parent_stat = os.stat(os.path.dirname(source))
        if expected_device is None or parent_stat.st_dev != expected_device:
            raise ValueError("The original volume is unavailable or cannot be verified; reconnect it and review again") from None
        return {"state": "removed", "source_path": source, "files": [], "file_count": 0,
                "source_device": expected_device, "source_inode": expected_inode}
    if expected_device is not None and source_stat.st_dev != expected_device:
        raise ValueError("The original volume changed; reconnect it and review again")
    if expected_inode is not None and source_stat.st_ino != expected_inode:
        raise ValueError("The original folder was replaced; review the replacement separately")
    if not stat.S_ISDIR(source_stat.st_mode):
        raise ValueError("The original folder has been replaced by a file")
    source_rows = _source_folder_rows(db, source)
    for row in source_rows:
        if db.conn.execute("SELECT 1 FROM photos WHERE folder_id = ? LIMIT 1", (row["id"],)).fetchone():
            raise ValueError("The original folder still contains cataloged photos")

    for row in source_rows or [{"id": None, "path": source}]:
        if _folder_owned_elsewhere(db, row):
            raise ValueError("The original folder is still used by another workspace")

    files, directories, identities = [], [], []

    def fail(error):
        raise error

    for root, dirs, names in os.walk(source, followlinks=False, onerror=fail):
        for name in sorted(dirs + names):
            path = os.path.join(root, name)
            identity = _identity(path)
            mode = identity[2]
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)) or os.path.islink(path):
                raise ValueError("Review linked or special files in the original folder manually")
            if stat.S_ISDIR(mode) and os.path.ismount(path):
                raise ValueError("Cannot clean up a folder containing another mounted volume")
            relative = os.path.relpath(path, source)
            identities.append([relative, identity])
            if stat.S_ISDIR(mode):
                directories.append(relative)
            else:
                files.append({"name": relative, "size": identity[3], "identity": identity})
            if len(identities) > 10000:
                raise ValueError("This folder has too many remaining entries; review it manually")
    files.sort(key=lambda item: item["name"])
    signature = [resolved, _identity(source), sorted(identities)]
    token = hashlib.sha256(json.dumps(signature).encode()).hexdigest()
    return {
        "state": "remaining" if files or directories else "empty",
        "source_path": source, "files": files, "file_count": len(files),
        "directories": directories, "review_token": token,
        "directory_inodes": {name: identity[1] for name, identity in identities
                             if stat.S_ISDIR(identity[2])},
        "xmp_count": sum(item["name"].lower().endswith(".xmp") for item in files),
        "source_device": source_stat.st_dev,
        "source_inode": source_stat.st_ino,
    }


def _protect_local_sources(db, source):
    """Protect original and managed paths even after staging rebases the catalog."""
    try:
        from .path_guard import path_contains
    except ImportError:
        from path_guard import path_contains
    mappings = db.conn.execute(
        "SELECT source_path, local_path FROM local_workspace_folders "
        "UNION SELECT source_path, local_path FROM local_folder_mappings"
    )
    for mapping in mappings:
        for path in (mapping["source_path"], mapping["local_path"]):
            if path_contains(source, path) or path_contains(path, source):
                raise ValueError("The original folder is used by Work Locally; sync or discard the local copy before cleanup")


def _folder_owned_elsewhere(db, folder):
    """Include inherited root membership without recreating explicitly removed links."""
    try:
        from .move import _path_equal_or_descends
    except ImportError:
        from move import _path_equal_or_descends
    if folder["id"] is not None and any(
        workspace["id"] != db._active_workspace_id
        for workspace in db.get_folder_workspaces(folder["id"])
    ):
        return True
    # The database accessor above uses stored path strings. Also cover physical
    # aliases of recursive roots while honoring the same removal view.
    for root in db.conn.execute(
        "SELECT wf.workspace_id, f.path FROM workspace_folders wf "
        "JOIN folders f ON f.id = wf.folder_id "
        "WHERE wf.is_root = 1 AND wf.workspace_id IS NOT ?", (db._active_workspace_id,),
    ):
        if folder["id"] in db._removed_workspace_folder_ids(root["workspace_id"]):
            continue
        if _path_equal_or_descends(folder["path"], root["path"], case_insensitive_root=None):
            return True
    return False


def _source_folder_rows(db, source):
    """Find catalog rows under the selected physical source, including aliases."""
    try:
        from .move import _path_equal_or_descends
    except ImportError:
        from move import _path_equal_or_descends
    # Existing ancestors are compared by filesystem identity, so case aliases
    # work on APFS/NTFS without conflating distinct case-sensitive directories.
    # Do not create case-probe directories during a read-only review.
    return [row for row in db.conn.execute("SELECT id, path FROM folders")
            if _path_equal_or_descends(row["path"], source, case_insensitive_root=None)]


def _remove_empty_source(db, source, expected_device, expected_inode):
    """Retire empty catalog rows and remove the directory under a writer lock."""
    db.conn.execute("BEGIN IMMEDIATE")
    try:
        # Recheck after obtaining the writer lock: a scanner may have added
        # photos or workspace links since the initial filesystem inventory.
        review = review_source(db, source, expected_device, expected_inode)
        if review["state"] not in ("empty", "removed"):
            raise ValueError("The folder changed. Review remaining files again before cleaning up")
        rows = sorted(_source_folder_rows(db, source),
                      key=lambda row: len(row["path"]), reverse=True)
        for row in rows:
            # A missing directory still needs the same catalog protections.
            if db.conn.execute("SELECT 1 FROM photos WHERE folder_id = ? LIMIT 1", (row["id"],)).fetchone():
                raise ValueError("The original folder still contains cataloged photos")
            if _folder_owned_elsewhere(db, row):
                raise ValueError("The original folder is still used by another workspace")
            db.conn.execute("DELETE FROM workspace_folders WHERE folder_id = ?", (row["id"],))
            db.conn.execute("DELETE FROM folders WHERE id = ?", (row["id"],))
            db.conn.execute("UPDATE photos SET last_move_source_folder_path = NULL "
                            "WHERE last_move_source_folder_path = ?", (row["path"],))
        # A failed rmdir rolls back the catalog changes. It never removes new
        # files, parent folders, or a directory retained by another workspace.
        if review["state"] == "empty":
            os.rmdir(source)
        db.conn.commit()
    except Exception:
        db.conn.rollback()
        raise
    if db._active_workspace_id is not None:
        db._new_images_cache.invalidate_workspaces(db._db_path, [db._active_workspace_id])


def finish_source(db, source, expected_device=None, expected_inode=None):
    """Remove only the selected source when empty, and report remaining files."""
    try:
        review = review_source(db, source, expected_device, expected_inode)
        if review["state"] in ("empty", "removed"):
            _remove_empty_source(db, source, review["source_device"], review.get("source_inode"))
            review["state"] = "removed"
        return {key: review[key] for key in
                ("state", "source_path", "file_count", "xmp_count", "source_device", "source_inode") if key in review}
    except (OSError, ValueError, sqlite3.Error) as exc:
        result = {"state": "unavailable", "source_path": source, "error": str(exc)}
        if expected_device is not None:
            result["source_device"] = expected_device
        if expected_inode is not None:
            result["source_inode"] = expected_inode
        return result


def cleanup_source(db, source, token, trash_paths):
    """Trash exactly the reviewed files; never fall back to permanent deletion."""
    review = review_source(db, source)
    if not token or token != review.get("review_token"):
        raise ValueError("The folder changed. Review remaining files again before cleaning up")
    trashed, failures = 0, []
    for item in review["files"]:
        path = os.path.join(source, item["name"])
        try:
            # Do not prevalidate a whole batch and then hand every pathname
            # to Trash: an editor can replace later files while earlier ones
            # are being processed. Recheck each file and its directory chain
            # immediately before the individual Trash operation.
            root_identity = _identity(source)
            if root_identity[:2] != [review["source_device"], review["source_inode"]]:
                raise ValueError("The original folder changed; review again")
            parent = os.path.dirname(item["name"])
            while parent:
                identity = _identity(os.path.join(source, parent))
                if not stat.S_ISDIR(identity[2]) or identity[:2] != [
                    review["source_device"], review["directory_inodes"][parent],
                ]:
                    raise ValueError("A containing folder changed; review again")
                parent = os.path.dirname(parent)
            if _identity(path) != item["identity"]:
                raise ValueError("File changed since review; review again before moving it to Trash")
            moved, _successful, failed = trash_paths([path])
            trashed += moved
            failures.extend(failed)
        except (OSError, ValueError) as exc:
            failures.append({"path": path, "error": str(exc)})
    # rmdir cannot remove a folder containing a new file or a failed Trash item.
    for relative in sorted(review["directories"], key=lambda p: p.count(os.sep), reverse=True):
        finish_source(db, os.path.join(source, relative), review["source_device"],
                      review["directory_inodes"][relative])
    result = finish_source(db, source, review["source_device"], review["source_inode"])
    result.update(trashed=trashed, failures=failures)
    return result
