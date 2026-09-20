"""Review and remove files left in the source of a completed folder move."""

import contextlib
import hashlib
import json
import os
import stat


def _identity(path):
    info = os.lstat(path)
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns]


def review_source(db, source):
    """Inventory without following links; protect catalog photos in all workspaces."""
    source = os.path.abspath(source)
    resolved = os.path.realpath(source)
    if os.path.islink(source) or os.path.ismount(source):
        raise ValueError("Cannot clean up a linked folder or volume root")
    if not os.path.exists(source):
        # A disconnected volume or inaccessible parent is not evidence that
        # cleanup succeeded. Surface that error instead of claiming removal.
        os.stat(os.path.dirname(source))
        return {"state": "removed", "source_path": source, "files": [], "file_count": 0}
    if not os.path.isdir(source):
        raise ValueError("The original folder has been replaced by a file")
    for row in db.conn.execute(
        "SELECT DISTINCT f.path FROM folders f JOIN photos p ON p.folder_id = f.id"
    ):
        path = os.path.realpath(row["path"])
        if path == resolved or path.startswith(resolved + os.sep):
            raise ValueError("The original folder still contains cataloged photos")

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
        "xmp_count": sum(item["name"].lower().endswith(".xmp") for item in files),
    }


def finish_source(db, source):
    """Remove only the selected source when empty, and report remaining files."""
    try:
        review = review_source(db, source)
        if review["state"] == "empty":
            os.rmdir(source)
            review["state"] = "removed"
        return {key: review[key] for key in
                ("state", "source_path", "file_count", "xmp_count") if key in review}
    except (OSError, ValueError) as exc:
        return {"state": "unavailable", "source_path": source, "error": str(exc)}


def cleanup_source(db, source, token, trash_paths):
    """Trash exactly the reviewed files; never fall back to permanent deletion."""
    review = review_source(db, source)
    if not token or token != review.get("review_token"):
        raise ValueError("The folder changed. Review remaining files again before cleaning up")
    paths = []
    for item in review["files"]:
        path = os.path.join(source, item["name"])
        if _identity(path) != item["identity"]:
            raise ValueError("The folder changed. Review remaining files again before cleaning up")
        paths.append(path)
    trashed, _successful, failures = trash_paths(paths) if paths else (0, set(), [])
    # rmdir cannot remove a folder containing a new file or a failed Trash item.
    for relative in sorted(review["directories"], key=lambda p: p.count(os.sep), reverse=True):
        with contextlib.suppress(OSError):
            os.rmdir(os.path.join(source, relative))
    result = finish_source(db, source)
    result.update(trashed=trashed, failures=failures)
    return result
