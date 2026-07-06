"""Verified recovery helpers for abandoned pipeline staging folders.

Old import-through-process runs staged photos under
``<vireo_dir>/staging/pipeline-*`` before archiving them. If a run failed or
was cancelled, those folders may be the only remaining copy of files that did
not make it to the archive. This module never deletes based on catalog state
alone: every staged file must match an enumerable archive folder entry by
filename and byte size before cleanup is allowed.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass


def _missing_mount_root(path: str) -> str | None:
    """Return a likely unavailable mount root for common archive locations."""
    posix = os.path.expanduser(path).replace("\\", "/")
    parts = posix.split("/")
    if len(parts) >= 3 and parts[0] == "" and parts[1] in {"Volumes", "mnt"}:
        root = f"/{parts[1]}/{parts[2]}"
        return root if not os.path.lexists(root) else None
    if len(parts) >= 4 and parts[0] == "" and parts[1] == "media":
        root = f"/media/{parts[2]}/{parts[3]}"
        return root if not os.path.lexists(root) else None
    return None


def _is_relative_to(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([
            os.path.realpath(path),
            os.path.realpath(root),
        ]) == os.path.realpath(root)
    except (OSError, ValueError):
        return False


@dataclass(frozen=True)
class StagingEntry:
    cleanup_root: str
    source_root: str


def staging_base(vireo_dir: str) -> str:
    return os.path.join(vireo_dir, "staging")


def _entry_for_pipeline_dir(path: str) -> StagingEntry:
    """Return cleanup root and likely import source root for one pipeline dir."""
    children = []
    direct_files = []
    try:
        for name in os.listdir(path):
            full = os.path.join(path, name)
            if os.path.isdir(full):
                children.append(full)
            elif os.path.isfile(full):
                direct_files.append(full)
    except OSError:
        pass
    # local_processing.staging_root created pipeline-*/<final-destination-name>.
    # If the parent contains exactly that one leaf and no direct files, use the
    # leaf as the import source so a recovery import preserves the original
    # destination base semantics.
    source_root = children[0] if len(children) == 1 and not direct_files else path
    return StagingEntry(cleanup_root=path, source_root=source_root)


def discover_orphaned_staging(vireo_dir: str) -> list[dict]:
    """List abandoned pipeline staging roots with cheap file/size summaries."""
    base = staging_base(vireo_dir)
    if not os.path.isdir(base):
        return []
    entries: list[dict] = []
    for name in sorted(os.listdir(base)):
        if not name.startswith("pipeline-"):
            continue
        cleanup_root = os.path.join(base, name)
        if not os.path.isdir(cleanup_root):
            continue
        entry = _entry_for_pipeline_dir(cleanup_root)
        file_count = 0
        total_bytes = 0
        for root, _dirs, files in os.walk(entry.source_root):
            for filename in files:
                path = os.path.join(root, filename)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                if not os.path.isfile(path):
                    continue
                file_count += 1
                total_bytes += st.st_size
        entries.append({
            "path": entry.cleanup_root,
            "source_root": entry.source_root,
            "name": os.path.basename(entry.cleanup_root),
            "file_count": file_count,
            "bytes": total_bytes,
        })
    return entries


def _resolve_entry(vireo_dir: str, cleanup_root: str) -> StagingEntry:
    base = staging_base(vireo_dir)
    root = os.path.realpath(cleanup_root)
    if not _is_relative_to(root, base):
        raise ValueError("staging path is outside the Vireo staging directory")
    if not os.path.basename(root).startswith("pipeline-"):
        raise ValueError("staging path is not a pipeline staging directory")
    if not os.path.isdir(root):
        raise ValueError("staging path was not found")
    return _entry_for_pipeline_dir(root)


def _catalog_candidates(db, filename: str, size: int, staging_root: str) -> list[dict]:
    ws_id = db._ws_id()
    rows = db.conn.execute(
        """SELECT p.id AS photo_id, p.filename, p.file_size, f.path AS folder_path
             FROM photos p
             JOIN folders f ON f.id = p.folder_id
             JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                                      AND wf.workspace_id = ?
            WHERE p.filename = ? AND p.file_size = ?
            ORDER BY f.path, p.id""",
        (ws_id, filename, int(size)),
    ).fetchall()
    out = []
    for row in rows:
        folder_path = row["folder_path"]
        if folder_path and _is_relative_to(folder_path, staging_root):
            continue
        out.append(dict(row))
    return out


def _archive_file_status(
    path: str,
    expected_size: int,
    listing_cache: dict[str, set[str] | OSError] | None = None,
) -> tuple[str, str | None]:
    mount_root = _missing_mount_root(path) or _missing_mount_root(os.path.dirname(path))
    if mount_root:
        return "unreachable", f"archive mount root is unavailable: {mount_root}"
    folder = os.path.dirname(path)
    cache = listing_cache if listing_cache is not None else {}
    cached = cache.get(folder)
    if cached is None:
        try:
            cached = set(os.listdir(folder))
        except OSError as exc:
            cached = exc
        cache[folder] = cached
    if isinstance(cached, OSError):
        return "unreachable", f"archive folder is not enumerable: {folder} ({cached})"
    names = cached
    basename = os.path.basename(path)
    if basename not in names:
        return "missing", f"archive folder is reachable but {basename} is not present"
    try:
        st = os.stat(path)
    except OSError as exc:
        return "unreachable", f"archive file could not be stat'ed: {path} ({exc})"
    if st.st_size != expected_size:
        return "size_mismatch", (
            f"archive file size differs: expected {expected_size}, got {st.st_size}"
        )
    return "verified", None


def _infer_destination(source_root: str, staged_file: str, folder_path: str) -> str:
    rel_dir = os.path.relpath(os.path.dirname(staged_file), source_root)
    if rel_dir in ("", "."):
        return folder_path
    rel_parts = [p for p in rel_dir.split(os.sep) if p and p != "."]
    folder_parts = os.path.normpath(folder_path).split(os.sep)
    if len(folder_parts) >= len(rel_parts) and folder_parts[-len(rel_parts):] == rel_parts:
        base_parts = folder_parts[:-len(rel_parts)]
        if not base_parts:
            return os.sep
        return os.sep.join(base_parts) or os.sep
    return folder_path


def verify_orphaned_staging(db, vireo_dir: str, cleanup_root: str) -> dict:
    """Reconcile a staging folder against the catalog and archive filesystem."""
    entry = _resolve_entry(vireo_dir, cleanup_root)
    files: list[tuple[str, str, int]] = []
    for root, _dirs, filenames in os.walk(entry.source_root):
        for filename in sorted(filenames):
            path = os.path.join(root, filename)
            try:
                st = os.stat(path)
            except OSError:
                continue
            if not os.path.isfile(path):
                continue
            rel = os.path.relpath(path, entry.source_root)
            files.append((path, rel, st.st_size))

    result = {
        "path": entry.cleanup_root,
        "source_root": entry.source_root,
        "name": os.path.basename(entry.cleanup_root),
        "file_count": len(files),
        "bytes": sum(size for _path, _rel, size in files),
        "verified": 0,
        "unaccounted": 0,
        "unreachable": 0,
        "details": [],
        "can_delete": False,
        "status": "unknown",
        "inferred_destination": None,
    }
    inferred: dict[str, int] = {}
    archive_listing_cache: dict[str, set[str] | OSError] = {}

    for staged_path, rel_path, size in files:
        candidates = _catalog_candidates(
            db, os.path.basename(staged_path), size, entry.cleanup_root,
        )
        detail = {
            "path": staged_path,
            "rel_path": rel_path,
            "size": size,
            "status": "unaccounted",
            "reason": "no cataloged archive photo matches this filename and size",
            "archive_path": None,
        }
        if not candidates:
            result["unaccounted"] += 1
            result["details"].append(detail)
            continue

        saw_unreachable = False
        last_reason = None
        for candidate in candidates:
            archive_path = os.path.join(
                candidate["folder_path"], candidate["filename"],
            )
            status, reason = _archive_file_status(
                archive_path, size, archive_listing_cache,
            )
            if status == "verified":
                detail.update({
                    "status": "verified",
                    "reason": None,
                    "archive_path": archive_path,
                })
                result["verified"] += 1
                dest = _infer_destination(
                    entry.source_root, staged_path, candidate["folder_path"],
                )
                inferred[dest] = inferred.get(dest, 0) + 1
                break
            if status == "unreachable":
                saw_unreachable = True
            last_reason = reason
        else:
            if saw_unreachable:
                detail["status"] = "unreachable"
                detail["reason"] = last_reason or "archive location is unreachable"
                result["unreachable"] += 1
            else:
                detail["status"] = "unaccounted"
                detail["reason"] = last_reason or "archive copy was not verified"
                result["unaccounted"] += 1
        result["details"].append(detail)

    if inferred:
        result["inferred_destination"] = max(inferred.items(), key=lambda kv: kv[1])[0]
    if result["unreachable"]:
        result["status"] = "unreachable"
    elif result["unaccounted"]:
        result["status"] = "needs_import"
    else:
        result["status"] = "safe_to_delete"
        result["can_delete"] = True
    return result


def delete_verified_staging(db, vireo_dir: str, cleanup_root: str) -> dict:
    """Delete a staging folder only after a fresh all-files-verified pass."""
    result = verify_orphaned_staging(db, vireo_dir, cleanup_root)
    if not result["can_delete"]:
        raise ValueError("staging folder is not fully verified in the archive")
    shutil.rmtree(result["path"])
    result["deleted"] = True
    return result
