"""Background job: scan the DB for duplicate groups and propose a resolution.

Read-only. Does not apply — the UI shows the preview and a user action (the
``/api/duplicates/apply`` endpoint) actually flags the losers as rejected.
"""
import os

from duplicates import DupCandidate, resolve_duplicates


def _row_to_info(row, folder_path):
    """Shape a photos row into the dict the UI consumes for a proposal entry."""
    filename = row["filename"] or ""
    full_path = os.path.join(folder_path or "", filename)
    return {
        "id": row["id"],
        "filename": filename,
        "path": full_path,
        "mtime": row["file_mtime"] or 0.0,
        "rating": row["rating"] if row["rating"] is not None else 0,
        "file_size": row["file_size"] if row["file_size"] is not None else 0,
    }


def run_duplicate_scan(job, db):
    """Work function for ``JobRunner.start('duplicate-scan', ...)``.

    Updates ``job['progress']`` as it walks groups. Returns a dict with a
    ``proposals`` list the UI can render; each proposal contains the
    winner/losers with full paths, mtimes, ratings, file sizes, and a
    per-loser reason supplied by :func:`duplicates.resolve_duplicates` (the
    single source of truth for tiebreaker reasons).
    """
    groups = db.find_duplicate_groups()
    total = len(groups)
    job["progress"] = {"current": 0, "total": total, "current_file": ""}

    proposals = []
    for i, g in enumerate(groups):
        photo_ids = g["photo_ids"]
        placeholders = ",".join("?" * len(photo_ids))
        rows = db.conn.execute(
            f"""SELECT p.id, p.filename, p.file_mtime, p.rating, p.file_size,
                       f.path AS folder_path
                FROM photos p
                LEFT JOIN folders f ON f.id = p.folder_id
                WHERE p.id IN ({placeholders})""",
            photo_ids,
        ).fetchall()

        info_by_id = {r["id"]: _row_to_info(r, r["folder_path"]) for r in rows}
        candidates = [
            DupCandidate(id=r["id"], path=info_by_id[r["id"]]["path"],
                         mtime=r["file_mtime"] or 0.0)
            for r in rows
        ]
        if len(candidates) < 2:
            # Race: rows could have been rejected between the group query and
            # this lookup. Skip silently.
            continue
        winner_id, losers_with_reasons = resolve_duplicates(candidates)
        winner_info = info_by_id[winner_id]
        losers = []
        for lid, reason in losers_with_reasons:
            linfo = dict(info_by_id[lid])
            linfo["reason"] = reason
            losers.append(linfo)

        proposals.append({
            "file_hash": g["file_hash"],
            "winner": winner_info,
            "losers": losers,
        })
        job["progress"]["current"] = i + 1
        # Show the winner's path (human-readable) rather than an opaque hash.
        job["progress"]["current_file"] = winner_info["path"]

    return {
        "proposals": proposals,
        "group_count": total,
        "loser_count": sum(len(p["losers"]) for p in proposals),
    }
