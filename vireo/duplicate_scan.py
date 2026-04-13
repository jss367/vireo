"""Background job: scan the DB for duplicate groups and propose a resolution.

Read-only. Does not apply — the UI shows the preview and a user action (the
``/api/duplicates/apply`` endpoint) actually flags the losers as rejected.
"""
import logging
import os

from duplicates import DupCandidate, _has_dup_suffix, resolve_duplicates

log = logging.getLogger(__name__)


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
    }


def _loser_reason(winner_info, loser_info):
    """Return a short string describing why this loser was picked.

    Matches the tiebreaker cascade in ``duplicates.resolve_duplicates``.
    """
    w_dirty = _has_dup_suffix(winner_info["path"])
    l_dirty = _has_dup_suffix(loser_info["path"])
    if l_dirty and not w_dirty:
        return "filename has dup suffix"
    w_len = len(winner_info["path"])
    l_len = len(loser_info["path"])
    if l_len > w_len:
        return "longer path"
    if loser_info["mtime"] > winner_info["mtime"]:
        return "later mtime"
    return "higher id"


def run_duplicate_scan(job, db):
    """Work function for ``JobRunner.start('duplicate-scan', ...)``.

    Updates ``job['progress']`` as it walks groups. Returns a dict with a
    ``proposals`` list the UI can render; each proposal contains the
    winner/losers with full paths, mtimes, ratings, and a per-loser reason.
    """
    groups = db.find_duplicate_groups()
    total = len(groups)
    job["progress"] = {"current": 0, "total": total, "current_file": ""}

    proposals = []
    for i, g in enumerate(groups):
        photo_ids = g["photo_ids"]
        placeholders = ",".join("?" * len(photo_ids))
        rows = db.conn.execute(
            f"""SELECT p.id, p.filename, p.file_mtime, p.rating,
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
        winner_id, loser_ids = resolve_duplicates(candidates)
        winner_info = info_by_id[winner_id]
        losers = []
        for lid in loser_ids:
            linfo = dict(info_by_id[lid])
            linfo["reason"] = _loser_reason(winner_info, linfo)
            losers.append(linfo)

        proposals.append({
            "file_hash": g["file_hash"],
            "winner": winner_info,
            "losers": losers,
        })
        job["progress"]["current"] = i + 1
        job["progress"]["current_file"] = g["file_hash"]

    return {
        "proposals": proposals,
        "group_count": total,
        "loser_count": sum(len(p["losers"]) for p in proposals),
    }
