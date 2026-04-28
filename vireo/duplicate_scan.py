"""Background job: scan the DB for duplicate groups and propose a resolution.

Read-only. Does not apply — the UI shows the preview and a user action (the
``/api/duplicates/apply`` endpoint) actually flags the losers as rejected.
"""
import os

from duplicate_buckets import bucket_unresolved_proposals
from duplicates import DupCandidate, resolve_duplicates

# SQLite's legacy ``SQLITE_MAX_VARIABLE_NUMBER`` cap is 999 on older builds.
# An auto-resolved group can exceed that when many `-2` copies of one
# file accumulate over repeated scans, so any single-statement IN-clause
# over a group's photo_ids would fail the duplicate-scan job entirely.
# Sized below 999 to leave headroom for additional bound parameters.
_SQL_PARAM_CHUNK = 900


def _fetch_photo_rows(db, photo_ids, columns, where_extra=""):
    """Run ``SELECT {columns} FROM photos p LEFT JOIN folders f WHERE p.id IN (...) {where_extra}``
    in chunks below the SQLite parameter cap. Returns a flat list of rows.
    """
    rows = []
    for i in range(0, len(photo_ids), _SQL_PARAM_CHUNK):
        chunk = photo_ids[i:i + _SQL_PARAM_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        sql = (
            f"SELECT {columns} "
            f"FROM photos p LEFT JOIN folders f ON f.id = p.folder_id "
            f"WHERE p.id IN ({placeholders}){where_extra}"
        )
        rows.extend(db.conn.execute(sql, chunk).fetchall())
    return rows


def _row_to_info(row, folder_path):
    """Shape a photos row into the dict the UI consumes for a proposal entry.

    ``exists`` is populated by stat-ing the path. The resolver uses it via
    Rule 0 (present beats missing) and the UI surfaces it as a warning so the
    user doesn't trash surviving copies of a row whose "winner" file is gone.
    """
    filename = row["filename"] or ""
    full_path = os.path.join(folder_path or "", filename)
    return {
        "id": row["id"],
        "filename": filename,
        "path": full_path,
        "mtime": row["file_mtime"] or 0.0,
        "rating": row["rating"] if row["rating"] is not None else 0,
        "file_size": row["file_size"] if row["file_size"] is not None else 0,
        "exists": os.path.exists(full_path),
    }


def _build_unresolved_proposal(db, group):
    """Return a proposal dict for an unresolved group, or None on race."""
    rows = _fetch_photo_rows(
        db, group["photo_ids"],
        columns="p.id, p.filename, p.file_mtime, p.rating, p.file_size, "
                "f.path AS folder_path",
        where_extra=" AND p.flag != 'rejected'",
    )

    info_by_id = {r["id"]: _row_to_info(r, r["folder_path"]) for r in rows}
    candidates = [
        DupCandidate(id=r["id"], path=info_by_id[r["id"]]["path"],
                     mtime=r["file_mtime"] or 0.0,
                     exists=info_by_id[r["id"]]["exists"])
        for r in rows
    ]
    if len(candidates) < 2:
        # Race: rows could have been rejected between find_duplicate_groups
        # and this lookup. Skip silently.
        return None
    winner_id, losers_with_reasons = resolve_duplicates(candidates)
    losers = []
    for lid, reason in losers_with_reasons:
        linfo = dict(info_by_id[lid])
        linfo["reason"] = reason
        losers.append(linfo)
    all_missing = not any(info["exists"] for info in info_by_id.values())
    return {
        "file_hash": group["file_hash"],
        "status": "unresolved",
        "winner": info_by_id[winner_id],
        "losers": losers,
        "all_missing": all_missing,
    }


def _build_resolved_proposal(db, group):
    """Return a proposal dict for a group already auto-resolved during scan.

    The kept (non-rejected) row is the winner; rejected rows sharing the hash
    are losers. Each loser is annotated with the resolver reason that the
    earlier auto-resolve would have produced — recomputed here because the
    DB doesn't persist the per-loser reason. Recomputing is safe because the
    resolver is pure and deterministic.

    Loser rows include a ``rejected: true`` flag so the UI can render them
    differently from "will-be-rejected" losers in unresolved groups.

    Returns None if a race shrinks the group below 2 active rows.
    """
    rows = _fetch_photo_rows(
        db, group["photo_ids"],
        columns="p.id, p.filename, p.file_mtime, p.rating, p.file_size, p.flag, "
                "f.path AS folder_path",
    )
    if len(rows) < 2:
        return None

    info_by_id = {r["id"]: _row_to_info(r, r["folder_path"]) for r in rows}
    kept = [r for r in rows if r["flag"] != "rejected"]
    rejected = [r for r in rows if r["flag"] == "rejected"]
    if len(kept) != 1 or not rejected:
        # Race: another resolution ran between find_duplicate_groups and now,
        # or the group's status changed shape. Skip — find_duplicate_groups
        # will surface it again on the next scan.
        return None

    # Auto-reopen: if the kept file is gone but a rejected sibling still
    # exists on disk, the DB-frozen winner is now a ghost while a survivor
    # is sitting unhandled. Un-reject the group and rebuild as unresolved
    # so Rule 0 (present beats missing) promotes the survivor.
    if not info_by_id[kept[0]["id"]]["exists"] and any(
        info_by_id[r["id"]]["exists"] for r in rejected
    ):
        db.reopen_duplicate_group(group["file_hash"])
        return _build_unresolved_proposal(db, group)

    candidates = [
        DupCandidate(id=r["id"], path=info_by_id[r["id"]]["path"],
                     mtime=r["file_mtime"] or 0.0,
                     exists=info_by_id[r["id"]]["exists"])
        for r in rows
    ]
    _winner_id, losers_with_reasons = resolve_duplicates(candidates)
    reasons = dict(losers_with_reasons)

    losers = []
    for r in rejected:
        linfo = dict(info_by_id[r["id"]])
        linfo["reason"] = reasons.get(r["id"], "auto-resolved")
        linfo["rejected"] = True
        losers.append(linfo)
    all_missing = not any(info["exists"] for info in info_by_id.values())
    return {
        "file_hash": group["file_hash"],
        "status": "resolved",
        "winner": info_by_id[kept[0]["id"]],
        "losers": losers,
        "all_missing": all_missing,
    }


def run_duplicate_scan(job, db, include_resolved=True):
    """Work function for ``JobRunner.start('duplicate-scan', ...)``.

    Updates ``job['progress']`` as it walks groups. Returns a dict with a
    ``proposals`` list the UI can render; each proposal contains the
    winner/losers with full paths, mtimes, ratings, file sizes, a
    per-loser reason supplied by :func:`duplicates.resolve_duplicates`,
    and a ``status`` field of ``'unresolved'`` or ``'resolved'``.

    ``include_resolved=True`` surfaces auto-resolved groups (kept row plus
    rejected hash-twins) so the user can review them and clean up loser
    files left on disk. The auto-resolve path during scan flags those rows
    as rejected silently, so without this they'd be invisible to the user.
    """
    groups = db.find_duplicate_groups(include_resolved=include_resolved)
    total = len(groups)
    job["progress"] = {"current": 0, "total": total, "current_file": ""}

    proposals = []
    for i, g in enumerate(groups):
        if g.get("status") == "resolved":
            proposal = _build_resolved_proposal(db, g)
        else:
            proposal = _build_unresolved_proposal(db, g)
        if proposal is None:
            continue

        proposals.append(proposal)
        job["progress"]["current"] = i + 1
        # Show the winner's path (human-readable) rather than an opaque hash.
        job["progress"]["current_file"] = proposal["winner"]["path"]

    return {
        "proposals": proposals,
        "buckets": bucket_unresolved_proposals(proposals),
        "group_count": total,
        "loser_count": sum(
            len(p["losers"]) for p in proposals if p["status"] == "unresolved"
        ),
        "resolved_group_count": sum(
            1 for p in proposals if p["status"] == "resolved"
        ),
        "resolved_loser_count": sum(
            len(p["losers"]) for p in proposals if p["status"] == "resolved"
        ),
    }
