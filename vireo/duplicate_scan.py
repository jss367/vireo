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


def _build_unresolved_proposal(db, group):
    """Return a proposal dict for an unresolved group, or None on race."""
    photo_ids = group["photo_ids"]
    placeholders = ",".join("?" * len(photo_ids))
    rows = db.conn.execute(
        f"""SELECT p.id, p.filename, p.file_mtime, p.rating, p.file_size,
                   f.path AS folder_path
            FROM photos p
            LEFT JOIN folders f ON f.id = p.folder_id
            WHERE p.id IN ({placeholders}) AND p.flag != 'rejected'""",
        photo_ids,
    ).fetchall()

    info_by_id = {r["id"]: _row_to_info(r, r["folder_path"]) for r in rows}
    candidates = [
        DupCandidate(id=r["id"], path=info_by_id[r["id"]]["path"],
                     mtime=r["file_mtime"] or 0.0)
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
    return {
        "file_hash": group["file_hash"],
        "status": "unresolved",
        "winner": info_by_id[winner_id],
        "losers": losers,
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
    photo_ids = group["photo_ids"]
    placeholders = ",".join("?" * len(photo_ids))
    rows = db.conn.execute(
        f"""SELECT p.id, p.filename, p.file_mtime, p.rating, p.file_size, p.flag,
                   f.path AS folder_path
            FROM photos p
            LEFT JOIN folders f ON f.id = p.folder_id
            WHERE p.id IN ({placeholders})""",
        photo_ids,
    ).fetchall()
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

    candidates = [
        DupCandidate(id=r["id"], path=info_by_id[r["id"]]["path"],
                     mtime=r["file_mtime"] or 0.0)
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
    return {
        "file_hash": group["file_hash"],
        "status": "resolved",
        "winner": info_by_id[kept[0]["id"]],
        "losers": losers,
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
