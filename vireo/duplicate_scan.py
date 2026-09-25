"""Background job: scan the DB for duplicate groups and propose a resolution.

Read-only. Does not apply — the UI shows the preview and a user action (the
``/api/duplicates/apply`` endpoint) actually flags the losers as rejected.
"""
import os

from duplicate_buckets import bucket_unresolved_proposals
from duplicates import DupCandidate, resolve_duplicates
from volume_reachability import get_shared as _volume_reachability

_EMPTY_FILE_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

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


def _volume_offline(path):
    """True when ``path`` sits on a mount-shaped volume that is not reachable.

    An unmounted NAS makes every file on it fail ``os.path.exists`` exactly
    as a deleted file would. Only this tells the two apart.
    """
    root, reachable = _volume_reachability().check(path)
    return root is not None and not reachable


def _row_to_info(row, folder_path):
    """Shape a photos row into the dict the UI consumes for a proposal entry.

    ``exists`` is populated by stat-ing the path. The resolver uses it via
    Rule 0 (present beats missing) and the UI surfaces it as a warning so the
    user doesn't trash surviving copies of a row whose "winner" file is gone.
    ``volume_offline`` marks a missing file whose volume is unreachable: its
    state is unknown, so it must not count as missing.
    """
    filename = row["filename"] or ""
    full_path = os.path.join(folder_path or "", filename)
    exists = os.path.exists(full_path)
    return {
        "id": row["id"],
        "filename": filename,
        "path": full_path,
        "mtime": row["file_mtime"] or 0.0,
        "rating": row["rating"] if row["rating"] is not None else 0,
        "file_size": row["file_size"] if row["file_size"] is not None else 0,
        "exists": exists,
        "volume_offline": not exists and _volume_offline(full_path),
    }


def _candidate(row, info):
    """A resolver candidate. A copy on an offline volume counts as present:
    Rule 0 (present beats missing) must not make it the loser just because
    its share is unmounted."""
    return DupCandidate(
        id=row["id"], path=info["path"], mtime=row["file_mtime"] or 0.0,
        exists=info["exists"] or info["volume_offline"],
    )


def _attach_edit_recipes(db, proposals):
    """Attach edit recipes to every candidate in duplicate proposals."""
    candidates = []
    for proposal in proposals:
        winner = proposal.get("winner")
        if isinstance(winner, dict):
            candidates.append(winner)
        candidates.extend(
            loser for loser in proposal.get("losers", [])
            if isinstance(loser, dict)
        )
    if not candidates:
        return proposals
    recipe_map = db.get_photo_edit_recipes(
        sorted({
            candidate["id"]
            for candidate in candidates
            if isinstance(candidate.get("id"), int)
            and not isinstance(candidate.get("id"), bool)
        })
    )
    for candidate in candidates:
        pid = candidate.get("id")
        candidate["edit_recipe"] = recipe_map.get(pid)
    return proposals


def _is_empty_file_group(file_hash, infos):
    return (
        file_hash == _EMPTY_FILE_SHA256
        and infos
        and all((info.get("file_size") or 0) == 0 for info in infos)
    )


def _build_unresolved_proposal(db, group):
    """Return a proposal dict for an unresolved group, or None on race."""
    rows = _fetch_photo_rows(
        db, group["photo_ids"],
        columns="p.id, p.filename, p.file_mtime, p.rating, p.file_size, "
                "f.path AS folder_path",
        where_extra=" AND (p.flag IS NULL OR p.flag != 'rejected')",
    )

    info_by_id = {r["id"]: _row_to_info(r, r["folder_path"]) for r in rows}
    candidates = [_candidate(r, info_by_id[r["id"]]) for r in rows]
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
    # ``all_missing`` is the "nothing on disk to keep" verdict the UI uses to
    # recommend orphan cleanup. Offline volumes are unknown, not missing —
    # counting them here would tell the user their archive is gone whenever a
    # NAS is unplugged. ``all_offline`` lets the UI say "reconnect to check"
    # instead.
    all_missing = not any(
        info["exists"] or info["volume_offline"]
        for info in info_by_id.values()
    )
    all_offline = (
        not all_missing
        and not any(info["exists"] for info in info_by_id.values())
    )
    empty_file_group = _is_empty_file_group(
        group["file_hash"], info_by_id.values(),
    )
    return {
        "file_hash": group["file_hash"],
        "status": "unresolved",
        "winner": info_by_id[winner_id],
        "losers": losers,
        "all_missing": all_missing,
        "all_offline": all_offline,
        "empty_file_group": empty_file_group,
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
                "f.path AS folder_path, "
                "EXISTS (SELECT 1 FROM duplicate_rejections d"
                " WHERE d.photo_id = p.id) AS duplicate_rejected",
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
    #
    # Not when the kept file's volume is offline: an unmounted share looks
    # exactly like a deleted file, and reopening would propose rejecting
    # the archive original. And only rows the duplicate resolver rejected
    # can bring the group back; a sibling the user rejected by hand stays
    # rejected (``reopen_duplicate_group`` skips it too).
    kept_info = info_by_id[kept[0]["id"]]
    if not kept_info["exists"] and not kept_info["volume_offline"] and any(
        r["duplicate_rejected"] and info_by_id[r["id"]]["exists"]
        for r in rejected
    ):
        db.reopen_duplicate_group(group["file_hash"])
        return _build_unresolved_proposal(db, group)

    candidates = [_candidate(r, info_by_id[r["id"]]) for r in rows]
    _winner_id, losers_with_reasons = resolve_duplicates(candidates)
    reasons = dict(losers_with_reasons)

    losers = []
    for r in rejected:
        linfo = dict(info_by_id[r["id"]])
        linfo["reason"] = reasons.get(r["id"], "auto-resolved")
        linfo["rejected"] = True
        losers.append(linfo)
    # See ``_build_unresolved_proposal`` for why offline volumes are unknown
    # rather than missing.
    all_missing = not any(
        info["exists"] or info["volume_offline"]
        for info in info_by_id.values()
    )
    all_offline = (
        not all_missing
        and not any(info["exists"] for info in info_by_id.values())
    )
    empty_file_group = _is_empty_file_group(
        group["file_hash"], info_by_id.values(),
    )
    if empty_file_group:
        for loser in losers:
            loser["reason"] = "empty file"
    return {
        "file_hash": group["file_hash"],
        "status": "resolved",
        "winner": info_by_id[kept[0]["id"]],
        "losers": losers,
        "all_missing": all_missing,
        "all_offline": all_offline,
        "empty_file_group": empty_file_group,
    }


def run_duplicate_scan(job, db, include_resolved=True, cancel_check=None):
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
        if cancel_check and cancel_check():
            break
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

    _attach_edit_recipes(db, proposals)
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
