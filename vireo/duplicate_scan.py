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


def revalidate_scan_result(db, result):
    """Drop entries of a stored scan result that no longer match the catalog.

    ``/api/duplicates/last-scan`` restores a scan that may be months old.
    Its entries name photos by id, but ``photos.id`` is a plain INTEGER
    PRIMARY KEY, so once an extra copy's row is deleted, SQLite hands that
    id to the next photo imported. Served as-is, the restored card shows the
    new photo's thumbnail under the old filename, which reads as two unrelated
    photos flagged as duplicates.

    An entry is current only while its id still names the same file (same
    path) with the group's ``file_hash``. A group whose kept photo is stale,
    or that has no current extra copy left, is dropped; buckets and counts
    are rebuilt from the groups that remain. Returns a new result dict with
    ``stale_group_count`` set to the number of groups dropped. Does no
    filesystem I/O, so a sleeping NAS cannot stall the page load.

    Also says whether a new scan would find the same groups, so the page
    only asks for one when it would show something different:
    ``new_group_count`` counts duplicate groups in the catalog now whose
    hash the scan never saw, and ``changed_group_count`` counts the scan's
    groups that a new scan would show differently (another copy added or
    gone, a decision applied since, or the group no longer a duplicate).
    """
    proposals = result.get("proposals") or []
    photo_ids = sorted({
        entry["id"]
        for p in proposals
        for entry in [p.get("winner") or {}] + list(p.get("losers") or [])
        if isinstance(entry.get("id"), int)
    })
    rows = _fetch_photo_rows(
        db, photo_ids,
        columns="p.id, p.filename, p.file_hash, f.path AS folder_path",
    )
    live = {
        r["id"]: (
            os.path.join(r["folder_path"] or "", r["filename"] or ""),
            r["file_hash"],
        )
        for r in rows
    }

    def current(entry, file_hash):
        return live.get(entry.get("id")) == (entry.get("path"), file_hash)

    kept = []
    for p in proposals:
        file_hash = p.get("file_hash")
        winner = p.get("winner") or {}
        losers = [
            loser for loser in p.get("losers") or []
            if current(loser, file_hash)
        ]
        if not current(winner, file_hash) or not losers:
            continue
        kept.append(dict(p, losers=losers))

    new_group_count, changed_group_count = _changes_since_scan(
        db, proposals, kept,
    )
    return dict(
        result,
        proposals=kept,
        buckets=bucket_unresolved_proposals(kept),
        group_count=len(kept),
        loser_count=sum(
            len(p["losers"]) for p in kept if p.get("status") == "unresolved"
        ),
        resolved_group_count=sum(
            1 for p in kept if p.get("status") == "resolved"
        ),
        resolved_loser_count=sum(
            len(p["losers"]) for p in kept if p.get("status") == "resolved"
        ),
        stale_group_count=len(proposals) - len(kept),
        new_group_count=new_group_count,
        changed_group_count=changed_group_count,
    )


def _group_state(status, photo_ids, winner_id):
    # The members alone don't say which copy the page shows as KEEP: a flag
    # edit can swap a resolved group's kept and rejected copies, and a
    # changed mtime can make the resolver pick the other copy of an
    # unresolved one, which ``/api/duplicates/apply`` would then keep.
    return status, frozenset(photo_ids), winner_id


def _entry_ids(proposal):
    return [
        e.get("id")
        for e in [proposal.get("winner") or {}] + list(proposal.get("losers") or [])
    ]


def _unresolved_winners(db, groups, shown_entries):
    """Return ``{file_hash: winner_id}`` the resolver would pick now for each
    unresolved group whose copies the restored scan all shows.

    Path and mtime come from the catalog; whether each file exists comes
    from the stored scan, so this does no filesystem I/O. A group with a
    copy the scan never saw differs in its members anyway.
    """
    groups = [
        g for g in groups
        if all(pid in shown_entries for pid in g["photo_ids"])
    ]
    rows = _fetch_photo_rows(
        db, sorted({pid for g in groups for pid in g["photo_ids"]}),
        columns="p.id, p.filename, p.file_mtime, f.path AS folder_path",
    )
    by_id = {r["id"]: r for r in rows}
    winners = {}
    for g in groups:
        candidates = []
        for pid in g["photo_ids"]:
            row = by_id.get(pid)
            if row is None:
                break
            entry = shown_entries[pid]
            candidates.append(DupCandidate(
                id=pid,
                path=os.path.join(row["folder_path"] or "", row["filename"] or ""),
                mtime=row["file_mtime"] or 0.0,
                exists=bool(entry.get("exists", True) or entry.get("volume_offline")),
            ))
        else:
            winners[g["file_hash"]] = resolve_duplicates(candidates)[0]
    return winners


def _changes_since_scan(db, stored_proposals, shown_proposals):
    """Compare the groups a restored scan shows with the catalog's groups now.

    Returns ``(new_group_count, changed_group_count)``. A group is the same
    when a new scan would show the same copies with the same one kept: an
    unresolved group lists its non-rejected copies and the copy the resolver
    picks from current metadata, a resolved one every copy and the one kept,
    as ``find_duplicate_groups`` reports them. A group dropped by
    revalidation counts as changed only if its hash still forms a group.
    """
    groups = db.find_duplicate_groups(include_resolved=True)
    shown_entries = {
        e.get("id"): e
        for p in shown_proposals
        for e in [p.get("winner") or {}] + list(p.get("losers") or [])
    }
    unresolved_winners = _unresolved_winners(
        db, [g for g in groups if g["status"] == "unresolved"], shown_entries,
    )
    current = {
        g["file_hash"]: _group_state(
            g["status"], g["photo_ids"],
            g.get("winner_id") if g["status"] == "resolved"
            else unresolved_winners.get(g["file_hash"]),
        )
        for g in groups
    }
    shown = {
        p.get("file_hash"): _group_state(
            p.get("status"), _entry_ids(p), (p.get("winner") or {}).get("id"),
        )
        for p in shown_proposals
    }
    scanned = {p.get("file_hash") for p in stored_proposals}
    new_group_count = sum(1 for h in current if h not in scanned)
    changed_group_count = sum(
        1 for h in scanned if shown.get(h) != current.get(h)
    )
    return new_group_count, changed_group_count


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

    Checks volume reachability BEFORE ``os.path.exists``. On a stale SMB/NFS
    mount a plain ``os.path.exists`` can block for minutes while the kernel
    waits for the transport, so the bounded reachability gate has to run
    first — otherwise the duplicate-scan worker can wedge on one row and
    never reach the fall-back.
    """
    filename = row["filename"] or ""
    full_path = os.path.join(folder_path or "", filename)
    offline = _volume_offline(full_path)
    exists = False if offline else os.path.exists(full_path)
    return {
        "id": row["id"],
        "filename": filename,
        "path": full_path,
        "mtime": row["file_mtime"] or 0.0,
        "rating": row["rating"] if row["rating"] is not None else 0,
        "file_size": row["file_size"] if row["file_size"] is not None else 0,
        "exists": exists,
        "volume_offline": offline,
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


def attach_workspace_names(db, proposals):
    """Set ``workspaces`` on every winner and loser to the names of the
    workspaces that show that copy.

    The scan is library-wide, so a group can pair a copy in the active
    workspace with one only another workspace shows; the card says which.
    Workspace membership changes after a scan, so a restored scan result
    gets this recomputed rather than trusting the stored names.
    """
    entries = [
        entry
        for p in proposals
        for entry in [p.get("winner")] + list(p.get("losers") or [])
        if isinstance(entry, dict)
        and isinstance(entry.get("id"), int)
        and not isinstance(entry.get("id"), bool)
    ]
    names = db.photo_workspace_names(sorted({e["id"] for e in entries}))
    for entry in entries:
        entry["workspaces"] = names.get(entry["id"], [])
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
    # instead, and requires EVERY entry to be on an offline volume: a group
    # with one reachable existing copy and one offline copy already shows
    # something we can act on, so it deserves the winner/loser-specific
    # warning, not the "everything is unreachable" banner.
    all_missing = not any(
        info["exists"] or info["volume_offline"]
        for info in info_by_id.values()
    )
    all_offline = (
        len(info_by_id) > 0
        and all(info["volume_offline"] for info in info_by_id.values())
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
    # rather than missing, and why ``all_offline`` requires every entry to
    # actually sit on an offline volume (not just every absent one).
    all_missing = not any(
        info["exists"] or info["volume_offline"]
        for info in info_by_id.values()
    )
    all_offline = (
        len(info_by_id) > 0
        and all(info["volume_offline"] for info in info_by_id.values())
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
    attach_workspace_names(db, proposals)
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
