"""Persistence for exact-duplicate groups (photos sharing a ``file_hash``).

``Database`` keeps the composition: it decides when to resolve, calls the
winner/loser merge, and re-tags the winner through ``Database.tag_photo`` so
the keyword-provenance fold and any patched ``tag_photo`` still apply. This
repository owns the SQL those steps read and write, plus the reads the
``/api/duplicates/*`` routes make (live twins of a hash, loser-file
candidates, the disk-cleanup summary). Every method is catalog-wide
(photos are global), so it takes no workspace id.

Callers reach it as ``db.duplicates`` (a fresh repository per access, see
``Database.duplicates``); the reads and ``reopen`` have no forwarding
wrappers on ``Database``. Resolution goes through
``services/duplicate_resolution.py`` (``apply_duplicate_resolution`` and
friends) and ``Database._apply_winner_loser_merge``, not the plan and
``reject`` methods here.

Also streams camera metadata for review-only same-shot candidates and stores
their library-wide review notes. Those notes do not change exact-byte cleanup.
Not to be confused with the top-level ``duplicates`` module, the pure resolver.
"""

import os
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from repositories.keyword_identity_sql import embedded_keyword_associations_for_merge
from sql_chunks import chunked


class _DeferredPlan:
    """Singleton sentinel returned by ``resolution_plan`` when at least one
    candidate is on an offline volume, so the resolver can't safely pick a
    winner yet. Distinct from ``None`` (which still means "fewer than 2
    non-rejected candidates, nothing to do") so callers can tell the two
    apart and communicate deferral back to the user.
    """
    __slots__ = ()

    def __repr__(self):
        return "DEFERRED_PLAN"


DEFERRED_PLAN = _DeferredPlan()


def _volume_offline(path):
    """True when ``path`` sits on a mount-shaped volume that is not reachable.

    Mirrors ``duplicate_scan._volume_offline`` so the auto-resolver used by
    ``add_photo`` and ``check_and_resolve_duplicates_for_hash`` treats an
    unmounted NAS copy as "state unknown" rather than "missing" — otherwise
    Rule 0 rejects the archive original whenever its share is unplugged, and
    the ``duplicate_rejections`` provenance written by ``reject`` would keep
    the group resolved when the share came back. Imported lazily to avoid a
    circular import at module load.
    """
    try:
        from volume_reachability import get_shared as _volume_reachability
    except ImportError:
        return False
    root, reachable = _volume_reachability().check(path)
    return root is not None and not reachable


class DuplicatesRepository:
    def __init__(self, conn: sqlite3.Connection, *, chunk_size: int = 800) -> None:
        self.conn = conn
        self.chunk_size = chunk_size

    def transaction(self) -> sqlite3.Connection:
        """The connection as a context manager: commit on success, roll back on error."""
        return self.conn

    def begin_review_write(self) -> None:
        """Serialize the catalog snapshot check and its review note write."""
        self.conn.execute("BEGIN IMMEDIATE")

    def same_shot_reviews(self, keys: Sequence[str]) -> dict[str, sqlite3.Row]:
        reviews = {}
        for batch in chunked(keys):
            rows = self.conn.execute(
                f"SELECT * FROM same_shot_reviews WHERE group_key IN ({','.join('?' * len(batch))})", batch,
            )
            reviews.update({row["group_key"]: row for row in rows})
        return reviews

    def save_same_shot_review(self, group_key: str, review_key: str, decision: str | None) -> None:
        if decision is None:
            self.conn.execute("DELETE FROM same_shot_reviews WHERE group_key=?", (group_key,))
        else:
            self.conn.execute(
                "INSERT INTO same_shot_reviews (group_key,review_key,decision) VALUES (?,?,?) "
                "ON CONFLICT(group_key) DO UPDATE SET review_key=excluded.review_key, "
                "decision=excluded.decision,updated_at=datetime('now')",
                (group_key, review_key, decision),
            )

    def same_shot_rows(self, photo_ids: Sequence[int] | None = None) -> Iterator[sqlite3.Row]:
        """Stream live, nonempty photos' camera metadata without reading originals.

        The background scan walks the catalog once. Restores read only the
        saved members, chunked below SQLite's parameter limit.
        """
        chunks = [None] if photo_ids is None else chunked(photo_ids)
        for ids in chunks:
            where = "" if ids is None else f" AND p.id IN ({','.join('?' * len(ids))})"
            cursor = self.conn.execute(
                "SELECT p.id, p.filename, p.file_hash, p.file_size, p.rating, "
                "p.timestamp, p.phash, p.exif_data, p.camera_make, p.camera_model, "
                "f.path AS folder_path FROM photos p JOIN folders f ON f.id = p.folder_id "
                "WHERE (p.flag IS NULL OR p.flag != 'rejected') "
                "AND p.file_size > 0 AND p.file_hash IS NOT NULL "
                "AND p.file_hash != '' AND p.exif_data IS NOT NULL" + where,
                () if ids is None else ids,
            )
            while rows := cursor.fetchmany(128):
                yield from rows

    def saved_same_shot_group(self, photo_id: int) -> str | None:
        """Only a member of the latest completed review can use library-wide media.

        Return one group's JSON, rather than sending the entire scan back
        through Python for every thumbnail. The service revalidates its ids,
        paths, hashes and camera identities before allowing access.
        """
        row = self.conn.execute(
            """SELECT g.value FROM (
                   SELECT result FROM job_history
                   WHERE type = 'duplicate-scan' AND status = 'completed'
                     AND result IS NOT NULL
                   ORDER BY finished_at DESC, started_at DESC LIMIT 1
               ) latest, json_each(
                   CASE WHEN json_valid(latest.result) THEN latest.result ELSE '{}' END,
                   '$.same_shot_groups'
               ) g
               WHERE EXISTS (SELECT 1 FROM json_each(g.value, '$.photos') p
                             WHERE json_extract(p.value, '$.id') = ?)
               LIMIT 1""",
            (photo_id,),
        ).fetchone()
        return row[0] if row else None

    # -- groups -------------------------------------------------------------

    def is_group_member(self, photo_id: int) -> bool:
        """Whether ``photo_id`` shares its ``file_hash`` with another photo.

        Counts rejected rows too, so the members of an already-resolved
        group (kept row plus rejected hash-twins) qualify.
        """
        row = self.conn.execute(
            "SELECT 1 FROM photos p "
            "JOIN photos o ON o.file_hash = p.file_hash AND o.id != p.id "
            "WHERE p.id = ? AND p.file_hash IS NOT NULL LIMIT 1",
            (photo_id,),
        ).fetchone()
        return row is not None

    def workspace_names(self, photo_ids: Iterable[int]) -> dict[int, list[str]]:
        """Return ``{photo_id: [workspace name, ...]}`` for the ids given.

        Names come back sorted; a photo no workspace shows maps to ``[]``.
        """
        names = {pid: [] for pid in photo_ids}
        ids = list(names)
        for i in range(0, len(ids), self.chunk_size):
            chunk = ids[i:i + self.chunk_size]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT DISTINCT v.photo_id, w.name "
                f"FROM photo_workspace_visibility v "
                f"JOIN workspaces w ON w.id = v.workspace_id "
                f"WHERE v.photo_id IN ({placeholders}) "
                f"ORDER BY w.name COLLATE NOCASE",
                chunk,
            ).fetchall()
            for r in rows:
                names[r["photo_id"]].append(r["name"])
        return names

    def live_ids_for_hash(self, file_hash: str) -> list[int]:
        """Return the ids of non-rejected photos with ``file_hash``."""
        dup_rows = self.conn.execute(
            "SELECT id FROM photos WHERE file_hash = ? AND (flag IS NULL OR flag != 'rejected')",
            (file_hash,),
        ).fetchall()
        return [r["id"] for r in dup_rows]

    def live_paths_for_hash(self, file_hash: str) -> list[sqlite3.Row]:
        """Rows (``filename``, ``path``) of the non-rejected photos with ``file_hash``.

        Inner-joins ``folders``, so a photo whose folder row is gone is absent.
        """
        return self.conn.execute(
            "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
            "WHERE p.file_hash = ? AND (p.flag IS NULL OR p.flag != 'rejected')",
            (file_hash,),
        ).fetchall()

    def loser_candidate_rows(self, photo_ids: Iterable[int]) -> dict[int, sqlite3.Row]:
        """``{photo_id: row}`` for the named photos that exist.

        Each row is ``id``, ``flag``, ``file_hash``, ``filename`` and
        ``folder_path`` (None when the folder row is gone). Chunked with
        ``sql_chunks.chunked`` (900 ids per statement).
        """
        rows_by_id = {}
        for chunk in chunked(photo_ids):
            placeholders = ",".join("?" * len(chunk))
            chunk_rows = self.conn.execute(
                f"""SELECT p.id, p.flag, p.file_hash, p.filename,
                           f.path AS folder_path
                    FROM photos p
                    LEFT JOIN folders f ON f.id = p.folder_id
                    WHERE p.id IN ({placeholders})""",
                chunk,
            ).fetchall()
            for r in chunk_rows:
                rows_by_id[r["id"]] = r
        return rows_by_id

    def loser_disk_summary(self) -> sqlite3.Row:
        """Row (``n``, ``total_bytes``): rejected photos whose hash a kept photo shares.

        ``total_bytes`` sums their stored ``file_size`` (0 when there are none).
        """
        return self.conn.execute(
            """
            SELECT COUNT(*) AS n, COALESCE(SUM(file_size), 0) AS total_bytes
            FROM photos p
            WHERE p.flag = 'rejected'
              AND p.file_hash IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM photos q
                  WHERE q.file_hash = p.file_hash AND (q.flag IS NULL OR q.flag != 'rejected')
              )
            """
        ).fetchone()

    def cleanup_rows(self) -> list[sqlite3.Row]:
        """Current rejected copies and all their kept anchors, without disk I/O.

        Uses the same eligibility as ``loser_disk_summary``, including hashes
        with several kept copies. A single statement keeps the members and
        their flags consistent if another request resolves a group meanwhile.
        """
        return self.conn.execute(
            """
            SELECT p.id, p.filename, p.file_hash, p.file_mtime, p.rating,
                   p.file_size, p.flag, f.path AS folder_path
            FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
            WHERE p.file_hash IN (
                SELECT r.file_hash FROM photos r
                WHERE r.flag = 'rejected' AND r.file_hash IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM photos k WHERE k.file_hash = r.file_hash
                      AND (k.flag IS NULL OR k.flag != 'rejected')
                  )
            )
            ORDER BY p.file_hash, p.id
            """
        ).fetchall()

    def find_groups(self, include_resolved: bool = False) -> list[dict[str, Any]]:
        """Return duplicate groups for the duplicate-scan job.

        Each group is ``{file_hash, photo_ids: [...], status}`` where
        ``status`` is either ``'unresolved'`` (2+ non-rejected rows; user
        action needed to pick a winner) or ``'resolved'`` (exactly one
        non-rejected row plus one or more rejected rows sharing the hash;
        the auto-resolver already handled it during scan, but the loser
        files may still be on disk; these also carry ``winner_id``).

        ``include_resolved=False`` (the default) returns only unresolved
        groups, preserving the legacy contract for callers that want
        actionable items. Pass True from the duplicates page to surface
        already-handled pairs so the user can clean up loser files from
        disk — those pairs are otherwise invisible.

        ``photo_ids`` includes both the kept and the rejected rows for
        resolved groups; downstream code disambiguates by re-querying
        ``flag`` per row.

        Finds the shared hashes on the covering ``file_hash`` index first and
        reads ``flag`` only for their rows: a ``GROUP BY`` over every photo
        that also reads ``flag`` costs ~0.4s on a 100k-photo catalog, which
        ``/api/duplicates/last-scan`` pays on every page load.
        """
        rows = self.conn.execute(
            """
            SELECT file_hash, id, flag
            FROM photos
            WHERE file_hash IN (
                SELECT file_hash FROM photos
                WHERE file_hash IS NOT NULL
                GROUP BY file_hash
                HAVING COUNT(*) > 1
            )
            ORDER BY file_hash, id
            """
        ).fetchall()
        members = {}
        for r in rows:
            members.setdefault(r["file_hash"], []).append(
                (r["id"], r["flag"] == "rejected")
            )

        unresolved = []
        resolved = []
        for file_hash, rows_for_hash in members.items():
            kept = [pid for pid, rejected in rows_for_hash if not rejected]
            if len(kept) > 1:
                unresolved.append({
                    "file_hash": file_hash,
                    "photo_ids": kept,
                    "status": "unresolved",
                })
            # Resolved: exactly 1 non-rejected row plus 1+ rejected rows
            # sharing the hash. Purely-rejected hashes (e.g. the user
            # manually rejected the only copy of a photo for non-duplicate
            # reasons) are excluded — without the kept-row anchor there is
            # no "loser of a duplicate group" to clean up.
            elif include_resolved and len(kept) == 1:
                resolved.append({
                    "file_hash": file_hash,
                    "photo_ids": [pid for pid, _ in rows_for_hash],
                    "status": "resolved",
                    # The single non-rejected row. A later flag edit that
                    # swaps which member is kept and which is rejected
                    # keeps ``photo_ids`` the same but changes the winner,
                    # so callers that fingerprint a resolved group need
                    # this to notice.
                    "winner_id": kept[0],
                })
        return unresolved + resolved

    def reopen(self, file_hash: str) -> int:
        """Un-reject the rows with ``file_hash`` that the duplicate resolver
        rejected (in its own transaction); return the count.

        Used by the duplicate scan when the kept file has gone missing on
        disk but a rejected sibling still exists, so the next proposal pass
        can promote the survivor. Rows the user rejected by hand (no
        ``duplicate_rejections`` row) stay rejected.
        """
        with self.conn:
            cur = self.conn.execute(
                "UPDATE photos SET flag = 'none' "
                "WHERE file_hash = ? AND flag = 'rejected' "
                "AND id IN (SELECT photo_id FROM duplicate_rejections)",
                (file_hash,),
            )
            return cur.rowcount

    # -- resolution plans -----------------------------------------------------

    def _resolution_rows(self, photo_ids):
        """Identity of selected rows and every member of their hash groups."""
        fields = "p.id, p.folder_id, p.filename, p.file_hash, p.file_mtime, p.rating, p.flag, p.xmp_import_pending, f.path AS folder_path"
        rows = {}
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
            marks = ",".join("?" for _ in chunk)
            for row in self.conn.execute(
                f"SELECT {fields} FROM photos p LEFT JOIN folders f ON f.id=p.folder_id WHERE p.id IN ({marks})",
                chunk,
            ).fetchall():
                rows[row["id"]] = row
        hashes = list(dict.fromkeys(row["file_hash"] for row in rows.values() if row["file_hash"] is not None))
        for chunk in self._chunks(hashes):
            marks = ",".join("?" for _ in chunk)
            for row in self.conn.execute(
                f"SELECT {fields} FROM photos p LEFT JOIN folders f ON f.id=p.folder_id WHERE p.file_hash IN ({marks})",
                chunk,
            ).fetchall():
                rows[row["id"]] = row
        return [rows[pid] for pid in sorted(rows)]

    def resolution_preflight(self, photo_ids):
        """Probe disk before locking and bind the result to catalog identity."""
        from duplicates import DupCandidate, resolve_duplicates

        rows = self._resolution_rows(photo_ids)
        snapshot = tuple(tuple(row) for row in rows)
        selected = set(photo_ids)
        candidates_rows = [row for row in rows if row["id"] in selected and row["flag"] != "rejected"]
        if len(candidates_rows) < 2:
            return snapshot, None
        if any(row["xmp_import_pending"] for row in rows):
            return snapshot, DEFERRED_PLAN
        candidates = []
        for row in candidates_rows:
            path = os.path.join(row["folder_path"] or "", row["filename"] or "")
            # Reachability precedes stat so an unhealthy mount cannot hang
            # while the catalog writer lock is held.
            if _volume_offline(path):
                return snapshot, DEFERRED_PLAN
            candidates.append(DupCandidate(
                id=row["id"], path=path, mtime=row["file_mtime"] or 0.0,
                exists=os.path.exists(path),
            ))
        winner, losers = resolve_duplicates(candidates)
        return snapshot, (winner, [pid for pid, _reason in losers])

    def resolution_plan(self, photo_ids, *, preflight=None):
        """Disk-free locked revalidation when the service supplies preflight.

        Membership, hash, flag, path, winner-selection fields and pending state
        must still match, including siblings absent from the caller's id list.
        Any change defers the group rather than applying a stale disk decision.
        Read-only callers may omit preflight for the original convenience API.
        """
        if preflight is None:
            preflight = self.resolution_preflight(photo_ids)
        rows = self._resolution_rows(photo_ids)
        if tuple(tuple(row) for row in rows) != preflight[0]:
            return DEFERRED_PLAN
        if preflight[1] is None:
            return None
        if any(row["xmp_import_pending"] for row in rows):
            return DEFERRED_PLAN
        return preflight[1]

    def _keep_folder_rows(self, file_hash: str) -> list[sqlite3.Row]:
        """Catalog identity and selection fields, including rejected members."""
        return self.conn.execute(
            """SELECT p.id, p.folder_id, p.filename, p.file_mtime, p.rating,
                      p.flag, p.xmp_import_pending, f.path AS folder_path
               FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
               WHERE p.file_hash = ? ORDER BY p.id""",
            (file_hash,),
        ).fetchall()

    def keep_folder_preflight(self, file_hash, keep_folder_norm):
        """Snapshot group identity and probe candidate paths before locking.

        The snapshot covers all members, flags, pending state, paths and
        winner-selection fields. A changed snapshot requires a fresh preflight.
        """
        rows = self._keep_folder_rows(file_hash)
        if any(row["xmp_import_pending"] for row in rows):
            return tuple(tuple(row) for row in rows), frozenset()
        present = frozenset(
            row["id"] for row in rows
            if row["flag"] != "rejected"
            and os.path.normpath(row["folder_path"] or "") == keep_folder_norm
            and os.path.exists(os.path.join(row["folder_path"] or "", row["filename"] or ""))
        )
        return tuple(tuple(row) for row in rows), present

    def keep_folder_plan(self, file_hash, keep_folder_norm, *, preflight=None):
        """Pick a folder winner; supplied preflight makes this disk-free.

        The service supplies a pre-lock preflight and calls this inside its
        writer transaction. A changed membership, path, flag, pending state
        or winner-selection field fails closed instead of trusting stale I/O.
        Direct read-only callers may omit preflight for the original behavior.
        """
        from duplicates import DupCandidate, resolve_duplicates

        if preflight is None:
            preflight = self.keep_folder_preflight(file_hash, keep_folder_norm)
        rows = self._keep_folder_rows(file_hash)
        if tuple(tuple(row) for row in rows) != preflight[0]:
            return None, None, "group changed during preflight"
        if any(row["xmp_import_pending"] for row in rows):
            return None, None, "sidecar keywords pending"
        rows = [row for row in rows if row["flag"] != "rejected"]
        if not rows:
            return None, None, "no candidates"
        if len(rows) < 2:
            return None, None, "fewer than 2 candidates"
        in_folder = [row for row in rows if os.path.normpath(row["folder_path"] or "") == keep_folder_norm]
        if not in_folder:
            return None, None, "no candidate in keep_folder"
        present_in_folder = [row for row in in_folder if row["id"] in preflight[1]]
        if not present_in_folder:
            return None, None, "keep_folder candidate missing on disk"
        if len(present_in_folder) == 1:
            winner_id = present_in_folder[0]["id"]
        else:
            candidates = [DupCandidate(
                id=row["id"], path=os.path.join(row["folder_path"] or "", row["filename"] or ""),
                mtime=row["file_mtime"] or 0.0, exists=True,
            ) for row in present_in_folder]
            winner_id, _ = resolve_duplicates(candidates)
        return winner_id, [row["id"] for row in rows if row["id"] != winner_id], None

    def keep_copy_refusal(self, photo_id):
        """Why ``photo_id`` can't become the kept copy of its group, or ``None``.

        Reasons: ``"not found"``, ``"not a duplicate"``, ``"volume offline"``
        or ``"missing on disk"``. Promoting a copy whose file is gone (or
        unknowable) would reject the copy that still exists. Does the disk
        checks, so callers run it before taking the writer lock; the folder
        and filename it checks don't change when flags do. Writes nothing.
        """
        row = self.conn.execute(
            """SELECT p.file_hash, p.filename, f.path AS folder_path
               FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
               WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()
        if row is None:
            return "not found"
        if not row["file_hash"] or self.conn.execute(
            "SELECT 1 FROM photos WHERE file_hash = ? AND id != ? LIMIT 1",
            (row["file_hash"], photo_id),
        ).fetchone() is None:
            return "not a duplicate"
        if row["folder_path"] is None:
            return "missing on disk"
        path = os.path.join(row["folder_path"], row["filename"] or "")
        # Reachability first: a stat on a stale network mount can block.
        if _volume_offline(path):
            return "volume offline"
        if not os.path.exists(path):
            return "missing on disk"
        return None

    def keep_copy_plan(self, photo_id):
        """Plan making ``photo_id`` the one kept copy of its ``file_hash``.

        Unlike the resolver plans this may promote a rejected copy: it backs
        the user's explicit "keep this copy instead" choice. Every other
        non-rejected copy of the hash becomes a loser; copies already
        rejected stay as they are. Reads only flags, so it is cheap to run
        inside the writer-locked transaction that applies it (two concurrent
        keeps in one group must not both plan against the same old keeper).
        Returns ``(plan, None)`` with ``plan`` a dict of ``file_hash``,
        ``loser_ids`` and ``winner_flag`` (the flag the kept copy ends with),
        or ``(None, reason)`` with ``"not found"``, ``"not a duplicate"``
        or ``"sidecar keywords pending"``.
        Writes nothing.
        """
        row = self.conn.execute(
            "SELECT file_hash, flag FROM photos WHERE id = ?", (photo_id,),
        ).fetchone()
        if row is None:
            return None, "not found"
        if not row["file_hash"]:
            return None, "not a duplicate"
        # This check runs inside the caller's writer-locked transaction.
        # Include the chosen copy and rejected siblings in the group guard.
        if self.conn.execute(
            "SELECT 1 FROM photos WHERE file_hash = ? AND xmp_import_pending != 0 LIMIT 1",
            (row["file_hash"],),
        ).fetchone() is not None:
            return None, "sidecar keywords pending"
        siblings = self.conn.execute(
            "SELECT id, flag FROM photos WHERE file_hash = ? AND id != ?",
            (row["file_hash"], photo_id),
        ).fetchall()
        if not siblings:
            return None, "not a duplicate"
        losers = [s for s in siblings if s["flag"] != "rejected"]
        # The copies are the same file, so a pick on the copy being replaced
        # belongs to the one that stays; a rejected copy comes back unflagged.
        flags = {row["flag"]} | {s["flag"] for s in losers}
        if "flagged" in flags:
            winner_flag = "flagged"
        elif row["flag"] == "rejected":
            winner_flag = "none"
        else:
            winner_flag = row["flag"]
        return {
            "file_hash": row["file_hash"],
            "loser_ids": [s["id"] for s in losers],
            "winner_flag": winner_flag,
        }, None

    # -- winner/loser merge ---------------------------------------------------

    def photo_metadata(self, photo_id):
        """Return the ``duplicates.PhotoMetadata`` the merge reads for a photo."""
        from duplicates import PhotoMetadata

        r = self.conn.execute(
            "SELECT rating FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()
        kw_rows = self.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?",
            (photo_id,),
        ).fetchall()
        pend = self.conn.execute(
            "SELECT 1 FROM pending_changes WHERE photo_id = ? LIMIT 1",
            (photo_id,),
        ).fetchone()
        return PhotoMetadata(
            id=photo_id,
            rating=(r["rating"] if r and r["rating"] is not None else 0),
            keyword_ids={kr["keyword_id"] for kr in kw_rows},
            # Collections in Vireo are rule-based (no junction table); skip.
            collection_ids=set(),
            has_pending_edit=pend is not None,
        )

    def set_rating(self, photo_id, rating):
        """Set a photo's rating. Does not commit (runs in the merge transaction)."""
        self.conn.execute(
            "UPDATE photos SET rating = ? WHERE id = ?",
            (rating, photo_id),
        )

    def loser_keyword_rows(self, loser_ids, *, survivor_id=None):
        """Read loser tags in chunks, respecting the survivor's embedded removals."""
        rows = []
        for chunk in self._chunks(loser_ids):
            loser_placeholders = ",".join("?" * len(chunk))
            rows.extend(self.conn.execute(
                f"""SELECT photo_id, keyword_id, source, species_provenance
                    FROM photo_keywords
                    WHERE photo_id IN ({loser_placeholders})""",
                chunk,
            ).fetchall())
        if survivor_id is None:
            return rows
        eligible = {
            photo_id: {row["id"] for row in embedded_keyword_associations_for_merge(
                self.conn, photo_id, survivor_id, include_non_embedded=True,
            )}
            for photo_id in {row["photo_id"] for row in rows}
        }
        return [row for row in rows if row["keyword_id"] in eligible[row["photo_id"]]]

    def copy_loser_embedded_offered_keys(self, winner_id, loser_ids):
        """Keep suppression on rejected rows and copy it onto their survivor."""
        for chunk in self._chunks(loser_ids):
            placeholders = ",".join("?" * len(chunk))
            self.conn.execute(
                f"""INSERT OR IGNORE INTO photo_embedded_keyword_offered
                    (photo_id, keyword_key)
                    SELECT ?, keyword_key FROM photo_embedded_keyword_offered
                    WHERE photo_id IN ({placeholders})""",
                (winner_id, *chunk),
            )

    def set_flag(self, photo_id, flag):
        """Set a photo's flag. Does not commit (runs in the merge transaction)."""
        self.conn.execute(
            "UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id),
        )

    def reject(self, loser_ids):
        """Flag ``loser_ids`` rejected. Does not commit (runs in the merge transaction)."""
        # Chunked — a single duplicate group's loser list can exceed the
        # bound-parameter cap.
        for chunk in self._chunks(loser_ids):
            loser_placeholders = ",".join("?" * len(chunk))
            self.conn.execute(
                f"UPDATE photos SET flag = 'rejected' WHERE id IN ({loser_placeholders})",
                list(chunk),
            )
            self.conn.executemany(
                "INSERT OR IGNORE INTO duplicate_rejections(photo_id) VALUES (?)",
                [(pid,) for pid in chunk],
            )

    def photo_rows_with_folder(self, photo_ids: Sequence[int], columns: str,
                               where_extra: str = "", *,
                               chunk_size: int) -> list[sqlite3.Row]:
        """``SELECT {columns} FROM photos p LEFT JOIN folders f WHERE p.id IN (...) {where_extra}``.

        The duplicate scan's row reads: ``columns`` and ``where_extra`` are
        the scan's own literal SQL fragments (``where_extra`` starts with
        `` AND``). The ids are sent ``chunk_size`` at a time, below SQLite's
        parameter cap; the caller passes its own cap so a patched one still
        applies. Returns the rows of every chunk, in order, as one list.
        """
        rows = []
        for i in range(0, len(photo_ids), chunk_size):
            chunk = photo_ids[i:i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            sql = (
                f"SELECT {columns} "
                f"FROM photos p LEFT JOIN folders f ON f.id = p.folder_id "
                f"WHERE p.id IN ({placeholders}){where_extra}"
            )
            rows.extend(self.conn.execute(sql, chunk).fetchall())
        return rows

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
