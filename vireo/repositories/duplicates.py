"""Persistence for exact-duplicate groups (photos sharing a ``file_hash``).

``Database`` keeps the composition: it decides when to resolve, calls the
winner/loser merge, and re-tags the winner through ``Database.tag_photo`` so
the keyword-provenance fold and any patched ``tag_photo`` still apply. This
repository owns the SQL those steps read and write. Every method is
catalog-wide (photos are global), so it takes no workspace id.

Not to be confused with the top-level ``duplicates`` module, the pure
resolver this repository feeds.
"""

import os


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
    except Exception:
        return False
    root, reachable = _volume_reachability().check(path)
    return root is not None and not reachable


class DuplicatesRepository:
    def __init__(self, conn, *, chunk_size=800):
        self.conn = conn
        self.chunk_size = chunk_size

    def transaction(self):
        """The connection as a context manager: commit on success, roll back on error."""
        return self.conn

    # -- groups -------------------------------------------------------------

    def live_ids_for_hash(self, file_hash):
        """Return the ids of non-rejected photos with ``file_hash``."""
        dup_rows = self.conn.execute(
            "SELECT id FROM photos WHERE file_hash = ? AND (flag IS NULL OR flag != 'rejected')",
            (file_hash,),
        ).fetchall()
        return [r["id"] for r in dup_rows]

    def find_groups(self, include_resolved=False):
        """Return duplicate groups; see ``Database.find_duplicate_groups``."""
        unresolved_rows = self.conn.execute(
            """
            SELECT file_hash, GROUP_CONCAT(id) AS ids
            FROM photos
            WHERE file_hash IS NOT NULL AND (flag IS NULL OR flag != 'rejected')
            GROUP BY file_hash
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        groups = [
            {
                "file_hash": r["file_hash"],
                "photo_ids": [int(x) for x in r["ids"].split(",")],
                "status": "unresolved",
            }
            for r in unresolved_rows
        ]

        if not include_resolved:
            return groups

        # Resolved groups: hashes where exactly 1 non-rejected row exists
        # AND at least 1 rejected row shares the hash. We exclude purely-
        # rejected hashes (e.g. user manually rejected the only copy of a
        # photo for non-duplicate reasons) — without the kept-row anchor
        # there is no "loser of a duplicate group" to clean up.
        resolved_rows = self.conn.execute(
            """
            SELECT file_hash,
                   GROUP_CONCAT(id) AS ids,
                   SUM(CASE WHEN flag IS NULL OR flag != 'rejected' THEN 1 ELSE 0 END) AS kept,
                   SUM(CASE WHEN flag  = 'rejected' THEN 1 ELSE 0 END) AS rejected
            FROM photos
            WHERE file_hash IS NOT NULL
            GROUP BY file_hash
            HAVING kept = 1 AND rejected >= 1
            """
        ).fetchall()
        groups.extend(
            {
                "file_hash": r["file_hash"],
                "photo_ids": [int(x) for x in r["ids"].split(",")],
                "status": "resolved",
            }
            for r in resolved_rows
        )
        return groups

    def reopen(self, file_hash):
        """Un-reject the rows with ``file_hash`` that the duplicate resolver
        rejected; return the count.

        Rows the user rejected by hand (no ``duplicate_rejections`` row)
        stay rejected.
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

    def resolution_plan(self, photo_ids):
        """Pick a winner among the non-rejected ``photo_ids``.

        Returns ``(winner_id, loser_ids)``, or None when fewer than 2
        non-rejected candidates remain. Writes nothing.
        """
        from duplicates import DupCandidate, resolve_duplicates

        if not photo_ids or len(photo_ids) < 2:
            return None

        # Chunked — a single duplicate group can exceed the bound-parameter
        # cap (see duplicate_scan.py, which chunks its own reads).
        rows = []
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" * len(chunk))
            rows.extend(self.conn.execute(
                f"""SELECT p.id, p.filename, p.file_mtime, p.rating, p.flag,
                           f.path AS folder_path
                    FROM photos p
                    LEFT JOIN folders f ON f.id = p.folder_id
                    WHERE p.id IN ({placeholders}) AND (p.flag IS NULL OR p.flag != 'rejected')""",
                list(chunk),
            ).fetchall())
        if len(rows) < 2:
            return None

        candidates = []
        for r in rows:
            path = os.path.join(r["folder_path"] or "", r["filename"] or "")
            # Stat each candidate so the resolver doesn't pick a winner
            # whose file was moved/deleted on disk. The DB row would
            # otherwise outvote a surviving twin solely on path-string
            # heuristics. A copy on an unreachable volume is "state
            # unknown", not "missing" — treat it as present so Rule 0
            # doesn't reject the archive original just because the NAS is
            # unplugged (which would then get frozen into the group by
            # the ``duplicate_rejections`` row ``reject`` writes).
            #
            # Probe volume reachability BEFORE ``os.path.exists``. This
            # auto-resolver runs from ``add_photo`` and
            # ``check_and_resolve_duplicates_for_hash`` on every import
            # and scan; on a stale SMB/NFS mount an unqualified stat can
            # block for minutes while the kernel waits for the transport,
            # wedging the import/scan worker before the bounded volume
            # gate ever runs. ``duplicate_scan._row_to_info`` uses the
            # same ordering.
            offline = _volume_offline(path)
            present = False if offline else os.path.exists(path)
            candidates.append(
                DupCandidate(
                    id=r["id"],
                    path=path,
                    mtime=r["file_mtime"] or 0.0,
                    exists=present or offline,
                )
            )
        winner_id, losers_with_reasons = resolve_duplicates(candidates)
        loser_ids = [lid for lid, _reason in losers_with_reasons]
        return winner_id, loser_ids

    def keep_folder_plan(self, file_hash, keep_folder_norm):
        """Pick the ``file_hash`` winner that lives in ``keep_folder_norm``.

        ``keep_folder_norm`` is already ``os.path.normpath``-ed (or ``""``).
        Returns ``(winner_id, loser_ids, None)`` when the group is
        actionable, else ``(None, None, reason)`` with the skip reasons
        documented on ``Database.bulk_resolve_by_folder``. Writes nothing.
        """
        from duplicates import DupCandidate, resolve_duplicates

        rows = self.conn.execute(
            """SELECT p.id, p.filename, p.file_mtime, p.rating,
                      f.path AS folder_path
               FROM photos p
               LEFT JOIN folders f ON f.id = p.folder_id
               WHERE p.file_hash = ? AND (p.flag IS NULL OR p.flag != 'rejected')""",
            (file_hash,),
        ).fetchall()
        if not rows:
            return None, None, "no candidates"
        if len(rows) < 2:
            return None, None, "fewer than 2 candidates"
        in_folder = [
            r for r in rows
            if os.path.normpath(r["folder_path"] or "") == keep_folder_norm
        ]
        if not in_folder:
            return None, None, "no candidate in keep_folder"
        # Existence-check the keep_folder candidate(s) before promoting.
        # If the row's file has been deleted externally but a sibling in
        # another folder still exists, force-picking the missing row as
        # winner would reject the surviving copy — and chained delete
        # would then trash it. Skip the hash instead.
        in_folder_paths = [
            (r, os.path.join(r["folder_path"] or "", r["filename"] or ""))
            for r in in_folder
        ]
        present_in_folder = [
            (r, p) for (r, p) in in_folder_paths if os.path.exists(p)
        ]
        if not present_in_folder:
            return None, None, "keep_folder candidate missing on disk"
        if len(present_in_folder) == 1:
            winner_id = present_in_folder[0][0]["id"]
        else:
            # Same-folder duplicates among the keep_folder candidates —
            # let the resolver pick deterministically among them. All
            # candidates passed in exist on disk (filtered above), so
            # Rule 0 is a no-op here.
            cands = [
                DupCandidate(
                    id=r["id"], path=p,
                    mtime=r["file_mtime"] or 0.0,
                    exists=True,
                )
                for (r, p) in present_in_folder
            ]
            winner_id, _ = resolve_duplicates(cands)
        loser_ids = [r["id"] for r in rows if r["id"] != winner_id]
        return winner_id, loser_ids, None

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

    def loser_keyword_rows(self, loser_ids):
        """Return every ``(keyword_id, source)`` row the losers carry, chunk by chunk."""
        rows = []
        for chunk in self._chunks(loser_ids):
            loser_placeholders = ",".join("?" * len(chunk))
            rows.extend(self.conn.execute(
                f"""SELECT keyword_id, source
                    FROM photo_keywords
                    WHERE photo_id IN ({loser_placeholders})""",
                chunk,
            ).fetchall())
        return rows

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

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
