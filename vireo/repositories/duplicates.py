"""Persistence for exact-duplicate groups (photos sharing a ``file_hash``).

``Database`` keeps the composition: it decides when to resolve, calls the
winner/loser merge, and re-tags the winner through ``Database.tag_photo`` so
the keyword-provenance fold and any patched ``tag_photo`` still apply. This
repository owns the SQL those steps read and write, plus the reads the
``/api/duplicates/*`` routes make (live twins of a hash, loser-file
candidates, the disk-cleanup summary). Every method is catalog-wide
(photos are global), so it takes no workspace id.

Not to be confused with the top-level ``duplicates`` module, the pure
resolver this repository feeds.
"""

import os

from keyword_identity import embedded_keyword_associations_for_merge
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
    def __init__(self, conn, *, chunk_size=800):
        self.conn = conn
        self.chunk_size = chunk_size

    def transaction(self):
        """The connection as a context manager: commit on success, roll back on error."""
        return self.conn

    # -- groups -------------------------------------------------------------

    def is_group_member(self, photo_id):
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

    def workspace_names(self, photo_ids):
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

    def live_ids_for_hash(self, file_hash):
        """Return the ids of non-rejected photos with ``file_hash``."""
        dup_rows = self.conn.execute(
            "SELECT id FROM photos WHERE file_hash = ? AND (flag IS NULL OR flag != 'rejected')",
            (file_hash,),
        ).fetchall()
        return [r["id"] for r in dup_rows]

    def live_paths_for_hash(self, file_hash):
        """Rows (``filename``, ``path``) of the non-rejected photos with ``file_hash``.

        Inner-joins ``folders``, so a photo whose folder row is gone is absent.
        """
        return self.conn.execute(
            "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id=p.folder_id "
            "WHERE p.file_hash = ? AND (p.flag IS NULL OR p.flag != 'rejected')",
            (file_hash,),
        ).fetchall()

    def loser_candidate_rows(self, photo_ids):
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

    def loser_disk_summary(self):
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

    def cleanup_rows(self):
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

    def find_groups(self, include_resolved=False):
        """Return duplicate groups; see ``Database.find_duplicate_groups``.

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

        Returns ``(winner_id, loser_ids)``, or ``None`` when fewer than 2
        non-rejected candidates remain, or the ``DEFERRED_PLAN`` sentinel
        when at least one candidate lives on an offline volume (state
        unknown; the resolver can't safely pick a winner until the volume
        returns). Writes nothing.
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
        any_offline = False
        for r in rows:
            path = os.path.join(r["folder_path"] or "", r["filename"] or "")
            # Stat each candidate so the resolver doesn't pick a winner
            # whose file was moved/deleted on disk. The DB row would
            # otherwise outvote a surviving twin solely on path-string
            # heuristics.
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
            if offline:
                any_offline = True
            candidates.append(
                DupCandidate(
                    id=r["id"],
                    path=path,
                    mtime=r["file_mtime"] or 0.0,
                    # Placeholder; overridden by the offline-defer below when
                    # we return None. When every candidate is reachable, this
                    # is the real on-disk state and Rule 0 applies as usual.
                    exists=present,
                )
            )
        # An offline candidate's on-disk state is unknown. Auto-resolution
        # can't safely pick a winner without confirming: if the offline
        # row wins by path/mtime and its file was actually deleted while
        # the volume was down, ``apply_duplicate_resolution`` would reject
        # the only reachable copy and stamp a ``duplicate_rejections`` row
        # that ``reopen_duplicate_group`` would then un-reject only after
        # the volume returns and the scan re-runs. Defer instead and let
        # the interactive duplicate scan surface the group; that scan
        # treats offline as "state unknown" for the user to resolve.
        # ``DEFERRED_PLAN`` is distinct from ``None`` so callers (and the
        # ``/api/duplicates/apply`` route) can tell "state unknown, keep
        # the group visible" apart from "fewer than 2 candidates, nothing
        # to do" and communicate the deferral back to the user.
        if any_offline:
            return DEFERRED_PLAN
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

    def loser_keyword_rows(self, loser_ids, *, survivor_id=None):
        """Read loser tags in chunks, respecting the survivor's embedded removals."""
        rows = []
        for chunk in self._chunks(loser_ids):
            loser_placeholders = ",".join("?" * len(chunk))
            rows.extend(self.conn.execute(
                f"""SELECT photo_id, keyword_id, source
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
