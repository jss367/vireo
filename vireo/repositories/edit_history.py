"""Persistence for the undo/redo edit history.

``Database`` keeps the composition: it picks the handler for each action
type, replays photo edits through the other domains' façade methods
(``tag_photo``, ``queue_change``, ``update_prediction_status``, ...), runs
the cache-linked grouping restores, and decides when to commit around them.
This repository owns the SQL those steps read and write: the
``edit_history`` / ``edit_history_items`` rows, the undo/redo cursor, stale
cache-entry retirement, the prediction-review replay, the relabel-curation
restore and re-apply, and pruning.

Methods that act on the active workspace's history use ``self.workspace_id``,
which resolves ``workspace_id_fn`` (``Database._ws_id``) when read: each of
them reads it before running any SQL, so with no workspace active they raise
``RuntimeError`` having touched nothing. Building the repository never
resolves it, and the id-keyed helpers never read it. The undo/redo cursor
reads skip ``non_undoable`` (``Database._NON_UNDOABLE``, read when the
façade builds the repository). Where a moved body called another
``Database`` method mid-statement, the façade passes that method in as a
callback (``restore_species_representative``, ``prediction_scope``) so
patches of it still apply.

Callers reach it as ``db.edit_history`` (a fresh repository per access, see
``Database.edit_history``) for the history listings, the cursor reads and the
id-keyed lookups; recording, undo and redo stay on ``Database``.
"""

import json
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any


class EditHistoryRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        workspace_id_fn: Callable[[], int],
        *,
        non_undoable: Sequence[str],
        restore_species_representative: Callable[..., Any],
    ) -> None:
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn
        # Action types that appear in history but cannot be reversed.
        self.non_undoable = non_undoable
        # ``Database._restore_species_representative``, for the relabel
        # curation restores.
        self.restore_species_representative = restore_species_representative

    @property
    def workspace_id(self) -> int:
        """The active workspace id, resolved at each read (raises if none)."""
        return self.workspace_id_fn()

    def commit(self) -> None:
        """Commit the connection's open transaction."""
        self.conn.commit()

    # -- history rows ---------------------------------------------------------

    def record(
        self, action_type: str, description: str, new_value: str | None,
        items: Iterable[Mapping[str, Any]], is_batch: bool = False, _commit: bool = True,
    ) -> int:
        """Insert an edit and its per-photo items; see ``Database.record_edit``.

        The façade runs the history prune after a committed record.
        """
        ws = self.workspace_id
        # Clear redo stack — new edit invalidates undone entries
        self.conn.execute(
            "DELETE FROM edit_history WHERE workspace_id = ? AND undone = 1",
            (ws,),
        )
        cur = self.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, new_value, is_batch) VALUES (?, ?, ?, ?, ?)",
            (ws, action_type, description, new_value, 1 if is_batch else 0),
        )
        edit_id = cur.lastrowid
        for item in items:
            self.conn.execute(
                "INSERT INTO edit_history_items (edit_id, photo_id, old_value, new_value) VALUES (?, ?, ?, ?)",
                (edit_id, item['photo_id'], item['old_value'], item['new_value']),
            )
        if _commit:
            self.conn.commit()
        return edit_id

    def list_recent(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        """Return recent edit history entries (most recent first) with item counts."""
        rows = self.conn.execute(
            """SELECT eh.*, COUNT(ehi.id) as item_count
               FROM edit_history eh
               LEFT JOIN edit_history_items ehi ON ehi.edit_id = eh.id
               WHERE eh.workspace_id = ? AND eh.undone = 0
               GROUP BY eh.id
               ORDER BY eh.created_at DESC, eh.id DESC
               LIMIT ? OFFSET ?""",
            (self.workspace_id, limit, offset),
        ).fetchall()
        entries = [dict(r) for r in rows]
        for entry in entries:
            if entry['action_type'] == 'pipeline_grouping':
                entry['new_value'] = None
        return entries

    def recipe_history_for_photo(self, photo_id: int, limit: int) -> list[sqlite3.Row]:
        """The active workspace's ``edit_recipe`` items for one photo, newest first.

        Rows carry ``id``, ``description``, ``created_at``, ``undone`` (from
        the edit) and ``old_value``, ``new_value`` (from the photo's item),
        ordered by ``created_at`` then ``id`` descending, at most ``limit``.
        Undone edits are included. Raises ``RuntimeError`` when no workspace
        is active.
        """
        return self.conn.execute(
            """SELECT eh.id, eh.description, eh.created_at, eh.undone,
                      ehi.old_value, ehi.new_value
               FROM edit_history eh
               JOIN edit_history_items ehi ON ehi.edit_id = eh.id
               WHERE eh.workspace_id = ?
                 AND eh.action_type = 'edit_recipe'
                 AND ehi.photo_id = ?
               ORDER BY eh.created_at DESC, eh.id DESC
               LIMIT ?""",
            (self.workspace_id, photo_id, limit),
        ).fetchall()

    # -- undo / redo cursor ---------------------------------------------------

    def next_undo(self) -> tuple[dict[str, Any], list[sqlite3.Row]] | None:
        """Return ``(entry, items)`` for the newest undoable edit, or None."""
        non_undoable = self.non_undoable
        placeholders = ",".join("?" for _ in non_undoable)
        entry = self.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (self.workspace_id, *non_undoable),
        ).fetchone()
        if not entry:
            return None
        entry = dict(entry)
        items = self.conn.execute(
            "SELECT * FROM edit_history_items WHERE edit_id = ?",
            (entry['id'],),
        ).fetchall()
        return entry, items

    def next_redo(self) -> tuple[dict[str, Any], list[sqlite3.Row]] | None:
        """Return ``(entry, items)`` for the oldest undone edit, or None."""
        non_undoable = self.non_undoable
        placeholders = ",".join("?" for _ in non_undoable)
        entry = self.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND undone = 1 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at ASC, id ASC LIMIT 1",
            (self.workspace_id, *non_undoable),
        ).fetchone()
        if not entry:
            return None
        entry = dict(entry)
        items = self.conn.execute(
            "SELECT * FROM edit_history_items WHERE edit_id = ?",
            (entry['id'],),
        ).fetchall()
        return entry, items

    def latest_undoable(self) -> sqlite3.Row | None:
        """``id`` and ``description`` of the edit undo would reverse next, or None."""
        non_undoable = self.non_undoable
        placeholders = ",".join("?" for _ in non_undoable)
        return self.conn.execute(
            f"SELECT id, description FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (self.workspace_id, *non_undoable),
        ).fetchone()

    def count_undoable(self) -> int:
        """How many of the active workspace's edits undo can still reverse.

        Not undone, and not one of the ``non_undoable`` action types.
        """
        non_undoable = self.non_undoable
        placeholders = ",".join("?" for _ in non_undoable)
        return self.conn.execute(
            f"SELECT COUNT(*) FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders})",
            (self.workspace_id, *non_undoable),
        ).fetchone()[0]

    def oldest_redoable(self) -> sqlite3.Row | None:
        """``id`` and ``description`` of the edit redo would replay next, or None."""
        non_undoable = self.non_undoable
        placeholders = ",".join("?" for _ in non_undoable)
        return self.conn.execute(
            f"SELECT id, description FROM edit_history WHERE workspace_id = ? AND undone = 1 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at ASC, id ASC LIMIT 1",
            (self.workspace_id, *non_undoable),
        ).fetchone()

    def item_photo_ids(self, edit_id: int, *, distinct: bool = False) -> list[int]:
        """The ``photo_id`` of each of an edit's items; ``distinct=True`` drops repeats."""
        sql = (
            "SELECT DISTINCT photo_id FROM edit_history_items WHERE edit_id = ?"
            if distinct else
            "SELECT photo_id FROM edit_history_items WHERE edit_id = ?"
        )
        return [row["photo_id"] for row in self.conn.execute(sql, (edit_id,))]

    def has_changed_items(self, edit_id: int) -> bool:
        """Whether any of an edit's items has ``old_value != new_value``."""
        return self.conn.execute(
            "SELECT 1 FROM edit_history_items WHERE edit_id = ? AND old_value != new_value LIMIT 1",
            (edit_id,),
        ).fetchone() is not None

    def mark_undone(self, entry_id: int) -> None:
        """Flag an entry as undone and commit."""
        self.conn.execute("UPDATE edit_history SET undone = 1 WHERE id = ?", (entry_id,))
        self.conn.commit()

    def mark_redone(self, entry_id: int) -> None:
        """Clear an entry's undone flag and commit."""
        self.conn.execute("UPDATE edit_history SET undone = 0 WHERE id = ?", (entry_id,))
        self.conn.commit()

    def retire_stale_grouping_entry(self, entry_id: int) -> None:
        """Retire stale cache state while retaining any reversible photo edit.

        The caller commits this retirement and reports it to the user before
        another action can run. This helper never applies a photo change.
        """
        row = self.conn.execute(
            "SELECT action_type, new_value, description FROM edit_history WHERE id = ?", (entry_id,),
        ).fetchone()
        if row and row['action_type'] == 'pipeline_grouping':
            photo_edit = json.loads(row['new_value']).get('photo_edit')
            if photo_edit:
                # A recompute invalidates structure, not the recorded photo edit.
                self.conn.execute(
                    "UPDATE edit_history SET new_value = ?, description = ? WHERE id = ?",
                    (json.dumps({'photo_edit': photo_edit, 'photo_only': True}),
                     "Photo changes from: " + row["description"], entry_id),
                )
                # The stale snapshot is never restored again; drop its blob.
                self.conn.execute(
                    "DELETE FROM edit_history_payloads WHERE edit_id = ?", (entry_id,),
                )
                return
        self.conn.execute("DELETE FROM edit_history WHERE id = ?", (entry_id,))

    def action_and_new_value(self, entry_id: int) -> sqlite3.Row | None:
        """Row (``action_type``, ``new_value``) of one entry, or None."""
        return self.conn.execute(
            "SELECT action_type, new_value FROM edit_history WHERE id = ?",
            (entry_id,),
        ).fetchone()

    # -- lookups used by the replay handlers ----------------------------------

    def keyword_name(self, keyword_id: int) -> str | None:
        """The keyword's ``name``, or None for an unknown id."""
        row = self.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        return row['name'] if row else None

    def prediction_scope(self, pred_id: int) -> tuple[int, str, str] | None:
        """``(detection_id, classifier_model, labels_fingerprint)`` or None."""
        row = self.conn.execute(
            """SELECT detection_id, classifier_model AS model, labels_fingerprint
               FROM predictions WHERE id = ?""",
            (pred_id,),
        ).fetchone()
        if not row:
            return None
        return (row["detection_id"], row["model"], row["labels_fingerprint"])

    def workspace_exists(self, workspace_id: int) -> bool:
        """True while the workspace row is still present."""
        return self.conn.execute(
            'SELECT 1 FROM workspaces WHERE id = ?', (workspace_id,),
        ).fetchone() is not None

    # -- prediction review replay ---------------------------------------------

    def undo_prediction_accept_statuses(
        self, pred_ids: Iterable[int], prediction_scope: Callable[[int], tuple[int, str, str] | None],
    ) -> None:
        """Reset each recorded prediction's scope to its pre-accept state.

        ``pred_ids`` is non-empty; ``prediction_scope`` is the façade's
        ``_prediction_scope``. See ``Database._undo_prediction_accept_statuses``.
        """
        ws = self.workspace_id
        seen_scopes = set()
        for pred_id in pred_ids:
            scope = prediction_scope(pred_id)
            if scope is None or scope in seen_scopes:
                continue
            seen_scopes.add(scope)
            siblings = self.conn.execute(
                """SELECT id, confidence FROM predictions
                   WHERE detection_id = ?
                     AND classifier_model = ?
                     AND labels_fingerprint = ?
                   ORDER BY confidence DESC""",
                scope,
            ).fetchall()
            # Flip any accepted/rejected review rows in this workspace back
            # to 'alternative' -- scoped to the same fingerprint so other
            # label sets' statuses are preserved.
            self.conn.execute(
                """UPDATE prediction_review SET status = 'alternative',
                                              reviewed_at = datetime('now')
                   WHERE workspace_id = ?
                     AND status IN ('accepted', 'rejected')
                     AND prediction_id IN (
                        SELECT id FROM predictions
                        WHERE detection_id = ?
                          AND classifier_model = ?
                          AND labels_fingerprint = ?
                     )""",
                (ws, *scope),
            )
            # Promote highest-confidence sibling back to 'pending'.
            if siblings:
                self.conn.execute(
                    """INSERT INTO prediction_review
                         (prediction_id, workspace_id, status, reviewed_at)
                       VALUES (?, ?, 'pending', datetime('now'))
                       ON CONFLICT(prediction_id, workspace_id)
                       DO UPDATE SET status = 'pending',
                                     reviewed_at = datetime('now')""",
                    (siblings[0]["id"], ws),
                )
        if seen_scopes:
            self.conn.commit()

    def reject_accept_siblings(self, accepted_by_scope: Mapping[tuple[int, str, str], set[int]]) -> None:
        """Re-reject the open siblings of each re-accepted scope.

        ``accepted_by_scope`` maps ``(detection_id, model, fingerprint)`` to
        the prediction ids the redo re-accepted there; see
        ``Database._redo_prediction_accept_statuses``.
        """
        ws = self.workspace_id
        for scope, accepted_ids in accepted_by_scope.items():
            placeholders = ",".join("?" * len(accepted_ids))
            sibs = self.conn.execute(
                f"""SELECT pr.id FROM predictions pr
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ?
                     AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND pr.id NOT IN ({placeholders})
                     AND COALESCE(pr_rev.status, 'pending')
                         IN ('pending', 'alternative')""",
                (ws, *scope, *accepted_ids),
            ).fetchall()
            for s in sibs:
                self.conn.execute(
                    """INSERT INTO prediction_review
                         (prediction_id, workspace_id, status, reviewed_at)
                       VALUES (?, ?, 'rejected', datetime('now'))
                       ON CONFLICT(prediction_id, workspace_id)
                       DO UPDATE SET status = 'rejected',
                                     reviewed_at = datetime('now')""",
                    (s["id"], ws),
                )
        if accepted_by_scope:
            self.conn.commit()

    # -- relabel curation -----------------------------------------------------

    def restore_relabel_curation(
        self, workspace_id: int, photo_id: int, new_species: str, curation: Mapping[str, Any] | None,
    ) -> None:
        """Undo the curation migration performed by ``api_highlights_relabel``.

        For each ``species_highlights`` row the relabel moved from an old
        species bucket to ``new_species``, delete the row at ``new_species``
        and re-insert it at the end of the old bucket (unless the photo
        already appears there). For each ``photo_preferences`` row moved
        by the relabel, delete the row at ``(new_species, purpose)`` and
        re-insert it at ``(old_species, purpose)``. For each rep-only
        ``species_representatives`` row moved with no matching
        ``photo_preferences`` row, delete the row at ``new_species`` and
        re-insert it at ``old_species``. Best-effort: if the target row no
        longer exists (state has changed since the relabel), the
        corresponding restore is a no-op. Representatives are restored
        through ``restore_species_representative`` (the façade's
        ``_restore_species_representative``).
        """
        restore_species_representative = self.restore_species_representative
        if not curation:
            return
        hl_prev = curation.get("hl_prev") or []
        pref_prev = curation.get("pref_prev") or []
        rep_prev = curation.get("rep_prev") or []
        for hl in hl_prev:
            # Newer relabels record {species, rank, dst_existed}; entries
            # from older relabels (before PR #1161 landed rank capture)
            # are plain species-name strings and fall back to
            # append-at-end with dst_existed=False.
            if isinstance(hl, dict):
                old_species = hl.get("species")
                target_rank = hl.get("rank")
                dst_existed = bool(hl.get("dst_existed", False))
            else:
                old_species = hl
                target_rank = None
                dst_existed = False
            if not old_species or old_species == new_species:
                continue
            if not dst_existed:
                # Only delete the destination row when the relabel
                # actually created it. If the photo was already
                # highlighted at `new_species` before the relabel,
                # rename_species_highlights_species skipped inserting a
                # duplicate — undo must not remove the pre-existing row.
                self.conn.execute(
                    """DELETE FROM species_highlights
                       WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                    (workspace_id, new_species, photo_id),
                )
            existing = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, old_species, photo_id),
            ).fetchone()
            if existing:
                continue
            if target_rank is None:
                rank = int(self.conn.execute(
                    """SELECT COALESCE(MAX(rank), 0) AS max_rank
                       FROM species_highlights
                       WHERE workspace_id = ? AND species = ?""",
                    (workspace_id, old_species),
                ).fetchone()["max_rank"] or 0) + 1
            else:
                try:
                    rank = int(target_rank)
                except (TypeError, ValueError):
                    rank = int(self.conn.execute(
                        """SELECT COALESCE(MAX(rank), 0) AS max_rank
                           FROM species_highlights
                           WHERE workspace_id = ? AND species = ?""",
                        (workspace_id, old_species),
                    ).fetchone()["max_rank"] or 0) + 1
            self.conn.execute(
                """INSERT INTO species_highlights
                       (workspace_id, species, photo_id, rank,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, old_species, photo_id, rank),
            )
        for pref in pref_prev:
            if not isinstance(pref, dict):
                continue
            purpose = pref.get("purpose")
            old_species = pref.get("species")
            dst_existed = bool(pref.get("dst_existed", False))
            rep_dst_existed = bool(pref.get("rep_dst_existed", False))
            rep_selected_order = pref.get("rep_selected_order")
            if not purpose or not old_species or old_species == new_species:
                continue
            # Only delete the (new_species, purpose) row when the relabel
            # created it. When the destination slot was already taken
            # before the relabel — either by this photo or a different
            # one — rename_photo_preferences_species's INSERT OR IGNORE
            # was ignored and no new row was written for this photo, so
            # undo must leave the destination alone.
            if not dst_existed:
                self.conn.execute(
                    """DELETE FROM photo_preferences
                       WHERE workspace_id = ? AND purpose = ?
                         AND species = ? AND photo_id = ?""",
                    (workspace_id, purpose, new_species, photo_id),
                )
            # Only delete the (new_species, photo_id) rep row when the
            # relabel created it. If the photo was already a global
            # representative for new_species before the retag — e.g. a
            # multi-species photo picked as rep for both A and B before
            # relabeling A→B — rename_species_representatives_species's
            # INSERT OR IGNORE skipped a duplicate and the destination
            # rep row is pre-existing; undo must leave it alone.
            # rep_dst_existed defaults to False for edit-history rows
            # written before this field was added, preserving the older
            # (over-eager) behavior for legacy undos.
            if not rep_dst_existed:
                self.conn.execute(
                    """DELETE FROM species_representatives
                       WHERE species = ? AND photo_id = ?""",
                    (new_species, photo_id),
                )
            # Restore the old-species preference unconditionally. The
            # previous gate on finding a `(new_species, purpose,
            # photo_id)` row skipped restore when the relabel collided
            # with a different photo holding the destination slot,
            # stranding the old species' representative.
            self.conn.execute(
                """INSERT OR IGNORE INTO photo_preferences
                       (workspace_id, purpose, species, photo_id,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, purpose, old_species, photo_id),
            )
            restore_species_representative(
                old_species, photo_id, selected_order=rep_selected_order,
            )
        for rep in rep_prev:
            if not isinstance(rep, dict):
                continue
            old_species = rep.get("species")
            dst_existed = bool(rep.get("dst_existed", False))
            rep_selected_order = rep.get("selected_order")
            if not old_species or old_species == new_species:
                continue
            # Only delete the (new_species, photo_id) rep row when the
            # relabel actually created it. If the photo was already a
            # global representative for new_species before the retag,
            # rename_species_representatives_species's INSERT OR IGNORE
            # skipped a duplicate and the destination row is pre-existing;
            # undo must leave it alone.
            if not dst_existed:
                self.conn.execute(
                    """DELETE FROM species_representatives
                       WHERE species = ? AND photo_id = ?""",
                    (new_species, photo_id),
                )
            restore_species_representative(
                old_species, photo_id, selected_order=rep_selected_order,
            )

    def reapply_relabel_curation(
        self, workspace_id: int, photo_id: int, new_species: str, curation: Mapping[str, Any] | None,
    ) -> None:
        """Redo the curation migration reversed by
        :meth:`restore_relabel_curation`. Moves rows from each recorded
        old species back onto ``new_species``.
        """
        restore_species_representative = self.restore_species_representative
        if not curation:
            return
        hl_prev = curation.get("hl_prev") or []
        pref_prev = curation.get("pref_prev") or []
        rep_prev = curation.get("rep_prev") or []
        for hl in hl_prev:
            # Accept both new dict form ({species, rank}) and legacy
            # string form for compatibility with older edit-history rows.
            if isinstance(hl, dict):
                old_species = hl.get("species")
            else:
                old_species = hl
            if not old_species or old_species == new_species:
                continue
            src = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, old_species, photo_id),
            ).fetchone()
            if not src:
                continue
            self.conn.execute(
                """DELETE FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, old_species, photo_id),
            )
            existing = self.conn.execute(
                """SELECT 1 FROM species_highlights
                   WHERE workspace_id = ? AND species = ? AND photo_id = ?""",
                (workspace_id, new_species, photo_id),
            ).fetchone()
            if existing:
                continue
            next_rank = int(self.conn.execute(
                """SELECT COALESCE(MAX(rank), 0) AS max_rank
                   FROM species_highlights
                   WHERE workspace_id = ? AND species = ?""",
                (workspace_id, new_species),
            ).fetchone()["max_rank"] or 0) + 1
            self.conn.execute(
                """INSERT INTO species_highlights
                       (workspace_id, species, photo_id, rank,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, new_species, photo_id, next_rank),
            )
        for pref in pref_prev:
            if not isinstance(pref, dict):
                continue
            purpose = pref.get("purpose")
            old_species = pref.get("species")
            if not purpose or not old_species or old_species == new_species:
                continue
            rep_selected_order = pref.get("rep_selected_order")
            row = self.conn.execute(
                """SELECT 1 FROM photo_preferences
                   WHERE workspace_id = ? AND purpose = ?
                     AND species = ? AND photo_id = ?""",
                (workspace_id, purpose, old_species, photo_id),
            ).fetchone()
            if not row:
                continue
            self.conn.execute(
                """DELETE FROM photo_preferences
                   WHERE workspace_id = ? AND purpose = ?
                     AND species = ? AND photo_id = ?""",
                (workspace_id, purpose, old_species, photo_id),
            )
            self.conn.execute(
                """DELETE FROM species_representatives
                   WHERE species = ? AND photo_id = ?""",
                (old_species, photo_id),
            )
            self.conn.execute(
                """INSERT OR IGNORE INTO photo_preferences
                       (workspace_id, purpose, species, photo_id,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (workspace_id, purpose, new_species, photo_id),
            )
            # Reuse the captured pre-relabel selected_order rather than
            # allocating a fresh MAX+1. The original relabel preserved the
            # source row's order via rename_species_representatives_species,
            # so redoing must restore that same order — otherwise an
            # undo/redo round trip can promote a secondary representative
            # above a pre-existing primary for new_species.
            restore_species_representative(
                new_species, photo_id, selected_order=rep_selected_order,
            )
        for rep in rep_prev:
            if not isinstance(rep, dict):
                continue
            old_species = rep.get("species")
            if not old_species or old_species == new_species:
                continue
            rep_selected_order = rep.get("selected_order")
            src = self.conn.execute(
                """SELECT 1 FROM species_representatives
                   WHERE species = ? AND photo_id = ?""",
                (old_species, photo_id),
            ).fetchone()
            if not src:
                continue
            self.conn.execute(
                """DELETE FROM species_representatives
                   WHERE species = ? AND photo_id = ?""",
                (old_species, photo_id),
            )
            restore_species_representative(
                new_species, photo_id, selected_order=rep_selected_order,
            )

    # -- pruning --------------------------------------------------------------

    def prune(self, max_entries: int, preserve_wildlife_discard: bool) -> None:
        """Delete this workspace's oldest done entries beyond ``max_entries``.

        Undone entries awaiting redo are never pruned. While
        ``preserve_wildlife_discard`` is true, a discarded Wildlife keyword
        add is kept too; see ``Database._prune_edit_history``.
        """
        ws = self.workspace_id
        protected_clause = ""
        if preserve_wildlife_discard:
            # A discarded manual keyword add deliberately leaves no pending
            # change or sidecar term. Until Wildlife retirement completes,
            # its discard item is therefore the only durable authorship
            # evidence and must not disappear under a small history limit.
            protected_clause = """
              AND NOT EXISTS (
                  SELECT 1 FROM edit_history_items protected_item
                  WHERE protected_item.edit_id = edit_history.id
                    AND edit_history.action_type = 'discard'
                    AND protected_item.old_value = 'keyword_add:Wildlife' COLLATE NOCASE
              )"""
        self.conn.execute(
            f"""DELETE FROM edit_history
                WHERE workspace_id = ? AND undone = 0
                  AND id NOT IN (
                      SELECT id FROM edit_history
                      WHERE workspace_id = ? AND undone = 0
                      ORDER BY created_at DESC, id DESC LIMIT ?
                  )
                  {protected_clause}""",
            (ws, ws, max_entries),
        )
        self.conn.commit()
