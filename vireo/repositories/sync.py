"""Persistence for the pending XMP sync queue (``pending_changes``).

``Database`` owns the active-workspace state and the composition: it decides
when cancelling a captured keyword queues its inverse (through
``Database.queue_change`` and ``Database._pending_keyword_sidecar_alias``),
when a clear also drops equivalent flat removals (through
``Database.clear_equivalent_flat_removals``), and whether flag sync is on.
Those calls stay on the façade so monkeypatches of ``Database`` methods keep
taking effect. This repository owns the SQL.

The active workspace is resolved lazily. Several of these methods only
consult it once a row turns up (the staged sync scopes) or inside a
``with conn:`` block (the sync claim), and the façade never resolved it
earlier than that, so the repository takes the resolver
(``Database._ws_id``) rather than an id and reads ``self.workspace_id`` at
exactly the points the original code called ``self._ws_id()``. Methods that
take an optional ``workspace_id`` fall back to it the same way.

``_commit`` flags are carried through unchanged: ``_commit=False`` means the
caller owns the transaction, and no method here commits unless the
``Database`` method it backs did.
"""

import contextlib
import os
import uuid

from keyword_normalization import keyword_match_key, normalize_keyword_display


class SyncRepository:
    def __init__(self, conn, resolve_workspace_id, *, chunk_size=800):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        self.chunk_size = chunk_size

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def commit(self):
        """Commit the connection's open transaction."""
        self.conn.commit()

    def _chunks(self, values, size=None):
        size = self.chunk_size if size is None else size
        values = list(values)
        return (
            values[index:index + size]
            for index in range(0, len(values), size)
        )

    # -- reads ----------------------------------------------------------------

    def count(self):
        """Return pending changes count."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM pending_changes WHERE workspace_id = ?",
            (self.workspace_id,),
        ).fetchone()[0]

    def list_all(self):
        """Return all pending changes ordered by creation time."""
        return self.conn.execute(
            "SELECT * FROM pending_changes WHERE workspace_id = ? ORDER BY created_at, id",
            (self.workspace_id,),
        ).fetchall()

    def staged_scope_by_photos(self, photo_ids):
        """Photo-id scoped variant of :meth:`staged_scope`.

        See ``Database.staged_sync_scope_by_photos``.
        """
        here_photos, here_changes, other_photos = set(), [], set()
        if not photo_ids:
            return here_changes, 0, 0, 0
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"SELECT id, photo_id, workspace_id, change_token "
                f"FROM pending_changes "
                f"WHERE photo_id IN ({placeholders})",
                tuple(chunk),
            ):
                if row["workspace_id"] == self.workspace_id:
                    identity = row["change_token"] or ("id", row["id"])
                    here_changes.append((identity, row["id"], row["photo_id"]))
                    here_photos.add(row["photo_id"])
                else:
                    other_photos.add(row["photo_id"])
        return (
            here_changes,
            len(here_photos),
            len(other_photos - here_photos),
            len(other_photos & here_photos),
        )

    def staged_scope(self, folder_ids):
        """Return ``(changes, photos_here, photos_elsewhere, photos_here_with_sibling_edits)``.

        See ``Database.staged_sync_scope`` for what each count means.
        """
        here_photos, here_changes, other_photos = set(), [], set()
        # The photo id rides along so a caller can tell which photos a pass
        # actually wrote without a second query.
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"SELECT pc.id, pc.photo_id, pc.workspace_id, pc.change_token "
                f"FROM pending_changes pc "
                f"JOIN photos p ON p.id = pc.photo_id "
                f"WHERE p.folder_id IN ({placeholders})",
                tuple(chunk),
            ):
                if row["workspace_id"] == self.workspace_id:
                    identity = row["change_token"] or ("id", row["id"])
                    here_changes.append((identity, row["id"], row["photo_id"]))
                    here_photos.add(row["photo_id"])
                else:
                    other_photos.add(row["photo_id"])
        return (
            here_changes,
            len(here_photos),
            len(other_photos - here_photos),
            len(other_photos & here_photos),
        )

    def keyword_removal_keys(self, photo_id, hierarchical=False):
        """Return normalized keyword keys awaiting removal for a photo.

        Reads across workspaces because photo metadata is global even though
        the sync queue is presented per workspace. ``keyword_remove_flat``
        suppresses flat XMP re-imports only; callers processing hierarchical
        entries request ``hierarchical=True`` and receive full removals only.
        """
        change_types = (
            ("keyword_remove",)
            if hierarchical
            else ("keyword_remove", "keyword_remove_flat")
        )
        placeholders = ",".join("?" for _ in change_types)
        rows = self.conn.execute(
            f"""SELECT value FROM pending_changes
                WHERE photo_id = ?
                  AND change_type IN ({placeholders})""",
            [photo_id, *change_types],
        ).fetchall()
        return {
            key
            for row in rows
            if (key := keyword_match_key(row["value"]))
        }

    def keyword_sidecar_alias(self, photo_id, workspace_id, value):
        """Return whether another queued keyword edit reaches this sidecar."""
        needs_inverse = False
        # Resolve all candidate sidecars, as sync does: differing
        # basenames or folder spellings may still alias one file.
        # Do not conservatively treat unrelated homonyms as shared;
        # that would turn a cancelled add into a destructive removal.
        candidates = self.conn.execute(
            "SELECT DISTINCT f.path, p.filename FROM photos p "
            "JOIN folders f ON f.id = p.folder_id "
            "JOIN pending_changes pc ON pc.photo_id = p.id "
            "WHERE p.id != ? AND pc.workspace_id = ? AND pc.value = ? COLLATE NOCASE "
            "AND pc.change_type IN ('keyword_add', 'keyword_remove', 'keyword_remove_flat')",
            (photo_id, workspace_id, value),
        ).fetchall()
        if candidates:
            own = self.conn.execute(
                "SELECT f.path, p.filename FROM photos p JOIN folders f ON f.id = p.folder_id "
                "WHERE p.id = ?", (photo_id,),
            ).fetchone()
            if own is not None:
                def sidecar_path(row):
                    return os.path.join(row["path"], os.path.splitext(row["filename"])[0] + ".xmp")

                own_path = os.path.normcase(os.path.realpath(sidecar_path(own)))
                for path in {sidecar_path(row) for row in candidates}:
                    other_path = os.path.normcase(os.path.realpath(path))
                    if own_path == other_path:
                        needs_inverse = True
                        break
                    if own_path.casefold() == other_path.casefold():
                        # Scheduling may over-group case variants;
                        # cancellation must confirm they are aliases.
                        with contextlib.suppress(OSError):
                            needs_inverse = os.path.samefile(own_path, other_path)
                        if needs_inverse:
                            break
        return needs_inverse

    # -- writes ---------------------------------------------------------------

    def queue(self, photo_id, change_type, value, workspace_id=None, _commit=True):
        """Add a change to the sync queue, skipping redundant intents.

        See ``Database.queue_change``. Returns the inserted pending change
        token, or None if already queued.
        """
        # Normalization choke point for sidecar-bound keyword names: the
        # queued value is written verbatim into XMP by sync_to_xmp, so a
        # stray-quote variant here would leak into sidecars even though
        # add_keyword stores the clean spelling. Normalizing in one place
        # also keeps the (photo_id, change_type, value) dedupe below and
        # the add/remove cancellation in app.py working on one spelling.
        if change_type in ("keyword_add", "keyword_remove", "keyword_remove_flat"):
            value = normalize_keyword_display(value)
            if not value:
                return None
        ws_id = workspace_id if workspace_id is not None else self.workspace_id
        if change_type == "rating":
            latest = self.conn.execute(
                "SELECT id, value FROM pending_changes WHERE photo_id = ? "
                "AND change_type = 'rating' AND workspace_id = ? ORDER BY id DESC LIMIT 1",
                (photo_id, ws_id),
            ).fetchone()
            existing = latest is not None and latest["value"] == value
            if existing:
                existing = self.conn.execute(
                    "SELECT 1 FROM pending_changes WHERE workspace_id = ? "
                    "AND id > ? AND photo_id != ? AND change_type = 'rating' LIMIT 1",
                    (ws_id, latest["id"], photo_id),
                ).fetchone() is None
        elif change_type in ("keyword_add", "keyword_remove", "keyword_remove_flat"):
            latest = self.conn.execute(
                "SELECT id, change_type, value FROM pending_changes WHERE photo_id = ? "
                "AND workspace_id = ? AND value = ? COLLATE NOCASE AND change_type IN "
                "('keyword_add', 'keyword_remove', 'keyword_remove_flat') "
                "ORDER BY created_at DESC, id DESC LIMIT 1",
                (photo_id, ws_id, value),
            ).fetchone()
            existing = (latest is not None and latest["change_type"] == change_type
                        and latest["value"] == value)
            if existing:
                # Another photo can share this sidecar. Do not discard a
                # repeated intent after an intervening edit on that photo.
                existing = self.conn.execute(
                    "SELECT 1 FROM pending_changes WHERE workspace_id = ? AND id > ? "
                    "AND photo_id != ? AND value = ? COLLATE NOCASE AND change_type IN "
                    "('keyword_add', 'keyword_remove', 'keyword_remove_flat') LIMIT 1",
                    (ws_id, latest["id"], photo_id, value),
                ).fetchone() is None
        else:
            existing = self.conn.execute(
                "SELECT id FROM pending_changes WHERE photo_id = ? AND change_type = ? AND value = ? AND workspace_id = ?",
                (photo_id, change_type, value, ws_id),
            ).fetchone()
        if existing:
            return None
        change_token = str(uuid.uuid4())
        self.conn.execute(
            "INSERT INTO pending_changes (photo_id, change_type, value, change_token, workspace_id) VALUES (?, ?, ?, ?, ?)",
            (photo_id, change_type, value, change_token, ws_id),
        )
        if _commit:
            self.conn.commit()
        return change_token

    def claim_for_sync(self, changes):
        """Mark selected edits as possibly written and return surviving rows.

        See ``Database.claim_pending_changes_for_sync``. Commits (or rolls
        back) through ``with conn:``.
        """
        claimed = {}
        with self.conn:
            for chunk in self._chunks(changes, size=400):
                placeholders = ",".join("(?, ?)" for _ in chunk)
                params = [part for c in chunk for part in (c["id"], c["change_token"] or "")]
                rows = self.conn.execute(
                    f"""UPDATE pending_changes SET sync_started = 1
                        WHERE workspace_id = ?
                          AND (id, COALESCE(change_token, '')) IN (VALUES {placeholders})
                        RETURNING *""",
                    [self.workspace_id, *params],
                ).fetchall()
                claimed.update({(c["id"], c["change_token"]): c for c in rows})
        return [claimed[key] for c in changes
                if (key := (c["id"], c["change_token"])) in claimed]

    def delete_matching(self, photo_id, workspace_id, change_type=None, value=None):
        """Delete a photo's pending changes in one workspace; return the rows.

        Does not commit: ``Database.remove_pending_changes`` queues any
        inverse in the same transaction and commits per its ``_commit``.
        """
        clauses = ["photo_id = ?", "workspace_id = ?"]
        params = [photo_id, workspace_id]
        if change_type is not None:
            clauses.append("change_type = ?")
            params.append(change_type)
        if value is not None:
            clauses.append("value = ?")
            params.append(value)

        return self.conn.execute(
            f"DELETE FROM pending_changes WHERE {' AND '.join(clauses)} RETURNING *",
            params,
        ).fetchall()

    def mark_sync_started(self, photo_id, workspace_id, change_type, value):
        """Flag matching rows as possibly written. Does not commit."""
        self.conn.execute(
            "UPDATE pending_changes SET sync_started = 1 "
            "WHERE photo_id = ? AND workspace_id = ? AND change_type = ? AND value = ?",
            (photo_id, workspace_id, change_type, value),
        )

    def remove_token(self, change_token):
        """Delete a single pending change by immutable token. Returns rows removed."""
        cur = self.conn.execute(
            "DELETE FROM pending_changes WHERE change_token = ? AND workspace_id = ?",
            (change_token, self.workspace_id),
        )
        self.conn.commit()
        return cur.rowcount

    def delete_by_ids(
        self, change_ids, *, clear_equivalent_flat_removals=False,
        expected_tokens=None,
    ):
        """Delete pending changes by id (token-checked when tokens are given).

        See ``Database.clear_pending``. Returns the flat keyword removals
        that were cleared when ``clear_equivalent_flat_removals`` is true
        (else an empty list). Does not commit.
        """
        workspace_id = self.workspace_id
        synced_changes = []
        if expected_tokens is None:
            for chunk in self._chunks(change_ids):
                placeholders = ",".join("?" for _ in chunk)
                if clear_equivalent_flat_removals:
                    rows = self.conn.execute(
                        f"""SELECT photo_id, change_type, value
                            FROM pending_changes
                            WHERE id IN ({placeholders}) AND workspace_id = ?
                              AND change_type = 'keyword_remove_flat'""",
                        [*chunk, workspace_id],
                    ).fetchall()
                    synced_changes.extend(rows)
                self.conn.execute(
                    f"DELETE FROM pending_changes WHERE id IN ({placeholders}) AND workspace_id = ?",
                    [*chunk, workspace_id],
                )
        else:
            if len(expected_tokens) != len(change_ids):
                raise ValueError(
                    "expected_tokens must be the same length as change_ids"
                )
            tokened = [tok for tok in expected_tokens if tok is not None]
            legacy_ids = [
                cid for cid, tok in zip(change_ids, expected_tokens, strict=True)
                if tok is None
            ]
            for chunk in self._chunks(tokened):
                placeholders = ",".join("?" for _ in chunk)
                if clear_equivalent_flat_removals:
                    rows = self.conn.execute(
                        f"""SELECT photo_id, change_type, value
                            FROM pending_changes
                            WHERE change_token IN ({placeholders})
                              AND workspace_id = ?
                              AND change_type = 'keyword_remove_flat'""",
                        [*chunk, workspace_id],
                    ).fetchall()
                    synced_changes.extend(rows)
                self.conn.execute(
                    f"DELETE FROM pending_changes "
                    f"WHERE change_token IN ({placeholders}) "
                    f"AND workspace_id = ?",
                    [*chunk, workspace_id],
                )
            for chunk in self._chunks(legacy_ids):
                placeholders = ",".join("?" for _ in chunk)
                if clear_equivalent_flat_removals:
                    rows = self.conn.execute(
                        f"""SELECT photo_id, change_type, value
                            FROM pending_changes
                            WHERE id IN ({placeholders})
                              AND workspace_id = ?
                              AND change_token IS NULL
                              AND change_type = 'keyword_remove_flat'""",
                        [*chunk, workspace_id],
                    ).fetchall()
                    synced_changes.extend(rows)
                self.conn.execute(
                    f"DELETE FROM pending_changes "
                    f"WHERE id IN ({placeholders}) "
                    f"AND workspace_id = ? "
                    f"AND change_token IS NULL",
                    [*chunk, workspace_id],
                )
        return synced_changes

    def delete_by_tokens(self, change_tokens, *, clear_equivalent_flat_removals=False):
        """Delete pending changes named by their immutable tokens.

        See ``Database.clear_pending_by_token``. Returns the flat keyword
        removals that were cleared when ``clear_equivalent_flat_removals`` is
        true (else an empty list). Does not commit.
        """
        workspace_id = self.workspace_id
        synced_changes = []
        for chunk in self._chunks(change_tokens):
            placeholders = ",".join("?" for _ in chunk)
            if clear_equivalent_flat_removals:
                synced_changes.extend(self.conn.execute(
                    f"""SELECT photo_id, change_type, value
                        FROM pending_changes
                        WHERE change_token IN ({placeholders}) AND workspace_id = ?
                          AND change_type = 'keyword_remove_flat'""",
                    [*chunk, workspace_id],
                ).fetchall())
            self.conn.execute(
                f"DELETE FROM pending_changes WHERE change_token IN ({placeholders}) "
                f"AND workspace_id = ?",
                [*chunk, workspace_id],
            )
        return synced_changes

    def clear_equivalent_flat_removals(self, changes, _commit=True):
        """Clear shared-sidecar flat removals represented by ``changes``."""
        shared_flat_removals = {
            (change["photo_id"], change["value"])
            for change in changes
            if change["change_type"] == "keyword_remove_flat"
        }
        if shared_flat_removals:
            self.conn.executemany(
                """DELETE FROM pending_changes
                   WHERE photo_id = ?
                     AND change_type = 'keyword_remove_flat'
                     AND value = ? COLLATE NOCASE""",
                shared_flat_removals,
            )
        if _commit:
            self.conn.commit()
