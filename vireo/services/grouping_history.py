"""Persist structural review edits in the workspace's ordinary undo history."""

import copy
import json
import os
from contextlib import contextmanager


class GroupingHistoryConflict(ValueError):
    """A newer grouping cannot safely be replaced by an older history entry.

    Bare ``GroupingHistoryConflict`` is transient — another writer holds the
    workspace regroup lock and undo is worth retrying. ``GroupingHistoryStale``
    means the cached encounter structure has diverged so far from this entry's
    snapshot that no future retry could succeed; callers must retire the
    history row instead of leaving it to block older undoable edits forever.
    """


class GroupingHistoryStale(GroupingHistoryConflict):
    """The cache no longer matches this entry — retire, don't retry."""


# Fields that ``/api/encounters/species`` mutates on the pipeline cache
# without changing encounter structure. Undo is strict LIFO, so any
# species edit that landed after a grouping edit has already been reverted
# by the time we compare snapshots here — its leftover cache state is not
# "newer work" that a grouping undo would silently overwrite.
_CACHE_SPECIES_ENCOUNTER_FIELDS = ("confirmed_species", "confirmed_species_list", "species_confirmed")
_CACHE_SPECIES_BURST_FIELDS = ("species_override",)


def _grouping_signature(encounters):
    """Return an encounters snapshot without cache-only species fields."""
    signature = []
    for enc in encounters or []:
        stripped = {
            k: v for k, v in enc.items()
            if k not in _CACHE_SPECIES_ENCOUNTER_FIELDS and k != "bursts"
        }
        stripped["bursts"] = [
            {k: v for k, v in b.items() if k not in _CACHE_SPECIES_BURST_FIELDS}
            if isinstance(b, dict) else {"photo_ids": b}
            for b in enc.get("bursts", [])
        ]
        signature.append(stripped)
    return signature


def _photo_ids_key(entry):
    """Stable dict key for the photo composition of an encounter or burst."""
    ids = entry.get("photo_ids") if isinstance(entry, dict) else entry
    return tuple(ids or ())


def _preserve_cleared_species_overrides(target, current, expected):
    """Carry ``clearBurstOverride`` clears from current into the restored target.

    ``clearBurstOverride`` writes ``species_override = None`` straight through
    ``/api/pipeline/save-cache`` without recording a history entry, so its
    change is invisible to ``_grouping_signature``. Without this pass, a
    subsequent grouping undo would silently resurrect the pre-clear override
    on the same burst. Match by photo_ids so bursts that moved between
    encounters (e.g. across a burst detach) still find their pair.

    Non-null species state in ``current`` is left alone: it's either the
    intended after-state of this grouping edit or leftover cache from a
    species edit that was itself undone (LIFO), and the caller relies on
    ``_grouping_signature`` treating that as unchanged. Only clears
    (values that ``current`` shows explicitly as ``None``) get carried
    forward.
    """
    expected_encs = {_photo_ids_key(e): e for e in expected or []}
    expected_bursts = {
        _photo_ids_key(b): b for e in expected or [] for b in e.get("bursts", []) if isinstance(b, dict)
    }
    current_bursts = {}
    current_encs = {}
    for enc in current or []:
        if not isinstance(enc, dict):
            continue
        current_encs[_photo_ids_key(enc)] = enc
        for b in enc.get("bursts", []) or []:
            if isinstance(b, dict):
                current_bursts[_photo_ids_key(b)] = b
    for enc in target or []:
        if not isinstance(enc, dict):
            continue
        cur_enc = current_encs.get(_photo_ids_key(enc))
        expected_enc = expected_encs.get(_photo_ids_key(enc), {})
        if cur_enc is not None:
            if ("confirmed_species" in cur_enc and cur_enc.get("confirmed_species") is None
                    and expected_enc.get("confirmed_species") is not None):
                enc["confirmed_species"] = None
            if ("species_confirmed" in cur_enc and not cur_enc.get("species_confirmed")
                    and expected_enc.get("species_confirmed")):
                enc["species_confirmed"] = False
        for b in enc.get("bursts", []) or []:
            if not isinstance(b, dict):
                continue
            cur_burst = current_bursts.get(_photo_ids_key(b))
            if cur_burst is None:
                continue
            if ("species_override" in cur_burst and cur_burst.get("species_override") is None
                    and expected_bursts.get(_photo_ids_key(b), {}).get("species_override") is not None):
                b["species_override"] = None


def _restored_summary(current_summary, encounters):
    """Rebuild the cache summary's structural counts for a restored group.

    ``restore_grouping_edit`` swaps ``encounters`` back to an older snapshot,
    but the surrounding ``summary`` mirrors the after state — its
    ``encounter_count`` and ``burst_count`` would keep advertising the
    post-edit structure and ``/api/pipeline/page-init`` would report
    counts that disagree with what the review page renders.
    """
    if not isinstance(current_summary, dict):
        summary = {}
    else:
        summary = dict(current_summary)
    summary["encounter_count"] = len(encounters or [])
    summary["burst_count"] = sum(
        len(enc.get("bursts") or []) for enc in (encounters or [])
        if isinstance(enc, dict)
    )
    return summary


# Keys of a ``pipeline_grouping`` change that hold whole-workspace encounter
# snapshots. They are stored in ``edit_history_payloads``; everything else
# (``photo_edit``, ``label_edit``, ``photo_only``) stays in the small
# ``edit_history.new_value`` so status/list/retire paths never load a snapshot.
GROUPING_SNAPSHOT_KEYS = ("before", "after")


def split_grouping_change(change):
    """Return ``(meta, snapshot)`` halves of a grouping change dict."""
    snapshot = {k: change[k] for k in GROUPING_SNAPSHOT_KEYS if k in change}
    meta = {k: v for k, v in change.items() if k not in GROUPING_SNAPSHOT_KEYS}
    return meta, snapshot


def store_grouping_snapshot(db, edit_id, snapshot):
    """Attach ``snapshot`` to history row ``edit_id`` (replacing any prior one)."""
    if not snapshot:
        db.conn.execute(
            "DELETE FROM edit_history_payloads WHERE edit_id = ?", (edit_id,),
        )
        return
    db.conn.execute(
        "INSERT OR REPLACE INTO edit_history_payloads (edit_id, payload) VALUES (?, ?)",
        (edit_id, json.dumps(snapshot)),
    )


def record_grouping_edit(db, description, change, items=(), *, _commit=False):
    """Record a ``pipeline_grouping`` edit, snapshot in the payload table.

    Same transaction contract as ``Database.record_edit``: with
    ``_commit=False`` the caller owns the commit and the later prune.
    """
    meta, snapshot = split_grouping_change(change)
    edit_id = db.record_edit(
        "pipeline_grouping", description, json.dumps(meta), items, _commit=False,
    )
    store_grouping_snapshot(db, edit_id, snapshot)
    if _commit:
        db.conn.commit()
        db._prune_edit_history()
    return edit_id


def convert_to_grouping_edit(db, edit_id, change):
    """Rewrite an existing photo edit as the grouping edit that wraps it."""
    meta, snapshot = split_grouping_change(change)
    db.conn.execute(
        "UPDATE edit_history SET action_type = 'pipeline_grouping', new_value = ? WHERE id = ?",
        (json.dumps(meta), edit_id),
    )
    store_grouping_snapshot(db, edit_id, snapshot)


def load_grouping_change(db, entry):
    """Return the full change dict for a ``pipeline_grouping`` history row.

    Merges the snapshot half back in from ``edit_history_payloads``. Rows
    written before the split still carry the snapshot inline and are read
    as-is, so an unmigrated database keeps undoing correctly.
    """
    change = json.loads(entry["new_value"] or "{}")
    if change.get("photo_only") or any(k in change for k in GROUPING_SNAPSHOT_KEYS):
        return change
    row = db.conn.execute(
        "SELECT payload FROM edit_history_payloads WHERE edit_id = ?",
        (entry["id"],),
    ).fetchone()
    if row is not None:
        change.update(json.loads(row[0]))
    return change


def save_grouping_edit(db, before, after, description, *, photo_edit=None, items=(), label_edit=False):
    """Save a detach and its history together, restoring the cache on failure.

    Callers hold the workspace regroup lock and SQLite writer transaction.
    Store only encounter structure, so undo never replaces photo metadata,
    flags, scores, or settings with an old whole-catalog snapshot.
    """
    from pipeline import save_results_raw

    cache_dir = os.path.dirname(db._db_path)
    if before["encounters"] == after["encounters"]:
        return
    change = {"before": before["encounters"], "after": after["encounters"]}
    if label_edit:
        change["label_edit"] = True
    if photo_edit:
        change["photo_edit"] = photo_edit
    record_grouping_edit(db, description, change, items)
    saved = False
    try:
        save_results_raw(after, cache_dir, db.require_workspace_id())
        saved = True
        db.conn.commit()
    except Exception:
        db.conn.rollback()
        if saved:
            save_results_raw(before, cache_dir, db.require_workspace_id())
        raise
    db._prune_edit_history()


def record_species_confirm_cache(
    db, *, species, target_enc, burst_index, submitted_photo_ids,
    new_override=None, new_encounter_state=None, description=None,
):
    """Record an ``/api/encounters/species`` call that only wrote the cache.

    When every submitted photo already carries the requested species keyword,
    the endpoint mutates ``confirmed_species`` / ``species_confirmed`` (or
    ``species_override`` for a burst) without any per-photo keyword change,
    so nothing is otherwise added to ``edit_history``. Without an entry,
    a preceding grouping edit would remain the newest undoable row, and
    its undo would silently discard this cache-only confirmation (grouping
    signatures ignore these fields by design because they belong to species
    LIFO, not to structure).

    Recording a lightweight ``species_confirm_cache`` entry restores LIFO
    ordering: undo reverts this cache write first, then the earlier grouping
    edit runs against pristine species state.

    Called from ``api_encounter_species`` *before* the cache mutation, inside
    the same transaction as the DB writes, with ``_commit=False``.
    """
    if not target_enc:
        return
    encounter_photo_ids = list(target_enc.get("photo_ids") or [])
    if burst_index is not None:
        bursts = target_enc.get("bursts") or []
        if not (0 <= burst_index < len(bursts)):
            return
        burst_photo_ids = list(bursts[burst_index].get("photo_ids") or [])
        prev_override = copy.deepcopy(bursts[burst_index].get("species_override"))
        payload = {
            "encounter_photo_ids": encounter_photo_ids,
            "burst_photo_ids": burst_photo_ids,
            "species": species,
            "previous_burst_override": prev_override,
        }
        if new_override is not None:
            # The exact override the endpoint is about to write (a species
            # set may carry several names, an unconfirmed edited baseline,
            # or the explicit-empty sentinel); restore compares against it.
            payload["new_burst_override"] = copy.deepcopy(new_override)
        new_value = json.dumps(payload)
        if description is None:
            description = f'Confirmed species "{species}" on 1 burst'
    else:
        prev_state = {
            "confirmed_species": target_enc.get("confirmed_species"),
            "species_confirmed": bool(target_enc.get("species_confirmed")),
        }
        payload = {
            "encounter_photo_ids": encounter_photo_ids,
            "submitted_photo_ids": list(submitted_photo_ids),
            "species": species,
            "previous_encounter_state": prev_state,
        }
        if new_encounter_state is not None:
            prev_state["confirmed_species_list"] = list(
                target_enc.get("confirmed_species_list")
                or ([target_enc["confirmed_species"]]
                    if target_enc.get("confirmed_species") else [])
            )
            payload["new_encounter_state"] = copy.deepcopy(new_encounter_state)
        new_value = json.dumps(payload)
        if description is None:
            description = (
                f'Confirmed species "{species}" on {len(submitted_photo_ids)} photos'
            )
    db.record_edit(
        "species_confirm_cache", description, new_value, [],
        is_batch=False, _commit=False,
    )


def _find_encounter_by_photo_ids(encounters, photo_ids):
    """Return the encounter dict whose photo_ids match, else None."""
    target_key = tuple(photo_ids or ())
    for enc in encounters or []:
        if isinstance(enc, dict) and tuple(enc.get("photo_ids") or ()) == target_key:
            return enc
    return None


@contextmanager
def restore_species_confirm_cache_edit(db, entry, *, undo):
    """Undo (or redo) a cache-only ``/api/encounters/species`` confirmation.

    Locates the recorded encounter / burst by its photo_ids and swaps the
    ``confirmed_species`` / ``species_confirmed`` (or ``species_override``)
    fields between the previous state and the ``species`` value that was
    written. Raises :class:`GroupingHistoryStale` when the encounter's
    composition has changed — a later grouping edit or pipeline recompute
    replaced the row — so the caller retires the entry rather than
    resurrecting a stale confirmation on the wrong photos.

    Uses ``restore_grouping_edit``'s ``current`` → mutate → save pattern so
    ``yield`` runs inside the workspace regroup lock and any DB failure
    inside the caller's transaction rolls back before the cache is put back.
    """
    from pipeline import load_results_raw, save_results_raw
    from pipeline_locks import acquire_workspace_regroup

    lock = acquire_workspace_regroup(db.require_workspace_id())
    if not lock.acquire(blocking=False):
        raise GroupingHistoryConflict(
            'Photo groups are being updated. Try again when processing finishes.'
        )
    try:
        cache_dir = os.path.dirname(db._db_path)
        current = load_results_raw(cache_dir, db.require_workspace_id())
        change = json.loads(entry["new_value"])
        if current is None:
            raise GroupingHistoryStale(
                "The photo groups have changed since this edit. "
                "This confirmation cannot be restored."
            )
        restored = copy.deepcopy(current)
        enc = _find_encounter_by_photo_ids(
            restored.get("encounters"), change.get("encounter_photo_ids"),
        )
        if enc is None:
            raise GroupingHistoryStale(
                "The photo groups have changed since this edit. "
                "This confirmation cannot be restored."
            )
        burst_photo_ids = change.get("burst_photo_ids")
        if burst_photo_ids is not None:
            target_burst = None
            for b in enc.get("bursts") or []:
                if isinstance(b, dict) and list(b.get("photo_ids") or []) == burst_photo_ids:
                    target_burst = b
                    break
            if target_burst is None:
                raise GroupingHistoryStale(
                    "The burst has changed since this edit. "
                    "This confirmation cannot be restored."
                )
            confirmed = change.get("new_burst_override")
            if confirmed is None:
                # Entries recorded before species sets: a single confirmed
                # species.
                confirmed = {"species": change["species"], "confirmed": True}
            previous = change.get("previous_burst_override")
            expected = confirmed if undo else previous
            if target_burst.get("species_override") != expected:
                raise GroupingHistoryStale(
                    "The burst label has changed since this edit. "
                    "This confirmation cannot be restored."
                )
            replacement = previous if undo else confirmed
            if replacement is None:
                target_burst.pop("species_override", None)
            else:
                target_burst["species_override"] = copy.deepcopy(replacement)
        else:
            previous = change.get("previous_encounter_state") or {}
            confirmed = change.get("new_encounter_state") or {
                "confirmed_species": change["species"], "species_confirmed": True,
            }
            # Entries recorded with species sets also carry the list; older
            # ones compare the two legacy fields only.
            with_list = "confirmed_species_list" in confirmed
            current_state = {
                "confirmed_species": enc.get("confirmed_species"),
                "species_confirmed": bool(enc.get("species_confirmed")),
            }
            if with_list:
                current_state["confirmed_species_list"] = list(
                    enc.get("confirmed_species_list")
                    or ([enc["confirmed_species"]]
                        if enc.get("confirmed_species") else [])
                )
            expected = confirmed if undo else previous
            if current_state != expected:
                raise GroupingHistoryStale(
                    "The encounter label has changed since this edit. "
                    "This confirmation cannot be restored."
                )
            replacement = previous if undo else confirmed
            if replacement.get("confirmed_species") is None:
                enc.pop("confirmed_species", None)
            else:
                enc["confirmed_species"] = replacement["confirmed_species"]
            enc["species_confirmed"] = bool(replacement.get("species_confirmed"))
            if with_list:
                enc["confirmed_species_list"] = list(
                    replacement.get("confirmed_species_list") or []
                )
        save_results_raw(restored, cache_dir, db.require_workspace_id())
        try:
            yield
        except Exception:
            if db.conn.in_transaction:
                db.conn.rollback()
            save_results_raw(current, cache_dir, db.require_workspace_id())
            raise
    finally:
        lock.release()


@contextmanager
def restore_grouping_edit(db, entry, *, undo):
    """Restore structure, retaining history if the write/commit fails.

    Refuse a stale snapshot after recomputation or a later structural edit
    instead of silently erasing that work. A leftover cache-only species
    override or confirmation from an already-reverted species edit is not a
    conflict — undo is strict LIFO, so any species entry newer than this
    grouping entry has already been undone by the time we get here.
    """
    from pipeline import load_results_raw, save_results_raw
    from pipeline_locks import acquire_workspace_regroup

    change = load_grouping_change(db, entry)
    if change.get("photo_only"):
        # A stale grouping snapshot was retired; its photo half still uses
        # the non-committing replay below and the caller's writer transaction.
        yield
        return

    # Undo already holds the database writer lock. Never wait here: processing
    # takes the regroup lock first and may need the database before releasing
    # it. Ordinary photo undo does not need this lock at all.
    lock = acquire_workspace_regroup(db.require_workspace_id())
    if not lock.acquire(blocking=False):
        raise GroupingHistoryConflict('Photo groups are being updated. Try again when processing finishes.')
    try:
        cache_dir = os.path.dirname(db._db_path)
        current = load_results_raw(cache_dir, db.require_workspace_id())
        expected = change["after" if undo else "before"]
        target = change["before" if undo else "after"]
        signature = (lambda value: value) if change.get("label_edit") else _grouping_signature
        if current is None or signature(current.get("encounters")) != signature(expected):
            raise GroupingHistoryStale(
                "The photo groups have changed since this edit. "
                "These groups cannot be restored without replacing newer work."
            )
        restored = copy.deepcopy(current)
        restored["encounters"] = copy.deepcopy(target)
        if not change.get("label_edit"):
            _preserve_cleared_species_overrides(
                restored["encounters"], current.get("encounters"), expected,
            )
        restored["summary"] = _restored_summary(
            current.get("summary"), restored["encounters"],
        )
        save_results_raw(restored, cache_dir, db.require_workspace_id())
        try:
            yield
        except Exception:
            # Roll back any pending DB writes (typically the caller's
            # ``UPDATE edit_history SET undone = ...`` or its commit) before
            # putting the cache back — otherwise a later commit on this
            # connection could flush the undone flag without the matching
            # grouping state on disk and leave history desynchronized.
            if db.conn.in_transaction:
                db.conn.rollback()
            save_results_raw(current, cache_dir, db.require_workspace_id())
            raise
    finally:
        lock.release()


def apply_grouping_photo_edit(db, entry, items, *, undo):
    """Replay the photo half of a grouping action without releasing its transaction."""
    change = json.loads(entry["new_value"]).get("photo_edit")
    if not change:
        return
    action = change["action_type"]
    for item in items:
        pid = item["photo_id"]
        if action == "flag":
            value = item["old_value" if undo else "new_value"]
            db.photo_review.set_flag(pid, value, _commit=False)
            db.queue_flag_change_if_enabled(pid, value, _commit=False)
            continue
        if action not in ("keyword_add", "species_replace"):
            raise ValueError("Unsupported photo edit in grouping history")
        old_ids = db._edit_old_value_meta(item["old_value"]).get("keyword_ids") or []
        new_ids = [int(item["new_value"])] if item["new_value"] else []
        remove_ids, add_ids = (new_ids, old_ids) if undo else (old_ids, new_ids)
        for kid in remove_ids:
            db.untag_photo(pid, kid, _commit=False)
            name = db._keyword_name(kid)
            if name and db.remove_pending_changes(pid, 'keyword_add', name, _commit=False) == 0:
                db.queue_change(pid, 'keyword_remove', name, _commit=False)
        for kid in add_ids:
            db.tag_photo(pid, kid, source='manual', _commit=False)
            name = db._keyword_name(kid)
            if name and db.remove_pending_changes(pid, 'keyword_remove', name, _commit=False) == 0:
                db.queue_change(pid, 'keyword_add', name, _commit=False)
