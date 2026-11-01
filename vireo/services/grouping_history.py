"""Persist structural review edits in the workspace's ordinary undo history."""

from __future__ import annotations

import copy
import json
import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from types import TracebackType

    from db import Database
    from services.service_types import JsonDict

    # An ``edit_history`` row as ``EditHistoryRepository.next_undo`` returns
    # it. Any: its columns are mixed SQLite values.
    HistoryEntry = Mapping[str, Any]


log = logging.getLogger(__name__)


class CacheRestore:
    """Puts the pipeline cache back if the decision it was saved in fails.

    The restore CM writes the pipeline results cache before the outer unit
    of work commits. Used as a context manager around the whole decision,
    so a failure anywhere after the save, including the unit's own commit,
    restores the cache the catalog still matches.

    ``add_cleanup`` registers a resource release — a regroup lock, say — to
    run after the restoration (whether or not it fired). The restore CM
    hands the regroup lock to ``cache_restore`` instead of releasing it in
    its own ``finally``, so the lock is still held while the outer commit
    runs AND while the cache is being put back. Cleanups run in LIFO order,
    like a nested context manager; one failing never stops the next.
    """

    def __init__(self) -> None:
        self.restore: Callable[[], object] | None = None
        self._cleanup: list[Callable[[], object]] = []

    def add_cleanup(self, fn: Callable[[], object]) -> None:
        """Register a cleanup to run after any restoration, regardless of outcome."""
        self._cleanup.append(fn)

    def __enter__(self) -> CacheRestore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is not None and self.restore is not None:
                try:
                    self.restore()
                except (OSError, TypeError, ValueError):
                    log.exception("Failed to restore pipeline cache after undo/redo failed")
        finally:
            while self._cleanup:
                fn = self._cleanup.pop()
                try:
                    fn()
                except RuntimeError:
                    log.exception("CacheRestore cleanup failed")


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


# These summaries are recomputed from live measurements and predictions when a
# saved review is reopened. They do not identify a user grouping edit.
_DERIVED_GROUP_FIELDS = ("species", "species_predictions", "focus_cohorts")


def _burst_signature(burst: JsonDict | list[int], *, include_confirmations: bool) -> JsonDict:
    if not isinstance(burst, dict):
        return {"photo_ids": burst}
    stripped = {key: value for key, value in burst.items()
                if key not in _DERIVED_GROUP_FIELDS + _CACHE_SPECIES_BURST_FIELDS}
    override = burst.get("species_override")
    if isinstance(override, dict) and (
        isinstance(override.get("species_list"), list)
        or (include_confirmations and override.get("confirmed"))
    ):
        # Explicit lists (even empty or unconfirmed) are user edits. Legacy
        # unconfirmed hints are derived; old single-value confirmation cache
        # leftovers retain the LIFO tolerance used by structural history.
        stripped["species_override"] = override
    return stripped


def _grouping_signature(
    encounters: Iterable[JsonDict] | None, *, include_confirmations: bool = False,
) -> list[JsonDict]:
    """Identify grouping edits independently of refreshed classifier/focus evidence."""
    excluded = _DERIVED_GROUP_FIELDS + (() if include_confirmations else _CACHE_SPECIES_ENCOUNTER_FIELDS)
    signature = []
    for enc in encounters or []:
        stripped = {key: value for key, value in enc.items() if key not in excluded and key != "bursts"}
        stripped["bursts"] = [
            _burst_signature(burst, include_confirmations=include_confirmations) for burst in enc.get("bursts", [])
        ]
        signature.append(stripped)
    return signature


def _photo_ids_key(entry: JsonDict | list[int] | None) -> tuple[int, ...]:
    """Stable dict key for the photo composition of an encounter or burst."""
    ids = entry.get("photo_ids") if isinstance(entry, dict) else entry
    return tuple(ids or ())


def _preserve_cleared_species_overrides(
    target: Iterable[JsonDict] | None, current: Iterable[JsonDict] | None,
    expected: Sequence[JsonDict] | None,
) -> None:
    """Carry ``clearBurstOverride`` clears from current into the restored target.

    Legacy direct cache clears can set ``species_override = None`` without
    recording history. A clear of a legacy single-value override is invisible
    to ``_grouping_signature``; without this pass, grouping undo would resurrect
    that pre-clear value. Explicit list edits remain checked by the signature,
    and the current clear endpoint records its own label-edit history. Match by
    photo_ids so bursts moved between encounters still find their pair.

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
    current_bursts: dict[tuple[int, ...], JsonDict] = {}
    current_encs: dict[tuple[int, ...], JsonDict] = {}
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


def _restored_summary(current_summary: object, encounters: Sequence[JsonDict] | None) -> JsonDict:
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


def split_grouping_change(change: JsonDict) -> tuple[JsonDict, JsonDict]:
    """Return ``(meta, snapshot)`` halves of a grouping change dict."""
    snapshot = {k: change[k] for k in GROUPING_SNAPSHOT_KEYS if k in change}
    meta = {k: v for k, v in change.items() if k not in GROUPING_SNAPSHOT_KEYS}
    return meta, snapshot


def store_grouping_snapshot(db: Database, edit_id: int, snapshot: JsonDict) -> None:
    """Attach ``snapshot`` to history row ``edit_id`` (replacing any prior one)."""
    if not snapshot:
        db.edit_history.delete_grouping_payload(edit_id)
        return
    db.edit_history.store_grouping_payload(edit_id, json.dumps(snapshot))


def record_grouping_edit(
    db: Database, description: str, change: JsonDict,
    items: Sequence[Mapping[str, object]] = (), *, _commit: bool = False,
) -> int:
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
        db.commit()
        db._prune_edit_history()
    return edit_id


def convert_to_grouping_edit(db: Database, edit_id: int, change: JsonDict) -> None:
    """Rewrite an existing photo edit as the grouping edit that wraps it."""
    meta, snapshot = split_grouping_change(change)
    db.edit_history.convert_to_grouping(edit_id, json.dumps(meta))
    store_grouping_snapshot(db, edit_id, snapshot)


def load_grouping_change(db: Database, entry: HistoryEntry) -> JsonDict:
    """Return the full change dict for a ``pipeline_grouping`` history row.

    Merges the snapshot half back in from ``edit_history_payloads``. Rows
    written before the split still carry the snapshot inline and are read
    as-is, so an unmigrated database keeps undoing correctly.
    """
    change = json.loads(entry["new_value"] or "{}")
    if change.get("photo_only") or any(k in change for k in GROUPING_SNAPSHOT_KEYS):
        return change
    row = db.edit_history.grouping_payload(entry["id"])
    if row is not None:
        change.update(json.loads(row[0]))
    return change


def save_grouping_edit(
    db: Database, before: JsonDict, after: JsonDict, description: str, *,
    photo_edit: JsonDict | None = None, items: Sequence[Mapping[str, object]] = (),
    label_edit: bool = False, on_record: Callable[[int], None] | None = None,
) -> None:
    """Save a detach and its history together, restoring the cache on failure.

    Callers hold the workspace regroup lock and SQLite writer transaction.
    Store only encounter structure, so undo never replaces photo metadata,
    flags, scores, or settings with an old whole-catalog snapshot.

    ``on_record`` runs after the edit row exists and before the commit so a
    caller can attach replay metadata (flag attribution snapshots, say) that
    must share this transaction.
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
    edit_id = record_grouping_edit(db, description, change, items)
    if on_record is not None:
        on_record(edit_id)
    saved = False
    try:
        save_results_raw(after, cache_dir, db.require_workspace_id())
        saved = True
        db.commit()
    except Exception:
        db.rollback()
        if saved:
            save_results_raw(before, cache_dir, db.require_workspace_id())
        raise
    db._prune_edit_history()


def record_species_confirm_cache(
    db: Database, *, species: str, target_enc: JsonDict | None, burst_index: int | None,
    submitted_photo_ids: Sequence[int], new_override: object = None,
    new_encounter_state: JsonDict | None = None, description: str | None = None,
) -> int | None:
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
        payload: JsonDict = {
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
    return db.record_edit(
        "species_confirm_cache", description, new_value, [],
        is_batch=False, _commit=False,
    )


def _find_encounter_by_photo_ids(
    encounters: Iterable[JsonDict] | None, photo_ids: Iterable[int] | None,
) -> JsonDict | None:
    """Return the encounter dict whose photo_ids match, else None."""
    target_key = tuple(photo_ids or ())
    for enc in encounters or []:
        if isinstance(enc, dict) and tuple(enc.get("photo_ids") or ()) == target_key:
            return enc
    return None


def _restore_burst_confirmation(
    enc: JsonDict, change: JsonDict, burst_photo_ids: list[int], *, undo: bool,
) -> None:
    """Swap a burst's ``species_override`` for a species-confirm undo/redo.

    Mutates the matching burst of ``enc`` in place; raises
    :class:`GroupingHistoryStale` when the burst or its label has changed.
    """
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


def _restore_encounter_confirmation(enc: JsonDict, change: JsonDict, *, undo: bool) -> None:
    """Swap an encounter's confirmed-species fields for a species-confirm undo/redo.

    Mutates ``enc`` in place; raises :class:`GroupingHistoryStale` when the
    encounter label has changed since the edit.
    """
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


@contextmanager
def restore_species_confirm_cache_edit(
    db: Database, entry: HistoryEntry, *, undo: bool,
    cache_restore: CacheRestore | None = None,
) -> Iterator[None]:
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
    When ``cache_restore`` is passed, the put-back callback is registered on
    it so the restoration scope extends through the caller's outer unit of
    work commit — a commit failure then rolls back the DB and puts the
    cache back together.
    """
    from pipeline import load_results_raw, save_results_raw
    from pipeline_locks import acquire_workspace_regroup

    lock = acquire_workspace_regroup(db.require_workspace_id())
    if not lock.acquire(blocking=False):
        raise GroupingHistoryConflict(
            'Photo groups are being updated. Try again when processing finishes.'
        )
    lock_transferred = False
    try:
        cache_dir = os.path.dirname(db._db_path)
        ws_id = db.require_workspace_id()
        current = load_results_raw(cache_dir, ws_id)
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
            _restore_burst_confirmation(enc, change, burst_photo_ids, undo=undo)
        else:
            _restore_encounter_confirmation(enc, change, undo=undo)
        save_results_raw(restored, cache_dir, ws_id)
        if cache_restore is not None:
            # Extend restoration through the caller's outer commit: the unit
            # of work rolls back the DB and then ``cache_restore.__exit__``
            # puts the cache back. Hand the regroup lock to ``cache_restore``
            # too, so no competing writer can slip in between the lock
            # release in our ``finally`` and the restoration; the lock stays
            # held through the whole restoration lifecycle.
            cache_restore.restore = lambda: save_results_raw(current, cache_dir, ws_id)
            cache_restore.add_cleanup(lock.release)
            lock_transferred = True
            yield
            return
        try:
            yield
        except Exception:
            if db.in_transaction:
                db.rollback()
            save_results_raw(current, cache_dir, ws_id)
            raise
    finally:
        if not lock_transferred:
            lock.release()


@contextmanager
def restore_grouping_edit(
    db: Database, entry: HistoryEntry, *, undo: bool,
    cache_restore: CacheRestore | None = None,
) -> Iterator[None]:
    """Restore structure, retaining history if the write/commit fails.

    Refuse a stale snapshot after recomputation or a later structural edit
    instead of silently erasing that work. A leftover cache-only species
    override or confirmation from an already-reverted species edit is not a
    conflict — undo is strict LIFO, so any species entry newer than this
    grouping entry has already been undone by the time we get here.

    When ``cache_restore`` is passed, the put-back callback is registered on
    it so the restoration scope extends through the caller's outer unit of
    work commit — a commit failure then rolls back the DB and puts the
    cache back together.
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
    lock_transferred = False
    try:
        cache_dir = os.path.dirname(db._db_path)
        ws_id = db.require_workspace_id()
        current = load_results_raw(cache_dir, ws_id)
        expected = change["after" if undo else "before"]
        target = change["before" if undo else "after"]
        include_confirmations = bool(change.get("label_edit"))
        current_signature = _grouping_signature(
            current.get("encounters") if current is not None else None, include_confirmations=include_confirmations,
        )
        if current is None or current_signature != _grouping_signature(expected, include_confirmations=include_confirmations):
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
        flags = {item["photo_id"]: item["old_value" if undo else "new_value"]
                 for item in change.get("flag_items", [])}
        for photo in restored.get("photos", []):
            if photo["id"] in flags:
                photo["flag"] = flags[photo["id"]]
        save_results_raw(restored, cache_dir, ws_id)
        if cache_restore is not None:
            # Extend restoration through the caller's outer commit: the unit
            # of work rolls back the DB and then ``cache_restore.__exit__``
            # puts the cache back. Hand the regroup lock to ``cache_restore``
            # too, so no competing writer can slip in between the lock
            # release in our ``finally`` and the restoration; the lock stays
            # held through the whole restoration lifecycle.
            cache_restore.restore = lambda: save_results_raw(current, cache_dir, ws_id)
            cache_restore.add_cleanup(lock.release)
            lock_transferred = True
            yield
            return
        try:
            yield
        except Exception:
            # Roll back any pending DB writes (typically the caller's
            # ``UPDATE edit_history SET undone = ...`` or its commit) before
            # putting the cache back — otherwise a later commit on this
            # connection could flush the undone flag without the matching
            # grouping state on disk and leave history desynchronized.
            if db.in_transaction:
                db.rollback()
            save_results_raw(current, cache_dir, ws_id)
            raise
    finally:
        if not lock_transferred:
            lock.release()


def apply_grouping_photo_edit(
    db: Database, entry: HistoryEntry, items: Iterable[sqlite3.Row], *, undo: bool,
) -> None:
    """Replay the photo half of a grouping action without releasing its transaction."""
    items = list(items)
    payload = json.loads(entry["new_value"])
    flag_items = payload.get("flag_items", []) if (payload.get("photo_edit") or {}).get("action_type") != "flag" else []
    for item in flag_items:
        value = item["old_value" if undo else "new_value"]
        db.photo_review.set_flag(item["photo_id"], value, _commit=False)
        db.queue_flag_change_if_enabled(item["photo_id"], value, _commit=False)
        db.photo_review.restore_flag_attribution_history(entry["id"], item["photo_id"], undo=undo)
    change = payload.get("photo_edit")
    if not change:
        return
    action = change["action_type"]
    for item in items:
        pid = item["photo_id"]
        if action == "flag":
            value = item["old_value" if undo else "new_value"]
            db.photo_review.set_flag(pid, value, _commit=False)
            db.queue_flag_change_if_enabled(pid, value, _commit=False)
            db.photo_review.restore_flag_attribution_history(entry["id"], pid, undo=undo)
            continue
        if action not in ("keyword_add", "species_replace"):
            raise ValueError("Unsupported photo edit in grouping history")
        old_meta = db._edit_old_value_meta(item["old_value"])
        if old_meta.get("no_tag"):
            continue
        old_ids = old_meta.get("keyword_ids") or []
        new_ids = [int(item["new_value"])] if item["new_value"] else []
        remove_ids, add_ids = (new_ids, old_ids) if undo else (old_ids, new_ids)
        for kid in remove_ids:
            db.untag_photo(pid, kid, _commit=False)
            name = db.edit_history.keyword_name(kid)
            if name and db.remove_pending_changes(pid, 'keyword_add', name, _commit=False) == 0:
                db.queue_change(pid, 'keyword_remove', name, _commit=False)
        for kid in add_ids:
            db.tag_photo(pid, kid, source='manual', _commit=False)
            name = db.edit_history.keyword_name(kid)
            if name and db.remove_pending_changes(pid, 'keyword_remove', name, _commit=False) == 0:
                db.queue_change(pid, 'keyword_add', name, _commit=False)
    db.species_provenance.replay_history(items, undo=undo)
