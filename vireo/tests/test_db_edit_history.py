"""Behavior pins for the edit-history domain of ``Database``.

The behavior tests exercise undo/redo history only through ``Database``
(public methods plus the private replay helpers, which stay on the façade),
so they hold regardless of whether the SQL lives in ``db.py`` or in
``repositories/edit_history.py``; the structural tests at the end keep it
in the repository. They cover recording and listing history rows, the
undo/redo cursor (non-undoable skipping, ordering, commit boundaries),
stale cache-linked retirement, the prediction-status replay, the
relabel-curation restore/re-apply, history pruning, and the pure old-value
parsers.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import pytest
from db import Database


@contextlib.contextmanager
def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def pids(db):
    fid = db.add_folder("/photos", name="photos")
    return [
        db.add_photo(fid, f"IMG_{i}.jpg", ".jpg", 100, 1700000000.0 + i)
        for i in range(3)
    ]


def _rating_edit(db, pid, old, new, desc=None):
    return db.record_edit(
        "rating", desc or f"rating {old}->{new}", str(new),
        [{"photo_id": pid, "old_value": str(old), "new_value": str(new)}],
    )


def _history_rows(db, where="1=1", params=()):
    with _reader(db) as conn:
        return [
            dict(r) for r in conn.execute(
                f"SELECT * FROM edit_history WHERE {where} ORDER BY id", params,
            ).fetchall()
        ]


def _raw_edit(db, action_type, new_value=None, *, workspace_id=None,
              undone=0, created_at=None, description="raw", items=()):
    ws = workspace_id if workspace_id is not None else db._active_workspace_id
    cur = db.conn.execute(
        "INSERT INTO edit_history (workspace_id, action_type, description, "
        "new_value, undone, created_at) VALUES (?, ?, ?, ?, ?, "
        "COALESCE(?, datetime('now')))",
        (ws, action_type, description, new_value, undone, created_at),
    )
    edit_id = cur.lastrowid
    for pid, old, new in items:
        db.conn.execute(
            "INSERT INTO edit_history_items (edit_id, photo_id, old_value, "
            "new_value) VALUES (?, ?, ?, ?)",
            (edit_id, pid, old, new),
        )
    db.conn.commit()
    return edit_id


# -- record_edit ----------------------------------------------------------


def test_record_edit_returns_id_stores_items_and_commits(db, pids):
    edit_id = db.record_edit(
        "rating", "Set rating", "5",
        [{"photo_id": pids[0], "old_value": "0", "new_value": "5"},
         {"photo_id": pids[1], "old_value": None, "new_value": "5"}],
        is_batch=True,
    )
    assert isinstance(edit_id, int)
    assert not db.conn.in_transaction
    with _reader(db) as conn:
        row = conn.execute(
            "SELECT * FROM edit_history WHERE id = ?", (edit_id,),
        ).fetchone()
        items = conn.execute(
            "SELECT photo_id, old_value, new_value FROM edit_history_items "
            "WHERE edit_id = ? ORDER BY id", (edit_id,),
        ).fetchall()
    assert row["workspace_id"] == db._active_workspace_id
    assert (row["action_type"], row["description"], row["new_value"]) == (
        "rating", "Set rating", "5",
    )
    assert row["is_batch"] == 1
    assert row["undone"] == 0
    assert [tuple(i) for i in items] == [
        (pids[0], "0", "5"), (pids[1], None, "5"),
    ]


def test_record_edit_is_batch_defaults_to_zero(db, pids):
    edit_id = _rating_edit(db, pids[0], 0, 1)
    assert _history_rows(db, "id = ?", (edit_id,))[0]["is_batch"] == 0


def test_record_edit_clears_only_this_workspaces_redo_stack(db, pids):
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    first = _rating_edit(db, pids[0], 0, 1)
    second = _rating_edit(db, pids[0], 1, 2)
    foreign_undone = _raw_edit(db, "rating", "3", workspace_id=other, undone=1)
    db.update_photo_rating(pids[0], 2)
    db.undo_last_edit()
    assert _history_rows(db, "id = ?", (second,))[0]["undone"] == 1

    third = _rating_edit(db, pids[0], 1, 4)

    ids = [r["id"] for r in _history_rows(db, "workspace_id = ?", (ws,))]
    assert ids == [first, third]
    assert _history_rows(db, "id = ?", (foreign_undone,))[0]["undone"] == 1


def test_record_edit_without_commit_skips_commit_and_prune(db, pids, monkeypatch):
    calls = []
    monkeypatch.setattr(
        db, "_prune_edit_history",
        lambda: calls.append(db.conn.in_transaction),
    )
    edit_id = db.record_edit(
        "rating", "uncommitted", "1",
        [{"photo_id": pids[0], "old_value": "0", "new_value": "1"}],
        _commit=False,
    )
    assert db.conn.in_transaction
    assert calls == []
    assert _history_rows(db, "id = ?", (edit_id,)) == []
    db.conn.commit()

    _rating_edit(db, pids[0], 1, 2)
    # Prune runs once, after the commit.
    assert calls == [False]


def test_record_edit_requires_active_workspace(db, pids):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        _rating_edit(db, pids[0], 0, 1)


# -- get_edit_history -----------------------------------------------------


def test_get_edit_history_filters_orders_counts_and_pages(db, pids):
    other = db.create_workspace("Other")
    same_ts = "2024-01-01 00:00:00"
    a = _raw_edit(db, "rating", "1", created_at=same_ts,
                  items=[(pids[0], "0", "1"), (pids[1], "0", "1")])
    b = _raw_edit(db, "flag", "flagged", created_at=same_ts)
    c = _raw_edit(db, "rating", "2", created_at="2024-01-02 00:00:00",
                  items=[(pids[0], "1", "2")])
    _raw_edit(db, "rating", "3", undone=1, created_at="2024-01-03 00:00:00")
    _raw_edit(db, "rating", "4", workspace_id=other)

    history = db.get_edit_history()
    assert isinstance(history, list)
    assert all(isinstance(e, dict) for e in history)
    assert [e["id"] for e in history] == [c, b, a]
    assert [e["item_count"] for e in history] == [1, 0, 2]
    assert history[0]["new_value"] == "2"

    assert [e["id"] for e in db.get_edit_history(limit=1, offset=1)] == [b]
    assert db.get_edit_history(limit=5, offset=3) == []


def test_get_edit_history_hides_grouping_payload(db, pids):
    _raw_edit(db, "pipeline_grouping", json.dumps({"photo_edit": None}))
    _raw_edit(db, "species_confirm_cache", '{"x": 1}')
    history = db.get_edit_history()
    by_type = {e["action_type"]: e["new_value"] for e in history}
    assert by_type == {"pipeline_grouping": None, "species_confirm_cache": '{"x": 1}'}


def test_get_edit_history_requires_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.get_edit_history()


# -- undo / redo cursor ---------------------------------------------------


def test_undo_returns_none_when_only_non_undoable_entries(db, pids):
    for action in Database._NON_UNDOABLE:
        _raw_edit(db, action, "x", items=[(pids[0], "a", "b")])
    assert db.undo_last_edit() is None
    assert db.redo_last_undo() is None
    assert all(r["undone"] == 0 for r in _history_rows(db))


def test_undo_skips_non_undoable_and_marks_entry_undone(db, pids):
    db.update_photo_rating(pids[0], 4)
    target = _rating_edit(db, pids[0], 2, 4, desc="the rating")
    for action in ("prediction_reject", "discard", "location_set",
                   "location_gps_review", "prediction_reviewed",
                   "prediction_replace_species"):
        _raw_edit(db, action, "x")

    entry = db.undo_last_edit()

    assert isinstance(entry, dict)
    assert entry["id"] == target
    assert entry["description"] == "the rating"
    assert entry["undone"] == 0  # the row as selected, before the flip
    assert not db.conn.in_transaction
    assert _history_rows(db, "id = ?", (target,))[0]["undone"] == 1
    assert db.get_photo(pids[0])["rating"] == 2


def test_undo_reads_non_undoable_from_instance(db, pids):
    _rating_edit(db, pids[0], 0, 1)
    db._NON_UNDOABLE = ("rating",)
    assert db.undo_last_edit() is None


def test_undo_picks_latest_and_redo_replays_oldest_first(db, pids):
    first = _raw_edit(db, "rating", "1", created_at="2024-01-01 00:00:00",
                      items=[(pids[0], "0", "1")])
    second = _raw_edit(db, "rating", "2", created_at="2024-01-02 00:00:00",
                       items=[(pids[0], "1", "2")])
    db.update_photo_rating(pids[0], 2)

    assert db.undo_last_edit()["id"] == second
    assert db.undo_last_edit()["id"] == first
    assert db.get_photo(pids[0])["rating"] == 0

    redone = db.redo_last_undo()
    assert redone["id"] == first
    assert not db.conn.in_transaction
    assert _history_rows(db, "id = ?", (first,))[0]["undone"] == 0
    assert db.get_photo(pids[0])["rating"] == 1
    assert db.redo_last_undo()["id"] == second
    assert db.get_photo(pids[0])["rating"] == 2
    assert db.redo_last_undo() is None


def test_undo_redo_ignore_other_workspaces(db, pids):
    other = db.create_workspace("Other")
    _raw_edit(db, "rating", "1", workspace_id=other, items=[(pids[0], "0", "1")])
    _raw_edit(db, "rating", "1", workspace_id=other, undone=1,
              items=[(pids[0], "0", "1")])
    assert db.undo_last_edit() is None
    assert db.redo_last_undo() is None


def test_undo_without_handler_still_advances_cursor(db, pids):
    edit_id = _raw_edit(db, "mystery_action", "x", items=[(pids[0], "a", "b")])
    assert db.undo_last_edit()["id"] == edit_id
    assert _history_rows(db, "id = ?", (edit_id,))[0]["undone"] == 1
    assert db.redo_last_undo()["id"] == edit_id
    assert _history_rows(db, "id = ?", (edit_id,))[0]["undone"] == 0


def test_undo_redo_require_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.undo_last_edit()
    with pytest.raises(RuntimeError):
        db.redo_last_undo()


def test_undo_and_redo_route_through_apply_hooks(db, pids, monkeypatch):
    edit_id = _raw_edit(db, "rating", "1", items=[(pids[0], "0", "1")])
    seen = []

    def fake_apply(kind):
        def apply(entry, items):
            seen.append((kind, entry["id"], [tuple(i) for i in items]))
        return apply

    monkeypatch.setattr(db, "_apply_undo", fake_apply("undo"))
    monkeypatch.setattr(db, "_apply_redo", fake_apply("redo"))
    db.undo_last_edit()
    db.redo_last_undo()
    item_row = _item_rows(db, edit_id)[0]
    assert seen == [("undo", edit_id, [item_row]), ("redo", edit_id, [item_row])]


def test_field_setters_route_through_facade(db, pids, monkeypatch):
    calls = []

    def recorder(name):
        return lambda *a, **k: calls.append((name, a, k))

    for name in ("update_photo_flag", "queue_flag_change_if_enabled",
                 "update_photo_wildlife_excluded", "set_color_label",
                 "remove_color_label", "set_photo_edit_recipe"):
        monkeypatch.setattr(db, name, recorder(name))
    pid = pids[0]
    for action, old, new in (
        ("flag", "none", "flagged"),
        ("wildlife_excluded", "0", "1"),
        ("color_label", "", "red"),
        ("edit_recipe", "", '{"exposure": 1}'),
    ):
        _raw_edit(db, action, new, items=[(pid, old, new)])

    for _ in range(4):
        db.undo_last_edit()
    undo_calls, calls[:] = list(calls), []
    for _ in range(4):
        db.redo_last_undo()

    assert undo_calls == [
        ("set_photo_edit_recipe", (pid, None), {"verify_workspace": False}),
        ("remove_color_label", (pid,), {}),
        ("update_photo_wildlife_excluded", (pid, False), {"verify_workspace": False}),
        ("update_photo_flag", (pid, "none"), {"verify_workspace": False}),
        ("queue_flag_change_if_enabled", (pid, "none"), {}),
    ]
    assert calls == [
        ("update_photo_flag", (pid, "flagged"), {"verify_workspace": False}),
        ("queue_flag_change_if_enabled", (pid, "flagged"), {}),
        ("update_photo_wildlife_excluded", (pid, True), {"verify_workspace": False}),
        ("set_color_label", (pid, "red"), {}),
        ("set_photo_edit_recipe", (pid, '{"exposure": 1}'), {"verify_workspace": False}),
    ]


def test_keyword_add_undo_redo_replays_prediction_and_curation(db, pids, monkeypatch):
    ws = db._active_workspace_id
    pid = pids[0]
    kid = db.add_keyword("Robin", is_species=True)
    db.tag_photo(pid, kid)
    p = _predictions(db, pid)
    _set_status(db, p["a_top"], "rejected")
    curation_calls = []
    monkeypatch.setattr(
        db, "_restore_relabel_curation",
        lambda *a: curation_calls.append(("restore",) + a),
    )
    monkeypatch.setattr(
        db, "_reapply_relabel_curation",
        lambda *a: curation_calls.append(("reapply",) + a),
    )
    curation = {"hl_prev": ["Old"]}
    old_value = json.dumps({
        "prediction_id": p["a_top"], "prediction_status": "alternative",
        "curation": curation,
    })
    db.record_edit(
        "keyword_add", "Added Robin", str(kid),
        [{"photo_id": pid, "old_value": old_value, "new_value": str(kid)}],
    )

    db.undo_last_edit()
    assert _status(db, p["a_top"]) == "alternative"
    assert not any(k["id"] == kid for k in db.get_photo_keywords(pid))
    db.redo_last_undo()
    assert _status(db, p["a_top"]) == "rejected"
    assert any(k["id"] == kid for k in db.get_photo_keywords(pid))
    assert curation_calls == [
        ("restore", ws, pid, "Robin", curation),
        ("reapply", ws, pid, "Robin", curation),
    ]


def test_keyword_remove_redo_untags_again(db, pids):
    kid = db.add_keyword("Hawk")
    db.record_edit(
        "keyword_remove", "Removed Hawk", str(kid),
        [{"photo_id": pids[0], "old_value": str(kid), "new_value": ""}],
    )
    db.undo_last_edit()
    assert any(k["id"] == kid for k in db.get_photo_keywords(pids[0]))
    db.redo_last_undo()
    assert not any(k["id"] == kid for k in db.get_photo_keywords(pids[0]))


def _item_rows(db, edit_id):
    with _reader(db) as conn:
        return [
            tuple(r) for r in conn.execute(
                "SELECT * FROM edit_history_items WHERE edit_id = ? ORDER BY id",
                (edit_id,),
            ).fetchall()
        ]


# -- cache-linked entries (pipeline_grouping / species_confirm_cache) -----


def _grouping_hooks(monkeypatch, *, stale=False, calls=None):
    import services.grouping_history as gh

    @contextlib.contextmanager
    def restore(db, entry, *, undo):
        if calls is not None:
            calls.append(("restore", entry["id"], undo))
        if stale:
            raise gh.GroupingHistoryStale("stale")
        yield

    def apply(db, entry, items, *, undo):
        if calls is not None:
            calls.append(("apply", entry["id"], len(items), undo,
                          db.conn.in_transaction))

    monkeypatch.setattr(gh, "restore_grouping_edit", restore)
    monkeypatch.setattr(gh, "restore_species_confirm_cache_edit", restore)
    monkeypatch.setattr(gh, "apply_grouping_photo_edit", apply)
    return gh


@pytest.mark.parametrize("direction", ["undo", "redo"])
def test_grouping_entry_success_flips_cursor_inside_restore(
    db, pids, monkeypatch, direction,
):
    calls = []
    _grouping_hooks(monkeypatch, calls=calls)
    undone = 0 if direction == "undo" else 1
    edit_id = _raw_edit(db, "pipeline_grouping", "{}", undone=undone,
                        items=[(pids[0], "a", "b")])
    method = db.undo_last_edit if direction == "undo" else db.redo_last_undo
    entry = method()
    assert entry["id"] == edit_id
    assert calls == [
        ("restore", edit_id, direction == "undo"),
        ("apply", edit_id, 1, direction == "undo", False),
    ]
    assert not db.conn.in_transaction
    assert _history_rows(db, "id = ?", (edit_id,))[0]["undone"] == 1 - undone


@pytest.mark.parametrize("direction", ["undo", "redo"])
def test_species_confirm_cache_success_flips_cursor(db, monkeypatch, direction):
    calls = []
    _grouping_hooks(monkeypatch, calls=calls)
    undone = 0 if direction == "undo" else 1
    edit_id = _raw_edit(db, "species_confirm_cache", "{}", undone=undone)
    method = db.undo_last_edit if direction == "undo" else db.redo_last_undo
    assert method()["id"] == edit_id
    assert calls == [("restore", edit_id, direction == "undo")]
    assert _history_rows(db, "id = ?", (edit_id,))[0]["undone"] == 1 - undone


_STALE_MSG = (
    "That grouping or species action was superseded by newer analysis. "
    "History has been refreshed; review the next action before trying again."
)


@pytest.mark.parametrize("direction", ["undo", "redo"])
def test_stale_grouping_with_photo_edit_keeps_photo_half(
    db, pids, monkeypatch, direction,
):
    gh = _grouping_hooks(monkeypatch, stale=True)
    undone = 0 if direction == "undo" else 1
    photo_edit = {"action_type": "flag"}
    edit_id = _raw_edit(
        db, "pipeline_grouping", json.dumps({"photo_edit": photo_edit, "x": 1}),
        undone=undone, description="Detach burst",
    )
    db.conn.execute(
        "INSERT INTO edit_history_payloads (edit_id, payload) VALUES (?, ?)",
        (edit_id, "{}"),
    )
    db.conn.commit()
    method = db.undo_last_edit if direction == "undo" else db.redo_last_undo

    with pytest.raises(gh.GroupingHistoryStale) as info:
        method()

    assert str(info.value) == _STALE_MSG
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__
    assert not db.conn.in_transaction
    row = _history_rows(db, "id = ?", (edit_id,))[0]
    assert json.loads(row["new_value"]) == {"photo_edit": photo_edit, "photo_only": True}
    assert row["description"] == "Photo changes from: Detach burst"
    assert row["undone"] == undone
    with _reader(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM edit_history_payloads WHERE edit_id = ?",
            (edit_id,),
        ).fetchone()[0] == 0


@pytest.mark.parametrize("direction", ["undo", "redo"])
@pytest.mark.parametrize("action,new_value", [
    ("pipeline_grouping", json.dumps({"before": [], "after": []})),
    ("species_confirm_cache", json.dumps({"photo_edit": {"a": 1}})),
])
def test_stale_cache_entry_without_photo_half_is_deleted(
    db, monkeypatch, direction, action, new_value,
):
    gh = _grouping_hooks(monkeypatch, stale=True)
    undone = 0 if direction == "undo" else 1
    edit_id = _raw_edit(db, action, new_value, undone=undone)
    method = db.undo_last_edit if direction == "undo" else db.redo_last_undo
    with pytest.raises(gh.GroupingHistoryStale, match="superseded"):
        method()
    assert not db.conn.in_transaction
    assert _history_rows(db, "id = ?", (edit_id,)) == []


def test_retire_stale_grouping_entry_on_missing_row_is_noop(db):
    keep = _raw_edit(db, "rating", "1")
    db._retire_stale_grouping_entry(keep + 100)
    assert [r["id"] for r in _history_rows(db)] == [keep]


def test_retire_stale_grouping_entry_does_not_commit(db):
    edit_id = _raw_edit(db, "pipeline_grouping", json.dumps({"photo_edit": None}))
    db._retire_stale_grouping_entry(edit_id)
    assert db.conn.in_transaction
    assert len(_history_rows(db, "id = ?", (edit_id,))) == 1
    db.conn.rollback()


# -- keyword helpers ------------------------------------------------------


def test_keyword_name_and_prediction_scope(db, pids):
    kid = db.add_keyword("Heron")
    assert db._keyword_name(kid) == "Heron"
    assert db._keyword_name(kid + 1000) is None

    preds = _predictions(db, pids[0])
    assert db._prediction_scope(preds["a_top"]) == (preds["det"], "m1", "fpA")
    assert db._prediction_scope(999999) is None


def test_undo_keyword_remove_of_deleted_keyword_is_noop(db, pids):
    edit_id = db.record_edit(
        "keyword_remove", "Removed", "987654",
        [{"photo_id": pids[0], "old_value": "987654", "new_value": ""}],
    )
    assert db.undo_last_edit()["id"] == edit_id
    assert db.get_photo_keywords(pids[0]) == []


def _pending(db, pid):
    with _reader(db) as conn:
        return sorted(
            (r["change_type"], r["value"], r["workspace_id"])
            for r in conn.execute(
                "SELECT change_type, value, workspace_id FROM pending_changes "
                "WHERE photo_id = ?", (pid,),
            ).fetchall()
        )


def test_prediction_accept_flat_removals_restore_only_live_workspaces(db, pids):
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    gone = db.create_workspace("Gone")
    db.set_active_workspace(ws)
    kid = db.add_keyword("Hawk")
    db.tag_photo(pids[0], kid)
    old_value = json.dumps({
        "keyword_only": True,
        "flat_removals": [
            {"workspace_id": other, "value": "Hawk"},
            {"workspace_id": gone, "value": "Hawk"},
        ],
    })
    db.record_edit(
        "prediction_accept", "Accepted", str(kid),
        [{"photo_id": pids[0], "old_value": old_value, "new_value": str(kid)}],
    )
    db.delete_workspace(gone)
    db.set_active_workspace(ws)

    db.undo_last_edit()
    assert ("keyword_remove_flat", "Hawk", other) in _pending(db, pids[0])
    assert all(p[2] != gone for p in _pending(db, pids[0]))

    db.redo_last_undo()
    assert ("keyword_remove_flat", "Hawk", other) not in _pending(db, pids[0])


# -- prediction status replay ---------------------------------------------


def _predictions(db, pid):
    """One detection with two fpA siblings under m1, one fpB and one m2 row."""
    det = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")[0]
    rows = {}
    for key, model, fp, species, conf in (
        ("a_top", "m1", "fpA", "Robin", 0.9),
        ("a_low", "m1", "fpA", "Wren", 0.4),
        ("a_mid", "m1", "fpA", "Finch", 0.6),
        ("b_top", "m1", "fpB", "Robin", 0.8),
        ("m2_top", "m2", "fpA", "Robin", 0.7),
    ):
        rows[key] = db.conn.execute(
            "INSERT INTO predictions (detection_id, classifier_model, "
            "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
            (det, model, fp, species, conf),
        ).lastrowid
    db.conn.commit()
    rows["det"] = det
    return rows


def _status(db, pred_id, ws=None):
    ws = ws if ws is not None else db._active_workspace_id
    with _reader(db) as conn:
        row = conn.execute(
            "SELECT status FROM prediction_review WHERE prediction_id = ? "
            "AND workspace_id = ?", (pred_id, ws),
        ).fetchone()
    return row["status"] if row else None


def _set_status(db, pred_id, status, ws=None):
    ws = ws if ws is not None else db._active_workspace_id
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, ?) ON CONFLICT(prediction_id, workspace_id) "
        "DO UPDATE SET status = excluded.status",
        (pred_id, ws, status),
    )
    db.conn.commit()


def test_undo_prediction_accept_resets_only_matching_fingerprint(db, pids):
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    db.set_active_workspace(ws)
    p = _predictions(db, pids[0])
    _set_status(db, p["a_mid"], "accepted")
    _set_status(db, p["a_low"], "rejected")
    _set_status(db, p["a_top"], "rejected")
    _set_status(db, p["b_top"], "accepted")
    _set_status(db, p["a_mid"], "accepted", ws=other)

    db._undo_prediction_accept_statuses({}, str(p["a_mid"]))

    assert not db.conn.in_transaction
    assert _status(db, p["a_top"]) == "pending"  # highest confidence sibling
    assert _status(db, p["a_mid"]) == "alternative"
    assert _status(db, p["a_low"]) == "alternative"
    assert _status(db, p["b_top"]) == "accepted"
    assert _status(db, p["m2_top"]) is None
    assert _status(db, p["a_mid"], ws=other) == "accepted"


def test_undo_prediction_accept_dedupes_scopes(db, pids):
    p = _predictions(db, pids[0])
    meta = {"prediction_id": p["a_mid"],
            "prediction_ids": [p["a_mid"], p["a_low"], p["m2_top"], "junk", None]}
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db._undo_prediction_accept_statuses(meta, None)
    finally:
        db.conn.set_trace_callback(None)
    updates = [s for s in statements if s.strip().startswith("UPDATE prediction_review")]
    assert len(updates) == 2  # (det, m1, fpA) once, (det, m2, fpA) once
    assert _status(db, p["a_top"]) == "pending"
    assert _status(db, p["m2_top"]) == "pending"


def test_undo_prediction_accept_commits_only_when_a_scope_matched(db, pids):
    db.conn.execute("UPDATE photos SET rating = 3 WHERE id = ?", (pids[0],))
    db._undo_prediction_accept_statuses({"prediction_ids": [999999]}, None)
    assert db.conn.in_transaction
    db.conn.rollback()


def test_undo_prediction_accept_empty_ids_return_before_workspace(db):
    db.set_active_workspace(None)
    assert db._undo_prediction_accept_statuses({}, None) is None
    assert db._undo_prediction_accept_statuses({"prediction_ids": ["x"]}, "") is None
    with pytest.raises(RuntimeError):
        db._undo_prediction_accept_statuses({}, "5")


def test_redo_prediction_accept_reaccepts_and_rejects_open_siblings(db, pids):
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    db.set_active_workspace(ws)
    p = _predictions(db, pids[0])
    _set_status(db, p["a_top"], "alternative")
    _set_status(db, p["a_low"], "accepted")  # already decided: left alone
    _set_status(db, p["b_top"], "pending")

    meta = {"prediction_ids": [p["a_mid"], 999999]}
    db._redo_prediction_accept_statuses(meta, None)

    assert not db.conn.in_transaction
    assert _status(db, p["a_mid"]) == "accepted"
    assert _status(db, p["a_top"]) == "rejected"
    assert _status(db, p["a_low"]) == "accepted"
    assert _status(db, p["b_top"]) == "pending"
    assert _status(db, p["m2_top"]) is None
    assert _status(db, p["a_top"], ws=other) is None


def test_redo_prediction_accept_excludes_every_batch_id(db, pids):
    p = _predictions(db, pids[0])
    db._redo_prediction_accept_statuses(
        {"prediction_ids": [p["a_mid"], p["a_top"]]}, None,
    )
    assert _status(db, p["a_mid"]) == "accepted"
    assert _status(db, p["a_top"]) == "accepted"
    assert _status(db, p["a_low"]) == "rejected"


def test_redo_prediction_accept_routes_through_update_prediction_status(
    db, pids, monkeypatch,
):
    p = _predictions(db, pids[0])
    calls = []
    real = db.update_prediction_status

    def spy(pred_id, status, *args, **kwargs):
        calls.append((pred_id, status, args, kwargs))
        return real(pred_id, status, *args, **kwargs)

    monkeypatch.setattr(db, "update_prediction_status", spy)
    db._redo_prediction_accept_statuses({}, str(p["a_low"]))
    assert calls == [(p["a_low"], "accepted", (), {})]


def test_redo_prediction_accept_missing_scope_does_not_commit(db, pids):
    db.conn.execute("UPDATE photos SET rating = 3 WHERE id = ?", (pids[0],))
    db._redo_prediction_accept_statuses({"prediction_id": 999999}, None)
    assert db.conn.in_transaction
    db.conn.rollback()


def test_redo_prediction_accept_resolves_workspace_before_status_writes(
    db, pids, monkeypatch,
):
    p = _predictions(db, pids[0])
    calls = []
    monkeypatch.setattr(
        db, "update_prediction_status", lambda *a, **k: calls.append(a),
    )
    db.set_active_workspace(None)
    assert db._redo_prediction_accept_statuses({}, None) is None
    with pytest.raises(RuntimeError):
        db._redo_prediction_accept_statuses({}, str(p["a_low"]))
    assert calls == []


def test_prediction_accept_undo_redo_round_trip(db, pids):
    p = _predictions(db, pids[0])
    kid = db.add_keyword("Finch", is_species=True)
    db.tag_photo(pids[0], kid)
    _set_status(db, p["a_mid"], "accepted")
    _set_status(db, p["a_top"], "rejected")
    _set_status(db, p["a_low"], "rejected")
    db.record_edit(
        "prediction_accept", "Accepted Finch", str(kid),
        [{"photo_id": pids[0], "old_value": str(p["a_mid"]), "new_value": str(kid)}],
    )
    db.undo_last_edit()
    assert _status(db, p["a_top"]) == "pending"
    assert _status(db, p["a_mid"]) == "alternative"
    assert not any(k["id"] == kid for k in db.get_photo_keywords(pids[0]))
    db.redo_last_undo()
    assert _status(db, p["a_mid"]) == "accepted"
    assert _status(db, p["a_top"]) == "rejected"
    assert any(k["id"] == kid for k in db.get_photo_keywords(pids[0]))


# -- relabel curation -----------------------------------------------------


def _hl(db, ws, species, pid, rank):
    db.conn.execute(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank) "
        "VALUES (?, ?, ?, ?)", (ws, species, pid, rank),
    )


def _pref(db, ws, purpose, species, pid):
    db.conn.execute(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) "
        "VALUES (?, ?, ?, ?)", (ws, purpose, species, pid),
    )


def _rep(db, species, pid, order):
    db.conn.execute(
        "INSERT INTO species_representatives (species, photo_id, selected_order) "
        "VALUES (?, ?, ?)", (species, pid, order),
    )


def _curation_state(db, ws):
    with _reader(db) as conn:
        hl = sorted(tuple(r) for r in conn.execute(
            "SELECT species, photo_id, rank FROM species_highlights "
            "WHERE workspace_id = ?", (ws,)))
        pref = sorted(tuple(r) for r in conn.execute(
            "SELECT purpose, species, photo_id FROM photo_preferences "
            "WHERE workspace_id = ?", (ws,)))
        rep = sorted(tuple(r) for r in conn.execute(
            "SELECT species, photo_id, selected_order FROM species_representatives"))
    return hl, pref, rep


@pytest.mark.parametrize("curation", [None, {}])
def test_restore_and_reapply_curation_noop_without_payload(db, pids, curation):
    ws = db._active_workspace_id
    db._restore_relabel_curation(ws, pids[0], "New", curation)
    db._reapply_relabel_curation(ws, pids[0], "New", curation)
    assert _curation_state(db, ws) == ([], [], [])
    assert not db.conn.in_transaction


def test_restore_relabel_curation_highlights(db, pids):
    ws = db._active_workspace_id
    p0, p1, p2 = pids
    _hl(db, ws, "New", p0, 1)
    _hl(db, ws, "Old3", p1, 7)      # bucket for the legacy string entry
    _hl(db, ws, "Old5", p0, 2)      # photo already in the old bucket
    _hl(db, ws, "Old6", p2, 4)
    db.conn.commit()
    curation = {"hl_prev": [
        {"species": "Old1", "rank": 3, "dst_existed": False},
        {"species": "Old2", "rank": "bad"},
        "Old3",
        "",
        "New",
        {"species": "New"},
        {"species": "Old5", "rank": 9},
        {"species": "Old6", "rank": None, "dst_existed": True},
    ]}

    db._restore_relabel_curation(ws, p0, "New", curation)

    # No commit of its own: the undo caller commits.
    assert db.conn.in_transaction
    db.conn.commit()
    hl, _, _ = _curation_state(db, ws)
    assert hl == sorted([
        ("Old1", p0, 3),
        ("Old2", p0, 1),       # non-int rank falls back to MAX+1 of empty
        ("Old3", p0, 8),       # legacy string appends after rank 7
        ("Old3", p1, 7),
        ("Old5", p0, 2),       # already present: untouched
        ("Old6", p0, 5),       # MAX+1 after p2's rank 4
        ("Old6", p2, 4),
    ])


def test_restore_relabel_curation_keeps_preexisting_destination(db, pids):
    ws = db._active_workspace_id
    _hl(db, ws, "New", pids[0], 1)
    db.conn.commit()
    db._restore_relabel_curation(
        ws, pids[0], "New",
        {"hl_prev": [{"species": "Old", "rank": 2, "dst_existed": True}]},
    )
    db.conn.commit()
    hl, _, _ = _curation_state(db, ws)
    assert hl == [("New", pids[0], 1), ("Old", pids[0], 2)]


def test_restore_relabel_curation_preferences_and_reps(db, pids, monkeypatch):
    ws = db._active_workspace_id
    p0, p1 = pids[0], pids[1]
    _pref(db, ws, "highlights", "New", p0)
    _pref(db, ws, "life_list", "New", p1)          # someone else's slot
    _rep(db, "New", p0, 5)
    _rep(db, "RepNew", p1, 1)
    db.conn.commit()
    restored = []
    real = db._restore_species_representative

    def spy(species, photo_id, selected_order=None):
        restored.append((species, photo_id, selected_order))
        return real(species, photo_id, selected_order=selected_order)

    monkeypatch.setattr(db, "_restore_species_representative", spy)
    curation = {
        "pref_prev": [
            "junk",
            {"purpose": "", "species": "OldX"},
            {"purpose": "highlights", "species": "New"},
            {"purpose": "highlights", "species": "OldA",
             "dst_existed": False, "rep_dst_existed": False,
             "rep_selected_order": 2},
            {"purpose": "life_list", "species": "OldB",
             "dst_existed": True, "rep_dst_existed": True},
        ],
        "rep_prev": [
            7,
            {"species": ""},
            {"species": "New"},
            {"species": "OldC", "dst_existed": True, "selected_order": "3"},
            {"species": "OldD", "dst_existed": False, "selected_order": 4},
        ],
    }

    db._restore_relabel_curation(ws, p0, "New", curation)
    db.conn.commit()

    _, pref, rep = _curation_state(db, ws)
    assert pref == sorted([
        ("highlights", "OldA", p0),
        ("life_list", "New", p1),
        ("life_list", "OldB", p0),
    ])
    rep_by_key = {(s, pid): order for s, pid, order in rep}
    assert ("New", p0) not in rep_by_key
    assert rep_by_key[("OldA", p0)] == 2
    assert rep_by_key[("OldC", p0)] == 3
    assert rep_by_key[("OldD", p0)] == 4
    assert ("OldB", p0) in rep_by_key          # legacy: fresh order
    assert rep_by_key[("RepNew", p1)] == 1
    assert restored == [
        ("OldA", p0, 2), ("OldB", p0, None), ("OldC", p0, "3"), ("OldD", p0, 4),
    ]


def test_reapply_relabel_curation_highlights(db, pids):
    ws = db._active_workspace_id
    p0, p1 = pids[0], pids[1]
    _hl(db, ws, "Old1", p0, 5)
    _hl(db, ws, "New", p1, 3)
    _hl(db, ws, "Old2", p0, 1)
    _hl(db, ws, "Old3", p0, 1)
    db.conn.commit()
    curation = {"hl_prev": [
        {"species": "Old1"},
        "Old2",
        "",
        {"species": "New"},
        "Missing",
    ]}

    db._reapply_relabel_curation(ws, p0, "New", curation)
    assert db.conn.in_transaction
    db.conn.commit()
    hl, _, _ = _curation_state(db, ws)
    assert hl == sorted([
        ("New", p0, 4),        # MAX(3)+1; the second move finds it and skips
        ("New", p1, 3),
        ("Old3", p0, 1),
    ])


def test_reapply_relabel_curation_preferences_and_reps(db, pids, monkeypatch):
    ws = db._active_workspace_id
    p0 = pids[0]
    _pref(db, ws, "highlights", "OldA", p0)
    _rep(db, "OldA", p0, 2)
    _rep(db, "OldC", p0, 6)
    db.conn.commit()
    restored = []
    real = db._restore_species_representative

    def spy(species, photo_id, selected_order=None):
        restored.append((species, photo_id, selected_order))
        return real(species, photo_id, selected_order=selected_order)

    monkeypatch.setattr(db, "_restore_species_representative", spy)
    curation = {
        "pref_prev": [
            None,
            {"purpose": "highlights"},
            {"purpose": "highlights", "species": "New"},
            {"purpose": "life_list", "species": "OldA"},   # no source row
            {"purpose": "highlights", "species": "OldA", "rep_selected_order": 9},
        ],
        "rep_prev": [
            "junk",
            {"species": None},
            {"species": "New"},
            {"species": "OldMissing", "selected_order": 1},
            {"species": "OldC", "selected_order": 6},
        ],
    }

    db._reapply_relabel_curation(ws, p0, "New", curation)
    db.conn.commit()

    _, pref, rep = _curation_state(db, ws)
    assert pref == [("highlights", "New", p0)]
    assert rep == [("New", p0, 6)]  # 9 first, then overwritten by OldC's 6
    assert restored == [("New", p0, 9), ("New", p0, 6)]


def test_species_replace_undo_redo_moves_curation(db, pids):
    ws = db._active_workspace_id
    p0 = pids[0]
    old_kid = db.add_keyword("Old Bird", is_species=True)
    new_kid = db.add_keyword("New Bird", is_species=True)
    db.tag_photo(p0, new_kid)
    _hl(db, ws, "New Bird", p0, 1)
    db.conn.commit()
    old_value = json.dumps({
        "keyword_ids": [old_kid],
        "curation": {"hl_prev": [{"species": "Old Bird", "rank": 1}]},
    })
    db.record_edit(
        "species_replace", "Relabel", str(new_kid),
        [{"photo_id": p0, "old_value": old_value, "new_value": str(new_kid)}],
    )

    db.undo_last_edit()
    hl, _, _ = _curation_state(db, ws)
    assert hl == [("Old Bird", p0, 1)]
    db.redo_last_undo()
    hl, _, _ = _curation_state(db, ws)
    assert hl == [("New Bird", p0, 1)]


# -- pruning --------------------------------------------------------------


@pytest.fixture
def max_history(monkeypatch):
    import config as cfg

    real_get = cfg.get
    box = {"value": 2}

    def fake_get(key):
        if key == "max_edit_history":
            return box["value"]
        return real_get(key)

    monkeypatch.setattr(cfg, "get", fake_get)
    return box


def test_prune_keeps_newest_and_undone_and_other_workspaces(db, pids, max_history):
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    db.set_active_workspace(ws)
    db.set_meta(Database._RETIRED_WILDLIFE_GENRE_KEY, "1")
    old1 = _raw_edit(db, "rating", "1", created_at="2024-01-01 00:00:00")
    old2 = _raw_edit(db, "rating", "2", created_at="2024-01-02 00:00:00")
    undone = _raw_edit(db, "rating", "3", undone=1, created_at="2024-01-01 00:00:00")
    new1 = _raw_edit(db, "rating", "4", created_at="2024-01-03 00:00:00")
    new2 = _raw_edit(db, "rating", "5", created_at="2024-01-03 00:00:00")
    foreign = _raw_edit(db, "rating", "6", workspace_id=other,
                        created_at="2020-01-01 00:00:00")

    db._prune_edit_history()

    assert not db.conn.in_transaction
    remaining = {r["id"] for r in _history_rows(db)}
    assert remaining == {undone, new1, new2, foreign}
    assert old1 not in remaining and old2 not in remaining


def test_prune_defaults_to_1000_when_unset(db, max_history):
    db.set_meta(Database._RETIRED_WILDLIFE_GENRE_KEY, "1")
    max_history["value"] = 0
    ids = [_raw_edit(db, "rating", str(i)) for i in range(3)]
    db._prune_edit_history()
    assert [r["id"] for r in _history_rows(db)] == ids


def test_prune_protects_wildlife_discard_until_retired(db, pids, max_history):
    max_history["value"] = 1
    db.set_meta(Database._RETIRED_WILDLIFE_GENRE_KEY, "0")
    protected = _raw_edit(db, "discard", None, created_at="2024-01-01 00:00:00",
                          items=[(pids[0], "keyword_add:WILDLIFE", None)])
    plain_discard = _raw_edit(db, "discard", None,
                              created_at="2024-01-01 00:00:01",
                              items=[(pids[0], "keyword_add:Hawk", None)])
    rating_wild = _raw_edit(db, "rating", None, created_at="2024-01-01 00:00:02",
                            items=[(pids[0], "keyword_add:Wildlife", None)])
    newest = _raw_edit(db, "rating", "1", created_at="2024-01-02 00:00:00")

    db._prune_edit_history()
    remaining = {r["id"] for r in _history_rows(db)}
    assert remaining == {protected, newest}
    assert plain_discard not in remaining and rating_wild not in remaining

    db.set_meta(Database._RETIRED_WILDLIFE_GENRE_KEY, "1")
    db._prune_edit_history()
    assert {r["id"] for r in _history_rows(db)} == {newest}


def test_prune_routes_through_get_meta(db, max_history, monkeypatch):
    keys = []
    monkeypatch.setattr(db, "get_meta", lambda key: keys.append(key) or "1")
    db._prune_edit_history()
    assert keys == [Database._RETIRED_WILDLIFE_GENRE_KEY]


def test_prune_requires_active_workspace(db, max_history):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db._prune_edit_history()


def test_record_edit_prunes_after_commit(db, pids, max_history):
    db.set_meta(Database._RETIRED_WILDLIFE_GENRE_KEY, "1")
    max_history["value"] = 1
    _raw_edit(db, "rating", "1", created_at="2020-01-01 00:00:00")
    newest = _rating_edit(db, pids[0], 0, 1)
    assert [r["id"] for r in _history_rows(db)] == [newest]


# -- pure parsers ---------------------------------------------------------


@pytest.mark.parametrize("raw,expected", [
    (None, {"keyword_id": None, "keyword_ids": []}),
    ("", {"keyword_id": None, "keyword_ids": []}),
    ("7", {"keyword_id": 7, "keyword_ids": [7]}),
    ("abc", {"keyword_id": None, "keyword_ids": []}),
    ("{", {"keyword_id": None}),
    ("  {bad json", {"keyword_id": None}),
    ('{"keyword_id": "x"}', {"keyword_id": None, "keyword_ids": []}),
    ('{"keyword_id": [1]}', {"keyword_id": None, "keyword_ids": []}),
    ('{"keyword_id": "4"}', {"keyword_id": 4, "keyword_ids": [4]}),
    ('{"keyword_id": 0}', {"keyword_id": None, "keyword_ids": []}),
    ('{"keyword_ids": ["1", "x", 2, null, [3]]}',
     {"keyword_id": None, "keyword_ids": [1, 2]}),
    ('{"keyword_id": 5, "keyword_ids": "nope", "no_tag": true}',
     {"keyword_id": 5, "keyword_ids": [5], "no_tag": True}),
])
def test_edit_old_value_meta(db, raw, expected):
    assert db._edit_old_value_meta(raw) == expected


def test_edit_old_value_meta_non_string_int(db):
    assert db._edit_old_value_meta(12) == {"keyword_id": 12, "keyword_ids": [12]}


@pytest.mark.parametrize("meta,fallback,ids", [
    (None, "3", [3]),
    ({}, None, []),
    ({"prediction_id": "2", "prediction_ids": [2, "5", "x", None, 5]}, "9", [2, 5]),
    ({"prediction_ids": "not-a-list"}, 4, [4]),
    ({"prediction_id": "bad"}, "bad", []),
])
def test_edit_prediction_ids(db, meta, fallback, ids):
    assert db._edit_prediction_ids(meta, fallback) == ids
    assert db._edit_prediction_id(meta, fallback) == (ids[0] if ids else None)


# -- structure ------------------------------------------------------------

_DELEGATING = (
    "record_edit", "get_edit_history", "undo_last_edit", "redo_last_undo",
    "_retire_stale_grouping_entry", "_keyword_name", "_prediction_scope",
    "_undo_keyword_add", "_undo_prediction_accept_statuses",
    "_redo_prediction_accept_statuses", "_restore_relabel_curation",
    "_reapply_relabel_curation", "_prune_edit_history",
)

_DOMAIN = _DELEGATING + (
    "_apply_undo", "_apply_redo", "_apply_edit_items", "_edit_set_flag",
    "_edit_set_wildlife_excluded", "_edit_set_color_label",
    "_edit_set_edit_recipe", "_undo_rating", "_redo_rating",
    "_flip_pending_keyword_change", "_retag_for_edit", "_untag_for_edit",
    "_undo_keyword_remove", "_redo_keyword_remove", "_redo_keyword_add",
    "_undo_species_replace", "_redo_species_replace", "_edit_old_value_meta",
    "_edit_prediction_ids", "_edit_prediction_id",
    "_restore_edit_prediction_status", "_reject_edit_prediction",
)


def _self_attrs(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    return fn, {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


@pytest.mark.parametrize("name", _DELEGATING)
def test_edit_history_method_delegates_to_repository(name):
    _, attrs = _self_attrs(name)
    assert "_edit_history_repository" in attrs, (
        f"Database.{name} no longer delegates to EditHistoryRepository"
    )


@pytest.mark.parametrize("name", _DOMAIN)
def test_edit_history_method_has_no_sql(name):
    _, attrs = _self_attrs(name)
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to EditHistoryRepository"
    )


@pytest.mark.parametrize("name,callback", [
    ("_undo_prediction_accept_statuses", "_prediction_scope"),
    ("_restore_relabel_curation", "_restore_species_representative"),
    ("_reapply_relabel_curation", "_restore_species_representative"),
])
def test_mid_statement_facade_calls_are_passed_as_bound_callbacks(name, callback):
    """The repository calls these façade methods mid-loop; pass them bound."""
    fn, _ = _self_attrs(name)
    passed = {
        arg.attr
        for call in ast.walk(fn)
        if isinstance(call, ast.Call)
        for arg in [*call.args, *(kw.value for kw in call.keywords)]
        if isinstance(arg, ast.Attribute)
        and isinstance(arg.value, ast.Name)
        and arg.value.id == "self"
    }
    assert callback in passed


def test_undo_prediction_accept_scope_lookup_is_patchable(db, pids, monkeypatch):
    p = _predictions(db, pids[0])
    seen = []
    real = db._prediction_scope
    monkeypatch.setattr(
        db, "_prediction_scope", lambda pid: seen.append(pid) or real(pid),
    )
    db._undo_prediction_accept_statuses({"prediction_ids": [p["a_mid"]]}, None)
    assert seen == [p["a_mid"]]


def test_edit_history_facade_signatures_are_unchanged():
    def params(name):
        return [
            (p.name, p.default)
            for p in inspect.signature(getattr(Database, name)).parameters.values()
        ]

    empty = inspect.Parameter.empty
    assert params("record_edit") == [
        ("self", empty), ("action_type", empty), ("description", empty),
        ("new_value", empty), ("items", empty), ("is_batch", False),
        ("_commit", True),
    ]
    assert params("get_edit_history") == [
        ("self", empty), ("limit", 50), ("offset", 0),
    ]
    assert params("undo_last_edit") == [("self", empty)]
    assert params("redo_last_undo") == [("self", empty)]
    assert params("_restore_relabel_curation") == [
        ("self", empty), ("workspace_id", empty), ("photo_id", empty),
        ("new_species", empty), ("curation", empty),
    ]
    assert params("_prune_edit_history") == [("self", empty)]
