# vireo/tests/test_saved_processes.py
"""Data-layer tests for user-editable saved processes."""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _db(tmp_path):
    from db import Database
    return Database(str(tmp_path / "test.db"))


def test_seeds_inserted_on_first_init(tmp_path):
    import process_strategies as ps
    db = _db(tmp_path)
    procs = db.processes.list_all()
    names = [p["name"] for p in procs]
    assert names == [s["name"] for s in ps.SEED_PROCESSES]
    assert all(p["is_seed"] for p in procs)
    # sort_order preserves the seed ordering.
    assert [p["sort_order"] for p in procs] == list(range(len(procs)))


def test_identify_seed_carries_species_review_and_no_misses(tmp_path):
    db = _db(tmp_path)
    identify = next(
        p for p in db.processes.list_all() if p["name"] == "Identify birds"
    )
    assert identify["skip_extract_masks"] is True
    assert identify["skip_eye_keypoints"] is True
    assert identify["skip_regroup"] is True
    assert identify["miss_enabled"] is False
    assert identify["review_mode"] == "species"


def test_full_seed_runs_everything(tmp_path):
    db = _db(tmp_path)
    full = next(p for p in db.processes.list_all() if p["name"] == "Full")
    assert full["skip_classify"] is False
    assert full["skip_extract_masks"] is False
    assert full["skip_eye_keypoints"] is False
    assert full["skip_regroup"] is False
    assert full["miss_enabled"] is True
    assert full["review_mode"] is None


def test_resolve_process_round_trips_all_six_fields(tmp_path):
    db = _db(tmp_path)
    identify = next(
        p for p in db.processes.list_all() if p["name"] == "Identify birds"
    )
    flags = db.resolve_process(identify["id"])
    assert flags == {
        "skip_classify": False,
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "skip_regroup": True,
        "miss_enabled": False,
        "review_mode": "species",
    }


def test_resolve_process_unknown_id_raises(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.resolve_process(99999)


def test_create_and_get_saved_process(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create(
        "My combo", skip_extract_masks=True, miss_enabled=False,
        review_mode="species",
    )
    proc = db.processes.get(pid)
    assert proc["name"] == "My combo"
    assert proc["skip_extract_masks"] is True
    assert proc["miss_enabled"] is False
    assert proc["review_mode"] == "species"
    assert proc["is_seed"] is False


def test_create_duplicate_name_rejected(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.processes.create("Identify birds")


def test_create_blank_name_rejected(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.processes.create("   ")


def test_create_bad_review_mode_rejected(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.processes.create("Bad", review_mode="whatever")


def test_update_saved_process_rename_and_flags(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create("Temp")
    assert db.update_saved_process(
        pid, name="Renamed", skip_classify=True, review_mode="species",
    )
    proc = db.processes.get(pid)
    assert proc["name"] == "Renamed"
    assert proc["skip_classify"] is True
    assert proc["review_mode"] == "species"


def test_update_partial_leaves_other_fields(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create(
        "Base", skip_regroup=True, miss_enabled=False, review_mode="species",
    )
    db.update_saved_process(pid, name="Base2")
    proc = db.processes.get(pid)
    assert proc["name"] == "Base2"
    assert proc["skip_regroup"] is True
    assert proc["miss_enabled"] is False
    assert proc["review_mode"] == "species"


def test_update_can_clear_review_mode(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create("HasReview", review_mode="species")
    db.update_saved_process(pid, review_mode=None)
    assert db.processes.get(pid)["review_mode"] is None


def test_update_missing_returns_false(tmp_path):
    db = _db(tmp_path)
    assert db.update_saved_process(99999, name="x") is False


def test_update_duplicate_name_rejected(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create("Unique")
    with pytest.raises(ValueError):
        db.update_saved_process(pid, name="Full")


def test_delete_saved_process(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create("Doomed")
    assert db.delete_saved_process(pid) is True
    assert db.processes.get(pid) is None
    assert db.delete_saved_process(pid) is False


def test_delete_nulls_referencing_workspace_default(tmp_path):
    db = _db(tmp_path)
    pid = db.processes.create("WsDefault")
    ws_id = db.create_workspace(
        "WS", config_overrides={"pipeline": {"default_process_id": pid}},
    )
    db.delete_saved_process(pid)
    ws = db.workspaces.get(ws_id)
    overrides = json.loads(ws["config_overrides"])
    # Explicit None (not a popped key) so a global default_process_id set
    # elsewhere does not silently re-adopt for this workspace via _deep_merge.
    assert overrides["pipeline"]["default_process_id"] is None


def test_delete_workspace_default_beats_global_default(tmp_path):
    """A workspace whose default pointed at the deleted process must fall
    back to import-only even when a global default_process_id is set."""
    db = _db(tmp_path)
    keep = db.processes.create("KeepGlobal")
    doomed = db.processes.create("Doomed")
    ws_id = db.create_workspace(
        "WS", config_overrides={"pipeline": {"default_process_id": doomed}},
    )
    db.set_active_workspace(ws_id)
    db.delete_saved_process(doomed)
    effective = db.get_effective_config(
        {"pipeline": {"default_process_id": keep}}
    )
    assert effective["pipeline"]["default_process_id"] is None


def test_delete_leaves_other_workspace_defaults_intact(tmp_path):
    db = _db(tmp_path)
    keep = db.processes.create("Keep")
    doomed = db.processes.create("Doomed")
    ws_id = db.create_workspace(
        "WS", config_overrides={"pipeline": {"default_process_id": keep}},
    )
    db.delete_saved_process(doomed)
    ws = db.workspaces.get(ws_id)
    overrides = json.loads(ws["config_overrides"])
    assert overrides["pipeline"]["default_process_id"] == keep


def test_seeds_not_reinserted_after_delete_all(tmp_path):
    """A user who deletes every process must not have seeds reappear."""
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    for p in db.processes.list_all():
        db.delete_saved_process(p["id"])
    assert db.processes.list_all() == []
    # Re-open: the db_meta marker must prevent re-seeding.
    db2 = Database(db_path)
    assert db2.processes.list_all() == []
