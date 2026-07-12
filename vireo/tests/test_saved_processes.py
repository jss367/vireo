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
    procs = db.get_saved_processes()
    names = [p["name"] for p in procs]
    assert names == [s["name"] for s in ps.SEED_PROCESSES]
    assert all(p["is_seed"] for p in procs)
    # sort_order preserves the seed ordering.
    assert [p["sort_order"] for p in procs] == list(range(len(procs)))


def test_identify_seed_carries_species_review_and_no_misses(tmp_path):
    db = _db(tmp_path)
    identify = next(
        p for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    assert identify["skip_extract_masks"] is True
    assert identify["skip_eye_keypoints"] is True
    assert identify["skip_regroup"] is True
    assert identify["miss_enabled"] is False
    assert identify["review_mode"] == "species"


def test_full_seed_runs_everything(tmp_path):
    db = _db(tmp_path)
    full = next(p for p in db.get_saved_processes() if p["name"] == "Full")
    assert full["skip_classify"] is False
    assert full["skip_extract_masks"] is False
    assert full["skip_eye_keypoints"] is False
    assert full["skip_regroup"] is False
    assert full["miss_enabled"] is True
    assert full["review_mode"] is None


def test_resolve_process_round_trips_all_six_fields(tmp_path):
    db = _db(tmp_path)
    identify = next(
        p for p in db.get_saved_processes() if p["name"] == "Identify birds"
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
    pid = db.create_saved_process(
        "My combo", skip_extract_masks=True, miss_enabled=False,
        review_mode="species",
    )
    proc = db.get_saved_process(pid)
    assert proc["name"] == "My combo"
    assert proc["skip_extract_masks"] is True
    assert proc["miss_enabled"] is False
    assert proc["review_mode"] == "species"
    assert proc["is_seed"] is False


def test_create_duplicate_name_rejected(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.create_saved_process("Identify birds")


def test_create_blank_name_rejected(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.create_saved_process("   ")


def test_create_bad_review_mode_rejected(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError):
        db.create_saved_process("Bad", review_mode="whatever")


def test_update_saved_process_rename_and_flags(tmp_path):
    db = _db(tmp_path)
    pid = db.create_saved_process("Temp")
    assert db.update_saved_process(
        pid, name="Renamed", skip_classify=True, review_mode="species",
    )
    proc = db.get_saved_process(pid)
    assert proc["name"] == "Renamed"
    assert proc["skip_classify"] is True
    assert proc["review_mode"] == "species"


def test_update_partial_leaves_other_fields(tmp_path):
    db = _db(tmp_path)
    pid = db.create_saved_process(
        "Base", skip_regroup=True, miss_enabled=False, review_mode="species",
    )
    db.update_saved_process(pid, name="Base2")
    proc = db.get_saved_process(pid)
    assert proc["name"] == "Base2"
    assert proc["skip_regroup"] is True
    assert proc["miss_enabled"] is False
    assert proc["review_mode"] == "species"


def test_update_can_clear_review_mode(tmp_path):
    db = _db(tmp_path)
    pid = db.create_saved_process("HasReview", review_mode="species")
    db.update_saved_process(pid, review_mode=None)
    assert db.get_saved_process(pid)["review_mode"] is None


def test_update_missing_returns_false(tmp_path):
    db = _db(tmp_path)
    assert db.update_saved_process(99999, name="x") is False


def test_update_duplicate_name_rejected(tmp_path):
    db = _db(tmp_path)
    pid = db.create_saved_process("Unique")
    with pytest.raises(ValueError):
        db.update_saved_process(pid, name="Full")


def test_delete_saved_process(tmp_path):
    db = _db(tmp_path)
    pid = db.create_saved_process("Doomed")
    assert db.delete_saved_process(pid) is True
    assert db.get_saved_process(pid) is None
    assert db.delete_saved_process(pid) is False


def test_delete_nulls_referencing_workspace_default(tmp_path):
    db = _db(tmp_path)
    pid = db.create_saved_process("WsDefault")
    ws_id = db.create_workspace(
        "WS", config_overrides={"pipeline": {"default_process_id": pid}},
    )
    db.delete_saved_process(pid)
    ws = db.get_workspace(ws_id)
    overrides = json.loads(ws["config_overrides"])
    # Explicit None (not a popped key) so a global default_process_id set
    # elsewhere does not silently re-adopt for this workspace via _deep_merge.
    assert overrides["pipeline"]["default_process_id"] is None


def test_delete_workspace_default_beats_global_default(tmp_path):
    """A workspace whose default pointed at the deleted process must fall
    back to import-only even when a global default_process_id is set."""
    db = _db(tmp_path)
    keep = db.create_saved_process("KeepGlobal")
    doomed = db.create_saved_process("Doomed")
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
    keep = db.create_saved_process("Keep")
    doomed = db.create_saved_process("Doomed")
    ws_id = db.create_workspace(
        "WS", config_overrides={"pipeline": {"default_process_id": keep}},
    )
    db.delete_saved_process(doomed)
    ws = db.get_workspace(ws_id)
    overrides = json.loads(ws["config_overrides"])
    assert overrides["pipeline"]["default_process_id"] == keep


def test_seeds_not_reinserted_after_delete_all(tmp_path):
    """A user who deletes every process must not have seeds reappear."""
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    for p in db.get_saved_processes():
        db.delete_saved_process(p["id"])
    assert db.get_saved_processes() == []
    # Re-open: the db_meta marker must prevent re-seeding.
    db2 = Database(db_path)
    assert db2.get_saved_processes() == []


def test_legacy_default_strategy_migrates_to_process_id(tmp_path):
    """An existing workspace override on the old default_strategy string maps
    to the seeded process id on init."""
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    # Simulate a pre-migration workspace by writing the old-shape override and
    # clearing the migration marker so a fresh handle re-runs it.
    ws_id = db.create_workspace(
        "Legacy",
        config_overrides={"pipeline": {"default_strategy": "identify"}},
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key='default_strategy_to_process_id'"
    )
    db.conn.commit()

    db2 = Database(db_path)
    ws = db2.get_workspace(ws_id)
    overrides = json.loads(ws["config_overrides"])
    identify = next(
        p for p in db2.get_saved_processes() if p["name"] == "Identify birds"
    )
    assert overrides["pipeline"]["default_process_id"] == identify["id"]
    assert "default_strategy" not in overrides["pipeline"]


def test_legacy_unknown_strategy_migrates_to_import_only(tmp_path):
    """An unrecognized legacy strategy is preserved as explicit import-only.

    The workspace had an *explicit* override, so migrating to a bare key
    (letting ``get_effective_config`` deep-merge the global default back in)
    would silently start auto-processing on a workspace whose user had set
    something specifically different. Explicit ``default_process_id: None``
    is the right rendering of "the user said not the global default".
    """
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db.create_workspace(
        "Legacy",
        config_overrides={"pipeline": {"default_strategy": "gone_forever"}},
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key='default_strategy_to_process_id'"
    )
    db.conn.commit()

    db2 = Database(db_path)
    overrides = json.loads(db2.get_workspace(ws_id)["config_overrides"])
    assert "default_strategy" not in overrides["pipeline"]
    assert overrides["pipeline"]["default_process_id"] is None


def test_legacy_null_strategy_migrates_to_explicit_null(tmp_path):
    """A workspace with ``default_strategy: null`` (explicit "import only")
    must migrate to ``default_process_id: null``, not to a bare key.

    Without an explicit null, popping the legacy key would let
    ``get_effective_config()``'s deep_merge inherit whatever the global
    ``default_process_id`` happens to be, silently starting auto-processing
    on a workspace that had explicitly said otherwise.
    """
    from db import Database
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db.create_workspace(
        "LegacyImportOnly",
        config_overrides={"pipeline": {"default_strategy": None}},
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key='default_strategy_to_process_id'"
    )
    db.conn.commit()

    db2 = Database(db_path)
    overrides = json.loads(db2.get_workspace(ws_id)["config_overrides"])
    assert "default_strategy" not in overrides["pipeline"]
    assert overrides["pipeline"]["default_process_id"] is None
