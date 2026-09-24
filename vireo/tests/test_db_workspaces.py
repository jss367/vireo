"""Behavior pins for the workspace domain of ``Database``.

The behavior tests exercise the workspace methods only through the public
``Database`` façade, so they hold regardless of whether the SQL lives in
``db.py`` or in ``repositories/workspaces.py``; the structural tests at the
end keep it in the repository. They cover workspace CRUD,
active-workspace restoration, config overrides and the legacy-config
migrations, label-set selection, navigation tabs, new-images snapshots, and
the new-images cache invalidation hooks.
"""

import ast
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import pytest
from db import (
    DEFAULT_TABS,
    SUBJECT_TYPES_DEFAULT,
    Database,
    normalize_browse_stack_config,
)


class _RecordingCache:
    """Stand-in for the shared new-images cache that records invalidations."""

    def __init__(self):
        self.invalidated = []

    def invalidate_workspaces(self, db_path, workspace_ids):
        self.invalidated.append((db_path, set(workspace_ids)))


@pytest.fixture
def cache(db, monkeypatch):
    recorder = _RecordingCache()
    monkeypatch.setattr(db, "_new_images_cache", recorder)
    return recorder


@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _raw_overrides(db, workspace_id, value):
    """Store a raw (possibly malformed) config_overrides string."""
    db.conn.execute(
        "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
        (value, workspace_id),
    )
    db.conn.commit()


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


# -- active workspace ---------------------------------------------------------


def test_ws_id_raises_without_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db._ws_id()


def test_restore_prefers_most_recently_opened_then_lowest_id(tmp_path):
    path = str(tmp_path / "restore.db")
    with Database(path) as first:
        default_id = first._ws_id()
        a = first.create_workspace("A")
        b = first.create_workspace("B")
        first.update_workspace(a, last_opened_at="2026-01-01T00:00:00")
        first.update_workspace(b, last_opened_at="2026-02-01T00:00:00")
    with Database(path, initialize_schema=False) as reopened:
        assert reopened._ws_id() == b
    with Database(path, initialize_schema=False) as reopened:
        reopened.conn.execute("UPDATE workspaces SET last_opened_at = NULL")
        reopened.conn.commit()
    with Database(path, initialize_schema=False) as reopened:
        # All unopened: lowest id wins.
        assert reopened._ws_id() == default_id


def test_restore_raises_when_no_workspace_exists(tmp_path):
    path = str(tmp_path / "empty.db")
    with Database(path) as first:
        first.conn.execute("DELETE FROM workspaces")
        first.conn.commit()
    with pytest.raises(RuntimeError, match="no workspace after schema"):
        Database(path, initialize_schema=False)


# -- CRUD ---------------------------------------------------------------------


def test_create_workspace_encodes_json_and_default_tabs(db, cache):
    ws_id = db.create_workspace(
        "Kenya", config_overrides={"a": 1}, ui_state={"panel": "open"}
    )
    with _reader(db) as other:
        row = other.execute(
            "SELECT * FROM workspaces WHERE id = ?", (ws_id,)
        ).fetchone()
    assert row["name"] == "Kenya"
    assert json.loads(row["config_overrides"]) == {"a": 1}
    assert json.loads(row["ui_state"]) == {"panel": "open"}
    assert json.loads(row["tabs"]) == DEFAULT_TABS
    assert cache.invalidated == [(db._db_path, {ws_id})]


def test_create_workspace_stores_falsy_json_fields_as_null(db):
    ws_id = db.create_workspace("Empty", config_overrides={}, ui_state={})
    row = db.get_workspace(ws_id)
    assert row["config_overrides"] is None
    assert row["ui_state"] is None


def test_create_workspace_duplicate_name_raises_integrity_error(db):
    db.create_workspace("Same")
    with pytest.raises(sqlite3.IntegrityError):
        db.create_workspace("Same")


def test_get_workspace_missing_returns_none(db):
    assert db.get_workspace(987654) is None


def test_get_workspaces_orders_pinned_first_then_case_insensitive_name(db):
    db.create_workspace("beta")
    alpha = db.create_workspace("Alpha")
    db.create_workspace("gamma")
    zulu = db.create_workspace("Zulu")
    db.update_workspace(zulu, pinned_at="2026-01-01T00:00:00")
    names = [row["name"] for row in db.get_workspaces()]
    assert names[0] == "Zulu"
    assert names[1:] == sorted(names[1:], key=str.lower)
    assert names.index("Alpha") < names.index("beta")
    assert db.get_workspace(alpha)["pinned_at"] is None


def test_update_workspace_sets_and_clears_fields(db):
    ws_id = db.create_workspace("Old", config_overrides={"x": 1}, ui_state={"y": 2})
    db.update_workspace(
        ws_id,
        name="New",
        config_overrides={"x": 3},
        ui_state={"y": 4},
        last_opened_at="2026-03-03T00:00:00",
        pinned_at="2026-03-04T00:00:00",
    )
    with _reader(db) as other:
        row = other.execute(
            "SELECT * FROM workspaces WHERE id = ?", (ws_id,)
        ).fetchone()
    assert row["name"] == "New"
    assert json.loads(row["config_overrides"]) == {"x": 3}
    assert json.loads(row["ui_state"]) == {"y": 4}
    assert row["last_opened_at"] == "2026-03-03T00:00:00"
    assert row["pinned_at"] == "2026-03-04T00:00:00"

    db.update_workspace(ws_id, config_overrides=None, ui_state=None, pinned_at=None)
    row = db.get_workspace(ws_id)
    assert row["config_overrides"] is None
    assert row["ui_state"] is None
    assert row["pinned_at"] is None
    # name=None and last_opened_at=None mean "leave unchanged".
    assert row["name"] == "New"
    assert row["last_opened_at"] == "2026-03-03T00:00:00"


def test_update_workspace_without_fields_is_a_noop(db):
    ws_id = db.create_workspace("Keep", config_overrides={"k": 1})
    assert db.update_workspace(ws_id) is None
    assert not db.conn.in_transaction
    row = db.get_workspace(ws_id)
    assert row["name"] == "Keep"
    assert json.loads(row["config_overrides"]) == {"k": 1}


def test_delete_workspace_commits_and_invalidates_cache(db, cache):
    ws_id = db.create_workspace("Gone")
    cache.invalidated.clear()
    db.delete_workspace(ws_id)
    with _reader(db) as other:
        assert other.execute(
            "SELECT 1 FROM workspaces WHERE id = ?", (ws_id,)
        ).fetchone() is None
    assert cache.invalidated == [(db._db_path, {ws_id})]


def test_delete_workspace_translates_pending_nas_error(db, cache):
    ws_id = db.create_workspace("Guarded")
    db.conn.execute(
        "CREATE TEMP TRIGGER guard BEFORE DELETE ON workspaces BEGIN "
        "SELECT RAISE(ABORT, 'Send pending photos to NAS before deleting this workspace'); "
        "END"
    )
    cache.invalidated.clear()
    with pytest.raises(ValueError, match="Send pending photos to NAS"):
        db.delete_workspace(ws_id)
    assert db.get_workspace(ws_id) is not None
    assert cache.invalidated == []


def test_delete_workspace_reraises_other_integrity_errors(db, cache):
    ws_id = db.create_workspace("Guarded")
    db.conn.execute(
        "CREATE TEMP TRIGGER guard BEFORE DELETE ON workspaces BEGIN "
        "SELECT RAISE(ABORT, 'some other constraint'); END"
    )
    cache.invalidated.clear()
    with pytest.raises(sqlite3.IntegrityError, match="some other constraint"):
        db.delete_workspace(ws_id)
    assert cache.invalidated == []


def test_ensure_default_workspace_returns_existing_or_creates(db):
    existing = db.conn.execute(
        "SELECT id FROM workspaces WHERE name = 'Default'"
    ).fetchone()[0]
    assert db.ensure_default_workspace() == existing
    db.update_workspace(existing, name="Renamed")
    created = db.ensure_default_workspace()
    assert created != existing
    assert db.get_workspace(created)["name"] == "Default"
    assert json.loads(db.get_workspace(created)["tabs"]) == DEFAULT_TABS


def test_set_workspace_group_state_commits(db):
    ws_id = db.create_workspace("Grouped")
    db.set_workspace_group_state(ws_id, "fp-1", "2026-04-01T00:00:00")
    with _reader(db) as other:
        row = other.execute(
            "SELECT last_grouped_at, last_group_fingerprint FROM workspaces "
            "WHERE id = ?",
            (ws_id,),
        ).fetchone()
    assert row["last_grouped_at"] == "2026-04-01T00:00:00"
    assert row["last_group_fingerprint"] == "fp-1"


# -- config overrides ---------------------------------------------------------


def test_get_effective_config_deep_merges_overrides(db):
    db.update_workspace(db._ws_id(), config_overrides={"pipeline": {"w_focus": 0.5}})
    merged = db.get_effective_config({"pipeline": {"w_focus": 0.1, "w_species": 1}, "k": 2})
    assert merged == {"pipeline": {"w_focus": 0.5, "w_species": 1}, "k": 2}


@pytest.mark.parametrize("raw", [None, "", "not json", "[1, 2]", "42"])
def test_get_effective_config_falls_back_to_global(db, raw):
    _raw_overrides(db, db._ws_id(), raw)
    global_config = {"k": 1}
    assert db.get_effective_config(global_config) is global_config


def test_browse_stack_settings_without_config_uses_defaults(db):
    db.update_workspace(db._ws_id(), config_overrides={"browse_stack_time_gap": 9})
    assert db.browse_stack_settings() == normalize_browse_stack_config(None)


def test_browse_stack_settings_applies_workspace_override(db):
    db.update_workspace(db._ws_id(), config_overrides={"browse_stack_time_gap": 9})
    settings = db.browse_stack_settings({"browse_stack_time_gap": 3})
    assert settings == normalize_browse_stack_config({"browse_stack_time_gap": 9})


def test_min_detector_confidence_takes_minimum_across_workspaces(db):
    low = db.create_workspace("Low", config_overrides={"detector_confidence": 0.05})
    db.create_workspace("High", config_overrides={"detector_confidence": 0.6})
    assert db.min_detector_confidence_across_workspaces({"detector_confidence": 0.3}) == 0.05
    db.delete_workspace(low)
    assert db.min_detector_confidence_across_workspaces({"detector_confidence": 0.3}) == 0.3


def test_min_detector_confidence_invalid_global_defaults_to_point_two(db):
    assert db.min_detector_confidence_across_workspaces({"detector_confidence": "x"}) == 0.2
    assert db.min_detector_confidence_across_workspaces({}) == 0.2


def test_min_detector_confidence_ignores_malformed_overrides(db):
    for name, raw in [("bad-json", "{"), ("list", "[1]"), ("bad-value", '{"detector_confidence": "nope"}')]:
        ws_id = db.create_workspace(name)
        _raw_overrides(db, ws_id, raw)
    assert db.min_detector_confidence_across_workspaces({"detector_confidence": 0.4}) == 0.4


def test_min_detector_confidence_without_workspaces_returns_default(db):
    db.conn.execute("DELETE FROM workspaces")
    db.conn.commit()
    assert db.min_detector_confidence_across_workspaces({"detector_confidence": 0.7}) == 0.7


def test_get_subject_types_filters_to_known_string_types(db, isolated_config):
    db.update_workspace(
        db._ws_id(),
        config_overrides={"subject_types": ["taxonomy", "bogus", 3, ["x"], "genre"]},
    )
    assert db.get_subject_types() == {"taxonomy", "genre"}


def test_get_subject_types_non_list_falls_back_to_default(db, isolated_config):
    db.update_workspace(db._ws_id(), config_overrides={"subject_types": "taxonomy"})
    assert db.get_subject_types() == set(SUBJECT_TYPES_DEFAULT)


# -- label-set selection ------------------------------------------------------


@pytest.mark.parametrize(
    "raw", [None, "", "{", "[1]", '{"active_labels": "one.txt"}', '{"other": 1}']
)
def test_get_workspace_active_labels_rejects_malformed_values(db, raw):
    _raw_overrides(db, db._ws_id(), raw)
    assert db.get_workspace_active_labels() is None


@pytest.mark.parametrize("raw", ["{", "[1]"])
def test_set_workspace_active_labels_replaces_malformed_overrides(db, raw):
    _raw_overrides(db, db._ws_id(), raw)
    db.set_workspace_active_labels(["a.txt"])
    row = db.get_workspace(db._ws_id())
    assert json.loads(row["config_overrides"]) == {"active_labels": ["a.txt"]}
    assert db.get_workspace_active_labels() == ["a.txt"]


def test_forget_label_file_removes_from_every_workspace(db):
    keep = db.create_workspace("Keep", config_overrides={"active_labels": ["b.txt"], "x": 1})
    drop = db.create_workspace("Drop", config_overrides={"active_labels": ["a.txt", "b.txt"], "x": 2})
    for name, raw in [("bad-json", "{"), ("list", "[1]"), ("not-list", '{"active_labels": "a.txt"}')]:
        _raw_overrides(db, db.create_workspace(name), raw)

    assert db.forget_label_file("a.txt") == 1
    assert not db.conn.in_transaction
    with _reader(db) as other:
        rows = {
            r["id"]: json.loads(r["config_overrides"])
            for r in other.execute(
                "SELECT id, config_overrides FROM workspaces WHERE id IN (?, ?)",
                (keep, drop),
            )
        }
    assert rows[drop] == {"active_labels": ["b.txt"], "x": 2}
    assert rows[keep] == {"active_labels": ["b.txt"], "x": 1}


def test_forget_label_file_unknown_file_changes_nothing(db):
    db.create_workspace("Keep", config_overrides={"active_labels": ["b.txt"]})
    assert db.forget_label_file("missing.txt") == 0


# -- navigation tabs ----------------------------------------------------------


@pytest.mark.parametrize("raw", [None, "", "not json", '{"a": 1}'])
def test_get_tabs_falls_back_to_defaults(db, raw):
    db.conn.execute("UPDATE workspaces SET tabs = ? WHERE id = ?", (raw, db._ws_id()))
    db.conn.commit()
    assert db.get_tabs() == DEFAULT_TABS


def test_get_tabs_drops_non_string_unknown_and_duplicate_ids(db):
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json.dumps([3, "browse", "zoom_test", "browse", "compare"]), db._ws_id()),
    )
    db.conn.commit()
    assert db.get_tabs() == ["browse", "id_conflicts"]


def test_get_tabs_for_missing_workspace_row_returns_defaults(db):
    db.set_active_workspace(987654)
    assert db.get_tabs() == DEFAULT_TABS


def test_set_tabs_validates_input(db):
    with pytest.raises(ValueError, match="must be a list"):
        db.set_tabs("browse")
    with pytest.raises(ValueError, match="must be a string"):
        db.set_tabs([1])
    with pytest.raises(ValueError, match="not a known nav id"):
        db.set_tabs(["nope"])
    with pytest.raises(ValueError, match="more than once"):
        db.set_tabs(["browse", "browse"])
    assert db.set_tabs(["browse"]) == ["browse"]
    with _reader(db) as other:
        stored = other.execute(
            "SELECT tabs FROM workspaces WHERE id = ?", (db._ws_id(),)
        ).fetchone()["tabs"]
    assert json.loads(stored) == ["browse"]


def test_pin_and_unpin_tab(db):
    db.set_tabs(["browse"])
    assert db.pin_tab("map") == ["browse", "map"]
    assert db.pin_tab("map") == ["browse", "map"]
    assert db.unpin_tab("browse") == ["map"]
    assert db.unpin_tab("browse") == ["map"]
    with pytest.raises(ValueError, match="not a known nav id"):
        db.pin_tab("nope")
    with pytest.raises(ValueError, match="not a known nav id"):
        db.unpin_tab("nope")


def test_tabs_require_an_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.get_tabs()


# -- new-images snapshots and cache -------------------------------------------


def test_new_images_snapshot_round_trip_dedupes_and_sorts(db):
    snap_id = db.create_new_images_snapshot(["/b.jpg", "/a.jpg", "/b.jpg"])
    snap = db.get_new_images_snapshot(snap_id)
    assert snap["id"] == snap_id
    assert snap["workspace_id"] == db._ws_id()
    assert snap["file_count"] == 2
    assert snap["file_paths"] == ["/a.jpg", "/b.jpg"]
    assert snap["created_at"]


def test_new_images_snapshot_allows_empty_paths(db):
    snap_id = db.create_new_images_snapshot(None)
    snap = db.get_new_images_snapshot(snap_id)
    assert snap["file_count"] == 0
    assert snap["file_paths"] == []


def test_new_images_snapshot_is_workspace_scoped(db):
    snap_id = db.create_new_images_snapshot(["/a.jpg"])
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    assert db.get_new_images_snapshot(snap_id) is None
    assert db.get_new_images_snapshot(snap_id + 1000) is None


def test_new_images_snapshot_out_of_range_id_short_circuits(db):
    # The range check runs before the active workspace is consulted.
    db.set_active_workspace(None)
    assert db.get_new_images_snapshot(1 << 63) is None
    assert db.get_new_images_snapshot(-(1 << 63) - 1) is None
    with pytest.raises(RuntimeError):
        db.get_new_images_snapshot(1)


def test_create_new_images_snapshot_requires_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.create_new_images_snapshot(["/a.jpg"])


def test_invalidate_new_images_cache_for_folders_empty_is_noop(db, cache):
    db.invalidate_new_images_cache_for_folders([])
    db.invalidate_new_images_cache_for_folders(None)
    assert cache.invalidated == []


def test_invalidate_new_images_cache_for_folders_chunks_and_scopes(db, cache):
    ws_a = db._ws_id()
    ws_b = db.create_workspace("B")
    db.create_workspace("Unlinked")
    folder_ids = [db.add_folder(f"/root/f{i}") for i in range(3)]
    db.conn.execute(
        "INSERT OR IGNORE INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (ws_b, folder_ids[2]),
    )
    db.conn.commit()
    cache.invalidated.clear()
    # 1200 ids forces three chunks of <=500; unknown ids match nothing.
    ids = folder_ids + list(range(10_000_000, 10_001_197))
    db.invalidate_new_images_cache_for_folders(iter(ids))
    assert cache.invalidated == [(db._db_path, {ws_a, ws_b})]


def test_invalidate_new_images_cache_for_workspace(db, cache):
    db.invalidate_new_images_cache_for_workspace(42)
    assert cache.invalidated == [(db._db_path, {42})]


def test_get_new_images_for_workspace_uses_cache(db, monkeypatch):
    import new_images

    calls = []

    def fake_count(database, workspace_id):
        calls.append(workspace_id)
        return {"new_count": len(calls)}

    monkeypatch.setattr(new_images, "count_new_images_for_workspace", fake_count)
    ws_id = db._ws_id()
    db.invalidate_new_images_cache_for_workspace(ws_id)
    first = db.get_new_images_for_workspace(ws_id)
    second = db.get_new_images_for_workspace(ws_id)
    assert first == second == {"new_count": 1}
    assert calls == [ws_id]
    db.invalidate_new_images_cache_for_workspace(ws_id)
    assert db.get_new_images_for_workspace(ws_id) == {"new_count": 2}


# -- legacy config migrations -------------------------------------------------


def _malformed_workspaces(db):
    """Workspaces whose overrides every legacy rewrite must skip."""
    for name, raw in [
        ("m-bad-json", "{"),
        ("m-list", "[1]"),
        ("m-no-pipeline", '{"x": 1}'),
        ("m-pipeline-list", '{"pipeline": [1]}'),
    ]:
        _raw_overrides(db, db.create_workspace(name), raw)


def _overrides(db, ws_id):
    raw = db.get_workspace(ws_id)["config_overrides"]
    return json.loads(raw) if raw else None


def test_rewrite_legacy_miss_thresholds(db):
    _malformed_workspaces(db)
    legacy = db.create_workspace(
        "Legacy",
        config_overrides={"pipeline": {"miss_det_confidence": 0.5, "miss_det_confidence_burst": 0.6}},
    )
    custom = db.create_workspace(
        "Custom",
        config_overrides={"pipeline": {"miss_det_confidence": 0.5, "miss_det_confidence_burst": 0.9}},
    )
    assert db.rewrite_legacy_miss_thresholds_in_workspaces(0.5, 0.6, 0.3, 0.4) == 1
    assert not db.conn.in_transaction
    assert _overrides(db, legacy)["pipeline"] == {
        "miss_det_confidence": 0.3, "miss_det_confidence_burst": 0.4,
    }
    assert _overrides(db, custom)["pipeline"]["miss_det_confidence_burst"] == 0.9
    assert db.rewrite_legacy_miss_thresholds_in_workspaces(0.5, 0.6, 0.3, 0.4) == 0


def test_rewrite_legacy_w_species_default(db):
    _malformed_workspaces(db)
    legacy = db.create_workspace("Legacy", config_overrides={"pipeline": {"w_species": 0.3}})
    custom = db.create_workspace("Custom", config_overrides={"pipeline": {"w_species": 0.8}})
    assert db.rewrite_legacy_w_species_default_in_workspaces(0.3, 0.5) == 1
    assert _overrides(db, legacy)["pipeline"]["w_species"] == 0.5
    assert _overrides(db, custom)["pipeline"]["w_species"] == 0.8
    assert db.rewrite_legacy_w_species_default_in_workspaces(0.3, 0.5) == 0


def test_rewrite_legacy_eye_detect_default_clears_fingerprint(db):
    _malformed_workspaces(db)
    legacy = db.create_workspace("Legacy", config_overrides={"pipeline": {"eye_detect_enabled": True}})
    off = db.create_workspace("Off", config_overrides={"pipeline": {"eye_detect_enabled": False}})
    db.set_workspace_group_state(legacy, "fp", "t")
    db.set_workspace_group_state(off, "fp", "t")
    assert db.rewrite_legacy_eye_detect_default_in_workspaces() == 1
    assert _overrides(db, legacy)["pipeline"]["eye_detect_enabled"] is False
    assert db.get_workspace(legacy)["last_group_fingerprint"] is None
    assert db.get_workspace(off)["last_group_fingerprint"] == "fp"
    assert db.rewrite_legacy_eye_detect_default_in_workspaces() == 0


def test_invalidate_group_fingerprints_without_explicit_eye_false(db):
    explicit_off = db.create_workspace("Off", config_overrides={"pipeline": {"eye_detect_enabled": False}})
    inherits = db.create_workspace("Inherits")
    bad_json = db.create_workspace("BadJson")
    _raw_overrides(db, bad_json, "{")
    listy = db.create_workspace("Listy")
    _raw_overrides(db, listy, "[1]")
    never_grouped = db.create_workspace("Never")
    for ws_id in (explicit_off, inherits, bad_json, listy):
        db.set_workspace_group_state(ws_id, "fp", "t")

    assert db.invalidate_group_fingerprints_without_explicit_eye_false() == 3
    assert not db.conn.in_transaction
    assert db.get_workspace(explicit_off)["last_group_fingerprint"] == "fp"
    for ws_id in (inherits, bad_json, listy):
        assert db.get_workspace(ws_id)["last_group_fingerprint"] is None
    assert db.get_workspace(never_grouped)["last_group_fingerprint"] is None
    assert db.invalidate_group_fingerprints_without_explicit_eye_false() == 0


# -- structure: the workspace SQL lives in the repository ---------------------

# Database methods whose SQL moved to repositories/workspaces.py. Each stays
# on Database as a thin wrapper so existing call sites keep working; none may
# reach the connection directly again.
_DELEGATING_WORKSPACE_METHODS = (
    "_restore_active_workspace",
    "invalidate_new_images_cache_for_folders",
    "create_workspace",
    "get_workspace",
    "get_workspaces",
    "update_workspace",
    "delete_workspace",
    "ensure_default_workspace",
    "set_workspace_group_state",
    "forget_label_file",
    "get_tabs",
    "set_tabs",
    "pin_tab",
    "unpin_tab",
    "create_new_images_snapshot",
    "get_new_images_snapshot",
    "rewrite_legacy_miss_thresholds_in_workspaces",
    "rewrite_legacy_w_species_default_in_workspaces",
    "rewrite_legacy_eye_detect_default_in_workspaces",
    "invalidate_group_fingerprints_without_explicit_eye_false",
)


@pytest.mark.parametrize("name", _DELEGATING_WORKSPACE_METHODS)
def test_workspace_method_delegates_to_repository(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to WorkspaceRepository"
    )
    assert "_workspace_repository" in attrs, (
        f"Database.{name} no longer delegates to WorkspaceRepository"
    )


def test_update_workspace_shares_the_unset_sentinel_with_the_repository():
    import db as db_module
    from repositories import UNSET
    from repositories.workspaces import WorkspaceRepository

    assert db_module._UNSET is UNSET
    facade = inspect.signature(Database.update_workspace).parameters
    repo = inspect.signature(WorkspaceRepository.update).parameters
    for field in ("config_overrides", "ui_state", "pinned_at"):
        assert facade[field].default is UNSET
        assert repo[field].default is UNSET
