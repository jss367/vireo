"""Behavior pins for the workspace domain of ``Database``.

The behavior tests exercise the workspace rows, tabs and snapshots through
the ``db.workspaces`` accessor, and create/delete, restoration and the cache
hooks through the ``Database`` methods that stay on the façade; the
structural tests at the end keep the SQL in the repository and the old
forwarding wrappers gone. They cover workspace CRUD,
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
from repositories.workspaces import WorkspaceRepository


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
        first.workspaces.update(a, last_opened_at="2026-01-01T00:00:00")
        first.workspaces.update(b, last_opened_at="2026-02-01T00:00:00")
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
    row = db.workspaces.get(ws_id)
    assert row["config_overrides"] is None
    assert row["ui_state"] is None


def test_create_workspace_duplicate_name_raises_integrity_error(db):
    db.create_workspace("Same")
    with pytest.raises(sqlite3.IntegrityError):
        db.create_workspace("Same")


def test_get_workspace_missing_returns_none(db):
    assert db.workspaces.get(987654) is None


def test_get_workspaces_orders_pinned_first_then_case_insensitive_name(db):
    db.create_workspace("beta")
    alpha = db.create_workspace("Alpha")
    db.create_workspace("gamma")
    zulu = db.create_workspace("Zulu")
    db.workspaces.update(zulu, pinned_at="2026-01-01T00:00:00")
    names = [row["name"] for row in db.workspaces.list_all()]
    assert names[0] == "Zulu"
    assert names[1:] == sorted(names[1:], key=str.lower)
    assert names.index("Alpha") < names.index("beta")
    assert db.workspaces.get(alpha)["pinned_at"] is None


def test_update_workspace_sets_and_clears_fields(db):
    ws_id = db.create_workspace("Old", config_overrides={"x": 1}, ui_state={"y": 2})
    db.workspaces.update(
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

    db.workspaces.update(ws_id, config_overrides=None, ui_state=None, pinned_at=None)
    row = db.workspaces.get(ws_id)
    assert row["config_overrides"] is None
    assert row["ui_state"] is None
    assert row["pinned_at"] is None
    # name=None and last_opened_at=None mean "leave unchanged".
    assert row["name"] == "New"
    assert row["last_opened_at"] == "2026-03-03T00:00:00"


def test_update_workspace_without_fields_is_a_noop(db):
    ws_id = db.create_workspace("Keep", config_overrides={"k": 1})
    assert db.workspaces.update(ws_id) is None
    assert not db.conn.in_transaction
    row = db.workspaces.get(ws_id)
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
    assert db.workspaces.get(ws_id) is not None
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
    db.workspaces.update(existing, name="Renamed")
    created = db.ensure_default_workspace()
    assert created != existing
    assert db.workspaces.get(created)["name"] == "Default"
    assert json.loads(db.workspaces.get(created)["tabs"]) == DEFAULT_TABS


def test_set_workspace_group_state_commits(db):
    ws_id = db.create_workspace("Grouped")
    db.workspaces.set_group_state(ws_id, "fp-1", "2026-04-01T00:00:00")
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
    db.workspaces.update(db._ws_id(), config_overrides={"pipeline": {"w_focus": 0.5}})
    merged = db.get_effective_config({"pipeline": {"w_focus": 0.1, "w_species": 1}, "k": 2})
    assert merged == {"pipeline": {"w_focus": 0.5, "w_species": 1}, "k": 2}


@pytest.mark.parametrize("raw", [None, "", "not json", "[1, 2]", "42"])
def test_get_effective_config_falls_back_to_global(db, raw):
    _raw_overrides(db, db._ws_id(), raw)
    global_config = {"k": 1}
    assert db.get_effective_config(global_config) is global_config


def test_browse_stack_settings_without_config_uses_defaults(db):
    db.workspaces.update(db._ws_id(), config_overrides={"browse_stack_time_gap": 9})
    assert db.browse_stack_settings() == normalize_browse_stack_config(None)


def test_browse_stack_settings_applies_workspace_override(db):
    db.workspaces.update(db._ws_id(), config_overrides={"browse_stack_time_gap": 9})
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
    db.workspaces.update(
        db._ws_id(),
        config_overrides={"subject_types": ["taxonomy", "bogus", 3, ["x"], "genre"]},
    )
    assert db.get_subject_types() == {"taxonomy", "genre"}


def test_get_subject_types_non_list_falls_back_to_default(db, isolated_config):
    db.workspaces.update(db._ws_id(), config_overrides={"subject_types": "taxonomy"})
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
    row = db.workspaces.get(db._ws_id())
    assert json.loads(row["config_overrides"]) == {"active_labels": ["a.txt"]}
    assert db.get_workspace_active_labels() == ["a.txt"]


def test_forget_label_file_removes_from_every_workspace(db):
    keep = db.create_workspace("Keep", config_overrides={"active_labels": ["b.txt"], "x": 1})
    drop = db.create_workspace("Drop", config_overrides={"active_labels": ["a.txt", "b.txt"], "x": 2})
    for name, raw in [("bad-json", "{"), ("list", "[1]"), ("not-list", '{"active_labels": "a.txt"}')]:
        _raw_overrides(db, db.create_workspace(name), raw)

    assert db.workspaces.forget_label_file("a.txt") == 1
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
    assert db.workspaces.forget_label_file("missing.txt") == 0


# -- navigation tabs ----------------------------------------------------------


@pytest.mark.parametrize("raw", [None, "", "not json", '{"a": 1}'])
def test_get_tabs_falls_back_to_defaults(db, raw):
    db.conn.execute("UPDATE workspaces SET tabs = ? WHERE id = ?", (raw, db._ws_id()))
    db.conn.commit()
    assert db.workspaces.get_tabs() == DEFAULT_TABS


def test_get_tabs_drops_non_string_unknown_and_duplicate_ids(db):
    db.conn.execute(
        "UPDATE workspaces SET tabs = ? WHERE id = ?",
        (json.dumps([3, "browse", "zoom_test", "browse", "compare"]), db._ws_id()),
    )
    db.conn.commit()
    assert db.workspaces.get_tabs() == ["browse", "id_conflicts"]


def test_get_tabs_for_missing_workspace_row_returns_defaults(db):
    db.set_active_workspace(987654)
    assert db.workspaces.get_tabs() == DEFAULT_TABS


def test_set_tabs_validates_input(db):
    with pytest.raises(ValueError, match="must be a list"):
        db.workspaces.set_tabs("browse")
    with pytest.raises(ValueError, match="must be a string"):
        db.workspaces.set_tabs([1])
    with pytest.raises(ValueError, match="not a known nav id"):
        db.workspaces.set_tabs(["nope"])
    with pytest.raises(ValueError, match="more than once"):
        db.workspaces.set_tabs(["browse", "browse"])
    assert db.workspaces.set_tabs(["browse"]) == ["browse"]
    with _reader(db) as other:
        stored = other.execute(
            "SELECT tabs FROM workspaces WHERE id = ?", (db._ws_id(),)
        ).fetchone()["tabs"]
    assert json.loads(stored) == ["browse"]


def test_pin_and_unpin_tab(db):
    db.workspaces.set_tabs(["browse"])
    assert db.workspaces.pin_tab("map") == ["browse", "map"]
    assert db.workspaces.pin_tab("map") == ["browse", "map"]
    assert db.workspaces.unpin_tab("browse") == ["map"]
    assert db.workspaces.unpin_tab("browse") == ["map"]
    with pytest.raises(ValueError, match="not a known nav id"):
        db.workspaces.pin_tab("nope")
    with pytest.raises(ValueError, match="not a known nav id"):
        db.workspaces.unpin_tab("nope")


def test_tabs_require_an_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.workspaces.get_tabs()


# -- new-images snapshots and cache -------------------------------------------


def test_new_images_snapshot_round_trip_dedupes_and_sorts(db):
    snap_id = db.workspaces.create_new_images_snapshot(["/b.jpg", "/a.jpg", "/b.jpg"])
    snap = db.get_new_images_snapshot(snap_id)
    assert snap["id"] == snap_id
    assert snap["workspace_id"] == db._ws_id()
    assert snap["file_count"] == 2
    assert snap["file_paths"] == ["/a.jpg", "/b.jpg"]
    assert snap["created_at"]


def test_new_images_snapshot_allows_empty_paths(db):
    snap_id = db.workspaces.create_new_images_snapshot(None)
    snap = db.get_new_images_snapshot(snap_id)
    assert snap["file_count"] == 0
    assert snap["file_paths"] == []


def test_new_images_snapshot_is_workspace_scoped(db):
    snap_id = db.workspaces.create_new_images_snapshot(["/a.jpg"])
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
        db.workspaces.create_new_images_snapshot(["/a.jpg"])


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


def test_get_workspace_id_by_name_matches_the_exact_name(db):
    ws_id = db.create_workspace("Shorebirds")
    assert db.workspaces.id_for_name("Shorebirds") == ws_id
    # ``workspaces.name`` has no NOCASE collation: the lookup is exact.
    assert db.workspaces.id_for_name("shorebirds") is None
    assert db.workspaces.id_for_name("Nope") is None


# -- structure: the workspace SQL lives in the repository ---------------------

# Coordinated Database methods over repositories/workspaces.py: each adds the
# new-images cache upkeep, the restore, or the snapshot id range check, so it
# stays on the façade; none may reach the connection directly again.
_DELEGATING_WORKSPACE_METHODS = (
    "_restore_active_workspace",
    "invalidate_new_images_cache_for_folders",
    "create_workspace",
    "delete_workspace",
    "ensure_default_workspace",
    "get_new_images_snapshot",
)

# The forwarding wrappers ``db.workspaces`` replaced.
_REMOVED_WORKSPACE_WRAPPERS = (
    "get_workspace",
    "get_workspace_id_by_name",
    "get_workspaces",
    "update_workspace",
    "set_workspace_group_state",
    "forget_label_file",
    "get_tabs",
    "set_tabs",
    "pin_tab",
    "unpin_tab",
    "create_new_images_snapshot",
)

# Repository methods behind a coordinated ``Database`` method. Production code
# calls the ``Database`` method instead, so the new-images cache stays right.
_FACADE_ONLY_REPOSITORY_METHODS = ("create", "delete", "get_new_images_snapshot")


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

    assert db_module._UNSET is UNSET
    repo = inspect.signature(WorkspaceRepository.update).parameters
    assert list(repo) == [
        "self", "workspace_id", "name", "config_overrides", "ui_state",
        "last_opened_at", "pinned_at",
    ]
    for field in ("config_overrides", "ui_state", "pinned_at"):
        assert repo[field].default is UNSET
    assert repo["name"].default is None
    assert repo["last_opened_at"].default is None


# -- the ``db.workspaces`` accessor ----------------------------------------------


def test_workspaces_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.workspaces`` builds a new repository each time, never a cached one.

    The workspace is passed as ``Database._ws_id`` itself, uncalled, so
    building the repository resolves nothing.
    """
    first, second = db.workspaces, db.workspaces
    assert isinstance(first, WorkspaceRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.workspace_id_fn == db._ws_id


def test_active_workspace_methods_raise_before_anything_without_a_workspace(db):
    """Tabs and snapshots need a workspace, exactly where the wrappers did.

    Reaching ``db.workspaces`` is fine; each call raises ``RuntimeError``
    before validating its input or running any SQL, as the wrapper's eager
    ``_ws_id()`` did (so a bad nav id still reports the missing workspace).
    """
    ws = db.require_workspace_id()
    db.workspaces.set_tabs(["browse"])
    db.set_active_workspace(None)
    repo = db.workspaces
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        for call in (
            repo.get_tabs,
            lambda: repo.set_tabs(["browse", "review"]),
            lambda: repo.set_tabs("not a list"),
            lambda: repo.pin_tab("logs"),
            lambda: repo.pin_tab("not_a_real_page"),
            lambda: repo.unpin_tab("browse"),
            lambda: repo.create_new_images_snapshot(["/a.jpg"]),
            lambda: repo.get_new_images_snapshot(1),
        ):
            with pytest.raises(RuntimeError, match="No active workspace set"):
                call()
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []
    db.set_active_workspace(ws)
    assert db.workspaces.get_tabs() == ["browse"]


def test_catalog_wide_methods_work_without_a_workspace(db):
    """The row methods take an id or span every workspace; none needs one active."""
    other = db.create_workspace("Other")
    folder = db.add_folder("/ws/linked", link_to_workspace=False)
    db.add_workspace_folder(other, folder)
    db.set_active_workspace(None)
    repo = db.workspaces
    assert repo.get(other)["name"] == "Other"
    assert repo.id_for_name("Other") == other
    assert "Other" in [w["name"] for w in repo.list_all()]
    repo.update(other, name="Renamed", config_overrides={"active_labels": ["/l.txt"]})
    assert repo.forget_label_file("/l.txt") == 1
    repo.set_group_state(other, "fp", 1)
    assert repo.ids_for_folders([folder]) == {other}
    assert repo.most_recently_opened_id() is not None
    assert repo.default_id() is not None
    row = db.workspaces.get(other)
    assert (row["name"], row["last_group_fingerprint"]) == ("Renamed", "fp")


def test_active_workspace_methods_follow_the_workspace_active_at_each_call(db):
    """A switch between two ``db.workspaces`` calls (or two calls on one held
    repository) reads and writes the new workspace."""
    ws = db.require_workspace_id()
    other = db.create_workspace("Other")
    held = db.workspaces
    db.workspaces.set_tabs(["browse"])
    db.set_active_workspace(other)
    db.workspaces.set_tabs(["review"])
    assert held.pin_tab("logs") == ["review", "logs"]
    snap = held.create_new_images_snapshot(["/o.jpg"])
    assert db.get_new_images_snapshot(snap)["workspace_id"] == other
    db.set_active_workspace(ws)
    assert held.get_tabs() == ["browse"]
    assert db.workspaces.get_new_images_snapshot(snap) is None


def test_workspaces_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.workspaces``; Database keeps no aliases."""
    for name in _REMOVED_WORKSPACE_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.workspaces"
    accessor = Database.__dict__["workspaces"]
    assert isinstance(accessor, property)
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(textwrap.dedent(inspect.getsource(accessor.fget))))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_workspace_repository" in attrs
    assert "conn" not in attrs


def test_production_code_creates_and_deletes_workspaces_through_the_facade():
    """Nothing outside the data layer reaches past a kept ``Database`` method."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    pattern = re.compile(
        r"\.workspaces\.(" + "|".join(_FACADE_ONLY_REPOSITORY_METHODS) + r")\("
    )
    offenders = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        if rel.parts[0] in ("tests", "repositories") or rel.name == "db.py":
            continue
        text = path.read_text(encoding="utf-8")
        offenders += [f"{rel}: {m.group(0)}" for m in pattern.finditer(text)]
    assert offenders == [], (
        "Call db.create_workspace / db.delete_workspace / "
        f"db.get_new_images_snapshot instead: {offenders}"
    )
