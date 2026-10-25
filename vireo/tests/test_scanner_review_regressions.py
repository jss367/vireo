"""Scanner boundary regressions using selected source and entirely fake I/O.

No scanner, catalog, image loader, process pool, or filesystem workflow is
imported or started. Only the definitions under test are compiled.
"""
import __future__

import ast
import contextlib
import os
from collections import deque
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest

SCANNER = Path(__file__).resolve().parents[1] / "scanner"


def _selected(module, names, namespace, owner=None):
    source = ast.parse((SCANNER / module).read_text(encoding="utf-8"))
    scope = source.body
    if owner:
        scope = next(n.body for n in scope if isinstance(n, ast.ClassDef) and n.name == owner)
    body = [n for n in scope if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
    assert len(body) == len(names)
    result = dict(namespace)
    exec(compile(ast.Module(body=body, type_ignores=[]), "<inert scanner regression>", "exec",
                 flags=__future__.annotations.compiler_flag), result)
    return result


@pytest.mark.parametrize("boundary", ["window", "first", "pending", "normal", "cancel"])
def test_pool_pause_requeues_and_joins_before_return(boundary):
    class Pause(Exception):
        pass

    events = []
    pool = SimpleNamespace(
        _processes={1: SimpleNamespace(terminate=lambda: events.append("terminate"))},
        submit=lambda *args: events.append("submit") or "invented future",
        shutdown=lambda **kw: events.append(("shutdown", kw)),
    )

    class Workers(int):
        def __mul__(self, other):
            if boundary == "window":
                raise Pause()
            return super().__mul__(other)

    checks = []

    def check():
        checks.append(True)
        if boundary == "first" or (boundary == "pending" and len(checks) == 2):
            raise Pause()
        if boundary == "cancel":
            raise ValueError("invented cancellation")

    ns = _selected("features.py", {"_pool_features"}, {
        "multiprocessing": SimpleNamespace(get_context=lambda _: "fake context"),
        "_SCAN_MP_METHOD": "fake", "ProcessPoolExecutor": lambda **_: pool,
        "deque": deque, "_ScanPauseRequested": Pause, "contextlib": contextlib,
        "_compute_file_features": object(),
    }, "_FeatureHasher")
    queue = deque([(PurePosixPath("/invented/a"), "/invented/a")])
    subject = SimpleNamespace(_check_cancelled_no_park=check, _pause_pending=lambda: False,
                              _feature_result=lambda _: ("fake phash", "fake hash"))
    generator = ns["_pool_features"](subject, Workers(2), queue)
    if boundary == "cancel":
        with pytest.raises(ValueError, match="invented cancellation"):
            list(generator)
    else:
        results = []
        while True:
            try:
                results.append(next(generator))
            except StopIteration as stop:
                assert stop.value is (boundary != "normal")
                break
        assert bool(results) is (boundary == "normal")
        assert bool(queue) is (boundary != "normal")
    assert events[-1] == ("shutdown", {"wait": True, **(
        {} if boundary == "normal" else {"cancel_futures": True})})
    assert ("terminate" in events) is (boundary != "normal")


@pytest.mark.parametrize("leaves", [[], ["/invented/root/leaf"], ["/invented/blocked"]])
def test_restricted_generator_survives_discovery_for_empty_leaf_status(leaves):
    observed = []

    def discovery(*args, **kwargs):
        scope = kwargs["restrict_dirs"]
        observed.extend(scope)  # Model discovery consuming its entire input.
        return SimpleNamespace(effective_restrict_dirs=[x for x in scope if "blocked" not in x])

    ns = _selected("scan_run.py", {"__init__"}, {
        "_ScanSignals": lambda *a: object(), "_ScanTally": lambda *a, **k: object(),
        "_status_callback_supports_phase": lambda _: False,
        "_Discovery": discovery, "_FeatureHasher": lambda **k: object(),
    }, "_ScanRun")
    subject = SimpleNamespace()
    resolved = SimpleNamespace(root_path=PurePosixPath("/invented/root"),
                               restrict_dirs=(x for x in leaves), frozen_files=None,
                               restrict_files=None, skip_paths=None)
    hooks = SimpleNamespace(status=None, progress=None, photo=None, photo_merged=None,
                            permission_error=None, sidecar_error=None)
    control = SimpleNamespace(cancel_check=None, pause_check=None, cancel_only_check=None)
    ns["__init__"](subject, "/invented/root", resolved, object(), {},
                   catalog_folder_path=object(), catalog_folder_aliases=object(),
                   options=SimpleNamespace(recursive=True), hooks=hooks, control=control)
    assert observed == leaves
    assert tuple(subject.restrict_dirs) == tuple(leaves)
    registered, stamps, stats = [], [], []

    class FakePath(PurePosixPath):
        def is_dir(self):
            stats.append(str(self))
            return True

    folders = _selected("folders.py", {"__init__", "register_scan_targets", "update_folder_status"},
                        {"os": os, "Path": FakePath,
                         "is_excluded_scan_path": lambda p: "blocked" in str(p)}, "_ScanFolders")
    db = SimpleNamespace(scanning=SimpleNamespace(
        set_folder_status=lambda *a, **k: stamps.append((a, k))),
        commit_with_retry=lambda: None)
    state = SimpleNamespace(ensure_folder=lambda p: registered.append(str(p)))
    folders["__init__"](state, db, FakePath("/invented/root"), restrict_dirs=subject.restrict_dirs,
                        effective_restrict_dirs=subject.discovery.effective_restrict_dirs,
                        register_restrict_dirs_as_roots=True, catalog_folder_path=object(), folder_aliases={})
    folders["register_scan_targets"](state)
    folders["update_folder_status"](state, "ok", True)
    assert registered == ["/invented/root"] + [x for x in leaves if "blocked" not in x]
    assert state.scoped_paths == {"/invented/root", *leaves}
    assert state._restrict_root_paths == {os.path.normpath(x) for x in leaves if "blocked" not in x}
    assert not any("blocked" in x for x in stats)
    assert stamps  # Zero indexed files still retain the explicit scope for status.


@pytest.mark.parametrize("missing", ["primary", "companion", "both", "neither"])
def test_stale_companion_pair_never_writes_reports_or_cleans(missing):
    events, actions = [], []
    primary = {"id": 1, "folder_id": 5, "filename": "invented.raw"}
    companion = {"id": 2, "folder_id": 5, "filename": "invented.jpg"}
    live = {1: None if missing in {"primary", "both"} else {"rating": 1},
            2: None if missing in {"companion", "both"} else {"rating": 2}}

    @contextlib.contextmanager
    def deletion():
        events.append("enter")
        yield SimpleNamespace(atomic_merge=lambda _photo_id: contextlib.nullcontext())
        events.append("exit")

    def read(photo_id):
        assert events[-1] in {"enter", ("read", 1)}
        events.append(("read", photo_id))
        return live[photo_id]

    def fake_write(*args, **kwargs):
        assert live[1] is not None and live[2] is not None
        events.append("write")

    helpers = {name: fake_write for name in [
        "prepare_photo_merge", "_keep_companion_import_identity", "_fill_primary_metadata_gaps", "_transfer_companion_keywords",
        "_rekey_companion_detections_onto_primary", "_transfer_companion_pending_changes",
        "_transfer_companion_inat_submissions", "_transfer_companion_edit_recipe", "_delete_companion_row",
        "_defer_primary_display_cache_invalidation"]}
    ns = _selected("companions.py", {"_read_metadata_transfer_rows", "_merge_companion_into_primary",
                                        "_pair_raw_jpeg_companions", "_try_automatic_companion_merge", "_MergedPhotoIds"}, {
        **helpers, "cast": lambda _kind, value: value,
        "IdentityMergeConflict": type("IdentityMergeConflict", (ValueError,), {}),
        "log": SimpleNamespace(warning=lambda *args: events.append("warning")),
        "_group_photos_by_folder_and_stem": lambda _: {(5, "invented"): [primary, companion]},
        "_pick_compatible_raw_jpeg_pairs": lambda _: [(primary, companion)],
        "catalog_masks_directory": lambda _: "/invented/masks",
        "_defer_companion_derivative_cleanup": lambda *a, **k: actions.append("cleanup"),
        "_run_post_commit_fs_actions": lambda _: events.append("postcommit"),
    })
    db = SimpleNamespace(companion_pairing=SimpleNamespace(
        row_deletion=deletion, metadata_transfer_row=read, set_companion_path=fake_write),
        commit_with_retry=lambda: events.append("commit"), in_transaction=False, _db_path="/invented/db")
    merged = ns["_pair_raw_jpeg_companions"](db, vireo_dir="/invented/vireo", thumb_cache_dir=None)
    assert dict(merged) == ({2: 1} if missing == "neither" else {})
    assert bool(merged.owners) is (missing == "neither")
    assert bool(merged.companions) is (missing == "neither")
    assert actions == (["cleanup"] if missing == "neither" else [])
    assert ("write" in events) is (missing == "neither")
    assert events[-3:] == ["exit", "commit", "postcommit"]
