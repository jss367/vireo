"""Selected AST; invented SQLite and fake images only. No product imports."""

import ast
import builtins
import contextlib
import io
import math
import posixpath
import re
import secrets
import sqlite3
import types
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def forbidden(*a, **k):
    raise AssertionError("Forbidden product/system I/O")


def load(file, ns, names=None, classes=None, assignments=False):
    nodes = []
    for node in ast.parse((ROOT / file).read_text(encoding="utf-8")).body:
        if isinstance(node, ast.FunctionDef) and (names is None or node.name in names):
            node.decorator_list = []
            nodes.append(node)
        elif assignments and isinstance(node, ast.Assign):
            nodes.append(node)
        elif classes and isinstance(node, ast.ClassDef) and node.name in classes:
            node.body = [x for x in node.body if isinstance(x, ast.FunctionDef) and x.name in classes[node.name]]
            nodes.append(node)
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "<inert-" + file + ">",
            "exec",
            flags=__import__("__future__").annotations.compiler_flag,
        ),
        ns,
    )


def nested(file, ns, names):
    nodes = [
        n
        for n in ast.walk(ast.parse((ROOT / file).read_text(encoding="utf-8")))
        if isinstance(n, ast.FunctionDef) and n.name in names
    ]
    for node in nodes:
        node.decorator_list = []
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            "<inert-route>",
            "exec",
            flags=__import__("__future__").annotations.compiler_flag,
        ),
        ns,
    )


pure = {"math": math, "os": types.SimpleNamespace(path=posixpath), "re": re, "datetime": datetime}
load(
    "best_batch.py",
    pure,
    {
        "_filename_sequence_key",
        "_parse_capture_timestamp",
        "_time_gap_seconds",
        "_filename_sequence_batch",
        "best_batch_scope",
        "limit_batch_rows",
    },
    assignments=True,
)
load("scoring.py", pure, {"_normalize_scoring_config"})
scoring_tree = ast.parse((ROOT / "scoring.py").read_text(encoding="utf-8"))
pure["SCORING_DEFAULTS"] = ast.literal_eval(
    next(
        node.value
        for node in scoring_tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "DEFAULTS" for target in node.targets)
    )
)
load("burst_evidence.py", pure, assignments=True)
# The builder must never import or invoke the real pipeline.
pure["build_best_batch_response"] = lambda *a: (None, "invented unscored")


def pure_existing_tests():
    ns = dict(pure)
    # Explicitly inspected pure controls; never discover fixture/app tests.
    names = {
        "test_group_folder_bursts_chains_filename_sequences_and_splits_on_gaps",
        "test_group_folder_bursts_keeps_extensions_and_prefixes_apart",
        "test_group_folder_bursts_without_timestamps_chains_by_sequence_only",
        "test_group_folder_bursts_unnumbered_names_need_timestamps",
        "test_coverage_counts_each_feature_separately",
        "test_assess_ambiguity_clear_burst_has_no_reasons",
        "test_assess_ambiguity_close_call_compares_raw_sharpness_of_non_rejected_cards",
        "test_assess_ambiguity_close_call_uses_eye_sharpness_when_both_frames_have_it",
        "test_assess_ambiguity_reports_unanalyzed_frames",
        "test_assess_ambiguity_no_eye_only_when_eye_detection_is_on",
        "test_assess_ambiguity_edge_and_multiple_subjects_look_at_the_best_frame",
        "test_assess_ambiguity_all_rejected",
        "test_ambiguity_codes_are_the_documented_set",
    }
    helpers = {"_row", "_evidence", "_card", "_full_coverage"}
    load("tests/test_burst_evidence.py", ns, names | helpers)
    for name in sorted(names):
        ns[name]()
    print("PASS", len(names), "existing pure coverage/ambiguity/grouping controls")


def invented_row(pid, name, seconds):
    return dict(
        id=pid,
        folder_id=1,
        filename=name,
        extension=".jpg",
        timestamp=f"2024-01-01T00:00:{seconds:02}",
        flag="none",
        rating=0,
    )


def compare_scope():
    rows = [invented_row(1, "DSC_0001.jpg", 0), invented_row(2, "IMG_0999.jpg", 1), invented_row(3, "bird.jpg", 2)]
    db = types.SimpleNamespace(
        require_workspace_id=lambda: 1,
        photos=types.SimpleNamespace(
            get=lambda pid, **kw: rows[pid - 1],
            best_batch_sequence_rows=lambda *a: rows,
            best_batch_folder_rows=lambda *a: rows,
        ),
    )
    listing = [[r["id"] for r in b] for b in pure["group_folder_bursts"](rows)]
    scoped, method = pure["best_batch_scope"](db, 1)
    assert listing == [[1, 2, 3]] and [r["id"] for r in scoped] == [1, 2, 3]
    assert method == "capture_time"
    print("PASS numbered-singleton capture-time fallback matches seed scope")


class Args(dict):
    def get(self, key, default=None, type=None):
        value = super().get(key, default)
        if type is not None:
            try:
                return type(value)
            except TypeError, ValueError:
                return default
        return value


class Image:
    NEAREST = 0

    def __init__(self, size=(3000, 2000), events=None, kind="image"):
        self.size, self.mode, self.events, self.kind = size, "RGB", events if events is not None else [], kind

    def crop(self, box):
        self.events.append(("crop", self.kind, box))
        return Image((box[2] - box[0], box[3] - box[1]), self.events, self.kind)

    def convert(self, mode):
        self.events.append(("convert", mode))
        self.mode = mode
        return self

    def resize(self, size, method):
        self.events.append(("resize", size, method))
        return Image(size, self.events, self.kind)

    def point(self, fn):
        self.events.append(("point", self.kind, fn(0), fn(128), fn(255)))
        return self

    def save(self, buf, **kw):
        self.events.append(("save", self.size, kw))
        buf.write(b"fake-jpeg")


def focus(photo=None, args=None, size=(3000, 2000), enabled=True, mask_path=None, detections=None, active_prompt=None, original_size=None):
    events = []
    photo = (
        photo
        if photo is not None
        else dict(id=1, folder_id=1, filename="invented.jpg", eye_x=0.5, eye_y=0.5, eye_tenengrad=40)
    )

    def get_photo(pid, **kw):
        assert kw == {"verify_workspace": True}
        events.append(("gate", pid))
        return photo

    boxes = [dict(category="animal", detector_model="invented", box_x=0.1, box_y=0.2, box_w=0.8, box_h=0.6)]
    cfg = types.SimpleNamespace(load=lambda: {"pipeline": {"eye_detect_enabled": enabled}, "preview_quality": 90})

    def loader(*a, **kw):
        events.append(("loader", kw))
        return (Image(size, events), "working_copy") if kw.get("return_source") else Image(size, events)

    pil = types.SimpleNamespace(
        NEAREST=0,
        open=lambda buf: Image((100, 100), events, "mask"),
        composite=lambda crop, shaded, mask: events.append(("composite",)) or crop,
    )
    helpers = {
        "math": math,
        "detection_store": types.SimpleNamespace(
            get_detections=lambda db, pid: db.get_detections(pid),
        ),
    }
    load("focus_crop.py", helpers)
    modules = {
        "focus_crop": types.SimpleNamespace(**helpers),
        "scoring": types.SimpleNamespace(_normalize_scoring_config=pure["_normalize_scoring_config"]),
        "config": cfg,
        "burst_evidence": types.SimpleNamespace(
            DEFAULT_FOCUS_CROP_SIZE=512,
            MIN_FOCUS_CROP_SIZE=128,
            MAX_FOCUS_CROP_SIZE=1024,
            numeric_query_arg=pure["numeric_query_arg"],
        ),
        "image_loader": types.SimpleNamespace(
            load_working_image=loader,
            load_image=lambda path, max_size=None: events.append(("original", path)) or Image(original_size or size, events, "original"),
        ),
        "PIL": types.SimpleNamespace(Image=pil),
        "io": io,
    }

    def response(data, **kw):
        return types.SimpleNamespace(data=data, headers={}, cache_control=types.SimpleNamespace())

    ns = {
        "__builtins__": {
            **vars(builtins),
            "__import__": lambda name, *a, **kw: modules[name] if name in modules else forbidden(),
        },
        "os": types.SimpleNamespace(
            path=types.SimpleNamespace(
                dirname=posixpath.dirname, join=posixpath.join, realpath=posixpath.normpath,
                isfile=lambda path: original_size is not None,
            ),
            sep="/",
        ),
        "config": {"THUMB_CACHE_DIR": "/invented/cache/thumbs"},
        "db_path": "/invented/catalog/db.sqlite",
        "request": types.SimpleNamespace(args=Args(args or {})),
        "Response": response,
        "json_error": lambda msg, status=400: (msg, status),
        "_read_mask_bytes": lambda path: events.append(("mask-read", path)) or b"fake-mask",
    }
    db = types.SimpleNamespace(
        photos=types.SimpleNamespace(get=get_photo),
        get_detections=lambda pid: boxes if detections is None else detections,
        folders=types.SimpleNamespace(get=lambda fid: {"path": "/invented"}),
        get_effective_config=lambda cfg: cfg,
        masks_features=types.SimpleNamespace(
            photo_mask_path=lambda pid: mask_path, active_prompt_detection=lambda pid: active_prompt or (boxes[0] if mask_path else None)
        ),
    )
    ns["get_db"] = lambda: db
    nested("web/media.py", ns, {"serve_focus_crop", "_focus_mask_bytes", "_focus_source_image", "_validated_focus_source", "_focus_geometry_error"})
    return ns["serve_focus_crop"](1), events


def focus_checks():
    res, events = focus(args={"size": "256"})
    assert res.headers["X-Vireo-Focus-Window"] == "1372,872,1628,1128"
    assert res.headers["X-Vireo-Focus-Scale"] == "1"
    assert not any(e[0] == "resize" for e in events)
    assert next(e for e in events if e[0] == "loader")[1]["max_size"] is None
    # Upright portrait returned by loader: use returned geometry, not cached landscape dimensions.
    photo = dict(
        id=1, folder_id=1, filename="invented.jpg", eye_x=0.25, eye_y=0.75, eye_tenengrad=40, width=3000, height=2000
    )
    res, _ = focus(photo=photo, args={"size": "256", "center": "0.25,0.75"}, size=(2000, 3000))
    assert res.headers["X-Vireo-Focus-Window"] == "372,2122,628,2378"
    res, _ = focus(args={"size": "1024"}, size=(900, 600))
    assert res.headers["X-Vireo-Focus-Window"] == "0,0,900,600"
    photo["eye_x"] = photo["eye_y"] = None
    res, _ = focus(photo=photo, args={"size": "128"})
    assert res.headers["X-Vireo-Focus-Basis"] == "subject"
    assert res.headers["X-Vireo-Focus-Window"] == "1436,936,1564,1064"
    res, events = focus(args={"mask": "1"}, mask_path="/invented/catalog/masks/1.png")
    assert res.headers["X-Vireo-Focus-Mask"] == "1"
    assert ("resize", (3000, 2000), 0) in events
    assert ("point", "mask", 0, 255, 255) in events
    assert ("point", "image", 0, 44, 89) in events
    res, events = focus(args={"mask": "1"}, mask_path="/elsewhere/mask.png")
    assert res.headers["X-Vireo-Focus-Mask"] == "0" and not any(e[0] == "mask-read" for e in events)
    for args in [{"size": "12"}, {"basis": "nose"}]:
        res, events = focus(args=args)
        assert res[1] == 400 and not any(e[0] == "loader" for e in events)
    res, events = focus(photo={})
    assert res[1] == 404 and events == [("gate", 1)]
    print("PASS fake native-window/orientation/size/mask/path/gating checks")
    res, _ = focus(enabled=False)
    assert res.headers["X-Vireo-Focus-Basis"] == "subject"
    assert res.headers["X-Vireo-Focus-Scale-Reference"] == "decoded-raster"
    assert res.headers["X-Vireo-Focus-Original-Scale"] == "unknown"
    assert res.headers["X-Vireo-Focus-Source"] == "working_copy"
    assert res.headers["X-Vireo-Focus-Raster-Size"] == "3000,2000"
    no_measurement = dict(id=1, folder_id=1, filename="invented.jpg", eye_x=0.5, eye_y=0.5, eye_tenengrad=None)
    res, _ = focus(photo=no_measurement, enabled=True)
    assert res.headers["X-Vireo-Focus-Basis"] == "subject"
    res, _ = focus(enabled=False, args={"basis": "eye"})
    assert res[1] == 404
    res, _ = focus(photo=dict(id=1, folder_id=1, filename="invented.jpg", eye_x=math.nan, eye_y=0.5, eye_tenengrad=40))
    assert res.headers["X-Vireo-Focus-Basis"] == "subject"
    print("PASS workspace eye scoring, explicit override, finite coordinates and raster provenance")


def rows_checks():
    ns = {}
    load(
        "repositories/photos.py",
        ns,
        names=set(),
        classes={
            "PhotoRepository": {
                "best_batch_rows_by_ids",
                "best_batch_sequence_rows",
                "best_batch_folder_rows",
                "count_folder_photos",
                "analysis_evidence_rows",
                "_chunks",
            }
        },
    )
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""CREATE TABLE folders(id,status);CREATE TABLE photos(id,folder_id,filename,extension,timestamp,flag,rating,quality_score,sharpness,mask_path,subject_tenengrad,eye_tenengrad,crop_complete,subject_size);CREATE TABLE photo_workspace_visibility(photo_id,workspace_id);CREATE TABLE detections(photo_id,category,detector_model,detector_confidence);
    INSERT INTO folders VALUES(1,'ok'),(2,'offline'),(3,'partial');
    INSERT INTO photos VALUES(1,1,'DSC_01.jpg','.jpg',NULL,'none',0,0,0,'mask',10,20,1,.1),(2,1,'DSC_02.jpg','.jpg',NULL,'none',0,0,0,NULL,NULL,NULL,NULL,NULL),(3,2,'DSC_03.jpg','.jpg',NULL,'none',0,0,0,NULL,NULL,NULL,NULL,NULL),(4,3,'DSC_04.jpg','.jpg',NULL,'none',0,0,0,NULL,NULL,NULL,NULL,NULL);
    INSERT INTO photo_workspace_visibility VALUES(1,1),(2,2),(3,1),(4,1);
    INSERT INTO detections VALUES(1,'animal','detector',.9),(1,'animal','detector',.1),(1,'animal','full-image',1),(1,'person','detector',.9);""")
    c.executescript("""ALTER TABLE photos ADD COLUMN active_mask_variant;
    CREATE TABLE photo_masks(photo_id,variant,path);
    UPDATE photos SET active_mask_variant='invented-v1' WHERE id=1;
    INSERT INTO photo_masks VALUES(1,'invented-v1','mask'),(1,'stale','other-mask'),(2,'invented-v1','mask');""")
    r = ns["PhotoRepository"]()
    r.conn = c
    r.workspace_id = 1
    r.chunk_size = 2
    assert [x["id"] for x in r.best_batch_rows_by_ids([1, 2, 3, 4])] == [1, 4]
    assert [x["id"] for x in r.best_batch_folder_rows(1)] == [1]
    assert [x["id"] for x in r.best_batch_sequence_rows(1, "DSC_")] == [1]
    assert r.count_folder_photos(1) == 1
    row = r.analysis_evidence_rows([1, 2, 3, 4], 0.2)[1]
    assert row["mask_variant"] == "invented-v1"
    c.execute("UPDATE photo_masks SET path='changed' WHERE photo_id=1 AND variant='invented-v1'")
    assert r.analysis_evidence_rows([1], 0.2)[1]["mask_variant"] is None
    c.execute("UPDATE photo_masks SET path='mask' WHERE photo_id=1 AND variant='invented-v1'")
    assert (row["has_mask"], row["has_quality"], row["has_eye"], row["detection_count"]) == (1, 1, 1, 1)
    c.execute(
        "INSERT INTO photos SELECT 5,folder_id,filename,extension,timestamp,flag,rating,quality_score,sharpness,mask_path,subject_tenengrad,eye_tenengrad,crop_complete,subject_size,active_mask_variant FROM photos WHERE id=1"
    )
    c.execute("INSERT INTO photo_workspace_visibility VALUES(5,1)")
    assert len(r.best_batch_folder_rows(1, limit=1)) == 1
    assert len(r.best_batch_folder_rows(1)) == 2
    c.close()
    print("PASS invented SQLite workspace/online/chunked evidence/confidence/category/fallback controls")


def malformed_and_bound():
    numeric = pure["numeric_query_arg"]
    cases = [
        ("margin", float, 0, 1, ["abc", "", "nan", "inf", "-inf", "-1", "1.1"]),
        ("focus_size", int, 128, 1024, ["abc", "", "128.0", "127", "1025"]),
        ("size", int, 128, 1024, ["abc", "", "128.0", "127", "1025"]),
        ("gap_seconds", float, 0, 120, ["abc", "", "nan", "inf", "-1", "121"]),
        ("max_photos", int, 2, 500, ["abc", "", "2.0", "1", "501"]),
    ]
    for name, convert, lo, hi, invalid in cases:
        assert numeric(Args(), name, lo, convert, lo, hi) == lo
        assert numeric(Args({name: str(hi)}), name, lo, convert, lo, hi) == hi
        for value in invalid:
            try:
                numeric(Args({name: value}), name, lo, convert, lo, hi)
            except ValueError:
                pass
            else:
                raise AssertionError((name, value))
    ns = dict(
        pure,
        json_error=lambda msg, status=400: (msg, status),
        get_db=lambda: object(),
        request=types.SimpleNamespace(args=Args()),
        best_batch_scope=forbidden,
    )
    nested("web/photos.py", ns, {"_evidence_pack_args", "api_photo_burst_evidence"})
    for name in ["margin", "focus_size", "gap_seconds", "max_photos"]:
        ns["request"].args = Args({name: "abc"})
        assert ns["api_photo_burst_evidence"](1)[1] == 400
    for value in ["abc", "", "128.0"]:
        result, events = focus(args={"size": value})
        assert result[1] == 400 and not any(e[0] == "loader" for e in events)
    folder_db = types.SimpleNamespace(
        folders=types.SimpleNamespace(get=lambda fid: {"id": fid, "path": "/invented", "name": "invented"}),
        active_workspace_id=1,
        workspace_folders=types.SimpleNamespace(visible_ids=lambda ws, ids: ids),
    )
    folder_ns = dict(
        pure,
        get_db=lambda: folder_db,
        request=types.SimpleNamespace(args=Args({"margin": "abc"})),
        json_error=lambda msg, status=400: (msg, status),
    )
    nested("web/folders.py", folder_ns, {"api_folder_bursts"})
    assert folder_ns["api_folder_bursts"](1)[1] == 400
    events = []

    def bounded_rows(fid, **kwargs):
        events.append(kwargs)
        return [None] * 5001

    db = types.SimpleNamespace(photos=types.SimpleNamespace(best_batch_folder_rows=bounded_rows))
    with contextlib.suppress(ValueError):
        pure["build_folder_bursts"](db, 1, effective_cfg={})
    assert events == [{"limit": 5001}]
    print("PASS strict finite numeric query parsing, route refusal before scope/image work and bounded folder read")


def alias_and_hooks():
    tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))
    aliases = ast.literal_eval(
        next(
            n.value
            for n in tree.body
            if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "_V1_ALIASES" for t in n.targets)
        )
    )
    views = {endpoint: object() for _, endpoint, _ in aliases}
    rules = []
    app = types.SimpleNamespace(view_functions=views, add_url_rule=lambda url, **kw: rules.append((url, kw)))
    ns = {"_V1_ALIASES": aliases}
    load("app.py", ns, {"_register_v1_aliases"})
    ns["_register_v1_aliases"](app, set())
    targets = {
        "/api/v1/photos/<int:photo_id>/burst-evidence",
        "/api/v1/photos/burst-evidence",
        "/api/v1/photos/<int:photo_id>/focus-crop",
        "/api/v1/folders/<int:folder_id>/bursts",
    }
    assert targets <= {url for url, _ in rules}
    for url, rule in rules:
        assert rule["view_func"] is views[next(endpoint for path, endpoint, _ in aliases if path == url)]
    request = types.SimpleNamespace(path="", method="POST", is_json=True, headers={}, get_json=lambda **kw: [1, 2])
    app.config = {"API_TOKEN": "invented-token"}
    ns = {"app": app, "request": request, "secrets": secrets, "json_error": lambda msg, status=400, **kw: (msg, status)}
    load("web/app_hooks.py", ns, {"_reject_non_object_json_body"})
    nested("web/app_hooks.py", ns, {"_enforce_api_v1_token"})
    for url in targets:
        request.path = url
        assert ns["_enforce_api_v1_token"]()[1] == 401
        request.headers = {"X-Vireo-Token": "invented-token"}
        assert ns["_enforce_api_v1_token"]() is None
        assert ns["_reject_non_object_json_body"]()[1] == 400
        request.headers = {}
    print("PASS all four aliases preserve view identity, token gating and non-object JSON hook")


def scoring_settings():
    settings = pure["_scoring_settings"](
        {
            "w_focus": 0.2,
            "reject_focus": 0.1,
            "pipeline": {"w_focus": 0.8, "reject_focus": 0.6, "eye_detect_enabled": True},
        },
        0.25,
    )
    assert "weights" in settings and "rejection_thresholds" in settings
    assert settings["weights"]["w_focus"] == 0.8
    assert settings["rejection_thresholds"]["reject_focus"] == 0.6
    assert settings["weights"]["w_exposure"] == pure["SCORING_DEFAULTS"]["w_exposure"]
    assert set(settings["weights"]) == {k for k in pure["SCORING_DEFAULTS"] if k.startswith("w_")}
    assert set(settings["rejection_thresholds"]) == {k for k in pure["SCORING_DEFAULTS"] if k.startswith("reject_")}
    assert settings["eye_detect_enabled"] is True and settings["close_call_margin"] == 0.25
    print("PASS complete effective weights and rejection thresholds with nested precedence")


def overlapping_scopes():
    rows = [invented_row(1, "DSC_0001.jpg", 0), invented_row(2, "DSC_0002.jpg", 1), invented_row(3, "bird.jpg", 2)]
    scopes = pure["folder_burst_scopes"](rows)
    db = types.SimpleNamespace(
        require_workspace_id=lambda: 1,
        photos=types.SimpleNamespace(
            get=lambda pid, **kw: rows[pid - 1],
            best_batch_sequence_rows=lambda *a: rows,
            best_batch_folder_rows=lambda *a: rows,
        ),
    )
    for scope in scopes:
        expected, method = pure["best_batch_scope"](db, scope["seed_photo_id"])
        assert [r["id"] for r in expected] == [r["id"] for r in scope["rows"]]
        assert method == scope["scope_method"]
    assert [scope["seed_photo_id"] for scope in scopes] == [1, 3]
    evidence = {
        r["id"]: dict(
            id=r["id"], has_mask=0, has_quality=0, has_eye=0, detection_count=0, crop_complete=None, subject_size=None
        )
        for r in rows
    }
    db.photos.best_batch_folder_rows = lambda *a, **kw: rows
    db.photos.analysis_evidence_rows = lambda *a: evidence
    payload = pure["build_folder_bursts"](db, 1, effective_cfg={})
    assert payload["burst_count"] == 2 and payload["photos_in_bursts"] == 3
    assert [burst["seed_photo_id"] for burst in payload["bursts"]] == [1, 3]
    print("PASS overlapping fallback preserves each seed scope and counts photos once")


def pack_checks():
    rows = [invented_row(1, "DSC_0001.jpg", 0), invented_row(2, "DSC_0002.jpg", 1)]
    evidence = {
        1: dict(id=1, has_mask=1, has_quality=1, has_eye=0, detection_count=2, crop_complete=0.5, subject_size=0.1)
    }
    db = types.SimpleNamespace(photos=types.SimpleNamespace(analysis_evidence_rows=lambda ids, floor: evidence))
    unscored = pure["build_evidence_pack"](db, 1, rows, effective_cfg={})
    assert unscored["scored"] is False and unscored["best_photo_id"] is None
    assert unscored["coverage"]["photos"] == 2 and unscored["coverage"]["with_quality"] == 1
    assert any(r["code"] == "unanalyzed_frames" for r in unscored["ambiguity"]["reasons"])
    pure["build_best_batch_response"] = lambda *a: (
        dict(
            cards=[dict(id=1, label="KEEP", filename="invented", sharpness=80, focus_basis="subject")], best_photo_id=1
        ),
        None,
    )
    scored = pure["build_evidence_pack"](
        db, 1, rows, effective_cfg={"pipeline": {"eye_detect_enabled": True}}, focus_size=256
    )
    assert scored["scored"] is True and scored["cards"][0]["detection_count"] == 2
    assert scored["cards"][0]["focus_crop_url"] == "/photos/1/focus-crop?size=256"
    codes = {r["code"] for r in scored["ambiguity"]["reasons"]}
    assert codes == {"unanalyzed_frames", "no_eye_located", "subject_touches_edge", "multiple_subjects"}
    print("PASS scored/unscored pack coverage, ambiguity and URL attachment with fake scorer")


def test_rescued_active_mask_prompt_drives_focus_crop():
    photo = dict(id=1, folder_id=1, filename="invented.jpg", eye_x=None, eye_y=None, eye_tenengrad=None)
    weak = dict(
        id=7,
        category="animal",
        detector_model="megadetector-v6",
        detector_confidence=0.15,
        box_x=0.6,
        box_y=0.3,
        box_w=0.2,
        box_h=0.2,
    )
    for basis in ["auto", "subject"]:
        result, _ = focus(photo=photo, args={"basis": basis}, detections=[], active_prompt=weak)
        assert not isinstance(result, tuple), result
        assert result.headers["X-Vireo-Focus-Basis"] == "subject"
        assert result.headers["X-Vireo-Focus-Center"] == "0.7000,0.4000"
    print("PASS rescued active-mask prompt centers auto and subject crops below the normal floor")


def test_active_prompt_requires_current_variant_path_model_and_geometry():
    ns = {}
    load("repositories/masks_features.py", ns, classes={"MasksFeaturesRepository": {"active_prompt_detection"}})
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    try:
        c.executescript("""
        CREATE TABLE photos(id,active_mask_variant,mask_path);
        CREATE TABLE photo_masks(photo_id,variant,path,detector_model,prompt_x,prompt_y,prompt_w,prompt_h);
        CREATE TABLE detections(id,photo_id,detector_model,category,detector_confidence,box_x,box_y,box_w,box_h);
        INSERT INTO photos VALUES(1,'v1','/invented/mask');
        INSERT INTO photo_masks VALUES(1,'v1','/invented/mask','megadetector-v6',.6,.3,.2,.2);
        INSERT INTO detections VALUES(7,1,'megadetector-v6','animal',.15,.6,.3,.2,.2),
            (8,1,'other-model','animal',.9,.6,.3,.2,.2);
        """)
        repo = ns["MasksFeaturesRepository"]()
        repo.conn = c
        assert repo.active_prompt_detection(1)["id"] == 7
        assert repo.active_prompt_detection(999) is None
        for sql in [
            "UPDATE photos SET active_mask_variant='old'",
            "UPDATE photos SET mask_path='/invented/other-mask'",
            "UPDATE photo_masks SET detector_model='missing-model'",
            "UPDATE photo_masks SET prompt_x=.5",
            "UPDATE detections SET category='person' WHERE id=7",
            "UPDATE detections SET detector_model='full-image' WHERE id=7",
        ]:
            c.execute(sql)
            assert repo.active_prompt_detection(1) is None
            c.rollback()
    finally:
        c.close()
    print("PASS invented SQLite active-mask identity checks reject stale and unrelated weak boxes")


def test_long_folder_scope_matches_default_seed_evidence_window():
    rows = [dict(invented_row(i, f"DSC_{i:04d}.jpg", 0), timestamp=None) for i in range(1, 201)]
    evidence = {
        r["id"]: dict(
            id=r["id"], has_mask=1, has_quality=1, has_eye=0, detection_count=1, crop_complete=1, subject_size=0.1
        )
        for r in rows
    }
    db = types.SimpleNamespace(
        require_workspace_id=lambda: 1,
        photos=types.SimpleNamespace(
            get=lambda pid, **kw: rows[pid - 1],
            best_batch_sequence_rows=lambda *a: rows,
            best_batch_folder_rows=lambda *a, **kw: rows,
            analysis_evidence_rows=lambda *a: evidence,
        ),
    )
    previous = pure["build_best_batch_response"]

    def ranked(db, seed, selected):
        return dict(
            photo_ids=[r["id"] for r in selected], count=len(selected), cards=[], best_photo_id=selected[-1]["id"]
        ), None

    pure["build_best_batch_response"] = ranked
    try:
        selected, _ = pure["best_batch_scope"](db, 1)
        expected = pure["build_evidence_pack"](db, 1, selected, effective_cfg={})
        folder = pure["build_folder_bursts"](db, 1, effective_cfg={})
        entry = folder["bursts"][0]
        assert entry["photo_ids"] == expected["photo_ids"]
        assert entry["count"] == 120 and entry["best_photo_id"] == expected["best_photo_id"]
        assert entry["scope_max_photos"] == 120 and entry["burst_photo_count"] == 200
        assert folder["photos_in_bursts"] == 200
    finally:
        pure["build_best_batch_response"] = previous
    print("PASS 200-frame folder chain scores the same 120-frame window as its default seed pack")


def test_existing_pure_controls():
    pure_existing_tests()


def test_capture_time_fallback():
    compare_scope()


def test_focus_geometry_masks_and_scoring():
    focus_checks()


def test_repository_scoping_and_limit():
    rows_checks()


def test_numeric_queries_and_bounded_work():
    malformed_and_bound()


def test_alias_auth_and_json_hooks():
    alias_and_hooks()


def test_effective_scoring_settings():
    scoring_settings()


def test_overlapping_seed_scopes():
    overlapping_scopes()


def test_evidence_pack_states():
    previous = pure["build_best_batch_response"]
    try:
        pack_checks()
    finally:
        pure["build_best_batch_response"] = previous





def test_forced_working_source_cannot_decode_original():
    events = []
    ns = {"os": types.SimpleNamespace(path=types.SimpleNamespace(join=posixpath.join, exists=lambda path: False)),
          "load_image": forbidden, "_load_working_copy_image": forbidden}
    load("image_loader.py", ns, {"load_working_image"})
    assert ns["load_working_image"]({"working_copy_path": "invented-missing", "folder_id": 1, "filename": "invented.raw"},
                                     "/invented", folders=None, return_source=True) == (None, "original")
    helper_ns = {"os": types.SimpleNamespace(path=posixpath), "config": {"THUMB_CACHE_DIR": "/invented/thumbs"}}
    modules = {"focus_crop": types.SimpleNamespace(prefer_original=forbidden),
               "image_loader": types.SimpleNamespace(load_image=forbidden,
                   load_working_image=lambda *a, **kw: events.append(kw) or (None, "original"))}
    helper_ns["__builtins__"] = {**vars(builtins), "__import__": lambda name, *a, **kw: modules[name]}
    nested("web/media.py", helper_ns, {"_focus_source_image"})
    assert helper_ns["_focus_source_image"](
        helper_ns["config"], {"folder_id": 1, "filename": "invented.raw"}, {"path": "/invented"}, "working",
    ) == (None, "original")
    assert events[0]["folders"] is None
    print("PASS forced working copy never resolves or decodes original on missing-copy fallback")


def test_focus_center_and_original_source_with_fake_images():
    for center in ("nan,0.5", "0.5,inf", "-0.1,0.5", "0.5", "0.5,0.5,0.5"):
        result, events = focus(args={"center": center})
        assert result[1] == 400 and not any(e[0] == "loader" for e in events)
    result, _ = focus(args={"source": "original"})
    assert result[1] == 404
    result, events = focus(args={"source": "original", "center": "0.25,0.75"}, original_size=(6000, 4000))
    assert any(e[0] == "original" for e in events)
    assert not any(e[0] == "loader" for e in events)
    assert result.headers["X-Vireo-Focus-Basis"] == "point"
    assert result.headers["X-Vireo-Focus-Source"] == "original"
    result, events = focus(args={"source": "working"}, original_size=(6000, 4000))
    assert not any(e[0] == "original" for e in events)
    assert result.headers["X-Vireo-Focus-Source"] == "working_copy"
    print("PASS center rejection/override and explicit original/working source with fake paths and rasters")


def test_all_hard_rejected_survives_rarity_promotion():
    coverage = dict(photos=2, with_quality=2)
    protected = dict(label="REVIEW", reject_reasons=["out_of_focus (F=0.1 < 0.35)"], sharpness=80)
    rejected = dict(label="REJECT", reject_reasons=["no_subject_mask"], sharpness=40)
    def codes(cards):
        return {r["code"] for r in pure["assess_ambiguity"](cards, coverage, eye_detect_enabled=False)["reasons"]}
    assert "all_rejected" in codes([protected, rejected])
    assert "all_rejected" in codes([protected])
    assert "all_rejected" not in codes([protected, dict(label="REVIEW", reject_reasons=[])])
    assert "all_rejected" not in codes([dict(label="KEEP", reject_reasons=[])])
    assert "all_rejected" not in codes([])
    print("PASS hard-reject warning survives rarity promotion; clean REVIEW/KEEP and empty packs stay clear")



def test_full_mask_urls_require_verified_active_variant():
    urls = pure["card_urls"]
    assert urls(7, has_mask=True, mask_variant="invented-v1")["mask_url"] == "/api/masks/7/invented-v1.png"
    for variant in (None, "", "../other", "a/b", "x?y", "x%2fy"):
        assert urls(7, has_mask=True, mask_variant=variant)["mask_url"] is None
    assert urls(7, has_mask=False, mask_variant="invented-v1")["mask_url"] is None
    assert pure["_attach_evidence"]({"id": 7}, None, 512)["mask_url"] is None
    evidence = dict(has_mask=True, has_quality=False, has_eye=False, detection_count=0,
                    crop_complete=None, subject_size=None, mask_variant="invented-v1")
    assert pure["_attach_evidence"]({"id": 7}, evidence, 512)["mask_url"] == "/api/masks/7/invented-v1.png"
    print("PASS active-mask URL identity, unavailable/null and variant path-component guards without mask reads")


if __name__ == "__main__":
    for name, check in list(globals().items()):
        if name.startswith("test_"):
            check()
