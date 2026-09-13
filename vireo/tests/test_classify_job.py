import dataclasses
import json

import pytest
from classify_job import ClassifyParams, run_classify_job
from PIL import Image


def test_classify_params_is_dataclass():
    """ClassifyParams is a dataclass with all required fields."""
    assert dataclasses.is_dataclass(ClassifyParams)
    fields = {f.name for f in dataclasses.fields(ClassifyParams)}
    assert fields == {
        "collection_id",
        "labels_file",
        "labels_files",
        "model_id",
        "model_name",
        "grouping_window",
        "similarity_threshold",
        "reclassify",
    }


def test_run_classify_job_is_callable():
    """run_classify_job exists and is callable."""
    assert callable(run_classify_job)


# ── Task 2: _load_taxonomy and _load_labels tests ──────────────────────────


class FakeRunner:
    """Minimal runner that records push_event calls."""

    def __init__(self):
        self.events = []
        self.cancelled = False
        self.steps = []

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        pass

    def update_step(self, job_id, step_id, **kwargs):
        self.steps.append((step_id, kwargs))

    def is_cancelled(self, job_id):
        return self.cancelled


def _make_job(job_id="classify-test"):
    return {
        "id": job_id,
        "progress": {"current": 0, "total": 0, "current_file": "", "rate": 0},
        "errors": [],
    }


def test_taxonomy_loads_when_file_exists(tmp_path):
    """Phase 1: taxonomy.json is loaded when present."""
    tax_data = {
        "last_updated": "2024-01-01",
        "taxa_by_common": {
            "northern cardinal": {
                "taxon_id": 9083,
                "scientific_name": "Cardinalis cardinalis",
                "common_name": "Northern Cardinal",
                "rank": "species",
                "lineage_names": [
                    "Animalia", "Chordata", "Aves",
                    "Passeriformes", "Cardinalidae", "Cardinalis",
                    "Cardinalis cardinalis",
                ],
                "lineage_ranks": [
                    "kingdom", "phylum", "class",
                    "order", "family", "genus", "species",
                ],
            }
        },
        "taxa_by_scientific": {},
    }
    tax_path = tmp_path / "taxonomy.json"
    tax_path.write_text(json.dumps(tax_data))

    from classify_job import _load_taxonomy

    tax = _load_taxonomy(str(tax_path))
    assert tax is not None
    assert tax.taxa_count >= 1


def test_taxonomy_returns_none_when_missing(tmp_path):
    """Phase 1: returns None when taxonomy.json doesn't exist."""
    from classify_job import _load_taxonomy

    tax = _load_taxonomy(str(tmp_path / "nonexistent.json"))
    assert tax is None


def test_load_labels_from_file(tmp_path):
    """Phase 2: labels loaded from a single file path."""
    labels_file = tmp_path / "labels.txt"
    labels_file.write_text("Northern Cardinal\nBlue Jay\nAmerican Robin\n")

    from classify_job import _load_labels

    labels, use_tol, label_metas = _load_labels(
        model_type="bioclip",
        model_str="hf-hub:imageomics/bioclip",
        labels_file=str(labels_file),
        labels_files=None,
    )
    assert labels == ["Northern Cardinal", "Blue Jay", "American Robin"]
    assert use_tol is False
    assert label_metas == [{"labels_file": str(labels_file)}]


def test_load_labels_tol_fallback(tmp_path):
    """Phase 2: Tree of Life mode when no labels and the model's ToL
    artifacts are installed on disk. Passing model_dir now gates
    label-free fallback on the artifacts being present — a bioclip-2.5
    install whose optional ToL files were skipped must NOT be routed to
    ToL mode (see test_load_labels_raises_when_tol_artifacts_missing)."""
    from unittest.mock import patch

    from classify_job import _load_labels

    (tmp_path / "tol_embeddings.npy").write_bytes(b"stub")
    (tmp_path / "tol_classes.json").write_bytes(b"[]")

    # Mock get_active_labels to return empty so we fall through to ToL
    with patch("classify_job.get_active_labels", return_value=[]):
        labels, use_tol, label_metas = _load_labels(
            model_type="bioclip",
            model_str="hf-hub:imageomics/bioclip",
            labels_file=None,
            labels_files=None,
            model_dir=str(tmp_path),
        )
    assert labels is None
    assert use_tol is True
    assert label_metas == []


def test_load_labels_raises_when_tol_artifacts_missing(tmp_path):
    """Regression: a ToL-supported model whose artifacts are absent must
    NOT be routed to Classifier(labels=None) — the raise here is what
    stops the pipeline from crashing later with FileNotFoundError inside
    the Classifier constructor.  Concrete scenario: bioclip-2.5 install
    from before the HF ToL upload landed, or after a skipped optional
    download."""
    from unittest.mock import patch

    from classify_job import _load_labels

    # tmp_path exists but has neither tol_embeddings.npy nor tol_classes.json.
    with patch("classify_job.get_active_labels", return_value=[]):
        with pytest.raises(RuntimeError, match="Tree of Life files"):
            _load_labels(
                model_type="bioclip",
                model_str="hf-hub:imageomics/bioclip-2.5-vith14",
                labels_file=None,
                labels_files=None,
                model_dir=str(tmp_path),
            )


def test_load_labels_timm_skips():
    """Phase 2: timm models skip label loading entirely."""
    from classify_job import _load_labels

    labels, use_tol, label_metas = _load_labels(
        model_type="timm",
        model_str="hf-hub:timm/some_model",
        labels_file=None,
        labels_files=None,
    )
    assert labels is None
    assert use_tol is False
    assert label_metas == []


def test_load_labels_raises_when_no_labels_unsupported_model():
    """Phase 2: raises RuntimeError when no labels and model doesn't support ToL."""
    from unittest.mock import patch

    from classify_job import _load_labels

    # Mock get_active_labels to return empty
    with patch("classify_job.get_active_labels", return_value=[]):
        with pytest.raises(RuntimeError, match="No labels available"):
            _load_labels(
                model_type="bioclip",
                model_str="hf-hub:some/unsupported-model",
                labels_file=None,
                labels_files=None,
            )


# ── describe_label_source: naming the label space on the Jobs page ─────────


def _params(labels_file=None, labels_files=None):
    return ClassifyParams(
        collection_id="1",
        labels_file=labels_file,
        labels_files=labels_files,
        model_id=None,
        model_name=None,
        grouping_window=60,
        similarity_threshold=0.5,
        reclassify=False,
    )


class _StubDB:
    """Minimal stand-in exposing only the workspace label lookup."""

    def __init__(self, active_labels=None):
        self._active_labels = active_labels

    def get_workspace_active_labels(self):
        return self._active_labels


def test_describe_label_source_names_workspace_sets():
    """Two active lists: the row names the species count and both lists."""
    from unittest.mock import patch

    from classify_job import describe_label_source

    saved = [
        {"labels_file": "/l/ca.txt", "name": "California, US Birds"},
        {"labels_file": "/l/wa.txt", "name": "Washington, US Birds"},
    ]
    db = _StubDB(["/l/ca.txt", "/l/wa.txt"])
    with patch("classify_job.get_saved_labels", return_value=saved):
        text = describe_label_source(
            _params(), db,
            labels=["a"] * 1327, use_tol=False, model_type="bioclip",
        )
    assert text == (
        "1,327 species from 2 lists: California, US Birds, Washington, US Birds"
    )


def test_describe_label_source_single_set(tmp_path):
    from unittest.mock import patch

    from classify_job import describe_label_source

    labels_file = tmp_path / "ca.txt"
    labels_file.write_text("Robin\n")
    saved = [{"labels_file": str(labels_file), "name": "California, US Birds"}]
    with patch("classify_job.get_saved_labels", return_value=saved):
        text = describe_label_source(
            _params(labels_file=str(labels_file)), _StubDB(),
            labels=["a"] * 812, use_tol=False, model_type="bioclip",
        )
    assert text == "812 species from California, US Birds"


def test_describe_label_source_truncates_many_sets(tmp_path):
    """More than three lists: name the first three and count the rest."""
    from unittest.mock import patch

    from classify_job import describe_label_source

    paths = []
    for i in range(5):
        p = tmp_path / f"{i}.txt"
        p.write_text("Robin\n")
        paths.append(str(p))
    saved = [{"labels_file": p, "name": f"List {i}"} for i, p in enumerate(paths)]
    with patch("classify_job.get_saved_labels", return_value=saved):
        text = describe_label_source(
            _params(labels_files=paths), _StubDB(),
            labels=["a"] * 40, use_tol=False, model_type="bioclip",
        )
    assert text == "40 species from 5 lists: List 0, List 1, List 2 and 2 more"


def test_describe_label_source_falls_back_to_filename(tmp_path):
    """An ad-hoc file with no saved metadata is named by its filename."""
    from unittest.mock import patch

    from classify_job import describe_label_source

    labels_file = tmp_path / "my_species.txt"
    labels_file.write_text("Robin\n")
    with patch("classify_job.get_saved_labels", return_value=[]):
        text = describe_label_source(
            _params(labels_file=str(labels_file)), _StubDB(),
            labels=["a", "b"], use_tol=False, model_type="bioclip",
        )
    assert text == "2 species from my_species.txt"


def test_describe_label_source_tree_of_life():
    """ToL mode must say so — it is a completely different label space."""
    from classify_job import describe_label_source

    text = describe_label_source(
        _params(), _StubDB(),
        labels=None, use_tol=True, model_type="bioclip", class_count=214000,
    )
    assert text == "Tree of Life: all 214,000 species (no species list active)"

    unknown = describe_label_source(
        _params(), _StubDB(), labels=None, use_tol=True, model_type="bioclip",
    )
    assert "Tree of Life" in unknown
    assert "no species list active" in unknown


def test_describe_label_source_timm_ignores_lists():
    """timm models carry a fixed head — active species lists do not apply."""
    from classify_job import describe_label_source

    text = describe_label_source(
        _params(labels_files=["/l/ca.txt"]), _StubDB(["/l/ca.txt"]),
        labels=None, use_tol=False, model_type="timm", class_count=10000,
    )
    assert text == "Model's own 10,000 built-in classes — species lists don't apply"


def test_resolve_label_sources_keeps_lookup_order(tmp_path):
    """Regression: the shared resolver still yields the same source paths."""
    from unittest.mock import patch

    from classify_job import _resolve_label_sources

    # ``labels_file`` (singular) and ``labels_files`` (plural) both require
    # the paths to exist on disk — ``_resolve_label_set_metas`` mirrors
    # ``_load_labels``'s file-existence check, so a stale configured path
    # falls through to the workspace list rather than being named by the
    # Jobs page or written into ``labels_fingerprints``.
    single = tmp_path / "a.txt"
    single.write_text("Robin\n")
    plural_a = tmp_path / "pa.txt"
    plural_a.write_text("Cardinal\n")
    plural_b = tmp_path / "pb.txt"
    plural_b.write_text("Blue Jay\n")
    with patch("classify_job.get_saved_labels", return_value=[]):
        assert _resolve_label_sources(
            _params(labels_files=[str(plural_a), str(plural_b)]),
            _StubDB(["/ws.txt"]),
        ) == [str(plural_a), str(plural_b)]
        assert _resolve_label_sources(
            _params(labels_file=str(single)), _StubDB(["/ws.txt"]),
        ) == [str(single)]
        assert _resolve_label_sources(
            _params(), _StubDB(["/ws.txt"]),
        ) == ["/ws.txt"]
        with patch(
            "classify_job.get_active_labels",
            return_value=[{"labels_file": "/g.txt"}, {"name": "no file"}],
        ):
            assert _resolve_label_sources(_params(), _StubDB()) == ["/g.txt"]


def test_resolve_label_set_metas_falls_back_when_labels_file_missing(tmp_path):
    """Regression: a configured ``labels_file`` that no longer exists on
    disk must not name the stale path on the Jobs page or in
    ``labels_fingerprints``. ``_load_labels`` ignores a missing single-file
    path and falls back to the workspace's active lists — ``_resolve_label_set_metas``
    has to do the same, otherwise the classify step shows lists that
    did NOT produce ``labels``.
    """
    from unittest.mock import patch

    from classify_job import _resolve_label_set_metas, _resolve_label_sources

    deleted = str(tmp_path / "gone.txt")  # never created
    saved = [{"labels_file": "/l/ws.txt", "name": "Workspace list"}]
    db = _StubDB(["/l/ws.txt"])
    with patch("classify_job.get_saved_labels", return_value=saved):
        metas = _resolve_label_set_metas(_params(labels_file=deleted), db)
        # Not the stale path — the workspace fallback.
        assert metas == [{"labels_file": "/l/ws.txt", "name": "Workspace list"}]
        # And the sidecar source list agrees.
        assert _resolve_label_sources(
            _params(labels_file=deleted), db,
        ) == ["/l/ws.txt"]


def test_resolve_label_set_metas_filters_missing_in_labels_files(tmp_path):
    """Regression: the plural ``labels_files`` branch mirrors the singular
    branch and drops paths whose file was deleted, matching what
    ``_load_labels`` actually loads via ``load_merged_labels_with_metas``.
    Without this, the Jobs page and ``labels_fingerprints`` would name
    lists that contributed nothing to ``labels``.
    """
    from unittest.mock import patch

    from classify_job import _resolve_label_set_metas

    existing = tmp_path / "birds.txt"
    existing.write_text("Robin\n")
    missing = str(tmp_path / "gone.txt")  # never created
    saved = [
        {"labels_file": str(existing), "name": "Birds"},
        {"labels_file": missing, "name": "Deleted"},
    ]
    with patch("classify_job.get_saved_labels", return_value=saved):
        metas = _resolve_label_set_metas(
            _params(labels_files=[str(existing), missing]), _StubDB(),
        )
        # Only the file that still exists on disk — the deleted path is dropped
        # to match ``_load_labels``'s behavior.
        assert metas == [{"labels_file": str(existing), "name": "Birds"}]


def test_load_labels_returns_metas_used_for_display(tmp_path):
    """The metadata returned by ``_load_labels`` must reflect what it
    actually consumed so downstream ``describe_label_source`` and
    ``_record_labels_fingerprint`` cannot drift from ``labels``.
    """
    from unittest.mock import patch

    from classify_job import _load_labels

    labels_file = tmp_path / "cardinals.txt"
    labels_file.write_text("Northern Cardinal\n")
    saved = [{"labels_file": str(labels_file), "name": "My Cardinals"}]
    with patch("classify_job.get_saved_labels", return_value=saved):
        labels, use_tol, label_metas = _load_labels(
            model_type="bioclip",
            model_str="hf-hub:imageomics/bioclip",
            labels_file=str(labels_file),
            labels_files=None,
        )
    assert labels == ["Northern Cardinal"]
    assert use_tol is False
    # The returned metadata carries the saved name — describe_label_source
    # uses it verbatim, no second lookup required.
    assert label_metas == [{"labels_file": str(labels_file), "name": "My Cardinals"}]


def test_describe_label_source_prefers_supplied_metas():
    """Regression: when the caller passes ``label_metas``, the description
    uses those and does NOT re-resolve from ``params`` and ``db``. This
    keeps the displayed name in sync with the ``labels`` that were loaded
    even if the workspace's active list changed between load and render.
    """
    from unittest.mock import patch

    from classify_job import describe_label_source

    # ``params`` and ``db`` point to a totally different set — supplied
    # ``label_metas`` must win.
    saved = [
        {"labels_file": "/l/loaded.txt", "name": "Actually Loaded"},
        {"labels_file": "/l/other.txt", "name": "Not Loaded"},
    ]
    db = _StubDB(["/l/other.txt"])
    with patch("classify_job.get_saved_labels", return_value=saved):
        text = describe_label_source(
            _params(), db,
            labels=["a", "b"], use_tol=False, model_type="bioclip",
            label_metas=[
                {"labels_file": "/l/loaded.txt", "name": "Actually Loaded"},
            ],
        )
    assert text == "2 species from Actually Loaded"


def test_load_labels_drops_missing_files_from_returned_metas(tmp_path):
    """Regression: ``load_merged_labels`` silently skips paths whose file is
    missing on disk (a saved list whose source was deleted, a workspace
    override still pointing at a since-removed file). Before the fix,
    ``_load_labels`` still returned metadata for those skipped paths — so
    ``describe_label_source`` named lists that contributed no classes and
    ``_record_labels_fingerprint`` wrote the same false provenance into
    ``labels_fingerprints``.
    """
    from unittest.mock import patch

    from classify_job import _load_labels

    present = tmp_path / "birds.txt"
    present.write_text("Robin\n")
    gone = tmp_path / "removed.txt"  # never created
    saved = [
        {"labels_file": str(present), "name": "Backyard Birds"},
        {"labels_file": str(gone), "name": "Removed Set"},
    ]
    with patch("classify_job.get_saved_labels", return_value=saved):
        labels, use_tol, label_metas = _load_labels(
            model_type="bioclip",
            model_str="hf-hub:imageomics/bioclip",
            labels_file=None,
            labels_files=[str(present), str(gone)],
        )
    assert labels == ["Robin"]
    assert use_tol is False
    # The missing file's metadata is dropped — only the list that actually
    # produced ``labels`` survives.
    assert label_metas == [{"labels_file": str(present), "name": "Backyard Birds"}]


def test_load_labels_workspace_fallback_drops_missing_files(tmp_path):
    """Same guarantee for the workspace-scoped fallback: a workspace override
    pointing at a deleted file must not name that stale list on the classify
    step even though ``load_merged_labels`` silently skipped it.
    """
    from unittest.mock import patch

    from classify_job import _load_labels

    present = tmp_path / "ws.txt"
    present.write_text("Cardinal\n")
    gone = tmp_path / "ws_gone.txt"
    db = _StubDB([str(present), str(gone)])
    saved = [
        {"labels_file": str(present), "name": "Workspace Set"},
        {"labels_file": str(gone), "name": "Deleted"},
    ]
    with patch("classify_job.get_saved_labels", return_value=saved):
        labels, use_tol, label_metas = _load_labels(
            model_type="bioclip",
            model_str="hf-hub:imageomics/bioclip",
            labels_file=None,
            labels_files=None,
            db=db,
        )
    assert labels == ["Cardinal"]
    assert use_tol is False
    assert label_metas == [{"labels_file": str(present), "name": "Workspace Set"}]


# ── Task 3: _detect_subjects tests ──────────────────────────────────────────


def test_detect_subjects_returns_detection_map(tmp_path):
    """Phase 5: returns detection map for photos with detectable subjects."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    # Create a real test image
    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    fake_detection = {
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.95,
        "category": "animal",
    }

    mock_db = MagicMock()
    mock_db.get_existing_detection_photo_ids.return_value = set()
    mock_db.write_detection_batch.return_value = [101]

    with patch("classify_job.detect_animals", return_value=[fake_detection]), \
         patch("classify_job.get_primary_detection", return_value=fake_detection), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detected == 1
    assert 1 in detection_map
    # detection_map now returns a list of detection dicts per photo
    assert isinstance(detection_map[1], list)
    assert len(detection_map[1]) == 1
    assert detection_map[1][0]["confidence"] == 0.95
    assert detection_map[1][0]["id"] == 101


def test_detect_subjects_skips_existing_detections(tmp_path):
    """Phase 5: skips photos that already have detections in the DB (unless reclassify)."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    # Photo 1 already has detections in the database
    mock_db.get_detector_run_photo_ids.return_value = {1}
    mock_db.get_detections.return_value = [
        {"id": 101, "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
         "detector_confidence": 0.9, "category": "animal"},
    ]

    # detect_animals should NOT be called since photo already has detections
    with patch("classify_job.detect_animals") as mock_detect:
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    mock_detect.assert_not_called()
    assert detected == 1
    assert 1 in detection_map
    assert isinstance(detection_map[1], list)
    assert detection_map[1][0]["id"] == 101


def test_detect_subjects_skips_weight_download_when_all_cached(tmp_path):
    """When every photo is already detected and reclassify=False, no fresh
    MegaDetector pass runs, so the auto-download should be skipped entirely.
    Prevents offline reruns from aborting on missing weights."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 1, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    mock_db.get_detector_run_photo_ids.return_value = {1}
    mock_db.get_detections.return_value = [
        {"id": 101, "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
         "detector_confidence": 0.9, "category": "animal"},
    ]

    with patch("detector.ensure_megadetector_weights") as mock_ensure:
        _detect_subjects(
            photos=photos, folders=folders, runner=runner, job=job,
            reclassify=False, db=mock_db,
        )

    mock_ensure.assert_not_called()


def test_detect_subjects_skips_weight_download_for_empty_reclassify(tmp_path):
    """An empty photo list with reclassify=True should not trigger the
    MegaDetector download. No photos = no detection pass, so the
    ~300 MB fetch would be pure waste and would also make offline no-op
    reclassifies fatally dependent on the network.

    Regression for Codex P2 review on #535."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    mock_db = MagicMock()
    mock_db.get_detector_run_photo_ids.return_value = set()

    with patch("detector.ensure_megadetector_weights") as mock_ensure:
        _detect_subjects(
            photos=[], folders={}, runner=runner, job=job,
            reclassify=True, db=mock_db,
        )

    mock_ensure.assert_not_called()


def test_detect_subjects_graceful_on_import_error():
    """Phase 5: returns empty map if PytorchWildlife not installed."""
    from unittest.mock import MagicMock

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    # _detect_subjects should handle ImportError gracefully
    detection_map, detected = _detect_subjects(
        photos=[],
        folders={},
        runner=runner,
        job=job,
        reclassify=False,
        db=MagicMock(),
    )
    assert detection_map == {}
    assert detected == 0


# ── Task 4: Multi-detection pipeline tests ───────────────────────────────────


def test_detect_batch_marks_processed_before_quality_scoring(tmp_path):
    """_detect_batch must add photo_id to processed_ids as soon as detection
    rows are committed to the DB, before quality-scoring calls.

    If compute_sharpness or update_photo_quality raises after write_detection_batch,
    the outer except catches the exception and processed_ids.add at the end of
    the per-photo loop body is never reached.  The photo would be missing from
    processed_ids, causing the reclassify purge in pipeline_job to skip
    deleting its stale pre-run detection rows — future non-reclassify runs
    would then reuse those stale rows indefinitely.

    Regression for Codex P2 review on #513, classify_job.py line 315.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 7, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    img = Image.new("RGB", (100, 100), color="red")
    img.save(str(tmp_path / "bird.jpg"))

    fake_detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
         "confidence": 0.9, "category": "animal"},
    ]

    mock_db = MagicMock()
    mock_db.write_detection_batch.return_value = [42]

    # quality scoring raises — simulates compute_sharpness or
    # update_photo_quality failing after the detection row is already saved.
    def raising_sharpness(*args, **kwargs):
        raise RuntimeError("simulated sharpness failure")

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", side_effect=raising_sharpness):
        detection_map, detected, processed_ids = _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=True,
            db=mock_db,
            already_detected_ids=set(),
        )

    # The detection was saved to the DB before quality scoring raised.
    mock_db.write_detection_batch.assert_called_once()
    # photo 7 must be in processed_ids even though quality scoring raised, so
    # the reclassify purge correctly removes its stale pre-run detection rows.
    assert 7 in processed_ids, (
        "photo_id must be in processed_ids after write_detection_batch even when "
        "quality-scoring raises — regression for Codex P2 on #513 line 315"
    )
    # detection_map should still contain the result from this run
    assert 7 in detection_map


def test_detect_batch_propagates_resource_wait_cancelled(tmp_path):
    """Regression: ``ResourceWaitCancelled`` from a MegaDetector
    inference-lease wait must escape ``_detect_batch``, not be
    swallowed by the ``except (ImportError, RuntimeError)`` arm
    (``ResourceWaitCancelled`` subclasses ``RuntimeError``).

    Codex P1: on the reclassify path the caller has already called
    ``db.clear_detections(photo["id"])`` for this photo before
    invoking ``_detect_batch``. If the cancel is silently swallowed
    here, the classify recovery rebuilds predictions using the
    full-image fallback and lands a committed catalog change even
    though the user pressed Stop.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch
    from resource_ledger import ResourceWaitCancelled

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 7, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    img = Image.new("RGB", (100, 100), color="red")
    img.save(str(tmp_path / "bird.jpg"))

    def raising_detect(*_args, **_kwargs):
        raise ResourceWaitCancelled(
            "Cancelled while waiting for GPU inference resources",
        )

    mock_db = MagicMock()

    with (
        patch("classify_job.detect_animals", side_effect=raising_detect),
        patch("classify_job.get_primary_detection", return_value=None),
        pytest.raises(ResourceWaitCancelled),
    ):
        _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=True,
            db=mock_db,
            already_detected_ids=set(),
        )


def test_detect_batch_does_not_pass_threshold_to_detector(tmp_path, monkeypatch):
    """detect_animals is called with just the image path — the workspace
    threshold is NOT applied at write time.

    Regression for the detection-storage redesign: the detector writes
    everything above RAW_CONF_FLOOR so results can be globally cached
    across workspaces. Any per-workspace threshold is applied as a
    read-time filter (get_detections / stats queries), not here.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _detect_batch
    from db import Database

    # Real DB with a workspace that overrides detector_confidence
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace(
        "Birds", config_overrides={"detector_confidence": 0.05}
    )
    db.set_active_workspace(ws_id)

    # Isolate global config so we don't read ~/.vireo/config.json
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    img = Image.new("RGB", (100, 100), color="red")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [{"id": 1, "filename": "bird.jpg", "folder_id": 10}]
    folders = {10: str(tmp_path)}

    captured = {}

    def fake_detect(image_path, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    runner = FakeRunner()
    job = _make_job()

    with patch("classify_job.detect_animals", side_effect=fake_detect), \
         patch("classify_job.get_primary_detection", return_value=None), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=True,
            db=db,
            already_detected_ids=set(),
        )

    assert "confidence_threshold" not in captured["kwargs"], (
        "detect_animals must not receive confidence_threshold; "
        f"got kwargs={captured['kwargs']!r}"
    )
    assert captured["args"] == (), (
        "detect_animals must only be called with the image path; "
        f"got extra positional args={captured['args']!r}"
    )


def test_detect_batch_stores_all_detections(tmp_path):
    """_detect_batch should store all detections, not just the primary."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos")
    pid = db.add_photo(fid, "multi.jpg", ".jpg", 1000, 1234567890.0)
    # Verify that save_detections stores all detections
    detections_list = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.1, "w": 0.2, "h": 0.3}, "confidence": 0.80, "category": "animal"},
        {"box": {"x": 0.3, "y": 0.5, "w": 0.15, "h": 0.2}, "confidence": 0.60, "category": "animal"},
    ]
    det_ids = db.save_detections(pid, detections_list, detector_model="MDV6")
    assert len(det_ids) == 3
    stored = db.get_detections(pid)
    assert len(stored) == 3


def test_detect_batch_returns_all_detections(tmp_path):
    """_detect_batch should return a list of all detections per photo, not just primary."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch

    runner = FakeRunner()
    job = _make_job()

    # Create a real test image
    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    fake_detections = [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.95, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.80, "category": "animal"},
    ]

    mock_db = MagicMock()
    mock_db.write_detection_batch.return_value = [101, 102]

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        detection_map, detected, _processed = _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
            already_detected_ids=set(),
        )

    assert detected == 1
    assert 1 in detection_map
    assert isinstance(detection_map[1], list)
    assert len(detection_map[1]) == 2
    assert detection_map[1][0]["id"] == 101
    assert detection_map[1][0]["box_x"] == 0.1
    assert detection_map[1][1]["id"] == 102
    assert detection_map[1][1]["box_x"] == 0.5
    mock_db.write_detection_batch.assert_called_once()


def test_detect_batch_handles_same_batch_detection_id_collapse(tmp_path):
    """If DB persistence collapses two detector outputs to one content ID,
    _detect_batch must classify the persisted row instead of strict-zip failing.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_batch

    runner = FakeRunner()
    job = _make_job()

    img = Image.new("RGB", (200, 200), color="green")
    img_path = str(tmp_path / "bird.jpg")
    img.save(img_path)

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10},
    ]
    folders = {10: str(tmp_path)}

    fake_detections = [
        {"box": {"x": 0.10001, "y": 0.2, "w": 0.3, "h": 0.4},
         "confidence": 0.80, "category": "animal"},
        {"box": {"x": 0.10002, "y": 0.2, "w": 0.3, "h": 0.4},
         "confidence": 0.95, "category": "animal"},
    ]

    mock_db = MagicMock()
    mock_db.write_detection_batch.return_value = [101]
    mock_db.get_detections.return_value = [{
        "id": 101,
        "box_x": 0.10002,
        "box_y": 0.2,
        "box_w": 0.3,
        "box_h": 0.4,
        "detector_confidence": 0.95,
        "category": "animal",
        "detector_model": "megadetector-v6",
    }]

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[1]), \
         patch("classify_job.compute_sharpness", return_value=50.0):
        detection_map, detected, _processed = _detect_batch(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
            already_detected_ids=set(),
        )

    assert detected == 1
    assert detection_map[1] == [{
        "id": 101,
        "box_x": 0.10002,
        "box_y": 0.2,
        "box_w": 0.3,
        "box_h": 0.4,
        "confidence": 0.95,
        "category": "animal",
        "detector_model": "megadetector-v6",
    }]
    mock_db.get_detections.assert_called_once_with(
        1, min_conf=0, detector_model="megadetector-v6",
    )


def test_detect_batch_skips_empty_photo_on_rerun(tmp_path, monkeypatch):
    """A photo with no animals is recorded in detector_runs; rerun skips detection."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "empty.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )

    call_count = {"n": 0}
    def fake_detect(image_path):
        call_count["n"] += 1
        return []  # no animals

    monkeypatch.setattr("classify_job.detect_animals", fake_detect)
    monkeypatch.setattr("classify_job.get_primary_detection", lambda dets: None)

    import classify_job
    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "empty.jpg"}]
    folders = {folder_id: "/tmp/p"}

    # First call: runs detection
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1

    # Second call: should skip because detector_runs has the row
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1, "detect_animals should not be re-called for empty photos"


def test_detect_batch_does_not_cache_failed_detector_runs(tmp_path, monkeypatch):
    """When detect_animals returns None (image decode error, ONNX crash,
    etc.), _detect_batch must NOT write a detector_runs row — otherwise
    future non-reclassify passes would skip the photo permanently,
    leaving it without detections unless the user forces --reclassify.
    A legitimate empty scene still gets cached (separate test).
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "broken.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    call_count = {"n": 0}
    def failing_detect(image_path):
        call_count["n"] += 1
        return None  # simulate detector failure

    monkeypatch.setattr("classify_job.detect_animals", failing_detect)
    monkeypatch.setattr("classify_job.get_primary_detection", lambda dets: None)

    import classify_job
    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "broken.jpg"}]
    folders = {folder_id: "/tmp/p"}

    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1, "detector was called"
    # No detector_run row should have been written for the failed run
    assert db.get_detector_run_photo_ids("megadetector-v6") == set()

    # A second pass must call the detector again (no cached "already done")
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 2, "failed photos must be retried on next pass"


def test_detect_batch_skips_quality_score_when_primary_below_threshold(
    tmp_path, monkeypatch
):
    """Photos whose only detection is below the workspace's
    detector_confidence threshold must NOT get a quality_score (or any
    subject_size / subject_sharpness from the noise box) — those values
    drive the highlights ranking, and a noise box that happens to span the
    frame would otherwise produce a sky-high ``subject_size`` and float
    these no-real-subject photos to the top of highlights.

    Regression for the "Mountain chickadee" highlights bug: photos with
    detector_confidence ~0.02 still received quality_score ~0.86 because
    the noise box covered ~98% of the frame.
    """
    from unittest.mock import patch

    from classify_job import _detect_batch
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "noise.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Pre-populate stale quality fields to confirm the fix also clears
    # rotten state from prior runs (self-healing — see CLAUDE.md memory
    # "App self-heals broken state").
    db.update_photo_quality(
        photo_id,
        subject_sharpness=2900.0,
        subject_size=0.98,
        quality_score=0.86,
        sharpness=2900.0,
    )

    img = Image.new("RGB", (100, 100), color="gray")
    img.save(str(tmp_path / "noise.jpg"))

    # Sub-threshold "noise" detection covering nearly the whole frame —
    # exactly what MegaDetector emits when there's no real subject.
    fake_detections = [
        {"box": {"x": 0.01, "y": 0.01, "w": 0.98, "h": 0.98},
         "confidence": 0.027, "category": "animal"},
    ]

    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "noise.jpg"}]
    folders = {folder_id: str(tmp_path)}

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", return_value=2900.0):
        _detect_batch(
            photos=photos, folders=folders, runner=None, job={"id": 0},
            reclassify=True, db=db,
            det_conf_threshold=0.2,
            already_detected_ids=set(),
        )

    row = db.conn.execute(
        "SELECT quality_score, subject_size, subject_sharpness FROM photos WHERE id = ?",
        (photo_id,),
    ).fetchone()
    assert row["quality_score"] is None, (
        "sub-threshold detection must not produce a quality_score; "
        f"got {row['quality_score']!r}"
    )
    assert row["subject_size"] is None, (
        "sub-threshold detection must not produce subject_size; "
        f"got {row['subject_size']!r}"
    )
    assert row["subject_sharpness"] is None, (
        "sub-threshold detection must not produce subject_sharpness; "
        f"got {row['subject_sharpness']!r}"
    )


def test_detect_batch_scores_normally_when_primary_at_threshold(
    tmp_path, monkeypatch
):
    """Counterpart to the sub-threshold test: a detection at or above the
    threshold still produces a quality_score so we don't break the happy
    path."""
    from unittest.mock import patch

    from classify_job import _detect_batch
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "bird.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )

    img = Image.new("RGB", (100, 100), color="gray")
    img.save(str(tmp_path / "bird.jpg"))

    fake_detections = [
        {"box": {"x": 0.3, "y": 0.3, "w": 0.4, "h": 0.4},
         "confidence": 0.85, "category": "animal"},
    ]

    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "bird.jpg"}]
    folders = {folder_id: str(tmp_path)}

    with patch("classify_job.detect_animals", return_value=fake_detections), \
         patch("classify_job.get_primary_detection", return_value=fake_detections[0]), \
         patch("classify_job.compute_sharpness", return_value=1500.0):
        _detect_batch(
            photos=photos, folders=folders, runner=None, job={"id": 0},
            reclassify=True, db=db,
            det_conf_threshold=0.2,
            already_detected_ids=set(),
        )

    row = db.conn.execute(
        "SELECT quality_score, subject_size FROM photos WHERE id = ?",
        (photo_id,),
    ).fetchone()
    assert row["quality_score"] is not None and row["quality_score"] > 0
    assert row["subject_size"] is not None


def test_classify_photos_reclassifies_when_gate_has_no_cached_rows(tmp_path):
    """If classifier_runs has a (model, fp) key but get_predictions_for_detection
    returns nothing (e.g. a prior pass stored `category == 'match'` which
    is intentionally not written, or transient ordering between the run
    record and _store_grouped_predictions), the detection must fall
    through to classification — not short-circuit forever.
    """
    from unittest.mock import MagicMock

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}

    mock_clf = MagicMock()
    mock_clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Robin", "score": 0.9}], None),
    ]
    mock_db = MagicMock()
    # Gate fires (run key present) but no cached prediction rows.
    mock_db.get_classifier_run_keys.return_value = {("BioCLIP", "fp-x")}
    mock_db.get_predictions_for_detection.return_value = []
    mock_db.get_photo_embedding.return_value = None

    # Need a real image on disk so _prepare_image succeeds.
    import os
    img_path = os.path.join(str(tmp_path), "bird.jpg")
    Image.new("RGB", (400, 400), color="green").save(img_path)

    detection_map = {
        1: [{"id": 101, "box_x": 0.1, "box_y": 0.1,
             "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
             "category": "animal"}],
    }

    _classify_photos(
        photos=photos,
        folders=folders,
        detection_map=detection_map,
        existing_preds=set(),
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
        labels_fingerprint="fp-x",
    )

    # The classifier must have actually been invoked — if the gate
    # short-circuited on the empty cached result, this assertion fails.
    assert (
        mock_clf.classify_batch_with_embedding.called
        or mock_clf.classify_with_embedding.called
    ), (
        "Gate fired with no cached rows and short-circuited classification; "
        "the detection is stranded until --reclassify."
    )


def test_reclassify_preserves_cache_on_model_load_failure(tmp_path, monkeypatch):
    """If the classifier fails to load, a reclassify must NOT have already
    purged cached predictions/detections — otherwise weight-corruption
    wipes shared-folder workspaces and there is no replacement.
    """
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    # Seed a collection the classify path can consume.
    coll_id = db.add_collection("c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]')

    # Force the classifier constructor to raise — simulates
    # weight-corruption or missing-weights at load time.
    import classifier as classifier_mod
    class BoomClassifier:
        def __init__(self, *a, **kw):
            raise RuntimeError("simulated weights corruption")
    monkeypatch.setattr(classifier_mod, "Classifier", BoomClassifier)

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id="BioCLIP",
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=True,
    )

    # Run should fail (classifier init crashes) but MUST NOT destroy the
    # cached prediction or detection.
    import contextlib
    with contextlib.suppress(Exception):
        run_classify_job(job, runner, db_path, ws, params)

    # Re-open the DB to read post-job state
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id=?",
        (det_id,),
    ).fetchone()["n"]
    dets_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM detections WHERE id=?",
        (det_id,),
    ).fetchone()["n"]
    assert preds_after == 1, (
        "Reclassify purge happened before model load failed — cached "
        "predictions were destroyed without replacement."
    )
    assert dets_after == 1, (
        "Detections purged before model load failure — cache lost."
    )


def test_reclassify_skips_purge_when_cancelled_during_model_load(tmp_path, monkeypatch):
    """If the user cancels while model load / embedding computation is
    running, the destructive reclassify purge (clear_predictions /
    clear_detections) MUST NOT execute. Without the pre-purge cancel
    gate, the post-detection gate returns with predictions_stored=0 but
    the cache is already wiped.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    # Hermetic global config so the run doesn't read or write the user's
    # ~/.vireo/config.json (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    coll_id = db.add_collection("c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]')

    runner = FakeRunner()

    # Classifier flips the runner to cancelled mid-init, then observes
    # the cancel_check and raises the same typed error the real BioCLIP
    # embedding path raises between labels. classify_job imports
    # Classifier at module load time, so the patch has to target that
    # reference rather than classifier.Classifier.
    import classify_job as cj
    class CancellingClassifier:
        def __init__(self, *a, cancel_check=None, **kw):
            runner.cancelled = True
            if cancel_check and cancel_check():
                raise cj.ClassificationCancelled("classification cancelled")
    monkeypatch.setattr(cj, "Classifier", CancellingClassifier)

    # Bypass the on-disk model registry — the test doesn't need real
    # weights since CancellingClassifier ignores its args. Seed a
    # directory with stub ToL artifacts so _load_labels' label-free
    # gate (tree_of_life_ready) succeeds and we exercise the cancel
    # path rather than the "ToL files not installed" error.
    fake_weights = tmp_path / "weights"
    fake_weights.mkdir()
    (fake_weights / "tol_embeddings.npy").write_bytes(b"stub")
    (fake_weights / "tol_classes.json").write_bytes(b"[]")
    monkeypatch.setattr(cj, "get_active_model", lambda: {
        "id": "BioCLIP",
        "name": "BioCLIP",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": str(fake_weights),
        "model_type": "bioclip",
        "downloaded": True,
    })

    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=True,
    )

    result = run_classify_job(job, runner, db_path, ws, params)

    # The cancel-before-purge gate returns a no-op result.
    assert result["predictions_stored"] == 0
    assert result["detected"] == 0

    finals = _final_step_statuses(runner)
    assert finals.get("load_model") == "cancelled"
    for step_id in ("detect", "classify", "finalize"):
        assert finals.get(step_id) == "cancelled", (
            f"Step {step_id!r} must be marked cancelled when model "
            f"initialization observes cancellation, got {finals.get(step_id)!r}"
        )

    # And — the whole point of the gate — cached predictions and
    # detections survive the cancelled run intact.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id=?",
        (det_id,),
    ).fetchone()["n"]
    dets_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM detections WHERE id=?",
        (det_id,),
    ).fetchone()["n"]
    assert preds_after == 1, (
        "Reclassify purge ran despite cancel during model load — "
        "cached predictions were destroyed without replacement."
    )
    assert dets_after == 1, (
        "Reclassify purge ran despite cancel during model load — "
        "cached detections were destroyed without replacement."
    )


def test_reclassify_finalizes_steps_on_resource_wait_cancel_during_load(
    tmp_path, monkeypatch,
):
    """Regression: when a classify job is cancelled during a cold ONNX
    model construction (``onnx_runtime.create_session``'s bound
    resource cancel probe fires), ``acquire_cached_classifier`` raises
    ``ResourceWaitCancelled`` — NOT ``ClassificationCancelled``. Without
    catching both shapes in ``run_classify_job``, the exception escapes
    to ``JobRunner`` which marks the top-level cancelled but leaves
    ``load_model`` at "running" and later steps at "pending" instead
    of finalizing them.

    Sibling to ``test_reclassify_skips_purge_when_cancelled_during_model_load``:
    the pre-purge gate + purge-preserving semantics from that test
    apply to this cancellation shape too.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database
    from resource_ledger import ResourceWaitCancelled

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(
        det_id, species="Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="legacy",
    )

    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]',
    )

    runner = FakeRunner()

    fake_weights = tmp_path / "weights"
    fake_weights.mkdir()
    (fake_weights / "tol_embeddings.npy").write_bytes(b"stub")
    (fake_weights / "tol_classes.json").write_bytes(b"[]")
    import classify_job as cj
    monkeypatch.setattr(cj, "get_active_model", lambda: {
        "id": "BioCLIP",
        "name": "BioCLIP",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": str(fake_weights),
        "model_type": "bioclip",
        "downloaded": True,
    })

    # Simulate the exact failure Codex flagged: cold-load lease cancel
    # surfaces as ``ResourceWaitCancelled`` from inside
    # ``acquire_cached_classifier`` — not ``ClassificationCancelled``.
    import classifier_cache
    def _cancelled_acquire(*args, **kwargs):
        raise ResourceWaitCancelled(
            "Cancelled while waiting for ONNX model construction resources",
        )
    monkeypatch.setattr(
        classifier_cache, "acquire_cached_classifier", _cancelled_acquire,
    )

    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=True,
    )

    # Must NOT raise: the cancellation shape must be caught locally so
    # JobRunner sees a normal return and finalizes step rows.
    result = run_classify_job(job, runner, db_path, ws, params)

    assert result["predictions_stored"] == 0
    assert result["detected"] == 0

    finals = _final_step_statuses(runner)
    assert finals.get("load_model") == "cancelled", (
        f"load_model must finalize cancelled on ResourceWaitCancelled "
        f"during cold load, got {finals.get('load_model')!r}"
    )
    for step_id in ("detect", "classify", "finalize"):
        assert finals.get(step_id) == "cancelled", (
            f"Step {step_id!r} must finalize cancelled when the cold "
            f"ONNX construction lease is cancelled, got "
            f"{finals.get(step_id)!r}"
        )

    # Preservation semantics from the ClassificationCancelled path
    # apply here too: the reclassify purge must not have run.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_after = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id=?",
        (det_id,),
    ).fetchone()["n"]
    assert preds_after == 1, (
        "Reclassify purge must not run when the cold construction "
        "lease is cancelled — cached prediction was destroyed."
    )


def test_classify_factory_cancel_check_uses_pure_cancel_probe(
    tmp_path, monkeypatch,
):
    """Regression: the classifier factory closure must call the runner's
    non-parking ``cancellation_requested`` probe — never ``is_cancelled``
    — because the factory runs while ``ModelCache._Entry.load_lock`` is
    held. If the factory parked on pause via ``is_cancelled`` →
    ``wait_if_paused``, any concurrent unpaused job requesting the same
    cache key would stall until Resume, since the polling waiter in
    ``model_cache.py:198`` cannot acquire the shared load_lock.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")
    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]',
    )

    class PureProbeRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.is_cancelled_calls_during_factory = 0
            self.pure_probe_calls_during_factory = 0
            self.in_factory = False

        def is_cancelled(self, job_id):
            if self.in_factory:
                self.is_cancelled_calls_during_factory += 1
            return super().is_cancelled(job_id)

        def cancellation_requested(self, job_id):
            if self.in_factory:
                self.pure_probe_calls_during_factory += 1
            return False

    runner = PureProbeRunner()

    fake_weights = tmp_path / "weights"
    fake_weights.mkdir()
    (fake_weights / "tol_embeddings.npy").write_bytes(b"stub")
    (fake_weights / "tol_classes.json").write_bytes(b"[]")
    import classify_job as cj
    monkeypatch.setattr(cj, "get_active_model", lambda: {
        "id": "BioCLIP",
        "name": "BioCLIP",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": str(fake_weights),
        "model_type": "bioclip",
        "downloaded": True,
    })

    # Intercept the factory: mark the "in factory" window, invoke the
    # closure once so its cancel_check fires against the runner, then
    # bail with a distinctive error that we catch in the assertion.
    captured = {}

    class _StopAfterProbe(RuntimeError):
        pass

    class _StubClassifier:
        """Minimal object that runs the caller's cancel_check exactly
        once inside the factory scope, then aborts so the classify
        pipeline unwinds without needing real ONNX weights."""

        def __init__(self, *args, cancel_check=None, **kwargs):
            if cancel_check is not None:
                cancel_check()
            raise _StopAfterProbe(
                "abort factory after probing cancel_check",
            )

    monkeypatch.setattr(cj, "Classifier", _StubClassifier)

    import classifier_cache
    original_acquire = classifier_cache.acquire_cached_classifier

    def _tracking_acquire(*, factory, **kwargs):
        runner.in_factory = True
        try:
            factory()
        except _StopAfterProbe as e:
            captured["factory_error"] = e
        finally:
            runner.in_factory = False
        raise _StopAfterProbe("stop before classification")

    monkeypatch.setattr(
        classifier_cache, "acquire_cached_classifier", _tracking_acquire,
    )

    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=False,
    )

    with pytest.raises(_StopAfterProbe):
        run_classify_job(job, runner, db_path, ws, params)

    assert captured.get("factory_error") is not None, (
        "Factory closure must have been invoked so cancel_check ran; "
        "if the classify job returned early, this regression cannot "
        "prove which probe the factory used."
    )
    assert runner.pure_probe_calls_during_factory >= 1, (
        "Factory cancel_check must call runner.cancellation_requested "
        "(pure probe) — got zero calls, meaning the fix regressed and "
        "the factory would park on pause while holding ModelCache "
        "entry.load_lock."
    )
    assert runner.is_cancelled_calls_during_factory == 0, (
        "Factory cancel_check must NOT call runner.is_cancelled — that "
        "probe parks inside wait_if_paused, so a pause request during "
        "cold model load would strand every unpaused sibling waiting "
        "on the same cache key."
    )


def test_classify_photos_surfaces_cached_full_image_predictions(tmp_path):
    """When a photo has no real detections and the full-image synthetic
    detection is gated by classifier_runs, the cached top prediction
    must still be surfaced into raw_results as `_existing: True` —
    otherwise non-reclassify reruns silently drop those photos from
    downstream grouping.
    """
    from unittest.mock import MagicMock

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}

    mock_clf = MagicMock()
    mock_db = MagicMock()
    # No real detections → full-image path. Existing full-image
    # detection is cached.
    mock_db.get_detections.return_value = [{"id": 999}]
    mock_db.get_classifier_run_keys.return_value = {("BioCLIP", "fp-x")}
    mock_db.get_predictions_for_detection.return_value = [
        {"species": "Robin", "confidence": 0.9, "detection_id": 999},
    ]
    mock_db.get_photo_embedding.return_value = None

    raw_results, failed, skipped = _classify_photos(
        photos=photos,
        folders=folders,
        detection_map={},  # no real detections → full-image branch
        existing_preds=set(),
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
        labels_fingerprint="fp-x",
    )

    assert skipped == 1, "cached full-image detection should count as skipped"
    assert len(raw_results) == 1, "cached full-image prediction must surface"
    assert raw_results[0]["_existing"] is True
    assert raw_results[0]["prediction"] == "Robin"
    assert raw_results[0]["detection_id"] == 999
    mock_clf.classify_with_embedding.assert_not_called()
    mock_clf.classify_batch_with_embedding.assert_not_called()


def test_classify_photos_reuses_full_image_detection_on_rerun(tmp_path, monkeypatch):
    """When a photo has no real detections, classify_photos falls back to a
    synthetic ('full-image') detection. Because save_detections is
    clear-and-reinsert per (photo, detector_model), calling it on every
    pass would generate a new id each time and cascade-delete prior
    predictions/classifier_runs tied to the old id. The non-reclassify
    path must reuse the existing full-image detection instead.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    # Pre-seed a full-image detection that a prior classify pass would have
    # left behind.
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0, "category": "animal"}],
        detector_model="full-image",
    )
    original_det_id = det_ids[0]

    # Sanity check the helper used by the reuse path — must use min_conf=0
    # because the synthetic full-image detection has confidence=0.
    existing = db.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert len(existing) == 1
    assert existing[0]["id"] == original_det_id

    # Simulate what classify_photos does on a subsequent pass: the reuse
    # branch must NOT call save_detections again, or it would cascade-delete
    # any cached predictions attached to the original detection.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) "
        "VALUES (?, 'bioclip', 'fp1', 'Robin', 0.8)",
        (original_det_id,),
    )
    db.conn.commit()

    # Reuse path via the helper
    reused = db.get_detections(
        photo_id, detector_model="full-image", min_conf=0,
    )
    assert reused[0]["id"] == original_det_id
    # Prediction still there
    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (original_det_id,),
    ).fetchone()["n"]
    assert n == 1


def test_store_grouped_predictions_writes_active_fingerprint(tmp_path):
    """Predictions produced under a given label set must be written with
    that set's fingerprint, not the default 'legacy'. Otherwise the
    fingerprint-aware skip gate (get_existing_prediction_photo_ids with
    labels_fingerprint=...) would miss them and force reclassification
    on every pass.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )

    import classify_job
    raw_results = [{
        "photo": {
            "id": photo_id, "filename": "a.jpg",
            "folder_id": folder_id, "timestamp": None, "burst_id": None,
        },
        "folder_path": "/tmp/p",
        "detection_id": det_ids[0],
        "prediction": "Robin",
        "confidence": 0.88,
        "alternatives": [],
        "taxonomy": {},
        "timestamp": None,
    }]
    classify_job._store_grouped_predictions(
        raw_results, job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=None,
        db=db,
        labels_fingerprint="fp-active",
    )

    row = db.conn.execute(
        "SELECT labels_fingerprint FROM predictions WHERE species=?", ("Robin",)
    ).fetchone()
    assert row is not None, "prediction was not stored"
    assert row["labels_fingerprint"] == "fp-active"

    # And the fingerprint-aware cache lookup must now find it.
    hits = db.get_existing_prediction_photo_ids(
        "bioclip-2", labels_fingerprint="fp-active",
    )
    assert hits == {photo_id}


def test_store_grouped_predictions_persists_match_for_cache(tmp_path, monkeypatch):
    """Already-labeled matches should not be pending review, but they still
    need prediction rows so the next run can reuse the classifier output.
    """
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    monkeypatch.setattr("compare.categorize", lambda *_args, **_kwargs: "match")

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    result = classify_job._store_grouped_predictions(
        raw_results=[{
            "photo": {
                "id": photo_id, "filename": "a.jpg",
                "folder_id": folder_id, "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin",
            "confidence": 0.88,
            "alternatives": [{"species": "Sparrow", "confidence": 0.12}],
            "taxonomy": {},
            "timestamp": None,
        }],
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["predictions_stored"] == 0
    assert result["already_labeled"] == 1

    cached = db.get_predictions_for_detection(
        det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="fp-active",
        min_classifier_conf=0,
    )
    assert [row["species"] for row in cached] == ["Robin", "Sparrow"]
    assert cached[0]["category"] == "match"

    reviewed = db.get_predictions(photo_ids=[photo_id])
    statuses = {row["species"]: row["status"] for row in reviewed}
    assert statuses == {"Robin": "accepted", "Sparrow": "alternative"}


def test_store_grouped_predictions_persists_group_match_for_cache(tmp_path, monkeypatch):
    """Already-labeled burst groups cache each detection's own species."""
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{idx}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for idx in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    monkeypatch.setattr("compare.categorize", lambda *_args, **_kwargs: "match")

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    raw_results = [
        {
            "photo": {
                "id": pid,
                "filename": f"{idx}.jpg",
                "folder_id": folder_id,
                "timestamp": None,
                "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin" if idx == 0 else "Sparrow",
            "confidence": 0.9 if idx == 0 else 0.5,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": datetime(2024, 1, 1, 12, 0, idx),
        }
        for idx, (pid, det_id) in enumerate(zip(photo_ids, det_ids, strict=True))
    ]

    result = classify_job._store_grouped_predictions(
        raw_results=raw_results,
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=10,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["predictions_stored"] == 0
    assert result["already_labeled"] == 2
    rows = db.conn.execute(
        "SELECT detection_id, species, category FROM predictions"
    ).fetchall()
    assert {(r["detection_id"], r["species"], r["category"]) for r in rows} == {
        (det_ids[0], "Robin", "match"),
        (det_ids[1], "Sparrow", "match"),
    }


def test_group_match_drops_per_frame_alternatives(tmp_path, monkeypatch):
    """Burst match caches only each member's primary species; alternatives
    are dropped so a high-confidence dissenting runner-up can't outrank the
    accepted primary via get_predictions_for_detection's confidence ordering.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{idx}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for idx in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "match")

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    # Both frames agree on "Robin" at 0.6, so consensus is Robin@~0.6.
    # Frame 0 carries a dissenting "Hawk" alternative at 0.95 — higher than
    # the consensus confidence. The old code cached it as an 'alternative'
    # row that won the confidence-DESC ordering and became the cached top-1.
    raw_results = [
        {
            "photo": {
                "id": pid,
                "filename": f"{idx}.jpg",
                "folder_id": folder_id,
                "timestamp": None,
                "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin",
            "confidence": 0.6,
            "alternatives": (
                [{"species": "Hawk", "confidence": 0.95}] if idx == 0 else []
            ),
            "taxonomy": {},
            "timestamp": datetime(2024, 1, 1, 12, 0, idx),
        }
        for idx, (pid, det_id) in enumerate(
            zip(photo_ids, det_ids, strict=True)
        )
    ]

    result = classify_job._store_grouped_predictions(
        raw_results=raw_results,
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=10,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["already_labeled"] == 2
    # No "Hawk" alternative row was persisted for either detection.
    rows = db.conn.execute(
        "SELECT detection_id, species, category, status "
        "FROM predictions "
        "LEFT JOIN prediction_review "
        "  ON prediction_review.prediction_id = predictions.id"
    ).fetchall()
    assert {
        (r["detection_id"], r["species"], r["category"], r["status"])
        for r in rows
    } == {
        (det_ids[0], "Robin", "match", "accepted"),
        (det_ids[1], "Robin", "match", "accepted"),
    }
    # The cached top-1 (confidence-DESC) is the consensus species, not Hawk.
    top = db.get_predictions_for_detection(det_ids[0], min_classifier_conf=0)
    assert top[0]["species"] == "Robin"


def test_match_then_unmatch_reenters_pending_on_reuse(tmp_path, monkeypatch):
    """A detection cached as a 'match' is auto-accepted and hidden from the
    review queue.  If the photo's XMP later stops matching, a non-reclassify
    run reuses the cached prediction; the stale 'accepted' review row must be
    downgraded so the prediction re-enters the pending queue.
    """
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    def run(category, alternatives, extra):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        return classify_job._store_grouped_predictions(
            raw_results=[{
                "photo": {
                    "id": photo_id, "filename": "a.jpg",
                    "folder_id": folder_id, "timestamp": None, "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": det_id,
                "prediction": "Robin",
                "confidence": 0.88,
                "alternatives": alternatives,
                "taxonomy": {},
                "timestamp": None,
                **extra,
            }],
            job_id="job-abc",
            model_name="bioclip-2",
            grouping_window=0,
            similarity_threshold=0.99,
            tax=Tax(),
            db=db,
            labels_fingerprint="fp-active",
        )

    # Run 1: photo already labeled -> match -> auto-accepted, out of queue.
    run("match", [{"species": "Sparrow", "confidence": 0.12}], {})
    statuses = {
        r["species"]: r["status"]
        for r in db.get_predictions(photo_ids=[photo_id])
    }
    assert statuses == {"Robin": "accepted", "Sparrow": "alternative"}

    # Run 2: XMP keyword removed -> no longer a match. The classify gate
    # surfaces the cached prediction (_existing) and skips inference.
    run("disagreement", [], {"_existing": True})

    rows = {r["species"]: r for r in db.get_predictions(photo_ids=[photo_id])}
    assert rows["Robin"]["status"] == "pending"        # back in the queue
    assert rows["Robin"]["category"] == "disagreement"  # stale marker cleared
    assert rows["Sparrow"]["status"] == "alternative"   # still nested


def test_match_becoming_multispecies_clears_stale_auto_accept(tmp_path, monkeypatch):
    """A previously auto-accepted single-species match must re-enter the
    pending queue when the sidecar later gains a second recognized taxon.

    Run 1: XMP has one recognized taxon -> single-species match ->
    ``_can_auto_accept_detection_prediction`` returns True ->
    ``_store_match_prediction`` writes the ``AUTO_MATCH_REVIEW_MARKER``
    review row so the detection stays out of the queue.

    Run 2 (non-reclassify, cache reused via ``_existing=True``): the sidecar
    now has two recognized taxa. The prediction still matches, so
    ``category`` remains ``"match"``, but auto-accept flips off. The pending
    path must drop the stale marker so ``status='accepted'`` no longer hides
    the detection from review.
    """
    import classify_job
    from db import Database
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    class Tax:
        def is_taxon(self, name):
            return name in {"Robin", "Sparrow"}

        def get_hierarchy(self, _species):
            return {}

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "match")

    def run(extra):
        return classify_job._store_grouped_predictions(
            raw_results=[{
                "photo": {
                    "id": photo_id, "filename": "a.jpg",
                    "folder_id": folder_id, "timestamp": None, "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": det_id,
                "prediction": "Robin",
                "confidence": 0.88,
                "alternatives": [],
                "taxonomy": {},
                "timestamp": None,
                **extra,
            }],
            job_id="job-abc",
            model_name="bioclip-2",
            grouping_window=0,
            similarity_threshold=0.99,
            tax=Tax(),
            db=db,
            labels_fingerprint="fp-active",
        )

    # Run 1: sole recognized taxon matches -> auto-accepted, out of queue.
    write_sidecar(tmp_path / "a.xmp", {"Robin"}, set())
    run({})
    accepted = db.get_predictions(photo_ids=[photo_id], status="accepted")
    assert [r["species"] for r in accepted] == ["Robin"]

    # Run 2: sidecar now has a second recognized taxon. Category stays
    # "match" but the match is no longer unambiguous -> pending path.
    write_sidecar(tmp_path / "a.xmp", {"Robin", "Sparrow"}, set())
    run({"_existing": True})

    row = db.get_predictions(photo_ids=[photo_id])[0]
    assert row["species"] == "Robin"
    assert row["category"] == "match"
    assert row["status"] == "pending"        # stale auto-accept cleared
    assert not db.get_predictions(photo_ids=[photo_id], status="accepted")


def test_multi_species_xmp_does_not_auto_accept_detection_match(tmp_path):
    """Photo-level multi-species matches stay pending for detection review."""
    import classify_job
    from db import Database
    from xmp import write_sidecar

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    write_sidecar(
        tmp_path / "a.xmp",
        {"Robin", "Sparrow"},
        set(),
    )

    class Tax:
        def is_taxon(self, name):
            return name in {"Robin", "Sparrow"}

        def relationship(self, existing, prediction):
            if existing == prediction:
                return "same"
            return "unrelated"

        def get_hierarchy(self, _species):
            return {}

    result = classify_job._store_grouped_predictions(
        raw_results=[{
            "photo": {
                "id": photo_id, "filename": "a.jpg",
                "folder_id": folder_id, "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin",
            "confidence": 0.88,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": None,
        }],
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["predictions_stored"] == 1
    assert result["already_labeled"] == 0
    rows = db.get_predictions(photo_ids=[photo_id])
    assert len(rows) == 1
    assert rows[0]["species"] == "Robin"
    assert rows[0]["category"] == "match"
    assert rows[0]["status"] == "pending"
    assert not db.get_predictions(photo_ids=[photo_id], status="accepted")


def test_hierarchy_keyword_still_auto_accepts_single_species_match(
    tmp_path, monkeypatch
):
    """An ancestor taxon keyword (e.g. ``Aves``) alongside the matched species
    must not force the detection back into pending review — it's the same
    species, just labeled at multiple ranks.
    """
    import classify_job
    from db import Database
    from xmp import write_sidecar

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "match")

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    write_sidecar(tmp_path / "a.xmp", {"Robin", "Aves"}, set())

    class Tax:
        def is_taxon(self, name):
            return name in {"Robin", "Aves"}

        def relationship(self, existing, prediction):
            if existing == prediction:
                return "same"
            if existing == "Aves" and prediction == "Robin":
                return "ancestor"
            return "unrelated"

        def get_hierarchy(self, _species):
            return {}

    result = classify_job._store_grouped_predictions(
        raw_results=[{
            "photo": {
                "id": photo_id, "filename": "a.jpg",
                "folder_id": folder_id, "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Robin",
            "confidence": 0.88,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": None,
        }],
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["already_labeled"] == 1
    assert result["predictions_stored"] == 0
    accepted = db.get_predictions(photo_ids=[photo_id], status="accepted")
    assert [r["species"] for r in accepted] == ["Robin"]


def test_multiple_distinct_descendants_stay_pending(tmp_path, monkeypatch):
    """A photo-level ``match`` on a broad prediction that has two unrelated
    descendant keywords in the sidecar describes multiple species and must
    NOT be auto-accepted.

    Concretely: prediction ``Sparrow`` with sidecar ``White-crowned Sparrow``
    + ``Golden-crowned Sparrow``. ``categorize()`` returns ``match`` because
    each keyword is a descendant of the prediction, but folding both under
    "same species" would hide a genuinely multi-species photo from review.
    Descendants only fold in when they resolve to one another (species +
    subspecies); sibling descendants must force pending review.
    """
    import classify_job
    from db import Database
    from xmp import write_sidecar

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "match")

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    write_sidecar(
        tmp_path / "a.xmp",
        {"White-crowned Sparrow", "Golden-crowned Sparrow"},
        set(),
    )

    class Tax:
        _species = {
            "Sparrow",
            "White-crowned Sparrow",
            "Golden-crowned Sparrow",
        }

        def is_taxon(self, name):
            return name in self._species

        def relationship(self, existing, prediction):
            if existing == prediction:
                return "same"
            descendants_of_sparrow = {
                "White-crowned Sparrow", "Golden-crowned Sparrow",
            }
            if (
                existing in descendants_of_sparrow
                and prediction == "Sparrow"
            ):
                return "descendant"
            if (
                existing == "Sparrow"
                and prediction in descendants_of_sparrow
            ):
                return "ancestor"
            if existing in descendants_of_sparrow and prediction in descendants_of_sparrow:
                return "sibling"
            return "unrelated"

        def get_hierarchy(self, _species):
            return {}

    result = classify_job._store_grouped_predictions(
        raw_results=[{
            "photo": {
                "id": photo_id, "filename": "a.jpg",
                "folder_id": folder_id, "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Sparrow",
            "confidence": 0.88,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": None,
        }],
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["predictions_stored"] == 1
    assert result["already_labeled"] == 0
    rows = db.get_predictions(photo_ids=[photo_id])
    assert len(rows) == 1
    assert rows[0]["species"] == "Sparrow"
    assert rows[0]["category"] == "match"
    assert rows[0]["status"] == "pending"
    assert not db.get_predictions(photo_ids=[photo_id], status="accepted")


def test_single_descendant_still_auto_accepts_match(tmp_path, monkeypatch):
    """A photo-level match on a broad prediction with a single descendant
    keyword (e.g. prediction ``Sparrow`` + sidecar ``White-crowned Sparrow``)
    still auto-accepts — only one species is asserted.
    """
    import classify_job
    from db import Database
    from xmp import write_sidecar

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "match")

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]
    write_sidecar(tmp_path / "a.xmp", {"White-crowned Sparrow"}, set())

    class Tax:
        def is_taxon(self, name):
            return name in {"Sparrow", "White-crowned Sparrow"}

        def relationship(self, existing, prediction):
            if existing == prediction:
                return "same"
            if existing == "White-crowned Sparrow" and prediction == "Sparrow":
                return "descendant"
            if existing == "Sparrow" and prediction == "White-crowned Sparrow":
                return "ancestor"
            return "unrelated"

        def get_hierarchy(self, _species):
            return {}

    result = classify_job._store_grouped_predictions(
        raw_results=[{
            "photo": {
                "id": photo_id, "filename": "a.jpg",
                "folder_id": folder_id, "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": det_id,
            "prediction": "Sparrow",
            "confidence": 0.88,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": None,
        }],
        job_id="job-abc",
        model_name="bioclip-2",
        grouping_window=0,
        similarity_threshold=0.99,
        tax=Tax(),
        db=db,
        labels_fingerprint="fp-active",
    )

    assert result["already_labeled"] == 1
    assert result["predictions_stored"] == 0
    accepted = db.get_predictions(photo_ids=[photo_id], status="accepted")
    assert [r["species"] for r in accepted] == ["Sparrow"]


@pytest.mark.parametrize("manual_status", ["accepted", "rejected"])
def test_match_flip_preserves_manual_review_on_reuse(
    tmp_path, monkeypatch, manual_status
):
    """A temporary XMP match must not erase an explicit user decision."""
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_id = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="MDV6",
    )[0]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    def run(category):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        return classify_job._store_grouped_predictions(
            raw_results=[{
                "photo": {
                    "id": photo_id, "filename": "a.jpg",
                    "folder_id": folder_id, "timestamp": None, "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": det_id,
                "prediction": "Robin",
                "confidence": 0.88,
                "alternatives": [],
                "taxonomy": {},
                "timestamp": None,
                "_existing": True,
            }],
            job_id="job-abc",
            model_name="bioclip-2",
            grouping_window=0,
            similarity_threshold=0.99,
            tax=Tax(),
            db=db,
            labels_fingerprint="fp-active",
        )

    run("disagreement")
    pred_id = db.get_predictions(photo_ids=[photo_id])[0]["id"]
    db.update_prediction_status(pred_id, manual_status)

    run("match")
    matched = db.get_predictions(photo_ids=[photo_id])[0]
    assert matched["category"] == "match"
    assert matched["status"] == manual_status

    run("disagreement")
    downgraded = db.get_predictions(photo_ids=[photo_id])[0]
    assert downgraded["category"] == "disagreement"
    assert downgraded["status"] == manual_status


def test_group_match_then_unmatch_reenters_pending_on_reuse(tmp_path, monkeypatch):
    """Burst groups cached as 'match' must also re-enter review when the
    photos stop matching and the cached predictions are reused.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    def run(category, extra):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        raw = [
            {
                "photo": {
                    "id": pid, "filename": f"{i}.jpg",
                    "folder_id": folder_id, "timestamp": None,
                    "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": did,
                "prediction": "Robin",
                "confidence": 0.9 - (i * 0.01),
                "alternatives": [],
                "taxonomy": {},
                "timestamp": datetime(2024, 1, 1, 12, 0, i),
                **extra,
            }
            for i, (pid, did) in enumerate(
                zip(photo_ids, det_ids, strict=True)
            )
        ]
        return classify_job._store_grouped_predictions(
            raw_results=raw, job_id="job-abc", model_name="bioclip-2",
            grouping_window=10, similarity_threshold=0.99, tax=Tax(),
            db=db, labels_fingerprint="fp-active",
        )

    run("match", {})
    accepted = db.get_predictions(photo_ids=photo_ids, status="accepted")
    assert {r["detection_id"] for r in accepted} == set(det_ids)

    run("disagreement", {"_existing": True})

    pending = db.get_predictions(photo_ids=photo_ids, status="pending")
    by_det = {r["detection_id"]: r for r in pending}
    assert set(by_det) == set(det_ids)
    for r in by_det.values():
        assert r["status"] == "pending"
        assert r["group_id"]  # burst grouping metadata reapplied
        assert r["category"] == "disagreement"


def test_mixed_group_match_then_unmatch_reenters_pending_for_dissenters(
    tmp_path, monkeypatch
):
    """A mixed burst caches each detection's own matched species. When it
    later stops matching and the cached rows are reused, every detection
    must re-enter the pending queue without grouped consensus review.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    # Frame 0 predicts Robin, frame 1 dissents with Sparrow -> consensus Robin.
    species_by_frame = ["Robin", "Sparrow"]

    def run(category, extra):
        monkeypatch.setattr("compare.categorize", lambda *_a, **_k: category)
        raw = [
            {
                "photo": {
                    "id": pid, "filename": f"{i}.jpg",
                    "folder_id": folder_id, "timestamp": None,
                    "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": did,
                "prediction": species_by_frame[i],
                "confidence": 0.9 if i == 0 else 0.5,
                "alternatives": [],
                "taxonomy": {},
                "timestamp": datetime(2024, 1, 1, 12, 0, i),
                **extra,
            }
            for i, (pid, did) in enumerate(
                zip(photo_ids, det_ids, strict=True)
            )
        ]
        return classify_job._store_grouped_predictions(
            raw_results=raw, job_id="job-abc", model_name="bioclip-2",
            grouping_window=10, similarity_threshold=0.99, tax=Tax(),
            db=db, labels_fingerprint="fp-active",
        )

    run("match", {})
    accepted = db.get_predictions(photo_ids=photo_ids, status="accepted")
    assert {r["detection_id"] for r in accepted} == set(det_ids)
    assert {r["species"] for r in accepted} == {"Robin", "Sparrow"}

    run("disagreement", {"_existing": True})

    pending = db.get_predictions(photo_ids=photo_ids, status="pending")
    by_det = {r["detection_id"]: r for r in pending}
    # Both detections, including the dissenting Sparrow frame, re-enter
    # review; none stay hidden as a stale auto-accepted match.
    assert set(by_det) == set(det_ids)
    assert by_det[det_ids[0]]["species"] == "Robin"
    assert by_det[det_ids[1]]["species"] == "Sparrow"
    for r in by_det.values():
        assert r["status"] == "pending"
        assert r["category"] == "disagreement"
        assert not r["group_id"]
    assert not db.get_predictions(photo_ids=photo_ids, status="accepted")


def test_mixed_burst_never_groups_so_displayed_species_is_accepted_species(
    tmp_path, monkeypatch
):
    """A prediction row's own species is always the species an accept tags.

    ``accept_prediction`` swaps in the burst consensus derived from
    ``prediction_review.individual`` whenever the row carries a ``group_id``.
    Any UI that labels a row with ``pr.species`` — Browse's prediction panel
    and the selection aggregation — would then be able to advertise Sparrow
    and tag Robin, which is the exact failure CORE_PHILOSOPHY.md's
    no-black-boxes rule forbids.

    The guard is ``group_reviewable`` in ``_store_grouped_predictions``: a
    burst is only stamped with a ``group_id``/``individual`` when every frame
    folds to ONE species key, so the consensus can never name a different
    species than the row. This locks that invariant end to end — the burst
    disagrees, so no member gets grouped, and accepting the minority frame
    tags the minority frame's own species.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(3)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "new")

    # Two Robin frames outvote one Sparrow frame inside a single burst.
    species_by_frame = ["Robin", "Robin", "Sparrow"]
    raw = [
        {
            "photo": {
                "id": pid, "filename": f"{i}.jpg", "folder_id": folder_id,
                "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": did,
            "prediction": species_by_frame[i],
            "confidence": 0.9,
            "alternatives": [],
            "taxonomy": {},
            "timestamp": datetime(2024, 1, 1, 12, 0, i),
        }
        for i, (pid, did) in enumerate(zip(photo_ids, det_ids, strict=True))
    ]
    classify_job._store_grouped_predictions(
        raw_results=raw, job_id="job-mix", model_name="bioclip-2",
        grouping_window=10, similarity_threshold=0.99, tax=Tax(),
        db=db, labels_fingerprint="fp-active",
    )

    rows = db.get_predictions(photo_ids=photo_ids, status="pending")
    by_det = {r["detection_id"]: r for r in rows}
    assert set(by_det) == set(det_ids)
    for det_id, row in by_det.items():
        assert not row["group_id"], (
            "a burst whose frames disagree must not be stamped with a "
            "group_id — a grouped accept would replace the row's own "
            f"species with the consensus (detection {det_id})"
        )
        assert not row["individual"]

    sparrow = by_det[det_ids[2]]
    assert sparrow["species"] == "Sparrow"
    result = db.accept_prediction(sparrow["id"])
    assert result["species"] == "Sparrow", (
        "accepting the row Browse labels 'Sparrow' tagged a different "
        "species"
    )
    names = {k["name"] for k in db.get_photo_keywords(photo_ids[2])}
    assert "Sparrow" in names
    assert "Robin" not in names


def test_grouped_burst_consensus_equals_each_row_species(tmp_path, monkeypatch):
    """The positive half: when a burst IS grouped, its consensus is the row's
    own species, so a panel labelled from ``pr.species`` tells the truth.

    ``group_reviewable`` only stamps a ``group_id`` on a burst whose frames
    all fold to one species key, and ``individual`` is built from that same
    fold — so the consensus ``accept_prediction`` applies is the row's own
    species by construction, not by coincidence.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "new")

    raw = [
        {
            "photo": {
                "id": pid, "filename": f"{i}.jpg", "folder_id": folder_id,
                "timestamp": None, "burst_id": None,
            },
            "folder_path": str(tmp_path),
            "detection_id": did,
            "prediction": "Robin",
            "confidence": 0.9 - (0.1 * i),
            "alternatives": [],
            "taxonomy": {},
            "timestamp": datetime(2024, 1, 1, 12, 0, i),
        }
        for i, (pid, did) in enumerate(zip(photo_ids, det_ids, strict=True))
    ]
    classify_job._store_grouped_predictions(
        raw_results=raw, job_id="job-uni", model_name="bioclip-2",
        grouping_window=10, similarity_threshold=0.99, tax=Tax(),
        db=db, labels_fingerprint="fp-active",
    )

    rows = db.get_predictions(photo_ids=photo_ids, status="pending")
    assert rows and all(r["group_id"] for r in rows)
    for row in rows:
        assert json.loads(row["individual"]) == {"Robin": 2}, (
            "a grouped burst's vote JSON must name exactly one species; a "
            "second key is what would let the consensus diverge from the row"
        )
    result = db.accept_prediction(rows[0]["id"])
    assert result["species"] == rows[0]["species"] == "Robin"


def test_ungrouped_burst_clears_stale_group_id_on_reuse(
    tmp_path, monkeypatch
):
    """Cached predictions from a previously reviewable burst must lose their
    ``group_id`` when the same detections are later reused outside that
    group — e.g. the grouping window shrinks so the burst dissolves into
    singletons. Otherwise group actions on any of the freshly-ungrouped
    detections would retag the whole stale burst together.
    """
    from datetime import datetime

    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path))
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_ids = [
        db.add_photo(
            folder_id, f"{i}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        for i in range(2)
    ]
    det_ids = [
        db.save_detections(
            pid,
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0.9,
                "category": "animal",
            }],
            detector_model="MDV6",
        )[0]
        for pid in photo_ids
    ]

    class Tax:
        def get_hierarchy(self, _species):
            return {}

    # category="new" keeps both runs on the pending path so we can watch
    # group_id transition without the auto-accept marker interfering.
    monkeypatch.setattr("compare.categorize", lambda *_a, **_k: "new")

    def run(grouping_window, extra):
        raw = [
            {
                "photo": {
                    "id": pid, "filename": f"{i}.jpg",
                    "folder_id": folder_id, "timestamp": None,
                    "burst_id": None,
                },
                "folder_path": str(tmp_path),
                "detection_id": did,
                "prediction": "Robin",
                "confidence": 0.9 - (i * 0.01),
                "alternatives": [],
                "taxonomy": {},
                # Frames 5 seconds apart: grouped under a 10s window,
                # ungrouped under a 1s window.
                "timestamp": datetime(2024, 1, 1, 12, 0, i * 5),
                **extra,
            }
            for i, (pid, did) in enumerate(
                zip(photo_ids, det_ids, strict=True)
            )
        ]
        return classify_job._store_grouped_predictions(
            raw_results=raw, job_id="job-abc", model_name="bioclip-2",
            grouping_window=grouping_window, similarity_threshold=0.99,
            tax=Tax(), db=db, labels_fingerprint="fp-active",
        )

    # Run 1: 10s window groups both frames -> group_reviewable (same
    # species) -> both detections share a burst group_id.
    run(10, {})
    initial = {r["detection_id"]: r
               for r in db.get_predictions(photo_ids=photo_ids)}
    gid_before = initial[det_ids[0]]["group_id"]
    assert gid_before, "Run 1 should assign a burst group_id"
    assert initial[det_ids[1]]["group_id"] == gid_before

    # Run 2: cache is reused (_existing=True) with a 1s window that
    # dissolves the burst into singletons. Each detection goes through
    # _store_pending_detection_prediction with group_id=None; the cached
    # rows must lose the stale group_id from Run 1.
    run(1, {"_existing": True})

    after = {r["detection_id"]: r
             for r in db.get_predictions(photo_ids=photo_ids)}
    for det_id in det_ids:
        assert not after[det_id]["group_id"], (
            f"Stale group_id survived cache reuse for detection {det_id}: "
            f"{after[det_id]['group_id']!r}"
        )
        assert not after[det_id]["vote_count"]
        assert not after[det_id]["total_votes"]
        assert not after[det_id]["individual"]


def test_stored_prediction_species_folds_curly_apostrophe(tmp_path):
    """Bundled label files contain curly apostrophes (`Geoffroy’s Tamarin`,
    `Bosc’s Fringe-toed lizard`). predictions.species is matched against
    keywords.name with exact / COLLATE NOCASE compares that cannot fold
    U+2019, so storing the raw label left an accepted ASCII keyword unable
    to match its own prediction. Fold on write so the variant cannot come
    back after the one-shot migration."""
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    try:
        folder_id = db.add_folder("/tmp/p")
        ws = db.create_workspace("A")
        db._active_workspace_id = ws
        db.add_workspace_folder(ws, folder_id)
        photo_id = db.add_photo(
            folder_id, "a.jpg", extension=".jpg", file_size=100,
            file_mtime=1.0,
        )
        det_id = db.save_detections(
            photo_id,
            [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
              "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]

        stored = classify_job._run_classifier_on_detection(
            db=db, detection_id=det_id, classifier_model="bioclip-2",
            labels=["Geoffroy’s Tamarin"], labels_fingerprint="fp1",
            classify_fn=lambda: [
                {"species": "Geoffroy’s Tamarin", "confidence": 0.77}
            ],
        )

        row = db.conn.execute(
            "SELECT species FROM predictions WHERE detection_id = ?",
            (det_id,),
        ).fetchone()
        assert row["species"] == "Geoffroy's Tamarin"
        # The returned dicts feed downstream accept/count logic, so they must
        # agree with what was persisted.
        assert stored[0]["species"] == "Geoffroy's Tamarin"
    finally:
        db.close()


def test_stored_prediction_species_preserves_okina(tmp_path):
    """U+02BB is a letter in `ʻApapane`, not a stray quote — the fold on the
    prediction write path must not touch it, or the Hawaii label set would
    store species that no longer match their keywords."""
    import classify_job
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    try:
        folder_id = db.add_folder("/tmp/p")
        ws = db.create_workspace("A")
        db._active_workspace_id = ws
        db.add_workspace_folder(ws, folder_id)
        photo_id = db.add_photo(
            folder_id, "a.jpg", extension=".jpg", file_size=100,
            file_mtime=1.0,
        )
        det_id = db.save_detections(
            photo_id,
            [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
              "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]

        classify_job._run_classifier_on_detection(
            db=db, detection_id=det_id, classifier_model="bioclip-2",
            labels=["Hawaiʻi ʻamakihi"], labels_fingerprint="fp1",
            classify_fn=lambda: [
                {"species": "Hawaiʻi ʻamakihi", "confidence": 0.77}
            ],
        )

        row = db.conn.execute(
            "SELECT species FROM predictions WHERE detection_id = ?",
            (det_id,),
        ).fetchone()
        assert row["species"] == "Hawaiʻi ʻamakihi"
    finally:
        db.close()


def test_classifier_skipped_when_run_already_recorded(tmp_path, monkeypatch):
    """If (detection, classifier_model, fingerprint) already ran, don't invoke again."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    # Pre-seed a classifier run — any subsequent invocation should bail
    db.record_classifier_run(det_id, "bioclip-2", "abc123", prediction_count=0)

    calls = {"n": 0}
    def fake_classify(*a, **kw):
        calls["n"] += 1
        return []
    monkeypatch.setattr("classify_job._run_classifier_on_detection", fake_classify)

    import classify_job
    classify_job._classify_detection_gated(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="abc123",
        labels=["Robin"], reclassify=False,
    )
    assert calls["n"] == 0, "classifier should be skipped when run key exists"


def test_classify_detection_gated_does_not_cache_zero_count(tmp_path, monkeypatch):
    """A classify_fn returning [] (transient failure or no-op test stub) must
    NOT be recorded as a completed classifier_run — otherwise the next
    non-reclassify pass short-circuits on the gate and the detection is
    permanently stranded without predictions.

    Mirrors the guard already in _record_batch_classifier_runs and the
    inline pipeline_job branch.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
          "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    import classify_job
    # classify_fn=None returns [] with no side effects (see
    # _run_classifier_on_detection). The gate must NOT write a run row.
    classify_job._classify_detection_gated(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="abc123",
        labels=["Robin"], reclassify=False,
    )
    assert db.get_classifier_run_keys(det_id) == set(), (
        "zero-prediction classify_fn must not record a run key"
    )


def test_record_batch_classifier_runs_skips_zero_count(tmp_path):
    """A failed classifier batch (no prediction for a detection) must not be
    cached as a completed run — otherwise the detection is permanently
    stranded on the next non-reclassify pass.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ok, det_failed = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.9},
            {"box": {"x": 0.5, "y": 0.5, "w": 0.5, "h": 0.5}, "confidence": 0.8},
        ],
        detector_model="megadetector-v6",
    )

    batch = [
        {"detection_id": det_ok, "img": object()},
        {"detection_id": det_failed, "img": object()},
    ]
    # Only the first detection made it into raw_results (second one failed)
    raw_results = [{"detection_id": det_ok, "species": "Robin", "confidence": 0.9}]

    import classify_job
    classify_job._record_batch_classifier_runs(
        db, batch, "bioclip-2", "abc123", raw_results
    )

    keys_ok = db.get_classifier_run_keys(det_ok)
    keys_failed = db.get_classifier_run_keys(det_failed)
    assert keys_ok == {("bioclip-2", "abc123")}, "successful detection should be cached"
    assert keys_failed == set(), "failed detection must NOT be cached"


def test_publish_classifier_runs_promotes_after_predictions_persist(tmp_path):
    """``_publish_classifier_runs_for_raw_results`` must run AFTER
    ``_store_grouped_predictions`` writes prediction rows.

    ``promote_and_publish_classifier_run`` reads the persisted
    ``predictions`` rows to synthesize the exportable artifact and only
    then stamps the classifier_runs row with the real
    ``runtime_fingerprint``. If we call it earlier — inside
    ``_record_batch_classifier_runs``, before predictions exist — the
    read returns nothing, the promote silently returns ``None``, and the
    classifier_runs row is left on ``runtime_fingerprint = 'legacy'``.
    ``exportable_artifacts`` filters by real (SHA-256) runtime, so those
    runs are silently omitted from cache bundle exports.
    """
    from computation_cache import (
        ArtifactStore,
        classifier_model_identity,
        exportable_artifacts,
    )
    from db import Database

    photo_hash = "1" * 64
    db = Database(str(tmp_path / "src.db"))
    folder_id = db.add_folder("/tmp/photos")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100,
        file_mtime=1.0, file_hash=photo_hash,
    )

    # Detector run with a real (non-legacy) runtime fingerprint — the
    # classification artifact composes with the detector runtime.
    from computation_cache import runtime_fingerprint, source_input

    detector_runtime = runtime_fingerprint({
        "type": "detection", "model": "megadetector-v6",
        "weights_sha256": "2" * 64, "pipeline": "detector-v1",
    })
    _input, det_input_fp = source_input(
        photo_hash, "vireo-detector-source-v1",
    )
    det_id = db.write_detection_batch(
        photo_id, "megadetector-v6",
        [{"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
          "confidence": 0.9, "category": "animal"}],
        runtime_fingerprint=detector_runtime,
        input_fingerprint=det_input_fp,
    )[0]

    labels_full = "5" * 64
    labels_short = labels_full[:12]
    db.upsert_labels_fingerprint(
        labels_short, "Test birds", [], 1, full_fingerprint=labels_full,
    )

    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "image_encoder.onnx").write_bytes(b"exact model bytes")
    identity = classifier_model_identity({
        "id": "bioclip-test",
        "model_str": "ViT-test",
        "model_type": "bioclip",
        "weights_path": str(model_dir),
        "files": ["image_encoder.onnx"],
        "source": "custom",
    })

    import classify_job
    batch = [{"detection_id": det_id, "img": object()}]
    raw_results = [{
        "detection_id": det_id, "species": "Robin", "confidence": 0.9,
    }]

    # First, _record_batch_classifier_runs runs *before* predictions
    # exist — matches what _classify_photos does after _flush_batch
    # appends to raw_results.  This writes the classifier_runs row but
    # MUST NOT try to publish yet.
    classify_job._record_batch_classifier_runs(
        db, batch, "BioCLIP", labels_short, raw_results,
        labels_fingerprint_full=labels_full, model_identity=identity,
    )
    run = db.conn.execute(
        "SELECT runtime_fingerprint FROM classifier_runs "
        "WHERE detection_id = ?", (det_id,),
    ).fetchone()
    assert run["runtime_fingerprint"] == "legacy", (
        "record_batch must NOT attempt to publish before predictions "
        "are persisted — that call silently no-ops and leaves the run "
        "stranded on 'legacy' runtime"
    )

    # Publishing without the persisted prediction rows still no-ops.
    store = ArtifactStore(tmp_path / "store")
    classify_job._publish_classifier_runs_for_raw_results(
        db, raw_results, "BioCLIP", labels_short,
        labels_fingerprint_full=labels_full, model_identity=identity,
    )
    run = db.conn.execute(
        "SELECT runtime_fingerprint FROM classifier_runs "
        "WHERE detection_id = ?", (det_id,),
    ).fetchone()
    assert run["runtime_fingerprint"] == "legacy"

    # Simulate _store_grouped_predictions persisting the prediction row,
    # then re-run the publish pass — the classifier_runs row now gets
    # its real runtime_fingerprint and the artifact appears in exports.
    db.add_prediction(
        det_id, species="Robin", confidence=0.9, model="BioCLIP",
        labels_fingerprint=labels_short,
    )
    classify_job._publish_classifier_runs_for_raw_results(
        db, raw_results, "BioCLIP", labels_short,
        labels_fingerprint_full=labels_full, model_identity=identity,
        # No `store` param on the helper — the default ArtifactStore
        # writes to the configured cache dir.  Publishing to that store
        # is a side-effect; the assertions below hit the DB stamp and
        # the exportable_artifacts view, which do not depend on which
        # store received the artifact.
    )
    run = db.conn.execute(
        "SELECT runtime_fingerprint, input_fingerprint, labels_fingerprint_full "
        "FROM classifier_runs WHERE detection_id = ?", (det_id,),
    ).fetchone()
    assert run["labels_fingerprint_full"] == labels_full
    assert len(run["runtime_fingerprint"]) == 64
    assert run["runtime_fingerprint"] != "legacy"
    assert len(run["input_fingerprint"]) == 64

    # The classifier run is now visible to bundle export.
    _artifacts, summary = exportable_artifacts(db)
    assert summary["classifier_runs"] == 1


def test_publish_classifier_runs_forwards_captured_input_source(monkeypatch):
    """Delayed promotion uses preparation-time provenance, not a later row."""
    import classify_job
    import computation_cache

    calls = []

    def capture_publish(*_args, **kwargs):
        calls.append(kwargs["source_is_original"])

    monkeypatch.setattr(
        computation_cache, "promote_and_publish_classifier_run",
        capture_publish,
    )

    classify_job._publish_classifier_runs_for_raw_results(
        object(),
        [
            {"detection_id": 1, "_input_source": "working_copy"},
            {"detection_id": 2, "_input_source": "original"},
        ],
        "BioCLIP", "abc123",
        labels_fingerprint_full="1" * 64,
        model_identity={"id": "test"},
    )

    assert calls == [False, True]


def test_all_photos_cache_satisfied_requires_matching_predictions_row(tmp_path):
    """A classifier_runs row with no matching ``predictions`` row is a
    torn write from a crashed local job — ``_record_batch_classifier_runs``
    commits the run *before* ``_store_grouped_predictions`` persists the
    prediction rows.  Counting it as covered would make the retry enter
    ``_finalize_cached_only``, which skips the missing prediction rows
    entirely and reports success without repairing them.
    """
    from classify_job import _all_photos_cache_satisfied
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")[0]

    # Torn write: classifier_runs exists, predictions do not.
    db.record_classifier_run(det_id, "BioCLIP", "fp-cached",
                             prediction_count=1)

    # Coverage query must refuse — with either the fully-specified
    # (model + fp) form and the fresh-install (both-unspecified) form.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint="fp-cached",
    ) is False
    assert _all_photos_cache_satisfied(db, [pid]) is False

    # After the missing prediction row lands, cache is now really
    # satisfied.
    db.add_prediction(
        det_id, species="Robin", confidence=0.9, model="BioCLIP",
        labels_fingerprint="fp-cached",
    )
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint="fp-cached",
    ) is True
    assert _all_photos_cache_satisfied(db, [pid]) is True


def test_classifier_fingerprint_upserted(tmp_path, monkeypatch):
    """When a classifier runs, the labels fingerprint is upserted."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )

    from labels_fingerprint import compute_fingerprint
    labels = ["Robin", "Sparrow"]
    expected_fp = compute_fingerprint(labels)

    import classify_job
    classify_job._record_labels_fingerprint(
        db, fingerprint=expected_fp, labels=labels,
        sources=["/tmp/active.txt"],
    )
    row = db.conn.execute(
        "SELECT display_name, label_count FROM labels_fingerprints WHERE fingerprint=?",
        (expected_fp,),
    ).fetchone()
    assert row["label_count"] == 2


def test_classify_photos_iterates_over_detections(tmp_path):
    """_classify_photos should classify each detection independently."""
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    # Create a test image
    img = Image.new("RGB", (200, 200), color="red")
    img_path = tmp_path / "multi.jpg"
    img.save(str(img_path))

    photos = [
        {"id": 1, "filename": "multi.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}
    # Two detections for photo 1
    detection_map = {
        1: [
            {"id": 101, "box_x": 0.1, "box_y": 0.1, "box_w": 0.3, "box_h": 0.3,
             "confidence": 0.95, "category": "animal"},
            {"id": 102, "box_x": 0.5, "box_y": 0.5, "box_w": 0.2, "box_h": 0.2,
             "confidence": 0.80, "category": "animal"},
        ]
    }
    existing_preds = set()

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds_1 = [{"species": "Northern Cardinal", "score": 0.95, "taxonomy": None}]
    fake_preds_2 = [{"species": "Blue Jay", "score": 0.88, "taxonomy": None}]

    mock_clf = MagicMock()
    mock_clf.classify_batch_with_embedding.return_value = [
        (fake_preds_1, fake_embedding),
        (fake_preds_2, fake_embedding),
    ]

    mock_db = MagicMock()
    mock_db.get_photo_embedding.return_value = None

    # Use side_effect to return a fresh Image each call, since _prepare_image
    # closes the original image after cropping (resource leak fix).
    with patch("classify_job.load_image", side_effect=lambda *a, **kw: Image.new("RGB", (200, 200))):
        raw_results, failed, skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=existing_preds,
            clf=mock_clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
        )

    assert len(raw_results) == 2
    assert raw_results[0]["detection_id"] == 101
    assert raw_results[0]["prediction"] == "Northern Cardinal"
    assert raw_results[1]["detection_id"] == 102
    assert raw_results[1]["prediction"] == "Blue Jay"
    assert failed == 0
    assert skipped == 0


# ── _classify_photos tests ──────────────────────────────────────────────────


def test_classify_photos_new_photo(tmp_path):
    """Phase 6: classifies a new photo and returns raw results."""
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    # Create a test image
    img = Image.new("RGB", (200, 200), color="red")
    img_path = tmp_path / "bird.jpg"
    img.save(str(img_path))

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}
    detection_map = {}
    existing_preds = set()

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Northern Cardinal", "score": 0.95, "taxonomy": None}]

    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)
    mock_clf.classify_batch_with_embedding.return_value = [(fake_preds, fake_embedding)]

    mock_db = MagicMock()
    mock_db.get_photo_embedding.return_value = None

    # Use side_effect to return a fresh Image each call, since _flush_batch
    # closes images after classification (resource leak fix).
    with patch("classify_job.load_image", side_effect=lambda *a, **kw: Image.new("RGB", (200, 200))):
        raw_results, failed, skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=existing_preds,
            clf=mock_clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
        )

    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Northern Cardinal"
    assert raw_results[0]["confidence"] == 0.95
    assert failed == 0
    assert skipped == 0
    # Fresh classification writes two embedding rows per detection: the
    # legacy photo-level (variant='') entry for backwards-compatible
    # consumers (culling, similarity search) and a per-detection
    # (variant='det:<id>') entry so multi-subject reruns from cache use
    # the correct per-subject vector instead of last-wins.
    embed_calls = mock_db.upsert_photo_embedding.call_args_list
    assert len(embed_calls) == 2
    variants = {call.kwargs.get("variant", "") for call in embed_calls}
    assert "" in variants
    assert any(v.startswith("det:") for v in variants)


def test_classify_photos_skips_existing(tmp_path):
    """Skipping is now per-detection via classifier_runs, not per-photo.

    When a detection's (model, fingerprint) has a cached classifier run,
    the classifier is not re-invoked, but the cached top-1 prediction is
    surfaced into raw_results so downstream grouping still sees it.
    """
    from unittest.mock import MagicMock

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    folders = {10: str(tmp_path)}

    mock_clf = MagicMock()
    mock_db = MagicMock()
    # Detection 101 has a cached classifier_run for (BioCLIP, legacy).
    mock_db.get_classifier_run_keys.return_value = {("BioCLIP", "legacy")}
    mock_db.get_predictions_for_detection.return_value = [
        {"species": "Northern Cardinal", "confidence": 0.95,
         "detection_id": 101},
    ]
    mock_db.get_photo_embedding.return_value = None

    detection_map = {
        1: [{"id": 101, "box_x": 0.1, "box_y": 0.1,
             "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
             "category": "animal"}],
    }

    raw_results, failed, skipped = _classify_photos(
        photos=photos,
        folders=folders,
        detection_map=detection_map,
        existing_preds=set(),  # dead parameter post-refactor
        clf=mock_clf,
        model_type="bioclip",
        model_name="BioCLIP",
        runner=runner,
        job=job,
        db=mock_db,
    )

    assert skipped == 1, "cached detection should count as skipped"
    assert len(raw_results) == 1, "cached prediction must be surfaced"
    assert raw_results[0]["_existing"] is True
    assert raw_results[0]["prediction"] == "Northern Cardinal"
    mock_clf.classify_with_embedding.assert_not_called()


# ── Top-N predictions tests ────────────────────────────────────────────────


def test_flush_batch_stores_top_n_predictions(tmp_path):
    """_flush_batch keeps top_k predictions per image, not just top-1."""
    from unittest.mock import MagicMock

    from classify_job import _flush_batch

    db = MagicMock()
    raw_results = []

    # Classifier returns 5 ranked predictions
    all_preds = [
        {"species": "Robin", "score": 0.70, "taxonomy": None},
        {"species": "Sparrow", "score": 0.15, "taxonomy": None},
        {"species": "Finch", "score": 0.10, "taxonomy": None},
        {"species": "Wren", "score": 0.03, "taxonomy": None},
        {"species": "Jay", "score": 0.02, "taxonomy": None},
    ]
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [(all_preds, None)]

    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    failed = _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results, top_k=3)
    assert failed == 0
    assert len(raw_results) == 1

    item = raw_results[0]
    # Should have top prediction as before
    assert item["prediction"] == "Robin"
    assert item["confidence"] == 0.70
    # Should also have alternatives list
    assert "alternatives" in item
    assert len(item["alternatives"]) == 2
    assert item["alternatives"][0]["species"] == "Sparrow"
    assert item["alternatives"][1]["species"] == "Finch"


def test_flush_batch_top_k_1_has_empty_alternatives():
    """_flush_batch with top_k=1 (default) produces empty alternatives list."""
    from unittest.mock import MagicMock

    from classify_job import _flush_batch

    db = MagicMock()
    raw_results = []

    all_preds = [
        {"species": "Robin", "score": 0.70, "taxonomy": None},
        {"species": "Sparrow", "score": 0.15, "taxonomy": None},
    ]
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [(all_preds, None)]

    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    failed = _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)
    assert failed == 0
    assert len(raw_results) == 1
    assert raw_results[0]["alternatives"] == []


def test_flush_batch_default_top_k_is_one():
    """Default top_k=1 preserves backward-compatible behavior (no alternatives)."""
    from unittest.mock import MagicMock

    from classify_job import _flush_batch

    db = MagicMock()
    raw_results = []

    all_preds = [
        {"species": "Robin", "score": 0.70, "taxonomy": None},
        {"species": "Sparrow", "score": 0.15, "taxonomy": None},
    ]
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [(all_preds, None)]

    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Robin"
    assert raw_results[0]["alternatives"] == []


# ── GPU lock scope ────────────────────────────────────────────────────────


def test_flush_batch_writes_per_detection_embedding(tmp_path):
    """_flush_batch persists a per-detection embedding alongside the
    photo-level row so multi-subject cache reruns don't reuse the last
    detection's vector for every subject.

    Regression for the Codex P2 review on PR #1294: when a multi-subject
    photo is rerun from the classifier cache, each cached detection was
    fetching the same ``(photo, model)`` row — set by whichever detection
    happened to classify last — so ``refine_groups_by_similarity`` merged
    distinct subjects or shuffled burst grouping on the non-inference
    path. Persist the embedding per ``det:{detection_id}`` variant so
    the cached path can restore each subject's own vector.
    """
    from unittest.mock import MagicMock

    import numpy as np
    from classify_job import _flush_batch
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/photos", name="photos")
    photo_id = db.add_photo(
        folder_id=folder_id, filename="bird.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=None, width=1, height=1,
    )
    # Simulate two qualifying detections on one photo.
    det_ids = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0.0, "y": 0.0, "w": 0.5, "h": 1.0},
             "confidence": 0.9, "category": "animal"},
            {"box": {"x": 0.5, "y": 0.0, "w": 0.5, "h": 1.0},
             "confidence": 0.9, "category": "animal"},
        ],
        detector_model="MegaDetector",
    )

    emb_a = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
    emb_b = np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32)

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Robin", "score": 0.9, "taxonomy": None}], emb_a),
        ([{"species": "Jay", "score": 0.9, "taxonomy": None}], emb_b),
    ]

    photo_row = {"id": photo_id, "filename": "bird.jpg", "timestamp": None}
    batch = [
        {"photo": photo_row, "detection_id": det_ids[0],
         "folder_path": "/photos", "image_path": "/photos/bird.jpg",
         "img": MagicMock()},
        {"photo": photo_row, "detection_id": det_ids[1],
         "folder_path": "/photos", "image_path": "/photos/bird.jpg",
         "img": MagicMock()},
    ]

    raw_results: list = []
    _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)

    # Per-detection variants recover each detection's own embedding
    # rather than whichever landed last on the photo-level row.
    per_a = db.get_photo_embedding(
        photo_id, "test-model", variant=f"det:{det_ids[0]}",
    )
    per_b = db.get_photo_embedding(
        photo_id, "test-model", variant=f"det:{det_ids[1]}",
    )
    assert per_a is not None and per_b is not None
    assert np.frombuffer(per_a, dtype=np.float32).tolist() == [1.0, 0.0, 0.0, 0.0]
    assert np.frombuffer(per_b, dtype=np.float32).tolist() == [0.0, 1.0, 0.0, 0.0]
    # Photo-level (variant='') stays populated for legacy consumers
    # (culling, similarity search); last-wins is unchanged from prior
    # behavior — just no longer the sole source of the classify cache.
    photo_level = db.get_photo_embedding(photo_id, "test-model")
    assert photo_level is not None


def test_flush_batch_does_not_hold_gpu_lock_around_helper_or_db():
    """``_flush_batch`` must not hold the GPU semaphore around the
    classifier helper call or the DB writes.

    Regression for the Codex P2 on PR #899: the process-wide GPU lock
    has been pushed down into the classifier implementations (around
    ``session.run`` only). At the ``_flush_batch`` level, neither the
    classifier helper invocation nor ``db.upsert_photo_embedding`` should
    see the lock held, so concurrent pipelines' detector/SAM/DINO GPU
    batches aren't blocked while this one does CPU preprocessing or DB
    work. The lock-held-during-``session.run`` half of this guarantee is
    asserted in ``test_classifier.py`` /
    ``test_timm_classifier.py``.
    """
    from unittest.mock import MagicMock

    import pipeline_locks
    from classify_job import _flush_batch

    snapshots = {}

    def record_inside_helper(images, threshold=0):
        snapshots["during_helper"] = pipeline_locks._GPU_SEMAPHORE._value
        return [
            (
                [{"species": "Robin", "score": 0.7, "taxonomy": None}],
                _FakeEmbedding(),
            )
            for _ in images
        ]

    def record_inside_db(photo_id, model_name, embedding_bytes, variant=""):
        snapshots["during_db_write"] = pipeline_locks._GPU_SEMAPHORE._value

    clf = MagicMock()
    clf.classify_batch_with_embedding.side_effect = record_inside_helper

    db = MagicMock()
    db.upsert_photo_embedding.side_effect = record_inside_db

    raw_results = []
    batch = [{
        "photo": {"id": 1, "filename": "bird.jpg", "timestamp": None},
        "detection_id": 10,
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "img": MagicMock(),
    }]

    baseline = pipeline_locks._GPU_SEMAPHORE._value
    _flush_batch(batch, clf, "bioclip", "test-model", db, raw_results)
    assert pipeline_locks._GPU_SEMAPHORE._value == baseline, (
        "semaphore must be released on the way out"
    )
    assert snapshots["during_helper"] == baseline, (
        "GPU lock must NOT be held around clf.classify_batch_with_embedding "
        "at the _flush_batch level — the lock now lives inside the classifier "
        "helpers, wrapping only ``session.run``"
    )
    assert snapshots["during_db_write"] == baseline, (
        "GPU lock must NOT be held during db.upsert_photo_embedding so "
        "concurrent pipelines can do GPU work while this one persists"
    )


class _FakeEmbedding:
    """Stand-in for a numpy array that implements only ``.tobytes()``."""

    def tobytes(self):
        return b"\x00" * 16


# ── Top-N: _store_grouped_predictions alternatives tests ─────────────────────


def test_store_grouped_predictions_saves_alternatives(tmp_path):
    """_store_grouped_predictions stores alternatives with status='alternative'."""
    from unittest.mock import patch

    from classify_job import _store_grouped_predictions
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0, timestamp="2024-01-15T10:00:00")
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")

    raw_results = [{
        "photo": {"id": pid, "filename": "bird.jpg", "timestamp": "2024-01-15T10:00:00"},
        "detection_id": det_ids[0],
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "prediction": "Robin",
        "confidence": 0.85,
        "timestamp": None,
        "filename": "bird.jpg",
        "embedding": None,
        "taxonomy": None,
        "alternatives": [
            {"species": "Sparrow", "confidence": 0.10, "taxonomy": None},
            {"species": "Finch", "confidence": 0.05, "taxonomy": None},
        ],
    }]

    with patch("xmp.read_keywords", return_value=[]), \
         patch("compare.categorize", return_value="new"):
        result = _store_grouped_predictions(
            raw_results=raw_results,
            job_id="test-job-123456",
            model_name="test-model",
            grouping_window=10,
            similarity_threshold=0.85,
            tax=None,
            db=db,
        )

    assert result["predictions_stored"] == 1

    all_preds = db.get_predictions()
    assert len(all_preds) == 3  # 1 pending + 2 alternatives

    pending = db.get_predictions(status="pending")
    assert len(pending) == 1
    assert pending[0]["species"] == "Robin"

    alts = db.get_predictions(status="alternative")
    assert len(alts) == 2
    alt_species = {p["species"] for p in alts}
    assert alt_species == {"Sparrow", "Finch"}


# ── _store_grouped_predictions tests ─────────────────────────────────────────


def test_store_grouped_predictions_single_photo():
    """Phase 7: single-photo group stores prediction directly."""
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird.jpg"},
            "detection_id": 101,
            "folder_path": "/photos",
            "prediction": "Northern Cardinal",
            "confidence": 0.95,
            "timestamp": None,
            "filename": "bird.jpg",
            "embedding": None,
            "taxonomy": {"order": "Passeriformes", "family": "Cardinalidae"},
        },
    ]

    result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    assert result["predictions_stored"] == 1
    assert result["burst_groups"] == 0
    mock_db.add_prediction.assert_called_once()
    call_kwargs = mock_db.add_prediction.call_args[1]
    assert call_kwargs["species"] == "Northern Cardinal"
    assert call_kwargs["detection_id"] == 101


def test_store_grouped_predictions_burst_group():
    """Phase 7: multi-photo group computes consensus and stores for all photos."""
    from datetime import datetime
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird1.jpg"},
            "detection_id": 101,
            "folder_path": "/photos",
            "prediction": "Northern Cardinal",
            "confidence": 0.95,
            "timestamp": datetime(2024, 1, 15, 10, 0, 0),
            "filename": "bird1.jpg",
            "embedding": None,
            "taxonomy": None,
        },
        {
            "photo": {"id": 2, "filename": "bird2.jpg"},
            "detection_id": 102,
            "folder_path": "/photos",
            "prediction": "Northern Cardinal",
            "confidence": 0.90,
            "timestamp": datetime(2024, 1, 15, 10, 0, 3),
            "filename": "bird2.jpg",
            "embedding": None,
            "taxonomy": None,
        },
    ]

    result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    assert result["predictions_stored"] == 2
    assert result["burst_groups"] >= 1
    assert mock_db.add_prediction.call_count == 2


def test_store_grouped_predictions_folds_species_before_group_consensus():
    """When the active labels carry both `Say's Phoebe` (ASCII) and
    `Say’s Phoebe` (U+2019) — a common situation with merged bundled
    label files — the classifier can return either spelling per frame.
    Before central folding was applied inside ``_store_grouped_predictions``,
    ``group_species`` was computed off the raw ``item["prediction"]``
    strings, so a two-frame burst with one of each spelling produced
    ``{'say\\'s phoebe', 'say’s phoebe'}`` and ``group_reviewable``
    became False. The consensus itself also split the votes.

    Because ``add_prediction`` folds the stored species centrally, both
    frames land on the same prediction row afterwards; the burst was
    unanimous. So the burst_group *must* survive with a group_id and
    the full vote count, or the survivor prediction silently loses its
    grouping metadata."""
    from datetime import datetime
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird1.jpg"},
            "detection_id": 101,
            "folder_path": "/photos",
            "prediction": "Say's Phoebe",
            "confidence": 0.95,
            "timestamp": datetime(2024, 1, 15, 10, 0, 0),
            "filename": "bird1.jpg",
            "embedding": None,
            "taxonomy": None,
        },
        {
            "photo": {"id": 2, "filename": "bird2.jpg"},
            "detection_id": 102,
            "folder_path": "/photos",
            # Curly-apostrophe variant of the same species name.
            "prediction": "Say’s Phoebe",
            "confidence": 0.90,
            "timestamp": datetime(2024, 1, 15, 10, 0, 3),
            "filename": "bird2.jpg",
            "embedding": None,
            "taxonomy": None,
        },
    ]

    result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    assert result["burst_groups"] == 1, (
        "unanimous burst was split into non-group predictions because "
        "raw species strings differ only by apostrophe glyph"
    )
    # Both frames should be stored with a group_id and full vote counts.
    calls = mock_db.add_prediction.call_args_list
    assert len(calls) == 2
    for c in calls:
        kwargs = c.kwargs or c[1]
        assert kwargs["group_id"] is not None, (
            "group_id dropped: group_reviewable was False, so the "
            "survivor prediction lost its burst grouping"
        )
        assert kwargs["vote_count"] == 2
        assert kwargs["total_votes"] == 2
        assert kwargs["individual"] is not None


def test_store_grouped_predictions_folds_case_for_burst_consensus():
    """When burst frames' predictions differ in both apostrophe glyph AND
    ASCII capitalization (e.g., `Say's Phoebe` and `Say’s phoebe`), the
    apostrophe fold alone still yields two distinct consensus keys.
    ``consensus_prediction`` keys on the raw string, so a semantically
    unanimous burst would report a `1/2` vote count. Meanwhile
    ``group_species`` already lowercases and would set
    ``group_reviewable=True``: the mismatch stored split ``individual``
    entries against an inconsistent count.

    Canonicalizing to the first-seen casing for each ASCII-lowercase
    fold key sums the vote correctly while ``individual_predictions``
    still shows a real display-cased name."""
    import json
    from datetime import datetime
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "bird1.jpg"},
            "detection_id": 101,
            "folder_path": "/photos",
            "prediction": "Say's Phoebe",
            "confidence": 0.95,
            "timestamp": datetime(2024, 1, 15, 10, 0, 0),
            "filename": "bird1.jpg",
            "embedding": None,
            "taxonomy": None,
        },
        {
            "photo": {"id": 2, "filename": "bird2.jpg"},
            "detection_id": 102,
            "folder_path": "/photos",
            # Curly apostrophe AND lowercase — differs from frame 1 in
            # two axes at once.
            "prediction": "Say’s phoebe",
            "confidence": 0.90,
            "timestamp": datetime(2024, 1, 15, 10, 0, 3),
            "filename": "bird2.jpg",
            "embedding": None,
            "taxonomy": None,
        },
    ]

    result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    assert result["burst_groups"] == 1
    calls = mock_db.add_prediction.call_args_list
    assert len(calls) == 2
    for c in calls:
        kwargs = c.kwargs or c[1]
        assert kwargs["group_id"] is not None
        assert kwargs["vote_count"] == 2, (
            "vote_count was split because case-variant apostrophe folds "
            "keyed consensus separately"
        )
        assert kwargs["total_votes"] == 2
        # After case folding the individual dict has one entry summing
        # to 2, not two entries of 1 each.
        votes = json.loads(kwargs["individual"])
        assert sum(votes.values()) == 2
        assert len(votes) == 1


# ── Task 6: run_classify_job full pipeline test ───────────────────────────────


def test_run_classify_job_full_pipeline(tmp_path):
    """run_classify_job orchestrates all phases end-to-end."""
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    runner = FakeRunner()
    job = _make_job()

    # Create test image
    img = Image.new("RGB", (200, 200), color="blue")
    img_path = tmp_path / "bird.jpg"
    img.save(str(img_path))

    # Set up mock DB
    mock_db_instance = MagicMock()
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "bird.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None
    # Subject-skip gate: with no subject types configured the gate is a no-op.
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Northern Cardinal", "score": 0.95, "taxonomy": None}]

    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)
    mock_clf.classify_batch_with_embedding.return_value = [(fake_preds, fake_embedding)]

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )

    with patch("classify_job.Database", return_value=mock_db_instance), \
         patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job._load_labels", return_value=(["Northern Cardinal"], False, [])), \
         patch("classify_job.Classifier", return_value=mock_clf), \
         patch("classify_job._detect_subjects", return_value=({}, 0)):
        result = run_classify_job(job, runner, str(tmp_path / "test.db"), 1, params)

    assert result["total"] == 1
    assert result["predictions_stored"] == 1
    assert result["failed"] == 0
    mock_db_instance.add_prediction.assert_called_once()


# ── Task 7: Integration test — route delegates to run_classify_job ─────────


def test_api_route_calls_run_classify_job(app_and_db):
    """The /api/jobs/classify route delegates to run_classify_job."""

    app, db = app_and_db
    client = app.test_client()

    # Create a collection so the request is valid
    import json as _json
    col_id = db.add_collection("Test", _json.dumps([{"type": "all"}]))

    captured = {}

    def fake_run(job, runner, db_path, workspace_id, params, vireo_dir=None):
        captured["params"] = params
        captured["workspace_id"] = workspace_id
        return {
            "total": 0,
            "predictions_stored": 0,
            "burst_groups": 0,
            "already_classified": 0,
            "already_labeled": 0,
            "detected": 0,
            "failed": 0,
        }

    import classify_job

    original = classify_job.run_classify_job
    classify_job.run_classify_job = fake_run
    try:
        resp = client.post(
            "/api/jobs/classify",
            json={"collection_id": col_id, "model_name": "TestModel"},
        )
    finally:
        classify_job.run_classify_job = original

    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("classify-")


# ── Task 8: _prepare_image working copy tests ─────────────────────────────────


def test_prepare_image_uses_working_copy(tmp_path):
    """_prepare_image loads from working copy when vireo_dir is provided."""
    from classify_job import _prepare_image

    # Set up vireo_dir with a working copy JPEG
    vireo_dir = tmp_path / "vireo"
    working_dir = vireo_dir / "working"
    working_dir.mkdir(parents=True)

    wc_img = Image.new("RGB", (2000, 1500), color="blue")
    wc_path = working_dir / "42.jpg"
    wc_img.save(str(wc_path), "JPEG")

    photo = {
        "id": 42,
        "folder_id": 10,
        "filename": "bird.nef",
        "working_copy_path": "working/42.jpg",
    }
    folders = {10: str(tmp_path / "photos")}

    # Do NOT create the original file — _prepare_image should use the working copy
    img, folder_path, image_path = _prepare_image(
        photo, folders, None, vireo_dir=str(vireo_dir)
    )

    assert img is not None
    # The result should be thumbnailed to 1024
    assert max(img.size) <= 1024
    assert img.info["_vireo_input_source"] == "working_copy"


def test_prepare_image_falls_back_without_working_copy(tmp_path):
    """_prepare_image falls back to load_image when no working copy exists."""

    from classify_job import _prepare_image

    vireo_dir = str(tmp_path / "vireo")

    # Create a real original image
    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    orig_img = Image.new("RGB", (2000, 1500), color="red")
    orig_img.save(str(photos_dir / "bird.jpg"), "JPEG")

    photo = {
        "id": 99,
        "folder_id": 10,
        "filename": "bird.jpg",
        "working_copy_path": None,
    }
    folders = {10: str(photos_dir)}

    img, folder_path, image_path = _prepare_image(
        photo, folders, None, vireo_dir=vireo_dir
    )

    assert img is not None
    assert max(img.size) <= 1024
    assert img.info["_vireo_input_source"] == "original"


def test_prepare_image_crops_detection_from_working_copy(tmp_path):
    """_prepare_image crops to detection bbox when using a working copy."""
    from classify_job import _prepare_image

    # Set up vireo_dir with a working copy
    vireo_dir = tmp_path / "vireo"
    working_dir = vireo_dir / "working"
    working_dir.mkdir(parents=True)

    wc_img = Image.new("RGB", (2000, 1500), color="green")
    wc_path = working_dir / "7.jpg"
    wc_img.save(str(wc_path), "JPEG")

    photo = {
        "id": 7,
        "folder_id": 10,
        "filename": "bird.arw",
        "working_copy_path": "working/7.jpg",
    }
    folders = {10: str(tmp_path / "photos")}

    detection = {
        "box_x": 0.2,
        "box_y": 0.2,
        "box_w": 0.4,
        "box_h": 0.4,
    }

    img, folder_path, image_path = _prepare_image(
        photo, folders, detection, vireo_dir=str(vireo_dir)
    )

    assert img is not None
    # Should be cropped and thumbnailed
    assert max(img.size) <= 1024


def test_prepare_image_no_vireo_dir_uses_original(tmp_path):
    """_prepare_image without vireo_dir loads original file directly."""
    from classify_job import _prepare_image

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    orig_img = Image.new("RGB", (800, 600), color="yellow")
    orig_img.save(str(photos_dir / "bird.jpg"), "JPEG")

    photo = {
        "id": 1,
        "folder_id": 10,
        "filename": "bird.jpg",
        "working_copy_path": "working/1.jpg",  # has path but no vireo_dir
    }
    folders = {10: str(photos_dir)}

    img, folder_path, image_path = _prepare_image(
        photo, folders, None  # no vireo_dir
    )

    assert img is not None


# ── Task 9: Subject-tagged skip-gate ──────────────────────────────────────────


def _setup_two_photo_classify_workspace(tmp_path):
    """Create a real DB with two photos in a static collection.
    p1 is tagged with a 'genre' keyword (Landscape); p2 is untagged.
    Returns (db_path, ws_id, col_id, p1, p2)."""
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)

    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    db.add_workspace_folder(ws, folder_id)
    p1 = db.add_photo(
        folder_id, "p1.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
    )
    p2 = db.add_photo(
        folder_id, "p2.jpg", extension=".jpg", file_size=100, file_mtime=2.0,
    )

    # Tag p1 with a genre keyword so it should be filtered out by the skip-gate.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)

    col_id = db.add_collection(
        "static",
        json.dumps([{"field": "photo_ids", "value": [p1, p2]}]),
    )
    db.conn.close()
    return db_path, ws, col_id, p1, p2


def _run_classify_capturing_photos(db_path, ws, col_id, reclassify):
    """Run run_classify_job with all heavy dependencies stubbed and return
    the list of photo IDs that flowed into _detect_subjects + the events
    pushed by the runner."""
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job

    runner = FakeRunner()
    job = _make_job()

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }

    captured_photos = []

    def _fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        captured_photos.extend([p["id"] for p in photos])
        return ({}, 0)

    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=reclassify,
    )

    with patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch(
            "classify_job._load_labels", return_value=(["Northern Cardinal"], False, []),
         ), \
         patch("classify_job.Classifier"), \
         patch("classify_job._detect_subjects", side_effect=_fake_detect_subjects):
        result = run_classify_job(job, runner, db_path, ws, params)

    return captured_photos, runner.events, result


def test_classify_job_skips_photos_with_subject_keywords(tmp_path):
    """When a photo has a keyword whose type is in the workspace's
    subject_types, the classifier doesn't include it in the run.

    Verified by capturing the photo IDs passed into the (stubbed)
    _detect_subjects step. Only p2 (untagged) should reach detection;
    p1 (tagged 'Landscape', type='genre') is skipped at the load step.
    """
    db_path, ws, col_id, p1, p2 = _setup_two_photo_classify_workspace(tmp_path)

    seen, events, _ = _run_classify_capturing_photos(
        db_path, ws, col_id, reclassify=False,
    )

    assert seen == [p2], (
        f"Expected only p2 ({p2}) to reach the detector, got {seen}"
    )

    # The skip-count should be surfaced in a progress event.
    progress_events = [
        d for (_jid, kind, d) in events
        if kind == "progress" and d.get("skipped_subject")
    ]
    assert progress_events, "Expected a progress event with skipped_subject"
    assert progress_events[0]["skipped_subject"] == 1
    assert progress_events[0]["phase"] == "Step 1/5: Loading photos"


def test_classify_job_publishes_label_source_on_classify_step(tmp_path):
    """The standalone classify job names its label space on the classify step.

    "Classify species" is the same row whether the run compares against two
    regional lists or the whole Tree of Life, so the row has to carry which
    one actually loaded.
    """
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job

    db_path, ws, col_id, _p1, _p2 = _setup_two_photo_classify_workspace(tmp_path)

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=["/l/ca.txt", "/l/wa.txt"],
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )
    saved = [
        {"labels_file": "/l/ca.txt", "name": "California, US Birds"},
        {"labels_file": "/l/wa.txt", "name": "Washington, US Birds"},
    ]

    runner = FakeRunner()
    with patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job.get_saved_labels", return_value=saved), \
         patch(
            "classify_job._load_labels",
            return_value=(["Northern Cardinal", "Blue Jay"], False, saved),
         ), \
         patch("classify_job.Classifier"), \
         patch(
            "classify_job._detect_subjects",
            side_effect=lambda *a, **k: ({}, 0),
         ):
        run_classify_job(_make_job(), runner, db_path, ws, params)

    published = [
        kwargs["label_source"]
        for (step_id, kwargs) in runner.steps
        if step_id == "classify" and "label_source" in kwargs
    ]
    assert published == [
        "2 species from 2 lists: California, US Birds, Washington, US Birds"
    ], f"classify step should name both active lists; got {published!r}"


def test_classify_job_reclassify_true_bypasses_subject_skip(tmp_path):
    """With reclassify=True, even subject-tagged photos are reprocessed,
    so users can verify or refresh existing tags."""
    db_path, ws, col_id, p1, p2 = _setup_two_photo_classify_workspace(tmp_path)

    seen, events, _ = _run_classify_capturing_photos(
        db_path, ws, col_id, reclassify=True,
    )

    assert sorted(seen) == sorted([p1, p2]), (
        f"reclassify=True should bypass the skip-gate; got {seen}"
    )
    skip_events = [
        d for (_jid, kind, d) in events
        if kind == "progress" and d.get("skipped_subject")
    ]
    assert not skip_events, (
        f"reclassify=True should not surface a skipped_subject event, got "
        f"{skip_events}"
    )


def test_classify_job_always_skips_wildlife_excluded_photos(tmp_path):
    """Explicit Not Wildlife state skips classification even on reclassify."""
    from db import Database

    db_path, ws, col_id, p1, p2 = _setup_two_photo_classify_workspace(tmp_path)
    db = Database(db_path)
    db.set_active_workspace(ws)
    db.update_photo_wildlife_excluded(p2, True)
    db.conn.close()

    seen, events, _ = _run_classify_capturing_photos(
        db_path, ws, col_id, reclassify=True,
    )

    assert seen == [p1]
    progress_events = [
        d for (_jid, kind, d) in events
        if kind == "progress" and d.get("skipped_wildlife_excluded")
    ]
    assert progress_events
    assert progress_events[0]["skipped_wildlife_excluded"] == 1


def test_classify_job_short_circuits_when_all_photos_skipped(tmp_path):
    """Regression: when the subject-skip filter empties the photo set, the
    job must short-circuit before model load. Loading a model is expensive
    and can fail; doing it for zero photos undermines the skip behavior."""
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    db.add_workspace_folder(ws, folder_id)
    p1 = db.add_photo(
        folder_id, "p1.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
    )
    # Tag p1 with a genre keyword so the skip-gate filters it out.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)
    col_id = db.add_collection(
        "static-only-tagged",
        json.dumps([{"field": "photo_ids", "value": [p1]}]),
    )
    db.conn.close()

    runner = FakeRunner()
    job = _make_job()

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }

    classifier_init_calls = []

    def _record_classifier_init(*args, **kwargs):
        classifier_init_calls.append((args, kwargs))
        raise AssertionError(
            "Classifier should NOT be initialized when no photos remain "
            "after subject-skip filtering."
        )

    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )

    with patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch(
            "classify_job._load_labels", return_value=(["Northern Cardinal"], False, []),
         ), \
         patch("classify_job.Classifier", side_effect=_record_classifier_init):
        result = run_classify_job(job, runner, db_path, ws, params)

    assert classifier_init_calls == [], (
        "Classifier was initialized despite zero photos to process. "
        "The early-return short-circuit was not taken."
    )
    assert result["total"] == 0
    assert result["predictions_stored"] == 0


def test_classify_job_zero_photos_skips_model_resolution(tmp_path):
    """Regression: when the subject-skip filter empties the photo set, the
    job must short-circuit BEFORE model resolution. Otherwise a user with no
    classifier downloaded would get a misleading 'No model available' error
    for a job that has zero work to do."""
    from unittest.mock import patch

    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    db.add_workspace_folder(ws, folder_id)
    p1 = db.add_photo(
        folder_id, "p1.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
    )
    # Tag with a genre keyword so the subject-skip gate filters it out.
    scene_kid = db.add_keyword("Landscape", kw_type="genre")
    db.tag_photo(p1, scene_kid)
    col_id = db.add_collection(
        "static-only-tagged",
        json.dumps([{"field": "photo_ids", "value": [p1]}]),
    )
    db.conn.close()

    runner = FakeRunner()
    job = _make_job()

    params = ClassifyParams(
        collection_id=col_id,
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name="TestModel",
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=False,
    )

    # Simulate the "no model downloaded" environment: get_active_model
    # returns None (which would normally raise RuntimeError downstream).
    # The short-circuit must execute first and avoid touching the model.
    with patch("classify_job.get_active_model", return_value=None), \
         patch("classify_job.get_models", return_value=[]):
        result = run_classify_job(job, runner, db_path, ws, params)

    assert result["total"] == 0
    assert result["predictions_stored"] == 0


def test_run_classifier_retries_on_database_is_locked(tmp_path):
    """Per-detection prediction commit must retry transient 'database is locked'.

    Concurrent pipelines on the same SQLite file (observed in production: a
    second pipeline failed at classify after ~5h with 'Fatal: database is
    locked') exceed the 30s busy_timeout under sustained writer contention.
    Without retry the whole stage aborts and the run is lost.
    """
    import sqlite3

    import classify_job
    from db import Database
    from tests.test_scanner import _FlakyConn

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(
        folder_id, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
          "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    locked = sqlite3.OperationalError("database is locked")
    db.conn = _FlakyConn(db.conn, fail_on_calls={1: locked, 2: locked})

    classify_job._run_classifier_on_detection(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels=["Robin"],
        labels_fingerprint="abc123",
        classify_fn=lambda: [{"species": "Robin", "confidence": 0.9}],
    )

    n = db.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()["n"]
    assert n == 1, "prediction must be persisted after transient lock retries"


# ── Cancellation and weights-degrade regressions ────────────────────────────


def test_detect_subjects_stops_when_cancelled(tmp_path):
    """A cancelled job exits the detection loop without processing photos."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10},
        {"id": 2, "filename": "b.jpg", "folder_id": 10},
    ]
    mock_db = MagicMock()
    # Everything cached → no weights download attempt before the loop.
    mock_db.get_detector_run_photo_ids.return_value = {1, 2}

    detect = MagicMock()
    with patch("classify_job.detect_animals", detect), \
         patch("classify_job.get_primary_detection", MagicMock()):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={10: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detection_map == {}
    assert detected == 0
    detect.assert_not_called()


def test_detect_subjects_skips_weight_download_when_cancelled(tmp_path):
    """A cancel landing during classifier-init must skip the ~300 MB
    MegaDetector weights download. The per-photo cancel check in the
    detection loop runs too late — hf_hub_download can't be interrupted
    once started, so the gate has to live before the download call."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10},
        {"id": 2, "filename": "b.jpg", "folder_id": 10},
    ]
    mock_db = MagicMock()
    # Nothing cached → needs_fresh_detection is True, the un-fixed code
    # would call ensure_megadetector_weights before noticing the cancel.
    mock_db.get_detector_run_photo_ids.return_value = set()

    detect = MagicMock()
    weights = MagicMock()
    with patch("classify_job.detect_animals", detect), \
         patch("classify_job.get_primary_detection", MagicMock()), \
         patch("detector.ensure_megadetector_weights", weights):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={10: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detection_map == {}
    assert detected == 0
    weights.assert_not_called()
    detect.assert_not_called()


def test_detect_subjects_reclassify_preserves_unprocessed_photos_on_cancel(tmp_path, monkeypatch):
    """For reclassify runs, cancellation mid-detection must NOT have wiped
    the detections of photos that hadn't been re-detected yet. Doing the
    clear upfront for the whole scope (the old behavior) stranded the
    unprocessed tail with empty state — worse than before the run started.
    The fix clears per-photo immediately before each photo is re-detected,
    so cancelled photos keep their cache.

    The cascaded predictions clear lives in ``_classify_photos`` instead of
    here, so this test verifies both the touched and untouched photos'
    predictions survive ``_detect_subjects`` — the classify loop is what
    rebuilds them.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _detect_subjects
    from db import Database

    # Hermetic global config (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid_first = db.add_photo(
        folder_id, "first.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    pid_second = db.add_photo(
        folder_id, "second.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0,
    )
    det_first = db.save_detections(pid_first, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_second = db.save_detections(pid_second, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_first, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")
    db.add_prediction(det_second, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    class FlipRunner(FakeRunner):
        """Cancel flips on right after the first photo has been processed."""

        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # First call: top of iteration 0 → not cancelled (process first).
            # Subsequent calls (top of iteration 1+) → cancelled (skip rest).
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    photos = [
        {"id": pid_first, "filename": "first.jpg", "folder_id": folder_id},
        {"id": pid_second, "filename": "second.jpg", "folder_id": folder_id},
    ]

    # detect_animals returns one detection for the first (only) photo
    # that completes; the second photo is never reached. compute_sharpness
    # is patched to a real callable (rather than ``None``) so that a stray
    # invocation would surface as an assertion failure on the real
    # behavior, not a ``TypeError`` from calling ``None``.
    with patch("classify_job.detect_animals",
               return_value=[{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                              "confidence": 0.8, "category": "animal"}]), \
         patch("classify_job.get_primary_detection",
               return_value={"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                             "confidence": 0.8}), \
         patch("classify_job.compute_sharpness", return_value=0.0):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={folder_id: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=True,
            db=db,
        )

    # The second photo's cached prediction and detection must survive
    # intact: it was never reached, so the per-photo clear never ran.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_second = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_second,),
    ).fetchone()["n"]
    dets_second = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM detections WHERE id = ?",
        (det_second,),
    ).fetchone()["n"]
    assert preds_second == 1, (
        "Mid-detection cancel on a reclassify run wiped the cache of a "
        "photo that was never reached — the upfront global purge leaked "
        "back in."
    )
    assert dets_second == 1, (
        "Mid-detection cancel on a reclassify run cleared detections of "
        "a photo that was never reached."
    )


def test_detect_subjects_reclassify_tracks_clear_when_detect_returns_none(tmp_path, monkeypatch):
    """For reclassify runs, ``_detect_subjects`` must record a photo for
    rebuild as soon as its prior detections have been cleared — not only
    when ``_detect_batch`` reports it processed.

    Regression for Codex P2: if ``detect_animals`` returns ``None`` (image
    decode failure / ONNX hiccup), ``_detect_batch`` deliberately omits
    the id from ``processed_ids`` so a future non-reclassify pass retries
    it. But in reclassify mode the per-photo ``clear_detections`` has
    already cascaded away the old detections + predictions; if the user
    then cancels before the next iteration, the run_classify_job cancel
    path sees an empty processed set and skips classification, leaving
    the photo with no detections and no predictions.

    The fix marks the photo for rebuild immediately after the clear, so
    the full-image fallback in ``_classify_photos`` rebuilds it.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _detect_subjects
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")

    runner = FakeRunner()
    job = _make_job()
    photos = [{"id": pid, "filename": "a.jpg", "folder_id": folder_id}]

    # detect_animals returns None → _detect_batch hits its early-continue
    # and the id never lands in batch_processed. The clear above still ran.
    with patch("classify_job.detect_animals", return_value=None), \
         patch("classify_job.get_primary_detection", return_value=None), \
         patch("classify_job.compute_sharpness", return_value=0.0):
        _detect_subjects(
            photos=photos,
            folders={folder_id: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=True,
            db=db,
        )

    processed = job.get("_detect_processed_ids")
    assert processed and pid in processed, (
        "Reclassify with detect_animals returning None must still mark "
        "the photo for rebuild — its prior detections were cleared and "
        "the classify path needs to know to replace them. Without this, "
        "a post-detect cancel strands the photo with no detections and "
        "no predictions."
    )


def test_classify_photos_stops_when_cancelled(tmp_path):
    """A cancelled job exits the classification loop without inference."""
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()
    clf = MagicMock()

    with patch("classify_job.load_image", MagicMock()):
        raw_results, failed, skipped = _classify_photos(
            photos=[{"id": 1, "filename": "a.jpg", "folder_id": 10,
                     "timestamp": None}],
            folders={10: str(tmp_path)},
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="test-model",
            runner=runner,
            job=job,
            db=MagicMock(),
        )

    assert raw_results == []
    assert failed == 0
    clf.classify_batch.assert_not_called()
    clf.classify_batch_with_embedding.assert_not_called()


def test_classify_photos_drops_pending_batch_on_mid_loop_cancel(tmp_path):
    """A cancel that lands after some photos queued into ``batch`` but
    before it reaches ``_BATCH_SIZE`` must drop the pending batch — not
    fall through to the post-loop flush. Otherwise the job runs classifier
    inference and writes classifier_runs rows for photos the user just
    cancelled."""
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    # Flip cancelled to True after the first photo has been processed
    # (queued into batch, but batch < _BATCH_SIZE so not yet flushed).
    class FlipRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # First call: start of iteration 0 — let it through.
            # Second call: start of iteration 1 — flip to cancelled.
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    clf = MagicMock()

    # Two photos, each with a synthetic full-image detection. _BATCH_SIZE
    # is well over 2, so neither flushes mid-loop — the only flush path is
    # the post-loop one, which must be skipped on cancel.
    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}

    # Make _prepare_image succeed without touching disk.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, failed, skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="test-model",
            runner=runner,
            job=job,
            db=mock_db,
        )

    # The post-loop flush must be gated on the cancel — classifier inference
    # never runs, and no classifier_runs rows are written for the pending
    # batch contents.
    clf.classify_batch.assert_not_called()
    clf.classify_batch_with_embedding.assert_not_called()
    mock_db.record_classifier_run.assert_not_called()
    assert raw_results == []


def test_weights_download_failure_degrades_to_full_image(tmp_path):
    """A failed MegaDetector weights download (e.g. network down) must
    degrade to full-image classification like any other detection failure,
    not propagate and fail the job — on reclassify runs the purge has
    already happened by then."""
    from unittest.mock import MagicMock, patch

    from classify_job import _detect_subjects

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 1, "filename": "a.jpg", "folder_id": 10}]
    mock_db = MagicMock()
    mock_db.get_detector_run_photo_ids.return_value = set()  # needs download

    with patch("classify_job.detect_animals", MagicMock()), \
         patch("classify_job.get_primary_detection", MagicMock()), \
         patch("detector.ensure_megadetector_weights",
               side_effect=RuntimeError("network down")):
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders={10: str(tmp_path)},
            runner=runner,
            job=job,
            reclassify=False,
            db=mock_db,
        )

    assert detection_map == {}
    assert detected == 0
    assert any("Detection unavailable" in e for e in job["errors"])


def test_classify_photos_reclassify_clears_predictions_per_photo(tmp_path):
    """For reclassify runs, _classify_photos must clear each photo's old
    predictions immediately before classifying it. The clear sits inside
    the loop so a mid-classify cancel leaves the unprocessed tail's old
    predictions intact, and a detection-setup failure that skips the
    detect loop entirely still has its stale predictions replaced by the
    fallback full-image classifier rather than coexisting with it.

    Verified at the DB API boundary: clear_predictions is called once per
    photo with the right (model, photo_id, fingerprint) triple.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
        ([{"species": "Robin", "score": 0.88, "taxonomy": None}], None),
    ]
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
            labels_fingerprint="fp-x",
            reclassify=True,
        )

    clear_calls = mock_db.clear_predictions.call_args_list
    assert len(clear_calls) == 2, (
        f"clear_predictions must be called once per photo for reclassify; "
        f"got {len(clear_calls)} calls"
    )
    photo_ids_cleared = {
        call.kwargs["collection_photo_ids"][0] for call in clear_calls
    }
    assert photo_ids_cleared == {1, 2}, (
        f"clear_predictions must target each photo individually; "
        f"got {photo_ids_cleared}"
    )
    for call in clear_calls:
        assert call.kwargs["model"] == "BioCLIP", (
            f"clear_predictions must scope to the model being run; got "
            f"{call.kwargs.get('model')!r}"
        )
        # No fingerprint filter: stale predictions tagged with prior
        # fingerprints (e.g. before a workspace label-set change) must
        # also be wiped in the fallback path, otherwise the
        # latest-fingerprint-per-detection filter in get_predictions
        # would surface them alongside the new fallback rows. The
        # normal reclassify path's per-photo clear_detections cascade
        # has already wiped predictions across all fingerprints, so an
        # unfiltered clear here is a no-op repeat in that path.
        assert call.kwargs.get("labels_fingerprint") is None, (
            "clear_predictions must NOT scope to the current "
            "fingerprint: that would leave stale predictions under "
            f"prior fingerprints visible; got {call.kwargs.get('labels_fingerprint')!r}"
        )


def test_classify_photos_no_reclassify_skips_predictions_clear(tmp_path):
    """The per-photo predictions clear must NOT fire for non-reclassify
    runs — otherwise every cached classification would be invalidated on
    every normal pass."""
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    job = _make_job()

    photos = [{"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None}]
    folders = {10: str(tmp_path)}

    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
    ]
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
            labels_fingerprint="fp-x",
            reclassify=False,
        )

    mock_db.clear_predictions.assert_not_called()


def test_classify_photos_reclassify_preserves_unclassified_tail_on_cancel(tmp_path, monkeypatch):
    """For reclassify runs, cancelling mid-classify must leave the
    unclassified tail's old predictions intact.

    Regression for Codex P1 review on vireo/classify_job.py line 933.
    Before the fix, the per-photo predictions clear ran in the detection
    loop, so every photo's old predictions were wiped before any
    classification happened — a cancel that landed mid-classify stranded
    the tail with cleared predictions and no replacement. The fix moves
    the clear into the classify loop so unreached photos never have it
    fire.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _classify_photos
    from db import Database

    # Hermetic global config (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid_first = db.add_photo(
        folder_id, "first.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    pid_second = db.add_photo(
        folder_id, "second.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0,
    )
    det_first = db.save_detections(pid_first, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_second = db.save_detections(pid_second, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_first, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")
    db.add_prediction(det_second, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    class FlipRunner(FakeRunner):
        """Cancel flips on right after the first photo enters the loop."""

        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # First call: top of iteration 0 → not cancelled (process first).
            # Subsequent calls: top of iteration 1+ → cancelled.
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    photos = [
        {"id": pid_first, "filename": "first.jpg", "folder_id": folder_id,
         "timestamp": None},
        {"id": pid_second, "filename": "second.jpg", "folder_id": folder_id,
         "timestamp": None},
    ]
    folders = {folder_id: str(tmp_path)}

    detection_map = {
        pid_first: [{"id": det_first}],
        pid_second: [{"id": det_second}],
    }

    from unittest.mock import MagicMock
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
    ]

    # _prepare_image returns a dummy so we never touch real image files;
    # the test is about the clear-call boundary, not classification accuracy.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="legacy",
            reclassify=True,
        )

    # The second photo's cached prediction must survive intact: its
    # iteration never started, so the per-photo clear never ran.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    preds_second = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_second,),
    ).fetchone()["n"]
    assert preds_second == 1, (
        "Mid-classify cancel on a reclassify run wiped the cache of a "
        "photo whose classify-loop iteration never started — the clear "
        "leaked out of the loop body."
    )


def test_classify_photos_full_image_fallback_replaces_stale_predictions_on_reclassify(tmp_path, monkeypatch):
    """When MegaDetector setup fails for a reclassify run, ``_classify_photos``
    runs full-image classification. The stale detector-based predictions
    must be cleared so they don't coexist with the fallback model's output.

    Regression for Codex P2 review on vireo/classify_job.py line 666.
    Before the fix, the per-photo clear lived inside ``_detect_subjects``
    so it was bypassed entirely when the try block raised; the fallback
    full-image classifier then wrote new predictions alongside the
    untouched stale rows.
    """
    from unittest.mock import patch

    import config as cfg
    from classify_job import _classify_photos
    from db import Database

    # Hermetic global config (per repo testing conventions).
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Pretend a prior megadetector run left a detection + prediction
    # behind. The reclassify-with-weights-failure path must not let this
    # prediction linger alongside the new full-image one.
    stale_det = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.4, "h": 0.4},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(stale_det, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="legacy")

    runner = FakeRunner()
    job = _make_job()

    from unittest.mock import MagicMock
    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.88, "taxonomy": None}], None),
    ]

    # detection_map is empty (the simulated setup failure produced
    # nothing) → the function takes the full-image synthetic branch.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=[{"id": pid, "filename": "a.jpg", "folder_id": folder_id,
                     "timestamp": None}],
            folders={folder_id: str(tmp_path)},
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="legacy",
            reclassify=True,
        )

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    # Stale BioCLIP prediction on the old megadetector detection must
    # be gone. The fallback classifier wrote a new prediction on the
    # synthetic full-image detection.
    stale_preds = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions "
        "WHERE detection_id = ? AND classifier_model = ?",
        (stale_det, "BioCLIP"),
    ).fetchone()["n"]
    assert stale_preds == 0, (
        "Stale detector-based predictions must be cleared by the classify "
        "loop's per-photo purge when reclassify falls back to full-image "
        "(detection setup failure). Found stale rows still attached to "
        f"the old megadetector detection ({stale_det})."
    )
    # Sanity: the fallback path actually produced a result so the test
    # didn't pass vacuously.
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Sparrow"


def test_classify_photos_full_image_fallback_clears_stale_predictions_across_fingerprints(
    tmp_path, monkeypatch,
):
    """Stale detector predictions tagged with a PRIOR ``labels_fingerprint``
    must also be wiped when the reclassify run falls back to full-image
    classification under a NEW fingerprint.

    Regression for Codex P2 review on vireo/classify_job.py line 1006.
    Before the fix, the per-photo purge filtered by the current fingerprint,
    so a workspace label-set change between runs left stale detector-based
    predictions under the old fingerprint untouched. ``get_predictions``'
    latest-fingerprint-per-(detection, classifier_model) filter then surfaces
    those stale rows for the old detection alongside the new fallback rows
    for the synthetic full-image detection, leaving users with mixed-model
    output for the same photo.
    """
    from unittest.mock import MagicMock, patch

    import config as cfg
    from classify_job import _classify_photos
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Prior run: megadetector detection + prediction under the OLD
    # workspace label fingerprint.
    stale_det = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.4, "h": 0.4},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(stale_det, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="fp-old")
    db.record_classifier_run(stale_det, "BioCLIP", "fp-old",
                             prediction_count=1)

    runner = FakeRunner()
    job = _make_job()

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.88, "taxonomy": None}], None),
    ]

    # Empty detection_map (setup failure) + new fingerprint = the bug case.
    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=[{"id": pid, "filename": "a.jpg", "folder_id": folder_id,
                     "timestamp": None}],
            folders={folder_id: str(tmp_path)},
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="fp-new",
            reclassify=True,
        )

    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    stale_preds = db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions "
        "WHERE detection_id = ? AND classifier_model = ?",
        (stale_det, "BioCLIP"),
    ).fetchone()["n"]
    assert stale_preds == 0, (
        "Stale fp-old prediction on the old megadetector detection must be "
        "cleared even though the current run uses fp-new — otherwise "
        "get_predictions' latest-fp-per-detection filter still surfaces it "
        "alongside the new full-image fallback prediction."
    )
    # The classifier_runs row under the old fingerprint must also be gone
    # so the next non-reclassify pass actually re-runs inference for that
    # detection if it ever reappears in detection_map.
    run_keys = db2.get_classifier_run_keys(stale_det)
    assert ("BioCLIP", "fp-old") not in run_keys, (
        "Stale fp-old classifier_runs row must be cleared alongside the "
        "stale prediction."
    )
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Sparrow"


def test_classify_photos_reclassify_flushes_pending_batch_on_cancel(tmp_path):
    """For reclassify runs, a mid-loop cancel must still flush the pending
    batch — its queued photos already had their old predictions cleared by
    the per-photo purge at the top of the iteration, so dropping the batch
    strands them with no predictions until a manual rerun.

    Regression for Codex P1 review on vireo/classify_job.py line 1178.
    Before the fix, the post-loop ``if batch and not cancelled`` gate
    dropped the pending batch on every cancel, including reclassify runs
    where each batched photo had a destructive ``clear_predictions`` fire
    just before it was queued.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder(str(tmp_path), name="p")

    pid_a = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    pid_b = db.add_photo(
        folder_id, "b.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0,
    )
    det_a = db.save_detections(pid_a, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_b = db.save_detections(pid_b, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    # Each photo has a prior prediction; the reclassify clear will wipe
    # them per-photo, and the test asserts the new run replaced them
    # rather than leaving them empty after the cancel.
    db.add_prediction(det_a, species="OldA", confidence=0.5,
                      model="BioCLIP", labels_fingerprint="legacy")
    db.add_prediction(det_b, species="OldB", confidence=0.5,
                      model="BioCLIP", labels_fingerprint="legacy")

    class FlipRunner(FakeRunner):
        """Cancel flips on after the second photo enters the loop."""

        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            # Calls 1 + 2: iterations 0 and 1 — let both through so both
            # land in ``batch``. Calls 3+: cancelled, breaking the loop
            # without flushing mid-iteration (batch < _BATCH_SIZE).
            return self._calls >= 3

    runner = FlipRunner()
    job = _make_job()
    photos = [
        {"id": pid_a, "filename": "a.jpg", "folder_id": folder_id,
         "timestamp": None},
        {"id": pid_b, "filename": "b.jpg", "folder_id": folder_id,
         "timestamp": None},
    ]
    folders = {folder_id: str(tmp_path)}

    detection_map = {
        pid_a: [{"id": det_a}],
        pid_b: [{"id": det_b}],
    }

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "NewA", "score": 0.92, "taxonomy": None}], None),
        ([{"species": "NewB", "score": 0.91, "taxonomy": None}], None),
    ]

    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=db,
            labels_fingerprint="legacy",
            reclassify=True,
        )

    # The pending batch must have flushed: both photos that had their old
    # predictions cleared got new predictions written, so raw_results
    # contains them both.
    assert len(raw_results) == 2, (
        "Reclassify mid-loop cancel must flush the pending batch — its "
        "queued photos already had their old predictions cleared and "
        "would otherwise be stranded empty. "
        f"Got {len(raw_results)} results."
    )
    predictions = {r["photo"]["id"]: r["prediction"] for r in raw_results}
    assert predictions == {pid_a: "NewA", pid_b: "NewB"}


def test_classify_photos_no_reclassify_drops_pending_batch_on_cancel(tmp_path):
    """The non-reclassify path must still drop the pending batch on cancel.

    Without a reclassify, those photos' cached predictions are untouched,
    so honoring the cancel signal here just skips wasted inference (and
    avoids writing classifier_runs rows for photos the user just said
    they're done with).
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    class FlipRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self._calls = 0

        def is_cancelled(self, job_id):
            self._calls += 1
            return self._calls >= 2

    runner = FlipRunner()
    job = _make_job()
    clf = MagicMock()

    mock_db = MagicMock()
    mock_db.get_detections.return_value = []
    mock_db.save_detections.return_value = [101]
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}

    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map={},
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="test-model",
            runner=runner,
            job=job,
            db=mock_db,
            reclassify=False,
        )

    clf.classify_batch.assert_not_called()
    clf.classify_batch_with_embedding.assert_not_called()
    mock_db.record_classifier_run.assert_not_called()
    assert raw_results == []


def test_classify_photos_finish_cleared_only_ignores_cancel(tmp_path):
    """When ``finish_cleared_only=True`` (set by ``run_classify_job`` after a
    post-detect cancel landed on a reclassify run), the classify loop must
    process every photo despite the cancel signal. The photos in this
    subset already had their old detections + cascaded predictions wiped
    during detection; bailing now would strand them empty.

    Also asserts the per-photo ``clear_predictions`` is NOT re-run — the
    cascade in ``_detect_subjects`` already did that, and re-issuing the
    DELETE would just waste a transaction.
    """
    from unittest.mock import MagicMock, patch

    from classify_job import _classify_photos

    runner = FakeRunner()
    runner.cancelled = True  # post-detect cancel signal already set
    job = _make_job()

    clf = MagicMock()
    clf.classify_batch_with_embedding.return_value = [
        ([{"species": "Sparrow", "score": 0.92, "taxonomy": None}], None),
    ]

    mock_db = MagicMock()
    mock_db.get_classifier_run_keys.return_value = set()
    mock_db.get_predictions_for_detection.return_value = []

    photos = [{"id": 1, "filename": "a.jpg", "folder_id": 10,
               "timestamp": None}]
    # Pre-populated detection_map: detection already done.
    detection_map = {
        1: [{"id": 999, "box_x": 0, "box_y": 0, "box_w": 1, "h": 1,
             "box_h": 1, "confidence": 0.9, "category": "animal"}],
    }
    folders = {10: str(tmp_path)}

    fake_img = MagicMock()
    with patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        raw_results, _, _ = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="BioCLIP",
            runner=runner,
            job=job,
            db=mock_db,
            labels_fingerprint="fp-x",
            reclassify=True,
            finish_cleared_only=True,
        )

    # Classification happened despite cancel = True.
    assert len(raw_results) == 1
    assert raw_results[0]["prediction"] == "Sparrow"
    # Per-photo clear must not have re-fired — the detection-loop cascade
    # already handled it.
    mock_db.clear_predictions.assert_not_called()


def test_run_classify_job_reclassify_cancel_after_detect_classifies_processed(tmp_path):
    """End-to-end: a reclassify run cancelled after detection has processed
    some photos must still classify that processed subset. Without this,
    the photos with completed detection would be stranded with new
    detections but no predictions until a manual rerun.

    Regression for Codex P1 review on vireo/classify_job.py line 1824
    (and the related Iu_1R / IubrR / IupmJ findings that flag the same
    half-state for the reclassify path).
    """
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    # The runner reports "cancelled" only after _detect_subjects returns,
    # mirroring a real user cancel landing during the detection phase but
    # only being observed when run_classify_job rechecks afterward.
    class PostDetectCancelRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.flipped = False

        def is_cancelled(self, job_id):
            return self.flipped

    runner = PostDetectCancelRunner()
    job = _make_job()

    # Mock DB: two photos in the collection, both with prior detections
    # and predictions. The "processed" one will be rebuilt via the
    # cancel-recovery path; the "untouched" one was never reached by
    # detection so it has no entry in detection_map.
    mock_db_instance = MagicMock()
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "processed.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
        {"id": 2, "filename": "untouched.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T11:00:00"},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )
    # The classify loop uses these to gate-check; empty returns force
    # full re-classification (which is what reclassify means anyway).
    mock_db_instance.get_classifier_run_keys.return_value = set()
    mock_db_instance.get_predictions_for_detection.return_value = []
    mock_db_instance.get_detections.return_value = []

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "NewProcessed", "score": 0.95, "taxonomy": None}]
    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)
    mock_clf.classify_batch_with_embedding.return_value = [
        (fake_preds, fake_embedding)
    ]

    # Stub _detect_subjects: only photo 1 ("processed") completed
    # detection. Flip cancel on as it returns.
    detect_called = {"n": 0}

    def fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        detect_called["n"] += 1
        assert reclassify is True
        runner.flipped = True
        # detection_map only includes the processed photo; the untouched
        # one was never reached (mid-detection cancel).
        return ({1: [{"id": 101, "box_x": 0, "box_y": 0,
                      "box_w": 0.4, "box_h": 0.4, "confidence": 0.85,
                      "category": "animal",
                      "detector_model": "megadetector-v6"}]}, 1)

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=True,
    )

    fake_img = MagicMock()
    with patch("classify_job.Database", return_value=mock_db_instance), \
         patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job._load_labels",
               return_value=(["NewProcessed"], False, [])), \
         patch("classify_job.Classifier", return_value=mock_clf), \
         patch("classify_job._detect_subjects",
               side_effect=fake_detect_subjects), \
         patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        result = run_classify_job(job, runner, str(tmp_path / "test.db"),
                                  1, params)

    assert detect_called["n"] == 1, "_detect_subjects must have been called"
    # The processed photo's classification was rebuilt despite the cancel —
    # add_prediction was called for it via finalization.
    assert mock_db_instance.add_prediction.call_count >= 1, (
        "Post-detect cancel on reclassify with non-empty detection_map "
        "must classify the processed subset and store predictions. "
        "add_prediction was never called — the recovery path bailed "
        "instead of classifying."
    )
    # All add_prediction calls must be for the processed photo (id 1).
    # The untouched photo (id 2) is not in detection_map and must not
    # be touched by the recovery path.
    for call in mock_db_instance.add_prediction.call_args_list:
        # add_prediction(detection_id, species=..., ...) — detection_id is
        # the first positional arg. Photo 1's stub detection id is 101.
        det_id = call.args[0] if call.args else call.kwargs.get("detection_id")
        assert det_id == 101, (
            f"Recovery path wrote a prediction for unexpected detection "
            f"{det_id}; only the processed subset should be classified."
        )
    assert result["detected"] == 1


def test_run_classify_job_finish_cleared_only_suspends_resource_cancel(tmp_path):
    """Regression: the finish_cleared_only preservation pass must run to
    completion even when the job-bound resource cancel probe is already
    True. The classifier's inference lease consults that probe; without
    suspending the binding for this pass every _flush_batch would raise
    ResourceWaitCancelled and the processed subset would be left with
    fresh detections but zero replacement predictions."""
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    class PostDetectCancelRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.flipped = False

        def is_cancelled(self, job_id):
            return self.flipped

    runner = PostDetectCancelRunner()
    job = _make_job()

    mock_db_instance = MagicMock()
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "processed.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )
    mock_db_instance.get_classifier_run_keys.return_value = set()
    mock_db_instance.get_predictions_for_detection.return_value = []
    mock_db_instance.get_detections.return_value = []

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Restored", "score": 0.95, "taxonomy": None}]

    # This classifier stand-in consults the bound resource cancel probe
    # exactly the way acquire_inference_resources does. If the binding is
    # still active during finish_cleared_only, the probe is True and this
    # raises — matching the real bug where the CPU inference lease aborts
    # every flush and the recovery pass writes no predictions.
    class ProbeCheckingClassifier:
        def __init__(self):
            self.calls = 0

        def _check(self):
            from resource_ledger import (
                ResourceWaitCancelled,
                resolve_resource_cancel_check,
            )
            probe = resolve_resource_cancel_check()
            if probe is not None and probe():
                raise ResourceWaitCancelled(
                    "bound cancel probe fired inside classifier"
                )

        def classify_batch_with_embedding(self, images, threshold=0):
            self._check()
            self.calls += 1
            return [(fake_preds, fake_embedding) for _ in images]

        def classify_with_embedding(self, image, threshold=0):
            self._check()
            self.calls += 1
            return (fake_preds, fake_embedding)

    mock_clf = ProbeCheckingClassifier()

    def fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        assert reclassify is True
        runner.flipped = True
        return ({1: [{"id": 101, "box_x": 0, "box_y": 0,
                      "box_w": 0.4, "box_h": 0.4, "confidence": 0.85,
                      "category": "animal",
                      "detector_model": "megadetector-v6"}]}, 1)

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=True,
    )

    fake_img = MagicMock()

    # Bind a job-level probe that is already True by the time classification
    # would fire, matching JobRunner._run_job for a cancelled job.
    from resource_ledger import bind_resource_cancel_check
    with (
        bind_resource_cancel_check(lambda: True),
        patch("classify_job.Database", return_value=mock_db_instance),
        patch("classify_job.get_active_model", return_value=fake_model),
        patch("classify_job.get_models", return_value=[fake_model]),
        patch("classify_job._load_taxonomy", return_value=None),
        patch("classify_job._load_labels",
              return_value=(["Restored"], False, [])),
        patch("classify_job.Classifier", return_value=mock_clf),
        patch("classify_job._detect_subjects",
              side_effect=fake_detect_subjects),
        patch("classify_job._prepare_image",
              return_value=(fake_img, str(tmp_path), "p")),
    ):
        run_classify_job(job, runner, str(tmp_path / "test.db"), 1, params)

    assert mock_clf.calls >= 1, (
        "Preservation pass must reach the classifier; the bound cancel "
        "probe should be suspended for finish_cleared_only."
    )
    assert mock_db_instance.add_prediction.call_count >= 1, (
        "Preservation pass must persist a replacement prediction so the "
        "processed subset is not stranded empty."
    )


def test_classify_photos_reclassify_cancel_flushes_pending_batch(tmp_path):
    """Regression: when a reclassify run is cancelled mid-classify, the
    tail preservation flush must still write predictions for photos
    whose old predictions the loop already cleared. CPU inference
    consults the bound resource cancel probe (True on this path), so
    without suspending the binding just for this tail flush every
    ``_flush_batch`` call would raise ``ResourceWaitCancelled``, the
    fallback per-image path would also raise, and the queued photos
    would be stranded with no replacement predictions."""
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import _classify_photos

    class MidClassifyCancelRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.checks = 0

        def is_cancelled(self, job_id):
            # Cancel fires on the second loop iteration, after photo 1
            # was queued into ``batch`` on iteration 0.
            self.checks += 1
            return self.checks >= 2

    runner = MidClassifyCancelRunner()
    job = _make_job()

    fake_db = MagicMock()
    fake_db.get_classifier_run_keys.return_value = set()
    fake_db.get_predictions_for_detection.return_value = []

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Restored", "score": 0.95, "taxonomy": None}]

    # This classifier stand-in consults the bound resource cancel probe
    # exactly the way ``acquire_inference_resources`` does. If the
    # binding is still active during the reclassify tail flush the
    # probe is True and this raises — matching the real bug where the
    # CPU inference lease aborts the flush and no replacement
    # predictions are written for the queued photos.
    class ProbeCheckingClassifier:
        def __init__(self):
            self.calls = 0

        def _check(self):
            from resource_ledger import (
                ResourceWaitCancelled,
                resolve_resource_cancel_check,
            )
            probe = resolve_resource_cancel_check()
            if probe is not None and probe():
                raise ResourceWaitCancelled(
                    "bound cancel probe fired inside classifier"
                )

        def classify_batch_with_embedding(self, images, threshold=0):
            self._check()
            self.calls += 1
            return [(fake_preds, fake_embedding) for _ in images]

        def classify_with_embedding(self, image, threshold=0):
            self._check()
            self.calls += 1
            return (fake_preds, fake_embedding)

    clf = ProbeCheckingClassifier()

    photos = [
        {"id": 1, "filename": "a.jpg", "folder_id": 10, "timestamp": None},
        {"id": 2, "filename": "b.jpg", "folder_id": 10, "timestamp": None},
    ]
    folders = {10: str(tmp_path)}
    detection_map = {
        1: [{"id": 101, "box_x": 0.0, "box_y": 0.0,
             "box_w": 1.0, "box_h": 1.0, "confidence": 0.9,
             "category": "animal",
             "detector_model": "megadetector-v6"}],
        2: [{"id": 102, "box_x": 0.0, "box_y": 0.0,
             "box_w": 1.0, "box_h": 1.0, "confidence": 0.9,
             "category": "animal",
             "detector_model": "megadetector-v6"}],
    }
    fake_img = MagicMock()

    from resource_ledger import bind_resource_cancel_check
    with (
        bind_resource_cancel_check(lambda: True),
        patch("classify_job._prepare_image",
              return_value=(fake_img, str(tmp_path), "p")),
    ):
        raw_results, failed, _skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="TestModel",
            runner=runner,
            job=job,
            db=fake_db,
            top_k=1,
            vireo_dir=str(tmp_path),
            labels_fingerprint="fp",
            reclassify=True,
            finish_cleared_only=False,
        )

    assert clf.calls >= 1, (
        "Reclassify tail flush must reach the classifier; the bound "
        "cancel probe should be suspended for this preservation pass."
    )
    assert failed == 0, (
        f"Preservation flush must not treat cancellation as failure "
        f"(got {failed} failures)"
    )
    assert len(raw_results) >= 1, (
        "Preservation flush must persist a replacement prediction so "
        "the queued reclassify photo is not stranded empty."
    )


def test_classify_photos_reclassify_cancel_mid_batch_flush_preserves_cleared(tmp_path):
    """Regression: when Stop arrives while a full 16-image reclassify
    batch is IN FLIGHT through ``_flush_batch`` (not just the final
    tail), those photos have already had their old predictions cleared
    by the per-photo ``clear_predictions`` above. The prior fix
    suspended the bound resource cancel probe only for the tail
    ``_flush_batch`` at line 1735, so a mid-loop flush at lines
    1568/1698 that hit ``ResourceWaitCancelled`` would strand the
    entire 16-photo batch empty. Suspend the probe on every reclassify
    flush — mid-loop AND tail — via the ``_flush_preserving_cleared``
    helper.
    """
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import _BATCH_SIZE, _classify_photos

    class NeverCancelRunner(FakeRunner):
        def is_cancelled(self, job_id):
            return False

    runner = NeverCancelRunner()
    job = _make_job()

    fake_db = MagicMock()
    fake_db.get_classifier_run_keys.return_value = set()
    fake_db.get_predictions_for_detection.return_value = []

    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "Restored", "score": 0.9, "taxonomy": None}]

    class ProbeCheckingClassifier:
        def __init__(self):
            self.calls = 0

        def _check(self):
            from resource_ledger import (
                ResourceWaitCancelled,
                resolve_resource_cancel_check,
            )
            probe = resolve_resource_cancel_check()
            if probe is not None and probe():
                raise ResourceWaitCancelled(
                    "bound cancel probe fired inside classifier",
                )

        def classify_batch_with_embedding(self, images, threshold=0):
            self._check()
            self.calls += 1
            return [(fake_preds, fake_embedding) for _ in images]

        def classify_with_embedding(self, image, threshold=0):
            self._check()
            self.calls += 1
            return (fake_preds, fake_embedding)

    clf = ProbeCheckingClassifier()

    # Enough photos to trigger at least one mid-loop flush at _BATCH_SIZE.
    # runner.is_cancelled never fires so the loop drains naturally; the
    # bound resource cancel probe simulates a cancel that arrived mid-run
    # (e.g. between the runner-poll and the flush) and stays true
    # throughout — the exact condition the reclassify mid-loop flush
    # must survive.
    n = _BATCH_SIZE + 3
    photos = [
        {"id": pid, "filename": f"p{pid}.jpg", "folder_id": 10,
         "timestamp": None}
        for pid in range(1, n + 1)
    ]
    folders = {10: str(tmp_path)}
    detection_map = {
        pid: [{"id": 100 + pid, "box_x": 0.0, "box_y": 0.0,
               "box_w": 1.0, "box_h": 1.0, "confidence": 0.9,
               "category": "animal",
               "detector_model": "megadetector-v6"}]
        for pid in range(1, n + 1)
    }
    fake_img = MagicMock()

    from resource_ledger import bind_resource_cancel_check
    with (
        bind_resource_cancel_check(lambda: True),
        patch("classify_job._prepare_image",
              return_value=(fake_img, str(tmp_path), "p")),
    ):
        raw_results, failed, _skipped = _classify_photos(
            photos=photos,
            folders=folders,
            detection_map=detection_map,
            existing_preds=set(),
            clf=clf,
            model_type="bioclip",
            model_name="TestModel",
            runner=runner,
            job=job,
            db=fake_db,
            top_k=1,
            vireo_dir=str(tmp_path),
            labels_fingerprint="fp",
            reclassify=True,
            finish_cleared_only=False,
        )

    assert failed == 0, (
        f"Mid-loop flushes must not treat the bound cancel probe as "
        f"failure on a reclassify run — the cleared photos would be "
        f"stranded empty otherwise. Got {failed} failures."
    )
    # Every photo's detection must have a raw_result — the mid-loop
    # flush must have produced predictions for the first _BATCH_SIZE
    # photos, and the tail flush must have handled the remainder.
    assert len(raw_results) == n, (
        f"Expected {n} raw_results (one per detection); got "
        f"{len(raw_results)}. A stranded mid-loop batch would drop the "
        f"first _BATCH_SIZE={_BATCH_SIZE} entries."
    )
    seen_detection_ids = {r.get("detection_id") for r in raw_results}
    expected_detection_ids = {100 + pid for pid in range(1, n + 1)}
    assert seen_detection_ids == expected_detection_ids, (
        f"Every reclassified detection must appear in raw_results; "
        f"missing {expected_detection_ids - seen_detection_ids}"
    )


def test_run_classify_job_reclassify_cancel_classifies_empty_scene_processed(tmp_path):
    """End-to-end: a reclassify run cancelled after detection must still
    classify photos that were re-detected as empty scenes (no detections
    in detection_map). Their old detections+predictions were already
    cascaded away by the per-photo ``clear_detections`` call in
    ``_detect_subjects``; without a full-image fallback classify pass they
    would be stranded with cleared predictions and no replacement.

    Regression for the Codex P1 review on commit faa47a43ad
    (vireo/classify_job.py line 1861 — "Track re-detected empty photos
    before returning"). The gate uses ``job["_detect_processed_ids"]``
    rather than ``detection_map.keys()`` so empty-scene photos are
    included in the rebuild subset.
    """
    from unittest.mock import MagicMock, patch

    import numpy as np
    from classify_job import ClassifyParams, run_classify_job

    class PostDetectCancelRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.flipped = False

        def is_cancelled(self, job_id):
            return self.flipped

    runner = PostDetectCancelRunner()
    job = _make_job()

    mock_db_instance = MagicMock()
    # Two photos: photo 1 was re-detected as empty (state mutated),
    # photo 2 was never reached.
    mock_db_instance.get_collection_photos.return_value = [
        {"id": 1, "filename": "empty.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T10:00:00"},
        {"id": 2, "filename": "untouched.jpg", "folder_id": 10,
         "timestamp": "2024-01-15T11:00:00"},
    ]
    mock_db_instance.get_folder_tree.return_value = [
        {"id": 10, "path": str(tmp_path), "name": "test"},
    ]
    mock_db_instance.get_existing_prediction_photo_ids.return_value = set()
    mock_db_instance.get_photo_embedding.return_value = None
    mock_db_instance.get_subject_types.return_value = set()
    mock_db_instance.filter_out_subject_tagged.side_effect = (
        lambda pids, _types: list(pids)
    )
    mock_db_instance.get_classifier_run_keys.return_value = set()
    mock_db_instance.get_predictions_for_detection.return_value = []
    # No prior full-image synthetic detection exists for photo 1 — the
    # classifier creates one for the full-image fallback path.
    mock_db_instance.get_detections.return_value = []
    # save_detections + write_detection_batch both return the synthetic
    # detection id for the full-image fallback path.  The classify loop
    # now writes both the detection and the detector_runs row in a single
    # ``write_detection_batch`` transaction to avoid torn state on crash,
    # so the mock has to answer that call too.
    mock_db_instance.save_detections.return_value = [201]
    mock_db_instance.write_detection_batch.return_value = [201]

    fake_model = {
        "id": "test-model",
        "name": "TestModel",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": "/tmp/weights.bin",
        "model_type": "bioclip",
        "downloaded": True,
    }
    fake_embedding = np.ones(512, dtype=np.float32)
    fake_preds = [{"species": "NewEmpty", "score": 0.95, "taxonomy": None}]
    mock_clf = MagicMock()
    mock_clf.classify_with_embedding.return_value = (fake_preds, fake_embedding)
    mock_clf.classify_batch_with_embedding.return_value = [
        (fake_preds, fake_embedding)
    ]

    # Stub _detect_subjects: photo 1 was processed but found empty
    # (recorded a detector_runs row, no detection_map entry). Photo 2 was
    # never reached. The processed set is stashed on the job so
    # run_classify_job's rebuild gate can include the empty-scene photo
    # — using detection_map.keys() alone would miss it.
    detect_called = {"n": 0}

    def fake_detect_subjects(photos, folders, runner, job, reclassify, db):
        detect_called["n"] += 1
        assert reclassify is True
        # Mirror what production _detect_subjects does: stash the processed
        # set on job for run_classify_job to consume on cancel.
        job["_detect_processed_ids"] = {1}
        runner.flipped = True
        return ({}, 0)

    params = ClassifyParams(
        collection_id="col-1",
        labels_file=None,
        labels_files=None,
        model_id=None,
        model_name=None,
        grouping_window=10,
        similarity_threshold=0.85,
        reclassify=True,
    )

    fake_img = MagicMock()
    with patch("classify_job.Database", return_value=mock_db_instance), \
         patch("classify_job.get_active_model", return_value=fake_model), \
         patch("classify_job.get_models", return_value=[fake_model]), \
         patch("classify_job._load_taxonomy", return_value=None), \
         patch("classify_job._load_labels",
               return_value=(["NewEmpty"], False, [])), \
         patch("classify_job.Classifier", return_value=mock_clf), \
         patch("classify_job._detect_subjects",
               side_effect=fake_detect_subjects), \
         patch("classify_job._prepare_image",
               return_value=(fake_img, str(tmp_path), "p")):
        result = run_classify_job(job, runner, str(tmp_path / "test.db"),
                                  1, params)

    assert detect_called["n"] == 1, "_detect_subjects must have been called"
    # The empty-scene photo (id 1) had its predictions cascaded away by
    # _detect_subjects.clear_detections; the recovery path must classify
    # it via the full-image fallback so it doesn't end up empty.
    assert mock_db_instance.add_prediction.call_count >= 1, (
        "Post-detect cancel on reclassify with an empty-scene processed "
        "photo must classify it via the full-image fallback. Without the "
        "_detect_processed_ids tracking, the photo is stranded with "
        "cleared predictions and no replacement."
    )
    # All add_prediction calls must be for the empty-scene photo's
    # synthetic full-image detection (id 201). Photo 2 was never
    # processed and must not be touched.
    for call in mock_db_instance.add_prediction.call_args_list:
        det_id = call.args[0] if call.args else call.kwargs.get("detection_id")
        assert det_id == 201, (
            f"Recovery path wrote a prediction for unexpected detection "
            f"{det_id}; only the processed empty-scene photo's synthetic "
            f"full-image detection (201) should be classified."
        )


# ---------------------------------------------------------------------------
# Early returns must not leave step rows stuck at "pending"
# ---------------------------------------------------------------------------


def _final_step_statuses(runner):
    """Map step_id -> last status reported via update_step."""
    finals = {}
    for step_id, kwargs in runner.steps:
        if "status" in kwargs:
            finals[step_id] = kwargs["status"]
    return finals


def test_empty_collection_finalizes_all_step_rows(tmp_path):
    """The total==0 short-circuit returns before model resolution; the
    load_taxonomy/load_model/detect/classify/finalize rows it skips must
    be flipped to a terminal status, not left pending forever.
    """
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    coll_id = db.add_collection("empty", "[]")

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name=None,
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=False,
    )

    result = run_classify_job(job, runner, db_path, ws, params)
    assert result["total"] == 0

    finals = _final_step_statuses(runner)
    terminal = {"completed", "failed", "cancelled"}
    for step_id in ("load_photos", "load_taxonomy", "load_model",
                    "detect", "classify", "finalize"):
        assert finals.get(step_id) in terminal, (
            f"Step {step_id!r} must reach a terminal status when the job "
            f"short-circuits with no photos, got {finals.get(step_id)!r}"
        )


def test_precancelled_job_finalizes_all_step_rows(tmp_path):
    """The pre-model-resolution cancel gate returns early; the remaining
    rows must be marked cancelled rather than left pending forever.
    """
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + "]}]",
    )

    runner = FakeRunner()
    runner.cancelled = True
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name=None,
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=False,
    )

    result = run_classify_job(job, runner, db_path, ws, params)
    assert result["predictions_stored"] == 0

    finals = _final_step_statuses(runner)
    for step_id in ("load_taxonomy", "load_model", "detect",
                    "classify", "finalize"):
        assert finals.get(step_id) == "cancelled", (
            f"Step {step_id!r} must be marked cancelled on the "
            f"pre-resolution cancel gate, got {finals.get(step_id)!r}"
        )


def test_store_pending_alt_that_folds_to_primary_does_not_hide_top1(tmp_path):
    """A pending primary + a curly-spelled alternative that folds to the
    same species must NOT overwrite the primary's review row with
    ``status='alternative'``.

    ``Database.add_prediction`` folds species centrally, so both the
    primary and the collision-alternative resolve to the same predictions
    row. The alternative's INSERT-OR-IGNORE is a no-op on that row, but
    the second call still re-queries and unconditionally upserts
    ``prediction_review`` — with ``status='alternative'``. Without the
    dedupe added in ``_store_pending_detection_prediction``, the only
    top-1 prediction for that detection would appear as an alternative
    and drop out of the pending queue.
    """
    from unittest.mock import patch

    from classify_job import _store_grouped_predictions
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0,
                       timestamp="2024-01-15T10:00:00")
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")

    raw_results = [{
        "photo": {"id": pid, "filename": "bird.jpg",
                  "timestamp": "2024-01-15T10:00:00"},
        "detection_id": det_ids[0],
        "folder_path": "/photos",
        "image_path": "/photos/bird.jpg",
        "prediction": "Say's phoebe",
        "confidence": 0.85,
        "timestamp": None,
        "filename": "bird.jpg",
        "embedding": None,
        "taxonomy": None,
        # Curly-spelled variant folds to the same species as the primary.
        # Also include a genuinely different alternative to confirm the
        # dedupe skips only the collision.
        "alternatives": [
            {"species": "Say’s phoebe", "confidence": 0.10, "taxonomy": None},
            {"species": "Cassin's kingbird", "confidence": 0.05,
             "taxonomy": None},
        ],
    }]

    with patch("xmp.read_keywords", return_value=[]), \
         patch("compare.categorize", return_value="new"):
        _store_grouped_predictions(
            raw_results=raw_results,
            job_id="test-alt-collision-1",
            model_name="test-model",
            grouping_window=10,
            similarity_threshold=0.85,
            tax=None,
            db=db,
        )

    all_preds = db.get_predictions()
    species = sorted(p["species"] for p in all_preds)
    assert species == ["Cassin's kingbird", "Say's phoebe"], species

    pending = db.get_predictions(status="pending")
    assert [p["species"] for p in pending] == ["Say's phoebe"], (
        "the primary must remain in the pending queue — an alternative that "
        "folds to the same species must not upsert status='alternative' onto it"
    )
    alts = db.get_predictions(status="alternative")
    assert [p["species"] for p in alts] == ["Cassin's kingbird"]


def test_store_match_alt_that_folds_to_primary_preserves_auto_accept(tmp_path):
    """Same collision on the match path: an auto-accepted primary with the
    ``AUTO_MATCH_REVIEW_MARKER`` must keep its accepted status when an
    alternative folds to it. Without the dedupe, the alternative's
    ``status='alternative'`` upsert would overwrite the auto-accept and
    the previously-labeled match would silently reappear in review."""
    from classify_job import _store_match_prediction
    from db import AUTO_MATCH_REVIEW_MARKER, Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")

    item = {
        "detection_id": det_ids[0],
        "prediction": "Say's phoebe",
        "confidence": 0.85,
        "taxonomy": None,
        "alternatives": [
            {"species": "Say’s phoebe", "confidence": 0.10, "taxonomy": None},
        ],
    }
    _store_match_prediction(
        db, item, model_name="test-model", labels_fingerprint="fp1",
    )

    rows = db.conn.execute(
        "SELECT p.species, pr.status, pr.individual "
        "FROM predictions p "
        "LEFT JOIN prediction_review pr "
        "  ON pr.prediction_id = p.id AND pr.workspace_id = ? "
        "WHERE p.detection_id = ?",
        (ws_id, det_ids[0]),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["species"] == "Say's phoebe"
    assert rows[0]["status"] == "accepted", (
        "the alternative that folds to the primary must not overwrite the "
        "auto-accept back to status='alternative'"
    )
    assert rows[0]["individual"] == AUTO_MATCH_REVIEW_MARKER


def test_store_match_alt_that_only_differs_in_case_preserves_auto_accept(
    tmp_path,
):
    """Downstream keyword joins already use ``COLLATE NOCASE``, so a merged
    label set yielding primary ``Say's Phoebe`` and alternative ``Say's
    phoebe`` is semantically one bird. The BINARY UNIQUE on
    ``predictions.species`` would still let both survive as distinct rows,
    with the alternative overwriting the primary's ``prediction_review``.
    The alternatives-dedupe must therefore match case-insensitively too,
    otherwise the auto-accepted match silently reappears in review.
    """
    from classify_job import _store_match_prediction
    from db import AUTO_MATCH_REVIEW_MARKER, Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")

    item = {
        "detection_id": det_ids[0],
        "prediction": "Say's Phoebe",
        "confidence": 0.85,
        "taxonomy": None,
        "alternatives": [
            {"species": "Say's phoebe", "confidence": 0.10, "taxonomy": None},
        ],
    }
    _store_match_prediction(
        db, item, model_name="test-model", labels_fingerprint="fp1",
    )

    rows = db.conn.execute(
        "SELECT p.species, pr.status, pr.individual "
        "FROM predictions p "
        "LEFT JOIN prediction_review pr "
        "  ON pr.prediction_id = p.id AND pr.workspace_id = ? "
        "WHERE p.detection_id = ?",
        (ws_id, det_ids[0]),
    ).fetchall()
    assert len(rows) == 1, (
        "case-differing alternative must be deduped: it and the primary are "
        "one species under COLLATE NOCASE"
    )
    assert rows[0]["status"] == "accepted"
    assert rows[0]["individual"] == AUTO_MATCH_REVIEW_MARKER


def test_store_match_alt_non_ascii_case_preserves_both_predictions(tmp_path):
    """SQLite ``COLLATE NOCASE`` and ``keyword_match_key`` fold only A-Z, so
    ``Éclair`` and ``éclair`` are distinct keywords on the DB side.
    ``str.lower()`` would collapse them into one dedupe key and drop the
    alternative; the ASCII-only fold used by ``_species_match_key`` must
    preserve both predictions so the alternative reaches the UI.
    """
    from classify_job import _store_match_prediction
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(folder_id=fid, filename="bird.jpg", extension=".jpg",
                       file_size=1000, file_mtime=1.0)
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="megadetector-v6")

    item = {
        "detection_id": det_ids[0],
        "prediction": "Éclair",
        "confidence": 0.85,
        "taxonomy": None,
        "alternatives": [
            # Non-ASCII case difference only. SQLite treats these as
            # distinct rows; a Python ``str.lower()`` dedupe would drop
            # this alternative and hide it from the user.
            {"species": "éclair", "confidence": 0.10, "taxonomy": None},
        ],
    }
    _store_match_prediction(
        db, item, model_name="test-model", labels_fingerprint="fp1",
    )

    species = sorted(
        r["species"]
        for r in db.conn.execute(
            "SELECT species FROM predictions WHERE detection_id = ?",
            (det_ids[0],),
        ).fetchall()
    )
    assert species == ["Éclair", "éclair"], (
        "non-ASCII case variants are distinct rows under COLLATE NOCASE; "
        "the alternative must not be deduped away by a Unicode ``.lower()``"
    )


def test_burst_consensus_non_ascii_case_variants_stay_split(tmp_path):
    """Two burst frames predicting ``Éclair`` and ``éclair`` are distinct
    species under SQLite ``COLLATE NOCASE``. A Python ``.lower()`` fold
    would report them as one unanimous species; the ASCII-only fold used
    by ``_species_match_key`` must keep them separate so ``group_species``
    has two entries and ``group_reviewable`` stays False (no group_id or
    inflated vote count on the stored predictions).
    """
    from datetime import datetime
    from unittest.mock import MagicMock

    from classify_job import _store_grouped_predictions

    mock_db = MagicMock()

    raw_results = [
        {
            "photo": {"id": 1, "filename": "a.jpg"},
            "detection_id": 201,
            "folder_path": "/photos",
            "prediction": "Éclair",
            "confidence": 0.95,
            "timestamp": datetime(2024, 1, 15, 10, 0, 0),
            "filename": "a.jpg",
            "embedding": None,
            "taxonomy": None,
        },
        {
            "photo": {"id": 2, "filename": "b.jpg"},
            "detection_id": 202,
            "folder_path": "/photos",
            "prediction": "éclair",
            "confidence": 0.90,
            "timestamp": datetime(2024, 1, 15, 10, 0, 3),
            "filename": "b.jpg",
            "embedding": None,
            "taxonomy": None,
        },
    ]

    _store_grouped_predictions(
        raw_results=raw_results,
        job_id="classify-test",
        model_name="BioCLIP",
        grouping_window=10,
        similarity_threshold=0.85,
        tax=None,
        db=mock_db,
    )

    calls = mock_db.add_prediction.call_args_list
    assert len(calls) == 2
    for c in calls:
        kwargs = c.kwargs or c[1]
        assert kwargs["group_id"] is None, (
            "non-ASCII case variants are distinct species; the burst must "
            "not collapse to a single group_id via ``.lower()``"
        )
        assert kwargs["vote_count"] is None
        assert kwargs["individual"] is None


def test_all_photos_cache_satisfied_requires_every_photo_covered(tmp_path):
    from classify_job import _all_photos_cache_satisfied
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    covered = db.add_photo(
        folder_id, "covered.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    uncovered = db.add_photo(
        folder_id, "bare.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(covered, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    # A classifier_run row without a matching predictions row is a torn
    # write from a crashed local job — _all_photos_cache_satisfied must
    # reject it as unsatisfied so the retry re-runs classification
    # instead of shortcutting to _finalize_cached_only.
    db.record_classifier_run(det_id, "BioCLIP", "abc123", prediction_count=1)
    db.add_prediction(
        det_id, species="Robin", confidence=0.9, model="BioCLIP",
        labels_fingerprint="abc123",
    )

    assert _all_photos_cache_satisfied(db, [covered]) is True
    assert _all_photos_cache_satisfied(db, [covered, uncovered]) is False
    # Empty photo list: no work possible, treated as not-satisfied so the
    # caller falls through to normal empty-collection handling.
    assert _all_photos_cache_satisfied(db, []) is False


def test_all_photos_cache_satisfied_requires_common_identity_when_either_unresolved(
    tmp_path,
):
    """When only one of (classifier_model, labels_fingerprint) is known —
    e.g. a fresh install where the model name is available but its label
    sources aren't — cached rows may still legitimately span multiple
    values for the unbound component.  ``_finalize_cached_only`` adopts
    the first row's identity for reconciliation, so the satisfaction
    check must refuse to short-circuit unless every covered row agrees
    on that component too.
    """
    from classify_job import _all_photos_cache_satisfied
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 2, "y": 2, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")
    # Same classifier, DIFFERENT label fingerprints across the two runs.
    # Add matching predictions rows so the coverage query treats each
    # classifier_run as a genuine cache row rather than a torn write.
    db.record_classifier_run(det_ids[0], "BioCLIP", "fp-a", prediction_count=1)
    db.add_prediction(
        det_ids[0], species="Robin", confidence=0.9, model="BioCLIP",
        labels_fingerprint="fp-a",
    )
    db.record_classifier_run(det_ids[1], "BioCLIP", "fp-b", prediction_count=1)
    db.add_prediction(
        det_ids[1], species="Sparrow", confidence=0.9, model="BioCLIP",
        labels_fingerprint="fp-b",
    )

    # Model known, fingerprint unresolved: the identity gate must refuse
    # this because the cached rows disagree on the unbound fingerprint.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP", labels_fingerprint=None,
    ) is False
    # Fingerprint known, model unresolved: mirror case (all rows share
    # the classifier, so if fingerprint is filtered to just one, only
    # matching rows count — narrow the coverage instead).
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model=None, labels_fingerprint="fp-a",
    ) is False
    # Both fully specified: filter narrows to a single identity, gate
    # not needed — but only one detection matches, so coverage fails.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP", labels_fingerprint="fp-a",
    ) is False

    # A historical row outside the resolved model filter must not pollute
    # the distinct-identity gate. Both classifiable detections below are
    # covered by BioCLIP/fp-a; the unrelated model is merely extra history.
    pid2 = db.add_photo(
        folder_id, "b.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det2 = db.save_detections(pid2, [
        {"box": {"x": 4, "y": 4, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
        {"box": {"x": 6, "y": 6, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")
    for det_id in det2:
        db.record_classifier_run(det_id, "BioCLIP", "fp-a", prediction_count=1)
        db.add_prediction(
            det_id, species="Robin", confidence=0.9, model="BioCLIP",
            labels_fingerprint="fp-a",
        )
    db.record_classifier_run(det2[0], "Other", "fp-x", prediction_count=1)
    db.add_prediction(
        det2[0], species="Other bird", confidence=0.8, model="Other",
        labels_fingerprint="fp-x",
    )
    assert _all_photos_cache_satisfied(
        db, [pid2], classifier_model="BioCLIP", labels_fingerprint=None,
    ) is True


def test_all_photos_cache_satisfied_rejects_stale_classifier_runtime(tmp_path):
    """When classifier weights or preprocessing change under the same
    ``classifier_model`` and label fingerprint, an unreviewed
    classifier_runs row from the OLD runtime must not satisfy this
    join.  Without the runtime filter, ``run_classify_job`` would
    shortcut to ``_finalize_cached_only`` on wrong-runtime results
    even though the ordinary ``_classify_photos`` gate would reject
    them via ``_runtime_aware_run_keys``.

    A manually-reviewed row must still count so the pin exception
    survives runtime changes until an explicit reclassify.
    """
    from classify_job import _all_photos_cache_satisfied
    from computation_cache import (
        classifier_model_identity,
        classifier_runtime_fingerprint,
        runtime_fingerprint,
        source_input,
    )
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    detector_runtime = runtime_fingerprint({
        "type": "detection", "model": "megadetector-v6",
        "weights_sha256": "2" * 64, "pipeline": "detector-v1",
    })
    _input, det_input_fp = source_input(
        "0" * 64, "vireo-detector-source-v1",
    )
    det_id = db.write_detection_batch(
        pid, "megadetector-v6",
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0.9, "category": "animal"}],
        runtime_fingerprint=detector_runtime,
        input_fingerprint=det_input_fp,
    )[0]

    labels_full = "5" * 64
    labels_short = labels_full[:12]
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "image_encoder.onnx").write_bytes(b"exact model bytes")
    identity = classifier_model_identity({
        "id": "bioclip-test",
        "model_str": "ViT-test",
        "model_type": "bioclip",
        "weights_path": str(model_dir),
        "files": ["image_encoder.onnx"],
        "source": "custom",
    })
    expected_runtime = classifier_runtime_fingerprint(
        identity, labels_full, detector_runtime,
    )
    assert expected_runtime is not None

    # A stale classifier_run: same (model, labels_short) but wrong
    # runtime_fingerprint (older classifier weights).
    stale_runtime = "9" * 64
    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint,
              runtime_fingerprint, prediction_count)
           VALUES (?, ?, ?, ?, ?)""",
        (det_id, "BioCLIP", labels_short, stale_runtime, 1),
    )
    db.add_prediction(
        det_id, species="Robin", confidence=0.9, model="BioCLIP",
        labels_fingerprint=labels_short,
    )
    db.conn.commit()

    # Without runtime constraint, historic behavior would treat this as
    # satisfied.  Confirm the old-shape call still returns True so we
    # know the fix is what changes the answer, not an unrelated regression.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
    ) is True

    # With the runtime constraint the stale row must be excluded.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
        model_identity=identity, labels_fingerprint_full=labels_full,
    ) is False

    # A row with the CORRECT runtime satisfies the check.
    db.conn.execute(
        "UPDATE classifier_runs SET runtime_fingerprint = ? WHERE detection_id = ?",
        (expected_runtime, det_id),
    )
    db.conn.commit()
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
        model_identity=identity, labels_fingerprint_full=labels_full,
    ) is True

    # A stale row with a real manual-review pin stays authoritative —
    # the pin exception mirrors ``get_classifier_run_keys``' behavior.
    db.conn.execute(
        "UPDATE classifier_runs SET runtime_fingerprint = ? WHERE detection_id = ?",
        (stale_runtime, det_id),
    )
    pred_row = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()
    db.conn.execute(
        """INSERT INTO prediction_review
             (prediction_id, workspace_id, status, reviewed_at, individual)
           VALUES (?, ?, 'accepted', datetime('now'), 'user-choice')""",
        (pred_row["id"], ws),
    )
    db.conn.commit()
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
        model_identity=identity, labels_fingerprint_full=labels_full,
    ) is True


def test_all_photos_cache_satisfied_rejects_pre_synonym_classifier_runtime(
    tmp_path, monkeypatch,
):
    """A run recorded before the scientific-name synonym map shipped must
    not satisfy the cache once the map is installed.

    The map changes what the classifier emits for outdated binomials
    ("Bubulcus ibis" -> the current "Ardea ibis" taxon, with its common
    name and full hierarchy), so a pre-synonym run and a post-synonym run
    are semantically different output for the same model, labels,
    taxonomy and detector runtime. If the runtime identity ignored the
    map, this join would report the collection as already classified and
    the reclassify that replaces the raw binomials would be skipped
    (Codex #1560 P2).
    """
    import taxonomy as tax_mod
    from classify_job import _all_photos_cache_satisfied
    from computation_cache import (
        classifier_model_identity,
        classifier_runtime_fingerprint,
        runtime_fingerprint,
        source_input,
    )
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    detector_runtime = runtime_fingerprint({
        "type": "detection", "model": "megadetector-v6",
        "weights_sha256": "2" * 64, "pipeline": "detector-v1",
    })
    _input, det_input_fp = source_input(
        "0" * 64, "vireo-detector-source-v1",
    )
    det_id = db.write_detection_batch(
        pid, "megadetector-v6",
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0.9, "category": "animal"}],
        runtime_fingerprint=detector_runtime,
        input_fingerprint=det_input_fp,
    )[0]

    labels_full = "5" * 64
    labels_short = labels_full[:12]
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "image_encoder.onnx").write_bytes(b"exact model bytes")
    identity = classifier_model_identity({
        "id": "bioclip-test",
        "model_str": "ViT-test",
        "model_type": "bioclip",
        "weights_path": str(model_dir),
        "files": ["image_encoder.onnx"],
        "source": "custom",
    })

    # The runtime an install with NO synonym map would have recorded.
    monkeypatch.setattr(tax_mod, "_SCIENTIFIC_SYNONYMS", {})
    pre_synonym_runtime = classifier_runtime_fingerprint(
        identity, labels_full, detector_runtime, taxonomy_identity="1" * 64,
    )
    assert pre_synonym_runtime is not None

    # Now the map ships. Same model, labels, taxonomy and detector.
    monkeypatch.setattr(
        tax_mod, "_SCIENTIFIC_SYNONYMS", {"bubulcus ibis": "Ardea ibis"},
    )
    post_synonym_runtime = classifier_runtime_fingerprint(
        identity, labels_full, detector_runtime, taxonomy_identity="1" * 64,
    )
    assert post_synonym_runtime is not None
    assert post_synonym_runtime != pre_synonym_runtime

    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint,
              runtime_fingerprint, prediction_count)
           VALUES (?, ?, ?, ?, ?)""",
        (det_id, "BioCLIP", labels_short, pre_synonym_runtime, 1),
    )
    db.add_prediction(
        det_id, species="Bubulcus ibis", confidence=0.9, model="BioCLIP",
        labels_fingerprint=labels_short,
    )
    db.conn.commit()

    # The pre-synonym row must NOT satisfy the cache now that the map is
    # present — otherwise classify short-circuits and the raw binomial
    # stays on screen.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
        model_identity=identity, labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is False

    # A row recorded with the current map is still accepted, so this is a
    # staleness gate and not a blanket cache disable.
    db.conn.execute(
        "UPDATE classifier_runs SET runtime_fingerprint = ? "
        "WHERE detection_id = ?",
        (post_synonym_runtime, det_id),
    )
    db.conn.commit()
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
        model_identity=identity, labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is True


def test_finalize_cached_only_respects_workspace_detector_threshold(
    tmp_path, monkeypatch,
):
    """A bundle imported from a machine with a lower detector_confidence
    can materialize classifier runs for raw detections that the
    destination workspace's threshold would hide.  The cache-only
    finalize path must apply the workspace-effective threshold before
    grouping — otherwise importing results silently changes which
    subjects are surfaced to review.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    # A workspace whose effective threshold hides everything below 0.5.
    ws = db.create_workspace(
        "Strict", config_overrides={"detector_confidence": 0.5},
    )
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Two detections: one clearly above the workspace threshold, one
    # clearly below. Only the classifiable, above-threshold row needs a
    # cached run for the all-cache-hit shortcut to be valid.
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 2, "y": 2, "w": 1, "h": 1}, "confidence": 0.3, "category": "animal"},
    ], detector_model="megadetector-v6")
    db.add_prediction(det_ids[0], species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="fp-cached")
    db.record_classifier_run(det_ids[0], "BioCLIP", "fp-cached",
                             prediction_count=1)

    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]',
    )

    import classify_job as classify_job_mod
    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [])

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None, labels_file=None,
        model_id=None, model_name=None,
        grouping_window=0, similarity_threshold=0.99,
        reclassify=False,
    )

    result = run_classify_job(job, runner, db_path, ws, params)
    # Only the above-threshold detection reaches finalization.  Without
    # the workspace threshold the count would be 2.
    assert result["already_classified"] == 1


def test_cached_only_shortcut_rearms_timm_label_desc_heal(
    tmp_path, monkeypatch,
):
    """A cached-only classify job must still fire the timm
    label_descriptions self-heal, otherwise a hot cache stops the
    bounded-retry state machine from ever running again.

    Before the fix, the only paths that drove the state machine were
    ``TimmClassifier.__init__`` (cache-miss construction) and
    ``acquire_cached_classifier``'s ``notify_reuse`` hook (cache-hit
    acquire). ``_all_photos_cache_satisfied``'s shortcut returns
    *without* acquiring a classifier at all, so on a long-lived process
    that only ever runs cached-only jobs, a first-probe failure during
    an outage would leave the installation stuck on ``failed`` for the
    rest of its life — even after connectivity recovered.
    """
    import classify_job as classify_job_mod
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.add_prediction(
        det_id, species="Western Cattle-Egret", confidence=0.9,
        model="timm-inat21", labels_fingerprint="fp-cached",
    )
    db.record_classifier_run(
        det_id, "timm-inat21", "fp-cached", prediction_count=1,
    )
    coll_id = db.add_collection(
        "c", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    model_dir = tmp_path / "timm-inat21-eva02-l"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"onnx bytes")
    model_str = "hf-hub:timm/eva02-test"
    fake_timm_model = {
        "id": "timm-inat21-eva02-l",
        "name": "timm-inat21",
        "model_str": model_str,
        "model_type": "timm",
        "weights_path": str(model_dir),
        "downloaded": True,
        "files": ["model.onnx"],
        "optional_files": ["label_descriptions.json"],
        "source": "custom",
    }
    monkeypatch.setattr(
        classify_job_mod, "get_models", lambda: [fake_timm_model],
    )
    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(
        classify_job_mod, "get_active_model", lambda: fake_timm_model,
    )

    calls = []

    def _spy_rearm(mstr, mdir):
        calls.append((mstr, mdir))
        return None

    import timm_classifier as tc
    monkeypatch.setattr(tc, "rearm_pending_label_desc_heal", _spy_rearm)

    # Force the cached-only shortcut deterministically. The rearm hook
    # is wired to fire *before* this predicate is checked, so a True
    # return here proves the ordering is right without needing every
    # identity axis to align.
    finalize_called = []

    def _fake_all_cache_satisfied(*a, **kw):
        # Rearm must already have fired by the time we get here.
        assert calls, (
            "rearm_pending_label_desc_heal must fire before "
            "_all_photos_cache_satisfied is consulted"
        )
        return True

    def _fake_finalize(*a, **kw):
        finalize_called.append(True)
        return {"already_classified": 1, "failed": 0, "classified": 0}

    monkeypatch.setattr(
        classify_job_mod, "_all_photos_cache_satisfied",
        _fake_all_cache_satisfied,
    )
    monkeypatch.setattr(
        classify_job_mod, "_finalize_cached_only", _fake_finalize,
    )

    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None, labels_file=None,
        model_id="timm-inat21-eva02-l", model_name=None,
        grouping_window=0, similarity_threshold=0.99,
        reclassify=False,
    )
    run_classify_job(_make_job(), FakeRunner(), db_path, ws, params)

    assert finalize_called, (
        "the cached-only shortcut should have been taken so this test "
        "exercises the same branch the fix targets"
    )

    assert calls == [(model_str, str(model_dir))], (
        "the cached-only shortcut must re-arm the timm label-descriptions "
        "heal before returning; otherwise a transient outage on the first "
        "probe strands the installation for the life of the process"
    )


def test_finalize_cached_only_includes_zero_confidence_full_image_anchor(
    tmp_path, monkeypatch,
):
    """Full-image anchors bypass the positive MegaDetector threshold."""
    import classify_job as classify_job_mod
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "empty.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0,
        "category": "animal",
    }], detector_model="full-image")[0]
    db.add_prediction(
        det_id, species="Full-image Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="fp-cached",
    )
    db.record_classifier_run(
        det_id, "BioCLIP", "fp-cached", prediction_count=1,
    )
    collection_id = db.add_collection(
        "empty", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [])
    result = run_classify_job(
        _make_job(), FakeRunner(), db_path, ws,
        ClassifyParams(
            collection_id=collection_id,
            labels_files=None, labels_file=None,
            model_id=None, model_name=None,
            grouping_window=0, similarity_threshold=0.99,
            reclassify=False,
        ),
    )

    assert result["already_classified"] == 1


def test_finalize_cached_only_treats_null_category_as_animal(
    tmp_path, monkeypatch,
):
    """Legacy NULL categories match the cache-coverage SQL's animal default."""
    import classify_job as classify_job_mod
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    folder_id = db.add_folder("/tmp/p", name="p")
    photo_id = db.add_photo(
        folder_id, "legacy.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    detection_id = db.save_detections(photo_id, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0.9,
        "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.conn.execute(
        "UPDATE detections SET category = NULL WHERE id = ?",
        (detection_id,),
    )
    db.add_prediction(
        detection_id, species="Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="fp-cached",
    )
    db.record_classifier_run(
        detection_id, "BioCLIP", "fp-cached", prediction_count=1,
    )
    db.conn.commit()
    collection_id = db.add_collection(
        "legacy", json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [])
    result = run_classify_job(
        _make_job(), FakeRunner(), db_path, workspace_id,
        ClassifyParams(
            collection_id=collection_id,
            labels_files=None, labels_file=None,
            model_id=None, model_name=None,
            grouping_window=0, similarity_threshold=0.99,
            reclassify=False,
        ),
    )

    assert result["already_classified"] == 1


def test_cached_only_path_publishes_label_source_on_classify_step(
    tmp_path, monkeypatch,
):
    """Regression: re-running classification against a fully cached
    collection must still record which lists produced its predictions on
    the classify step. Before the fix, ``_finalize_cached_only``
    short-circuited ``run_classify_job`` before the label_source
    assignment ran, so the classify history row for cached-only runs was
    the unlabeled state the whole feature set out to eliminate.

    Rather than staging a full model on disk to satisfy the cache-check
    machinery, we monkey-patch the shortcut predicates directly. That
    isolates the specific behavior under test (label_source publication
    for the cache-only path) from the identity-fingerprint plumbing that
    other tests already cover.
    """
    import classify_job as classify_job_mod
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.add_prediction(
        det_id, species="Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="fp-cached",
    )
    db.record_classifier_run(
        det_id, "BioCLIP", "fp-cached", prediction_count=1,
    )
    coll_id = db.add_collection(
        "c", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    # An active labels_file that ``_load_labels`` will actually load, so
    # the peek path returns real labels and the cached-only branch has a
    # concrete label space to describe.
    labels_file = tmp_path / "birds.txt"
    labels_file.write_text("Robin\nBlue Jay\n")

    # Stand in for a downloaded model — the peek only reads model metadata,
    # not weights, and the shortcut skips construction entirely.
    model_dir = tmp_path / "bioclip"
    model_dir.mkdir()
    fake_model = {
        "id": "bioclip-x",
        "name": "BioCLIP",
        "model_str": "hf-hub:imageomics/bioclip",
        "model_type": "bioclip",
        "weights_path": str(model_dir),
        "downloaded": True,
    }
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [fake_model])
    monkeypatch.setattr(
        classify_job_mod, "get_active_model", lambda: fake_model,
    )
    monkeypatch.setattr(
        classify_job_mod, "get_saved_labels",
        lambda: [{"labels_file": str(labels_file), "name": "Backyard Birds"}],
    )
    monkeypatch.setattr(
        classify_job_mod, "get_active_labels",
        lambda: [{"labels_file": str(labels_file), "name": "Backyard Birds"}],
    )

    # Force the cache-only shortcut regardless of whether the identity
    # fingerprints happen to align — this test is about label_source, not
    # the fingerprint match logic (which is exercised by other tests).
    monkeypatch.setattr(
        classify_job_mod, "_all_photos_cache_satisfied",
        lambda *a, **kw: True,
    )

    runner = FakeRunner()
    run_classify_job(
        _make_job(), runner, db_path, ws,
        ClassifyParams(
            collection_id=coll_id,
            labels_files=[str(labels_file)], labels_file=None,
            model_id="bioclip-x", model_name=None,
            grouping_window=0, similarity_threshold=0.99,
            reclassify=False,
        ),
    )

    # The classify step carries a label_source line naming the actual
    # list, not "Tree of Life" or a blank string. Runs on the cache-only
    # path publish it via ``_finalize_cached_only`` — the model-loading
    # branch's copy never runs on this path.
    classify_label_sources = [
        kwargs["label_source"]
        for step_id, kwargs in runner.steps
        if step_id == "classify" and "label_source" in kwargs
    ]
    assert classify_label_sources, (
        "cached-only run must publish a label_source line on the classify "
        "step; otherwise its history entry is indistinguishable from a "
        "pre-feature run"
    )
    assert "Backyard Birds" in classify_label_sources[-1]
    assert "2 species" in classify_label_sources[-1]


def test_cached_only_path_publishes_source_when_peek_fails(
    tmp_path, monkeypatch,
):
    """Regression: on a fresh install with an imported cache, ``_load_labels``
    can raise (selected lists deleted, Tree-of-Life artifacts missing) and
    the classify job falls back to model-only cache filtering. Before the
    fix, ``_finalize_cached_only`` only published a ``label_source`` when
    the peek succeeded, so this shortcut left the classify history row
    unlabeled — exactly the state the whole feature set out to eliminate.
    Recover the source from ``labels_fingerprints`` so the row still names
    which list produced the cached predictions.
    """
    import classify_job as classify_job_mod
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    db.add_prediction(
        det_id, species="Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="fp-cached",
    )
    db.record_classifier_run(
        det_id, "BioCLIP", "fp-cached", prediction_count=1,
    )
    # Populate the sidecar as the original run would have — this is the
    # provenance the cached-only fallback recovers from.
    db.upsert_labels_fingerprint(
        fingerprint="fp-cached",
        display_name="California, US Birds",
        sources=["/imported/california.txt"],
        label_count=1327,
    )
    coll_id = db.add_collection(
        "c", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    # No models are installed anywhere, and no label list is active — the
    # label peek raises RuntimeError, so ``peek_succeeded`` stays False.
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    monkeypatch.setattr(classify_job_mod, "get_saved_labels", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])

    runner = FakeRunner()
    run_classify_job(
        _make_job(), runner, db_path, ws,
        ClassifyParams(
            collection_id=coll_id,
            labels_files=None, labels_file=None,
            model_id=None, model_name=None,
            grouping_window=0, similarity_threshold=0.99,
            reclassify=False,
        ),
    )

    classify_label_sources = [
        kwargs["label_source"]
        for step_id, kwargs in runner.steps
        if step_id == "classify" and "label_source" in kwargs
    ]
    assert classify_label_sources, (
        "cached-only run must publish a label_source even when the label "
        "peek raised; without it the classify step is unlabeled and the "
        "history row is indistinguishable from a pre-feature run"
    )
    text = classify_label_sources[-1]
    assert "cached" in text.lower()
    assert "california.txt" in text
    assert "1,327 species" in text


def test_describe_cached_label_source_falls_back_when_no_sidecar(
    tmp_path, monkeypatch,
):
    """When the peek fails AND no ``labels_fingerprints`` row exists for
    the cached fingerprint, publish an explicit \"label source unavailable\"
    line rather than inventing a description — an honest unknown beats a
    misleading name.
    """
    from classify_job import _describe_cached_label_source
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")[0]
    # Pair the run with a matching predictions row so the eligibility
    # filter can select it — otherwise the "no sidecar" branch is never
    # reached and this test would collapse into the eligibility test
    # below.
    db.add_prediction(
        det_id, species="Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="fp-orphan",
    )
    db.record_classifier_run(
        det_id, "BioCLIP", "fp-orphan", prediction_count=1,
    )
    # Deliberately NO upsert_labels_fingerprint call — the sidecar is empty.

    text = _describe_cached_label_source(db, [pid])
    assert text == "Reused cached predictions (label source unavailable)"


def test_describe_cached_label_source_skips_ineligible_runs(
    tmp_path, monkeypatch,
):
    """When a photo carries cached runs for BOTH an eligible detection
    (the one ``_all_photos_cache_satisfied`` counts as covered) and an
    ineligible one — a sub-threshold detection, or a torn classifier_run
    with no matching predictions row — the fingerprint lookup must only
    consider the eligible run's fingerprint. Otherwise the persisted
    ``label_source`` names a list that did not produce these predictions,
    because the ``DISTINCT ... LIMIT 1`` query might pick the excluded
    row first.
    """
    from classify_job import _describe_cached_label_source
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    # Two real detections: one above the workspace threshold (eligible),
    # one below (ineligible). Distinct boxes so ``detection_id`` (which
    # is content-addressed) does not collapse them into one row.
    dets = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
        {"box": {"x": 2, "y": 2, "w": 1, "h": 1},
         "confidence": 0.05, "category": "animal"},
    ], detector_model="megadetector-v6")
    eligible_det = dets[0]
    ineligible_det = dets[1]

    # Eligible detection: run against the list actually reused
    # ("fp-eligible") — a matching predictions row proves the cache hit
    # is real.
    db.add_prediction(
        eligible_det, species="Robin", confidence=0.9,
        model="BioCLIP", labels_fingerprint="fp-eligible",
    )
    db.record_classifier_run(
        eligible_det, "BioCLIP", "fp-eligible", prediction_count=1,
    )

    # Ineligible detection: a stale run for a DIFFERENT label set
    # ("fp-below-threshold"). Also insert a "torn" classifier_run on the
    # eligible detection under a THIRD fingerprint ("fp-torn") that has
    # no matching predictions row — covers the second exclusion in the
    # coverage query.
    db.add_prediction(
        ineligible_det, species="Sparrow", confidence=0.5,
        model="BioCLIP", labels_fingerprint="fp-below-threshold",
    )
    db.record_classifier_run(
        ineligible_det, "BioCLIP", "fp-below-threshold", prediction_count=1,
    )
    db.record_classifier_run(
        eligible_det, "BioCLIP", "fp-torn", prediction_count=1,
    )

    # Populate only the sidecar for the fingerprint that should win.
    db.upsert_labels_fingerprint(
        fingerprint="fp-eligible",
        display_name="California",
        sources=["/imported/california.txt"],
        label_count=1327,
    )
    # And a decoy for the fingerprint that MUST NOT be reported.
    db.upsert_labels_fingerprint(
        fingerprint="fp-below-threshold",
        display_name="Should-not-appear",
        sources=["/imported/should-not-appear.txt"],
        label_count=42,
    )
    db.upsert_labels_fingerprint(
        fingerprint="fp-torn",
        display_name="Also-should-not-appear",
        sources=["/imported/also-should-not-appear.txt"],
        label_count=7,
    )

    text = _describe_cached_label_source(
        db, [pid], classifier_model="BioCLIP",
        detector_confidence=0.2,
    )
    assert text is not None
    assert "california.txt" in text
    assert "1,327 species" in text
    assert "should-not-appear" not in text
    assert "also-should-not-appear" not in text


def test_run_classify_job_short_circuits_when_cache_covers_every_photo(
    tmp_path, monkeypatch,
):
    """A fresh install with an imported cache but no classifier weights
    must not fail with "No model available" — every photo already has a
    completed classifier run, so model resolution is unnecessary.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="BioCLIP", labels_fingerprint="fp-cached")
    db.record_classifier_run(det_id, "BioCLIP", "fp-cached",
                             prediction_count=1)

    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]',
    )

    # Model resolution WOULD fail here — no active model exists. The
    # short-circuit must run before this raises.
    import classify_job as classify_job_mod
    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [])

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name=None,
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=False,
    )

    result = run_classify_job(job, runner, db_path, ws, params)
    assert result["already_classified"] == result["total"] == 1
    assert result["failed"] == 0
    # Cached prediction stays exactly one row — the finalization pass
    # updates review state / grouping in place but must not double up
    # the ``predictions`` row for a reused detection.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    assert db2.conn.execute(
        "SELECT COUNT(*) AS n FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()["n"] == 1


def test_run_classify_job_rejects_unknown_model_id_before_cache_shortcut(
    tmp_path, monkeypatch,
):
    """A stale job referencing a deleted model must fail with 'not found',
    not silently succeed by matching any classifier's cached rows.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_id = db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    # A classifier run exists — but from a different (unrelated) model
    # under a different name than the request. Without the explicit-id
    # rejection, the cache satisfaction check would accept it since
    # ``desired_classifier_model`` would fall back to None.
    db.add_prediction(det_id, species="Robin", confidence=0.9,
                      model="SomeOtherModel", labels_fingerprint="fp-x")
    db.record_classifier_run(det_id, "SomeOtherModel", "fp-x",
                             prediction_count=1)

    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]',
    )

    import classify_job as classify_job_mod
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    # get_models returns models but NONE match the requested id.
    monkeypatch.setattr(
        classify_job_mod, "get_models",
        lambda: [{"id": "other-id", "name": "SomeOtherModel",
                  "downloaded": True, "model_type": "bioclip",
                  "model_str": "", "weights_path": ""}],
    )

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None, labels_file=None,
        model_id="stale-id",  # references a model no longer in get_models
        model_name=None,
        grouping_window=0, similarity_threshold=0.99, reclassify=False,
    )

    with pytest.raises(RuntimeError, match="stale-id.*not found"):
        run_classify_job(job, runner, db_path, ws, params)


def test_run_classify_job_reconciles_group_metadata_on_reuse(
    tmp_path, monkeypatch,
):
    """When every photo is cache-satisfied, the finalize pass must still
    run: bursts must get a group_id, otherwise imported predictions never
    receive the consensus / group metadata a fresh classification pass
    produces.
    """
    import config as cfg
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    # Two photos captured within a burst window — imported cache carries
    # matching predictions for each so grouping/consensus should apply.
    pid_a = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
        timestamp="2024-01-01T10:00:00",
    )
    pid_b = db.add_photo(
        folder_id, "b.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
        timestamp="2024-01-01T10:00:01",
    )
    det_a = db.save_detections(pid_a, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    det_b = db.save_detections(pid_b, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9,
         "category": "animal"},
    ], detector_model="megadetector-v6")[0]
    for det_id in (det_a, det_b):
        db.add_prediction(det_id, species="Robin", confidence=0.9,
                          model="BioCLIP", labels_fingerprint="fp-cached")
        db.record_classifier_run(det_id, "BioCLIP", "fp-cached",
                                 prediction_count=1)

    coll_id = db.add_collection(
        "c",
        '[{"field":"photo_ids","value":['
        + str(pid_a) + ',' + str(pid_b) + ']}]',
    )

    import classify_job as classify_job_mod
    monkeypatch.setattr(classify_job_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(classify_job_mod, "get_active_model", lambda: None)
    monkeypatch.setattr(classify_job_mod, "get_models", lambda: [])

    runner = FakeRunner()
    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None, labels_file=None,
        model_id=None, model_name=None,
        grouping_window=10, similarity_threshold=0.99, reclassify=False,
    )

    result = run_classify_job(job, runner, db_path, ws, params)
    assert result["failed"] == 0
    # Burst grouping must have run — two nearby photos with the same
    # species belong in one reviewable group. Old code returned early
    # and left prediction_review.group_id NULL on the reused rows.
    db2 = Database(db_path)
    db2.set_active_workspace(ws)
    rows = db2.conn.execute(
        """SELECT pr_rev.group_id AS group_id
             FROM predictions pr
             JOIN prediction_review pr_rev
               ON pr_rev.prediction_id = pr.id
              AND pr_rev.workspace_id = ?
            WHERE pr.detection_id IN (?, ?)""",
        (ws, det_a, det_b),
    ).fetchall()
    assert len(rows) == 2
    group_ids = {r["group_id"] for r in rows}
    assert None not in group_ids, (
        "group metadata should be stamped by the reused-cache finalize path"
    )
    assert len(group_ids) == 1, "both frames should share one burst group"


def test_pre_heal_classifier_run_does_not_satisfy_cache(tmp_path):
    """A run recorded before label_descriptions.json landed must not be
    treated as an exact match once the file is there.

    TimmClassifier heals a missing label_descriptions.json on a
    background thread during ordinary operation, and that mapping is
    what turns a raw class id into the common name stored on a
    prediction. Every other identity axis — required files, pinned
    revision, label set, taxonomy, synonym map, detector runtime — is
    unchanged across the heal, so keying only on those let the pre-heal
    run compare equal to a post-heal one: `_all_photos_cache_satisfied`
    reported the collection as already classified and the reclassify
    that replaces the scientific names never ran (Codex #1560 P2).
    """
    from classify_job import _all_photos_cache_satisfied
    from computation_cache import (
        classifier_model_identity,
        classifier_runtime_fingerprint,
        runtime_fingerprint,
        source_input,
    )
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    detector_runtime = runtime_fingerprint({
        "type": "detection", "model": "megadetector-v6",
        "weights_sha256": "2" * 64, "pipeline": "detector-v1",
    })
    _input, det_input_fp = source_input(
        "0" * 64, "vireo-detector-source-v1",
    )
    det_id = db.write_detection_batch(
        pid, "megadetector-v6",
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0.9, "category": "animal"}],
        runtime_fingerprint=detector_runtime,
        input_fingerprint=det_input_fp,
    )[0]

    labels_full = "5" * 64
    labels_short = labels_full[:12]
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"exact model bytes")
    active_model = {
        "id": "timm-inat21-eva02-l",
        "model_str": "hf-hub:timm/eva02-test",
        "model_type": "timm",
        "weights_path": str(model_dir),
        "files": ["model.onnx"],
        "optional_files": ["label_descriptions.json"],
        "source": "custom",
    }

    # Pre-heal: the optional mapping has not been fetched yet.
    pre_heal_identity = classifier_model_identity(active_model)
    pre_heal_runtime = classifier_runtime_fingerprint(
        pre_heal_identity, labels_full, detector_runtime,
        taxonomy_identity="1" * 64,
    )
    assert pre_heal_runtime is not None

    # The background heal publishes the mapping.
    (model_dir / "label_descriptions.json").write_text(
        json.dumps({"Bubulcus ibis": "Cattle Egret, Bird"}),
    )
    post_heal_identity = classifier_model_identity(active_model)
    post_heal_runtime = classifier_runtime_fingerprint(
        post_heal_identity, labels_full, detector_runtime,
        taxonomy_identity="1" * 64,
    )
    assert post_heal_runtime is not None
    assert post_heal_runtime != pre_heal_runtime

    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint,
              runtime_fingerprint, prediction_count)
           VALUES (?, ?, ?, ?, ?)""",
        (det_id, "timm-inat21", labels_short, pre_heal_runtime, 1),
    )
    db.add_prediction(
        det_id, species="Bubulcus ibis", confidence=0.9, model="timm-inat21",
        labels_fingerprint=labels_short,
    )
    db.conn.commit()

    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="timm-inat21",
        labels_fingerprint=labels_short,
        model_identity=post_heal_identity,
        labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is False

    # A run recorded against the healed mapping is still reused, so this
    # is a staleness gate and not a blanket cache disable.
    db.conn.execute(
        "UPDATE classifier_runs SET runtime_fingerprint = ? "
        "WHERE detection_id = ?",
        (post_heal_runtime, det_id),
    )
    db.conn.commit()
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="timm-inat21",
        labels_fingerprint=labels_short,
        model_identity=post_heal_identity,
        labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is True


def test_all_photos_cache_satisfied_excludes_unreviewed_legacy_timm_after_heal(
    tmp_path,
):
    """A legacy classifier_run for a timm model whose
    ``label_descriptions.json`` has since transitioned from missing to
    present must not satisfy the cache unless it was manually reviewed.

    ``_all_photos_cache_satisfied`` grandfathers ``runtime_fingerprint
    = 'legacy'`` for pre-portable rows across ordinary runtime axis
    changes. That exception cannot survive the label-descriptions heal
    though: the mapping is what turns a raw class id into the stored
    common name, so an unreviewed legacy row's species is stale
    relative to what a run made today would emit ("Bubulcus ibis" ->
    "Western Cattle-Egret"). The manual-review branch still authorizes
    such rows across the enrichment change, matching the runtime-change
    pin exception (Codex #1560 P2).
    """
    from classify_job import _all_photos_cache_satisfied
    from computation_cache import (
        classifier_model_identity,
        runtime_fingerprint,
        source_input,
    )
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    detector_runtime = runtime_fingerprint({
        "type": "detection", "model": "megadetector-v6",
        "weights_sha256": "2" * 64, "pipeline": "detector-v1",
    })
    _input, det_input_fp = source_input(
        "0" * 64, "vireo-detector-source-v1",
    )
    det_id = db.write_detection_batch(
        pid, "megadetector-v6",
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0.9, "category": "animal"}],
        runtime_fingerprint=detector_runtime,
        input_fingerprint=det_input_fp,
    )[0]

    labels_full = "5" * 64
    labels_short = labels_full[:12]
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"exact model bytes")
    active_model = {
        "id": "timm-inat21-eva02-l",
        "model_str": "hf-hub:timm/eva02-test",
        "model_type": "timm",
        "weights_path": str(model_dir),
        "files": ["model.onnx"],
        "optional_files": ["label_descriptions.json"],
        "source": "custom",
    }

    # A legacy classifier_run (predates portable runtime tracking).
    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint,
              runtime_fingerprint, prediction_count)
           VALUES (?, ?, ?, ?, ?)""",
        (det_id, "timm-inat21", labels_short, "legacy", 1),
    )
    db.add_prediction(
        det_id, species="Bubulcus ibis", confidence=0.9,
        model="timm-inat21", labels_fingerprint=labels_short,
    )
    db.conn.commit()

    pre_heal_identity = classifier_model_identity(active_model)
    assert (
        pre_heal_identity["label_descriptions_identity"]
        == "no-label-descriptions"
    )

    # Pre-heal: enrichment absent, the legacy row is grandfathered.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="timm-inat21",
        labels_fingerprint=labels_short,
        model_identity=pre_heal_identity,
        labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is True

    # The heal publishes the mapping.
    (model_dir / "label_descriptions.json").write_text(
        json.dumps({"Bubulcus ibis": "Cattle Egret, Bird"}),
    )
    post_heal_identity = classifier_model_identity(active_model)
    assert (
        post_heal_identity["label_descriptions_identity"]
        != "no-label-descriptions"
    )

    # Post-heal: the unreviewed legacy row must no longer satisfy the
    # cache — a fresh run would rewrite the raw binomial.
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="timm-inat21",
        labels_fingerprint=labels_short,
        model_identity=post_heal_identity,
        labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is False

    # A manual accept/reject on the same row keeps it authoritative
    # across the enrichment change, matching the runtime-change pin
    # exception (auto-match reviews do not count).
    pred_row = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?",
        (det_id,),
    ).fetchone()
    db.conn.execute(
        """INSERT INTO prediction_review
             (prediction_id, workspace_id, status, reviewed_at, individual)
           VALUES (?, ?, 'accepted', datetime('now'), 'user-choice')""",
        (pred_row["id"], ws),
    )
    db.conn.commit()
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="timm-inat21",
        labels_fingerprint=labels_short,
        model_identity=post_heal_identity,
        labels_fingerprint_full=labels_full,
        taxonomy_identity="1" * 64,
    ) is True


def test_all_photos_cache_satisfied_still_grandfathers_legacy_bioclip(tmp_path):
    """The post-heal legacy exclusion is timm-specific.

    Non-timm classifiers (bioclip, test doubles) do not carry a
    ``label_descriptions_identity`` axis and therefore cannot experience
    the missing-to-present enrichment transition. Their legacy rows
    must keep being grandfathered as before.
    """
    from classify_job import _all_photos_cache_satisfied
    from computation_cache import (
        classifier_model_identity,
        runtime_fingerprint,
        source_input,
    )
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    detector_runtime = runtime_fingerprint({
        "type": "detection", "model": "megadetector-v6",
        "weights_sha256": "2" * 64, "pipeline": "detector-v1",
    })
    _input, det_input_fp = source_input(
        "0" * 64, "vireo-detector-source-v1",
    )
    det_id = db.write_detection_batch(
        pid, "megadetector-v6",
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0.9, "category": "animal"}],
        runtime_fingerprint=detector_runtime,
        input_fingerprint=det_input_fp,
    )[0]

    labels_full = "5" * 64
    labels_short = labels_full[:12]
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "image_encoder.onnx").write_bytes(b"exact model bytes")
    identity = classifier_model_identity({
        "id": "bioclip-test",
        "model_str": "ViT-test",
        "model_type": "bioclip",
        "weights_path": str(model_dir),
        "files": ["image_encoder.onnx"],
        "source": "custom",
    })
    assert "label_descriptions_identity" not in identity

    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint,
              runtime_fingerprint, prediction_count)
           VALUES (?, ?, ?, ?, ?)""",
        (det_id, "BioCLIP", labels_short, "legacy", 1),
    )
    db.add_prediction(
        det_id, species="Robin", confidence=0.9, model="BioCLIP",
        labels_fingerprint=labels_short,
    )
    db.conn.commit()
    assert _all_photos_cache_satisfied(
        db, [pid], classifier_model="BioCLIP",
        labels_fingerprint=labels_short,
        model_identity=identity, labels_fingerprint_full=labels_full,
    ) is True


def test_classifier_identity_uses_the_mapping_the_instance_consumed(tmp_path):
    """Publication must be stamped with what the classifier read, not a
    fresh disk probe.

    The heal thread starts at classifier construction and can publish
    label_descriptions.json while the job is still running. If the run
    were stamped from a probe taken at publish time, this pre-heal run
    would be recorded under the post-heal identity — and then every
    later cache check would accept it as an exact match, permanently
    skipping the reclassify that replaces its raw binomials.
    """
    from computation_cache import (
        classifier_model_identity,
        with_consumed_label_descriptions,
    )

    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"exact model bytes")
    active_model = {
        "id": "timm-inat21-eva02-l",
        "model_str": "hf-hub:timm/eva02-test",
        "model_type": "timm",
        "weights_path": str(model_dir),
        "files": ["model.onnx"],
        "optional_files": ["label_descriptions.json"],
        "source": "custom",
    }

    class _PreHealClassifier:
        # Built before the file landed, so it emits scientific names.
        label_descriptions_identity = "no-label-descriptions"

    # The heal lands between construction and this probe.
    (model_dir / "label_descriptions.json").write_text(
        json.dumps({"Bubulcus ibis": "Cattle Egret, Bird"}),
    )
    probed = classifier_model_identity(active_model)
    assert probed["label_descriptions_identity"] != "no-label-descriptions"

    stamped = with_consumed_label_descriptions(probed, _PreHealClassifier())
    assert stamped["label_descriptions_identity"] == "no-label-descriptions"
    assert stamped != probed
    # The probe result is not mutated in place — other callers keep theirs.
    assert probed["label_descriptions_identity"] != "no-label-descriptions"

    # A classifier that exposes nothing (bioclip, doubles) leaves the
    # probed value untouched.
    class _NoSnapshot:
        pass

    assert with_consumed_label_descriptions(probed, _NoSnapshot()) == probed


def test_classify_factory_pause_parks_outside_load_lock_then_retries(
    tmp_path, monkeypatch,
):
    """A pause during classifier construction must unwind the factory (so
    ``ModelCache._Entry.load_lock`` is released), park the job at its own
    boundary, and construct again after Resume. Before this, a pause during
    label-embedding setup was only honored once every embedding had been
    computed — for a 1255-label set on a loaded machine that meant the
    "pausing" state lasted hours.
    """
    import config as cfg
    from classifier import ClassifierLoadPaused
    from classify_job import ClassifyParams, run_classify_job
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p", name="p")
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(pid, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1},
         "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")
    coll_id = db.add_collection(
        "c", '[{"field":"photo_ids","value":[' + str(pid) + ']}]',
    )

    events = []

    class PausingRunner(FakeRunner):
        def __init__(self):
            super().__init__()
            self.paused = False

        def cancellation_requested(self, job_id):
            return False

        def pause_requested(self, job_id):
            return self.paused

        def is_cancelled(self, job_id):
            # The job's own pause boundary: simulate the user resuming.
            if self.paused:
                events.append("park")
                self.paused = False
            return False

    runner = PausingRunner()

    fake_weights = tmp_path / "weights"
    fake_weights.mkdir()
    (fake_weights / "tol_embeddings.npy").write_bytes(b"stub")
    (fake_weights / "tol_classes.json").write_bytes(b"[]")
    import classify_job as cj
    monkeypatch.setattr(cj, "get_active_model", lambda: {
        "id": "BioCLIP",
        "name": "BioCLIP",
        "model_str": "hf-hub:imageomics/bioclip",
        "weights_path": str(fake_weights),
        "model_type": "bioclip",
        "downloaded": True,
    })

    class _StopAfterRetry(RuntimeError):
        pass

    class _StubClassifier:
        """Behaves like the real constructor's embedding loop: steps down
        with ClassifierLoadPaused while a pause is pending, otherwise
        aborts the test run with a distinctive error."""

        def __init__(self, *args, pause_check=None, **kwargs):
            events.append("construct")
            if events.count("construct") == 1:
                # The user clicks Pause while embeddings are computing.
                runner.paused = True
            if pause_check is not None and pause_check():
                raise ClassifierLoadPaused("classifier load paused")
            raise _StopAfterRetry("stop after the post-resume construction")

    monkeypatch.setattr(cj, "Classifier", _StubClassifier)

    import classifier_cache

    def _tracking_acquire(*, factory, **kwargs):
        events.append("acquire")
        return factory()

    monkeypatch.setattr(
        classifier_cache, "acquire_cached_classifier", _tracking_acquire,
    )

    job = _make_job()
    params = ClassifyParams(
        collection_id=coll_id,
        labels_files=None,
        labels_file=None,
        model_id=None,
        model_name="BioCLIP",
        grouping_window=0,
        similarity_threshold=0.99,
        reclassify=False,
    )

    with pytest.raises(_StopAfterRetry):
        run_classify_job(job, runner, db_path, ws, params)

    assert events == [
        "acquire", "construct", "park", "acquire", "construct",
    ], (
        "expected: factory steps down for the pause, the job parks with the "
        "load lock released, then constructs again after Resume; "
        f"got {events}"
    )
    assert runner.paused is False
