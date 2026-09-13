import io
import os
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from computation_cache import (  # noqa: E402
    ArtifactStore,
    CacheFormatError,
    artifact_digest,
    canonical_bytes,
    classification_input,
    classifier_model_identity,
    exportable_artifacts,
    import_bundle,
    materialize_artifacts,
    materialize_local_store,
    promote_and_publish_classifier_run,
    read_bundle,
    runtime_fingerprint,
    sha256_file,
    source_input,
    validate_artifact,
    write_bundle,
)

PHOTO_HASH = "1" * 64
RUNTIME = runtime_fingerprint({
    "type": "detection",
    "model": "megadetector-v6",
    "weights_sha256": "2" * 64,
    "pipeline": "detector-v1",
})
CLASSIFIER_RUNTIME = runtime_fingerprint({
    "type": "classification",
    "model": "bioclip-2.5",
    "weights_sha256": "4" * 64,
    "labels_fingerprint": "3" * 64,
    "detector_runtime_fingerprint": RUNTIME,
})


def detection_artifact(subjects=None):
    input_block, input_fingerprint = source_input(
        PHOTO_HASH, "vireo-detector-source-v1",
    )
    return {
        "artifact_schema": 1,
        "type": "detection",
        "detector_model": "megadetector-v6",
        "photo_sha256": PHOTO_HASH,
        "runtime_fingerprint": RUNTIME,
        "input_fingerprint": input_fingerprint,
        "input": input_block,
        "completed": True,
        "subjects": subjects if subjects is not None else [{
            "key": "d0",
            "kind": "box",
            "box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
            "confidence": 0.9123456789,
            "category": "animal",
        }],
    }


def classification_artifact(candidates=None, classifier_runtime=None,
                            match=None):
    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    subjects = [{"key": "d0", "kind": "box", "box": box, "category": "animal"}]
    subjects[0]["candidates"] = candidates if candidates is not None else [{
        "species": "Robin",
        "confidence": 0.9,
    }]
    if match is not None:
        subjects[0]["match"] = match
    if classifier_runtime is None:
        classifier_runtime = runtime_fingerprint({
            "type": "classification",
            "model": "bioclip-2.5",
            "weights_sha256": "4" * 64,
            "labels_fingerprint": "3" * 64,
            "detector_runtime_fingerprint": RUNTIME,
        })
    input_block, input_fp = classification_input(PHOTO_HASH, RUNTIME, subjects)
    return {
        "artifact_schema": 1,
        "type": "classification",
        "classifier_model": "bioclip-2.5",
        "detector_model": "megadetector-v6",
        "detector_runtime_fingerprint": RUNTIME,
        "labels": {
            "fingerprint": "3" * 64,
            "short_fingerprint": ("3" * 12),
        },
        "photo_sha256": PHOTO_HASH,
        "runtime_fingerprint": classifier_runtime,
        "input_fingerprint": input_fp,
        "input": input_block,
        "completed": True,
        "subjects": subjects,
    }


def test_canonical_json_normalizes_negative_zero_and_rejects_nan():
    assert canonical_bytes({"z": -0.0, "a": 1}) == b'{"a":1,"z":0.0}'
    with pytest.raises(CacheFormatError, match="NaN"):
        canonical_bytes({"confidence": float("nan")})


def test_empty_detection_result_is_valid_completed_computation():
    artifact = detection_artifact(subjects=[])
    assert validate_artifact(artifact)["subjects"] == []


def test_megadetector_runtime_identity_includes_tiled_fallback(tmp_path, monkeypatch):
    import computation_cache
    import detector

    weights = tmp_path / "model.onnx"
    weights.write_bytes(b"model")
    baseline = computation_cache.megadetector_runtime_fingerprint(str(weights))

    monkeypatch.setattr(detector, "TILED_CROP_FRACTION", 0.55)
    changed = computation_cache.megadetector_runtime_fingerprint(str(weights))

    assert changed != baseline


def test_input_fingerprint_and_numeric_bounds_are_verified():
    artifact = detection_artifact()
    artifact["input_fingerprint"] = "9" * 64
    with pytest.raises(CacheFormatError, match="does not match"):
        validate_artifact(artifact)

    artifact = detection_artifact()
    artifact["subjects"][0]["box"]["x"] = -0.1
    with pytest.raises(CacheFormatError, match=r"in \[0, 1\]"):
        validate_artifact(artifact)

    artifact = detection_artifact()
    artifact["photo_sha256"] = "8" * 64
    with pytest.raises(CacheFormatError, match="source does not match photo"):
        validate_artifact(artifact)


def test_numeric_validators_reject_oversized_json_integers_cleanly():
    # A JSON int outside the C-double range makes math.isfinite raise
    # OverflowError; that used to escape the import route as a 500. Both
    # validators must convert it into a CacheFormatError so the HTTP
    # boundary returns a 400 with a readable message instead.
    huge = 10 ** 400
    artifact = detection_artifact()
    artifact["subjects"][0]["confidence"] = huge
    with pytest.raises(CacheFormatError, match=r"in \[0, 1\]"):
        validate_artifact(artifact)

    artifact = detection_artifact()
    artifact["subjects"][0]["box"]["x"] = huge
    with pytest.raises(CacheFormatError, match=r"in \[0, 1\]"):
        validate_artifact(artifact)


def test_materialize_picks_lowest_digest_when_lookup_identity_ties(tmp_path):
    # Two divergent detection artifacts sharing the same lookup identity
    # (photo × detector × runtime × input) must resolve to the same
    # winner regardless of manifest order, or two installs importing the
    # same object set would surface different boxes.  The tiebreak is
    # the lexicographically lowest ``artifact_digest``.
    destination, _folder_id, _photo_id = _database_with_photo(
        tmp_path / "destination.db", "dst.jpg",
    )
    a = detection_artifact(subjects=[{
        "key": "d0", "kind": "box",
        "box": {"x": 0.10, "y": 0.20, "w": 0.30, "h": 0.40},
        "confidence": 0.9, "category": "animal",
    }])
    b = detection_artifact(subjects=[{
        "key": "d0", "kind": "box",
        "box": {"x": 0.11, "y": 0.21, "w": 0.30, "h": 0.40},
        "confidence": 0.8, "category": "animal",
    }])
    # Same lookup identity fields:
    assert a["photo_sha256"] == b["photo_sha256"]
    assert a["detector_model"] == b["detector_model"]
    assert a["runtime_fingerprint"] == b["runtime_fingerprint"]
    assert a["input_fingerprint"] == b["input_fingerprint"]
    # Different content → different digest:
    assert artifact_digest(a) != artifact_digest(b)
    expected_box = (
        a["subjects"][0]["box"] if artifact_digest(a) < artifact_digest(b)
        else b["subjects"][0]["box"]
    )
    # Both orderings should install the same winner.
    for order in ([a, b], [b, a]):
        clean, _folder, _photo = _database_with_photo(
            tmp_path / f"dst-{order.index(a)}.db", "dst.jpg",
        )
        result = materialize_artifacts(
            clean, order, known_runtimes={RUNTIME},
        )
        assert result["detector_runs_applied"] == 1
        row = clean.conn.execute(
            "SELECT box_x, box_y FROM detections",
        ).fetchone()
        assert (row["box_x"], row["box_y"]) == (
            expected_box["x"], expected_box["y"],
        )


def test_artifact_store_is_content_addressed_atomic_and_idempotent(tmp_path):
    store = ArtifactStore(tmp_path / "store")
    artifact = detection_artifact()
    digest, created = store.put(artifact)
    assert created is True
    assert digest == artifact_digest(artifact)
    expected = store.object_path(artifact, digest)
    assert expected.read_bytes() == canonical_bytes(artifact)
    assert not list(expected.parent.glob(".incoming-*"))

    same_digest, created = store.put(artifact)
    assert same_digest == digest
    assert created is False
    assert store.stats() == {
        "object_count": 1,
        "total_bytes": len(canonical_bytes(artifact)),
    }


def test_bundle_round_trip_validates_before_publishing(tmp_path):
    artifact = detection_artifact()
    bundle = tmp_path / "results.vireo-cache"
    manifest = write_bundle(bundle, [artifact, artifact], device_label="Studio Mac")
    assert manifest["object_count"] == 1

    read_manifest, artifacts = read_bundle(bundle)
    assert read_manifest["device_label"] == "Studio Mac"
    assert artifacts == [artifact]

    store = ArtifactStore(tmp_path / "store")
    first = import_bundle(bundle, store)
    second = import_bundle(bundle, store)
    assert (first["added"], first["already_present"]) == (1, 0)
    assert (second["added"], second["already_present"]) == (0, 1)


def _rewrite_bundle(source, destination, transform):
    with zipfile.ZipFile(source) as archive:
        members = {info.filename: archive.read(info) for info in archive.infolist()}
    transform(members)
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)


def test_corrupt_bundle_publishes_nothing(tmp_path):
    good = tmp_path / "good.vireo-cache"
    bad = tmp_path / "bad.vireo-cache"
    write_bundle(good, [detection_artifact()])

    def corrupt(members):
        object_name = next(name for name in members if name.startswith("objects/"))
        members[object_name] = members[object_name] + b" "

    _rewrite_bundle(good, bad, corrupt)
    store = ArtifactStore(tmp_path / "store")
    with pytest.raises(CacheFormatError, match="size|digest"):
        import_bundle(bad, store)
    assert not (tmp_path / "store").exists()


def _second_detection_artifact():
    return detection_artifact(subjects=[{
        "key": "d0",
        "kind": "box",
        "box": {"x": 0.5, "y": 0.55, "w": 0.25, "h": 0.3},
        "confidence": 0.7654321,
        "category": "animal",
    }])


def test_multi_object_bundle_bad_late_object_publishes_nothing(tmp_path):
    good = tmp_path / "good.vireo-cache"
    bad = tmp_path / "bad.vireo-cache"
    write_bundle(good, [detection_artifact(), _second_detection_artifact()])

    def corrupt_last(members):
        object_names = sorted(name for name in members if name.startswith("objects/"))
        assert len(object_names) == 2
        members[object_names[-1]] = members[object_names[-1]] + b" "

    _rewrite_bundle(good, bad, corrupt_last)
    store = ArtifactStore(tmp_path / "store")
    with pytest.raises(CacheFormatError, match="size|digest"):
        import_bundle(bad, store)
    assert not (tmp_path / "store").exists()


def test_multi_object_bundle_bad_manifest_total_publishes_nothing(tmp_path):
    good = tmp_path / "good.vireo-cache"
    bad = tmp_path / "bad.vireo-cache"
    write_bundle(good, [detection_artifact(), _second_detection_artifact()])

    def bump_total(members):
        import json as _json
        manifest = _json.loads(members["manifest.json"])
        manifest["uncompressed_bytes"] = manifest["uncompressed_bytes"] + 1
        members["manifest.json"] = canonical_bytes(manifest)

    _rewrite_bundle(good, bad, bump_total)
    store = ArtifactStore(tmp_path / "store")
    with pytest.raises(CacheFormatError, match="byte total"):
        import_bundle(bad, store)
    assert not (tmp_path / "store").exists()


def test_bundle_rejects_traversal_and_undeclared_members(tmp_path):
    bundle = tmp_path / "unsafe.vireo-cache"
    manifest = {
        "format": "vireo-computation-cache",
        "format_version": 1,
        "created_at": "2026-08-08T00:00:00+00:00",
        "device_label": None,
        "object_count": 0,
        "uncompressed_bytes": 0,
        "objects": [],
    }
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("manifest.json", canonical_bytes(manifest))
        archive.writestr("../outside", b"bad")
    with pytest.raises(CacheFormatError, match="unsafe ZIP path"):
        read_bundle(bundle)
    assert not (tmp_path / "outside").exists()


def test_bundle_rejects_symlinks(tmp_path):
    bundle = tmp_path / "symlink.vireo-cache"
    manifest = {
        "format": "vireo-computation-cache",
        "format_version": 1,
        "created_at": "2026-08-08T00:00:00+00:00",
        "device_label": None,
        "object_count": 0,
        "uncompressed_bytes": 0,
        "objects": [],
    }
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("manifest.json", canonical_bytes(manifest))
        info = zipfile.ZipInfo("objects/link.json")
        info.create_system = 3
        info.external_attr = (0o120777 << 16)
        archive.writestr(info, b"target")
    with pytest.raises(CacheFormatError, match="symbolic links"):
        read_bundle(bundle)


def test_bundle_does_not_leak_source_paths(tmp_path):
    artifact = detection_artifact()
    bundle = tmp_path / "portable.vireo-cache"
    write_bundle(bundle, [artifact])
    body = bundle.read_bytes()
    assert os.fsencode("/Users/alice/Pictures") not in body
    assert b"IMG_0001.CR3" not in body


def _database_with_photo(path, filename, file_hash=PHOTO_HASH):
    from db import Database

    db = Database(str(path))
    folder_id = db.add_folder("/tmp/photos")
    workspace_id = db.create_workspace("Photos")
    db._active_workspace_id = workspace_id
    db.add_workspace_folder(workspace_id, folder_id)
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename=filename,
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
        file_hash=file_hash,
    )
    return db, folder_id, photo_id


def test_database_export_bundle_import_and_duplicate_fanout(tmp_path):
    source, _folder_id, source_photo = _database_with_photo(
        tmp_path / "source.db", "source.jpg",
    )
    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    detector_input, detector_input_fp = source_input(
        PHOTO_HASH, "vireo-detector-source-v1",
    )
    detection_id = source.write_detection_batch(
        source_photo,
        "megadetector-v6",
        [{"box": box, "confidence": 0.91, "category": "animal"}],
        runtime_fingerprint=RUNTIME,
        input_fingerprint=detector_input_fp,
    )[0]

    labels_full = "3" * 64
    labels_short = labels_full[:12]
    classifier_runtime = runtime_fingerprint({
        "type": "classification",
        "model": "bioclip-2.5",
        "weights_sha256": "4" * 64,
        "labels_fingerprint": labels_full,
        "detector_runtime_fingerprint": RUNTIME,
    })
    classifier_subjects = [{
        "key": "d0", "kind": "box", "box": box, "category": "animal",
    }]
    _classifier_input, classifier_input_fp = classification_input(
        PHOTO_HASH, RUNTIME, classifier_subjects,
    )
    source.add_prediction(
        detection_id,
        "European Robin",
        0.87,
        "bioclip-2.5",
        taxonomy={"scientific_name": "Erithacus rubecula", "taxon_id": 123},
        labels_fingerprint=labels_short,
        labels_fingerprint_full=labels_full,
    )
    source.record_classifier_run(
        detection_id,
        "bioclip-2.5",
        labels_short,
        prediction_count=1,
        labels_fingerprint_full=labels_full,
        runtime_fingerprint=classifier_runtime,
        input_fingerprint=classifier_input_fp,
    )
    source.upsert_labels_fingerprint(
        labels_short,
        "European birds",
        ["europe.txt"],
        1,
        full_fingerprint=labels_full,
    )

    artifacts, summary = exportable_artifacts(source)
    assert summary["detector_runs"] == 1
    assert summary["classifier_runs"] == 1
    assert {artifact["type"] for artifact in artifacts} == {
        "detection", "classification",
    }
    classification_only, _classification_summary = exportable_artifacts(
        source, artifact_types={"classification"},
    )
    assert {artifact["type"] for artifact in classification_only} == {
        "detection", "classification",
    }, "classification exports must include their detector dependency"

    bundle = tmp_path / "shared.vireo-cache"
    write_bundle(bundle, artifacts)
    store = ArtifactStore(tmp_path / "destination-store")
    imported = import_bundle(bundle, store)
    # ``import_bundle`` now streams the bundle through the store instead
    # of accumulating every artifact in Python, so it returns the trusted
    # runtime fingerprint sets rather than the parsed artifacts.  The
    # test still exercises the full plant pipeline by materializing from
    # the on-disk store — which is what the HTTP import route does too.
    assert imported["detector_runtimes"] == {RUNTIME}
    assert imported["classifier_runtimes"] == {CLASSIFIER_RUNTIME}

    destination, folder_id, first_photo = _database_with_photo(
        tmp_path / "destination.db", "renamed.jpg",
    )
    second_photo = destination.add_photo(
        folder_id=folder_id,
        filename="copy.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
    )
    # Exact duplicates are legitimate fan-out targets.  Set this directly so
    # the normal import-time duplicate resolver does not reject either fixture.
    destination.conn.execute(
        "UPDATE photos SET file_hash = ? WHERE id = ?",
        (PHOTO_HASH, second_photo),
    )
    destination.conn.commit()

    applied = materialize_local_store(
        destination, store=store,
        known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert applied["matched_photos"] == 2
    assert applied["detector_runs_applied"] == 2
    assert applied["classifier_runs_applied"] == 2
    for photo_id in (first_photo, second_photo):
        prediction = destination.conn.execute(
            """SELECT p.species, p.labels_fingerprint_full, p.source_taxon_id
               FROM predictions p
               JOIN detections d ON d.id = p.detection_id
               WHERE d.photo_id = ?""",
            (photo_id,),
        ).fetchone()
        assert dict(prediction) == {
            "species": "European Robin",
            "labels_fingerprint_full": labels_full,
            "source_taxon_id": 123,
        }
    assert destination.conn.execute(
        "SELECT COUNT(*) AS c FROM prediction_review",
    ).fetchone()["c"] == 0, "portable output must not transfer review state"

    repeat = materialize_local_store(
        destination, store=store,
        known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert repeat["detector_runs_applied"] == 0
    assert repeat["classifier_runs_applied"] == 0
    assert repeat["already_materialized"] == 4


def test_computation_cache_http_export_and_import(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    # Materialization refuses to plant detector rows carrying an unknown
    # runtime; monkeypatch the local fingerprint so the fake ``RUNTIME``
    # value the test fixture writes is treated as this install's runtime.
    import computation_cache

    monkeypatch.setattr(
        computation_cache, "megadetector_runtime_fingerprint",
        lambda *_args, **_kwargs: RUNTIME,
    )
    app.config["COMPUTATION_CACHE_DIR"] = str(tmp_path / "http-store")
    photo_id = db.conn.execute("SELECT id FROM photos ORDER BY id LIMIT 1").fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET file_hash = ? WHERE id = ?", (PHOTO_HASH, photo_id),
    )
    db.conn.commit()
    _input, input_fp = source_input(PHOTO_HASH, "vireo-detector-source-v1")
    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    db.write_detection_batch(
        photo_id,
        "megadetector-v6",
        [{"box": box, "confidence": 0.91, "category": "animal"}],
        runtime_fingerprint=RUNTIME,
        input_fingerprint=input_fp,
    )

    client = app.test_client()
    status = client.get("/api/computation-cache")
    assert status.status_code == 200
    assert status.get_json()["exportable"]["detector_runs"] == 1

    exported = client.get("/api/computation-cache/export?types=detection")
    assert exported.status_code == 200
    assert exported.headers["Content-Disposition"].endswith('.vireo-cache"')
    manifest, artifacts = read_bundle(io.BytesIO(exported.data))
    assert manifest["object_count"] == 1
    assert artifacts[0]["photo_sha256"] == PHOTO_HASH

    db.conn.execute("DELETE FROM detections WHERE photo_id = ?", (photo_id,))
    db.conn.execute("DELETE FROM detector_runs WHERE photo_id = ?", (photo_id,))
    db.conn.commit()
    imported = client.post(
        "/api/computation-cache/import",
        data={"file": (io.BytesIO(exported.data), "results.vireo-cache")},
        content_type="multipart/form-data",
    )
    assert imported.status_code == 200
    body = imported.get_json()
    assert body["added"] == 1
    assert body["detector_runs_applied"] == 1
    assert db.conn.execute(
        "SELECT runtime_fingerprint FROM detector_runs WHERE photo_id = ?",
        (photo_id,),
    ).fetchone()["runtime_fingerprint"] == RUNTIME


def test_classification_only_export_forwards_stored_detector_dependencies(
    app_and_db, tmp_path, monkeypatch,
):
    """``exportable_artifacts`` dependency-closes its output so classifier
    exports always drag their detector dependencies along.  The HTTP
    route must mirror that expansion for the local object store too —
    otherwise detector artifacts forwarded from ``iter_artifacts`` (e.g.
    imports whose photos were not yet cataloged when they landed) get
    dropped whenever the user picks only "Species classification", and a
    destination without the detector run has to reproduce detection from
    weights it may not have.
    """
    import computation_cache

    app, _db = app_and_db
    monkeypatch.setattr(
        computation_cache, "megadetector_runtime_fingerprint",
        lambda *_args, **_kwargs: RUNTIME,
    )
    store_dir = tmp_path / "http-store"
    app.config["COMPUTATION_CACHE_DIR"] = str(store_dir)

    # Plant a detection artifact directly in the local object store — this
    # is exactly the shape ``iter_artifacts`` returns for objects imported
    # before their photos were cataloged.
    store = computation_cache.ArtifactStore(store_dir)
    detection = detection_artifact()
    store.put(detection)

    response = app.test_client().get(
        "/api/computation-cache/export?types=classification",
    )
    assert response.status_code == 200
    _manifest, artifacts = read_bundle(io.BytesIO(response.data))
    detection_types = [a["type"] for a in artifacts if a["type"] == "detection"]
    assert detection_types, (
        "classification-only export must forward detector artifacts from the "
        "local object store so the destination can materialize the classifier "
        "runs without loading detector weights"
    )


def test_computation_cache_http_rejects_invalid_bundle(app_and_db, tmp_path):
    app, _db = app_and_db
    store_root = tmp_path / "http-store"
    app.config["COMPUTATION_CACHE_DIR"] = str(store_root)
    response = app.test_client().post(
        "/api/computation-cache/import",
        data={"file": (io.BytesIO(b"not a zip"), "bad.vireo-cache")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert not store_root.exists()


def test_fresh_classifier_run_is_promoted_and_published(tmp_path):
    source, _folder_id, photo_id = _database_with_photo(
        tmp_path / "source.db", "source.jpg",
    )
    _input, detector_input_fp = source_input(
        PHOTO_HASH, "vireo-detector-source-v1",
    )
    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    detection_id = source.write_detection_batch(
        photo_id,
        "megadetector-v6",
        [{"box": box, "confidence": 0.91, "category": "animal"}],
        runtime_fingerprint=RUNTIME,
        input_fingerprint=detector_input_fp,
    )[0]
    labels_full = "5" * 64
    labels_short = labels_full[:12]
    source.upsert_labels_fingerprint(
        labels_short, "Test birds", [], 1, full_fingerprint=labels_full,
    )
    source.add_prediction(
        detection_id, "Robin", 0.92, "BioCLIP",
        labels_fingerprint=labels_short,
    )
    source.record_classifier_run(
        detection_id, "BioCLIP", labels_short, prediction_count=1,
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
    store = ArtifactStore(tmp_path / "store")
    digest = promote_and_publish_classifier_run(
        source,
        detection_id,
        "BioCLIP",
        labels_short,
        labels_full,
        identity,
        store=store,
    )
    assert len(digest) == 64
    run = source.conn.execute(
        """SELECT labels_fingerprint_full, runtime_fingerprint,
                  input_fingerprint
           FROM classifier_runs WHERE detection_id = ?""",
        (detection_id,),
    ).fetchone()
    assert run["labels_fingerprint_full"] == labels_full
    assert len(run["runtime_fingerprint"]) == 64
    assert len(run["input_fingerprint"]) == 64
    stored = list(store.iter_artifacts())
    assert len(stored) == 1
    assert stored[0][1]["type"] == "classification"
    assert stored[0][1]["subjects"][0]["candidates"][0]["species"] == "Robin"

    artifacts, summary = exportable_artifacts(source)
    assert summary["classifier_runs"] == 1
    assert any(item["type"] == "classification" for item in artifacts)


def test_evicted_working_copy_classifier_run_stays_local_only(tmp_path):
    """A photo with a ``working_copy_path`` was classified from the
    extracted working-copy JPEG (see ``classify_job._prepare_image``
    when ``vireo_dir`` is set), not from the original bytes. Even if quota
    eviction later clears that mutable catalog path, the v1
    input identity carries only ``p.file_hash``, so publishing that run
    would advertise a working-copy-derived prediction as if it came from
    the original — a foreign install (or the same install with a
    different working copy) would then materialize predictions computed
    on different pixels. The publisher must decline these runs.
    """
    source, _folder_id, photo_id = _database_with_photo(
        tmp_path / "source.db", "source.jpg",
    )
    source.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        (str(tmp_path / "wc" / "source.jpg"), photo_id),
    )
    source.conn.commit()

    _input, detector_input_fp = source_input(
        PHOTO_HASH, "vireo-detector-source-v1",
    )
    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    detection_id = source.write_detection_batch(
        photo_id,
        "megadetector-v6",
        [{"box": box, "confidence": 0.91, "category": "animal"}],
        runtime_fingerprint=RUNTIME,
        input_fingerprint=detector_input_fp,
    )[0]
    labels_full = "5" * 64
    labels_short = labels_full[:12]
    source.upsert_labels_fingerprint(
        labels_short, "Test birds", [], 1, full_fingerprint=labels_full,
    )
    source.add_prediction(
        detection_id, "Robin", 0.92, "BioCLIP",
        labels_fingerprint=labels_short,
    )
    source.record_classifier_run(
        detection_id, "BioCLIP", labels_short, prediction_count=1,
    )

    # Quota eviction wins after inference but before delayed promotion.
    source.conn.execute(
        "UPDATE photos SET working_copy_path = NULL WHERE id = ?",
        (photo_id,),
    )
    source.conn.commit()

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
    store = ArtifactStore(tmp_path / "store")
    digest = promote_and_publish_classifier_run(
        source, detection_id, "BioCLIP", labels_short,
        labels_full, identity, store=store, source_is_original=False,
    )
    assert digest is None, (
        "working-copy-backed runs must not publish — pixels differ from "
        "the original bytes the v1 input identity advertises"
    )
    run = source.conn.execute(
        "SELECT runtime_fingerprint FROM classifier_runs "
        "WHERE detection_id = ?", (detection_id,),
    ).fetchone()
    assert run["runtime_fingerprint"] == "legacy", (
        "declined publish must leave the classifier_runs row on 'legacy' "
        "so exportable_artifacts continues to skip it"
    )
    assert list(store.iter_artifacts()) == []


def test_companion_backed_destination_photo_is_not_materialized(tmp_path):
    """A v1 artifact declares only the original ``photo_sha256``.  When
    the destination catalog has a photo with the same original file_hash
    but a ``companion_path`` (RAW+JPEG), Vireo processes the companion
    rendition — different pixels — so applying the artifact would install
    detections/classifications for a rendition the sender did not compute
    on.  ``materialize_artifacts`` must skip companion-backed rows and
    leave the object stored-unmatched instead.
    """
    destination, folder_id, plain_photo_id = _database_with_photo(
        tmp_path / "destination.db", "plain.jpg",
    )
    # Skip the auto-duplicate resolver (which would reject one of the
    # two rows) by setting the shared hash via UPDATE after insert —
    # the scanner path uses the same shape.
    companion_photo_id = destination.add_photo(
        folder_id=folder_id,
        filename="raw-plus-jpeg.cr2",
        extension=".cr2",
        file_size=2048,
        file_mtime=2.0,
    )
    destination.conn.execute(
        "UPDATE photos SET companion_path = ?, file_hash = ? WHERE id = ?",
        ("/tmp/photos/raw-plus-jpeg.jpg", PHOTO_HASH, companion_photo_id),
    )
    destination.conn.commit()

    result = materialize_artifacts(
        destination, [detection_artifact()],
        known_runtimes={RUNTIME},
    )
    assert result["detector_runs_applied"] == 1, (
        "the plain (non-companion) sibling must still receive the artifact"
    )

    applied_photo_ids = [
        row["photo_id"] for row in destination.conn.execute(
            "SELECT photo_id FROM detector_runs "
            "WHERE detector_model = 'megadetector-v6' ORDER BY photo_id"
        ).fetchall()
    ]
    assert applied_photo_ids == [plain_photo_id], (
        "companion-backed photo must be excluded from materialization even "
        "when it shares the original file_hash — its rendition can differ"
    )


def test_working_copy_backed_destination_photo_is_not_materialized(tmp_path):
    """A v1 artifact identifies only the original bytes, so it cannot be
    applied to a destination that classifies a derived working-copy JPEG.
    """
    destination, folder_id, plain_photo_id = _database_with_photo(
        tmp_path / "destination.db", "plain.jpg",
    )
    working_copy_photo_id = destination.add_photo(
        folder_id=folder_id,
        filename="edited.jpg",
        extension=".jpg",
        file_size=2048,
        file_mtime=2.0,
    )
    destination.conn.execute(
        "UPDATE photos SET working_copy_path = ?, file_hash = ? WHERE id = ?",
        ("working/edited.jpg", PHOTO_HASH, working_copy_photo_id),
    )
    destination.conn.commit()

    result = materialize_artifacts(
        destination, [detection_artifact()],
        known_runtimes={RUNTIME},
    )
    assert result["detector_runs_applied"] == 1
    applied_photo_ids = [
        row["photo_id"] for row in destination.conn.execute(
            "SELECT photo_id FROM detector_runs "
            "WHERE detector_model = 'megadetector-v6' ORDER BY photo_id"
        ).fetchall()
    ]
    assert applied_photo_ids == [plain_photo_id], (
        "working-copy-backed photos must be excluded because the artifact "
        "does not identify the derived rendition's pixels"
    )


def test_tree_of_life_sentinel_short_fingerprint_is_accepted():
    """Tree-of-Life classification uses the well-known ``tol`` sentinel as
    the classifier_runs short key (compute_fingerprint([]) → "tol") while
    classify/pipeline jobs synthesize a portable 64-char labels.fingerprint
    from the classifier identity. The digest-prefix rule that guards other
    label sets must not reject this synthetic identity — otherwise
    ``promote_and_publish_classifier_run`` marks the row portable and then
    fails to publish, and a later default export returns 400.
    """
    from labels_fingerprint import TOL_SENTINEL

    artifact = classification_artifact()
    artifact["labels"]["fingerprint"] = "a" * 64
    artifact["labels"]["short_fingerprint"] = TOL_SENTINEL
    assert (
        validate_artifact(artifact)["labels"]["short_fingerprint"]
        == TOL_SENTINEL
    )


def test_non_tol_short_fingerprint_that_does_not_match_full_is_rejected():
    """The TOL exemption must not weaken the guard for arbitrary short keys.
    A crafted bundle claiming ``short_fingerprint="legacy"`` — which
    matches the historical placeholder for pre-migration rows — with a
    mismatched full digest would still be able to replace unreviewed
    predictions on a legacy row under a foreign label set.
    """
    artifact = classification_artifact()
    artifact["labels"]["fingerprint"] = "b" * 64
    artifact["labels"]["short_fingerprint"] = "legacy"
    with pytest.raises(CacheFormatError, match="first 12 chars"):
        validate_artifact(artifact)


def test_labels_count_out_of_sqlite_int64_range_is_rejected():
    """A bundle carrying an arbitrarily large JSON integer for
    ``labels.count`` (e.g. 10**100) would pass a naive non-negativity
    check and only fail with ``OverflowError`` when materialization binds
    the value to a SQLite integer column — leaving the bundle object
    already published in the local store.  Reject out-of-range values
    up front instead.
    """
    artifact = classification_artifact()
    artifact["labels"]["count"] = 10 ** 100
    with pytest.raises(CacheFormatError, match="SQLite"):
        validate_artifact(artifact)

    # Boundary check: exactly 2**63 - 1 (the SQLite signed int64 max)
    # is accepted; one past it is rejected.
    artifact["labels"]["count"] = (1 << 63) - 1
    assert validate_artifact(artifact)["labels"]["count"] == (1 << 63) - 1
    artifact["labels"]["count"] = 1 << 63
    with pytest.raises(CacheFormatError, match="SQLite"):
        validate_artifact(artifact)


def test_classification_box_category_shape_is_validated_before_publishing():
    """A classification box subject may omit ``category`` (materialize falls
    back to ``"animal"``) but a non-string value must be rejected up
    front. ``compute_detection_id`` feeds category to ``positive_int_hash``
    which calls ``len()`` on it; a bool/int/None would raise ``TypeError``
    mid-materialization AFTER the bundle object had already been
    published, leaving an unmaterialized entry behind and returning 500
    to the import route. ``classification_input`` hashes only key/kind/box,
    so mutating category after construction keeps the input_fingerprint
    valid — the shape check has to run before the type check would ever
    fire.
    """
    for bad in (True, False, 42, None, [], {"x": 1}):
        artifact = classification_artifact()
        artifact["subjects"][0]["category"] = bad
        with pytest.raises(CacheFormatError, match="category"):
            validate_artifact(artifact)

    # An empty string or overly long value is also rejected — the field
    # is meant to carry a short taxonomic label like ``"animal"``.
    artifact = classification_artifact()
    artifact["subjects"][0]["category"] = ""
    with pytest.raises(CacheFormatError, match="category"):
        validate_artifact(artifact)
    artifact = classification_artifact()
    artifact["subjects"][0]["category"] = "x" * 200
    with pytest.raises(CacheFormatError, match="category"):
        validate_artifact(artifact)

    # Omission is allowed — materialize supplies the default.
    artifact = classification_artifact()
    del artifact["subjects"][0]["category"]
    assert "category" not in validate_artifact(artifact)["subjects"][0]


def test_taxonomy_field_shape_is_validated_before_publishing():
    artifact = classification_artifact(candidates=[{
        "species": "Robin",
        "confidence": 0.9,
        "taxonomy": ["genus", "Erithacus"],
    }])
    with pytest.raises(CacheFormatError, match="taxonomy must be an object"):
        validate_artifact(artifact)

    artifact = classification_artifact(candidates=[{
        "species": "Robin",
        "confidence": 0.9,
        "taxonomy": {"genus": ["Erithacus"]},
    }])
    with pytest.raises(CacheFormatError, match="taxonomy field 'genus'"):
        validate_artifact(artifact)

    # A valid taxonomy — omitted or all-string fields — passes through.
    ok = classification_artifact(candidates=[{
        "species": "Robin",
        "confidence": 0.9,
        "taxonomy": {"genus": "Erithacus", "family": "Muscicapidae"},
    }])
    assert validate_artifact(ok)["subjects"][0]["candidates"][0]["taxonomy"] == {
        "genus": "Erithacus", "family": "Muscicapidae",
    }


def test_unknown_detector_runtime_is_quarantined_not_installed(tmp_path):
    destination, _folder_id, photo_id = _database_with_photo(
        tmp_path / "destination.db", "photo.jpg",
    )
    # Neither the built-in ``full-image`` synthetic runtime nor the
    # currently-installed MegaDetector fingerprint matches RUNTIME, so
    # ``materialize_artifacts`` (without an override) must decline to plant
    # arbitrary-runtime detections in the catalog.
    applied = materialize_artifacts(destination, [detection_artifact()])
    assert applied["unknown_runtime"] == 1
    assert applied["detector_runs_applied"] == 0
    assert destination.conn.execute(
        "SELECT COUNT(*) AS c FROM detector_runs WHERE photo_id = ?",
        (photo_id,),
    ).fetchone()["c"] == 0

    # A caller that recognizes the runtime (e.g. after weights install)
    # can pass it explicitly and the same artifact then materializes.
    applied_ok = materialize_artifacts(
        destination, [detection_artifact()], known_runtimes={RUNTIME},
    )
    assert applied_ok["detector_runs_applied"] == 1
    assert applied_ok["unknown_runtime"] == 0


def test_materialize_selects_single_detection_per_photo_and_model(tmp_path):
    """When the local store holds trusted detection artifacts from
    multiple runtimes for the same (photo, detector_model), materialize
    must NOT feed them all through ``write_detection_batch`` in
    sequence.  Each runtime change deletes the previously selected
    detections and cascades their unreviewed predictions; the settled
    winner would then depend on iteration order rather than on which
    runtime the catalog already has installed.  Materialize must pick
    one candidate per logical run so re-application is a no-op and
    routine reapply cannot churn detections.
    """
    destination, _folder_id, photo_id = _database_with_photo(
        tmp_path / "destination.db", "photo.jpg",
    )

    other_runtime = runtime_fingerprint({
        "type": "detection",
        "model": "megadetector-v6",
        "weights_sha256": "9" * 64,
        "pipeline": "detector-v1",
    })
    other = detection_artifact()
    other["runtime_fingerprint"] = other_runtime

    # First pass: no existing detector_runs, both runtimes trusted.
    # Expect exactly one detector_runs row (not two writes that churn).
    first = materialize_artifacts(
        destination, [detection_artifact(), other],
        known_runtimes={RUNTIME, other_runtime},
    )
    assert first["detector_runs_applied"] == 1
    installed = destination.conn.execute(
        """SELECT runtime_fingerprint FROM detector_runs
           WHERE photo_id = ? AND detector_model = 'megadetector-v6'""",
        (photo_id,),
    ).fetchone()
    assert installed is not None
    installed_runtime = installed["runtime_fingerprint"]
    assert installed_runtime in {RUNTIME, other_runtime}

    # Second pass with the same inputs: the catalog now has an existing
    # runtime; materialize must PREFER the matching artifact so the
    # write is an idempotent no-op rather than a churn between runtimes.
    second = materialize_artifacts(
        destination, [detection_artifact(), other],
        known_runtimes={RUNTIME, other_runtime},
    )
    assert second["detector_runs_applied"] == 0
    assert second["already_materialized"] == 1
    still_installed = destination.conn.execute(
        """SELECT runtime_fingerprint FROM detector_runs
           WHERE photo_id = ? AND detector_model = 'megadetector-v6'""",
        (photo_id,),
    ).fetchone()["runtime_fingerprint"]
    assert still_installed == installed_runtime, (
        "Re-materialization must not swap runtimes; the loop was "
        "churning through every candidate again."
    )


def test_classification_deferred_until_detector_run_available(tmp_path):
    destination, _folder_id, photo_id = _database_with_photo(
        tmp_path / "destination.db", "photo.jpg",
    )
    labels_short = "3" * 12
    destination.upsert_labels_fingerprint(
        labels_short, "Test labels", [], 1, full_fingerprint="3" * 64,
    )
    # Only the classification artifact is present — no detector artifact
    # was exported.  materialize_artifacts must count it as deferred (not
    # applied) since the detector_run dependency does not exist yet.
    only_classification = [classification_artifact()]
    deferred = materialize_artifacts(
        destination, only_classification,
        known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert deferred["classifier_runs_applied"] == 0
    assert deferred["classifier_deferred_pending_detection"] >= 1

    # Once the detector dependency is materialized (or produced locally
    # by a subsequent detection run), reapplying picks the classifier up.
    together = materialize_artifacts(
        destination,
        [detection_artifact(), classification_artifact()],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert together["detector_runs_applied"] == 1
    assert together["classifier_runs_applied"] == 1


def test_unknown_classifier_runtime_is_quarantined_not_installed(tmp_path):
    """A classification artifact whose classifier runtime this install
    cannot reproduce must stay quarantined in the object store — its
    predictions must not surface as authoritative results until a
    matching classifier is verified locally or the caller explicitly
    trusts the runtime.
    """
    destination, _folder_id, _photo_id = _database_with_photo(
        tmp_path / "destination.db", "photo.jpg",
    )
    destination.upsert_labels_fingerprint(
        "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
    )

    # Detector runtime is recognized (via known_runtimes), but no
    # classifier runtime is trusted and no local classifier can
    # reproduce the artifact's runtime_fingerprint. The classification
    # must be counted as unknown_classifier_runtime and NOT applied.
    applied = materialize_artifacts(
        destination,
        [detection_artifact(), classification_artifact()],
        known_runtimes={RUNTIME},
    )
    assert applied["detector_runs_applied"] == 1
    assert applied["classifier_runs_applied"] == 0
    assert applied["unknown_classifier_runtime"] == 1
    assert destination.conn.execute(
        "SELECT COUNT(*) AS c FROM predictions",
    ).fetchone()["c"] == 0

    # A caller that has verified the classifier through some other
    # channel (a classify job that just resolved its own runtime, an
    # install-time model registry) can pass it explicitly and the
    # same artifact then materializes.
    applied_ok = materialize_artifacts(
        destination,
        [detection_artifact(), classification_artifact()],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert applied_ok["classifier_runs_applied"] == 1
    assert applied_ok["unknown_classifier_runtime"] == 0


def test_artifact_store_persists_trusted_runtimes(tmp_path):
    """Trust recorded on import survives store re-instantiation and merges."""
    store = ArtifactStore(tmp_path / "cache")
    assert store.trusted_runtimes() == (set(), set())
    store.record_trusted_runtimes(
        detector_runtimes={RUNTIME},
        classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    # Fresh instance reads back the same on-disk trust — this is what
    # subsequent classify_job / pipeline calls rely on.
    reread = ArtifactStore(tmp_path / "cache")
    det, clf = reread.trusted_runtimes()
    assert det == {RUNTIME}
    assert clf == {CLASSIFIER_RUNTIME}

    # A second record merges rather than replacing.
    other_runtime = "a" * 64
    reread.record_trusted_runtimes(detector_runtimes={other_runtime})
    det, clf = ArtifactStore(tmp_path / "cache").trusted_runtimes()
    assert det == {RUNTIME, other_runtime}
    assert clf == {CLASSIFIER_RUNTIME}

    # Malformed fingerprints are silently dropped — record_trusted_runtimes
    # only persists lowercase SHA-256s so a corrupted trust.json can never
    # widen the whitelist to arbitrary strings.
    reread.record_trusted_runtimes(detector_runtimes={"not-a-hash"})
    det, _ = ArtifactStore(tmp_path / "cache").trusted_runtimes()
    assert "not-a-hash" not in det


def test_materialize_local_store_honors_persisted_trust(tmp_path):
    """A bundle imported before the photo exists still lands on rescan."""
    destination, _folder_id, _photo_id = _database_with_photo(
        tmp_path / "destination.db", "photo.jpg",
    )
    destination.upsert_labels_fingerprint(
        "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
    )
    store = ArtifactStore(tmp_path / "cache")
    # Publish detector + classifier artifacts into the store as an import
    # would, then record the trust decision the API endpoint takes.
    for artifact in (detection_artifact(), classification_artifact()):
        store.put(artifact)
    store.record_trusted_runtimes(
        detector_runtimes={RUNTIME},
        classifier_runtimes={CLASSIFIER_RUNTIME},
    )

    # No per-call known_runtimes supplied — mirrors the run_classify_job /
    # pipeline_job flow where nothing about the bundle's trust survived
    # the original import HTTP call.
    applied = materialize_local_store(destination, store=store)
    assert applied["detector_runs_applied"] == 1
    assert applied["classifier_runs_applied"] == 1

    # Without persisted trust, the same store would have quarantined both.
    fresh_store = ArtifactStore(tmp_path / "cache-untrusted")
    for artifact in (detection_artifact(), classification_artifact()):
        fresh_store.put(artifact)
    destination2, _f, _p = _database_with_photo(
        tmp_path / "destination2.db", "photo.jpg",
    )
    destination2.upsert_labels_fingerprint(
        "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
    )
    untrusted = materialize_local_store(destination2, store=fresh_store)
    assert untrusted["detector_runs_applied"] == 0
    assert untrusted["classifier_runs_applied"] == 0


def test_sha256_file_retains_hashes_across_paths(tmp_path):
    """Hashing one custom-model file must not evict cached hashes for other
    files in the same directory. Prior to this regression check, the cache
    was cleared on every miss, forcing each multi-file custom model's
    ONNX sidecars to be re-read on every identity computation.
    """
    import computation_cache

    a = tmp_path / "image_encoder.onnx"
    b = tmp_path / "text_encoder.onnx"
    a.write_bytes(b"aaa")
    b.write_bytes(b"bbb")

    computation_cache._FILE_DIGEST_CACHE.clear()
    hash_a = sha256_file(str(a))
    hash_b = sha256_file(str(b))

    stat_a = os.stat(str(a))
    key_a = (os.path.abspath(str(a)), stat_a.st_size, stat_a.st_mtime_ns)
    stat_b = os.stat(str(b))
    key_b = (os.path.abspath(str(b)), stat_b.st_size, stat_b.st_mtime_ns)

    assert computation_cache._FILE_DIGEST_CACHE[key_a] == hash_a
    assert computation_cache._FILE_DIGEST_CACHE[key_b] == hash_b


def test_sha256_file_evicts_stale_entries_for_the_same_path(tmp_path):
    """A rewritten file with a new size/mtime must supersede its stale
    cache entry; unrelated paths must survive.
    """
    import computation_cache

    a = tmp_path / "weights.onnx"
    b = tmp_path / "text_features.pt"
    a.write_bytes(b"v1")
    b.write_bytes(b"unchanged")

    computation_cache._FILE_DIGEST_CACHE.clear()
    sha256_file(str(a))
    sha256_file(str(b))

    stat_b_before = os.stat(str(b))
    key_b = (
        os.path.abspath(str(b)), stat_b_before.st_size, stat_b_before.st_mtime_ns,
    )

    # Rewrite ``a`` so its stat changes; ``sha256_file`` must evict the
    # stale ``a`` entry but keep the entry for ``b``.
    a.write_bytes(b"v2-longer")
    os.utime(str(a), (stat_b_before.st_mtime + 1, stat_b_before.st_mtime + 1))
    new_hash_a = sha256_file(str(a))

    stat_a_after = os.stat(str(a))
    key_a_new = (
        os.path.abspath(str(a)), stat_a_after.st_size, stat_a_after.st_mtime_ns,
    )
    assert computation_cache._FILE_DIGEST_CACHE.get(key_a_new) == new_hash_a
    assert key_b in computation_cache._FILE_DIGEST_CACHE

    # Old key for ``a`` should be gone.
    stale_a_keys = [
        k for k in computation_cache._FILE_DIGEST_CACHE
        if k[0] == os.path.abspath(str(a)) and k != key_a_new
    ]
    assert stale_a_keys == []


def test_http_import_then_classify_job_uses_configured_cache_dir(
    app_and_db, tmp_path, monkeypatch,
):
    """When an operator overrides ``COMPUTATION_CACHE_DIR``, an HTTP-imported
    bundle whose photos are cataloged only later must still plant on the
    background classify job's next materialize call. Prior to the fix,
    ``run_classify_job``'s pre-model ``materialize_local_store(thread_db)``
    fell back to the default ``~/.vireo/computation-cache`` and never saw
    the imported artifact, so catalog-later reuse silently failed outside
    the default configuration.
    """
    import computation_cache

    app, db = app_and_db
    monkeypatch.setattr(
        computation_cache, "megadetector_runtime_fingerprint",
        lambda *_args, **_kwargs: RUNTIME,
    )
    # Point the default anywhere BUT the overridden store so a regression
    # (background job falling back to the default) can't accidentally
    # hit the same artifact via the wrong path.
    monkeypatch.setattr(
        computation_cache, "DEFAULT_CACHE_DIR",
        str(tmp_path / "would-be-wrong-default"),
    )

    store_dir = tmp_path / "http-store"
    app.config["COMPUTATION_CACHE_DIR"] = str(store_dir)

    # HTTP-import a detection bundle into the overridden store. Do this
    # BEFORE the photo is cataloged with its file hash so materialize
    # can't plant during the import — the background classify call is
    # the one that has to find the artifact.
    exported_store = ArtifactStore(tmp_path / "source")
    exported_store.put(detection_artifact())
    exported_artifacts = [
        artifact for _digest, artifact
        in (exported_store.iter_artifacts() or ())
    ]
    bundle_path = tmp_path / "bundle.vireo-cache"
    write_bundle(bundle_path, exported_artifacts)
    client = app.test_client()
    with open(bundle_path, "rb") as handle:
        response = client.post(
            "/api/computation-cache/import",
            data={"file": (handle, "bundle.vireo-cache")},
            content_type="multipart/form-data",
        )
    assert response.status_code == 200
    # The import happened before the photo carried a matching file_hash,
    # so nothing planted yet — the artifact is sitting in the overridden
    # store waiting for a later job to find it.
    assert response.get_json()["detector_runs_applied"] == 0

    photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1",
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET file_hash = ? WHERE id = ?", (PHOTO_HASH, photo_id),
    )
    db.conn.commit()

    # Directly exercise the background job's cache-reapply plumbing:
    # ArtifactStore(configured_dir) must find the imported artifact.
    reapply_store = ArtifactStore(str(store_dir))
    reapplied = materialize_local_store(db, store=reapply_store)
    assert reapplied["detector_runs_applied"] == 1

    # Confirm the default location was empty — if the background job had
    # ignored the override, this is the only place its materialize would
    # have looked, and reuse would have silently failed.
    default_store = ArtifactStore(computation_cache.DEFAULT_CACHE_DIR)
    assert list(default_store.iter_artifacts() or ()) == []


@pytest.mark.parametrize("reviewed", [False, True])
def test_materialize_handles_predictions_without_classifier_run(tmp_path, reviewed):
    db, _, _ = _database_with_photo(tmp_path / "orphan.db", "photo.jpg")
    materialize_artifacts(db, [detection_artifact()], known_runtimes={RUNTIME})
    detection_id = db.conn.execute("SELECT id FROM detections").fetchone()[0]
    db.add_prediction(detection_id, "Robin", .5, "bioclip-2.5",
                      status="accepted" if reviewed else "pending", labels_fingerprint="3" * 12,
                      taxonomy={"scientific_name": "Old species"})
    assert db.conn.execute("SELECT count(*) FROM classifier_runs").fetchone()[0] == 0
    artifact = classification_artifact(candidates=[{"species": "Robin", "confidence": .9,
                                                  "taxonomy": {"scientific_name": "Erithacus rubecula", "taxon_id": 123}}])
    result = materialize_artifacts(db, [artifact], known_runtimes={RUNTIME},
                                   known_classifier_runtimes={CLASSIFIER_RUNTIME})
    row = db.conn.execute("SELECT scientific_name, source_taxon_id, confidence FROM predictions").fetchone()
    if reviewed:
        assert tuple(row) == ("Old species", None, .5)
        assert result["pinned_older_runtime"] == 1
        assert db.conn.execute("SELECT count(*) FROM classifier_runs").fetchone()[0] == 0
        assert db.conn.execute("SELECT status FROM prediction_review").fetchone()[0] == "accepted"
    else:
        assert tuple(row) == ("Erithacus rubecula", 123, .9)
        assert result["classifier_runs_applied"] == 1
    db.close()


def _materialized_classification(tmp_path, name, **artifact_kwargs):
    """Materialize one classification artifact and hand back the catalog."""
    destination, _folder_id, photo_id = _database_with_photo(
        tmp_path / name, "photo.jpg",
    )
    destination.upsert_labels_fingerprint(
        "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
    )
    applied = materialize_artifacts(
        destination,
        [detection_artifact(), classification_artifact(**artifact_kwargs)],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={CLASSIFIER_RUNTIME},
    )
    assert applied["classifier_runs_applied"] == 1
    return destination, photo_id


def test_classification_artifact_carries_match_strength(tmp_path):
    """Cache-materialized predictions must keep their absolute match score.

    ``classifier_runs`` is the re-classification gate, so a detection that
    arrives from the cache is never inferred again locally. If the artifact
    dropped the score, those photos would report "match strength not recorded"
    forever despite originating from real inference — and the calibration
    script would silently lose exactly the rows it derives a floor from.
    """
    destination, _photo_id = _materialized_classification(
        tmp_path, "with-match.db",
        candidates=[{"species": "Robin", "confidence": 0.9,
                     "match_score": 0.31}],
        match={"max_match_score": 0.31, "match_margin": 0.04,
               "top_species": "Robin", "label_count": 1255,
               "score_kind": "cosine"},
    )
    assert destination.conn.execute(
        "SELECT match_score FROM predictions",
    ).fetchone()["match_score"] == 0.31
    row = destination.conn.execute(
        "SELECT * FROM classifier_match_scores",
    ).fetchone()
    assert row["max_match_score"] == 0.31
    assert row["match_margin"] == 0.04
    assert row["top_species"] == "Robin"
    assert row["label_count"] == 1255
    assert row["score_kind"] == "cosine"


def test_artifact_without_match_strength_materializes_as_not_recorded(tmp_path):
    """Pre-feature artifacts must land as NULL, never as a zero score.

    Zero is a measurement — "the best label in the list matched nothing" — and
    an artifact that never carried the field made no such measurement. The
    fields are additive and optional at schema 1 precisely so old objects stay
    valid; they must also stay honest.
    """
    destination, _photo_id = _materialized_classification(
        tmp_path, "no-match.db",
    )
    assert destination.conn.execute(
        "SELECT match_score FROM predictions",
    ).fetchone()["match_score"] is None
    assert destination.conn.execute(
        "SELECT COUNT(*) AS c FROM classifier_match_scores",
    ).fetchone()["c"] == 0


def test_match_strength_survives_publish_and_export(tmp_path):
    """The publisher must read the score out of the catalog, not drop it."""
    source, _folder_id, photo_id = _database_with_photo(
        tmp_path / "source.db", "source.jpg",
    )
    _input, detector_input_fp = source_input(
        PHOTO_HASH, "vireo-detector-source-v1",
    )
    box = {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4}
    detection_id = source.write_detection_batch(
        photo_id,
        "megadetector-v6",
        [{"box": box, "confidence": 0.91, "category": "animal"}],
        runtime_fingerprint=RUNTIME,
        input_fingerprint=detector_input_fp,
    )[0]
    labels_full = "5" * 64
    labels_short = labels_full[:12]
    source.upsert_labels_fingerprint(
        labels_short, "Test birds", [], 1, full_fingerprint=labels_full,
    )
    source.add_prediction(
        detection_id, "Robin", 0.92, "BioCLIP",
        labels_fingerprint=labels_short, match_score=0.31,
    )
    source.record_classifier_run(
        detection_id, "BioCLIP", labels_short, prediction_count=1,
    )
    source.record_classifier_match_score(
        detection_id, "BioCLIP", labels_short, max_match_score=0.31,
        match_margin=0.04, top_species="Robin", label_count=1255,
        score_kind="cosine",
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
    store = ArtifactStore(tmp_path / "store")
    promote_and_publish_classifier_run(
        source, detection_id, "BioCLIP", labels_short, labels_full,
        identity, store=store,
    )
    published = [a for _digest, a in store.iter_artifacts()]
    assert len(published) == 1
    subject = published[0]["subjects"][0]
    assert subject["candidates"][0]["match_score"] == 0.31
    assert subject["match"]["max_match_score"] == 0.31
    assert subject["match"]["score_kind"] == "cosine"
    # run_at is local bookkeeping; carrying it would change the
    # content-addressed digest of an otherwise identical run.
    assert "run_at" not in subject["match"]

    artifacts, _summary = exportable_artifacts(source)
    exported = [a for a in artifacts if a["type"] == "classification"]
    assert exported and exported[0]["subjects"][0]["match"][
        "max_match_score"
    ] == 0.31


def test_match_strength_fields_are_validated(tmp_path):
    """Unbounded on both sides — a logit is routinely far outside [0, 1] —
    so these cannot reuse the confidence validator. Only type and finiteness
    are checkable; the scale is named by ``score_kind``.
    """
    ok = classification_artifact(
        candidates=[{"species": "Robin", "confidence": 0.9,
                     "match_score": -3.5}],
        match={"max_match_score": 42.0, "score_kind": "logit"},
    )
    assert validate_artifact(ok)["subjects"][0]["match"]["max_match_score"] == 42.0

    with pytest.raises(CacheFormatError, match="match_score must be numeric"):
        validate_artifact(classification_artifact(
            candidates=[{"species": "Robin", "confidence": 0.9,
                         "match_score": "0.3"}],
        ))
    with pytest.raises(CacheFormatError, match="subject match must be an object"):
        validate_artifact(classification_artifact(match=[0.3]))
    with pytest.raises(CacheFormatError, match="match.label_count"):
        validate_artifact(classification_artifact(
            match={"max_match_score": 0.3, "label_count": -1},
        ))


def test_replacement_artifact_without_match_clears_prior_summary(tmp_path):
    """Materializing a recognized artifact from a different runtime replaces
    that run's predictions. If the incoming artifact carries no ``match``
    block, the previous runtime's summary must be removed as part of the
    replacement — leaving it attached would let a stale verdict outlive the
    predictions it described, exactly the "not recorded" case older artifacts
    are meant to express.
    """
    destination, _folder_id, _photo_id = _database_with_photo(
        tmp_path / "replace.db", "photo.jpg",
    )
    destination.upsert_labels_fingerprint(
        "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
    )
    # First materialization records a summary row.
    first_runtime = runtime_fingerprint({
        "type": "classification",
        "model": "bioclip-2.5",
        "weights_sha256": "4" * 64,
        "labels_fingerprint": "3" * 64,
        "detector_runtime_fingerprint": RUNTIME,
        "revision": "v1",
    })
    materialize_artifacts(
        destination,
        [
            detection_artifact(),
            classification_artifact(
                classifier_runtime=first_runtime,
                candidates=[{"species": "Robin", "confidence": 0.9,
                             "match_score": 0.31}],
                match={"max_match_score": 0.31, "top_species": "Robin",
                       "label_count": 1255, "score_kind": "cosine"},
            ),
        ],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={first_runtime},
    )
    assert destination.conn.execute(
        "SELECT COUNT(*) FROM classifier_match_scores",
    ).fetchone()[0] == 1

    # Second materialization from a different runtime with an OLDER-shape
    # artifact (no `match` block). The replacement must drop the prior
    # summary; leaving it would attribute a runtime-A verdict to a
    # runtime-B prediction that made no such measurement.
    second_runtime = runtime_fingerprint({
        "type": "classification",
        "model": "bioclip-2.5",
        "weights_sha256": "4" * 64,
        "labels_fingerprint": "3" * 64,
        "detector_runtime_fingerprint": RUNTIME,
        "revision": "v2",
    })
    materialize_artifacts(
        destination,
        [classification_artifact(classifier_runtime=second_runtime)],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={second_runtime},
    )
    assert destination.conn.execute(
        "SELECT COUNT(*) FROM classifier_match_scores",
    ).fetchone()[0] == 0, (
        "The prior runtime's summary must not survive replacement by an "
        "artifact that omits the match block."
    )
    destination.close()


def test_replacement_artifact_with_match_overwrites_prior_summary(tmp_path):
    """A replacement carrying its own ``match`` block writes fresh values in
    place of the old ones. Regression guard for the delete-then-insert order:
    the delete must not remove the new summary too.
    """
    destination, _folder_id, _photo_id = _database_with_photo(
        tmp_path / "replace-with-match.db", "photo.jpg",
    )
    destination.upsert_labels_fingerprint(
        "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
    )
    first_runtime = runtime_fingerprint({
        "type": "classification",
        "model": "bioclip-2.5",
        "weights_sha256": "4" * 64,
        "labels_fingerprint": "3" * 64,
        "detector_runtime_fingerprint": RUNTIME,
        "revision": "v1",
    })
    materialize_artifacts(
        destination,
        [
            detection_artifact(),
            classification_artifact(
                classifier_runtime=first_runtime,
                candidates=[{"species": "Robin", "confidence": 0.9,
                             "match_score": 0.31}],
                match={"max_match_score": 0.31, "top_species": "Robin",
                       "label_count": 1255, "score_kind": "cosine"},
            ),
        ],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={first_runtime},
    )
    second_runtime = runtime_fingerprint({
        "type": "classification",
        "model": "bioclip-2.5",
        "weights_sha256": "4" * 64,
        "labels_fingerprint": "3" * 64,
        "detector_runtime_fingerprint": RUNTIME,
        "revision": "v2",
    })
    materialize_artifacts(
        destination,
        [classification_artifact(
            classifier_runtime=second_runtime,
            candidates=[{"species": "Sparrow", "confidence": 0.88,
                         "match_score": 0.62}],
            match={"max_match_score": 0.62, "top_species": "Sparrow",
                   "label_count": 1255, "score_kind": "cosine"},
        )],
        known_runtimes={RUNTIME},
        known_classifier_runtimes={second_runtime},
    )
    row = destination.conn.execute(
        "SELECT max_match_score, top_species FROM classifier_match_scores",
    ).fetchone()
    assert row["max_match_score"] == 0.62
    assert row["top_species"] == "Sparrow"
    destination.close()


def test_enriched_artifact_wins_over_pre_feature_same_identity(tmp_path):
    """Adding the ``match`` block only changes the artifact digest — the
    lookup identity (schema, runtime fingerprints, input fingerprint) stays
    the same because the input identity is composed only of source hash,
    detector runtime, and subject key/kind/box. A pre-feature artifact and
    an enriched one for the same run therefore collide in
    ``materialize_artifacts`` deduplication, and without a preference the
    digest tiebreaker can drop the enriched one, permanently locking in the
    "match strength not recorded" state behind the ``classifier_runs`` gate.

    The enriched artifact must win regardless of manifest order.
    """
    enriched = classification_artifact(
        candidates=[{"species": "Robin", "confidence": 0.9,
                     "match_score": 0.31}],
        match={"max_match_score": 0.31, "top_species": "Robin",
               "label_count": 1255, "score_kind": "cosine"},
    )
    pre_feature = classification_artifact(
        candidates=[{"species": "Robin", "confidence": 0.9}],
    )
    # Sanity: same lookup identity, different digests.
    assert enriched["photo_sha256"] == pre_feature["photo_sha256"]
    assert enriched["runtime_fingerprint"] == pre_feature["runtime_fingerprint"]
    assert enriched["input_fingerprint"] == pre_feature["input_fingerprint"]
    assert enriched["labels"]["fingerprint"] == pre_feature["labels"]["fingerprint"]
    assert enriched["detector_runtime_fingerprint"] == pre_feature[
        "detector_runtime_fingerprint"
    ]
    assert artifact_digest(enriched) != artifact_digest(pre_feature)

    for order in ([enriched, pre_feature], [pre_feature, enriched]):
        destination, _folder_id, _photo_id = _database_with_photo(
            tmp_path / f"identity-{'enriched-first' if order[0] is enriched else 'pre-first'}.db",
            "photo.jpg",
        )
        destination.upsert_labels_fingerprint(
            "3" * 12, "Test labels", [], 1, full_fingerprint="3" * 64,
        )
        materialize_artifacts(
            destination,
            [detection_artifact(), *order],
            known_runtimes={RUNTIME},
            known_classifier_runtimes={CLASSIFIER_RUNTIME},
        )
        row = destination.conn.execute(
            "SELECT max_match_score, top_species, score_kind "
            "FROM classifier_match_scores",
        ).fetchone()
        assert row is not None, (
            "The enriched artifact must survive dedup regardless of "
            "manifest order — otherwise a same-identity pre-feature "
            "artifact hides real inference behind 'not recorded'."
        )
        assert row["max_match_score"] == 0.31
        assert row["top_species"] == "Robin"
        assert row["score_kind"] == "cosine"
        destination.close()
