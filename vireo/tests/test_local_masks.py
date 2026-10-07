"""Tests for edit-mask snapshots (local adjustments, PR 1).

Covers snapshot creation from the active photo_masks file, source-digest
staleness, loading, and grace-window GC. See
docs/plans/2026-07-03-local-adjustments-design.md (trimmed v1 scope).
"""

import hashlib
import os
import sys
import threading
import time

import numpy as np
import pytest
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import local_masks


def _write_mask(path, width=80, height=60, box=(20, 10, 40, 40)):
    arr = np.zeros((height, width), dtype=np.uint8)
    x, y, w, h = box
    arr[y:y + h, x:x + w] = 255
    Image.fromarray(arr, "L").save(path, "PNG")
    return path


def _mask_row(path, variant="sam2-small"):
    return {
        "variant": variant,
        "path": path,
        "detector_model": "megadetector-v6",
        "prompt_x": 0.25,
        "prompt_y": 0.17,
        "prompt_w": 0.5,
        "prompt_h": 0.66,
    }


def _local_recipe(mask):
    return {
        "local": {
            "mask": mask,
            "regions": [
                {"region": "subject", "adjustments": {"exposure": 1}},
            ],
        }
    }


def test_create_snapshot_copies_and_is_content_addressed(tmp_path):
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    row = _mask_row(src)

    mask = local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )

    assert set(mask) == {"ref", "source_digest"}
    assert len(mask["ref"]) == 12 and int(mask["ref"], 16) >= 0
    snap = local_masks.snapshot_path(str(tmp_path), 1, mask["ref"])
    assert os.path.exists(snap)
    with Image.open(snap) as img, Image.open(src) as orig:
        assert np.array_equal(np.asarray(img.convert("L")),
                              np.asarray(orig.convert("L")))

    # Same source bytes -> same ref (idempotent, no duplicate files).
    again = local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    assert again["ref"] == mask["ref"]


def test_create_snapshot_refreshes_mtime_on_reuse(tmp_path):
    # An aged, currently-unreferenced snapshot that a new create_snapshot()
    # returns must have its mtime bumped, so the GC grace window is measured
    # from *this* request — otherwise a stale-mask sweep can delete the file
    # after we returned its ref but before the recipe save re-references it.
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    row = _mask_row(src)
    mask = local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    snap = local_masks.snapshot_path(str(tmp_path), 1, mask["ref"])

    aged = time.time() - 30 * 24 * 3600
    os.utime(snap, (aged, aged))
    assert os.path.getmtime(snap) < time.time() - 24 * 3600

    local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    assert os.path.getmtime(snap) > time.time() - 60


def test_source_digest_tracks_source_inputs(tmp_path):
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    row = _mask_row(src)
    base = local_masks.source_digest(row)

    # Same inputs -> same digest.
    assert local_masks.source_digest(_mask_row(src)) == base

    # Different prompt -> different digest.
    moved = _mask_row(src)
    moved["prompt_x"] = 0.4
    assert local_masks.source_digest(moved) != base

    # Different file bytes -> different digest.
    _write_mask(src, box=(5, 5, 20, 20))
    assert local_masks.source_digest(_mask_row(src)) != base


def test_create_snapshot_digest_matches_snapshotted_bytes(tmp_path):
    """The returned ``source_digest`` must describe the bytes actually
    frozen into the snapshot file. If it were computed by re-reading the
    live mask path, a mask-extraction job rewriting the file mid-snapshot
    could leave ``ref`` (snapshot bytes) and ``source_digest`` (live bytes)
    describing different content — ``is_stale()`` would report ``False`` for
    a snapshot that no longer matches its source, and the render would use
    stale pixels while claiming to be current."""
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    row = _mask_row(src)
    mask = local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    snap = local_masks.snapshot_path(str(tmp_path), 1, mask["ref"])
    with open(snap, "rb") as f:
        snap_bytes = f.read()
    assert mask["source_digest"] == local_masks._source_digest_from_bytes(
        snap_bytes, row
    )


def test_create_snapshot_digest_survives_concurrent_source_rewrite(tmp_path, monkeypatch):
    """When a mask-extraction job rewrites the live mask between the
    snapshot copy and the digest, ``source_digest`` must describe the
    bytes we snapshotted, not the new live bytes — otherwise ``is_stale()``
    would report ``False`` for a snapshot that no longer matches, and the
    render would silently use stale pixels while claiming to be current."""
    src = str(tmp_path / "1.sam2-small.png")
    _write_mask(src, box=(20, 10, 40, 40))
    row = _mask_row(src)
    original_digest = local_masks.source_digest(row)

    real_image_open = local_masks.Image.open
    rewritten = {"done": False}

    def rewrite_and_open(*args, **kwargs):
        # ``Image.open`` runs after ``data = f.read()`` in create_snapshot,
        # so mutating the live file here reproduces the TOCTOU: with the
        # old code the subsequent source_digest() call would re-read the
        # (now different) live bytes and disagree with ``ref``.
        if not rewritten["done"]:
            _write_mask(src, box=(5, 5, 10, 10))
            rewritten["done"] = True
        return real_image_open(*args, **kwargs)

    monkeypatch.setattr(local_masks.Image, "open", rewrite_and_open)
    mask = local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(80, 60),
    )
    # Sanity: the live file's digest really did change during the call.
    assert local_masks.source_digest(row) != original_digest
    # But the recorded source_digest describes the snapshotted bytes.
    assert mask["source_digest"] == original_digest


def test_create_snapshot_concurrent_publishes_dont_race_on_shared_tempfile(tmp_path):
    """Two POSTs for the same (photo, mask) racing on snapshot creation
    must both succeed. With a deterministic ``dest + ".tmp"`` path both
    writers would name the same tmp file; whichever ``os.replace()`` lands
    first steals the other's tmp path, and the loser raises
    ``FileNotFoundError`` (500 to the client). A per-call ``mkstemp`` name
    keeps the writers isolated."""
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    row = _mask_row(src)

    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def worker():
        try:
            barrier.wait(timeout=5)
            local_masks.create_snapshot(
                photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
                native_size=(80, 60),
            )
        except BaseException as exc:  # includes threading errors
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent create_snapshot raised: {errors!r}"
    with open(src, "rb") as f:
        expected_ref = hashlib.sha1(f.read()).hexdigest()[:12]
    assert os.path.exists(
        local_masks.snapshot_path(str(tmp_path), 1, expected_ref)
    )
    # No leftover *.png.tmp files — every writer either publishes or
    # cleans up its own tmp.
    leftover = sorted(
        n for n in os.listdir(local_masks.edit_masks_dir(str(tmp_path)))
        if n.endswith(".tmp")
    )
    assert leftover == [], f"leaked tempfiles: {leftover!r}"


def test_create_snapshot_rejects_corrupt_mask_file(tmp_path):
    # A truncated / non-image mask file must surface as ValueError, not
    # PIL's UnidentifiedImageError, so the snapshot endpoint returns a
    # recoverable 400 ("regenerate the mask") instead of a 500.
    src = str(tmp_path / "1.sam2-small.png")
    with open(src, "wb") as f:
        f.write(b"not a real png")

    with pytest.raises(ValueError, match="not a readable image"):
        local_masks.create_snapshot(
            photo_id=1, mask_row=_mask_row(src), vireo_dir=str(tmp_path),
            native_size=(800, 600),
        )


def test_create_snapshot_rejects_valid_header_truncated_body(tmp_path):
    # A mask-extraction job interrupted mid-write can leave a file with a
    # valid PNG header (Image.open() succeeds, .size is readable) but a
    # truncated IDAT body that only fails when Pillow actually decodes.
    # Without a forced decode the snapshot endpoint would accept the file,
    # copy it verbatim, and every subsequent render would silently disable
    # local adjustments when load_snapshot() hits the same decode error.
    good = str(tmp_path / "good.png")
    _write_mask(good, width=80, height=60)
    full = open(good, "rb").read()
    # Slice off half the file — the PNG header and IHDR chunk are intact,
    # so Image.open() reports width/height, but decoding the pixel stream
    # will fail.
    truncated = str(tmp_path / "1.sam2-small.png")
    with open(truncated, "wb") as f:
        f.write(full[: len(full) // 2])

    with pytest.raises(ValueError, match="not a readable image"):
        local_masks.create_snapshot(
            photo_id=1, mask_row=_mask_row(truncated),
            vireo_dir=str(tmp_path), native_size=(800, 600),
        )


def test_create_snapshot_rejects_aspect_mismatch(tmp_path):
    # 80x60 mask (4:3) against a 16:9 photo must refuse rather than
    # misalign local weights.
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))

    with pytest.raises(ValueError, match="aspect"):
        local_masks.create_snapshot(
            photo_id=1, mask_row=_mask_row(src), vireo_dir=str(tmp_path),
            native_size=(1920, 1080),
        )


def test_load_snapshot_roundtrip_and_missing(tmp_path):
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    mask = local_masks.create_snapshot(
        photo_id=1, mask_row=_mask_row(src), vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    recipe = _local_recipe(dict(mask))

    loaded = local_masks.load_snapshot(str(tmp_path), 1, recipe)
    assert loaded is not None and loaded.mode == "L"
    assert loaded.size == (80, 60)

    # Recipes without local load nothing.
    assert local_masks.load_snapshot(str(tmp_path), 1, {"rotation": 90}) is None
    assert local_masks.load_snapshot(str(tmp_path), 1, None) is None

    # Missing file: None, no raise (renderer disables the local pass).
    os.remove(local_masks.snapshot_path(str(tmp_path), 1, mask["ref"]))
    assert local_masks.load_snapshot(str(tmp_path), 1, recipe) is None


def test_staleness_compares_source_metadata(tmp_path):
    src = _write_mask(str(tmp_path / "1.sam2-small.png"))
    row = _mask_row(src)
    mask = local_masks.create_snapshot(
        photo_id=1, mask_row=row, vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    recipe = _local_recipe(dict(mask))

    assert local_masks.is_stale(recipe, row) is False

    # Prompt moved (detector re-run) -> stale.
    moved = dict(row)
    moved["prompt_x"] = 0.4
    assert local_masks.is_stale(recipe, moved) is True

    # Mask file rewritten in place -> stale.
    _write_mask(src, box=(5, 5, 20, 20))
    assert local_masks.is_stale(recipe, row) is True

    # No live mask row at all -> treated as stale (mask went away).
    assert local_masks.is_stale(recipe, None) is True

    # Recipes without local are never stale.
    assert local_masks.is_stale({"rotation": 90}, row) is False


def test_gc_respects_references_history_and_grace(tmp_path, monkeypatch):
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, width=800, height=600,
    )

    src = _write_mask(str(tmp_path / "mask-src.png"))
    mask = local_masks.create_snapshot(
        photo_id=pid, mask_row=_mask_row(src), vireo_dir=str(tmp_path),
        native_size=(800, 600),
    )
    referenced = local_masks.snapshot_path(str(tmp_path), pid, mask["ref"])
    db.set_photo_edit_recipe(pid, _local_recipe(dict(mask)))

    # A second, unreferenced snapshot file: old enough to collect.
    orphan = local_masks.snapshot_path(str(tmp_path), pid, "0123456789ab")
    _write_mask(orphan, box=(1, 1, 10, 10))
    old = time.time() - 7 * 24 * 3600
    os.utime(orphan, (old, old))

    # A third, unreferenced but recent file: inside the grace window.
    recent = local_masks.snapshot_path(str(tmp_path), pid, "ba9876543210")
    _write_mask(recent, box=(2, 2, 10, 10))

    result = local_masks.gc_edit_masks(db, str(tmp_path))

    assert not os.path.exists(orphan)
    assert os.path.exists(referenced)
    assert os.path.exists(recent)
    assert result["deleted"] == 1

    # Clearing the recipe keeps the ref alive through edit history.
    db.set_photo_edit_recipe(pid, None)
    history_rows = db.conn.execute(
        "SELECT COUNT(*) AS n FROM edit_history_items "
        "WHERE old_value LIKE '%' || ? || '%' OR new_value LIKE '%' || ? || '%'",
        (mask["ref"], mask["ref"]),
    ).fetchone()
    os.utime(referenced, (old, old))
    result = local_masks.gc_edit_masks(db, str(tmp_path))
    if history_rows["n"]:
        assert os.path.exists(referenced)
    else:
        # No history captured the ref (recipe API records history at the
        # app layer, not db.set_photo_edit_recipe) — then it must collect.
        assert not os.path.exists(referenced)
    db.close()


def test_gc_rechecks_mtime_before_deleting(tmp_path, monkeypatch):
    """A concurrent POST /local-mask/snapshot that reuses an unreferenced
    file refreshes its mtime between the sweep's initial mtime check and the
    ``os.remove()`` call. Without a second stat, the sweep would still delete
    the just-touched file and break the ref before the recipe save can pin
    it. The recheck must catch that refresh."""
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, width=800, height=600,
    )

    orphan = local_masks.snapshot_path(str(tmp_path), pid, "0123456789ab")
    os.makedirs(os.path.dirname(orphan), exist_ok=True)
    _write_mask(orphan, box=(1, 1, 10, 10))
    old = time.time() - 30 * 24 * 3600
    os.utime(orphan, (old, old))

    real_getmtime = os.path.getmtime
    calls = {"n": 0}

    def flaky_getmtime(path):
        calls["n"] += 1
        # First stat: aged, so the sweep decides to delete. Simulate a
        # concurrent create_snapshot() refreshing the mtime in between.
        if calls["n"] == 1:
            return old
        os.utime(path, None)
        return real_getmtime(path)

    monkeypatch.setattr(local_masks.os.path, "getmtime", flaky_getmtime)
    result = local_masks.gc_edit_masks(db, str(tmp_path))

    assert os.path.exists(orphan), (
        "recheck should have caught the mtime refresh and skipped delete"
    )
    assert result["deleted"] == 0
    assert result["kept"] == 1
    db.close()


def test_brush_correction_is_immutable_and_keeps_source_staleness(tmp_path):
    from image_edits import normalize_recipe

    path = _write_mask(str(tmp_path / 'source.png'))
    row = _mask_row(path)
    original = local_masks.create_snapshot(photo_id=1, mask_row=row, vireo_dir=str(tmp_path))
    options = dict(vireo_dir=str(tmp_path), photo_id=1, radius=0.06)
    added = local_masks.correct_snapshot(**options, mask=original, mode='add', points=[[0.05, 0.5], [0.15, 0.5]])
    removed = local_masks.correct_snapshot(**options, mask=added, mode='subtract', points=[[0.5, 0.5]])
    assert len({original['ref'], added['ref'], removed['ref']}) == 3
    assert original['source_digest'] == added['source_digest'] == removed['source_digest']
    before = np.asarray(local_masks.load_snapshot(str(tmp_path), 1, _local_recipe(original)))
    after = np.asarray(local_masks.load_snapshot(str(tmp_path), 1, _local_recipe(removed)))
    assert before[30, 8] == 0 and after[30, 8] == 255
    assert before[30, 40] == 255 and after[30, 40] == 0
    # Corrections can be saved before the first exposure adjustment.
    recipe = normalize_recipe({'local': {'mask': removed, 'regions': []}})
    assert recipe['local']['mask']['corrected'] is True
    assert not local_masks.is_stale(recipe, row)
    _write_mask(path, box=(0, 0, 10, 10))
    assert local_masks.is_stale(recipe, row)
    assert np.array_equal(before, np.asarray(local_masks.load_snapshot(str(tmp_path), 1, _local_recipe(original))))
    # Identical strokes reuse content rather than mutate prior history.
    assert local_masks.correct_snapshot(**options, mask=original, mode='add', points=[[0.05, 0.5], [0.15, 0.5]]) == added


@pytest.mark.parametrize('override', [
    {'mode': 'replace'}, {'radius': True}, {'radius': float('nan')},
    {'radius': 0}, {'radius': 0.5}, {'radius': 10 ** 400},
    {'softness': True}, {'softness': -0.1}, {'softness': 1.1}, {'softness': float('nan')},
    {'strength': False}, {'strength': 0}, {'strength': 1.1}, {'strength': float('inf')},
    {'points': []}, {'points': [[0.5, 0.5]] * 2049},
    {'points': [[float('inf'), 0.5]]}, {'points': [[True, 0.5]]},
    {'points': [[-0.1, 0.5]]}, {'points': [[0.5]]},
    {'mask': {'ref': '../outside', 'source_digest': 'test'}},
])
def test_invalid_brush_does_not_publish(tmp_path, override):
    options = dict(vireo_dir=str(tmp_path), photo_id=1,
                   mask={'ref': 'a' * 12, 'source_digest': 'test'},
                   mode='add', radius=0.05, points=[[0.5, 0.5]])
    options.update(override)
    with pytest.raises(ValueError):
        local_masks.correct_snapshot(**options)
    assert not (tmp_path / 'edit-masks').exists()


def test_brush_snapshot_cannot_cross_photo_ids(tmp_path):
    original = local_masks.create_snapshot(photo_id=1, mask_row=_mask_row(_write_mask(str(tmp_path / 'source.png'))), vireo_dir=str(tmp_path))
    with pytest.raises(ValueError, match='missing'):
        local_masks.correct_snapshot(vireo_dir=str(tmp_path), photo_id=2,
                                     mask=original, mode='add', radius=0.05, points=[[0.5, 0.5]])


@pytest.mark.parametrize('mode, initial, expected', [('add', 0, 128), ('subtract', 255, 128)])
def test_soft_brush_fades_inward_and_strength_applies_once(tmp_path, mode, initial, expected):
    path = str(tmp_path / 'source.png')
    Image.new('L', (201, 201), initial).save(path)
    original = local_masks.create_snapshot(photo_id=1, mask_row=_mask_row(path), vireo_dir=str(tmp_path))
    options = dict(vireo_dir=str(tmp_path), photo_id=1, mask=original,
                   mode=mode, radius=0.1, softness=1, strength=0.5)
    def pixels(result):
        return np.asarray(local_masks.load_snapshot(str(tmp_path), 1, _local_recipe(result)))

    single = local_masks.correct_snapshot(**options, points=[[0.5, 0.5]])
    repeated = local_masks.correct_snapshot(**options, points=[[0.5, 0.5]] * 10)
    assert repeated == single  # sampling/retracing cannot stack opacity
    after = pixels(single)
    assert after[100, 100] == expected
    effect = abs(after[100].astype(int) - initial)
    assert effect[100] > effect[110] > effect[118] > 0
    assert effect[123] == 0
    assert (pixels(original) == initial).all()  # history's pixels stay intact
    second = local_masks.correct_snapshot(**{**options, 'mask': single}, points=[[0.5, 0.5]])
    assert abs(int(pixels(second)[100, 100]) - initial) > effect[100]


def test_soft_brush_at_photo_edge_does_not_fade_along_clipped_edge(tmp_path):
    path = str(tmp_path / 'source.png')
    Image.new('L', (201, 201)).save(path)
    original = local_masks.create_snapshot(photo_id=1, mask_row=_mask_row(path), vireo_dir=str(tmp_path))
    corrected = local_masks.correct_snapshot(vireo_dir=str(tmp_path), photo_id=1,
                                            mask=original, mode='add', radius=0.1,
                                            points=[[0, 0.5]], softness=1, strength=0.5)
    pixels = np.asarray(local_masks.load_snapshot(str(tmp_path), 1, _local_recipe(corrected)))
    assert pixels[100, 0] == 128
    assert 0 < pixels[100, 15] < pixels[100, 5] < 128
