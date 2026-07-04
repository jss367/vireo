"""Tests for edit-mask snapshots (local adjustments, PR 1).

Covers snapshot creation from the active photo_masks file, source-digest
staleness, loading, and grace-window GC. See
docs/plans/2026-07-03-local-adjustments-design.md (trimmed v1 scope).
"""

import os
import sys
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
