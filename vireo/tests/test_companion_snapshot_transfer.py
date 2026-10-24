"""Companion pairing preserves current and historical local-adjustment masks."""

import json
import os
from pathlib import Path
from types import SimpleNamespace

import local_masks
import pytest
from db import Database
from PIL import Image
from scanner import companions
from services import photo_ingest


@pytest.fixture
def catalog(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    folder_id = db.add_folder(str(tmp_path / "photos"))
    ids = {}
    for extension in (".cr3", ".jpg"):
        ids[extension] = photo_ingest.add_photo(
            db, folder_id=folder_id, filename=f"bird{extension}",
            extension=extension, file_size=100, file_mtime=1.0,
        )
    root = str(tmp_path / "vireo")
    recipes = []
    for color in (50, 150, 250):
        source = tmp_path / f"mask-{color}.png"
        Image.new("L", (8, 6), color).save(source)
        mask = local_masks.create_snapshot(
            photo_id=ids[".jpg"], mask_row={"path": str(source)}, vireo_dir=root,
        )
        recipes.append({"local": {"mask": mask, "regions": [
            {"region": "subject", "adjustments": {"exposure": 1}},
        ]}})
    db.set_photo_edit_recipe(ids[".jpg"], recipes[2])
    # The middle snapshot exists only in a historical new_value, and the
    # first only in an old_value. Neither may be lost when history moves.
    for before, after in zip(recipes[:-1], recipes[1:], strict=True):
        db.record_edit("edit_recipe", "Adjust exposure", json.dumps(after), [{
            "photo_id": ids[".jpg"], "old_value": json.dumps(before),
            "new_value": json.dumps(after),
        }])
    db.commit()
    cat = SimpleNamespace(
        db=db, root=root, jpeg=ids[".jpg"], raw=ids[".cr3"],
        recipes=recipes, folder_id=folder_id,
    )
    try:
        yield cat
    finally:
        db.close()


def _pair(cat):
    return companions._pair_raw_jpeg_companions(
        cat.db, vireo_dir=cat.root, thumb_cache_dir=os.path.join(cat.root, "thumbnails"),
    )


def _path(cat, photo_id, recipe):
    return Path(local_masks.snapshot_path(cat.root, photo_id, recipe["local"]["mask"]["ref"]))


def _assert_masks(cat, photo_id):
    for recipe, color in zip(cat.recipes, (50, 150, 250), strict=True):
        mask = local_masks.load_snapshot(cat.root, photo_id, recipe)
        assert mask is not None
        with mask:
            assert mask.size == (8, 6)
            assert mask.getextrema() == (color, color)


def _assert_pair_is_separate(cat):
    assert cat.db.photos.get(cat.jpeg) is not None
    assert cat.db.photos.get(cat.raw)["companion_path"] is None
    assert cat.db.get_photo_edit_recipe(cat.raw) is None
    assert cat.db.get_photo_edit_recipe(cat.jpeg)["local"] == cat.recipes[-1]["local"]
    history = cat.db.conn.execute("SELECT photo_id FROM edit_history_items").fetchall()
    assert {row["photo_id"] for row in history} == {cat.jpeg}
    _assert_masks(cat, cat.jpeg)


def test_pairing_preserves_masks_without_enumerating_directory(catalog, monkeypatch):
    def unreadable(_):
        raise PermissionError("directory cannot be enumerated")

    monkeypatch.setattr(local_masks.os, "listdir", unreadable)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)
    assert catalog.db.photos.get(catalog.jpeg) is None
    # Undo and redo must actually load the historically referenced masks.
    for action, color in ((catalog.db.undo_last_edit, 150), (catalog.db.redo_last_undo, 250)):
        assert action() is not None
        recipe = catalog.db.get_photo_edit_recipe(catalog.raw)
        with local_masks.load_snapshot(catalog.root, catalog.raw, recipe) as mask:
            assert mask.getextrema() == (color, color)


@pytest.mark.parametrize("failure", ["publish", "write"])
def test_failed_copy_keeps_pair_and_masks_usable_then_retries(catalog, monkeypatch, caplog, failure):
    target = "replace_file" if failure == "publish" else "fsync"
    owner = local_masks if failure == "publish" else local_masks.os
    original = getattr(owner, target)

    def blocked(*args):
        raise PermissionError("snapshot publication is locked")

    monkeypatch.setattr(owner, target, blocked)
    assert _pair(catalog) == {}
    _assert_pair_is_separate(catalog)
    assert "later scan will retry" in caplog.text
    assert not list(Path(local_masks.edit_masks_dir(catalog.root)).glob("*.tmp"))
    assert not list(Path(local_masks.edit_masks_dir(catalog.root)).glob(f"{catalog.raw}.*.png"))

    monkeypatch.setattr(owner, target, original)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)
    assert _pair(catalog) == {}
    _assert_masks(catalog, catalog.raw)


def test_partial_copy_can_retry_without_removing_originals(catalog, monkeypatch):
    original = local_masks.replace_file
    calls = []

    def fail_second(src, dest):
        calls.append(dest)
        if len(calls) == 2:
            raise OSError("second snapshot cannot be published")
        original(src, dest)

    monkeypatch.setattr(local_masks, "replace_file", fail_second)
    assert _pair(catalog) == {}
    _assert_pair_is_separate(catalog)
    assert len(list(Path(local_masks.edit_masks_dir(catalog.root)).glob(f"{catalog.raw}.*.png"))) == 1

    monkeypatch.setattr(local_masks, "replace_file", original)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)


def test_commit_failure_keeps_original_masks_and_history(catalog, monkeypatch):
    def fail_commit():
        raise RuntimeError("commit failed after copies were prepared")

    with monkeypatch.context() as patch:
        patch.setattr(catalog.db, "commit_with_retry", fail_commit)
        with pytest.raises(RuntimeError, match="commit failed"):
            _pair(catalog)
    catalog.db.rollback()
    _assert_pair_is_separate(catalog)
    _assert_masks(catalog, catalog.raw)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)


def test_interruption_after_copy_preserves_masks_on_database_reopen(catalog, monkeypatch):
    original = local_masks.prepare_snapshot_transfer

    def interrupt(*args):
        original(*args)
        raise SystemExit("interrupted before pairing commit")

    with monkeypatch.context() as patch:
        patch.setattr(local_masks, "prepare_snapshot_transfer", interrupt)
        with pytest.raises(SystemExit, match="interrupted"):
            _pair(catalog)
    db_path = catalog.db._db_path
    catalog.db.close()
    catalog.db = Database(db_path)
    _assert_pair_is_separate(catalog)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)


def test_cleanup_failure_leaves_transferred_masks_usable(catalog, monkeypatch):
    original = local_masks.os.remove
    sources = {str(_path(catalog, catalog.jpeg, recipe)) for recipe in catalog.recipes}

    def locked(path):
        if str(path) in sources:
            raise PermissionError("old snapshot is still open")
        original(path)

    monkeypatch.setattr(local_masks.os, "remove", locked)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)
    _assert_masks(catalog, catalog.jpeg)


def test_pairing_never_renames_original_snapshots(catalog, monkeypatch):
    original = local_masks.replace_file
    sources = {str(_path(catalog, catalog.jpeg, recipe)) for recipe in catalog.recipes}

    def locked_source(src, dest):
        assert str(src) not in sources, "originals must remain available until commit"
        original(src, dest)

    monkeypatch.setattr(local_masks, "replace_file", locked_source)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)


def test_cleanup_preserves_masks_owned_by_recycled_companion_id(catalog, monkeypatch):
    original = companions._run_post_commit_fs_actions

    def recycle_before_cleanup(actions):
        photo_id = photo_ingest.add_photo(
            catalog.db, folder_id=catalog.folder_id, filename="new.jpg",
            extension=".jpg", file_size=100, file_mtime=1.0,
        )
        assert photo_id == catalog.jpeg
        catalog.db.set_photo_edit_recipe(photo_id, catalog.recipes[-1])
        catalog.db.commit()
        original(actions)

    monkeypatch.setattr(companions, "_run_post_commit_fs_actions", recycle_before_cleanup)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)
    with local_masks.load_snapshot(catalog.root, catalog.jpeg, catalog.recipes[-1]) as mask:
        assert mask.getextrema() == (250, 250)


def test_mask_failure_does_not_block_unrelated_pairs(catalog, monkeypatch):
    other = {}
    for extension in (".jpg", ".cr3"):
        other[extension] = photo_ingest.add_photo(
            catalog.db, folder_id=catalog.folder_id, filename=f"other{extension}",
            extension=extension, file_size=100, file_mtime=1.0,
        )

    def blocked(*args):
        raise OSError("snapshot cannot be published")

    monkeypatch.setattr(local_masks, "replace_file", blocked)
    assert _pair(catalog) == {other[".jpg"]: other[".cr3"]}
    _assert_pair_is_separate(catalog)


def test_masked_recipe_is_not_moved_without_cache_directory(catalog):
    assert companions._pair_raw_jpeg_companions(catalog.db) == {}
    _assert_pair_is_separate(catalog)


@pytest.mark.parametrize("missing", [False, True])
def test_unavailable_historical_snapshot_defers_pairing(catalog, missing):
    path = _path(catalog, catalog.jpeg, catalog.recipes[0])
    data = path.read_bytes()
    if missing:
        path.unlink()
    else:
        path.write_bytes(b"broken snapshot")
    assert _pair(catalog) == {}
    assert catalog.db.photos.get(catalog.jpeg) is not None
    assert catalog.db.get_photo_edit_recipe(catalog.raw) is None
    # Restoring the historical snapshot makes the next scan recoverable.
    path.write_bytes(data)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)


def test_preparation_repairs_corrupt_destination_without_touching_source(catalog):
    for recipe in catalog.recipes:
        _path(catalog, catalog.raw, recipe).write_bytes(b"partial earlier copy")
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    _assert_masks(catalog, catalog.raw)


def test_cleanup_retains_source_if_prepared_copy_disappears(catalog):
    values = catalog.db.companion_pairing.recipe_snapshot_values(catalog.jpeg)
    refs = local_masks.prepare_snapshot_transfer(catalog.root, catalog.jpeg, catalog.raw, values)
    for recipe in catalog.recipes:
        _path(catalog, catalog.raw, recipe).unlink()
    local_masks.cleanup_snapshot_transfer(catalog.root, catalog.jpeg, catalog.raw, refs)
    _assert_masks(catalog, catalog.jpeg)


def test_existing_primary_recipe_wins_without_copying_companion_masks(catalog, monkeypatch):
    catalog.db.set_photo_edit_recipe(catalog.raw, {"rotation": 90})

    def unexpected(*args):
        pytest.fail("the companion's recipe will not be transferred")

    monkeypatch.setattr(local_masks, "prepare_snapshot_transfer", unexpected)
    assert _pair(catalog) == {catalog.jpeg: catalog.raw}
    assert catalog.db.get_photo_edit_recipe(catalog.raw)["rotation"] == 90
