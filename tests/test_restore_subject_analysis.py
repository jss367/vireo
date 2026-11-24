"""The one-off restore puts back only measurements that still describe the photo."""
import hashlib
import importlib.util
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "vireo"))

VARIANT = "sam2-large"
BOX = {"x": .2, "y": .2, "w": .4, "h": .4}
MEASURED = {"quality_score": .8, "subject_sharpness": 9.0, "subject_tenengrad": 120.0, "bg_tenengrad": 12.0,
            "crop_complete": 1.0, "dino_subject_embedding": b"embedding", "subject_detail": 5000.0,
            "subject_detail_px": 300.0}


@pytest.fixture
def restore(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "restore_subject_analysis", ROOT / "scripts" / "restore_subject_analysis.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def fake_load_image(path, *, max_size, raw_decode):
        from image_loader import RAW_DECODE_ALIGNED

        assert raw_decode == RAW_DECODE_ALIGNED
        image = Image.new("RGB", (30, 20))
        image.info.update(vireo_native_size=image.size, vireo_aligned_rendition="image")
        return image

    monkeypatch.setattr("image_loader.load_image", fake_load_image)
    return module


class Catalogs:
    """A live catalog that lost its measurements, and the snapshot from before."""

    def __init__(self, tmp_path):
        from db import Database
        from services import workspace_folders

        self.live, self.backup = tmp_path / "vireo.db", tmp_path / "vireo.db.pre-v57.bak"
        self.masks = tmp_path / "masks"
        self.masks.mkdir()
        self.db = Database(str(self.live))
        self.sources = tmp_path / "photos"
        self.sources.mkdir()
        self.folder = workspace_folders.add_folder(self.db, str(self.sources))
        self.boxes = {}

    def photo(self, name, boxes=(BOX,), *, runtime="legacy", extension=".jpg"):
        from services import detection_store, photo_ingest

        source = self.sources / name
        source.write_bytes((name.encode() * 100)[:100])
        os.utime(source, (1, 1))
        pid = photo_ingest.add_photo(self.db, self.folder, name, extension, 100, 1.0)
        self.db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?",
                             (hashlib.sha256(source.read_bytes()).hexdigest(), pid))
        dids = detection_store.write_detection_batch(self.db, pid, "megadetector-v6", [
            {"box": box, "confidence": .9 - i / 10, "category": "animal"} for i, box in enumerate(boxes)], runtime)
        path = self.masks / f"{pid}.{VARIANT}.png"
        Image.fromarray(np.full((20, 30), 255, np.uint8)).save(path)
        self.db.masks_features.upsert_mask(pid, VARIANT, str(path), "megadetector-v6", tuple(boxes[0].values()))
        self.db.conn.execute(
            f"UPDATE photos SET mask_path=?, active_mask_variant=?, dino_embedding_variant='vit-b14', "
            f"{', '.join(c + '=?' for c in MEASURED)} WHERE id=?", (str(path), VARIANT, *MEASURED.values(), pid))
        self.db.conn.execute(
            "UPDATE photo_masks SET subject_tenengrad=120, subject_detail=5000, subject_detail_px=300 "
            "WHERE photo_id=?", (pid,))
        self.db.subjects.set_primary_state(pid, dids[0], .2)
        self.boxes[pid] = dids
        return pid

    def lose(self, *, measurements=True):
        """Retire the masks; optionally seed compatible live box measurements.

        The default models a catalog whose canonical box analysis was already
        recomputed after migration. Missing-evidence tests leave it retired.
        """
        from schema import _retire_unbound_detector_caches

        self.db.commit()
        with closing(sqlite3.connect(self.backup)) as copy:
            self.db.conn.backup(copy)
            copy.execute("ALTER TABLE detections DROP COLUMN frame_id")
            copy.commit()
        _retire_unbound_detector_caches(self.db.conn)
        if measurements:
            self.db.subjects.save_analyses("synthetic-current-source", [
                {"detection_id": ids[0], "crop": BOX, "quality_score": .8, "exposure_ev": 0,
                 "features": {"subject_sharpness": 9.0}} for ids in self.boxes.values()])
        self.db.commit()

    def run(self, restore, *args):
        self.db.commit()
        self.db.close()
        result = restore.main([str(self.live), str(self.backup), *args])
        from db import Database

        self.db = Database(str(self.live))
        return result

    def row(self, pid):
        return dict(self.db.conn.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone())

    def mask(self, pid):
        return self.db.masks_features.get_mask(pid, VARIANT)


@pytest.fixture
def catalogs(tmp_path):
    made = Catalogs(tmp_path)
    yield made
    made.db.close()


def test_restores_the_mask_and_its_measurements_but_not_the_detail(catalogs, restore, capsys):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    assert catalogs.row(pid)["mask_path"] is None
    assert catalogs.run(restore) == 0
    assert "1 masks on 1 photos can be restored" in capsys.readouterr().out
    assert catalogs.mask(pid) is None, "a report must not write"
    assert not list(catalogs.live.parent.glob("*.pre-restore-*"))

    catalogs.run(restore, "--apply")
    row, mask = catalogs.row(pid), catalogs.mask(pid)
    for column, value in MEASURED.items():
        assert row[column] == (None if column.startswith("subject_detail") else value)
    assert row["mask_path"] == mask["path"]
    assert (mask["subject_tenengrad"], mask["subject_detail"], mask["subject_detail_px"]) == (120, None, None)
    saved, = catalogs.live.parent.glob("*.pre-restore-*")
    with closing(sqlite3.connect(saved)) as copy:
        assert copy.execute("SELECT COUNT(*) FROM photo_masks").fetchone() == (0,)
    catalogs.run(restore)
    assert "0 masks on 0 photos" in capsys.readouterr().out


def test_user_decisions_made_since_are_untouched(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    catalogs.db.conn.execute("UPDATE photos SET rating=4, flag='flagged' WHERE id=?", (pid,))
    catalogs.run(restore, "--apply")
    row = catalogs.row(pid)
    assert (row["rating"], row["flag"], row["quality_score"]) == (4, "flagged", .8)


def _newer_mask(catalogs, pid):
    catalogs.db.masks_features.upsert_mask(pid, VARIANT, "new.png", "megadetector-v6", (.1, .1, .1, .1))


def _redetected(catalogs, pid):
    from services import detection_store

    detection_store.write_detection_batch(catalogs.db, pid, "megadetector-v6", [
        {"box": BOX, "confidence": .9, "category": "animal", "frame_id": "f" * 64}], "legacy")


def _another_runtime(catalogs, pid):
    catalogs.db.conn.execute("UPDATE detections SET runtime_fingerprint='newer' WHERE photo_id=?", (pid,))


def _rejected(catalogs, pid):
    catalogs.db.conn.execute("UPDATE detections SET subject_verdict='not_animal' WHERE photo_id=?", (pid,))


def _moved_box(catalogs, pid):
    catalogs.db.conn.execute("UPDATE detections SET box_x=box_x+0.1 WHERE photo_id=?", (pid,))


def _companion(catalogs, pid):
    catalogs.db.conn.execute("UPDATE photos SET companion_path='a.jpg' WHERE id=?", (pid,))


def _companion_frame(catalogs, pid):
    from detection_id import COMPANION_FRAME_RUNTIME

    catalogs.db.conn.execute(
        "UPDATE detections SET runtime_fingerprint=? WHERE photo_id=?", (COMPANION_FRAME_RUNTIME, pid))


def _changed_file(catalogs, pid):
    catalogs.db.conn.execute("UPDATE photos SET file_size=101 WHERE id=?", (pid,))


def _missing_png(catalogs, pid):
    (catalogs.masks / f"{pid}.{VARIANT}.png").unlink()


def _rewritten_png(catalogs, pid):
    from masking import write_mask_png

    write_mask_png(np.ones((20, 30), bool), catalogs.masks / f"{pid}.{VARIANT}.png", frame_id="f" * 64)


def _truncated_png(catalogs, pid):
    (catalogs.masks / f"{pid}.{VARIANT}.png").write_bytes(b"not a png")


def _header_valid_truncated_png(catalogs, pid):
    """A PNG whose header opens cleanly but whose pixel data is truncated."""
    path = catalogs.masks / f"{pid}.{VARIANT}.png"
    data = path.read_bytes()
    # PNG signature (8) + IHDR chunk (25) is enough for Image.open to
    # report the size; dropping the IDAT and IEND chunks after that makes
    # a lazy open succeed while load() fails on missing pixels.
    path.write_bytes(data[:33])


def _deleted_photo(catalogs, pid):
    catalogs.db.conn.execute("DELETE FROM photos WHERE id=?", (pid,))


@pytest.mark.parametrize(("change", "reason"), [
    (_newer_mask, "already has a newer mask"),
    (_redetected, "on the same box"),
    (_another_runtime, "detected again since, or the box was removed"),
    (_rejected, "not an animal"),
    (_moved_box, "or the box was removed"),
    (_companion, "JPEG companion"),
    (_companion_frame, "JPEG companion"),
    (_changed_file, "file changed"),
    (_missing_png, "mask file missing"),
    (_rewritten_png, "mask file was rewritten"),
    (_truncated_png, "mask file unreadable"),
    (_header_valid_truncated_png, "mask file unreadable"),
    (_deleted_photo, "no longer in the catalog"),
])
def test_leaves_alone_what_no_longer_describes_the_photo(catalogs, restore, capsys, change, reason):
    changed, intact = catalogs.photo("changed.jpg"), catalogs.photo("intact.jpg")
    catalogs.lose()
    change(catalogs, changed)
    before = dict(catalogs.mask(changed)) if catalogs.mask(changed) else None
    catalogs.run(restore, "--apply")
    assert reason in capsys.readouterr().out
    after = catalogs.mask(changed)
    assert (dict(after) if after else None) == before
    if change is not _deleted_photo:
        assert catalogs.row(changed)["quality_score"] is None
    assert catalogs.row(intact)["quality_score"] == .8


def test_a_moved_primary_gets_its_old_mask_back_inactive(catalogs, restore):
    other = {"x": .6, "y": .6, "w": .2, "h": .2}
    pid = catalogs.photo("two.jpg", (BOX, other))
    catalogs.lose()
    catalogs.db.subjects.set_choice(pid, catalogs.boxes[pid][1])
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    assert catalogs.row(pid)["mask_path"] is None
    assert catalogs.row(pid)["quality_score"] is None


def test_an_embedding_from_another_model_is_not_restored(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    catalogs.db.conn.execute("UPDATE photos SET dino_embedding_variant='vit-l14' WHERE id=?", (pid,))
    catalogs.run(restore, "--apply")
    assert catalogs.row(pid)["dino_subject_embedding"] is None
    assert catalogs.row(pid)["quality_score"] == .8


def test_refuses_while_vireo_is_running_and_when_the_backup_is_too_new(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    with pytest.raises(SystemExit, match="Quit Vireo"):
        restore.main([str(catalogs.live), str(catalogs.backup), "--apply"])
    assert catalogs.mask(pid) is None
    with closing(sqlite3.connect(catalogs.backup)) as copy:
        copy.execute("ALTER TABLE detections ADD COLUMN frame_id TEXT")
    with pytest.raises(SystemExit, match="after migration 54"):
        catalogs.run(restore)
    from db import Database
    catalogs.db = Database(str(catalogs.live))


def test_a_raw_without_a_near_full_preview_is_left_alone(catalogs, restore, capsys, monkeypatch):
    """A RAW whose aligned decode rejects the embedded preview cannot reuse the old mask.

    ``DecodedFrame.holds_mask`` refuses an unmarked mask on a demosaiced
    aligned frame (``every_decode_agrees`` is False). The restore must
    detect that at plan time so the projected measurements, which were
    taken against a potentially different rendition, stay off the photo.
    """
    changed = catalogs.photo("rejected.nef", extension=".nef")
    intact = catalogs.photo("intact.jpg")
    catalogs.lose()

    def fake_load_image(path, *, max_size=None, raw_decode=None, **_kwargs):
        if str(path).endswith(".nef"):
            # A RAW whose preview is now rejected: aligned decode demosaics
            # the sensor and marks the rendition as ``sensor``, which is
            # not a shared rendition.
            image = Image.new("RGB", (max_size or 30, max_size or 20))
            image.info["vireo_aligned_rendition"] = "sensor"
            image.info["vireo_native_size"] = image.size
            return image
        image = Image.new("RGB", (30, 20))
        image.info.update(vireo_native_size=image.size, vireo_aligned_rendition="image")
        return image

    monkeypatch.setattr("image_loader.load_image", fake_load_image)
    catalogs.run(restore, "--apply")
    assert "today's decoded frame does not hold the mask" in capsys.readouterr().out
    assert catalogs.row(changed)["mask_path"] is None
    assert catalogs.row(changed)["quality_score"] is None
    assert catalogs.row(intact)["quality_score"] == .8


@pytest.mark.parametrize(("extension", "rendition", "native", "eligible"), [
    (".jpg", "image", (30, 20), True),
    (".jpg", "image", (60, 40), True),
    (".jpg", "image", (30, 21), False),
    (".nef", "embedded-preview", (60, 40), True),
    (".nef", "embedded-preview", (30, 21), False),
])
def test_restore_requires_mask_dimensions_to_match_the_original_frame(
        catalogs, restore, monkeypatch, extension, rendition, native, eligible):
    pid = catalogs.photo("synthetic" + extension, extension=extension)
    catalogs.lose()

    def fake_load_image(path, *, max_size, raw_decode):
        from image_loader import RAW_DECODE_ALIGNED

        assert Path(path).name == "synthetic" + extension
        assert max_size == 30 and raw_decode == RAW_DECODE_ALIGNED
        image = Image.new("RGB", native)
        image.info.update(vireo_native_size=native, vireo_aligned_rendition=rendition)
        return image

    monkeypatch.setattr("image_loader.load_image", fake_load_image)
    catalogs.run(restore)
    assert catalogs.mask(pid) is None, "reporting must not restore anything"
    catalogs.run(restore, "--apply")
    assert (catalogs.mask(pid) is not None) is eligible
    row = catalogs.row(pid)
    assert (row["mask_path"] is not None) is eligible
    for column, value in MEASURED.items():
        assert row[column] == (value if eligible and not column.startswith("subject_detail") else None)


@pytest.mark.parametrize("extension", [".jpg", ".nef"])
@pytest.mark.parametrize("missing", ["folder", "null_path", "empty_path"])
def test_an_unresolved_source_never_bypasses_frame_validation(
        catalogs, restore, monkeypatch, capsys, extension, missing):
    pid = catalogs.photo("synthetic" + extension, extension=extension)
    catalogs.lose()
    if missing == "folder":
        catalogs.db.conn.execute("UPDATE photos SET folder_id=NULL WHERE id=?", (pid,))
    else:
        catalogs.db.conn.execute("UPDATE folders SET path=? WHERE id=?",
                                 (None if missing == "null_path" else "", catalogs.folder))

    def unresolved_decode(*args, **kwargs):
        pytest.fail("an unresolved source must be skipped before decoding")

    monkeypatch.setattr("image_loader.load_image", unresolved_decode)
    catalogs.db.commit()
    catalogs.db.close()
    # Read with SQLite directly: application startup repairs folder metadata
    # and cannot open the deliberately NULL folder path in this fixture.
    for options in ([], ["--apply"]):
        restore.main([str(catalogs.live), str(catalogs.backup), *options])
        with closing(sqlite3.connect(catalogs.live)) as live:
            live.row_factory = sqlite3.Row
            assert live.execute("SELECT 1 FROM photo_masks WHERE photo_id=?", (pid,)).fetchone() is None
            row = live.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
            assert all(row[key] is None for key in MEASURED)
    assert "source path unavailable" in capsys.readouterr().out


def test_missing_primary_state_keeps_an_eligible_mask_inactive(catalogs, restore):
    pid = catalogs.photo("legacy.jpg")
    catalogs.lose()
    catalogs.db.conn.execute("DELETE FROM photo_subject_state WHERE photo_id=?", (pid,))
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    assert catalogs.row(pid)["mask_path"] is None
    assert all(catalogs.row(pid)[key] is None for key in MEASURED)
    assert catalogs.db.subjects.primary_state(pid) is None
    assert catalogs.db.audit.subject_findings(.2) == []


@pytest.mark.parametrize("change", ["mtime", "size", "same_metadata_bytes", "missing"])
def test_unrescanned_source_changes_restore_neither_mask_nor_measurements(catalogs, restore, change):
    pid = catalogs.photo("synthetic.jpg")
    catalogs.lose()
    source = catalogs.sources / "synthetic.jpg"
    if change == "mtime":
        os.utime(source, (2, 2))
    elif change == "missing":
        source.unlink()
    else:
        source.write_bytes(b"x" * (101 if change == "size" else 100))
        os.utime(source, (1, 1))
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is None
    assert all(catalogs.row(pid)[key] is None for key in MEASURED)


@pytest.mark.parametrize("changed", [False, True])
def test_unhashed_sources_still_require_matching_metadata(catalogs, restore, changed):
    pid = catalogs.photo("unhashed.jpg")
    catalogs.db.conn.execute("UPDATE photos SET file_hash=NULL WHERE id=?", (pid,))
    catalogs.lose()
    if changed:
        os.utime(catalogs.sources / "unhashed.jpg", (2, 2))
    catalogs.run(restore, "--apply")
    assert (catalogs.mask(pid) is not None) is not changed
    assert (catalogs.row(pid)["quality_score"] == .8) is not changed


def test_source_substitution_during_decode_is_not_restored(catalogs, restore, monkeypatch):
    import image_loader

    pid = catalogs.photo("racing.jpg")
    catalogs.lose()
    decode = image_loader.load_image

    def substitute(path, **options):
        replacement = Path(path).with_suffix(".replacement")
        replacement.write_bytes(b"x" * 100)
        os.utime(replacement, (1, 1))
        replacement.replace(path)
        return decode(path, **options)

    monkeypatch.setattr(image_loader, "load_image", substitute)
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is None
    assert all(catalogs.row(pid)[key] is None for key in MEASURED)


@pytest.mark.parametrize("evidence", ["missing", "other_detection", "quality", "sharpness", "invalid", "matching"])
def test_only_compatible_existing_measurements_allow_projection(catalogs, restore, evidence):
    other = {"x": .6, "y": .6, "w": .2, "h": .2}
    pid = catalogs.photo("evidence.jpg", (BOX, other))
    catalogs.lose(measurements=False)
    did = catalogs.boxes[pid][0]
    if evidence != "missing":
        catalogs.db.subjects.save_analyses("synthetic-current-source", [{
            "detection_id": catalogs.boxes[pid][1] if evidence == "other_detection" else did,
            "crop": BOX, "quality_score": .7 if evidence == "quality" else .8, "exposure_ev": 0,
            "features": {"subject_sharpness": 8.0 if evidence == "sharpness" else 9.0}}])
    if evidence == "invalid":
        catalogs.db.conn.execute("UPDATE detection_subjects SET features='not json' WHERE detection_id=?", (did,))
    before = catalogs.db.subjects.analyses_for_photo(pid)
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    row = catalogs.row(pid)
    assert (row["mask_path"] is not None) is (evidence == "matching")
    for column, value in MEASURED.items():
        assert row[column] == (value if evidence == "matching" and not column.startswith("subject_detail") else None)
    assert catalogs.db.subjects.analyses_for_photo(pid) == before, "never fabricate or replace measurement evidence"
    assert catalogs.db.audit.subject_findings(.2) == []


def test_a_failure_part_way_changes_nothing(catalogs, restore, monkeypatch):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    monkeypatch.setattr(restore, "restore_projections", lambda *_args: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        catalogs.run(restore, "--apply")
    from db import Database
    catalogs.db = Database(str(catalogs.live))
    assert catalogs.mask(pid) is None
