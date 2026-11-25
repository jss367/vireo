"""The one-off restore puts back only measurements that still describe the photo."""
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
#: Measured from the mask, and so restored with it.
FROM_MASK = {"subject_tenengrad": 120.0, "bg_tenengrad": 12.0, "crop_complete": 1.0,
             "dino_subject_embedding": b"embedding"}
#: Measured elsewhere, by readings that differ from today's: left for Process.
REMEASURED = {"quality_score": .8, "subject_sharpness": 9.0, "subject_detail": 5000.0, "subject_detail_px": 300.0,
              "eye_x": .4, "eye_tenengrad": 7.0, "eye_kp_fingerprint": "eyes"}
MEASURED = FROM_MASK | REMEASURED


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
        self.db.conn.execute("UPDATE photos SET camera_model='CAMERA', width=60, height=40 WHERE id=?", (pid,))
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
        self.db.subjects.save_analyses("older-analysis", [{
            "detection_id": dids[0], "crop": BOX, "quality_score": MEASURED["quality_score"], "exposure_ev": 0,
            "features": {"subject_sharpness": MEASURED["subject_sharpness"]}}])
        self.boxes[pid] = dids
        return pid

    def lose(self):
        from schema import _retire_unbound_detector_caches

        self.db.commit()
        with closing(sqlite3.connect(self.backup)) as copy:
            self.db.conn.backup(copy)
            copy.execute("ALTER TABLE detections DROP COLUMN frame_id")
            copy.commit()
        _retire_unbound_detector_caches(self.db.conn)
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

    def active(self, pid):
        return self.row(pid)["mask_path"] is not None

    def mask(self, pid):
        return self.db.masks_features.get_mask(pid, VARIANT)


@pytest.fixture
def catalogs(tmp_path):
    made = Catalogs(tmp_path)
    yield made
    made.db.close()


def test_restores_the_mask_and_what_was_measured_from_it(catalogs, restore, capsys):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    assert catalogs.row(pid)["mask_path"] is None
    assert catalogs.run(restore) == 0
    assert "1 masks on 1 photos can be restored" in capsys.readouterr().out
    assert catalogs.mask(pid) is None, "a report must not write"
    assert not list(catalogs.live.parent.glob("*.pre-restore-*"))

    catalogs.run(restore, "--apply")
    assert "Restored 1 masks; 1 are active again" in capsys.readouterr().out
    row, mask = catalogs.row(pid), catalogs.mask(pid)
    assert {c: row[c] for c in MEASURED} == FROM_MASK | dict.fromkeys(REMEASURED)
    assert (row["mask_path"], row["active_mask_variant"]) == (mask["path"], VARIANT)
    assert (mask["subject_tenengrad"], mask["subject_detail"], mask["subject_detail_px"]) == (120, None, None)
    saved, = catalogs.live.parent.glob("*.pre-restore-*")
    with closing(sqlite3.connect(saved)) as copy:
        assert copy.execute("SELECT COUNT(*) FROM photo_masks").fetchone() == (0,)
    catalogs.run(restore, "--apply")
    assert "0 masks on 0 photos" in capsys.readouterr().out
    assert len(list(catalogs.live.parent.glob("*.pre-restore-*"))) == 1, "nothing to write, nothing to save"


def test_box_measurements_come_from_process_and_keep_the_mask_active(catalogs, restore):
    """No older box score returns, and measuring the box again leaves the restored mask alone."""
    from subjects import sync_primary

    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    catalogs.run(restore, "--apply")
    assert catalogs.db.subjects.analyses_for_photo(pid) == {}
    assert catalogs.db.audit.subject_findings(.2) == []
    catalogs.db.subjects.save_analyses("today", [{
        "detection_id": catalogs.boxes[pid][0], "crop": BOX, "quality_score": .6, "exposure_ev": 0,
        "features": {"subject_sharpness": 3.0}}])
    sync_primary(catalogs.db, pid, min_conf=.2)
    row = catalogs.row(pid)
    assert (row["quality_score"], row["subject_sharpness"]) == (.6, 3.0)
    assert {c: row[c] for c in FROM_MASK} == FROM_MASK
    assert row["mask_path"] == catalogs.mask(pid)["path"]
    assert catalogs.db.audit.subject_findings(.2) == []


def test_a_box_measured_since_is_kept(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    catalogs.db.conn.execute("UPDATE photos SET quality_score=.6, subject_sharpness=3 WHERE id=?", (pid,))
    catalogs.run(restore, "--apply")
    row = catalogs.row(pid)
    assert (row["quality_score"], row["subject_sharpness"], catalogs.active(pid)) == (.6, 3.0, True)


def test_user_decisions_made_since_are_untouched(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    catalogs.db.conn.execute("UPDATE photos SET rating=4, flag='flagged' WHERE id=?", (pid,))
    catalogs.run(restore, "--apply")
    row = catalogs.row(pid)
    assert (row["rating"], row["flag"], catalogs.active(pid)) == (4, "flagged", True)


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


def _source(catalogs, name="changed.jpg"):
    return catalogs.sources / name


def _edited_original(catalogs, pid):
    _source(catalogs).write_bytes(b"x" * 101)
    os.utime(_source(catalogs), (1, 1))


def _touched_original(catalogs, pid):
    os.utime(_source(catalogs), (2, 2))


def _missing_original(catalogs, pid):
    _source(catalogs).unlink()


def _directory_original(catalogs, pid):
    _source(catalogs).unlink()
    _source(catalogs).mkdir()


def _unknown_folder(catalogs, pid):
    catalogs.db.conn.execute("UPDATE photos SET folder_id=NULL WHERE id=?", (pid,))


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
    (_edited_original, "changed on disk"),
    (_touched_original, "changed on disk"),
    (_missing_original, "not found on disk"),
    (_directory_original, "not found on disk"),
    (_unknown_folder, "path unknown"),
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
        assert not catalogs.active(changed)
    assert catalogs.active(intact)


def test_a_moved_primary_gets_its_old_mask_back_inactive(catalogs, restore):
    other = {"x": .6, "y": .6, "w": .2, "h": .2}
    pid = catalogs.photo("two.jpg", (BOX, other))
    catalogs.lose()
    catalogs.db.subjects.set_choice(pid, catalogs.boxes[pid][1])
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    assert not catalogs.active(pid)


@pytest.mark.parametrize(("recorded_then", "recorded_now", "active"), [
    (True, True, True), (False, False, True), (False, True, True), (True, False, False)])
def test_a_primary_vireo_has_forgotten_keeps_the_mask_inactive(
        catalogs, restore, recorded_then, recorded_now, active):
    """A catalog that never recorded a primary is not one that lost it."""
    pid = catalogs.photo("a.jpg")
    if not recorded_then:
        catalogs.db.subjects.clear_primary_state(pid)
    catalogs.lose()
    if recorded_now:
        catalogs.db.subjects.set_primary_state(pid, catalogs.boxes[pid][0], .2)
    else:
        catalogs.db.subjects.clear_primary_state(pid)
        # The migration finds a photo's mask either through its primary or
        # through its active mask; both leave the same cleared photo.
        catalogs.db.subjects.clear_primary_features(pid)
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    assert catalogs.active(pid) is active
    assert catalogs.db.audit.subject_findings(.2) == []


def test_a_primary_below_the_floor_keeps_its_old_mask_inactive(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.db.conn.execute("UPDATE detections SET detector_confidence=.15 WHERE photo_id=?", (pid,))
    catalogs.lose()
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    assert not catalogs.active(pid)


def test_an_embedding_from_another_model_is_not_restored(catalogs, restore):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    catalogs.db.conn.execute("UPDATE photos SET dino_embedding_variant='vit-l14' WHERE id=?", (pid,))
    catalogs.run(restore, "--apply")
    assert catalogs.row(pid)["dino_subject_embedding"] is None
    assert catalogs.active(pid)


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


def _decodes(monkeypatch, decode):
    """Answer each aligned decode with ``decode(name)``: (native size, rendition), or None."""
    asked = []

    def load_image(path, *, max_size, raw_decode):
        from image_loader import RAW_DECODE_ALIGNED

        assert max_size == 30 and raw_decode == RAW_DECODE_ALIGNED
        asked.append(Path(path).name)
        answer = decode(Path(path).name)
        if answer is None:
            return None
        image = Image.new("RGB", answer[0])
        image.info.update(vireo_native_size=answer[0], vireo_aligned_rendition=answer[1])
        return image

    monkeypatch.setattr("image_loader.load_image", load_image)
    return asked


@pytest.mark.parametrize(("extension", "rendition", "native", "eligible"), [
    (".jpg", "image", (30, 20), True),
    (".jpg", "image", (60, 40), True),
    (".jpg", "image", (30, 21), False),
    (".nef", "embedded-preview", (60, 40), True),
    (".nef", "embedded-preview", (30, 21), False),
    (".nef", "sensor", (60, 40), False),
])
def test_a_mask_must_lie_in_todays_frame(catalogs, restore, monkeypatch, capsys, extension, rendition, native, eligible):
    pid = catalogs.photo("synthetic" + extension, extension=extension)
    catalogs.lose()
    _decodes(monkeypatch, lambda _name: (native, rendition))
    catalogs.run(restore)
    assert catalogs.mask(pid) is None, "a report must not write"
    catalogs.run(restore, "--apply")
    assert (catalogs.mask(pid) is not None) is eligible
    assert catalogs.active(pid) is eligible
    assert ("does not hold the mask" in capsys.readouterr().out) is not eligible


def test_one_refused_sample_leaves_its_whole_group_alone(catalogs, restore, monkeypatch):
    """RAWs of one camera and frame share an answer; another camera's does not."""
    raws = [catalogs.photo(f"{i}.nef", extension=".nef") for i in range(30)]
    other = catalogs.photo("other.nef", extension=".nef")
    catalogs.db.conn.execute("UPDATE photos SET camera_model='OTHER' WHERE id=?", (other,))
    catalogs.lose()
    refused = {"14.nef"}
    asked = _decodes(monkeypatch, lambda name: ((60, 40), "sensor" if name in refused else "embedded-preview"))
    catalogs.run(restore, "--apply")
    assert "14.nef" in asked and "other.nef" in asked
    assert len(asked) == restore.SAMPLES_PER_GROUP + 1, "a sample, not every original"
    assert not any(catalogs.mask(pid) for pid in raws)
    assert catalogs.active(other)


def test_an_undecodable_sample_does_not_answer_for_its_group(catalogs, restore, monkeypatch):
    raws = [catalogs.photo(f"{i}.nef", extension=".nef") for i in range(3)]
    lost = catalogs.photo("lost.nef", extension=".nef")
    catalogs.db.conn.execute("UPDATE photos SET camera_model='OTHER' WHERE id=?", (lost,))
    catalogs.lose()
    _decodes(monkeypatch, lambda name: None if name in ("1.nef", "lost.nef") else ((60, 40), "embedded-preview"))
    catalogs.run(restore, "--apply")
    assert all(catalogs.active(pid) for pid in raws)
    assert catalogs.mask(lost) is None, "a group with no decoded sample has no answer"


def test_each_ordinary_image_answers_for_itself(catalogs, restore, monkeypatch):
    good, bad = catalogs.photo("good.jpg"), catalogs.photo("bad.jpg")
    catalogs.lose()
    _decodes(monkeypatch, lambda name: ((30, 21) if name == "bad.jpg" else (30, 20), "image"))
    catalogs.run(restore, "--apply")
    assert (catalogs.active(good), catalogs.mask(bad)) == (True, None)


def test_a_failure_part_way_changes_nothing(catalogs, restore, monkeypatch):
    pid = catalogs.photo("a.jpg")
    catalogs.lose()
    monkeypatch.setattr(restore, "restore_projections", lambda *_args: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        catalogs.run(restore, "--apply")
    from db import Database
    catalogs.db = Database(str(catalogs.live))
    assert catalogs.mask(pid) is None
