"""The one-off restore puts back only measurements that still describe the photo."""
import importlib.util
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
def restore():
    spec = importlib.util.spec_from_file_location(
        "restore_subject_analysis", ROOT / "scripts" / "restore_subject_analysis.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
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
        self.folder = workspace_folders.add_folder(self.db, str(tmp_path / "photos"))
        self.boxes = {}

    def photo(self, name, boxes=(BOX,), *, runtime="legacy"):
        from services import detection_store, photo_ingest

        pid = photo_ingest.add_photo(self.db, self.folder, name, ".nef", 100, 1.0)
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
        self.boxes[pid] = dids
        return pid

    def lose(self):
        """Take the snapshot, then do to the live catalog what migration 54 did."""
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

    def mask(self, pid):
        return self.db.masks_features.get_mask(pid, VARIANT)


@pytest.fixture
def catalogs(tmp_path):
    made = Catalogs(tmp_path)
    yield made
    made.db.close()


def test_restores_the_mask_and_its_measurements_but_not_the_detail(catalogs, restore, capsys):
    pid = catalogs.photo("a.nef")
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
    pid = catalogs.photo("a.nef")
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
    (_deleted_photo, "no longer in the catalog"),
])
def test_leaves_alone_what_no_longer_describes_the_photo(catalogs, restore, capsys, change, reason):
    changed, intact = catalogs.photo("changed.nef"), catalogs.photo("intact.nef")
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
    pid = catalogs.photo("two.nef", (BOX, other))
    catalogs.lose()
    catalogs.db.subjects.set_choice(pid, catalogs.boxes[pid][1])
    catalogs.run(restore, "--apply")
    assert catalogs.mask(pid) is not None
    assert catalogs.row(pid)["mask_path"] is None
    assert catalogs.row(pid)["quality_score"] is None


def test_an_embedding_from_another_model_is_not_restored(catalogs, restore):
    pid = catalogs.photo("a.nef")
    catalogs.lose()
    catalogs.db.conn.execute("UPDATE photos SET dino_embedding_variant='vit-l14' WHERE id=?", (pid,))
    catalogs.run(restore, "--apply")
    assert catalogs.row(pid)["dino_subject_embedding"] is None
    assert catalogs.row(pid)["quality_score"] == .8


def test_refuses_while_vireo_is_running_and_when_the_backup_is_too_new(catalogs, restore):
    pid = catalogs.photo("a.nef")
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


def test_a_failure_part_way_changes_nothing(catalogs, restore, monkeypatch):
    pid = catalogs.photo("a.nef")
    catalogs.lose()
    monkeypatch.setattr(restore, "restore_projections", lambda *_args: 1 / 0)
    with pytest.raises(ZeroDivisionError):
        catalogs.run(restore, "--apply")
    from db import Database
    catalogs.db = Database(str(catalogs.live))
    assert catalogs.mask(pid) is None
