"""Randomized sequences of the operations that touch users' original photos.

Every other file-safety test in the suite is a hand-written scenario that pins
one bug somebody already found. This module instead lets Hypothesis drive
random sequences of the real operations against a scratch filesystem and a
scratch catalog, and after every step checks the promises a photo library has
to keep no matter what order things happen in:

1. **No photo copy is lost.** Each image copy on disk before a step remains
   somewhere afterwards (library, card, user folder or Trash), unless that
   step permanently deleted that copy. Identical copies count separately.
2. **No file is silently overwritten.** A path that held an image before a
   step and still exists afterwards holds the same bytes.
3. **The catalog tells the truth.** Every photo row points at a file that
   exists, whose bytes match the row's recorded hash, and no two rows claim
   the same file.
4. **A finished import catalogs the whole card.** After an import that
   reports no failures and was not interrupted, every image on that card is
   represented by a catalog row whose file has the same bytes.
5. **No photo drops out of a workspace.** A photo a workspace could see
   before a step is still visible to it afterwards, unless the step deleted
   the photo. (Moves rewrite folder links; this is where that goes wrong.)

Imports are interrupted two ways: a cancel, which the job handles, and a
crash, where an exception escapes mid-batch the way a killed process stops
mid-batch, after some files have landed and before they are cataloged.

When a sequence breaks one of these, Hypothesis shrinks it to the shortest
sequence that still fails and prints it as Python calls, which can be pasted
in as an ordinary regression test.

Budgets: the default profile (40 sequences of up to 15 steps) takes seconds
and is derandomized, so a red PR run reproduces locally with the same
command.
``VIREO_INVARIANTS_PROFILE=nightly`` runs a much larger random search; see
``.github/workflows/photo-safety-nightly.yml``.

What is stubbed, and why:

- ExifTool: tests must not depend on it (CI has none). Images carry their
  capture time in Pillow-written EXIF, which is what the import's duplicate
  gate and folder planning read first.
- Trash: the real helper uses send2trash / Finder and would touch the
  machine's actual Trash. The stub moves files into a scratch ``trash/``
  directory, which counts as "still on disk" for invariant 1.
- Nothing else. Copies, verification, moves (including the real rsync when
  it is installed), scans and catalog writes are the production code.
"""

import hashlib
import os
import random
import shutil
import sys
import tempfile
from collections import Counter
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import HealthCheck, event, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402
from hypothesis.stateful import (  # noqa: E402
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)
from PIL import ExifTags, Image  # noqa: E402

IMAGE_EXTENSIONS = (".jpg", ".jpeg")
CARDS = ("card_a", "card_b")
# A small name pool on purpose: different photos sharing a camera filename is
# the everyday case that collision handling exists for.
CAMERA_NAMES = tuple(f"DSC_{i:04d}.jpg" for i in range(1, 7))
# Capture days spread photos over a few destination folders.
CAPTURE_DAYS = (
    datetime(2026, 5, 1, 9, 0, 0),
    datetime(2026, 5, 2, 9, 0, 0),
    datetime(2026, 6, 15, 9, 0, 0),
)

settings.register_profile(
    "pr",
    max_examples=40,
    stateful_step_count=15,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    print_blob=True,
)
settings.register_profile(
    "nightly",
    max_examples=200,
    stateful_step_count=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    print_blob=True,
)
PROFILE = os.environ.get("VIREO_INVARIANTS_PROFILE", "pr")


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _error_kind(error):
    """An operation's error message with paths and numbers stripped, for events."""
    import re

    text = error.get("error") if isinstance(error, dict) else str(error)
    text = re.sub(r"/[^\s'\"]+", "<path>", str(text))
    return re.sub(r"\d+", "N", text)[:120]


def _write_photo(path, seed, captured_at):
    """A small JPEG whose bytes are unique to ``seed``, with EXIF capture time."""
    rng = random.Random(seed)
    img = Image.new("RGB", (24, 24))
    img.putdata([
        (rng.randrange(256), rng.randrange(256), rng.randrange(256))
        for _ in range(24 * 24)
    ])
    exif = img.getexif()
    exif[ExifTags.Base.DateTimeOriginal] = captured_at.strftime("%Y:%m:%d %H:%M:%S")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    img.save(path, exif=exif, quality=90)
    ts = captured_at.timestamp()
    os.utime(path, (ts, ts))


class _SimulatedCrash(BaseException):
    """Stands in for the process dying. ``BaseException`` so the import's own
    ``except Exception`` cleanup cannot catch it, as it could not catch a kill."""


class _Runner:
    """JobRunner stand-in.

    ``cancel_after`` progress events cancels the job; ``crash_after`` progress
    events raises ``_SimulatedCrash`` out of the job instead.
    """

    def __init__(self, cancel_after=None, crash_after=None):
        self.cancel_after = cancel_after
        self.crash_after = crash_after
        self.progress_events = 0
        self.cancelled = set()

    def push_event(self, job_id, event_type, data):
        if event_type == "progress":
            self.progress_events += 1
            if self.crash_after is not None and self.progress_events >= self.crash_after:
                raise _SimulatedCrash()
            if self.cancel_after is not None and self.progress_events >= self.cancel_after:
                self.cancelled.add(job_id)

    def set_steps(self, job_id, steps):
        pass

    def update_step(self, job_id, step_id, **kwargs):
        pass

    def is_cancelled(self, job_id):
        return job_id in self.cancelled

    def cancellation_requested(self, job_id):
        return job_id in self.cancelled

    def pause_requested(self, job_id):
        return False

    def wait_if_paused(self, job_id, *, publish_paused=False):
        return job_id in self.cancelled

    def checkpoint_live_jobs(self):
        return 0

    def flush_partial_result(self, job, cancel_check=None):
        return True


class PhotoSafetyMachine(RuleBasedStateMachine):
    """One scratch library per example; rules are the user-visible operations."""

    def __init__(self):
        super().__init__()
        import metadata
        import scanner

        # No ExifTool (see the module docstring). Patched per example and
        # restored in teardown, since this runs as a unittest TestCase.
        self._restore = [
            (scanner, "extract_metadata", scanner.extract_metadata),
            (metadata, "extract_metadata", metadata.extract_metadata),
        ]
        for module, name, _ in self._restore:
            setattr(module, name, lambda *a, **k: {})
        # Resolved, because the catalog stores resolved folder paths (macOS
        # temp dirs live under /var -> /private/var).
        self.root = os.path.realpath(tempfile.mkdtemp(prefix="vireo-invariants-"))
        self.cards = {name: os.path.join(self.root, "cards", name) for name in CARDS}
        self.library = os.path.join(self.root, "library")
        self.own_folder = os.path.join(self.root, "own", "shoot")
        self.moved = os.path.join(self.root, "moved")
        self.trash = os.path.join(self.root, "trash")
        self.thumbs = os.path.join(self.root, "thumbs")
        for path in (*self.cards.values(), self.library, self.own_folder,
                     self.moved, self.trash, self.thumbs):
            os.makedirs(path, exist_ok=True)
        self.db_path = os.path.join(self.root, "catalog.db")

        from db import Database

        self.db = Database(self.db_path)
        self.default_ws = self.db._active_workspace_id
        self.other_ws = self.db.create_workspace("Other")
        self.db.set_active_workspace(self.default_ws)
        self.ws_id = self.default_ws
        self.next_seed = 0
        self.next_job = 0
        self.capture_offset = 0
        # Number of copies of each digest a permanent delete may remove.
        self.allowed_to_vanish = Counter()
        self.snapshot = self._disk_snapshot()
        self.visibility = self._workspace_visibility()
        # Set by steps built on ``move.move_photos``; see
        # ``test_move_photos_keeps_photo_visible_in_sharing_workspaces``.
        self.visibility_known_broken = False

    def teardown(self):
        try:
            self.db.close()
        finally:
            for module, name, original in self._restore:
                setattr(module, name, original)
            shutil.rmtree(self.root, ignore_errors=True)

    # -- helpers -------------------------------------------------------------

    def _new_capture_time(self, day_index):
        # Unique to the second, so the import's metadata-first duplicate gate
        # (filename, size, capture time) never matches two different photos.
        self.capture_offset += 1
        return CAPTURE_DAYS[day_index] + timedelta(seconds=self.capture_offset)

    def _disk_snapshot(self):
        """``{path: sha256}`` for every image under the scratch root."""
        found = {}
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames[:] = [d for d in dirnames if d != "thumbs" and not d.startswith(".")]
            for name in filenames:
                if name.lower().endswith(IMAGE_EXTENSIONS):
                    path = os.path.join(dirpath, name)
                    found[path] = _sha256(path)
        return found

    def _catalog_rows(self):
        return [
            dict(r) for r in self.db.conn.execute(
                "SELECT p.id, p.filename, p.file_hash, f.path AS folder_path "
                "FROM photos p JOIN folders f ON f.id = p.folder_id"
            )
        ]

    def _visible_photo_ids(self):
        """Photos the active workspace can see: the only ones its routes act on."""
        return [
            r[0] for r in self.db.conn.execute(
                "SELECT p.id FROM photos p JOIN workspace_folders wf "
                "ON wf.folder_id = p.folder_id AND wf.workspace_id = ? ORDER BY p.id",
                (self.ws_id,),
            )
        ]

    def _job(self):
        self.next_job += 1
        return {
            "id": f"import-{self.next_job}",
            "type": "import",
            "status": "running",
            "progress": {"current": 0, "total": 0, "current_file": ""},
            "result": None,
            "errors": [],
            "config": {},
            "workspace_id": self.ws_id,
        }

    def _card_images(self, card):
        return sorted(
            n for n in os.listdir(self.cards[card])
            if n.lower().endswith(IMAGE_EXTENSIONS)
        )

    def _loaded_cards(self):
        return [card for card in CARDS if self._card_images(card)]

    def _workspace_visibility(self):
        """``{photo_id: {workspace ids that can see it}}``."""
        visible = {}
        for row in self.db.conn.execute(
            "SELECT p.id, wf.workspace_id FROM photos p "
            "JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
        ):
            visible.setdefault(row[0], set()).add(row[1])
        return visible

    def _photo_folders(self):
        """Folders with photos that the active workspace can see."""
        return [
            dict(r) for r in self.db.conn.execute(
                "SELECT f.id, f.path FROM folders f "
                "JOIN workspace_folders wf ON wf.folder_id = f.id AND wf.workspace_id = ? "
                "WHERE EXISTS (SELECT 1 FROM photos WHERE folder_id = f.id) "
                "ORDER BY f.id",
                (self.ws_id,),
            )
        ]

    def _has_visible_photos(self):
        return bool(self._visible_photo_ids())

    def _cataloged_hashes(self):
        hashes = set()
        for row in self._catalog_rows():
            path = os.path.join(row["folder_path"], row["filename"])
            if os.path.isfile(path):
                hashes.add(_sha256(path))
        return hashes

    def _deleter(self):
        import app as app_module
        from services.photo_deletion import PhotoDeletion

        return PhotoDeletion(
            {"THUMB_CACHE_DIR": self.thumbs},
            chunked=app_module._chunked,
            trash_paths=self._trash_paths,
            snapshot_parent_device=app_module._snapshot_parent_device,
            path_confirmed_gone=app_module._path_confirmed_gone,
        )

    def _trash_paths(self, filepaths, progress_callback=None, already_missing_out=None,
                     **_kwargs):
        moved, successful, failures = 0, set(), []
        for path in dict.fromkeys(filepaths):
            if not os.path.exists(path):
                successful.add(path)
                if already_missing_out is not None:
                    already_missing_out.add(path)
                continue
            target = os.path.join(self.trash, f"{len(os.listdir(self.trash))}-{os.path.basename(path)}")
            shutil.move(path, target)
            moved += 1
            successful.add(path)
        return moved, successful, failures

    # -- environment: new photos appear --------------------------------------

    @initialize(
        names=st.lists(st.sampled_from(CAMERA_NAMES), min_size=1, max_size=4),
        day=st.integers(0, len(CAPTURE_DAYS) - 1),
    )
    def first_shoot(self, names, day):
        self.shoot_onto_card(CARDS[0], names, day)

    @rule(
        card=st.sampled_from(CARDS),
        names=st.lists(st.sampled_from(CAMERA_NAMES), min_size=1, max_size=3),
        day=st.integers(0, len(CAPTURE_DAYS) - 1),
    )
    def shoot_onto_card(self, card, names, day):
        """New photos land on a card; a reused name means a different photo
        replaces the card file, as when a card is formatted and reshot."""
        for name in dict.fromkeys(names):
            self.next_seed += 1
            _write_photo(os.path.join(self.cards[card], name), self.next_seed,
                         self._new_capture_time(day))
        # Replacing a file on the card is the user's action, not Vireo's.
        self.snapshot = self._disk_snapshot()

    @rule(card=st.sampled_from(CARDS), data=st.data())
    def copy_existing_photo_onto_card(self, card, data):
        """A byte-identical duplicate of a photo already on disk."""
        existing = sorted(p for p in self.snapshot if os.sep + "trash" + os.sep not in p)
        if not existing:
            return
        source = data.draw(st.sampled_from(existing))
        name = f"COPY_{self.next_seed:04d}.jpg"
        self.next_seed += 1
        shutil.copy2(source, os.path.join(self.cards[card], name))
        self.snapshot = self._disk_snapshot()

    @rule(count=st.integers(1, 3), day=st.integers(0, len(CAPTURE_DAYS) - 1))
    def add_to_own_folder(self, count, day):
        for _ in range(count):
            self.next_seed += 1
            _write_photo(
                os.path.join(self.own_folder, f"IMG_{self.next_seed:04d}.jpg"),
                self.next_seed, self._new_capture_time(day),
            )
        self.snapshot = self._disk_snapshot()

    # -- Vireo operations ----------------------------------------------------

    def _run_import(self, card, runner):
        from import_job import ImportParams, run_import_job

        params = ImportParams(sources=[self.cards[card]], destination=self.library)
        result = run_import_job(self._job(), runner, self.db_path, self.ws_id, params)
        # The job writes through its own connection; end any transaction this
        # one holds so the checks below read what the job committed.
        self.db.conn.commit()
        return result

    @precondition(lambda self: self._loaded_cards())
    @rule(data=st.data())
    def import_card(self, data):
        card = data.draw(st.sampled_from(self._loaded_cards()), label="card")
        card_hashes = {
            _sha256(os.path.join(self.cards[card], n)) for n in self._card_images(card)
        }
        result = self._run_import(card, _Runner())
        event(f"import: copied={bool(result.get('copied'))} "
              f"failed={bool(result.get('failed'))} cancelled={bool(result.get('cancelled'))}")
        if result.get("cancelled") or result.get("failed"):
            return
        missing = card_hashes - self._cataloged_hashes()
        assert not missing, (
            f"import of {card} reported success (copied={result.get('copied')}, "
            f"skipped={result.get('skipped')}) but {len(missing)} of its photos "
            "are not in the catalog"
        )

    @precondition(lambda self: self._loaded_cards())
    @rule(data=st.data(), crash_after=st.integers(1, 8))
    def import_card_crashes(self, data, crash_after):
        """The app dies part-way through an import; the next import resumes."""
        card = data.draw(st.sampled_from(self._loaded_cards()), label="card")
        try:
            self._run_import(card, _Runner(crash_after=crash_after))
        except _SimulatedCrash:
            event("import crashed mid-run")
            # A dead process releases its locks and drops its open
            # transaction; collect the job's abandoned connection.
            import gc

            gc.collect()
            self.db.conn.rollback()
        else:
            event("import finished before the crash point")

    @rule()
    def switch_workspace(self):
        self.ws_id = self.other_ws if self.ws_id == self.default_ws else self.default_ws
        self.db.set_active_workspace(self.ws_id)

    @precondition(lambda self: self._has_visible_photos())
    @rule(data=st.data())
    def share_folder_with_other_workspace(self, data):
        folder = data.draw(st.sampled_from(self._photo_folders()))
        other = self.other_ws if self.ws_id == self.default_ws else self.default_ws
        self.db.add_workspace_folder(other, folder["id"])

    @precondition(lambda self: self._loaded_cards())
    @rule(data=st.data(), cancel_after=st.integers(1, 6))
    def import_card_interrupted(self, data, cancel_after):
        """The user cancels (or the app stops) part-way through an import."""
        card = data.draw(st.sampled_from(self._loaded_cards()), label="card")
        result = self._run_import(card, _Runner(cancel_after=cancel_after))
        event(f"interrupted import: cancelled={bool(result.get('cancelled'))}")

    @rule()
    def scan_own_folder(self):
        import scanner

        scanner.scan(os.path.dirname(self.own_folder), self.db, incremental=True,
                     thumb_cache_dir=self.thumbs)

    @precondition(lambda self: self._has_visible_photos())
    @rule(data=st.data())
    def move_some_photos(self, data):
        import move

        self.visibility_known_broken = True

        visible = self._visible_photo_ids()
        if not visible:
            return
        rows = self._catalog_rows()
        chosen = data.draw(st.lists(st.sampled_from(visible),
                                    min_size=1, max_size=3, unique=True))
        dest = data.draw(st.sampled_from([
            os.path.join(self.moved, "picked"),
            os.path.join(self.library, "picked"),
            self.own_folder,
        ]))
        os.makedirs(dest, exist_ok=True)
        result = move.move_photos(self.db, chosen, dest)
        event(f"move_photos: moved={bool(result.get('moved'))} errors={bool(result.get('errors'))}")
        for error in result.get("errors") or []:
            event(f"move_photos error: {_error_kind(error)}")

    @precondition(lambda self: self._has_visible_photos())
    @rule(data=st.data())
    def move_a_folder(self, data):
        import move

        folders = self._photo_folders()
        if not folders:
            return
        folder = data.draw(st.sampled_from(folders))
        dest = data.draw(st.sampled_from([self.moved, os.path.join(self.moved, "nested")]))
        os.makedirs(dest, exist_ok=True)
        # The call shapes the app uses: the Move Folder route (``merge`` from
        # the request), and the Work Locally / NAS archive flows, which merge
        # into folders Vireo already tracks and verify contents first.
        shape = data.draw(st.sampled_from(["route", "route_merge", "archive"]))
        kwargs = {
            "route": {},
            "route_merge": {"merge": True},
            "archive": {"merge": True, "allow_tracked_merge": True, "verify_contents": True},
        }[shape]
        result = move.move_folder(self.db, folder["id"], dest, **kwargs)
        event(f"move_folder: {sorted(k for k, v in (result or {}).items() if v)[:4]}")
        for error in (result or {}).get("errors") or []:
            event(f"move_folder error: {_error_kind(error)}")

    @precondition(lambda self: self._has_visible_photos())
    @rule(data=st.data())
    def move_a_folder_by_date(self, data):
        """Move Folder with a date template: photos re-sorted by capture date."""
        import move

        self.visibility_known_broken = True

        folders = self._photo_folders()
        if not folders:
            return
        folder = data.draw(st.sampled_from(folders))
        dest = os.path.join(self.moved, "by-date")
        os.makedirs(dest, exist_ok=True)
        result = move.move_folder_by_date(self.db, folder["id"], dest, "%Y/%m")
        event(f"move_folder_by_date: {sorted(k for k, v in (result or {}).items() if v)[:4]}")
        for error in (result or {}).get("errors") or []:
            event(f"move_folder_by_date error: {_error_kind(error)}")

    @precondition(lambda self: self._has_visible_photos())
    @rule(data=st.data(), mode=st.sampled_from(["vireo", "disk", "disk_permanent"]))
    def delete_photos(self, data, mode):
        visible = self._visible_photo_ids()
        if not visible:
            return
        rows = self._catalog_rows()
        chosen = data.draw(st.lists(st.sampled_from(visible),
                                    min_size=1, max_size=3, unique=True))
        if mode == "disk_permanent":
            for row in rows:
                if row["id"] in chosen:
                    path = os.path.join(row["folder_path"], row["filename"])
                    if os.path.isfile(path):
                        self.allowed_to_vanish[_sha256(path)] += 1
        result = self._deleter().run_batch_delete(self.db, chosen, mode=mode)
        event(f"delete {mode}: ok={result.get('ok')}")

    # -- invariants ----------------------------------------------------------

    @invariant()
    def no_photo_lost_or_overwritten(self):
        now = self._disk_snapshot()
        overwritten = [
            p for p, digest in self.snapshot.items()
            if p in now and now[p] != digest
        ]
        assert not overwritten, f"files overwritten in place: {overwritten}"
        lost = Counter(self.snapshot.values()) - Counter(now.values()) - self.allowed_to_vanish
        assert not lost, (
            f"{sum(lost.values())} photo copy/copies vanished from disk without a permanent delete: "
            + ", ".join(p for p, d in self.snapshot.items() if d in lost)
        )
        self.snapshot = now
        self.allowed_to_vanish = Counter()

    @invariant()
    def no_photo_drops_out_of_a_workspace(self):
        now = self._workspace_visibility()
        if self.visibility_known_broken:
            # move_photos links its destination only to the active workspace.
            # Remove this once the strict xfail below starts passing.
            self.visibility = now
            self.visibility_known_broken = False
            return
        dropped = {
            pid: sorted(before - now.get(pid, set()))
            for pid, before in self.visibility.items()
            if pid in now and before - now[pid]
        }
        assert not dropped, f"photos lost workspace visibility: {dropped}"
        # A row that disappeared was deleted; catalog_matches_disk and the
        # loss check cover whether that was allowed.
        self.visibility = now

    @invariant()
    def catalog_matches_disk(self):
        seen = {}
        for row in self._catalog_rows():
            path = os.path.join(row["folder_path"], row["filename"])
            assert os.path.isfile(path), (
                f"photo {row['id']} points at {path}, which does not exist"
            )
            if row["file_hash"]:
                assert _sha256(path) == row["file_hash"], (
                    f"photo {row['id']} records a hash that does not match {path}"
                )
            key = os.path.normcase(os.path.normpath(path))
            assert key not in seen, (
                f"photos {seen[key]} and {row['id']} both claim {path}"
            )
            seen[key] = row["id"]


PhotoSafetyMachine.TestCase.settings = settings.get_profile(PROFILE)
TestPhotoSafetyInvariants = PhotoSafetyMachine.TestCase


@pytest.mark.xfail(
    strict=True,
    reason=(
        "move_photos links its destination folder only to the active "
        "workspace, so a photo in a folder shared with another workspace "
        "drops out of that workspace when moved (Move Photos, move rules, "
        "Move Folder with a date template). Found by PhotoSafetyMachine. When "
        "this passes, drop the xfail and the visibility_known_broken carve-out."
    ),
)
def test_move_photos_keeps_photo_visible_in_sharing_workspaces(tmp_path):
    import move
    from db import Database

    shared = tmp_path / "shared"
    picked = tmp_path / "picked"
    picked.mkdir()
    _write_photo(str(shared / "DSC_0001.jpg"), 1, datetime(2026, 5, 1, 9, 0, 1))
    db = Database(str(tmp_path / "catalog.db"))
    try:
        mine = db._active_workspace_id
        theirs = db.create_workspace("Other")
        db.set_active_workspace(mine)
        folder_id = db.add_folder(str(shared), name="shared")
        db.add_workspace_folder(theirs, folder_id)
        photo_id = db.add_photo(
            folder_id=folder_id, filename="DSC_0001.jpg", extension=".jpg",
            file_size=os.path.getsize(shared / "DSC_0001.jpg"), file_mtime=1.0,
        )

        result = move.move_photos(db, [photo_id], str(picked))

        assert result["moved"] == 1
        visible_to = {
            row[0] for row in db.conn.execute(
                "SELECT wf.workspace_id FROM photos p JOIN workspace_folders wf "
                "ON wf.folder_id = p.folder_id WHERE p.id = ?",
                (photo_id,),
            )
        }
        assert visible_to == {mine, theirs}
    finally:
        db.close()


@pytest.mark.parametrize(
    "remaining_paths,allowed_deletions,should_fail",
    [
        (["copy-a.jpg"], 0, True),
        ([], 1, True),
        (["copy-a.jpg"], 1, False),
        (["moved-a.jpg", "moved-b.jpg"], 0, False),
        (["copy-a.jpg", "copy-b.jpg", "new-copy.jpg"], 0, False),
    ],
    ids=["lost-duplicate", "delete-does-not-exempt-other-copy", "intentional-delete",
         "moved-copies", "additional-copy"],
)
def test_loss_invariant_counts_identical_copies(remaining_paths, allowed_deletions, should_fail):
    # Exercise the invariant itself without starting a random filesystem
    # sequence. Both original files deliberately contain identical bytes.
    machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
    machine.snapshot = {"copy-a.jpg": "same-bytes", "copy-b.jpg": "same-bytes"}
    machine.allowed_to_vanish = Counter({"same-bytes": allowed_deletions})
    machine._disk_snapshot = lambda: dict.fromkeys(remaining_paths, "same-bytes")
    if should_fail:
        with pytest.raises(AssertionError, match="vanished from disk"):
            machine.no_photo_lost_or_overwritten()
    else:
        machine.no_photo_lost_or_overwritten()
        assert machine.snapshot == dict.fromkeys(remaining_paths, "same-bytes")
        assert not machine.allowed_to_vanish
