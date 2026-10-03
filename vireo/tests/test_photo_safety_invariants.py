"""Randomized sequences of the operations that touch users' original photos.

Every other file-safety test in the suite is a hand-written scenario that pins
one bug somebody already found. This module instead lets Hypothesis drive
random sequences of the real operations against a scratch filesystem and a
scratch catalog, and after every step checks the promises a photo library has
to keep no matter what order things happen in:

1. **No photo copy is lost.** Each image copy on disk before a step remains
   somewhere afterwards (library, card, user folder or Trash), unless that
   step permanently deleted that copy or a merge deliberately consolidated it
   into an existing, byte-verified identical file. Other copies count separately.
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
        self.known_move_visibility_losses = {}
        self.allowed_catalog_deletions = set()

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
            "LEFT JOIN workspace_folders wf ON wf.folder_id = p.folder_id"
        ):
            workspaces = visible.setdefault(row[0], set())
            if row[1] is not None:
                workspaces.add(row[1])
        return visible

    def _record_known_move_visibility_losses(self, before_rows, chosen):
        # The pinned bug permits only a genuinely moved photo to lose its
        # old non-active workspace links. Active and unrelated access stays
        # checked, including after a partial or refused move.
        before_paths = {r["id"]: os.path.join(r["folder_path"], r["filename"]) for r in before_rows}
        after_paths = {
            r["id"]: os.path.join(r["folder_path"], r["filename"]) for r in self._catalog_rows()
        }
        for pid in chosen:
            if pid in after_paths and before_paths[pid] != after_paths[pid]:
                self.known_move_visibility_losses[pid] = self.visibility.get(pid, set()) - {self.ws_id}

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
        visible = set(self._visible_photo_ids())
        for row in self._catalog_rows():
            if row["id"] not in visible:
                continue
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
        self._check_finished_import(result, card, card_hashes)

    def _check_finished_import(self, result, card, card_hashes):
        if not result.get("ok") or result.get("cancelled") or result.get("failed"):
            return
        missing = card_hashes - self._cataloged_hashes()
        assert not missing, (
            f"import of {card} reported success (copied={result.get('copied')}, "
            f"skipped={result.get('skipped')}) but {len(missing)} of its photos "
            "are not visible in the active workspace catalog"
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
        self._record_known_move_visibility_losses(rows, chosen)
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
        before_disk = self._disk_snapshot()
        before_rows = self._catalog_rows()
        result = move.move_folder(self.db, folder["id"], dest, **kwargs)
        if kwargs.get("merge"):
            target = (result or {}).get("merged_into_existing") or os.path.join(dest, os.path.basename(folder["path"]))
            self._record_verified_consolidations(folder["path"], target, before_disk, before_rows, result)
        event(f"move_folder: {sorted(k for k, v in (result or {}).items() if v)[:4]}")
        for error in (result or {}).get("errors") or []:
            event(f"move_folder error: {_error_kind(error)}")

    @precondition(lambda self: self._has_visible_photos())
    @rule(data=st.data())
    def move_a_folder_by_date(self, data):
        """Move Folder with a date template: photos re-sorted by capture date."""
        import move

        folders = self._photo_folders()
        if not folders:
            return
        folder = data.draw(st.sampled_from(folders))
        dest = os.path.join(self.moved, "by-date")
        os.makedirs(dest, exist_ok=True)
        before_rows = self._catalog_rows()
        chosen = [r["id"] for r in before_rows if r["folder_path"] == folder["path"]]
        result = move.move_folder_by_date(self.db, folder["id"], dest, "%Y/%m")
        self._record_known_move_visibility_losses(before_rows, chosen)
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
        permanent_hashes = {}
        if mode == "disk_permanent":
            for row in rows:
                if row["id"] in chosen:
                    path = os.path.join(row["folder_path"], row["filename"])
                    if os.path.isfile(path):
                        permanent_hashes[row["id"]] = (path, _sha256(path))
        result = self._deleter().run_batch_delete(self.db, chosen, mode=mode)
        self._record_successful_deletions(chosen, result, permanent_hashes)
        event(f"delete {mode}: ok={result.get('ok')}")

    def _record_successful_deletions(self, chosen, result, permanent_hashes):
        if not result.get("ok"):
            return
        remaining = {row["id"] for row in self._catalog_rows()}
        succeeded = set(chosen) - set(result.get("failed_photo_ids") or []) - remaining
        self.allowed_catalog_deletions.update(succeeded)
        self.allowed_to_vanish.update(
            permanent_hashes[pid][1]
            for pid in succeeded
            if pid in permanent_hashes and not os.path.lexists(permanent_hashes[pid][0])
        )

    def _record_verified_consolidations(self, source_root, target_root, before_disk, before_rows, result):
        if not result or result.get("errors") or source_root == target_root:
            return
        after_disk = self._disk_snapshot()
        verified = {}
        for source, digest in before_disk.items():
            if os.path.commonpath([source_root, source]) != source_root:
                continue
            target = os.path.join(target_root, os.path.relpath(source, source_root))
            # Only this exact source copy may be consolidated, into the exact
            # receiver that already had identical bytes and still has them.
            if source not in after_disk and before_disk.get(target) == digest and after_disk.get(target) == digest:
                verified[source] = target
                self.allowed_to_vanish[digest] += 1
        # A tracked archive merge can also intentionally fold staged rows
        # into surviving rows. Transfer their visibility obligations instead
        # of silently forgetting the staged photo's prior workspace access.
        after_rows = {
            os.path.join(r["folder_path"], r["filename"]): r for r in self._catalog_rows()
        }
        dropped = set(result.get("dropped_photo_ids") or [])
        remaining_ids = {row["id"] for row in after_rows.values()}
        for row in before_rows:
            source = os.path.join(row["folder_path"], row["filename"])
            if row["id"] not in dropped or row["id"] in remaining_ids or source not in verified:
                continue
            receiver = after_rows.get(verified[source])
            if receiver is None or receiver["file_hash"] != before_disk[source]:
                continue
            self.allowed_catalog_deletions.add(row["id"])
            self.visibility.setdefault(receiver["id"], set()).update(self.visibility.get(row["id"], set()))

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
            f"{sum(lost.values())} photo copy/copies vanished from disk without a permanent delete or verified consolidation: "
            + ", ".join(p for p, d in self.snapshot.items() if d in lost)
        )
        self.snapshot = now
        self.allowed_to_vanish = Counter()

    @invariant()
    def no_photo_drops_out_of_a_workspace(self):
        now = self._workspace_visibility()
        new_invisible = {
            pid: sorted(workspaces)
            for pid, workspaces in now.items()
            if pid not in self.visibility and self.ws_id not in workspaces
        }
        assert not new_invisible, (
            f"newly cataloged photos invisible to active workspace {self.ws_id}: {new_invisible}"
        )
        disappeared = set(self.visibility) - set(now) - self.allowed_catalog_deletions
        assert not disappeared, f"catalog photos disappeared without a successful delete: {sorted(disappeared)}"
        dropped = {
            pid: sorted(before - now[pid] - self.known_move_visibility_losses.get(pid, set()))
            for pid, before in self.visibility.items()
            if pid in now and before - now[pid] - self.known_move_visibility_losses.get(pid, set())
        }
        assert not dropped, f"photos lost workspace visibility: {dropped}"
        self.visibility = now
        self.known_move_visibility_losses = {}
        self.allowed_catalog_deletions = set()

    @invariant()
    def catalog_matches_disk(self):
        seen = {}
        for row in self._catalog_rows():
            path = os.path.join(row["folder_path"], row["filename"])
            assert os.path.isfile(path), (
                f"photo {row['id']} points at {path}, which does not exist"
            )
            assert row["file_hash"], f"photo {row['id']} has no recorded file hash"
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
        "this passes, drop the xfail and the known_move_visibility_losses carve-out."
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


@pytest.mark.parametrize("delete_catalog_row", [False, True], ids=["linkless-photo", "deleted-photo"])
def test_visibility_invariant_distinguishes_linkless_from_deleted(delete_catalog_row):
    import sqlite3
    from types import SimpleNamespace

    # Use real SQL to exercise the visibility query as well as the invariant.
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(
            "CREATE TABLE photos (id INTEGER, folder_id INTEGER);"
            "CREATE TABLE workspace_folders (folder_id INTEGER, workspace_id INTEGER);"
            "INSERT INTO photos VALUES (1, 10);"
            "INSERT INTO workspace_folders VALUES (10, 1);"
        )
        machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
        machine.db = SimpleNamespace(conn=conn)
        machine.known_move_visibility_losses = {}
        machine.allowed_catalog_deletions = set()
        machine.ws_id = 1
        machine.visibility = machine._workspace_visibility()
        assert machine.visibility == {1: {1}}
        conn.execute("DELETE FROM workspace_folders")
        if delete_catalog_row:
            conn.execute("DELETE FROM photos")
            machine.allowed_catalog_deletions = {1}
            machine.no_photo_drops_out_of_a_workspace()
            assert machine.visibility == {}
        else:
            with pytest.raises(AssertionError, match="photos lost workspace visibility"):
                machine.no_photo_drops_out_of_a_workspace()
    finally:
        conn.close()


@pytest.mark.parametrize("linked_workspace", [None, 2, 1], ids=["no-link", "wrong-workspace", "active-workspace"])
def test_new_catalog_photo_must_be_visible_to_active_workspace(linked_workspace):
    import sqlite3
    from types import SimpleNamespace

    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(
            "CREATE TABLE photos (id INTEGER, folder_id INTEGER);"
            "CREATE TABLE workspace_folders (folder_id INTEGER, workspace_id INTEGER);"
        )
        machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
        machine.db = SimpleNamespace(conn=conn)
        machine.ws_id = 1
        machine.known_move_visibility_losses = {}
        machine.allowed_catalog_deletions = set()
        machine.visibility = machine._workspace_visibility()
        conn.execute("INSERT INTO photos VALUES (1, 10)")
        if linked_workspace is not None:
            conn.execute("INSERT INTO workspace_folders VALUES (10, ?)", (linked_workspace,))
        if linked_workspace == machine.ws_id:
            machine.no_photo_drops_out_of_a_workspace()
            assert machine.visibility == {1: {1}}
        else:
            with pytest.raises(AssertionError, match="newly cataloged photos invisible"):
                machine.no_photo_drops_out_of_a_workspace()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "loss,should_fail",
    [("moved-sharing", False), ("moved-active", True), ("unrelated-sharing", True),
     ("undeclared-row-delete", True), ("declared-row-delete", False)],
)
def test_visibility_exemptions_are_limited_to_declared_ids_and_workspaces(loss, should_fail):
    import sqlite3
    from types import SimpleNamespace

    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(
            "CREATE TABLE photos (id INTEGER, folder_id INTEGER);"
            "CREATE TABLE workspace_folders (folder_id INTEGER, workspace_id INTEGER);"
            "INSERT INTO photos VALUES (1, 10), (2, 20);"
            "INSERT INTO workspace_folders VALUES (10, 1), (10, 2), (20, 1), (20, 2);"
        )
        machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
        machine.db = SimpleNamespace(conn=conn)
        machine.ws_id = 1
        machine.visibility = machine._workspace_visibility()
        machine.known_move_visibility_losses = {1: {2}}
        machine.allowed_catalog_deletions = set()
        if loss == "moved-sharing":
            conn.execute("DELETE FROM workspace_folders WHERE folder_id=10 AND workspace_id=2")
        elif loss == "moved-active":
            conn.execute("DELETE FROM workspace_folders WHERE folder_id=10 AND workspace_id=1")
        elif loss == "unrelated-sharing":
            conn.execute("DELETE FROM workspace_folders WHERE folder_id=20 AND workspace_id=2")
        else:
            conn.execute("DELETE FROM photos WHERE id=1")
            if loss == "declared-row-delete":
                machine.allowed_catalog_deletions = {1}
        if should_fail:
            with pytest.raises(AssertionError, match="photos (lost|disappeared)"):
                machine.no_photo_drops_out_of_a_workspace()
        else:
            machine.no_photo_drops_out_of_a_workspace()
            assert not machine.known_move_visibility_losses
            assert not machine.allowed_catalog_deletions
    finally:
        conn.close()


@pytest.mark.parametrize("stored_hash", [None, "", "wrong-hash", "correct"],
                         ids=["null-hash", "empty-hash", "wrong-hash", "matching-hash"])
def test_catalog_invariant_requires_matching_hash(tmp_path, stored_hash):
    path = tmp_path / "photo.jpg"
    path.write_bytes(b"readable-photo-bytes")
    machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
    recorded = _sha256(path) if stored_hash == "correct" else stored_hash
    machine._catalog_rows = lambda: [
        {"id": 1, "folder_path": str(tmp_path), "filename": path.name, "file_hash": recorded}
    ]
    if stored_hash == "correct":
        machine.catalog_matches_disk()
    else:
        with pytest.raises(AssertionError, match="(no recorded file hash|hash that does not match)"):
            machine.catalog_matches_disk()


@pytest.mark.parametrize(
    "workspace,ok,should_fail",
    [(2, True, True), (1, True, False), (2, False, False)],
    ids=["other-workspace-only", "active-workspace", "reported-failure"],
)
def test_finished_import_checks_active_workspace_catalog(tmp_path, workspace, ok, should_fail):
    import sqlite3
    from types import SimpleNamespace

    path = tmp_path / "photo.jpg"
    path.write_bytes(b"imported-photo")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(
            "CREATE TABLE photos (id INTEGER, folder_id INTEGER, filename TEXT, file_hash TEXT);"
            "CREATE TABLE folders (id INTEGER, path TEXT);"
            "CREATE TABLE workspace_folders (folder_id INTEGER, workspace_id INTEGER);"
            "INSERT INTO photos VALUES (1, 10, 'photo.jpg', NULL);"
        )
        conn.execute("INSERT INTO folders VALUES (10, ?)", (str(tmp_path),))
        conn.execute("INSERT INTO workspace_folders VALUES (10, ?)", (workspace,))
        machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
        machine.db = SimpleNamespace(conn=conn)
        machine.ws_id = 1
        result = {"ok": ok, "copied": 0, "skipped": 1, "failed": 0}
        hashes = {_sha256(path)}
        if should_fail:
            with pytest.raises(AssertionError, match="not visible in the active workspace"):
                machine._check_finished_import(result, "card", hashes)
        else:
            machine._check_finished_import(result, "card", hashes)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "ok,failed_ids,remaining_ids,selected_gone,allowed",
    [(True, [1], [1], False, False), (False, [], [], True, False),
     (True, [], [1], True, False), (True, [], [], False, False), (True, [], [], True, True)],
    ids=["failed-id", "failed-operation", "row-retained", "wrong-copy-deleted", "successful-delete"],
)
def test_only_successful_deletions_allow_identical_copy_loss(
    tmp_path, ok, failed_ids, remaining_ids, selected_gone, allowed,
):
    selected = tmp_path / "selected.jpg"
    selected.write_bytes(b"same-bytes")
    machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
    machine.allowed_catalog_deletions = set()
    machine.allowed_to_vanish = Counter()
    machine._catalog_rows = lambda: [{"id": pid} for pid in remaining_ids]
    if selected_gone:
        selected.unlink()
    machine._record_successful_deletions(
        [1], {"ok": ok, "failed_photo_ids": failed_ids}, {1: (str(selected), "same-bytes")},
    )
    assert machine.allowed_catalog_deletions == ({1} if ok and not failed_ids and not remaining_ids else set())
    machine.snapshot = {"selected.jpg": "same-bytes", "card-copy.jpg": "same-bytes"}
    machine._disk_snapshot = lambda: {"surviving-copy.jpg": "same-bytes"}
    if allowed:
        machine.no_photo_lost_or_overwritten()
    else:
        with pytest.raises(AssertionError, match="vanished from disk"):
            machine.no_photo_lost_or_overwritten()


@pytest.mark.parametrize(
    "extra_loss,should_fail", [(None, False), ("unique", True), ("unrelated-copy", True), ("receiver", True)],
)
def test_verified_consolidation_preserves_unique_bytes_and_unrelated_copies(tmp_path, extra_loss, should_fail):
    source = str(tmp_path / "source")
    target = str(tmp_path / "target")
    staged = os.path.join(source, "duplicate.jpg")
    receiver = os.path.join(target, "duplicate.jpg")
    unique = os.path.join(source, "unique.jpg")
    unique_receiver = os.path.join(target, "unique.jpg")
    card = str(tmp_path / "card.jpg")
    before = {staged: "same", receiver: "same", card: "same", unique: "unique"}
    after = {receiver: "same", card: "same", unique_receiver: "unique"}
    if extra_loss == "unique":
        del after[unique_receiver]
    elif extra_loss == "unrelated-copy":
        del after[card]
    elif extra_loss == "receiver":
        del after[receiver]
    machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
    machine.snapshot = before
    machine.allowed_to_vanish = Counter()
    machine.allowed_catalog_deletions = set()
    machine.visibility = {}
    machine._disk_snapshot = lambda: after
    machine._catalog_rows = lambda: []
    machine._record_verified_consolidations(source, target, before, [], {"moved": 1, "errors": []})
    if should_fail:
        with pytest.raises(AssertionError, match="vanished from disk"):
            machine.no_photo_lost_or_overwritten()
    else:
        machine.no_photo_lost_or_overwritten()
        assert set(before.values()) <= set(after.values())


def test_reimport_moved_duplicate_is_visible_in_receiving_workspace(request):
    import move

    machine = PhotoSafetyMachine()
    try:
        machine.first_shoot(["DSC_0001.jpg"], 0)
        first = machine._run_import(CARDS[0], _Runner())
        assert first["ok"] and first["copied"] == 1
        folder = machine._photo_folders()[0]
        moved = move.move_folder(machine.db, folder["id"], machine.moved)
        assert moved["moved"] == 1 and not moved["errors"]
        machine.switch_workspace()
        result = machine._run_import(CARDS[0], _Runner())
        assert result["ok"] and not result["failed"]
        # Mark only after setup and job-success assertions have passed. A
        # broken setup or failed import must not become this known xfail.
        request.node.add_marker(pytest.mark.xfail(strict=True, raises=AssertionError, reason=(
            "Re-importing a moved duplicate reports success without making it visible "
            "in the receiving workspace; photo-specific versus folder-wide access "
            "requires a product decision."
        )))
        machine._check_finished_import(result, CARDS[0], {
            _sha256(os.path.join(machine.cards[CARDS[0]], "DSC_0001.jpg")),
        })
    finally:
        machine.teardown()


def test_crashed_import_archive_merge_consolidates_verified_copy():
    import gc

    import move

    machine = PhotoSafetyMachine()
    try:
        machine.first_shoot(["DSC_0001.jpg", "DSC_0002.jpg"], 0)
        assert machine._run_import(CARDS[0], _Runner())["ok"]
        machine.no_photo_lost_or_overwritten()
        machine.no_photo_drops_out_of_a_workspace()
        folder = machine._photo_folders()[0]
        assert not move.move_folder(machine.db, folder["id"], machine.moved)["errors"]
        machine.no_photo_lost_or_overwritten()
        machine.no_photo_drops_out_of_a_workspace()
        removed = machine._visible_photo_ids()[0]
        result = machine._deleter().run_batch_delete(machine.db, [removed], mode="vireo")
        machine._record_successful_deletions([removed], result, {})
        machine.no_photo_lost_or_overwritten()
        machine.no_photo_drops_out_of_a_workspace()
        with pytest.raises(_SimulatedCrash):
            machine._run_import(CARDS[0], _Runner(crash_after=4))
        gc.collect()
        machine.db.conn.rollback()
        machine.no_photo_lost_or_overwritten()
        machine.no_photo_drops_out_of_a_workspace()
        folder = next(f for f in machine._photo_folders() if f["path"].startswith(machine.library + os.sep))
        before_disk = machine._disk_snapshot()
        before_rows = machine._catalog_rows()
        result = move.move_folder(
            machine.db, folder["id"], machine.moved,
            merge=True, allow_tracked_merge=True, verify_contents=True,
        )
        assert not result["errors"]
        machine._record_verified_consolidations(
            folder["path"], result["merged_into_existing"], before_disk, before_rows, result,
        )
        assert sum(machine.allowed_to_vanish.values()) == 1
        machine.no_photo_lost_or_overwritten()
        machine.no_photo_drops_out_of_a_workspace()
        machine.catalog_matches_disk()
        assert set(before_disk.values()) <= set(machine.snapshot.values())
    finally:
        machine.teardown()


@pytest.mark.parametrize("preserve_source_access", [True, False])
def test_consolidated_catalog_identity_retains_old_workspace_access(tmp_path, preserve_source_access):
    source = str(tmp_path / "source")
    target = str(tmp_path / "target")
    original = os.path.join(source, "photo.jpg")
    receiver = os.path.join(target, "photo.jpg")
    before_rows = [{"id": 1, "folder_path": source, "filename": "photo.jpg", "file_hash": "same"}]
    after_rows = [{"id": 2, "folder_path": target, "filename": "photo.jpg", "file_hash": "same"}]
    before_disk = {original: "same", receiver: "same"}
    machine = PhotoSafetyMachine.__new__(PhotoSafetyMachine)
    machine.allowed_to_vanish = Counter()
    machine.allowed_catalog_deletions = set()
    machine.known_move_visibility_losses = {}
    machine.ws_id = 1
    machine.visibility = {1: {1}, 2: {2}}
    machine._disk_snapshot = lambda: {receiver: "same"}
    machine._catalog_rows = lambda: after_rows
    machine._workspace_visibility = lambda: {2: {1, 2} if preserve_source_access else {2}}
    machine._record_verified_consolidations(
        source, target, before_disk, before_rows, {"errors": [], "dropped_photo_ids": [1]},
    )
    if preserve_source_access:
        machine.no_photo_drops_out_of_a_workspace()
    else:
        with pytest.raises(AssertionError, match="photos lost workspace visibility"):
            machine.no_photo_drops_out_of_a_workspace()
