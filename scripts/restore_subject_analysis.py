#!/usr/bin/env python3
"""Put back the masks and mask measurements migration 54 retired.

Migration 54 deleted every MegaDetector mask and cleared the measurements
projected from them, because none recorded which decoded frame it was made in.
``vireo/decoded_frame.py`` now recognizes the ones that can only have been made
in today's frame, so this copies them back from the snapshot Vireo took before
migrating (``vireo.db.pre-v57.bak``).

Nothing is replaced. A mask is restored only when

* the live catalog has no mask for that photo and SAM variant;
* the same detection (id, box and detector runtime) is still there, unrejected,
  and has not been written again by a frame-aware detector run;
* the original is on disk with the size and modified time both catalogs record,
  and it has no JPEG companion;
* the mask PNG is still on disk and is not referenced by another photo; and
* today's decode of the original holds the mask. A RAW whose full-size camera JPEG
  (``JpgFromRaw``) the scanner recorded shares an answer with the others of its
  camera, sensor size and mask size, of which a sample is decoded: how near to
  full that JPEG is, is a property of the camera and its frame. Every other
  original is decoded. Process proves each file's own frame again before it
  reuses that file's mask.

The mask becomes active again, with the values measured from it, where it was
active before and its region is still the primary. Process only reuses an active
mask; it runs SAM again over an inactive one.

Four kinds of value are left unmeasured for Process, which needs no SAM for any:

* ``quality_score``, ``subject_sharpness`` and the ``detection_subjects`` rows
  they come from. They are measured in the detector box, and analysis versions
  before 4 read another rendition: on boxes measured both ways none agree;
* ``subject_detail``, which was read from mixed renderings; and
* the eye values, which nothing has compared with today's.

Without ``--apply`` it only reports. Quit Vireo first; --apply refuses while
another process holds the catalog, and saves a copy of it beside itself. Checking
every original takes a while over a network share (about 15 minutes for 61,000).

    python scripts/restore_subject_analysis.py ~/.vireo/vireo.db ~/.vireo/vireo.db.pre-v57.bak
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sqlite3
import stat
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "vireo"))

from detection_id import COMPANION_FRAME_RUNTIME  # noqa: E402
from image_loader import RAW_EXTENSIONS  # noqa: E402
from PIL import Image  # noqa: E402
from repositories.subject_order import primary_order_sql  # noqa: E402

DETECTOR = "megadetector-v6"
#: The columns ``clear_primary_features`` emptied that were measured from the mask.
PHOTO_COLUMNS = (
    "mask_path", "active_mask_variant", "subject_size", "subject_clip_high", "subject_clip_low",
    "subject_y_median", "subject_tenengrad", "bg_tenengrad", "crop_complete", "bg_separation",
    "phash_crop", "noise_estimate", "dino_subject_embedding",
)
#: Originals decoded for each group of RAWs that share a camera, frame and mask size.
SAMPLES_PER_GROUP = 12
MASK_COLUMNS = (
    "photo_id", "variant", "path", "created_at", "detector_model", "prompt_x", "prompt_y", "prompt_w",
    "prompt_h", "subject_size", "subject_tenengrad", "bg_tenengrad", "crop_complete",
    "quality_input_recipe", "subject_clip_high", "subject_clip_low", "subject_y_median",
    "bg_separation", "phash_crop", "noise_estimate",
)

# The catalogs are compared here; the file on disk is checked by ``_source_problem``.
_SAME_FILE = ("l.file_hash IS o.file_hash AND l.file_size IS o.file_size "
              "AND l.file_mtime IS o.file_mtime AND l.filename IS o.filename")

_CANDIDATES = f"""
    SELECT m.photo_id, m.variant, m.path, d.id AS detection_id,
           lf.path AS folder_path, l.filename, l.file_size, l.file_mtime, l.camera_model, l.width, l.height,
           CASE WHEN json_valid(l.exif_data) THEN json_extract(l.exif_data, '$.EXIF.JpgFromRawLength') END
               AS embedded_jpeg,
           EXISTS (SELECT 1 FROM main.photo_masks x
                    WHERE x.photo_id = m.photo_id AND x.variant = m.variant) AS has_mask,
           d.frame_id IS NOT NULL AS redetected,
           d.subject_verdict IS NOT NULL AS rejected,
           d.id IS NULL AS box_gone,
           (l.companion_path IS NOT NULL OR d.runtime_fingerprint = '{COMPANION_FRAME_RUNTIME}'
            OR EXISTS (SELECT 1 FROM main.companion_identities c WHERE c.photo_id = m.photo_id)) AS companion,
           l.id IS NULL AS photo_gone,
           NOT ({_SAME_FILE}) AS file_changed
      FROM bak.photo_masks m
      JOIN bak.photos o ON o.id = m.photo_id
      JOIN bak.detections od ON od.photo_id = m.photo_id AND od.detector_model = m.detector_model
           AND (od.box_x, od.box_y, od.box_w, od.box_h) = (m.prompt_x, m.prompt_y, m.prompt_w, m.prompt_h)
      LEFT JOIN main.photos l ON l.id = m.photo_id
      LEFT JOIN main.folders lf ON lf.id = l.folder_id
      LEFT JOIN main.detections d ON d.id = od.id AND d.photo_id = m.photo_id
           AND d.detector_model = od.detector_model
           AND d.runtime_fingerprint IN (od.runtime_fingerprint, '{COMPANION_FRAME_RUNTIME}')
           AND (d.box_x, d.box_y, d.box_w, d.box_h) = (od.box_x, od.box_y, od.box_w, od.box_h)
     WHERE m.detector_model = '{DETECTOR}'
"""

#: Why a mask is left alone, in the order the reasons are tested.
_REASONS = (
    ("photo_gone", "photo no longer in the catalog"),
    ("has_mask", "already has a newer mask"),
    ("companion", "has a JPEG companion"),
    ("box_gone", "detected again since, or the box was removed"),
    ("redetected", "detected again since, on the same box"),
    ("rejected", "region marked not an animal"),
    ("file_changed", "file changed since"),
)


def _mask_problem(path: str, masks_dir: Path, shared: set[str]) -> tuple[str | None, tuple[int, int] | None]:
    """Why the mask at ``path`` cannot be restored, or its size."""
    if path in shared:
        return "mask file used by more than one photo", None
    if not Path(path).resolve().is_relative_to(masks_dir):
        return "mask outside the catalog's masks folder", None
    try:
        with Image.open(path) as mask:
            # A file rewritten since carries the frame it belongs to, and
            # the old measurements do not describe it.
            if mask.info.get("vireo_subject_frame_id") is not None:
                return "mask file was rewritten", None
            # Opening reads only the header; truncated pixels fail here.
            mask.load()
            return None, mask.size
    except FileNotFoundError:
        return "mask file missing", None
    except (OSError, ValueError, SyntaxError, Image.DecompressionBombError):
        return "mask file unreadable", None


def _source_path(row: sqlite3.Row) -> Path:
    return Path(row["folder_path"]) / row["filename"]


def _source_problem(row: sqlite3.Row) -> str | None:
    """Why the original on disk is not the file both catalogs describe."""
    if not (row["folder_path"] and row["filename"]):
        return "original's path unknown"
    try:
        found = _source_path(row).stat()
    except (OSError, ValueError):
        return "original not found on disk"
    if not stat.S_ISREG(found.st_mode):
        return "original not found on disk"
    if found.st_size != row["file_size"] or found.st_mtime != row["file_mtime"]:
        return "original changed on disk since it was scanned"
    return None


def _frame_holds_mask(row: sqlite3.Row, mask_size: tuple[int, int]) -> bool | None:
    """Whether today's aligned decode of the original holds its mask.

    None when it cannot be decoded, or is not the file that was checked: it is
    checked again after the decode, so a file replaced meanwhile says nothing.
    """
    from decoded_frame import DecodedFrame
    from image_loader import RAW_DECODE_ALIGNED, load_image

    image = load_image(_source_path(row), max_size=max(mask_size), raw_decode=RAW_DECODE_ALIGNED)
    if image is None:
        return None
    with image:
        if _source_problem(row):
            return None
        return DecodedFrame.of(image).holds_mask(None, mask_size)


def _rows_in_frame(rows: list[sqlite3.Row], mask_size: tuple[int, int]) -> list[sqlite3.Row]:
    """The ``rows`` of one group whose masks lie in today's frame, judged by a spread sample.

    One refusal, or no answer at all, leaves the whole group alone. A sampled
    original that gave no answer is left alone itself.
    """
    step = max(len(rows) // SAMPLES_PER_GROUP, 1)
    answers = {row["photo_id"]: _frame_holds_mask(row, mask_size) for row in rows[::step][:SAMPLES_PER_GROUP]}
    if False in answers.values() or True not in answers.values():
        return []
    return [row for row in rows if answers.get(row["photo_id"], True)]


def _frame_group(row: sqlite3.Row) -> tuple[object, ...]:
    """With the mask's size, what decides whether it lies in today's frame.

    An ordinary image, or a RAW with no full-size camera JPEG on record, decides
    for itself.
    """
    extension = Path(row["filename"]).suffix.lower()
    if extension not in RAW_EXTENSIONS or not row["embedded_jpeg"]:
        return (row["photo_id"],)
    return (extension, row["camera_model"], row["width"], row["height"])


def plan_masks(conn: sqlite3.Connection, masks_dir: Path) -> tuple[list[sqlite3.Row], dict[str, int]]:
    """The backup masks to restore, and how many are left alone for each reason."""
    skipped: dict[str, int] = defaultdict(int)
    shared = {row[0] for row in conn.execute(
        f"SELECT path FROM (SELECT path, photo_id FROM bak.photo_masks WHERE detector_model='{DETECTOR}' "
        "UNION SELECT path, photo_id FROM main.photo_masks) GROUP BY path HAVING COUNT(DISTINCT photo_id) > 1")}
    intact: dict[tuple[int, str], tuple[sqlite3.Row, tuple[int, int]]] = {}
    for row in conn.execute(_CANDIDATES):
        reason = next((text for column, text in _REASONS if row[column]), None)
        size = None
        if reason is None:
            reason, size = _mask_problem(row["path"], masks_dir, shared)
        if size is None:
            skipped[reason or "mask file unreadable"] += 1
        else:
            intact[row["photo_id"], row["variant"]] = (row, size)
    originals = {row["photo_id"]: row for row, _ in intact.values()}
    print(f"Checking {len(originals)} originals...", flush=True)
    with ThreadPoolExecutor(max_workers=16) as pool:
        problems = dict(zip(originals, pool.map(_source_problem, originals.values()), strict=True))
    groups: dict[tuple[tuple[object, ...], tuple[int, int]], list[sqlite3.Row]] = defaultdict(list)
    for row, size in intact.values():
        problem = problems[row["photo_id"]]
        if problem:
            skipped[problem] += 1
        else:
            groups[_frame_group(row), size].append(row)
    chosen: list[sqlite3.Row] = []
    for (_, size), rows in groups.items():
        held = _rows_in_frame(rows, size)
        chosen += held
        if len(held) < len(rows):
            skipped["today's decoded frame does not hold the mask"] += len(rows) - len(held)
    return chosen, dict(skipped)


def restore_masks(conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> None:
    columns = ", ".join(MASK_COLUMNS)
    conn.executemany(
        f"INSERT INTO main.photo_masks ({columns}) "
        f"SELECT {columns} FROM bak.photo_masks WHERE photo_id=? AND variant=?",
        [(row["photo_id"], row["variant"]) for row in rows],
    )


def restore_projections(conn: sqlite3.Connection, rows: list[sqlite3.Row], floor: float) -> int:
    """Make a restored mask active again where it was, if its region is still the primary.

    The primary is resolved as ``set_active_variant`` resolves it at the
    workspace's floor; below-floor activation belongs to the pipeline's
    contextual rescue, which this has no evidence for. A photo whose primary
    moved, or whose primary Vireo has forgotten since, keeps the restored mask
    inactive, and Process masks the region that is primary now. A catalog old
    enough never to have recorded a primary needs none here.
    """
    primary = (
        "SELECT id FROM main.detections WHERE photo_id=? AND detector_confidence>=? AND subject_verdict IS NULL "
        f"AND detector_model!='full-image' AND category='animal' ORDER BY {primary_order_sql()} LIMIT 1"
    )
    # dino_embedding_variant was not cleared, so an embedding goes back only
    # beside the variant that made it.
    assignments = ", ".join(
        f"{c}=o.{c}" if c != "dino_subject_embedding" else
        f"{c}=CASE WHEN photos.dino_embedding_variant IS o.dino_embedding_variant THEN o.{c} END"
        for c in PHOTO_COLUMNS
    )
    restored = 0
    for row in rows:
        photo_id = row["photo_id"]
        top = conn.execute(primary, (photo_id, floor)).fetchone()
        if top is None or top["id"] != row["detection_id"]:
            continue
        then, now = (conn.execute(
            f"SELECT detection_id FROM {catalog}.photo_subject_state WHERE photo_id=?", (photo_id,)).fetchone()
            for catalog in ("bak", "main"))
        if (now is None and then is not None) or (now is not None and now["detection_id"] != row["detection_id"]):
            continue
        restored += conn.execute(
            f"UPDATE main.photos SET {assignments}, subject_detail=NULL, subject_detail_px=NULL, "
            "quality_input_recipe=o.quality_input_recipe FROM bak.photos o WHERE o.id=photos.id AND photos.id=? "
            "AND photos.mask_path IS NULL AND o.active_mask_variant=? AND o.mask_path=?",
            (photo_id, row["variant"], row["path"]),
        ).rowcount
    return restored


@contextlib.contextmanager
def open_catalogs(live: Path, backup: Path, *, write: bool):
    conn = sqlite3.connect(f"{live.resolve().as_uri()}?mode={'rw' if write else 'ro'}", uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("ATTACH ? AS bak", (f"{backup.resolve().as_uri()}?mode=ro&immutable=1",))
        if "frame_id" in {row[1] for row in conn.execute("PRAGMA bak.table_info(detections)")}:
            raise SystemExit(f"{backup} was taken after migration 54; it holds nothing to restore.")
        yield conn
    finally:
        conn.close()


def _lock_exclusively(conn: sqlite3.Connection) -> None:
    """Refuse while Vireo has the catalog open: its caches would not see these rows.

    Any connection left open holds a shared lock on a WAL catalog, which
    exclusive locking mode cannot share.
    """
    conn.execute("PRAGMA busy_timeout=0")
    conn.execute("PRAGMA main.locking_mode=EXCLUSIVE")
    try:
        conn.execute("BEGIN EXCLUSIVE")
    except sqlite3.OperationalError as error:
        raise SystemExit(f"The catalog is in use ({error}). Quit Vireo and run this again.") from error


def _save_copy(conn: sqlite3.Connection, live: Path) -> Path:
    """A private copy of the catalog, taken under the exclusive lock."""
    saved = live.with_name(f"{live.name}.pre-restore-{time.strftime('%Y%m%d-%H%M%S')}.bak")
    os.close(os.open(saved, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    with contextlib.closing(sqlite3.connect(saved)) as copy:
        conn.backup(copy, name="main")
    return saved


def apply(conn: sqlite3.Connection, rows: list[sqlite3.Row], floor: float) -> int:
    conn.execute("BEGIN EXCLUSIVE")
    try:
        restore_masks(conn, rows)
        active = restore_projections(conn, rows, floor)
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
    return active


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("live", type=Path)
    parser.add_argument("backup", type=Path)
    parser.add_argument("--masks-dir", type=Path, help="default: the masks folder beside the catalog")
    parser.add_argument("--detector-confidence", type=float, default=0.2)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    masks_dir = (args.masks_dir or args.live.parent / "masks").resolve()
    with open_catalogs(args.live, args.backup, write=args.apply) as conn:
        if args.apply:
            # Exclusive locking mode keeps the lock after the rollback, so
            # nothing can change the catalog between the checks and the writes.
            _lock_exclusively(conn)
            conn.execute("ROLLBACK")
        rows, skipped = plan_masks(conn, masks_dir)
        print(f"{len(rows)} masks on {len({row['photo_id'] for row in rows})} photos can be restored.")
        for reason, count in sorted(skipped.items(), key=lambda item: -item[1]):
            print(f"  {count} left alone: {reason}")
        if not args.apply:
            print("Nothing was changed. Pass --apply to restore them.")
            return 0
        if not rows:
            return 0
        print(f"Saved the catalog as {_save_copy(conn, args.live)}")
        active = apply(conn, rows, args.detector_confidence)
        print(f"Restored {len(rows)} masks; {active} are active again. Run Process to measure the rest.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
