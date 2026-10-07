"""Streaming pipeline job -- overlaps I/O stages and interleaves detect+classify.

This module orchestrates the full pipeline (scan -> thumbnails -> classify ->
extract-masks -> regroup) as a single background job with concurrent stages
connected by queues.

Stage implementations live in ``pipeline_stages``. This entry module owns
thread and pause participation, shared-lock lifetimes, and final results. It
passes each stage its per-run state and the helper callbacks it needs; stages
never import this module. Helper entry points remain here for existing callers
and diagnostics.

Existing standalone jobs (/api/jobs/scan, /api/jobs/classify, etc.) are
untouched. This is an additive orchestration layer.
"""

import contextlib
import logging
import math
import os
import queue
import tempfile
import threading
import time
import uuid
from functools import partial

from db import Database, commit_with_retry
from file_replace import replace_file
from job_contract import progress_event
from pipeline_locks import (
    acquire_workspace_regroup,
    release_archive_destination,
    try_reserve_archive_destination,
)
from pipeline_stages import classification, detection, features, grouping, media, models, scanning
from pipeline_stages.context import PipelineControl, PipelineRun
from pipeline_stages.context import PipelineParams as PipelineParams
from render_source import (
    photo_value as _photo_value,
)
from render_source import (
    recipe_render_source,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from render_source import (
    scaled_recipe_source_dimensions as _scaled_recipe_source_dimensions,
)
from render_source import (
    working_copy_path_if_satisfies as _working_copy_path_if_satisfies,
)
from resource_ledger import (
    bind_resource_cancel_check,
    bind_resource_owner,
    bind_resource_pure_cancel_check,
    suspend_resource_wait_timing,
)
from volume_reachability import (
    load_known_mount_roots as _load_known_mount_roots,  # noqa: F401
)
from volume_reachability import (
    mount_root_candidates as _archive_mount_root_candidates,
)
from volume_reachability import (
    record_known_mount_roots as _record_known_mount_roots,  # noqa: F401
)
from volume_reachability import seed_known_mount_roots as _seed_known_mount_roots

log = logging.getLogger(__name__)

_SENTINEL = object()  # unique end-of-stream marker

# How many times classify will pause for a vanished source volume before it
# stops asking. Resuming without actually remounting the share would otherwise
# pause again on the very next photo, forever.
_MAX_SOURCE_OFFLINE_PAUSES = 3


def _rollback_failed_mask_photo(thread_db, photo_id):
    """Reset the reused mask-stage connection or abort if it cannot recover."""
    try:
        thread_db.conn.rollback()
    except Exception as exc:
        log.exception("Mask extraction rollback failed for photo %s", photo_id)
        raise RuntimeError(
            f"Mask extraction rollback failed for photo {photo_id}"
        ) from exc


def _fsync_mask_file(path):
    """Make a completed staged PNG durable before publishing its name."""
    # Windows' CRT _commit backend rejects a read-only descriptor with EBADF.
    # Request write access even though the bytes are already complete.
    with open(path, "rb+") as handle:
        os.fsync(handle.fileno())


def _fsync_mask_directory(path):
    """Persist a published mask directory entry where Python supports it."""
    # Python cannot open directory handles for fsync on Windows. The staged
    # file itself is still flushed there before os.replace; POSIX platforms
    # additionally flush the directory entry before SQLite may commit it.
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class _StagedMaskFile:
    """Publish a new immutable mask generation around a database commit.

    The predecessor remains at its database-referenced path until the new
    path has committed.  A process interruption can therefore leave an
    unreferenced generation behind, but can never change the bytes behind a
    committed path.
    """

    def __init__(self, stage_dir, staged_path, final_path, previous_path=None):
        self.stage_dir = stage_dir
        self.staged_path = staged_path
        self.final_path = final_path
        self.previous_path = previous_path
        self.installed = False

    @classmethod
    def create(
        cls, mask, masks_dir, photo_id, variant, save_mask,
        previous_path=None,
    ):
        os.makedirs(masks_dir, exist_ok=True)
        stage_dir = tempfile.mkdtemp(prefix=".mask-stage-", dir=masks_dir)
        staged_path = None
        try:
            staged_path = save_mask(mask, stage_dir, photo_id, variant)
            stem, extension = os.path.splitext(os.path.basename(staged_path))
            # Never overwrite the path referenced by the currently committed
            # photo_masks row. SQLite can atomically switch that row to this
            # immutable generation after the file is fully published.
            final_path = os.path.join(
                masks_dir,
                f"{stem}.generation-{uuid.uuid4().hex}{extension}",
            )
            return cls(
                stage_dir, staged_path, final_path,
                previous_path=previous_path,
            )
        except Exception:
            if staged_path:
                with contextlib.suppress(OSError):
                    os.unlink(staged_path)
            with contextlib.suppress(OSError):
                os.rmdir(stage_dir)
            raise

    def install(self):
        """Atomically publish the new generation at its unique path."""
        _fsync_mask_file(self.staged_path)
        replace_file(self.staged_path, self.final_path)
        self.installed = True
        _fsync_mask_directory(os.path.dirname(self.final_path))

    def restore(self):
        """Discard a generation whose database transaction did not commit."""
        if self.installed:
            # The published path is uniquely suffixed (.generation-<uuid>.png)
            # and no committed row references it, so an unlink failure here —
            # e.g. an antivirus scanner holding the fresh PNG open on Windows —
            # is equivalent to a mid-write process interruption: the file is
            # left as unreferenced disk garbage rather than propagating and
            # marking the entire extract-masks stage fatal for every remaining
            # photo. Log so the leftover is discoverable.
            try:
                os.unlink(self.final_path)
            except FileNotFoundError:
                pass
            except OSError:
                log.warning(
                    "Failed to unlink rolled-back mask generation %s; "
                    "leaving unreferenced file in place",
                    self.final_path,
                    exc_info=True,
                )
        self.installed = False
        self._cleanup()

    def finish(self):
        """Keep the committed generation and discard its predecessor."""
        self.installed = False
        if self.previous_path:
            masks_dir = os.path.realpath(os.path.dirname(self.final_path))
            previous_real = os.path.realpath(self.previous_path)
            if (
                previous_real != os.path.realpath(self.final_path)
                and os.path.dirname(previous_real) == masks_dir
            ):
                # The database already committed the new generation, so an
                # old-file cleanup error must not turn a successful photo into
                # a reported extraction failure.
                with contextlib.suppress(OSError):
                    os.unlink(self.previous_path)
        self._cleanup()

    def _cleanup(self):
        for path in (self.staged_path,):
            if path:
                with contextlib.suppress(OSError):
                    os.unlink(path)
        with contextlib.suppress(OSError):
            os.rmdir(self.stage_dir)


def _archive_mount_baseline(
    path: str,
    known_mounted_roots: set[str] | None = None,
) -> dict[str, bool]:
    """Snapshot whether ``path``'s mount-root candidates are live mounts.

    Pairs with ``_unmounted_since_baseline`` to catch the case
    ``_missing_archive_mount_root`` structurally cannot see: a mount point
    that *persists* as an empty directory after the share detaches. Linux
    ``/mnt/<name>`` behaves that way (macOS removes ``/Volumes/<share>``,
    which is why ``lexists`` is enough there), so on Linux an unmounted
    archive looks exactly like a valid empty destination — ``os.makedirs``
    succeeds and the import writes onto the system disk under the stale
    mount point. See PR #1394 review (Codex P1 r3687190865).

    ``os.path.ismount`` is the right primitive rather than a hand-rolled
    ``st_dev`` comparison: on POSIX it already compares the directory's
    device against its parent's, and ``_archive_mount_root_candidates``
    deliberately returns Windows drive letters with a trailing separator
    so ismount accepts them.

    Recording the *baseline* is what makes this safe. An ordinary local
    directory that merely looks mount-shaped (a plain ``/mnt/photos`` the
    user created by hand) is False here and stays False, so it can never
    trip the staleness check — a bare "is it mounted?" test would refuse
    that destination outright and break working setups.

    ``known_mounted_roots`` seeds the baseline as ``True`` for any
    candidate the caller previously observed as a live mount (see
    ``_load_known_mount_roots`` / ``_record_known_mount_roots``). Without
    it, a share that was already detached BEFORE this run started
    escapes the guard: its baseline is False and no mounted → unmounted
    transition can fire against a False baseline, so the persistent
    ``/mnt/<name>`` stub still passes the per-batch guard and copies
    land on the local disk. Cross-run history closes that hole without
    refusing hand-made local dirs — those never enter the known-set to
    begin with (no run ever observed them as a live mount) and their
    baseline stays False. See PR #1396 review (Codex P1 r3687401636).
    """
    known = known_mounted_roots or set()
    # Custom mount locations (for example ``/srv/photos``) have no lexical
    # marker. Install durable evidence before candidate resolution so a
    # detached root is still recognised without touching its subtree.
    _seed_known_mount_roots(known)
    baseline = {}
    candidates = _archive_mount_root_candidates(path)
    # Candidates and confidence come from the same resolution (the list
    # carries ``conclusive``); two calls could disagree under saturation.
    conclusive = getattr(candidates, "conclusive", True)
    if not conclusive:
        # Do not encode uncertainty as a mounted baseline. Both consumers of
        # True entries perform later synchronous filesystem probes
        # (``_mount_identity_baseline`` and ``_unmounted_since_baseline``),
        # which can hang on the same dead mount that made bounded alias
        # resolution time out. Abort before either path can touch it.
        root = candidates[-1] if candidates else path
        raise RuntimeError(
            f"Volume prefix {root} could not be inspected in time; "
            "reconnect it and retry."
        )
    for root in candidates:
        baseline[root] = root in known or os.path.ismount(root)
    return baseline


def _unmounted_since_baseline(baseline: dict[str, bool]) -> str | None:
    """Return a root that was mounted at baseline and no longer is.

    Deliberately one-directional: only a True → False transition counts.
    A root that was never a mount is an ordinary directory and is ignored,
    and a root still reporting mounted is left to the existing offline
    probes (a stale-but-registered SMB mount keeps ``ismount`` True while
    every read raises EIO — see ``_mount_root_offline``).
    """
    for root, was_mounted in baseline.items():
        if was_mounted and not os.path.ismount(root):
            return root
    return None


def _mount_identity(root: str):
    """Return a mount-instance identity, preferring Linux's mount ID."""
    normalized = os.path.normpath(root)
    try:
        with open("/proc/self/mountinfo", encoding="utf-8") as mountinfo:
            for line in mountinfo:
                fields = line.split(" - ", 1)[0].split()
                if len(fields) < 5:
                    continue
                mount_point = fields[4]
                for escaped, literal in (
                    ("\\040", " "), ("\\011", "\t"),
                    ("\\012", "\n"), ("\\134", "\\"),
                ):
                    mount_point = mount_point.replace(escaped, literal)
                if os.path.normpath(mount_point) == normalized:
                    # This ID changes even when the same filesystem is
                    # detached and remounted; device/inode may not.
                    return ("mountinfo", fields[0], fields[2], fields[3])
    except OSError:
        pass
    try:
        stat_result = os.stat(root)
    except OSError:
        return None
    return ("stat", stat_result.st_dev, stat_result.st_ino)


def _mount_identity_baseline(baseline: dict[str, bool]) -> dict[str, object]:
    """Snapshot live mount instances represented by a mounted baseline."""
    return {
        root: _mount_identity(root)
        for root, was_mounted in baseline.items()
        if was_mounted
    }


def _changed_mount_since_baseline(identities: dict[str, object]) -> str | None:
    """Return a mount whose instance disappeared or was replaced."""
    for root, prior_identity in identities.items():
        if prior_identity is None or _mount_identity(root) != prior_identity:
            return root
    return None


def _missing_archive_mount_root(path: str) -> str | None:
    """Return a likely missing mount root that must not be auto-created.

    "Missing" here means the mount-root directory itself doesn't exist —
    which is what the archive-parent preflight needs, so ``makedirs``
    doesn't silently create a stub directory and write onto the local
    disk when the intended NAS wasn't mounted. A mount point that
    persists after unmount (Linux ``/mnt/<name>``) is NOT flagged by
    this helper — see ``_source_offline_reason`` for the mid-run outage
    check that also treats a directory-still-there-but-not-mounted case
    as offline.
    """
    candidates = _archive_mount_root_candidates(path)
    if candidates and not getattr(candidates, "conclusive", True):
        # Could not inspect the mount prefix in time: refuse rather than
        # risk creating a stub and writing onto the local disk.
        return candidates[0]
    for mount_root in candidates:
        if not os.path.lexists(mount_root):
            return mount_root
    return None


def _mount_root_offline(mount_root: str) -> bool:
    """Whether ``mount_root`` appears to not have a filesystem mounted at it.

    Callers only get here when a subtree read has already failed, so we
    need positive evidence the root was actually a mount before scoping
    the outage to the whole share. Two shapes count as offline:

    * The directory is entirely gone — macOS removes ``/Volumes/<share>``
      on eject, so ``lexists`` alone is enough.
    * A stale mount whose device is dead raises EIO from ``listdir`` /
      stat probes — we're in the "reads are already failing" path so an
      errored probe can't be considered healthy.

    ``ismount`` is intentionally NOT used as an early "still mounted →
    healthy" short-circuit. A stale SMB/NFS mount can keep its
    mount-point metadata after the server disconnects, so
    ``os.path.ismount`` still returns True while every read against
    the root raises EIO. Trusting ismount alone would classify the
    dropped share as folder-scoped and classify would keep reissuing
    reads across the dead share instead of pausing for reconnection
    (Codex #1388 P1 r3664211201). Probe with ``listdir`` first.

    A root that exists and is readable — populated OR empty — is
    treated as online. Populated is proof either that the mount is
    live or that this is an ordinary local directory with siblings
    to work through; either way any specific missing subtree is a
    folder-scoped issue (Codex #1388 P1 r3663642357). Empty is
    ambiguous — it could be an unmounted stub OR an ordinary local
    ``/mnt/photos`` whose only child (the deleted collection folder)
    just went away — so err folder-scoped rather than paint every
    folder-only outage as a whole-mount outage (Codex #1388 P1
    r3664348752). If the mount truly is dead, the ``listdir`` OSError
    branch above still fires and catches the case that matters
    (mount registered but server unreachable).
    """
    if not os.path.lexists(mount_root):
        return True
    # Probe the root itself before consulting ``ismount``: a dead SMB/NFS
    # mount that still shows up in ``mount`` will keep ``ismount == True``
    # but every read raises EIO. Catching that here is what lets classify
    # scope the outage to the mount and pause, rather than falling
    # through to the folder-scoped branch and hammering the dead share.
    try:
        os.listdir(mount_root)
    except OSError:
        return True
    # Readable root: whether populated (real mount OR local dir with
    # siblings) or empty (empty mount OR local dir whose only child
    # was deleted), we can't reliably prove the missing subtree is a
    # mount-wide outage. Callers should treat it as online and take
    # the folder-scoped branch.
    return False


def _source_offline_reason(
    folder_path: str, image_path: str,
) -> tuple[str, str] | None:
    """Return ``(scope, reason)`` for an unreachable source, or ``None``.

    Distinguishes "this one file won't load" (corrupt RAW, bad permissions —
    genuinely that photo's failure) from "the volume holding the collection
    went away" (every remaining read will fail too). Only the second warrants
    stopping the run: a dropped SMB/NFS share turns each read into an instant
    EIO, so a stage that keeps going marks the entire untouched remainder of
    the collection "failed" — which reads to the user as "your photos are
    broken" rather than "reconnect the share".

    ``scope`` says how far the outage reaches, so callers can react at the
    right blast radius:

    * ``"mount"`` — the shared volume is gone. Every remaining photo on it
      will fail the same way; classify should pause the whole run so the
      user can reconnect.
    * ``"folder"`` — the mount root is still present and mounted but this
      one folder no longer resolves. Other folders in the collection may
      be healthy, so the caller should skip photos in this folder as
      unreachable and keep processing rather than stopping cold. This
      covers a single deleted or renamed local folder — the incident
      fix's original global-stop behavior would have stranded later
      healthy folders (and, on ``reclassify=True``, cleared their
      existing predictions during finalization with no replacement).

    Deliberately conservative — an image path with no mount-shaped
    prefix under a readable containing folder returns ``None``, so a
    single unreadable file inside an ordinary local tree stays a
    per-photo failure. A readable folder under a mount-shaped path is
    still probed at the mount root, since a stale SMB/NFS mount can
    keep folder metadata cached (Codex #1388 P1 r3665254569) — a
    successful root probe there also returns ``None``.
    """
    if not folder_path:
        return None
    # A dead SMB/NFS mount can keep folder metadata cached: reads
    # against the folder's files still raise EIO, but
    # ``os.path.isdir(folder_path)`` returns True from the stale stat.
    # Probe every mount-shaped root candidate first (Codex #1388 P1
    # r3665254569) so that case is scoped mount-wide and classify
    # pauses for reconnection, rather than being misread as "one bad
    # file" and letting classify keep hammering the dead share. A
    # successful root probe leaves the caller with the ordinary
    # "readable folder → per-photo failure" outcome below.
    candidates = _archive_mount_root_candidates(image_path)
    if candidates and not getattr(candidates, "conclusive", True):
        return "mount", f"volume {candidates[0]} could not be inspected in time"
    for mount_root in candidates:
        if _mount_root_offline(mount_root):
            return "mount", f"volume {mount_root} is not mounted"
    # No mount-root candidate is offline. A readable folder means the
    # file is the problem, not the source — protects a local
    # ``/mnt/photos/...`` catalog (unusual, but legal) from tripping
    # the folder-scoped branch just because a single file was deleted.
    if os.path.isdir(folder_path):
        return None
    return "folder", f"folder {folder_path} is unreadable"


def _preflight_mask_outcomes(
    thread_db, dropped_photos, sam2_variant, dinov2_variant,
    detector_confidence, contextual_weak_ids=None,
    weak_detection_confidence=None, raw_subject_analysis=False,
):
    """Split pre-flight-dropped photos into ``(already_masked, at_risk)``.

    The pre-flight removes photos on an unreachable source before the mask
    loop runs, so their outcome has to be derived rather than observed. This
    mirrors the loop's own two gates so the derivation can't drift from it:

    * **Worklist candidacy** — a photo with no qualifying detection would
      never have been read for masking, so an outage doesn't change its
      fate. It lands in neither set: counting it would turn an unrelated
      outage into a Fatal Extract failure and tell the user to reconnect for
      photos that would still have no mask candidate. Weak-rescued frames
      (photos in ``contextual_weak_ids``) apply the loop's lower
      ``weak_detection_confidence`` floor with the MDv6/animal filter —
      matching the exact second branch of the mask loop's detection lookup.
      Ordinary photos take the strict ``detector_confidence`` floor with
      the full-image synthetic filter. A weak-confidence detection alone is
      NOT enough to count as at-risk: the loop requires ``contextual_weak_
      runs`` to have surfaced the frame first (matching-anchor-species gate
      included), so counting arbitrary weak-conf offline photos as at-risk
      inflates the outage report for photos the mask worklist would never
      read (Codex #1392 P2 r3687403366).
    * **Cache validity** — the same test the loop's cache branch applies:
      a ``photo_masks`` row for this variant whose stored prompt and detector
      still match the current primary detection, whose file is on disk, and
      whose photos row is active on the configured SAM *and* DINO variants.
      A stale prompt means the mask must be regenerated, which an offline
      source makes impossible — so it is at risk, not a cache hit. Treating
      it as a hit would suppress the outage and leave scoring using a mask
      and embedding derived from a detection that no longer applies.

    Only the at-risk set is unmasked-and-unreachable, so only it should
    drive the unreadable count, the reconnect message, and the failure latch.
    """
    contextual_weak_ids = contextual_weak_ids or set()
    already_masked: set = set()
    at_risk: set = set()
    for photo in dropped_photos:
        photo_id = photo["id"]
        # Mirror ``extract_masks_stage``'s per-photo detection lookup: a
        # weak-rescued photo takes the lower floor with the MDv6/animal
        # filter, everyone else takes the strict floor with the full-image
        # filter. Applying only the lower floor to every photo would count
        # unrelated sub-threshold detections as at-risk, inflating outages
        # for photos the loop would never open.
        if (
            photo_id in contextual_weak_ids
            and weak_detection_confidence is not None
        ):
            dets = [
                d for d in thread_db.get_detections(
                    photo_id,
                    min_conf=weak_detection_confidence,
                    detector_model="megadetector-v6",
                )
                if d["category"] == "animal"
            ]
        else:
            dets = [
                d for d in thread_db.get_detections(
                    photo_id, min_conf=detector_confidence,
                )
                if d["detector_model"] != "full-image"
            ]
        if not dets:
            continue
        primary = dets[0]
        existing = thread_db.get_photo_mask(photo_id, sam2_variant)
        if existing is None or not existing["path"]:
            at_risk.add(photo_id)
            continue
        cached_prompt = (
            existing["prompt_x"], existing["prompt_y"],
            existing["prompt_w"], existing["prompt_h"],
        )
        live_prompt = (
            primary["box_x"], primary["box_y"],
            primary["box_w"], primary["box_h"],
        )
        state = thread_db.conn.execute(
            "SELECT active_mask_variant, dino_embedding_variant, quality_input_recipe "
            "FROM photos WHERE id = ?",
            (photo_id,),
        ).fetchone()
        if (
            existing["detector_model"] == primary["detector_model"]
            and cached_prompt == live_prompt
            and os.path.isfile(existing["path"])
            and state is not None
            and not raw_subject_analysis
            and state["quality_input_recipe"] is None
            and state["active_mask_variant"] == sam2_variant
            and state["dino_embedding_variant"] == dinov2_variant
        ):
            already_masked.add(photo_id)
        else:
            at_risk.add(photo_id)
    return already_masked, at_risk


def _extract_masks_early_exit(
    reason_key: str, subthreshold: int, preflight_unreadable: int,
    preflight_masked: int, offline_latched: bool, outage_error=None,
) -> tuple[str, str, dict, dict]:
    """Stage status, runner step status, and result payload for an
    ``extract_masks`` early return.

    The stage bails out early when nothing in the worklist carries a
    qualifying detection. That is normally a benign ``skipped``, but the
    pre-flight source-offline branch may already have latched a ``failed``
    status and appended a Fatal error. Overwriting it with ``skipped`` lets
    the end-of-run rollup — which reads only stage statuses — finish the job
    green while the dropped photos stay unmasked and get hard-rejected in
    Process Review as ``no_subject_mask`` (Codex #1392 P1).

    The total counts the pre-flight-dropped photos for the same reason the
    finalizer does: reporting ``unreadable`` against a hard-coded zero total
    publishes an impossible tally.

    The runner step status is deliberately not the stage status for a benign
    exit. ``JobRunner.update_step`` only finalizes ``completed``/``failed``/
    ``cancelled``, and the Jobs page only renders a summary for those, so
    sending ``skipped`` would leave a hollow pending-style row with no
    duration and no explanation of why the stage did nothing.

    A latched exit also carries the outage detail onto the step. The Jobs
    page renders error text from the step fields, so without it a row failed
    by a dead source shows only the benign "no detections" summary — hiding
    the reconnect instruction and blaming detections for the failure.

    ``preflight_unreadable > 0`` fails the stage on its own, not only when
    ``offline_latched`` is set. The pre-flight branch deliberately does not
    re-latch when classify already emitted a source-offline Fatal (to avoid
    a duplicate error entry), but the extract stage still had unreadable
    photos — so its row on the Jobs page must show failed too, with the
    outage detail attached (Codex #1392 P2 r3687403367). Callers pass the
    extract-owned outage message even when it wasn't appended to
    ``errors`` so the step can carry it here.
    """
    should_fail = offline_latched or preflight_unreadable > 0
    step_extra = (
        {"error": outage_error, "error_count": preflight_unreadable}
        if should_fail and outage_error else {}
    )
    return (
        "failed" if should_fail else "skipped",
        "failed" if should_fail else "completed",
        step_extra,
        {
            "masked": preflight_masked, "skipped": 0, "failed": 0,
            "total": preflight_unreadable + preflight_masked,
            "unreadable": preflight_unreadable,
            "subthreshold": subthreshold,
            "reason": reason_key,
        },
    )


def _still_offline_folder_ids_of(thread_db, folder_ids) -> set:
    """Return the folder IDs whose stored ``folders.path`` is unreachable.

    Twin of ``_still_offline_folder_ids`` that takes folder IDs directly
    instead of a photo-seed set. Downstream stages use this to catch a
    dead source that classify never observed — a fully-cached classify
    (every detection + classifier result already stored) makes no image
    opens, so ``source_offline_state["skipped_photo_ids"]`` stays empty
    even if every remaining file lives on a disconnected share. Without
    an always-probe pass over the downstream worklist, ``extract_masks``
    / ``eye_keypoints`` would then reopen the dead source, hammering it
    photo-by-photo — the exact pattern the classify pause is meant to
    prevent (Codex #1388 P1 r3664891993).

    Each unique folder path is probed once (``isdir`` returns False for
    every child of a dead mount, so a single probe covers folder- and
    mount-scoped outages alike). Folders the DB no longer knows about
    are silently dropped, and the lookup is chunked under SQLite's
    bind-variable limit so large worklists don't raise ``too many SQL
    variables`` before the filter can run (see ``db.py``'s
    ``_SQLITE_PARAM_CHUNK_SIZE``).
    """
    if not folder_ids:
        return set()
    ids = [int(fid) for fid in folder_ids]
    _CHUNK = 900
    probe_cache: dict = {}
    still_offline: set = set()
    for i in range(0, len(ids), _CHUNK):
        chunk = ids[i:i + _CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows = thread_db.conn.execute(
            f"SELECT id, path FROM folders WHERE id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            fid = row["id"] if hasattr(row, "keys") else row[0]
            path = row["path"] if hasattr(row, "keys") else row[1]
            if not path:
                still_offline.add(fid)
                continue
            if path not in probe_cache:
                probe_cache[path] = os.path.isdir(path)
            if not probe_cache[path]:
                still_offline.add(fid)
    return still_offline


def _still_offline_folder_ids(thread_db, seed_photo_ids) -> set:
    """Return the folder IDs whose ``folders.path`` is still unreadable.

    Downstream stages (extract_masks, eye_keypoints) use this to expand
    the classify-observed ``source_offline_state["skipped_photo_ids"]``
    set from photo-ID-scoped exclusion to folder-scoped exclusion.
    Classify only ever adds a photo to that set after calling
    ``_prepare_image``; the non-reclassify cache branch short-circuits
    before that call, so cached photos in the same offline folder
    never enter the seed set (Codex #1388 P2 r3664694179). Filtering
    by folder catches them too — otherwise they slip through and force
    mask/eye-keypoint stages to reopen the dead source, the exact
    downstream-hammering pattern the classify pause is meant to
    prevent.

    Each unique folder path is probed once (dead mount → ``isdir``
    False for every child, so a single probe suffices for either
    mount- or folder-scoped outages). Anything the DB no longer knows
    about is silently dropped — a photo that was purged since
    classify can't be filtered by folder either.

    The seed lookup is chunked under SQLite's bind-variable limit
    (999 on legacy builds this repo explicitly accommodates — see
    ``db.py``'s ``_SQLITE_PARAM_CHUNK_SIZE``). Without chunking a
    large offline folder would raise ``OperationalError: too many
    SQL variables`` before the mask/eye-keypoint stages could get
    past this filter and continue with photos from healthy folders
    (Codex #1388 P2 r3664525158).
    """
    if not seed_photo_ids:
        return set()
    ids = [int(pid) for pid in seed_photo_ids]
    _CHUNK = 900
    probe_cache: dict = {}
    still_offline_folder_ids: set = set()
    for i in range(0, len(ids), _CHUNK):
        chunk = ids[i:i + _CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows = thread_db.conn.execute(
            f"SELECT DISTINCT p.folder_id, f.path "
            f"FROM photos p JOIN folders f ON p.folder_id = f.id "
            f"WHERE p.id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            fid = row["folder_id"] if hasattr(row, "keys") else row[0]
            path = row["path"] if hasattr(row, "keys") else row[1]
            if not path:
                still_offline_folder_ids.add(fid)
                continue
            if path not in probe_cache:
                probe_cache[path] = os.path.isdir(path)
            if not probe_cache[path]:
                still_offline_folder_ids.add(fid)
    return still_offline_folder_ids




def resolve_remote_archive(target, subpath):
    """Resolve a saved remote target + subpath into the pipeline's
    remote-archive context.

    ``target`` is a validated dict from ``config.get_remote_target``.
    ``subpath`` names the archive folder under BOTH base paths — it is
    required (unlike the Move page, where the moved folder's own name
    provides the landing leaf) because ``move_folder`` lands the staged
    folder inside a parent keeping its name: the subpath's last segment is
    the staging root's name, so the archive lands at exactly
    ``remote_path/subpath`` over SSH while the catalog is repointed at
    exactly ``mount_path/subpath``.

    Raises ValueError with a user-facing message when the pieces can't form
    a safe archive destination. Returns a dict:

    * ``target`` — the target passed in.
    * ``subpath`` — the sanitized relative subpath.
    * ``parent_subpath`` — subpath minus its last segment ("" for a single
      segment); feed this to ``build_remote_move_spec`` so the staged leaf
      lands at the full subpath.
    * ``ssh_final`` — NAS-side path the archive lands at.
    * ``mount_final`` — local mount path the catalog points at afterward.
    * ``display`` — ``user@host:ssh_final`` for messages/UI.
    """
    import posixpath

    from move import rsync_dest_spec, sanitize_subpath

    sub = sanitize_subpath(subpath)  # raises ValueError on absolute / '..'
    if not sub:
        raise ValueError(
            "remote_subpath is required — it names the archive folder under "
            "the remote target's base path (e.g. \"2026/kenya-trip\")."
        )
    mount_path = (target.get("mount_path") or "").strip()
    if not mount_path:
        raise ValueError(
            "This remote target has no local mount path, so archived photos "
            "couldn't stay in your library. Add a mount path under "
            "Settings → Remote targets."
        )
    if not os.path.isabs(mount_path):
        raise ValueError(
            "This remote target's local mount path isn't absolute "
            f"(\"{mount_path}\"). Archived photos would be repointed to a "
            "path relative to the server's working directory and appear "
            "missing. Set an absolute mount path under Settings → Remote "
            "targets."
        )
    ssh_final = posixpath.join(target["remote_path"], sub)
    mount_final = os.path.join(mount_path, *sub.split("/"))
    return {
        "target": target,
        "subpath": sub,
        "parent_subpath": posixpath.dirname(sub),
        "ssh_final": ssh_final,
        "mount_final": mount_final,
        "display": rsync_dest_spec(target, ssh_final),
    }


def _should_abort(abort_event):
    """Check if the pipeline should abort."""
    return abort_event.is_set()


class _PipelinePauseGate:
    """Park every active pipeline worker before publishing ``paused``.

    Phase one has four concurrent workers.  Letting the first worker that
    reaches a checkpoint publish ``paused`` would be misleading because the
    other three could still be scanning, rendering, or loading a model.  This
    gate tracks the active participants and only confirms the pause once all
    of them are at safe checkpoints.  Later pipeline stages use the same gate
    with one active participant.
    """

    def __init__(self, runner, job_id):
        self._runner = runner
        self._job_id = job_id
        self._lock = threading.Lock()
        self._active = set()
        self._parked = set()

    def _pause_requested(self):
        probe = getattr(self._runner, "pause_requested", None)
        return bool(probe and probe(self._job_id))

    def _cancellation_requested(self):
        probe = getattr(self._runner, "cancellation_requested", None)
        if probe is not None:
            return probe(self._job_id)
        return self._runner.is_cancelled(self._job_id)

    def register_many(self, participant_names):
        with self._lock:
            self._active.update(participant_names)

    def register(self, participant_name):
        self.register_many((participant_name,))

    def unregister(self, participant_name):
        with self._lock:
            self._active.discard(participant_name)
            self._parked.discard(participant_name)
            all_parked = not self._active or self._active <= self._parked
        if all_parked and self._pause_requested():
            self._runner.mark_paused(self._job_id)

    def checkpoint(self, participant_name):
        """Wait through a requested pause and return cancellation state."""
        if not self._pause_requested():
            return self._cancellation_requested()

        with self._lock:
            if participant_name not in self._active:
                return self._cancellation_requested()
            self._parked.add(participant_name)
            all_parked = self._active <= self._parked

        if all_parked:
            self._runner.mark_paused(self._job_id)

        try:
            return self._runner.wait_if_paused(
                self._job_id, publish_paused=False,
            )
        finally:
            with self._lock:
                self._parked.discard(participant_name)


def _recipe_render_source(photo, recipe, max_size, vireo_dir, folders):
    """Thin wrapper around the shared resolver, returning just the path.

    Pipeline callers don't need the ``using_working_copy`` flag, so the second
    element of :func:`render_source.recipe_render_source` is dropped.
    """
    return recipe_render_source(photo, recipe, max_size, vireo_dir, folders)[0]


def _incomplete_model_message(model_name, is_custom=False):
    if is_custom:
        return (
            f"Model '{model_name}' appears to be missing required files. "
            f"Ensure all model files are present in the model directory."
        )
    return (
        f"Model '{model_name}' is incomplete. "
        f"Open Settings → Models and click Repair to finish the download."
    )


def _looks_like_missing_external_data(err):
    """Heuristic: does this exception look like ONNXRuntime failing to find
    an external-data sidecar? Matches the specific message the runtime
    raises when a graph references external weights that aren't on disk."""
    msg = str(err).lower()
    return (
        "model_path must not be empty" in msg
        or "external data" in msg
    )


_RAW_EXTENSIONS = (".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf")


def _thumb_raw_decode_kwargs(photo, recipe):
    """Return raw_decode kwargs for generate_thumbnail."""
    if not recipe or not photo:
        return {}
    filename = _photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in _RAW_EXTENSIONS:
        return {}
    from image_loader import RAW_DECODE_LINEAR
    return {"raw_decode": RAW_DECODE_LINEAR}


def _thumb_min_source_size_kwargs(photo, recipe, thumb_size, source_path):
    if not recipe or not photo:
        return {}
    filename = _photo_value(photo, "filename") or ""
    if os.path.splitext(filename)[1].lower() not in _RAW_EXTENSIONS:
        return {}
    if os.path.splitext(source_path or "")[1].lower() not in _RAW_EXTENSIONS:
        return {}
    load_max_size = None if recipe.get("crop") else thumb_size
    return {
        "min_source_size": _scaled_recipe_source_dimensions(photo, load_max_size),
    }


def _retry_thumbnail_with_companion(
    thread_db, generate_thumbnail, photo, photo_id, raw_source_path,
    cache_dir, thumb_size, recipe, folder_path,
):
    """Mirror serve_thumb's RAW->companion fallback for pipeline jobs."""
    if not photo or not folder_path:
        return None
    companion_rel = _photo_value(photo, "companion_path")
    if not companion_rel:
        return None
    companion_abs = os.path.join(folder_path, companion_rel)
    if (
        not os.path.exists(companion_abs)
        or os.path.abspath(companion_abs) == os.path.abspath(raw_source_path)
    ):
        return None
    log.info(
        "Pipeline thumbnail RAW decode failed for photo %s; "
        "falling back to companion JPEG",
        photo_id,
    )
    file_mtime = _photo_value(photo, "file_mtime")
    if file_mtime is not None:
        try:
            thread_db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            commit_with_retry(thread_db.conn)
        except Exception:
            # Without the marker the RAW decode is simply retried next time.
            log.warning("Could not record RAW decode failure for photo %s", photo_id, exc_info=True)
    recipe_kwargs = {"recipe": recipe} if recipe else {}
    if recipe:
        recipe_kwargs["camera_metadata"] = photo
        recipe_kwargs["native_size"] = (
            _recipe_source_dimensions(photo)
        )
    return generate_thumbnail(
        photo_id,
        companion_abs,
        cache_dir,
        size=thumb_size,
        **recipe_kwargs,
    )


def _retry_thumbnail_with_working_copy(
    thread_db, generate_thumbnail, photo, photo_id, raw_source_path,
    cache_dir, thumb_size, recipe, vireo_dir,
):
    """Retry an edited RAW thumbnail from a near-full local JPEG copy."""
    if not photo or not recipe or not vireo_dir:
        return None
    if os.path.splitext(raw_source_path or "")[1].lower() not in _RAW_EXTENSIONS:
        return None
    wc_path = _working_copy_path_if_satisfies(
        photo, recipe, thumb_size, vireo_dir, thumbnail_tolerance=True,
    )
    if not wc_path or os.path.abspath(wc_path) == os.path.abspath(raw_source_path):
        return None
    log.info(
        "Pipeline thumbnail RAW decode failed for photo %s; "
        "falling back to near-full JPEG working copy",
        photo_id,
    )
    file_mtime = _photo_value(photo, "file_mtime")
    if file_mtime is not None:
        try:
            thread_db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            commit_with_retry(thread_db.conn)
        except Exception:
            # Without the marker the RAW decode is simply retried next time.
            log.warning("Could not record RAW decode failure for photo %s", photo_id, exc_info=True)
    return generate_thumbnail(
        photo_id,
        wc_path,
        cache_dir,
        size=thumb_size,
        recipe=recipe,
        native_size=_recipe_source_dimensions(photo),
        camera_metadata=photo,
    )


_CLASSIFIER_BUNDLE_FIELDS = (
    "clf", "model_type", "model_name", "model_str",
    "labels", "use_tol", "active_model", "labels_fingerprint",
    "labels_fingerprint_full", "classifier_model_identity",
)


def _taxonomy_fingerprint(tax):
    """Stable key component representing the taxonomy backing a classifier.

    Used in the timm classifier cache key so a pipeline that loads the
    classifier with no taxonomy (or a stale one) does not get reused after
    a taxonomy download or refresh updates the on-disk file. ``None`` is
    a valid input — pipelines run without taxonomy still hit the cache
    consistently among themselves.
    """
    if tax is None:
        return ("no-tax",)
    path = getattr(tax, "_path", None)
    if not path:
        return ("inline", id(tax))
    try:
        st = os.stat(path)
        return (path, int(st.st_mtime_ns), st.st_size)
    except OSError:
        return (path, None, None)


def _weights_fingerprint(weights_path, files):
    """Compatibility wrapper for the shared classifier cache identity."""
    from classifier_cache import model_files_fingerprint

    return model_files_fingerprint(weights_path, files)


def _release_classifier_cache_handle(loaded_models):
    """Release the cache handle AND drop the bundle's strong refs.

    Releasing the handle alone isn't enough: ``loaded_models`` still
    holds ``clf`` (the live Classifier/ONNX session) so the GC can't
    reclaim it. After idle eviction removes the cache entry, a second
    pipeline that loads the same model gets a fresh session — doubling
    VRAM — while this pipeline's stale ``clf`` is still pinned. Dropping
    the bundle fields here lets idle eviction actually free VRAM and
    lets same-model reloads hit the cache.

    Idempotent: no-op if no handle present (classify skipped before any
    model loaded).
    """
    handle = loaded_models.pop("_cache_handle", None)
    for k in _CLASSIFIER_BUNDLE_FIELDS:
        loaded_models.pop(k, None)
    if handle is None:
        return
    try:
        handle.release()
    except Exception:
        # Releasing a cache handle must never break pipeline cleanup.
        # The cache's idle timer will reclaim leaked entries eventually.
        log.exception("ModelCache: handle release raised; leak will be reclaimed by idle timer")


def _find_broken_metadata_folders(db, photo_ids):
    """Find folders containing photos with broken metadata in the given scope.

    A photo is considered broken if EXIF extraction never produced usable
    output (``exif_data IS NULL``) and either ``timestamp IS NULL`` or,
    for RAW files, dimensions under 1000px — the latter indicates the
    embedded JPEG thumbnail leaked through instead of the true sensor
    size. The ``exif_data IS NULL`` clause mirrors the scanner's
    ``exif_extracted`` guard: once ExifTool has stored output for a
    photo, the scanner won't retry regardless of signal, so flagging
    such rows here would cause the repair path to fire on every pipeline
    run without accomplishing anything.

    Rows whose file no longer exists on disk are filtered out: the
    scanner only repairs files it can rediscover via ``Path.iterdir()``,
    so a missing-file row would stay broken forever and keep the
    collection stuck in repair mode on every run instead of returning to
    the fast-path "Skipped (using collection)" summary.

    IDs are queried in chunks of at most 900 to stay safely under
    SQLite's default bound-parameter limit (SQLITE_LIMIT_VARIABLE_NUMBER,
    typically 999 in production builds). Without chunking, large
    collections would hit ``OperationalError: too many SQL variables``
    and abort the scan stage.

    Returns a list of ``(folder_path, file_paths)`` tuples where
    ``file_paths`` is a list of absolute image paths in that folder.
    Empty list when nothing needs repair.
    """
    if not photo_ids:
        return []
    raw_list = ",".join(f"'{e}'" for e in _RAW_EXTENSIONS)
    ids = list(photo_ids)
    _CHUNK = 900
    by_folder: dict[str, list[str]] = {}
    for i in range(0, len(ids), _CHUNK):
        chunk = ids[i : i + _CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows = db.conn.execute(
            f"""SELECT f.path AS folder_path, p.filename
                FROM photos p
                JOIN folders f ON p.folder_id = f.id
                WHERE p.id IN ({placeholders})
                  AND p.exif_data IS NULL
                  AND (p.timestamp IS NULL
                       OR (p.extension IN ({raw_list})
                           AND p.width IS NOT NULL AND p.width < 1000))
                ORDER BY f.path, p.filename""",
            tuple(chunk),
        ).fetchall()
        for r in rows:
            full_path = os.path.join(r["folder_path"], r["filename"])
            if not os.path.isfile(full_path):
                continue
            by_folder.setdefault(r["folder_path"], []).append(full_path)
    return [(fp, paths) for fp, paths in by_folder.items()]


# Approximate relative runtime cost per stage, used to weight the overall
# progress bar so a fast stage finishing doesn't push the bar to 100%.
# Heuristic: classify dominates on big imports; detect and eye_keypoints are
# also GPU-heavy; ingest / model_loader / regroup are quick.
STAGE_WEIGHTS = {
    "ingest": 2,
    "scan": 8,
    "thumbnails": 6,
    "previews": 6,
    "model_loader": 2,
    "detect": 15,
    "classify": 30,
    "extract_masks": 10,
    "eye_keypoints": 15,
    "regroup": 6,
    "misses": 4,
}


_CLASSIFICATION_ETA_MIN_ATTEMPTS = 16


def _cached_classify_detections(detections, floor, *, contextual_weak=False):
    """Filter cached detector rows to the classify runtime's candidates.

    Contextual-weak rescues are defined exclusively from MegaDetector V6
    evidence.  Keep that restriction on the initial cached-detection path as
    well as the later DB fallback so runtime selection matches the cache ETA
    preflight and a stale foreign-detector row cannot displace the cached
    MegaDetector crop.
    """
    return [
        detection for detection in detections
        if detection.get("detector_model") != "full-image"
        and detection.get("category", "animal") == "animal"
        and (
            not contextual_weak
            or detection.get("detector_model") == "megadetector-v6"
        )
        and detection.get(
            "confidence", detection.get("detector_confidence", 0),
        ) >= floor
    ]


def _remove_attempted_cache_hits(attempted_photo_ids, cache_hit_ids):
    """Remove photos with any inference attempt from the cache-hit bucket.

    A multi-detection photo can produce one real cache hit and still require
    inference for another detection. Once that inference is attempted, the
    photo is not wholly cached even if inference fails and produces no result
    to trigger the ordinary successful-promotion path. This is independent of
    whether the cache preflight counted the photo in its estimate.
    """
    attempted_cache_hits = set(attempted_photo_ids) & set(cache_hit_ids)
    cache_hit_ids.difference_update(attempted_cache_hits)
    return len(attempted_cache_hits)


def _record_unattempted_cache_hit(
    photo_id, inferred_photo_ids, attempted_photo_ids, cache_hit_ids,
):
    """Record a cache-hit photo only while it remains wholly cached.

    A failed inference can be flushed before a later detection on the same
    photo reaches its cache hit.  In that ordering the photo is already an
    observed inference attempt, so restoring it to the cache-hit bucket would
    make the ETA treat the same photo as both cached and uncached work.
    """
    if (
        photo_id in inferred_photo_ids
        or photo_id in attempted_photo_ids
        or photo_id in cache_hit_ids
    ):
        return False
    cache_hit_ids.add(photo_id)
    return True


def _classification_eta_progress(
    *, total, seen, cached_estimate, cache_hits, inference_attempts,
    classified, elapsed, cache_overcount=0,
    unclassifiable_estimate=0, unclassifiable_seen=0,
):
    """Return step-progress fields for a cache-aware classification ETA.

    ``seen / elapsed`` is not a useful inference rate: a cache-heavy prefix
    can be walked hundreds of times faster than uncached photos are decoded
    and sent through the model.  Estimate only from photos that actually
    entered an inference batch, while using the preflight cache count to
    subtract cache hits that are still expected later in the collection.

    ``elapsed`` must be inference-active seconds only — the wall time
    spent preparing images (open/decode/crop/resize) plus the wall time
    inside ``_flush_batch`` — not the wall time since the per-spec
    start. Passing walltime here would let the cache-traversal prefix
    bleed into the denominator: on a spec that spends thirty seconds
    walking a cached prefix before ever hitting inference, ``elapsed``
    would be ~30s while ``inference_attempts`` is still zero, and by the
    time the first inference lands the effective rate is derated by that
    entire prefix. The runtime tracks an ``inference_seconds`` accumulator
    that only ticks around ``_prepare_image`` calls for photos entering
    the batch and around ``_flush_batch`` itself, and passes it here.
    Excluding ``_prepare_image`` cost would understate the ETA on
    RAW/JPEG-heavy or slow-storage runs where preparation dominates
    per-photo latency (Codex #1468 P2).

    ``cache_overcount`` is the count of photos where the preflight assumed
    a cache hit (a classifier_runs row existed) but runtime found no cached
    predictions and fell through to fresh inference. Subtracting it from
    ``cached_estimate`` keeps ``remaining_uncached`` from collapsing to zero
    on collections dominated by run-key-without-predictions rows, where the
    preflight overcounts because it can't see the empty-predictions case
    (e.g. a prior pass wrote ``category == 'match'`` with no predictions).

    ``unclassifiable_estimate`` / ``unclassifiable_seen`` count photos the
    runtime skips at the ``raw_real_dets`` continue branch (has real
    detections but none above the classify floor and not a contextual-weak
    rescue). Runtime never enters an inference batch for them, so the
    unvisited share of them is subtracted from ``remaining_uncached`` —
    otherwise a tail of below-threshold photos inflates a numeric ETA
    that the runtime will actually traverse without work (Codex #1468 P2).

    The first classifier call is deliberately flushed as a one-photo batch
    for cancellation responsiveness and includes model warm-up.  Wait for a
    normal batch's worth of attempts before publishing a numeric ETA.
    """
    total = max(int(total or 0), 0)
    seen = min(max(int(seen or 0), 0), total)
    cached_estimate = min(max(int(cached_estimate or 0), 0), total)
    cache_hits = min(max(int(cache_hits or 0), 0), seen)
    inference_attempts = max(int(inference_attempts or 0), 0)
    classified = max(int(classified or 0), 0)
    elapsed = max(float(elapsed or 0), 0.0)
    cache_overcount = max(int(cache_overcount or 0), 0)
    unclassifiable_estimate = min(
        max(int(unclassifiable_estimate or 0), 0), total,
    )
    unclassifiable_seen = min(
        max(int(unclassifiable_seen or 0), 0), unclassifiable_estimate,
    )

    # Project future cache hits from the observed success rate of preflight
    # cache predictions. ``cache_hits + cache_overcount`` is the count of
    # preflight-expected cache attempts we've observed; ``cache_hits`` is
    # the subset where the runtime cache-hit predicate actually fired.
    # Scaling ``cached_estimate`` by this rate corrects the preflight for
    # the run-key-without-predictions case without waiting for the whole
    # collection to be visited — a collection dominated by these rows
    # converges to zero projected cache hits within a single batch.
    observed_cache_attempts = cache_hits + cache_overcount
    if observed_cache_attempts > 0:
        hit_success_rate = cache_hits / observed_cache_attempts
    else:
        # No preflight-expected cache observations yet — trust preflight
        # so early ETAs don't collapse before any evidence has arrived.
        hit_success_rate = 1.0
    corrected_cached_estimate = int(cached_estimate * hit_success_rate)
    remaining_photos = max(total - seen, 0)
    expected_future_cache_hits = max(
        corrected_cached_estimate - cache_hits, 0,
    )
    expected_future_unclassifiable = max(
        unclassifiable_estimate - unclassifiable_seen, 0,
    )
    remaining_uncached = max(
        remaining_photos
        - expected_future_cache_hits
        - expected_future_unclassifiable,
        0,
    )
    expected_uncached = max(
        total - corrected_cached_estimate - unclassifiable_estimate, 0,
    )
    min_attempts = min(
        _CLASSIFICATION_ETA_MIN_ATTEMPTS,
        max(expected_uncached, 1),
    )

    fields = {
        "eta_kind": "classification",
        "eta_state": "estimating",
        "eta_seconds": None,
        "eta_rate_per_min": None,
        "cache_hits": cache_hits,
        "classified": classified,
        "inference_attempts": inference_attempts,
        "remaining_uncached": remaining_uncached,
    }
    if seen >= total:
        fields["eta_state"] = "finishing"
        fields["eta_seconds"] = 0
        return fields
    # When preflight determines every remaining photo is cached or
    # unclassifiable (expected_uncached collapses to zero), no inference
    # batch will ever fire — the "Estimating after the first uncached
    # batch…" state below would then never resolve and the Jobs page
    # would linger on that message throughout a fully cached or fully
    # skipped traversal. Return the finishing signal directly instead
    # (Codex #1468 P2).
    if expected_uncached <= 0:
        fields["eta_state"] = "finishing"
        fields["eta_seconds"] = 0
        return fields
    if inference_attempts < min_attempts or elapsed <= 0:
        return fields

    rate_per_sec = inference_attempts / elapsed
    if rate_per_sec <= 0:
        return fields
    fields["eta_state"] = "ready"
    fields["eta_rate_per_min"] = round(rate_per_sec * 60, 1)
    fields["eta_seconds"] = round(remaining_uncached / rate_per_sec)
    return fields


def _stage_fraction(info):
    """Return a 0..1 completion fraction for one stage entry.

    'failed' stages still contribute their partial progress: heavy stages
    like classify and extract_masks often process most items before marking
    themselves failed due to per-item errors, and dropping their weight to
    0 would make the overall bar lurch backward when that failure surfaces.

    Prefer ``seen`` (every photo the stage iterated past, regardless of
    outcome) when present; fall back to ``count`` for stages that don't
    surface seen. Without this, classify's per-photo split into ``count``
    (inferred) + ``cached`` would leave the overall pipeline bar stuck on
    cached-heavy runs, since count alone stops growing once the cache
    starts hitting."""
    status = info.get("status", "pending")
    if status in ("completed", "skipped"):
        return 1.0
    if status not in ("running", "failed"):
        return 0.0
    total = info.get("total") or 0
    progressed = info.get("seen")
    if progressed is None:
        progressed = info.get("count") or 0
    if total <= 0:
        return 0.0
    if progressed >= total:
        return 1.0
    return progressed / total


def _weighted_progress(stages):
    """Overall pipeline progress as (current, total), weighted by stage cost.

    Scaled so total == sum(STAGE_WEIGHTS.values()), which keeps the UI's
    `Math.round(current/total * 100)` rendering whole percent steps.

    Uses floor rather than round: a done-but-not-quite value like 99.94
    must not render as 100 because the overall bar reaching total is what
    the UI treats as 'pipeline complete'. Only a genuinely completed
    pipeline (all stages completed/skipped) produces done == total."""
    total = sum(STAGE_WEIGHTS.values())
    if total == 0:
        return 0, 0
    done = sum(
        weight * _stage_fraction(stages.get(name, {}))
        for name, weight in STAGE_WEIGHTS.items()
    )
    return int(math.floor(done)), total


# Serializes snapshot-and-push of `stages` across the pipeline's daemon
# threads (scanner, thumbnail, model_loader, ...). All of them emit
# progress events whose payload includes a shallow copy of the shared
# `stages` dict; without this lock a thread can build the snapshot, get
# preempted, and land its stale event after another thread has already
# pushed events with newer counts — producing a non-monotonic SSE stream
# (the CI flake on test_pipeline_multi_source_ingest_progress_is_monotonic).
# Lock order: _progress_lock outside, JobRunner._lock inside.
_progress_lock = threading.Lock()


def _progress_event(stages, stage_id, phase, **extra):
    """Build a push_event 'progress' payload with weighted overall current/total.

    Call sites pass per-stage context (stage_id, phase, current_file, rate,
    eta_seconds, step_id). Per-stage counts still live in `stages[...]` and
    reach the UI via the `stages` snapshot, so step-level bars are unaffected."""
    current, total = _weighted_progress(stages)
    data = progress_event(
        phase,
        current,
        total,
        stage_id=stage_id,
        stages={k: dict(v) for k, v in stages.items()},
        # JobRunner.push_event merges progress payloads into job["progress"]
        # rather than replacing them, so a sub-phase (e.g. "Extracting metadata"
        # with phase_current/phase_total set) would otherwise linger through
        # every later stage that omits these keys and keep the /api/jobs
        # poll — and thus the jobs page + navbar sub-progress bar — rendering
        # a stale phase. Default the triple to None here so callers with no
        # active sub-phase actively clear it; the update() below lets callers
        # with a real sub-phase override.
        phase_current=None,
        phase_total=None,
        phase_label=None,
    )
    data.update(extra)
    return data


def _emit_progress(runner, job_id, stages, stage_id, phase, **extra):
    """Atomically snapshot `stages` and push a progress event.

    Replaces the unguarded ``runner.push_event(..., _progress_event(...))``
    pattern at every per-stage callback. Holding ``_progress_lock`` across
    the snapshot construction and the push_event call prevents a stale
    snapshot from another thread from landing out of order in the event
    log."""
    with _progress_lock:
        runner.push_event(
            job_id, "progress",
            _progress_event(stages, stage_id, phase, **extra),
        )


def _update_stages(runner, job_id, stages):
    """Push a stages progress update with weighted overall current/total.

    Snapshot+push are atomic under ``_progress_lock`` so concurrent emits
    from other pipeline threads can't produce stale events that land out
    of order."""
    with _progress_lock:
        current, total = _weighted_progress(stages)
        runner.push_event(job_id, "progress", {
            "phase": _current_phase(stages),
            "current": current,
            "total": total,
            "stages": {k: dict(v) for k, v in stages.items()},
        })


def _current_phase(stages):
    """Determine the primary phase label from stage statuses."""
    for name in ["misses", "regroup", "eye_keypoints", "extract_masks", "classify", "detect",
                 "model_loader", "previews", "thumbnails", "scan", "ingest"]:
        info = stages.get(name, {})
        if info.get("status") == "running":
            return info.get("label", name)
    return "Pipeline"


def _collapse_scan_roots(paths):
    """Reduce ``paths`` to the minimal non-overlapping ancestor set.

    Descendants of a kept path are dropped (the scanner walks recursively).
    The filesystem root needs special handling because ``'/' + os.sep``
    is ``'//'`` and would not prefix-match a child like ``/sub``.
    """
    candidates = sorted(set(paths), key=len)
    kept: list[str] = []
    for cand in candidates:
        is_descendant = False
        for k in kept:
            prefix = k if k.endswith(os.sep) else k + os.sep
            if cand.startswith(prefix):
                is_descendant = True
                break
        if not is_descendant:
            kept.append(cand)
    kept.sort()
    return kept


class _RunControl:
    """Cancellation and pause participation for one pipeline run."""

    def __init__(self, runner, job):
        self.runner = runner
        self.job = job
        self.abort = threading.Event()
        self.pause_gate = _PipelinePauseGate(runner, job["id"])
        self.pause_context = threading.local()

    def cancellation_requested(self):
        probe = getattr(self.runner, "cancellation_requested", None)
        if probe is not None:
            return probe(self.job["id"])
        return self.runner.is_cancelled(self.job["id"])

    def pause_checkpoint(self):
        participant = getattr(self.pause_context, "participant", None)
        if participant is None:
            return self.cancellation_requested()
        # checkpoint() performs its own pause test. Suspend around the whole
        # call so a pause arriving between two separate probes cannot park a
        # resource waiter while its contention clock is still running. When
        # there is no pause, this excludes only the negligible probe itself.
        with suspend_resource_wait_timing():
            cancelled = self.pause_gate.checkpoint(participant)
        if cancelled:
            self.abort.set()
        return cancelled

    def should_abort(self, abort_event):
        """Shadow the module-level helper inside this run so the existing safe
        cancellation boundaries double as pause checkpoints.  Calls made from a
        library-owned helper thread have no registered participant and remain a
        non-blocking cancellation probe; the owning pipeline worker parks at its
        next outer boundary instead."""
        if self.pause_checkpoint():
            abort_event.set()
        # Resolve through the module namespace so tests and diagnostics that
        # replace the pipeline's abort policy still observe every checkpoint.
        return globals()["_should_abort"](abort_event)

    def should_abort_without_pause(self, abort_event):
        """Check cancellation without parking while a shared lock is held."""
        if self.cancellation_requested():
            abort_event.set()
        return globals()["_should_abort"](abort_event)

    def pause_or_cancel_pending(self):
        """Non-parking pause/cancel probe for ledger waits held under a lock.

        The bound ``_pause_checkpoint`` probe parks on pause, so a resource
        wait launched from a critical section (e.g. inside
        ``acquire_photo_mask``) would keep that lock held for the entire
        pause. Swap this probe in around such critical sections and the
        ledger raises ``ResourceWaitCancelled`` instead, unwinding out of
        the lock so the outer stage can park at a safe boundary.
        """
        probe = getattr(self.runner, "pause_requested", None)
        if probe is not None and probe(self.job["id"]):
            return True
        return self.cancellation_requested()

    def pipeline_control(self):
        return PipelineControl(
            should_abort=self.should_abort,
            should_abort_without_pause=self.should_abort_without_pause,
            pause_checkpoint=self.pause_checkpoint,
            cancellation_requested=self.cancellation_requested,
            pause_or_cancel_pending=self.pause_or_cancel_pending,
        )

    def run_pause_participant(self, participant, work_fn, *, pre_registered=False):
        if not pre_registered:
            self.pause_gate.register(participant)
        self.pause_context.participant = participant
        try:
            # Context variables do not propagate into Python threads. Bind
            # each pipeline participant explicitly so scanner/model waits are
            # attributed to the parent job's diagnostics, and so ledger waits
            # (including CPU inference on the ``cpu_ml`` lane) wake promptly
            # on cancellation or park promptly on pause instead of blocking
            # until the current holder releases. The pure-cancel probe is
            # bound alongside the pause-aware one so ``acquire_session_cache_lock``
            # can release on cancel without parking the lock holder inside
            # ``wait_if_paused`` — that would keep every unpaused peer
            # waiting on the same DINO/detector/SAM/keypoint model until
            # Resume.
            with (
                bind_resource_owner(self.job["id"]),
                bind_resource_cancel_check(self.pause_checkpoint),
                bind_resource_pure_cancel_check(self.cancellation_requested),
            ):
                self.pause_checkpoint()
                return work_fn()
        finally:
            self.pause_context.participant = None
            self.pause_gate.unregister(participant)

    def start_cancel_watcher(self):
        """Bridge user-initiated cancellation (runner.cancel_job) to the local
        abort Event so all stages that already honor `abort` stop promptly."""
        cancel_watcher_stop = threading.Event()
        cancel_watcher = threading.Thread(
            target=self._watch_for_cancel, args=(cancel_watcher_stop,),
            daemon=True,
        )
        cancel_watcher.start()
        return cancel_watcher_stop

    def _watch_for_cancel(self, cancel_watcher_stop):
        while not cancel_watcher_stop.is_set():
            # This watcher must never park for Pause: it is not pipeline
            # work and would otherwise publish ``paused`` before the real
            # stage workers have reached safe checkpoints.
            if self.cancellation_requested():
                self.abort.set()
                return
            if cancel_watcher_stop.wait(0.25):
                return


def _effective_cache_dirs(db_path, thumb_cache_dir):
    """Return the run's (thumbnail cache dir, vireo dir)."""
    # Effective thumbnail cache directory for every internal call below.
    # Falls back to the historical ``<db_dir>/thumbnails`` convention when
    # the caller didn't supply an explicit value — matches prior behavior
    # for the default ~/.vireo layout.
    effective_thumb_cache_dir = thumb_cache_dir or os.path.join(
        os.path.dirname(db_path), "thumbnails",
    )
    # vireo_dir must match the Flask serve convention — app.py computes
    # ``vireo_dir = os.path.dirname(THUMB_CACHE_DIR)`` for
    # previews/working. When the caller provided thumb_cache_dir
    # explicitly we derive from its parent; otherwise fall back to the
    # db_dir (same as the historical layout where everything sits
    # alongside vireo.db).
    effective_vireo_dir = (
        os.path.dirname(thumb_cache_dir)
        if thumb_cache_dir
        else os.path.dirname(db_path)
    )
    return effective_thumb_cache_dir, effective_vireo_dir


def _resolve_archive_destination(params):
    """Return the run's (final_destination, remote_archive)."""
    final_destination = params.destination if params.local_processing else None
    remote_archive = None
    if params.local_processing and params.remote_target_id:
        # Prefer the snapshot captured at enqueue time so a settings edit
        # between click-Start and slot-open cannot redirect the archive to a
        # different host/mount than the jobs panel is showing. The Settings
        # fallback is a last resort for callers (mainly tests) that build
        # PipelineParams by hand without pre-resolving the target.
        target = params.remote_target_snapshot
        if not target:
            import config as _cfg_mod

            target = _cfg_mod.get_remote_target(params.remote_target_id)
        if not target:
            raise RuntimeError(
                f"Remote target '{params.remote_target_id}' not found — it "
                "may have been removed from Settings after this job was "
                "queued. Pick a saved remote target and retry."
            )
        try:
            remote_archive = resolve_remote_archive(
                target, params.remote_subpath,
            )
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        # Everything below that keys off final_destination locally — the
        # in-flight destination reservation, the tracked-destination
        # preflight, the staging-root name — cares about where the CATALOG
        # will point after the archive, which for a remote destination is
        # the target's local mount path, not the NAS-side path.
        final_destination = remote_archive["mount_final"]
    return final_destination, remote_archive


def _reserve_archive_destination(job, params, final_destination,
                                 effective_vireo_dir):
    """Reserve the archive destination; return whether it was reserved."""
    if not (params.local_processing and final_destination):
        return False
    from local_processing import staging_root

    # Reserve the final destination across the whole process BEFORE any
    # staging or scanning starts. SLOT_CAP=2 in JobRunner means two
    # local-processing pipelines can race past the storage stage's
    # DB-only overlap check; without this reservation the second one
    # would only fail inside ``move_folder`` after staging and
    # processing everything. ``release_archive_destination`` runs in
    # the finally below so retries can re-claim once this run ends.
    if not try_reserve_archive_destination(final_destination):
        raise RuntimeError(
            f"Archive destination {final_destination} is already "
            "being used by another local-processing pipeline. Wait "
            "for that job to finish, or pick a different destination."
        )

    # final_destination (not params.destination, which is None for a
    # remote archive): the staging root's basename is the leaf the
    # archive move lands at, and for remote that's the mount-path leaf —
    # the same last-subpath-segment as the NAS side.
    params.destination = staging_root(
        effective_vireo_dir, job["id"], final_destination,
    )
    return True


def _scope_to_source_snapshot(params, db_path, workspace_id):
    """Snapshot-scoped pipelines: load the snapshot up front so scan targets
    are derived from the captured file paths (not a folder the user picked
    later). Raises if the snapshot has been garbage-collected — the API
    layer is expected to return 404 before this job ever runs, but we fail
    loud here to avoid silently running an unbounded scan."""
    snapshot_paths: list[str] | None = None
    if params.source_snapshot_id is not None:
        db_ro = Database(db_path)
        db_ro.set_active_workspace(workspace_id)
        snap = db_ro.get_new_images_snapshot(params.source_snapshot_id)
        if snap is None:
            raise ValueError(
                f"snapshot {params.source_snapshot_id} not found"
            )
        snapshot_paths = list(snap["file_paths"])
        # Collapse to the minimal non-overlapping ancestor set: if the
        # snapshot has files at both /root/a.jpg and /root/sub/b.jpg the
        # naive derived roots (/root, /root/sub) would make the scanner walk
        # /root/sub twice — once on its own, once as a descendant of /root.
        scan_roots = _collapse_scan_roots(
            [os.path.dirname(p) for p in snapshot_paths]
        )
        # Override any source/sources/collection_id the caller passed; the
        # snapshot is the single source of truth for what to scan.
        params.sources = scan_roots
        params.source = None
        params.collection_id = None
    return snapshot_paths


def _initial_stages():
    return {
        "storage": {"status": "pending", "label": "Checking local storage"},
        "ingest": {"status": "pending", "count": 0, "label": "Importing photos"},
        "scan": {"status": "pending", "count": 0, "label": "Checking metadata"},
        "thumbnails": {"status": "pending", "count": 0, "label": "Generating thumbnails"},
        "previews": {"status": "pending", "count": 0, "label": "Generating previews"},
        "model_loader": {"status": "pending", "label": "Loading models"},
        "detect": {"status": "pending", "count": 0, "label": "Detecting subjects"},
        "classify": {"status": "pending", "count": 0, "cached": 0, "seen": 0, "label": "Classifying species"},
        "extract_masks": {"status": "pending", "count": 0, "label": "Extracting features"},
        "eye_keypoints": {"status": "pending", "count": 0, "label": "Detecting eye keypoints"},
        "regroup": {"status": "pending", "label": "Grouping encounters"},
        "misses": {"status": "pending", "count": 0, "label": "Flagging missed shots"},
        "archive": {"status": "pending", "count": 0, "label": "Archiving photos"},
    }


def _effective_model_ids(params):
    """Normalize model_ids: prefer the explicit list, fall back to the legacy
    single `model_id`, and finally to `[]` which means "use the active model
    from config." This is the knob the multi-model fix hangs off of."""
    if params.model_ids:
        return list(params.model_ids)
    elif params.model_id:
        return [params.model_id]
    else:
        return []


def _resolve_model_specs(params, effective_model_ids):
    """Resolve model specs EARLY so per-model `classify:<id>` step_defs can
    carry the model's display name as their label. Labels are immutable
    after set_steps, so we cannot defer this to model_loader_stage.

    Resolution failures are captured (not raised) so the job still sets up
    its step tree and the model_loader stage can surface a clean error.
    For any id we fail to resolve we still emit a per-model step — labeled
    with the id — so the user sees exactly which model broke."""
    resolved_specs: list = []
    resolution_error: str | None = None
    if not params.skip_classify:
        try:
            from models import get_active_model, get_models
            if effective_model_ids:
                by_id = {m["id"]: m for m in get_models()}
                for mid in effective_model_ids:
                    spec = by_id.get(mid)
                    if not spec or not spec.get("downloaded"):
                        raise RuntimeError(
                            f"Model '{mid}' not found or not downloaded."
                        )
                    resolved_specs.append(spec)
            else:
                spec = get_active_model()
                if not spec:
                    raise RuntimeError(
                        "No model available. Download one in Settings."
                    )
                resolved_specs.append(spec)
        except Exception as e:
            # Reported to the user through the pipeline's model step.
            log.warning("Could not resolve pipeline models", exc_info=True)
            resolution_error = str(e)
    return resolved_specs, resolution_error


def _pipeline_step_defs(params, effective_model_ids, resolved_specs,
                        resolution_error):
    """Define step tracking for the jobs page."""
    step_defs = []
    if params.destination:
        if params.local_processing:
            step_defs.append({"id": "storage", "label": "Check local storage"})
        step_defs.append({"id": "ingest", "label": "Import photos"})
    step_defs.extend([
        {"id": "scan", "label": "Check metadata"},
        {"id": "thumbnails", "label": "Generate thumbnails"},
        {"id": "previews", "label": "Generate previews"},
    ])
    if not params.skip_classify:
        step_defs.append({"id": "model_loader", "label": "Load models"})
        step_defs.append({"id": "detect", "label": "Detect subjects"})
        step_defs.extend(_classify_step_defs(
            effective_model_ids, resolved_specs, resolution_error,
        ))
    if not params.skip_extract_masks:
        step_defs.append({"id": "extract_masks", "label": "Extract features"})
        step_defs.append({"id": "eye_keypoints", "label": "Detect eye keypoints"})
    if not params.skip_regroup:
        step_defs.append({"id": "regroup", "label": "Group encounters"})
        step_defs.append({"id": "misses", "label": "Flag missed shots"})
    elif not params.skip_classify:
        step_defs.append({"id": "regroup", "label": "Prepare review"})
    if params.local_processing:
        step_defs.append({"id": "archive", "label": "Archive to destination"})
    return step_defs


def _classify_step_defs(effective_model_ids, resolved_specs, resolution_error):
    """One row per model — label = model display name, id = classify:<mid>.

    When resolution partially failed (e.g. 3 ids requested, 2nd not
    downloaded), resolved_specs is a non-empty prefix of the requested
    list. Emitting rows from resolved_specs alone would hide the later
    failed ids — their "failed" update_step calls would then no-op
    silently. Drive row creation off effective_model_ids whenever
    resolution reported an error, so every requested model has a visible
    step the model_loader stage can mark 'failed'."""
    step_defs = []
    if resolved_specs and not resolution_error:
        for spec in resolved_specs:
            step_defs.append({
                "id": f"classify:{spec['id']}",
                "label": f"Classify with {spec['name']}",
            })
    elif effective_model_ids:
        # Partial or total resolution failure: use display names from any
        # resolved specs we did get, fall back to the raw id otherwise.
        by_id = {s["id"]: s for s in resolved_specs}
        for mid in effective_model_ids:
            spec = by_id.get(mid)
            label = (
                f"Classify with {spec['name']}" if spec
                else f"Classify with {mid}"
            )
            step_defs.append({
                "id": f"classify:{mid}",
                "label": label,
            })
    else:
        # No ids, no resolved spec (active-model resolution failed).
        # One placeholder row keeps the step tree consistent.
        step_defs.append({
            "id": "classify:__unresolved__",
            "label": "Classify species",
        })
    return step_defs


class _StageState:
    """Containers and helpers shared between one run's stages."""

    def __init__(self, params, control):
        self.params = params
        self.control = control
        self.scan_to_thumb = queue.Queue(maxsize=200)
        self.collected_photo_ids = []
        self.collection_ready = threading.Event()
        self.models_ready = threading.Event()
        # Set when the model loader fails for a reason other than cancel; the
        # orchestrator turns it into ``abort`` once the model-free stages have
        # finished (see model_loader_stage).
        self.model_loader_failed = threading.Event()
        self.loaded_models = {}  # populated by model_loader thread

        # Shared state between detect_stage and classify_stage. Written by
        # detect_stage, consumed by classify_stage. Populated even on early
        # exit so classify_stage can reason about "detection ran but produced
        # nothing" vs. "detection never executed".
        self.detect_state = {
            "photos": [],        # list of photo dicts for the collection
            "folders": {},       # {folder_id: path}
            "detections": {},    # {photo_id: [detection_dict, ...]}
            "processed_ids": set(),  # photo_ids whose _detect_batch iteration completed
            "pre_run_det_ids": {},   # snapshot for reclassify purge
            "total_detected": 0,
            "ran": False,        # True once detect_stage's body executed (even if no-op)
        }

        # Photos classify skipped because their folder went offline mid-run
        # (folder-scoped outage; abort stays clear so healthy folders keep
        # processing). Downstream source-reading stages (extract_masks,
        # eye_keypoints) filter these out so they don't walk back into the
        # missing folder and re-record the same photos as mask/keypoint
        # failures, obscuring the intended source-offline diagnosis
        # (Codex #1388 P2 r3664058173). Held on a dict rather than
        # ``stages["classify"]`` because ``_update_stages`` pushes stage
        # dicts as JSON to SSE consumers, and a raw ``set`` isn't
        # JSON-serialisable.
        self.source_offline_state: dict = {"skipped_photo_ids": set()}

    def put_scan_item(self, item):
        """Put into the scan/thumbnail queue without defeating Pause.

        Both ordinary photo items and the end sentinel can otherwise block
        forever on a full queue after the thumbnail worker has parked.
        """
        while not self.control.should_abort(self.control.abort):
            try:
                self.scan_to_thumb.put(item, timeout=0.5)
                return True
            except queue.Full:
                continue
        return False

    @contextlib.contextmanager
    def prepare_scan_item(self, item):
        """Wait for Pause/backpressure before entering a publication lock.

        The scanner is the queue's only producer. Once it observes space,
        only the consumer can change its occupancy until this publication,
        so the guarded enqueue needs neither a wait nor a pause checkpoint.
        """
        while not self.control.should_abort(self.control.abort):
            with self.scan_to_thumb.not_full:
                if self.scan_to_thumb._qsize() < self.scan_to_thumb.maxsize:
                    break
                self.scan_to_thumb.not_full.wait(timeout=0.5)
        else:
            yield None
            return
        yield lambda: self.scan_to_thumb.put_nowait(item)

    def filter_excluded(self, photos):
        """Remove photos excluded by user selection in preview."""
        if not self.params.exclude_photo_ids:
            return photos
        return [p for p in photos if p["id"] not in self.params.exclude_photo_ids]


def _wire_scan_stages(run, shared, *, skip_scan, snapshot_paths,
                      effective_thumb_cache_dir, effective_vireo_dir,
                      final_destination, remote_archive,
                      missing_originals_invalidator):
    """Bind the scan, collection, thumbnail and preview stages to this run."""
    scanner_stage = partial(
        scanning.scanner_stage, run=run,
        _SENTINEL=_SENTINEL,
        _filter_excluded=shared.filter_excluded,
        _find_broken_metadata_folders=_find_broken_metadata_folders,
        _missing_archive_mount_root=_missing_archive_mount_root,
        _put_scan_item=shared.put_scan_item,
        _prepare_scan_item=shared.prepare_scan_item,
        collected_photo_ids=shared.collected_photo_ids,
        effective_thumb_cache_dir=effective_thumb_cache_dir,
        effective_vireo_dir=effective_vireo_dir,
        final_destination=final_destination,
        missing_originals_invalidator=missing_originals_invalidator,
        remote_archive=remote_archive,
        skip_scan=skip_scan,
        snapshot_paths=snapshot_paths,
    )

    _collection_stage_body = partial(
        scanning._collection_stage_body, run=run,
        collected_photo_ids=shared.collected_photo_ids,
        skip_scan=skip_scan,
        snapshot_paths=snapshot_paths,
    )

    collection_stage = partial(
        scanning.collection_stage, run=run,
        _collection_stage_body=_collection_stage_body,
        collection_ready=shared.collection_ready,
    )

    thumbnail_stage = partial(
        media.thumbnail_stage, run=run,
        _RAW_EXTENSIONS=_RAW_EXTENSIONS,
        _SENTINEL=_SENTINEL,
        _filter_excluded=shared.filter_excluded,
        _recipe_render_source=_recipe_render_source,
        _retry_thumbnail_with_companion=_retry_thumbnail_with_companion,
        _retry_thumbnail_with_working_copy=_retry_thumbnail_with_working_copy,
        _thumb_min_source_size_kwargs=_thumb_min_source_size_kwargs,
        _thumb_raw_decode_kwargs=_thumb_raw_decode_kwargs,
        effective_thumb_cache_dir=effective_thumb_cache_dir,
        effective_vireo_dir=effective_vireo_dir,
        scan_to_thumb=shared.scan_to_thumb,
        skip_scan=skip_scan,
    )

    previews_stage = partial(
        media.previews_stage, run=run,
        _filter_excluded=shared.filter_excluded,
        effective_vireo_dir=effective_vireo_dir,
        skip_scan=skip_scan,
    )
    return {
        "scanner": scanner_stage,
        "collection": collection_stage,
        "thumbnail": thumbnail_stage,
        "previews": previews_stage,
    }


def _wire_model_stages(run, control, shared, *, effective_vireo_dir,
                       computation_cache_dir, effective_model_ids,
                       resolved_specs, resolution_error):
    """Bind the model, detection, classification, feature and grouping
    stages to this run."""
    _load_model_bundle = partial(
        models.load_model_bundle, run=run,
        _incomplete_model_message=_incomplete_model_message,
        _looks_like_missing_external_data=_looks_like_missing_external_data,
        pause_context=control.pause_context,
    )

    model_loader_stage = partial(
        models.model_loader_stage, run=run,
        _filter_excluded=shared.filter_excluded,
        _load_model_bundle=_load_model_bundle,
        loaded_models=shared.loaded_models,
        model_loader_failed=shared.model_loader_failed,
        models_ready=shared.models_ready,
        resolution_error=resolution_error,
        resolved_specs=resolved_specs,
    )

    detect_stage = partial(
        detection.detect_stage, run=run,
        _filter_excluded=shared.filter_excluded,
        collection_ready=shared.collection_ready,
        computation_cache_dir=computation_cache_dir,
        detect_state=shared.detect_state,
        effective_vireo_dir=effective_vireo_dir,
        loaded_models=shared.loaded_models,
        models_ready=shared.models_ready,
    )

    classify_stage = partial(
        classification.classify_stage, run=run,
        _MAX_SOURCE_OFFLINE_PAUSES=_MAX_SOURCE_OFFLINE_PAUSES,
        _cached_classify_detections=_cached_classify_detections,
        _classification_eta_progress=_classification_eta_progress,
        _load_model_bundle=_load_model_bundle,
        _record_unattempted_cache_hit=_record_unattempted_cache_hit,
        _release_classifier_cache_handle=_release_classifier_cache_handle,
        _remove_attempted_cache_hits=_remove_attempted_cache_hits,
        _source_offline_reason=_source_offline_reason,
        detect_state=shared.detect_state,
        effective_model_ids=effective_model_ids,
        loaded_models=shared.loaded_models,
        source_offline_state=shared.source_offline_state,
    )

    extract_masks_stage = partial(
        features.extract_masks_stage, run=run,
        _StagedMaskFile=_StagedMaskFile,
        _extract_masks_early_exit=_extract_masks_early_exit,
        _filter_excluded=shared.filter_excluded,
        _preflight_mask_outcomes=_preflight_mask_outcomes,
        _rollback_failed_mask_photo=_rollback_failed_mask_photo,
        _source_offline_reason=_source_offline_reason,
        _still_offline_folder_ids_of=_still_offline_folder_ids_of,
        source_offline_state=shared.source_offline_state,
    )

    eye_keypoints_stage = partial(
        features.eye_keypoints_stage, run=run,
        _still_offline_folder_ids_of=_still_offline_folder_ids_of,
        source_offline_state=shared.source_offline_state,
    )

    regroup_stage = partial(
        grouping.regroup_stage, run=run,
    )

    miss_stage = partial(
        grouping.miss_stage, run=run,
    )
    return {
        "model_loader": model_loader_stage,
        "detect": detect_stage,
        "classify": classify_stage,
        "extract_masks": extract_masks_stage,
        "eye_keypoints": eye_keypoints_stage,
        "regroup": regroup_stage,
        "misses": miss_stage,
    }


def _run_phase_one(control, stage_fns):
    """Phase 1: scan + thumbnails + model loading (concurrent)."""
    threads = {}

    phase_one = {
        "scanner": stage_fns["scanner"],
        "collection": stage_fns["collection"],
        "thumbnail": stage_fns["thumbnail"],
        "model_loader": stage_fns["model_loader"],
    }
    # Register the complete phase before starting its first thread.  If the
    # first worker reaches Pause immediately, it must wait for the other
    # three rather than declaring the whole pipeline paused on its own.
    control.pause_gate.register_many(phase_one)
    for name, stage_fn in phase_one.items():
        threads[name] = threading.Thread(
            target=control.run_pause_participant,
            args=(name, stage_fn),
            kwargs={"pre_registered": True},
            daemon=True,
        )

    for t in threads.values():
        t.start()

    # Wait for scan-related threads to finish
    threads["scanner"].join()
    threads["collection"].join()
    threads["thumbnail"].join()
    threads["model_loader"].join()


def _run_later_phases(control, shared, stage_fns, workspace_id, archive_stage):
    """Run every stage after phase one, one pause participant at a time."""
    # Phase 1.5: previews (needs scan complete, runs before classify).
    #
    # This and every later stage are always invoked — even when `abort`
    # is set — so their step rows reach a terminal status. Gating the
    # call on abort would leave the runner.set_steps-created rows
    # persisted as "pending" with no finished_at, forever. Each stage
    # checks abort internally and marks itself "Skipped".
    control.run_pause_participant("previews", stage_fns["previews"])

    # A model-loader failure deferred its abort so the model-free stages
    # above could finish; from here on every stage needs the model (or
    # its output), so stop them now.
    if shared.model_loader_failed.is_set():
        control.abort.set()

    # Phase 2: detect (needs collection; runs MegaDetector once across all
    # photos so each per-model classify step reuses cached detections
    # instead of re-running the detector).
    #
    # Always invoked — even when `abort` is set by an earlier stage — so
    # the `detect` step row reaches a terminal status. Skipping the call
    # would leave the row pending forever on a model-loader failure.
    # detect_stage handles abort internally and marks itself skipped.
    control.run_pause_participant("detect", stage_fns["detect"])

    # Phase 3: classify per model (reads cached detections from detect_stage).
    # Always invoked for the same reason: every `classify:<model_id>` row
    # must land in a terminal state so the jobs tree finalizes cleanly on
    # a loader-triggered abort.
    control.run_pause_participant("classify", stage_fns["classify"])

    # Phase 3: extract-masks (needs classify output)
    control.run_pause_participant("extract_masks", stage_fns["extract_masks"])

    # Phase 3.5: eye keypoints (needs masks + classifier output). No-op when
    # SuperAnimal weights are absent — users opt in on the pipeline models
    # card. Per-photo failures log and continue rather than abort the stage.
    control.run_pause_participant("eye_keypoints", stage_fns["eye_keypoints"])

    control.run_pause_participant(
        "regroup_and_misses",
        partial(
            _run_regroup_and_misses, control, workspace_id,
            stage_fns["regroup"], stage_fns["misses"],
        ),
    )

    control.run_pause_participant("archive", archive_stage)


def _run_regroup_and_misses(control, workspace_id, regroup_stage, miss_stage):
    """Phases 4 + 5: regroup and miss detection. Held under the
    per-workspace regroup lock TOGETHER so a concurrent same-workspace
    pipeline can't slip a regroup_stage in between this run's
    regroup_stage and its miss_stage — that would leave the persisted
    miss flags + ``miss_computed_at`` paired with a grouping
    (burst_id / pipeline_results_ws*.json) the miss computation never
    saw. Pipelines targeting different workspaces share neither
    stage's state and don't contend here.

    miss_stage's own gate covers the regroup-failed and abort cases, so
    both stages can be invoked unconditionally and still reach a
    terminal step status."""
    if control.abort.is_set():
        # Both stages early-return as "Skipped" without touching
        # grouping state, so the lock isn't needed — and skipping the
        # calls would leave their step rows pending forever. Staying
        # outside the lock also keeps an aborted/cancelled run from
        # blocking behind a concurrent pipeline's regroup.
        regroup_stage()
        miss_stage()
    else:
        # Pause checkpoints deliberately surround this critical
        # section rather than living inside either stage: their shared
        # lock must span regroup + misses atomically, but a paused job
        # must not retain it and block another pipeline indefinitely.
        with acquire_workspace_regroup(workspace_id):
            regroup_stage()
            miss_stage()
    control.pause_checkpoint()


def _raise_if_stages_failed(job, result, stages, errors, cancellation_requested):
    """If any stage ended in 'failed' and the job wasn't cancelled, propagate
    the failure so JobRunner marks the whole job as failed rather than
    silently recording it as completed. Cancellation takes precedence:
    a cancelled job stays cancelled even if stages crashed on the way down."""
    failed_stages = [
        name for name, s in stages.items() if s.get("status") == "failed"
    ]
    if failed_stages and not cancellation_requested():
        # Stash the structured result on the job BEFORE raising so the
        # completion event and job_history still carry per-stage details
        # (stages dict, errors list). Without this, the pipeline UI loses
        # the "Failed: [stage_name]" mapping on the card that owned the
        # failure because it reads result.result.stages / .errors.
        job["result"] = result
        # Prefer a "[stage] Fatal: …" error from one of the failed stages
        # rather than blindly using errors[0], which may be a non-fatal
        # per-photo warning (e.g. "Photo <id>: mask extraction failed")
        # logged before the stage-level failure. Falling back to errors[0]
        # when no stage-fatal entry exists keeps backward compatibility for
        # any edge case where a stage marks itself failed without appending a
        # Fatal error; the final fallback covers an empty errors list.
        first_error = next(
            (e for e in errors if any(e.startswith(f"[{s}] Fatal:") for s in failed_stages)),
            errors[0] if errors else f"stage '{failed_stages[0]}' failed",
        )
        # Record the fatal error for _persist_job so it can store the stage
        # failure message rather than job["errors"][0], which may be a
        # non-fatal per-photo warning that was logged before this failure.
        job["_fatal_error"] = first_error
        raise RuntimeError(first_error)


def run_pipeline_job(job, runner, db_path, workspace_id, params,
                     thumb_cache_dir=None,
                     missing_originals_invalidator=None,
                     computation_cache_dir=None):
    """Execute streaming pipeline. Called by JobRunner in a background thread.

    Args:
        job: job dict from JobRunner (has id, progress, errors, etc.)
        runner: JobRunner instance for push_event()
        db_path: path to SQLite database
        workspace_id: active workspace ID
        params: PipelineParams with request parameters
        thumb_cache_dir: configured thumbnail cache directory. Forwarded
            to scanner.scan() and used by the thumbnail stage so the
            pipeline writes and invalidates the real cache even when
            ``--thumb-dir`` points outside ``dirname(db_path)/thumbnails``.
            Defaults to that convention for backward compatibility.
        missing_originals_invalidator: optional zero-arg callable that
            drops the Flask app's Missing Originals cache for this DB.
            Called after every scanned root in the finally block, mirroring
            api_job_scan / api_job_import_full so a pipeline scan that
            touches disk doesn't leave GET /api/photos/missing serving a
            pre-scan ghost list.
        computation_cache_dir: portable-cache root the HTTP import route
            writes to (``app.config["COMPUTATION_CACHE_DIR"]``). When
            provided, the detect stage reads from the same store so bundles
            imported before their photos were cataloged still plant outside
            the default location.

    Returns:
        dict with stage results, duration, and errors
    """
    job["_start_time"] = time.time()
    # Stash the job's configured cache root so downstream helpers
    # (``publish_detection_artifact`` inside ``_detect_batch``,
    # ``promote_and_publish_classifier_run`` inside the per-model publish
    # pass) write freshly computed artifacts into the SAME cache the
    # status / export / catalog-reapplication paths use when
    # ``COMPUTATION_CACHE_DIR`` is overridden. Without this the publishers
    # fall back to the default ``~/.vireo/computation-cache`` and the
    # artifacts disappear from the configured cache as soon as the backing
    # database rows are removed. The path (not an ``ArtifactStore``
    # instance) is stashed so ``jsonify(job)`` on the /api/jobs/<id> route
    # keeps working — the helpers reconstruct the wrapper on demand.
    job["_computation_cache_dir"] = computation_cache_dir
    control = _RunControl(runner, job)

    errors = job["errors"]  # shared list, append is thread-safe
    if params.destination or params.local_processing or params.remote_target_id:
        raise RuntimeError(
            "Pipeline import/archive mode has been removed. Use the Import "
            "page or /api/jobs/import-photos to copy photos into the archive, "
            "then run Process on the imported workspace photos."
        )

    effective_thumb_cache_dir, effective_vireo_dir = _effective_cache_dirs(
        db_path, thumb_cache_dir,
    )
    final_destination, remote_archive = _resolve_archive_destination(params)
    archive_destination_reserved = _reserve_archive_destination(
        job, params, final_destination, effective_vireo_dir,
    )

    try:
        snapshot_paths = _scope_to_source_snapshot(params, db_path, workspace_id)
        cancel_watcher_stop = control.start_cancel_watcher()
        stages = _initial_stages()
        effective_model_ids = _effective_model_ids(params)
        resolved_specs, resolution_error = _resolve_model_specs(
            params, effective_model_ids,
        )
        runner.set_steps(job["id"], _pipeline_step_defs(
            params, effective_model_ids, resolved_specs, resolution_error,
        ))

        result = {"stages": {}}
        collection_id = params.collection_id
        shared = _StageState(params, control)
        skip_scan = collection_id is not None

        # Mark ingest as skipped when not in copy mode so SSE events
        # don't show a perpetually-pending stage.
        if not params.destination:
            stages["ingest"]["status"] = "skipped"
        if not params.local_processing:
            stages["storage"]["status"] = "skipped"
            stages["archive"]["status"] = "skipped"

        def archive_stage():
            """Retired import/archive path guard.

            Importing photos now runs through import_job.py and
            /api/jobs/import-photos. The old local-processing archive stage
            intentionally has no cleanup/deindex path left here: orphaned
            staging folders are reconciled by staging_recovery.py before any
            deletion is offered.
            """
            if params.local_processing or params.destination:
                raise RuntimeError(
                    "Pipeline import/archive mode has been removed. Use the "
                    "Import page or /api/jobs/import-photos to copy photos "
                    "into the archive."
                )
            return

        run = PipelineRun(
            job=job, runner=runner, db_path=db_path,
            workspace_id=workspace_id, params=params, abort=control.abort,
            stages=stages, result=result, errors=errors,
            collection_id=collection_id, database_factory=Database,
            emit_progress=_emit_progress, update_stages=_update_stages,
            control=control.pipeline_control(),
        )
        stage_fns = _wire_scan_stages(
            run, shared,
            skip_scan=skip_scan,
            snapshot_paths=snapshot_paths,
            effective_thumb_cache_dir=effective_thumb_cache_dir,
            effective_vireo_dir=effective_vireo_dir,
            final_destination=final_destination,
            remote_archive=remote_archive,
            missing_originals_invalidator=missing_originals_invalidator,
        )
        stage_fns.update(_wire_model_stages(
            run, control, shared,
            effective_vireo_dir=effective_vireo_dir,
            computation_cache_dir=computation_cache_dir,
            effective_model_ids=effective_model_ids,
            resolved_specs=resolved_specs,
            resolution_error=resolution_error,
        ))

        # --- Launch threads ---
        _run_phase_one(control, stage_fns)
        _run_later_phases(control, shared, stage_fns, workspace_id, archive_stage)

        cancel_watcher_stop.set()

        elapsed = time.time() - job["_start_time"]
        result["duration"] = round(elapsed, 1)
        result["errors"] = list(errors)
        result["notes"] = list(run.notes)

        _raise_if_stages_failed(
            job, result, stages, errors, control.cancellation_requested,
        )
        # No stage failed, so the run's own verdict decides the status
        # instead of the runner's any-error rollup. ``errors`` also carries
        # notes that explain a benign skip (no detections to mask, an
        # optional weight download that failed); those keep the run green.
        # Anything else recorded there still failed it, e.g. a directory
        # the scan was refused, which leaves the scan stage "completed".
        # The notes stay in ``errors`` (and ``result["notes"]`` names them)
        # so the Process page and Jobs details keep showing them.
        noted = set(run.notes)
        result["ok"] = all(e in noted for e in errors)
        return result
    finally:
        if archive_destination_reserved:
            release_archive_destination(final_destination)
