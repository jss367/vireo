"""Streaming pipeline job -- overlaps I/O stages and interleaves detect+classify.

This module orchestrates the full pipeline (scan -> thumbnails -> classify ->
extract-masks -> regroup) as a single background job with concurrent stages
connected by queues.

Existing standalone jobs (/api/jobs/scan, /api/jobs/classify, etc.) are
untouched. This is an additive orchestration layer.
"""

import contextlib
import json
import logging
import math
import os
import queue
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass

import numpy as np
from artifact_flight import ArtifactProducerFailed
from classifier_cache import acquire_cached_classifier
from db import Database, commit_with_retry
from job_contract import progress_event
from pipeline_locks import (
    acquire_photo_mask,
    acquire_workspace_regroup,
    release_archive_destination,
    try_reserve_archive_destination,
)
from preview_materializer import (
    PreviewMaterializationError,
    PreviewSourceUnavailable,
    materialize_preview,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
)
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
    ResourceWaitCancelled,
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
        os.replace(self.staged_path, self.final_path)
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
    weak_detection_confidence=None,
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
            "SELECT active_mask_variant, dino_embedding_variant "
            "FROM photos WHERE id = ?",
            (photo_id,),
        ).fetchone()
        if (
            existing["detector_model"] == primary["detector_model"]
            and cached_prompt == live_prompt
            and os.path.isfile(existing["path"])
            and state is not None
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


@dataclass
class PipelineParams:
    """Parameters for a streaming pipeline job."""

    collection_id: int | None = None
    source: str | None = None
    sources: list | None = None
    source_snapshot_id: int | None = None
    destination: str | None = None
    local_processing: bool = False
    # Remote (SSH) archive destination for local-processing runs: the id of a
    # saved remote target (config remote_targets) plus a required relative
    # subpath naming the archive folder under the target's base paths.
    # Mutually exclusive with ``destination``; resolved via
    # ``resolve_remote_archive``. The staged tree is rsynced over SSH to
    # ``remote_path/subpath`` and the catalog is repointed at
    # ``mount_path/subpath``, mirroring the Move page's remote folder moves.
    remote_target_id: str | None = None
    remote_subpath: str = ""
    # Snapshot of the resolved remote target dict (from cfg.get_remote_target)
    # captured at ENQUEUE time so a queued run archives to the destination the
    # user saw when they clicked Start, not whatever the saved target got edited
    # to before the pipeline slot opened. The API always populates this
    # alongside ``remote_target_id``; when it is None, ``run_pipeline_job``
    # falls back to re-reading the mutable target (mostly for direct-call
    # tests). Mirrors how the move-folder endpoint builds its remote spec
    # before enqueueing.
    remote_target_snapshot: dict | None = None
    file_types: str = "both"
    folder_template: str = "%Y/%Y-%m-%d"
    skip_duplicates: bool = True
    # Identify duplicates by content hash alone (reads every byte of every
    # source file). Default False: metadata-first matching with a hash
    # fallback — see import_dedup.
    verify_by_hash: bool = False
    labels_file: str | None = None
    labels_files: list | None = None
    model_id: str | None = None
    model_ids: list | None = None
    reclassify: bool = False
    skip_extract_masks: bool = False
    skip_regroup: bool = False
    # Distinguishes the identify preset's species-only review from a
    # generic ``skip_regroup=True`` run. Only set to ``"species"`` by
    # ``process_strategies.identify`` — Advanced/Custom on the Process
    # page and API clients sending ``skip_regroup: true`` without a
    # strategy leave this ``None`` so regroup_stage skips cleanly instead
    # of overwriting the workspace cache with all-REVIEW output.
    review_mode: str | None = None
    skip_classify: bool = False
    skip_eye_keypoints: bool = False
    # Per-run override for the config-gated eye-detect setting. Semantics
    # match miss_enabled: None defers to the workspace-effective
    # ``pipeline.eye_detect_enabled``, a bool wins over workspace config in
    # both directions. Set to True by the Process page when the user
    # explicitly checks the Eye Keypoints stage box — that box is a
    # per-run opt-in that must override the (default-off) Settings value
    # so preflight and scoring see the enabled state. Left None by
    # strategy expansion (the saved-process flag expansion) so a
    # ``full`` strategy chain from after-import respects the user's
    # Settings default instead of silently forcing eye detection on.
    eye_detect_override: bool | None = None
    # Per-run override for the config-gated misses stage. None defers to the
    # workspace-effective ``pipeline.miss_enabled`` (today's behavior); a
    # bool wins over workspace config in BOTH directions, mirroring how the
    # skip_* flags override workspace defaults. Process strategies
    # (process_strategies.py) set this so e.g. cull_ready suppresses misses
    # on a workspace that has them enabled.
    miss_enabled: bool | None = None
    download_taxonomy: bool = True
    # None means "use the workspace-effective preview_max_size setting".
    # Explicit values are kept for API/back-compat and tests that need to pin
    # a preview tier.
    preview_max_size: int | None = None
    exclude_paths: set | None = None
    exclude_photo_ids: set | None = None
    recursive: bool = True


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
    from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS
    return {"raw_decode": RAW_DECODE_PRESERVE_HIGHLIGHTS}


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
        with contextlib.suppress(Exception):
            thread_db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            commit_with_retry(thread_db.conn)
    recipe_kwargs = {"recipe": recipe} if recipe else {}
    if recipe:
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
        with contextlib.suppress(Exception):
            thread_db.conn.execute(
                "UPDATE photos SET"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                " WHERE id=?",
                (file_mtime, photo_id),
            )
            commit_with_retry(thread_db.conn)
    return generate_thumbnail(
        photo_id,
        wc_path,
        cache_dir,
        size=thumb_size,
        recipe=recipe,
        native_size=_recipe_source_dimensions(photo),
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
    abort = threading.Event()
    pause_gate = _PipelinePauseGate(runner, job["id"])
    pause_context = threading.local()

    def _cancellation_requested():
        probe = getattr(runner, "cancellation_requested", None)
        if probe is not None:
            return probe(job["id"])
        return runner.is_cancelled(job["id"])

    def _pause_checkpoint():
        participant = getattr(pause_context, "participant", None)
        if participant is None:
            return _cancellation_requested()
        # checkpoint() performs its own pause test. Suspend around the whole
        # call so a pause arriving between two separate probes cannot park a
        # resource waiter while its contention clock is still running. When
        # there is no pause, this excludes only the negligible probe itself.
        with suspend_resource_wait_timing():
            cancelled = pause_gate.checkpoint(participant)
        if cancelled:
            abort.set()
        return cancelled

    # Shadow the module-level helper inside this run so the existing safe
    # cancellation boundaries double as pause checkpoints.  Calls made from a
    # library-owned helper thread have no registered participant and remain a
    # non-blocking cancellation probe; the owning pipeline worker parks at its
    # next outer boundary instead.
    def _should_abort(abort_event):
        if _pause_checkpoint():
            abort_event.set()
        # Resolve through the module namespace so tests and diagnostics that
        # replace the pipeline's abort policy still observe every checkpoint.
        return globals()["_should_abort"](abort_event)

    def _should_abort_without_pause(abort_event):
        """Check cancellation without parking while a shared lock is held."""
        if _cancellation_requested():
            abort_event.set()
        return globals()["_should_abort"](abort_event)

    def _pause_or_cancel_pending():
        """Non-parking pause/cancel probe for ledger waits held under a lock.

        The bound ``_pause_checkpoint`` probe parks on pause, so a resource
        wait launched from a critical section (e.g. inside
        ``acquire_photo_mask``) would keep that lock held for the entire
        pause. Swap this probe in around such critical sections and the
        ledger raises ``ResourceWaitCancelled`` instead, unwinding out of
        the lock so the outer stage can park at a safe boundary.
        """
        probe = getattr(runner, "pause_requested", None)
        if probe is not None and probe(job["id"]):
            return True
        return _cancellation_requested()

    def _run_pause_participant(participant, work_fn, *, pre_registered=False):
        if not pre_registered:
            pause_gate.register(participant)
        pause_context.participant = participant
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
                bind_resource_owner(job["id"]),
                bind_resource_cancel_check(_pause_checkpoint),
                bind_resource_pure_cancel_check(_cancellation_requested),
            ):
                _pause_checkpoint()
                return work_fn()
        finally:
            pause_context.participant = None
            pause_gate.unregister(participant)

    errors = job["errors"]  # shared list, append is thread-safe
    if params.destination or params.local_processing or params.remote_target_id:
        raise RuntimeError(
            "Pipeline import/archive mode has been removed. Use the Import "
            "page or /api/jobs/import-photos to copy photos into the archive, "
            "then run Process on the imported workspace photos."
        )

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
    archive_destination_reserved = False
    if params.local_processing and final_destination:
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
        archive_destination_reserved = True

        # final_destination (not params.destination, which is None for a
        # remote archive): the staging root's basename is the leaf the
        # archive move lands at, and for remote that's the mount-path leaf —
        # the same last-subpath-segment as the NAS side.
        params.destination = staging_root(
            effective_vireo_dir, job["id"], final_destination,
        )

    try:
        # Snapshot-scoped pipelines: load the snapshot up front so scan targets
        # are derived from the captured file paths (not a folder the user picked
        # later). Raises if the snapshot has been garbage-collected — the API
        # layer is expected to return 404 before this job ever runs, but we fail
        # loud here to avoid silently running an unbounded scan.
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

        # Bridge user-initiated cancellation (runner.cancel_job) to the local
        # abort Event so all stages that already honor `abort` stop promptly.
        cancel_watcher_stop = threading.Event()

        def _cancel_watcher():
            while not cancel_watcher_stop.is_set():
                # This watcher must never park for Pause: it is not pipeline
                # work and would otherwise publish ``paused`` before the real
                # stage workers have reached safe checkpoints.
                if _cancellation_requested():
                    abort.set()
                    return
                if cancel_watcher_stop.wait(0.25):
                    return

        cancel_watcher = threading.Thread(target=_cancel_watcher, daemon=True)
        cancel_watcher.start()

        stages = {
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
        source_offline_state: dict = {"skipped_photo_ids": set()}

        # Normalize model_ids: prefer the explicit list, fall back to the legacy
        # single `model_id`, and finally to `[]` which means "use the active model
        # from config." This is the knob the multi-model fix hangs off of.
        if params.model_ids:
            effective_model_ids = list(params.model_ids)
        elif params.model_id:
            effective_model_ids = [params.model_id]
        else:
            effective_model_ids = []

        # Resolve model specs EARLY so per-model `classify:<id>` step_defs can
        # carry the model's display name as their label. Labels are immutable
        # after set_steps, so we cannot defer this to model_loader_stage.
        #
        # Resolution failures are captured (not raised) so the job still sets up
        # its step tree and the model_loader stage can surface a clean error.
        # For any id we fail to resolve we still emit a per-model step — labeled
        # with the id — so the user sees exactly which model broke.
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
                resolution_error = str(e)

        # Define step tracking for the jobs page
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
            # One row per model — label = model display name, id = classify:<mid>.
            # When resolution partially failed (e.g. 3 ids requested, 2nd not
            # downloaded), resolved_specs is a non-empty prefix of the requested
            # list. Emitting rows from resolved_specs alone would hide the later
            # failed ids — their "failed" update_step calls would then no-op
            # silently. Drive row creation off effective_model_ids whenever
            # resolution reported an error, so every requested model has a visible
            # step the model_loader stage can mark 'failed'.
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
        runner.set_steps(job["id"], step_defs)

        result = {"stages": {}}
        collection_id = params.collection_id
        scan_to_thumb = queue.Queue(maxsize=200)
        collected_photo_ids = []
        collection_ready = threading.Event()
        models_ready = threading.Event()
        loaded_models = {}  # populated by model_loader thread

        def _put_scan_item(item):
            """Put into the scan/thumbnail queue without defeating Pause.

            Both ordinary photo items and the end sentinel can otherwise block
            forever on a full queue after the thumbnail worker has parked.
            """
            while not _should_abort(abort):
                try:
                    scan_to_thumb.put(item, timeout=0.5)
                    return True
                except queue.Full:
                    continue
            return False
        # Resolved in collection_stage once the scanner has committed photo rows.
        # When set (i.e. snapshot-scoped runs), the collection is trimmed to this
        # set so every downstream stage (classify, extract_masks, eye_keypoints,
        # regroup) operates only on the files captured in the snapshot — files
        # that landed in the folder after the snapshot are scanned (we walk the
        # folder) but not further processed.
        snapshot_photo_ids: set[int] | None = None

        skip_scan = collection_id is not None

        def _filter_excluded(photos):
            """Remove photos excluded by user selection in preview."""
            if not params.exclude_photo_ids:
                return photos
            return [p for p in photos if p["id"] not in params.exclude_photo_ids]

        # Mark ingest as skipped when not in copy mode so SSE events
        # don't show a perpetually-pending stage.
        if not params.destination:
            stages["ingest"]["status"] = "skipped"
        if not params.local_processing:
            stages["storage"]["status"] = "skipped"
            stages["archive"]["status"] = "skipped"

        # --- Stage functions ---

        def scanner_stage():
            nonlocal collection_id

            # Note: stages["scan"]["status"] is NOT set to "running" here. It is
            # flipped to "running" just before each do_scan() call below, so
            # numScan doesn't pulse during the ingest sub-phase.
            # Collect the scan roots actually fed to do_scan so the finally clause
            # can invalidate the new-images cache for each one, matching the
            # try/finally pattern used by api_job_scan / api_job_import_full in
            # vireo/app.py. scanner.scan commits photo rows incrementally, so even
            # a mid-scan failure needs invalidation.
            scanned_roots: list[str] = []
            thread_db = None
            try:
                import config as cfg
                from scanner import ScanCancelled
                from scanner import scan as do_scan

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective_cfg = thread_db.get_effective_config(cfg.load())
                pipeline_cfg = effective_cfg.get("pipeline", {})

                def photo_cb(photo_id, path):
                    collected_photo_ids.append(photo_id)
                    # Abort/pause-aware: a blocking put would wedge the scanner
                    # if the thumbnail consumer parked or failed on a full queue.
                    _put_scan_item((photo_id, path))
                    stages["scan"]["count"] = len(collected_photo_ids)
                    runner.update_step(job["id"], "scan",
                                       current_file=os.path.basename(path))

                def status_cb(message, phase_current=None, phase_total=None, phase_label=None):
                    runner.update_step(job["id"], "scan", current_file=message)
                    extra = {"current_file": message}
                    if phase_current is not None or phase_total is not None:
                        extra.update({
                            "phase_current": phase_current,
                            "phase_total": phase_total,
                            "phase_label": phase_label,
                        })
                    _emit_progress(
                        runner, job["id"], stages, "scan",
                        phase_label or message, **extra,
                    )

                def cancel_check():
                    return _should_abort(abort) or _cancellation_requested()

                def scan_pause_check():
                    # Non-blocking pause probe for scanner.scan. When the
                    # pipeline is about to pause, this returns True *before*
                    # ``cancel_check`` parks — giving the scanner a chance to
                    # terminate its worker processes and release its CPU
                    # permits back to the ledger, so replacement scans,
                    # model loads, and CPU inference aren't starved for the
                    # duration of the pause.
                    probe = getattr(runner, "pause_requested", None)
                    return bool(probe and probe(job["id"]))

                # Accumulator so multi-folder scans (repair loop, scan-in-place
                # with sources=[...]) don't rewind the overall progress at each
                # folder boundary. scan() reports (current, total) local to the
                # invocation; we fold those into cumulative counters that the
                # weighted overall bar reads via stages["scan"].
                scan_acc = {"prior": 0, "last_total": 0}

                def progress_cb(current, total):
                    scan_acc["last_total"] = total
                    cum_current = scan_acc["prior"] + current
                    cum_total = scan_acc["prior"] + total
                    stages["scan"]["count"] = cum_current
                    stages["scan"]["total"] = cum_total
                    elapsed = time.time() - job["_start_time"]
                    rate = round(cum_current / max(elapsed, 0.01) * 60, 1)  # files/min
                    remaining = cum_total - cum_current
                    rate_per_sec = cum_current / max(elapsed, 0.01)
                    eta = round(remaining / rate_per_sec) if rate_per_sec > 0 and cum_current >= 10 else None
                    runner.update_step(job["id"], "scan",
                                       progress={"current": cum_current, "total": cum_total})
                    _emit_progress(
                        runner, job["id"], stages, "scan", "Scanning photos",
                        rate=rate,
                        eta_seconds=eta,
                    )

                def advance_scan_acc():
                    scan_acc["prior"] += scan_acc["last_total"]
                    scan_acc["last_total"] = 0

                # Collection mode: no scan targets, but check whether any
                # photos in the collection have broken metadata (NULL timestamp
                # or RAW thumbnail-sized dimensions) that would poison
                # downstream stages. If so, run a targeted repair scan on just
                # the affected folders. This is the self-healing path — when
                # nothing's broken, we keep the historical "Skipped" summary.
                if skip_scan:
                    coll_photos = thread_db.get_collection_photos(
                        collection_id, per_page=999999,
                    )
                    # Respect the user's preview-time exclusions: photos removed
                    # from this run must not be rescanned or have their metadata
                    # rewritten as a side effect of repair.
                    in_scope_photos = _filter_excluded(coll_photos)
                    broken = _find_broken_metadata_folders(
                        thread_db, [p["id"] for p in in_scope_photos],
                    )
                    if not broken:
                        stages["scan"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "scan", status="completed",
                            summary="Skipped (using collection)",
                        )
                        _update_stages(runner, job["id"], stages)
                        _put_scan_item(_SENTINEL)
                        return

                    total_broken = sum(len(paths) for _, paths in broken)
                    stages["scan"]["label"] = (
                        f"Repair metadata ({total_broken} photos)"
                    )
                    stages["scan"]["status"] = "running"
                    runner.update_step(
                        job["id"], "scan", status="running",
                        summary=(f"Repairing {total_broken} photos in "
                                 f"{len(broken)} folder"
                                 f"{'s' if len(broken) != 1 else ''}"),
                    )
                    _update_stages(runner, job["id"], stages)

                    # Display-only callback for the repair path: updates the
                    # scan step's current_file indicator but does NOT enqueue
                    # into scan_to_thumb. In collection mode thumbnail_stage
                    # already replays the full collection against the thumb
                    # cache, so enqueueing here would double-process every
                    # repaired photo and inflate the thumbnail totals.
                    def repair_photo_cb(photo_id, path):
                        runner.update_step(
                            job["id"], "scan",
                            current_file=os.path.basename(path),
                        )

                    unreachable = 0
                    for folder_path, file_paths in broken:
                        if not os.path.isdir(folder_path):
                            log.warning(
                                "Repair scan skipped for missing folder: %s",
                                folder_path,
                            )
                            unreachable += 1
                            continue
                        # Track the repair folder so the outer finally
                        # invalidates the Missing Originals cache for it —
                        # scanner.scan touches the folder on disk and can
                        # revalidate a restored original that a ready
                        # /api/photos/missing payload still lists as a
                        # ghost. Matches the append pattern used by the
                        # normal ingest/scan-in-place paths below.
                        scanned_roots.append(folder_path)
                        try:
                            # restrict_files limits discovery to the known
                            # broken photos in this folder. Without it, new
                            # untracked files in the same folder would get
                            # ingested as a side effect of the repair.
                            do_scan(
                                folder_path, thread_db,
                                progress_callback=progress_cb,
                                incremental=True,
                                extract_full_metadata=pipeline_cfg.get(
                                    "extract_full_metadata", True,
                                ),
                                photo_callback=repair_photo_cb,
                                status_callback=status_cb,
                                restrict_dirs=[folder_path],
                                restrict_files=set(file_paths),
                                vireo_dir=effective_vireo_dir,
                                thumb_cache_dir=effective_thumb_cache_dir,
                                cancel_check=cancel_check,
                                pause_check=scan_pause_check,
                                cancel_only_check=_cancellation_requested,
                                register_restrict_dirs_as_roots=False,
                                allow_photo_inserts=False,
                            )
                        except (OSError, RuntimeError) as e:
                            if isinstance(e, ScanCancelled) and (
                                _should_abort(abort) or _cancellation_requested()
                            ):
                                abort.set()
                                stages["scan"]["status"] = "skipped"
                                runner.update_step(
                                    job["id"], "scan",
                                    status="completed",
                                    summary="Cancelled",
                                )
                                break
                            log.warning(
                                "Repair scan failed for %s: %s", folder_path, e,
                            )
                            unreachable += 1
                        finally:
                            advance_scan_acc()

                    if _should_abort(abort) or _cancellation_requested():
                        stages["scan"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "scan",
                            status="completed",
                            summary="Cancelled",
                        )
                        _put_scan_item(_SENTINEL)
                        return

                    from metadata import scan_metadata_warning

                    summary = f"{total_broken} photos repaired"
                    if unreachable:
                        summary += (f", {unreachable} folder"
                                    f"{'s' if unreachable != 1 else ''} unreachable")
                    # Mirror the standalone scan/import paths in app.py: append
                    # the missing-exiftool warning so a repair scan that lost
                    # metadata reads as degraded, not as a clean success.
                    metadata_warning = scan_metadata_warning()
                    if metadata_warning:
                        summary += f" — {metadata_warning}"
                    stages["scan"]["status"] = "completed"
                    runner.update_step(
                        job["id"], "scan", status="completed", summary=summary,
                    )
                    _put_scan_item(_SENTINEL)
                    return

                # Determine source folder(s)
                sources = params.sources or ([params.source] if params.source else [])

                if params.destination:
                    from pathlib import Path

                    from import_dedup import CatalogIndex, DuplicateChecker
                    from ingest import ingest as do_ingest

                    # Duplicate-oracle infrastructure shared by the
                    # local-processing preflight and the ingest loop. The
                    # catalog index is loaded once; every prediction pass
                    # gets a FRESH checker over it (seen-state must not
                    # leak between predictions or into the real ingest),
                    # while the shared times cache keeps each source
                    # file's EXIF header read to once per run.
                    dedup_times_cache: dict = {}
                    catalog_index = None
                    if params.skip_duplicates:
                        catalog_index = CatalogIndex.from_db(thread_db)

                    def _fresh_checker():
                        if catalog_index is None:
                            return None
                        return DuplicateChecker(
                            catalog_index,
                            verify_by_hash=params.verify_by_hash,
                            times_cache=dedup_times_cache,
                        )

                    if params.local_processing:
                        from local_processing import (
                            archive_conflict_report,
                            existing_archive_bytes,
                            format_bytes,
                            non_duplicate_files,
                            selected_source_files,
                            storage_plan,
                            total_file_bytes,
                        )
                        stages["storage"]["status"] = "running"
                        runner.update_step(job["id"], "storage", status="running")
                        _update_stages(runner, job["id"], stages)

                        def _bail_storage(msg):
                            # collection_stage spins on stages["scan"]["status"]
                            # until it reaches a terminal value, so a storage
                            # failure that skips scan and ingest entirely must
                            # mark both as skipped here — otherwise its join()
                            # blocks the whole pipeline forever.
                            errors.append(f"[storage] Fatal: {msg}")
                            stages["storage"]["status"] = "failed"
                            runner.update_step(
                                job["id"], "storage",
                                status="failed", error=msg,
                            )
                            for skipped in ("ingest", "scan"):
                                stages[skipped]["status"] = "skipped"
                                runner.update_step(
                                    job["id"], skipped,
                                    status="completed", summary="Skipped",
                                )
                            abort.set()
                            _put_scan_item(_SENTINEL)

                        try:
                            if remote_archive is not None:
                                import move as move_mod

                                # Refuse BEFORE staging/processing hours of
                                # work, in the same spirit as the local
                                # archive-parent checks below: a missing GNU
                                # rsync or an unreachable target would
                                # otherwise only surface at the final archive
                                # move, stranding processed results in
                                # staging.
                                rsync_bin = move_mod.resolve_rsync_bin(
                                    effective_cfg.get("rsync_bin", "") or "",
                                )
                                if rsync_bin and not move_mod.is_gnu_rsync(
                                    rsync_bin,
                                ):
                                    rsync_bin = ""
                                if not rsync_bin:
                                    _bail_storage(
                                        "No usable GNU rsync was found for the "
                                        "remote archive. Install GNU rsync for "
                                        "your platform or set its executable "
                                        "under Settings → Paths."
                                    )
                                    return
                                conn = move_mod.test_remote_connection(
                                    remote_archive["target"], rsync_bin,
                                )
                                if not conn.get("ok"):
                                    _bail_storage(
                                        "Remote archive target "
                                        f"'{remote_archive['target']['name']}'"
                                        f" ({remote_archive['display']}) "
                                        "isn't usable: "
                                        f"{conn.get('message') or 'connection test failed'}"
                                    )
                                    return

                            # A tracked archive destination (the import lands at
                            # or inside a folder Vireo already manages) is no
                            # longer a hard failure: the archive move opts into
                            # merging (allow_tracked_merge=True) and folds the
                            # staged tree into the existing archive. The precise
                            # per-file content-conflict guard below
                            # (conflicting_archive_paths) still refuses any
                            # same-path file whose bytes differ.
                            #
                            # BUT — the merge only supports the "exact overlap"
                            # (destination IS a tracked folder) and the "ancestor
                            # overlap" (destination is INSIDE a tracked folder)
                            # cases. A tracked row STRICTLY BELOW the destination
                            # (e.g. /Photos/USA already tracked and the user
                            # picks /Photos) is the "wrap a fresh parent around
                            # an existing tracked subtree" case which
                            # move_folder refuses even with allow_tracked_merge.
                            # Without an early refuse here the pipeline would
                            # stage and process everything, then fail only at
                            # the archive step and leave processed results
                            # stranded under staging. Mirror move_folder's
                            # alias-folded check so a symlink or case-only alias
                            # of the tracked path is treated as the exact-match
                            # case, not a descendant.
                            from move import (
                                _path_equal_or_descends,
                                _tracked_destination_overlap,
                            )
                            # For a remote archive, final_destination is the
                            # catalog-facing MOUNT path (see where
                            # remote_archive is resolved) — the tracked check
                            # applies there too, because a prior remote
                            # archive to the same target leaves tracked rows
                            # at the mount path and the archive move merges
                            # into (or refuses around) those exactly like a
                            # local destination.
                            preflight_tracked = _tracked_destination_overlap(
                                thread_db, -1, final_destination,
                            )
                            if preflight_tracked and not _path_equal_or_descends(
                                final_destination, preflight_tracked["path"],
                            ):
                                _bail_storage(
                                    f"Archive destination {final_destination} "
                                    "sits above a folder Vireo already manages "
                                    f"({preflight_tracked['path']}). Merging "
                                    "around a tracked subfolder isn't "
                                    "supported. Pick the tracked folder itself "
                                    "or a location outside it."
                                )
                                return

                            if remote_archive is None:
                                # Make sure the archive parent exists NOW. Otherwise
                                # the pipeline would stage and process everything,
                                # then fail at the final move_folder call when rsync
                                # tries to write to a missing parent — leaving the
                                # staged copy stranded under ~/.vireo/staging with no
                                # archive at the final destination. Nested archive
                                # targets like /mnt/nas/NewShoot/Photos are the
                                # common case: the parent /mnt/nas/NewShoot may not
                                # have been created yet by the user.
                                #
                                # All four checks in this branch are
                                # local-filesystem-only. For a remote archive
                                # the destination lives on the NAS: the SSH
                                # connection test above already proved the
                                # remote base is a writable directory, and
                                # move_folder's remote path mkdir-p's the
                                # subpath parents itself. The local mount
                                # path deliberately isn't probed — it may
                                # legitimately be unmounted while archiving
                                # over SSH (that's the point of this mode).
                                archive_parent = os.path.dirname(
                                    os.path.normpath(final_destination),
                                )
                                # Use lexists so a broken/dangling symlink at
                                # final_destination is caught here too. os.path
                                # .exists returns False for a broken symlink, so
                                # a stale link left by an unmounted or moved
                                # archive root would slip through, let the
                                # pipeline stage and process everything, and
                                # only fail when move_folder/rsync tried to
                                # create a directory at a path already occupied
                                # by that symlink entry.
                                if (
                                    os.path.lexists(final_destination)
                                    and not os.path.isdir(final_destination)
                                ):
                                    _bail_storage(
                                        f"Archive destination {final_destination} "
                                        "already exists and is not a directory."
                                    )
                                    return
                                # Existing archive roots can be mounted volumes; new
                                # archive leaves have to probe the existing parent.
                                archive_space_path = (
                                    final_destination
                                    if os.path.exists(final_destination)
                                    else archive_parent
                                )
                                missing_mount_root = (
                                    _missing_archive_mount_root(final_destination)
                                    or _missing_archive_mount_root(archive_parent)
                                )
                                if missing_mount_root:
                                    _bail_storage(
                                        f"Archive mount root {missing_mount_root} "
                                        "is not available. Check that the "
                                        "destination drive is mounted and writable."
                                    )
                                    return
                                try:
                                    os.makedirs(archive_parent, exist_ok=True)
                                except OSError as exc:
                                    _bail_storage(
                                        f"Archive parent {archive_parent} could "
                                        f"not be created: {exc}. Check that the "
                                        "destination drive is mounted and writable."
                                    )
                                    return

                            os.makedirs(params.destination, exist_ok=True)
                            selected_files = selected_source_files(
                                sources,
                                params.file_types,
                                recursive=params.recursive,
                                exclude_paths=params.exclude_paths,
                            )
                            # When skip_duplicates is on, ingest() will skip
                            # sources that duplicate cataloged photos before
                            # they ever reach staging. Give the conflict
                            # preflight a fresh instance of the same
                            # duplicate oracle so a duplicate-source that
                            # happens to share an archive path with an
                            # unrelated file does not falsely abort the run
                            # — ingest will not copy it, so it cannot
                            # conflict at archive time.

                            from move import _case_insensitive_root

                            def _indexed_archive_paths(root: str) -> set[str]:
                                # Fold symlink/case aliases before deciding a
                                # cataloged row belongs under this destination:
                                # the tracked-destination preflight above
                                # accepts alias-equal roots via
                                # _path_equal_or_descends, so anything less here
                                # would drop indexed rows whose stored path uses
                                # a different alias than the user-picked
                                # destination (symlink target vs. link, or a
                                # case-only twin on case-insensitive POSIX like
                                # default APFS — os.path.normcase is a no-op on
                                # POSIX and os.path.realpath preserves the
                                # supplied spelling, so a lexical
                                # commonpath/is_relative_to check misses the
                                # case-only alias). Dropping the row would then
                                # feed an empty index to
                                # archive_conflict_report and get a
                                # zero-byte/truncated indexed archive file
                                # labelled as unindexed failed-copy debris —
                                # telling the user to remove a cataloged file.
                                # Probe the case-insensitive fold root once and
                                # reuse it per row so
                                # _path_equal_or_descends' listdir/samefile
                                # probe doesn't re-run per catalog folder.
                                root_path = Path(os.path.normpath(root))
                                root_real = os.path.normcase(
                                    os.path.realpath(root),
                                )
                                dest_ci_root = _case_insensitive_root(root)
                                indexed: set[str] = set()
                                rows = thread_db.conn.execute(
                                    """SELECT f.path, p.filename
                                         FROM photos p
                                         JOIN folders f ON f.id = p.folder_id"""
                                ).fetchall()
                                for row in rows:
                                    folder = row["path"]
                                    if not _path_equal_or_descends(
                                        folder, root,
                                        case_insensitive_root=dest_ci_root,
                                    ):
                                        continue
                                    # Extract the below-root portion of the
                                    # folder path so we can rebase onto the
                                    # user-picked root spelling
                                    # (archive_conflict_report joins
                                    # `path`/rel_folder/filename to build the
                                    # dest_key we're matching against). A
                                    # realpath-based string prefix covers the
                                    # same-case and symlink-alias cases; the
                                    # case-fold branch mirrors
                                    # _path_equal_or_descends' probed-CI-root
                                    # logic so a POSIX case-only alias still
                                    # yields the right tail.
                                    folder_real = os.path.normcase(
                                        os.path.realpath(folder),
                                    )
                                    rel_suffix: str | None = None
                                    if folder_real == root_real:
                                        rel_suffix = ""
                                    elif folder_real.startswith(
                                        root_real + os.sep,
                                    ):
                                        rel_suffix = folder_real[
                                            len(root_real) + 1:
                                        ]
                                    elif dest_ci_root:
                                        root_low = root_real.lower()
                                        folder_low = folder_real.lower()
                                        if folder_low == root_low:
                                            rel_suffix = ""
                                        elif folder_low.startswith(
                                            root_low + os.sep,
                                        ):
                                            rel_suffix = folder_real[
                                                len(root_real) + 1:
                                            ]
                                    if rel_suffix is None:
                                        # _path_equal_or_descends accepted the
                                        # row via a samefile walk-up (missing
                                        # intermediate leaf whose parent aliases
                                        # to root), so the realpath spelling
                                        # doesn't line up as a string prefix and
                                        # we can't safely rebase onto the
                                        # user-picked spelling. Catalog folders
                                        # exist on disk by construction — this
                                        # branch is rare — so drop the row
                                        # rather than fabricate a spelling.
                                        continue
                                    indexed_folder = (
                                        root_path if rel_suffix == ""
                                        else root_path.joinpath(
                                            *rel_suffix.split(os.sep),
                                        )
                                    )
                                    indexed.add(
                                        str(indexed_folder / row["filename"]),
                                    )
                                return indexed

                            if remote_archive is None:
                                archive_report = archive_conflict_report(
                                    final_destination,
                                    selected_files,
                                    params.folder_template,
                                    duplicate_checker=_fresh_checker(),
                                    indexed_paths=_indexed_archive_paths(
                                        final_destination,
                                    ),
                                )
                                archive_conflicts = (
                                    archive_report["empty"]
                                    + archive_report["partial"]
                                    + archive_report["conflicts"]
                                )
                            else:
                                # The conflict report walks the destination
                                # tree, which for a remote archive lives on
                                # the NAS and isn't locally walkable. The
                                # archive move itself runs the equivalent
                                # guard over SSH before any file is copied —
                                # move_folder's remote merge path probes with
                                # ``rsync -an --existing --checksum`` and
                                # refuses on any same-path file whose bytes
                                # differ — so a conflict still cancels
                                # cleanly, just at archive time instead of
                                # here.
                                archive_conflicts = []
                            if archive_conflicts:
                                incomplete = (
                                    archive_report["empty"]
                                    + archive_report["partial"]
                                )
                                if incomplete:
                                    # Only surface incomplete-file paths in
                                    # this branch: the message tells the user
                                    # to remove empty/partial debris, so
                                    # mixing full-content conflict paths into
                                    # the example list would point them at
                                    # files that are neither empty nor
                                    # truncated.
                                    incomplete_examples = ", ".join(
                                        incomplete[:3],
                                    )
                                    incomplete_more = (
                                        f" and {len(incomplete) - 3} more"
                                        if len(incomplete) > 3 else ""
                                    )
                                    bits = []
                                    if archive_report["empty"]:
                                        bits.append(
                                            f"{len(archive_report['empty'])} empty"
                                        )
                                    if archive_report["partial"]:
                                        bits.append(
                                            f"{len(archive_report['partial'])} "
                                            "partial"
                                        )
                                    _bail_storage(
                                        "Archive destination contains "
                                        f"{' and '.join(bits)} unindexed file"
                                        f"{'s' if len(incomplete) != 1 else ''} "
                                        "at incoming import paths: "
                                        f"{incomplete_examples}"
                                        f"{incomplete_more}. This looks like "
                                        "an interrupted previous archive "
                                        "copy. Remove or replace those "
                                        "incomplete files, then retry; Vireo "
                                        "will not suffix around likely "
                                        "corrupt archive files."
                                    )
                                    return
                                examples = ", ".join(archive_conflicts[:3])
                                more = (
                                    f" and {len(archive_conflicts) - 3} more"
                                    if len(archive_conflicts) > 3 else ""
                                )
                                _bail_storage(
                                    "Archive destination already contains "
                                    "different files at the same import paths: "
                                    f"{examples}{more}. Pick an empty archive "
                                    "folder, remove the conflicting files, or "
                                    "import without local processing."
                                )
                                return
                            planning_files = selected_files
                            if (
                                params.skip_duplicates
                                and selected_files
                                and catalog_index is not None
                            ):
                                # Plan against the exact files ingest will
                                # stage, not the full selection. This keeps
                                # both source_bytes and resume credit aligned
                                # with skip_duplicates even when the unfiltered
                                # plan appears to have enough space.
                                planning_files = non_duplicate_files(
                                    selected_files, _fresh_checker(),
                                )
                            source_bytes = total_file_bytes(planning_files)
                            remote_summary_bits = []
                            if remote_archive is None:
                                # When a previous archive attempt left a partial
                                # untracked directory at final_destination, the
                                # retry uses move_folder(..., merge=True), which
                                # rsyncs only the missing files. Credit the bytes
                                # already published so the preflight doesn't
                                # reject a retry whose remaining delta would fit.
                                existing_bytes = existing_archive_bytes(
                                    final_destination,
                                    planning_files,
                                    params.folder_template,
                                )
                                plan = storage_plan(
                                    params.destination, source_bytes,
                                    archive_parent=archive_space_path,
                                    archive_existing_bytes=existing_bytes,
                                )
                            else:
                                # Staging-only local plan (the archive volume
                                # is the NAS, never the same device), then a
                                # remote df probe for the archive side.
                                # Probe failures degrade to "check skipped" —
                                # logged and surfaced in the step summary and
                                # result payload, never faked as numbers; the
                                # archive move's own rsync failure is the
                                # backstop if space actually runs out.
                                from local_processing import (
                                    RESERVED_FREE_BYTES,
                                )
                                from move import _remote_free_bytes
                                plan = storage_plan(
                                    params.destination, source_bytes,
                                )
                                target = remote_archive["target"]
                                # No merge/resume credit for a remote archive:
                                # the local resume-credit path (existing_archive_bytes)
                                # compares each destination file's size+content
                                # against the source, but a remote equivalent
                                # would need a per-file walk over SSH. A
                                # whole-tree `du` reports every byte at the
                                # path — including unrelated files or stale
                                # partials that rsync --ignore-existing will
                                # still copy past — which could cancel out
                                # source_bytes and let the preflight pass on
                                # a nearly-full NAS. Budget the full source
                                # here; a retry whose remaining delta would
                                # actually fit but the full source wouldn't
                                # is a rare batch-reject we take over the
                                # false-positive that lets processing burn
                                # hours before the transfer fails on space.
                                archive_delta = source_bytes
                                plan["archive_existing_bytes"] = 0
                                plan["archive_required_bytes"] = archive_delta
                                # df the configured base (just verified as an
                                # existing writable dir by the connection
                                # test) rather than the not-yet-created leaf.
                                remote_free = _remote_free_bytes(
                                    target, target["remote_path"],
                                )
                                plan["archive_free_bytes"] = remote_free
                                if remote_free is None:
                                    log.warning(
                                        "Couldn't probe free space at %s; "
                                        "skipping remote free-space check",
                                        remote_archive["display"],
                                    )
                                    remote_summary_bits.append(
                                        "remote free-space check skipped "
                                        "(probe failed)"
                                    )
                                    plan["archive_usable_bytes"] = None
                                else:
                                    archive_usable = max(
                                        0, remote_free - RESERVED_FREE_BYTES,
                                    )
                                    plan["archive_usable_bytes"] = archive_usable
                                    plan["archive_enough"] = (
                                        archive_delta <= archive_usable
                                    )
                                    plan["enough"] = (
                                        plan["staging_enough"]
                                        and plan["archive_enough"]
                                    )
                                    plan["batching_required"] = not plan["enough"]
                                    remote_summary_bits.append(
                                        f"{format_bytes(remote_free)} free at "
                                        f"{target['name']}"
                                    )
                            result["local_processing"] = {
                                **plan,
                                "staging_destination": params.destination,
                                "final_destination": final_destination,
                            }
                            if remote_archive is not None:
                                target = remote_archive["target"]
                                result["local_processing"]["remote"] = {
                                    "target_id": target["id"],
                                    "target_name": target["name"],
                                    "host": target["host"],
                                    "user": target["user"],
                                    "ssh_destination": remote_archive["ssh_final"],
                                    "free_space_checked": (
                                        plan["archive_free_bytes"] is not None
                                    ),
                                }
                            if plan["batching_required"]:
                                # Tell the user which volume came up short — the
                                # destination running out of room reads as a
                                # different problem (pick a bigger archive
                                # drive) than the staging volume running out
                                # (free space on ~/.vireo or batch later).
                                if not plan.get("archive_enough", True):
                                    if remote_archive is not None:
                                        _bail_storage(
                                            "Remote archive needs about "
                                            f"{format_bytes(plan['archive_required_bytes'])}, "
                                            "but only "
                                            f"{format_bytes(plan['archive_usable_bytes'] or 0)} "
                                            "is free at "
                                            f"{remote_archive['display']} after "
                                            "the free-space reserve. Free space "
                                            "on the remote volume or pick a "
                                            "different target or subpath."
                                        )
                                    else:
                                        _bail_storage(
                                            "Archive destination needs about "
                                            f"{format_bytes(plan['archive_required_bytes'])}, "
                                            "but only "
                                            f"{format_bytes(plan['archive_usable_bytes'] or 0)} "
                                            f"is free under {archive_parent} after "
                                            "the free-space reserve. Free space at "
                                            "the destination or pick a different "
                                            "archive folder."
                                        )
                                else:
                                    _bail_storage(
                                        "Local processing needs about "
                                        f"{format_bytes(plan['required_bytes'])}, but "
                                        f"only {format_bytes(plan['usable_bytes'])} is "
                                        "available after keeping local free-space "
                                        "reserve. This import needs "
                                        f"{plan['batch_count']} local-processing "
                                        "batches; automatic batch execution is not "
                                        "available in this build yet."
                                    )
                                return
                            summary = (
                                f"{format_bytes(plan['required_bytes'])} needed, "
                                f"{format_bytes(plan['usable_bytes'])} available"
                            )
                            if remote_summary_bits:
                                summary += "; " + "; ".join(remote_summary_bits)
                            stages["storage"]["status"] = "completed"
                            runner.update_step(
                                job["id"], "storage",
                                status="completed",
                                summary=summary,
                            )
                        except Exception as e:
                            log.exception("Pipeline local-storage preflight failed")
                            _bail_storage(str(e))
                            return

                    # Same accumulator pattern as scan_acc: do_ingest() is called
                    # once per source folder, with (current, total) local to each
                    # call. Without accumulation, overall progress rewinds at each
                    # source boundary.
                    ingest_acc = {"prior": 0, "last_total": 0}

                    def ingest_cb(current, total, filename):
                        ingest_acc["last_total"] = total
                        cum_current = ingest_acc["prior"] + current
                        cum_total = ingest_acc["prior"] + total
                        stages["ingest"]["count"] = cum_current
                        stages["ingest"]["total"] = cum_total
                        runner.update_step(job["id"], "ingest",
                                           current_file=filename,
                                           progress={"current": cum_current, "total": cum_total})
                        _emit_progress(
                            runner, job["id"], stages, "ingest", "Importing photos",
                            current_file=filename,
                        )

                    def advance_ingest_acc():
                        ingest_acc["prior"] += ingest_acc["last_total"]
                        ingest_acc["last_total"] = 0

                if params.destination:
                    # Copy mode: ingest all sources first, then scan destination
                    # subfolders that received files.
                    stages["ingest"]["status"] = "running"
                    runner.update_step(job["id"], "ingest", status="running")
                    _update_stages(runner, job["id"], stages)

                    # One shared checker across the whole source loop: files
                    # copied by earlier iterations are recorded in it, so
                    # later sources treat them as duplicates even before the
                    # DB scan (this replaces the old accumulated-hashes
                    # re-read of every copied file between sources).
                    ingest_checker = _fresh_checker()
                    all_copied_paths: list = []
                    all_duplicate_folders: set = set()
                    total_copied = 0
                    total_skipped = 0
                    total_failed = 0
                    for src_folder in sources:
                        try:
                            result_info = do_ingest(
                                source_dir=src_folder,
                                destination_dir=params.destination,
                                db=thread_db,
                                file_types=params.file_types,
                                folder_template=params.folder_template,
                                skip_duplicates=params.skip_duplicates,
                                progress_callback=ingest_cb,
                                duplicate_checker=ingest_checker,
                                skip_paths=params.exclude_paths,
                                recursive=params.recursive,
                            )
                        finally:
                            advance_ingest_acc()
                        all_copied_paths.extend(result_info.get("copied_paths", []))
                        all_duplicate_folders.update(result_info.get("duplicate_folders", []))
                        total_copied += result_info.get("copied", 0)
                        total_skipped += result_info.get("skipped_duplicate", 0)
                        total_failed += result_info.get("failed", 0)

                    # In local-processing mode, ingest failures must fail the
                    # ingest stage so archive_stage's "any earlier stage failed"
                    # gate skips publishing. ingest() catches per-file copy
                    # errors (unreadable source, disk full mid-card) and returns
                    # a non-zero ``failed`` count without raising. Without
                    # propagating that here the archive step would happily move
                    # the partial staging tree to the user's final destination
                    # — publishing a partial result that the rest of the
                    # pipeline would otherwise treat as a successful import.
                    parts = []
                    if total_copied:
                        parts.append(f"{total_copied} copied")
                    if total_skipped:
                        parts.append(f"{total_skipped} skipped")
                    if total_failed:
                        parts.append(f"{total_failed} failed")
                    summary = ", ".join(parts) or "0 files"
                    if params.local_processing and total_failed:
                        msg = (
                            f"{total_failed} file"
                            f"{'s' if total_failed != 1 else ''} failed to copy "
                            f"during ingest; archive skipped to avoid publishing "
                            f"a partial result"
                        )
                        errors.append(f"[ingest] Fatal: {msg}")
                        stages["ingest"]["status"] = "failed"
                        runner.update_step(
                            job["id"], "ingest",
                            status="failed",
                            error=msg,
                            summary=summary,
                        )
                        # Stop the run here. Without abort, scanner/previews/
                        # classify/regroup would all execute against the
                        # partial subset ingest did manage to copy — regroup
                        # in particular overwrites the workspace pipeline
                        # results with photo IDs that archive_stage's
                        # deindex_staging is then going to delete, leaving
                        # the workspace pointing at rows that no longer
                        # exist. archive_stage already gates on abort and
                        # any earlier-stage failure, so this also publishes
                        # nothing. Finalize the remaining step rows as
                        # skipped so the SSE clients don't see perpetually
                        # pending stages.
                        abort.set()
                        stages["scan"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "scan",
                            status="completed",
                            summary="Skipped (ingest failed)",
                        )
                        _update_stages(runner, job["id"], stages)
                        # The finally clause at the bottom of scanner_stage
                        # puts the sentinel on scan_to_thumb so the
                        # thumbnail consumer drains and exits.
                        return
                    else:
                        stages["ingest"]["status"] = "completed"
                        # Ingest is the only stage that ever reads the source
                        # (SD card/etc.) — everything after this point works
                        # from the copy. Record counts so the UI can tell the
                        # user the card is safe to eject instead of leaving
                        # them to guess. Only claim this when every discovered
                        # file actually made it off the card: local_processing
                        # aborts above on any failure, but plain copy mode
                        # (local_processing=False) reaches this branch even
                        # with total_failed > 0, and the card still holds
                        # files that never got copied.
                        if total_failed == 0:
                            stages["ingest"]["copied"] = total_copied
                            stages["ingest"]["skipped_duplicate"] = total_skipped
                            result["stages"]["ingest"] = {
                                "copied": total_copied,
                                "skipped_duplicate": total_skipped,
                            }
                        runner.update_step(
                            job["id"], "ingest", status="completed",
                            summary=summary,
                        )
                    _update_stages(runner, job["id"], stages)

                    # Scan only the destination subfolders that actually contain
                    # files we care about, not the entire destination tree. Use
                    # restrict_dirs so the scanner still roots the folder hierarchy
                    # at the destination, preserving parent folder links. Include
                    # folders that received copies AND folders that already hold
                    # duplicates of the source files — both need to be linked to
                    # the active workspace. Guard every candidate at this seam:
                    # scanner._ensure_folder recurses parents until it equals the
                    # scan root; a non-descendant path would recurse all the way
                    # to '/', so restrict_dirs must contain only descendants of
                    # params.destination. ingest() already enforces this, but
                    # we re-check here to keep the invariant local and obvious.
                    # Both sides are lexically normalized via os.path.normpath so
                    # a stored path containing ``..`` can't defeat the check.
                    import os as _os
                    dest_p = Path(_os.path.normpath(params.destination))

                    def _under_destination(path: str) -> bool:
                        return Path(_os.path.normpath(path)).is_relative_to(dest_p)

                    restrict_set: set[str] = set()
                    if all_copied_paths:
                        restrict_set.update(
                            str(Path(p).parent) for p in all_copied_paths
                            if _under_destination(str(Path(p).parent))
                        )
                    restrict_set.update(
                        f for f in all_duplicate_folders if _under_destination(f)
                    )
                    restrict = sorted(restrict_set) if restrict_set else None
                    # Flip scan to running and reset job progress so status
                    # events during enumeration don't carry ingest's numbers.
                    stages["scan"]["status"] = "running"
                    runner.update_step(job["id"], "scan", status="running")
                    job["progress"]["current"] = 0
                    job["progress"]["total"] = 0
                    _update_stages(runner, job["id"], stages)
                    # Surface kernel-level enumeration denials (macOS TCC EPERM,
                    # POSIX EACCES) into job["errors"]. Without this, scanner.scan
                    # silently skipped the subtree and the scan stage finished as
                    # "completed" with 0 photos — a black-box outcome the user
                    # reads as "no photos found here" when the truth is "Vireo
                    # was denied access". Dedup per path because the walk may
                    # signal the same dir more than once.
                    denied_seen: set[str] = set()
                    def _on_denied(path: str) -> None:
                        if path in denied_seen:
                            return
                        denied_seen.add(path)
                        errors.append(
                            f"[scan] PERMISSION_DENIED: {path} — macOS or "
                            f"filesystem refused enumeration. On macOS open "
                            f"System Settings → Privacy & Security → Files "
                            f"and Folders (or Removable/Network Volumes) and "
                            f"grant Vireo access."
                        )
                    scanned_roots.append(params.destination)
                    do_scan(
                        params.destination, thread_db,
                        progress_callback=progress_cb,
                        incremental=True,
                        extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                        photo_callback=photo_cb,
                        status_callback=status_cb,
                        restrict_dirs=restrict,
                        vireo_dir=effective_vireo_dir,
                        thumb_cache_dir=effective_thumb_cache_dir,
                        permission_error_callback=_on_denied,
                        cancel_check=cancel_check,
                        pause_check=scan_pause_check,
                        cancel_only_check=_cancellation_requested,
                    )
                else:
                    # Scan-in-place: scan each source folder independently.
                    stages["scan"]["status"] = "running"
                    runner.update_step(job["id"], "scan", status="running")
                    job["progress"]["current"] = 0
                    job["progress"]["total"] = 0
                    _update_stages(runner, job["id"], stages)
                    # See ingest branch above — same denial-surfacing rationale.
                    denied_seen: set[str] = set()
                    def _on_denied(path: str) -> None:
                        if path in denied_seen:
                            return
                        denied_seen.add(path)
                        errors.append(
                            f"[scan] PERMISSION_DENIED: {path} — macOS or "
                            f"filesystem refused enumeration. On macOS open "
                            f"System Settings → Privacy & Security → Files "
                            f"and Folders (or Removable/Network Volumes) and "
                            f"grant Vireo access."
                        )
                    # Snapshot-scoped: hand the scanner the exact file set
                    # captured at snapshot time so a file that landed in the
                    # folder AFTER the snapshot doesn't get cataloged here.
                    # Without this, the scan walks the whole folder, commits
                    # a photos row for the late arrival, then the collection
                    # stage filters it out of downstream work AND the finally
                    # block invalidates the new-images cache — orphaning the
                    # file (cataloged in DB, never classified, never
                    # re-surfaced by a later banner probe). Skipping it at
                    # scan time keeps it uncataloged so the next probe
                    # rediscovers it.
                    snapshot_files_set = (
                        set(snapshot_paths) if snapshot_paths is not None else None
                    )
                    for src_folder in sources:
                        scanned_roots.append(src_folder)
                        snapshot_restrict_dirs = None
                        snapshot_restrict_files = None
                        if snapshot_files_set is not None:
                            src_norm = os.path.normpath(src_folder)
                            prefix = (
                                src_norm if src_norm.endswith(os.sep)
                                else src_norm + os.sep
                            )
                            files_under_src = [
                                p for p in snapshot_paths
                                if os.path.normpath(p).startswith(prefix)
                            ]
                            snapshot_restrict_files = set(files_under_src)
                            snapshot_restrict_dirs = sorted(
                                {os.path.dirname(p) for p in files_under_src}
                            )
                        try:
                            do_scan(
                                src_folder, thread_db,
                                progress_callback=progress_cb,
                                incremental=True,
                                extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                                photo_callback=photo_cb,
                                skip_paths=params.exclude_paths,
                                status_callback=status_cb,
                                recursive=params.recursive,
                                restrict_dirs=snapshot_restrict_dirs,
                                restrict_files=snapshot_restrict_files,
                                vireo_dir=effective_vireo_dir,
                                thumb_cache_dir=effective_thumb_cache_dir,
                                permission_error_callback=_on_denied,
                                cancel_check=cancel_check,
                                pause_check=scan_pause_check,
                                cancel_only_check=_cancellation_requested,
                            )
                        finally:
                            advance_scan_acc()
                if _should_abort(abort) or _cancellation_requested():
                    stages["scan"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "scan", status="completed", summary="Cancelled",
                    )
                else:
                    from metadata import scan_metadata_warning

                    stages["scan"]["status"] = "completed"
                    # Pipeline scans use scanner.scan exactly like the standalone
                    # /api/jobs/scan path, so a missing exiftool silently strips
                    # capture dates, GPS, and camera info here too. Append the
                    # same warning the standalone path appends.
                    scan_summary = f"{stages['scan']['count']} photos"
                    metadata_warning = scan_metadata_warning()
                    if metadata_warning:
                        scan_summary += f" — {metadata_warning}"
                    runner.update_step(job["id"], "scan", status="completed",
                                       summary=scan_summary)
            except Exception as e:
                if isinstance(e, ScanCancelled) and (
                    _should_abort(abort) or _cancellation_requested()
                ):
                    abort.set()
                    stages["scan"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "scan", status="completed", summary="Cancelled",
                    )
                else:
                    errors.append(f"[scan] Fatal: {e}")
                    log.exception("Pipeline scan stage failed")
                    abort.set()
                    stages["scan"]["status"] = "failed"
                    runner.update_step(job["id"], "scan", status="failed", error=str(e))
            finally:
                # Invalidate the new-images cache for every root fed to do_scan,
                # on both success and exception paths. scanner.scan commits photo
                # rows incrementally, so even a mid-scan failure can leave DB
                # state that invalidates cached new-image counts. Mirrors the
                # try/finally in api_job_scan and api_job_import_full.
                if thread_db is not None and scanned_roots:
                    from new_images import invalidate_new_images_after_scan
                    for scanned_root in scanned_roots:
                        try:
                            invalidate_new_images_after_scan(thread_db, scanned_root)
                        except Exception:
                            log.exception(
                                "Failed to invalidate new-images cache for %s",
                                scanned_root,
                            )
                        # scanner.scan touches disk and may add or remove
                        # photo rows; a ready Missing Originals payload
                        # computed before the pipeline scan can now be
                        # stale (e.g. user restored an original before
                        # running Process). Standalone scan / import jobs
                        # already invalidate here — mirror that for
                        # pipeline scans so GET /api/photos/missing does
                        # not keep serving the pre-scan photo list.
                        if missing_originals_invalidator is not None:
                            try:
                                missing_originals_invalidator()
                            except Exception:
                                log.exception(
                                    "Failed to invalidate missing-originals "
                                    "cache for %s",
                                    scanned_root,
                                )
                _put_scan_item(_SENTINEL)
                _update_stages(runner, job["id"], stages)

        def collection_stage():
            """Wait for scan to finish, build collection, signal classifier."""
            nonlocal collection_id, snapshot_photo_ids

            if skip_scan:
                collection_ready.set()
                return

            # Wait for scanner to complete (don't check abort -- we want the
            # collection regardless so the user can see scanned photos)
            while True:
                # Pause is different from abort: this worker can park while it
                # waits without giving up the collection the scanner produced.
                _pause_checkpoint()
                if stages["scan"]["status"] in ("completed", "failed", "skipped"):
                    break
                time.sleep(0.1)

            # Snapshot-scoped runs: resolve the captured file paths to photo IDs
            # now that the scanner has committed rows, and trim the collection
            # to exactly that set. The scan stage already restricted the walk
            # to the snapshot's file set via ``restrict_dirs`` + ``restrict_files``
            # so late arrivals aren't cataloged in the first place; this filter
            # is a belt-and-suspenders trim in case a pre-existing (already
            # cataloged) photo somehow ends up in ``collected_photo_ids``. Any
            # snapshot path that never resolved (file was moved/deleted between
            # snapshot and pipeline run) is logged so an unexpectedly small
            # collection is auditable.
            if snapshot_paths is not None:
                resolver_db = Database(db_path)
                resolver_db.set_active_workspace(workspace_id)
                # Split each snapshot path into (dirname, basename) and match on
                # the two columns directly. Concatenating with a hardcoded '/'
                # would mismatch Windows paths captured via os.path.join, where
                # both the snapshot and folders.path use backslash separators.
                pairs = [os.path.split(p) for p in snapshot_paths]
                resolved: set[int] = set()
                # 2 placeholders per pair; cap below SQLite's default 999-param
                # limit (pre-3.32) with headroom.
                _CHUNK = 400
                for i in range(0, len(pairs), _CHUNK):
                    chunk = pairs[i : i + _CHUNK]
                    values = ",".join("(?, ?)" for _ in chunk)
                    flat_params = tuple(v for pair in chunk for v in pair)
                    rows = resolver_db.conn.execute(
                        f"""SELECT p.id
                              FROM photos p
                              JOIN folders f ON f.id = p.folder_id
                             WHERE (f.path, p.filename) IN (VALUES {values})""",
                        flat_params,
                    ).fetchall()
                    resolved.update(r["id"] for r in rows)
                snapshot_photo_ids = resolved

                missing = len(snapshot_paths) - len(snapshot_photo_ids)
                log.info(
                    "pipeline: snapshot %s had %d files, %d ingested, %d missing on disk",
                    params.source_snapshot_id,
                    len(snapshot_paths),
                    len(snapshot_photo_ids),
                    missing,
                )

                # Filter collected_photo_ids to the snapshot set. collected_photo_ids
                # is only read by this stage (to build the collection); the thumbnail
                # queue has already drained it independently.
                collected_photo_ids[:] = [
                    pid for pid in collected_photo_ids if pid in snapshot_photo_ids
                ]

            if not collected_photo_ids:
                collection_ready.set()
                return

            try:
                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)
                from datetime import datetime as dt

                name = "Pipeline " + dt.now().strftime("%Y-%m-%d %H:%M")
                collection_id = thread_db.add_collection(
                    name,
                    json.dumps([{"field": "photo_ids", "value": collected_photo_ids}]),
                )
                result["collection_id"] = collection_id
            except Exception as e:
                errors.append(f"[collection] Fatal: {e}")
                log.exception("Pipeline collection stage failed")
                abort.set()
            finally:
                collection_ready.set()

        def thumbnail_stage():
            stages["thumbnails"]["status"] = "running"
            runner.update_step(job["id"], "thumbnails", status="running")
            _update_stages(runner, job["id"], stages)
            try:
                from thumbnails import (
                    _is_working_copy_source,
                    _retry_thumbnail_after_working_copy_eviction,
                    generate_thumbnail,
                )

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                import config as cfg
                effective_cfg = thread_db.get_effective_config(cfg.load())
                thumb_size = effective_cfg.get("display", {}).get("thumbnail_size", 300)

                # Write thumbnails to the configured cache dir so custom
                # --thumb-dir layouts receive the files the Flask serve
                # route (reading from app.config["THUMB_CACHE_DIR"]) will
                # look for. Falls back to <db_dir>/thumbnails only when the
                # caller passed no explicit override.
                cache_dir = effective_thumb_cache_dir
                os.makedirs(cache_dir, exist_ok=True)

                generated = 0
                skipped = 0
                failed = 0
                failed_photos = []
                failure_detail_limit = 100

                def _record_failure(photo_id, photo_path, reason):
                    if len(failed_photos) >= failure_detail_limit:
                        return
                    failed_photos.append({
                        "id": photo_id,
                        "filename": os.path.basename(photo_path or ""),
                        "reason": reason,
                    })

                # Mark photos.thumb_path so the dashboard's coverage query
                # (`thumb_path IS NOT NULL`) reflects each freshly-generated or
                # already-cached thumbnail. Batched so the writer lock isn't held
                # per-row under sustained scan throughput.
                THUMB_PATH_BATCH = 25
                pending_thumb_paths = []

                def _flush_thumb_paths():
                    if pending_thumb_paths:
                        thread_db.conn.executemany(
                            "UPDATE photos SET thumb_path=? WHERE id=?",
                            pending_thumb_paths,
                        )
                        commit_with_retry(thread_db.conn)
                        pending_thumb_paths.clear()

                while True:
                    # Continue draining after cancellation, but park before
                    # taking another item when the whole pipeline is paused.
                    _pause_checkpoint()
                    try:
                        item = scan_to_thumb.get(timeout=1.0)
                    except queue.Empty:
                        # Keep draining even if abort is set -- we want thumbnails
                        # for any photos already scanned. Only stop on sentinel.
                        if _should_abort(abort) and scan_to_thumb.empty():
                            break
                        continue
                    if item is _SENTINEL:
                        break
                    photo_id, photo_path = item
                    try:
                        thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
                        already_exists = os.path.exists(thumb_path)
                        recipe = thread_db.get_photo_edit_recipe(photo_id)
                        detail_photo = None
                        if recipe:
                            detail_photo = thread_db.get_photo(photo_id)
                            if detail_photo:
                                folder_row = thread_db.get_folder(detail_photo["folder_id"])
                                folders = (
                                    {folder_row["id"]: folder_row["path"]}
                                    if folder_row else {}
                                )
                                photo_path = _recipe_render_source(
                                    detail_photo,
                                    recipe,
                                    thumb_size,
                                    effective_vireo_dir,
                                    folders,
                                )
                                if (
                                    os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                                    and _has_current_working_copy_failure(
                                        detail_photo,
                                        effective_vireo_dir,
                                        trust_existing_working_copy=False,
                                        live_source_path=photo_path,
                                        folder_path=folders.get(detail_photo["folder_id"]),
                                    )
                                ):
                                    failed += 1
                                    _record_failure(
                                        photo_id, photo_path,
                                        "RAW decode previously failed and no "
                                        "acceptable fallback is available",
                                    )
                                    stages["thumbnails"]["count"] = (
                                        generated + skipped + failed
                                    )
                                    continue
                        recipe_kwargs = {"recipe": recipe} if recipe else {}
                        if recipe:
                            recipe_kwargs["native_size"] = (
                                _recipe_source_dimensions(detail_photo)
                            )
                        raw_decode_kwargs = _thumb_raw_decode_kwargs(
                            detail_photo, recipe,
                        )
                        min_size_kwargs = _thumb_min_source_size_kwargs(
                            detail_photo, recipe, thumb_size, photo_path,
                        )
                        result_path = generate_thumbnail(
                            photo_id,
                            photo_path,
                            cache_dir,
                            size=thumb_size,
                            **recipe_kwargs,
                            **raw_decode_kwargs,
                            **min_size_kwargs,
                        )
                        if (
                            result_path is None
                            and detail_photo is not None
                            and _is_working_copy_source(
                                detail_photo,
                                photo_path,
                                effective_vireo_dir,
                            )
                        ):
                            result_path, photo_path = (
                                _retry_thumbnail_after_working_copy_eviction(
                                    detail_photo,
                                    photo_path,
                                    cache_dir,
                                    thumb_size,
                                    85,
                                    recipe,
                                    folders.get(detail_photo["folder_id"]),
                                    effective_vireo_dir,
                                )
                            )
                        if (
                            result_path is None
                            and detail_photo is not None
                            and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                        ):
                            result_path = _retry_thumbnail_with_companion(
                                thread_db, generate_thumbnail, detail_photo,
                                photo_id, photo_path, cache_dir, thumb_size,
                                recipe, folders.get(detail_photo["folder_id"]),
                            )
                        if (
                            result_path is None
                            and detail_photo is not None
                            and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                        ):
                            result_path = _retry_thumbnail_with_working_copy(
                                thread_db, generate_thumbnail, detail_photo,
                                photo_id, photo_path, cache_dir, thumb_size,
                                recipe, effective_vireo_dir,
                            )
                        if result_path is None:
                            failed += 1
                            _record_failure(
                                photo_id, photo_path,
                                "No acceptable thumbnail render source",
                            )
                        elif already_exists:
                            skipped += 1
                            pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                        else:
                            generated += 1
                            pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                        if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                            _flush_thumb_paths()
                    except Exception as exc:
                        failed += 1
                        _record_failure(photo_id, photo_path, str(exc))
                        log.debug(
                            "Thumbnail failed for photo %s", photo_id,
                            exc_info=True,
                        )
                    # Include failed in the progress counter so the dashboard
                    # reflects all work attempted, not just successes. Mixed
                    # success/failure must not hide behind a 0/N progress bar.
                    stages["thumbnails"]["count"] = generated + skipped + failed
                    processed = generated + skipped + failed
                    # Use scan count directly regardless of whether scan has
                    # completed yet — this avoids the total staying at 0/? when
                    # the thumbnail worker catches up with scan before scan's
                    # status flips to "completed".
                    scan_total = stages["scan"].get("count", 0)
                    stages["thumbnails"]["total"] = scan_total
                    runner.update_step(job["id"], "thumbnails",
                                       current_file=os.path.basename(photo_path),
                                       progress={"current": processed, "total": scan_total})
                    elapsed = time.time() - job["_start_time"]
                    rate = round(processed / max(elapsed, 0.01) * 60, 1)
                    _emit_progress(
                        runner, job["id"], stages, "thumbnails", "Generating thumbnails",
                        current_file=os.path.basename(photo_path),
                        rate=rate,
                    )

                # Collection mode: the scanner is skipped so the queue above was
                # empty. Iterate the collection's photos directly — mirrors the
                # pattern used by previews_stage — so replays against an existing
                # collection still regenerate any missing thumbs.
                if skip_scan and collection_id:
                    coll_photos = _filter_excluded(
                        thread_db.get_collection_photos(collection_id, per_page=999999)
                    )
                    folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
                    total = len(coll_photos)
                    for photo in coll_photos:
                        if _should_abort(abort):
                            break
                        photo_id = photo["id"]
                        folder_path = folders.get(photo["folder_id"], "")
                        photo_path = os.path.join(folder_path, photo["filename"])
                        thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
                        already_exists = os.path.exists(thumb_path)
                        try:
                            recipe = thread_db.get_photo_edit_recipe(photo_id)
                            detail_photo = None
                            if recipe:
                                detail_photo = thread_db.get_photo(photo_id) or photo
                                photo_path = _recipe_render_source(
                                    detail_photo,
                                    recipe,
                                    thumb_size,
                                    effective_vireo_dir,
                                    folders,
                                )
                                if (
                                    os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                                    and _has_current_working_copy_failure(
                                        detail_photo,
                                        effective_vireo_dir,
                                        trust_existing_working_copy=False,
                                        live_source_path=photo_path,
                                        folder_path=folders.get(detail_photo["folder_id"]),
                                    )
                                ):
                                    failed += 1
                                    _record_failure(
                                        photo_id, photo_path,
                                        "RAW decode previously failed and no "
                                        "acceptable fallback is available",
                                    )
                                    stages["thumbnails"]["count"] = (
                                        generated + skipped + failed
                                    )
                                    continue
                            recipe_kwargs = {"recipe": recipe} if recipe else {}
                            if recipe:
                                recipe_kwargs["native_size"] = (
                                    _recipe_source_dimensions(detail_photo)
                                )
                            raw_decode_kwargs = _thumb_raw_decode_kwargs(
                                detail_photo, recipe,
                            )
                            min_size_kwargs = _thumb_min_source_size_kwargs(
                                detail_photo, recipe, thumb_size, photo_path,
                            )
                            result_path = generate_thumbnail(
                                photo_id,
                                photo_path,
                                cache_dir,
                                size=thumb_size,
                                **recipe_kwargs,
                                **raw_decode_kwargs,
                                **min_size_kwargs,
                            )
                            if (
                                result_path is None
                                and detail_photo is not None
                                and _is_working_copy_source(
                                    detail_photo,
                                    photo_path,
                                    effective_vireo_dir,
                                )
                            ):
                                result_path, photo_path = (
                                    _retry_thumbnail_after_working_copy_eviction(
                                        detail_photo,
                                        photo_path,
                                        cache_dir,
                                        thumb_size,
                                        85,
                                        recipe,
                                        folder_path,
                                        effective_vireo_dir,
                                    )
                                )
                            if (
                                result_path is None
                                and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                            ):
                                fallback_photo = detail_photo or photo
                                result_path = _retry_thumbnail_with_companion(
                                    thread_db, generate_thumbnail, fallback_photo,
                                    photo_id, photo_path, cache_dir, thumb_size,
                                    recipe, folder_path,
                                )
                            if (
                                result_path is None
                                and detail_photo is not None
                                and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                            ):
                                result_path = _retry_thumbnail_with_working_copy(
                                    thread_db, generate_thumbnail, detail_photo,
                                    photo_id, photo_path, cache_dir, thumb_size,
                                    recipe, effective_vireo_dir,
                                )
                            if result_path is None:
                                failed += 1
                                _record_failure(
                                    photo_id, photo_path,
                                    "No acceptable thumbnail render source",
                                )
                            elif already_exists:
                                skipped += 1
                                pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                            else:
                                generated += 1
                                pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                            if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                                _flush_thumb_paths()
                        except Exception as exc:
                            failed += 1
                            _record_failure(photo_id, photo_path, str(exc))
                            log.debug(
                                "Thumbnail failed for photo %s", photo_id,
                                exc_info=True,
                            )
                        stages["thumbnails"]["count"] = generated + skipped + failed
                        stages["thumbnails"]["total"] = total
                        processed = generated + skipped + failed
                        runner.update_step(
                            job["id"], "thumbnails",
                            current_file=os.path.basename(photo_path),
                            progress={"current": processed, "total": total},
                        )
                        elapsed = time.time() - job["_start_time"]
                        rate = round(processed / max(elapsed, 0.01) * 60, 1)
                        _emit_progress(
                            runner, job["id"], stages, "thumbnails", "Generating thumbnails",
                            current_file=os.path.basename(photo_path),
                            rate=rate,
                        )

                # Flush any thumb_path updates from the final partial batch.
                _flush_thumb_paths()

                from thumbnails import format_summary as thumb_summary
                thumb_result = {
                    "generated": generated,
                    "skipped": skipped,
                    "failed": failed,
                }
                if failed_photos:
                    thumb_result["failed_photos"] = failed_photos
                if failed > len(failed_photos):
                    thumb_result["failed_photos_truncated"] = (
                        failed - len(failed_photos)
                    )
                processed = generated + skipped + failed
                # Per-photo failures leave coverage gaps but do not invalidate
                # thumbnails that were generated successfully. Keep the stage
                # terminal and expose the affected photos as repair details;
                # fatal setup/runtime exceptions still take the except path.
                stages["thumbnails"]["status"] = "completed"
                stages["thumbnails"]["error_count"] = failed
                thumb_rollup = (
                    f"{failed} of {processed} thumbnails need attention"
                    if failed > 0 else None
                )
                if thumb_rollup:
                    result.setdefault("warnings", []).append(
                        f"[thumbnails] {thumb_rollup}"
                    )
                runner.update_step(job["id"], "thumbnails", status="completed",
                                   summary=thumb_summary(thumb_result),
                                   error_count=failed,
                                   error=thumb_rollup,
                                   progress={"current": processed, "total": processed})
                result["stages"]["thumbnails"] = thumb_result
            except Exception as e:
                errors.append(f"[thumbnails] Fatal: {e}")
                log.exception("Pipeline thumbnail stage failed")
                stages["thumbnails"]["status"] = "failed"
                runner.update_step(job["id"], "thumbnails", status="failed", error=str(e))
                # This stage is the sole consumer of scan_to_thumb. Anything
                # above can raise BEFORE the drain loop (import, Database(),
                # cfg.load(), os.makedirs) — if we just returned, the scanner
                # would eventually block forever in put() once the queue fills,
                # wedging threads["scanner"].join() and leaking a pipeline slot
                # until restart. Set abort so the scanner stops producing, then
                # drain whatever is already queued. Stop at the sentinel, or
                # when the queue stays empty (the sentinel may already have
                # been consumed by the main loop before a late failure; with
                # abort set, photo_cb no longer blocks, so breaking on Empty
                # is safe).
                abort.set()
                while True:
                    try:
                        item = scan_to_thumb.get(timeout=1.0)
                    except queue.Empty:
                        break
                    if item is _SENTINEL:
                        break
            _update_stages(runner, job["id"], stages)

        def previews_stage():
            """Generate preview images for browsed photos."""
            if abort.is_set():
                stages["previews"]["status"] = "skipped"
                runner.update_step(job["id"], "previews", status="completed",
                                   summary="Skipped")
                return

            stages["previews"]["status"] = "running"
            runner.update_step(job["id"], "previews", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                import config as cfg

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective = thread_db.get_effective_config(cfg.load())
                raw_size = (
                    params.preview_max_size
                    if params.preview_max_size is not None
                    else effective.get("preview_max_size", 1920)
                )
                if raw_size == 0:
                    # "Full resolution" — /full redirects to /original, so
                    # there's no size-suffixed file to warm. Skip rather
                    # than produce untracked {id}.jpg files.
                    runner.update_step(
                        job["id"], "previews", status="completed",
                        summary="Skipped (preview_max_size=0 → serves originals)",
                    )
                    stages["previews"]["status"] = "completed"
                    return
                max_size = int(raw_size or 1920)
                preview_quality = effective.get("preview_quality", 90)
                # Must match the Flask serve convention — app.py reads, reaps,
                # and evicts previews under dirname(THUMB_CACHE_DIR)/previews.
                # Using dirname(db_path) here would, with a custom --thumb-dir,
                # warm previews (and preview_cache rows) under a root the app
                # never serves from.
                base_dir = effective_vireo_dir
                preview_dir = os.path.join(base_dir, "previews")
                os.makedirs(preview_dir, exist_ok=True)

                if collection_id:
                    photos = _filter_excluded(thread_db.get_collection_photos(collection_id, per_page=999999))
                elif not skip_scan:
                    # Scan ran but produced no photos — skip previews to avoid
                    # unexpectedly processing the entire workspace.
                    runner.update_step(job["id"], "previews", status="completed",
                                       summary="Skipped (no photos scanned)")
                    stages["previews"]["status"] = "completed"
                    return
                else:
                    photos = thread_db.get_photos(per_page=999999)

                folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
                total = len(photos)
                generated = 0
                skipped = 0
                failed = 0

                for i, photo in enumerate(photos):
                    if _should_abort(abort):
                        break
                    detail_photo = thread_db.get_photo(photo["id"]) or photo
                    cache_path = os.path.join(preview_dir, f'{photo["id"]}_{max_size}.jpg')
                    recipe = thread_db.get_photo_edit_recipe(photo["id"])
                    if os.path.exists(cache_path):
                        cache_row = None
                        with contextlib.suppress(Exception):
                            cache_row = thread_db.preview_cache_get(photo["id"], max_size)
                        if recipe and cache_row is None:
                            with contextlib.suppress(OSError):
                                os.remove(cache_path)
                            if os.path.exists(cache_path):
                                skipped += 1
                                log.info(
                                    "Skipping untracked edited preview for photo %s; "
                                    "existing cache file could not be removed",
                                    photo["id"],
                                )
                                continue
                        else:
                            skipped += 1
                            try:
                                if cache_row is None:
                                    thread_db.preview_cache_insert(
                                        photo["id"],
                                        max_size,
                                        os.path.getsize(cache_path),
                                    )
                            except Exception:
                                pass  # photo may have been deleted mid-pipeline
                            continue
                    if not os.path.exists(cache_path):
                        folder_path = folders.get(detail_photo["folder_id"])
                        try:
                            materialized = materialize_preview(
                                thread_db,
                                detail_photo,
                                folder_path,
                                size=max_size,
                                vireo_dir=base_dir,
                                preview_quality=preview_quality,
                                recipe=recipe,
                                cache_path=cache_path,
                            )
                        except PreviewSourceUnavailable as exc:
                            skipped += 1
                            log.info(
                                "Skipping pipeline preview for photo %s: %s",
                                photo["id"], exc,
                            )
                        except ArtifactProducerFailed as exc:
                            if isinstance(exc.__cause__, PreviewSourceUnavailable):
                                skipped += 1
                                log.info(
                                    "Skipping pipeline preview for photo %s: %s",
                                    photo["id"], exc.__cause__,
                                )
                            else:
                                failed += 1
                        except PreviewMaterializationError:
                            # image_loader logged the source failure; count it
                            # here so it remains visible in the stage rollup.
                            failed += 1
                        else:
                            if materialized.generated:
                                generated += 1
                            else:
                                skipped += 1

                    stages["previews"]["count"] = i + 1
                    stages["previews"]["total"] = total
                    runner.update_step(job["id"], "previews",
                                       current_file=photo["filename"],
                                       progress={"current": i + 1, "total": total})
                    _emit_progress(
                        runner, job["id"], stages, "previews", "Generating previews",
                        current_file=photo["filename"],
                        rate=round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01) * 60, 1
                        ),
                    )

                # One eviction pass after the stage so preview_cache_max_mb is
                # enforced even when the pipeline is the only producer (e.g.
                # first-run ingest). Writes happen per-photo above to avoid
                # per-row fsyncs.
                from preview_cache import evict_if_over_quota
                evict_if_over_quota(thread_db, base_dir)

                result["stages"]["previews"] = {
                    "generated": generated, "skipped": skipped, "failed": failed, "total": total
                }
                final_status = "failed" if failed > 0 else "completed"
                stages["previews"]["status"] = final_status
                previews_rollup = (
                    f"[previews] {failed} of {total} previews failed to generate"
                    if failed > 0 else None
                )
                if previews_rollup:
                    errors.append(previews_rollup)
                summary_parts = [f"{generated} generated"]
                if skipped:
                    summary_parts.append(f"{skipped} cached")
                if failed:
                    summary_parts.append(f"{failed} failed")
                runner.update_step(job["id"], "previews", status=final_status,
                                   summary=", ".join(summary_parts),
                                   error_count=failed,
                                   error=previews_rollup)
            except Exception as e:
                errors.append(f"[previews] Fatal: {e}")
                log.exception("Pipeline previews stage failed")
                stages["previews"]["status"] = "failed"
                runner.update_step(job["id"], "previews", status="failed", error=str(e))

            _update_stages(runner, job["id"], stages)

        def _load_model_bundle(active_model, tax, thread_db, progress_step="model_loader"):
            """Turn a resolved model spec into a ready-to-use classifier bundle.

            Loads labels for the model and constructs the Classifier/TimmClassifier,
            translating ONNXRuntime's cryptic missing-weights errors into an
            actionable "Repair" hint. Called by both the model_loader stage (for
            the first model) and the classify stage (for each subsequent model in
            a multi-model run).
            """
            from classify_job import (
                _load_labels,
                _record_labels_fingerprint,
                _sources_from_metas,
            )
            from labels_fingerprint import compute_fingerprint, compute_full_fingerprint
            from models import _classify_model_state

            model_str = active_model["model_str"]
            weights_path = active_model["weights_path"]
            model_type = active_model.get("model_type", "bioclip")
            model_name = active_model["name"]
            model_is_custom = active_model.get("source") == "custom"
            embedding_started = time.monotonic()
            embedding_start_count = None

            def embedding_progress(current, total):
                nonlocal embedding_start_count, embedding_started
                if embedding_start_count is None:
                    embedding_start_count = current
                    embedding_started = time.monotonic()
                phase = f"Preparing species labels for {model_name}"
                detail = f"{current:,} / {total:,} labels ready"
                completed = current - embedding_start_count
                elapsed = time.monotonic() - embedding_started
                if completed >= 5 and elapsed >= 5 and current < total:
                    minutes = math.ceil((total - current) * elapsed / completed / 60)
                    detail += f" · about {minutes:,} min remaining"
                runner.update_step(
                    job["id"], progress_step, current_file=detail,
                    progress={"current": current, "total": total, "unit": "labels"},
                )
                stage_id = "model_loader" if progress_step == "model_loader" else "classify"
                if stage_id == "model_loader":
                    stages[stage_id].update(count=current, total=total, label=phase)
                _emit_progress(
                    runner, job["id"], stages, stage_id, phase,
                    current_file=detail, step_id=progress_step,
                    rate=0,
                    phase_current=current, phase_total=total, phase_label="Species labels",
                )

            labels, use_tol, label_metas = _load_labels(
                model_type=model_type,
                model_str=model_str,
                labels_file=params.labels_file,
                labels_files=params.labels_files,
                db=thread_db,
                model_dir=weights_path,
            )
            # Compute a content-addressable fingerprint for the active label set
            # and record it in the labels_fingerprints sidecar. Kept on the bundle
            # so classify_stage can pass it to record_classifier_run for each
            # (detection, model, fingerprint) triple. Source paths come from
            # the metadata ``_load_labels`` actually consumed so the sidecar
            # cannot name lists that did not produce ``labels``.
            fp = compute_fingerprint(labels)
            fp_full = compute_full_fingerprint(labels)
            if len(fp_full) != 64:
                fp_full = None
            label_sources = _sources_from_metas(label_metas)
            _record_labels_fingerprint(
                thread_db, fp, labels, sources=label_sources,
                full_fingerprint=fp_full,
            )

            # Preflight: validate the on-disk model before handing it to
            # ONNXRuntime. A stale _check_onnx_downloaded result (e.g. after
            # the user deleted a .onnx.data file, or the download manifest
            # changed) would otherwise surface as an opaque ONNXRuntime crash.
            # "unverified" is accepted here: all files are present, only the
            # SHA256 cross-check with HuggingFace was skipped (transient network
            # issue). The lazy verify_if_needed call below will retry the hash
            # check, and get_models() already treats these as downloaded, so
            # rejecting them here turns a warning into a hard pipeline failure.
            files = active_model.get("files", [])
            if files and weights_path:
                state = _classify_model_state(weights_path, files)
                if state not in ("ok", "unverified"):
                    raise RuntimeError(
                        _incomplete_model_message(model_name, model_is_custom)
                    )

            # Lazy SHA256 verification: for known models (those with an
            # hf_subdir), hash every LFS file on first load in this process
            # and compare against HuggingFace's reported SHA256. Catches
            # silent corruption and truncated downloads that slipped past
            # hf_hub_download. Result is cached in-process so subsequent
            # pipeline runs pay zero cost.
            hf_subdir = active_model.get("hf_subdir")
            if hf_subdir and not model_is_custom and weights_path:
                import model_verify
                try:
                    model_verify.verify_if_needed(
                        active_model["id"], weights_path, hf_subdir,
                        optional_files=active_model.get("optional_files"),
                    )
                except model_verify.ModelCorruptError as verify_err:
                    log.warning(
                        "Lazy verification failed for %s: %s",
                        active_model["id"], verify_err,
                    )
                    raise RuntimeError(
                        _incomplete_model_message(model_name, model_is_custom)
                    ) from verify_err
                except model_verify.VerifyError as verify_err:
                    # Can't reach HF to fetch expected hashes — log and
                    # proceed. This keeps offline pipeline runs working
                    # when the model is already on disk.
                    log.warning(
                        "Skipping verification for %s (could not fetch "
                        "expected hashes): %s",
                        active_model["id"], verify_err,
                    )

            try:
                from classifier import ClassifierLoadPaused
            except ImportError:
                class ClassifierLoadPaused(RuntimeError):
                    pass

            def _construct_classifier():
                nonlocal embedding_start_count, embedding_started
                embedding_start_count = None
                embedding_started = time.monotonic()
                try:
                    from classifier import ClassificationCancelled
                except ImportError:
                    class ClassificationCancelled(RuntimeError):
                        pass

                # Non-parking cancel probe: this factory runs under
                # ``ModelCache.entry.load_lock`` (see ``model_cache.acquire``),
                # and ``Classifier._compute_embeddings_with_progress`` invokes
                # this callback between labels while custom-label embeddings
                # are being computed. A parking probe here would call
                # ``_pause_checkpoint`` and block on ``wait_if_paused`` while
                # the shared load_lock is still held, so any unpaused peer
                # waiting on the same cache entry would stay blocked until
                # Resume (Codex discussion_r3791005913 — sibling of the
                # resource-probe rebinding in ``model_cache.py``, which only
                # covers the bound resource cancel probe, not this explicit
                # captured callback). Cancel still fires; pause is honored at
                # the next outer ``_pause_checkpoint`` after ``load_lock`` is
                # released.
                def cancel_check():
                    return (
                        _should_abort_without_pause(abort)
                        or _cancellation_requested()
                    )

                # Non-parking pause probe. A pause request makes the
                # classifier checkpoint the label embeddings finished so
                # far and raise ``ClassifierLoadPaused`` instead of
                # parking under ``load_lock``; the acquire loop below
                # parks this participant at its normal checkpoint with
                # every lock released and constructs again on Resume.
                # Threads without a registered pause participant cannot
                # park, so they never abort for pause and instead honor
                # it at the owning worker's next boundary.
                def pause_check():
                    if getattr(pause_context, "participant", None) is None:
                        return False
                    probe = getattr(runner, "pause_requested", None)
                    return bool(probe is not None and probe(job["id"]))

                if model_type == "timm":
                    if cancel_check():
                        raise ClassificationCancelled("classification cancelled")
                    from timm_classifier import TimmClassifier
                    return TimmClassifier(model_str, taxonomy=tax)
                if cancel_check():
                    raise ClassificationCancelled("classification cancelled")
                from classifier import Classifier
                if not use_tol:
                    from classify_job import _reuse_saved_label_embeddings
                    _reuse_saved_label_embeddings(thread_db, model_str, weights_path, labels, cancel_check)
                return Classifier(
                    labels=None if use_tol else labels,
                    model_str=model_str,
                    pretrained_str=weights_path,
                    embedding_progress_callback=embedding_progress,
                    cancel_check=cancel_check,
                    pause_check=pause_check,
                )

            # The shared cache key includes an ordered, canonical label identity
            # when use_tol=False because Classifier captures embeddings in that
            # exact column order. Two pipelines with different labels or order
            # must not share a session. Tree-of-Life mode reads precomputed
            # embeddings and is label-independent, so its key is constant.
            #
            # The timm key also varies by taxonomy fingerprint: TimmClassifier
            # captures the taxonomy at construction and resolves common names /
            # hierarchy from it on every prediction. Reusing a classifier loaded
            # against a stale taxonomy (or no taxonomy) after a later run
            # downloads or refreshes one would silently emit predictions
            # missing the enrichment, so a change in taxonomy must miss the
            # cache and rebuild.
            #
            # The weights fingerprint catches in-place model replacement
            # (Repair, custom re-register). Without it, a pipeline started
            # before the old session's idle timer fires would reuse the
            # stale ONNX session on the new bytes.
            if model_type == "timm":
                from computation_cache import taxonomy_identity

                tax_fp = taxonomy_identity(tax)
            else:
                tax_fp = None
            files = active_model.get("files")

            # _construct_classifier may trigger the ONNX self-heal path
            # (create_session_with_self_heal) which deletes corrupt weights
            # and redownloads them inside the factory. The shared acquisition
            # helper rekeys the entry to the post-load file fingerprint.
            cache_handle = None
            try:
                while True:
                    try:
                        cache_handle = acquire_cached_classifier(
                            model_type=model_type,
                            model_str=model_str,
                            weights_path=weights_path,
                            labels=None if use_tol else labels,
                            factory=_construct_classifier,
                            files=files,
                            # Fold in optional-artifact presence: without
                            # this a Repair that downloads timm's
                            # label_descriptions.json (or bioclip-2.5's ToL
                            # files) would not invalidate the entry already
                            # loaded from the pre-repair install, and
                            # subsequent pipeline runs would keep using a
                            # stale classifier constructed without those
                            # artifacts.
                            optional_files=active_model.get("optional_files"),
                            taxonomy_fingerprint=tax_fp,
                            cancel_check=lambda: (
                                _should_abort(abort)
                                or _cancellation_requested()
                            ),
                        )
                        clf = cache_handle.__enter__()
                        break
                    except ClassifierLoadPaused:
                        # The factory stepped out of the shared load lock
                        # with its label embeddings checkpointed. Park this
                        # participant at the pipeline's pause gate (no lock
                        # held), then construct again so the computation
                        # resumes from the checkpoint. A Cancel during the
                        # pause surfaces on the retry through the factory's
                        # own cancel probe.
                        log.info(
                            "Classifier load for %s paused; parking until "
                            "Resume", model_name,
                        )
                        _pause_checkpoint()
                        # ``pause_check`` only fires on a thread with a
                        # registered participant, so the gate parks us
                        # above. Should it ever return with the pause
                        # still pending (and no cancel), wait on the
                        # runner directly rather than spin through
                        # construction until Resume.
                        pause_probe = getattr(runner, "pause_requested", None)
                        direct_wait = getattr(runner, "wait_if_paused", None)
                        if (
                            pause_probe is not None
                            and direct_wait is not None
                            and pause_probe(job["id"])
                            and not _cancellation_requested()
                        ):
                            direct_wait(job["id"], publish_paused=False)
                        continue
            except Exception as load_err:
                # ONNXRuntime signals missing external-data with a
                # "model_path must not be empty" / "Initializer" error. Treat
                # any load failure as an incomplete-model hint for the user —
                # but only when we can confirm the on-disk files are actually
                # bad. A transient ONNX failure (memory pressure, mmap race,
                # test-suite monkeypatches from another process) should not
                # permanently mark a healthy install as "Incomplete".
                if _looks_like_missing_external_data(load_err):
                    import model_verify

                    files_ok = False
                    hf_subdir = active_model.get("hf_subdir")
                    if (
                        weights_path
                        and hf_subdir
                        and not model_is_custom
                    ):
                        try:
                            result = model_verify.verify_model(
                                weights_path, hf_subdir,
                                optional_files=active_model.get(
                                    "optional_files"
                                ),
                            )
                            files_ok = result.ok
                        except model_verify.VerifyError:
                            # Network unavailable — can't confirm either way.
                            # Fall through to the conservative path that writes
                            # the sentinel so the user sees Repair.
                            files_ok = False

                    if files_ok:
                        # Files match HF hashes exactly — the ONNX error is
                        # transient, not corruption. Do NOT write
                        # .verify_failed; do NOT tell the user to Repair. Just
                        # re-raise with a retry hint.
                        log.warning(
                            "ONNXRuntime load failed for %s but on-disk files "
                            "pass SHA256 verification — treating as transient.",
                            active_model.get("id", "<unknown>"),
                        )
                        raise RuntimeError(
                            f"Model '{model_name}' failed to load "
                            f"(transient ONNXRuntime error). Retry the "
                            f"pipeline. If this keeps happening, restart Vireo."
                        ) from load_err

                    # Files are bad or unverifiable — write the sentinel so
                    # Settings surfaces the Repair button.
                    if weights_path:
                        sentinel_path = os.path.join(
                            weights_path,
                            model_verify.VERIFY_FAILED_SENTINEL,
                        )
                        try:
                            with open(sentinel_path, "w") as f:
                                f.write(f"onnx-load-failure: {load_err}\n")
                        except OSError:
                            pass
                    raise RuntimeError(
                        _incomplete_model_message(model_name, model_is_custom)
                    ) from load_err
                raise

            try:
                from computation_cache import (
                    classifier_model_identity,
                    fingerprint,
                    with_consumed_label_descriptions,
                )

                portable_model_identity = classifier_model_identity(active_model)
                # Stamp the identity with the label_descriptions.json the
                # constructed classifier actually read, not what the disk
                # shows now: TimmClassifier's background heal publishes
                # that file during normal operation and can land between
                # the classifier's read and this probe. See
                # computation_cache.with_consumed_label_descriptions.
                portable_model_identity = with_consumed_label_descriptions(
                    portable_model_identity, clf,
                )
                if fp_full is None and use_tol and portable_model_identity:
                    fp_full = fingerprint({
                        "label_space": "tree-of-life",
                        "model": portable_model_identity,
                    })
                    _record_labels_fingerprint(
                        thread_db, fp, labels, sources=label_sources,
                        full_fingerprint=fp_full,
                    )
            except (OSError, ValueError):
                portable_model_identity = None

            # What this model actually compares photos against — the merged
            # species lists, Tree of Life, or a timm model's fixed head. The
            # classify step shows it so the row names the label space, not
            # just the weights.
            from classify_job import describe_label_source

            label_source = describe_label_source(
                params, thread_db,
                labels=labels,
                use_tol=use_tol,
                model_type=model_type,
                class_count=getattr(clf, "label_space_size", None),
                label_metas=label_metas,
            )

            return {
                "clf": clf,
                "_cache_handle": cache_handle,
                "model_type": model_type,
                "model_name": model_name,
                "model_str": model_str,
                "labels": labels,
                "label_source": label_source,
                "labels_fingerprint": fp,
                "labels_fingerprint_full": fp_full,
                "classifier_model_identity": portable_model_identity,
                "use_tol": use_tol,
                "active_model": active_model,
            }

        def model_loader_stage():
            if params.skip_classify:
                stages["model_loader"]["status"] = "skipped"
                runner.update_step(job["id"], "model_loader", status="completed",
                                   summary="Skipped")
                _update_stages(runner, job["id"], stages)
                models_ready.set()
                return
            stages["model_loader"]["status"] = "running"
            runner.update_step(job["id"], "model_loader", status="running",
                               current_file="Resolving model...")
            _update_stages(runner, job["id"], stages)
            try:

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                if collection_id:
                    candidate_photos = _filter_excluded(
                        thread_db.get_collection_photos(collection_id, per_page=999999)
                    )
                    photo_ids = [p["id"] for p in candidate_photos]
                    candidate_ids = thread_db.filter_out_wildlife_excluded(photo_ids)
                    if candidate_photos and not candidate_ids:
                        stages["model_loader"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "model_loader", status="completed",
                            summary="Skipped (all photos marked not wildlife)",
                        )
                        _update_stages(runner, job["id"], stages)
                        models_ready.set()
                        return

                # Specs were pre-resolved at job start so step_defs could carry
                # the model's display name on each `classify:<id>` row. If that
                # resolution raised, surface the same error here — model_loader
                # is the stage that owns "no model / bad id" failures.
                if resolution_error:
                    raise RuntimeError(resolution_error)

                first_name = resolved_specs[0]["name"]
                runner.update_step(job["id"], "model_loader", current_file=first_name)

                # Download taxonomy if missing/unusable and requested. Mirrors the
                # availability check used by /api/pipeline/page-init: a 0-byte stub
                # from an interrupted download "exists" but is not a usable
                # taxonomy, and the user opted into a download to recover from
                # exactly that state.
                from models import get_taxonomy_info
                from taxonomy import TAXONOMY_JSON_PATH, find_taxonomy_json
                taxonomy_path = find_taxonomy_json()
                if params.download_taxonomy and not get_taxonomy_info().get("available"):
                    try:
                        from taxonomy import download_taxonomy
                        _emit_progress(
                            runner, job["id"], stages, "model_loader", "Downloading taxonomy...",
                        )
                        # Always write new downloads to the persistent path.
                        taxonomy_path = TAXONOMY_JSON_PATH
                        download_taxonomy(taxonomy_path, progress_callback=lambda msg:
                            _emit_progress(
                                runner, job["id"], stages, "model_loader", msg,
                            )
                        )
                    except Exception as e:
                        log.warning("Taxonomy download failed, continuing without: %s", e)

                # Taxonomy is shared across every classifier in the run.
                # Use load_local_taxonomy() so a corrupt persistent file
                # falls back to the legacy package-dir copy.
                from taxonomy import load_local_taxonomy
                tax = load_local_taxonomy()
                loaded_models["tax"] = tax
                # Portable identity for the taxonomy backing this pipeline.
                # Threaded through publish and cache-match paths so that
                # classifier runs made against one taxonomy revision are
                # not conflated with runs from a different revision on
                # another installation. See computation_cache.taxonomy_identity.
                from computation_cache import (
                    taxonomy_identity as _taxonomy_identity_fn,
                )
                loaded_models["taxonomy_identity"] = _taxonomy_identity_fn(tax)
                loaded_models["resolved_specs"] = resolved_specs

                # Load the first classifier so classify_stage can start as soon
                # as scan completes; any remaining specs are loaded inside
                # classify_stage so we never hold more than one model in memory.
                _emit_progress(
                    runner, job["id"], stages, "model_loader", f"Loading {first_name}...",
                )

                try:
                    bundle = _load_model_bundle(resolved_specs[0], tax, thread_db)
                    loaded_models.update(bundle)
                except Exception as preload_err:
                    is_classification_cancelled = (
                        preload_err.__class__.__name__ == "ClassificationCancelled"
                    )
                    if (
                        _cancellation_requested()
                        or is_classification_cancelled
                        or str(preload_err) == "classification cancelled"
                    ):
                        raise
                    if len(resolved_specs) > 1:
                        # Other models remain — don't abort the whole pipeline.
                        log.warning(
                            "First model %s failed to load, %d remaining: %s",
                            first_name, len(resolved_specs) - 1, preload_err,
                        )
                        loaded_models["preload_error"] = str(preload_err)
                    else:
                        # Single model — fatal, let the outer handler abort.
                        raise

                loaded_models["pending_specs"] = resolved_specs[1:]

                stages["model_loader"]["status"] = "completed"
                summary = ", ".join(s["name"] for s in resolved_specs)
                if "preload_error" in loaded_models:
                    summary += f" ({first_name} failed to preload)"
                runner.update_step(job["id"], "model_loader", status="completed",
                                   summary=summary)
            except Exception as e:
                abort.set()
                is_classification_cancelled = (
                    e.__class__.__name__ == "ClassificationCancelled"
                )
                if (
                    _cancellation_requested()
                    or is_classification_cancelled
                    or str(e) == "classification cancelled"
                ):
                    stages["model_loader"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "model_loader",
                        status="completed", summary="Skipped (cancelled)",
                    )
                else:
                    errors.append(f"[model_loader] Fatal: {e}")
                    log.exception("Pipeline model loader stage failed")
                    stages["model_loader"]["status"] = "failed"
                    runner.update_step(
                        job["id"], "model_loader", status="failed", error=str(e),
                    )
            finally:
                models_ready.set()
                _update_stages(runner, job["id"], stages)

        # Shared state between detect_stage and classify_stage. Written by
        # detect_stage, consumed by classify_stage. Populated even on early
        # exit so classify_stage can reason about "detection ran but produced
        # nothing" vs. "detection never executed".
        detect_state = {
            "photos": [],        # list of photo dicts for the collection
            "folders": {},       # {folder_id: path}
            "detections": {},    # {photo_id: [detection_dict, ...]}
            "processed_ids": set(),  # photo_ids whose _detect_batch iteration completed
            "pre_run_det_ids": {},   # snapshot for reclassify purge
            "total_detected": 0,
            "ran": False,        # True once detect_stage's body executed (even if no-op)
        }

        def detect_stage():
            """Run MegaDetector across every collection photo once, ahead of any
            classification. Populates detect_state so each per-model classify
            step can pull cached detections rather than re-running MegaDetector.

            Splitting detect out (it used to run interleaved with model 1's
            classify loop) lets users see detection as its own row in the jobs
            view, and lets a multi-model run amortize one detection pass across
            every classifier.
            """
            collection_ready.wait()
            models_ready.wait()

            has_models_to_try = (
                "clf" in loaded_models
                or loaded_models.get("resolved_specs")
            )
            if (
                params.skip_classify
                or abort.is_set()
                or not collection_id
                or not has_models_to_try
            ):
                stages["detect"]["status"] = "skipped"
                runner.update_step(job["id"], "detect", status="completed",
                                   summary="Skipped")
                _update_stages(runner, job["id"], stages)
                return

            stages["detect"]["status"] = "running"
            # Also mark the aggregate classify stage as running so the pipeline
            # wizard's "Classify" card (which predates the detect/classify split)
            # shows activity during the detect pre-pass instead of waiting for
            # the first per-model classify step to start.
            stages["classify"]["status"] = "running"
            runner.update_step(job["id"], "detect", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                from classify_job import _BATCH_SIZE, _detect_batch

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                photos = _filter_excluded(
                    thread_db.get_collection_photos(collection_id, per_page=999999)
                )
                photo_ids = [p["id"] for p in photos]
                kept_ids = set(thread_db.filter_out_wildlife_excluded(photo_ids))
                skipped_wildlife = len(photos) - len(kept_ids)
                if skipped_wildlife:
                    log.info(
                        "Skipping %d photo(s) marked not wildlife",
                        skipped_wildlife,
                    )
                photos = [p for p in photos if p["id"] in kept_ids]
                folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
                total = len(photos)
                detect_state["photos"] = photos
                detect_state["folders"] = folders
                detect_state["ran"] = True

                try:
                    from computation_cache import (
                        ArtifactStore,
                        materialize_local_store,
                    )

                    cache_store = (
                        ArtifactStore(computation_cache_dir)
                        if computation_cache_dir else None
                    )
                    reused = (
                        materialize_local_store(thread_db, store=cache_store)
                        if not params.reclassify else {}
                    )
                    detect_state["portable_reused"] = reused
                except Exception:
                    log.warning(
                        "Could not apply local computation cache", exc_info=True,
                    )
                    detect_state["portable_reused"] = {}

                # Reclassify semantics (see prior interleaved implementation for
                # history): start with an empty already_detected so EVERY photo
                # is re-detected; snapshot pre-run detection IDs so we can purge
                # them after this detect pass completes. On a non-reclassify run,
                # pre-seed already_detected from detector_runs so _detect_batch
                # reuses rows instead of re-invoking MegaDetector — including
                # empty-scene photos (box_count=0) which would otherwise be
                # re-detected forever by a legacy detections-only seed.
                if params.reclassify:
                    already_detected: set = set()
                    detector_runtime = None
                    photo_ids_list = [p["id"] for p in photos]
                    pre_run_det_ids: dict = getattr(
                        thread_db, "get_detection_ids_for_photos", lambda _: {}
                    )(photo_ids_list)
                else:
                    try:
                        from computation_cache import megadetector_runtime_fingerprint

                        detector_runtime = megadetector_runtime_fingerprint()
                    except (OSError, ValueError):
                        detector_runtime = None
                    if detector_runtime is not None:
                        already_detected = set(
                            thread_db.get_detector_run_photo_ids(
                                "megadetector-v6",
                                runtime_fingerprint=detector_runtime,
                            )
                        )
                    else:
                        already_detected = set(
                            thread_db.get_detector_run_photo_ids("megadetector-v6")
                        )
                    pre_run_det_ids = {}
                job["_detector_runtime_fingerprint"] = detector_runtime
                detect_state["pre_run_det_ids"] = pre_run_det_ids

                # Ensure MegaDetector weights only when we actually need fresh
                # detection work — an offline rerun over already-detected photos
                # should not trigger a ~300 MB download.
                needs_fresh_detection = bool(photos) and (
                    params.reclassify
                    or any(p["id"] not in already_detected for p in photos)
                )
                if needs_fresh_detection:
                    from detector import ensure_megadetector_weights

                    def _dl_progress(phase, current, total_steps):
                        # Weight download is a sub-phase of detect; don't treat
                        # its bytes as detect's stage-level counter or the bar
                        # jumps ahead before any photo has been detected.
                        _emit_progress(
                            runner, job["id"], stages, "detect", phase,
                        )

                    weights_path = ensure_megadetector_weights(
                        progress_callback=_dl_progress,
                    )
                    from computation_cache import megadetector_runtime_fingerprint

                    detector_runtime = megadetector_runtime_fingerprint(weights_path)
                    job["_detector_runtime_fingerprint"] = detector_runtime
                    if not params.reclassify and detector_runtime is not None:
                        already_detected = set(
                            thread_db.get_detector_run_photo_ids(
                                "megadetector-v6",
                                runtime_fingerprint=detector_runtime,
                            )
                        )

                this_run_detections: dict = detect_state["detections"]
                processed_ids: set = detect_state["processed_ids"]
                total_detected = 0
                start_time = time.time()

                for batch_start in range(0, total, _BATCH_SIZE):
                    if _should_abort(abort):
                        break
                    batch = photos[batch_start:batch_start + _BATCH_SIZE]
                    batch_idx = batch_start + len(batch)

                    stages["detect"]["count"] = batch_idx
                    stages["detect"]["total"] = total
                    _emit_progress(
                        runner, job["id"], stages, "detect", "Detecting subjects",
                        rate=round(
                            batch_idx / max(time.time() - start_time, 0.01) * 60,
                            1,
                        ),
                    )
                    runner.update_step(
                        job["id"], "detect",
                        progress={"current": batch_idx, "total": total},
                    )

                    # GPU serialisation lives inside detector.detect_animals()
                    # — wrapping the whole batch here would hold the semaphore
                    # across DB writes and CPU sharpness/quality work, blocking
                    # a concurrent pipeline's GPU stages on this pipeline's
                    # non-GPU work.
                    det_map, det_count, det_processed = _detect_batch(
                        batch, folders, runner, job,
                        params.reclassify, thread_db,
                        already_detected_ids=already_detected,
                        cached_detections=None,
                    )
                    total_detected += det_count
                    already_detected.update(det_processed)
                    for pid, dets in det_map.items():
                        this_run_detections.setdefault(pid, dets)
                    for pid in det_processed:
                        this_run_detections.setdefault(pid, [])
                    processed_ids.update(det_processed)

                detect_state["total_detected"] = total_detected
                # The stale-detection purge is DEFERRED to classify_stage and
                # only fires after the first model successfully classifies.
                # Deleting the pre-run detection rows here would cascade through
                # the predictions FK and destroy prior results in the case where
                # every classifier ends up failing to load — leaving the user
                # with no detections AND no predictions. See classify_stage for
                # the actual delete.

                # Reapply the local computation cache now that fresh
                # detector_runs exist. Classification artifacts whose
                # detector dependency was absent at the pre-detection
                # materialize call get a second chance to land here, so
                # bundles carrying only classifications still surface.
                #
                # We ALSO pre-create synthetic full-image detector rows
                # for every empty-scene photo before the reapply. The
                # classify stage below creates those rows lazily
                # per-photo, so without pre-creating them here a cached
                # full_image classification artifact has no anchor to
                # attach to at reapply time — the classify stage then
                # runs the classifier itself even though the answer is
                # already sitting in the local store.
                if not params.reclassify:
                    try:
                        import config as cfg
                        from computation_cache import (
                            CacheFormatError,
                            full_image_runtime_fingerprint,
                            materialize_local_store,
                            source_input,
                        )

                        # Pre-create full-image anchors for empty-scene
                        # photos, and promote any legacy full-image
                        # ``detector_runs`` row to the portable runtime
                        # so imported full-image classifications
                        # actually attach on materialize instead of
                        # being deferred by the runtime-fingerprint
                        # gate.
                        full_runtime = full_image_runtime_fingerprint()
                        # Also broaden the anchor set to include photos
                        # whose only detections are noise (< detector_
                        # confidence) so cached full-image classifiers
                        # attach on this materialize instead of forcing
                        # the classify stage's lazy per-photo anchor
                        # creation — which happens AFTER materialize and
                        # re-runs inference for an answer already sitting
                        # in the local store. The runtime fallback at
                        # ``classify_stage`` fires under the same
                        # predicate (no usable animal box at the strict
                        # threshold, no confident non-animal box), so
                        # mirror it exactly here to avoid pre-creating
                        # anchors the fallback would refuse to use.
                        try:
                            _effective_cfg = thread_db.get_effective_config(
                                cfg.load()
                            )
                            _det_conf = _effective_cfg.get(
                                "detector_confidence", 0.2,
                            )
                        except Exception:
                            _effective_cfg = {}
                            _det_conf = 0.2

                        _pipeline_cfg = _effective_cfg.get("pipeline", {})
                        _weak_enabled = _pipeline_cfg.get(
                            "weak_detection_rescue_enabled", True,
                        )
                        _weak_conf = _pipeline_cfg.get(
                            "weak_detection_confidence", 0.12,
                        )
                        _contextual_weak_ids = set()
                        if (
                            _weak_enabled
                            and _weak_conf < _det_conf
                            and photos
                        ):
                            from weak_detections import (
                                contextual_weak_photo_ids,
                            )

                            _raw_mdv6 = thread_db.get_detections_for_photos(
                                [p["id"] for p in photos],
                                min_conf=_weak_conf,
                                detector_model="megadetector-v6",
                            )
                            _contextual_weak_ids = contextual_weak_photo_ids(
                                photos,
                                _raw_mdv6,
                                detector_confidence=_det_conf,
                                weak_confidence=_weak_conf,
                                max_gap=_pipeline_cfg.get(
                                    "burst_time_gap", 3.0,
                                ),
                            )

                        def _needs_full_image_anchor(pid):
                            if pid in _contextual_weak_ids:
                                return False
                            dets = this_run_detections.get(pid) or []
                            if not dets:
                                return True
                            has_usable_animal = any(
                                d.get("detector_model") != "full-image"
                                and d.get("category", "animal") == "animal"
                                and d.get(
                                    "confidence",
                                    d.get("detector_confidence", 0),
                                ) >= _det_conf
                                for d in dets
                            )
                            if has_usable_animal:
                                return False
                            confident_non_animal = any(
                                d.get("detector_model") != "full-image"
                                and d.get("category", "animal") != "animal"
                                and d.get(
                                    "confidence",
                                    d.get("detector_confidence", 0),
                                ) >= _det_conf
                                for d in dets
                            )
                            return not confident_non_animal

                        empty_scene_ids = [
                            photo["id"] for photo in photos
                            if photo["id"] in processed_ids
                            and _needs_full_image_anchor(photo["id"])
                        ]
                        for photo_id in empty_scene_ids:
                            identity = thread_db.conn.execute(
                                """SELECT file_hash, companion_path
                                   FROM photos WHERE id = ?""",
                                (photo_id,),
                            ).fetchone()
                            full_input = None
                            if (
                                identity is not None
                                and not identity["companion_path"]
                            ):
                                try:
                                    _block, full_input = source_input(
                                        identity["file_hash"],
                                        "vireo-detector-source-v1",
                                    )
                                except (ValueError, CacheFormatError):
                                    # source_input raises CacheFormatError on
                                    # NULL / non-canonical file_hash values.
                                    # Falling through to the outer
                                    # ``except Exception`` would abandon the
                                    # post-detect reapply for the whole
                                    # detect stage; keep full_input=None so
                                    # the surrounding write still runs.
                                    full_input = None
                            existing_full = thread_db.get_detections(
                                photo_id, detector_model="full-image",
                                min_conf=0,
                            )
                            if not existing_full:
                                thread_db.save_detections(
                                    photo_id,
                                    [{
                                        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                        "confidence": 0,
                                        "category": "animal",
                                    }],
                                    detector_model="full-image",
                                    runtime_fingerprint=full_runtime,
                                )
                            else:
                                # Promote a legacy full-image detection so
                                # its runtime_fingerprint matches the run
                                # we're about to record. exportable_artifacts
                                # reads the detector runtime from
                                # ``detections`` — leaving it at 'legacy'
                                # makes future exports emit an empty
                                # full-image detection artifact for this
                                # photo, dropping the attached classifier
                                # run.
                                thread_db.conn.execute(
                                    """UPDATE detections
                                          SET runtime_fingerprint = ?
                                        WHERE photo_id = ?
                                          AND detector_model = 'full-image'
                                          AND (runtime_fingerprint IS NULL
                                               OR runtime_fingerprint != ?)""",
                                    (full_runtime, photo_id, full_runtime),
                                )
                                thread_db.conn.commit()
                            existing_run = thread_db.conn.execute(
                                """SELECT runtime_fingerprint FROM detector_runs
                                   WHERE photo_id = ?
                                     AND detector_model = 'full-image'""",
                                (photo_id,),
                            ).fetchone()
                            if (
                                existing_run is None
                                or existing_run["runtime_fingerprint"]
                                != full_runtime
                            ):
                                thread_db.record_detector_run(
                                    photo_id, "full-image", box_count=1,
                                    runtime_fingerprint=full_runtime,
                                    input_fingerprint=full_input,
                                )

                        known_runtimes = {full_runtime}
                        if detector_runtime is not None:
                            known_runtimes.add(detector_runtime)
                        # Compute expected classifier runtimes for any
                        # classifier the model_loader stage already
                        # resolved. Without this the classifier-runtime
                        # quarantine drops bundle classifications even
                        # when this install carries the exact matching
                        # classifier. Multi-classifier pipelines only
                        # get the first classifier's runtime here;
                        # later classifiers land during their own
                        # classify_stage invocations of the cache.
                        known_classifier_runtimes = set()
                        pl_identity = loaded_models.get(
                            "classifier_model_identity",
                        )
                        pl_fp_full = loaded_models.get(
                            "labels_fingerprint_full",
                        )
                        if (
                            pl_identity
                            and isinstance(pl_fp_full, str)
                            and len(pl_fp_full) == 64
                        ):
                            try:
                                from computation_cache import (
                                    classifier_runtime_fingerprint,
                                )

                                pl_tax_identity = loaded_models.get(
                                    "taxonomy_identity", "no-tax",
                                )
                                for det_rt in (detector_runtime, full_runtime):
                                    if not det_rt:
                                        continue
                                    crt = classifier_runtime_fingerprint(
                                        pl_identity, pl_fp_full, det_rt,
                                        taxonomy_identity=pl_tax_identity,
                                    )
                                    if crt:
                                        known_classifier_runtimes.add(crt)
                            except Exception:
                                known_classifier_runtimes = set()
                        from computation_cache import ArtifactStore

                        reapply_store = (
                            ArtifactStore(computation_cache_dir)
                            if computation_cache_dir else None
                        )
                        post = materialize_local_store(
                            thread_db, store=reapply_store,
                            known_runtimes=known_runtimes,
                            known_classifier_runtimes=(
                                known_classifier_runtimes or None
                            ),
                        )
                        if post.get("classifier_runs_applied"):
                            prior = detect_state.get("portable_reused") or {}
                            prior_applied = prior.get(
                                "classifier_runs_applied", 0,
                            )
                            merged = dict(prior)
                            merged["classifier_runs_applied"] = (
                                prior_applied
                                + post["classifier_runs_applied"]
                            )
                            detect_state["portable_reused"] = merged
                    except Exception:
                        log.warning(
                            "Could not reapply local computation cache after detect",
                            exc_info=True,
                        )

                stages["detect"]["status"] = "completed"
                runner.update_step(
                    job["id"], "detect", status="completed",
                    summary=(
                        f"{total_detected} animals detected in {total} photos"
                        if total else "No photos to detect"
                    ),
                )
                result["stages"]["detect"] = {
                    "total": total,
                    "detected": total_detected,
                    "processed": len(processed_ids),
                }
            except Exception as e:
                errors.append(f"[detect] Fatal: {e}")
                log.exception("Pipeline detect stage failed")
                abort.set()
                stages["detect"]["status"] = "failed"
                runner.update_step(job["id"], "detect", status="failed",
                                   error=str(e))

            _update_stages(runner, job["id"], stages)

        def classify_stage():
            """Run one classifier per model against the pre-computed detections.

            Each model drives its own `classify:<model_id>` step so users see
            per-model progress, duration, and summary instead of an aggregate.
            A model that fails to load is marked `failed` on its own row — the
            run continues with remaining models.
            """
            has_models_to_try = (
                "clf" in loaded_models
                or loaded_models.get("resolved_specs")
            )
            if (
                params.skip_classify
                or abort.is_set()
                or not collection_id
                or not has_models_to_try
            ):
                # Distinguish loader-driven abort (model resolution or preload
                # failure) from benign skips (skip_classify, user cancellation,
                # missing collection): rows for the former must surface as
                # 'failed' so the per-model failure is visible on the job tree
                # — the whole point of splitting classify into per-model rows.
                loader_failed = stages["model_loader"]["status"] == "failed"
                loader_err = next(
                    (e for e in errors if e.startswith("[model_loader] Fatal:")),
                    None,
                )
                row_status = "failed" if loader_failed else "completed"
                row_summary = (
                    "Model load failed" if loader_failed else "Skipped"
                )
                stages["classify"]["status"] = (
                    "failed" if loader_failed else "skipped"
                )

                specs_for_step_ids = loaded_models.get("resolved_specs") or []
                if specs_for_step_ids:
                    for spec in specs_for_step_ids:
                        runner.update_step(
                            job["id"], f"classify:{spec['id']}",
                            status=row_status, summary=row_summary,
                            error=loader_err if loader_failed else None,
                        )
                else:
                    for mid in (effective_model_ids or ["__unresolved__"]):
                        runner.update_step(
                            job["id"], f"classify:{mid}",
                            status=row_status, summary=row_summary,
                            error=loader_err if loader_failed else None,
                        )
                # model_loader may have already loaded the first classifier
                # before this early-return path was hit (e.g. abort.is_set()).
                # Release its cache handle so a same-key reload can be a hit
                # and idle eviction can reclaim VRAM.
                _release_classifier_cache_handle(loaded_models)
                _update_stages(runner, job["id"], stages)
                return

            stages["classify"]["status"] = "running"
            _update_stages(runner, job["id"], stages)

            # Track which per-model rows have reached a terminal state so a
            # fatal error raised by one model doesn't overwrite the status of
            # already-completed models (P2 from the Codex review). Defined
            # outside the try so the except handler can read them.
            completed_step_ids: set = set()
            failed_step_ids: set = set()

            try:
                import config as cfg
                from classify_job import (
                    _BATCH_SIZE,
                    _cached_prediction_taxonomy,
                    _flush_batch,
                    _prepare_image,
                    _publish_classifier_runs_for_raw_results,
                    _record_batch_classifier_runs,
                    _store_grouped_predictions,
                )

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                user_cfg = thread_db.get_effective_config(cfg.load())
                grouping_window = user_cfg.get("grouping_window_seconds", 5)
                similarity_threshold = user_cfg.get("similarity_threshold", 0.85)
                detector_confidence = user_cfg.get("detector_confidence", 0.2)
                pipeline_cfg = user_cfg.get("pipeline", {})
                weak_rescue_enabled = pipeline_cfg.get(
                    "weak_detection_rescue_enabled", True,
                )
                weak_detection_confidence = pipeline_cfg.get(
                    "weak_detection_confidence", 0.12,
                )

                tax = loaded_models["tax"]
                # Fingerprint for the FIRST model is preloaded by model_loader_stage.
                # Each subsequent iteration reloads its own bundle (with its own fp)
                # inside the loop, so we read loaded_models["labels_fingerprint"]
                # per-spec below rather than capturing a single value here.
                resolved_specs_local = loaded_models.get("resolved_specs") or [
                    loaded_models["active_model"]
                ]

                photos = detect_state["photos"]
                folders = detect_state["folders"]
                cached_detections = detect_state["detections"]
                total = len(photos)

                # A low-confidence box is not globally promoted. Only select
                # weak runs bracketed by ordinary detections in one tightly
                # timed sequence. This gives the classifier a chance to
                # validate threshold-cliff frames without making every weak
                # MegaDetector result eligible throughout the library.
                contextual_weak_ids = set()
                if (
                    weak_rescue_enabled
                    and weak_detection_confidence < detector_confidence
                    and photos
                ):
                    from weak_detections import contextual_weak_photo_ids

                    raw_mdv6_detections = thread_db.get_detections_for_photos(
                        [p["id"] for p in photos],
                        min_conf=weak_detection_confidence,
                        detector_model="megadetector-v6",
                    )
                    contextual_weak_ids = contextual_weak_photo_ids(
                        photos,
                        raw_mdv6_detections,
                        detector_confidence=detector_confidence,
                        weak_confidence=weak_detection_confidence,
                        max_gap=pipeline_cfg.get("burst_time_gap", 3.0),
                    )
                    if contextual_weak_ids:
                        log.info(
                            "Classification: rescuing %d contextual "
                            "weak-detection photo(s)",
                            len(contextual_weak_ids),
                        )

                total_predictions_stored = 0
                total_full_image_fallbacks = 0
                total_failed = 0
                total_skipped_existing = 0
                # Track unique photo IDs that failed in any model so the rollup
                # message always produces a valid X-of-N ratio. total_failed sums
                # per-model failures and can exceed total in multi-model runs.
                failed_photo_ids: set = set()
                # Photos we never opened because their containing folder was
                # unreachable at read time. Kept apart from ``failed`` (those
                # ARE actual per-photo decode failures) so the rollup can name
                # unreachable photos honestly, and kept as unique photo IDs so
                # a folder outage that hits the same photos across every model
                # in a multi-model run doesn't multiply-count.
                source_skipped_photo_ids: set = set()

                skipped_model_names: list = []
                models_succeeded = 0
                # Track photo IDs actually processed by the first successful
                # model's classify loop so the stale-detection purge is scoped
                # to reclassified photos only. Using detect_state["processed_ids"]
                # (all detected photos) would incorrectly delete detections for
                # photos that weren't reached if the job was aborted mid-classify.
                first_model_photo_ids: set = set()
                fresh_full_image_ids_by_photo: dict = {}

                # ``reason`` latches only when classify is giving up for good,
                # so the remaining models in a multi-model run skip themselves
                # instead of re-discovering the same dead share. ``pauses``
                # bounds the reconnect ping-pong: a user who resumes without
                # actually fixing the mount gets a few tries, not an infinite
                # pause/retry loop.
                source_offline: dict = {"reason": None, "pauses": 0}

                def _publish_pause_reason(reason, step_id):
                    """Surface the offline reason on transient progress/pause state.

                    Codex #1388 P1 r3663383513: the pause itself was only
                    reported via server-side log; the jobs UI showed a generic
                    ``paused`` state with no indication of *why* classify
                    parked, so users could not tell they needed to remount a
                    named volume and might keep pressing Resume until the
                    bounded retry budget converted a recoverable outage into a
                    failed run. Publish the reason via the progress event
                    (mirrored onto ``job['progress']`` for /api/jobs polls and
                    the SSE stream alike) and set the classify step's
                    ``current_file`` so the reason renders directly beneath
                    the paused pipeline step, right next to the Resume button.
                    Kept off ``errors`` so a successful resume still finalizes
                    as ``completed``.
                    """
                    message = (
                        f"Paused: source {reason}. Reconnect it and press "
                        f"Resume to keep classifying."
                    )
                    if step_id is not None:
                        with contextlib.suppress(Exception):
                            runner.update_step(
                                job["id"], step_id, current_file=message,
                            )
                    _emit_progress(
                        runner, job["id"], stages, "classify",
                        message,
                        step_id=step_id,
                        pause_reason=message,
                    )

                def _clear_pause_reason(step_id):
                    """Drop the pause banner once the user has resumed.

                    Leaving a stale ``pause_reason`` on ``job['progress']``
                    after a successful reconnect would keep the "reconnect
                    and Resume" banner visible while classify happily runs.
                    """
                    if step_id is not None:
                        with contextlib.suppress(Exception):
                            runner.update_step(
                                job["id"], step_id, current_file="",
                            )
                    _emit_progress(
                        runner, job["id"], stages, "classify",
                        "Resumed classification",
                        step_id=step_id,
                        pause_reason=None,
                    )

                def _handle_source_offline(reason, step_id=None):
                    """Park on a dead source; report whether we may continue.

                    Returns True when the run was paused and then resumed, so
                    the caller should keep classifying (the user reconnected
                    the share). Returns False when classify should stop: the
                    runner can't park, the job was cancelled, or we've already
                    paused for this too many times.
                    """
                    if source_offline["pauses"] >= _MAX_SOURCE_OFFLINE_PAUSES:
                        source_offline["reason"] = reason
                        return False
                    source_offline["pauses"] += 1
                    # Don't push a pause message into ``errors``: a successful
                    # reconnect+resume leaves classify completing normally, but
                    # templates/pipeline.html treats every ``[classify]`` error
                    # as a failed stage and suppresses the success redirect
                    # (Codex #1388 P1). ``pause_job`` below already flips the
                    # job state to ``pausing``/``paused`` (that's what the UI
                    # renders while parked), and the give-up path below
                    # appends its own ``[classify] Fatal:`` entry — so a run
                    # that never comes back still surfaces a real terminal
                    # error, and a run that does come back does not.
                    log.warning("Classify paused: source offline (%s)", reason)
                    # Publish BEFORE calling pause_job so the reason is already
                    # in job['progress'] by the time the UI reacts to the
                    # ``pausing`` status flip. Otherwise the banner would only
                    # appear after the next progress event, which for a fully
                    # blocked read might be minutes away.
                    _publish_pause_reason(reason, step_id)

                    pause = getattr(runner, "pause_job", None)
                    paused = False
                    if pause is not None:
                        with contextlib.suppress(Exception):
                            paused = bool(pause(job["id"]))
                    if not paused:
                        # ``pause_job`` returns False in two very different
                        # cases: (a) the job is not pausable at all — a
                        # non-pausable pipeline or a direct
                        # ``run_pipeline_job`` call in a test — genuinely
                        # nothing to park on, and (b) the user already
                        # pressed Pause while this network read was blocked,
                        # so the job's public state is already ``pausing``
                        # and a second pause request is a no-op. Treating (b)
                        # as "can't pause" would waste the user's Pause
                        # click by giving up instead of parking (Codex
                        # #1388 P2 r3663383518); check
                        # ``pause_requested`` and fall through to the
                        # checkpoint so the run parks on the user's
                        # already-in-flight request.
                        pause_probe = getattr(runner, "pause_requested", None)
                        already_pausing = False
                        if pause_probe is not None:
                            with contextlib.suppress(Exception):
                                already_pausing = bool(pause_probe(job["id"]))
                        if not already_pausing:
                            # Nothing to park on. Stop rather than spin
                            # through the rest of the collection
                            # collecting instant EIOs.
                            source_offline["reason"] = reason
                            return False

                    # Blocks here until the user resumes or cancels. Classify
                    # is a registered pause participant, so this is the same
                    # safe boundary the per-photo cancel check already uses.
                    if _pause_checkpoint():
                        source_offline["reason"] = reason
                        return False
                    # Successful resume: drop the stale "reconnect and Resume"
                    # banner before the caller retries the read.
                    _clear_pause_reason(step_id)
                    return True

                from datetime import datetime as dt

                for spec_idx, active_spec in enumerate(resolved_specs_local):
                    step_id = f"classify:{active_spec['id']}"
                    if _should_abort(abort):
                        # Anything not yet touched stays pending; mark it skipped
                        # so the job tree finalizes cleanly.
                        runner.update_step(job["id"], step_id,
                                           status="completed",
                                           summary="Skipped (cancelled)")
                        continue
                    if source_offline["reason"]:
                        # The share died during an earlier model. Every read for
                        # this one would fail the same way, so say so instead of
                        # running up a second identical failure count (the
                        # incident's "0 predictions, 865 failed" second model).
                        runner.update_step(
                            job["id"], step_id, status="completed",
                            summary=(
                                f"Skipped (source {source_offline['reason']})"
                            ),
                        )
                        continue

                    runner.update_step(job["id"], step_id, status="running")

                    if spec_idx == 0 and "clf" in loaded_models:
                        # First model preloaded by model_loader_stage.
                        clf = loaded_models["clf"]
                        model_type = loaded_models["model_type"]
                        model_name = loaded_models["model_name"]
                    else:
                        # Don't reset stages["classify"]["count"] here — it now
                        # accumulates real inferences per-photo (Task 3); jumping
                        # it to spec_idx * total would silently double-count the
                        # cached hits from prior specs.  total is left at its
                        # multi-spec value (set by the batch-end push of the
                        # previous spec, or unchanged on first entry); explicitly
                        # restate it so the "Loading next model..." event always
                        # carries the multi-spec total.
                        stages["classify"]["total"] = total * len(resolved_specs_local)
                        _emit_progress(
                            runner, job["id"], stages, "classify", f"Loading {active_spec['name']}...",
                            step_id=step_id,
                        )
                        # Drop the prior model's per-photo payload BEFORE loading
                        # the next bundle so we don't hold old results + new model
                        # weights concurrently. Without this, multi-model runs on
                        # large collections can hit transient OOMs.
                        with contextlib.suppress(NameError, UnboundLocalError):
                            raw_results.clear()  # noqa: F821 — bound in prior iter
                        # Release the previous spec's cache handle so its
                        # refcount drops and a same-cache-key reload below (or
                        # in another pipeline) can be a hit. Must happen
                        # BEFORE popping ``clf`` so we don't lose the only
                        # reference to the bundle that owns the handle.
                        _release_classifier_cache_handle(loaded_models)
                        for k in ("clf", "model_type", "model_name", "model_str",
                                  "labels", "label_source", "use_tol",
                                  "active_model"):
                            loaded_models.pop(k, None)
                        clf = None
                        try:
                            bundle = _load_model_bundle(active_spec, tax, thread_db, progress_step=step_id)
                        except Exception as model_err:
                            is_classification_cancelled = (
                                model_err.__class__.__name__ == "ClassificationCancelled"
                            )
                            if (
                                _should_abort(abort)
                                or _cancellation_requested()
                                or is_classification_cancelled
                                or str(model_err) == "classification cancelled"
                            ):
                                abort.set()
                                runner.update_step(
                                    job["id"], step_id,
                                    status="completed",
                                    summary="Skipped (cancelled)",
                                )
                                completed_step_ids.add(step_id)
                                continue
                            log.warning(
                                "Skipping model %s: %s",
                                active_spec["name"], model_err,
                            )
                            skipped_model_names.append(active_spec["name"])
                            runner.update_step(
                                job["id"], step_id,
                                status="failed",
                                error=str(model_err),
                                summary=f"Failed to load: {model_err}",
                            )
                            failed_step_ids.add(step_id)
                            continue
                        loaded_models.update(bundle)
                        clf = bundle["clf"]
                        model_type = bundle["model_type"]
                        model_name = bundle["model_name"]

                    # Name the label space on the row. Set for both branches
                    # here: the first model's bundle was built by
                    # model_loader_stage and merged into loaded_models there.
                    if loaded_models.get("label_source"):
                        runner.update_step(
                            job["id"], step_id,
                            label_source=loaded_models["label_source"],
                        )

                    # The fingerprint for THIS model's label set — pinned by
                    # model_loader_stage for the first model and by _load_model_bundle
                    # for subsequent ones. Used to key the classifier_runs gate so
                    # a repeat pass over the same (detection, model, fingerprint)
                    # skips work instead of re-running inference. Hoisted above
                    # the reclassify clear so the clear can scope by fingerprint.
                    spec_fp = loaded_models.get("labels_fingerprint", "legacy")

                    # The reclassify clear (wipes prior predictions for this
                    # model+fingerprint) is intentionally deferred to just before
                    # _store_grouped_predictions below — clearing here and then
                    # cancelling mid-classify would leave the predictions table
                    # empty for this model with no replacement, erasing the
                    # user's prior classifications instead of preserving them.

                    # No photo-level short-circuit: it would hide detections
                    # that newly cross the workspace's detector_confidence
                    # threshold on photos that already had a cached prediction
                    # for some other detection. The per-detection
                    # classifier_runs gate below handles skipping correctly
                    # and still surfaces cached results into raw_results so
                    # grouping sees them.
                    # Pre-flight cache estimate. One indexed query so the UI
                    # can display "~M cached, ~K to classify" before the first
                    # inference runs and ETAs are honest from the start. The
                    # estimate may overcount if a run key exists but no cached
                    # predictions do (e.g. a prior pass wrote
                    # ``category == 'match'`` with no predictions); the live
                    # ``cached`` counter reflects actual skips, and
                    # ``photos_cache_overcounted_in_spec`` (populated in the
                    # fall-through branch below) lets ``_classification_eta_progress``
                    # reconcile the estimate from observed misses so
                    # ``remaining_uncached`` doesn't collapse to zero on
                    # collections dominated by these rows.
                    #
                    # We keep the full preflight-cached photo id set (not
                    # just its count) so overcount attribution can be
                    # scoped: recording an overcount for a photo the
                    # preflight never counted would deflate the projected
                    # cache-hit rate for unrelated photos and prematurely
                    # inflate ``remaining_uncached`` (Codex #1468 P2).
                    # Contextual-weak photos use the lower
                    # ``weak_detection_confidence`` floor to match the
                    # runtime cache-hit predicate; otherwise the cached
                    # weak tail is omitted from ``cached_est`` and the
                    # ETA overstates remaining time (Codex #1468 P2).
                    #
                    # Skipped on reclassify runs because the cache gate below
                    # is bypassed and every photo is re-inferred. In a
                    # multi-model run each model owns its own Jobs step, so
                    # accumulate the per-spec estimates for the stage while
                    # retaining this spec's value for its ETA.
                    cached_est = 0
                    preflight_cached_ids: set = set()
                    # Photos whose runtime path deterministically skips
                    # inference because they have no eligible animal target
                    # but do have a confident non-animal box. Without
                    # subtracting them, a person/vehicle tail inflates the ETA
                    # even though runtime traverses it without model work.
                    #
                    # Whenever detection ran, pass its in-memory map so both
                    # this skip estimate and the cache estimate below use the
                    # exact candidates the classify loop will consume. Rows
                    # from another detector model can remain in the DB after
                    # either a reclassify or an ordinary runtime-fingerprint
                    # miss; letting those stale rows into preflight can invent
                    # work or hide a fresh cache hit (Codex #1468 P2).
                    preflight_unclassifiable_ids: set = (
                        thread_db.get_unclassifiable_photos(
                            [p["id"] for p in photos],
                            contextual_weak_photo_ids=contextual_weak_ids,
                            weak_confidence=(
                                weak_detection_confidence
                                if contextual_weak_ids
                                else None
                            ),
                            fresh_detections_by_photo=(
                                detect_state["detections"]
                                if detect_state.get("ran")
                                else None
                            ),
                            # ``processed_ids`` is what ``_detect_batch``
                            # actually completed this run. Photos absent
                            # from it are ones the detector raised on and
                            # runtime will DB-fallback for (see
                            # ``photo_dets`` else branch below), so they
                            # must be evaluated against DB rows here too
                            # instead of treated as "no fresh animal"
                            # (Codex #1468 P2).
                            fresh_processed_photo_ids=(
                                detect_state["processed_ids"]
                                if detect_state.get("ran")
                                else None
                            ),
                        )
                    )
                    if not params.reclassify:
                        # Precompute the classifier runtime_fingerprint the
                        # runtime gate will accept for each distinct
                        # ``detections.runtime_fingerprint`` present in this
                        # batch. Passing this map lets the preflight reject
                        # obsolete-runtime classifier_runs rows the same way
                        # the per-detection ``get_classifier_run_key_gate``
                        # does at runtime; without it, the preflight
                        # overcounts rows whose runtime rolled since the
                        # prior classify pass, and no observation can correct
                        # the estimate until those rows are visited — so the
                        # UI reads "finishing…" while inference is still
                        # pending (Codex #1468 P2).
                        preflight_expected_rt_map = None
                        portable_labels_full = loaded_models.get(
                            "labels_fingerprint_full"
                        )
                        portable_model_identity = loaded_models.get(
                            "classifier_model_identity"
                        )
                        portable_tax_identity = loaded_models.get(
                            "taxonomy_identity", "no-tax",
                        )
                        if portable_labels_full and portable_model_identity:
                            from computation_cache import (
                                classifier_runtime_fingerprint,
                            )
                            detector_runtimes: set = set()
                            photo_id_list = [p["id"] for p in photos]
                            det_chunk = 500
                            for i in range(0, len(photo_id_list), det_chunk):
                                chunk = photo_id_list[i:i + det_chunk]
                                placeholders = ",".join("?" * len(chunk))
                                rows = thread_db.conn.execute(
                                    f"SELECT DISTINCT runtime_fingerprint "
                                    f"FROM detections "
                                    f"WHERE photo_id IN ({placeholders})",
                                    chunk,
                                ).fetchall()
                                for row in rows:
                                    detector_runtimes.add(
                                        row["runtime_fingerprint"]
                                    )
                            preflight_expected_rt_map = {}
                            for det_rt in detector_runtimes:
                                if det_rt is None:
                                    # Detector runtime not recorded — matches
                                    # ``classifier_runtime_for_detection``'s
                                    # None return; permissive entry preserves
                                    # the unfiltered fallback.
                                    preflight_expected_rt_map[det_rt] = None
                                else:
                                    preflight_expected_rt_map[det_rt] = (
                                        classifier_runtime_fingerprint(
                                            portable_model_identity,
                                            portable_labels_full,
                                            det_rt,
                                            taxonomy_identity=(
                                                portable_tax_identity
                                            ),
                                        )
                                    )
                        preflight_cached_ids = (
                            thread_db.get_classifier_run_cache_hits(
                                [p["id"] for p in photos],
                                model_name,
                                spec_fp,
                                contextual_weak_photo_ids=(
                                    contextual_weak_ids
                                ),
                                weak_confidence=(
                                    weak_detection_confidence
                                    if contextual_weak_ids
                                    else None
                                ),
                                fresh_detections_by_photo=(
                                    detect_state["detections"]
                                    if detect_state.get("ran")
                                    else None
                                ),
                                fresh_processed_photo_ids=(
                                    detect_state["processed_ids"]
                                    if detect_state.get("ran")
                                    else None
                                ),
                                expected_classifier_runtime_by_detector_runtime=(
                                    preflight_expected_rt_map
                                ),
                            )
                        )
                        cached_est = len(preflight_cached_ids)
                        stages["classify"]["cached_estimate"] = (
                            stages["classify"].get("cached_estimate", 0) + cached_est
                        )
                    # Set total BEFORE the pre-flight event so the UI's
                    # ``stageTotal - stageCachedEst`` subtraction renders the
                    # real "to classify" count on the first event the user
                    # sees, not 0.
                    stages["classify"]["total"] = total * len(resolved_specs_local)
                    _emit_progress(
                        runner, job["id"], stages, "classify",
                        f"Classifying with {active_spec['name']}",
                        step_id=step_id,
                    )
                    raw_results: list = []
                    failed = 0
                    # Photos we never got to look at because the source volume
                    # was offline. Kept apart from ``failed`` so the summary
                    # never reports an unreachable share as broken photos.
                    source_skipped = 0
                    skipped_existing = 0
                    full_image_fallbacks = 0
                    stages["classify"].setdefault("cached", 0)
                    stages["classify"].setdefault("seen", 0)
                    # Photo-scoped bookkeeping for ``count`` (inferred) and
                    # ``cached``. ``total`` and ``cached_estimate`` count PHOTOS
                    # (× specs), so multi-subject photos with several qualifying
                    # detections must not each add multiple ticks to ``count`` /
                    # ``cached`` — otherwise the UI's ``inferred · cached /
                    # total`` line can read ``2 inferred / 1`` on a two-subject
                    # photo, and the cached-preflight would understate remaining
                    # work when only one of the photo's detections is cached.
                    # A photo lands in ``photos_cached_in_spec`` on its first
                    # cache-hit detection and is promoted to
                    # ``photos_inferred_in_spec`` (decrementing ``cached``,
                    # incrementing ``count``) the moment any of its detections
                    # actually runs inference. Reset per spec so a photo can be
                    # counted once per (photo × spec) — matching ``total``.
                    photos_cached_in_spec: set = set()
                    photos_inferred_in_spec: set = set()
                    # Includes failed inference attempts as well as successful
                    # ones. ETA throughput is about completed model work, not
                    # only predictions that happened to persist successfully.
                    photos_attempted_in_spec: set = set()
                    # Preflight's ``cached_est`` counts any photo whose
                    # qualifying detections all have classifier_runs rows,
                    # but the runtime cache-hit predicate additionally
                    # requires actual predictions to exist. When we hit the
                    # fall-through case (run key present, predictions
                    # missing — see the classify loop below) the photo will
                    # end up in ``photos_inferred_in_spec``, so subtract it
                    # from the preflight estimate in the ETA calculation
                    # instead of leaving a phantom future cache hit that
                    # collapses ``remaining_uncached`` to zero.
                    photos_cache_overcounted_in_spec: set = set()
                    # Photos observed to hit the confident-non-animal skip
                    # branch this spec. Paired with
                    # ``preflight_unclassifiable_ids`` so the ETA can
                    # subtract the unvisited share from remaining work
                    # instead of treating a fast tail as inference work
                    # (Codex #1468 P2).
                    photos_unclassifiable_in_spec: set = set()
                    # Per-spec tracking of photos whose per-photo iteration
                    # was actually entered in THIS spec. Used by the
                    # reclassify clear below so it never wipes predictions
                    # for photos this spec never opened — on a mount-scoped
                    # give-up we break out of the batch loop before reaching
                    # later photos, and on a folder-scoped outage every photo
                    # under the missing folder is skipped without ever
                    # producing a replacement prediction. Without this,
                    # ``clear_predictions`` on a ``reclassify=True`` run
                    # would delete the prior predictions for those photos
                    # even though nothing in this run had a chance to
                    # rewrite them (Codex #1388 P1).
                    spec_reached_photo_ids: set = set()
                    # Per-spec source skips, scoped to just THIS spec's
                    # reads. Codex #1388 P2 (r3663642360): if the folder
                    # was unreachable for model A but returned before
                    # model B, the aggregate ``source_skipped_photo_ids``
                    # would still exclude that photo from model B's
                    # reclassify clear — leaving model B's stale
                    # prediction in place. Because ``add_prediction``
                    # uses ``INSERT OR IGNORE``, that stale row can win
                    # over the fresh result and retain the wrong
                    # species/confidence. Track skips per spec for the
                    # clear; the aggregate set below still drives the
                    # outage rollup at end-of-run.
                    spec_source_skipped_photo_ids: set = set()
                    # Photos that iterated past the inner abort check IN THIS spec.
                    # Used for the per-spec ``runner.update_step`` progress (which
                    # is bounded by ``total``, not the multi-spec stage total) and
                    # for the batch-end rate calc.  Captures every branch — cache
                    # hit, successful inference, no-detection, image decode fail,
                    # inference fail — so spec-level progress reaches ``total`` at
                    # the end of the spec regardless of outcome mix.
                    processed_in_spec = 0
                    start_time = time.time()
                    # Wall time since ``start_time`` includes cache-lookup and
                    # result-building for the cache-hit prefix, which can dwarf
                    # actual model work on cache-heavy collections. Dividing
                    # ``inference_attempts`` by that walltime yields an
                    # artificially low rate and inflates the ETA for the
                    # uncached tail. Accumulate the seconds spent inside
                    # ``_flush_batch`` here so the ETA rate reflects
                    # inference throughput only (Codex #1468 P2).
                    inference_seconds = 0.0
                    batch_size = 32  # classification batch granularity
                    inference_batch_size = _BATCH_SIZE
                    inference_batch: list = []
                    has_flushed_in_spec = False

                    def _close_pending_inference(inference_batch=inference_batch):
                        for entry in inference_batch:
                            with contextlib.suppress(Exception):
                                entry["img"].close()
                        inference_batch.clear()

                    def _flush_pending_inference(
                        inference_batch=inference_batch,
                        raw_results=raw_results,
                        clf=clf,
                        model_type=model_type,
                        model_name=model_name,
                        spec_fp=spec_fp,
                        photos_cached_in_spec=photos_cached_in_spec,
                        photos_inferred_in_spec=photos_inferred_in_spec,
                        photos_attempted_in_spec=photos_attempted_in_spec,
                        photos_cache_overcounted_in_spec=(
                            photos_cache_overcounted_in_spec
                        ),
                    ):
                        nonlocal failed, has_flushed_in_spec, inference_seconds
                        if not inference_batch:
                            return

                        pending = list(inference_batch)
                        inference_batch.clear()
                        has_flushed_in_spec = True
                        attempted_photo_ids = {
                            entry["photo"]["id"] for entry in pending
                        }
                        photos_attempted_in_spec.update(attempted_photo_ids)
                        removed_cache_hits = _remove_attempted_cache_hits(
                            attempted_photo_ids,
                            photos_cached_in_spec,
                        )
                        if removed_cache_hits:
                            stages["classify"]["cached"] = max(
                                0,
                                stages["classify"].get("cached", 0)
                                - removed_cache_hits,
                            )
                        pre_len = len(raw_results)
                        # GPU serialisation lives inside _flush_batch around the
                        # inference call so the DB upserts/result-building afterward
                        # don't hold the semaphore while the GPU is idle.
                        _flush_started = time.time()
                        n_batch_failed = _flush_batch(
                            pending, clf, model_type, model_name,
                            thread_db, raw_results,
                        )
                        inference_seconds += max(
                            time.time() - _flush_started, 0.0,
                        )
                        failed += n_batch_failed

                        successful_det_ids = {
                            r.get("detection_id") for r in raw_results[pre_len:]
                        }
                        if n_batch_failed:
                            for entry in pending:
                                if entry.get("detection_id") not in successful_det_ids:
                                    failed_photo_ids.add(entry["photo"]["id"])

                        _record_batch_classifier_runs(
                            thread_db, pending, model_name, spec_fp, raw_results,
                            pre_len,
                            labels_fingerprint_full=loaded_models.get(
                                "labels_fingerprint_full"
                            ),
                            model_identity=loaded_models.get(
                                "classifier_model_identity"
                            ),
                        )

                        # Photo-scoped ``count`` bookkeeping: each distinct
                        # photo whose flush yielded at least one successful
                        # classification counts once. If it was previously
                        # bucketed as fully-cached (an earlier detection hit
                        # the cache), migrate it — decrement ``cached`` and
                        # add it to ``count`` — so ``count + cached`` stays
                        # bounded by the (photo-scoped) ``total``.
                        new_photo_ids = {
                            r["photo"]["id"] for r in raw_results[pre_len:]
                        }
                        promoted = new_photo_ids & photos_cached_in_spec
                        if promoted:
                            photos_cached_in_spec.difference_update(promoted)
                            stages["classify"]["cached"] = max(
                                0,
                                stages["classify"].get("cached", 0)
                                - len(promoted),
                            )
                        newly_inferred = new_photo_ids - photos_inferred_in_spec
                        if newly_inferred:
                            photos_inferred_in_spec.update(newly_inferred)
                            stages["classify"]["count"] = (
                                stages["classify"].get("count", 0)
                                + len(newly_inferred)
                            )

                    for batch_start in range(0, total, batch_size):
                        if _should_abort(abort) or source_offline["reason"]:
                            break
                        batch = photos[batch_start:batch_start + batch_size]

                        for photo in batch:
                            # Per-photo abort check so cancel takes effect within
                            # one inference (~seconds) instead of waiting for the
                            # next batch boundary (~32 photos). The outer batch
                            # loop's check at the top of the next iteration will
                            # then break out of the batch loop entirely.
                            # ``source_offline`` rides the same boundary: once
                            # the share is gone there is nothing left to read,
                            # so stop pulling photos rather than spending the
                            # rest of the batch collecting instant EIOs.
                            if _should_abort(abort) or source_offline["reason"]:
                                break
                            processed_in_spec += 1
                            stages["classify"]["seen"] = (
                                stages["classify"].get("seen", 0) + 1
                            )
                            # Photo entered THIS spec's per-photo body; scopes
                            # the reclassify clear below so unreached photos
                            # keep their prior predictions.
                            spec_reached_photo_ids.add(photo["id"])
                            # Record this photo as classify-processed for the first
                            # successful model. Used by the stale-detection purge to
                            # restrict deletions to photos actually reclassified.
                            if models_succeeded == 0:
                                first_model_photo_ids.add(photo["id"])

                            # Pull every qualifying detection for this photo from the
                            # detect-stage cache. Fall back to db.get_detections()
                            # only for photos whose per-photo detect iteration
                            # never completed (e.g. mid-batch exception, or the
                            # detect stage was skipped for an already-detected
                            # non-reclassify run — in which case the DB holds the
                            # authoritative rows). If MegaDetector produced no
                            # real rows at all, synthesize a full-image anchor so
                            # classifiers still get one attempt and future reruns
                            # can hit classifier_runs for that attempt.
                            full_image_fallback = False
                            is_contextual_weak = (
                                photo["id"] in contextual_weak_ids
                            )
                            detection_floor = (
                                weak_detection_confidence
                                if is_contextual_weak
                                else detector_confidence
                            )
                            if photo["id"] in cached_detections:
                                # cached_detections from _detect_batch can include
                                # full-image rows when an earlier pass synthesized
                                # them (legacy db state); filter to match the
                                # fallback-query branch below so classifiers only
                                # see real, qualifying animal boxes. _detect_batch's
                                # fresh cache contains raw low-confidence boxes too,
                                # while DB reads normally apply this threshold.
                                photo_dets = _cached_classify_detections(
                                    cached_detections[photo["id"]],
                                    detection_floor,
                                    contextual_weak=is_contextual_weak,
                                )
                            else:
                                photo_dets = [
                                    {
                                        "id": d["id"],
                                        "box_x": d["box_x"],
                                        "box_y": d["box_y"],
                                        "box_w": d["box_w"],
                                        "box_h": d["box_h"],
                                        "confidence": d["detector_confidence"],
                                        "category": d["category"],
                                    }
                                    for d in thread_db.get_detections(
                                        photo["id"], min_conf=detection_floor,
                                        # Contextual rescue is defined only
                                        # from MegaDetector V6 evidence. Keep
                                        # the detector-failure DB fallback on
                                        # that same candidate set so a stale,
                                        # higher-confidence foreign row cannot
                                        # diverge from the cache preflight's
                                        # selected crop (Codex #1468 P2).
                                        detector_model=(
                                            "megadetector-v6"
                                            if is_contextual_weak
                                            else None
                                        ),
                                    )
                                    if d["detector_model"] != "full-image"
                                    and d["category"] == "animal"
                                ]
                            if is_contextual_weak and not photo_dets:
                                # detect_state intentionally caches only rows
                                # passing the ordinary workspace threshold on
                                # reuse runs. Recover the raw weak row from the
                                # database for this explicitly selected bridge.
                                photo_dets = [
                                    {
                                        "id": d["id"],
                                        "box_x": d["box_x"],
                                        "box_y": d["box_y"],
                                        "box_w": d["box_w"],
                                        "box_h": d["box_h"],
                                        "confidence": d["detector_confidence"],
                                        "category": d["category"],
                                    }
                                    for d in thread_db.get_detections(
                                        photo["id"],
                                        min_conf=weak_detection_confidence,
                                        detector_model="megadetector-v6",
                                    )
                                    if d["category"] == "animal"
                                ]
                            if is_contextual_weak and photo_dets:
                                # One best weak crop is enough to validate the
                                # bridge. Classifying every low-confidence box
                                # would multiply work and false-positive risk.
                                #
                                # Explicit ``detector_confidence DESC, id ASC``
                                # tie-break so the runtime picks the same
                                # detection the preflight's cache-hit query
                                # picks. ``cached_detections`` comes back from
                                # ``_detect_batch`` in raw detector/NMS output
                                # order — no ID tie-break — while
                                # ``get_classifier_run_cache_hits`` ranks by
                                # ``detector_confidence DESC, id ASC``. Without
                                # this sort, two equal-confidence weak boxes
                                # can leave runtime inferring the higher-ID box
                                # while the preflight marked the photo cached
                                # via a portable-cache run on the lower-ID box;
                                # the overcount tracker then can't correct it
                                # because the runtime-selected detection has
                                # no run key of its own (Codex #1468 P2).
                                photo_dets = sorted(
                                    photo_dets,
                                    key=lambda d: (
                                        -float(
                                            d.get(
                                                "confidence",
                                                d.get("detector_confidence", 0),
                                            ) or 0
                                        ),
                                        d.get("id", 0),
                                    ),
                                )[:1]
                            detections_to_classify = photo_dets
                            if not detections_to_classify:
                                # No animal box is usable at either the
                                # ordinary threshold or the contextual
                                # weak-rescue floor. Treat this the same as a
                                # true no-detection photo and give the
                                # classifier a full-image attempt. Raw
                                # detector output is retained for
                                # diagnostics/cache reuse, but a 1% noise box
                                # must not suppress classification of an
                                # otherwise visible subject.
                                #
                                # Exception: MegaDetector emitted a confident
                                # non-animal (person/vehicle) box at or above
                                # ``detector_confidence``. Sending the entire
                                # human/vehicle frame to the wildlife
                                # classifier would persist a spurious species
                                # prediction. Skip the fallback in that case
                                # and leave the photo without a classifier
                                # run, matching the old raw-detection guard's
                                # intent while still rescuing sub-threshold
                                # animal photos.
                                confident_non_animal = False
                                if photo["id"] in cached_detections:
                                    confident_non_animal = any(
                                        d.get("detector_model") != "full-image"
                                        and d.get("category", "animal") != "animal"
                                        and d.get(
                                            "confidence",
                                            d.get("detector_confidence", 0),
                                        ) >= detector_confidence
                                        for d in cached_detections[photo["id"]]
                                    )
                                else:
                                    confident_non_animal = any(
                                        d["detector_model"] != "full-image"
                                        and d["category"] != "animal"
                                        for d in thread_db.get_detections(
                                            photo["id"],
                                            min_conf=detector_confidence,
                                        )
                                    )
                                if confident_non_animal:
                                    # This is the only no-target path that
                                    # skips inference. Track it so the ETA
                                    # does not project confident person/
                                    # vehicle tails as pending model work.
                                    photos_unclassifiable_in_spec.add(
                                        photo["id"],
                                    )
                                    continue

                                existing_full = thread_db.get_detections(
                                    photo["id"],
                                    detector_model="full-image",
                                    min_conf=0,
                                )
                                if existing_full and not params.reclassify:
                                    full_det_id = existing_full[0]["id"]
                                else:
                                    from computation_cache import (
                                        full_image_runtime_fingerprint,
                                        source_input,
                                    )

                                    full_runtime = full_image_runtime_fingerprint()
                                    identity = thread_db.conn.execute(
                                        """SELECT file_hash, companion_path
                                           FROM photos WHERE id = ?""",
                                        (photo["id"],),
                                    ).fetchone()
                                    full_input = None
                                    if identity is not None and not identity["companion_path"]:
                                        try:
                                            _block, full_input = source_input(
                                                identity["file_hash"],
                                                "vireo-detector-source-v1",
                                            )
                                        except ValueError:
                                            full_input = None
                                    # Combine save_detections + record_detector_run
                                    # into one transaction via write_detection_batch
                                    # so a crash between them can't leave a
                                    # full-image detection row without its matching
                                    # detector_runs row — mirroring the invariant
                                    # documented for write_detection_batch.
                                    full_det_ids = thread_db.write_detection_batch(
                                        photo["id"], "full-image",
                                        [{
                                            "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                            "confidence": 0,
                                            "category": "animal",
                                        }],
                                        runtime_fingerprint=full_runtime,
                                        input_fingerprint=full_input,
                                    )
                                    full_det_id = full_det_ids[0]
                                detections_to_classify = [{
                                    "id": full_det_id,
                                    "box_x": 0,
                                    "box_y": 0,
                                    "box_w": 1,
                                    "box_h": 1,
                                    "confidence": 0,
                                    "category": "animal",
                                    "detector_model": "full-image",
                                }]
                                full_image_fallback = True
                                full_image_fallbacks += 1
                                fresh_full_image_ids_by_photo.setdefault(
                                    photo["id"], set(),
                                ).add(full_det_id)

                            for detection in detections_to_classify:
                                # Classifier-run gate: skip work when this exact
                                # (detection, classifier_model, labels_fingerprint)
                                # triple was already classified. Reclassify bypasses
                                # the gate so users can force a fresh pass. When
                                # gated, surface the cached top-1 prediction into
                                # raw_results so downstream grouping/storage sees
                                # it — otherwise the cached detection would silently
                                # drop out of the grouping pipeline.
                                if not params.reclassify:
                                    expected_classifier_runtime = None
                                    portable_labels_full = loaded_models.get(
                                        "labels_fingerprint_full"
                                    )
                                    portable_model_identity = loaded_models.get(
                                        "classifier_model_identity"
                                    )
                                    portable_tax_identity = loaded_models.get(
                                        "taxonomy_identity", "no-tax",
                                    )
                                    if portable_labels_full and portable_model_identity:
                                        from computation_cache import (
                                            classifier_runtime_for_detection,
                                        )

                                        expected_classifier_runtime = (
                                            classifier_runtime_for_detection(
                                                thread_db,
                                                detection["id"],
                                                portable_model_identity,
                                                portable_labels_full,
                                                taxonomy_identity=(
                                                    portable_tax_identity
                                                ),
                                            )
                                        )
                                    # Fetch both accepted and gate-rejected
                                    # keys in one query. ``rejected_keys`` is
                                    # the "existed but the runtime_fingerprint
                                    # rule rejected it" set — the preflight
                                    # ``count_classifier_runs`` ignores that
                                    # rule, so without recording these as
                                    # overcounts below, a photo whose only key
                                    # is gate-rejected sits in ``cached_est``
                                    # forever and ETA prematurely reads
                                    # "finishing…" on runs where the detector
                                    # fingerprint has rolled since the prior
                                    # classifier pass (Codex #1468 P2).
                                    if expected_classifier_runtime is not None:
                                        run_keys, rejected_keys = (
                                            thread_db
                                            .get_classifier_run_key_gate(
                                                detection["id"],
                                                expected_classifier_runtime,
                                            )
                                        )
                                    else:
                                        # No expected runtime fingerprint means
                                        # portable identity isn't wired up
                                        # (legacy path); fall back to the
                                        # unfiltered gate. Nothing to reconcile
                                        # because the preflight and the gate
                                        # then agree on which rows count.
                                        run_keys = (
                                            thread_db.get_classifier_run_keys(
                                                detection["id"],
                                            )
                                        )
                                        rejected_keys = set()
                                    if (model_name, spec_fp) in run_keys:
                                        cached = thread_db.get_predictions_for_detection(
                                            detection["id"],
                                            classifier_model=model_name,
                                            labels_fingerprint=spec_fp,
                                            min_classifier_conf=0,
                                        )
                                        if cached:
                                            skipped_existing += 1
                                            # Photo-scoped ``cached`` bucket:
                                            # only the FIRST cached detection
                                            # per photo (per spec) ticks the
                                            # counter. A subsequent inferred
                                            # detection on the same photo will
                                            # promote it into ``count`` in the
                                            # flush path above.
                                            if _record_unattempted_cache_hit(
                                                photo["id"],
                                                photos_inferred_in_spec,
                                                photos_attempted_in_spec,
                                                photos_cached_in_spec,
                                            ):
                                                stages["classify"]["cached"] += 1
                                            top = cached[0]
                                            folder_path = folders.get(photo["folder_id"], "")
                                            image_path = os.path.join(
                                                folder_path, photo["filename"],
                                            )
                                            timestamp = None
                                            if photo["timestamp"]:
                                                with contextlib.suppress(ValueError, TypeError):
                                                    timestamp = dt.fromisoformat(
                                                        photo["timestamp"]
                                                    )
                                            embedding = None
                                            if model_type != "timm":
                                                # Prefer the per-detection
                                                # embedding so multi-subject
                                                # cache reruns don't reuse a
                                                # single last-wins photo-level
                                                # vector for every detection.
                                                # Fall back to the photo-level
                                                # entry only when the photo has
                                                # a single qualifying detection
                                                # — there the photo-level row
                                                # unambiguously belongs to it,
                                                # so legacy data (classified
                                                # before per-detection variants
                                                # were written) still refines
                                                # correctly.
                                                emb_blob = thread_db.get_photo_embedding(
                                                    photo["id"], model_name,
                                                    variant=f"det:{detection['id']}",
                                                )
                                                if not emb_blob and len(
                                                    detections_to_classify
                                                ) == 1:
                                                    emb_blob = thread_db.get_photo_embedding(
                                                        photo["id"], model_name,
                                                    )
                                                if emb_blob:
                                                    embedding = np.frombuffer(
                                                        emb_blob, dtype=np.float32,
                                                    )
                                            raw_results.append({
                                                "photo": photo,
                                                "detection_id": detection["id"],
                                                "folder_path": folder_path,
                                                "image_path": image_path,
                                                "prediction": top["species"],
                                                "confidence": top["confidence"],
                                                "timestamp": timestamp,
                                                "filename": photo["filename"],
                                                "embedding": embedding,
                                                "taxonomy": _cached_prediction_taxonomy(top),
                                                "_existing": True,
                                            })
                                            continue
                                        # Run key with no cached rows (e.g.
                                        # prior pass stored `category == 'match'`
                                        # so the prediction was intentionally not
                                        # written). Fall through to re-classify
                                        # instead of stranding the detection.
                                        # Reconcile the preflight's cache
                                        # estimate: it counted this photo based
                                        # solely on the run key existing, but
                                        # runtime will actually infer. Only
                                        # record the overcount when the
                                        # preflight actually counted this
                                        # photo — otherwise a multi-detection
                                        # photo whose other detection lacked
                                        # any run key was never in
                                        # ``cached_est`` to begin with, and
                                        # treating this fall-through as a
                                        # failed preflight prediction would
                                        # deflate the projection for
                                        # unrelated cache-heavy tails
                                        # (Codex #1468 P2).
                                        if (
                                            photo["id"] in preflight_cached_ids
                                        ):
                                            photos_cache_overcounted_in_spec.add(
                                                photo["id"],
                                            )
                                    elif (
                                        model_name, spec_fp,
                                    ) in rejected_keys:
                                        # A classifier_runs row exists but its
                                        # ``runtime_fingerprint`` no longer
                                        # matches (typically the detector was
                                        # re-run since the prior classify).
                                        # Preflight counted this photo as
                                        # cached; runtime will infer. Record
                                        # the overcount so
                                        # ``_classification_eta_progress`` can
                                        # deflate its projected future cache
                                        # hits — otherwise a collection made
                                        # entirely of these rows sees
                                        # ``remaining_uncached`` collapse to
                                        # zero after the first uncached batch
                                        # and the UI reports "finishing…"
                                        # while most photos still need
                                        # inference (Codex #1468 P2). Same
                                        # preflight-membership guard: an
                                        # unrelated detection without any
                                        # run key on this photo means the
                                        # preflight never counted it, so
                                        # this branch isn't a preflight
                                        # miss to reconcile.
                                        if (
                                            photo["id"] in preflight_cached_ids
                                        ):
                                            photos_cache_overcounted_in_spec.add(
                                                photo["id"],
                                            )

                                # Track image preparation time separately from
                                # the offline pause loop below so it can be
                                # folded into ``inference_seconds`` only when
                                # the photo actually enters the inference
                                # batch. Cache-hit and image-decode-fail paths
                                # exit the loop above without contributing to
                                # ``inference_attempts``, so their prep time
                                # (if any) must not count toward the rate;
                                # a photo whose retries eventually succeed
                                # DOES count, so accumulate across every
                                # ``_prepare_image`` call for this detection
                                # (Codex #1468 P2).
                                _det_prep_seconds = 0.0
                                _prep_started = time.time()
                                img, folder_path, image_path = _prepare_image(
                                    photo, folders,
                                    None if full_image_fallback else detection,
                                )
                                _det_prep_seconds += max(
                                    time.time() - _prep_started, 0.0,
                                )
                                # Before blaming the photo, check whether the
                                # source itself vanished. A dropped share
                                # fails every remaining read instantly, so
                                # counting these as per-photo failures would
                                # report the untouched remainder of the
                                # collection as broken images.
                                #
                                # Loop rather than one-shot pause+retry so a
                                # mount that stays dead across a resume also
                                # gets latched when this happens to be the
                                # last photo (or last detection) needing an
                                # image read: silently continuing on the
                                # failed retry would leave source_offline
                                # ["reason"] unset, abort clear, and
                                # extract_masks / eye_keypoints would still
                                # walk every detected photo reissuing reads
                                # against the dead share (Codex #1388 P1
                                # r3663278142). _handle_source_offline
                                # bounds the total pause/retry cycles via
                                # _MAX_SOURCE_OFFLINE_PAUSES, so this
                                # can't loop forever.
                                gave_up_on_source = False
                                while img is None:
                                    offline = _source_offline_reason(
                                        folder_path, image_path,
                                    )
                                    if offline is None:
                                        # The source is reachable; this one
                                        # file is genuinely broken.
                                        failed += 1
                                        failed_photo_ids.add(photo["id"])
                                        break
                                    scope, reason = offline
                                    if scope == "folder":
                                        # A single missing folder is not
                                        # evidence the whole source is
                                        # offline; skip this photo as
                                        # unreachable and let later photos
                                        # in healthy folders keep processing.
                                        # Guard the counter with the per-spec
                                        # skipped-photo set so a multi-subject
                                        # photo (N qualifying detections)
                                        # counts once, not N times — the
                                        # per-spec ``total`` and step summary
                                        # are photo-scoped, and without this
                                        # the row could report e.g. ``3
                                        # unreachable`` out of ``1`` photo
                                        # (Codex #1388 P2 r3664348763).
                                        if (
                                            photo["id"]
                                            not in spec_source_skipped_photo_ids
                                        ):
                                            source_skipped += 1
                                        source_skipped_photo_ids.add(
                                            photo["id"]
                                        )
                                        spec_source_skipped_photo_ids.add(
                                            photo["id"]
                                        )
                                        break
                                    # Mount-scoped: park and wait for the
                                    # user to reconnect. On give-up,
                                    # _handle_source_offline latches
                                    # source_offline["reason"] and returns
                                    # False — we then break the per-photo
                                    # loop so remaining photos short-circuit
                                    # the same way (and the finalization
                                    # rollup sets abort so downstream stages
                                    # skip the dead source).
                                    if not _handle_source_offline(
                                        reason, step_id=step_id,
                                    ):
                                        # Give-up: image never loaded, so
                                        # this photo is unreached — same
                                        # bucket as the folder-scoped skip
                                        # above. Keeps the reclassify clear
                                        # below from wiping its prior
                                        # prediction (Codex #1388 P1
                                        # r3663159360).
                                        source_skipped_photo_ids.add(
                                            photo["id"]
                                        )
                                        spec_source_skipped_photo_ids.add(
                                            photo["id"]
                                        )
                                        gave_up_on_source = True
                                        break
                                    # Resumed. Retry the same read before
                                    # advancing — silently skipping the
                                    # paused-during photo would leave it
                                    # unclassified even though the source
                                    # came back, and on a reclassify run
                                    # finalization would clear its old
                                    # prediction with no replacement
                                    # (Codex #1388 P2). If the retry also
                                    # fails, the loop re-probes and either
                                    # pauses again (bounded), degrades to
                                    # folder-scope, or gives up.
                                    _prep_started = time.time()
                                    img, folder_path, image_path = (
                                        _prepare_image(
                                            photo, folders,
                                            None if full_image_fallback
                                            else detection,
                                        )
                                    )
                                    _det_prep_seconds += max(
                                        time.time() - _prep_started, 0.0,
                                    )
                                    if img is not None:
                                        # Successful recovery: refund the
                                        # pause budget so an unrelated
                                        # outage later in the run — a
                                        # second share that drops, or the
                                        # same share dropping again hours
                                        # later — still gets its own
                                        # bounded retry window. Without
                                        # this, three separately-recovered
                                        # outages exhaust the budget and
                                        # the fourth outage immediately
                                        # takes the give-up branch without
                                        # ever offering Resume (Codex
                                        # #1388 P2 r3663816327). The
                                        # ``pauses`` bound is meant to
                                        # protect against a user who
                                        # resumes WITHOUT actually
                                        # remounting the share — a
                                        # successful read is proof the
                                        # share IS back, so the counter
                                        # can honestly reset.
                                        source_offline["pauses"] = 0
                                if gave_up_on_source:
                                    break
                                if img is None:
                                    continue
                                # This detection is about to enter the flush
                                # batch — attribute its full preparation cost
                                # to the rate denominator so ``_prepare_image``
                                # time (open + decode + crop + resize) shows
                                # up alongside ``_flush_batch`` time. Without
                                # this, RAW/JPEG-heavy or slow-storage runs
                                # publish a rate that only reflects GPU work
                                # and understate the ETA (Codex #1468 P2).
                                inference_seconds += _det_prep_seconds
                                inference_batch.append({
                                    "photo": photo,
                                    "detection_id": detection["id"],
                                    "folder_path": folder_path,
                                    "image_path": image_path,
                                    "img": img,
                                })
                                # Flush the first real inference immediately. That
                                # preserves the existing cancel checkpoint after model
                                # warm-up, then later images batch normally for GPU
                                # throughput.
                                if (
                                    not has_flushed_in_spec
                                    or len(inference_batch) >= inference_batch_size
                                ):
                                    _flush_pending_inference()

                        # Batch boundary: surface the per-photo accumulated
                        # count + cached to the UI. Replaces the old per-batch
                        # pre-advance which lied about progress when batches
                        # contained cache hits.
                        if _should_abort(abort):
                            _close_pending_inference()
                        else:
                            _flush_pending_inference()
                        stages["classify"]["total"] = total * len(resolved_specs_local)
                        elapsed = max(time.time() - start_time, 0.01)
                        _emit_progress(
                            runner, job["id"], stages, "classify",
                            f"Classifying with {active_spec['name']}"
                            + (
                                f" ({spec_idx + 1}/{len(resolved_specs_local)})"
                                if len(resolved_specs_local) > 1 else ""
                            ),
                            step_id=step_id,
                            rate=round(processed_in_spec / elapsed * 60, 1),
                        )
                        runner.update_step(
                            job["id"], step_id,
                            progress={
                                "current": processed_in_spec,
                                "total": total,
                                **_classification_eta_progress(
                                    total=total,
                                    seen=processed_in_spec,
                                    cached_estimate=cached_est,
                                    cache_hits=len(photos_cached_in_spec),
                                    inference_attempts=len(
                                        photos_attempted_in_spec
                                    ),
                                    classified=len(photos_inferred_in_spec),
                                    # Inference-active seconds only. Wall time
                                    # since ``start_time`` includes the cache-
                                    # traversal prefix and would deflate the
                                    # per-attempt rate on cache-heavy runs
                                    # (Codex #1468 P2).
                                    elapsed=inference_seconds,
                                    cache_overcount=len(
                                        photos_cache_overcounted_in_spec
                                    ),
                                    unclassifiable_estimate=len(
                                        preflight_unclassifiable_ids
                                    ),
                                    unclassifiable_seen=len(
                                        photos_unclassifiable_in_spec
                                    ),
                                ),
                            },
                        )

                    if _should_abort(abort):
                        _close_pending_inference()
                    else:
                        _flush_pending_inference()

                    # Skip the grouping/storage finalization on cancel — it can
                    # take a minute on large collections and the user has already
                    # asked us to stop. Per-photo counters are accurate, no
                    # corrective fixup needed.
                    if _should_abort(abort):
                        _emit_progress(
                            runner, job["id"], stages, "classify",
                            f"Cancelled — {processed_in_spec} of "
                            f"{total} processed",
                            step_id=step_id,
                        )
                        runner.update_step(
                            job["id"], step_id,
                            status="completed",
                            progress={
                                "current": processed_in_spec,
                                "total": total,
                            },
                            summary=(
                                f"Cancelled "
                                f"({processed_in_spec} of {total} processed)"
                            ),
                        )
                        continue

                    # Reclassify clear, deferred from the top of the per-spec body
                    # so a mid-batch cancel above leaves the user's prior
                    # predictions intact (Codex P1 review on #710).  Scope by
                    # labels_fingerprint so reclassifying one workspace's label
                    # set doesn't wipe another workspace's cached predictions on
                    # the same photos under its own fingerprint (shared-folder
                    # setups).  ``clear_run_keys=False`` because the per-photo
                    # ``record_classifier_run`` calls inside the loop above
                    # already wrote fresh classifier_runs rows for processed
                    # detections — wiping them here would strand the gate and
                    # force the next non-reclassify pass to re-infer everything.
                    #
                    # Scope to photos this spec actually reached AND wasn't
                    # skipped as source-offline: an unreached photo (mount-
                    # scoped give-up, or every photo under a vanished folder)
                    # is one this run had no chance to rewrite, so wiping
                    # its prior prediction would leave it with nothing at
                    # all (Codex #1388 P1). Use the per-spec skipped set
                    # rather than the aggregate one so a photo skipped for
                    # an earlier spec but successfully reached this spec
                    # still has its stale prior prediction cleared before
                    # ``_store_grouped_predictions`` writes the fresh row —
                    # ``add_prediction`` is ``INSERT OR IGNORE`` and would
                    # otherwise keep the stale species/confidence (Codex
                    # #1388 P2 r3663642360). Fall back to the collection-
                    # wide clear when no source outage hit this spec —
                    # that's still the desired behavior on a clean
                    # reclassify.
                    if params.reclassify:
                        if spec_source_skipped_photo_ids:
                            clear_ids = list(
                                spec_reached_photo_ids
                                - spec_source_skipped_photo_ids
                            )
                        else:
                            clear_ids = [p["id"] for p in photos]
                        if clear_ids:
                            thread_db.clear_predictions(
                                model=model_name,
                                collection_photo_ids=clear_ids,
                                labels_fingerprint=spec_fp,
                                clear_run_keys=False,
                            )

                    group_result = _store_grouped_predictions(
                        raw_results, job["id"], model_name,
                        grouping_window, similarity_threshold, tax, thread_db,
                        labels_fingerprint=spec_fp,
                    )
                    # promote_and_publish reads persisted predictions written
                    # by _store_grouped_predictions above; running it inside
                    # _record_batch_classifier_runs would no-op because those
                    # rows don't exist yet, leaving fresh classifier_runs
                    # stranded on runtime_fingerprint = 'legacy' and out of
                    # bundle exports.
                    # Reconstruct the configured ArtifactStore from the
                    # path stashed by ``run_pipeline_job`` so classifier
                    # artifacts published here land in the same cache the
                    # status / export / catalog-reapplication paths use
                    # when ``COMPUTATION_CACHE_DIR`` is overridden.
                    from computation_cache import ArtifactStore
                    _publish_cache_dir = job.get("_computation_cache_dir")
                    _publish_store = (
                        ArtifactStore(_publish_cache_dir)
                        if _publish_cache_dir else None
                    )
                    _publish_classifier_runs_for_raw_results(
                        thread_db, raw_results, model_name, spec_fp,
                        labels_fingerprint_full=loaded_models.get(
                            "labels_fingerprint_full"
                        ),
                        model_identity=loaded_models.get(
                            "classifier_model_identity"
                        ),
                        taxonomy_identity=loaded_models.get(
                            "taxonomy_identity", "no-tax",
                        ),
                        store=_publish_store,
                    )
                    preds = group_result["predictions_stored"]
                    total_predictions_stored += preds
                    total_full_image_fallbacks += full_image_fallbacks
                    total_failed += failed
                    total_skipped_existing += skipped_existing
                    models_succeeded += 1
                    # A per-model row is "completed" only when it actually
                    # finished classifying every photo it was asked to. If
                    # the mount died (source_offline["reason"] latched) or
                    # a folder outage left photos unread
                    # (spec_source_skipped_photo_ids), the row should be
                    # ``failed`` — the Jobs page reads ``step.status``
                    # directly and auto-collapses ``completed`` rows without
                    # warnings, so leaving this ``completed`` would show a
                    # failed job with a green, collapsed classifier row
                    # (Codex #1388 P2 r3664058179). The later stage-status
                    # rollup at the end of the function isn't mapped back
                    # to the ``classify:<model>`` step, so the fix has to
                    # land here.
                    spec_gave_up = bool(source_offline["reason"])
                    spec_had_skips = bool(spec_source_skipped_photo_ids)
                    spec_step_status = (
                        "failed" if (spec_gave_up or spec_had_skips)
                        else "completed"
                    )
                    if spec_step_status == "failed":
                        failed_step_ids.add(step_id)
                    else:
                        completed_step_ids.add(step_id)

                    # Reclassify stale-row purge: only fires after the FIRST
                    # successful model has written fresh predictions, so a run
                    # where every model fails to load leaves prior detections
                    # (and their cascaded predictions) intact. Photos whose
                    # detect iteration never completed keep their old rows.
                    if (
                        params.reclassify
                        and models_succeeded == 1
                        and detect_state["pre_run_det_ids"]
                    ):
                        pre_ids = detect_state["pre_run_det_ids"]
                        # Scope the purge to photos whose detect AND classify
                        # iterations both completed in this run. Using only
                        # classify coverage would delete rows for photos that
                        # hit the db.get_detections() fallback (i.e. never got
                        # a fresh detect). Using only detect coverage would
                        # delete rows for photos the classifier never reached.
                        # The intersection guarantees there's a replacement
                        # detection AND that the classifier considered it.
                        #
                        # Also subtract source-skipped photos: ``first_model_
                        # photo_ids`` is added to at the TOP of the per-photo
                        # body (before the image read), so a photo whose read
                        # later failed with the source offline is still in
                        # that set. Without this subtraction, the purge below
                        # would delete any pre-run detection ids whose boxes
                        # differ from the fresh boxes even for photos we
                        # never classified — cascading through their prior
                        # predictions despite the ``clear_predictions``
                        # exclusion that already spares them (Codex #1388
                        # P1 r3663922709).
                        purge_ids = (
                            (first_model_photo_ids
                             & detect_state["processed_ids"])
                            - source_skipped_photo_ids
                        )
                        # Delete only pre-run ids the current run did NOT
                        # re-produce. Detection ids are content-addressed
                        # (vireo/detection_id.py), so re-detecting the same boxes
                        # yields the SAME ids as the pre-run snapshot, and
                        # write_detection_batch UPSERTs them with the freshly
                        # written predictions now hanging off them. Deleting every
                        # pre-run id unconditionally would cascade-delete those
                        # predictions (and the live detection rows) for every photo
                        # whose boxes didn't change — the common reclassify case.
                        # Compare against the ids THIS run actually re-detected
                        # (detect_state["detections"] is the in-memory map
                        # _detect_batch built). A no-detection photo may also
                        # have a freshly used synthetic full-image anchor from
                        # the fallback path; preserve that id so the purge does
                        # not cascade-delete the new fallback prediction. Other
                        # pre-run rows on empty photos are stale and get purged
                        # (write_detection_batch([]) already cleared the
                        # MegaDetector rows at the data layer — this is the
                        # belt-and-suspenders pass and cross-model cleanup). A
                        # photo re-detected with the same boxes has its ids in
                        # the fresh set, so they survive.
                        fresh_by_photo = detect_state["detections"]
                        stale_ids = [
                            det_id
                            for photo_id, id_set in pre_ids.items()
                            if photo_id in purge_ids
                            for det_id in id_set
                            if det_id not in (
                                {
                                    d["id"]
                                    for d in fresh_by_photo.get(photo_id, [])
                                }
                                | fresh_full_image_ids_by_photo.get(photo_id, set())
                            )
                        ]
                        if stale_ids:
                            getattr(
                                thread_db,
                                "delete_detections_by_ids",
                                lambda _: None,
                            )(stale_ids)
                            log.debug(
                                "reclassify: purged %d stale detection rows for "
                                "%d photos (%d not in purge scope, rows preserved)",
                                len(stale_ids),
                                len(purge_ids & pre_ids.keys()),
                                len(pre_ids) - len(purge_ids & pre_ids.keys()),
                            )

                    parts = [f"{preds} predictions"]
                    if skipped_existing:
                        parts.append(f"{skipped_existing} cached")
                    if full_image_fallbacks:
                        parts.append(f"{full_image_fallbacks} full-image fallback")
                    if contextual_weak_ids:
                        parts.append(
                            f"{len(contextual_weak_ids)} weak detections rescued"
                        )
                    if failed:
                        parts.append(f"{failed} failed")
                    if source_skipped:
                        parts.append(
                            f"{source_skipped} unreachable (source offline)"
                        )
                    if source_offline["reason"]:
                        # Name the photos we never reached rather than folding
                        # them into a count that reads as "classified".
                        parts.append(
                            f"stopped after {processed_in_spec} of {total} — "
                            f"source {source_offline['reason']}"
                        )
                    # Attach the outage as ``error=`` on the failed row so the
                    # Jobs page shows a human-readable reason next to the
                    # collapsed step, matching the shape of the
                    # model-load-failure branch above (line 4271).
                    step_error = None
                    if spec_step_status == "failed":
                        if spec_gave_up:
                            step_error = (
                                f"Stopped after {processed_in_spec} of "
                                f"{total} — source {source_offline['reason']}"
                            )
                        else:
                            step_error = (
                                f"{len(spec_source_skipped_photo_ids)} of "
                                f"{total} photos unreachable "
                                "(source offline)"
                            )
                    runner.update_step(
                        job["id"], step_id, status=spec_step_status,
                        summary=", ".join(parts),
                        error=step_error,
                    )

                # Cancellation takes precedence over the all-models-failed-to-load
                # signal: if the user cancelled mid-classify after a prior model
                # had already been added to skipped_model_names, raising here
                # would misclassify the cancel as a fatal load failure and
                # overwrite the per-model 'Cancelled' summary in the exception
                # handler.
                if (
                    models_succeeded == 0
                    and skipped_model_names
                    and not _should_abort(abort)
                ):
                    raise RuntimeError(
                        f"All {len(skipped_model_names)} model(s) failed to load: "
                        + ", ".join(skipped_model_names)
                    )

                # Roll up per-photo failures into a single classify stage status
                # + errors[] entry, matching the pattern in #562. Per-model step
                # rows already carry their own summary; the stage status reflects
                # the whole classify pass.  error_count uses unique failed photo
                # IDs (not per-model attempt count) so the badge can never
                # exceed total photos.
                n_failed_photos = len(failed_photo_ids)
                n_source_skipped_photos = len(source_skipped_photo_ids)
                # Publish the source-skipped set to downstream stages BEFORE
                # deciding to set ``abort``. extract_masks and eye_keypoints
                # need this even on the folder-scoped path (abort deliberately
                # stays clear so healthy folders keep processing) — otherwise
                # they still walk every detected photo in the missing folder
                # and re-issue reads against the offline share (Codex #1388
                # P2 r3664058173).
                source_offline_state["skipped_photo_ids"] = (
                    set(source_skipped_photo_ids)
                )
                if source_offline["reason"]:
                    # Must carry the "[classify] Fatal:" prefix: the end-of-run
                    # rollup picks the job's headline error by that marker and
                    # would otherwise fall back to errors[0] — likely an
                    # unrelated per-photo warning logged much earlier.
                    errors.append(
                        f"[classify] Fatal: source "
                        f"{source_offline['reason']}. Reconnect it and run "
                        f"Process again to classify the rest — photos already "
                        f"classified will be skipped."
                    )
                    # extract_masks_stage / eye_keypoints_stage only gate on
                    # abort.is_set(); with the source dead there is nothing
                    # for them to read either, so without this they walk every
                    # detected photo and reproduce the exact "N failed"
                    # pattern this PR fixes — just one stage later, before
                    # the failed-stage rollup at the end of the pipeline gets
                    # a chance to short-circuit them.
                    abort.set()
                elif n_source_skipped_photos > 0:
                    # Folder-scoped outage: some photos were unreachable but
                    # the source as a whole is not necessarily gone (other
                    # folders in the collection may still be healthy). Do NOT
                    # abort — later stages can still make progress on the
                    # reachable photos — but the classify pass did not open
                    # every photo it was asked to, so surface a fatal-prefixed
                    # error so the end-of-run rollup names the outage instead
                    # of letting the run finish silent-green.
                    errors.append(
                        f"[classify] Fatal: {n_source_skipped_photos} of "
                        f"{total} photos unreachable (source offline). "
                        f"Reconnect the missing folder(s) and run Process "
                        f"again to classify the rest."
                    )
                # A run that gave up on a dead source, or that skipped photos
                # because their folder was unreachable, did NOT classify the
                # whole collection, so it must not land on the job tree as a
                # clean green stage — "completed" here would tell the user
                # their photos were processed when some were never opened.
                stages["classify"]["status"] = (
                    "failed"
                    if (
                        total_failed > 0
                        or source_offline["reason"]
                        or n_source_skipped_photos > 0
                    )
                    else "completed"
                )
                if total_failed > 0:
                    errors.append(
                        f"[classify] {n_failed_photos} of {total} photos "
                        "failed to classify"
                    )
                result["stages"]["classify"] = {
                    "total": total,
                    "predictions_stored": total_predictions_stored,
                    "detected": detect_state["total_detected"],
                    "failed": total_failed,
                    "source_offline": source_offline["reason"],
                    "source_skipped": n_source_skipped_photos,
                    "already_classified": total_skipped_existing,
                    "full_image_fallbacks": total_full_image_fallbacks,
                    "weak_detection_rescues": len(contextual_weak_ids),
                    "model_count": len(resolved_specs_local),
                    "models_succeeded": models_succeeded,
                    "models_skipped": len(skipped_model_names),
                    "skipped_model_names": skipped_model_names,
                }
            except Exception as e:
                errors.append(f"[classify] Fatal: {e}")
                log.exception("Pipeline classify stage failed")
                abort.set()
                stages["classify"]["status"] = "failed"
                # Only surface the fatal error on rows that haven't already
                # reached a terminal state. Without this, a late-loop exception
                # would overwrite the 'completed' status of earlier models that
                # finished successfully, misreporting per-model outcomes.
                specs_for_step_ids = loaded_models.get("resolved_specs") or []
                for spec in specs_for_step_ids:
                    sid = f"classify:{spec['id']}"
                    if sid in completed_step_ids or sid in failed_step_ids:
                        continue
                    runner.update_step(
                        job["id"], sid,
                        status="failed", error=str(e),
                    )
            finally:
                # Release the held classifier so subsequent pipelines can reuse
                # the cached session (or the idle timer can reclaim VRAM). Runs
                # whether classify completed cleanly, errored mid-loop, or hit
                # the fatal-exception path above.
                _release_classifier_cache_handle(loaded_models)

            _update_stages(runner, job["id"], stages)

        def extract_masks_stage():
            """Run SAM2 mask extraction + DINOv2 embeddings after classify."""
            if params.skip_extract_masks or abort.is_set() or not collection_id:
                stages["extract_masks"]["status"] = "skipped"
                runner.update_step(job["id"], "extract_masks", status="completed",
                                   summary="Skipped")
                return

            stages["extract_masks"]["status"] = "running"
            runner.update_step(job["id"], "extract_masks", status="running")
            _update_stages(runner, job["id"], stages)

            # Latched by the source-offline branch when the mask stage owns
            # the outage (fully-cached classify + offline masks folder). The
            # finalizer below preserves ``failed`` when this is set: without
            # it, ``em_failed`` is zero (offline photos are pre-filtered from
            # the worklist), and the finalizer flips the stage back to
            # ``completed`` — silently masking the outage. The end-of-run
            # rollup at ~L7005 reads only stage ``status`` values, so the job
            # would complete "successfully" with the missing masks folded
            # away in ``errors`` (Codex #1388 P1 r3665130244).
            em_offline_latched = False
            # Photos the pre-flight probe removed from the worklist because
            # their folder was already unreachable. They never reach the
            # per-photo loop, so without carrying the count forward every
            # counter reads zero on a stage that simultaneously reports
            # unreachable photos — which the Extract card then rendered as
            # "No photos needed masks" (Codex #1392 P2).
            em_preflight_unreadable = 0
            # Dropped photos that already carried a usable mask. They are a
            # successful outcome — the online cache-hit path counts exactly
            # this state as ``masked`` — so they belong in the counters
            # instead of vanishing from the stage's coverage entirely
            # (Codex #1392 P2).
            em_preflight_masked = 0
            # The pre-flight outage message, kept so an early exit can put it
            # on the failed step instead of a benign "no detections" summary.
            em_offline_preflight_error = None

            try:
                import config as cfg
                from dino_embed import embed, embed_batch, embedding_to_blob
                from masking import (
                    crop_completeness,
                    crop_subject,
                    generate_mask,
                    render_proxy,
                    save_mask,
                )
                from quality import compute_all_quality_features

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective_cfg = thread_db.get_effective_config(cfg.load())
                pipeline_cfg = effective_cfg.get("pipeline", {})
                sam2_variant = pipeline_cfg.get("sam2_variant")
                dinov2_variant = pipeline_cfg.get("dinov2_variant")
                proxy_longest_edge = pipeline_cfg.get("proxy_longest_edge")

                masks_dir = os.path.join(os.path.dirname(db_path), "masks")
                os.makedirs(masks_dir, exist_ok=True)

                photos = _filter_excluded(thread_db.get_collection_photos(collection_id, per_page=999999))

                # The mask loop applies two detection-confidence floors: a
                # strict one for ordinary photos, and a lower
                # ``weak_detection_confidence`` floor with an MDv6/animal
                # filter for photos rescued by ``contextual_weak_runs``. The
                # pre-flight offline probe below has to know both, and know
                # the *precise* rescue set — a plain "look this low"
                # candidacy floor over-counts unrelated sub-threshold photos
                # as at-risk, inflating the outage report (Codex #1392 P2
                # r3687403366). Compute them on the pre-filter ``photos``
                # list so the eligibility set covers photos we're about to
                # drop, and mirror the loop's anchor-species gate exactly.
                detector_confidence = effective_cfg.get("detector_confidence", 0.2)
                weak_rescue_enabled = pipeline_cfg.get(
                    "weak_detection_rescue_enabled", True,
                )
                weak_detection_confidence = pipeline_cfg.get(
                    "weak_detection_confidence", 0.12,
                )

                contextual_weak_ids: set = set()
                if (
                    weak_rescue_enabled
                    and weak_detection_confidence < detector_confidence
                    and photos
                ):
                    from weak_detections import contextual_weak_runs
                    raw_mdv6_dets = thread_db.get_detections_for_photos(
                        [p["id"] for p in photos],
                        min_conf=weak_detection_confidence,
                        detector_model="megadetector-v6",
                    )
                    weak_runs = contextual_weak_runs(
                        photos,
                        raw_mdv6_dets,
                        detector_confidence=detector_confidence,
                        weak_confidence=weak_detection_confidence,
                        max_gap=pipeline_cfg.get("burst_time_gap", 3.0),
                    )
                    weak_scope_ids = {
                        photo_id
                        for run in weak_runs
                        for photo_id in (
                            run["left_photo_id"],
                            *run["photo_ids"],
                            run["right_photo_id"],
                        )
                    }
                    if weak_scope_ids:
                        # Mask only frames that pass the same matching-species
                        # anchor gate as encounter grouping. Candidate weak
                        # runs with conflicting or unclassified anchors remain
                        # ordinary sub-threshold detections throughout.
                        from pipeline import load_photo_features
                        weak_features = load_photo_features(
                            thread_db,
                            config=effective_cfg,
                            photo_ids=weak_scope_ids,
                        )
                        contextual_weak_ids = {
                            feature["id"]
                            for feature in weak_features
                            if feature.get("subject_uncertain")
                        }

                # Drop photos whose folder is offline. Without this we'd
                # render_proxy every one of them, they'd all skip with
                # proxy=None, and the stage summary would land as "N
                # skipped" — obscuring the real cause (missing folder)
                # with a mask-extraction failure count (Codex #1388 P2
                # r3664058173). The folder-scoped branch in classify
                # deliberately leaves ``abort`` clear so healthy folders
                # keep processing here; this filter is how "healthy
                # folders keep processing" stays true without dragging
                # the missing folder's photos along.
                #
                # Always probe the worklist's folders — do NOT gate on
                # ``source_skipped_photo_ids`` (Codex #1388 P1
                # r3664891993). A fully-cached classify (every detection
                # + classifier result already stored, only masks missing
                # — e.g. after a SAM variant change) makes no image
                # opens, so the seed set stays empty even though every
                # remaining file is on an unreachable share. Without the
                # unconditional probe, extract_masks would then reopen
                # the dead source photo-by-photo, count each failed
                # render_proxy as merely skipped, and the pipeline
                # could finish "successfully" with no masks made.
                #
                # Filter by FOLDER, not by photo id: the classify seed
                # only accumulates photos that reached ``_prepare_image``,
                # so the non-reclassify cache branch — which appends the
                # cached prediction to raw_results and ``continue``s
                # without any disk touch — never contributes its photos
                # to the seed set. An ID-only filter would leave those
                # cached photos in place, and mask/eye-keypoint stages
                # would reopen the same dead source (Codex #1388 P2
                # r3664694179). Re-probing per folder also handles the
                # multi-spec recovery case where a folder that dropped
                # for spec A comes back before mask extraction runs, so
                # its photos aren't silently excluded (Codex #1388 P2
                # r3664348758).
                def _row_folder_id(row):
                    # sqlite.Row raises IndexError on missing columns; a
                    # test-shape dict raises KeyError. Both mean "no
                    # folder to probe" for this row — leave it in place
                    # rather than treating it as offline.
                    try:
                        fid = row["folder_id"]
                    except (KeyError, IndexError):
                        return None
                    return fid

                worklist_folder_ids = {
                    fid for fid in (_row_folder_id(p) for p in photos)
                    if fid is not None
                }
                still_offline_folder_ids = _still_offline_folder_ids_of(
                    thread_db, worklist_folder_ids,
                )
                if still_offline_folder_ids:
                    kept, dropped = [], []
                    for p in photos:
                        if _row_folder_id(p) in still_offline_folder_ids:
                            dropped.append(p)
                        else:
                            kept.append(p)
                    photos = kept
                    dropped_ids = {p["id"] for p in dropped}
                    # The unreadable count exists to explain `no_subject_mask`
                    # rejections in Process Review. A dropped photo that
                    # already carries an active mask for the configured
                    # variant won't be rejected for that, so counting it
                    # overstates the damage from an outage that never harmed
                    # it (Codex #1392 P2).
                    # Photos that will be rejected as `no_subject_mask`
                    # because of this outage — i.e. the dropped ones that
                    # don't already have a mask. The already-masked ones need
                    # no source read and are at no risk, so they drive neither
                    # the count nor the failure latch below: latching on them
                    # would fail the stage and demand a reconnect that would
                    # change nothing (Codex #1392 P2).
                    already_masked_ids, at_risk_dropped_ids = (
                        _preflight_mask_outcomes(
                            thread_db, dropped, sam2_variant, dinov2_variant,
                            detector_confidence,
                            contextual_weak_ids=contextual_weak_ids,
                            weak_detection_confidence=(
                                weak_detection_confidence
                                if weak_rescue_enabled
                                and weak_detection_confidence
                                < detector_confidence
                                else None
                            ),
                        )
                    )
                    em_preflight_unreadable = len(at_risk_dropped_ids)
                    em_preflight_masked = len(already_masked_ids)
                    # Publish so eye_keypoints (later downstream) sees
                    # the same offline set without having to re-probe
                    # every folder from scratch.
                    prior_skipped = (
                        source_offline_state.get("skipped_photo_ids")
                        or set()
                    )
                    source_offline_state["skipped_photo_ids"] = (
                        set(prior_skipped) | dropped_ids
                    )
                    already_flagged_classify = any(
                        e.startswith("[classify] Fatal:") for e in errors
                    )
                    if at_risk_dropped_ids and not already_flagged_classify:
                        # Latch the outage immediately so the finalizer
                        # doesn't flip the stage back to ``completed``
                        # (offline photos were removed above, so
                        # ``em_failed`` is zero at the bottom of the loop
                        # regardless — Codex #1388 P1 r3665130244).
                        # The Fatal error string itself is built below
                        # once ``total`` (the kept mask-candidate count) is
                        # known, so its denominator matches the stage
                        # result's ``total`` instead of the whole
                        # collection worklist (Codex #1392 P2 r3687499184).
                        stages["extract_masks"]["status"] = "failed"
                        em_offline_latched = True
                    log.warning(
                        "Extract-masks: dropped %d photo(s) from %d "
                        "offline folder(s); source not reachable.",
                        len(dropped_ids), len(still_offline_folder_ids),
                    )

                # Build a map of photo_id -> primary detection (highest confidence)
                # from the detections table. Only photos with detections and without
                # masks need processing.
                #
                # Skip synthetic full-image detections (detector_model='full-image').
                # Those rows exist only to give classify predictions a non-NULL FK
                # anchor for photos where MegaDetector found no animals — they are
                # not real subject boxes and should not drive mask extraction or
                # count toward the photos_with_detections safeguard below (which
                # surfaces the "weights missing / no detections" diagnostic).
                # Note: we intentionally do NOT short-circuit when the photo
                # already has *some* mask in the photos table — that legacy
                # check ignored which SAM variant produced the mask, so a
                # config change to a different variant would never re-run.
                # The per-photo cache check happens inside the loop below
                # against photo_masks(photo_id, sam2_variant).
                #
                # Track sub-threshold-only photos separately so the silent-
                # completion guard can distinguish "no detection rows at all"
                # from "rows exist but every confidence is below
                # detector_confidence" — the user's remediation differs
                # (download weights vs lower the threshold).
                #
                # ``detector_confidence`` / ``weak_rescue_enabled`` /
                # ``weak_detection_confidence`` / ``contextual_weak_ids`` are
                # captured above (before the pre-flight offline probe) so
                # ``_preflight_mask_outcomes`` can apply the same weak-rescue
                # eligibility the loop uses.  Reusing them here keeps that
                # single-sourced.

                photo_det_map = {}
                photos_with_detections = 0
                photos_subthreshold_only = 0
                for p in photos:
                    # Pass the captured detector_confidence explicitly so the
                    # floor matches effective_cfg (and the standalone
                    # /api/jobs/extract-masks path), not whatever cfg.load()
                    # would re-read from disk inside get_detections. With the
                    # legacy `mask_path IS NULL` prefilter gone, sub-threshold-
                    # only photos would otherwise enter SAM extraction on
                    # variant cache misses.
                    #
                    # Contextual weak-rescue photos get the lower floor plus
                    # the same MDv6/animal constraints that classify_stage and
                    # load_photo_features apply, so mask extraction picks up
                    # the same bracketed frame those two stages already opted
                    # in to.
                    if p["id"] in contextual_weak_ids:
                        dets = [
                            d for d in thread_db.get_detections(
                                p["id"],
                                min_conf=weak_detection_confidence,
                                detector_model="megadetector-v6",
                            )
                            if d["category"] == "animal"
                        ]
                    else:
                        dets = [
                            d for d in thread_db.get_detections(
                                p["id"], min_conf=detector_confidence,
                            )
                            if d["detector_model"] != "full-image"
                        ]
                    if dets:
                        photos_with_detections += 1
                        primary = dets[0]  # already ordered by confidence DESC
                        photo_det_map[p["id"]] = {
                            "photo": p,
                            "det_box": {
                                "x": primary["box_x"],
                                "y": primary["box_y"],
                                "w": primary["box_w"],
                                "h": primary["box_h"],
                            },
                            "detector_model": primary["detector_model"],
                            # Stored prompt provenance: full-precision bbox
                            # tuple. detections.box_* are normalized REAL
                            # values in [0, 1], so int()-truncating would
                            # collapse every prompt to (0, 0, 0, 0) and the
                            # cache/staleness check would never invalidate
                            # on bbox change. SQLite's column type affinity
                            # accepts REAL into the INTEGER-declared
                            # columns and stores them verbatim.
                            "prompt": (
                                primary["box_x"],
                                primary["box_y"],
                                primary["box_w"],
                                primary["box_h"],
                            ),
                        }
                    else:
                        # No qualifying detection — but check whether sub-
                        # threshold rows exist so the silent-completion guard
                        # can distinguish "weights never ran" from "threshold
                        # too high". This counter only matters for photos
                        # that haven't been masked yet (an already-masked
                        # photo isn't in the "what got skipped silently"
                        # diagnostic regardless of threshold).
                        raw_dets = [
                            d for d in thread_db.get_detections(p["id"], min_conf=0)
                            if d["detector_model"] != "full-image"
                        ]
                        if raw_dets:
                            has_mask = thread_db.conn.execute(
                                "SELECT mask_path FROM photos WHERE id=?", (p["id"],)
                            ).fetchone()[0]
                            if not has_mask:
                                photos_subthreshold_only += 1

                photos_to_process = [
                    photo_det_map[pid] for pid in photo_det_map
                ]

                folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
                total = len(photos_to_process)
                # Now that ``total`` is known, build the preflight outage
                # message. The denominator has to be the mask-candidate
                # total (loop total + preflight-dropped mask candidates),
                # matching the stage result's ``total``. Using
                # ``total_before`` — the whole collection worklist —
                # produced messages like "1 of 100 photos unreachable"
                # when only 1 was a mask candidate and the result showed
                # ``total: 1``, so the reader couldn't reconcile the two
                # (Codex #1392 P2 r3687499184).
                if em_preflight_unreadable > 0:
                    em_offline_preflight_error = (
                        f"[extract_masks] Fatal: "
                        f"{em_preflight_unreadable} of "
                        f"{total + em_preflight_unreadable + em_preflight_masked} "
                        f"photos unreachable (source offline). "
                        f"Reconnect the missing folder(s) and run "
                        f"Process again to extract the rest."
                    )
                    # Latch was set above only when this stage owns the
                    # outage (classify didn't already emit a Fatal), so
                    # this gate mirrors the previous append-in-preflight
                    # condition and avoids a duplicate Fatal entry.
                    if em_offline_latched:
                        errors.append(em_offline_preflight_error)
                masked = 0
                skipped = 0
                em_failed = 0
                # Photos whose source file could not be read. Kept out of
                # ``skipped`` because the two mean opposite things to the
                # user: a skip is "SAM found no subject in this frame" (a
                # real answer about the photo), while an unreadable source
                # is "we never got to look at it". Both leave the photo
                # unmasked, and scoring hard-rejects every unmasked photo
                # with ``no_subject_mask`` — so folding them together let a
                # dropped share present as a clean "N masked, M skipped"
                # completion while two-thirds of the library silently became
                # rejects in Process Review.
                em_unreadable = 0
                # Folders proven unreachable by a failed read during this
                # loop. Every later photo under them is counted unreadable
                # without reissuing a read: on a dead SMB/NFS share each
                # attempt is an instant EIO, so re-probing hundreds of
                # photos only slows the give-up down.
                em_offline_folder_ids: set = set()
                # Unreadable photos attributable to a latched source outage,
                # tracked apart from the total so the reconnect message can't
                # claim credit for a corrupt file in a healthy folder — or for
                # photos behind a *different* dead source (Codex #1392 P2).
                em_offline_unreadable = 0
                em_offline_reasons: list = []
                start_time = time.time()

                # If the input collection has photos but none carry detections,
                # surface a clear status instead of silently completing with
                # masked=0 — otherwise the pipeline rejects every photo with
                # no_subject_mask without explaining why masks were never made.
                # Distinguish two cases:
                #   (a) weights missing → actionable remediation
                #   (b) weights present → legitimate outcome (empty scenes,
                #       strict confidence threshold, non-wildlife photos)
                # Only fire this diagnostic when classify actually ran in this
                # invocation.  If classify was skipped (skip_classify=True, no
                # models available, abort, etc.) zero detections is expected and
                # appending an extract_masks error would be factually incorrect.
                classify_ran = stages["classify"]["status"] not in ("skipped", "pending")
                if photos_with_detections == 0 and len(photos) > 0 and classify_ran:
                    weights_present = False
                    try:
                        from detector import MEGADETECTOR_ONNX_PATH
                        weights_present = os.path.isfile(MEGADETECTOR_ONNX_PATH)
                    except ImportError:
                        weights_present = False

                    if photos_subthreshold_only > 0:
                        reason = (
                            f"{photos_subthreshold_only} photo(s) have detections but every "
                            f"detection is below the current detector_confidence threshold "
                            f"({detector_confidence}). The pipeline will reject these photos "
                            "with `no_subject_mask`. Lower `detector_confidence` in workspace "
                            "settings to extract masks for them."
                        )
                        summary = (
                            f"Skipped — {photos_subthreshold_only} photo(s) below "
                            f"detector_confidence threshold ({detector_confidence})"
                        )
                    elif weights_present:
                        reason = (
                            f"No detections produced for {len(photos)} photo(s). MegaDetector ran but "
                            "found no animals meeting the confidence threshold. The pipeline will "
                            "reject every photo with `no_subject_mask`. Lower `detector_confidence` "
                            "in settings or rerun classify with a different threshold if detections "
                            "were expected."
                        )
                        summary = "Skipped — MegaDetector produced no detections"
                    else:
                        reason = (
                            f"No detections available for {len(photos)} photo(s). MegaDetector "
                            "weights are not downloaded, so the classify stage ran on full images "
                            "and stored no detections. Without detections the mask extraction stage "
                            "has nothing to process, and the pipeline will reject every photo with "
                            "`no_subject_mask`. Download MegaDetector V6 from the pipeline models "
                            "page and rerun the pipeline."
                        )
                        summary = "Skipped — MegaDetector weights not downloaded"

                    log.warning("Pipeline extract-masks: %s", reason)
                    errors.append(f"[extract_masks] {reason}")
                    if photos_subthreshold_only > 0:
                        em_reason = "all_subthreshold"
                    elif weights_present:
                        em_reason = "no_detections"
                    else:
                        em_reason = "weights_missing"
                    exit_status, exit_step_status, exit_step_extra, exit_payload = (
                        _extract_masks_early_exit(
                            em_reason, photos_subthreshold_only,
                            em_preflight_unreadable, em_preflight_masked,
                            em_offline_latched, em_offline_preflight_error,
                        )
                    )
                    stages["extract_masks"]["status"] = exit_status
                    runner.update_step(
                        job["id"], "extract_masks", status=exit_step_status,
                        summary=summary, **exit_step_extra,
                    )
                    result["stages"]["extract_masks"] = exit_payload
                    _update_stages(runner, job["id"], stages)
                    return

                # Mixed-state guard: photos_with_detections > 0 (some photos have
                # qualifying detections — already masked from a prior run) but
                # there are also unmasked photos whose only detections are below
                # the threshold. Without this branch the stage completes silently
                # with "0 masked, 0 skipped" and the user has no way to discover
                # why the unmasked photos were never processed. Production hit
                # this when 4166 of 5054 photos were already masked and the
                # remaining 727 had only sub-threshold detections.
                if total == 0 and photos_subthreshold_only > 0 and classify_ran:
                    reason = (
                        f"{photos_subthreshold_only} photo(s) have detections but every "
                        f"detection is below the current detector_confidence threshold "
                        f"({detector_confidence}). The pipeline will reject these photos "
                        "with `no_subject_mask`. Lower `detector_confidence` in workspace "
                        "settings to extract masks for them."
                    )
                    summary = (
                        f"Skipped — {photos_subthreshold_only} photo(s) below "
                        f"detector_confidence threshold ({detector_confidence})"
                    )
                    log.warning("Pipeline extract-masks: %s", reason)
                    errors.append(f"[extract_masks] {reason}")
                    exit_status, exit_step_status, exit_step_extra, exit_payload = (
                        _extract_masks_early_exit(
                            "all_subthreshold", photos_subthreshold_only,
                            em_preflight_unreadable, em_preflight_masked,
                            em_offline_latched, em_offline_preflight_error,
                        )
                    )
                    stages["extract_masks"]["status"] = exit_status
                    runner.update_step(
                        job["id"], "extract_masks", status=exit_step_status,
                        summary=summary, **exit_step_extra,
                    )
                    result["stages"]["extract_masks"] = exit_payload
                    _update_stages(runner, job["id"], stages)
                    return

                # Auto-download SAM2 + DINOv2 weights on first pipeline run.
                # Mirrors the MegaDetector auto-download pattern (commit 90cd0f9):
                # without this, first-time users hit 1 FileNotFoundError per
                # photo instead of either getting the weights automatically
                # or seeing one actionable message.
                #
                # The download is deferred until the loop hits the first true
                # cache miss. With per-variant photo_masks, the worklist now
                # includes every photo with a detection (cache hits are
                # filtered inside the loop, not by a `mask_path IS NULL`
                # prefilter), so gating on ``total > 0`` would force a
                # multi-hundred-MB download even on a fully-cached rerun in
                # an offline / fresh-checkout environment. ``_ensure_weights``
                # is idempotent and a no-op on second invocation.
                from dino_embed import ensure_dinov2_weights
                from masking import ensure_sam2_weights

                def _dl_progress(phase, current, total_steps):
                    _emit_progress(
                        runner, job["id"], stages, "extract_masks", phase,
                    )

                _weights_ensured = [False]

                def _ensure_weights():
                    if _weights_ensured[0]:
                        return
                    ensure_sam2_weights(
                        variant=sam2_variant, progress_callback=_dl_progress,
                    )
                    ensure_dinov2_weights(
                        variant=dinov2_variant, progress_callback=_dl_progress,
                    )
                    _weights_ensured[0] = True

                processed = 0
                # ``while`` (not ``for``) so a pause that arrives mid-photo
                # can unwind the per-photo mask lock and retry the SAME
                # index after resume without holding the lock across the
                # entire pause. See the pause-unwind branch of the
                # ``except ResourceWaitCancelled`` handler below.
                i = 0
                while i < len(photos_to_process):
                    entry = photos_to_process[i]
                    if _should_abort(abort):
                        break

                    photo = entry["photo"]
                    det_box = entry["det_box"]
                    photo_id = photo["id"]
                    folder_path = folders.get(photo["folder_id"], "")
                    image_path = os.path.join(folder_path, photo["filename"])
                    mask_file_stage = None

                    try:
                        # Per-photo serialisation. Two pipelines whose
                        # collections overlap can both reach this photo.
                        # Without this lock:
                        #
                        #   - Same variant: both write the same
                        #     ``masks/{photo_id}.{variant}.png`` file and
                        #     can corrupt each other's bytes mid-write.
                        #   - Different variants (e.g. two workspaces
                        #     sharing folders but configured with sam2-small
                        #     vs sam2-large): the per-variant mask files
                        #     don't collide, BUT both runs denormalise into
                        #     the same ``photos`` row via
                        #     ``set_active_mask_variant`` and
                        #     ``update_photo_embeddings``. Their writes can
                        #     interleave, leaving photos.active_mask_variant
                        #     pointing at one variant while photos.dino_*
                        #     embeddings were cropped from the other's mask.
                        #     regroup reads these denormalised columns, so
                        #     the corruption would silently flow into
                        #     grouping.
                        #
                        # Keyed by photo_id alone — not (photo_id, variant) —
                        # so the cross-variant collision in (2) is covered.
                        # Workspace isn't part of the key because photos are
                        # global in Vireo.
                        #
                        # ``bind_resource_cancel_check(_pause_or_cancel_pending)``
                        # swaps the outer parking pause probe for a
                        # non-parking one for the duration of this lock:
                        # a pause requested while the SAM/DINOv2 inference
                        # lease is contended raises ``ResourceWaitCancelled``
                        # here instead of parking beneath the photo lock.
                        # An unrelated unpaused pipeline reaching the same
                        # photo would otherwise block for the entire pause.
                        with bind_resource_cancel_check(
                            _pause_or_cancel_pending,
                        ), acquire_photo_mask(photo_id):
                            # Cache hit: a row already exists for (photo, variant)
                            # AND its stored prompt + detector still match the
                            # current primary detection AND the file is on disk.
                            # In that case the SAM result is unchanged, so we
                            # only re-activate the mask (cheap denormalize) and
                            # skip the heavy SAM + DINOv2 work.
                            existing = thread_db.get_photo_mask(
                                photo_id, sam2_variant,
                            )
                            if existing is not None:
                                cached_prompt = (
                                    existing["prompt_x"], existing["prompt_y"],
                                    existing["prompt_w"], existing["prompt_h"],
                                )
                                if (existing["detector_model"]
                                        == entry["detector_model"]
                                        and cached_prompt == entry["prompt"]
                                        and existing["path"]
                                        and os.path.isfile(existing["path"])):
                                    # The cheap skip (re-activate only) is correct
                                    # ONLY when the photos row is already fully
                                    # consistent for this variant. set_active_mask_
                                    # variant denormalises this variant's mask
                                    # features, but it does NOT touch the
                                    # dino_* embeddings — those still describe
                                    # whatever mask was active when they were last
                                    # computed. If the row is currently active on a
                                    # different SAM variant (e.g. two workspaces
                                    # share a folder but use sam2-small vs -large),
                                    # re-activating would leave the denormalised
                                    # mask features describing this variant while
                                    # the subject embedding was cropped from the
                                    # other variant's mask. regroup reads both off
                                    # the photos row, so it would mix them. Only
                                    # skip when active_mask_variant AND
                                    # dino_embedding_variant already match; else
                                    # fall through to the full recompute, which
                                    # writes set_active_mask_variant +
                                    # update_photo_embeddings together.
                                    state = thread_db.conn.execute(
                                        "SELECT active_mask_variant, "
                                        "dino_embedding_variant FROM photos "
                                        "WHERE id = ?",
                                        (photo_id,),
                                    ).fetchone()
                                    if (state is not None
                                            and state["active_mask_variant"]
                                            == sam2_variant
                                            and state["dino_embedding_variant"]
                                            == dinov2_variant):
                                        masked += 1
                                        processed = i + 1
                                        i += 1
                                        continue

                            # Past the cache check, so this photo genuinely
                            # needs a source read. If its folder was already
                            # proven unreachable, account for it without
                            # paying for a read we know will fail — but only
                            # here, after the cache branch: a cached mask
                            # only stats the local mask file, so it succeeds
                            # even when the source volume is gone and must
                            # still count as masked (Codex #1392 P2). Progress
                            # is pushed too, otherwise the bar freezes on the
                            # photo the outage started on while the stage
                            # walks the rest of the worklist.
                            if photo["folder_id"] in em_offline_folder_ids:
                                em_unreadable += 1
                                em_offline_unreadable += 1
                                processed = i + 1
                                stages["extract_masks"]["count"] = processed
                                runner.update_step(
                                    job["id"], "extract_masks",
                                    progress={
                                        "current": processed, "total": total,
                                    },
                                    error_count=em_failed + em_unreadable,
                                )
                                i += 1
                                continue

                            # First true cache miss: ensure SAM2 + DINOv2 weights
                            # are present before render_proxy/generate_mask runs.
                            # No-op on subsequent iterations.
                            _ensure_weights()

                            proxy = render_proxy(image_path, longest_edge=proxy_longest_edge)
                            if proxy is None:
                                # The source didn't load. Ask how far the
                                # problem reaches before deciding whether to
                                # keep going: one corrupt RAW is this photo's
                                # failure, but a share that dropped mid-run
                                # will fail every remaining read the same way.
                                em_unreadable += 1
                                offline = _source_offline_reason(
                                    folder_path, image_path,
                                )
                                if offline is not None:
                                    _scope, reason = offline
                                    em_offline_unreadable += 1
                                    if reason not in em_offline_reasons:
                                        em_offline_reasons.append(reason)
                                    # Latch the folder, not the run. A
                                    # mount-scoped outage proves the photos
                                    # on that volume are gone, but a
                                    # collection can span several volumes and
                                    # the local disk — writing off everything
                                    # still unprocessed would skip masks we
                                    # could have made and file those photos
                                    # under an outage that never touched them
                                    # (Codex #1392 P1). Other folders cost one
                                    # failed read each to discover, which is
                                    # nothing next to a wrongly abandoned run.
                                    em_offline_folder_ids.add(
                                        photo["folder_id"],
                                    )
                                    processed = i + 1
                                    i += 1
                                    continue
                                processed = i + 1
                                i += 1
                                continue
                            if _should_abort_without_pause(abort):
                                break

                            # GPU serialisation lives inside masking.generate_mask
                            # (around the encoder/decoder session.run calls). The
                            # wider wrap previously here held the semaphore through
                            # SAM weight load + image preprocessing + prompt-coord
                            # math, blocking other pipelines' GPU work for CPU-only
                            # phases.
                            mask = generate_mask(proxy, det_box, variant=sam2_variant)
                            if mask is None:
                                skipped += 1
                                processed = i + 1
                                i += 1
                                continue
                            if _should_abort_without_pause(abort):
                                break

                            completeness = crop_completeness(mask)
                            features = compute_all_quality_features(proxy, mask)
                            if _should_abort_without_pause(abort):
                                break

                            # Per-mask features (move from photos row into
                            # photo_masks; set_active_mask_variant denormalizes
                            # them back into photos for downstream readers).
                            mask_subject_tenengrad = features.pop(
                                "subject_tenengrad", None,
                            )
                            mask_bg_tenengrad = features.pop("bg_tenengrad", None)
                            # Mask-derived subject_size: fraction of frame
                            # covered by the boolean mask. Replaces the
                            # detection-bbox approximation classify uses.
                            total_pixels = float(mask.size)
                            if total_pixels > 0:
                                mask_subject_size = float(
                                    np.count_nonzero(mask) / total_pixels
                                )
                            else:
                                mask_subject_size = None

                            # GPU serialisation lives inside dino_embed.embed /
                            # embed_batch (around the session.run call). The wider
                            # wrap previously here held the semaphore through
                            # per-image resize/normalize preprocessing.
                            subject_crop = crop_subject(proxy, mask, margin=0.15)
                            if subject_crop is not None:
                                embs = embed_batch(
                                    [subject_crop, proxy], variant=dinov2_variant,
                                )
                                subj_emb_blob = embedding_to_blob(embs[0])
                                global_emb_blob = embedding_to_blob(embs[1])
                            else:
                                subj_emb_blob = None
                                global_emb_blob = embedding_to_blob(
                                    embed(proxy, variant=dinov2_variant),
                                )

                            mask_file_stage = _StagedMaskFile.create(
                                mask,
                                masks_dir,
                                photo_id,
                                sam2_variant,
                                save_mask,
                                previous_path=(
                                    existing["path"] if existing else None
                                ),
                            )
                            mask_path = mask_file_stage.final_path
                            thread_db.upsert_photo_mask(
                                photo_id=photo_id,
                                variant=sam2_variant,
                                path=mask_path,
                                detector_model=entry["detector_model"],
                                prompt_x=entry["prompt"][0],
                                prompt_y=entry["prompt"][1],
                                prompt_w=entry["prompt"][2],
                                prompt_h=entry["prompt"][3],
                                subject_size=mask_subject_size,
                                subject_tenengrad=mask_subject_tenengrad,
                                bg_tenengrad=mask_bg_tenengrad,
                                crop_complete=completeness,
                                _commit=False,
                            )
                            thread_db.set_active_mask_variant(
                                photo_id, sam2_variant, _commit=False,
                            )
                            # Remaining (non-mask) per-photo features still land
                            # on the photos row.  mask_path / crop_complete /
                            # subject_tenengrad / bg_tenengrad now flow via
                            # set_active_mask_variant above, so they are
                            # intentionally NOT passed here.
                            if features:
                                thread_db.update_photo_pipeline_features(
                                    photo_id, **features, _commit=False,
                                )
                            thread_db.update_photo_embeddings(
                                photo_id,
                                dino_subject_embedding=subj_emb_blob,
                                dino_global_embedding=global_emb_blob,
                                variant=dinov2_variant,
                                _commit=False,
                            )
                            # Publish a new immutable PNG, then atomically move
                            # the mask row, active denormalized fields, quality
                            # features, and embeddings to that generation. The
                            # predecessor remains readable until the commit;
                            # finish removes it only after the database points
                            # at the new path.
                            mask_file_stage.install()
                            commit_with_retry(thread_db.conn)
                            mask_file_stage.finish()
                            mask_file_stage = None
                            masked += 1
                    except ResourceWaitCancelled:
                        # The bound non-parking probe raised this because a
                        # pause OR cancellation is pending. The per-photo
                        # mask lock has already been released by exiting
                        # the ``with`` above. If cancellation is what fired,
                        # propagate to end the stage; otherwise park at a
                        # safe outer boundary (no locks held) and retry the
                        # same photo after resume — do NOT advance ``i``.
                        if _cancellation_requested():
                            raise
                        _pause_checkpoint()
                        if _cancellation_requested():
                            raise
                        continue
                    except Exception:
                        em_failed += 1
                        log.warning("Mask extraction failed for photo %s", photo_id, exc_info=True)
                        # A failed write can leave this connection inside a
                        # stale WAL snapshot. Without a rollback, every later
                        # photo fails immediately with the same ``database is
                        # locked`` error even after the competing writer has
                        # moved on.
                        try:
                            _rollback_failed_mask_photo(thread_db, photo_id)
                        finally:
                            if mask_file_stage is not None:
                                mask_file_stage.restore()

                    processed = i + 1
                    stages["extract_masks"]["count"] = processed
                    stages["extract_masks"]["total"] = total
                    runner.update_step(job["id"], "extract_masks",
                                       progress={"current": processed, "total": total},
                                       error_count=em_failed + em_unreadable)
                    _emit_progress(
                        runner, job["id"], stages, "extract_masks",
                        "Extracting features (SAM2 + DINOv2)",
                        rate=round(processed / max(time.time() - start_time, 0.01) * 60, 1),
                    )
                    i += 1

                if _should_abort(abort):
                    # Distinguish a user cancel from a clean completion: pin a
                    # "Cancelled" summary on the final step update so the job
                    # tree doesn't report a half-done stage as if it ran to
                    # term. Mirrors the classify-cancel path PR #710 added.
                    stages["extract_masks"]["status"] = "completed"
                    em_summary = (
                        f"Cancelled ({processed} of {total} processed)"
                        if total else "Cancelled"
                    )
                    runner.update_step(
                        job["id"], "extract_masks", status="completed",
                        progress={"current": processed, "total": total},
                        summary=em_summary,
                        # Warnings on the Jobs page render off the step's
                        # ``error_count`` — the mid-loop updates and the
                        # normal finalizer both include unreadable counts,
                        # so the cancel branch has to as well or a cancel
                        # that arrives after a source dropped shows a
                        # zero-warning row alongside a result that records
                        # positive ``unreadable`` (Codex #1392 P2
                        # r3687499188).
                        error_count=(
                            em_failed + em_unreadable
                            + em_preflight_unreadable
                        ),
                    )
                    result["stages"]["extract_masks"] = {
                        "masked": masked + em_preflight_masked,
                        "skipped": skipped, "failed": em_failed,
                        "unreadable": em_unreadable + em_preflight_unreadable,
                        # Preflight-inclusive, matching the normal finalizer:
                        # the loop total alone can't cover photos that never
                        # reached the loop (Codex #1392 P2).
                        "total": (
                            total + em_preflight_unreadable
                            + em_preflight_masked
                        ),
                        "cancelled": True,
                    }
                else:
                    # Reported count spans both the photos this loop failed to
                    # read and the ones the pre-flight dropped; the rollup
                    # messages below stay keyed on the in-loop count because
                    # the pre-flight branch already appended its own error.
                    em_unreadable_all = em_unreadable + em_preflight_unreadable
                    # ``total`` counts only what reached the loop, so it can't
                    # be the denominator once pre-flight drops are folded into
                    # the outcome counts — that publishes impossible tallies
                    # like "3 unreadable of 0" and breaks any consumer deriving
                    # coverage from them (Codex #1392 P2). Progress reporting
                    # keeps using the loop-scoped ``total``.
                    em_masked_all = masked + em_preflight_masked
                    em_total_candidates = (
                        total + em_preflight_unreadable + em_preflight_masked
                    )
                    final_status = (
                        "failed"
                        if em_failed > 0 or em_unreadable_all > 0
                        or em_offline_latched
                        else "completed"
                    )
                    stages["extract_masks"]["status"] = final_status
                    # Source outages own the headline: "reconnect this volume"
                    # is the actionable message, and its Fatal prefix keeps
                    # the end-of-run summary from picking a lesser warning.
                    # Each cause reports only the photos it actually explains
                    # — a corrupt file in a healthy folder is not recovered
                    # by reconnecting a drive.
                    em_other_unreadable = em_unreadable - em_offline_unreadable
                    em_fatal_msg = None
                    if em_offline_unreadable > 0:
                        em_fatal_msg = (
                            f"[extract_masks] Fatal: {em_offline_unreadable} "
                            f"of {em_total_candidates} photos unreachable — "
                            f"{'; '.join(em_offline_reasons)}. Reconnect the "
                            f"source and run Process again; until then these "
                            f"photos have no mask and Process Review rejects "
                            f"them as `no_subject_mask`."
                        )
                    em_secondary_rollups = []
                    if em_other_unreadable > 0:
                        em_secondary_rollups.append(
                            f"[extract_masks] {em_other_unreadable} of "
                            f"{em_total_candidates} photos could not be read, "
                            f"so they have no mask and Process Review rejects "
                            f"them as `no_subject_mask`."
                        )
                    if em_failed > 0:
                        em_secondary_rollups.append(
                            f"[extract_masks] {em_failed} of {em_total_candidates} photos "
                            f"failed mask extraction"
                        )
                    # Emit the actionable Fatal last so any collapse-by-stage
                    # consumer that keeps whichever entry appeared last for
                    # each stage still sees the reconnect instruction rather
                    # than the generic "failed mask extraction" rollup
                    # (Codex #1392 P2 r3687118058). The Extract card and
                    # top-level banner already prefer Fatal explicitly; this
                    # protects other listeners (job event streams, exports)
                    # from the same silent-last-wins collapse.
                    errors.extend(em_secondary_rollups)
                    if em_fatal_msg:
                        errors.append(em_fatal_msg)
                    em_rollup = em_fatal_msg or (
                        em_secondary_rollups[0]
                        if em_secondary_rollups else None
                    )
                    em_summary_parts = [
                        f"{em_masked_all} masked", f"{skipped} skipped",
                    ]
                    if em_unreadable_all:
                        em_summary_parts.append(
                            f"{em_unreadable_all} unreadable",
                        )
                    if em_failed:
                        em_summary_parts.append(f"{em_failed} failed")
                    if photos_subthreshold_only > 0:
                        em_summary_parts.append(
                            f"{photos_subthreshold_only} below detector_confidence "
                            f"({detector_confidence})"
                        )
                    runner.update_step(job["id"], "extract_masks", status=final_status,
                                       summary=", ".join(em_summary_parts),
                                       error_count=em_failed + em_unreadable_all,
                                       error=em_rollup)
                    result["stages"]["extract_masks"] = {
                        "masked": em_masked_all, "skipped": skipped,
                        "failed": em_failed,
                        "unreadable": em_unreadable_all,
                        "total": em_total_candidates,
                        "subthreshold": photos_subthreshold_only,
                    }
            except ResourceWaitCancelled:
                # A cancelled resource wait is a cooperative stage cancel,
                # not a mask-processing failure. Mirror the normal abort
                # finalizer above because this exception exits the per-photo
                # loop before that branch is reached.
                abort.set()
                stages["extract_masks"]["status"] = "completed"
                runner.update_step(
                    job["id"], "extract_masks", status="completed",
                    progress={"current": processed, "total": total},
                    summary=(
                        f"Cancelled ({processed} of {total} processed)"
                        if total else "Cancelled"
                    ),
                    error_count=(
                        em_failed + em_unreadable
                        + em_preflight_unreadable
                    ),
                )
                result["stages"]["extract_masks"] = {
                    "masked": masked + em_preflight_masked,
                    "skipped": skipped,
                    "failed": em_failed,
                    "unreadable": (
                        em_unreadable + em_preflight_unreadable
                    ),
                    "total": (
                        total + em_preflight_unreadable
                        + em_preflight_masked
                    ),
                    "cancelled": True,
                }
            except Exception as e:
                errors.append(f"[extract_masks] Fatal: {e}")
                log.exception("Pipeline extract-masks stage failed")
                stages["extract_masks"]["status"] = "failed"
                runner.update_step(job["id"], "extract_masks", status="failed", error=str(e))

            _update_stages(runner, job["id"], stages)

        def eye_keypoints_stage():
            """Run per-photo eye keypoint detection between mask extraction and scoring.

            No-op when the stage is disabled by config, when no SuperAnimal
            weights are on disk (users opt-in via the pipeline models card),
            or when no eligible photos remain. Per-photo failures are logged
            and do not abort the stage.
            """
            if (
                params.skip_eye_keypoints
                or params.skip_extract_masks
                or abort.is_set()
                or not collection_id
            ):
                stages["eye_keypoints"]["status"] = "skipped"
                runner.update_step(
                    job["id"], "eye_keypoints", status="completed", summary="Skipped",
                )
                return

            stages["eye_keypoints"]["status"] = "running"
            runner.update_step(job["id"], "eye_keypoints", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                import config as cfg
                from pipeline import (
                    _resolve_collection_photo_ids,
                    detect_eye_keypoints_stage,
                    eye_keypoint_stage_preflight,
                )

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)
                effective_cfg = thread_db.get_effective_config(cfg.load())
                pipeline_cfg = dict(effective_cfg.get("pipeline", {}))

                # Apply the per-run eye-detect override only when the caller
                # sent an explicit signal. ``skip_eye_keypoints=False`` alone
                # is not proof of opt-in: ``the "Full" saved process`` sets it
                # to False as a base default, so an after-import ``full``
                # chain would otherwise force ``eye_detect_enabled=True``
                # against a workspace whose Settings default is False (the
                # new default) — triggering SuperAnimal downloads and eye-
                # based scoring by default. ``eye_detect_override`` is the
                # explicit signal the Process page sends alongside its
                # checkbox state; strategy expansion leaves it None, so a
                # chained ``full`` run respects the user's Settings value.
                if params.eye_detect_override is not None:
                    pipeline_cfg["eye_detect_enabled"] = params.eye_detect_override

                # Mirror the stage-level preflight so a no-op run doesn't pay the
                # O(N) eligibility join cost or report a misleading
                # "0 of N processed" summary on large libraries.
                skip_reason = eye_keypoint_stage_preflight(pipeline_cfg)
                if skip_reason is not None:
                    stages["eye_keypoints"]["status"] = "skipped"
                    runner.update_step(
                        job["id"], "eye_keypoints",
                        status="completed", summary=f"Skipped — {skip_reason}",
                    )
                    result["stages"]["eye_keypoints"] = {
                        "processed": 0, "total": 0, "skipped": skip_reason,
                    }
                    _update_stages(runner, job["id"], stages)
                    return

                collection_photo_ids = (
                    _resolve_collection_photo_ids(thread_db, collection_id)
                    if collection_id is not None else None
                )
                # Honor preview-deselection so the eye stage matches the set of
                # photos extract/regroup will act on. Without this the stage
                # mutates eye_* for unchecked photos and those values are locked
                # in by the eye_tenengrad IS NULL idempotency guard on reruns.
                if params.exclude_photo_ids and collection_photo_ids is not None:
                    collection_photo_ids = {
                        pid for pid in collection_photo_ids
                        if pid not in params.exclude_photo_ids
                    }
                photos_for_stage = thread_db.list_photos_for_eye_keypoint_stage(
                    photo_ids=collection_photo_ids,
                )
                # Defensive second filter: when collection_photo_ids is None
                # (whole-workspace path) the DB query above returned every
                # eligible row, so excluded IDs would otherwise still influence
                # the download planner below and trigger weights for variants no
                # included photo routes to.
                if params.exclude_photo_ids:
                    photos_for_stage = [
                        p for p in photos_for_stage
                        if p["id"] not in params.exclude_photo_ids
                    ]

                # Drop photos whose folder is offline. eye_keypoints also
                # opens the source image (via the pipeline
                # detect_eye_keypoints_stage → keypoint runners), so
                # without this it would walk every unreachable photo and
                # record them as eye-detection failures — the same
                # downstream-hammering pattern the classify pause is
                # meant to prevent (Codex #1388 P2 r3664058173).
                #
                # Always probe the worklist's folders — not just when
                # classify populated ``source_skipped_photo_ids`` (Codex
                # #1388 P1 r3664891993). A fully-cached-classify /
                # eye-only rerun makes no image reads in classify, so
                # the seed set can stay empty even when every remaining
                # file is on an unreachable share.
                #
                # Filter by FOLDER, not by photo id — cached photos
                # never populate the classify seed set, so an ID-only
                # filter would leave them in the worklist (Codex #1388
                # P2 r3664694179). Re-probing per folder also lets a
                # folder that recovered between classify and this stage
                # rejoin (Codex #1388 P2 r3664348758).
                def _row_folder_id(row):
                    # sqlite.Row raises IndexError on missing columns; a
                    # test-shape dict raises KeyError. Both mean "no
                    # folder to probe" for this row.
                    try:
                        fid = row["folder_id"]
                    except (KeyError, IndexError):
                        return None
                    return fid

                worklist_folder_ids = {
                    fid for fid in (
                        _row_folder_id(p) for p in photos_for_stage
                    ) if fid is not None
                }
                still_offline_folder_ids = _still_offline_folder_ids_of(
                    thread_db, worklist_folder_ids,
                )
                if still_offline_folder_ids:
                    dropped_ids = {
                        p["id"] for p in photos_for_stage
                        if _row_folder_id(p) in still_offline_folder_ids
                    }
                    photos_for_stage = [
                        p for p in photos_for_stage
                        if _row_folder_id(p) not in still_offline_folder_ids
                    ]
                    # The stage-level call at the bottom re-resolves
                    # photos from ``collection_id`` and filters via
                    # ``exclude_photo_ids`` — the local filter above
                    # only drives the weight-download planner and
                    # ``total``. Merge the offline IDs into the stage's
                    # exclusion set so the actual keypoint runners
                    # don't reopen the dead source (CodeRabbit
                    # r3664548813).
                    existing_exclude = (
                        set(params.exclude_photo_ids)
                        if params.exclude_photo_ids else set()
                    )
                    params.exclude_photo_ids = (
                        existing_exclude | dropped_ids
                    )
                    # Publish for anyone downstream that may probe
                    # ``source_offline_state`` later.
                    prior_skipped = (
                        source_offline_state.get("skipped_photo_ids")
                        or set()
                    )
                    source_offline_state["skipped_photo_ids"] = (
                        set(prior_skipped) | dropped_ids
                    )
                    if dropped_ids:
                        log.warning(
                            "Eye-keypoints: dropped %d photo(s) from "
                            "%d offline folder(s); source not "
                            "reachable.",
                            len(dropped_ids),
                            len(still_offline_folder_ids),
                        )
                total = len(photos_for_stage)
                start_time = time.time()
                processed = {"count": 0}

                def _progress(phase, current, total_steps):
                    processed["count"] = current
                    stages["eye_keypoints"]["count"] = current
                    stages["eye_keypoints"]["total"] = total_steps
                    runner.update_step(
                        job["id"], "eye_keypoints",
                        progress={"current": current, "total": total_steps},
                    )
                    _emit_progress(
                        runner, job["id"], stages, "eye_keypoints", phase,
                        rate=round(
                            current / max(time.time() - start_time, 0.01) * 60, 1
                        ),
                    )

                # Auto-download SuperAnimal weights on first pipeline run.
                # Mirrors the SAM2/DINOv2 auto-download pattern in extract_masks
                # (commit 90cd0f9): without this, every photo silently skips on
                # a fresh install. Only fetch variants the per-photo router
                # would actually pick — a collection of out-of-scope classes
                # (fish/reptiles/invertebrates) shouldn't pay the bandwidth
                # cost for weights that will never be used.
                if total > 0:
                    import keypoints as kp
                    from pipeline import _resolve_keypoint_model

                    # Mirror Gate 1 in _process_photo_for_eye: rows whose
                    # classifier confidence is below eye_classifier_conf_gate
                    # get skipped at run time, so they shouldn't influence
                    # which variants get downloaded — otherwise an all-low-
                    # confidence collection still pays the bandwidth cost
                    # for weights no photo can reach.
                    conf_gate = pipeline_cfg.get(
                        "eye_classifier_conf_gate", 0.5,
                    )
                    needed_models = []
                    for row in photos_for_stage:
                        if (row.get("species_conf") or 0.0) < conf_gate:
                            continue
                        model_name = _resolve_keypoint_model(thread_db, row)
                        if model_name and model_name not in needed_models:
                            needed_models.append(model_name)

                    # Use a separate download-progress callback so a cancel
                    # during/just after weight download doesn't leak the
                    # download counter into `processed['count']` (which would
                    # surface as e.g. "Cancelled (1 of N processed)" before any
                    # photo has actually been touched).
                    def _dl_progress(phase, current, total_steps):
                        _emit_progress(
                            runner, job["id"], stages, "eye_keypoints", phase,
                        )

                    # Preserve a stable order (quadruped, bird) for tests and
                    # log readability when both variants are needed. Re-check
                    # abort between models so a cancel that arrives after the
                    # first weights download can short-circuit the second
                    # multi-hundred-MB fetch instead of forcing the user to
                    # wait through it.
                    #
                    # Eye Keypoints is an optional stage: a transient HF /
                    # network failure must degrade to a skipped stage, not a
                    # hard pipeline failure. Without this guard the RuntimeError
                    # raised by ensure_keypoint_weights bubbles to the outer
                    # except, marks the stage 'failed', and tanks the whole
                    # run for a first-run/offline user who never opted into
                    # eye keypoints in the first place.
                    try:
                        for kp_model in (
                            "superanimal-quadruped", "superanimal-bird",
                        ):
                            if _should_abort(abort):
                                break
                            if kp_model in needed_models:
                                kp.ensure_keypoint_weights(
                                    kp_model, progress_callback=_dl_progress,
                                )
                    except ResourceWaitCancelled:
                        # A cancel or pause that fires while another thread
                        # holds the per-model download lock surfaces here
                        # as ResourceWaitCancelled from
                        # ``acquire_session_cache_lock`` — that is a
                        # cooperative cancel, not a download failure.
                        # Finalize the stage as cancelled the same way the
                        # extract_masks path handles its own
                        # ResourceWaitCancelled (line ~7293) so the job
                        # tree does not report "failed to download
                        # keypoint weights" for what was actually a user
                        # cancel.
                        abort.set()
                        stages["eye_keypoints"]["status"] = "completed"
                        runner.update_step(
                            job["id"], "eye_keypoints",
                            status="completed",
                            summary="Cancelled",
                        )
                        result["stages"]["eye_keypoints"] = {
                            "processed": 0, "total": total,
                            "cancelled": True,
                        }
                        _update_stages(runner, job["id"], stages)
                        return
                    except Exception as dl_err:
                        log.warning(
                            "Eye keypoints stage skipped — weight download "
                            "failed: %s", dl_err,
                        )
                        errors.append(f"[eye_keypoints] {dl_err}")
                        stages["eye_keypoints"]["status"] = "skipped"
                        runner.update_step(
                            job["id"], "eye_keypoints",
                            status="completed",
                            summary=(
                                f"Skipped — failed to download keypoint "
                                f"weights: {dl_err}"
                            ),
                        )
                        result["stages"]["eye_keypoints"] = {
                            "processed": 0, "total": total,
                            "skipped": "weight_download_failed",
                        }
                        _update_stages(runner, job["id"], stages)
                        return

                detect_eye_keypoints_stage(
                    thread_db, config=pipeline_cfg, progress_callback=_progress,
                    collection_id=collection_id,
                    exclude_photo_ids=params.exclude_photo_ids,
                    abort_check=lambda: _should_abort(abort),
                )

                stages["eye_keypoints"]["status"] = "completed"
                if _should_abort(abort):
                    # Match the classify- and extract_masks-cancel summaries so
                    # the job tree distinguishes a user cancel from a clean
                    # finish that happened to process the same count.
                    summary = (
                        f"Cancelled ({processed['count']} of {total} processed)"
                        if total else "Cancelled"
                    )
                    runner.update_step(
                        job["id"], "eye_keypoints",
                        status="completed", summary=summary,
                    )
                    result["stages"]["eye_keypoints"] = {
                        "processed": processed["count"], "total": total,
                        "cancelled": True,
                    }
                else:
                    summary = (
                        f"{processed['count']} of {total} photos processed"
                        if total else "No eligible photos"
                    )
                    runner.update_step(
                        job["id"], "eye_keypoints",
                        status="completed", summary=summary,
                    )
                    result["stages"]["eye_keypoints"] = {
                        "processed": processed["count"], "total": total,
                    }
            except ResourceWaitCancelled:
                # ``detect_eye_keypoints_stage`` catches ``ResourceWaitCancelled``
                # per-photo in ``pipeline.py`` today, so this outer handler is
                # currently unreachable from the per-photo inference path. Keep
                # it as defense-in-depth: a future regression in that inner
                # handler — or a fresh call site inside this outer ``try``
                # (e.g. a cache-lock guard reached before the download loop) —
                # would otherwise get caught below as ``Exception`` and be
                # recorded as "[eye_keypoints] Fatal", inflating the failure
                # count for what was actually a cooperative cancel. Mirror the
                # ``extract_masks`` handler at line ~7293 so the summary lines
                # up with the other stages' cancel outcomes.
                abort.set()
                stages["eye_keypoints"]["status"] = "completed"
                summary = (
                    f"Cancelled ({processed['count']} of {total} processed)"
                    if total else "Cancelled"
                )
                runner.update_step(
                    job["id"], "eye_keypoints",
                    status="completed", summary=summary,
                )
                result["stages"]["eye_keypoints"] = {
                    "processed": processed["count"], "total": total,
                    "cancelled": True,
                }
            except Exception as e:
                errors.append(f"[eye_keypoints] Fatal: {e}")
                log.exception("Pipeline eye-keypoints stage failed")
                stages["eye_keypoints"]["status"] = "failed"
                runner.update_step(
                    job["id"], "eye_keypoints", status="failed", error=str(e),
                )

            _update_stages(runner, job["id"], stages)

        def regroup_stage():
            """Run pipeline grouping + scoring + triage from cached features."""
            if params.skip_regroup:
                # Only take the species-review save path when the caller
                # explicitly asked for it (the identify preset sets
                # ``review_mode="species"`` via process_strategies). A
                # classify-only run without that opt-in — Advanced/Custom
                # on the Process page, or an API client sending
                # ``skip_regroup: true`` — must NOT overwrite
                # ``pipeline_results_ws*.json`` with all-REVIEW species
                # output, since that would silently reintroduce the
                # culling-pipeline downgrade the reviewer flagged (the
                # user just wanted to refresh classifications, not turn
                # the workspace cache into a species-review cache).
                do_species = (
                    params.review_mode == "species"
                    and not abort.is_set()
                    and collection_id
                    and not params.skip_classify
                )
                if not do_species:
                    stages["regroup"]["status"] = "skipped"
                    runner.update_step(job["id"], "regroup", status="completed",
                                       summary="Skipped")
                    # Emit a progress event so the SSE stream (and tests
                    # asserting on the last progress payload) can see the
                    # stage's terminal "skipped" state. Without this, a
                    # downstream miss_stage that also short-circuits
                    # leaves the last stages dict stuck at whatever the
                    # detect/classify stage emitted last.
                    _update_stages(runner, job["id"], stages)
                    return
                try:
                    import config as cfg
                    from pipeline import (
                        load_photo_features,
                        run_species_review_pipeline,
                        save_results,
                    )

                    thread_db = Database(db_path)
                    thread_db.set_active_workspace(workspace_id)

                    effective_cfg = thread_db.get_effective_config(cfg.load())
                    pipeline_cfg = effective_cfg.get("pipeline", {})

                    photos = load_photo_features(
                        thread_db,
                        collection_id=collection_id,
                        config=effective_cfg,
                    )
                    if params.exclude_photo_ids:
                        photos = [
                            p for p in photos
                            if p["id"] not in params.exclude_photo_ids
                        ]
                    if not photos:
                        result["stages"]["review"] = {
                            "error": "No photos with pipeline features found.",
                        }
                    else:
                        results = run_species_review_pipeline(
                            photos,
                            config=pipeline_cfg,
                            emit_trace=True,
                        )
                        cache_dir = os.path.dirname(db_path)
                        # Don't preserve miss_computed_at from any prior
                        # full run. The identify strategy skips the miss
                        # stage entirely, so the cache we're writing has
                        # no misses of its own; carrying the old marker
                        # forward would make Pipeline Review call
                        # /api/misses?since=<old marker> and render miss
                        # rows from the previous full run as if they were
                        # produced by this identify pass.
                        save_results(
                            results,
                            cache_dir,
                            workspace_id,
                            preserve_miss_marker=False,
                        )
                        # The species-only pipeline overwrites
                        # pipeline_results_ws*.json with review-only output
                        # (no burst/keep/reject scoring). If a prior full
                        # regroup left a valid last_group_fingerprint stamped
                        # on the workspace, pipeline_plan._group_plan would
                        # match it against current settings and report
                        # "done-prior" — silently letting an advanced Group
                        # & Score run be skipped even though the cache no
                        # longer contains any triage output. Invalidate the
                        # stamp so a subsequent full run is correctly shown
                        # as will-run.
                        thread_db.set_workspace_group_state(
                            workspace_id=workspace_id,
                            fingerprint=None,
                            when_ts=None,
                        )
                        result["stages"]["review"] = results.get("summary", {})

                    stages["regroup"]["status"] = "completed"
                    runner.update_step(
                        job["id"], "regroup",
                        status="completed",
                        summary="Review results ready" if photos else "No photos to group",
                    )
                except Exception as e:
                    errors.append(f"[review] Fatal: {e}")
                    log.exception("Pipeline species-review stage failed")
                    stages["regroup"]["status"] = "failed"
                    runner.update_step(
                        job["id"], "regroup", status="failed", error=str(e),
                    )
                _update_stages(runner, job["id"], stages)
                return

            if abort.is_set() or not collection_id:
                stages["regroup"]["status"] = "skipped"
                runner.update_step(job["id"], "regroup", status="completed",
                                   summary="Skipped")
                return

            stages["regroup"]["status"] = "running"
            runner.update_step(job["id"], "regroup", status="running")
            _update_stages(runner, job["id"], stages)

            # The per-workspace regroup lock is now acquired by the
            # orchestrator (see run_pipeline_job body) so it spans BOTH
            # regroup_stage and miss_stage atomically — the inner lock
            # that used to live here would deadlock against the outer one
            # (Python locks aren't reentrant). The deferred-update pattern
            # likewise goes away: runner.update_step is fine to call here
            # because nothing under JobRunner._lock acquires the workspace
            # regroup lock, so there's no cycle to invert.
            try:
                import config as cfg
                from pipeline import load_photo_features, run_full_pipeline, save_results

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective_cfg = thread_db.get_effective_config(cfg.load())
                pipeline_cfg = dict(effective_cfg.get("pipeline", {}))

                # Mirror the eye_keypoints_stage per-run override so scoring
                # honors the same explicit intent. Without carrying the
                # override into pipeline_cfg here, ``run_full_pipeline``
                # reloads workspace config with ``eye_detect_enabled=False``
                # (the new default) and ``score_encounter`` ignores the
                # ``eye_tenengrad`` values the eye stage just wrote — so the
                # visible checkbox would affect only the expensive keypoint
                # pass, not the culling result the user actually sees.
                # Gated on ``eye_detect_override`` (an explicit per-run
                # signal), NOT on ``not skip_eye_keypoints``, because the
                # latter is False by default in ``the "Full" saved process``
                # too — using it would force eye scoring on for any chained
                # ``full`` run regardless of workspace Settings.
                if params.eye_detect_override is not None:
                    pipeline_cfg["eye_detect_enabled"] = params.eye_detect_override

                photos = load_photo_features(thread_db, collection_id=collection_id, config=effective_cfg)
                if params.exclude_photo_ids:
                    photos = [p for p in photos if p["id"] not in params.exclude_photo_ids]
                if not photos:
                    result["stages"]["regroup"] = {"error": "No photos with pipeline features found."}
                    stages["regroup"]["status"] = "completed"
                    runner.update_step(
                        job["id"], "regroup",
                        status="completed", summary="No photos to group",
                    )
                else:
                    results = run_full_pipeline(photos, config=pipeline_cfg, emit_trace=True)
                    cache_dir = os.path.dirname(db_path)
                    save_results(results, cache_dir, workspace_id)

                    # Stamp the grouping fingerprint + timestamp BEFORE marking
                    # the step completed, so a partial regroup that crashes
                    # between here and update_step doesn't end up labeled "fresh"
                    # with a stale fp.
                    #
                    # Only stamp when the regroup actually covered the whole
                    # workspace — if it ran on a filtered subset (a
                    # sub-collection, or with exclude_photo_ids set) some
                    # workspace photos were intentionally not regrouped, so
                    # claiming workspace-level freshness would let the pipeline
                    # page hide a real stale state.
                    from pipeline import (
                        _resolve_collection_photo_ids,
                        compute_group_fingerprint,
                    )
                    collection_photo_ids = _resolve_collection_photo_ids(
                        thread_db, collection_id,
                    )
                    ws_photo_ids = {
                        r["id"] for r in thread_db.conn.execute(
                            """SELECT p.id
                                 FROM photos p
                                 JOIN workspace_folders wf
                                   ON wf.folder_id = p.folder_id
                                WHERE wf.workspace_id = ?""",
                            (workspace_id,),
                        ).fetchall()
                    }
                    # A per-run eye override that differs from the
                    # workspace's own effective ``eye_detect_enabled`` means
                    # this run's KEEP/REJECT decisions came from scoring
                    # settings the workspace's normal state wouldn't produce.
                    # ``compute_group_fingerprint`` reads only encounter/burst
                    # keys — not ``eye_detect_enabled`` — so stamping it here
                    # would mark eye-scored (or eye-disabled) results as
                    # settings-fresh for a later plan run against the
                    # workspace's real settings. Treat that as a partial run
                    # so ``pipeline_plan`` reports the cache as needing to
                    # re-run instead of hiding the mismatch.
                    workspace_eye_setting = bool(
                        effective_cfg.get("pipeline", {}).get(
                            "eye_detect_enabled", False,
                        )
                    )
                    per_run_eye_override_differs = (
                        params.eye_detect_override is not None
                        and bool(params.eye_detect_override) != workspace_eye_setting
                    )
                    covered_full_workspace = (
                        not params.exclude_photo_ids
                        and ws_photo_ids.issubset(collection_photo_ids)
                        and not per_run_eye_override_differs
                    )
                    if covered_full_workspace:
                        thread_db.set_workspace_group_state(
                            workspace_id=workspace_id,
                            fingerprint=compute_group_fingerprint(effective_cfg),
                            when_ts=int(time.time()),
                        )
                    else:
                        # Partial run — save_results just clobbered
                        # pipeline_results_ws*.json with subset output, so any
                        # pre-existing fingerprint now points at a cache that
                        # no longer reflects the full workspace. Invalidate so
                        # the pipeline page surfaces the staleness as will-run
                        # instead of falsely reporting done-prior.
                        thread_db.set_workspace_group_state(
                            workspace_id=workspace_id,
                            fingerprint=None,
                            when_ts=None,
                        )

                    stages["regroup"]["status"] = "completed"
                    summary_info = results.get("summary", {})
                    groups = summary_info.get("groups", "")
                    runner.update_step(
                        job["id"], "regroup",
                        status="completed",
                        summary=f"{groups} groups" if groups else "Done",
                    )
                    result["stages"]["regroup"] = summary_info
            except Exception as e:
                errors.append(f"[regroup] Fatal: {e}")
                log.exception("Pipeline regroup stage failed")
                stages["regroup"]["status"] = "failed"
                runner.update_step(
                    job["id"], "regroup", status="failed", error=str(e),
                )
            _update_stages(runner, job["id"], stages)

        def miss_stage():
            """Compute miss-detection flags for the workspace after regroup.

            Runs last so burst_id is available. Uses only per-photo features
            already computed by earlier stages — no model inference.
            """
            # Skip when classify was skipped: classify_miss depends on fresh
            # detections/classifications written by the classify stage, and
            # without them it would mass-flag "no_subject" on photos whose
            # subjects simply weren't re-evaluated this run.
            #
            # Also skip when regroup failed: miss classification depends on
            # regroup's burst_id output, so running here after a regroup
            # failure would overwrite miss_* flags with stale context during
            # an already-failing job. regroup_stage marks itself "failed"
            # without setting abort, so check the stage status explicitly.
            if (
                params.skip_regroup
                or params.skip_classify
                or abort.is_set()
                or not collection_id
                or stages["regroup"].get("status") == "failed"
            ):
                stages["misses"]["status"] = "skipped"
                runner.update_step(job["id"], "misses", status="completed",
                                   summary="Skipped")
                return

            # Hoisted from the try: block below so the miss_enabled guard can
            # read effective config before the transient "running" status is
            # written.
            try:
                from datetime import UTC, datetime

                import config as cfg
                from misses import compute_misses_for_workspace
                from pipeline import load_results_raw, save_results_raw

                thread_db = Database(db_path)
                thread_db.set_active_workspace(workspace_id)

                effective_cfg = thread_db.get_effective_config(cfg.load())
                pipeline_cfg = effective_cfg.get("pipeline", {})
            except Exception as e:
                # Mark the stage failed BEFORE returning. The transient
                # "running" write is *below* this guard, so without stamping
                # "failed" here the stage would stay "pending"; the pipeline
                # finalizer treats absence-of-failed as success and would
                # wrongly mark the whole job completed despite a fatal setup
                # error (cfg.load, Database(...), or any import raising).
                stages["misses"]["status"] = "failed"
                runner.update_step(job["id"], "misses", status="failed",
                                   error=str(e))
                errors.append(f"[misses] Fatal: {e}")
                log.exception("Pipeline miss-detection setup failed")
                _update_stages(runner, job["id"], stages)
                return

            # Effective miss_enabled: per-run PipelineParams override wins
            # over workspace config, mirroring how other skip_* flags
            # override workspace defaults. Inject the effective value into
            # pipeline_cfg *before* the guard so both branches — the
            # short-circuit skip AND the fall-through to compute — see the
            # same value: compute_misses_for_workspace reads
            # pipeline_cfg["miss_enabled"] itself, so a strategy that
            # enables misses on a workspace where they're disabled would
            # otherwise get a silent 0 from compute.
            if params.miss_enabled is not None:
                pipeline_cfg = {**pipeline_cfg,
                                "miss_enabled": params.miss_enabled}
            miss_enabled = pipeline_cfg.get("miss_enabled", True)
            if not miss_enabled:
                # Do NOT fall through to compute_misses_for_workspace: it
                # returns 0 when disabled and the completion path would then
                # stamp "0 photos evaluated", which reads as "misses ran and
                # found none" rather than "misses were disabled". Skipping
                # here also leaves the miss_computed_at cache marker
                # unstamped, which pipeline_review's "current-run misses"
                # shortcut depends on.
                stages["misses"]["status"] = "skipped"
                runner.update_step(job["id"], "misses", status="completed",
                                   summary="Skipped")
                _update_stages(runner, job["id"], stages)
                return

            stages["misses"]["status"] = "running"
            runner.update_step(job["id"], "misses", status="running")
            _update_stages(runner, job["id"], stages)

            try:
                # Share one timestamp between the DB write and the saved
                # pipeline-results cache so pipeline_review's "Review misses"
                # shortcut can gate on actual recomputation in this run and
                # scope /misses?since=... to exactly what was just written.
                now_ts = datetime.now(UTC).isoformat(timespec="microseconds")

                n = compute_misses_for_workspace(
                    thread_db,
                    pipeline_cfg,
                    collection_id=collection_id,
                    exclude_photo_ids=params.exclude_photo_ids,
                    now=now_ts,
                )

                stages["misses"]["status"] = "completed"
                stages["misses"]["count"] = n
                runner.update_step(job["id"], "misses", status="completed",
                                   summary=f"{n} photos evaluated")
                result["stages"]["misses"] = {"evaluated": n}

                # Mark the cached results so the review UI knows misses
                # were actually recomputed this run. Without this, the
                # shortcut would surface stale miss flags from a prior
                # run as "current-run misses" whenever miss_enabled=False
                # or the stage was skipped.
                if miss_enabled:
                    cache_dir = os.path.dirname(db_path)
                    cached = load_results_raw(cache_dir, workspace_id)
                    if cached is not None:
                        cached["miss_computed_at"] = now_ts
                        save_results_raw(cached, cache_dir, workspace_id)
            except Exception as e:
                errors.append(f"[misses] Fatal: {e}")
                log.exception("Pipeline miss-detection stage failed")
                stages["misses"]["status"] = "failed"
                runner.update_step(job["id"], "misses", status="failed", error=str(e))

            _update_stages(runner, job["id"], stages)

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

        # --- Launch threads ---

        threads = {}

        # Phase 1: scan + thumbnails + model loading (concurrent)
        phase_one = {
            "scanner": scanner_stage,
            "collection": collection_stage,
            "thumbnail": thumbnail_stage,
            "model_loader": model_loader_stage,
        }
        # Register the complete phase before starting its first thread.  If the
        # first worker reaches Pause immediately, it must wait for the other
        # three rather than declaring the whole pipeline paused on its own.
        pause_gate.register_many(phase_one)
        for name, stage_fn in phase_one.items():
            threads[name] = threading.Thread(
                target=_run_pause_participant,
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

        # Phase 1.5: previews (needs scan complete, runs before classify).
        #
        # This and every later stage are always invoked — even when `abort`
        # is set — so their step rows reach a terminal status. Gating the
        # call on abort would leave the runner.set_steps-created rows
        # persisted as "pending" with no finished_at, forever. Each stage
        # checks abort internally and marks itself "Skipped".
        _run_pause_participant("previews", previews_stage)

        # Phase 2: detect (needs collection; runs MegaDetector once across all
        # photos so each per-model classify step reuses cached detections
        # instead of re-running the detector).
        #
        # Always invoked — even when `abort` is set by an earlier stage — so
        # the `detect` step row reaches a terminal status. Skipping the call
        # would leave the row pending forever on a model-loader failure.
        # detect_stage handles abort internally and marks itself skipped.
        _run_pause_participant("detect", detect_stage)

        # Phase 3: classify per model (reads cached detections from detect_stage).
        # Always invoked for the same reason: every `classify:<model_id>` row
        # must land in a terminal state so the jobs tree finalizes cleanly on
        # a loader-triggered abort.
        _run_pause_participant("classify", classify_stage)

        # Phase 3: extract-masks (needs classify output)
        _run_pause_participant("extract_masks", extract_masks_stage)

        # Phase 3.5: eye keypoints (needs masks + classifier output). No-op when
        # SuperAnimal weights are absent — users opt in on the pipeline models
        # card. Per-photo failures log and continue rather than abort the stage.
        _run_pause_participant("eye_keypoints", eye_keypoints_stage)

        # Phases 4 + 5: regroup and miss detection. Held under the
        # per-workspace regroup lock TOGETHER so a concurrent same-workspace
        # pipeline can't slip a regroup_stage in between this run's
        # regroup_stage and its miss_stage — that would leave the persisted
        # miss flags + ``miss_computed_at`` paired with a grouping
        # (burst_id / pipeline_results_ws*.json) the miss computation never
        # saw. Pipelines targeting different workspaces share neither
        # stage's state and don't contend here.
        #
        # miss_stage's own gate covers the regroup-failed and abort cases, so
        # both stages can be invoked unconditionally and still reach a
        # terminal step status.
        def _run_regroup_and_misses():
            if abort.is_set():
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
            _pause_checkpoint()

        _run_pause_participant(
            "regroup_and_misses", _run_regroup_and_misses,
        )

        _run_pause_participant("archive", archive_stage)

        cancel_watcher_stop.set()

        elapsed = time.time() - job["_start_time"]
        result["duration"] = round(elapsed, 1)
        result["errors"] = list(errors)

        # If any stage ended in 'failed' and the job wasn't cancelled, propagate
        # the failure so JobRunner marks the whole job as failed rather than
        # silently recording it as completed. Cancellation takes precedence:
        # a cancelled job stays cancelled even if stages crashed on the way down.
        failed_stages = [
            name for name, s in stages.items() if s.get("status") == "failed"
        ]
        if failed_stages and not _cancellation_requested():
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

        return result
    finally:
        if archive_destination_reserved:
            release_archive_destination(final_destination)
