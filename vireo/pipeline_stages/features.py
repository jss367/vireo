"""Features stages for the streaming photo pipeline."""

import logging
import os
import time
from dataclasses import dataclass

import numpy as np
from db import commit_with_retry
from pipeline_locks import (
    acquire_photo_mask,
)
from pipeline_stages.context import PipelineRun
from resource_ledger import (
    ResourceWaitCancelled,
    bind_resource_cancel_check,
)

log = logging.getLogger(__name__)


def extract_masks_stage(
    run: PipelineRun,
    *,
    _StagedMaskFile,
    _extract_masks_early_exit,
    _filter_excluded,
    _preflight_mask_outcomes,
    _rollback_failed_mask_photo,
    _source_offline_reason,
    _still_offline_folder_ids_of,
    source_offline_state,
):
    """Run SAM2 mask extraction + DINOv2 embeddings after classify."""
    if run.params.skip_extract_masks or run.abort.is_set() or not run.collection_id:
        run.stages["extract_masks"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "extract_masks", status="completed",
                           summary="Skipped")
        return

    run.stages["extract_masks"]["status"] = "running"
    run.runner.update_step(run.job["id"], "extract_masks", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)

    masks = _MaskPass(
        run,
        staged_mask_file=_StagedMaskFile,
        extract_masks_early_exit=_extract_masks_early_exit,
        filter_excluded=_filter_excluded,
        preflight_mask_outcomes=_preflight_mask_outcomes,
        rollback_failed_mask_photo=_rollback_failed_mask_photo,
        source_offline_reason=_source_offline_reason,
        still_offline_folder_ids_of=_still_offline_folder_ids_of,
        source_offline_state=source_offline_state,
    )
    try:
        masks.setup()
        masks.drop_offline_folders()
        masks.map_primary_detections()
        masks.open_worklist()
        if masks.exit_without_mask_candidates():
            return
        masks.extract_all()
        if run.control.should_abort(run.abort):
            masks.finish_cancelled()
        else:
            masks.finalize()
    except ResourceWaitCancelled:
        # A cancelled resource wait is a cooperative stage cancel,
        # not a mask-processing failure. Mirror the normal abort
        # finalizer above because this exception exits the per-photo
        # loop before that branch is reached.
        run.abort.set()
        masks.finish_cancelled()
    except Exception as e:
        run.errors.append(f"[extract_masks] Fatal: {e}")
        log.exception("Pipeline extract-masks stage failed")
        run.stages["extract_masks"]["status"] = "failed"
        run.runner.update_step(run.job["id"], "extract_masks", status="failed", error=str(e))

    run.update_stages(run.runner, run.job["id"], run.stages)


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


# How ``_MaskPass._extract_photo`` tells the photo loop to move on:
# ``_NEXT_PHOTO`` advances to the next index without the per-photo
# progress update, ``_STOP`` ends the loop. ``None`` falls through to the
# progress update.
_NEXT_PHOTO = "next"
_STOP = "stop"


@dataclass
class _MaskPhoto:
    """One worklist photo: its source paths and in-flight mask generation."""

    entry: dict
    photo: object
    det_box: dict
    photo_id: int
    folder_path: str
    image_path: str
    existing: object = None
    mask_file_stage: object = None


class _MaskPass:
    """State shared across one extract_masks stage run."""

    def __init__(
        self,
        run,
        *,
        staged_mask_file,
        extract_masks_early_exit,
        filter_excluded,
        preflight_mask_outcomes,
        rollback_failed_mask_photo,
        source_offline_reason,
        still_offline_folder_ids_of,
        source_offline_state,
    ):
        self.run = run
        self.staged_mask_file = staged_mask_file
        self.extract_masks_early_exit = extract_masks_early_exit
        self.filter_excluded = filter_excluded
        self.preflight_mask_outcomes = preflight_mask_outcomes
        self.rollback_failed_mask_photo = rollback_failed_mask_photo
        self.source_offline_reason = source_offline_reason
        self.still_offline_folder_ids_of = still_offline_folder_ids_of
        self.source_offline_state = source_offline_state

        # Latched by the source-offline branch when the mask stage owns
        # the outage (fully-cached classify + offline masks folder). The
        # finalizer below preserves ``failed`` when this is set: without
        # it, ``em_failed`` is zero (offline photos are pre-filtered from
        # the worklist), and the finalizer flips the stage back to
        # ``completed`` — silently masking the outage. The end-of-run
        # rollup at ~L7005 reads only stage ``status`` values, so the job
        # would complete "successfully" with the missing masks folded
        # away in ``errors`` (Codex #1388 P1 r3665130244).
        self.em_offline_latched = False
        # Photos the pre-flight probe removed from the worklist because
        # their folder was already unreachable. They never reach the
        # per-photo loop, so without carrying the count forward every
        # counter reads zero on a stage that simultaneously reports
        # unreachable photos — which the Extract card then rendered as
        # "No photos needed masks" (Codex #1392 P2).
        self.em_preflight_unreadable = 0
        # Dropped photos that already carried a usable mask. They are a
        # successful outcome — the online cache-hit path counts exactly
        # this state as ``masked`` — so they belong in the counters
        # instead of vanishing from the stage's coverage entirely
        # (Codex #1392 P2).
        self.em_preflight_masked = 0
        # The pre-flight outage message, kept so an early exit can put it
        # on the failed step instead of a benign "no detections" summary.
        self.em_offline_preflight_error = None

    # -- setup -------------------------------------------------------

    def setup(self):
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

        self.embed = embed
        self.embed_batch = embed_batch
        self.embedding_to_blob = embedding_to_blob
        self.crop_completeness = crop_completeness
        self.crop_subject = crop_subject
        self.generate_mask = generate_mask
        self.render_proxy = render_proxy
        self.save_mask = save_mask
        self.compute_all_quality_features = compute_all_quality_features

        self.thread_db = self.run.database_factory(self.run.db_path)
        self.thread_db.set_active_workspace(self.run.workspace_id)

        self.effective_cfg = self.thread_db.get_effective_config(cfg.load())
        self.pipeline_cfg = self.effective_cfg.get("pipeline", {})
        self.sam2_variant = self.pipeline_cfg.get("sam2_variant")
        self.dinov2_variant = self.pipeline_cfg.get("dinov2_variant")
        self.proxy_longest_edge = self.pipeline_cfg.get("proxy_longest_edge")
        self.raw_session = None
        if self.run.params.raw_subject_analysis:
            from raw_analysis import RawAnalysisSession

            self.raw_session = RawAnalysisSession(
                max_size=self.proxy_longest_edge or 1536,
                sam2_variant=self.sam2_variant or "sam2-small",
            )

        self.masks_dir = os.path.join(os.path.dirname(self.run.db_path), "masks")
        os.makedirs(self.masks_dir, exist_ok=True)

        self.photos = self.filter_excluded(
            self.thread_db.get_collection_photos(self.run.collection_id, per_page=999999),
        )

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
        self.detector_confidence = self.effective_cfg.get("detector_confidence", 0.2)
        self.weak_rescue_enabled = self.pipeline_cfg.get(
            "weak_detection_rescue_enabled", True,
        )
        self.weak_detection_confidence = self.pipeline_cfg.get(
            "weak_detection_confidence", 0.12,
        )
        self.contextual_weak_ids = self._find_contextual_weak_ids()

    def _find_contextual_weak_ids(self):
        contextual_weak_ids: set = set()
        if (
            self.weak_rescue_enabled
            and self.weak_detection_confidence < self.detector_confidence
            and self.photos
        ):
            from weak_detections import contextual_weak_runs
            raw_mdv6_dets = self.thread_db.get_detections_for_photos(
                [p["id"] for p in self.photos],
                min_conf=self.weak_detection_confidence,
                detector_model="megadetector-v6",
            )
            weak_runs = contextual_weak_runs(
                self.photos,
                raw_mdv6_dets,
                detector_confidence=self.detector_confidence,
                weak_confidence=self.weak_detection_confidence,
                max_gap=self.pipeline_cfg.get("burst_time_gap", 3.0),
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
                    self.thread_db,
                    config=self.effective_cfg,
                    photo_ids=weak_scope_ids,
                )
                contextual_weak_ids = {
                    feature["id"]
                    for feature in weak_features
                    if feature.get("subject_uncertain")
                }
        return contextual_weak_ids

    def drop_offline_folders(self):
        """Drop photos whose folder is offline.

        Without this we'd render_proxy every one of them, they'd all skip
        with proxy=None, and the stage summary would land as "N skipped" —
        obscuring the real cause (missing folder) with a mask-extraction
        failure count (Codex #1388 P2 r3664058173). The folder-scoped
        branch in classify deliberately leaves ``abort`` clear so healthy
        folders keep processing here; this filter is how "healthy folders
        keep processing" stays true without dragging the missing folder's
        photos along.

        Always probe the worklist's folders — do NOT gate on
        ``source_skipped_photo_ids`` (Codex #1388 P1 r3664891993). A
        fully-cached classify (every detection + classifier result already
        stored, only masks missing — e.g. after a SAM variant change)
        makes no image opens, so the seed set stays empty even though
        every remaining file is on an unreachable share. Without the
        unconditional probe, extract_masks would then reopen the dead
        source photo-by-photo, count each failed render_proxy as merely
        skipped, and the pipeline could finish "successfully" with no
        masks made.

        Filter by FOLDER, not by photo id: the classify seed only
        accumulates photos that reached ``_prepare_image``, so the
        non-reclassify cache branch — which appends the cached prediction
        to raw_results and ``continue``s without any disk touch — never
        contributes its photos to the seed set. An ID-only filter would
        leave those cached photos in place, and mask/eye-keypoint stages
        would reopen the same dead source (Codex #1388 P2 r3664694179).
        Re-probing per folder also handles the multi-spec recovery case
        where a folder that dropped for spec A comes back before mask
        extraction runs, so its photos aren't silently excluded
        (Codex #1388 P2 r3664348758).
        """
        worklist_folder_ids = {
            fid for fid in (_row_folder_id(p) for p in self.photos)
            if fid is not None
        }
        still_offline_folder_ids = self.still_offline_folder_ids_of(
            self.thread_db, worklist_folder_ids,
        )
        if not still_offline_folder_ids:
            return
        kept, dropped = [], []
        for p in self.photos:
            if _row_folder_id(p) in still_offline_folder_ids:
                dropped.append(p)
            else:
                kept.append(p)
        self.photos = kept
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
            self.preflight_mask_outcomes(
                self.thread_db, dropped, self.sam2_variant, self.dinov2_variant,
                self.detector_confidence,
                raw_subject_analysis=self.run.params.raw_subject_analysis,
                contextual_weak_ids=self.contextual_weak_ids,
                weak_detection_confidence=(
                    self.weak_detection_confidence
                    if self.weak_rescue_enabled
                    and self.weak_detection_confidence
                    < self.detector_confidence
                    else None
                ),
            )
        )
        self.em_preflight_unreadable = len(at_risk_dropped_ids)
        self.em_preflight_masked = len(already_masked_ids)
        # Publish so eye_keypoints (later downstream) sees
        # the same offline set without having to re-probe
        # every folder from scratch.
        prior_skipped = (
            self.source_offline_state.get("skipped_photo_ids")
            or set()
        )
        self.source_offline_state["skipped_photo_ids"] = (
            set(prior_skipped) | dropped_ids
        )
        already_flagged_classify = any(
            e.startswith("[classify] Fatal:") for e in self.run.errors
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
            self.run.stages["extract_masks"]["status"] = "failed"
            self.em_offline_latched = True
        log.warning(
            "Extract-masks: dropped %d photo(s) from %d "
            "offline folder(s); source not reachable.",
            len(dropped_ids), len(still_offline_folder_ids),
        )

    def map_primary_detections(self):
        """Build a map of photo_id -> selected primary detection.

        Built from the detections table. Only photos with detections and
        without masks need processing.

        Skip synthetic full-image detections (detector_model='full-image').
        Those rows exist only to give classify predictions a non-NULL FK
        anchor for photos where MegaDetector found no animals — they are
        not real subject boxes and should not drive mask extraction or
        count toward the photos_with_detections safeguard below (which
        surfaces the "weights missing / no detections" diagnostic).
        Note: we intentionally do NOT short-circuit when the photo
        already has *some* mask in the photos table — that legacy
        check ignored which SAM variant produced the mask, so a
        config change to a different variant would never re-run.
        The per-photo cache check happens inside the loop below
        against photo_masks(photo_id, sam2_variant).

        Track sub-threshold-only photos separately so the silent-
        completion guard can distinguish "no detection rows at all"
        from "rows exist but every confidence is below
        detector_confidence" — the user's remediation differs
        (download weights vs lower the threshold).

        ``detector_confidence`` / ``weak_rescue_enabled`` /
        ``weak_detection_confidence`` / ``contextual_weak_ids`` are
        captured above (before the pre-flight offline probe) so
        ``_preflight_mask_outcomes`` can apply the same weak-rescue
        eligibility the loop uses.  Reusing them here keeps that
        single-sourced.
        """
        thread_db = self.thread_db
        photo_det_map = {}
        self.photos_with_detections = 0
        self.photos_subthreshold_only = 0
        for p in self.photos:
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
            if p["id"] in self.contextual_weak_ids:
                dets = [
                    d for d in thread_db.get_detections(
                        p["id"],
                        min_conf=self.weak_detection_confidence,
                        detector_model="megadetector-v6",
                    )
                    if d["category"] == "animal"
                ]
            else:
                dets = [
                    d for d in thread_db.get_detections(
                        p["id"], min_conf=self.detector_confidence,
                    )
                    if d["detector_model"] != "full-image"
                ]
            if dets:
                self.photos_with_detections += 1
                primary = dets[0]  # selected primary first
                photo_det_map[p["id"]] = {
                    "photo": p,
                    "detection_id": primary["id"],
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
                        self.photos_subthreshold_only += 1

        self.photos_to_process = [
            photo_det_map[pid] for pid in photo_det_map
        ]

    def open_worklist(self):
        self.folders = {f["id"]: f["path"] for f in self.thread_db.get_folder_tree()}
        self.total = len(self.photos_to_process)
        # Now that ``total`` is known, build the preflight outage
        # message. The denominator has to be the mask-candidate
        # total (loop total + preflight-dropped mask candidates),
        # matching the stage result's ``total``. Using
        # ``total_before`` — the whole collection worklist —
        # produced messages like "1 of 100 photos unreachable"
        # when only 1 was a mask candidate and the result showed
        # ``total: 1``, so the reader couldn't reconcile the two
        # (Codex #1392 P2 r3687499184).
        if self.em_preflight_unreadable > 0:
            self.em_offline_preflight_error = (
                f"[extract_masks] Fatal: "
                f"{self.em_preflight_unreadable} of "
                f"{self.total + self.em_preflight_unreadable + self.em_preflight_masked} "
                f"photos unreachable (source offline). "
                f"Reconnect the missing folder(s) and run "
                f"Process again to extract the rest."
            )
            # Latch was set above only when this stage owns the
            # outage (classify didn't already emit a Fatal), so
            # this gate mirrors the previous append-in-preflight
            # condition and avoids a duplicate Fatal entry.
            if self.em_offline_latched:
                self.run.errors.append(self.em_offline_preflight_error)
        self.masked = 0
        self.skipped = 0
        self.em_failed = 0
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
        self.em_unreadable = 0
        # Folders proven unreachable by a failed read during this
        # loop. Every later photo under them is counted unreadable
        # without reissuing a read: on a dead SMB/NFS share each
        # attempt is an instant EIO, so re-probing hundreds of
        # photos only slows the give-up down.
        self.em_offline_folder_ids: set = set()
        # Unreadable photos attributable to a latched source outage,
        # tracked apart from the total so the reconnect message can't
        # claim credit for a corrupt file in a healthy folder — or for
        # photos behind a *different* dead source (Codex #1392 P2).
        self.em_offline_unreadable = 0
        self.em_offline_reasons: list = []
        self.start_time = time.time()

    # -- early exits -------------------------------------------------

    def exit_without_mask_candidates(self):
        """Explain an empty worklist on the step; True when the stage is done."""
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
        classify_ran = self.run.stages["classify"]["status"] not in ("skipped", "pending")
        if self.photos_with_detections == 0 and len(self.photos) > 0 and classify_ran:
            weights_present = False
            try:
                from detector import MEGADETECTOR_ONNX_PATH
                weights_present = os.path.isfile(MEGADETECTOR_ONNX_PATH)
            except ImportError:
                weights_present = False

            if self.photos_subthreshold_only > 0:
                reason, summary = self._subthreshold_reason()
            elif weights_present:
                reason = (
                    f"No detections produced for {len(self.photos)} photo(s). MegaDetector ran but "
                    "found no animals meeting the confidence threshold. The pipeline will "
                    "reject every photo with `no_subject_mask`. Lower `detector_confidence` "
                    "in settings or rerun classify with a different threshold if detections "
                    "were expected."
                )
                summary = "Skipped — MegaDetector produced no detections"
            else:
                reason = (
                    f"No detections available for {len(self.photos)} photo(s). MegaDetector "
                    "weights are not downloaded, so the classify stage ran on full images "
                    "and stored no detections. Without detections the mask extraction stage "
                    "has nothing to process, and the pipeline will reject every photo with "
                    "`no_subject_mask`. Download MegaDetector V6 from the pipeline models "
                    "page and rerun the pipeline."
                )
                summary = "Skipped — MegaDetector weights not downloaded"

            if self.photos_subthreshold_only > 0:
                em_reason = "all_subthreshold"
            elif weights_present:
                em_reason = "no_detections"
            else:
                em_reason = "weights_missing"
            self._exit_early(em_reason, reason, summary)
            return True

        # Mixed-state guard: photos_with_detections > 0 (some photos have
        # qualifying detections — already masked from a prior run) but
        # there are also unmasked photos whose only detections are below
        # the threshold. Without this branch the stage completes silently
        # with "0 masked, 0 skipped" and the user has no way to discover
        # why the unmasked photos were never processed. Production hit
        # this when 4166 of 5054 photos were already masked and the
        # remaining 727 had only sub-threshold detections.
        if self.total == 0 and self.photos_subthreshold_only > 0 and classify_ran:
            reason, summary = self._subthreshold_reason()
            self._exit_early("all_subthreshold", reason, summary)
            return True
        return False

    def _subthreshold_reason(self):
        reason = (
            f"{self.photos_subthreshold_only} photo(s) have detections but every "
            f"detection is below the current detector_confidence threshold "
            f"({self.detector_confidence}). The pipeline will reject these photos "
            "with `no_subject_mask`. Lower `detector_confidence` in workspace "
            "settings to extract masks for them."
        )
        summary = (
            f"Skipped — {self.photos_subthreshold_only} photo(s) below "
            f"detector_confidence threshold ({self.detector_confidence})"
        )
        return reason, summary

    def _exit_early(self, em_reason, reason, summary):
        run = self.run
        log.warning("Pipeline extract-masks: %s", reason)
        exit_status, exit_step_status, exit_step_extra, exit_payload = (
            self.extract_masks_early_exit(
                em_reason, self.photos_subthreshold_only,
                self.em_preflight_unreadable, self.em_preflight_masked,
                self.em_offline_latched, self.em_offline_preflight_error,
            )
        )
        # No detection qualified for a mask (empty scenes, or everything
        # below detector_confidence): the stage skipped and the reason says
        # what to change, but the run did not fail. Missing MegaDetector
        # weights, or an exit the pre-flight outage already failed, stay
        # errors: the run could not do what it was asked.
        if exit_status != "failed" and em_reason in (
            "no_detections", "all_subthreshold",
        ):
            run.note(f"[extract_masks] {reason}")
        else:
            run.errors.append(f"[extract_masks] {reason}")
        run.stages["extract_masks"]["status"] = exit_status
        run.runner.update_step(
            run.job["id"], "extract_masks", status=exit_step_status,
            summary=summary, **exit_step_extra,
        )
        run.result["stages"]["extract_masks"] = exit_payload
        run.update_stages(run.runner, run.job["id"], run.stages)

    # -- photo loop --------------------------------------------------

    def _dl_progress(self, phase, current, total_steps):
        self.run.emit_progress(
            self.run.runner, self.run.job["id"], self.run.stages, "extract_masks", phase,
        )

    def _ensure_weights(self):
        if self._weights_ensured:
            return
        self.ensure_sam2_weights(
            variant=self.sam2_variant, progress_callback=self._dl_progress,
        )
        self.ensure_dinov2_weights(
            variant=self.dinov2_variant, progress_callback=self._dl_progress,
        )
        self._weights_ensured = True

    def extract_all(self):
        run = self.run
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

        self.ensure_dinov2_weights = ensure_dinov2_weights
        self.ensure_sam2_weights = ensure_sam2_weights
        self._weights_ensured = False

        self.processed = 0
        # ``while`` (not ``for``) so a pause that arrives mid-photo
        # can unwind the per-photo mask lock and retry the SAME
        # index after resume without holding the lock across the
        # entire pause. See the pause-unwind branch of the
        # ``except ResourceWaitCancelled`` handler below.
        i = 0
        while i < len(self.photos_to_process):
            entry = self.photos_to_process[i]
            if run.control.should_abort(run.abort):
                break

            photo = entry["photo"]
            det_box = entry["det_box"]
            photo_id = photo["id"]
            folder_path = self.folders.get(photo["folder_id"], "")
            item = _MaskPhoto(
                entry=entry,
                photo=photo,
                det_box=det_box,
                photo_id=photo_id,
                folder_path=folder_path,
                image_path=os.path.join(folder_path, photo["filename"]),
            )

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
                #     ``masks_features.update_embeddings``. Their writes can
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
                    run.control.pause_or_cancel_pending,
                ), acquire_photo_mask(item.photo_id):
                    outcome = self._extract_photo(i, item)
                if outcome == _STOP:
                    break
                if outcome == _NEXT_PHOTO:
                    i += 1
                    continue
            except ResourceWaitCancelled:
                # The bound non-parking probe raised this because a
                # pause OR cancellation is pending. The per-photo
                # mask lock has already been released by exiting
                # the ``with`` above. If cancellation is what fired,
                # propagate to end the stage; otherwise park at a
                # safe outer boundary (no locks held) and retry the
                # same photo after resume — do NOT advance ``i``.
                if run.control.cancellation_requested():
                    raise
                run.control.pause_checkpoint()
                if run.control.cancellation_requested():
                    raise
                continue
            except Exception:
                self.em_failed += 1
                log.warning("Mask extraction failed for photo %s", item.photo_id, exc_info=True)
                # A failed write can leave this connection inside a
                # stale WAL snapshot. Without a rollback, every later
                # photo fails immediately with the same ``database is
                # locked`` error even after the competing writer has
                # moved on.
                try:
                    self.rollback_failed_mask_photo(self.thread_db, item.photo_id)
                finally:
                    if item.mask_file_stage is not None:
                        item.mask_file_stage.restore()

            self.processed = i + 1
            run.stages["extract_masks"]["count"] = self.processed
            run.stages["extract_masks"]["total"] = self.total
            run.runner.update_step(run.job["id"], "extract_masks",
                               progress={"current": self.processed, "total": self.total},
                               error_count=self.em_failed + self.em_unreadable)
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "extract_masks",
                "Extracting features (SAM2 + DINOv2)",
                rate=round(self.processed / max(time.time() - self.start_time, 0.01) * 60, 1),
            )
            i += 1

    def _extract_photo(self, i, item):
        """Mask one photo under its lock; say how the loop moves on."""
        run = self.run
        current = self._sync_primary_subject(item.photo_id)
        if not current:
            self.skipped += 1
            return _NEXT_PHOTO
        selected = current[0]
        item.det_box = {k: selected["box_" + k] for k in "xywh"}
        item.entry["detector_model"] = selected["detector_model"]
        item.entry["prompt"] = tuple(selected["box_" + k] for k in "xywh")
        if self._reuse_cached_mask(item):
            self.masked += 1
            self.processed = i + 1
            return _NEXT_PHOTO

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
        if item.photo["folder_id"] in self.em_offline_folder_ids:
            self.em_unreadable += 1
            self.em_offline_unreadable += 1
            self.processed = i + 1
            run.stages["extract_masks"]["count"] = self.processed
            run.runner.update_step(
                run.job["id"], "extract_masks",
                progress={
                    "current": self.processed, "total": self.total,
                },
                error_count=self.em_failed + self.em_unreadable,
            )
            return _NEXT_PHOTO

        # First true cache miss: ensure SAM2 + DINOv2 weights
        # are present before render_proxy/generate_mask runs.
        # No-op on subsequent iterations.
        self._ensure_weights()

        proxy = self.render_proxy(item.image_path, longest_edge=self.proxy_longest_edge)
        if proxy is None:
            # The source didn't load. Ask how far the
            # problem reaches before deciding whether to
            # keep going: one corrupt RAW is this photo's
            # failure, but a share that dropped mid-run
            # will fail every remaining read the same way.
            self._count_unreadable_source(item)
            self.processed = i + 1
            return _NEXT_PHOTO
        if run.control.should_abort_without_pause(run.abort):
            return _STOP

        # GPU serialisation lives inside masking.generate_mask
        # (around the encoder/decoder session.run calls). The
        # wider wrap previously here held the semaphore through
        # SAM weight load + image preprocessing + prompt-coord
        # math, blocking other pipelines' GPU work for CPU-only
        # phases.
        mask = self.generate_mask(proxy, item.det_box, variant=self.sam2_variant)
        if mask is None:
            self.skipped += 1
            self.processed = i + 1
            return _NEXT_PHOTO
        if run.control.should_abort_without_pause(run.abort):
            return _STOP

        completeness, features, analysis_report = self._analyze_subject(
            item, proxy, mask,
        )
        if run.control.should_abort_without_pause(run.abort):
            return _STOP

        self._store_mask(item, proxy, mask, completeness, features, analysis_report)
        self.masked += 1
        return None

    def _sync_primary_subject(self, photo_id):
        """Resolve both state and extraction against the same candidate set,
        including MDv6-only weak rescue. Synchronizing under the lock clears
        old eye/DINO results when the effective primary changed.
        """
        from subjects import retained, sync_primary
        subject_floor = (self.weak_detection_confidence
            if photo_id in self.contextual_weak_ids else self.detector_confidence)
        subject_detector = ("megadetector-v6"
            if photo_id in self.contextual_weak_ids else None)
        sync_primary(
            self.thread_db, photo_id, min_conf=subject_floor,
            detector_model=subject_detector,
        )
        commit_with_retry(self.thread_db.conn)
        return retained(
            self.thread_db, photo_id, min_conf=subject_floor,
            detector_model=subject_detector,
        )

    def _reuse_cached_mask(self, item):
        """Re-activate an unchanged cached mask instead of rerunning SAM.

        Cache hit: a row already exists for (photo, variant) AND its stored
        prompt + detector still match the current primary detection AND the
        file is on disk. In that case the SAM result is unchanged, so we
        only re-activate the mask (cheap denormalize) and skip the heavy
        SAM + DINOv2 work.
        """
        entry = item.entry
        item.existing = existing = self.thread_db.masks_features.get_mask(
            item.photo_id, self.sam2_variant,
        )
        if existing is None:
            return False
        cached_prompt = (
            existing["prompt_x"], existing["prompt_y"],
            existing["prompt_w"], existing["prompt_h"],
        )
        if not (existing["detector_model"]
                == entry["detector_model"]
                and cached_prompt == entry["prompt"]
                and existing["path"]
                and os.path.isfile(existing["path"])):
            return False
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
        # masks_features.update_embeddings together.
        # Subject switching (subjects.sync_primary)
        # clears both active_mask_variant AND
        # dino_subject_embedding atomically inside
        # ``_clear_primary_features``, so an
        # active_mask_variant that still equals
        # sam2_variant is enough to prove the
        # denormalised subject state is fresh —
        # a subject A→B→A round-trip would have
        # nulled the variant here before the
        # embedding could go stale (Codex P2
        # r4056402007).
        state = self.thread_db.conn.execute(
            "SELECT active_mask_variant, "
            "dino_embedding_variant, quality_input_recipe FROM photos "
            "WHERE id = ?",
            (item.photo_id,),
        ).fetchone()
        return bool(
            state is not None
            and not self.run.params.raw_subject_analysis
            and state["quality_input_recipe"] is None
            and state["active_mask_variant"]
            == self.sam2_variant
            and state["dino_embedding_variant"]
            == self.dinov2_variant
        )

    def _count_unreadable_source(self, item):
        self.em_unreadable += 1
        offline = self.source_offline_reason(
            item.folder_path, item.image_path,
        )
        if offline is not None:
            _scope, reason = offline
            self.em_offline_unreadable += 1
            if reason not in self.em_offline_reasons:
                self.em_offline_reasons.append(reason)
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
            self.em_offline_folder_ids.add(
                item.photo["folder_id"],
            )

    def _analyze_subject(self, item, proxy, mask):
        completeness = self.crop_completeness(mask)
        features = self.compute_all_quality_features(proxy, mask)
        features["quality_input_recipe"] = None
        analysis_report = None
        if self.raw_session is not None:
            from raw_analysis import (
                RECIPE,
                analyze_subject,
                resize_linear,
                scoring_features,
            )

            linear = self.raw_session.load(item.image_path)
            if (linear is not None and abs(
                linear.shape[1] / linear.shape[0] - proxy.width / proxy.height
            ) <= 0.02):
                corrected, analysis_report = analyze_subject(
                    resize_linear(linear, proxy.size), mask,
                )
                corrected.close()
                analysis_report.update(self.raw_session.source_metadata())
                features = scoring_features(analysis_report)
                features["quality_input_recipe"] = RECIPE
        return completeness, features, analysis_report

    def _store_mask(self, item, proxy, mask, completeness, features, analysis_report):
        thread_db = self.thread_db
        entry = item.entry
        photo_id = item.photo_id
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
        subject_crop = self.crop_subject(proxy, mask, margin=0.15)
        if subject_crop is not None:
            embs = self.embed_batch(
                [subject_crop, proxy], variant=self.dinov2_variant,
            )
            subj_emb_blob = self.embedding_to_blob(embs[0])
            global_emb_blob = self.embedding_to_blob(embs[1])
        else:
            subj_emb_blob = None
            global_emb_blob = self.embedding_to_blob(
                self.embed(proxy, variant=self.dinov2_variant),
            )

        item.mask_file_stage = self.staged_mask_file.create(
            mask,
            self.masks_dir,
            photo_id,
            self.sam2_variant,
            self.save_mask,
            previous_path=(
                item.existing["path"] if item.existing else None
            ),
        )
        mask_path = item.mask_file_stage.final_path
        thread_db.masks_features.upsert_mask(
            photo_id=photo_id,
            variant=self.sam2_variant,
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
            quality_input_recipe=features.pop("quality_input_recipe", None),
            subject_clip_high=features.pop("subject_clip_high", None),
            subject_clip_low=features.pop("subject_clip_low", None),
            subject_y_median=features.pop("subject_y_median", None),
            bg_separation=features.pop("bg_separation", None),
            phash_crop=features.pop("phash_crop", None),
            noise_estimate=features.pop("noise_estimate", None),
            _commit=False,
        )
        thread_db.set_active_mask_variant(
            photo_id, self.sam2_variant, _commit=False,
            weak_rescue_min_conf=(self.weak_detection_confidence
                if photo_id in self.contextual_weak_ids else None),
        )
        # Remaining (non-mask) per-photo features still land
        # on the photos row.  mask_path / crop_complete /
        # subject_tenengrad / bg_tenengrad now flow via
        # set_active_mask_variant above, so they are
        # intentionally NOT passed here.
        if features:
            thread_db.masks_features.update_pipeline_features(
                photo_id, **features, _commit=False,
            )
        if analysis_report is not None:
            thread_db.masks_features.save_subject_raw_analysis(
                entry["detection_id"], analysis_report, _commit=False,
            )
        thread_db.masks_features.update_embeddings(
            photo_id,
            dino_subject_embedding=subj_emb_blob,
            dino_global_embedding=global_emb_blob,
            variant=self.dinov2_variant,
            _commit=False,
        )
        # Publish a new immutable PNG, then atomically move
        # the mask row, active denormalized fields, quality
        # features, and embeddings to that generation. The
        # predecessor remains readable until the commit;
        # finish removes it only after the database points
        # at the new path.
        item.mask_file_stage.install()
        commit_with_retry(thread_db.conn)
        item.mask_file_stage.finish()
        item.mask_file_stage = None

    # -- rollup ------------------------------------------------------

    def finish_cancelled(self):
        """Distinguish a user cancel from a clean completion: pin a
        "Cancelled" summary on the final step update so the job tree
        doesn't report a half-done stage as if it ran to term. Mirrors the
        classify-cancel path PR #710 added.
        """
        run = self.run
        run.stages["extract_masks"]["status"] = "completed"
        progress = {"current": self.processed, "total": self.total}
        em_summary = (
            f"Cancelled ({self.processed} of {self.total} processed)"
            if self.total else "Cancelled"
        )
        run.runner.update_step(
            run.job["id"], "extract_masks", status="completed",
            progress=progress,
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
                self.em_failed + self.em_unreadable
                + self.em_preflight_unreadable
            ),
        )
        run.result["stages"]["extract_masks"] = {
            "masked": self.masked + self.em_preflight_masked,
            "skipped": self.skipped, "failed": self.em_failed,
            "unreadable": self.em_unreadable + self.em_preflight_unreadable,
            # Preflight-inclusive, matching the normal finalizer:
            # the loop total alone can't cover photos that never
            # reached the loop (Codex #1392 P2).
            "total": (
                self.total + self.em_preflight_unreadable
                + self.em_preflight_masked
            ),
            "cancelled": True,
        }

    def finalize(self):
        run = self.run
        em_failed = self.em_failed
        em_unreadable = self.em_unreadable
        em_offline_unreadable = self.em_offline_unreadable
        photos_subthreshold_only = self.photos_subthreshold_only
        # Reported count spans both the photos this loop failed to
        # read and the ones the pre-flight dropped; the rollup
        # messages below stay keyed on the in-loop count because
        # the pre-flight branch already appended its own error.
        em_unreadable_all = em_unreadable + self.em_preflight_unreadable
        # ``total`` counts only what reached the loop, so it can't
        # be the denominator once pre-flight drops are folded into
        # the outcome counts — that publishes impossible tallies
        # like "3 unreadable of 0" and breaks any consumer deriving
        # coverage from them (Codex #1392 P2). Progress reporting
        # keeps using the loop-scoped ``total``.
        em_masked_all = self.masked + self.em_preflight_masked
        em_total_candidates = (
            self.total + self.em_preflight_unreadable + self.em_preflight_masked
        )
        final_status = (
            "failed"
            if em_failed > 0 or em_unreadable_all > 0
            or self.em_offline_latched
            else "completed"
        )
        run.stages["extract_masks"]["status"] = final_status
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
                f"{'; '.join(self.em_offline_reasons)}. Reconnect the "
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
        run.errors.extend(em_secondary_rollups)
        if em_fatal_msg:
            run.errors.append(em_fatal_msg)
        em_rollup = em_fatal_msg or (
            em_secondary_rollups[0]
            if em_secondary_rollups else None
        )
        em_summary_parts = [
            f"{em_masked_all} masked", f"{self.skipped} skipped",
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
                f"({self.detector_confidence})"
            )
        run.runner.update_step(run.job["id"], "extract_masks", status=final_status,
                           summary=", ".join(em_summary_parts),
                           error_count=em_failed + em_unreadable_all,
                           error=em_rollup)
        run.result["stages"]["extract_masks"] = {
            "masked": em_masked_all, "skipped": self.skipped,
            "failed": em_failed,
            "unreadable": em_unreadable_all,
            "total": em_total_candidates,
            "subthreshold": photos_subthreshold_only,
        }


def eye_keypoints_stage(
    run: PipelineRun,
    *,
    _still_offline_folder_ids_of,
    source_offline_state,
):
    """Run per-photo eye keypoint detection between mask extraction and scoring.

    No-op when the stage is disabled by config, when no SuperAnimal
    weights are on disk (users opt-in via the pipeline models card),
    or when no eligible photos remain. Per-photo failures are logged
    and do not abort the stage.
    """
    if (
        run.params.skip_eye_keypoints
        or run.params.skip_extract_masks
        or run.abort.is_set()
        or not run.collection_id
    ):
        run.stages["eye_keypoints"]["status"] = "skipped"
        run.runner.update_step(
            run.job["id"], "eye_keypoints", status="completed", summary="Skipped",
        )
        return

    run.stages["eye_keypoints"]["status"] = "running"
    run.runner.update_step(run.job["id"], "eye_keypoints", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)

    eyes = _EyeKeypointPass(
        run,
        still_offline_folder_ids_of=_still_offline_folder_ids_of,
        source_offline_state=source_offline_state,
    )
    try:
        eyes.setup()
        if eyes.exit_on_preflight_skip():
            return
        eyes.load_worklist()
        eyes.drop_offline_folders()
        eyes.start_counting()
        if eyes.exit_without_weights():
            return
        eyes.detect()
        eyes.finalize()
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
        run.abort.set()
        eyes.finish_cancelled()
    except Exception as e:
        run.errors.append(f"[eye_keypoints] Fatal: {e}")
        log.exception("Pipeline eye-keypoints stage failed")
        run.stages["eye_keypoints"]["status"] = "failed"
        run.runner.update_step(
            run.job["id"], "eye_keypoints", status="failed", error=str(e),
        )

    run.update_stages(run.runner, run.job["id"], run.stages)


class _EyeKeypointPass:
    """State shared across one eye_keypoints stage run."""

    def __init__(
        self,
        run,
        *,
        still_offline_folder_ids_of,
        source_offline_state,
    ):
        self.run = run
        self.still_offline_folder_ids_of = still_offline_folder_ids_of
        self.source_offline_state = source_offline_state

    # -- setup -------------------------------------------------------

    def setup(self):
        import config as cfg
        from pipeline import (
            _resolve_collection_photo_ids,
            detect_eye_keypoints_stage,
            eye_keypoint_stage_preflight,
        )

        self.resolve_collection_photo_ids = _resolve_collection_photo_ids
        self.detect_eye_keypoints_stage = detect_eye_keypoints_stage
        self.eye_keypoint_stage_preflight = eye_keypoint_stage_preflight

        self.thread_db = self.run.database_factory(self.run.db_path)
        self.thread_db.set_active_workspace(self.run.workspace_id)
        effective_cfg = self.thread_db.get_effective_config(cfg.load())
        self.pipeline_cfg = dict(effective_cfg.get("pipeline", {}))

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
        if self.run.params.eye_detect_override is not None:
            self.pipeline_cfg["eye_detect_enabled"] = self.run.params.eye_detect_override

    def exit_on_preflight_skip(self):
        """Mirror the stage-level preflight so a no-op run doesn't pay the
        O(N) eligibility join cost or report a misleading
        "0 of N processed" summary on large libraries.

        True when the stage is done.
        """
        run = self.run
        skip_reason = self.eye_keypoint_stage_preflight(self.pipeline_cfg)
        if skip_reason is None:
            return False
        run.stages["eye_keypoints"]["status"] = "skipped"
        run.runner.update_step(
            run.job["id"], "eye_keypoints",
            status="completed", summary=f"Skipped — {skip_reason}",
        )
        run.result["stages"]["eye_keypoints"] = {
            "processed": 0, "total": 0, "skipped": skip_reason,
        }
        run.update_stages(run.runner, run.job["id"], run.stages)
        return True

    # -- worklist ----------------------------------------------------

    def load_worklist(self):
        params = self.run.params
        self.eye_exclude_ids = set(params.exclude_photo_ids or ())
        collection_photo_ids = (
            self.resolve_collection_photo_ids(self.thread_db, self.run.collection_id)
            if self.run.collection_id is not None else None
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
        self.photos = self.thread_db.list_photos_for_eye_keypoint_stage(
            photo_ids=collection_photo_ids,
        )
        # Defensive second filter: when collection_photo_ids is None
        # (whole-workspace path) the DB query above returned every
        # eligible row, so excluded IDs would otherwise still influence
        # the download planner below and trigger weights for variants no
        # included photo routes to.
        if params.exclude_photo_ids:
            self.photos = [
                p for p in self.photos
                if p["id"] not in params.exclude_photo_ids
            ]

    def drop_offline_folders(self):
        """Drop photos whose folder is offline.

        eye_keypoints also opens the source image (via the pipeline
        detect_eye_keypoints_stage → keypoint runners), so without this it
        would walk every unreachable photo and record them as eye-detection
        failures — the same downstream-hammering pattern the classify pause
        is meant to prevent (Codex #1388 P2 r3664058173).

        Always probe the worklist's folders — not just when classify
        populated ``source_skipped_photo_ids`` (Codex #1388 P1
        r3664891993). A fully-cached-classify / eye-only rerun makes no
        image reads in classify, so the seed set can stay empty even when
        every remaining file is on an unreachable share.

        Filter by FOLDER, not by photo id — cached photos never populate
        the classify seed set, so an ID-only filter would leave them in the
        worklist (Codex #1388 P2 r3664694179). Re-probing per folder also
        lets a folder that recovered between classify and this stage rejoin
        (Codex #1388 P2 r3664348758).
        """
        worklist_folder_ids = {
            fid for fid in (
                _row_folder_id(p) for p in self.photos
            ) if fid is not None
        }
        still_offline_folder_ids = self.still_offline_folder_ids_of(
            self.thread_db, worklist_folder_ids,
        )
        if not still_offline_folder_ids:
            return
        dropped_ids = {
            p["id"] for p in self.photos
            if _row_folder_id(p) in still_offline_folder_ids
        }
        self.photos = [
            p for p in self.photos
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
        self.eye_exclude_ids.update(dropped_ids)
        # Publish for anyone downstream that may probe
        # ``source_offline_state`` later.
        prior_skipped = (
            self.source_offline_state.get("skipped_photo_ids")
            or set()
        )
        self.source_offline_state["skipped_photo_ids"] = (
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

    def start_counting(self):
        self.total = len(self.photos)
        self.start_time = time.time()
        self.processed = 0

    def _progress(self, phase, current, total_steps):
        run = self.run
        self.processed = current
        run.stages["eye_keypoints"]["count"] = current
        run.stages["eye_keypoints"]["total"] = total_steps
        run.runner.update_step(
            run.job["id"], "eye_keypoints",
            progress={"current": current, "total": total_steps},
        )
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "eye_keypoints", phase,
            rate=round(
                current / max(time.time() - self.start_time, 0.01) * 60, 1
            ),
        )

    # -- weights -----------------------------------------------------

    def exit_without_weights(self):
        """Auto-download SuperAnimal weights on first pipeline run.

        Mirrors the SAM2/DINOv2 auto-download pattern in extract_masks
        (commit 90cd0f9): without this, every photo silently skips on
        a fresh install. Only fetch variants the per-photo router
        would actually pick — a collection of out-of-scope classes
        (fish/reptiles/invertebrates) shouldn't pay the bandwidth
        cost for weights that will never be used.

        True when a cancelled or failed download finished the stage.
        """
        if self.total <= 0:
            return False
        import keypoints as kp
        from pipeline import _resolve_keypoint_model

        needed_models = self._needed_models(_resolve_keypoint_model)

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
                if self.run.control.should_abort(self.run.abort):
                    break
                if kp_model in needed_models:
                    kp.ensure_keypoint_weights(
                        kp_model, progress_callback=self._dl_progress,
                    )
        except ResourceWaitCancelled:
            self._finish_download_cancelled()
            return True
        except Exception as dl_err:
            # Degrade an optional-stage weight-download failure to a
            # skipped stage; _skip_failed_download logs and records it.
            self._skip_failed_download(dl_err)
            return True
        return False

    def _needed_models(self, resolve_keypoint_model):
        # Mirror Gate 1 in _process_photo_for_eye: rows whose
        # classifier confidence is below eye_classifier_conf_gate
        # get skipped at run time, so they shouldn't influence
        # which variants get downloaded — otherwise an all-low-
        # confidence collection still pays the bandwidth cost
        # for weights no photo can reach.
        conf_gate = self.pipeline_cfg.get(
            "eye_classifier_conf_gate", 0.5,
        )
        needed_models = []
        for row in self.photos:
            if (row.get("species_conf") or 0.0) < conf_gate:
                continue
            model_name = resolve_keypoint_model(self.thread_db, row)
            if model_name and model_name not in needed_models:
                needed_models.append(model_name)
        return needed_models

    def _dl_progress(self, phase, current, total_steps):
        """A separate download-progress callback, so a cancel during/just
        after weight download doesn't leak the download counter into
        ``processed`` (which would surface as e.g. "Cancelled (1 of N
        processed)" before any photo has actually been touched).
        """
        self.run.emit_progress(
            self.run.runner, self.run.job["id"], self.run.stages, "eye_keypoints", phase,
        )

    def _finish_download_cancelled(self):
        """A cancel or pause that fires while another thread holds the
        per-model download lock surfaces as ResourceWaitCancelled from
        ``acquire_session_cache_lock`` — that is a cooperative cancel, not
        a download failure. Finalize the stage as cancelled the same way
        the extract_masks path handles its own ResourceWaitCancelled (line
        ~7293) so the job tree does not report "failed to download
        keypoint weights" for what was actually a user cancel.
        """
        run = self.run
        run.abort.set()
        run.stages["eye_keypoints"]["status"] = "completed"
        run.runner.update_step(
            run.job["id"], "eye_keypoints",
            status="completed",
            summary="Cancelled",
        )
        run.result["stages"]["eye_keypoints"] = {
            "processed": 0, "total": self.total,
            "cancelled": True,
        }
        run.update_stages(run.runner, run.job["id"], run.stages)

    def _skip_failed_download(self, dl_err):
        run = self.run
        log.warning(
            "Eye keypoints stage skipped — weight download "
            "failed: %s", dl_err,
        )
        # An optional stage that skipped: shown to the user, not a failure.
        run.note(f"[eye_keypoints] {dl_err}")
        run.stages["eye_keypoints"]["status"] = "skipped"
        run.runner.update_step(
            run.job["id"], "eye_keypoints",
            status="completed",
            summary=(
                f"Skipped — failed to download keypoint "
                f"weights: {dl_err}"
            ),
        )
        run.result["stages"]["eye_keypoints"] = {
            "processed": 0, "total": self.total,
            "skipped": "weight_download_failed",
        }
        run.update_stages(run.runner, run.job["id"], run.stages)

    # -- detection and rollup ----------------------------------------

    def detect(self):
        run = self.run
        self.detect_eye_keypoints_stage(
            self.thread_db, config=self.pipeline_cfg, progress_callback=self._progress,
            collection_id=run.collection_id,
            exclude_photo_ids=self.eye_exclude_ids,
            abort_check=lambda: run.control.should_abort(run.abort),
        )

    def finalize(self):
        run = self.run
        run.stages["eye_keypoints"]["status"] = "completed"
        if run.control.should_abort(run.abort):
            # Match the classify- and extract_masks-cancel summaries so
            # the job tree distinguishes a user cancel from a clean
            # finish that happened to process the same count.
            self._report_cancelled()
            return
        summary = (
            f"{self.processed} of {self.total} photos processed"
            if self.total else "No eligible photos"
        )
        run.runner.update_step(
            run.job["id"], "eye_keypoints",
            status="completed", summary=summary,
        )
        run.result["stages"]["eye_keypoints"] = {
            "processed": self.processed, "total": self.total,
        }

    def finish_cancelled(self):
        self.run.stages["eye_keypoints"]["status"] = "completed"
        self._report_cancelled()

    def _report_cancelled(self):
        run = self.run
        summary = (
            f"Cancelled ({self.processed} of {self.total} processed)"
            if self.total else "Cancelled"
        )
        run.runner.update_step(
            run.job["id"], "eye_keypoints",
            status="completed", summary=summary,
        )
        run.result["stages"]["eye_keypoints"] = {
            "processed": self.processed, "total": self.total,
            "cancelled": True,
        }
