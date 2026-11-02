"""Standalone SAM2/DINOv2 mask extraction, independent of HTTP request handling.

The caller supplies a runner and a worker-thread database factory already bound
to the launching workspace. Settings are captured before the job is queued;
models load lazily when it runs. Photo locks and staged-file rollback retain the
same publication boundary as Process mask extraction.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING

from repositories.masks_features import MaskQuality, PipelineFeatures
from services import detection_store, subject_masks

if TYPE_CHECKING:
    from sqlite3 import Row
    from threading import Lock

    import numpy as np
    from db import Database
    from jobs import JobRunner
    from pipeline_job import _StagedMaskFile
    from services.service_types import JobDict, JsonDict

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MaskExtractionOptions:
    """Workspace-effective settings captured when the extraction is requested."""

    collection_id: int | None
    sam2_variant: str
    dinov2_variant: str
    proxy_longest_edge: int
    min_detector_conf: float


def extract_masks(
    job: JobDict, db_path: str, *, runner: JobRunner,
    open_db: Callable[[], Database], options: MaskExtractionOptions,
) -> JsonDict:
    """Run mask extraction using the caller's workspace-bound worker database.

    ``open_db`` runs on the worker thread. This service closes the returned
    connection on success, cancellation or failure. No Flask context is needed.
    """
    with closing(open_db()) as db:
        return _MaskExtractionRun(job, db_path, runner=runner, db=db, options=options).run()


def _import_mask_extraction_deps() -> SimpleNamespace:
    """Import the SAM2 / DINOv2 / quality stack when the job starts."""
    import numpy as np
    from dino_embed import embed, embed_batch, embedding_to_blob
    from masking import (
        crop_completeness,
        crop_subject,
        generate_mask,
        render_proxy,
        save_mask,
    )
    from pipeline_job import (
        _rollback_failed_mask_photo,
        _StagedMaskFile,
    )
    from pipeline_locks import acquire_photo_mask
    from quality import compute_all_quality_features
    from resource_ledger import ResourceWaitCancelled
    from subjects import sync_primary

    return SimpleNamespace(
        np=np,
        embed=embed,
        embed_batch=embed_batch,
        embedding_to_blob=embedding_to_blob,
        crop_completeness=crop_completeness,
        crop_subject=crop_subject,
        generate_mask=generate_mask,
        render_proxy=render_proxy,
        save_mask=save_mask,
        rollback_failed_mask_photo=_rollback_failed_mask_photo,
        StagedMaskFile=_StagedMaskFile,
        acquire_photo_mask=acquire_photo_mask,
        compute_all_quality_features=compute_all_quality_features,
        ResourceWaitCancelled=ResourceWaitCancelled,
        sync_primary=sync_primary,
    )


def _mask_candidate(photo: Row, det: Row) -> JsonDict:
    """The per-photo record the extract-masks loop works from."""
    return {
        "id": photo["id"],
        "folder_id": photo["folder_id"],
        "filename": photo["filename"],
        "detector_model": det["detector_model"],
        "detection_box": json.dumps({
            "x": det["box_x"], "y": det["box_y"],
            "w": det["box_w"], "h": det["box_h"],
        }),
        "detection_conf": det["detector_confidence"],
        # Full-precision REAL prompt — see pipeline_job
        # for the rationale; int() would truncate the
        # normalized [0,1] bbox to (0,0,0,0) and break
        # cache invalidation on bbox change.
        "prompt": (
            det["box_x"], det["box_y"],
            det["box_w"], det["box_h"],
        ),
    }


@dataclass
class _MaskPhoto:
    """One photo's pass through the extract-masks loop."""

    index: int
    photo: JsonDict
    photo_id: int
    image_path: str
    existing: JsonDict | None = None
    mask_file_stage: _StagedMaskFile | None = None


class _MaskExtractionRun:
    """The ``/api/jobs/extract-masks`` work function and its run-wide state."""

    def __init__(
        self, job: JobDict, db_path: str, *, runner: JobRunner,
        db: Database, options: MaskExtractionOptions,
    ) -> None:
        self.runner = runner
        self.thread_db = db
        self.job = job
        self.db_path = db_path
        self.collection_id = options.collection_id
        self.sam2_variant = options.sam2_variant
        self.dinov2_variant = options.dinov2_variant
        self.proxy_longest_edge = options.proxy_longest_edge
        self.min_detector_conf = options.min_detector_conf

    def run(self) -> JsonDict:
        deps = self.deps = _import_mask_extraction_deps()
        runner = self.runner
        job = self.job

        self.masks_dir = os.path.join(os.path.dirname(self.db_path), "masks")
        os.makedirs(self.masks_dir, exist_ok=True)

        # Get photos that have a real (non full-image) detection. The
        # legacy `mask_path IS NULL` gate would silently skip every
        # photo whose stored mask was made by a *different* SAM
        # variant — leaving photo_masks empty for the configured
        # variant. The per-photo cache check inside the loop below
        # against photo_masks(photo_id, sam2_variant) handles
        # "already cached for this variant" correctly.
        if self.collection_id:
            photos = self._collection_candidates()
        else:
            photos = self._workspace_candidates()

        folders = {f["id"]: f["path"] for f in self.thread_db.folders.tree()}
        self.total = len(photos)
        self.masked = 0
        self.skipped = 0
        self.failed = 0
        # A source file that never opened is not the same as "SAM found
        # no subject here", even though both leave the photo unmasked.
        # Only the second is an answer about the photo; the first means
        # we never looked. Folding them together would let a dropped share
        # finish this job green while every unmasked photo went on to be
        # hard-rejected in Process Review as `no_subject_mask`. Mirrors
        # extract_masks_stage in pipeline_job.
        self.unreadable = 0
        job["_start_time"] = time.time()

        for i, photo in enumerate(photos):
            if runner.is_cancelled(job["id"]):
                break
            photo_id = photo["id"]
            folder_path = folders.get(photo["folder_id"], "")
            image_path = os.path.join(folder_path, photo["filename"])

            photo_mask_lock = self._acquire_photo_mask_lock(photo_id)
            if photo_mask_lock is None:
                break
            item = _MaskPhoto(i, photo, photo_id, image_path)
            try:
                if not self._extract_locked_photo(item):
                    continue
            except deps.ResourceWaitCancelled:
                # Job cancelled while waiting for the CPU inference
                # lease. Bail out of the loop immediately instead of
                # marking every remaining photo as a mask failure
                # (which would then reopen the source, preprocess it,
                # and hit the same cancellation on the next iteration).
                log.info(
                    "extract-masks cancelled while waiting for inference resources",
                )
                break
            except Exception:  # broad: per-photo boundary; the failure is counted and listed in the job errors
                self.failed += 1
                log.warning(
                    "Mask extraction failed for photo %s", photo_id, exc_info=True
                )
                job["errors"].append(
                    f"Photo {photo_id}: mask extraction failed"
                )
                try:
                    deps.rollback_failed_mask_photo(self.thread_db, photo_id)
                finally:
                    if item.mask_file_stage is not None:
                        item.mask_file_stage.restore()
            finally:
                photo_mask_lock.release()

            self._push_progress(
                i,
                photo["filename"]
                # ``photo`` may be a ``sqlite3.Row``, whose ``in``
                # tests values, not column names: keep ``.keys()``.
                if hasattr(photo, "__getitem__") and "filename" in photo.keys()  # noqa: SIM118
                else str(photo_id),
            )

        return self._result()

    def _collection_candidates(self) -> list[JsonDict]:
        thread_db = self.thread_db
        coll_photos = thread_db.collections.get_photos(
            self.collection_id, per_page=999999
        )
        photos = []
        for p in coll_photos:
            dets = [
                d for d in detection_store.get_detections(
                    thread_db, p["id"], min_conf=self.min_detector_conf,
                )
                if d["detector_model"] != "full-image"
            ]
            if dets:
                photos.append(_mask_candidate(p, dets[0]))
        return photos

    def _workspace_candidates(self) -> list[JsonDict]:
        """All workspace photos with at least one real (non full-image) detection.

        One query keeps this one round-trip to SQLite per workspace; the
        per-photo cache check inside the loop handles "skip when already
        masked for this variant".
        """
        rows = self.thread_db.masks_features.workspace_mask_candidate_detections(
            self.min_detector_conf,
        )
        seen = set()
        photos = []
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            photos.append(_mask_candidate(r, r))
        return photos

    def _acquire_photo_mask_lock(self, photo_id: int) -> Lock | None:
        """Take the photo's mask lock, or return None once the job is cancelled.

        Share the same photo-wide critical section as Process jobs.
        This standalone route writes the deterministic predecessor
        filename, so serializing its full cache-check/write sequence
        lets Process safely reclaim that predecessor after commit.
        """
        runner = self.runner
        job_id = self.job["id"]
        photo_mask_lock = self.deps.acquire_photo_mask(photo_id)
        while not runner.is_cancelled(job_id):
            if not photo_mask_lock.acquire(timeout=0.1):
                continue
            # A pause can arrive during acquisition. Release before
            # parking so another Process job can use this photo.
            if (runner.pause_requested(job_id)
                    or runner.cancellation_requested(job_id)):
                photo_mask_lock.release()
                continue
            return photo_mask_lock
        return None

    def _extract_locked_photo(self, item: _MaskPhoto) -> bool:
        """Mask one photo under its lock.

        Returns False when the photo is finished without the loop's
        trailing progress event (skipped, unreadable, or a cache hit,
        which pushes its own).
        """
        deps = self.deps
        photo = item.photo
        if not self._refresh_primary(item):
            self.skipped += 1
            return False
        if self._reuse_cached_mask(item):
            return False

        # Load working-resolution proxy
        proxy = deps.render_proxy(
            item.image_path, longest_edge=self.proxy_longest_edge,
        )
        if proxy is None:
            self.unreadable += 1
            return False

        # Parse detection box
        det_box = photo["detection_box"]
        if isinstance(det_box, str):
            det_box = json.loads(det_box)

        # Generate mask via SAM2
        mask = deps.generate_mask(proxy, det_box, variant=self.sam2_variant)
        if mask is None:
            self.skipped += 1
            return False

        # Compute crop completeness + all quality features
        completeness = deps.crop_completeness(mask)
        features = deps.compute_all_quality_features(proxy, mask)

        subj_emb_blob, global_emb_blob = self._embed(proxy, mask)
        self._store_mask(
            item, mask, completeness, features, subj_emb_blob, global_emb_blob,
        )
        self.masked += 1
        return True

    def _refresh_primary(self, item: _MaskPhoto) -> bool:
        """Re-read the primary detection; False when the photo has none now."""
        thread_db = self.thread_db
        photo_id = item.photo_id
        photo = item.photo
        # Sync the primary before deciding whether a cached
        # mask still applies. A workspace confidence-floor
        # change (or another workspace sharing this photo
        # writing photo_subject_state with a different floor)
        # can promote a new primary since this job was queued;
        # sync_primary clears mask_path, active_mask_variant,
        # dino_subject_embedding, and eye_*/eye_kp_fingerprint
        # when the primary detection actually changed, so the
        # subsequent eye stage recomputes for the newly
        # published subject instead of retaining the previous
        # subject's eye focus.
        self.deps.sync_primary(
            thread_db, photo_id, min_conf=self.min_detector_conf,
        )
        thread_db.commit_with_retry()
        # A user can change primary after this job was queued.
        current = [d for d in detection_store.get_detections(thread_db, photo_id, min_conf=self.min_detector_conf)
                   if d["detector_model"] != "full-image"]
        if not current:
            return False
        selected = current[0]
        photo["detector_model"] = selected["detector_model"]
        photo["prompt"] = tuple(selected["box_" + k] for k in "xywh")
        photo["detection_box"] = {k: selected["box_" + k] for k in "xywh"}
        return True

    def _reuse_cached_mask(self, item: _MaskPhoto) -> bool:
        """Count a still-valid cached mask as masked; True on a cache hit.

        Cache hit: photo_masks already has a row for
        (photo, configured variant) AND its stored prompt
        + detector still match the current primary
        detection AND the file is on disk AND the photos row
        is already fully consistent for this (sam, dino)
        pair. A subject A→B→A round-trip clears
        dino_subject_embedding via sync_primary while the
        matching photo_masks row for A remains cached; a
        bare cache-hit shortcut would then re-activate the
        mask and skip DINO, leaving dino_subject_embedding
        null even though the job reported the photo
        processed. Mirror the Process pipeline's
        active_mask_variant/dino_embedding_variant guard.
        """
        thread_db = self.thread_db
        photo = item.photo
        existing = item.existing = thread_db.masks_features.get_mask(
            item.photo_id, self.sam2_variant,
        )
        if existing is None:
            return False
        cached_prompt = (
            existing["prompt_x"], existing["prompt_y"],
            existing["prompt_w"], existing["prompt_h"],
        )
        if not (existing["detector_model"]
                == photo["detector_model"]
                and cached_prompt == photo["prompt"]
                and existing["path"]
                and os.path.isfile(existing["path"])):
            return False
        state = thread_db.masks_features.photo_mask_state(item.photo_id)
        if (state is not None
                and state["active_mask_variant"]
                == self.sam2_variant
                and state["dino_embedding_variant"]
                == self.dinov2_variant
                and state["quality_input_recipe"] is None):
            self.masked += 1
            self._push_progress(item.index, photo["filename"])
            return True
        # Denormalised subject state is stale: fall
        # through to the full recompute below, which
        # writes set_active_mask_variant +
        # masks_features.update_embeddings atomically.
        return False

    def _embed(self, proxy: np.ndarray, mask: np.ndarray) -> tuple[bytes | None, bytes]:
        """Return the (subject, global) DINOv2 embedding blobs.

        Compute DINOv2 embeddings — one batched call when both
        subject and global are needed, since DINOv2 is CPU-only
        for external-data ONNX exports and per-call overhead is
        the bottleneck.
        """
        deps = self.deps
        subject_crop = deps.crop_subject(proxy, mask, margin=0.15)
        if subject_crop is not None:
            embs = deps.embed_batch(
                [subject_crop, proxy], variant=self.dinov2_variant,
            )
            subj_emb_blob = deps.embedding_to_blob(embs[0])
            global_emb_blob = deps.embedding_to_blob(embs[1])
        else:
            subj_emb_blob = None
            global_emb_blob = deps.embedding_to_blob(
                deps.embed(proxy, variant=self.dinov2_variant),
            )
        return subj_emb_blob, global_emb_blob

    def _store_mask(
        self, item: _MaskPhoto, mask: np.ndarray, completeness: float, features: JsonDict, subj_emb_blob: bytes | None,
        global_emb_blob: bytes,
    ) -> None:
        """Stage the mask file, write its rows in one commit, then publish it."""
        deps = self.deps
        thread_db = self.thread_db
        photo_id = item.photo_id
        photo = item.photo
        existing = item.existing
        item.mask_file_stage = stage = deps.StagedMaskFile.create(
            mask,
            self.masks_dir,
            photo_id,
            self.sam2_variant,
            deps.save_mask,
            previous_path=(
                existing["path"] if existing else None
            ),
        )
        mask_path = stage.final_path

        # Per-mask features: pop them out of `features` so
        # they land on the photo_masks row and not on the
        # photos row directly. set_active_mask_variant
        # denormalizes them back into photos for downstream
        # readers.
        mask_subject_tenengrad = features.pop(
            "subject_tenengrad", None,
        )
        mask_bg_tenengrad = features.pop("bg_tenengrad", None)
        # Mask-derived subject_size: fraction of frame
        # covered by the boolean mask.
        total_pixels = float(mask.size)
        if total_pixels > 0:
            mask_subject_size = float(
                deps.np.count_nonzero(mask) / total_pixels
            )
        else:
            mask_subject_size = None

        thread_db.masks_features.upsert_mask(
            photo_id=photo_id,
            variant=self.sam2_variant,
            path=mask_path,
            detector_model=photo["detector_model"],
            prompt=photo["prompt"],
            quality=MaskQuality(
                subject_size=mask_subject_size,
                subject_tenengrad=mask_subject_tenengrad,
                bg_tenengrad=mask_bg_tenengrad,
                crop_complete=completeness,
                quality_input_recipe=None,
                subject_clip_high=features.pop("subject_clip_high", None),
                subject_clip_low=features.pop("subject_clip_low", None),
                subject_y_median=features.pop("subject_y_median", None),
                bg_separation=features.pop("bg_separation", None),
                phash_crop=features.pop("phash_crop", None),
                noise_estimate=features.pop("noise_estimate", None),
            ),
        )
        subject_masks.set_active_mask_variant(
            thread_db, photo_id, self.sam2_variant,
        )
        # Remaining (non-mask) per-photo features still land
        # on the photos row. mask_path / crop_complete /
        # subject_tenengrad / bg_tenengrad / subject_size
        # flow via set_active_mask_variant above and are
        # intentionally NOT passed here.
        if features:
            thread_db.masks_features.update_pipeline_features(
                photo_id, PipelineFeatures(**features),
            )
        thread_db.masks_features.update_embeddings(
            photo_id,
            dino_subject_embedding=subj_emb_blob,
            dino_global_embedding=global_emb_blob,
            variant=self.dinov2_variant,
        )
        stage.install()
        thread_db.commit_with_retry()
        stage.finish()
        item.mask_file_stage = None

    def _push_progress(self, i: int, current_file: str) -> None:
        job = self.job
        self.runner.push_event(
            job["id"],
            "progress",
            {
                "current": i + 1,
                "total": self.total,
                "current_file": current_file,
                "rate": round(
                    (i + 1)
                    / max(time.time() - job["_start_time"], 0.01),
                    1,
                ),
                "phase": "Extracting features (SAM2 + DINOv2)",
            },
        )

    def _result(self) -> JsonDict:
        # Opt into JobRunner's ok/errors convention when photos went
        # unmasked. Styling the card honestly isn't enough: the Jobs
        # page, job history, API clients and the completion event all
        # read the job status, and "completed" there tells the user their
        # library was processed when some of it was never opened.
        unreadable, failed, total = self.unreadable, self.failed, self.total
        job_errors = []
        if unreadable:
            job_errors.append(
                f"{unreadable} of {total} photos could not be read, so "
                f"they have no mask and Process Review rejects them as "
                f"`no_subject_mask`. Reconnect the source and run Extract "
                f"again."
            )
        if failed:
            job_errors.append(
                f"{failed} of {total} photos failed mask extraction"
            )
        return {"masked": self.masked, "skipped": self.skipped,
                "failed": failed, "unreadable": unreadable, "total": total,
                "ok": not job_errors, "errors": job_errors}
