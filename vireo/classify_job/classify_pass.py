"""The classify pass: batching images through the classifier.

Prepares each detection's image, runs batches through the model, gates on
cached classifier runs, and records and publishes the batch's classifier runs.
"""

from __future__ import annotations

import contextlib
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from classify_job.common import _photo_timestamp, load_image, load_working_image, log
from classify_job.store_predictions import _cached_prediction_taxonomy
from keyword_normalization import folded_species_key as _folded_species_key
from resource_ledger import ResourceWaitCancelled
from services import detection_store

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Mapping, Sequence
    from collections.abc import Set as AbstractSet
    from contextlib import AbstractContextManager
    from datetime import datetime

    import numpy as np
    from classify_job.classify_types import (
        AnyClassifier,
        BatchEntry,
        DetectionDict,
        DetectionMap,
        PhotoRecord,
        Prediction,
        RawResult,
    )
    from computation_cache import ArtifactStore
    from db import Database
    from jobs import JobRunner
    from PIL import Image
    from raw_analysis import RawAnalysisSession
    from services.service_types import JobDict
    # One image's classifier output: its ranked predictions and, for
    # embedding models, the image embedding.
    _ImageResult = tuple[list[Prediction], np.ndarray | None]
    from classifier import Classifier
    from timm_classifier import TimmClassifier

# ``_folded_species_key`` / ``_species_match_key`` live in
# ``keyword_normalization`` with the rest of the name folds. The private
# aliases are kept because the grouping code and its tests read better with
# the leading underscore signalling "this module's rule".


_BATCH_SIZE = 16


@dataclass(frozen=True)
class ClassifierModel:
    """The loaded classifier a classify pass runs, and how its runs are keyed."""

    clf: Any
    """The loaded classifier (``Classifier`` or ``TimmClassifier``)."""
    model_type: str
    """``"timm"`` for a timm classifier, else an embedding model such as
    BioCLIP; picks the batch API and whether cached embeddings are reused."""
    model_name: str
    """The name predictions and classifier runs are recorded under."""
    labels_fingerprint: str | None = None
    """Keys the per-detection classifier-runs gate with ``model_name``;
    None records the legacy sentinel."""
    top_k: int = 1
    """Predictions kept per detection."""


def _prepare_image(
    photo: PhotoRecord, folders: Mapping[int, str], detection: DetectionDict | None,
    vireo_dir: str | None = None, raw_analysis: RawAnalysisSession | None = None,
) -> tuple[Image.Image | None, str, str]:
    """Load and crop a photo to a specific detection's bounding box.

    Args:
        photo: photo dict
        folders: {folder_id: path} mapping
        detection: detection dict with box_x, box_y, box_w, box_h keys
            (or None for full image classification)
        vireo_dir: optional path to ~/.vireo/; when set, tries to load the
            pre-extracted working copy JPEG before falling back to the
            original file via load_image().
        raw_analysis: optional stage-owned RawAnalysisSession for corrected
            subject crops. Unsupported inputs use the ordinary loading path.

    Returns:
        (PIL.Image, folder_path, image_path) or (None, folder_path, image_path) on failure.
    """
    from PIL import Image

    folder_path = folders.get(photo["folder_id"], "")
    image_path = os.path.join(folder_path, photo["filename"])

    img = None
    analysis_report = None
    input_source = "original"
    if raw_analysis is not None:
        img, analysis_report = raw_analysis.prepare(image_path, detection)
    # ``return_source=True`` returns ``(image, source kind)``, and
    # ``image_loader`` yields a ``FloatImage`` only in RAW_DECODE_LINEAR
    # mode, which neither load here requests.
    if img is None and vireo_dir and load_working_image is not None:
        img, input_source = cast("tuple[Image.Image | None, str]", load_working_image(
            photo, vireo_dir, max_size=None, folders=folders,
            return_source=True,
        ))
    if img is None:
        img = cast("Image.Image | None", load_image(image_path, max_size=None))
        input_source = "original"
    if img is None:
        return None, folder_path, image_path

    # Crop to detection bounding box with padding
    if detection and not img.info.get("_vireo_subject_crop"):
        iw, ih = img.size
        pad_w = detection["box_w"] * 0.2
        pad_h = detection["box_h"] * 0.2
        x1 = max(0, int((detection["box_x"] - pad_w) * iw))
        y1 = max(0, int((detection["box_y"] - pad_h) * ih))
        x2 = min(iw, int((detection["box_x"] + detection["box_w"] + pad_w) * iw))
        y2 = min(ih, int((detection["box_y"] + detection["box_h"] + pad_h) * ih))
        crop = img.crop((x1, y1, x2, y2))
        if crop.size[0] >= 50 and crop.size[1] >= 50:
            img.close()
            img = crop
        else:
            crop.close()

    img.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
    # Classification artifacts are published after predictions are persisted,
    # which may be long after this image was loaded. Carry the actual pixel
    # source with the in-memory image so quota eviction cannot make a
    # working-copy-backed result look original-backed by clearing the row.
    img.info["_vireo_input_source"] = input_source
    if analysis_report is not None:
        img.info["_vireo_raw_analysis"] = analysis_report
    return img, folder_path, image_path


def _match_stats(all_preds: list[Prediction], model_type: str) -> dict[str, Any] | None:
    """Summarize how well the best label in the list actually matched.

    Every caller here classifies with ``threshold=0``, so ``all_preds`` is the
    complete ranked list and ``all_preds[0]`` is the argmax over every label —
    including the ones that will never clear the confidence threshold and so
    never become prediction rows. That is exactly the point: whether *anything*
    in the list fit can only be answered by looking at the labels that lost,
    and the softmax'd ``score`` cannot answer it at all because it renormalizes
    to 1 over whichever labels happen to be loaded.

    Returns None when the classifier did not report raw scores (an older or
    stubbed classifier), which downstream must treat as "not recorded" rather
    than as a low score.
    """
    if not all_preds:
        return None
    top_raw = all_preds[0].get("raw_score")
    if top_raw is None:
        return None
    # Imported here rather than at module scope so the score kind stays owned
    # by the classifier that produces it (no second copy of the mapping to
    # drift), without pulling the ONNX-heavy modules in at import time. Both
    # are already loaded by the time any batch is flushed.
    if model_type == "timm":
        from timm_classifier import SCORE_KIND as score_kind
    else:
        from classifier import SCORE_KIND as score_kind
    margin = None
    if len(all_preds) > 1:
        second_raw = all_preds[1].get("raw_score")
        if second_raw is not None:
            margin = float(top_raw) - float(second_raw)
    # Fold ``top_species`` through the same normalization ``add_prediction``
    # applies to its ``species`` column. The calibration query in
    # ``scripts/calibrate_match_threshold.py`` joins this summary against the
    # normalized keyword and the normalized prediction row, so a bundled label
    # with a curly apostrophe (``Swinhoe’s White-eye``) would otherwise be
    # recorded here in the un-normalized form and miss both sides of the join,
    # dropping the confirmed-correct row into the ``incorrect`` bucket and
    # biasing the fitted floor.
    return {
        "max_match_score": float(top_raw),
        "match_margin": margin,
        "top_species": _folded_species_key(all_preds[0].get("species")),
        "label_count": len(all_preds),
        "score_kind": score_kind,
    }


def _flush_batch(
    batch: Sequence[BatchEntry], clf: AnyClassifier, model_type: str, model_name: str, db: Database,
    raw_results: list[RawResult], top_k: int = 1, propagate_cancel: bool = False,
) -> int:
    """Classify a batch of prepared images and append results.

    Returns the number of failures within this batch.

    ``propagate_cancel=True`` re-raises ``ResourceWaitCancelled`` (a Stop
    while the batch waited for an inference slot) instead of retrying each
    image and counting every one as a failure. The images are still closed.
    """

    images = [entry["img"] for entry in batch]
    failed = 0

    try:
        batch_results, failed = _classify_batch_images(
            batch, images, clf, model_type, propagate_cancel,
        )

        for entry, result in zip(batch, batch_results, strict=True):
            if result is None:
                continue
            all_preds, embedding = result

            if embedding is not None:
                _store_batch_embedding(db, entry, model_name, embedding)

            if not all_preds:
                continue

            raw_results.append(
                _batch_raw_result(entry, all_preds, embedding, model_type, top_k),
            )
    finally:
        # Close all PIL images to avoid resource leaks
        for entry in batch:
            entry["img"].close()

    return failed


def _classify_batch_images(
    batch: Sequence[BatchEntry], images: list[Image.Image], clf: AnyClassifier, model_type: str,
    propagate_cancel: bool,
) -> tuple[list[_ImageResult | None], int]:
    """Classify a batch at once, falling back to one image at a time.

    Returns ``(batch_results, failed)``: one ``(preds, embedding)`` pair per
    entry (``None`` where single-image classification failed) and the
    number of those failures.
    """
    failed = 0
    # GPU serialisation across concurrent pipelines lives inside the
    # classifier helpers (around the ``session.run`` calls), so this
    # path holds no process-wide lock around preprocessing, DB upserts,
    # or result-building. See ``Classifier._get_image_embedding`` and
    # ``TimmClassifier.classify[_batch]``.
    # ``model_type`` "timm" means a ``TimmClassifier``; any other is a
    # ``Classifier``.
    batch_results: list[_ImageResult | None] | None
    try:
        if model_type == "timm":
            batch_preds = cast("TimmClassifier", clf).classify_batch(images, threshold=0)
            batch_results = [(preds, None) for preds in batch_preds]
        else:
            batch_results = cast("Classifier", clf).classify_batch_with_embedding(images, threshold=0)
    except ResourceWaitCancelled:
        if propagate_cancel:
            raise
        log.warning("Batch classification failed, falling back to single-image", exc_info=True)
        batch_results = None
    except Exception:  # broad: the batch path falls back to per-image classification on any inference failure
        log.warning("Batch classification failed, falling back to single-image", exc_info=True)
        batch_results = None
    if batch_results is None:
        batch_results = []
        for entry in batch:
            try:
                if model_type == "timm":
                    preds = clf.classify(entry["img"], threshold=0)
                    batch_results.append((preds, None))
                else:
                    preds, emb = cast("Classifier", clf).classify_with_embedding(entry["img"], threshold=0)
                    batch_results.append((preds, emb))
            except ResourceWaitCancelled:
                if propagate_cancel:
                    raise
                log.warning("Classification failed for %s", entry["photo"]["filename"], exc_info=True)
                batch_results.append(None)
                failed += 1
            except Exception:  # broad: per-image boundary; inference raises undocumented errors and one image must not stop the batch
                log.warning("Classification failed for %s", entry["photo"]["filename"], exc_info=True)
                batch_results.append(None)
                failed += 1
    return batch_results, failed


def _store_batch_embedding(db: Database, entry: BatchEntry, model_name: str, embedding: np.ndarray) -> None:
    """Persist one classified entry's embedding by photo and by detection."""
    emb_bytes = embedding.tobytes()
    detection_store.upsert_photo_embedding(
        db, entry["photo"]["id"], model_name, emb_bytes,
    )
    # Also key by detection so multi-subject reruns from
    # cache pick each detection's own vector instead of the
    # last-detection-wins photo-level row. Without this, two
    # subjects on the same photo would share one cached
    # embedding and refine_groups_by_similarity would merge
    # them (or misgroup them across a burst) on non-reclassify
    # reruns even though no fresh inference ran.
    det_id = entry.get("detection_id")
    if det_id is not None:
        detection_store.upsert_photo_embedding(
            db, entry["photo"]["id"], model_name, emb_bytes,
            variant=f"det:{det_id}",
        )


def _batch_raw_result(
    entry: BatchEntry, all_preds: list[Prediction], embedding: np.ndarray | None, model_type: str,
    top_k: int,
) -> RawResult:
    """The raw classifier result for one entry with predictions."""
    # Computed from the unfiltered list, before anything below is
    # allowed to drop labels.
    match = _match_stats(all_preds, model_type)

    top = all_preds[0]
    log.info(
        '%s: "%s" at %.0f%%',
        entry["photo"]["filename"],
        top["species"],
        top["score"] * 100,
    )

    timestamp = _photo_timestamp(entry["photo"])

    # Build alternatives list (predictions 2..top_k)
    alternatives = []
    for alt_pred in all_preds[1:top_k]:
        alternatives.append({
            "species": alt_pred["species"],
            "confidence": alt_pred["score"],
            "raw_score": alt_pred.get("raw_score"),
            "taxonomy": alt_pred.get("taxonomy"),
        })

    raw_result = {
        "photo": entry["photo"],
        "detection_id": entry.get("detection_id"),
        "folder_path": entry["folder_path"],
        "image_path": entry["image_path"],
        "prediction": top["species"],
        "confidence": top["score"],
        "raw_score": top.get("raw_score"),
        "match": match,
        "timestamp": timestamp,
        "filename": entry["photo"]["filename"],
        "embedding": embedding,
        "taxonomy": top.get("taxonomy"),
        "alternatives": alternatives,
    }
    input_source = getattr(entry["img"], "info", {}).get(
        "_vireo_input_source"
    )
    if input_source in ("original", "working_copy"):
        raw_result["_input_source"] = input_source
    return raw_result


class _ClassifyPhotosPass:
    """Run-wide state for one ``_classify_photos`` call.

    Holds the accumulated ``raw_results``, the failure and cached-skip
    counters, the pending inference ``batch`` and whether a Stop
    cancelled a flush.
    """

    def __init__(
        self, photos: Sequence[sqlite3.Row], folders: dict[int, str], detection_map: DetectionMap,
        model: ClassifierModel, runner: JobRunner, job: JobDict, db: Database,
        vireo_dir: str | None, reclassify: bool, non_animal_ids: AbstractSet[int],
    ) -> None:
        self.folders = folders
        self.detection_map = detection_map
        self.model = model
        self.runner = runner
        self.job = job
        self.db = db
        self.vireo_dir = vireo_dir
        # Fall back to the legacy sentinel when the caller didn't compute a
        # fingerprint — matches the default used by classifier_runs.
        self.fp = model.labels_fingerprint or "legacy"
        self.reclassify = reclassify

        self.raw_results = []
        self.failed = 0
        self.skipped_existing = 0
        self.total = len(photos)
        self.batch = []
        self.cancelled = False
        self.portable_labels_full = job.get("_labels_fingerprint_full")
        self.portable_model_identity = job.get("_classifier_model_identity")
        self.portable_taxonomy_identity = job.get("_taxonomy_identity", "no-tax")
        self.non_animal_ids = non_animal_ids
        job["_classify_cancelled"] = False

        # On a reclassify run every photo has its predictions cleared BEFORE
        # being queued into ``batch`` (per-photo ``clear_predictions`` below
        # at line 1442). Any subsequent ``_flush_batch`` failure therefore
        # strands those photos empty — including the mid-loop 16-image
        # flushes at lines 1568 / 1696, not just the tail flush at line
        # 1735. Suspend the bound resource cancel probe around EVERY flush
        # on a reclassify so ``acquire_inference_resources`` inside
        # ``_flush_batch`` completes even if cancel arrived while the flush
        # was in flight. The loop-level cancel check at line 1399 uses
        # ``runner.is_cancelled`` directly (not the bound probe), so the
        # loop still breaks promptly on cancel; only in-progress and tail
        # flushes are protected. Non-reclassify flushes stay unaffected.
        from resource_ledger import bind_resource_cancel_check

        self._bind_resource_cancel_check = bind_resource_cancel_check
        self.start_time = time.time()

    # -- flushing ----------------------------------------------------

    def _flush_preserving_cleared(self) -> AbstractContextManager[object]:
        if self.reclassify:
            return self._bind_resource_cancel_check(None)
        return contextlib.nullcontext()

    def flush_pending(self, pending: Sequence[BatchEntry]) -> bool:
        """Flush ``pending``; False when a Stop cancelled its inference.

        A Stop that lands while the batch waits for an inference slot raises
        ``ResourceWaitCancelled``. Propagating it (rather than letting
        ``_flush_batch`` retry each image and count every one as a failure)
        lets the loop stop cleanly: the batch's images are closed, no
        classifier runs are recorded for it, and the run is reported as
        cancelled. Reclassify flushes suspend the resource cancel probe, so
        this only fires for runs whose unreached photos keep their cache.
        """
        pre_len = len(self.raw_results)
        try:
            with self._flush_preserving_cleared():
                self.failed += _flush_batch(
                    pending, self.model.clf, self.model.model_type, self.model.model_name,
                    self.db, self.raw_results,
                    top_k=self.model.top_k, propagate_cancel=True,
                )
        except ResourceWaitCancelled:
            log.info("Classify job cancelled during batch inference")
            self.job["_classify_cancelled"] = True
            return False
        _record_batch_classifier_runs(
            self.db, pending, self.model.model_name, self.fp, self.raw_results,
            pre_len,
            labels_fingerprint_full=self.job.get("_labels_fingerprint_full"),
            model_identity=self.job.get("_classifier_model_identity"),
        )
        return True

    def _enqueue(
        self, photo: sqlite3.Row, detection_id: int, folder_path: str, image_path: str,
        img: Image.Image,
    ) -> None:
        """Queue one prepared image, flushing once the batch is full."""
        self.batch.append({
            "photo": photo,
            "detection_id": detection_id,
            "folder_path": folder_path,
            "image_path": image_path,
            "img": img,
        })

        if len(self.batch) >= _BATCH_SIZE:
            if not self.flush_pending(self.batch):
                self.cancelled = True
            self.batch = []

    # -- classifier-run gate -----------------------------------------

    def _runtime_aware_run_keys(self, detection_id: int) -> set[tuple[str, str]]:
        expected_runtime = None
        if self.portable_labels_full and self.portable_model_identity:
            try:
                from computation_cache import classifier_runtime_for_detection

                expected_runtime = classifier_runtime_for_detection(
                    self.db,
                    detection_id,
                    self.portable_model_identity,
                    self.portable_labels_full,
                    taxonomy_identity=self.portable_taxonomy_identity,
                )
            except (OSError, ValueError):
                expected_runtime = None
        if expected_runtime is None:
            return self.db.model_runs.get_classifier_run_keys(detection_id)
        return self.db.model_runs.get_classifier_run_keys(
            detection_id, runtime_fingerprint=expected_runtime,
        )

    def _cached_embedding(self, photo: sqlite3.Row, detection_id: int, photo_level_fallback: bool) -> np.ndarray | None:
        embedding = None
        if self.model.model_type != "timm":
            # Prefer per-detection variant so multi-subject photos don't
            # reuse the last-detection-wins photo-level vector (see
            # _flush_batch). Fall back to the photo-level entry only when
            # this photo has a single qualifying detection so legacy data
            # still refines correctly.
            emb_blob = self.db.masks_features.get_embedding(
                photo["id"], self.model.model_name,
                variant=f"det:{detection_id}",
            )
            if not emb_blob and photo_level_fallback:
                emb_blob = self.db.masks_features.get_embedding(
                    photo["id"], self.model.model_name,
                )
            if emb_blob:
                import numpy as np
                embedding = np.frombuffer(emb_blob, dtype=np.float32)
        return embedding

    def _reuse_cached_run(
        self, photo: sqlite3.Row, detection_id: int, folder_path: str, image_path: str,
        timestamp: Callable[[], datetime | None], photo_level_fallback: bool,
    ) -> bool:
        """True when the classifier-run gate skips inference for a detection.

        If (detection, model, fingerprint) has a run key AND has cached
        prediction rows, surface the cached top-1 and skip inference. If
        the run key exists but no cached rows do (e.g. the prior pass
        stored `category == 'match'` which is intentionally not written,
        or transient ordering between record_classifier_run and
        _store_grouped_predictions), DON'T short-circuit — otherwise the
        photo is stranded until the user forces --reclassify. Fall through
        to re-classify instead.

        Exception: a run key with a measured ``classifier_match_scores``
        summary is a completed zero-candidate run — the classifier looked
        at this detection, nothing cleared the confidence floor, and the
        match-score row is its output-of-record. Re-running would discard
        the "nothing in your list fits" verdict this feature exists to
        preserve, and on an install without weights it would fail model
        loading instead of reusing the cache (Codex P2 on 22cc0ac). No
        top-1 to surface — just skip inference for this detection.
        """
        run_keys = self._runtime_aware_run_keys(detection_id)
        if (self.model.model_name, self.fp) not in run_keys:
            return False
        cached = detection_store.get_predictions_for_detection(
            self.db, detection_id,
            classifier_model=self.model.model_name,
            labels_fingerprint=self.fp,
            min_classifier_conf=0,
        )
        if cached:
            self.skipped_existing += 1
            top = cached[0]  # ordered by confidence DESC
            photo_timestamp = timestamp()
            embedding = self._cached_embedding(
                photo, detection_id, photo_level_fallback,
            )
            self.raw_results.append({
                "photo": photo,
                "detection_id": detection_id,
                "folder_path": folder_path,
                "image_path": image_path,
                "prediction": top["species"],
                "confidence": top["confidence"],
                "timestamp": photo_timestamp,
                "filename": photo["filename"],
                "embedding": embedding,
                "taxonomy": _cached_prediction_taxonomy(top),
                "alternatives": [],
                "_existing": True,
            })
            return True
        # Run key without cached rows: if a measured match-score summary
        # exists for the same triple the run legitimately produced zero
        # candidates — honor that outcome instead of re-running.
        if self.db.model_runs.has_classifier_match_score(
            detection_id, self.model.model_name, self.fp,
        ):
            self.skipped_existing += 1
            return True
        # Otherwise the run key is a torn write or a deliberately-hidden
        # ``match`` row — fall through to classify this detection.
        return False

    # -- per photo ---------------------------------------------------

    def classify_photo(self, i: int, photo: sqlite3.Row) -> None:
        """Clear, report progress for and classify or queue one photo."""
        photo_detections = detection_store.with_reviewer_regions(
            self.db, photo["id"], self.detection_map.get(photo["id"], []))
        # Per-photo reclassify predictions purge. Lives here (rather than
        # alongside ``detections.clear`` in the detection loop) so that:
        #   1. A mid-classify cancel leaves the unprocessed tail with its
        #      old predictions intact — without this gate they'd already
        #      be cleared and the cancel would strand them with new
        #      detections and no predictions.
        #   2. When detection setup fails (missing weights, etc.) and the
        #      job degrades to full-image classification, the stale
        #      detector-based predictions still get replaced rather than
        #      lingering alongside the fallback model's output.
        # ``clear_predictions`` also wipes the matching ``classifier_runs``
        # rows so the per-detection skip gate doesn't short-circuit the
        # fresh inference about to run.
        #
        # Scoped to this run's ``labels_fingerprint`` when the photo has
        # detections to classify: predictions are shared across
        # workspaces, and a workspace classifying the same photo against
        # a different label set owns the rows under its own fingerprint
        # (and its review state hangs off them). The rows this run
        # replaces are the ones under ``fp``; older fingerprints on these
        # detections are superseded by ``get_predictions``' newest-
        # fingerprint filter once the new rows land.
        #
        # Unscoped in the fallback path (no candidates in
        # ``detection_map`` because detection setup failed or found
        # nothing usable): the full-image anchor gets the new rows while
        # the photo's stored boxes keep theirs, so predictions under a
        # prior fingerprint on those boxes would stay "newest" for their
        # detections and surface alongside the fallback output.
        if self.reclassify:
            with self.db.transaction():
                self.db.predictions.clear(
                    model=self.model.model_name,
                    collection_photo_ids=[photo["id"]],
                    labels_fingerprint=(
                        self.fp if photo_detections else None
                    ),
                    exclude_detector_model="reviewer-selected",
                )

        self._report_progress(i, photo)

        if photo["id"] in self.non_animal_ids and not any(
                d.get("detector_model") == "reviewer-selected" for d in photo_detections):
            # A confident person or vehicle box takes this photo out of
            # scope for species classification. MegaDetector writes only
            # replace rows for ``megadetector-v6``, so any prior
            # ``full-image`` detection (and its cascaded classifier rows)
            # would still surface a stale species prediction — reads pick
            # the newest fingerprint per detection independently, and the
            # full-image detection lives under its own detector_model.
            # Retire the full-image detection here; the CASCADE takes
            # its predictions, classifier_runs and match-score rows in
            # one transaction, and the call is a no-op when nothing
            # was there.
            with self.db.transaction():
                self.db.detections.clear(photo["id"], detector_model="full-image")
            return

        folder_path = self.folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])

        if photo_detections:
            self._classify_detections(
                photo, photo_detections, folder_path, image_path,
            )
        else:
            self._classify_full_image(photo, folder_path, image_path)

    def _report_progress(self, i: int, photo: sqlite3.Row) -> None:
        job, runner, total = self.job, self.runner, self.total
        job["progress"]["current"] = i + 1
        job["progress"]["current_file"] = photo["filename"]
        runner.update_step(
            job["id"], "classify",
            progress={"current": i + 1, "total": total},
        )
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": i + 1,
                "total": total,
                "current_file": photo["filename"],
                "rate": round(
                    (i + 1) / max(time.time() - self.start_time, 0.01), 1,
                ),
                "phase": "Step 5/5: Classifying species",
            },
        )

    def _classify_detections(
        self, photo: sqlite3.Row, photo_detections: list[DetectionDict], folder_path: str, image_path: str,
    ) -> None:
        """Classify each detection independently.

        No photo-level short-circuit here: a prior short-circuit that
        skipped photos with any cached prediction under (model, fp)
        silently dropped newly-surfaced detections after the user lowered
        `detector_confidence`, leaving them unclassified until
        --reclassify. The per-detection classifier_runs gate below handles
        incremental work correctly.
        """
        timestamp = _photo_timestamp(photo)

        for detection in photo_detections:
            if not self.reclassify and self._reuse_cached_run(
                photo, detection["id"], folder_path, image_path,
                lambda: timestamp,
                photo_level_fallback=len(photo_detections) == 1,
            ):
                continue

            img, det_folder_path, det_image_path = _prepare_image(
                photo, self.folders, detection, vireo_dir=self.vireo_dir
            )
            if img is None:
                self.failed += 1
                continue

            self._enqueue(
                photo, detection["id"], det_folder_path, det_image_path, img,
            )
            if self.cancelled:
                break

    def _full_image_detection_id(self, photo: sqlite3.Row) -> int:
        """Use (or create) a full-image synthetic detection for ``photo``.

        It carries the classifier output when the photo has no detections.
        save_detections is now idempotent under content-addressed IDs, but
        reading the existing row first avoids an UPSERT + stale-cleanup
        roundtrip on the common path. min_conf=0 because the synthetic
        full-image detection is written with confidence=0 — the default
        threshold filter would hide it.
        """
        db = self.db
        existing_full = detection_store.get_detections(
            db, photo["id"], detector_model="full-image", min_conf=0,
        )
        if existing_full and not self.reclassify:
            return existing_full[0]["id"]
        full_image_det = [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
                           "confidence": 0, "category": "animal"}]
        from computation_cache import (
            full_image_runtime_fingerprint,
            source_input,
        )
        full_runtime = full_image_runtime_fingerprint()
        identity = db.model_runs.detector_source_identity(photo["id"])
        full_input = None
        if identity is not None and not identity["companion_path"]:
            try:
                _block, full_input = source_input(
                    identity["file_hash"], "vireo-detector-source-v1",
                )
            except ValueError:
                full_input = None
        # Wrap both writes in the single ``write_detection_batch``
        # transaction so a crash between save_detections and
        # record_detector_run can't leave a full-image detection
        # row without its matching detector_runs row — that torn
        # state would fool the runtime-aware reuse gates into
        # treating the photo as never detected.
        full_det_ids = detection_store.write_detection_batch(
            db, photo["id"], "full-image", full_image_det,
            runtime_fingerprint=full_runtime,
            input_fingerprint=full_input,
        )
        return full_det_ids[0]

    def _classify_full_image(self, photo: sqlite3.Row, folder_path: str, image_path: str) -> None:
        full_det_id = self._full_image_detection_id(photo)
        # Gate check for the synthetic full-image detection too.
        # Mirror the regular detection branch: when gated, surface the
        # cached top-1 prediction into raw_results so downstream
        # grouping/storage still sees it. Without this, non-reclassify
        # reruns silently drop cached full-image photos even though
        # those photos were intentionally kept in the cache.
        #
        # Run key without cached rows: mirror the boxed-detection gate
        # above. A measured ``classifier_match_scores`` row for the same
        # triple is a completed zero-candidate run — re-running would
        # discard the "nothing in your list fits" verdict this feature
        # exists to preserve, and on an install without weights it would
        # fail model loading instead of reusing the cache (Codex P2 on
        # f074d0c). Otherwise the run key is a torn write or a
        # deliberately-hidden ``match`` row → fall through to re-classify
        # this full-image detection.
        #
        # Full-image detections are always single per (photo, model), so
        # the photo-level embedding row is unambiguously theirs. Prefer the
        # per-detection variant for parity with new writes; fall back to
        # the photo-level entry.
        if not self.reclassify and self._reuse_cached_run(
            photo, full_det_id, folder_path, image_path,
            lambda: _photo_timestamp(photo),
            photo_level_fallback=True,
        ):
            return
        img, folder_path, image_path = _prepare_image(photo, self.folders, None, vireo_dir=self.vireo_dir)
        if img is None:
            self.failed += 1
            return

        self._enqueue(photo, full_det_id, folder_path, image_path, img)


def _classify_photos(
    photos: Sequence[sqlite3.Row], folders: dict[int, str], detection_map: DetectionMap,
    model: ClassifierModel, runner: JobRunner, job: JobDict, db: Database, *,
    vireo_dir: str | None = None, reclassify: bool = False, finish_cleared_only: bool = False,
    non_animal_ids: AbstractSet[int] = frozenset(),
) -> tuple[list[RawResult], int, int]:
    """Classify detections in batches, cropping to each detection's bounding box.

    For each photo, iterates over all detections (from detection_map) and
    classifies each one independently. Photos without detections are
    classified as full images.

    Images are passed directly to classifiers as PIL objects (no temp file I/O).
    Multiple images are batched into a single forward pass for throughput.

    ``model`` is the ``ClassifierModel`` to run. A per-detection
    classifier_runs gate keyed on (detection_id, model.model_name,
    model.labels_fingerprint) short-circuits re-work when the same triple
    already ran. reclassify=True bypasses the gate.

    ``finish_cleared_only`` is set by ``run_classify_job`` when a post-detect
    cancel landed in reclassify mode. The processed subset of ``photos``
    already had its detections rewritten, and any box the new run dropped
    took its predictions with it; we must run classification on them anyway
    to avoid stranding them with no predictions. In this mode the
    cancel-break at the top of the loop is suppressed (we're rebuilding
    work detection already changed, not starting new work).

    Photos in ``non_animal_ids`` (only confident person or vehicle boxes;
    see ``_classifier_candidates``) are not classified.

    Returns:
        (raw_results, failed_count, skipped_existing_count)
    """
    if load_image is None:
        raise ImportError("image_loader module is required for classification")

    run = _ClassifyPhotosPass(
        photos, folders, detection_map, model, runner, job, db,
        vireo_dir, reclassify, non_animal_ids,
    )

    for i, photo in enumerate(photos):
        if not finish_cleared_only and runner.is_cancelled(job["id"]):
            # Already-classified work is in raw_results (committed via the
            # in-loop flushes); the pending `batch` is queued items that
            # haven't run through the model yet. In reclassify mode the
            # final flush still drains the batch (those photos already had
            # their old predictions cleared above and would otherwise end
            # up empty); in non-reclassify mode the batch is dropped to
            # honor the cancel signal. The per-photo reclassify clear
            # below only fires for photos we actually reach, so the
            # unclassified tail keeps its old predictions intact.
            log.info(
                "Classify job cancelled during classification (%d/%d)",
                i, run.total,
            )
            run.cancelled = True
            break

        run.classify_photo(i, photo)

        if run.cancelled:
            break

    # Flush remaining images. The pending batch holds photos that haven't
    # been classified yet — for non-reclassify cancels we drop it (those
    # photos still have their cached predictions, so honoring the cancel
    # signal here just skips wasted inference). For reclassify cancels we
    # must flush instead: each queued photo already had its old predictions
    # wiped by the per-photo ``clear_predictions`` above, and bailing here
    # would strand them with no predictions until a manual rerun. Flushing
    # finishes the rebuild for the queued tail without picking up any new
    # photos (the cancel check at the top of the loop still blocks those).
    if run.batch and (not run.cancelled or reclassify):
        # A reclassify tail flush is a deliberate preservation pass.
        # CPU inference consults the bound resource cancel probe, which
        # is already True on this cancelled path, so leaving the
        # binding active would make ``_flush_batch`` raise
        # ``ResourceWaitCancelled`` for the whole batch, the fallback
        # per-image path would also raise, and no replacement
        # predictions would be written for the queued photos whose old
        # predictions the loop already cleared. The
        # ``_flush_preserving_cleared`` helper suspends the binding on
        # every reclassify flush so inference completes; ``JobRunner``
        # still owns any hard shutdown via the runner-side deadline.
        run.flush_pending(run.batch)

    return run.raw_results, run.failed, run.skipped_existing


def _classify_summary_parts(raw_results: list[RawResult], skipped_existing: int, failed: int) -> list[str]:
    """Summary fragments for the classify step ("N classified, M cached").

    Cache hits that surfaced a stored top-1 are in ``raw_results`` flagged
    ``_existing``, while a skip backed only by a measured match-score record
    counts toward ``skipped_existing`` without a row. Fresh inferences are
    therefore counted directly; ``len(raw_results) - skipped_existing``
    went negative once the second kind appeared.
    """
    classified = sum(1 for r in raw_results if not r.get("_existing"))
    parts = [f"{classified} classified"]
    if skipped_existing:
        parts.append(f"{skipped_existing} cached")
    if failed:
        parts.append(f"{failed} failed")
    return parts


def _record_batch_classifier_runs(
    db: Database, batch: Sequence[BatchEntry], model_name: str, labels_fingerprint: str,
    raw_results: list[RawResult], raw_results_start: int = 0,
    labels_fingerprint_full: str | None = None, model_identity: dict[str, Any] | None = None,
) -> None:
    """Record classifier_runs rows for every detection in ``batch``.

    ``batch`` is the list of entries that were just passed to _flush_batch.
    ``raw_results_start`` is ``len(raw_results)`` captured *before* that
    _flush_batch call, so only the rows this flush appended
    (``raw_results[raw_results_start:]``) are tallied. Earlier rows belong to
    prior batches; re-scanning the whole growing list on every flush would
    make the bookkeeping O(n^2) across a large collection. Called after
    _flush_batch has appended to ``raw_results`` so the run row is only
    written for detections that actually produced classifier output.

    Portable-artifact promotion (``promote_and_publish_classifier_run``) is
    NOT done here even when ``labels_fingerprint_full`` /
    ``model_identity`` are provided — that function reads the persisted
    ``predictions`` rows to synthesize the exportable artifact, and those
    rows are not written until ``_store_grouped_predictions`` runs later.
    Callers must invoke ``_publish_classifier_runs_for_raw_results`` after
    ``_store_grouped_predictions`` completes so fresh runs do not stay
    stranded on ``runtime_fingerprint = 'incomplete'``.
    """
    if not batch:
        return
    # Tally how many of this flush's raw_results entries reference each
    # detection_id. Entries without a detection_id (unusual, but possible on
    # synthesized rows) are ignored. For a per-detection batch this is
    # typically 0 or 1.
    counts: dict = {}
    for r in raw_results[raw_results_start:]:
        did = r.get("detection_id")
        if did is not None:
            counts[did] = counts.get(did, 0) + 1
    seen: set = set()
    for entry in batch:
        did = entry.get("detection_id")
        if did is None or did in seen:
            continue
        seen.add(did)
        # Only record the run for detections that actually produced a
        # prediction. A count of 0 means the classifier failed (transient
        # load error, decode error, etc.) — caching it as "done" would
        # permanently strand the detection on the next non-reclassify run.
        n = counts.get(did, 0)
        if n <= 0:
            continue
        # Batch boundary: one detection's run reservation.
        with db.transaction():
            db.model_runs.record_classifier_run(
                did, model_name, labels_fingerprint,
                prediction_count=n,
                runtime_fingerprint="incomplete",
                labels_fingerprint_full=labels_fingerprint_full,
                input_recipe=entry.get("input_recipe"),
            )


def _publish_classifier_runs_for_raw_results(
    db: Database, raw_results: list[RawResult], classifier_model: str, labels_fingerprint: str,
    labels_fingerprint_full: str | None = None, model_identity: dict[str, Any] | None = None,
    taxonomy_identity: str = "no-tax", store: ArtifactStore | None = None,
) -> None:
    """Publish portable classifier artifacts for freshly-classified detections.

    ``promote_and_publish_classifier_run`` reads the persisted ``predictions``
    rows for a detection to build the exportable artifact and only then
    stamps the classifier_runs row with the real ``runtime_fingerprint``.
    Predictions are written by ``_store_grouped_predictions``, so the
    promotion pass belongs AFTER it — running it earlier (inside
    ``_record_batch_classifier_runs``, before those rows exist) silently
    no-ops and leaves classifier_runs stranded on
    ``runtime_fingerprint = 'legacy'``, excluding those runs from cache
    bundle exports.

    Entries from the reuse path carry ``_existing = True``; their
    runtime_fingerprint is already populated from the earlier successful
    publish, so they are skipped here.

    ``store`` is the caller's configured ``ArtifactStore``. Callers that
    honor ``COMPUTATION_CACHE_DIR`` must forward the job's store here so
    freshly promoted artifacts land in the same cache the status / export /
    catalog-reapplication paths use; passing ``None`` falls back to the
    default ``~/.vireo/computation-cache`` location.
    """
    if not labels_fingerprint_full or not model_identity or not raw_results:
        return
    from computation_cache import promote_and_publish_classifier_run

    seen: set = set()
    for result in raw_results:
        det_id = result.get("detection_id")
        if det_id is None or det_id in seen or result.get("_existing"):
            continue
        seen.add(det_id)
        try:
            input_source = result.get("_input_source")
            promote_and_publish_classifier_run(
                db,
                det_id,
                classifier_model,
                labels_fingerprint,
                labels_fingerprint_full,
                model_identity,
                store=store,
                taxonomy_identity=taxonomy_identity,
                source_is_original=(
                    input_source == "original"
                    if input_source in ("original", "working_copy")
                    else None
                ),
            )
        except Exception:  # broad: publishing the portable cache is best-effort; the local result already committed
            log.warning(
                "Could not publish portable classifier result for detection %s",
                det_id, exc_info=True,
            )
