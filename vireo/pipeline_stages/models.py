"""Model loading and shared classifier construction for the photo pipeline."""

from __future__ import annotations

import logging
import math
import os
import time
from typing import TYPE_CHECKING, Any, NoReturn

from classifier_cache import acquire_cached_classifier
from pipeline_stages.context import PipelineRun

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from classifier import Classifier
    from db import Database
    from pipeline_stages.stage_types import (
        ClassifierHandle,
        FilterExcluded,
        LoadedModels,
        LoadModelBundle,
        ModelBundle,
        ModelSpec,
    )
    from taxonomy import Taxonomy
    from timm_classifier import TimmClassifier

log = logging.getLogger(__name__)


def load_model_bundle(
    active_model: ModelSpec, tax: Taxonomy | None, thread_db: Database, progress_step: str = "model_loader",
    *,
    run: PipelineRun,
    _incomplete_model_message: Callable[[str, bool], str],
    _looks_like_missing_external_data: Callable[[BaseException], bool],
    pause_context: threading.local,
) -> ModelBundle:
    """Turn a resolved model spec into a ready-to-use classifier bundle.

    Loads labels for the model and constructs the Classifier/TimmClassifier,
    translating ONNXRuntime's cryptic missing-weights errors into an
    actionable "Repair" hint. Called by both the model_loader stage (for
    the first model) and the classify stage (for each subsequent model in
    a multi-model run).
    """
    load = _BundleLoad(
        active_model, tax, thread_db, progress_step,
        run=run,
        incomplete_model_message=_incomplete_model_message,
        pause_context=pause_context,
    )
    load.load_labels()
    load.preflight_model_files()
    load.verify_weights_lazily()

    ClassifierLoadPaused = _classifier_load_paused_class()

    def _construct_classifier() -> Classifier | TimmClassifier:
        load.restart_embedding_progress()
        ClassificationCancelled = _classification_cancelled_class()

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
        def cancel_check() -> bool:
            return (
                run.control.should_abort_without_pause(run.abort)
                or run.control.cancellation_requested()
            )

        if cancel_check():
            raise ClassificationCancelled("classification cancelled")
        if load.model_type == "timm":
            from timm_classifier import TimmClassifier
            return TimmClassifier(load.model_str, taxonomy=tax)
        from classifier import Classifier
        if not load.use_tol:
            from classify_job import _reuse_saved_label_embeddings
            _reuse_saved_label_embeddings(thread_db, load.model_str, load.weights_path, load.labels, cancel_check)
        return Classifier(
            labels=None if load.use_tol else load.labels,
            model_str=load.model_str,
            pretrained_str=load.weights_path,
            embedding_progress_callback=load.embedding_progress,
            cancel_check=cancel_check,
            pause_check=load.pause_check,
        )

    cache_key = load.classifier_cache_key()

    # _construct_classifier may trigger the ONNX self-heal path
    # (create_session_with_self_heal) which deletes corrupt weights
    # and redownloads them inside the factory. The shared acquisition
    # helper rekeys the entry to the post-load file fingerprint.
    def _acquire() -> ClassifierHandle:
        return acquire_cached_classifier(
            factory=_construct_classifier,
            cancel_check=lambda: (
                run.control.should_abort(run.abort)
                or run.control.cancellation_requested()
            ),
            **cache_key,
        )

    clf, cache_handle = _enter_cached_classifier(
        load, _acquire, _looks_like_missing_external_data, ClassifierLoadPaused,
    )
    return load.bundle(clf, cache_handle)


def _enter_cached_classifier(
    load: _BundleLoad, acquire: Callable[[], ClassifierHandle],
    looks_like_missing_external_data: Callable[[BaseException], bool],
    paused_class: type[BaseException],
) -> tuple[Classifier | TimmClassifier, ClassifierHandle]:
    """Acquire and enter the shared classifier, retrying after a pause.

    Returns ``(clf, cache_handle)``. A load failure that looks like
    missing ONNX external data becomes the incomplete-model error.
    """
    try:
        while True:
            try:
                cache_handle = acquire()
                clf = cache_handle.__enter__()
                return clf, cache_handle
            except paused_class:
                # The factory stepped out of the shared load lock
                # with its label embeddings checkpointed. Park this
                # participant at the pipeline's pause gate (no lock
                # held), then construct again so the computation
                # resumes from the checkpoint. A Cancel during the
                # pause surfaces on the retry through the factory's
                # own cancel probe.
                load.park_paused_load()
                continue
    except Exception as load_err:  # broad: re-raises every load failure, as the incomplete-model error when ONNX data is missing
        if looks_like_missing_external_data(load_err):
            load.raise_incomplete_model(load_err)
        raise


class ClassifierLoadPaused(RuntimeError):
    """Stands in for ``classifier.ClassifierLoadPaused`` without the classifier.

    ``model_cache._is_caller_cancelled_error`` and ``embedding_cache``
    recognize a paused producer by the exact class name, so the stand-in
    must keep the real class's name for a waiting sibling to retry instead
    of inheriting the pause.
    """


class ClassificationCancelled(RuntimeError):
    """Stands in for ``classifier.ClassificationCancelled`` without the classifier.

    Recognized by class name like ``ClassifierLoadPaused`` above, and by the
    ``__name__ == "ClassificationCancelled"`` checks in the pipeline stages.
    """


def _classifier_load_paused_class() -> type[RuntimeError]:
    try:
        from classifier import ClassifierLoadPaused as real_class
    except ImportError:
        return ClassifierLoadPaused
    return real_class


def _classification_cancelled_class() -> type[RuntimeError]:
    try:
        from classifier import ClassificationCancelled as real_class
    except ImportError:
        return ClassificationCancelled
    return real_class


class _BundleLoad:
    """State shared by the phases of one ``load_model_bundle`` call."""

    def __init__(
        self, active_model: ModelSpec, tax: Taxonomy | None, thread_db: Database, progress_step: str,
        *,
        run: PipelineRun,
        incomplete_model_message: Callable[[str, bool], str],
        pause_context: threading.local,
    ) -> None:
        self.active_model = active_model
        self.tax = tax
        self.thread_db = thread_db
        self.progress_step = progress_step
        self.run = run
        self.incomplete_model_message = incomplete_model_message
        self.pause_context = pause_context

        self.model_str = active_model["model_str"]
        self.weights_path = active_model["weights_path"]
        self.model_type = active_model.get("model_type", "bioclip")
        self.model_name = active_model["name"]
        self.model_is_custom = active_model.get("source") == "custom"
        self.embedding_started = time.monotonic()
        self.embedding_start_count = None

    # -- labels ------------------------------------------------------

    def load_labels(self) -> None:
        from classify_job import _load_labels, _sources_from_metas
        from labels_fingerprint import compute_fingerprint, compute_full_fingerprint

        run = self.run
        self.labels, self.use_tol, self.label_metas = _load_labels(
            model_type=self.model_type,
            model_str=self.model_str,
            labels_file=run.params.labels_file,
            labels_files=run.params.labels_files,
            db=self.thread_db,
            model_dir=self.weights_path,
        )
        # Compute a content-addressable fingerprint for the active label set
        # and record it in the labels_fingerprints sidecar. Kept on the bundle
        # so classify_stage can pass it to record_classifier_run for each
        # (detection, model, fingerprint) triple. Source paths come from
        # the metadata ``_load_labels`` actually consumed so the sidecar
        # cannot name lists that did not produce ``labels``.
        self.fp = compute_fingerprint(self.labels)
        self.fp_full = compute_full_fingerprint(self.labels)
        if len(self.fp_full) != 64:
            self.fp_full = None
        self.label_sources = _sources_from_metas(self.label_metas)
        self._record_labels_fingerprint()

    def _record_labels_fingerprint(self) -> None:
        from classify_job import _record_labels_fingerprint

        _record_labels_fingerprint(
            self.thread_db, self.fp, self.labels, sources=self.label_sources,
            full_fingerprint=self.fp_full,
        )

    def embedding_progress(self, current: int, total: int) -> None:
        if self.embedding_start_count is None:
            self.embedding_start_count = current
            self.embedding_started = time.monotonic()
        run = self.run
        progress_step = self.progress_step
        phase = f"Preparing species labels for {self.model_name}"
        detail = f"{current:,} / {total:,} labels ready"
        completed = current - self.embedding_start_count
        elapsed = time.monotonic() - self.embedding_started
        if completed >= 5 and elapsed >= 5 and current < total:
            minutes = math.ceil((total - current) * elapsed / completed / 60)
            detail += f" · about {minutes:,} min remaining"
        run.runner.update_step(
            run.job["id"], progress_step, current_file=detail,
            progress={"current": current, "total": total, "unit": "labels"},
        )
        stage_id = "model_loader" if progress_step == "model_loader" else "classify"
        if stage_id == "model_loader":
            run.stages[stage_id].update(count=current, total=total, label=phase)
        run.emit_progress(
            run.runner, run.job["id"], run.stages, stage_id, phase,
            current_file=detail, step_id=progress_step,
            rate=0,
            phase_current=current, phase_total=total, phase_label="Species labels",
        )

    def restart_embedding_progress(self) -> None:
        self.embedding_start_count = None
        self.embedding_started = time.monotonic()

    # -- on-disk model checks ----------------------------------------

    def preflight_model_files(self) -> None:
        """Preflight: validate the on-disk model before handing it to
        ONNXRuntime. A stale _check_onnx_downloaded result (e.g. after
        the user deleted a .onnx.data file, or the download manifest
        changed) would otherwise surface as an opaque ONNXRuntime crash.
        "unverified" is accepted here: all files are present, only the
        SHA256 cross-check with HuggingFace was skipped (transient network
        issue). The lazy verify_if_needed call that follows will retry the
        hash check, and get_models() already treats these as downloaded, so
        rejecting them here turns a warning into a hard pipeline failure.
        """
        from models import _classify_model_state

        files = self.active_model.get("files", [])
        if files and self.weights_path:
            state = _classify_model_state(self.weights_path, files)
            if state not in ("ok", "unverified"):
                raise RuntimeError(
                    self.incomplete_model_message(self.model_name, self.model_is_custom)
                )

    def verify_weights_lazily(self) -> None:
        """Lazy SHA256 verification: for known models (those with an
        hf_subdir), hash every LFS file on first load in this process
        and compare against HuggingFace's reported SHA256. Catches
        silent corruption and truncated downloads that slipped past
        hf_hub_download. Result is cached in-process so subsequent
        pipeline runs pay zero cost.
        """
        active_model = self.active_model
        hf_subdir = active_model.get("hf_subdir")
        if hf_subdir and not self.model_is_custom and self.weights_path:
            import model_verify
            try:
                model_verify.verify_if_needed(
                    active_model["id"], self.weights_path, hf_subdir,
                    optional_files=active_model.get("optional_files"),
                )
            except model_verify.ModelCorruptError as verify_err:
                log.warning(
                    "Lazy verification failed for %s: %s",
                    active_model["id"], verify_err,
                )
                raise RuntimeError(
                    self.incomplete_model_message(self.model_name, self.model_is_custom)
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

    # -- classifier acquisition --------------------------------------

    def pause_check(self) -> bool:
        """Non-parking pause probe. A pause request makes the
        classifier checkpoint the label embeddings finished so
        far and raise ``ClassifierLoadPaused`` instead of
        parking under ``load_lock``; the acquire loop in
        ``_enter_cached_classifier`` parks this participant at its normal
        checkpoint with every lock released and constructs again on
        Resume. Threads without a registered pause participant cannot
        park, so they never abort for pause and instead honor
        it at the owning worker's next boundary.
        """
        if getattr(self.pause_context, "participant", None) is None:
            return False
        probe = getattr(self.run.runner, "pause_requested", None)
        return bool(probe is not None and probe(self.run.job["id"]))

    def classifier_cache_key(self) -> dict[str, Any]:
        """The keyword arguments that key this model in the classifier cache."""
        # The shared cache key includes an ordered, canonical label identity
        # when use_tol=False because Classifier captures embeddings in that
        # exact column order. Two pipelines with different labels or order
        # must not share a session. Tree-of-Life mode reads precomputed
        # embeddings and is label-independent, so its key is constant.
        #
        # The weights fingerprint catches in-place model replacement
        # (Repair, custom re-register). Without it, a pipeline started
        # before the old session's idle timer fires would reuse the
        # stale ONNX session on the new bytes.
        tax_fp = self.cache_taxonomy_fingerprint()
        files = self.active_model.get("files")
        return {
            "model_type": self.model_type,
            "model_str": self.model_str,
            "weights_path": self.weights_path,
            "labels": None if self.use_tol else self.labels,
            "files": files,
            # Fold in optional-artifact presence: without
            # this a Repair that downloads timm's
            # label_descriptions.json (or bioclip-2.5's ToL
            # files) would not invalidate the entry already
            # loaded from the pre-repair install, and
            # subsequent pipeline runs would keep using a
            # stale classifier constructed without those
            # artifacts.
            "optional_files": self.active_model.get("optional_files"),
            "taxonomy_fingerprint": tax_fp,
        }

    def cache_taxonomy_fingerprint(self) -> str | None:
        """The timm key also varies by taxonomy fingerprint: TimmClassifier
        captures the taxonomy at construction and resolves common names /
        hierarchy from it on every prediction. Reusing a classifier loaded
        against a stale taxonomy (or no taxonomy) after a later run
        downloads or refreshes one would silently emit predictions
        missing the enrichment, so a change in taxonomy must miss the
        cache and rebuild.
        """
        if self.model_type == "timm":
            from computation_cache import taxonomy_identity

            return taxonomy_identity(self.tax)
        return None

    def park_paused_load(self) -> None:
        run = self.run
        log.info(
            "Classifier load for %s paused; parking until "
            "Resume", self.model_name,
        )
        run.control.pause_checkpoint()
        # ``pause_check`` only fires on a thread with a
        # registered participant, so the gate parks us
        # above. Should it ever return with the pause
        # still pending (and no cancel), wait on the
        # runner directly rather than spin through
        # construction until Resume.
        pause_probe = getattr(run.runner, "pause_requested", None)
        direct_wait = getattr(run.runner, "wait_if_paused", None)
        if (
            pause_probe is not None
            and direct_wait is not None
            and pause_probe(run.job["id"])
            and not run.control.cancellation_requested()
        ):
            direct_wait(run.job["id"], publish_paused=False)

    def raise_incomplete_model(self, load_err: BaseException) -> NoReturn:
        """ONNXRuntime signals missing external-data with a
        "model_path must not be empty" / "Initializer" error. Treat
        any load failure as an incomplete-model hint for the user —
        but only when we can confirm the on-disk files are actually
        bad. A transient ONNX failure (memory pressure, mmap race,
        test-suite monkeypatches from another process) should not
        permanently mark a healthy install as "Incomplete".
        """
        import model_verify

        active_model = self.active_model
        weights_path = self.weights_path
        files_ok = False
        hf_subdir = active_model.get("hf_subdir")
        if (
            weights_path
            and hf_subdir
            and not self.model_is_custom
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
                f"Model '{self.model_name}' failed to load "
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
            self.incomplete_model_message(self.model_name, self.model_is_custom)
        ) from load_err

    # -- bundle metadata ---------------------------------------------

    def bundle(self, clf: Classifier | TimmClassifier, cache_handle: ClassifierHandle) -> ModelBundle:
        """The ready-to-use classifier bundle the classify stage consumes."""
        portable_model_identity = self.portable_model_identity(clf)
        label_source = self.describe_label_source(clf)

        return {
            "clf": clf,
            "_cache_handle": cache_handle,
            "model_type": self.model_type,
            "model_name": self.model_name,
            "model_str": self.model_str,
            "labels": self.labels,
            "label_source": label_source,
            "labels_fingerprint": self.fp,
            "labels_fingerprint_full": self.fp_full,
            "classifier_model_identity": portable_model_identity,
            "use_tol": self.use_tol,
            "active_model": self.active_model,
        }

    def portable_model_identity(self, clf: Classifier | TimmClassifier) -> dict[str, Any] | None:
        try:
            from computation_cache import (
                classifier_model_identity,
                fingerprint,
                with_consumed_label_descriptions,
            )

            portable_model_identity = classifier_model_identity(self.active_model)
            # Stamp the identity with the label_descriptions.json the
            # constructed classifier actually read, not what the disk
            # shows now: TimmClassifier's background heal publishes
            # that file during normal operation and can land between
            # the classifier's read and this probe. See
            # computation_cache.with_consumed_label_descriptions.
            portable_model_identity = with_consumed_label_descriptions(
                portable_model_identity, clf,
            )
            if self.fp_full is None and self.use_tol and portable_model_identity:
                self.fp_full = fingerprint({
                    "label_space": "tree-of-life",
                    "model": portable_model_identity,
                })
                self._record_labels_fingerprint()
        except (OSError, ValueError):
            portable_model_identity = None

        if self.run.params.raw_subject_analysis and portable_model_identity is not None:
            from raw_analysis import RECIPE

            portable_model_identity = {
                **portable_model_identity, "raw_subject_analysis": RECIPE,
            }
        return portable_model_identity

    def describe_label_source(self, clf: Classifier | TimmClassifier) -> str:
        """What this model actually compares photos against — the merged
        species lists, Tree of Life, or a timm model's fixed head. The
        classify step shows it so the row names the label space, not
        just the weights.
        """
        from classify_job import describe_label_source

        return describe_label_source(
            self.run.params, self.thread_db,
            labels=self.labels,
            use_tol=self.use_tol,
            model_type=self.model_type,
            class_count=getattr(clf, "label_space_size", None),
            label_metas=self.label_metas,
        )


def model_loader_stage(
    run: PipelineRun,
    *,
    _filter_excluded: FilterExcluded,
    _load_model_bundle: LoadModelBundle,
    loaded_models: LoadedModels,
    model_loader_failed: threading.Event,
    models_ready: threading.Event,
    resolution_error: str | None,
    resolved_specs: list[ModelSpec],
) -> None:
    if run.params.skip_classify:
        run.stages["model_loader"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "model_loader", status="completed",
                           summary="Skipped")
        run.update_stages(run.runner, run.job["id"], run.stages)
        models_ready.set()
        return
    run.stages["model_loader"]["status"] = "running"
    run.runner.update_step(run.job["id"], "model_loader", status="running",
                       current_file="Resolving model...")
    run.update_stages(run.runner, run.job["id"], run.stages)
    try:

        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)

        if run.collection_id and _skip_when_no_wildlife(
            run, thread_db, run.collection_id, _filter_excluded, models_ready,
        ):
            return

        # Specs were pre-resolved at job start so step_defs could carry
        # the model's display name on each `classify:<id>` row. If that
        # resolution raised, surface the same error here — model_loader
        # is the stage that owns "no model / bad id" failures.
        if resolution_error:
            raise RuntimeError(resolution_error)

        first_name = resolved_specs[0]["name"]
        run.runner.update_step(run.job["id"], "model_loader", current_file=first_name)

        tax = _load_run_taxonomy(run, loaded_models)
        loaded_models["resolved_specs"] = resolved_specs

        # Load the first classifier so classify_stage can start as soon
        # as scan completes; any remaining specs are loaded inside
        # classify_stage so we never hold more than one model in memory.
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "model_loader", f"Loading {first_name}...",
        )

        try:
            bundle = _load_model_bundle(resolved_specs[0], tax, thread_db)
            loaded_models.update(bundle)
        except Exception as preload_err:  # broad: model loaders raise undocumented errors; with more models queued, one failure is not fatal
            if _is_classification_cancellation(run, preload_err):
                raise
            if len(resolved_specs) > 1:
                # Other models remain — don't abort the whole pipeline.
                log.warning(
                    "First model %s failed to load, %d remaining: %s",
                    first_name, len(resolved_specs) - 1, preload_err,
                    exc_info=True,
                )
                loaded_models["preload_error"] = str(preload_err)
            else:
                # Single model — fatal, let the outer handler abort.
                raise

        loaded_models["pending_specs"] = resolved_specs[1:]

        _mark_models_loaded(run, loaded_models, resolved_specs, first_name)
    except Exception as e:  # broad: stage boundary; a failure is recorded as this stage's fatal error, a cancel as skipped
        if _is_classification_cancellation(run, e):
            run.abort.set()
            run.stages["model_loader"]["status"] = "skipped"
            run.runner.update_step(
                run.job["id"], "model_loader",
                status="completed", summary="Skipped (cancelled)",
            )
        else:
            # Don't set ``abort`` here. It is also the scanner's and
            # thumbnailer's cancel signal, so setting it mid-phase-one
            # stopped a scan that doesn't need the model and labeled
            # it "Cancelled" although nobody cancelled. The run
            # orchestrator sets ``abort`` once the model-free stages
            # (scan, thumbnails, previews) have finished, so every
            # model-dependent stage still skips.
            model_loader_failed.set()
            run.errors.append(f"[model_loader] Fatal: {e}")
            log.exception("Pipeline model loader stage failed")
            run.stages["model_loader"]["status"] = "failed"
            run.runner.update_step(
                run.job["id"], "model_loader", status="failed", error=str(e),
            )
    finally:
        models_ready.set()
        run.update_stages(run.runner, run.job["id"], run.stages)


def _is_classification_cancellation(run: PipelineRun, err: BaseException) -> bool:
    """Whether a model-load failure is a cancellation rather than an error."""
    is_classification_cancelled = (
        err.__class__.__name__ == "ClassificationCancelled"
    )
    return bool(
        run.control.cancellation_requested()
        or is_classification_cancelled
        or str(err) == "classification cancelled"
    )


def _load_run_taxonomy(run: PipelineRun, loaded_models: dict) -> Taxonomy | None:
    """Download the taxonomy if requested, then load it for every classifier."""
    # Download taxonomy if missing/unusable and requested. Mirrors the
    # availability check used by /api/pipeline/page-init: a 0-byte stub
    # from an interrupted download "exists" but is not a usable
    # taxonomy, and the user opted into a download to recover from
    # exactly that state.
    from models import get_taxonomy_info
    from taxonomy import TAXONOMY_JSON_PATH, find_taxonomy_json
    taxonomy_path = find_taxonomy_json()
    download_error = None
    if run.params.download_taxonomy and not get_taxonomy_info().get("available"):
        try:
            from taxonomy import download_taxonomy
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "model_loader", "Downloading taxonomy...",
            )
            # Always write new downloads to the persistent path.
            taxonomy_path = TAXONOMY_JSON_PATH
            download_taxonomy(taxonomy_path, progress_callback=lambda msg:
                run.emit_progress(
                    run.runner, run.job["id"], run.stages, "model_loader", msg,
                )
            )
        except Exception as e:  # broad: download failures are network/HTTP/IO; the run continues with the local taxonomy and notes why
            log.warning("Taxonomy download failed, continuing without: %s", e, exc_info=True)
            download_error = str(e) or type(e).__name__

    # Taxonomy is shared across every classifier in the run.
    # Use load_local_taxonomy() so a corrupt persistent file
    # falls back to the legacy package-dir copy.
    from taxonomy import load_local_taxonomy, taxonomy_fallback_notice
    failures: list[dict[str, str]] = []
    tax = load_local_taxonomy(failures=failures)
    loaded_models["tax"] = tax
    # A run that goes without the taxonomy it was asked to fetch, or past a
    # taxonomy file it could not read, still classifies validly, so it is a
    # note (the run stays green) rather than an error. Tagged
    # ``[taxonomy]``, not ``[model_loader]``: no stage skipped, so the
    # Process page must not mark the Classify card "Skipped".
    notice = taxonomy_fallback_notice(tax, failures, download_error)
    if notice:
        run.note(f"[taxonomy] {notice}")
        loaded_models["taxonomy_notice"] = notice
        reason = (
            "taxonomy download failed" if download_error
            else "taxonomy file unreadable"
        )
        loaded_models["taxonomy_summary"] = (
            f"no taxonomy ({reason})" if tax is None
            else f"older taxonomy copy ({reason})"
        )
    # Portable identity for the taxonomy backing this pipeline.
    # Threaded through publish and cache-match paths so that
    # classifier runs made against one taxonomy revision are
    # not conflated with runs from a different revision on
    # another installation. See computation_cache.taxonomy_identity.
    from computation_cache import (
        taxonomy_identity as _taxonomy_identity_fn,
    )
    loaded_models["taxonomy_identity"] = _taxonomy_identity_fn(tax)
    return tax


def _skip_when_no_wildlife(
    run: PipelineRun, thread_db: Database, collection_id: int, _filter_excluded: FilterExcluded,
    models_ready: threading.Event,
) -> bool:
    """Skip the stage when every collection photo is marked not wildlife."""
    candidate_photos = _filter_excluded(
        thread_db.collections.get_photos(collection_id, per_page=999999)
    )
    photo_ids = [p["id"] for p in candidate_photos]
    candidate_ids = thread_db.photos.filter_out_wildlife_excluded(photo_ids)
    if candidate_photos and not candidate_ids:
        run.stages["model_loader"]["status"] = "skipped"
        run.runner.update_step(
            run.job["id"], "model_loader", status="completed",
            summary="Skipped (all photos marked not wildlife)",
        )
        run.update_stages(run.runner, run.job["id"], run.stages)
        models_ready.set()
        return True
    return False


def _mark_models_loaded(
    run: PipelineRun, loaded_models: LoadedModels, resolved_specs: list[ModelSpec], first_name: str,
) -> None:
    run.stages["model_loader"]["status"] = "completed"
    summary = ", ".join(s["name"] for s in resolved_specs)
    if "preload_error" in loaded_models:
        summary += f" ({first_name} failed to preload)"
    if "taxonomy_summary" in loaded_models:
        summary += f" — {loaded_models['taxonomy_summary']}"
    # A completed step with an ``error`` reads as a warning on the Jobs
    # page, which shows the full notice under the step.
    notice = {"error": loaded_models["taxonomy_notice"]} if "taxonomy_notice" in loaded_models else {}
    run.runner.update_step(run.job["id"], "model_loader", status="completed",
                       summary=summary, **notice)
