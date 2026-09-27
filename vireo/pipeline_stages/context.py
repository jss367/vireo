"""Per-run state and cancellation boundaries shared by processing stages.

Each pipeline invocation creates its own context. Collection creation updates
``collection_id`` before signalling its ready event; detection and model
outputs travel through separate, explicitly passed containers. Stages never
capture another run's state or import the scheduling module.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db import Database
    from jobs import JobRunner


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
    # Experimental, per-run opt-in. Normal browsing and editing stay unchanged.
    raw_subject_analysis: bool = False
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


@dataclass(frozen=True)
class PipelineControl:
    """Keep parking checkpoints distinct from probes used inside locks.

    These callbacks are bound by the orchestrator to this run's pause gate
    and thread-local participant. Library helper threads retain the original
    non-parking behavior because they have no registered participant.
    """

    should_abort: Callable[[threading.Event], bool]
    should_abort_without_pause: Callable[[threading.Event], bool]
    pause_checkpoint: Callable[[], bool]
    cancellation_requested: Callable[[], bool]
    pause_or_cancel_pending: Callable[[], bool]


@dataclass
class PipelineRun:
    """Inputs and outputs owned by one invocation of the pipeline.

    ``stages``, ``result`` and ``errors`` retain the job's existing objects,
    so progress and failure aggregation observe exactly what stages write.
    ``database_factory`` creates worker-owned connections; it is never a
    connection shared between threads. The progress callbacks preserve the
    public entry module's event format.
    """

    job: dict
    runner: JobRunner
    db_path: str
    workspace_id: int
    params: PipelineParams
    abort: threading.Event
    stages: dict
    result: dict
    errors: list[str]
    collection_id: int | None
    database_factory: Callable[..., Database]
    emit_progress: Callable[..., None]
    update_stages: Callable[..., None]
    control: PipelineControl
