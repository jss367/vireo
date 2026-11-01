"""Type names for what ``create_app`` injects into the blueprint factories.

Nothing here runs: each name describes something a factory receives
(``app.config``, a getter, one of app.py's late-bound helpers, or a bound
method of a per-app service object such as ``MissingOriginals``,
``PhotoDeletion``, ``FolderMoves``, ``PipelineChain``, ``VisualScope`` or
``RenderCache``) as the blueprints call it, so a factory can say what it
receives without importing those services at runtime. Shapes the services
already name (``JsonError``, ``JobWork``, ``PathOutcome``) are reused from
there.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from flask.typing import ResponseReturnValue
    from jobs import JobRunner
    from services.folder_moves import FolderMoveRequest, MoveChain, StagedTransfer
    from services.photo_deletion import BatchDeleteResult, DeleteProgress, PathOutcome
    from services.service_types import JobDict, JobResult, JobWork, JsonDict
    from services.visual_scope import VisualClause

# ``app.config``: Flask's config mapping, whose values are of mixed types.
type AppConfig = Mapping[str, Any]

# The per-request catalog connection (``web.app_hooks.get_request_db``
# bound to the app's database path).
type GetDb = Callable[[], Database]

# ``lambda: app._job_runner``: looked up per call so tests can swap the
# runner on the app.
type GetRunner = Callable[[], JobRunner]

# ``MissingOriginals.payload``: the cached Missing Originals snapshot for a
# folder (``None`` for the whole workspace).
type MissingOriginalsPayload = Callable[[Database, int | None], JsonDict]

# ``FolderMoves.guard_error``: the message blocking a folder move, or None.
type GuardMoveFolder = Callable[[Database, int], str | None]

# ``RenderCache.invalidate_photo_render_cache``.
type InvalidatePhotoRenderCache = Callable[[Database, Iterable[int]], None]

# app.py's mount-table snapshot: the network-volume roots, or None when the
# mount table could not be read (callers then treat ``/Volumes`` as network).
type NetworkRoots = set[str] | None

# ``PipelineChain.chain_after_move``: enqueue the NAS moves a chained
# process run asked for, once that run ends.
type ChainAfterMove = Callable[[JobDict, JobResult, JsonDict, int], None]


class AppTrashPaths(Protocol):
    """``app._trash_paths`` (late-bound): ``(moved, successful, failures)``."""

    def __call__(
        self, filepaths: Sequence[str],
        progress_callback: Callable[[int, int, str], None] | None = None,
        already_missing_out: set[str] | None = None, network_roots: NetworkRoots = ...,
    ) -> PathOutcome: ...


class EnqueueProcessJob(Protocol):
    """``PipelineChain.enqueue_process_job``: ``(job_id, model_warning, blocker)``."""

    def __call__(
        self, thread_db: Database, runner: JobRunner, workspace_id: int, *,
        collection_id: int, process_id: int, chained_from: str | None = None,
        expanded: JsonDict | None = None, after_move: JsonDict | None = None,
    ) -> tuple[str | None, str | None, str | None]: ...


class PendingLocalWorkspaceTransition(Protocol):
    """``FolderMoves.pending_local_workspace_transition``."""

    def __call__(self, workspace_id: int | None, db: Database | None = None) -> JobDict | None: ...


class StartMoveFolderJob(Protocol):
    """``FolderMoves.start_job``: the new move-folder job's id."""

    def __call__(
        self, runner: JobRunner, workspace_id: int | None, request: FolderMoveRequest, *,
        chain: MoveChain | None = None, staging: StagedTransfer | None = None,
    ) -> str: ...


class PreparedPhotoView(Protocol):
    """The registered ``/original`` or ``/preview`` view (late-bound), which
    the prepare-full-resolution job renders through."""

    def __call__(self, photo_id: int, *, _prepare_source: Row | None = None) -> ResponseReturnValue: ...


class InvalidateMissingOriginals(Protocol):
    """``MissingOriginals.invalidate``."""

    def __call__(self, workspace_ids: Iterable[int] | None = None) -> None: ...


class StartMissingOriginalsScan(Protocol):
    """``MissingOriginals.start_scan``: the scan's first payload."""

    def __call__(self, db: Database, folder_id: int | None = None, automatic: bool = False) -> JsonDict: ...


class ClearPreviewCacheInvalid(Protocol):
    """``RenderCache.clear_preview_cache_invalid``."""

    def __call__(self, db: Database, photo_id: int, size: int | str) -> None: ...


class CleanupCachedFiles(Protocol):
    """``PhotoDeletion.cleanup_cached_files_for_deleted_photos``."""

    def __call__(
        self, files: Sequence[JsonDict],
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> None: ...


class RunBatchDelete(Protocol):
    """``PhotoDeletion.run_batch_delete``."""

    def __call__(
        self, db: Database, photo_ids: object, mode: str = "vireo",
        include_companions: bool = False,
        progress_callback: DeleteProgress | None = None,
    ) -> BatchDeleteResult: ...


class BuildScanWork(Protocol):
    """``services.scan_work.build_scan_work`` bound to the app."""

    def __call__(
        self, roots: str | Iterable[str], incremental: bool, active_ws: int | None,
        repair_missing_metadata: bool = False,
    ) -> JobWork: ...


class ResolveVisual(Protocol):
    """``VisualScope.resolve``: ``(info, ordered_ids, sims_by_pid)``."""

    def __call__(
        self, db: Database, rules: JsonDict | Sequence[JsonDict], visual: VisualClause,
        collection_id: int | None = None, folder_id: int | None = None,
        candidate_photo_ids: Iterable[int] | None = None,
        include_offline_folders: bool = False,
    ) -> tuple[JsonDict, list[int] | None, dict[int, float] | None]: ...
