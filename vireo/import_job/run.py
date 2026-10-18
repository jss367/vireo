"""``run_import_job``: the import job's entry point."""

from __future__ import annotations

from typing import TYPE_CHECKING

from db import Database
from import_job.batch_loop import _ImportBatchLoop
from import_job.cataloging import _extract_deferred_working_copies
from import_job.destination import _build_destination_context
from import_job.planning import _plan_import
from import_job.progress import _ImportEtaEstimator, _make_emitter
from import_job.result import _finalize_import, _finalize_planned
from import_job.resume import (
    _add_carried_working_copy_folders,
    _publish_resume_scope,
    _recover_parent_landings,
)
from import_job.state import _ImportRunState
from import_job.transports import _make_transport

if TYPE_CHECKING:
    from import_job.state import ImportParams
    from jobs import JobRunner
    from services.service_types import JobDict, JsonDict


def run_import_job(job: JobDict, runner: JobRunner, db_path: str, workspace_id: int, params: ImportParams) -> JsonDict:
    """Copy card(s) -> archive, hash-verify, and catalog incrementally.

    THE import orchestrator, for both destinations: picks the byte
    transport from ``params.remote_target`` (``_LocalTransport`` /
    ``_RsyncTransport``) and runs the single batch loop over it — every
    non-transport behavior lands here exactly once.

    Returns the result dict (counts + per-folder breakdown). The catalog
    is committed per batch; cancellation and crashes leave every already-
    verified file cataloged and nothing else.
    """
    from pipeline_job import (
        _missing_archive_mount_root,
        _unmounted_since_baseline,
    )
    from scanner import scan

    db = Database(db_path)
    db.set_active_workspace(workspace_id)

    # Normalized destination + mount baseline + guards. See
    # ``_build_destination_context`` — the ordering inside it
    # (normalize → baseline → guards) is load-bearing.
    ctx = _build_destination_context(db, params)

    # Byte transport — the ONLY transport-divergent code. Everything
    # below runs identically over either.
    transport = _make_transport(job, runner, params, ctx)

    runner.set_steps(job["id"], [
        {"id": "import", "label": "Copy & catalog"},
    ])
    runner.update_step(job["id"], "import", status="running")

    state = _ImportRunState(log_label=transport.log_label)
    eta = _ImportEtaEstimator(
        expected_new=(params.checked_count if params.skip_duplicates else None),
    )

    _emit = _make_emitter(job, runner, state, eta)

    # --- Discover → select → batch (shared planning phase) ----------
    # Destructured BY NAME (see ``_ImportPlan``: same-typed field
    # transpositions survive a positional unpack and the parity test).
    # ``plan.files``/``plan.timestamps`` are consumed inside the planner;
    # the loop below works from ``plan.batches``.
    # Discovery can take minutes on a large card. Publish an empty scope
    # first, so a restart during it still leaves a resumable row.
    if not _publish_resume_scope(
        job, db, state, None, runner=runner, require_flush=True,
    ):
        # Stopped while waiting for the database; planning has no
        # cancellation probe, so don't start it.
        state.cancelled = True
        return _finalize_import(
            job, runner, db, state, params,
            discovered=0, include_paths=None, source_snapshots={},
            deselected=0, vanished_paths=set(), appeared=0,
            remote_unverified=False,
        )
    plan = _plan_import(db, params, _emit, state)
    # The source snapshots gate a resume's drift check (a different card
    # at the same path), so they must be on the row before any copy.
    if not _publish_resume_scope(
        job, db, state, plan.source_snapshots, runner=runner,
        require_flush=True,
    ):
        state.cancelled = True
        return _finalize_planned(
            job, runner, db, state, params, plan, remote_unverified=False,
        )

    # --- Ledger: see ``_ImportBatchLoop`` for the run-wide buckets.
    loop = _ImportBatchLoop(
        job=job, runner=runner, db=db, workspace_id=workspace_id,
        params=params, ctx=ctx, transport=transport, state=state,
        emit=_emit, plan=plan, scan=scan,
        unmounted_since_baseline=_unmounted_since_baseline,
        missing_root_check=lambda: _missing_archive_mount_root(ctx.destination),
    )
    loop.run(plan.batches)

    _recover_parent_landings(state, params, db)
    _add_carried_working_copy_folders(state, params, db)
    _extract_deferred_working_copies(state, params, runner, job, db)

    # ``remote_unverified`` is the honesty gate — only a transport that
    # independently confirms bytes at the destination may leave it
    # False; see the append in ``_append_run_unsafe_files``. Degenerates to
    # False on the local transport (every copy is hash-verified),
    # exactly the parameter's old default there. ``attests_bytes`` is
    # the run-constant binding of ``transport.attests_bytes`` above.
    return _finalize_planned(
        job, runner, db, state, params, plan,
        remote_unverified=not loop.attests_bytes,
    )
