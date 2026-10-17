"""Resume bookkeeping: the landed-file record a resumed run takes over,
and the recovery of an interrupted parent's landings.
"""

import logging
import os

from import_dedup import compute_file_hash
from import_job.snapshots import _capture_photo_fingerprints, _chunks

log = logging.getLogger(__name__)


def _landed_identity(path):
    """``[size, mtime_ns]`` of the file at ``path``, or None if unreadable."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return [st.st_size, st.st_mtime_ns]


def _is_parent_landing(folder, name, file_hash, landed):
    """Whether ``folder/name`` still holds bytes the parent landed.

    ``landed`` maps a recorded path to ``[size, mtime_ns, hash]``. A
    cataloged hash (the row's own file) must equal the verified hash the
    parent recorded, which catches a replacement that was rescanned;
    without one (a merged companion, or a row not yet hash-stamped) the
    file must still have the recorded size and mtime.
    """
    if not name:
        return False
    path = os.path.join(folder, name)
    identity = landed.get(path)
    if identity is None:
        return False
    recorded_hash = identity[2] if len(identity) > 2 else None
    if file_hash and recorded_hash:
        return file_hash == recorded_hash
    if list(identity[:2]) == [-1, -1]:
        # Recorded without a stat (see ``_record_landed_files``) and no
        # cataloged hash to compare (a merged companion): read the bytes.
        try:
            return bool(recorded_hash) and compute_file_hash(path) == recorded_hash
        except OSError:
            return False
    return _landed_identity(path) == list(identity[:2])


def _record_landed_files(job, db, state, batch_st, source_snapshots, runner):
    """Record this batch's landed files before cataloging it.

    Recorded first, so an interrupted row never lacks a landing whose photo
    row exists: a resume finds every row this run cataloged, even ones its
    checkpointed ``photo_ids`` missed (``_recover_parent_landings``). Size,
    mtime and the verified hash pin each path to the bytes this run put
    there.
    """
    for landed in batch_st.landed:
        path = os.path.normpath(landed.dest_path)
        # A stat that fails right now (a network archive hiccup) must not
        # drop the landing from the record; the verified hash alone still
        # lets a resume match the cataloged row.
        identity = _landed_identity(path) or [-1, -1]
        state.landed_files[path] = identity + [landed.verified_hash]
    return _publish_resume_scope(
        job, db, state, source_snapshots, runner=runner, require_flush=True,
    )


def _publish_resume_scope(job, db, state, source_snapshots, runner=None,
                          require_flush=False):
    """Publish what a Resume needs if Vireo dies before this run finishes.

    The runner checkpoints ``partial_result`` onto the job's history row
    and the startup sweep keeps it on the interrupted row, so a recovery
    retry can carry the photos this run already landed (with the same
    identity and source checks a finished run's result offers). Each
    publish is a fresh dict, and when ``runner`` is passed we flush the
    checkpoint synchronously: without it the 2s checkpoint thread is the
    only path to disk, and a crash between a batch commit and its next
    tick would keep an older ``photo_ids`` list on the interrupted row —
    a resume with ``skip_duplicates=true`` would then skip those files
    as already imported while omitting them from the carried scope, so
    their working-copy recovery and requested processing would silently
    be dropped.
    """
    fresh = state.imported_photo_ids.difference(
        int(pid) for pid in state.resume_fingerprints
    )
    if fresh:
        state.resume_fingerprints.update(
            _capture_photo_fingerprints(db, fresh),
        )
    job["partial_result"] = {
        "photo_ids": sorted(state.imported_photo_ids),
        "photo_fingerprints": dict(state.resume_fingerprints),
        "source_snapshots": source_snapshots,
        "landed_files": dict(state.landed_files),
    }
    if runner is not None and require_flush:
        # The landed files must be on the history row before the batch is
        # cataloged, or a crash right after the catalog commit leaves them
        # out of a resume. Failing the import instead would replace the
        # row's result with an error and lose the scope earlier batches
        # recorded, so wait for the database; Stop ends the wait (the
        # caller then leaves the batch uncataloged).
        def cancel_check():
            return runner.is_cancelled(job["id"])
        waiting = False
        while not runner.flush_partial_result(job, cancel_check=cancel_check):
            if runner.is_cancelled(job["id"]):
                return False
            if not waiting:
                waiting = True
                log.warning(
                    "Import %s: waiting for the database to record import "
                    "progress before cataloging", job.get("id"),
                )
                runner.update_step(
                    job["id"], "import",
                    current_file="Waiting for the database to record progress",
                )
        return True
    elif runner is not None:
        # Best-effort synchronous flush. ``checkpoint_live_jobs`` swallows
        # transient ``OperationalError`` on its own — a checkpoint that
        # can't grab the lock right now will still be retried by the 2s
        # timer thread, so the failure mode degrades to the pre-fix
        # window rather than losing the batch.
        runner.checkpoint_live_jobs()
    return True


def _recover_parent_landings(state, params, db):
    """Find the catalog rows an interrupted parent import landed.

    A resume skips (or, with duplicate skipping off, adopts) the parent's
    landed files. The parent's checkpointed ``photo_ids`` can lag its
    catalog commits (a kill between a batch's commit and the next
    checkpoint), but it recorded every landed file before cataloging it,
    so look the rows up by those paths: a photo whose own file or
    companion (a JPEG merged into its RAW) still holds the parent's bytes
    (``_is_parent_landing``) is the parent's. Rows other imports put
    elsewhere never match, and neither does a file replaced since.
    """
    landed = params.recover_landed_files or {}
    folders = {os.path.dirname(path) for path in landed}
    for folder in folders:
        rows = db.imports.folder_photo_identities(folder)
        for row in rows:
            if (_is_parent_landing(folder, row["filename"], row["file_hash"], landed)
                    or _is_parent_landing(folder, row["companion_path"], None, landed)):
                state.recovered_photo_ids.add(row["id"])


def _add_carried_working_copy_folders(state, params, db):
    """Put the folders of carried photos still lacking a working copy into
    the deferred extraction scope. A parent interrupted before (or during)
    its working-copy pass left them without one, and this run skips their
    files as already imported, so nothing else would extract them.
    """
    carried = set(params.carry_photo_ids or ()) | state.recovered_photo_ids
    if not params.vireo_dir or not carried:
        return
    for chunk in _chunks(sorted(carried)):
        rows = db.imports.folders_lacking_working_copy(chunk)
        state.wc_dest_folders.update(row["path"] for row in rows)
