"""The per-batch guard chain and the mount-loss rollback.

``_batch_preflight`` owns the order of the batch guards; see its
docstring before calling any of its step helpers.
"""

import os

from import_job.state import (
    _counts,
    _fail,
    _fail_whole_batch,
    _reclassify_landed_failed,
)


def _batch_preflight(state, emit, db, *, rel, batch, queued, ctx,
                     missing_root_check):
    """Run the per-batch guard chain; return ``dest_folder`` or ``None``.

    ``None`` means the whole batch was refused — every file booked
    failed with a specific reason and progress emitted — and the caller
    must ``continue`` to the next batch.

    ORDER IS LOAD-BEARING (the 2026-07-30 incident ordering): sticky
    mount-loss refusal → dest-under-source → missing mount root → stale
    mount → makedirs → folder-status promotion. Each step assumes the
    ones above it already passed — in particular ``os.makedirs`` must
    not run until every mount check has, or it recreates the vacated
    mount point as a local shadow directory. The sequence lives in this
    ONE function: the step helpers it calls (``_refuse_unmounted_archive``
    and ``_create_dest_folder``) are called from nowhere else, so it
    cannot be partially reused out of order.
    """
    # A detach observed in an earlier batch is sticky for the rest of
    # the run: the intra-run duplicate cache and the job-wide checker
    # hold identities for files whose archive claim was rolled back,
    # and consulting them against a remounted share would count fresh
    # card files as duplicates of copies that never completed. Refuse
    # every remaining file rather than risk a stale-cache hit.
    # See PR #1400 review (Codex P2 r3688614624).
    if state.mount_ever_lost:
        _fail_whole_batch(
            state, rel, batch,
            f"archive mount root {state.mount_ever_lost} detached "
            "earlier in this import; the intra-run duplicate "
            "cache still holds identities for files whose "
            "archive claim was rolled back, so no further batch "
            "can be trusted to consult it",
        )
        emit(
            f"{rel}: archive unmounted", state.emitted, queued,
        )
        return None

    # Normalize so the "/" strftime puts in ``rel`` (e.g. "2026/07-03")
    # lines up with what scanner stores. Scanner wraps paths in
    # ``Path(...)`` before writing the folder row and building its
    # restrict_files set, which on Windows rewrites mid-path "/" to
    # "\\"; a raw os.path.join here would leave copied files invisible
    # to the restricted scan and unfindable in the post-scan lookup.
    dest_folder = (
        os.path.normpath(os.path.join(ctx.destination, rel))
        if rel != "." else ctx.destination
    )
    # Reject the whole batch before creating any directories on the
    # card. The per-file guard (``_reject_source_backed_dest``, both
    # paths) already refuses ``dest_file``
    # under a source, but that check runs AFTER ``os.makedirs``, so
    # a rejected unsafe import would still create the archive
    # directory tree on the source (or raise on read-only media,
    # killing the background job instead of returning a controlled
    # unsafe result). This mirror at the batch boundary keeps the
    # failure quiet and preserves the ``ok`` field for the API.
    # See PR #1107 review.
    #
    # On the remote path the same guard also protects the duplicate
    # gate: when the mount base is an ancestor of a selected source and
    # the folder template maps back into that source folder,
    # ``dest_folder`` (and therefore every ``cand_mount`` under it)
    # resolves inside a source root. The per-file collision walk would
    # hash those source-backed candidates, byte-match them against the
    # card, and count them as ``skipped_duplicate`` — with
    # ``verify_by_hash=true`` that would let ``safe_to_format`` go
    # green over a card whose bytes never crossed the network. See PR
    # #1113 review. (Since spec decision 13 the shared walk refuses
    # source-backed candidates on its own, so that is now defense in
    # depth rather than the only guard — but this batch-level refusal
    # is still the one that keeps ``os.makedirs`` off the card, and it
    # is still the honest verdict for a geometry where EVERY candidate
    # is poisoned rather than just one entry.)
    if ctx.path_under_any_source(dest_folder):
        _fail_whole_batch(
            state, rel, batch,
            "destination folder resolves inside a source directory "
            "(dest_folder would be created under the card being "
            "imported); formatting the card would erase the archive "
            "copy",
        )
        emit(
            f"{rel}: {_counts(state, rel)['copied']} copied · "
            f"{_counts(state, rel)['skipped_duplicate']} already present",
            state.emitted, queued,
        )
        return None
    if _refuse_unmounted_archive(
        state, emit, rel=rel, batch=batch, queued=queued, ctx=ctx,
        missing_root_check=missing_root_check,
    ):
        return None
    return _create_dest_folder(
        state, emit, db, rel=rel, batch=batch, queued=queued,
        dest_folder=dest_folder,
    )


def _refuse_unmounted_archive(state, emit, *, rel, batch, queued, ctx,
                              missing_root_check):
    """``_batch_preflight``'s mount checks: missing mount root, then stale
    mount. Returns True when the batch was refused (every file booked
    failed and the refusal phase emitted). Called only from
    ``_batch_preflight``, after the sticky mount-loss and dest-under-source
    refusals and before ``_create_dest_folder``.
    """
    # Function-level import: pipeline_job imports from this module.
    from pipeline_job import _unmounted_since_baseline

    # Mount-root check: if the archive's mount root has gone (share
    # never attached, or unmounted mid-run), refuse the batch instead
    # of letting ``os.makedirs`` recreate the vacated mount point as a
    # plain local directory. That shadow tree would take the card's
    # bytes onto the internal disk, look like a successful import, and
    # then hide the moment the real share remounts over it. Probed per
    # batch rather than once up front: a card import runs for hours
    # against a network archive, and the share can drop *during* the
    # run — a Tailscale/SMB archive did exactly that on 2026-07-30
    # (unmounted two hours into an import, after which ``os.makedirs``
    # walked straight into the vacated mount point), and only a
    # root-owned ``/Volumes`` turned the shadow write into a crash
    # instead of silent data misplacement. A start-of-job preflight
    # cannot see a mid-run drop; the probe is a couple of
    # ``os.path.lexists`` calls on the mount root, so paying it once
    # per destination folder is free next to the copy work.
    #
    # On the remote (SSH) path the stakes are the same shape: the SSH
    # rsync still writes to the NAS, but the subsequent scan reads the
    # fresh local shadow and leaves the import uncataloged/failed;
    # worse, on macOS/Linux that shadow root can also prevent the real
    # share from remounting at the configured path. ``missing_root_check``
    # wraps the pipeline path's ``_missing_archive_mount_root`` helper
    # (only fires for the ``/Volumes/X``, ``/mnt/X``, and
    # ``/media/user/X`` shapes that denote removable/network mount
    # roots). See PR #1113 review.
    missing_mount_root = missing_root_check()
    if missing_mount_root:
        _fail_whole_batch(
            state, rel, batch,
            f"archive mount root {missing_mount_root} is not "
            "available (destination drive is not mounted; refusing "
            "to create a shadow directory tree under it, which "
            "would prevent the real share from remounting)",
        )
        # Specific refusal phase — spec decision 3.
        emit(
            f"{rel}: archive unavailable", state.emitted, queued,
        )
        return True
    # The check above only sees a mount point that VANISHED. Linux
    # keeps ``/mnt/<name>`` as an empty directory after the share
    # detaches, so there the destination still "exists" and
    # ``os.makedirs`` below would happily build the archive tree on
    # the system disk and copy the card into it — bytes that vanish
    # under the real share the moment it remounts, after
    # safe_to_format may already have gone green. Catch that by
    # comparing against the baseline taken at job start: only a
    # mounted → unmounted transition counts, so an ordinary local
    # directory that merely looks mount-shaped is never refused.
    # Matters more on the remote path, not less: rsync keeps pushing
    # to the NAS while the batch scan reads a local shadow, so the
    # import lands bytes remotely and catalogs nothing.
    stale_mount_root = _unmounted_since_baseline(ctx.mount_baseline)
    if stale_mount_root:
        _fail_whole_batch(
            state, rel, batch,
            f"archive mount root {stale_mount_root} is no longer "
            "mounted (it was at the start of this import; the "
            "directory persists but the share has detached, so "
            "copying here would write to the local disk under a "
            "stale mount point)",
        )
        emit(
            f"{rel}: archive unmounted", state.emitted, queued,
        )
        return True
    return False


def _create_dest_folder(state, emit, db, *, rel, batch, queued, dest_folder):
    """``_batch_preflight``'s last steps: create ``dest_folder`` and promote
    its folder row out of ``'missing'``. Returns ``dest_folder``, or
    ``None`` when the folder could not be created (the batch is booked
    failed and the refusal phase emitted). Called only from
    ``_batch_preflight``, once every mount check has passed.
    """
    # A stale-but-registered mount (ismount still true while reads
    # fail), a read-only or permission-changed parent, or a full
    # disk all still surface here — and an uncaught OSError out of
    # this call tears down the whole background job, which is exactly
    # how the 2026-07-30 import died after two hours. Whatever the
    # cause, book it as a per-file failure and keep the card marked
    # unsafe to format instead of crashing the run.
    try:
        os.makedirs(dest_folder, exist_ok=True)
    except OSError as e:
        _fail_whole_batch(
            state, rel, batch,
            f"could not create destination folder {dest_folder}: {e}",
        )
        emit(
            f"{rel}: destination unavailable", state.emitted, queued,
        )
        return None

    # Promote any pre-existing folder row for this destination out of
    # ``'missing'``. Standalone scans run ``folders.check_health()`` as
    # their preflight, so a reattached archive drive transitions
    # ``missing`` → ``ok`` before its files become visible in the
    # workspace again. The import path calls ``scanner.scan()``
    # directly, and scan's success stamp only clears ``'partial'``
    # (see ``_update_folder_status(only_from_partial=True)``), so a
    # folder row still marked ``'missing'`` (e.g. from a prior health
    # check when the NAS mount was absent) would keep the archive
    # drive's photos filtered out of workspace queries even after this
    # import successfully lands and hash-stamps files into it, and
    # safe_to_format could go green over folders the UI won't show.
    # We just makedirs'd ``dest_folder`` so the path definitely exists;
    # any row still labelled ``'missing'`` is stale. Preserve
    # ``'partial'`` (a real prior-scan signal that the folder needs a
    # rescan). See PR #1107 / PR #1113 reviews.
    db.imports.promote_missing_folder_at(dest_folder)
    db.commit()

    return dest_folder


def _rollback_on_mount_loss(state, batch_st, attests_bytes,
                            extra_rollback=None, *, db=None, workspace_id=None):
    """Undo this batch's bookings after a mount-loss detection.

    The caller keeps the final post-loop probe at its own call site and
    calls this only when ``batch_st.mount_lost`` is set. A detach
    invalidates every accepted "already present" claim in this batch —
    the twin each one matched may be a shadow file on the mount stub
    rather than a real object on the archive — and everything landed
    before the detach may sit in the local shadow too.

    ORDER IS LOAD-BEARING: dup_skips rollback → grant/promotion rollback
    (same proof as the dup_skips: a detached mount invalidates the twin
    the grant rested on) → ``extra_rollback`` (both paths pass it — it
    drops the queued-but-untransferred ``to_transfer`` entries; a no-op
    locally) → landed rollback → ``state.mount_ever_lost`` LAST.
    The run-wide sticky flag asserts "this batch's bookings were rolled
    back", so it must not trip until every rollback above has run — an
    exception partway through must not leave later batches refused
    while this batch's counts still stand.

    ``db`` and ``workspace_id`` are passed by the batch loop and used to
    revoke the ``workspace_photos`` grants (and demote the
    status-promoted folders) this batch's accepted duplicates created
    via :meth:`PhotoVisibilityRepository.grant_verified_twins_tracked`. Pre-existing
    grants are deliberately not touched: the tracked grant path recorded
    only the ids this batch inserted anew.
    """
    # Accepted duplicate skips rest on a twin that may live in the
    # local shadow rather than on the share, so a detach invalidates
    # the "already present" claim exactly as it invalidates a copy.
    # These never enter ``landed``, so they need their own rollback:
    # without it a duplicate-only batch reports every file safely
    # accounted for and the card looks safe to erase over an archive
    # holding none of the bytes. ``dup_dirs`` is dropped for the same
    # reason — linking those folders would pull shadow paths into the
    # workspace. See PR #1396 review (Codex P1 r3687506040).
    for skipped_file, counted_unverified in batch_st.dup_skips:
        state.skipped_duplicate -= 1
        _counts(state, batch_st.rel)["skipped_duplicate"] -= 1
        if counted_unverified:
            state.unverified_duplicate -= 1
        _fail(
            state, batch_st.rel, skipped_file,
            f"archive mount root {batch_st.mount_lost} detached mid-batch; "
            "the duplicate this file matched cannot be confirmed "
            "to be on the archive rather than in a local shadow",
        )
    batch_st.dup_skips = []
    batch_st.dup_dirs = set()
    # The accepted duplicate skips above also committed workspace_photos
    # grants (and may have promoted folders out of ``'missing'``) for
    # verified twins. The mount detach invalidates the proof those rested
    # on — a shadow file on the detached mount stub can match the source
    # bytes just as a real archive object can — so a failed batch must
    # not leave workspace visibility expanded or make a missing folder
    # read as present. Revoke only what the tracked grant path inserted
    # this batch, leaving pre-existing grants and prior promotions
    # untouched.
    if db is not None and workspace_id is not None:
        if batch_st.dup_granted_photo_ids:
            db.photo_visibility.revoke_grants(workspace_id, batch_st.dup_granted_photo_ids)
        if batch_st.dup_promoted_folder_ids:
            db.photo_visibility.demote_folders_to_missing(batch_st.dup_promoted_folder_ids)
        if batch_st.dup_granted_photo_ids or batch_st.dup_promoted_folder_ids:
            db.commit()
    batch_st.dup_granted_photo_ids = []
    batch_st.dup_promoted_folder_ids = []
    if extra_rollback is not None:
        extra_rollback()
    # Anything that landed BEFORE the detach is sitting in the local
    # shadow, not on the archive. Roll those out of copied/
    # skipped_duplicate into failed via the origin-switching helper
    # and drop them: cataloging them would record archive paths for
    # bytes that vanish when the real share remounts, and leaving them
    # booked as copied could let safe_to_format go green over a card
    # that is still the only copy. Emptying ``landed`` also skips the
    # catalog scan for this batch.
    for entry in batch_st.landed:
        _reclassify_landed_failed(
            state, batch_st.rel, entry,
            f"archive mount root {batch_st.mount_lost} detached "
            "mid-batch; this file landed in a local shadow "
            "of the archive, not on the share",
            attests_bytes,
        )
    batch_st.landed = []
    # Trip the run-wide sticky flag so every remaining batch is
    # refused at the top of the loop rather than allowed to consult
    # the intra-run duplicate cache (which still holds identities
    # for the files just rolled back above; the checker has no
    # removal API). LAST — see the docstring. See PR #1400 review
    # (Codex P2 r3688614624).
    state.mount_ever_lost = batch_st.mount_lost


def _drop_queued_transfers(state, batch_st, rel):
    # Queued-but-untransferred files: nothing landed, so no
    # counter rollback — book the failure and drop the queue
    # so the rsync below has nothing to send.
    #
    # (Formerly a nested def whose default-arg binding of
    # ``batch_st`` / ``rel`` silenced ruff B023; module-level
    # explicit parameters replace that binding.)
    for queued_file, _dest_basename, _queued_hash, _sz, _mt \
            in batch_st.to_transfer:
        _fail(
            state, rel, queued_file,
            f"archive mount root {batch_st.mount_lost} detached before "
            "this file was transferred",
        )
    batch_st.to_transfer = []
