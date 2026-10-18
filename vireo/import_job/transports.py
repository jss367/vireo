"""The local-copy and rsync (SSH) transports that move a batch's bytes."""

import functools
import os
import posixpath

from import_dedup import compute_file_hash
from import_job.adoption import _WALK_CANCELLED, _WALK_HANDLED, _CardFile, _resolve_dest_collision
from import_job.copy_verify import DestReadCancelled, copy_and_hash_verify
from import_job.progress import _IMPORT_ETA_PROGRESS_KEYS
from import_job.state import _counts, _fail, _LandedFile, _record_checker
from job_contract import progress_event
from scanner import EMPTY_FILE_SHA256

# Verdicts returned by the transports' ``enqueue``. The CALLER maps them
# onto the per-file loop's control flow, and the mapping is load-bearing:
# _ENQ_CANCELLED must map to ``break`` (Stop interrupted a destination
# read inside the transport — every further step this batch would touch
# the same, possibly dead, mount; mapping it to ``continue`` would
# reintroduce the wedged-mount cancellation pin fixed in PR #1423) — the
# same contract as ``_GATE_CANCELLED``. _ENQ_HANDLED maps to ``continue``
# (the file reached a terminal bucket or an adopted landing inside the
# transport); _ENQ_QUEUED also advances to the next file — the bytes move
# later, in ``flush_batch``.
_ENQ_HANDLED = "handled"


_ENQ_QUEUED = "queued"


_ENQ_CANCELLED = "cancelled"


def _book_copied(state, rel, attests_bytes):
    """Book one landed copy into the run counters.

    The two pre-merge sites incremented in different statement orders
    (local: copied → verified → folder count; rsync: copied → folder
    count → verified-if-attested). The statements are independent
    increments with no reads between them, so one order is picked here:
    copied → folder count → verified when the transport attests bytes
    (``attests_bytes=True`` reproduces the local transport's
    unconditional increment).
    """
    state.copied += 1
    _counts(state, rel)["copied"] += 1
    if attests_bytes:
        state.verified += 1


def _landed_copied(source_file, *, dest_path, verified_hash, src_size,
                   src_mtime_ns):
    """The ``origin="copied"`` ledger entry, shared by both transports.

    ``verified_hash`` arrives ledger-normalized (``EMPTY_FILE_SHA256``
    for zero-byte, never None) — see the call sites' normalization
    comments for the raw-vs-normalized split.
    """
    return _LandedFile(
        dest_path=dest_path,
        verified_hash=verified_hash,
        source_path=str(source_file),
        origin="copied",
        src_size=src_size,
        src_mtime_ns=src_mtime_ns,
    )


def _lazy_source_hash(checker, source_file):
    """Zero-arg callable returning ``source_file``'s hash, read at most once.

    The checker's ``content_hash`` when duplicate skipping is on (``None``
    for a zero-byte source), else ``compute_file_hash``. A read error
    propagates from the first call; later calls return None without
    re-reading.
    """
    _sh_cache = [False, None]

    def _src_hash_cached():
        if not _sh_cache[0]:
            _sh_cache[0] = True
            _sh_cache[1] = (
                checker.content_hash(source_file)
                if checker is not None
                else compute_file_hash(str(source_file))
            )
        return _sh_cache[1]

    return _src_hash_cached


class _LocalTransport:
    """Byte transport for a locally mounted archive destination.

    Every file is copied AND hash-verified inside ``enqueue``
    (``copy_and_hash_verify`` reads the landed bytes back), so every
    ``copied`` booking also increments ``verified`` — ``attests_bytes``
    is unconditionally True. Nothing is deferred to ``flush_batch``.
    """

    attests_bytes = True
    dest_noun = "archive"
    log_label = "Import"

    def __init__(self, params, ctx):
        self._params = params
        self._ctx = ctx

    def enqueue(self, state, batch_st, *, source_file, rel, checker,
                stop_requested):
        # Local-rebinding shim: the body below is the local path's
        # per-file middle (moved verbatim in PR 7; its collision walk is
        # shared with the rsync transport since PR 7b).
        dest_folder = batch_st.dest_folder
        _stop_requested = stop_requested
        # Capture card-side (size, mtime_ns) BEFORE the copy so the
        # deferred working-copy pass can identity-check the card
        # override at extraction time. Byte-identical files have the
        # same size AND mtime; a rewrite between now and the end-of-
        # run extractor bumps mtime, and a remounted different card
        # at the same path has an unrelated mtime for its coincidence
        # of same-sized file. Without this the size-only check would
        # accept a same-size collision and cache a working copy for
        # the wrong bytes. Stat errors are the same class as
        # copy_and_hash_verify's OSError handling — fail just this
        # source rather than escape and kill the whole background job.
        try:
            src_stat = source_file.stat()
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        src_size = src_stat.st_size
        src_mtime_ns = src_stat.st_mtime_ns
        try:
            # Source hash is potentially needed by the shared collision
            # walk (once per hashed candidate) and by the
            # copy_and_hash_verify src_hash arg. Compute lazily and cache
            # in a small closure so nothing hashes the card twice — and
            # so a run with no collisions never hashes the card at all.
            _src_hash_cached = _lazy_source_hash(checker, source_file)

            # Destination basename + collision handling, shared with
            # the rsync transport (``_resolve_dest_collision``). No
            # reservation map: bytes land inside this method, so the
            # filesystem itself is the reservation.
            verdict, dest_basename = _resolve_dest_collision(
                state, batch_st, self._ctx,
                _CardFile(source_file, rel, _src_hash_cached, src_size, src_mtime_ns),
                checker=checker, claims=None, stop_requested=_stop_requested,
            )
            if verdict is _WALK_HANDLED:
                return _ENQ_HANDLED
            if verdict is _WALK_CANCELLED:
                # Plain Stop between candidate probes. Book the run as
                # cancelled but leave ``dest_read_cancelled`` alone —
                # the mount is healthy, so this batch's earlier landings
                # must still be probed and cataloged.
                state.cancelled = True
                return _ENQ_CANCELLED
            dest_file = os.path.join(dest_folder, dest_basename)

            src_hash = (
                checker.content_hash(source_file)
                if checker is not None else None
            )
            ok, file_hash = copy_and_hash_verify(
                str(source_file), dest_file, src_hash=src_hash,
            )
        except DestReadCancelled:
            # Stop arrived mid-read against the destination (adopt/
            # collision hashing above). The file is neither copied nor
            # failed — it stays on the card for the next run, like any
            # file the cancel got to before its batch.
            state.cancelled = True
            batch_st.dest_read_cancelled = True
            return _ENQ_CANCELLED
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        if not ok:
            _fail(
                state, rel, source_file,
                "copy verification failed (destination bytes do not "
                "match the source)",
            )
            return _ENQ_HANDLED
        _book_copied(state, rel, self.attests_bytes)
        batch_st.landed.append(_landed_copied(
            source_file, dest_path=dest_file,
            # ``copy_and_hash_verify`` returns the destination-read hash
            # (EMPTY_FILE_SHA256 for zero-byte) — already ledger-normalized.
            verified_hash=file_hash,
            src_size=src_size, src_mtime_ns=src_mtime_ns,
        ))
        _record_checker(state, checker, source_file, dest_folder, file_hash)
        return _ENQ_HANDLED

    def flush_batch(self, state, batch_st, *, rel, checker):
        # Copies land (and are verified) inside ``enqueue``; nothing is
        # deferred, so there is never anything to flush —
        # ``batch_st.to_transfer`` is always empty on this transport. The
        # orchestrator calls this unconditionally on both transports.
        return


def _skip_queued_identical(state, batch_st, *, source_file, rel, checker,
                           src_hash):
    """Book an rsync-transport duplicate skip against a byte-identical
    file this batch already queued; returns True when booked.
    """
    # Intra-batch same-content dedup: an earlier file THIS BATCH
    # queued a byte-identical source under a different basename
    # (e.g. ``DSC_0001.jpg`` and ``DSC_0002.jpg`` with the same
    # bytes). The batch's rsync hasn't happened yet, so the DB
    # twin lookup and ``checker.match()`` in ``_duplicate_gate`` can't see it —
    # skip here to match the local path's post-copy behaviour.
    # Gated on ``checker`` (``skip_duplicates=True``): the local
    # path only skips same-content/different-name twins through
    # ``DuplicateChecker``, so when the user disabled duplicate
    # skipping both files must be rsynced/cataloged under
    # distinct names instead of the second one being silently
    # counted as ``skipped_duplicate`` (which would otherwise let
    # a verified run report ``safe_to_format=True`` because
    # ``copied + skipped_duplicate == discovered`` without an
    # off-card row for the second file). See PR #1113 review.
    if (
        checker is not None
        and src_hash is not None
        and src_hash in batch_st.queued_src_hashes
    ):
        state.skipped_duplicate += 1
        _counts(state, rel)["skipped_duplicate"] += 1
        batch_st.dup_skips.append((source_file, False))
        _record_checker(state, checker, source_file, batch_st.dest_folder, src_hash)
        return True
    return False


class _RsyncTransport:
    """Byte transport for a remote (SSH) archive destination (Task 2.7).

    ``enqueue`` reserves a destination basename and queues the file on
    ``batch_st.to_transfer``; ``flush_batch`` moves the queued bytes with
    the per-batch rsync (flat batch + per-file renamed transfers +
    optional ``remote_verify_files``) and books the outcomes. Transfers
    go to ``remote_path/subpath/<rel>`` over SSH (``move.remote`` plumbing);
    photos are cataloged at ``mount_path/subpath/<rel>`` —
    ``params.destination`` is the local mount base, so ``scan()`` walks
    the just-rsynced files exactly as it would a local copy. The mount
    is also what makes the shared duplicate gate honest here: a
    duplicate skip is only honest when the cataloged twin's bytes are
    confirmed at the destination, and the twin's file is locally
    readable through the mount, so the gate's on-disk re-hash contract
    applies unchanged.

    Verification honesty contract: rsync's own transfer integrity by
    default; a ``--checksum`` dry-run (``move.remote_verify_files``)
    only when ``params.verify_by_hash``. Catalog rows get
    ``hash_status='ok'`` + ``hash_checked_at`` ONLY on the checksum
    path; otherwise both stay NULL (no invented status values).
    Consequently a remote import without ``verify_by_hash`` honestly
    reports ``safe_to_format=False`` with the reason
    ``"enable verify_by_hash for remote verification"`` — the card is
    off-loaded but its landing wasn't independently hash-confirmed.
    Each ``_LandedFile`` carries the card-side hash so the shared
    stamping loop can cross-check the scanned MOUNT row against the
    hash confirmed on the NAS: without that carry-through, a stale or
    misconfigured mount base already containing ``<folder>/<filename>``
    would let scan() populate the row from unrelated bytes (or an
    unmounted base would leave no catalog trail at all) while blind
    stamping flipped ``safe_to_format`` green over storage this run
    never touched. See PR #1113 review.

    All ``move`` seams are resolved at call time as
    ``move_mod.<name>(...)`` (the package's re-exports) so the tests'
    monkeypatch surface on the ``move`` package keeps working unchanged.
    """

    dest_noun = "mount"
    log_label = "Remote import"

    def __init__(self, job, runner, params, ctx):
        self._job = job
        self._runner = runner
        self._params = params
        self._ctx = ctx
        # Preamble moved verbatim from the pre-merge remote import
        # function (PR 7 transport merge).
        rt = params.remote_target
        remote = rt["remote"]                 # build_remote_move_spec dict
        rsync_bin = rt.get("rsync_bin") or remote.get("rsync_bin")
        ssh_base = rt["ssh_base"]             # remote_path/subpath (NAS side)

        import move as move_mod

        self._remote = remote
        self._rsync_bin = rsync_bin
        self._ssh_base = ssh_base
        self._move_mod = move_mod

    @property
    def attests_bytes(self):
        # A landed file carries a destination-attested hash only when
        # ``params.verify_by_hash`` made the independent card->NAS check
        # (rsync alone attests nothing).
        return self._params.verify_by_hash

    def _emit_transfer(self, state, rel, transfer_current, transfer_total,
                       current_file):
        """Report one actually-transferred file of the current batch rsync.

        Leaves ``current``/``total`` (and the ETA fields derived from them)
        exactly as the last ordinary ``_emit`` set them: this is the truth
        channel next to the prepared-files counter, not a second driver of
        it. ``_emit`` clears the transfer keys, so they exist only while a
        batch is on the wire.

        (``state`` is a parameter rather than a capture: the pre-move
        closure captured the enclosing run's ``state`` for
        ``state.folder_counts``; a method constructed before the run state
        exists takes it per call instead.)
        """
        job = self._job
        runner = self._runner
        extra = {
            "phase_current": transfer_current,
            "phase_total": transfer_total,
            "phase_label": "Transferring batch",
            **{k: job["progress"][k] for k in _IMPORT_ETA_PROGRESS_KEYS
               if k in job["progress"]},
        }
        job["progress"]["phase_current"] = transfer_current
        job["progress"]["phase_total"] = transfer_total
        job["progress"]["phase_label"] = "Transferring batch"
        job["progress"]["current_file"] = current_file
        runner.update_step(
            job["id"], "import",
            current_file=current_file,
            progress={
                "current": job["progress"]["current"],
                "total": job["progress"]["total"], **extra,
            },
        )
        runner.push_event(
            job["id"], "progress",
            progress_event(
                f"{rel}: transferring",
                job["progress"]["current"], job["progress"]["total"],
                current_file,
                # The Import page re-renders the folder table from each
                # event, so a transfer event without the snapshot would
                # blank the table for the whole batch transfer. (``rel_``
                # to avoid confusion with this function's ``rel``
                # parameter.)
                folders={
                    rel_: dict(counts)
                    for rel_, counts in state.folder_counts.items()
                },
                **extra,
            ),
        )

    def enqueue(self, state, batch_st, *, source_file, rel, checker,
                stop_requested):
        # Local-rebinding shim: the body below is the remote path's
        # per-file middle, moved verbatim; these bindings keep its lines
        # byte-identical to the pre-move text.
        dest_folder = batch_st.dest_folder
        _fold_basename = self._ctx.fold_basename
        _stop_requested = stop_requested
        # Working-copy identity, captured at decision time — before
        # any bytes move — so a source that changes mid-transfer
        # cannot look clean to the working-copy identity check.
        # Mirrors the local path, which stats before it hashes (and,
        # like it, fails the file when the source cannot be stat'd —
        # stat-first also means a vanished source costs one syscall,
        # not a full hash read that gets thrown away). Spec
        # decision 7.
        try:
            st = source_file.stat()
            src_size, src_mtime_ns = st.st_size, st.st_mtime_ns
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        try:
            src_hash = (
                checker.content_hash(source_file)
                if checker is not None
                else compute_file_hash(str(source_file))
            )
        except OSError as e:
            _fail(state, rel, source_file, str(e))
            return _ENQ_HANDLED
        if _skip_queued_identical(
            state, batch_st, source_file=source_file, rel=rel,
            checker=checker, src_hash=src_hash,
        ):
            return _ENQ_HANDLED
        # Collision parity (FIX 2): rsync lands files flat by basename,
        # so two different card files with the same basename in one batch
        # would clobber on the NAS. Assign a distinct dest basename per
        # colliding file, mirroring ingest()/the local path: a byte-
        # identical file already at the destination (a prior run's copy,
        # or an earlier card file this batch) is a skip; a different one
        # advances through numeric suffixes. ``claimed_basenames`` tracks
        # names taken by earlier files IN THIS BATCH; the mount is
        # locally readable so already-landed bytes are checked on disk.
        # The walk itself is shared with the local transport (PR 7b);
        # ``src_hash`` is already computed above for the intra-batch
        # dedup, so the callable is a constant here rather than lazy.
        try:
            verdict, dest_basename = _resolve_dest_collision(
                state, batch_st, self._ctx,
                _CardFile(source_file, rel, lambda: src_hash, src_size, src_mtime_ns),
                checker=checker, claims=batch_st.claimed_basenames,
                stop_requested=_stop_requested,
            )
        except DestReadCancelled:
            # Stop arrived mid-read against a candidate on the (possibly
            # dead) mount. Neither this file nor the rest of the batch
            # may take another step: every one of them touches the same
            # mount, and advancing would pin cancelling for the mount's
            # own timeout. The file stays on the card for the next run.
            # (Before PR 7b this was caught per-candidate inside the loop
            # and re-checked after it; the walk propagates instead, so
            # both transports book a dest-read cancel in exactly one
            # place. See PR #1423.)
            state.cancelled = True
            batch_st.dest_read_cancelled = True
            return _ENQ_CANCELLED
        if verdict is _WALK_HANDLED:
            return _ENQ_HANDLED
        if verdict is _WALK_CANCELLED:
            # Plain Stop between candidate probes — see the local
            # transport's twin. Not a dest-read cancel: the mount is
            # healthy, so this batch's earlier adoptions must still be
            # probed and cataloged.
            state.cancelled = True
            return _ENQ_CANCELLED
        batch_st.claimed_basenames[_fold_basename(dest_basename)] = src_hash
        # Only track queued source hashes when duplicate skipping is
        # enabled — the intra-batch dedup that consults this map is
        # gated on ``checker`` above, so populating it with
        # ``skip_duplicates=False`` would just be dead state.
        if checker is not None and src_hash is not None:
            batch_st.queued_src_hashes[src_hash] = dest_folder
        batch_st.to_transfer.append(
            (source_file, dest_basename, src_hash,
             src_size, src_mtime_ns))
        return _ENQ_QUEUED

    def flush_batch(self, state, batch_st, *, rel, checker):
        # The remote path's transfer phase, moved verbatim in PR 7
        # (including its ``to_transfer``/``cancelled`` guard) and since
        # split into the ``_transfer_queued`` / ``_book_transferred``
        # steps below.
        remote = self._remote
        move_mod = self._move_mod
        ssh_dest = (
            posixpath.join(self._ssh_base, *rel.split("/")) if rel != "."
            else self._ssh_base
        )
        # Honor cancellation before any network transfer starts. The break
        # inside the per-file queue-building loop above sets ``cancelled``
        # and exits the loop, but ``to_transfer`` still holds files that
        # were queued (decided but not yet sent). Without this guard the
        # rsync block below would start copying a partial batch after Stop
        # was requested. Queued files that never rsync stay on the card
        # and will be picked up by the next run; adopted ``landed``
        # entries for files already visible on the mount are still
        # cataloged by the batch-scan block below. See PR #1113
        # review.
        if batch_st.to_transfer and not state.cancelled:
            extra_args = self._rsync_extra_args()
            rsync_target = move_mod.rsync_dest_spec(remote, ssh_dest)
            # rsync creates the leaf itself but not intermediate parents.
            ok_mkdir, mkdir_detail = move_mod._remote_mkdir_p(remote, ssh_dest)
            if not ok_mkdir:
                for sf, _bn, _sh, _sz, _mt in batch_st.to_transfer:
                    _fail(state, rel, sf,
                          f"remote mkdir failed for {ssh_dest}: {mkdir_detail}")
            else:
                transferred = self._transfer_queued(
                    state, batch_st, rel=rel, ssh_dest=ssh_dest,
                    rsync_target=rsync_target, extra_args=extra_args,
                )
                self._book_transferred(
                    state, batch_st, rel=rel, checker=checker,
                    transferred=transferred, rsync_target=rsync_target,
                )

    def _rsync_extra_args(self):
        """The rsync flags every transfer of this run passes."""
        remote = self._remote
        # ``--ignore-existing`` protects against basename-race overwrites:
        # two remote import jobs (or a job racing another writer) that
        # both passed the earlier mount-side os.path.exists check for
        # DSC_0001.jpg would otherwise both rsync to the same NAS name
        # with plain ``rsync -a``, and the second writer would clobber
        # the first's already-verified bytes. ``--ignore-existing`` tells
        # rsync's receiver to skip files that already exist there, so
        # the first landing's bytes stay put. On the verify path, the
        # subsequent ``rsync -an --checksum`` step then detects the
        # mismatch between the second writer's card bytes and the
        # first-writer bytes on the NAS and fails that specific file
        # honestly; without verification the honesty gate already
        # reports safe_to_format=False for the whole run, so a masked
        # race can't quietly flip the pill green. Crash-recovery already
        # avoids re-transferring files it saw on the mount (hash match
        # -> skip; hash mismatch -> advance to a suffix that doesn't
        # exist), so no legitimate flow relies on rsync overwriting an
        # existing destination file. See PR #1113 review.
        extra_args = [
            "-e", self._move_mod._ssh_rsh_string(remote),
            "--partial-dir=.rsync-partial",
            "--ignore-existing",
            # ``--copy-links`` dereferences symlinked source files so
            # the NAS receives their referenced bytes instead of a
            # symlink. The base command is ``rsync -a``, which preserves
            # symlinks; without ``-L`` a curated card folder that
            # symlinks to ``/Volumes/Card/DCIM/IMG_0001.JPG`` would send
            # the symlink itself to the NAS. With ``verify_by_hash``
            # the mount-side scan follows the symlink through the local
            # mount, so ``safe_to_format`` can still go green — and
            # then formatting/unmounting the card breaks the archived
            # copy. Card-local symlinks aren't legitimately preserved
            # by an archive of the card contents, and the local path's
            # ingest reads the referenced file bytes too. See PR #1113
            # review.
            "--copy-links",
        ]
        if remote.get("bwlimit_kbps"):
            extra_args.append(f"--bwlimit={int(remote['bwlimit_kbps'])}")
        return extra_args

    def _do_rsync(self, src_specs, target, dest_is_dir, extra_args,
                  progress_cb=None):
        """One streamed rsync; returns ``(rc, stderr, timed_out)``."""
        try:
            rc, stderr, timed_out = self._move_mod._run_rsync_streamed(
                None, target, [], len(src_specs), progress_cb,
                rsync_bin=self._rsync_bin, extra_args=extra_args,
                src_specs=src_specs,
                src_specs_dest_is_dir=dest_is_dir,
                # Stop must reach the subprocess: a slow batch
                # can otherwise pin "cancelling" for hours,
                # since cancellation is only observed at file/
                # batch boundaries the transfer never yields.
                # Non-blocking probe: this fires from the rsync
                # watchdog thread, and ``is_cancelled`` would
                # park it inside ``wait_if_paused`` on Pause,
                # disabling both stall detection and Stop until
                # the user resumes. Pause is handled at the
                # existing batch-boundary check in ``_transfer_queued``.
                cancel_check=lambda: self._runner.cancellation_requested(
                    self._job["id"]),
            )
            return rc, stderr, timed_out
        except OSError as exc:
            return 1, str(exc), False

    def _rsync_cancelled(self, rc):
        # A Stop mid-transfer kills the subprocess via the
        # cancel_check in ``_do_rsync`` and surfaces as a nonzero exit.
        # That is cancelled work, not a pile of per-file
        # failures: queued files stay on the card for the next
        # run, and whatever rsync landed before the kill is
        # adopted by crash-recovery like any mid-batch stop
        # (--partial-dir preserves the interrupted file).
        # Non-blocking probe matches the watchdog; the
        # batch-boundary ``is_cancelled`` in ``_transfer_queued`` is where
        # pause is actually observed.
        return rc != 0 and self._runner.cancellation_requested(
            self._job["id"])

    def _transfer_queued(self, state, batch_st, *, rel, ssh_dest,
                         rsync_target, extra_args):
        """Rsync this batch's queued files; return the ones that landed.

        Each returned entry is ``(sf, dest_basename, src_hash, src_size,
        src_mtime_ns, nas_full_path)``. Failures are booked here; a Stop
        sets ``state.cancelled`` and books nothing for the files it cut
        off.
        """
        job = self._job
        runner = self._runner
        remote = self._remote
        _emit_transfer = functools.partial(self._emit_transfer, state)
        # Split into the flat fast path (dest basename == card
        # basename) and collision-renamed files (transferred and
        # verified individually to an explicit NAS filename, since a
        # flat --files-from list to one dir can't rename).
        flat = [
            (sf, bn, sh, sz, mt)
            for sf, bn, sh, sz, mt in batch_st.to_transfer
            if bn == sf.name
        ]
        renamed = [
            (sf, bn, sh, sz, mt)
            for sf, bn, sh, sz, mt in batch_st.to_transfer
            if bn != sf.name
        ]
        transferred = []   # (sf, dest_basename, src_hash, src_size, src_mtime_ns, nas_full_path)
        batch_size = len(batch_st.to_transfer)
        # Flat batch: one rsync into the dir. rsync names each file
        # as it lands; forward that as the batch's honest
        # actually-crossed-the-network counter next to the
        # prepared-files counter (which already reads ``emitted``).
        if flat:
            rc, stderr, timed_out = self._do_rsync(
                [str(sf) for sf, _bn, _sh, _sz, _mt in flat],
                rsync_target, True,
                extra_args,
                progress_cb=lambda done, _tot, name, _label,
                _rel=rel, _bs=batch_size:
                    _emit_transfer(_rel, done, _bs, name))
            if timed_out:
                for sf, _bn, _sh, _sz, _mt in flat:
                    _fail(state, rel, sf, "rsync stalled (no progress)")
            elif self._rsync_cancelled(rc):
                state.cancelled = True
            elif rc != 0:
                for sf, _bn, _sh, _sz, _mt in flat:
                    _fail(state, rel, sf, f"rsync failed: {stderr.strip()}")
            else:
                for sf, bn, sh, sz, mt in flat:
                    transferred.append((
                        sf, bn, sh, sz, mt,
                        posixpath.join(ssh_dest, bn)))
        # Renamed files: one rsync each to the explicit NAS file
        # path (rsync <card> user@host:/dir/DSC_0001_1.jpg).
        for sf, bn, sh, sz, mt in renamed:
            if state.cancelled or runner.is_cancelled(job["id"]):
                state.cancelled = True
                break
            nas_full = posixpath.join(ssh_dest, bn)
            rc, stderr, timed_out = self._do_rsync(
                [str(sf)],
                self._move_mod.rsync_dest_spec(remote, nas_full), False,
                extra_args)
            if timed_out:
                _fail(state, rel, sf, "rsync stalled (no progress)")
            elif self._rsync_cancelled(rc):
                state.cancelled = True
                break
            elif rc != 0:
                _fail(state, rel, sf, f"rsync failed: {stderr.strip()}")
            else:
                transferred.append((sf, bn, sh, sz, mt, nas_full))
                # ``len(transferred)`` counts only files that truly
                # landed, so a failed flat batch can't inflate the
                # renamed files' transfer counter.
                _emit_transfer(rel, len(transferred), batch_size, bn)
        return transferred

    def _book_transferred(self, state, batch_st, *, rel, checker,
                          transferred, rsync_target):
        """Verify (under ``verify_by_hash``) and book each transferred file."""
        remote = self._remote
        rsync_bin = self._rsync_bin
        move_mod = self._move_mod
        attests_bytes = self.attests_bytes
        dest_folder = batch_st.dest_folder
        for sf, bn, src_hash, sz, mt, nas_full in transferred:
            dest_path = os.path.join(dest_folder, bn)
            # Independent verification (Task 2.7 FIX 1): card -> NAS,
            # opt-in behind ``verify_by_hash`` (it reads every NAS
            # byte; same knob the local path uses). This compares the
            # actual CARD file against its NAS counterpart — the only
            # check that confirms the card's bytes landed intact;
            # comparing the SMB mount view against the NAS would be
            # near-tautological (same physical storage). By default
            # the transfer relies on rsync's own integrity checking
            # and the run reports ``safe_to_format=False`` (honesty
            # gate below) because no independent hash was made.
            if self._params.verify_by_hash:
                if bn == sf.name:
                    v = move_mod.remote_verify_files(
                        rsync_bin, [str(sf)], rsync_target,
                        remote, dest_is_dir=True)
                else:
                    # Collision-renamed: verify against the explicit
                    # NAS name (file->file), not the card basename.
                    v = move_mod.remote_verify_files(
                        rsync_bin, [str(sf)],
                        move_mod.rsync_dest_spec(remote, nas_full),
                        remote, dest_is_dir=False)
                if v is not None:
                    name, detail = v
                    reason = (
                        f"remote verification failed "
                        f"({detail or name})"
                        if name == "__ERROR__"
                        else f"remote verification: '{name}' missing "
                             f"or differs at destination"
                    )
                    _fail(state, rel, sf, reason)
                    continue
            _book_copied(state, rel, attests_bytes)
            batch_st.landed.append(_landed_copied(
                sf, dest_path=dest_path,
                # checker.content_hash returns None for size-0;
                # the ledger convention is EMPTY, scan's row
                # convention is NULL — normalize here, once,
                # instead of in every consumer. Do NOT
                # normalize ``src_hash`` itself: the
                # intra-batch dedup maps
                # (claimed_basenames/queued_src_hashes), the
                # adopt gate, and ``_record_checker`` rely on
                # the None convention.
                verified_hash=src_hash
                if src_hash is not None
                else EMPTY_FILE_SHA256,
                src_size=sz, src_mtime_ns=mt,
            ))
            _record_checker(state, checker, sf, dest_folder, src_hash)


def _make_transport(job, runner, params, ctx):
    """Pick the run's byte transport from ``params.remote_target``.

    Local: every file is copied and hash-verified inside ``enqueue``.
    Rsync (SSH): ``enqueue`` reserves and queues; ``flush_batch`` moves
    the batch's bytes.
    """
    return (
        _RsyncTransport(job, runner, params, ctx)
        if params.remote_target is not None
        else _LocalTransport(params, ctx)
    )
