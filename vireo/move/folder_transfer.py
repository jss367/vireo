"""``move_folder``'s copy and its verification (``_FolderTransfer``)."""

import contextlib
import logging
import os
import posixpath
import shutil
import sys

from job_runner.work_control import JobCancelled

from .folder_catalog import _plan_moved_file_mtimes
from .mounted_verify import mounted_verification_target, verify_mounted_copy
from .remote import _remote_mkdir_p, _ssh_rsh_string, resolve_rsync_bin
from .transfer import (
    RSYNC_STALL_TIMEOUT,
    _copy_tree_with_progress,
    _rsync_finder_metadata_exclude_file,
    _run_rsync_streamed,
)
from .verify import (
    _first_missing_source_file,
    _fresh_copy_mismatch,
    _remote_verify_complete,
)

# The package's one logger, ``move``, shared by every module in it.
log = logging.getLogger("move")


def _folder_move_rsync_args(remote, dest_exists):
    """Choose ``move_folder``'s rsync binary, transport args and flags.

    Returns ``(rsync_bin, extra_args, rsync_flags, error)``; ``error`` is the
    move's refusal dict when a remote move has no usable GNU rsync.
    """
    # Use rsync for a robust copy. A merge/resume uses --ignore-existing so
    # rsync only creates files absent at the destination and NEVER overwrites
    # a file already there: this resumes an interrupted move (missing files get
    # copied, already-copied ones are left alone) while guaranteeing a merge
    # cannot destroy pre-existing destination data. A local fresh move uses
    # --checksum for integrity; a remote fresh move skips it (the destination
    # is empty, and the post-transfer --checksum dry-run verifies integrity).
    # Any genuine same-name collision (an existing dest file that differs from
    # the source) is left untouched here and caught by the post-copy
    # verification below, which then refuses to delete the originals.
    #
    # Remote uses --partial-dir (instead of plain --partial) so a stalled or
    # cancelled transfer leaves the partial in `.rsync-partial/` rather than
    # at the destination filename. That keeps --ignore-existing honest: only
    # *complete* dest files are skipped, so the next run resumes the partial
    # from `.rsync-partial/` instead of treating it as already-moved (which
    # would then fail the --checksum verify forever, stranding the partial
    # until the user manually deletes it).
    # Prefer a discovered GNU rsync for local moves on POSIX. Finder-launched
    # macOS apps usually inherit a sparse PATH, so a bare ``rsync`` resolves
    # to Apple's legacy openrsync even when Homebrew GNU rsync is installed.
    # openrsync has been observed spinning after a transient SMB short read;
    # GNU rsync exits with a useful error instead. Windows rsync distributions
    # expect POSIX-style paths and can misread a native ``C:\...`` source as
    # remote-shell syntax, so retain the prior bare-name behavior there. Keep
    # the bare-name fallback on POSIX when GNU rsync is unavailable (and the
    # shutil fallback in ``_FolderTransfer.copy_tree`` when no rsync exists at all).
    rsync_bin = "rsync"
    if sys.platform != "win32":
        rsync_bin = resolve_rsync_bin() or rsync_bin
    extra_args = None
    if remote:
        rsync_bin = remote.get("rsync_bin")
        if not rsync_bin:
            return None, None, None, {"moved": 0, "errors": [
                "No usable GNU rsync binary is available for remote moves. "
                "Install it or set the GNU rsync path in Settings."
            ]}
        extra_args = [
            "-e", _ssh_rsh_string(remote),
            "--partial-dir=.rsync-partial",
        ]
        if remote.get("bwlimit_kbps"):
            extra_args.append(f"--bwlimit={int(remote['bwlimit_kbps'])}")
        rsync_flags = ["--ignore-existing"] if dest_exists else []
    else:
        rsync_flags = ["--ignore-existing"] if dest_exists else ["--checksum"]
    return rsync_bin, extra_args, rsync_flags, None


def _rsync_copy_error(returncode, stderr, timed_out):
    """``move_folder``'s refusal dict for a stalled or failed rsync, or None."""
    if timed_out:
        mins = RSYNC_STALL_TIMEOUT // 60
        # rsync can emit the real cause (for example, the exact NAS file that
        # returned a short read) and then wedge instead of exiting.  Do not
        # throw that diagnostic away in favor of the generic watchdog text.
        # Bound it because stderr can contain one warning per source file.
        detail = stderr.strip()
        if len(detail) > 1000:
            detail = "…" + detail[-999:]
        reported = f" rsync reported: {detail}" if detail else ""
        return {"moved": 0, "errors": [
            f"rsync stalled — no progress for over {mins} minutes, so the "
            f"copy was stopped. Originals are untouched; re-run with "
            f"merge/resume to continue from where it left off.{reported}"
        ]}
    if returncode != 0:
        return {"moved": 0, "errors": [f"rsync failed: {stderr.strip()}"]}
    return None


class _FolderTransfer:
    """Copy the source tree to ``target`` and verify the copy.

    Owns the source-file count, the rsync binary and the planned
    ``file_mtime`` corrections. Reads the destination views and
    ``dest_exists`` from ``target``. Each phase returns the result dict to
    abort with, or None to continue. A failed phase removes only a
    destination this move created, never a pre-existing merge target, and
    never touches the source.
    """

    def __init__(self, db, src_path, target, *, remote, verify_contents,
                 progress_cb, progress, control=None):
        self.db = db
        self.src_path = src_path
        self.target = target
        self.remote = remote
        self.verify_contents = verify_contents
        # ``progress_cb`` is the caller's raw callback (may be None), handed
        # to the copy and timestamp helpers; ``progress`` is the coordinator's
        # None-safe wrapper around it.
        self.progress_cb = progress_cb
        self.progress = progress
        self.control = control
        self.total_files = 0
        self.rsync_bin = None
        self.mtime_updates = []

    def ensure_remote_parent(self):
        """For a fresh remote move, ensure the destination's PARENT directory
        exists on the NAS.

        rsync creates the leaf folder itself but not its intermediate parents,
        so a configured subpath like ``USA/2026`` that has never been written
        before would fail with ``mkdir ... failed: No such file or directory``
        even though every preceding check passed. Skip on a merge — if the leaf
        exists, the parents must too. Skip when the parent is empty (the bare
        base "/", or a transfer_dest with no parent component) since mkdir-p
        there is meaningless.
        """
        if self.remote and not self.target.dest_exists:
            parent_dir = posixpath.dirname(self.target.transfer_dest)
            if parent_dir and parent_dir != "/":
                ok, detail = _remote_mkdir_p(self.remote, parent_dir)
                if not ok:
                    return {"moved": 0, "errors": [
                        f"Couldn't create the remote destination's parent "
                        f"directory '{parent_dir}' on the NAS: {detail}. "
                        f"Check permissions or pre-create the subpath."
                    ]}
        return None

    def copy_tree(self):
        src_path = self.src_path
        remote = self.remote
        dest_exists = self.target.dest_exists
        progress_cb = self.progress_cb
        # Count source files up front so the copy phase reports against a real
        # denominator from the first file. This count is the progress denominator
        # only — the fresh-move verification below deliberately recounts the
        # source at verify time rather than trusting this pre-copy number.
        total_files = sum(1 for _, _, files in os.walk(src_path) for _ in files)
        self.total_files = total_files
        self.progress(0, total_files, "", "Copying files")

        rsync_bin, extra_args, rsync_flags, error = _folder_move_rsync_args(
            remote, dest_exists)
        if error is not None:
            return error
        self.rsync_bin = rsync_bin
        # A merge deliberately skips Finder metadata (``.DS_Store``). Without the
        # exclude, rsync ``--ignore-existing`` would leave a same-name file at the
        # destination in place -- fine on its own -- but the post-copy verifier
        # would then compare bytes/size and refuse the delete, so the originals
        # would be preserved for a difference that is not a photo collision.
        # Excluding on the copy AND the checks keeps the source's copy in place
        # until the whole tree is removed after a successful merge; a failed
        # merge leaves it untouched with the rest of the source.
        exclude_ctx = (_rsync_finder_metadata_exclude_file(src_path)
                       if dest_exists else contextlib.nullcontext([]))
        with exclude_ctx as metadata_excludes:
            rsync_flags += metadata_excludes
            try:
                returncode, stderr, timed_out = self._copy_rsync(
                    rsync_flags, total_files, progress_cb, rsync_bin, extra_args)
            except FileNotFoundError:
                if remote:
                    # No shutil fallback over SSH — the binary path was resolved
                    # before the move started, so this means it vanished. Surface
                    # it plainly.
                    return {"moved": 0, "errors": [
                        f"GNU rsync not found at '{rsync_bin}'. Install GNU rsync "
                        f"or set its path in Settings."
                    ]}
                # Local rsync missing: fall back to shutil. skip_existing mirrors
                # --ignore-existing for a merge; a fresh move copies everything.
                try:
                    _copy_tree_with_progress(
                        src_path, self.target.catalog_path, dest_exists, total_files,
                        self._copy_progress if self.control else progress_cb,
                        **({"control": self.control} if self.control else {}),
                    )
                    returncode, stderr, timed_out = 0, "", False
                except JobCancelled:
                    # A paused job released its serialization slot. Another
                    # transfer may now own files in this destination; cancel
                    # must never run destructive fresh-copy cleanup here.
                    raise
                except Exception as exc:
                    log.warning("Copy fallback failed for %s", src_path, exc_info=True)
                    # Only remove a destination we created — never one that
                    # pre-existed (a merge target may hold the user's own files).
                    if self._owns_destination():
                        shutil.rmtree(self.target.catalog_path, ignore_errors=True)
                    return {"moved": 0, "errors": [f"Copy failed: {exc}"]}
            except OSError as exc:
                # rsync exists but could not be started: not executable
                # (PermissionError), an argv over the platform limit (E2BIG),
                # or the like. Popen failed, so nothing was copied and the
                # destination is as it was; report it as the move's failure
                # rather than letting a raw exception end the job.
                log.warning("Could not start rsync (%s) for %s", rsync_bin,
                            src_path, exc_info=True)
                return {"moved": 0, "errors": [
                    f"Couldn't start rsync ('{rsync_bin}'): {exc}. Nothing was "
                    f"copied. Originals untouched."
                ]}

        error = _rsync_copy_error(returncode, stderr, timed_out)
        if error is not None:
            return error
        # rsync emits only transferred files. A successful merge also handled
        # the already-present files (and deliberately excluded Finder metadata),
        # so finish the phase's source-file count before entering verification.
        self.progress(total_files, total_files, "", "Copying files")
        return None

    def _owns_destination(self):
        """A released transfer slot invalidates ownership of a fresh copy."""
        return not self.target.dest_exists and not (self.control and self.control.generation)

    def _copy_progress(self, current, total, filename, phase):
        """The shutil fallback has finished the file before this checkpoint."""
        self.control.checkpoint()
        self.progress(current, total, filename, phase)

    def _copy_rsync(self, flags, total, progress_cb, rsync_bin, extra_args):
        """Reap rsync on Pause; keep completed files and resume partials."""
        if self.control is None:
            return _run_rsync_streamed(
                self.src_path, self.target.rsync_target, flags, total,
                progress_cb, rsync_bin=rsync_bin, extra_args=extra_args)
        # Partial files must never occupy final names: --ignore-existing
        # would otherwise skip a truncated file forever on Resume or Retry.
        flags = [*flags, "--partial-dir=.rsync-partial"]
        while True:
            self.control.checkpoint()
            if self.control.generation:
                # Files may have arrived while the transfer slot was released.
                # Resume must preserve them, even if this began as a fresh copy.
                flags = [flag for flag in flags if flag != "--checksum"]
                if "--ignore-existing" not in flags:
                    flags.append("--ignore-existing")
            stopped = False

            def stop_requested():
                nonlocal stopped
                stopped = stopped or self.control.stop_requested()
                return stopped

            result = _run_rsync_streamed(
                self.src_path, self.target.rsync_target, flags, total,
                progress_cb, rsync_bin=rsync_bin, extra_args=extra_args,
                cancel_check=stop_requested)
            if not stopped:
                return result
            self.control.checkpoint()

    def plan_mtime_corrections(self):
        """Read the destination's timestamps BEFORE verifying, not after.

        This pass stats every catalogued photo on both sides, which on a
        network mount is minutes of wall clock. Running it between
        verification and the ``rmtree`` below would push those minutes into
        the one window where a destination file going missing, or being
        replaced, is never noticed before the originals are deleted. Ordering
        it ahead of verification means the byte-level check remains the last
        thing that touches the destination, exactly as it was before this pass
        existed -- so no re-verification is owed, and the expensive one never
        runs twice.

        Safe to read this early: rsync has finished, so the destination is
        final, and the plan is read-only until it is applied after the cascade.

        Local destinations only. For a remote move ``transfer_dest`` lives on
        the far side of an SSH connection and cannot be stat'd, and reaching
        the same files back through ``catalog_path`` would walk a mount that
        need not even be mounted for the transfer to have succeeded -- for a
        rename rsync performed on the remote filesystem, where the timestamp
        is preserved anyway.
        """
        if not self.remote:
            self.mtime_updates, problem = _plan_moved_file_mtimes(
                self.db, self.src_path, self.target.transfer_dest,
                progress_cb=self.progress,
            )
            if problem is not None:
                # Verification below would catch this too. Failing here just
                # spares the user a full byte-for-byte pass over a destination
                # already known to be incomplete.
                #
                # Clean up the same way the fresh-move count check does. A
                # destination this move created is ours to remove, and leaving a
                # partial tree behind would turn the documented all-or-nothing
                # retry into one that demands a merge. A destination that was
                # already there is never removed -- it may hold the user's own
                # files.
                if self._owns_destination():
                    shutil.rmtree(self.target.transfer_dest, ignore_errors=True)
                return {"moved": 0, "errors": [
                    f"Verification failed: {problem}. Originals preserved."
                ]}
        return None

    def verify_copy(self):
        """Verify before deleting originals."""
        src_path = self.src_path
        transfer_dest = self.target.transfer_dest
        dest_exists = self.target.dest_exists
        if self.remote:
            # One rsync dry-run with no per-file output: no count to show.
            self.progress(0, 0, "", "Verifying copy")
        else:
            # Recounted rather than reusing the pre-copy ``total_files`` so the
            # bar's denominator is the tree the verifier actually walks.
            verify_total = sum(1 for _, _, files in os.walk(src_path)
                               for _ in files)

            def report(checked, name):
                self.progress(checked, max(verify_total, checked), name,
                              "Verifying copy")
            report(0, "")
        if self.remote:
            # The local filesystem can't be walked to confirm a remote copy, so
            # run a --checksum dry-run over SSH: any file it would still transfer
            # is missing or differs at the destination. Covers both fresh and
            # merge moves, and is the safety backstop replacing the local
            # content-conflict and file-count checks.
            verify = _remote_verify_complete(self.rsync_bin, src_path,
                                             self.target.rsync_target, self.remote,
                                             is_merge=dest_exists,
                                             **({"control": self.control} if self.control else {}))
            if verify is not None:
                name, detail = verify
                if name == "__ERROR__":
                    return {"moved": 0, "errors": [
                        f"Verification could not be completed ({detail}). "
                        f"Originals preserved."
                    ]}
                return {"moved": 0, "errors": [
                    f"Verification failed: '{name}' is missing or differs at the "
                    f"destination. Originals preserved."
                ]}
        elif dest_exists or self.verify_contents:
            # Merge: the destination may legitimately hold extra unrelated
            # files (and leftover temp files from an interrupted run), so a
            # count comparison is meaningless. Instead require that every
            # source file is present at the destination with a matching size.
            missing = self._verify_local_copy(report)
            if missing is not None:
                return {"moved": 0, "errors": [
                    f"Verification failed ({missing}). The source and destination "
                    f"could not be confirmed to match. Originals preserved."
                ]}
        else:
            verify_error = _fresh_copy_mismatch(src_path, transfer_dest,
                                                progress=report)
            if verify_error is not None:
                if self._owns_destination():
                    shutil.rmtree(transfer_dest, ignore_errors=True)
                return {"moved": 0, "errors": [
                    f"Verification failed: {verify_error}. Originals preserved."
                ]}
        return None

    def _verify_local_copy(self, report):
        """Use a proven SSH route for mounted byte checks when available."""
        remote = None
        if self.verify_contents:
            remote = mounted_verification_target(
                self.target.transfer_dest, self.db.effective_config())
        if remote:
            handled, mismatch = verify_mounted_copy(
                self.src_path, self.target.transfer_dest, remote,
                is_merge=self.target.dest_exists, progress=self.progress,
                control=self.control,
            )
            if handled:
                return mismatch

            total = sum(len(files) for _, _, files in os.walk(self.src_path))

            def report(checked, name):
                self.progress(checked, max(total, checked), name,
                              "Verifying copy locally (NAS verification unavailable)")

        return _first_missing_source_file(
            self.src_path, self.target.transfer_dest,
            verify_contents=self.verify_contents, is_merge=self.target.dest_exists,
            progress=report)
