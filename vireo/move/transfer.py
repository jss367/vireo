"""Copying files for a move.

``_copy_and_verify`` copies one photo file; ``_run_rsync_streamed`` runs
rsync with per-file progress and a stall watchdog; ``_copy_tree_with_progress``
is the shutil fallback when no rsync exists. The Finder-metadata helpers
build the ``.DS_Store`` excludes a merge passes to every rsync it runs.
"""

import contextlib
import os
import shutil
import subprocess
import tempfile
import threading
import time

from job_runner.subprocess_control import process_group_kwargs, stop_process_tree

try:
    from ..proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs
try:
    from ..staged_copy import copy_via_temp
except ImportError:
    from staged_copy import copy_via_temp

# How long rsync may make NO forward progress (no file transferred and no
# stderr activity) before we treat it as wedged and kill it. This is a STALL
# watchdog, not a total-runtime cap: a healthy copy of thousands of RAW files
# over a slow network share can legitimately run for many hours, and it is
# allowed to as long as it keeps moving data. The window is generous because
# rsync is silent during its initial file-list build and the destination scan
# a merge (--ignore-existing) performs, which over a slow SMB share can take
# several minutes before the first file transfers.
#
# Known limitation: progress is detected per FILE (rsync emits one
# --out-format=%n line per item, not continuous byte progress), so a single
# file whose transfer exceeds this window over a very slow link could be
# killed mid-flight. That can't happen for Vireo's data — source files are RAW
# frames (tens of MB; ~12s/file even on the slow SMB share this was built for),
# orders of magnitude under the window — so it's accepted rather than paying
# the complexity of char-level --info=progress2 byte-progress parsing.
RSYNC_STALL_TIMEOUT = 1800  # 30 minutes


def _copy_and_verify(src, dst):
    """Copy a single file and verify size matches. Returns True on success.

    The copy goes through a hidden sibling temp file (``copy_via_temp``),
    so a failed copy never leaves a truncated file at ``dst`` that would
    block every retry as "already exists" and be cataloged by a rescan.
    ``OSError`` from the copy propagates for the caller to record against
    this one photo.
    """
    copy_via_temp(src, dst)
    if os.path.getsize(src) != os.path.getsize(dst):
        os.remove(dst)
        return False
    return True


def _copy_tree_with_progress(src_path, dest_path, skip_existing, total_files,
                             progress_cb, control=None):
    """Recursively copy src_path into dest_path, reporting each file copied.

    ``skip_existing`` mirrors rsync ``--ignore-existing`` (merge/resume): a
    destination file that already exists is never overwritten. Used only as
    the shutil fallback when rsync is unavailable, so a fresh move (creating
    dest_path) and a merge share one progress-emitting walk.
    """
    # os.walk swallows scandir errors by default, so an unreadable source
    # subdirectory would be silently skipped here — and the fresh-move count
    # verification (also a default os.walk) would skip it too, so the counts
    # match and the catalog update + rmtree(src) proceed on an incomplete
    # copy. shutil.copytree (the old fallback) raised instead; re-raise so the
    # caller aborts the move before anything is deleted.
    def _raise(err):
        raise err

    copied = 0
    created_dirs = []
    for root, dirs, files in os.walk(src_path, onerror=_raise):
        rel = os.path.relpath(root, src_path)
        target_dir = dest_path if rel == "." else os.path.join(dest_path, rel)
        os.makedirs(target_dir, exist_ok=True)
        created_dirs.append((root, target_dir))
        # os.walk lists a symlinked subdirectory in `dirs` but, with its
        # default followlinks=False, never recurses into it — so its contents
        # would be silently dropped here while the post-copy file-count
        # verification (also a default os.walk) skips it on both sides and
        # still matches, letting rmtree(src) delete the originals. Recreate
        # each directory symlink as a symlink at the destination, matching the
        # primary rsync -a path (which preserves symlinks rather than
        # following them) and keeping the verification counts consistent.
        for d in dirs:
            if control:
                control.checkpoint()
            src_sub = os.path.join(root, d)
            if not os.path.islink(src_sub):
                continue
            dst_sub = os.path.join(target_dir, d)
            if (skip_existing or (control and control.generation)) and os.path.lexists(dst_sub):
                continue
            os.symlink(os.readlink(src_sub), dst_sub)
        for fn in files:
            if control:
                control.checkpoint()
            # Merges skip Finder metadata (``.DS_Store``) — the destination
            # keeps its own copy, and the source's is discarded when the
            # source tree is removed after a successful merge. Fresh moves
            # carry them along, matching the primary rsync path.
            if skip_existing and fn in FINDER_METADATA_FILES:
                continue
            src_file = os.path.join(root, fn)
            dst_file = os.path.join(target_dir, fn)
            # lexists (not exists): a broken or symlinked destination entry
            # still counts as present for a merge, so we never dereference it
            # and write through to its target. exists() returns False for a
            # broken symlink and would fall through to copy2 below.
            if (skip_existing or (control and control.generation)) and os.path.lexists(dst_file):
                continue
            if os.path.islink(src_file):
                # Preserve the symlink rather than copy2's dereferenced target,
                # matching rsync -a and the directory-symlink handling above.
                os.symlink(os.readlink(src_file), dst_file)
            else:
                shutil.copy2(src_file, dst_file)
            copied += 1
            if progress_cb:
                progress_cb(copied, total_files, fn, "Copying files")

    # Mirror directory metadata (mode, mtime) for a fresh move, matching the
    # primary rsync -a path and the shutil.copytree this fallback replaced.
    # os.makedirs creates dirs with default permissions, so without this a
    # private 0700 source folder would land as 0755 and the original metadata
    # is lost once the source is deleted. copy2 above already preserves file
    # metadata. Run after all contents exist so child writes don't re-bump a
    # parent's mtime. Skipped on a merge: a pre-existing destination dir keeps
    # the user's own metadata rather than being overwritten with the source's.
    if not skip_existing and not (control and control.generation):
        for src_dir, target_dir in created_dirs:
            shutil.copystat(src_dir, target_dir)


def _run_rsync_streamed(src_path, dest_spec, rsync_flags, total_files,
                        progress_cb, rsync_bin="rsync", extra_args=None,
                        stall_timeout=RSYNC_STALL_TIMEOUT, src_specs=None,
                        src_specs_dest_is_dir=True, cancel_check=None):
    """Run rsync, reporting each transferred file through progress_cb.

    rsync's ``--out-format=%n`` prints the relative name of every item it
    transfers (directories end in ``/``). Streaming that line-by-line lets
    the move job show live per-file progress instead of a frozen bar while a
    large copy runs, without changing rsync's copy semantics.

    ``dest_spec`` is a local path for a local move or ``user@host:/path`` for
    a remote (SSH) move; ``rsync_bin`` selects the binary (a bundled GNU rsync
    for remote moves) and ``extra_args`` carries the SSH transport flags
    (``-e ssh ...``, ``--partial``, ``--bwlimit``). ``rsync_flags`` is a list;
    empty strings are dropped so a fresh move can pass no extra flag.

    By default the whole ``src_path`` directory is transferred (``src_path
    + "/"``), the shape ``move_folder`` uses. ``src_specs`` overrides this
    with an explicit list of source *file* paths — the import job's per-batch
    remote copy passes the batch's card files (which land flat by basename
    under ``dest_spec/``), so no local staging tree is materialized. When
    ``src_specs`` is given, ``dest_spec`` is suffixed with ``/`` (files land
    inside the destination directory) unless ``src_specs_dest_is_dir`` is
    False — the import job's collision-rename path passes a single source
    file and an explicit remote FILE path (e.g.
    ``user@host:/dir/DSC_0001_1.jpg``) so the renamed file lands under the
    chosen name rather than its own basename.

    Returns ``(returncode, stderr, timed_out)``. ``timed_out`` is True when
    rsync made no forward progress for ``stall_timeout`` seconds and was
    killed. This is a STALL watchdog rather than a total-runtime cap: a
    healthy but slow transfer (e.g. thousands of RAW files over a network
    share) runs for as long as it keeps moving data, while a genuinely wedged
    rsync still gets reaped. The clock resets on every transferred file and
    on stderr activity, so only true silence trips it.

    ``cancel_check`` (optional callable -> bool) is the caller's Stop
    signal: when it returns True the subprocess is killed within a couple
    of seconds instead of draining the batch. The kill surfaces as an
    ordinary nonzero returncode with ``timed_out`` False — the caller
    already holds the cancel flag it passed in, so it can tell a
    cancellation from a transfer failure without a fourth return value.

    stdout is attached to a pty, not a pipe, wherever the platform allows.
    Apple's openrsync block-buffers stdout when it's a pipe, so its
    ``--out-format`` lines arrive in one burst at process exit — the parent
    sees total silence while a healthy transfer runs, and the watchdog kills
    any copy slower than stall_timeout (a NAS archive of a large shoot can
    never finish). On a pty both openrsync and GNU rsync line-buffer, so
    per-file names stream as they transfer.
    """
    cmd = _rsync_streamed_command(
        src_path, dest_spec, rsync_flags, rsync_bin, extra_args,
        src_specs, src_specs_dest_is_dir)
    proc, stdout_stream = _spawn_rsync_on_pty(cmd)
    state = {"timed_out": False}
    # Last time rsync showed any sign of life (process start counts, so the
    # silent file-list/scan phase before the first transfer isn't a stall).
    last_activity = {"t": time.monotonic()}
    done = threading.Event()

    def _watchdog():
        # Poll well below stall_timeout so a stall is detected promptly once
        # the window elapses, without busy-waiting. With a cancel_check the
        # poll drops to 1s: Stop has to kill the subprocess within a couple
        # of seconds, not at the stall watchdog's leisurely interval.
        poll = 1.0 if cancel_check is not None else min(stall_timeout, 30)
        while not done.wait(poll):
            if cancel_check is not None and cancel_check():
                stop_process_tree(proc)
                return
            if time.monotonic() - last_activity["t"] > stall_timeout:
                state["timed_out"] = True
                stop_process_tree(proc)
                return

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    stderr_thread, stderr_chunks = _start_stderr_drain(proc, last_activity)

    completed = False
    try:
        _stream_rsync_progress(
            stdout_stream, total_files, progress_cb, last_activity)
        proc.wait()
        completed = True
    finally:
        done.set()
        if not completed:
            stop_process_tree(proc)
            proc.wait()
        if stdout_stream is not None and stdout_stream is not proc.stdout:
            with contextlib.suppress(OSError):
                stdout_stream.close()
        stderr_thread.join()
        watchdog.join()
    return proc.returncode, "".join(stderr_chunks), state["timed_out"]


def _rsync_streamed_command(src_path, dest_spec, rsync_flags, rsync_bin,
                            extra_args, src_specs, src_specs_dest_is_dir):
    """Build ``_run_rsync_streamed``'s rsync argv."""
    cmd = [rsync_bin, "-a", "--out-format=%n"]
    cmd += list(extra_args or [])
    cmd += [f for f in (rsync_flags or []) if f]
    if src_specs is not None:
        cmd += list(src_specs)
        cmd += [dest_spec + "/" if src_specs_dest_is_dir else dest_spec]
    else:
        cmd += [src_path + "/", dest_spec + "/"]
    return cmd


def _spawn_rsync_on_pty(cmd):
    """Start rsync with stdout on a pty where possible.

    Returns ``(proc, stdout_stream)``; the stream is the pty master when one
    was opened, else ``proc.stdout``. See ``_run_rsync_streamed`` for why.
    """
    master_fd = slave_fd = None
    if hasattr(os, "openpty"):
        try:
            master_fd, slave_fd = os.openpty()
        except OSError:
            master_fd = slave_fd = None
    stdout_arg = subprocess.PIPE if slave_fd is None else slave_fd
    try:
        proc = subprocess.Popen(
            cmd, stdout=stdout_arg, stderr=subprocess.PIPE, text=True,
            **no_window_kwargs(), **process_group_kwargs(),
        )
    except BaseException:
        # Popen can raise after os.openpty() succeeded (bad rsync_bin,
        # PermissionError, KeyboardInterrupt). Close both pty fds before
        # re-raising so a failed invocation doesn't leak two fds each time.
        for fd in (master_fd, slave_fd):
            if fd is not None:
                os.close(fd)
        raise
    stdout_stream = proc.stdout
    if slave_fd is not None:
        os.close(slave_fd)
        if stdout_stream is None:
            stdout_stream = os.fdopen(master_fd, "r", errors="replace")
            master_fd = None  # closed via stdout_stream below
    if master_fd is not None:
        os.close(master_fd)
    return proc, stdout_stream


def _start_stderr_drain(proc, last_activity):
    """Start draining rsync's stderr; return ``(thread, chunks)``."""
    # Drain stderr on a separate thread. rsync can emit a lot of stderr (e.g.
    # many permission-denied or "file vanished" notices); if that fills the
    # pipe buffer while this thread is blocked reading stdout, rsync blocks on
    # the stderr write and neither side progresses — a deadlock that the stall
    # watchdog would only break after stall_timeout. Reading both streams
    # concurrently keeps the error surfacing promptly, matching the old
    # subprocess.run(capture_output=True) behavior. Reading stderr is also a
    # liveness signal, so it resets the stall clock too.
    stderr_chunks = []

    def _drain_stderr():
        # Iterate line-by-line rather than read(4096): a fixed-size read on a
        # buffered text stream blocks until the buffer fills or EOF, so sparse
        # rsync diagnostics ("file vanished", permission notices) wouldn't
        # refresh last_activity until 4096 chars had accumulated — long enough
        # that the watchdog could kill a transfer that's still emitting
        # stderr. Line iteration yields each message as it's flushed, so every
        # stderr emission counts as liveness.
        for line in proc.stderr:
            stderr_chunks.append(line)
            last_activity["t"] = time.monotonic()

    stderr_thread = threading.Thread(target=_drain_stderr)
    stderr_thread.start()
    return stderr_thread, stderr_chunks


def _stream_rsync_progress(stdout_stream, total_files, progress_cb,
                           last_activity):
    """Report each file rsync's ``--out-format=%n`` stream names."""
    copied = 0
    try:
        for line in stdout_stream:
            # The pty's line discipline translates \n to \r\n (ONLCR),
            # so strip \r too or every filename grows a trailing CR.
            name = line.rstrip("\r\n")
            last_activity["t"] = time.monotonic()
            if not name or name.endswith("/"):
                continue  # directory entry, not a file
            copied += 1
            if progress_cb:
                progress_cb(copied, total_files, os.path.basename(name),
                            "Copying files")
    except OSError:
        # Reading the pty master after the child exits raises EIO on
        # Linux (macOS returns plain EOF) — both just mean end-of-output.
        pass


# Finder writes .DS_Store (window layout, icon positions) into every folder it
# opens, so a staged import the user browsed and the archive folder it merges
# into each hold their own copy with different bytes. That is not a collision
# between photos and must not refuse the merge. A merge ignores these on both
# sides: the pre-merge conflict check, the copy and the post-copy verification
# all skip them, so the destination keeps its own copy and the source's is
# discarded only when the whole source tree is removed after a successful
# merge. If any of those steps refuses the merge, the originals -- .DS_Store
# included -- are left untouched.
FINDER_METADATA_FILES = frozenset({".DS_Store"})


def _escape_rsync_pattern(component):
    """Escape rsync wildmatch metacharacters in a literal path component so
    the resulting include/exclude pattern matches only that spelling. rsync
    treats ``*``, ``?`` and ``[`` as wildcards in patterns (``**`` matches
    multiple components; ``\\`` escapes the next character), so an on-disk
    directory literally named ``a*`` would otherwise turn the generated
    ``--exclude=/a*/.DS_Store`` into a glob that also drops an unrelated
    ``abc/.DS_Store`` subtree — the transfer and verification skip those
    entries, and the post-merge source removal then destroys them.

    Backslash is intentionally encoded as the character class ``[\\\\]``
    rather than doubled. In wildmatch a bare ``\\`` escapes the next
    character (which fails for a trailing ``\\`` and is only interpreted as
    an escape when the pattern contains at least one other wildcard), so
    doubling to ``\\\\`` would emit a pattern that matches two literal
    backslashes on filesystems where wildmatch isn't triggered — a real
    sibling with two backslashes would then swap places with the intended
    target and the transfer/verify would drop its ``.DS_Store`` subtree
    from the merge, letting the post-merge source removal delete it. A
    character class matches exactly one literal ``\\`` under wildmatch
    (its brackets also force wildmatch mode for the other escapes in this
    same pattern) and is a stable rewrite for every literal component.

    A single character-by-character pass rather than chained ``str.replace``
    calls: the class ``[\\\\]`` inserted for a backslash contains a ``[``
    and two ``\\`` characters that would themselves be re-escaped by a
    later replace, either doubling the escape or breaking the class.
    """
    out = []
    for ch in component:
        if ch == "\\":
            # A character class matching exactly one literal backslash.
            # Emits 4 chars: [ \ \ ]
            out.append("[\\\\]")
        elif ch in "*?[":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def _rsync_finder_metadata_exclude_patterns(src_path):
    """Anchored rsync exclude patterns for regular Finder metadata FILES
    under ``src_path`` (one pattern per file, without any ``--exclude=``
    prefix — the caller decides whether to splat them as argv flags or
    stream them through ``--exclude-from``).

    Used for merges only. A fresh move still carries these along, matching
    the pre-fix behavior; only a merge into a destination that already
    holds its own copies needs to skip them so a Finder-managed difference
    doesn't fail the copy or the verification.

    The exclude has to skip regular files while leaving directories and
    symlinks with the same name alone: a pattern that matches every entry
    type would silently drop those from the transfer, the conflict probe
    and the checksum verify, and ``shutil.rmtree(src_path)`` after a
    "successful" merge would then delete them from the source without ever
    copying them. rsync's filter language can't distinguish entry type on
    its own, so we walk ``src_path`` ourselves, collect the regular-file
    ``.DS_Store`` entries (not directories, not symlinks — a ``.DS_Store``
    symlink to a directory is classified as a symlink by rsync and would
    slip past an ``--include=<name>/`` guard; a FIFO/socket/device lands in
    ``os.walk``'s ``files`` list too, and ``rsync -a`` would recreate it
    from ``-D``, so an exclude would drop it from every side of the merge
    while the post-merge ``shutil.rmtree`` still deletes it from the
    source), and emit an anchored ``/<relpath>`` for each. Every parent
    component in the relative path is passed through
    ``_escape_rsync_pattern`` so a real directory whose name contains rsync
    wildmatch metacharacters (``*``/``?``/``[``/``\\``) doesn't widen the
    exclude into a glob that catches unrelated sibling subtrees. The
    excludes apply to both sides of every rsync command they're passed to
    (transfer, ``--existing`` conflict probe, ``--checksum`` verify), so a
    same-path Finder-managed difference at the destination is skipped
    alongside its source twin, and every other entry type — including
    symlinks and any real directory that happens to be named
    ``.DS_Store`` — is untouched.
    """
    patterns = []
    if not os.path.isdir(src_path):
        return patterns
    for root, _, files in os.walk(src_path):
        for fn in files:
            if fn not in FINDER_METADATA_FILES:
                continue
            full = os.path.join(root, fn)
            if os.path.islink(full):
                continue
            if not os.path.isfile(full):
                continue
            rel = os.path.relpath(full, src_path).replace(os.sep, "/")
            escaped = "/".join(_escape_rsync_pattern(part)
                               for part in rel.split("/"))
            patterns.append(f"/{escaped}")
    return patterns


def _rsync_finder_metadata_excludes(src_path):
    """rsync ``--exclude=PATTERN`` args, one per regular Finder metadata
    file under ``src_path``. Thin wrapper around
    ``_rsync_finder_metadata_exclude_patterns`` for tests and callers that
    build a small argv directly; production merge callers should prefer
    ``_rsync_finder_metadata_exclude_file`` to keep argv fixed-size even
    when the source tree carries thousands of ``.DS_Store`` files (each
    ``--exclude=`` is its own argv entry, and ``execve``'s argument-size
    limit is finite on every platform — ~256 KB on older macOS — so a
    large enough merge would fail with ``E2BIG`` before rsync could start).
    """
    return [f"--exclude={p}"
            for p in _rsync_finder_metadata_exclude_patterns(src_path)]


@contextlib.contextmanager
def _rsync_finder_metadata_exclude_file(src_path):
    """Yield rsync flags that exclude every regular ``.DS_Store`` file
    under ``src_path`` via ``--exclude-from=<file>`` plus ``--from0``.

    Passing the patterns through a file (rather than a fresh ``--exclude=``
    argv entry per file, as ``_rsync_finder_metadata_excludes`` returns)
    keeps rsync's argv fixed-size no matter how many ``.DS_Store`` files
    the merge source contains. A photo archive with thousands of them can
    otherwise exceed the platform's ``execve`` argument-size limit before
    rsync starts (macOS is the tightest, historically ~256 KB) — the
    ``E2BIG`` that results is a plain ``OSError`` at ``subprocess.run``
    time, and the primary transfer only catches ``FileNotFoundError``, so
    it would escape the normal move-error path.

    Records are written NUL-delimited (bytes via ``os.fsencode``) and
    rsync is asked to parse them that way with ``--from0``. Newline is a
    legal POSIX filename byte, so a plain ``\\n`` join would split one
    pattern like ``/foo\\nbar/.DS_Store`` into two filter records
    (``/foo`` and ``bar/.DS_Store``) — rsync would then exclude an
    unrelated ``/foo`` subtree on both the transfer and the ``--checksum``
    verify, letting the post-merge ``shutil.rmtree`` destroy source files
    that never landed at the destination. NUL is the one byte the
    filesystem cannot put in a filename, so it is the only safe record
    separator.

    Yields ``[]`` when there are no patterns (no temp file is created);
    otherwise yields ``['--from0', '--exclude-from=<file>']`` flags and
    cleans up the file on exit.
    """
    patterns = _rsync_finder_metadata_exclude_patterns(src_path)
    if not patterns:
        yield []
        return
    fd, path = tempfile.mkstemp(
        prefix="vireo-rsync-excludes-", suffix=".bin")
    try:
        with os.fdopen(fd, "wb") as f:
            for pattern in patterns:
                f.write(os.fsencode(pattern))
                f.write(b"\0")
        yield ["--from0", f"--exclude-from={path}"]
    finally:
        with contextlib.suppress(OSError):
            os.unlink(path)
