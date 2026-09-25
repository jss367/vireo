"""Copy a file so that a failure never leaves a partial file at the destination.

``shutil.copy2`` straight onto the final name leaves a truncated file there
when the copy dies part-way (disk full, a NAS dropping out, a crash). The next
scan catalogs it as a photo, and a retry sees the name taken and routes the
real bytes to ``name_1.ext``. ``copy_via_temp`` writes to a hidden sibling
temp file and only promotes it once the copy finished.
"""

import binascii
import contextlib
import os
import shutil
import tempfile

_COPY_CHUNK = 1 << 20  # 1 MiB per read; small enough for slow NAS transfers

# fd-based metadata ops aren't universal: ``os.fchmod`` is Unix-only, and
# ``os.utime`` accepts an fd only on platforms that list it in
# ``os.supports_fd``. On Windows the fallback promote path used to raise
# ``AttributeError``/``TypeError`` after copying the bytes, which the
# rollback in ``_promote_by_placeholder`` treated as a failure and, in
# ``move_photos``, bypassed the ``except OSError`` handler and aborted
# the batch. Path-based fallbacks guarded by an inode re-check keep the
# same "don't leak metadata onto a racer's file" guarantee.
_UTIME_SUPPORTS_FD = os.utime in getattr(os, "supports_fd", set())
_HAS_FCHMOD = hasattr(os, "fchmod")

# On Windows a file descriptor opened without O_BINARY defaults to text
# translation, which turns lone LF bytes in binary photo data into CRLF
# on write and corrupts the file silently. Add the flag when the platform
# provides it; POSIX doesn't define it and doesn't need it.
_O_BINARY = getattr(os, "O_BINARY", 0)


def copy_via_temp(src, dst):
    """Copy ``src`` to ``dst`` through a hidden sibling temp file.

    Never overwrites: raises ``FileExistsError`` if ``dst`` exists at
    promote time. The promote is a no-overwrite ``os.link``; on
    filesystems without hard links (exFAT, some SMB/NFS shares) it
    falls back to a two-step promote that avoids ``os.replace`` on the
    hot path so a concurrent writer's file at the same name is never
    overwritten:

    1. Claim ``dst`` with ``open(O_CREAT | O_EXCL | O_WRONLY)``. This is
       the same kernel-level race gate the loser of a same-name write
       loses ``FileExistsError`` on before touching any bytes.
    2. Write the temp file's bytes into that still-open placeholder fd
       — not to the name — so nothing about the transfer can overwrite a
       file another writer creates at ``dst`` after our claim. When the
       write finishes we re-check ``st_nlink`` on the fd; if a
       concurrent writer unlinked and recreated ``dst`` during the
       transfer the placeholder inode is orphaned (nlink == 0) and we
       raise ``FileExistsError`` so we don't report success over bytes
       that never reached the named destination.

    A ``check-then-``replace`` sequence in this window would silently
    overwrite that concurrent writer's file (any interposed unlink and
    same-name recreate would land on top of our ``os.replace``), and if
    the replace itself failed the cleanup could unlink the writer's
    replacement instead of our placeholder.

    Any ``OSError`` from the copy propagates, and the temp file is
    always removed. On failure the destination is exactly as it was:
    the fallback rolls back its placeholder only when the entry at
    ``dst`` is still the inode we claimed, so a concurrent writer's
    replacement (an inode we didn't claim) is left in place.
    """
    dst_dir = os.path.dirname(dst) or "."
    fd, tmp = tempfile.mkstemp(
        dir=dst_dir,
        prefix="." + os.path.basename(dst) + ".",
        suffix=".partial",
    )
    os.close(fd)
    try:
        shutil.copy2(src, tmp)
        try:
            os.link(tmp, dst)
        except FileExistsError:
            raise
        except OSError:
            _promote_by_placeholder(tmp, dst)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)


def _promote_by_placeholder(tmp, dst):
    """Promote ``tmp`` to ``dst`` through an ``O_CREAT | O_EXCL`` claim.

    See ``copy_via_temp`` for the race the ``st_nlink`` check closes.
    """
    # O_EXCL is the atomic no-overwrite gate. A concurrent writer that
    # already created ``dst`` gets us FileExistsError here, before we
    # touch any bytes; raise so ``copy_via_temp`` cleans up ``tmp``.
    # ``O_BINARY`` keeps the fd in binary mode on Windows so ``os.write``
    # cannot LF→CRLF-translate photo bytes; on POSIX it is 0.
    claim_fd = os.open(
        dst, os.O_CREAT | os.O_EXCL | os.O_WRONLY | _O_BINARY, 0o644,
    )
    fd_open = True
    try:
        claim_ino = os.fstat(claim_fd).st_ino
        try:
            with open(tmp, "rb") as fin:
                while True:
                    chunk = fin.read(_COPY_CHUNK)
                    if not chunk:
                        break
                    off = 0
                    while off < len(chunk):
                        off += os.write(claim_fd, chunk[off:])
            # If another writer unlinked our claim mid-write, our bytes
            # went to an orphan and ``dst`` either does not exist or now
            # points to that writer's file. Either way this promote did
            # NOT deliver ``tmp``'s bytes to ``dst``; refuse rather than
            # report success over the other writer's data.
            if os.fstat(claim_fd).st_nlink == 0:
                raise FileExistsError(
                    f"{dst}: placeholder was replaced during promote"
                )
            # Match ``copy2``'s metadata transfer. Prefer fd-based ops so
            # a concurrent unlink+recreate between the nlink check and
            # these calls cannot leak our metadata onto the other
            # writer's file. On Windows those ops aren't available, so
            # fall back to path-based ops guarded by an inode re-check.
            src_stat = os.stat(tmp)
            _apply_metadata(
                claim_fd, dst, claim_ino,
                (src_stat.st_atime_ns, src_stat.st_mtime_ns),
                src_stat.st_mode,
            )
        except BaseException:
            # Release the O_EXCL claim BEFORE running the rollback. On
            # Windows a handle opened without delete-sharing blocks
            # ``os.rename`` on the same path, so ``_rollback_placeholder``
            # would fail its atomic detach, swallow the ``OSError``, and
            # leave the partially written final ``dst`` behind for the
            # next scan to catalog as a corrupt photo. POSIX doesn't
            # need the early close, but doing it here keeps the two
            # platforms on one code path.
            os.close(claim_fd)
            fd_open = False
            # Roll back only when the entry at ``dst`` still points to
            # the inode we claimed. A concurrent writer's replacement
            # has a different inode and must stay.
            #
            # A ``stat`` + ``unlink`` on ``dst`` looks like it does this
            # but has a TOCTOU window: an interposed ``unlink`` +
            # ``create`` between the two calls hands us a matching inode
            # check followed by an ``unlink`` of the racer's file. Even
            # gated by ``fstat(claim_fd).st_nlink == 1`` the ``unlink``
            # is still a separate syscall on the same path — the racer
            # can slip in between. ``rename`` on POSIX is atomic — it
            # moves whatever entry is at ``dst`` right now to a unique
            # scratch path we chose, so nothing can be interposed.
            # Only then do we compare inodes: if the scratch is our
            # placeholder, we unlink that scratch (our own name — safe);
            # if it isn't, we rename it back and leave the writer's
            # file in place.
            _rollback_placeholder(dst, claim_ino)
            raise
    finally:
        if fd_open:
            os.close(claim_fd)


def _rollback_placeholder(dst, claim_ino):
    """Detach ``dst`` atomically and unlink it only if it is our placeholder.

    See the rollback block in ``_promote_by_placeholder`` for why.
    """
    dst_dir = os.path.dirname(dst) or "."
    dst_name = os.path.basename(dst)
    # A unique sibling name so ``os.rename`` can never overwrite an
    # unrelated file. urandom keeps it collision-free across processes;
    # the leading dot keeps a stray leftover invisible to gallery scans.
    scratch = os.path.join(
        dst_dir,
        "." + dst_name + "." + binascii.hexlify(os.urandom(8)).decode() +
        ".rollback",
    )
    try:
        os.rename(dst, scratch)
    except FileNotFoundError:
        return
    except OSError:
        # A rename failure (e.g. cross-device, unusual filesystem) means
        # we can't safely detach; leave ``dst`` alone rather than risk a
        # racer's file.
        return
    try:
        if os.stat(scratch).st_ino == claim_ino:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(scratch)
            return
    except FileNotFoundError:
        return
    # Not ours: the entry at ``dst`` at rollback time was a concurrent
    # writer's replacement, and ``scratch`` now holds their bytes. Try
    # to put them back. Prefer ``os.link`` on the odd chance hard links
    # work here (we entered this fallback because they didn't for the
    # promote, but detach paths can differ on some shares); otherwise
    # fall back to a ``lexists``-gated ``os.rename`` — a narrow race
    # since we own ``scratch``'s unique name. If ``dst`` is taken
    # again in that window, leave the ``scratch`` in place with its
    # ``.rollback`` marker so nothing of theirs is overwritten.
    try:
        os.link(scratch, dst)
    except FileExistsError:
        return
    except OSError:
        try:
            if not os.path.lexists(dst):
                os.rename(scratch, dst)
        except OSError:
            return
        return
    with contextlib.suppress(FileNotFoundError):
        os.unlink(scratch)


def _apply_metadata(fd, dst, claim_ino, times_ns, mode):
    """Copy atime/mtime/mode from the temp onto the claimed destination.

    Uses fd-based ops where the platform supports them (Unix). On
    platforms where they aren't available (Windows), falls back to
    path-based ops guarded by an inode re-check both before and after
    so a concurrent unlink+recreate cannot leak our metadata onto
    another writer's file.
    """
    if _UTIME_SUPPORTS_FD:
        os.utime(fd, ns=times_ns)
    else:
        _guarded_path_op(dst, claim_ino, lambda: os.utime(dst, ns=times_ns))
    if _HAS_FCHMOD:
        os.fchmod(fd, mode)
    else:
        _guarded_path_op(dst, claim_ino, lambda: os.chmod(dst, mode))


def _guarded_path_op(dst, claim_ino, op):
    """Run ``op`` only while ``dst`` still points to ``claim_ino``.

    Re-checks the inode before and after ``op`` so an interposed
    unlink+recreate raises ``FileExistsError`` instead of applying
    metadata to another writer's file. Any ``OSError`` from ``op``
    propagates so the surrounding rollback can drop our placeholder.
    """
    try:
        current_ino = os.stat(dst).st_ino
    except FileNotFoundError as exc:
        raise FileExistsError(
            f"{dst}: placeholder was replaced during promote"
        ) from exc
    if current_ino != claim_ino:
        raise FileExistsError(
            f"{dst}: placeholder was replaced during promote"
        )
    op()
    try:
        after_ino = os.stat(dst).st_ino
    except FileNotFoundError as exc:
        raise FileExistsError(
            f"{dst}: placeholder was replaced during promote"
        ) from exc
    if after_ino != claim_ino:
        raise FileExistsError(
            f"{dst}: placeholder was replaced during promote"
        )
