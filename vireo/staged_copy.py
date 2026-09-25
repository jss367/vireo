"""Copy a file so that a failure never leaves a partial file at the destination.

``shutil.copy2`` straight onto the final name leaves a truncated file there
when the copy dies part-way (disk full, a NAS dropping out, a crash). The next
scan catalogs it as a photo, and a retry sees the name taken and routes the
real bytes to ``name_1.ext``. ``copy_via_temp`` writes to a hidden sibling
temp file and only promotes it once the copy finished.
"""

import contextlib
import os
import shutil
import tempfile


def copy_via_temp(src, dst):
    """Copy ``src`` to ``dst`` through a hidden sibling temp file.

    Never overwrites: raises ``FileExistsError`` if ``dst`` exists by the
    time the copy is promoted. The promote is a no-overwrite ``os.link``;
    on filesystems without hard links (exFAT, some SMB/NFS shares) it
    falls back to an atomic exclusive-create claim of ``dst`` (an
    ``open`` with ``O_CREAT | O_EXCL`` — a check-then-``replace`` would
    silently overwrite a file created by a concurrent writer inside the
    window between the two calls) followed by ``os.replace`` onto that
    just-claimed empty placeholder. Any ``OSError`` from the copy
    propagates, and the temp file is always removed, so on failure the
    destination is exactly as it was.
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
            # Kernel-level race: exactly one caller wins the O_EXCL
            # claim, and the loser gets FileExistsError before touching
            # any bytes.
            try:
                claim_fd = os.open(
                    dst, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644,
                )
            except FileExistsError:
                raise
            os.close(claim_fd)
            try:
                os.replace(tmp, dst)
            except BaseException:
                # Roll back the empty placeholder so a failed promote
                # never leaves a zero-byte artifact under ``dst``. This
                # covers OSError from the replace and cancellations
                # (KeyboardInterrupt / SystemExit) that would otherwise
                # skip the cleanup.
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(dst)
                raise
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
