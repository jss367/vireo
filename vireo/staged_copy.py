"""Copy a file so that a failure never leaves a partial file at the destination.

``shutil.copy2`` straight onto the final name leaves a truncated file there
when the copy dies part-way (disk full, a NAS dropping out, a crash). The next
scan catalogs it as a photo, and a retry sees the name taken and routes the
real bytes to ``name_1.ext``. ``copy_via_temp`` writes to a hidden sibling
temp file and only promotes it once the copy finished.
"""

import contextlib
import errno
import os
import shutil
import tempfile


def copy_via_temp(src, dst):
    """Copy ``src`` to ``dst`` through a hidden sibling temp file.

    Never overwrites: raises ``FileExistsError`` if ``dst`` exists by the
    time the copy is promoted. The promote is a no-overwrite ``os.link``;
    on filesystems without hard links (exFAT, some SMB/NFS shares) it falls
    back to an existence check followed by ``os.replace``. Any ``OSError``
    from the copy propagates, and the temp file is always removed, so on
    failure the destination is exactly as it was.
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
            if os.path.lexists(dst):
                raise FileExistsError(
                    errno.EEXIST, os.strerror(errno.EEXIST), dst,
                ) from None
            os.replace(tmp, dst)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
