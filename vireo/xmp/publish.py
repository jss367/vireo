"""Atomic sidecar publication.

``_write_tree_atomic`` serializes a tree into a temporary file beside the
sidecar, carries the original's owner, mode, ACLs, extended attributes and
flags over to it, and publishes it with ``replace_file``. A failure at any
step leaves the previous sidecar intact.
"""

import errno
import logging
import os
import stat
import sys
import uuid
from pathlib import Path

from file_replace import replace_file

# Every module of the package logs as ``xmp``, the logger the single-module
# version used.
log = logging.getLogger("xmp")

# Extended attributes no process is allowed to write. macOS stamps
# ``com.apple.provenance`` on files touched by a launched binary and only the
# kernel may set it, so copying it always fails. Losing it on the replacement
# costs nothing; refusing to publish the sidecar because of it costs the user
# every pending change.
_UNCOPYABLE_XATTRS = frozenset({"com.apple.provenance"})

# Errors that mean "this destination will never accept this attribute" rather
# than "this write went wrong". Retrying them on the next sync cannot help.
_XATTR_SKIP_ERRNOS = frozenset(
    {errno.EACCES, errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP}
)

# Attributes that carry access control rather than annotation. Linux stores
# POSIX ACLs and security labels as extended attributes, so failing to copy
# one would publish a sidecar that is readable by more people than the
# original. Never skip these, whatever the errno.
#
# The Linux ``system.`` namespace is reserved for kernel-managed attributes
# with access-control semantics -- ``system.posix_acl_access`` /
# ``system.posix_acl_default`` on native filesystems, ``system.nfs4_acl`` on
# NFSv4 exports, ``system.richacl`` on RichACL mounts. Missing one and
# skipping it on EACCES/EPERM/ENOTSUP would publish a sidecar with weaker
# access than the original, so the prefix is the whole namespace rather than
# an enumerated allow-list.
_CRITICAL_XATTR_PREFIXES = ("system.", "security.")

# Attribute names already reported, so a 2,000-photo sync logs each cause once.
_reported_xattr_skips = set()

_darwin_xattr_api = None


def _log_skipped_xattr(name, source, error):
    """Record an extended attribute the replacement sidecar cannot carry."""
    if name in _UNCOPYABLE_XATTRS:
        log.debug("Skipping kernel-owned xattr %s on %s: %s", name, source, error)
        return
    if name not in _reported_xattr_skips:
        _reported_xattr_skips.add(name)
        log.warning(
            "Could not preserve extended attribute %s (e.g. on %s): %s",
            name, source, error,
        )


def _darwin_xattr():
    """Bind the macOS xattr syscalls once; Python's os module omits them."""
    global _darwin_xattr_api
    if _darwin_xattr_api is None:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        libc.listxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                   ctypes.c_size_t, ctypes.c_int]
        libc.listxattr.restype = ctypes.c_ssize_t
        libc.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                  ctypes.c_void_p, ctypes.c_size_t,
                                  ctypes.c_uint32, ctypes.c_int]
        libc.getxattr.restype = ctypes.c_ssize_t
        libc.setxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                  ctypes.c_void_p, ctypes.c_size_t,
                                  ctypes.c_uint32, ctypes.c_int]
        libc.setxattr.restype = ctypes.c_int
        libc.removexattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p,
                                     ctypes.c_int]
        libc.removexattr.restype = ctypes.c_int
        _darwin_xattr_api = (ctypes, libc)
    return _darwin_xattr_api


def _darwin_xattr_error(path):
    """Raise the errno the last xattr syscall on ``path`` set."""
    ctypes, _ = _darwin_xattr()
    code = ctypes.get_errno()
    raise OSError(code, os.strerror(code), str(path))


def _darwin_list_xattrs(path):
    ctypes, libc = _darwin_xattr()
    encoded = os.fsencode(path)
    size = libc.listxattr(encoded, None, 0, 0)
    if size < 0:
        _darwin_xattr_error(path)
    if size == 0:
        return []
    names = ctypes.create_string_buffer(size)
    size = libc.listxattr(encoded, names, size, 0)
    if size < 0:
        _darwin_xattr_error(path)
    return [n.decode() for n in names.raw[:size].split(b"\0") if n]


def _darwin_get_xattr(path, name):
    ctypes, libc = _darwin_xattr()
    encoded, key = os.fsencode(path), name.encode()
    size = libc.getxattr(encoded, key, None, 0, 0, 0)
    if size < 0:
        _darwin_xattr_error(path)
    value = ctypes.create_string_buffer(max(size, 1))
    size = libc.getxattr(encoded, key, value, size, 0, 0)
    if size < 0:
        _darwin_xattr_error(path)
    return value.raw[:size]


def _darwin_set_xattr(path, name, value):
    _, libc = _darwin_xattr()
    if libc.setxattr(os.fsencode(path), name.encode(), value, len(value), 0, 0) != 0:
        _darwin_xattr_error(path)


def _darwin_remove_xattr(path, name):
    _, libc = _darwin_xattr()
    if libc.removexattr(os.fsencode(path), name.encode(), 0) != 0:
        _darwin_xattr_error(path)


def _copy_xattrs(source, destination, list_xattrs, get_xattr, set_xattr,
                 remove_xattr):
    """Mirror ``source``'s extended attributes onto its replacement.

    Copy attribute by attribute rather than in one all-or-nothing call: a
    single attribute the destination refuses must not sink the whole sidecar
    write. macOS stamps ``com.apple.provenance`` on written files and no
    process may set it, so on an SMB share
    ``copyfile(COPYFILE_ACL | COPYFILE_XATTR)`` failed with EACCES for every
    sidecar that had already been written once -- the sync created each
    sidecar and could then never update it again.
    """
    def skippable(name, error):
        return (
            error.errno in _XATTR_SKIP_ERRNOS
            and not name.startswith(_CRITICAL_XATTR_PREFIXES)
        )

    attributes = {}
    for name in list_xattrs(source):
        try:
            attributes[name] = get_xattr(source, name)
        except OSError as error:
            if not skippable(name, error):
                raise
            _log_skipped_xattr(name, source, error)
    for name in set(list_xattrs(destination)) - attributes.keys():
        try:
            remove_xattr(destination, name)
        except OSError as error:
            if not skippable(name, error):
                raise
    for name, value in attributes.items():
        try:
            set_xattr(destination, name, value)
        except OSError as error:
            if not skippable(name, error):
                raise
            _log_skipped_xattr(name, source, error)


def _preserve_sidecar_access(source, destination, source_stat):
    """Copy access metadata, failing before replacement if preservation fails."""
    if os.name == "posix":
        destination_stat = destination.stat()
        if (destination_stat.st_uid, destination_stat.st_gid) != (
            source_stat.st_uid, source_stat.st_gid,
        ):
            os.chown(destination, source_stat.st_uid, source_stat.st_gid)

    os.chmod(destination, stat.S_IMODE(source_stat.st_mode))
    if sys.platform == "darwin":
        # macOS ACLs are not exposed through Python's xattr API. copyfile(3)
        # copies them natively; omit COPYFILE_DATA and COPYFILE_STAT so the
        # newly serialized content and its modification time stay intact.
        # Access control is not best-effort, so an ACL copy failure still
        # aborts before replacement.
        ctypes, _ = _darwin_xattr()

        # Copy extended attributes BEFORE applying the source ACL: a macOS
        # ACL can allow file-data writes but deny writeextattr, and once
        # the source ACL is in place, setxattr on the destination would
        # return EACCES and _copy_xattrs would then skip the (non-critical)
        # source metadata, publishing a sidecar without it.
        _copy_xattrs(source, destination, _darwin_list_xattrs,
                     _darwin_get_xattr, _darwin_set_xattr, _darwin_remove_xattr)

        copyfile = ctypes.CDLL(None, use_errno=True).copyfile
        copyfile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p,
                             ctypes.c_uint32]
        copyfile.restype = ctypes.c_int
        copyfile_acl = 1 << 0
        if copyfile(os.fsencode(source), os.fsencode(destination), None,
                    copyfile_acl) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), str(source))
    elif hasattr(os, "listxattr"):
        # Linux exposes POSIX ACLs as system.posix_acl_access. Unlike
        # shutil.copystat, do not silently ignore permission-copy failures --
        # only the attributes the destination structurally cannot hold.
        _copy_xattrs(source, destination, os.listxattr, os.getxattr,
                     os.setxattr, os.removexattr)

    if hasattr(source_stat, "st_flags"):
        os.chflags(destination, source_stat.st_flags)


def _write_tree_atomic(tree, xmp_path):
    """Publish a complete sidecar without truncating the previous version.

    Keep the temporary file on the same filesystem for atomic replacement,
    preserve existing permissions, and follow sidecar symlinks just as the
    former direct write did. Failed writes leave the original intact.
    """
    path = Path(xmp_path).resolve()
    try:
        source_stat = path.stat()
    except FileNotFoundError:
        source_stat = None
    if source_stat is not None:
        # Replacing a directory entry bypasses the file's write protection on
        # POSIX. Probe write access without truncation to retain the previous
        # writer's permission checks, including ACLs and read-only flags.
        os.close(os.open(path, os.O_WRONLY))
    temp_path = None
    try:
        candidate = path.parent / f".vireo-xmp-{uuid.uuid4().hex}.tmp"
        # 0666 lets the process umask set permissions for new sidecars,
        # matching a direct write (mkstemp would silently restrict to 0600).
        fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        temp_path = candidate
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            tree.write(stream, xml_declaration=True, encoding="unicode")
            stream.flush()
            os.fsync(stream.fileno())
        if source_stat is not None:
            _preserve_sidecar_access(path, temp_path, source_stat)
        replace_file(temp_path, path)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
