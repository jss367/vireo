"""Pinned, no-follow cache access. Cleanup never removes directory anchors.

No filesystem access occurs at import. Tests can inject the OS and flock
collaborators; production uses the same descriptor-relative operations.
"""

import contextlib
import os
import stat


class UnsafeCacheCleanup(OSError):
    """The platform or changing cache cannot support a safe cleanup."""


def _identity(st):
    if st.st_ino <= 0:
        raise UnsafeCacheCleanup("Filesystem does not provide stable inode identities")
    return st.st_dev, st.st_ino


def _file_identity(st):
    return (*_identity(st), st.st_mode, st.st_size, st.st_mtime_ns)


def supported():
    return (
        {os.open, os.stat, os.readlink, os.unlink, os.mkdir} <= os.supports_dir_fd
        and os.listdir in os.supports_fd
        and hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY")
    )


class CacheTree:
    """Keep the cache and every traversed directory pinned until cleanup ends."""

    sep = os.sep

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.path = self
        self.nodes = {}
        self.observed = {}
        self.lock_files = {}

    def __enter__(self):
        if not supported():
            raise UnsafeCacheCleanup("Safe cache cleanup requires no-follow directory descriptors")
        if self.root.startswith(os.sep * 2):
            raise UnsafeCacheCleanup("Ambiguous cache-root prefix")
        try:
            fd, identity = self._pin(os.sep)
            self.nodes[os.sep] = (fd, identity, None, None)
            current = os.sep
            for name in self.root.split(os.sep):
                if name:
                    current = self._open_child(current, name)
            self.verify()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_args):
        for fd, _identity_value, _parent, _name in reversed(list(self.nodes.values())):
            os.close(fd)
        self.nodes.clear()

    def _pin(self, name, parent_fd=None):
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        try:
            return fd, _identity(os.fstat(fd))
        except BaseException:
            os.close(fd)
            raise

    def _open_child(self, parent, name, create=False):
        path = os.path.join(parent, name)
        if path in self.nodes:
            return path
        parent_fd = self.nodes[parent][0]
        if create:
            with contextlib.suppress(FileExistsError):
                os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        fd, identity = self._pin(name, parent_fd)
        self.nodes[path] = (fd, identity, parent_fd, name)
        return path

    def _relative(self, path):
        path = os.path.abspath(path)
        rel = os.path.relpath(path, self.root)
        if rel == os.pardir or rel.startswith(os.pardir + os.sep) or os.path.isabs(rel):
            raise UnsafeCacheCleanup(f"Path is outside the pinned cache: {path}")
        return path, [] if rel == "." else rel.split(os.sep)

    def directory(self, path, create=False):
        _path, parts = self._relative(path)
        self.verify()
        current = self.root
        for name in parts:
            current = self._open_child(current, name, create=create)
        self.verify()
        return self.nodes[current][0]

    def verify(self):
        for fd, identity, parent_fd, name in self.nodes.values():
            if _identity(os.fstat(fd)) != identity:
                raise UnsafeCacheCleanup("Pinned directory identity changed")
            if parent_fd is not None:
                named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if not stat.S_ISDIR(named.st_mode) or _identity(named) != identity:
                    raise UnsafeCacheCleanup("Cache directory or ancestor was replaced")
        for path, (fd, identity) in self.lock_files.items():
            parent = self.nodes[os.path.dirname(path)][0]
            named = os.stat(os.path.basename(path), dir_fd=parent, follow_symlinks=False)
            if _identity(named) != identity or _identity(os.fstat(fd)) != identity:
                raise UnsafeCacheCleanup("A held download lock was replaced")

    def listdir(self, path):
        return os.listdir(self.directory(path))

    def lstat(self, path):
        path, _parts = self._relative(path)
        if path == self.root:
            return os.fstat(self.directory(path))
        fd = self.directory(os.path.dirname(path))
        st = os.stat(os.path.basename(path), dir_fd=fd, follow_symlinks=False)
        self.observed.setdefault(path, _file_identity(st))
        return st

    def _is(self, path, predicate):
        try:
            return predicate(self.lstat(path).st_mode)
        except (FileNotFoundError, NotADirectoryError):
            return False

    def isdir(self, path):
        return self._is(path, stat.S_ISDIR)

    def isfile(self, path):
        return self._is(path, stat.S_ISREG)

    def islink(self, path):
        return self._is(path, stat.S_ISLNK)

    def realpath(self, path):
        path, _parts = self._relative(path)
        for _attempt in range(40):
            if not self.islink(path):
                return path
            fd = self.directory(os.path.dirname(path))
            target = os.readlink(os.path.basename(path), dir_fd=fd)
            path, _parts = self._relative(os.path.join(os.path.dirname(path), target))
        raise UnsafeCacheCleanup("Cache link cycle")

    def walk(self, path):
        dirs, files = [], []
        for name in sorted(self.listdir(path)):
            child = os.path.join(path, name)
            if self.isdir(child):
                dirs.append(name)
            elif self.islink(child) and self.isdir(self.realpath(child)):
                continue
            else:
                files.append(name)
        yield path, dirs, files
        for name in dirs:
            yield from self.walk(os.path.join(path, name))

    def unlink(self, path):
        path, _parts = self._relative(path)
        expected = self.observed.get(path)
        if expected is None or _file_identity(self.lstat(path)) != expected:
            raise UnsafeCacheCleanup("Cache file changed since collection")
        self.verify()
        fd = self.directory(os.path.dirname(path))
        os.unlink(os.path.basename(path), dir_fd=fd)


class HubLocks:
    """Hold known blob targets, including lockfiles that did not exist yet."""

    def __init__(self, tree):
        self.tree = tree
        self.held = {}

    def __enter__(self):
        return self

    def hold(self, paths):
        import fcntl

        for path in sorted(set(paths) - self.held.keys()):
            parent = self.tree.directory(os.path.dirname(path), create=True)
            fd = os.open(os.path.basename(path), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                         0o600, dir_fd=parent)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise UnsafeCacheCleanup("Download lock is not a regular file")
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BaseException:
                os.close(fd)
                raise
            self.held[path] = fd
            self.tree.lock_files[path] = fd, _identity(os.fstat(fd))
        self.tree.verify()

    def __exit__(self, *_args):
        import fcntl

        for path, fd in reversed(list(self.held.items())):
            self.tree.lock_files.pop(path, None)
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        self.held.clear()
