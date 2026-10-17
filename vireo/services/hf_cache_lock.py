"""Serialize all Vireo cache producers and cleanup across threads/processes."""

import functools
import os
import threading
from collections.abc import Callable


class SharedDownloadLock:
    """Reentrant thread lock plus a lazily opened process lock outside the cache."""

    def __init__(self, lock_path=None):
        self.lock_path = lock_path
        self.thread_lock = threading.RLock()
        self.local = threading.local()

    def acquire(self, blocking=True):
        if not self.thread_lock.acquire(blocking=blocking):
            return False
        depth = getattr(self.local, "depth", 0)
        if depth:
            self.local.depth = depth + 1
            return True
        try:
            from filelock import FileLock, Timeout

            path = self.lock_path or os.path.expanduser("~/.vireo/hf-cache-access.lock")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            lock = FileLock(path, is_singleton=False)
            try:
                lock.acquire(timeout=-1 if blocking else 0)
            except Timeout:
                self.thread_lock.release()
                return False
            self.local.lock = lock
            self.local.depth = 1
            return True
        except BaseException:
            self.thread_lock.release()
            raise

    def release(self):
        self.local.depth -= 1
        try:
            if not self.local.depth:
                self.local.lock.release()
                del self.local.lock
        finally:
            self.thread_lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_args):
        self.release()


DOWNLOAD_LOCK = SharedDownloadLock()


def cache_access[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(function)
    def locked(*args: P.args, **kwargs: P.kwargs) -> R:
        with DOWNLOAD_LOCK:
            return function(*args, **kwargs)
    return locked
