"""Cache cleanup regressions with only invented nodes and fake descriptors.

Run directly with Python to avoid the application/conftest. Product modules are
never imported: selected definitions execute with injected, in-memory I/O only.
"""

import ast
import builtins
import contextlib
import functools
import logging
import posixpath
import stat
import unittest
from collections.abc import Callable
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace as S

VIREO = Path(__file__).resolve().parents[1]
ROOT = "/invented/hub"
NAME = "models--jss367--vireo-onnx-models"
REPO = ROOT + "/" + NAME
KEY = "jss367/vireo-onnx-models/group"


class MemoryOS:
    O_RDONLY, O_RDWR, O_CREAT, O_DIRECTORY, O_NOFOLLOW = 0, 2, 64, 65536, 131072
    sep = "/"
    pardir = ".."

    def __init__(self):
        self.nodes = {1: {"kind": "dir", "children": {}, "size": 0, "mtime": 1}}
        self.root = 1
        self.next_inode = 1
        self.fds = {}
        self.next_fd = 100
        self.unlinked = []
        self.externally_held = set()
        self.held_locks = set()
        self.fail_unlink = set()
        self.before_unlink = None
        self.path = S(**{name: getattr(posixpath, name) for name in (
            "join", "dirname", "basename", "relpath", "isabs", "abspath")})
        self.path.isdir = lambda p: self._kind(p) == "dir"
        self.path.isfile = lambda p: self._kind(p) == "file"
        self.path.islink = lambda p: self._kind(p) == "link"
        self.path.lexists = lambda p: self._kind(p) is not None
        self.path.realpath = self._realpath
        self.supports_dir_fd = {self.open, self.stat, self.readlink, self.unlink, self.mkdir}
        self.supports_fd = {self.listdir}

    def _lookup(self, path, dir_fd=None):
        current = self.root if path.startswith("/") else self.fds[dir_fd]
        for part in path.split("/"):
            if not part or part == ".":
                continue
            node = self.nodes[current]
            if node["kind"] != "dir":
                raise NotADirectoryError(path)
            try:
                current = node["children"][part]
            except KeyError:
                raise FileNotFoundError(path) from None
        return current

    def _kind(self, path):
        try:
            return self.nodes[self._lookup(path)]["kind"]
        except (FileNotFoundError, NotADirectoryError):
            return None

    def _realpath(self, path):
        """Follow symlink ancestors step by step so a parked ``~/.cache``
        resolves before any ``O_NOFOLLOW`` descent begins."""
        assert path.startswith("/"), path
        remaining = [p for p in path.split("/") if p]
        current = "/"
        for _step in range(80):
            if not remaining:
                return current
            part = remaining.pop(0)
            nxt = posixpath.normpath(posixpath.join(current, part))
            if self._kind(nxt) == "link":
                target = self.nodes[self._lookup(nxt)]["target"]
                target_parts = [p for p in target.split("/") if p]
                if target.startswith("/"):
                    current = "/"
                remaining = target_parts + remaining
            else:
                current = nxt
        raise OSError("too many symlink levels")

    def add(self, path, kind="file", size=10, target=None):
        path = posixpath.normpath(path)
        parent_path, name = posixpath.split(path)
        if self._kind(parent_path) is None:
            self.add(parent_path, "dir")
        parent = self._lookup(parent_path)
        self.next_inode += 1
        inode = self.next_inode
        self.nodes[inode] = {"kind": kind, "children": {}, "size": size,
                             "mtime": 1, "target": target}
        self.nodes[parent]["children"][name] = inode
        return inode

    def mkdir(self, name, mode=0o700, dir_fd=None):
        del mode
        parent = self.fds[dir_fd]
        if name in self.nodes[parent]["children"]:
            raise FileExistsError(name)
        self.next_inode += 1
        self.nodes[self.next_inode] = {"kind": "dir", "children": {}, "size": 0, "mtime": 1}
        self.nodes[parent]["children"][name] = self.next_inode

    def open(self, path, flags, mode=0o600, dir_fd=None):
        del mode
        try:
            inode = self._lookup(path, dir_fd)
        except FileNotFoundError:
            if not flags & self.O_CREAT:
                raise
            parent = self.fds[dir_fd]
            self.next_inode += 1
            inode = self.next_inode
            self.nodes[inode] = {"kind": "file", "children": {}, "size": 0, "mtime": 1}
            self.nodes[parent]["children"][path] = inode
        kind = self.nodes[inode]["kind"]
        if kind == "link" and flags & self.O_NOFOLLOW:
            raise OSError("no-follow rejected link")
        if flags & self.O_DIRECTORY and kind != "dir":
            raise NotADirectoryError(path)
        self.next_fd += 1
        self.fds[self.next_fd] = inode
        return self.next_fd

    def _stat(self, inode):
        node = self.nodes[inode]
        mode = {"dir": stat.S_IFDIR, "file": stat.S_IFREG, "link": stat.S_IFLNK}[node["kind"]]
        return S(st_dev=1, st_ino=inode, st_mode=mode, st_size=node["size"],
                 st_mtime_ns=node["mtime"], st_mtime=node["mtime"])

    def fstat(self, fd):
        return self._stat(self.fds[fd])

    def stat(self, path, dir_fd=None, follow_symlinks=False):
        assert not follow_symlinks
        return self._stat(self._lookup(path, dir_fd))

    def lstat(self, path):
        return self.stat(path)

    def readlink(self, name, dir_fd=None):
        return self.nodes[self._lookup(name, dir_fd)]["target"]

    def listdir(self, fd):
        assert isinstance(fd, int), "cache enumeration must use a pinned descriptor"
        return list(self.nodes[self.fds[fd]]["children"])

    def unlink(self, name, dir_fd=None):
        assert dir_fd is not None, "no pathname deletion is permitted"
        if self.before_unlink:
            hook, self.before_unlink = self.before_unlink, None
            hook()
        inode = self._lookup(name, dir_fd)
        if inode in self.fail_unlink:
            raise PermissionError("invented unlink failure")
        assert self.nodes[inode]["kind"] != "dir"
        self.unlinked.append(inode)
        del self.nodes[self.fds[dir_fd]]["children"][name]

    def close(self, fd):
        self.fds.pop(fd)

    def flock(self, fd, operation):
        inode = self.fds[fd]
        if operation == 4:
            self.held_locks.discard(inode)
        elif inode in self.externally_held or inode in self.held_locks:
            raise BlockingIOError("invented busy hub lock")
        else:
            self.held_locks.add(inode)


def definitions(path, namespace):
    tree = ast.parse((VIREO / path).read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)


def service():
    fs = MemoryOS()
    fs.add(ROOT, "dir")
    fs.add(REPO + "/blobs", "dir")
    fs.add(REPO + "/snapshots", "dir")
    fake_models = S(ONNX_REPO="jss367/vireo-onnx-models", KNOWN_MODELS=[], get_models=lambda: [])
    modules = {
        "fcntl": S(flock=fs.flock, LOCK_EX=1, LOCK_NB=2, LOCK_UN=4),
        "models": fake_models,
        "services.storage_paths": S(huggingface_repositories=lambda: {NAME}, directory_bytes=lambda _p: 0),
    }

    def importer(name, *_args, **_kwargs):
        if name not in modules:
            raise AssertionError("Unexpected product/system import: " + name)
        return modules[name]

    ns = {"__builtins__": {**vars(builtins), "__import__": importer},
          "os": fs, "stat": stat, "contextlib": contextlib,
          "log": logging.getLogger("invented-cache"), "time": S(time=lambda: 100),
          "INCOMPLETE_GRACE_SECS": 86400, "UNREFERENCED_SUFFIX": "#unreferenced",
          "_ACTIVE_TREE": ContextVar("fake-tree", default=None)}
    definitions(Path("services/hf_cache_tree.py"), ns)
    definitions(Path("services/hf_cache.py"), ns)
    ns["DOWNLOAD_LOCK"] = S(acquire=lambda **_k: True, release=lambda: None)
    return fs, ns


def payload(fs, group="group", etag="blob", size=10):
    blob = REPO + "/blobs/" + etag
    snapshot = REPO + "/snapshots/rev/" + group + "/model"
    if fs._kind(blob) is None:
        fs.add(blob, size=size)
    fs.add(snapshot, "link", target="../../../blobs/" + etag)
    return blob, snapshot


def cleanup(ns, keys=None):
    if keys is None:
        return ns["remove_unused"](ROOT, "/invented/models")
    return ns["remove"](keys, ROOT, "/invented/models")


class DescriptorSafetyTests(unittest.TestCase):
    def test_normal_cleanup_pins_everything_and_keeps_metadata_and_shells(self):
        fs, ns = service()
        blob, snap = payload(fs)
        metadata = fs.add(REPO + "/refs/main")
        result = cleanup(ns, [KEY, KEY])
        self.assertEqual(result, {"removed": [KEY], "failed": [], "freed": 10})
        self.assertIsNone(fs._kind(blob))
        self.assertIsNone(fs._kind(snap))
        self.assertEqual(fs._kind(REPO), "dir")
        self.assertNotIn(metadata, fs.unlinked)
        self.assertFalse(fs.fds)

    def test_root_ancestor_and_repository_replacements_fail_closed(self):
        for path in ("/invented", ROOT, REPO):
            for kind in ("dir", "link"):
                with self.subTest(path=path, kind=kind):
                    fs, ns = service()
                    payload(fs)
                    original = ns["_collect"]
                    swapped = False

                    def collect(*args, _original=original, _fs=fs, _path=path, _kind=kind):
                        nonlocal swapped
                        result = _original(*args)
                        if not swapped:
                            _fs.add(_path, _kind, target="/invented/outside")
                            swapped = True
                        return result

                    ns["_collect"] = collect
                    with self.assertRaises(ns["UnsafeCacheCleanup"]):
                        cleanup(ns)
                    self.assertFalse(fs.unlinked)
                    self.assertFalse(fs.fds)

    def test_replacement_after_last_check_cannot_redirect_unlink(self):
        fs, ns = service()
        _blob, snapshot = payload(fs)
        original_inode = fs._lookup(snapshot)
        replacement = []

        def swap():
            fs.add(REPO, "dir")
            _path, fresh = payload(fs)
            replacement.append(fs._lookup(fresh))

        fs.before_unlink = swap
        with self.assertRaises(ns["UnsafeCacheCleanup"]):
            cleanup(ns)
        self.assertIn(original_inode, fs.unlinked)
        self.assertNotIn(replacement[0], fs.unlinked)
        self.assertFalse(fs.fds)

    def test_unsupported_platform_never_deletes(self):
        fs, ns = service()
        payload(fs)
        ns["supported"] = lambda: False
        with self.assertRaises(ns["UnsafeCacheCleanup"]):
            cleanup(ns)
        self.assertFalse(fs.unlinked)

    def test_symlinked_ancestor_resolves_before_the_no_follow_walk(self):
        # A user-owned symlink in the cache's ancestry (``~/.cache`` parked
        # on another disk, or ``/tmp -> /private/tmp`` on macOS) is resolved
        # once at init so the pinned, component-by-component ``O_NOFOLLOW``
        # walk never trips on it. If the resolved target itself is missing
        # or itself unsafe, the walk still fails closed.
        fs, ns = service()
        _blob, _snap = payload(fs)
        # Link an ancestor to the real cache root. The resolved walk pins
        # the real path and cleanup proceeds exactly as it would without
        # the link.
        fs.add("/alias", "link", target="/invented")
        result = ns["remove"]([KEY], "/alias/hub", "/invented/models")
        self.assertEqual(result["removed"], [KEY])
        self.assertFalse(fs.fds)
        # A symlink pointing to a path that does not exist still fails the
        # walk — now with FileNotFoundError on the resolved target instead
        # of ELOOP on the link itself — and never deletes anything.
        fs, ns = service()
        fs.add("/invented", "link", target="/elsewhere")
        with self.assertRaises(OSError), ns["CacheTree"](ROOT):
            self.fail("a symlink to a nonexistent target must not open")
        self.assertFalse(fs.unlinked)

    def test_missing_selected_blob_lock_target_is_held_before_deletion(self):
        fs, ns = service()
        payload(fs)
        original = fs.unlink
        lock = ROOT + "/.locks/" + NAME + "/blob.lock"

        def unlink(*args, **kwargs):
            self.assertIn(fs._lookup(lock), fs.held_locks)
            return original(*args, **kwargs)

        fs.unlink = unlink
        fs.supports_dir_fd.add(unlink)
        self.assertEqual(cleanup(ns)["freed"], 10)
        self.assertFalse(fs.held_locks)

    def test_missing_blob_still_has_its_future_lock_target_held(self):
        fs, ns = service()
        _blob, snapshot = payload(fs)
        fs.nodes[fs._lookup(REPO + "/blobs")]["children"].pop("blob")
        locked = []

        def downloading():
            lock = fs._lookup(ROOT + "/.locks/" + NAME + "/blob.lock")
            locked.append(lock in fs.held_locks)

        fs.before_unlink = downloading
        self.assertEqual(cleanup(ns)["freed"], 0)
        self.assertIsNone(fs._kind(snapshot))
        self.assertEqual(locked, [True])

    def test_new_blob_target_during_collection_is_locked_before_selection(self):
        fs, ns = service()
        payload(fs)
        collect = ns["_collect"]
        calls = 0

        def changed(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                payload(fs, "late", "new-blob", 20)
            return collect(*args)

        ns["_collect"] = changed
        self.assertEqual(cleanup(ns)["freed"], 30)
        self.assertGreaterEqual(calls, 3)
        self.assertIsNotNone(fs._kind(ROOT + "/.locks/" + NAME + "/new-blob.lock"))

    def test_download_target_churn_refuses_cleanup_before_any_unlink(self):
        fs, ns = service()
        payload(fs)
        collect = ns["_collect"]
        calls = 0

        def changed(*args):
            nonlocal calls
            calls += 1
            payload(fs, f"late-{calls}", f"blob-{calls}")
            return collect(*args)

        ns["_collect"] = changed
        with self.assertRaises(ns["DownloadInProgress"]):
            cleanup(ns)
        self.assertEqual(calls, 4)
        self.assertFalse(fs.unlinked)
        self.assertFalse(fs.held_locks)
        self.assertFalse(fs.fds)

    def test_missing_process_lock_support_cannot_silently_continue(self):
        fs, ns = service()
        payload(fs)

        def unavailable(**_kwargs):
            raise ImportError("invented missing lock dependency")

        ns["DOWNLOAD_LOCK"].acquire = unavailable
        with self.assertRaises(ns["UnsafeCacheCleanup"]):
            cleanup(ns)
        self.assertFalse(fs.unlinked)
        self.assertFalse(fs.fds)

    def test_late_unselected_partial_snapshot_and_metadata_survive(self):
        fs, ns = service()
        payload(fs)
        late = []

        def arrive():
            lock = fs.add(ROOT + "/.locks/" + NAME + "/new.lock")
            fs.externally_held.add(lock)
            late.append(fs.add(REPO + "/blobs/new.incomplete", size=33))
            late.append(fs.add(REPO + "/snapshots/new/new/model", size=44))
            late.append(fs.add(REPO + "/refs/new", size=55))

        fs.before_unlink = arrive
        result = cleanup(ns)
        self.assertEqual(result["freed"], 10)
        self.assertTrue(all(inode not in fs.unlinked for inode in late))

    def test_late_same_etag_downloader_cannot_acquire_the_precreated_lock(self):
        fs, ns = service()
        payload(fs)
        blocked = []

        def download():
            fd = fs.open(ROOT + "/.locks/" + NAME + "/blob.lock", fs.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):
                    fs.flock(fd, 3)
                blocked.append(True)
            finally:
                fs.close(fd)

        fs.before_unlink = download
        self.assertEqual(cleanup(ns)["freed"], 10)
        self.assertEqual(blocked, [True])

    def test_busy_external_hub_lock_prevents_every_unlink(self):
        fs, ns = service()
        payload(fs)
        lock = fs.add(ROOT + "/.locks/" + NAME + "/blob.lock")
        fs.externally_held.add(lock)
        with self.assertRaises(ns["DownloadInProgress"]):
            cleanup(ns)
        self.assertFalse(fs.unlinked)

    def test_purge_deduplicates_and_rechecks_install_state_under_the_guard(self):
        fs, ns = service()
        payload(fs)
        self.assertEqual(ns["_purge_keys"]([KEY, KEY], ROOT, "/invented/models"), 10)
        self.assertEqual(len(fs.unlinked), 2)
        fs, ns = service()
        payload(fs)

        def acquire(**_kwargs):
            fs.add("/invented/models/group", "dir")
            return True

        ns["DOWNLOAD_LOCK"].acquire = acquire
        self.assertEqual(ns["_purge_keys"]([KEY], ROOT, "/invented/models"), 0)
        self.assertFalse(fs.unlinked)
        self.assertFalse(fs.fds)

    def test_shared_owner_failures_and_installed_exclusion_remain(self):
        fs, ns = service()
        blob, _snap = payload(fs, "group", "shared")
        payload(fs, "other", "shared")
        fs.fail_unlink.add(fs._lookup(blob))
        result = cleanup(ns)
        self.assertEqual(result["removed"], [])
        self.assertEqual({f["key"] for f in result["failed"]}, {KEY, "jss367/vireo-onnx-models/other"})
        self.assertEqual(result["freed"], 0)
        fs, ns = service()
        payload(fs)
        fs.add("/invented/models/group", "dir")
        with self.assertRaises(ValueError):
            cleanup(ns, [KEY])
        self.assertEqual(cleanup(ns)["removed"], [])
        self.assertFalse(fs.unlinked)

    def test_a_blob_shared_with_an_installed_group_is_retained_and_not_advertised_unused(self):
        fs, ns = service()
        blob, _snapshot = payload(fs, "group", "shared")
        payload(fs, "live", "shared")
        fs.add("/invented/models/live", "dir")
        with ns["CacheTree"](ROOT) as tree:
            token = ns["_ACTIVE_TREE"].set(tree)
            try:
                inventory = ns["inventory"](ROOT, "/invented/models")
            finally:
                ns["_ACTIVE_TREE"].reset(token)
        self.assertEqual(inventory["unused_size"], 0)
        self.assertEqual(inventory["installed_size"], 10)
        result = cleanup(ns)
        self.assertEqual(result, {"removed": [KEY], "failed": [], "freed": 0})
        self.assertEqual(fs._kind(blob), "file")

    def test_snapshot_links_outside_the_pinned_cache_cannot_be_collected(self):
        fs, ns = service()
        victim = fs.add("/outside/blob", size=99)
        fs.add(REPO + "/snapshots/rev/group/model", "link", target="/outside/blob")
        with self.assertRaises(ns["UnsafeCacheCleanup"]):
            cleanup(ns)
        self.assertNotIn(victim, fs.unlinked)
        self.assertFalse(fs.unlinked)
        self.assertFalse(fs.fds)


class SharedGuardTests(unittest.TestCase):
    def test_unsupported_cleanup_route_returns_409_with_inert_collaborators(self):
        _fs, service_ns = service()
        service_ns["supported"] = lambda: False
        tree = ast.parse((VIREO / "web/storage.py").read_text())
        route = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "api_storage_hf_cache_remove")
        route.decorator_list = []
        fake_service = S(
            remove_unused=lambda: cleanup(service_ns),
            UnsafeCacheCleanup=service_ns["UnsafeCacheCleanup"],
            DownloadInProgress=service_ns["DownloadInProgress"],
        )

        def importer(name, *_args, **_kwargs):
            if name != "services":
                raise AssertionError("Unexpected route import: " + name)
            return S(hf_cache=fake_service)

        ns = {"__builtins__": {**vars(builtins), "__import__": importer},
              "request": S(get_json=lambda **_k: {"unused": True}),
              "json_error": lambda message, status=400: (message, status)}
        exec(compile(ast.Module(body=[route], type_ignores=[]), "<inert-storage-route>", "exec"), ns)
        message, status = ns[route.name]()
        self.assertEqual(status, 409)
        self.assertIn("cleanup refused", message)
        self.assertFalse(_fs.unlinked)

    def test_nested_access_and_a_second_process_lock_contend_without_io(self):
        held, events = set(), []

        class Timeout(Exception):
            pass

        class FileLock:
            def __init__(self, path, **_kwargs):
                self.path = path

            def acquire(self, timeout):
                events.append((self.path, timeout))
                if self.path in held:
                    raise Timeout()
                held.add(self.path)

            def release(self):
                held.remove(self.path)

        thread_lock = S(acquire=lambda **_k: True, release=lambda: None)
        def importer(name, *_args, **_kwargs):
            if name != "filelock":
                raise AssertionError("Unexpected guard import: " + name)
            return S(FileLock=FileLock, Timeout=Timeout)

        ns = {"__builtins__": {**vars(builtins), "__import__": importer},
              "functools": functools, "Callable": Callable,
              "threading": S(RLock=lambda: thread_lock, local=S),
              "os": S(path=S(dirname=posixpath.dirname), makedirs=lambda *_a, **_k: None)}
        definitions(Path("services/hf_cache_lock.py"), ns)
        first = ns["SharedDownloadLock"]("/invented/state/guard")
        other = ns["SharedDownloadLock"]("/invented/state/guard")
        self.assertTrue(first.acquire(blocking=False))
        self.assertTrue(first.acquire(blocking=False))
        self.assertEqual(len(events), 1)
        self.assertFalse(other.acquire(blocking=False))
        first.release()
        self.assertTrue(held)
        first.release()
        self.assertFalse(held)
        ns["DOWNLOAD_LOCK"] = other

        @ns["cache_access"]
        def work():
            self.assertTrue(held)
            raise ValueError("invented failure")

        with self.assertRaises(ValueError):
            work()
        self.assertFalse(held)

    def test_all_vireo_download_and_install_writers_have_the_shared_guard(self):
        # Each pipeline-model loader keeps its fast path OUTSIDE the guard
        # (``cache_access`` is applied to the ``_hf_download_*`` fetch helper
        # it calls on a miss, not the public ``ensure_*`` function), so a
        # pipeline whose weights are already on disk does not wait for an
        # unrelated multi-gigabyte download to release ``DOWNLOAD_LOCK``.
        targets = {
            "models.py": {"remove_model", "register_model", "_hf_download_with_retry", "download_model", "download_hf_model", "_load_upstream_timm_config", "ensure_timm_label_descriptions", "_purge_hf_cache_file"},
            "dino_embed.py": {"_hf_download_dinov2_weights"},
            "detector.py": {"_hf_download_megadetector_weights"},
            "masking.py": {"_hf_download_sam2_weights"},
            "keypoints.py": {"_hf_download_keypoint_weights"},
        }
        for path, names in targets.items():
            tree = ast.parse((VIREO / path).read_text())
            found = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)
                     and any(isinstance(d, ast.Name) and d.id == "cache_access" for d in n.decorator_list)}
            self.assertTrue(names <= found, (path, names - found))
        # Each public pipeline-weight loader checks the installed files
        # BEFORE calling into its fetch helper, so its own definition must
        # not carry ``cache_access`` any more.
        for path, name in (("dino_embed.py", "ensure_dinov2_weights"),
                           ("detector.py", "ensure_megadetector_weights"),
                           ("masking.py", "ensure_sam2_weights"),
                           ("keypoints.py", "ensure_keypoint_weights")):
            tree = ast.parse((VIREO / path).read_text())
            unguarded = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
            self.assertEqual(len(unguarded), 1, (path, name))
            self.assertFalse(any(isinstance(d, ast.Name) and d.id == "cache_access"
                                 for d in unguarded[0].decorator_list), (path, name))
        tree = ast.parse((VIREO / "web/models.py").read_text())
        guarded_workers = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "work"
                           and any(isinstance(d, ast.Name) and d.id == "cache_access" for d in n.decorator_list)]
        self.assertEqual(len(guarded_workers), 2)

    def test_plain_snapshot_copy_is_retained_without_guessing_its_lock(self):
        fs, ns = service()
        fs.add(REPO + "/snapshots/rev/group/model", size=10)
        result = cleanup(ns, [KEY])
        self.assertEqual(result["removed"], [])
        self.assertEqual(result["freed"], 0)
        self.assertEqual(len(result["failed"]), 1)
        self.assertFalse(fs.unlinked)

    def test_retained_copies_are_not_advertised_or_selected_as_reclaimable(self):
        fs, ns = service()
        fs.add(REPO + "/snapshots/rev/copies/model", size=99)
        payload(fs, "group", "shared", 10)
        payload(fs, "other", "shared", 10)
        fs.add(REPO + "/snapshots/rev/group/copy", size=30)
        with ns["CacheTree"](ROOT) as tree:
            token = ns["_ACTIVE_TREE"].set(tree)
            try:
                inventory = ns["inventory"](ROOT, "/invented/models")
            finally:
                ns["_ACTIVE_TREE"].reset(token)
        entries = {e["key"]: e for e in inventory["entries"]}
        copies = entries["jss367/vireo-onnx-models/copies"]
        self.assertEqual((copies["size"], copies["retained_size"], copies["removable"]), (0, 99, False))
        self.assertEqual(entries[KEY]["size"], 0)  # Shared real blob is counted once in the union.
        self.assertEqual(entries[KEY]["shared_size"], 10)
        self.assertEqual(entries[KEY]["retained_size"], 30)
        self.assertEqual(inventory["unused_size"], 10)
        self.assertEqual(inventory["retained_size"], 129)
        result = cleanup(ns)
        self.assertEqual(result["freed"], 10)
        self.assertEqual({f["key"] for f in result["failed"]}, {KEY})
        self.assertEqual(fs._kind(REPO + "/snapshots/rev/copies/model"), "file")
        self.assertEqual(fs._kind(REPO + "/snapshots/rev/group/copy"), "file")

    def test_automatic_purge_raises_with_partial_counts_and_owner_failures(self):
        fs, ns = service()
        blob, _snapshot = payload(fs)
        fs.fail_unlink.add(fs._lookup(blob))
        with self.assertRaises(ns["CachePurgeIncomplete"]) as caught:
            ns["_purge_keys"]([KEY], ROOT, "/invented/models")
        self.assertEqual(caught.exception.result["freed"], 0)
        self.assertEqual([f["key"] for f in caught.exception.result["failed"]], [KEY])

    def test_plain_copy_only_bulk_cleanup_does_not_claim_removable_bytes(self):
        fs, ns = service()
        fs.add(REPO + "/snapshots/rev/group/model", size=99)
        self.assertEqual(cleanup(ns), {"freed": 0, "failed": [], "removed": []})
        self.assertFalse(fs.unlinked)

    def test_both_model_removal_routes_preserve_partial_and_identity_failures(self):
        for kind in ("partial", "identity"):
            with self.subTest(kind=kind):
                _fs, service_ns = service()
                error = (service_ns["CachePurgeIncomplete"]({"freed": 7, "failed": [{"key": KEY, "error": "permission"}]})
                         if kind == "partial" else service_ns["UnsafeCacheCleanup"]("identity changed"))

                def purge(_identifier, _error=error, **_kwargs):
                    raise _error

                service_module = S(purge_model=purge, purge_subfolder=purge, purge_outcome=service_ns["purge_outcome"])
                removed_paths = []
                fake_os = S(path=S(join=posixpath.join, isdir=lambda _p: True))
                fake_shutil = S(rmtree=lambda path, _paths=removed_paths: _paths.append(path))
                modules = {"services.hf_cache": service_module, "shutil": fake_shutil, "masking": S()}

                def importer(name, *_args, _modules=modules, **_kwargs):
                    if name not in _modules:
                        raise AssertionError("Unexpected removal import: " + name)
                    return _modules[name]

                ns = {"__builtins__": {**vars(builtins), "__import__": importer}, "os": fake_os,
                      "shutil": fake_shutil, "log": logging.getLogger("invented-model-removal"),
                      "_CONFIG_LOCK": contextlib.nullcontext(), "_load_config": lambda: {"models": []},
                      "KNOWN_MODELS": [{"id": "model"}], "DEFAULT_MODELS_DIR": "/invented/models",
                      "_inside_models_dir": lambda _p: True, "jsonify": lambda result: result,
                      "json_error": lambda message, status=400: (message, status),
                      "request": S(get_json=lambda **_k: {"model_id": "sam2-tiny"}),
                      "PIPELINE_MODELS": {"sam2-tiny": {"subfolder": "group"}},
                      "pipeline_models_dir": lambda: "/invented/models"}
                tree = ast.parse((VIREO / "models.py").read_text())
                functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                             and n.name in {"remove_model", "_purge_hf_cache_for_model"}]
                for function in functions:
                    function.decorator_list = []
                exec(compile(ast.Module(body=functions, type_ignores=[]), "<inert-model-removal>", "exec"), ns)
                modules["models"] = S(remove_model=ns["remove_model"])
                tree = ast.parse((VIREO / "web/models.py").read_text())
                routes = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                          and n.name in {"api_remove_model", "api_models_pipeline_delete"}]
                for route in routes:
                    route.decorator_list = []
                exec(compile(ast.Module(body=routes, type_ignores=[]), "<inert-model-routes>", "exec"), ns)
                for result in (ns["api_remove_model"]("model"), ns["api_models_pipeline_delete"]()):
                    self.assertIs(result["ok"], False)
                    self.assertTrue(result["cache_cleanup"]["failed"])
                    self.assertEqual(result["cache_cleanup"]["freed"], 7 if kind == "partial" else 0)
                self.assertEqual(len(removed_paths), 2)  # Only invented collaborators were invoked.
                ns["_load_config"] = lambda: {"models": [{"id": "model", "weights_path": "/invented/custom"}]}
                ns["_model_is_managed"] = lambda _m: False
                ns["_inside_models_dir"] = lambda _p: False
                ns["_save_config"] = lambda _config: None
                fake_os.path.lexists = lambda _p: True
                custom_result = ns["api_remove_model"]("model")
                self.assertIs(custom_result["ok"], False)
                self.assertEqual(custom_result["kept_path"], "/invented/custom")
                self.assertEqual(len(removed_paths), 2)


if __name__ == "__main__":
    unittest.main()
