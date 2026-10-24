"""Fetch one Hugging Face file straight into Vireo's models folder.

A plain ``hf_hub_download`` stores the bytes in the shared hub cache
(``~/.cache/huggingface/hub``) and returns that path. Copying from there into
``~/.vireo/models`` kept two full copies of every model: ``shutil.copy2`` is a
byte copy on every platform, APFS included (it goes through ``fcopyfile``,
which does not clone).

``fetch_hub_file`` downloads with ``local_dir`` instead, so the hub cache gets
no blob. The staging directory sits beside the destination, which makes the
final ``replace_file`` a same-volume rename: no second copy even while
publishing, and a reader sees either no file or the whole file. An interrupted
transfer leaves its resume data in the staging directory, so the next attempt
continues where it stopped; a successful one removes the directory.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import tempfile

from file_replace import replace_file
from services.hf_cache_lock import DOWNLOAD_LOCK

STAGING_DIRNAME = ".hf-download"


class DownloadBatch:
    """Resume an unfinished multi-file download at its original immutable revision."""

    def __init__(self, repo_id: str, local_dir: str, *, subfolder: str | None = None) -> None:
        self.repo_id = repo_id
        self.subfolder = subfolder
        self.local_dir = local_dir
        self.path = os.path.join(local_dir, ".vireo-download-progress.json")
        try:
            with open(self.path) as stream:
                saved = json.load(stream)
        except (OSError, ValueError):
            saved = {}
        if not isinstance(saved, dict):
            saved = {}
        revision = saved.get("revision")
        if (saved.get("repo_id") == repo_id and saved.get("subfolder") == subfolder
                and isinstance(revision, str) and re.fullmatch(r"[0-9a-fA-F]{40}", revision)):
            self.revision = revision
            self.completed = saved.get("completed", {})
        else:
            from huggingface_hub import HfApi

            self.revision = HfApi().model_info(repo_id).sha
            if not isinstance(self.revision, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", self.revision):
                raise RuntimeError(f"Could not resolve an immutable revision for {repo_id}")
            self.completed = {}
        if not isinstance(self.completed, dict):
            self.completed = {}
        os.makedirs(local_dir, exist_ok=True)
        # Persist the pin before the first transfer, including failed first files.
        self._save()

    def _signature(self, filename: str) -> list[int] | None:
        try:
            st = os.stat(os.path.join(self.local_dir, filename))
        except OSError:
            return None
        return [st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino]

    def pending(self, filenames: list[str]) -> list[str]:
        return [name for name in filenames if self.completed.get(name) is None
                or self.completed[name] != self._signature(name)]

    def record(self, filename: str) -> None:
        self.completed[filename] = self._signature(filename)
        self._save()

    def _save(self) -> None:
        # Publish the small receipt atomically. No model bytes are duplicated.
        fd, temporary = tempfile.mkstemp(dir=self.local_dir)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump({"repo_id": self.repo_id, "subfolder": self.subfolder,
                           "revision": self.revision, "completed": self.completed}, stream)
            replace_file(temporary, self.path)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(temporary)

    def finish(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)


def fetch_hub_file(repo_id: str, filename: str, dest_path: str, *,
                   subfolder: str | None = None,
                   revision: str | None = None) -> str:
    """Download ``filename`` from ``repo_id`` to ``dest_path``; return ``dest_path``.

    ``filename`` may be repo-relative (``onnx/model.onnx``); ``subfolder`` and
    ``revision`` mean what they mean to ``hf_hub_download``. A file already in
    the hub cache from an older Vireo is copied out of it by the hub client, so
    no network transfer happens then either.
    """
    from huggingface_hub import hf_hub_download

    staging_root = os.path.join(os.path.dirname(dest_path) or ".", STAGING_DIRNAME)
    # Isolate resume data by source and destination. Finishing a different
    # file must not erase an interrupted transfer in the same model folder.
    identity = json.dumps([repo_id, subfolder, filename, revision, os.path.basename(dest_path)])
    staging = os.path.join(staging_root, hashlib.sha256(identity.encode()).hexdigest())
    kwargs: dict[str, str] = {"repo_id": repo_id, "filename": filename, "local_dir": staging}
    if subfolder:
        kwargs["subfolder"] = subfolder
    if revision:
        kwargs["revision"] = revision
    # Held across fetch and publish so a Storage-page cleanup or a second
    # download never sees, or removes, another transfer's staging files.
    with DOWNLOAD_LOCK:
        os.makedirs(staging, exist_ok=True)
        replace_file(hf_hub_download(**kwargs), dest_path)
        shutil.rmtree(staging, ignore_errors=True)
        # Other interrupted transfers (or retained cleanup failures)
        # still own this parent; never remove them recursively.
        with contextlib.suppress(OSError):
            os.rmdir(staging_root)
    return dest_path
