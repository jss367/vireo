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
import shutil

from file_replace import replace_file
from services.hf_cache_lock import DOWNLOAD_LOCK

STAGING_DIRNAME = ".hf-download"


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
