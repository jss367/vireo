"""Explicit Vireo-owned locations, including split catalog/cache layouts.

Never inventory the parent of a custom cache directory: that volume may
contain unrelated user data. Recorded Work Locally roots are exact copies,
not the user's entire selected destination volume.
"""

from __future__ import annotations

import os
import re
import sys
from typing import TYPE_CHECKING, Any

from app_paths import default_path, media_root

if TYPE_CHECKING:
    from collections.abc import Mapping

    from db import Database


def huggingface_cache_dir() -> str:
    return os.path.abspath(os.path.expanduser(
        os.environ.get("HF_HUB_CACHE") or os.environ.get("HUGGINGFACE_HUB_CACHE")
        or os.path.join(os.environ.get("HF_HOME") or os.path.join(
            os.environ.get("XDG_CACHE_HOME", "~/.cache"), "huggingface"), "hub")
    ))


def huggingface_repositories(*, managed_only: bool = False) -> set[str]:
    """Exact repositories for reporting, or managed-provenance cleanup scope.

    The default preserves non-destructive reporting of all registrations.
    Cleanup includes registrations only when Vireo owns their lifecycle.
    Known and exact legacy repositories remain explicitly included.
    """
    from models import KNOWN_MODELS, ONNX_REPO, get_models

    if managed_only:
        from models import get_managed_model_registrations

        registered = get_managed_model_registrations()
    else:
        registered = get_models()
    repos = {ONNX_REPO}
    for model in [*KNOWN_MODELS, *registered]:
        for field in ("model_str", "source"):
            value = model.get(field, "")
            if value.startswith("hf-hub:"):
                repos.add(value.removeprefix("hf-hub:"))
    # Original BioCLIP checkpoints are still accepted by the runtime.
    repos.update({"imageomics/bioclip", "imageomics/bioclip-2", "imageomics/bioclip-2.5"})
    return {"models--" + repo.replace("/", "--") for repo in repos}


# Any: ``config`` is the Flask app config, whose values are heterogeneous.
def auxiliary_paths(db_path: str, thumb_dir: str, config: Mapping[str, Any], db: Database) -> list[str]:
    import classifier
    import config as cfg
    import labels
    import models
    import taxonomy

    catalog = os.path.dirname(os.path.abspath(db_path))
    generated = media_root(os.path.abspath(thumb_dir))
    paths = [os.path.join(generated, name) for name in (
        "originals", "external-edits", "external-dng", "inat-uploads",
        "inat-exports", "edit-masks", "staging", "local-folders", "local-workspaces",
    )]
    paths += [
        thumb_dir, os.path.join(generated, "previews"), os.path.join(generated, "working"),
        os.path.join(generated, "offline"), classifier.CACHE_DIR, models.DEFAULT_MODELS_DIR,
        config["CARD_CLEANUP_DIR"], config["COMPUTATION_CACHE_DIR"],
        os.path.join(catalog, "taxonomy.json"), os.path.join(catalog, "taxonomy"),
        os.path.join(catalog, "masks"), os.path.join(catalog, "vireo.log"),
        *[os.path.join(catalog, f"vireo.log.{i}") for i in range(1, 4)],
        *[f"{db_path}{suffix}" for suffix in ("-wal", "-shm", "-journal")],
        cfg.CONFIG_PATH, cfg.CONFIG_PATH + ".corrupt", models.CONFIG_PATH,
        labels.LABELS_DIR, taxonomy.TAXONOMY_JSON_PATH,
    ]
    paths += [default_path(name) for name in (
        "active_labels", "ssh", "tools", "runtime", "runtime_lock",
        "gui_clients", "gui_clients_lock", "log",
    )]
    paths += [default_path("runtime") + ".tmp"]
    paths += [default_path("log") + f".{i}" for i in range(1, 4)]
    paths += db.storage.local_copy_paths()
    if sys.platform == "darwin":
        paths.append(default_path("desktop_logs_macos"))
    # Do not combine directory keys in a dict: global config and catalog
    # commonly share a parent and both exact filename patterns must apply.
    patterns = [
        (catalog, re.compile(re.escape(os.path.basename(db_path)) +
                            r"\.pre-v\d+\.bak\Z|(?:pipeline|culling)_results_ws[1-9][0-9]*\.json\Z")),
        (os.path.dirname(models.CONFIG_PATH), re.compile(
            re.escape(os.path.basename(models.CONFIG_PATH)) + r"\.corrupt(?:\.[0-9a-f]+)?\Z")),
    ]
    for directory, pattern in patterns:
        try:
            with os.scandir(directory) as entries:
                paths.extend(entry.path for entry in entries
                             if pattern.fullmatch(entry.name) and entry.is_file())
        except OSError:
            continue
    if generated == catalog:
        # Preserve the dedicated conventional root's catch-all breakdown.
        try:
            with os.scandir(catalog) as entries:
                paths.extend(entry.path for entry in entries)
        except OSError:
            pass
    return paths


def directory_bytes(path: str) -> int:
    """Logical file bytes without following nested symlinks or duplicate blobs."""
    total = 0
    for root, dirs, files in os.walk(path):
        dirs[:] = [name for name in dirs if not os.path.islink(os.path.join(root, name))]
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:
                continue
    return total


def location_id(path: str) -> str:
    import hashlib
    return "managed-" + hashlib.sha256(os.path.abspath(path).encode()).hexdigest()[:20]
