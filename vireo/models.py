"""Model and taxonomy registry for Vireo."""

import json
import logging
import os
import shutil

log = logging.getLogger(__name__)

DEFAULT_MODELS_DIR = os.path.expanduser("~/.vireo/models")
CONFIG_PATH = os.path.expanduser("~/.vireo/models.json")

# Known models that can be downloaded
KNOWN_MODELS = [
    {
        "id": "bioclip-vit-b-16",
        "name": "BioCLIP",
        "model_type": "bioclip",
        "model_str": "ViT-B-16",
        "source": "hf-hub:imageomics/bioclip",
        "description": "2024 model trained on TreeOfLife-10M. Smallest and fastest BioCLIP variant.",
        "size_mb": 400,
        "architecture": "ViT-B/16",
        "parameters": "150M",
    },
    {
        "id": "bioclip-2",
        "name": "BioCLIP-2",
        "model_type": "bioclip",
        "model_str": "hf-hub:imageomics/bioclip-2",
        "source": "hf-hub:imageomics/bioclip-2",
        "description": "2025 model with ViT-L/14 backbone, 428M parameters. Higher accuracy than v1, slower on CPU.",
        "size_mb": 1500,
        "architecture": "ViT-L/14",
        "parameters": "428M",
    },
    {
        "id": "bioclip-2.5-vith14",
        "name": "BioCLIP-2.5",
        "model_type": "bioclip",
        "model_str": "hf-hub:imageomics/bioclip-2.5-vith14",
        "source": "hf-hub:imageomics/bioclip-2.5-vith14",
        "description": "2025 model with ViT-H/14 backbone, 986M parameters. Largest BioCLIP variant.",
        "size_mb": 3900,
        "architecture": "ViT-H/14",
        "parameters": "986M",
    },
    {
        "id": "timm-inat21-eva02-l",
        "name": "iNat21 (EVA-02 Large)",
        "model_type": "timm",
        "model_str": "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21",
        "source": "timm",
        "description": "EVA-02 Large fine-tuned on iNaturalist 2021. 10K species, 92% top-1. No label files needed.",
        "size_mb": 1200,
        "architecture": "EVA-02 Large",
        "parameters": "304M",
    },
]


def _load_config():
    """Load the model config, creating defaults if missing."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {"models": [], "active_model": None}


def _save_config(config):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


def get_models():
    """Return list of all models (known + custom) with download status."""
    config = _load_config()
    registered = {m["id"]: m for m in config.get("models", [])}

    result = []
    for km in KNOWN_MODELS:
        entry = {**km, "downloaded": False, "weights_path": None,
                 "model_type": km.get("model_type", "bioclip")}
        if km["id"] in registered:
            reg = registered[km["id"]]
            path = reg.get("weights_path", "")
            entry["weights_path"] = path
            entry["downloaded"] = bool(path and os.path.exists(path))
        # Also check legacy path
        if not entry["downloaded"] and km["id"] == "bioclip-vit-b-16":
            legacy = "/tmp/bioclip_model/open_clip_pytorch_model.bin"
            if os.path.exists(legacy):
                entry["weights_path"] = legacy
                entry["downloaded"] = True
        result.append(entry)

    # Add custom models
    for mid, m in registered.items():
        if not any(km["id"] == mid for km in KNOWN_MODELS):
            path = m.get("weights_path", "")
            result.append(
                {
                    "id": mid,
                    "name": m.get("name", mid),
                    "model_str": m.get("model_str", "ViT-B-16"),
                    "source": "custom",
                    "description": m.get("description", "Custom model"),
                    "weights_path": path,
                    "downloaded": bool(path and os.path.exists(path)),
                }
            )

    return result


def get_active_model():
    """Return the currently active model config, or the first downloaded one."""
    config = _load_config()
    models = get_models()
    active_id = config.get("active_model")

    if active_id:
        for m in models:
            if m["id"] == active_id and m["downloaded"]:
                return m

    # Fall back to first downloaded model
    for m in models:
        if m["downloaded"]:
            return m

    return None


def set_active_model(model_id):
    """Set the active model."""
    config = _load_config()
    config["active_model"] = model_id
    _save_config(config)


def remove_model(model_id):
    """Remove a model's weights from disk and unregister it.

    Deletes local weights (both our managed copy and the HF cache entry),
    and removes it from models.json. Returns True if found.
    """
    config = _load_config()
    models = config.get("models", [])

    found = None
    for m in models:
        if m["id"] == model_id:
            found = m
            break

    if not found:
        # Check if it's a known model with a legacy path
        known = {km["id"]: km for km in KNOWN_MODELS}
        if model_id in known:
            # Known model, not registered — check default paths
            path = os.path.join(DEFAULT_MODELS_DIR, model_id)
            if os.path.isdir(path):
                shutil.rmtree(path)
                return True
        return False

    # Delete local weights
    weights_path = found.get("weights_path", "")
    if weights_path and os.path.exists(weights_path):
        if os.path.isdir(weights_path):
            shutil.rmtree(weights_path)
        else:
            # Delete the file and its parent dir if it's inside our models dir
            os.unlink(weights_path)
            parent = os.path.dirname(weights_path)
            if parent.startswith(DEFAULT_MODELS_DIR) and os.path.isdir(parent):
                remaining = os.listdir(parent)
                if not remaining:
                    os.rmdir(parent)

    # Remove from config
    config["models"] = [m for m in models if m["id"] != model_id]
    if config.get("active_model") == model_id:
        config["active_model"] = None
    _save_config(config)

    log.info("Removed model %s (weights: %s)", model_id, weights_path)
    return True


def register_model(model_id, name, model_str, weights_path, description=""):
    """Register a model (custom or after download)."""
    config = _load_config()
    models = config.get("models", [])

    # Update if exists, add if not
    found = False
    for m in models:
        if m["id"] == model_id:
            m["name"] = name
            m["model_str"] = model_str
            m["weights_path"] = weights_path
            m["description"] = description
            found = True
            break
    if not found:
        models.append(
            {
                "id": model_id,
                "name": name,
                "model_str": model_str,
                "weights_path": weights_path,
                "description": description,
            }
        )

    config["models"] = models
    _save_config(config)


def _get_cache_file_size(repo_id, filename):
    """Check how much of a file has been downloaded in the HF cache."""
    cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
    # HF cache uses a specific directory structure
    repo_dir = os.path.join(cache_dir, "models--" + repo_id.replace("/", "--"))
    if not os.path.isdir(repo_dir):
        return 0
    # Look for incomplete download files
    blobs_dir = os.path.join(repo_dir, "blobs")
    if not os.path.isdir(blobs_dir):
        return 0
    # Find the largest file (likely the partial download)
    max_size = 0
    for f in os.listdir(blobs_dir):
        fp = os.path.join(blobs_dir, f)
        if os.path.isfile(fp):
            max_size = max(max_size, os.path.getsize(fp))
    return max_size


def _hf_download_with_retry(repo_id, filename, local_dir, progress_callback=None):
    """Download from HuggingFace with retry on connection failures.

    Uses the HF cache for reliable resume. Keeps retrying as long as
    progress is being made. Stops after 3 consecutive failures with
    no progress.
    """
    import shutil
    import time as _time

    from huggingface_hub import hf_hub_download

    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "300")

    attempt = 0
    stalled_count = 0
    last_progress = 0
    max_stalled = 3  # give up after 3 consecutive failures with no progress

    while True:
        attempt += 1
        try:
            if progress_callback:
                if attempt == 1:
                    progress_callback(f"Downloading {filename} from {repo_id}...")
                else:
                    progress_callback(f"Resuming download (attempt {attempt})...")

            log.info("Downloading %s from %s (attempt %d)", filename, repo_id, attempt)

            cached_path = hf_hub_download(
                repo_id=repo_id,
                filename=filename,
            )

            # Copy from cache to our models directory
            os.makedirs(local_dir, exist_ok=True)
            dest_path = os.path.join(local_dir, filename)
            if cached_path != dest_path:
                shutil.copy2(cached_path, dest_path)

            log.info("Download complete: %s", dest_path)
            return dest_path

        except Exception as e:
            err_str = str(e)
            is_metadata_error = (
                "cannot find the requested files in the local cache" in err_str
            )

            # Check if we made progress since last attempt
            current_size = _get_cache_file_size(repo_id, filename)
            if current_size > last_progress:
                log.info(
                    "Download progress: %d MB downloaded so far",
                    current_size // (1024 * 1024),
                )
                stalled_count = 0
                last_progress = current_size
            elif is_metadata_error:
                # Metadata check failures are transient — don't count as stalls
                log.info(
                    "Attempt %d: HF metadata check failed (transient), retrying...",
                    attempt,
                )
            else:
                stalled_count += 1
                log.warning(
                    "Download attempt %d: no progress (%d/%d stalled): %s",
                    attempt,
                    stalled_count,
                    max_stalled,
                    e,
                )

            if stalled_count >= max_stalled:
                raise RuntimeError(
                    f"Download stalled after {attempt} attempts with no new data. "
                    f"Downloaded {last_progress // (1024 * 1024)} MB so far. "
                    f"Try again — the download will resume from where it left off."
                ) from e

            wait = 3
            if progress_callback:
                mb = current_size // (1024 * 1024)
                progress_callback(f"Connection lost at {mb} MB, retrying in {wait}s...")
            _time.sleep(wait)


class _DownloadStalled(TimeoutError):
    """Raised when a download stalls.  Carries how many bytes tqdm reported."""

    def __init__(self, message, bytes_downloaded=0):
        super().__init__(message)
        self.bytes_downloaded = bytes_downloaded


def _download_with_byte_progress(repo_id, filename, file_size,
                                  progress_callback=None,
                                  stall_timeout=300):
    """Download a file into the HF cache with byte-level progress.

    Disables XET (which stalls on large files and doesn't support resume)
    so HuggingFace falls back to plain HTTP with .incomplete-file resume.
    Intercepts progress via ``tqdm_class`` and runs the download in a
    daemon thread for stall detection.

    Args:
        repo_id: HuggingFace repo (e.g. "timm/eva02_large...")
        filename: File within the repo
        file_size: Expected file size in bytes (from repo metadata)
        progress_callback: callable(bytes_downloaded, file_size, rate_bytes_per_sec)
        stall_timeout: Seconds with no progress before raising _DownloadStalled.
    """
    import threading
    import time as _time

    # Disable XET: it downloads via a content-addressed chunk-cache that
    # doesn't support resume across retries and doesn't reliably report
    # progress.  Plain HTTP writes to .incomplete files and resumes.
    # Patch the name where _download_to_tmp_and_move actually calls it.
    import huggingface_hub.file_download as _hf_fd
    from huggingface_hub import hf_hub_download
    from tqdm.auto import tqdm as base_tqdm
    _hf_fd.is_xet_available = lambda: False
    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "300")

    lock = threading.Lock()
    state = {
        "bytes": 0,
        "last_update": _time.monotonic(),
        "start": _time.monotonic(),
    }

    class _ProgressTqdm(base_tqdm):
        """Intercepts tqdm updates from http_get."""

        _last_cb = 0.0

        def __init__(self, *args, **kwargs):
            # HF's _get_progress_bar_context passes name= which
            # tqdm.std.tqdm rejects; strip it before calling super.
            kwargs.pop("name", None)
            super().__init__(*args, **kwargs)
            _ProgressTqdm._last_cb = 0.0
            # If resuming, initial is already set by http_get
            if self.initial:
                with lock:
                    state["bytes"] = int(self.initial)

        def update(self, n=1):
            super().update(n)
            now = _time.monotonic()
            with lock:
                state["bytes"] = int(self.n)
                state["last_update"] = now
            if progress_callback and (now - _ProgressTqdm._last_cb) >= 0.5:
                _ProgressTqdm._last_cb = now
                elapsed = now - state["start"]
                rate = self.n / elapsed if elapsed > 0 else 0
                progress_callback(min(int(self.n), file_size), file_size, rate)

    result = [None]
    error = [None]
    done = threading.Event()

    def do_download():
        try:
            result[0] = hf_hub_download(
                repo_id=repo_id, filename=filename,
                tqdm_class=_ProgressTqdm,
            )
        except Exception as e:
            error[0] = e
        finally:
            done.set()

    dl_thread = threading.Thread(target=do_download, daemon=True)
    dl_thread.start()

    while not done.is_set():
        now = _time.monotonic()
        with lock:
            stall_duration = now - state["last_update"]
            current_bytes = state["bytes"]
        if stall_timeout and stall_duration > stall_timeout:
            mb_done = current_bytes // (1024 * 1024)
            mb_total = file_size // (1024 * 1024)
            raise _DownloadStalled(
                f"Download stalled: no new data for {stall_timeout}s "
                f"({mb_done}/{mb_total} MB)",
                bytes_downloaded=current_bytes,
            )
        done.wait(2.0)

    if error[0]:
        if not hasattr(error[0], "bytes_downloaded"):
            with lock:
                error[0].bytes_downloaded = state["bytes"]
        raise error[0]
    return result[0]


def download_model(model_id, progress_callback=None):
    """Download a known model. Returns the weights path."""
    known = {m["id"]: m for m in KNOWN_MODELS}
    if model_id not in known:
        raise ValueError(f"Unknown model: {model_id}")

    km = known[model_id]
    os.makedirs(DEFAULT_MODELS_DIR, exist_ok=True)

    try:
        from huggingface_hub import hf_hub_download  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "huggingface_hub not installed. Run: pip install huggingface_hub"
        )

    source = km.get("source", "")

    if source.startswith("hf-hub:"):
        # For hf-hub models, open_clip manages its own cache. We download
        # each file individually so we can report progress.
        repo_id = source.replace("hf-hub:", "")
        log.info("Pre-warming HF cache for %s (%s)", km["name"], repo_id)

        from huggingface_hub import hf_hub_download, list_repo_files

        files = list_repo_files(repo_id)
        total_files = len(files)
        cache_dir = None

        for fi, filename in enumerate(files):
            if progress_callback:
                size_hint = ""
                if filename.endswith((".safetensors", ".bin")):
                    size_hint = f' ({km.get("size_mb", "?")} MB)'
                progress_callback(
                    f"Downloading {fi + 1}/{total_files}: {filename}{size_hint}",
                    current=fi,
                    total=total_files,
                )

            log.info(
                "Downloading %s/%s (%d/%d)", repo_id, filename, fi + 1, total_files
            )
            path = hf_hub_download(repo_id, filename)

            # The first file's parent directory is the cache dir
            if cache_dir is None:
                cache_dir = os.path.dirname(path)

        if progress_callback:
            progress_callback(
                f'{km["name"]} download complete!',
                current=total_files,
                total=total_files,
            )
        log.info("Model cached at: %s", cache_dir)

        register_model(model_id, km["name"], source, cache_dir, km["description"])
        return cache_dir

    elif model_id == "bioclip-vit-b-16":
        # BioCLIP v1 uses a direct weights file, not hf-hub scheme
        path = _hf_download_with_retry(
            "imageomics/bioclip",
            "open_clip_pytorch_model.bin",
            os.path.join(DEFAULT_MODELS_DIR, "bioclip"),
            progress_callback=progress_callback,
        )
        register_model(model_id, km["name"], km["model_str"], path, km["description"])
        return path

    elif km.get("model_type") == "timm":
        # Pre-download files from HF with byte-level progress, then let
        # timm load from cache.
        try:
            import timm
        except ImportError:
            raise RuntimeError("timm not installed. Run: pip install timm")

        model_name = km["model_str"]
        hf_repo = model_name.replace("hf-hub:", "")

        from huggingface_hub import HfApi

        log.info("Fetching file list for %s", hf_repo)
        if progress_callback:
            progress_callback(
                f"Fetching file list for {km['name']}...",
                current=0,
                total=0,
            )

        # Get file metadata (names + sizes)
        api = HfApi()
        repo_info = api.model_info(hf_repo, files_metadata=True)
        files_meta = {
            s.rfilename: s.size or 0
            for s in (repo_info.siblings or [])
        }
        total_bytes = sum(files_meta.values())
        downloaded_bytes = [0]
        total_files = len(files_meta)

        import time as _time

        for fi, (filename, file_size) in enumerate(files_meta.items()):
            def file_progress(current_bytes, _total, rate,
                              _fn=filename, _fs=file_size):
                overall = downloaded_bytes[0] + current_bytes
                size_mb = current_bytes // (1024 * 1024)
                total_mb = _fs // (1024 * 1024) if _fs else 0
                if progress_callback:
                    progress_callback(
                        f"{_fn} ({size_mb}/{total_mb} MB)",
                        current=overall,
                        total=total_bytes,
                        rate=rate,
                    )

            log.info(
                "Downloading %s/%s (%d/%d, %d MB)",
                hf_repo, filename, fi + 1, total_files,
                file_size // (1024 * 1024),
            )

            if progress_callback:
                progress_callback(
                    f"Downloading {fi + 1}/{total_files}: {filename}",
                    current=downloaded_bytes[0],
                    total=total_bytes,
                    rate=0,
                )

            # Retry loop — stalls and connection errors are common for
            # multi-GB files on HF's XET storage backend.
            max_stalled = 5
            stalled_count = 0

            for attempt in range(1, max_stalled + 1):
                try:
                    _download_with_byte_progress(
                        hf_repo, filename, file_size,
                        progress_callback=file_progress,
                    )
                    break  # success
                except Exception as e:
                    peak = getattr(e, "bytes_downloaded", 0)

                    if peak > 0:
                        stalled_count = 0
                        log.info(
                            "Download interrupted at %d MB, "
                            "retrying (attempt %d): %s",
                            peak // (1024 * 1024), attempt, e,
                        )
                    else:
                        stalled_count += 1
                        log.warning(
                            "Download stalled with no progress "
                            "(attempt %d, %d/%d stalls): %s",
                            attempt, stalled_count, max_stalled, e,
                        )

                    if stalled_count >= max_stalled:
                        raise RuntimeError(
                            f"Download of {filename} stalled after "
                            f"{attempt} attempts with no progress. "
                            f"Try again — the download will resume "
                            f"from where it left off."
                        ) from e

                    wait = min(3 * attempt, 15)
                    if progress_callback:
                        progress_callback(
                            f"Retrying {filename} in {wait}s "
                            f"(attempt {attempt + 1})...",
                            current=downloaded_bytes[0],
                            total=total_bytes,
                            rate=0,
                        )
                    _time.sleep(wait)

            downloaded_bytes[0] += file_size

        if progress_callback:
            progress_callback(
                f"Loading {km['name']} into timm...",
                current=total_bytes,
                total=total_bytes,
            )

        # All files are cached — timm.create_model finds them instantly
        log.info("All files cached, loading timm model: %s", model_name)
        timm.create_model(model_name, pretrained=True)

        cache_dir_root = os.path.expanduser("~/.cache/huggingface/hub")
        repo_dir = os.path.join(
            cache_dir_root, "models--" + hf_repo.replace("/", "--")
        )
        if not os.path.isdir(repo_dir):
            repo_dir = model_name

        if progress_callback:
            progress_callback(
                f"{km['name']} download complete!",
                current=total_bytes,
                total=total_bytes,
            )

        register_model(model_id, km["name"], model_name, repo_dir, km["description"])
        log.info("timm model cached at: %s", repo_dir)
        return repo_dir

    raise ValueError(f"No download handler for {model_id}")


def download_hf_model(repo_id, progress_callback=None):
    """Download a model from any HuggingFace repo.

    Args:
        repo_id: HuggingFace repo ID (e.g., 'imageomics/bioclip-2.5-vith14')
        progress_callback: optional callable(message)

    Returns:
        dict with model_id, weights_path, name
    """
    try:
        from huggingface_hub import hf_hub_download, list_repo_files
    except ImportError:
        raise RuntimeError(
            "huggingface_hub not installed. Run: pip install huggingface_hub"
        )

    os.makedirs(DEFAULT_MODELS_DIR, exist_ok=True)

    # Generate a model ID from the repo
    model_id = "hf-" + repo_id.replace("/", "-").lower()
    slug = repo_id.split("/")[-1]
    local_dir = os.path.join(DEFAULT_MODELS_DIR, slug)

    # Find the weights file in the repo
    if progress_callback:
        progress_callback(f"Scanning {repo_id} for model files...")

    log.info("Listing files in HuggingFace repo: %s", repo_id)
    try:
        files = list_repo_files(repo_id)
    except Exception as e:
        raise RuntimeError(f"Could not access HuggingFace repo '{repo_id}': {e}")

    # Look for common weight file names
    weight_candidates = [
        "open_clip_pytorch_model.bin",
        "pytorch_model.bin",
        "model.safetensors",
        "open_clip_model.safetensors",
    ]
    weight_file = None
    for candidate in weight_candidates:
        if candidate in files:
            weight_file = candidate
            break

    if not weight_file:
        # Try any .bin or .safetensors file
        for f in files:
            if f.endswith(".bin") or f.endswith(".safetensors"):
                weight_file = f
                break

    if not weight_file:
        raise RuntimeError(
            f"No model weights found in {repo_id}. " f"Files: {', '.join(files[:10])}"
        )

    log.info("Found weights file: %s in %s", weight_file, repo_id)

    # Download the weights
    path = _hf_download_with_retry(
        repo_id,
        weight_file,
        local_dir,
        progress_callback=progress_callback,
    )

    # Determine model_str — use hf-hub: prefix for open_clip compatibility
    model_str = f"hf-hub:{repo_id}"

    # Register the model
    name = slug.replace("-", " ").title()
    register_model(
        model_id, name, model_str, path, f"Downloaded from HuggingFace: {repo_id}"
    )

    log.info("Model registered: %s (%s)", name, path)
    return {"model_id": model_id, "weights_path": path, "name": name}


def get_taxonomy_info():
    """Return taxonomy status info."""
    taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
    if not os.path.exists(taxonomy_path):
        return {
            "available": False,
            "path": taxonomy_path,
            "taxa_count": 0,
            "last_updated": None,
        }

    try:
        with open(taxonomy_path) as f:
            # Only read the metadata, not the full taxa dicts
            # Read first few bytes to get last_updated without parsing the whole file
            raw = f.read(200)
        import re

        updated_match = re.search(r'"last_updated"\s*:\s*"([^"]+)"', raw)
        last_updated = updated_match.group(1) if updated_match else None

        # Get file size as a proxy for taxa count
        size = os.path.getsize(taxonomy_path)
        # Rough estimate: ~150 bytes per taxon entry
        taxa_estimate = size // 150

        return {
            "available": True,
            "path": taxonomy_path,
            "taxa_count": taxa_estimate,
            "last_updated": last_updated,
            "file_size": size,
        }
    except Exception:
        return {
            "available": True,
            "path": taxonomy_path,
            "taxa_count": 0,
            "last_updated": None,
        }
