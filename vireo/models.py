"""Model and taxonomy registry for Vireo.

All models are ONNX format, downloaded from the jss367/vireo-onnx-models
HuggingFace repository into ~/.vireo/models/{model-id}/.
"""

import json
import logging
import os
import shutil

log = logging.getLogger(__name__)

DEFAULT_MODELS_DIR = os.path.expanduser("~/.vireo/models")
CONFIG_PATH = os.path.expanduser("~/.vireo/models.json")

# HuggingFace repo containing all ONNX models
ONNX_REPO = "jss367/vireo-onnx-models"

# Known models that can be downloaded.
# Each entry specifies which ONNX files are needed and the subdirectory
# within the HF repo where they live.
KNOWN_MODELS = [
    {
        "id": "bioclip-vit-b-16",
        "name": "BioCLIP",
        "model_type": "bioclip",
        "model_str": "ViT-B-16",
        "hf_subdir": "bioclip-vit-b-16",
        "files": [
            "image_encoder.onnx",
            "text_encoder.onnx",
            "tokenizer.json",
            "config.json",
        ],
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
        "hf_subdir": "bioclip-2",
        "files": [
            "image_encoder.onnx",
            "text_encoder.onnx",
            "tokenizer.json",
            "config.json",
            "tol_embeddings.npy",
            "tol_classes.json",
        ],
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
        "hf_subdir": "bioclip-2.5-vith14",
        "files": [
            "image_encoder.onnx",
            "text_encoder.onnx",
            "tokenizer.json",
            "config.json",
            "tol_embeddings.npy",
            "tol_classes.json",
        ],
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
        "hf_subdir": "timm-eva02-large-inat21",
        "files": [
            "model.onnx",
            "class_names.json",
            "label_descriptions.json",
            "config.json",
        ],
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


def _check_onnx_downloaded(model_dir, files):
    """Check if all required ONNX files exist in a model directory.

    Args:
        model_dir: path to the model directory
        files: list of filenames that must be present

    Returns:
        True if the directory exists and contains at least the ONNX files
    """
    if not os.path.isdir(model_dir):
        return False
    # At minimum, check that the .onnx files exist
    onnx_files = [f for f in files if f.endswith(".onnx")]
    return all(
        os.path.isfile(os.path.join(model_dir, f))
        for f in onnx_files
    )


def get_models():
    """Return list of all models (known + custom) with download status."""
    config = _load_config()
    registered = {m["id"]: m for m in config.get("models", [])}

    result = []
    for km in KNOWN_MODELS:
        model_dir = os.path.join(DEFAULT_MODELS_DIR, km["id"])
        downloaded = _check_onnx_downloaded(model_dir, km.get("files", []))

        entry = {
            **km,
            "downloaded": downloaded,
            "weights_path": model_dir if downloaded else None,
            "model_type": km.get("model_type", "bioclip"),
        }

        # Also check registered path if different
        if not downloaded and km["id"] in registered:
            reg = registered[km["id"]]
            reg_path = reg.get("weights_path", "")
            if reg_path and os.path.isdir(reg_path):
                # Check if ONNX files exist at the registered path
                onnx_files = [f for f in km.get("files", []) if f.endswith(".onnx")]
                if all(os.path.isfile(os.path.join(reg_path, f)) for f in onnx_files):
                    entry["downloaded"] = True
                    entry["weights_path"] = reg_path

        result.append(entry)

    # Add custom models
    for mid, m in registered.items():
        if not any(km["id"] == mid for km in KNOWN_MODELS):
            path = m.get("weights_path", "")
            # Custom models: check for any .onnx file in the directory
            downloaded = False
            if path and os.path.isdir(path):
                downloaded = any(
                    f.endswith(".onnx")
                    for f in os.listdir(path)
                )
            elif path and os.path.isfile(path) and path.endswith(".onnx"):
                downloaded = True
            result.append(
                {
                    "id": mid,
                    "name": m.get("name", mid),
                    "model_str": m.get("model_str", "ViT-B-16"),
                    "source": "custom",
                    "description": m.get("description", "Custom model"),
                    "weights_path": path,
                    "downloaded": downloaded,
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

    Deletes local ONNX model files and removes it from models.json.
    Returns True if found.
    """
    config = _load_config()
    models = config.get("models", [])

    found = None
    for m in models:
        if m["id"] == model_id:
            found = m
            break

    if not found:
        # Check if it's a known model with a default path
        known = {km["id"]: km for km in KNOWN_MODELS}
        if model_id in known:
            path = os.path.join(DEFAULT_MODELS_DIR, model_id)
            if os.path.isdir(path):
                shutil.rmtree(path)
                return True
        return False

    # Delete local model directory
    weights_path = found.get("weights_path", "")
    if weights_path and os.path.exists(weights_path):
        if os.path.isdir(weights_path):
            shutil.rmtree(weights_path)
        else:
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


def _hf_download_with_retry(repo_id, filename, local_dir,
                            subfolder=None, progress_callback=None):
    """Download a file from HuggingFace with retry on connection failures.

    Uses hf_hub_download for reliable resume. Keeps retrying as long as
    progress is being made. Stops after 3 consecutive failures with
    no progress.

    Args:
        repo_id: HuggingFace repo ID
        filename: filename within the repo (or subfolder)
        local_dir: destination directory for the file
        subfolder: optional subfolder within the repo
        progress_callback: optional callable(message)
    """
    import time as _time

    from huggingface_hub import hf_hub_download

    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "300")

    attempt = 0
    stalled_count = 0
    max_stalled = 3

    while True:
        attempt += 1
        try:
            if progress_callback:
                if attempt == 1:
                    progress_callback(f"Downloading {filename}...")
                else:
                    progress_callback(f"Resuming download (attempt {attempt})...")

            log.info(
                "Downloading %s/%s%s (attempt %d)",
                repo_id,
                f"{subfolder}/" if subfolder else "",
                filename,
                attempt,
            )

            kwargs = {
                "repo_id": repo_id,
                "filename": filename,
            }
            if subfolder:
                kwargs["subfolder"] = subfolder

            cached_path = hf_hub_download(**kwargs)

            # Copy from cache to our models directory
            os.makedirs(local_dir, exist_ok=True)
            dest_path = os.path.join(local_dir, filename)
            if cached_path != dest_path:
                shutil.copy2(cached_path, dest_path)

            log.info("Download complete: %s", dest_path)
            return dest_path

        except Exception as e:
            stalled_count += 1
            log.warning(
                "Download attempt %d failed (%d/%d stalled): %s",
                attempt, stalled_count, max_stalled, e,
            )

            if stalled_count >= max_stalled:
                raise RuntimeError(
                    f"Download of {filename} failed after {attempt} attempts. "
                    f"Try again — the download will resume from where it left off."
                ) from e

            wait = 3
            if progress_callback:
                progress_callback(f"Connection error, retrying in {wait}s...")
            _time.sleep(wait)


def download_model(model_id, progress_callback=None):
    """Download a known model from jss367/vireo-onnx-models.

    Downloads all required ONNX files for the model into
    ~/.vireo/models/{model-id}/.

    Returns the model directory path.
    """
    known = {m["id"]: m for m in KNOWN_MODELS}
    if model_id not in known:
        raise ValueError(f"Unknown model: {model_id}")

    km = known[model_id]
    model_dir = os.path.join(DEFAULT_MODELS_DIR, model_id)
    os.makedirs(model_dir, exist_ok=True)

    try:
        from huggingface_hub import hf_hub_download  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "huggingface_hub not installed. Run: pip install huggingface_hub"
        )

    files = km.get("files", [])
    hf_subdir = km.get("hf_subdir", model_id)
    total_files = len(files)

    for fi, filename in enumerate(files):
        if progress_callback:
            size_hint = ""
            if filename.endswith(".onnx"):
                size_hint = f' ({km.get("size_mb", "?")} MB)'
            progress_callback(
                f"Downloading {fi + 1}/{total_files}: {filename}{size_hint}",
                current=fi,
                total=total_files,
            )

        _hf_download_with_retry(
            ONNX_REPO,
            filename,
            model_dir,
            subfolder=hf_subdir,
            progress_callback=progress_callback,
        )

    if progress_callback:
        progress_callback(
            f'{km["name"]} download complete!',
            current=total_files,
            total=total_files,
        )

    log.info("Model downloaded to: %s", model_dir)
    register_model(
        model_id, km["name"], km.get("model_str", model_id),
        model_dir, km["description"],
    )
    return model_dir


def download_hf_model(repo_id, progress_callback=None):
    """Download a model from any HuggingFace repo.

    Looks for ONNX model files in the repo. Downloads them into
    ~/.vireo/models/{slug}/.

    Args:
        repo_id: HuggingFace repo ID (e.g., 'imageomics/bioclip-2.5-vith14')
        progress_callback: optional callable(message)

    Returns:
        dict with model_id, weights_path, name
    """
    try:
        from huggingface_hub import list_repo_files
    except ImportError:
        raise RuntimeError(
            "huggingface_hub not installed. Run: pip install huggingface_hub"
        )

    os.makedirs(DEFAULT_MODELS_DIR, exist_ok=True)

    # Generate a model ID from the repo
    model_id = "hf-" + repo_id.replace("/", "-").lower()
    slug = repo_id.split("/")[-1]
    local_dir = os.path.join(DEFAULT_MODELS_DIR, slug)

    # Find ONNX files in the repo
    if progress_callback:
        progress_callback(f"Scanning {repo_id} for ONNX model files...")

    log.info("Listing files in HuggingFace repo: %s", repo_id)
    try:
        files = list_repo_files(repo_id)
    except Exception as e:
        raise RuntimeError(f"Could not access HuggingFace repo '{repo_id}': {e}")

    # Look for ONNX files
    onnx_files = [f for f in files if f.endswith(".onnx")]

    if not onnx_files:
        raise RuntimeError(
            f"No ONNX model files found in {repo_id}. "
            f"Files: {', '.join(files[:10])}"
        )

    log.info("Found ONNX files: %s in %s", onnx_files, repo_id)

    # Download all ONNX files and common config files
    config_files = [f for f in files if f.endswith((".json", ".npy"))]
    to_download = onnx_files + config_files

    for fi, filename in enumerate(to_download):
        if progress_callback:
            progress_callback(
                f"Downloading {fi + 1}/{len(to_download)}: {filename}",
            )
        _hf_download_with_retry(
            repo_id, filename, local_dir,
            progress_callback=progress_callback,
        )

    # Determine model_str — use hf-hub: prefix for compatibility
    model_str = f"hf-hub:{repo_id}"

    # Register the model
    name = slug.replace("-", " ").title()
    register_model(
        model_id, name, model_str, local_dir,
        f"Downloaded from HuggingFace: {repo_id}",
    )

    log.info("Model registered: %s (%s)", name, local_dir)
    return {"model_id": model_id, "weights_path": local_dir, "name": name}


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
