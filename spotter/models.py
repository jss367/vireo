"""Model and taxonomy registry for Spotter."""

import json
import logging
import os
import shutil

log = logging.getLogger(__name__)

DEFAULT_MODELS_DIR = os.path.expanduser("~/.spotter/models")
CONFIG_PATH = os.path.expanduser("~/.spotter/models.json")

# Known models that can be downloaded
KNOWN_MODELS = [
    {
        'id': 'bioclip-vit-b-16',
        'name': 'BioCLIP',
        'model_str': 'ViT-B-16',
        'source': 'hf-hub:imageomics/bioclip',
        'description': 'BioCLIP v1 — trained on TreeOfLife-10M. Good general-purpose species classifier.',
        'size_mb': 400,
        'architecture': 'ViT-B/16',
        'parameters': '150M',
    },
    {
        'id': 'bioclip-2',
        'name': 'BioCLIP-2',
        'model_str': 'hf-hub:imageomics/bioclip-2',
        'source': 'hf-hub:imageomics/bioclip-2',
        'description': 'BioCLIP v2 — improved accuracy, larger model. Best quality but slower on CPU.',
        'size_mb': 1500,
        'architecture': 'ViT-L/14',
        'parameters': '428M',
    },
    {
        'id': 'bioclip-2.5-vith14',
        'name': 'BioCLIP-2.5',
        'model_str': 'hf-hub:imageomics/bioclip-2.5-vith14',
        'source': 'hf-hub:imageomics/bioclip-2.5-vith14',
        'description': 'BioCLIP v2.5 — latest model with ViT-H/14 backbone. Best accuracy, largest model.',
        'size_mb': 3900,
        'architecture': 'ViT-H/14',
        'parameters': '986M',
    },
]


def _load_config():
    """Load the model config, creating defaults if missing."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {'models': [], 'active_model': None}


def _save_config(config):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=2)


def get_models():
    """Return list of all models (known + custom) with download status."""
    config = _load_config()
    registered = {m['id']: m for m in config.get('models', [])}

    result = []
    for km in KNOWN_MODELS:
        entry = {**km, 'downloaded': False, 'weights_path': None}
        if km['id'] in registered:
            reg = registered[km['id']]
            path = reg.get('weights_path', '')
            entry['weights_path'] = path
            entry['downloaded'] = bool(path and os.path.exists(path))
        # Also check legacy path
        if not entry['downloaded'] and km['id'] == 'bioclip-vit-b-16':
            legacy = '/tmp/bioclip_model/open_clip_pytorch_model.bin'
            if os.path.exists(legacy):
                entry['weights_path'] = legacy
                entry['downloaded'] = True
        result.append(entry)

    # Add custom models
    for mid, m in registered.items():
        if not any(km['id'] == mid for km in KNOWN_MODELS):
            path = m.get('weights_path', '')
            result.append({
                'id': mid,
                'name': m.get('name', mid),
                'model_str': m.get('model_str', 'ViT-B-16'),
                'source': 'custom',
                'description': m.get('description', 'Custom model'),
                'weights_path': path,
                'downloaded': bool(path and os.path.exists(path)),
            })

    return result


def get_active_model():
    """Return the currently active model config, or the first downloaded one."""
    config = _load_config()
    models = get_models()
    active_id = config.get('active_model')

    if active_id:
        for m in models:
            if m['id'] == active_id and m['downloaded']:
                return m

    # Fall back to first downloaded model
    for m in models:
        if m['downloaded']:
            return m

    return None


def set_active_model(model_id):
    """Set the active model."""
    config = _load_config()
    config['active_model'] = model_id
    _save_config(config)


def remove_model(model_id):
    """Remove a model's weights from disk and unregister it.

    Deletes local weights (both our managed copy and the HF cache entry),
    and removes it from models.json. Returns True if found.
    """
    config = _load_config()
    models = config.get('models', [])

    found = None
    for m in models:
        if m['id'] == model_id:
            found = m
            break

    if not found:
        # Check if it's a known model with a legacy path
        known = {km['id']: km for km in KNOWN_MODELS}
        if model_id in known:
            # Known model, not registered — check default paths
            path = os.path.join(DEFAULT_MODELS_DIR, model_id)
            if os.path.isdir(path):
                shutil.rmtree(path)
                return True
        return False

    # Delete local weights
    weights_path = found.get('weights_path', '')
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
    config['models'] = [m for m in models if m['id'] != model_id]
    if config.get('active_model') == model_id:
        config['active_model'] = None
    _save_config(config)

    log.info("Removed model %s (weights: %s)", model_id, weights_path)
    return True


def register_model(model_id, name, model_str, weights_path, description=''):
    """Register a model (custom or after download)."""
    config = _load_config()
    models = config.get('models', [])

    # Update if exists, add if not
    found = False
    for m in models:
        if m['id'] == model_id:
            m['name'] = name
            m['model_str'] = model_str
            m['weights_path'] = weights_path
            m['description'] = description
            found = True
            break
    if not found:
        models.append({
            'id': model_id,
            'name': name,
            'model_str': model_str,
            'weights_path': weights_path,
            'description': description,
        })

    config['models'] = models
    _save_config(config)


def _get_cache_file_size(repo_id, filename):
    """Check how much of a file has been downloaded in the HF cache."""
    cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
    # HF cache uses a specific directory structure
    repo_dir = os.path.join(cache_dir, 'models--' + repo_id.replace('/', '--'))
    if not os.path.isdir(repo_dir):
        return 0
    # Look for incomplete download files
    blobs_dir = os.path.join(repo_dir, 'blobs')
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
    from huggingface_hub import hf_hub_download
    import shutil
    import time as _time

    os.environ.setdefault('HF_HUB_DOWNLOAD_TIMEOUT', '300')

    attempt = 0
    stalled_count = 0
    last_progress = 0
    max_stalled = 3  # give up after 3 consecutive failures with no progress

    while True:
        attempt += 1
        try:
            if progress_callback:
                if attempt == 1:
                    progress_callback(f'Downloading {filename} from {repo_id}...')
                else:
                    progress_callback(f'Resuming download (attempt {attempt})...')

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
            is_metadata_error = 'cannot find the requested files in the local cache' in err_str

            # Check if we made progress since last attempt
            current_size = _get_cache_file_size(repo_id, filename)
            if current_size > last_progress:
                log.info("Download progress: %d MB downloaded so far",
                         current_size // (1024 * 1024))
                stalled_count = 0
                last_progress = current_size
            elif is_metadata_error:
                # Metadata check failures are transient — don't count as stalls
                log.info("Attempt %d: HF metadata check failed (transient), retrying...", attempt)
            else:
                stalled_count += 1
                log.warning("Download attempt %d: no progress (%d/%d stalled): %s",
                            attempt, stalled_count, max_stalled, e)

            if stalled_count >= max_stalled:
                raise RuntimeError(
                    f"Download stalled after {attempt} attempts with no new data. "
                    f"Downloaded {last_progress // (1024 * 1024)} MB so far. "
                    f"Try again — the download will resume from where it left off."
                ) from e

            wait = 3
            if progress_callback:
                mb = current_size // (1024 * 1024)
                progress_callback(f'Connection lost at {mb} MB, retrying in {wait}s...')
            _time.sleep(wait)


def download_model(model_id, progress_callback=None):
    """Download a known model. Returns the weights path."""
    known = {m['id']: m for m in KNOWN_MODELS}
    if model_id not in known:
        raise ValueError(f"Unknown model: {model_id}")

    km = known[model_id]
    os.makedirs(DEFAULT_MODELS_DIR, exist_ok=True)

    try:
        from huggingface_hub import hf_hub_download  # noqa: F401
    except ImportError:
        raise RuntimeError("huggingface_hub not installed. Run: pip install huggingface_hub")

    source = km.get('source', '')

    if source.startswith('hf-hub:'):
        # For hf-hub models, open_clip manages its own cache. We download
        # each file individually so we can report progress.
        repo_id = source.replace('hf-hub:', '')
        log.info("Pre-warming HF cache for %s (%s)", km['name'], repo_id)

        from huggingface_hub import hf_hub_download, list_repo_files

        files = list_repo_files(repo_id)
        total_files = len(files)
        cache_dir = None

        for fi, filename in enumerate(files):
            if progress_callback:
                size_hint = ''
                if filename.endswith(('.safetensors', '.bin')):
                    size_hint = f' ({km.get("size_mb", "?")} MB)'
                progress_callback(
                    f'Downloading {fi + 1}/{total_files}: {filename}{size_hint}',
                    current=fi,
                    total=total_files,
                )

            log.info("Downloading %s/%s (%d/%d)", repo_id, filename, fi + 1, total_files)
            path = hf_hub_download(repo_id, filename)

            # The first file's parent directory is the cache dir
            if cache_dir is None:
                cache_dir = os.path.dirname(path)

        if progress_callback:
            progress_callback(f'{km["name"]} download complete!', current=total_files, total=total_files)
        log.info("Model cached at: %s", cache_dir)

        register_model(model_id, km['name'], source, cache_dir, km['description'])
        return cache_dir

    elif model_id == 'bioclip-vit-b-16':
        # BioCLIP v1 uses a direct weights file, not hf-hub scheme
        path = _hf_download_with_retry(
            'imageomics/bioclip', 'open_clip_pytorch_model.bin',
            os.path.join(DEFAULT_MODELS_DIR, 'bioclip'),
            progress_callback=progress_callback,
        )
        register_model(model_id, km['name'], km['model_str'], path, km['description'])
        return path

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
        raise RuntimeError("huggingface_hub not installed. Run: pip install huggingface_hub")

    os.makedirs(DEFAULT_MODELS_DIR, exist_ok=True)

    # Generate a model ID from the repo
    model_id = 'hf-' + repo_id.replace('/', '-').lower()
    slug = repo_id.split('/')[-1]
    local_dir = os.path.join(DEFAULT_MODELS_DIR, slug)

    # Find the weights file in the repo
    if progress_callback:
        progress_callback(f'Scanning {repo_id} for model files...')

    log.info("Listing files in HuggingFace repo: %s", repo_id)
    try:
        files = list_repo_files(repo_id)
    except Exception as e:
        raise RuntimeError(f"Could not access HuggingFace repo '{repo_id}': {e}")

    # Look for common weight file names
    weight_candidates = [
        'open_clip_pytorch_model.bin',
        'pytorch_model.bin',
        'model.safetensors',
        'open_clip_model.safetensors',
    ]
    weight_file = None
    for candidate in weight_candidates:
        if candidate in files:
            weight_file = candidate
            break

    if not weight_file:
        # Try any .bin or .safetensors file
        for f in files:
            if f.endswith('.bin') or f.endswith('.safetensors'):
                weight_file = f
                break

    if not weight_file:
        raise RuntimeError(
            f"No model weights found in {repo_id}. "
            f"Files: {', '.join(files[:10])}"
        )

    log.info("Found weights file: %s in %s", weight_file, repo_id)

    # Download the weights
    path = _hf_download_with_retry(
        repo_id, weight_file, local_dir,
        progress_callback=progress_callback,
    )

    # Determine model_str — use hf-hub: prefix for open_clip compatibility
    model_str = f'hf-hub:{repo_id}'

    # Register the model
    name = slug.replace('-', ' ').title()
    register_model(model_id, name, model_str, path,
                   f'Downloaded from HuggingFace: {repo_id}')

    log.info("Model registered: %s (%s)", name, path)
    return {'model_id': model_id, 'weights_path': path, 'name': name}


def get_taxonomy_info():
    """Return taxonomy status info."""
    taxonomy_path = os.path.join(os.path.dirname(__file__), 'taxonomy.json')
    if not os.path.exists(taxonomy_path):
        return {
            'available': False,
            'path': taxonomy_path,
            'taxa_count': 0,
            'last_updated': None,
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
            'available': True,
            'path': taxonomy_path,
            'taxa_count': taxa_estimate,
            'last_updated': last_updated,
            'file_size': size,
        }
    except Exception:
        return {
            'available': True,
            'path': taxonomy_path,
            'taxa_count': 0,
            'last_updated': None,
        }
