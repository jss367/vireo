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
    },
    {
        'id': 'bioclip-2',
        'name': 'BioCLIP-2',
        'model_str': 'hf-hub:imageomics/bioclip-2',
        'source': 'hf-hub:imageomics/bioclip-2',
        'description': 'BioCLIP v2 — improved accuracy, requires HuggingFace download.',
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


def _hf_download_with_retry(repo_id, filename, local_dir, progress_callback=None, max_retries=5):
    """Download from HuggingFace with retry on connection failures.

    hf_hub_download automatically resumes partial downloads from cache,
    so retries pick up where they left off rather than starting over.
    """
    from huggingface_hub import hf_hub_download
    import time as _time

    # Increase HF timeout for large files
    os.environ.setdefault('HF_HUB_DOWNLOAD_TIMEOUT', '120')

    for attempt in range(max_retries):
        try:
            if progress_callback:
                if attempt == 0:
                    progress_callback(f'Downloading {filename} from {repo_id}...')
                else:
                    progress_callback(f'Resuming download ({attempt + 1}/{max_retries})...')

            log.info("Downloading %s from %s (attempt %d/%d)",
                     filename, repo_id, attempt + 1, max_retries)

            path = hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                local_dir=local_dir,
            )

            log.info("Download complete: %s", path)
            return path

        except Exception as e:
            log.warning("Download attempt %d failed: %s", attempt + 1, e)
            if attempt == max_retries - 1:
                raise RuntimeError(
                    f"Download failed after {max_retries} attempts: {e}\n"
                    f"The download will resume from where it left off if you try again."
                ) from e
            if progress_callback:
                progress_callback(f'Connection lost, resuming in {5 * (attempt + 1)}s...')
            _time.sleep(5 * (attempt + 1))  # increasing backoff: 5s, 10s, 15s, 20s


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

    if model_id == 'bioclip-vit-b-16':
        path = _hf_download_with_retry(
            'imageomics/bioclip', 'open_clip_pytorch_model.bin',
            os.path.join(DEFAULT_MODELS_DIR, 'bioclip'),
            progress_callback=progress_callback,
        )
        register_model(model_id, km['name'], km['model_str'], path, km['description'])
        return path

    elif model_id == 'bioclip-2':
        path = _hf_download_with_retry(
            'imageomics/bioclip-2', 'open_clip_pytorch_model.bin',
            os.path.join(DEFAULT_MODELS_DIR, 'bioclip-2'),
            progress_callback=progress_callback,
        )
        register_model(model_id, km['name'], 'hf-hub:imageomics/bioclip-2',
                       path, km['description'])
        return path

    raise ValueError(f"No download handler for {model_id}")


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
