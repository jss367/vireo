"""Model and taxonomy registry for Vireo.

All models are ONNX format, downloaded from the jss367/vireo-onnx-models
HuggingFace repository into ~/.vireo/models/{model-id}/.
"""

import contextlib
import json
import logging
import os
import shutil

import model_verify

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
        "source": "hf-hub:imageomics/bioclip",
        "hf_subdir": "bioclip-vit-b-16",
        "files": [
            "image_encoder.onnx",
            "image_encoder.onnx.data",
            "text_encoder.onnx",
            "text_encoder.onnx.data",
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
        "source": "hf-hub:imageomics/bioclip-2",
        "hf_subdir": "bioclip-2",
        "files": [
            "image_encoder.onnx",
            "image_encoder.onnx.data",
            "text_encoder.onnx",
            "text_encoder.onnx.data",
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
        "source": "hf-hub:imageomics/bioclip-2.5-vith14",
        "hf_subdir": "bioclip-2.5-vith14",
        "files": [
            "image_encoder.onnx",
            "image_encoder.onnx.data",
            "text_encoder.onnx",
            "text_encoder.onnx.data",
            "tokenizer.json",
            "config.json",
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
        "source": "timm",
        "hf_subdir": "timm-eva02-large-inat21",
        "files": [
            "model.onnx",
            "model.onnx.data",
            "class_names.json",
            "config.json",
        ],
        "optional_files": [
            "label_descriptions.json",
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
    """Check if all required model files exist and look usable.

    Returns True only when _classify_model_state returns 'ok'.
    """
    return _classify_model_state(model_dir, files) == "ok"


def _classify_model_state(model_dir, files):
    """Return 'ok', 'missing', 'incomplete', or 'unverified' for a model dir.

    - 'missing':    directory doesn't exist, or no required file is present.
    - 'incomplete': directory exists with some but not all required files,
                    OR model_verify has written a .verify_failed sentinel
                    into the directory (hash mismatch detected at load
                    time or by manual Verify-all).
    - 'unverified': all files present, but SHA256 verification could not be
                    run (HuggingFace metadata API unreachable at download
                    or verify time). Indicated by .verify_skipped sentinel.
                    Files are probably fine — they just couldn't be
                    cryptographically confirmed.
    - 'ok':         all files present and verification passed (or has never
                    been attempted because this is an unpinned legacy install).
    """
    if not os.path.isdir(model_dir):
        return "missing"

    if os.path.isfile(
        os.path.join(model_dir, model_verify.VERIFY_FAILED_SENTINEL)
    ):
        return "incomplete"

    present = [
        f for f in files if os.path.isfile(os.path.join(model_dir, f))
    ]
    if not present:
        return "missing"
    if len(present) < len(files):
        return "incomplete"

    if os.path.isfile(
        os.path.join(model_dir, model_verify.VERIFY_SKIPPED_SENTINEL)
    ):
        return "unverified"

    return "ok"


def get_models():
    """Return list of all models (known + custom) with download status.

    Each entry includes a `state` field with one of:
      - "ok":         model files are all present and pass validation
      - "incomplete": model directory exists but some files are missing or
                      an .onnx.data sidecar is below the size floor
      - "missing":    model directory doesn't exist or has no files

    The legacy `downloaded` boolean is True only for state == "ok".
    """
    config = _load_config()
    registered = {m["id"]: m for m in config.get("models", [])}

    result = []
    for km in KNOWN_MODELS:
        model_dir = os.path.join(DEFAULT_MODELS_DIR, km["id"])
        files = km.get("files", [])
        state = _classify_model_state(model_dir, files)

        # If the default dir doesn't have the model, check any custom
        # registered path before giving up.
        if state != "ok" and km["id"] in registered:
            reg_path = registered[km["id"]].get("weights_path", "")
            if reg_path and reg_path != model_dir:
                reg_state = _classify_model_state(reg_path, files)
                if reg_state in ("ok", "unverified"):
                    model_dir = reg_path
                    state = reg_state

        # "unverified" means all files are present — the model is usable,
        # just not cryptographically confirmed. Treat as downloaded so the
        # pipeline and "Use This" action still work, and surface the
        # reason so Settings can render the caveat.
        downloaded = state in ("ok", "unverified")
        entry = {
            **km,
            "downloaded": downloaded,
            "state": state,
            "weights_path": model_dir if downloaded else None,
            "model_type": km.get("model_type", "bioclip"),
        }
        if state == "unverified":
            entry["verify_skipped_reason"] = (
                model_verify.read_verify_skipped_reason(model_dir)
            )
        result.append(entry)

    # Add custom models
    for mid, m in registered.items():
        if not any(km["id"] == mid for km in KNOWN_MODELS):
            path = m.get("weights_path", "")
            # Custom models: require a .onnx file AND config.json so that
            # a partial download (missing metadata) is not reported as ready.
            downloaded = False
            if path and os.path.isdir(path):
                has_onnx = any(
                    f.endswith(".onnx")
                    for f in os.listdir(path)
                )
                has_config = os.path.isfile(os.path.join(path, "config.json"))
                downloaded = has_onnx and has_config
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
                    "state": "ok" if downloaded else "missing",
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
                            subfolder=None, progress_callback=None,
                            revision=None):
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
        revision: optional HF commit SHA to pin the download to.
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
            if revision:
                kwargs["revision"] = revision

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

    # Pin the download to a specific HF commit SHA. Fetching that revision
    # up front means (a) every file in this download comes from the same
    # immutable snapshot, (b) verification uses those hashes forever
    # regardless of later main updates, and (c) fetch_expected_hashes
    # hits the same revision as hf_hub_download.
    #
    # The revision lookup (model-info API) and the hash fetch (tree API)
    # are independent endpoints. Split them into two try blocks so that a
    # transient outage of the model-info endpoint doesn't silently
    # disable SHA256 verification — we can still verify against 'main'
    # even if we can't pin to a specific SHA.
    pinned_revision: str | None = None
    verification_ran = False
    expected_hashes: dict[str, str] = {}
    skipped_reason: str | None = None

    try:
        pinned_revision = model_verify.fetch_latest_revision(ONNX_REPO)
    except model_verify.VerifyError as e:
        log.warning(
            "Could not fetch latest revision for %s: %s. "
            "Will verify hashes against 'main' instead of an immutable pin.",
            ONNX_REPO, e,
        )

    # Fall back to "main" if the revision lookup failed. That way the tree
    # API still gets a usable revision and verification can proceed, even
    # though the downloaded files won't be pinned in .hf_revision.
    revision_for_hashes = pinned_revision or "main"
    try:
        expected_hashes = model_verify.fetch_expected_hashes(
            hf_subdir, revision=revision_for_hashes
        )
        verification_ran = True
    except model_verify.VerifyError as e:
        log.warning(
            "Could not fetch expected hashes for %s@%s: %s. "
            "Proceeding without post-download verification.",
            hf_subdir, revision_for_hashes, e,
        )
        # Remember the reason so Settings → Models can surface "Unverified"
        # with the underlying cause instead of the failure being invisible.
        skipped_reason = str(e)

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

        _download_and_verify_file(
            filename=filename,
            model_dir=model_dir,
            hf_subdir=hf_subdir,
            expected_hashes=expected_hashes,
            revision=pinned_revision,
            progress_callback=progress_callback,
        )

    # Clear the verify-failed sentinel and persist the revision pin only
    # if we actually ran SHA256 verification and every file matched its
    # expected hash (a hash mismatch would have raised VerifyError out of
    # the loop above, so reaching here with verification_ran=True means
    # everything passed).
    if verification_ran:
        sentinel_path = os.path.join(
            model_dir, model_verify.VERIFY_FAILED_SENTINEL
        )
        if os.path.isfile(sentinel_path):
            with contextlib.suppress(OSError):
                os.unlink(sentinel_path)
        # Successful verification also clears any stale .verify_skipped from
        # a prior download where the HF API was temporarily unreachable.
        model_verify.clear_verify_skipped(model_dir)
        if pinned_revision is not None:
            model_verify.write_pinned_revision(model_dir, pinned_revision)
        else:
            # Verification ran against "main" (model-info API was unavailable
            # so pinned_revision is None).  Clear any existing .hf_revision so
            # that future verify_model calls also use "main" instead of reading
            # a stale SHA from a previous install and fetching expected hashes
            # for the wrong revision — which would cause false mismatches and
            # unnecessary Repair prompts.
            rev_path = os.path.join(model_dir, model_verify.REVISION_FILE)
            with contextlib.suppress(OSError):
                os.unlink(rev_path)
    else:
        # Hash fetch was unavailable so verification was skipped. We still
        # need to update (or clear) the revision pin so that a subsequent
        # verify_model call reads the correct revision rather than a stale
        # SHA from a previous install.
        #
        # - If we know which revision we downloaded from (pinned_revision is
        #   not None — the model-info API responded even though the tree API
        #   failed), write that revision so verify_model pins to the right
        #   commit once the tree API comes back online.
        # - If revision lookup also failed (pinned_revision is None), delete
        #   any existing .hf_revision. A stale pin would cause verify_model
        #   to fetch expected hashes for the old SHA and report false
        #   mismatches for files that are actually correct.
        rev_path = os.path.join(model_dir, model_verify.REVISION_FILE)
        if pinned_revision is not None:
            model_verify.write_pinned_revision(model_dir, pinned_revision)
        else:
            with contextlib.suppress(OSError):
                os.unlink(rev_path)

        # Record the skipped-verification state so Settings → Models can
        # show "Unverified — could not reach HuggingFace" with the cause,
        # rather than pretending the download fully succeeded.
        if skipped_reason:
            model_verify.write_verify_skipped(model_dir, skipped_reason)

        # SHA256 verification was unavailable (HF tree API unreachable).
        # Apply a minimal size floor to weight sidecar files so that a
        # truncated or stub download is surfaced immediately rather than
        # being registered as a healthy model that later fails at runtime.
        # Only .onnx.data files are checked — in external-data ONNX layouts
        # the graph .onnx file can legitimately be much smaller than the
        # floor while the real weights live in the .onnx.data sidecar.
        for filename in files:
            if not filename.endswith(".onnx.data"):
                continue
            local_path = os.path.join(model_dir, filename)
            actual_size = os.path.getsize(local_path) if os.path.isfile(local_path) else 0
            if actual_size < _MIN_BINARY_MODEL_BYTES:
                # Write the verify-failed sentinel so _classify_model_state
                # reports 'incomplete' and get_models() shows Repair.
                # Without this, the truncated file stays on disk and the
                # model is treated as healthy on next check.
                sentinel = os.path.join(
                    model_dir, model_verify.VERIFY_FAILED_SENTINEL
                )
                with open(sentinel, "w") as f:
                    f.write(f"size-floor: {filename} {actual_size} < {_MIN_BINARY_MODEL_BYTES}\n")
                raise RuntimeError(
                    f"Downloaded {km['name']} ({filename}) appears truncated "
                    f"({actual_size:,} bytes, expected ≥ {_MIN_BINARY_MODEL_BYTES:,} bytes). "
                    "Open Settings → Models and click Repair to retry the download."
                )

    state = _classify_model_state(model_dir, files)
    # "unverified" is an acceptable post-download state: every required file
    # is present, only the cryptographic check was skipped because the HF
    # metadata API wasn't reachable. The .verify_skipped sentinel makes
    # that visible in Settings and doesn't block the user.
    if state not in ("ok", "unverified"):
        raise RuntimeError(
            f"Downloaded {km['name']} failed post-download validation "
            f"({state}). Some files may be missing in {model_dir}."
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
    # The on-disk bytes just changed, so drop any cached "verified" marker
    # for this model_id — the next pipeline run will re-verify.
    model_verify.clear_verified_cache(model_id)
    return model_dir


_MAX_HASH_RETRIES = 2  # 1 initial attempt + 2 retries = 3 total per file

# Minimum size for .onnx.data weight sidecar files when post-download SHA256
# verification is unavailable (HF tree API unreachable).  Guards against
# truncated or stub downloads being silently registered as healthy models.
# Only applied to .onnx.data files — graph .onnx files can legitimately be
# smaller than this floor in external-data ONNX layouts.
_MIN_BINARY_MODEL_BYTES = 10 * 1024 * 1024  # 10 MB


def _download_and_verify_file(
    filename, model_dir, hf_subdir, expected_hashes, progress_callback,
    revision=None,
):
    """Download one file and verify its SHA256 against expected_hashes.

    When `revision` is not None, hf_hub_download is pinned to that commit
    SHA so the cache is keyed on an immutable snapshot. On mismatch,
    deletes the file from both the local model dir and the HuggingFace
    cache (otherwise hf_hub_download would happily hand back the same
    corrupt blob on retry) and retries up to _MAX_HASH_RETRIES. On final
    mismatch, raises VerifyError.
    """
    attempts = 0
    while True:
        _hf_download_with_retry(
            ONNX_REPO,
            filename,
            model_dir,
            subfolder=hf_subdir,
            progress_callback=progress_callback,
            revision=revision,
        )

        expected_sha = expected_hashes.get(filename)
        if expected_sha is None:
            # Not an LFS file — HF didn't give us a hash, so we can't verify.
            return

        local_path = os.path.join(model_dir, filename)
        actual_sha = model_verify.sha256_file(local_path)
        if actual_sha == expected_sha:
            return

        attempts += 1
        log.warning(
            "hash mismatch for %s (attempt %d): expected %s..., got %s...",
            filename, attempts, expected_sha[:8], actual_sha[:8],
        )
        if attempts > _MAX_HASH_RETRIES:
            # Write .verify_failed before raising so _classify_model_state
            # reports 'incomplete' even though download_model's post-loop
            # sentinel logic is skipped by the exception.  Without this,
            # a repair on an already-installed model leaves all files
            # present and the model appears 'ok' despite proven corruption.
            sentinel = os.path.join(
                model_dir, model_verify.VERIFY_FAILED_SENTINEL
            )
            try:
                with open(sentinel, "w") as f:
                    f.write(
                        f"hash-mismatch: {filename} "
                        f"expected {expected_sha[:8]}... "
                        f"got {actual_sha[:8]}...\n"
                    )
            except OSError:
                pass
            raise model_verify.VerifyError(
                f"{filename} failed SHA256 verification after "
                f"{_MAX_HASH_RETRIES + 1} attempts "
                f"(expected {expected_sha[:8]}..., got {actual_sha[:8]}...)"
            )

        if progress_callback:
            progress_callback(
                f"Re-downloading corrupted {filename} "
                f"(retry {attempts}/{_MAX_HASH_RETRIES})..."
            )
        with contextlib.suppress(OSError):
            os.unlink(local_path)
        _purge_hf_cache_file(filename, hf_subdir, revision=revision)


def _purge_hf_cache_file(filename, hf_subdir, revision=None):
    """Delete a cached file from the HuggingFace cache so the next
    hf_hub_download call fetches fresh bytes instead of returning the
    corrupt blob it previously cached.

    The HF cache layout is:
        blobs/<oid>                       <- actual file bytes
        snapshots/<revision>/{path}       -> ../../blobs/<oid> symlink

    `try_to_load_from_cache` returns the snapshot path, which is
    typically a symlink into blobs/. Unlinking only the symlink would
    leave the blob intact and hf_hub_download would happily relink to
    the same corrupt bytes on retry. So we resolve the symlink to its
    target and delete both.

    `revision` must match the revision used in the hf_hub_download call
    that produced the cached file; without it try_to_load_from_cache
    resolves the entry for the default branch (main) instead of the
    pinned snapshot, leaving the corrupt blob for the pinned commit
    untouched and causing repeated hash-mismatch retries.
    """
    try:
        import huggingface_hub
    except ImportError:
        return

    lookup_kwargs: dict = dict(
        repo_id=ONNX_REPO,
        filename=f"{hf_subdir}/{filename}" if hf_subdir else filename,
    )
    if revision is not None:
        lookup_kwargs["revision"] = revision

    try:
        cached = huggingface_hub.try_to_load_from_cache(**lookup_kwargs)
    except Exception as e:
        log.debug("HF cache lookup failed for %s: %s", filename, e)
        return

    if not isinstance(cached, str):
        return

    # Resolve the symlink (if any) to the actual blob target before we
    # unlink the symlink itself — otherwise os.path.realpath on a broken
    # symlink is meaningless.
    blob_target = None
    if os.path.islink(cached):
        blob_target = os.path.realpath(cached)
    elif os.path.isfile(cached):
        blob_target = cached

    with contextlib.suppress(OSError):
        if os.path.islink(cached) or os.path.isfile(cached):
            os.unlink(cached)
            log.info("Purged HF cache snapshot entry: %s", cached)

    if blob_target and blob_target != cached and os.path.isfile(blob_target):
        with contextlib.suppress(OSError):
            os.unlink(blob_target)
            log.info("Purged HF cache blob target: %s", blob_target)


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
