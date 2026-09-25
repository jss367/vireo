"""Model and taxonomy registry for Vireo.

All models are ONNX format, downloaded from the jss367/vireo-onnx-models
HuggingFace repository into ~/.vireo/models/{model-id}/.
"""

import contextlib
import json
import logging
import os
import shutil
import tempfile
import threading

import model_verify

log = logging.getLogger(__name__)

DEFAULT_MODELS_DIR = os.path.expanduser("~/.vireo/models")
CONFIG_PATH = os.path.expanduser("~/.vireo/models.json")

# HuggingFace repo containing all ONNX models
ONNX_REPO = "jss367/vireo-onnx-models"

# BioCLIP model_strs whose model *type* can classify with no label list
# (open-vocabulary all-species Tree of Life). This is the capability
# question — "if the artifacts were installed, could this model do ToL?"
# — not the readiness question. A model's ToL artifacts (tol_embeddings.npy
# + tol_classes.json) can be declared in either its KNOWN_MODELS "files"
# manifest (required — the model is 'incomplete' without them) or
# "optional_files" (best-effort — the model still installs for label-list
# classification if the artifacts aren't on HF yet or on disk).
#
# Callers that route into Classifier(labels=None) or advertise
# "classification ready" in the UI must use tree_of_life_ready() below,
# which also checks the artifacts are on disk — otherwise the readiness
# signal lies about a bioclip-2.5 install whose optional ToL files were
# skipped (HF hadn't uploaded them, list_repo_files failed, etc.) and
# the pipeline crashes in Classifier's constructor with FileNotFoundError.
TOL_SUPPORTED_MODEL_STRS = frozenset({
    "hf-hub:imageomics/bioclip",
    "hf-hub:imageomics/bioclip-2",
    "hf-hub:imageomics/bioclip-2.5-vith14",
})

# Filenames that make up a Tree of Life install for a supporting model.
TOL_ARTIFACT_FILES = ("tol_embeddings.npy", "tol_classes.json")


def supports_tree_of_life(model_str):
    """True if the given model_str's TYPE ships Tree of Life text embeddings.

    Capability only — does NOT check whether the artifacts are installed on
    this host. Use `tree_of_life_ready(model_str, model_dir)` at any site
    that needs to know "can we actually run ToL right now" (label-free
    classification, UI readiness).
    """
    return model_str in TOL_SUPPORTED_MODEL_STRS


def tree_of_life_ready(model_str, model_dir):
    """True if the model supports ToL AND its artifacts are on disk.

    Combines `supports_tree_of_life(model_str)` with a presence check for
    `tol_embeddings.npy` and `tol_classes.json` in `model_dir`. Returns
    False (rather than raising) when `model_dir` is empty/None so callers
    can treat "not installed" and "install is incomplete" uniformly.

    Concrete example this guards against: bioclip-2.5-vith14 declares its
    ToL artifacts as `optional_files`, so a download can succeed without
    them (HF hasn't uploaded them yet, `list_repo_files` failed, or the
    optional download itself failed). A pure `supports_tree_of_life` gate
    would then route classification to `Classifier(labels=None)`, which
    raises FileNotFoundError at construction, and the UI would have
    already advertised "classification ready" — see CORE_PHILOSOPHY:
    "no black boxes".
    """
    if not supports_tree_of_life(model_str):
        return False
    if not model_dir:
        return False
    return all(
        os.path.isfile(os.path.join(model_dir, f))
        for f in TOL_ARTIFACT_FILES
    )


# The model a fresh install downloads and classifies with by default (the
# welcome flow downloads this, and get_active_model() prefers it when the user
# hasn't chosen one). BioCLIP-2.5 is the strongest variant and ships Tree of
# Life embeddings, so it classifies label-free out of the box — no regional
# species list required for a new user to get results.
DEFAULT_MODEL_ID = "bioclip-2.5-vith14"

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
        "supports_label_lists": True,
        "label_list_tag": "uses label list",
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
        "supports_label_lists": True,
        "label_list_tag": "uses label list",
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
        # ToL artifacts are optional so that 2.5 installs stay usable for
        # normal label-list classification even while the artifacts are
        # being uploaded to ONNX_REPO — listing them under "files" would
        # cause download_model() to fail on the missing HF entry and mark
        # existing 2.5 installs as `incomplete` on any state check. See
        # docs/tol-embeddings.md for the enablement runbook.
        "optional_files": [
            "tol_embeddings.npy",
            "tol_classes.json",
        ],
        "description": "2025 model with ViT-H/14 backbone, 986M parameters. Largest BioCLIP variant.",
        "size_mb": 3900,
        "architecture": "ViT-H/14",
        "parameters": "986M",
        "supports_label_lists": True,
        "label_list_tag": "uses label list",
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
        "supports_label_lists": False,
        "label_list_tag": "fixed 10K species",
    },
]


# Serializes every read-modify-write of models.json in this process, so a
# download's ``register_model`` and a concurrent ``set_active_model`` cannot
# each load the old file and save over the other's change.
_CONFIG_LOCK = threading.RLock()


def _default_config():
    return {"models": [], "active_model": None}


def _load_config():
    """Load the model config, creating defaults if missing.

    An unreadable or corrupt file is kept as ``models.json.corrupt`` and
    treated as empty, rather than failing every models, readiness and
    pipeline request until the file is fixed by hand.
    """
    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    except FileNotFoundError:
        return _default_config()
    except (OSError, ValueError):
        log.warning("Could not read %s; treating it as empty", CONFIG_PATH,
                    exc_info=True)
        with contextlib.suppress(OSError):
            shutil.copy2(CONFIG_PATH, CONFIG_PATH + ".corrupt")
        return _default_config()
    if not isinstance(config, dict):
        log.warning("%s is not a JSON object; treating it as empty", CONFIG_PATH)
        return _default_config()
    if not isinstance(config.get("models"), list):
        config["models"] = []
    return config


def _save_config(config):
    """Write models.json atomically (temp file in the same dir + replace)."""
    config_dir = os.path.dirname(CONFIG_PATH)
    os.makedirs(config_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=config_dir, prefix=".models.", suffix=".json.tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, CONFIG_PATH)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


def _inside_models_dir(path):
    """True if ``path`` resolves strictly inside ``DEFAULT_MODELS_DIR``."""
    root = os.path.realpath(DEFAULT_MODELS_DIR)
    target = os.path.realpath(path)
    if target == root:
        return False
    try:
        return os.path.commonpath([root, target]) == root
    except ValueError:  # different drives on Windows
        return False


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
        # Declared-but-absent optional files are advertised so the UI can
        # offer a Repair-style re-download when they land on HF. Without
        # this, an existing bioclip-2.5 install stays in state=='ok' even
        # after tol_embeddings.npy is uploaded, and the readiness message
        # at /api/pipeline/page-init ("click Repair in Settings → Models")
        # points at a button that never appears — the Repair button only
        # renders for state=='incomplete', which we intentionally don't
        # trigger for absent optionals (that would mark 2.5 unusable for
        # label-list mode). Keep the list on `missing`/'incomplete' entries
        # too so the same repair pass fetches optionals when it repairs.
        optional_declared = km.get("optional_files") or []
        if optional_declared and downloaded:
            missing_optional = [
                f for f in optional_declared
                if not os.path.isfile(os.path.join(model_dir, f))
            ]
            if missing_optional:
                entry["missing_optional_files"] = missing_optional
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
                    "supports_label_lists": True,
                    "label_list_tag": "uses label list",
                }
            )

    return result


def build_self_heal_redownloader(model_dir):
    """Return a zero-arg callable that re-downloads the known model living
    at ``model_dir``, or ``None`` when no known model matches.

    Used by the classifier / timm_classifier self-heal path so that when
    ONNXRuntime rejects a model file on load, the on-disk copy is replaced
    with a fresh download from HuggingFace. Custom user-registered models
    and unknown paths return ``None`` — the self-heal wrapper then surfaces
    the original ONNX load error instead of silently deleting bytes we
    have no way to replace.
    """
    if not model_dir:
        return None
    try:
        normalized = os.path.realpath(model_dir)
    except OSError:
        normalized = model_dir
    for km in KNOWN_MODELS:
        km_dir = os.path.join(DEFAULT_MODELS_DIR, km["id"])
        if os.path.realpath(km_dir) == normalized or km_dir == model_dir:
            # Bind via default args so the closure captures the current
            # loop values rather than late-binding references (ruff B023).
            def _redownload(_model_id=km["id"], _model_dir=model_dir):
                log.warning(
                    "Self-heal: re-downloading %s into %s after corrupt "
                    "model load failure",
                    _model_id, _model_dir,
                )
                download_model(_model_id)

            return _redownload
    return None


def get_active_model():
    """Return the currently active model config, or the default when unset.

    Priority: the explicitly-selected active_model, then DEFAULT_MODEL_ID
    when downloaded AND label-free-ready (Tree of Life artifacts on disk),
    then any downloaded model (KNOWN_MODELS order).

    The ToL-ready gate on the default matters because bioclip-2.5's ToL
    artifacts are declared as `optional_files`: an install can succeed
    without them and stay usable for label-list mode. In that partial
    state, overriding a fully-ready ToL model (e.g. bioclip-2, which
    carries its ToL files as required) with 2.5 would flip the welcome
    flow to "setup blocked" and make label-free classification raise in
    Classifier's label loader. Only take over the fallback when the
    default can actually classify on its own; otherwise fall through so a
    ToL-ready model already on disk still wins.
    """
    config = _load_config()
    models = get_models()
    active_id = config.get("active_model")

    if active_id:
        for m in models:
            if m["id"] == active_id and m["downloaded"]:
                return m

    for m in models:
        if (
            m["id"] == DEFAULT_MODEL_ID
            and m["downloaded"]
            and tree_of_life_ready(
                m.get("model_str", ""), m.get("weights_path")
            )
        ):
            return m

    # Otherwise fall back to the first downloaded model.
    for m in models:
        if m["downloaded"]:
            return m

    return None


def set_active_model(model_id):
    """Set the active model."""
    with _CONFIG_LOCK:
        config = _load_config()
        config["active_model"] = model_id
        _save_config(config)


def remove_model(model_id):
    """Remove a model's weights from disk and unregister it.

    Files are deleted only when they live inside ``DEFAULT_MODELS_DIR``,
    where downloads land. A custom model registered with weights elsewhere
    (the user's own folder) is only unregistered; its files are left in
    place and reported back as ``kept_path``.

    Returns ``None`` if the model is unknown, otherwise a dict with
    ``files_deleted`` (bool) and ``kept_path`` (str or ``None``).
    """
    with _CONFIG_LOCK:
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
                if os.path.isdir(path) and _inside_models_dir(path):
                    shutil.rmtree(path)
                    return {"files_deleted": True, "kept_path": None}
            return None

        files_deleted = False
        kept_path = None
        weights_path = found.get("weights_path") or ""
        if weights_path and os.path.lexists(weights_path):
            if not _inside_models_dir(weights_path):
                log.info(
                    "Unregistering model %s without deleting %s: it is "
                    "outside %s", model_id, weights_path, DEFAULT_MODELS_DIR,
                )
                kept_path = weights_path
            elif os.path.isdir(weights_path) and not os.path.islink(weights_path):
                shutil.rmtree(weights_path)
                files_deleted = True
            else:
                os.unlink(weights_path)
                files_deleted = True
                parent = os.path.dirname(weights_path)
                if (
                    _inside_models_dir(parent) and os.path.isdir(parent)
                    and not os.listdir(parent)
                ):
                    os.rmdir(parent)

        config["models"] = [m for m in models if m["id"] != model_id]
        if config.get("active_model") == model_id:
            config["active_model"] = None
        _save_config(config)

    log.info("Removed model %s (weights: %s, deleted: %s)",
             model_id, weights_path, files_deleted)
    return {"files_deleted": files_deleted, "kept_path": kept_path}


def register_model(model_id, name, model_str, weights_path, description=""):
    """Register a model (custom or after download)."""
    with _CONFIG_LOCK:
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


def _needs_atomic_publish(filename):
    """True if `filename` must be published via a temp copy + os.replace.

    Only the small JSON metadata files need this. They can be read by
    another thread or process *while* a download or self-heal is writing
    them — the background label_descriptions heal racing a
    TimmClassifier init is the concrete case — so a reader must see
    either the whole file or no file at all, never a torn one.

    Deliberately NOT applied to the weight artifacts (.onnx, .onnx.data,
    .npy). shutil.copy2 is a real byte copy on Windows and Linux (only
    APFS clones cheaply), so staging those would put a second full-size
    copy beside a model that is already on disk: a Repair of the 3.9 GB
    BioCLIP-2.5 fetching one optional metadata file would transiently
    need ~7.8 GB and can fail with ENOSPC where it previously fit.
    Nothing reads weights concurrently with a download — an ONNX session
    opens them once at classifier init, after the download has returned.

    Suffix-based rather than a size threshold on purpose: the rule stays
    legible and doesn't silently change behaviour as models grow.
    """
    return filename.endswith(".json")


@contextlib.contextmanager
def _staged_sibling(dest_path):
    """Yield a unique staging path beside `dest_path` for atomic publish.

    A fixed ``<dest>.tmp`` name is *shared* by every concurrent writer,
    which throws away the guarantee the os.replace exists for. The
    background label_descriptions heal and a Settings Repair are both
    supported recovery paths and can run at the same time — Repair calls
    ensure_timm_label_descriptions directly — so both can reach the same
    staging pathname. One writer's open() then truncates the inode the
    other is still filling, one of them renames that shared inode onto
    the target and briefly publishes a partial document, and the loser
    fails outright because its temporary pathname has disappeared.
    mkstemp hands each invocation its own inode, so the only thing that
    overlaps is the os.replace itself, and that is atomic.

    Staged in the destination's own directory so the replace stays within
    one filesystem (what makes it atomic) — and, for the models dir,
    so it can't strand a copy on a different volume.

    The descriptor mkstemp opens is closed before the path is yielded:
    Windows refuses to replace a file that any handle still has open, so
    every caller must reach os.replace with nothing open on the staging
    file. Callers here either shutil.copy2 onto the path or open/close it
    in a ``with`` block, both of which leave no handle behind.

    Removes the staging file if it still exists on exit, so a failed
    writer never litters the model directory. After a successful
    os.replace there is nothing left to remove.
    """
    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(dest_path) or ".",
        prefix=f"{os.path.basename(dest_path)}.part-",
    )
    os.close(fd)
    try:
        yield tmp_path
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)


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

    from huggingface_hub import hf_hub_download, try_to_load_from_cache

    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "300")

    attempt = 0
    stalled_count = 0
    max_stalled = 3

    while True:
        attempt += 1
        try:
            kwargs = {
                "repo_id": repo_id,
                "filename": filename,
            }
            if subfolder:
                kwargs["subfolder"] = subfolder
            if revision:
                kwargs["revision"] = revision

            # Detect cache hit before triggering any network activity, so the
            # log + UI can distinguish "1.2 GB came over the wire" from
            # "the blob was already in the HF cache and we just cloned it
            # into the model dir" (a few hundred ms on APFS even for huge
            # files). Without this distinction a sub-second 'download' of
            # a multi-GB model looks suspicious instead of correct.
            from_cache = False
            if attempt == 1:
                try:
                    pre_cached = try_to_load_from_cache(
                        repo_id=repo_id,
                        filename=(f"{subfolder}/{filename}"
                                  if subfolder else filename),
                        revision=revision,
                    )
                    from_cache = (
                        isinstance(pre_cached, str)
                        and os.path.isfile(pre_cached)
                    )
                except Exception as e:
                    # Cache lookup is best-effort — if it fails, fall through
                    # to the network path; hf_hub_download will sort it out.
                    log.debug("Cache lookup failed for %s: %s", filename, e)

            label_path = f"{subfolder}/{filename}" if subfolder else filename
            if progress_callback:
                if attempt == 1:
                    if from_cache:
                        progress_callback(
                            f"Found {filename} in HF cache, copying..."
                        )
                    else:
                        progress_callback(
                            f"Downloading {filename} from Hugging Face..."
                        )
                else:
                    progress_callback(f"Resuming download (attempt {attempt})...")

            if from_cache:
                log.info(
                    "%s/%s already in HF cache — no network download needed",
                    repo_id, label_path,
                )
            else:
                log.info(
                    "Downloading %s/%s (attempt %d)",
                    repo_id, label_path, attempt,
                )

            cached_path = hf_hub_download(**kwargs)

            # Copy from cache to our models directory.  On APFS shutil.copy2
            # uses clonefile() (copy-on-write metadata-only), so cloning a
            # 1+ GB blob takes milliseconds and shares disk pages with the
            # cache until either side is modified.
            dest_path = os.path.join(local_dir, filename)
            # filename can be repo-relative (e.g. "onnx/model.onnx" for
            # custom HF repos with the standard onnx/ layout) — create the
            # full destination directory, not just local_dir, or copy2
            # raises FileNotFoundError that the retry loop misclassifies
            # as a connection failure.
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            if cached_path != dest_path:
                if _needs_atomic_publish(filename):
                    # Publish atomically. copy2 straight onto dest_path
                    # makes the destination visible while it is still
                    # being written, so a concurrent reader (e.g. the
                    # background label_descriptions heal racing a
                    # TimmClassifier init) can json.load a torn file, and
                    # a process exit mid-copy leaves a truncated file
                    # whose mere existence suppresses future repairs.
                    # Copy beside it, then os.replace (atomic on POSIX
                    # and Windows) so readers only ever see the whole
                    # file or no file. Cheap here: these are small
                    # metadata files — see _needs_atomic_publish for why
                    # the multi-GB weight artifacts must not do this.
                    # The staging name is unique per invocation (see
                    # _staged_sibling) so two concurrent publishers of the
                    # same file never share one staging inode.
                    with _staged_sibling(dest_path) as tmp_path:
                        shutil.copy2(cached_path, tmp_path)
                        os.replace(tmp_path, dest_path)
                else:
                    # Weights: copy straight into place so a download
                    # never needs 2x the model size on disk. A failed
                    # copy still leaves nothing behind — copy2 has
                    # already truncated any previous copy, so the
                    # half-written remains are worthless and would only
                    # look like a healthy artifact to the next
                    # completeness check.
                    try:
                        shutil.copy2(cached_path, dest_path)
                    except BaseException:
                        with contextlib.suppress(OSError):
                            os.unlink(dest_path)
                        raise

            if from_cache:
                size_mb = os.path.getsize(dest_path) / 1024 / 1024
                log.info(
                    "Linked from cache: %s (%.1f MB, no network transfer)",
                    dest_path, size_mb,
                )
            else:
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

    # Optional files (best-effort). Downloaded only after required files
    # succeed so a missing/unavailable optional never turns into a hard
    # failure of the install. Use case: ToL artifacts declared under a
    # model's `optional_files` (e.g. bioclip-2.5-vith14 while its
    # tol_embeddings.npy is being uploaded to ONNX_REPO) — the model must
    # still install successfully and be usable for label-list mode until
    # the artifacts land.
    optional_files_list = km.get("optional_files", [])
    if optional_files_list:
        _download_optional_files(
            optional_files_list, model_dir, hf_subdir,
            expected_hashes=expected_hashes,
            revision=pinned_revision,
            progress_callback=progress_callback,
        )

    # Settings → Models offers Repair as the fix for a model that is
    # missing its optional files, so Repair has to actually be able to
    # fix them. For label_descriptions.json (the scientific→common name
    # mapping) the ONNX repo copy is only one of two sources: when the
    # repo doesn't carry the file yet, the loop above skips it silently
    # and Repair would be a permanent no-op. ensure_timm_label_descriptions
    # also derives the mapping from the upstream timm config, so run it
    # here — it returns immediately when the file is already usable, and
    # never raises.
    model_str = km.get("model_str") or ""
    if (
        "label_descriptions.json" in optional_files_list
        and model_str.startswith("hf-hub:")
    ):
        ensure_timm_label_descriptions(
            model_dir, model_str, progress_callback=progress_callback,
        )
        # Repair means "try again". The background heal's per-installation
        # attempt budget and backoff window are invisible to the user, and
        # Repair leaves already-verified required artifacts alone, so the
        # installation generation they are keyed to does not change —
        # without an explicit reset the button cannot revive a heal that
        # failed during an outage.
        try:
            import timm_classifier
            timm_classifier.reset_label_desc_heal_state(model_str)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("Could not reset label-description heal state: %s", e)

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
    revision=None, optional=False,
):
    """Download one file and verify its SHA256 against expected_hashes.

    When `revision` is not None, hf_hub_download is pinned to that commit
    SHA so the cache is keyed on an immutable snapshot. On mismatch,
    deletes the file from both the local model dir and the HuggingFace
    cache (otherwise hf_hub_download would happily hand back the same
    corrupt blob on retry) and retries up to _MAX_HASH_RETRIES. On final
    mismatch, raises VerifyError.

    When `optional` is True, a final hash mismatch skips the shared
    .verify_failed sentinel write so a corrupt optional file doesn't flip
    the whole model's state to 'incomplete'. The caller is expected to
    delete the local file on failure.

    A file that is already on disk and already matches its expected
    SHA256 is left alone: no download, no copy, no rewrite.
    """
    local_path = os.path.join(model_dir, filename)

    # Repair calls download_model for the whole manifest even when it only
    # needs one file (e.g. the newly optional label_descriptions.json).
    # Re-fetching a 3.9 GB weight blob that is already byte-for-byte
    # correct costs a full copy out of the HF cache for no benefit, so
    # hash what's on disk first and skip whatever already passes.
    # Only possible when HF gave us an expected hash — without one we
    # can't tell "correct" from "corrupt", so those still get downloaded.
    pre_sha = expected_hashes.get(filename)
    if (
        pre_sha is not None
        and os.path.isfile(local_path)
        and model_verify.sha256_file(local_path) == pre_sha
    ):
        log.info(
            "%s already present and matches expected SHA256 — "
            "skipping download", filename,
        )
        if progress_callback:
            progress_callback(f"{filename} already verified, skipping")
        return

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
            # Skipped for optional files — their corruption must not gate
            # the whole model's completeness.
            if not optional:
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


def _download_optional_files(
    filenames, model_dir, hf_subdir, expected_hashes, revision,
    progress_callback,
):
    """Best-effort download of a model's optional_files.

    Uses list_repo_files as a preflight so that optional files not yet
    uploaded to the HF repo are skipped silently without burning the
    retry budget in _hf_download_with_retry on a 404. Any per-file
    download or verification failure is logged; if this invocation
    published a replacement that then failed verification, the replaced
    file is removed. A pre-existing usable file is left in place when the
    failure happened before it was replaced (transient network error or
    ENOSPC in the atomic staging path) — required files remain the source
    of truth for install completeness, so a missing/broken optional never
    fails the install, and a Repair must never turn a working
    ``label_descriptions.json`` into a missing one just because HF blipped.
    """
    if not filenames:
        return

    try:
        from huggingface_hub import list_repo_files
    except ImportError:
        log.info(
            "huggingface_hub.list_repo_files unavailable; "
            "skipping optional downloads for %s.", hf_subdir,
        )
        return

    lookup_revision = revision or "main"
    try:
        repo_files = set(list_repo_files(
            ONNX_REPO, revision=lookup_revision,
        ))
    except Exception as e:
        log.info(
            "Could not list %s@%s to probe optional files (%s); "
            "skipping optional downloads.",
            ONNX_REPO, lookup_revision, e,
        )
        return

    for fi, filename in enumerate(filenames):
        hf_path = f"{hf_subdir}/{filename}" if hf_subdir else filename
        if hf_path not in repo_files:
            log.info(
                "Optional file %s not present in %s@%s; skipping.",
                hf_path, ONNX_REPO, lookup_revision,
            )
            continue
        if progress_callback:
            progress_callback(
                f"Downloading optional {fi + 1}/{len(filenames)}: {filename}",
            )
        local_path = os.path.join(model_dir, filename)
        pre_signature = _path_signature(local_path)
        try:
            _download_and_verify_file(
                filename=filename,
                model_dir=model_dir,
                hf_subdir=hf_subdir,
                expected_hashes=expected_hashes,
                revision=revision,
                progress_callback=progress_callback,
                optional=True,
            )
        except (model_verify.VerifyError, RuntimeError) as e:
            post_signature = _path_signature(local_path)
            if pre_signature is not None and post_signature == pre_signature:
                # Atomic staging (or the network probe before it) failed
                # without touching the previously usable destination. Leave
                # it in place — otherwise a Repair against a flaky network
                # or a full disk would silently degrade a working install.
                log.warning(
                    "Optional file %s could not be refreshed (%s); "
                    "keeping the previously installed copy at %s.",
                    filename, e, local_path,
                )
            else:
                log.warning(
                    "Optional file %s could not be downloaded/verified (%s); "
                    "removing partial file. Model remains usable without it.",
                    filename, e,
                )
                with contextlib.suppress(OSError):
                    if os.path.isfile(local_path):
                        os.unlink(local_path)


def _path_signature(path):
    """``(size, mtime_ns, inode)`` identifying the current on-disk file,
    or None when it is absent or unstat-able.

    Includes ``st_ino`` because ``os.replace`` swaps the destination inode
    even when the replacement lands with the same ``(size, mtime_ns)``
    (``shutil.copy2`` preserves the source's mtime), so an atomic
    republish is always visible as a change.
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_size, int(st.st_mtime_ns), st.st_ino)


def _load_upstream_timm_config(model_str):
    """Fetch the upstream timm repo's config.json for a hf-hub model_str.

    The timm config embeds ``label_descriptions`` ({"Sturnus vulgaris":
    "European Starling, Bird"}) alongside the label names, so it can
    reconstruct the common-name mapping even when our ONNX repo predates
    the label_descriptions.json export.
    """
    from huggingface_hub import hf_hub_download

    repo_id = model_str.removeprefix("hf-hub:")
    config_path = hf_hub_download(repo_id=repo_id, filename="config.json")
    with open(config_path) as f:
        return json.load(f)


def _open_file_signature(fileobj):
    """``(size, mtime_ns)`` taken from an already-open file's descriptor.

    Read off the descriptor rather than the path so it describes the
    bytes this reader actually got, even if the file is atomically
    republished (Repair, self-heal) a moment later. A path stat taken
    after the read would describe the *replacement*.
    """
    try:
        st = os.fstat(fileobj.fileno())
    except OSError:
        return None
    return (st.st_size, int(st.st_mtime_ns))


def read_label_descriptions(path):
    """Return ``(mapping_or_None, signature_or_None)``.

    ``signature`` identifies the file state this call actually consumed
    (see ``_open_file_signature``); None when the file was absent or
    could not be stat-ed. Callers that cache the parsed result key it by
    this signature, so an instance is never treated as having read a
    file that only landed after it read.

    See ``load_label_descriptions`` for the mapping semantics.
    """
    signature = None
    try:
        with open(path) as f:
            signature = _open_file_signature(f)
            data = json.load(f)
    except FileNotFoundError:
        return None, None
    except (OSError, ValueError) as e:
        log.warning("Unusable %s (%s); will re-heal.", path, e)
        return None, signature
    if not isinstance(data, dict):
        return None, signature
    if not all(isinstance(v, str) for v in data.values()):
        # Parseable JSON but the wrong schema: a null/number/list value
        # would sail past this gate and then blow up on ``.rsplit`` while
        # the classifier builds its common-name map — and because the file
        # parses, the heal would never repair it. Same "existence
        # suppresses repair" hazard as a truncated write, so treat it the
        # same way and report the file as unusable. Rejected whole rather
        # than filtered: a half-valid mapping is still a broken artifact,
        # and silently keeping the good half would mark it healed and
        # leak scientific names for every dropped entry forever.
        log.warning(
            "Unusable %s (non-string description values); will re-heal.",
            path,
        )
        return None, signature
    return data, signature


def load_label_descriptions(path):
    """Return the {scientific name: description} mapping, or None.

    None means "not usable, heal it": missing, unreadable, not parseable
    as a JSON object, or a JSON object whose values are not all strings.
    Existence alone is not enough — a file truncated by a process exit
    mid-write (or a disk-full write) still passes os.path.isfile but
    blows up json.load, and a file that parses but carries a null or a
    list where a description belongs blows up the classifier's
    ``rsplit`` instead. Vireo repairs broken state itself rather than
    asking the user to delete files, so every "do I need to heal?" check
    goes through this.

    An empty mapping is a complete document (the model simply has no
    common names) and is returned as-is, not treated as broken.
    """
    return read_label_descriptions(path)[0]


def label_descriptions_usable(path):
    """True when `path` holds a parseable descriptions mapping."""
    return load_label_descriptions(path) is not None


def ensure_timm_label_descriptions(model_dir, model_str, progress_callback=None):
    """Self-heal a missing label_descriptions.json for a timm model.

    Installs that predate the label_descriptions.json export resolve
    common names through taxonomy.json alone; when that lookup misses
    (e.g. the 2021 class list says "Bubulcus ibis" but the current
    taxonomy renamed it to "Ardea ibis"), the raw scientific name leaks
    into predictions and the review UI. Try, in order:

    1. nothing to do — a parseable file already exists
    2. the vireo ONNX repo's copy (normal optional-file path)
    3. derive it from the upstream timm repo's config.json, which embeds
       the same {"Genus species": "Common Name, Category"} mapping

    Best-effort: never raises; returns True when the file exists
    afterwards.
    """
    target = os.path.join(model_dir, "label_descriptions.json")
    descs, inspected = read_label_descriptions(target)
    if descs is not None:
        return True

    # Present but unparseable when we looked — a torn write from an
    # earlier build or a killed process. Drop it so `list_models` stops
    # reporting the optional file as installed (its check is
    # existence-based) and stops hiding the Repair affordance.
    #
    # Only when it is still the artifact we inspected, though. This heal
    # runs on a background thread and can overlap the Settings Repair
    # job, which publishes a *valid* label_descriptions.json with
    # os.replace. Unlinking unconditionally would delete that good
    # repair, and if our own repository/config fetches below then fail
    # we would record the generation as `failed` and suppress every
    # later heal — the exact "user must restart Vireo" state this whole
    # path exists to avoid.
    #
    # ``inspected is None`` means the file was absent (or unstat-able),
    # so there is nothing to drop.
    if inspected is not None:
        recheck, current = read_label_descriptions(target)
        if recheck is not None:
            # Someone published a usable mapping between our read and
            # now. The repair already happened; report success and, above
            # all, do not delete it.
            return True
        if current is not None and current == inspected:
            with contextlib.suppress(OSError):
                os.unlink(target)
        # Otherwise the file changed since we inspected it. Leave it for
        # the next heal pass to re-inspect: both recovery paths below
        # stage-and-replace, so keeping it costs nothing this round.

    km = next(
        (m for m in KNOWN_MODELS if m.get("model_str") == model_str), None
    )
    if km and "label_descriptions.json" in (km.get("optional_files") or []):
        # _download_optional_files writes each file to
        # os.path.join(model_dir, filename). _hf_download_with_retry
        # already publishes .json artifacts atomically, so the target
        # can't be seen half-written — but "complete" is not the same as
        # "usable": HF can hand back a file that is itself truncated, and
        # its mere existence at the published path would make every later
        # heal's check treat it as already healed. Download into a scratch
        # subdir on the same filesystem, validate, and only then
        # os.replace onto the published target.
        scratch_dir = tempfile.mkdtemp(
            prefix=".label_desc_heal_", dir=model_dir,
        )
        scratch_target = os.path.join(scratch_dir, "label_descriptions.json")
        try:
            try:
                _download_optional_files(
                    ["label_descriptions.json"], scratch_dir,
                    km.get("hf_subdir", km["id"]), {}, None, progress_callback,
                )
            except Exception as e:
                log.info(
                    "Optional label_descriptions.json download failed for %s: %s",
                    model_str, e,
                )
            # Validate before publishing: a scratch file that arrived
            # truncated or unparseable must not be promoted, or its
            # existence at `target` would suppress every later repair.
            if label_descriptions_usable(scratch_target):
                try:
                    os.replace(scratch_target, target)
                except OSError as e:
                    log.warning(
                        "Could not publish healed label_descriptions.json "
                        "for %s: %s", model_str, e,
                    )
                else:
                    log.info(
                        "Self-healed label_descriptions.json for %s from %s",
                        model_str, ONNX_REPO,
                    )
                    return True
        finally:
            shutil.rmtree(scratch_dir, ignore_errors=True)

    try:
        config = _load_upstream_timm_config(model_str)
    except Exception as e:
        log.info(
            "Could not fetch upstream timm config for %s: %s. "
            "Predictions keep using taxonomy-based common names.",
            model_str, e,
        )
        return False

    descs = config.get("label_descriptions")
    if not isinstance(descs, dict) or not descs:
        log.info(
            "Upstream timm config for %s has no label_descriptions; "
            "cannot self-heal common names.", model_str,
        )
        return False

    # Stage under a name unique to this invocation, never a shared
    # "<target>.tmp": a background heal and a Settings Repair both reach
    # this fallback when the ONNX repo has no copy of the file, and a
    # shared staging inode would let one truncate the other's bytes and
    # publish a torn document. mkstemp creates the file 0600 and leaves
    # it that way; ~/.vireo is single-user data, so private is the right
    # default. The handle is closed before os.replace — Windows will not
    # replace a file that anything still has open.
    try:
        with _staged_sibling(target) as tmp_target:
            with open(tmp_target, "w") as f:
                json.dump(descs, f)
                # Closing only hands the bytes to the page cache; a crash
                # right after the rename would otherwise expose exactly
                # the empty/partial target this path exists to prevent.
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_target, target)
    except OSError as e:
        # Losing the publish race is not a failure to heal. On Windows two
        # os.replace calls onto one destination serialize and the loser
        # gets PermissionError ("Access is denied") even though the file
        # it wanted is now on disk — see the same handling in
        # local_masks.snapshot. This is a post-condition check, not a
        # retry loop: if a usable document is published, this heal's goal
        # is met and reporting failure would only arm a needless retry.
        if label_descriptions_usable(target):
            log.info(
                "label_descriptions.json for %s was published by a "
                "concurrent writer while this heal was staging (%s)",
                model_str, e,
            )
            return True
        log.warning("Could not write %s: %s", target, e)
        return False

    log.info(
        "Self-healed label_descriptions.json for %s from upstream timm "
        "config (%d entries)", model_str, len(descs),
    )
    return True


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


_TAXONOMY_MIN_USABLE_BYTES = 1_000_000


def get_taxonomy_info():
    """Return taxonomy status info.

    ``available`` means *the file on disk is usable as a taxonomy*, not just
    that a path exists. A 0-byte stub from an interrupted download or a
    truncated write must report ``available: False`` so the UI keeps the
    "Download taxonomy" affordance visible — the prior file-existence check
    was hiding that affordance for unrecoverable users (CORE_PHILOSOPHY:
    "Show the user what's happening / No black boxes").

    Cheap-but-effective check: the real iNat taxonomy is hundreds of MB,
    so anything under ~1MB is structurally broken. Full JSON parsing here
    would dominate page-init latency for the common case where the file
    is valid; the deeper parse happens at first ``Taxonomy(path)`` use.
    """
    from taxonomy import find_taxonomy_json
    taxonomy_path = find_taxonomy_json()
    if not os.path.exists(taxonomy_path):
        return {
            "available": False,
            "path": taxonomy_path,
            "taxa_count": 0,
            "last_updated": None,
        }

    # Wrap the stat in the same fault path as the read: the file existed
    # at the exists() check above, but a concurrent taxonomy refresh can
    # remove or replace it between calls. Without this guard, getsize()
    # would raise FileNotFoundError/OSError straight out of the helper
    # and 500 every caller (including /api/pipeline/page-init) in exactly
    # the transient window where we should degrade to "unavailable".
    try:
        size = os.path.getsize(taxonomy_path)
    except OSError:
        return {
            "available": False,
            "path": taxonomy_path,
            "taxa_count": 0,
            "last_updated": None,
            "corrupt": True,
        }

    if size < _TAXONOMY_MIN_USABLE_BYTES:
        return {
            "available": False,
            "path": taxonomy_path,
            "taxa_count": 0,
            "last_updated": None,
            "file_size": size,
            "corrupt": True,
        }

    try:
        with open(taxonomy_path) as f:
            # Only read the metadata, not the full taxa dicts
            raw = f.read(200)
        import re

        updated_match = re.search(r'"last_updated"\s*:\s*"([^"]+)"', raw)
        last_updated = updated_match.group(1) if updated_match else None

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
            "available": False,
            "path": taxonomy_path,
            "taxa_count": 0,
            "last_updated": None,
            "file_size": size,
            "corrupt": True,
        }
