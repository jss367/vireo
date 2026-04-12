"""SHA256 verification of ONNX model files against HuggingFace LFS oids.

HuggingFace stores LFS file hashes as SHA256 in the `lfs.oid` field of
the tree API. This module fetches those expected hashes and compares
them to locally-computed SHA256s, replacing the 10 MB size-floor
heuristic in models.py that only caught the narrowest truncation case.
"""

import contextlib
import hashlib
import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

ONNX_REPO = "jss367/vireo-onnx-models"
_TREE_API = "https://huggingface.co/api/models/{repo}/tree/{revision}/{subdir}"
_MODEL_INFO_API = "https://huggingface.co/api/models/{repo}"
_FETCH_TIMEOUT = 30  # seconds

# Filename (inside each model directory) that pins the HF commit SHA the
# model was downloaded from. Read by verify_if_needed so that upstream
# updates to main don't flip previously-good local files to "incomplete".
REVISION_FILE = ".hf_revision"


VERIFY_FAILED_SENTINEL = ".verify_failed"


class VerifyError(Exception):
    """Raised when verification can't be performed (network / HTTP error)
    or when a file's SHA256 does not match the expected hash."""


class ModelCorruptError(Exception):
    """Raised by verify_if_needed when an installed model fails its
    integrity check. pipeline_job.model_loader_stage translates this
    into the existing 'open Settings → Models and click Repair' message."""

    def __init__(self, model_id: str, result: "VerifyResult"):
        self.model_id = model_id
        self.result = result
        parts = []
        if result.missing:
            parts.append(f"missing={result.missing}")
        if result.mismatches:
            parts.append(f"mismatches={result.mismatches}")
        super().__init__(
            f"model {model_id} failed integrity check: {', '.join(parts)}"
        )


@dataclass
class VerifyResult:
    ok: bool
    mismatches: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)


# Per-process cache of model ids that have already passed verification.
# Cleared by clear_verified_cache after a successful re-download.
_verified_this_process: set[str] = set()

# Per-process cache of recent hash-fetch failures: model_id → timestamp.
# When verify_if_needed catches a VerifyError (transient network issue) it
# records the time here so that subsequent pipeline starts within the same
# process skip the 30-second network timeout rather than stalling every run.
# The entry expires after _VERIFY_ERROR_TTL seconds; after that the next
# call retries the network check.
_verify_error_cache: dict[str, float] = {}
_VERIFY_ERROR_TTL = 300  # 5 minutes


def sha256_file(path: str) -> str:
    """Return the hex SHA256 of a file, read in 1 MB chunks.

    On modern hardware (SHA-NI / ARMv8 crypto), hashlib runs at ~2 GB/s,
    so even the largest model files in the repo verify in ~1 second.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_latest_revision(repo_id: str) -> str:
    """Return the current commit SHA on main for the given HF repo.

    Used by download_model to pin new downloads to an immutable revision
    so that upstream updates to main won't cause previously-downloaded
    files to fail verification.
    """
    url = _MODEL_INFO_API.format(repo=repo_id)
    try:
        with urllib.request.urlopen(url, timeout=_FETCH_TIMEOUT) as resp:
            payload = json.loads(resp.read())
    except Exception as e:
        raise VerifyError(
            f"failed to fetch latest revision for {repo_id}: {e}"
        ) from e
    sha = payload.get("sha")
    if not sha:
        raise VerifyError(
            f"HuggingFace model-info for {repo_id} did not include a sha"
        )
    return sha


def fetch_expected_hashes(
    hf_subdir: str, revision: str = "main"
) -> dict[str, str]:
    """Fetch expected SHA256 hashes from the HuggingFace tree API.

    Returns a dict mapping basename -> hex SHA256 for every LFS file under
    the given subdirectory of ONNX_REPO at the given revision. Non-LFS
    files (config.json, tokenizer.json) are omitted — their integrity is
    covered by _classify_model_state's file-presence check plus the clear
    parse errors they produce at load time.

    `revision` defaults to "main" for backwards compatibility with models
    that predate the revision-pinning scheme, but download_model always
    passes the commit SHA captured at download time.

    Raises VerifyError on any network or HTTP failure.
    """
    url = _TREE_API.format(
        repo=ONNX_REPO, revision=revision, subdir=hf_subdir
    )
    try:
        with urllib.request.urlopen(url, timeout=_FETCH_TIMEOUT) as resp:
            payload = json.loads(resp.read())
    except Exception as e:
        raise VerifyError(
            f"failed to fetch expected hashes for {hf_subdir}@{revision}: {e}"
        ) from e

    result: dict[str, str] = {}
    for entry in payload:
        lfs = entry.get("lfs")
        if not lfs or "oid" not in lfs:
            continue
        basename = os.path.basename(entry["path"])
        result[basename] = lfs["oid"]
    return result


def _read_pinned_revision(model_dir: str) -> str:
    """Return the pinned HF commit SHA for a model, or 'main' if none.

    Models downloaded before revision-pinning was added have no
    .hf_revision file; for those we fall back to 'main' so verification
    still runs against the current upstream bytes (same behavior as the
    initial implementation).
    """
    path = os.path.join(model_dir, REVISION_FILE)
    if not os.path.isfile(path):
        return "main"
    try:
        with open(path) as f:
            sha = f.read().strip()
        return sha or "main"
    except OSError:
        return "main"


def write_pinned_revision(model_dir: str, revision: str) -> None:
    """Persist the HF commit SHA this model was downloaded from so that
    future verifications use the immutable revision instead of main.
    Called by download_model after a successful download+verify."""
    with contextlib.suppress(OSError), open(os.path.join(model_dir, REVISION_FILE), "w") as f:
        f.write(revision)


def verify_model(
    model_dir: str, hf_subdir: str, revision: str | None = None
) -> VerifyResult:
    """Verify that LFS files in model_dir match the hashes HF reports.

    Uses the commit SHA pinned in .hf_revision (written at download time)
    so that upstream updates to main don't reclassify previously-good
    local files as corrupt. Falls back to 'main' for models downloaded
    before revision pinning existed.

    If ``revision`` is provided it overrides the on-disk pin — used by
    verify_if_needed when it resolves the current main SHA for unpinned
    legacy installs.

    Non-LFS files (config.json, tokenizer.json) are not checked here —
    that's _classify_model_state's job (file presence) and the model
    loader's job (parse-time validation).
    """
    if revision is None:
        revision = _read_pinned_revision(model_dir)
    expected = fetch_expected_hashes(hf_subdir, revision=revision)
    mismatches: list[str] = []
    missing: list[str] = []
    for basename, expected_sha in expected.items():
        path = os.path.join(model_dir, basename)
        if not os.path.isfile(path):
            missing.append(basename)
            continue
        if sha256_file(path) != expected_sha:
            mismatches.append(basename)
    return VerifyResult(
        ok=not mismatches and not missing,
        mismatches=mismatches,
        missing=missing,
    )


def _has_pinned_revision(model_dir: str) -> bool:
    """Return True if the model directory has a .hf_revision file."""
    return os.path.isfile(os.path.join(model_dir, REVISION_FILE))


def verify_if_needed(model_id: str, model_dir: str, hf_subdir: str) -> None:
    """Verify the model unless already verified in this process.

    **Pinned installs** (have .hf_revision): hard-fail on mismatch — the
    pinned revision is the exact commit that was downloaded, so a mismatch
    is unambiguous corruption.  Writes .verify_failed and raises
    ModelCorruptError.

    **Unpinned legacy installs** (no .hf_revision): soft-fail on mismatch.
    These predate revision pinning and must be compared against current
    main, but a mismatch could be upstream version drift rather than
    corruption.  On success the install is auto-pinned for future checks;
    on mismatch a warning is logged but the pipeline is not blocked.

    On hash-fetch failure (VerifyError — network outage, transient HF API
    error), records the failure time in _verify_error_cache and returns
    without writing .verify_failed so the pipeline continues fail-open.
    """
    if model_id in _verified_this_process:
        return

    # Fail-open for recent hash-fetch failures so repeated pipeline starts
    # don't each stall for the full _FETCH_TIMEOUT on every model.
    error_ts = _verify_error_cache.get(model_id)
    if error_ts is not None and (time.monotonic() - error_ts) < _VERIFY_ERROR_TTL:
        return

    pinned = _has_pinned_revision(model_dir)

    if not pinned:
        _verify_unpinned(model_id, model_dir, hf_subdir)
        return

    # --- Pinned install: hard-fail on mismatch ---
    try:
        result = verify_model(model_dir, hf_subdir)
    except VerifyError:
        _verify_error_cache[model_id] = time.monotonic()
        return

    if not result.ok:
        try:
            with open(os.path.join(model_dir, VERIFY_FAILED_SENTINEL), "w") as f:
                f.write(f"{result.missing}|{result.mismatches}\n")
        except OSError:
            pass
        raise ModelCorruptError(model_id, result)
    _verified_this_process.add(model_id)
    _verify_error_cache.pop(model_id, None)
    sentinel = os.path.join(model_dir, VERIFY_FAILED_SENTINEL)
    if os.path.isfile(sentinel):
        with contextlib.suppress(OSError):
            os.unlink(sentinel)


def _verify_unpinned(model_id: str, model_dir: str, hf_subdir: str) -> None:
    """Verify a legacy install that has no .hf_revision pin.

    Fetches the current main SHA, verifies against it, and auto-pins on
    success.  On mismatch, logs a warning but does NOT write .verify_failed
    or raise — the mismatch is ambiguous (version drift vs corruption) and
    blocking the pipeline would be a migration regression.
    """
    try:
        latest_rev = fetch_latest_revision(ONNX_REPO)
    except VerifyError:
        _verify_error_cache[model_id] = time.monotonic()
        return

    try:
        result = verify_model(model_dir, hf_subdir, revision=latest_rev)
    except VerifyError:
        _verify_error_cache[model_id] = time.monotonic()
        return

    if result.ok:
        write_pinned_revision(model_dir, latest_rev)
        _verified_this_process.add(model_id)
        _verify_error_cache.pop(model_id, None)
        sentinel = os.path.join(model_dir, VERIFY_FAILED_SENTINEL)
        if os.path.isfile(sentinel):
            with contextlib.suppress(OSError):
                os.unlink(sentinel)
    else:
        log.warning(
            "Model %s (no revision pin) does not match current main: "
            "mismatches=%s, missing=%s. Use 'Verify all models' in "
            "Settings or click Repair to re-download.",
            model_id, result.mismatches, result.missing,
        )
        # Cache as checked so we don't re-warn on every pipeline start.
        _verified_this_process.add(model_id)


def clear_verified_cache(model_id: str) -> None:
    """Drop a model from the per-process verification cache so that the
    next call to verify_if_needed will re-hash. Called by download_model
    after a successful re-download."""
    _verified_this_process.discard(model_id)
    _verify_error_cache.pop(model_id, None)


def verify_all_models(progress_callback=None) -> dict[str, VerifyResult]:
    """Verify every installed known-model that's in the 'ok' state.

    Used by the "Verify all models" button in Settings. Skips:
      - Models without an hf_subdir (custom models have no HF source of truth).
      - Models in 'incomplete' or 'missing' state (nothing to verify if files
        aren't all present yet — the Settings UI already shows Repair).

    On mismatch, writes the .verify_failed sentinel so _classify_model_state
    will report 'incomplete' and the Settings UI surfaces the Repair button.

    Returns a dict mapping model_id -> VerifyResult for every model that
    was actually verified.
    """
    import models as _models  # avoid circular import at module load

    results: dict[str, VerifyResult] = {}
    for m in _models.get_models():
        if m.get("source") == "custom":
            continue
        if not m.get("hf_subdir"):
            continue
        if m.get("state") != "ok":
            continue
        model_id = m["id"]
        weights_path = m.get("weights_path")
        if not weights_path:
            continue
        if progress_callback:
            progress_callback(f"Verifying {model_id}...")
        try:
            result = verify_model(weights_path, m["hf_subdir"])
        except VerifyError as e:
            # Network or HTTP failure — can't verify, but don't mark the
            # model as corrupt. Record the result for the caller and skip
            # sentinel writing so a transient outage doesn't flip healthy
            # models to 'incomplete' and break pipelines.
            results[model_id] = VerifyResult(
                ok=False, missing=[f"<hash fetch failed: {e}>"]
            )
            continue
        results[model_id] = result
        if not result.ok:
            sentinel = os.path.join(weights_path, VERIFY_FAILED_SENTINEL)
            try:
                with open(sentinel, "w") as f:
                    f.write(
                        f"{result.missing}|{result.mismatches}\n"
                    )
            except OSError:
                pass
            _verified_this_process.discard(model_id)
        else:
            _verified_this_process.add(model_id)
            # Auto-pin legacy installs on first successful verification
            # so future checks use an immutable revision.
            if not _has_pinned_revision(weights_path):
                try:
                    rev = fetch_latest_revision(ONNX_REPO)
                    write_pinned_revision(weights_path, rev)
                except VerifyError:
                    pass
    return results
