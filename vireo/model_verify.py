"""SHA256 verification of ONNX model files against HuggingFace LFS oids.

HuggingFace stores LFS file hashes as SHA256 in the `lfs.oid` field of
the tree API. This module fetches those expected hashes and compares
them to locally-computed SHA256s, replacing the 10 MB size-floor
heuristic in models.py that only caught the narrowest truncation case.
"""

import contextlib
import hashlib
import json
import os
import time
import urllib.request
from dataclasses import dataclass, field

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

# Short-lived failure cache: model_id -> timestamp of last VerifyError from
# verify_if_needed. Entries expire after _VERIFY_FAILURE_TTL seconds so that
# a transient network outage doesn't permanently suppress re-verification
# within the same process (e.g. after network is restored), but does prevent
# repeated 30-second hash-fetch stalls on every pipeline start.
_verify_failure_timestamps: dict[str, float] = {}
_VERIFY_FAILURE_TTL = 300  # 5 minutes


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


def verify_model(model_dir: str, hf_subdir: str) -> VerifyResult:
    """Verify that LFS files in model_dir match the hashes HF reports.

    Uses the commit SHA pinned in .hf_revision (written at download time)
    so that upstream updates to main don't reclassify previously-good
    local files as corrupt. Falls back to 'main' for models downloaded
    before revision pinning existed.

    Non-LFS files (config.json, tokenizer.json) are not checked here —
    that's _classify_model_state's job (file presence) and the model
    loader's job (parse-time validation).
    """
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


def verify_if_needed(model_id: str, model_dir: str, hf_subdir: str) -> None:
    """Verify the model unless already verified in this process.

    On success, adds model_id to the per-process cache and deletes any
    stale .verify_failed sentinel. On failure, writes the sentinel and
    raises ModelCorruptError without populating the cache (so a Repair
    flow will see the failure state again if verification is retried
    without a fresh download).

    If hash-fetching raises VerifyError (network/API failure), the error
    is re-raised so callers can fail-open, and the failure timestamp is
    recorded. Subsequent calls within _VERIFY_FAILURE_TTL seconds skip the
    expensive hash-fetch entirely and return immediately, avoiding repeated
    30-second stalls on every pipeline start while the network is down.
    """
    if model_id in _verified_this_process:
        return
    last_failure = _verify_failure_timestamps.get(model_id)
    if last_failure is not None and (time.monotonic() - last_failure) < _VERIFY_FAILURE_TTL:
        return
    try:
        result = verify_model(model_dir, hf_subdir)
    except VerifyError:
        _verify_failure_timestamps[model_id] = time.monotonic()
        raise
    if not result.ok:
        try:
            with open(os.path.join(model_dir, VERIFY_FAILED_SENTINEL), "w") as f:
                f.write(f"{result.missing}|{result.mismatches}\n")
        except OSError:
            pass
        raise ModelCorruptError(model_id, result)
    _verified_this_process.add(model_id)
    _verify_failure_timestamps.pop(model_id, None)
    sentinel = os.path.join(model_dir, VERIFY_FAILED_SENTINEL)
    if os.path.isfile(sentinel):
        with contextlib.suppress(OSError):
            os.unlink(sentinel)


def clear_verified_cache(model_id: str) -> None:
    """Drop a model from the per-process verification cache so that the
    next call to verify_if_needed will re-hash. Called by download_model
    after a successful re-download."""
    _verified_this_process.discard(model_id)
    _verify_failure_timestamps.pop(model_id, None)


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
    return results
