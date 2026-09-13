"""timm-based classifier for species classification (iNaturalist 2021).

Uses ONNX Runtime for inference with pre-exported EVA-02 models.
Model files are stored in ~/.vireo/models/timm-inat21-eva02-l/.
"""

import json
import logging
import os
import threading
import time

import numpy as np
import onnx_runtime

log = logging.getLogger(__name__)

# Map model_str identifiers to local model directory names
_MODEL_DIR_MAP = {
    "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21": "timm-inat21-eva02-l",
}

_MODELS_ROOT = os.path.expanduser("~/.vireo/models")

# Bounded retry for the background label_descriptions.json heal.
#
# Keyed by (model_str, installation generation) rather than model_str
# alone: removing the model in Settings and re-downloading it produces a
# *different installation*, and the previous installation's verdict must
# not carry over. Without the generation, a "failed" would suppress the
# heal for a fresh download that could now succeed, and a "done" would
# suppress it for a fresh download whose optional-file fetch did not
# supply label_descriptions.json — leaking scientific names until the
# user restarts Vireo. Vireo repairs its own broken model state; "restart
# the app" is not a fix.
#
# ``_HEAL_MAX_ATTEMPTS`` caps *both* "failed" and "done" retries. A
# permanent "failed" block would strand the installation whenever the
# first probe happened to run while HF was unreachable: connectivity
# comes back, but no later classifier construction re-probes, and
# Settings Repair does not reliably recover this state either (verified
# required artifacts are skipped, so the installation generation does not
# change). Allowing up to _HEAL_MAX_ATTEMPTS probes per installation
# bounds the network cost of a persistent outage to a small burst rather
# than a probe per classify job, while still letting a transient outage
# self-recover. A "done" entry does not block either, because reaching
# this function at all means the file is missing or unusable again
# (callers only spawn when the read came back None) — the recorded
# "done" is contradicted by the disk, and the same attempt cap keeps a
# pathological "heal reports success but the file stays unusable" state
# from firing a network probe per classify job.
#
# Once that burst is spent, failures switch from "immediate retry" to
# "rate-limited retry" rather than to "never again". Three probes are a
# few seconds of a genuine outage; connectivity often returns minutes or
# hours later, and stopping there would still leak raw scientific names
# for the life of the installation with no in-app remedy — Repair leaves
# already-verified required artifacts alone, so the installation
# generation this state is keyed to never changes. So each further
# consecutive failure doubles the wait, capped at
# _HEAL_RETRY_MAX_SECONDS. The retry count past the burst is deliberately
# unbounded: bounded *rate*, not bounded *attempts*. At the cap this is
# one probe an hour on a background thread.
#
# "done" is not rate-limited this way — it is capped outright, because a
# heal that keeps reporting success while the file stays unusable is a
# bug, not a network condition, and waiting longer cannot fix it.
_HEAL_MAX_ATTEMPTS = 3
_HEAL_RETRY_BASE_SECONDS = 60.0
_HEAL_RETRY_MAX_SECONDS = 3600.0
_HEAL_LOCK = threading.Lock()
_HEAL_STATE: dict = {}  # heal key -> "in_flight" | "done" | "failed"
_HEAL_ATTEMPTS: dict = {}  # heal key -> int
_HEAL_FAILURES: dict = {}  # heal key -> consecutive failure count
_HEAL_RETRY_AT: dict = {}  # heal key -> monotonic deadline for the next try
_HEAL_THREADS: dict = {}  # model_str -> threading.Thread (kept for tests)


def reset_label_desc_heal_state(model_str=None):
    """Forget recorded heal verdicts so the next construction retries.

    Called by ``models.download_model``, i.e. when the user clicks Repair
    in Settings → Models. Repair means "try this again": a spent attempt
    budget or an open backoff window is invisible to the user, so
    honouring the button they just pressed means clearing both. Without
    this, a verdict recorded during an outage survives Repair entirely —
    Repair skips required artifacts that already verify, so the
    installation generation the verdict is keyed to never changes and
    the user has no in-app way back to correct common names.

    ``model_str=None`` clears every model. In-flight entries are left
    alone: their worker is about to record its own verdict, and dropping
    the marker would let a duplicate worker start alongside it.
    """
    with _HEAL_LOCK:
        keys = [
            k for k in list(_HEAL_STATE)
            if (model_str is None or k[0] == model_str)
            and _HEAL_STATE.get(k) != "in_flight"
        ]
        for k in keys:
            _HEAL_STATE.pop(k, None)
            _HEAL_ATTEMPTS.pop(k, None)
            _HEAL_FAILURES.pop(k, None)
            _HEAL_RETRY_AT.pop(k, None)


def _installation_generation(model_dir):
    """Identity of the model installation currently in ``model_dir``.

    ``model.onnx`` is the artifact every installation writes, so its
    identity changes when the user removes the model and downloads it
    again. Deriving the generation from disk rather than from a
    ``remove_model()`` callback means no deletion path can bypass it —
    wiping ~/.vireo/models by hand resets the heal state just the same
    as the Settings button does.

    ``st_ctime_ns`` and ``st_ino`` are folded in alongside size/mtime
    because ``download_model`` publishes weights via ``shutil.copy2``,
    which preserves the source file's mtime — a re-download from an
    unchanged HF cache blob can therefore land bytes with an identical
    ``(size, mtime_ns)`` tuple. ``st_ctime_ns`` (metadata-change time on
    POSIX, creation time on Windows) is set by the OS when the copy
    creates the destination and cannot be spoofed by ``copy2``;
    ``st_ino`` is a fresh inode for any newly created file. Either
    would suffice; using both keeps the marker robust across
    filesystems that report one of them as a constant.
    """
    try:
        st = os.stat(os.path.join(model_dir, "model.onnx"))
    except OSError:
        return None
    return (
        st.st_size,
        int(st.st_mtime_ns),
        int(st.st_ctime_ns),
        st.st_ino,
    )


def rearm_pending_label_desc_heal(model_str, model_dir):
    """Re-attempt a background heal from paths that never build a classifier.

    The bounded-retry state machine in ``_spawn_label_desc_heal`` normally
    fires from ``TimmClassifier.__init__`` (first construction) and from
    ``TimmClassifier.notify_reuse`` (later cache-hit acquires via
    ``acquire_cached_classifier``). Both paths require the classifier to
    be acquired. ``classify_job._all_photos_cache_satisfied``'s cached-
    only shortcut returns without any classifier acquisition, so on a
    long-lived process that only ever runs cached-only jobs, a heal that
    failed during a transient outage would stay ``failed`` for the rest
    of the process's life and connectivity coming back would never
    trigger a retry. This function lets such paths exercise the same
    state machine directly, without loading the ONNX session.

    No-ops when ``label_descriptions.json`` already contains a usable
    mapping (nothing to heal) or when the state machine judges the
    installation to be in-flight, done, or inside a backoff window.
    Returns the spawned Thread when a heal was started, else None.
    """
    label_desc_path = os.path.join(model_dir, "label_descriptions.json")
    try:
        import models as _models_mod
        descs = _models_mod.load_label_descriptions(label_desc_path)
    except Exception:
        descs = None
    if descs is not None:
        return None
    return _spawn_label_desc_heal(model_dir, model_str)


def _spawn_label_desc_heal(model_dir, model_str):
    """Best-effort async heal of a missing label_descriptions.json.

    Runs the network-touching self-heal in a daemon thread so
    TimmClassifier construction stays offline-only after the initial
    model download — matching the docs' promise (help.json: "Only for
    downloading AI models on first use"). Existing classifier instances
    use the taxonomy fallback until the file is on disk; the next
    TimmClassifier construction picks it up.

    Bounded, then rate-limited, per installation generation (see
    _HEAL_STATE). Returns the spawned Thread, or None when a prior
    attempt for this installation already settled the question or is
    still inside its retry backoff window.
    """
    key = (model_str, _installation_generation(model_dir))
    with _HEAL_LOCK:
        state = _HEAL_STATE.get(key)
        if state == "in_flight":
            return None
        attempts = _HEAL_ATTEMPTS.get(key, 0)
        if state == "done" and attempts >= _HEAL_MAX_ATTEMPTS:
            return None
        if state == "failed" and attempts >= _HEAL_MAX_ATTEMPTS:
            # Immediate-retry burst spent; fall back to the backoff
            # schedule rather than giving up on the installation.
            retry_at = _HEAL_RETRY_AT.get(key)
            if retry_at is not None and time.monotonic() < retry_at:
                return None
        _HEAL_STATE[key] = "in_flight"
        _HEAL_ATTEMPTS[key] = attempts + 1

    def _worker():
        try:
            import models as _models_mod
            ok = _models_mod.ensure_timm_label_descriptions(
                model_dir, model_str,
            )
        except Exception as e:
            log.info(
                "label_descriptions heal failed for %s: %s", model_str, e,
            )
            ok = False
        with _HEAL_LOCK:
            _HEAL_STATE[key] = "done" if ok else "failed"
            if ok:
                _HEAL_FAILURES.pop(key, None)
                _HEAL_RETRY_AT.pop(key, None)
            else:
                failures = _HEAL_FAILURES.get(key, 0) + 1
                _HEAL_FAILURES[key] = failures
                delay = min(
                    _HEAL_RETRY_BASE_SECONDS
                    * (2 ** max(0, failures - _HEAL_MAX_ATTEMPTS)),
                    _HEAL_RETRY_MAX_SECONDS,
                )
                _HEAL_RETRY_AT[key] = time.monotonic() + delay
                log.info(
                    "label_descriptions heal for %s failed (%d in a row); "
                    "next attempt no sooner than %.0fs from now",
                    model_str, failures, delay,
                )

    thread = threading.Thread(
        target=_worker,
        name=f"label-desc-heal:{model_str}",
        daemon=True,
    )
    with _HEAL_LOCK:
        _HEAL_THREADS[model_str] = thread
    thread.start()
    return thread


class TimmClassifier:
    """Wraps an ONNX model for species classification.

    Uses a supervised model with a fixed class set (10K iNat21 species).
    No label files or text embeddings needed.

    Args:
        model_str: timm model identifier (e.g. "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21")
        taxonomy: optional Taxonomy instance for enriching predictions with hierarchy
    """

    def __init__(self, model_str, taxonomy=None):
        # Resolve model directory from model_str
        dir_name = _MODEL_DIR_MAP.get(model_str)
        if dir_name is None:
            raise ValueError(
                f"Unknown timm model: {model_str}. "
                f"Known models: {list(_MODEL_DIR_MAP.keys())}"
            )

        model_dir = os.path.join(_MODELS_ROOT, dir_name)
        model_path = os.path.join(model_dir, "model.onnx")
        class_names_path = os.path.join(model_dir, "class_names.json")
        label_desc_path = os.path.join(model_dir, "label_descriptions.json")
        config_path = os.path.join(model_dir, "config.json")

        # Validate required files exist
        for path, desc in [
            (model_path, "ONNX model"),
            (class_names_path, "class names"),
            (config_path, "preprocessing config"),
        ]:
            if not os.path.isfile(path):
                raise FileNotFoundError(
                    f"{desc} not found at {path}. "
                    "Download the model from the Models page in Settings."
                )

        import models as _models_mod

        # Installs that predate the label_descriptions.json export have no
        # scientific→common mapping and leak raw binomials ("Bubulcus
        # ibis") into predictions whenever the taxonomy fallback misses.
        # Read the file exactly once, here, *before* spawning any repair:
        # this instance must never observe a file the heal is publishing
        # underneath it, so there is no torn-read window no matter how
        # the writer behaves.
        #
        # A present-but-unparseable file counts as missing. Existence
        # alone must not suppress the repair, or one truncated write
        # would leak scientific names forever (and, if we let json.load
        # raise here, brick every classify job).
        descs, label_desc_signature = _models_mod.read_label_descriptions(
            label_desc_path,
        )

        # Record what this instance actually consumed for
        # ``label_descriptions.json`` so ``acquire_cached_classifier``
        # keys the entry by that state, not by whatever the disk shows
        # after construction. The heal spawned below can publish the
        # file mid-construction (network probes can take a full HF
        # timeout, but they can also finish before the ONNX session
        # opens); without this snapshot the post-load fingerprint would
        # match the healed disk state, rekey this pre-heal instance
        # under the healed fingerprint, and every later classify job
        # would reuse the stale classifier — its ``_common_names``
        # stays empty until eviction, so raw scientific names would
        # leak indefinitely under regular use.
        # The signature comes from the descriptor that produced ``descs``
        # (``read_label_descriptions``), not from a stat taken afterwards:
        # a Repair or heal that republishes the file between the read and
        # a path stat would otherwise stamp this instance with the
        # replacement's identity — the same "keyed by artifacts it never
        # read" hazard, just a narrower window.
        label_desc_state = label_desc_signature if descs is not None else None
        self.optional_files_snapshot = {
            "label_descriptions.json": label_desc_state,
        }

        # Portable content identity of the same consumed mapping.
        # ``optional_files_snapshot`` is a local (size, mtime_ns) stat
        # tuple — fine for keying this process's classifier cache, but
        # meaningless on another machine. Runs published to the portable
        # computation cache are stamped with *this* value instead (see
        # ``computation_cache.with_consumed_label_descriptions``), so a
        # run recorded before the background heal supplies the file is
        # never mistaken for a post-heal run whose species names differ.
        from computation_cache import label_descriptions_identity
        self.label_descriptions_identity = label_descriptions_identity(descs)

        # Remember the model coordinates so cache reuse (see
        # ``notify_reuse`` below) can re-arm the background heal without
        # having to pass them in from every caller.
        self._model_dir = model_dir
        self._model_str = model_str

        # Spawn the heal off the startup path — the network probes can
        # take a full HF timeout when offline, and classifier construction
        # runs per classify job. This instance proceeds with the taxonomy
        # fallback; the next TimmClassifier picks up the healed file.
        if descs is None:
            _spawn_label_desc_heal(model_dir, model_str)

        # Load ONNX session with self-heal on corruption: if the bytes
        # are truncated / partial, delete them and re-download once
        # rather than bricking the classify pipeline.

        redownload = _models_mod.build_self_heal_redownloader(model_dir)
        log.info("Loading timm ONNX model: %s", model_path)
        self._session = onnx_runtime.create_session_with_self_heal(
            model_path, redownload=redownload,
        )
        self._input_name = self._session.get_inputs()[0].name

        # Load class names (list of scientific names, index = class id)
        with open(class_names_path) as f:
            self._class_names = json.load(f)

        # Load preprocessing config (input_size, mean, std)
        with open(config_path) as f:
            preproc = json.load(f)
        self._input_size = tuple(preproc["input_size"][-2:])  # (H, W) from [C, H, W]
        self._mean = preproc["mean"]
        self._std = preproc["std"]

        # Build scientific -> common name mapping from label_descriptions
        # Format: {"Sturnus vulgaris": "European Starling, Bird"}
        self._common_names = {}
        for sci_name, desc in (descs or {}).items():
            # desc format: "Common Name, Category" -- take part before last comma
            parts = desc.rsplit(", ", 1)
            common = parts[0] if len(parts) > 1 else desc
            # If common name equals scientific name, it has no common name
            if common.lower() != sci_name.lower():
                self._common_names[sci_name.lower()] = common

        # Also use taxonomy.json for any names not in label_descriptions
        self._taxonomy = taxonomy

        log.info(
            "TimmClassifier ready: %d classes, %d common name mappings",
            len(self._class_names),
            len(self._common_names),
        )

    @property
    def label_space_size(self):
        """How many built-in classes this model can return, or None if unknown.

        timm models carry a fixed classifier head, so species lists never apply
        to them; the Jobs page says so and names the size (see
        ``classify_job.describe_label_source``).
        """
        classes = getattr(self, "_class_names", None)
        return len(classes) if classes is not None else None

    def notify_reuse(self):
        """Re-arm the ``label_descriptions.json`` heal on cache reuse.

        ``__init__`` spawns the heal once, but the process-wide
        ``ModelCache`` reuses this instance across subsequent classify
        jobs. When the first heal happened to fail (e.g. HF was
        unreachable during that probe), later classify jobs would then
        never re-enter ``__init__``, so ``_spawn_label_desc_heal``'s
        bounded-retry state machine — which allows up to
        ``_HEAL_MAX_ATTEMPTS`` probes per installation once
        connectivity returns — was unreachable. That left this instance
        stuck under the ``label_descriptions.json = None`` fingerprint
        indefinitely: an outage bracketing one probe leaked raw
        scientific names for the rest of the process's life.

        ``acquire_cached_classifier`` calls this on every acquire so
        the bounded retry actually fires from the acquisition path,
        not only from ``__init__``. Concurrent duplicate calls are
        already deduped by ``_HEAL_LOCK`` (an ``in_flight`` entry is
        a no-op), so calling it on a fresh construction — where
        ``__init__`` already spawned — is cheap and correct.
        """
        if (
            self.optional_files_snapshot.get("label_descriptions.json")
            is not None
        ):
            return
        _spawn_label_desc_heal(self._model_dir, self._model_str)

    def _resolve_common_name(self, scientific_name):
        """Map a scientific name to a common name.

        Priority: taxonomy common name > taxonomy-resolved current
        scientific name (via synonym resolution) > label_descriptions
        bundled with the model > input scientific name as-is.

        Taxonomy is consulted first so that a healed
        ``label_descriptions.json`` shipping the iNat21-vintage common
        name (e.g. ``"Bubulcus ibis" -> "Cattle Egret, Bird"``) does not
        override the taxonomy's current preferred name
        (``"Western Cattle-Egret"``). Persisting the stale form leaves
        ``/api/predictions/compare`` unable to canonicalize the species
        — the taxonomy does not index ``"Cattle Egret"`` and the
        scientific-name synonym map cannot resolve a common name — so
        the comparison falls back to text identity and reports a false
        disagreement with a model that emitted the current name.

        The taxonomy-resolved *scientific* name is preferred over the
        model-era label mapping when synonym resolution rewrote the
        input (``taxon.scientific_name != scientific_name``): on legacy
        or offline installs lacking ``label_descriptions.json``, the
        old-name fall-through was persisting raw obsolete binomials
        (``"Bubulcus ibis"``) with matching ``auto:`` tags even though
        the resolved taxon already carried the accepted current name
        (``"Ardea ibis"``). A taxonomy hit that echoes the input
        (direct match with no common name) still falls through to the
        label mapping, so classes the taxonomy tracks only structurally
        keep their common-name fallback.
        """
        if self._taxonomy:
            taxon = self._taxonomy.lookup(scientific_name)
            if taxon:
                common = taxon.get("common_name")
                if common:
                    return common
                canonical_sci = taxon.get("scientific_name")
                if canonical_sci and canonical_sci != scientific_name:
                    return canonical_sci

        key = scientific_name.lower()
        if key in self._common_names:
            return self._common_names[key]

        return scientific_name

    def _build_results(self, probs, threshold):
        """Build sorted prediction dicts from a probability array."""
        indexed = sorted(enumerate(probs), key=lambda x: x[1], reverse=True)
        results = []
        for idx, score in indexed:
            score = float(score)
            if score < threshold:
                break  # sorted, so all remaining are below threshold

            scientific_name = self._class_names[idx]
            common_name = self._resolve_common_name(scientific_name)

            # Build taxonomy hierarchy
            taxonomy = {"scientific_name": scientific_name}
            if self._taxonomy:
                hierarchy = self._taxonomy.get_hierarchy(scientific_name)
                if hierarchy:
                    taxonomy = hierarchy
                elif common_name != scientific_name:
                    hierarchy = self._taxonomy.get_hierarchy(common_name)
                    if hierarchy:
                        taxonomy = hierarchy
                    else:
                        taxonomy["scientific_name"] = scientific_name

            results.append(
                {
                    "species": common_name,
                    "score": score,
                    "auto_tag": f"auto:{common_name}",
                    "confidence_tag": f"auto:confidence:{score:.2f}",
                    "taxonomy": taxonomy,
                }
            )
        return results

    def _preprocess(self, image):
        """Preprocess a PIL Image for ONNX inference.

        Args:
            image: PIL Image (will be converted to RGB)

        Returns:
            numpy float32 array of shape (1, 3, H, W)
        """
        return onnx_runtime.preprocess_image(
            image,
            size=self._input_size,
            mean=self._mean,
            std=self._std,
        )

    def classify(self, image, threshold=0.1):
        """Classify an image and return predictions above threshold.

        Args:
            image: file path (str) or PIL Image
            threshold: minimum confidence to include (default 0.1 -- lower than
                BioCLIP's 0.4 since probability is spread across 10K classes)

        Returns:
            list of dicts with species, score, auto_tag, confidence_tag, taxonomy
        """
        from PIL import Image as PILImage
        from pipeline_locks import acquire_inference_resources

        if isinstance(image, str | os.PathLike):
            with PILImage.open(image) as img:
                input_arr = self._preprocess(img)
        else:
            input_arr = self._preprocess(image)

        # Provider-aware coordination is scoped tightly to the forward pass.
        # Preprocessing above runs without a lease; CPU sessions take their
        # configured permits and cpu_ml, while accelerator sessions take the
        # per-batch GPU semaphore.
        with acquire_inference_resources(self._session):
            output = self._session.run(None, {self._input_name: input_arr})
        logits = output[0]  # shape: (1, num_classes)
        probs = onnx_runtime.softmax(logits, axis=-1).flatten()

        return self._build_results(probs, threshold)

    def classify_batch(self, images, threshold=0.1):
        """Classify multiple PIL images.

        Args:
            images: list of PIL Images
            threshold: minimum confidence to include

        Returns:
            list of prediction lists (one per image)
        """
        if not images:
            return []

        from pipeline_locks import acquire_inference_resources

        input_arr = np.concatenate(
            [self._preprocess(img) for img in images],
            axis=0,
        )
        # Same provider-aware inference lease as classify(), scoped tightly
        # around the forward pass. Preprocessing and result building remain
        # outside it.
        with acquire_inference_resources(self._session):
            output = self._session.run(None, {self._input_name: input_arr})
        logits = output[0]
        probs_batch = onnx_runtime.softmax(logits, axis=-1)

        results = []
        for probs in probs_batch:
            results.append(self._build_results(probs, threshold))
        return results
