"""Label selection and loading for a classify run.

Resolves which label files a run classifies against (explicit files, then the
workspace's active sets, then the global ones), loads and merges them, reuses
saved label embeddings, and records the label set's fingerprint row.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from classify_job.common import ClassificationCancelled, UnusableLabelsError, log
from labels import (
    get_active_labels,
    get_saved_labels,
    load_merged_labels,
    load_merged_labels_with_metas,
    read_label_file,
)
from services import workspace_config

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from classify_job.classify_types import LabelSetMeta
    from db import Database


def _any_present(label_sets: Iterable[Mapping[str, Any]]) -> bool:
    """Does this selection still name a file on disk?

    A selection that names only deleted files is not a selection to
    refuse — it is one to fall back from. Deleting a set in Settings now
    clears it from every workspace, but a selection stored before that
    fix (or removed outside the app) must not lock classification out
    with no checkbox left to untick.
    """
    return any(
        os.path.exists(ls.get("labels_file", "")) for ls in label_sets
    )


def _saved_label_metas(paths: Iterable[str]) -> list[LabelSetMeta]:
    """The saved metadata for each label-file path, or a bare stand-in."""
    saved = get_saved_labels()
    saved_by_file = {s["labels_file"]: s for s in saved}
    return [saved_by_file.get(p, {"labels_file": p}) for p in paths]


def _select_labels(
    labels_file: str | None, labels_files: list[str] | None, db: Database | None,
) -> tuple[list[str] | None, list[LabelSetMeta], bool]:
    """Load the label set the caller, the workspace or the globals chose.

    Returns ``(labels, label_metas, selected)``; ``selected`` says whether a
    list was actually chosen, which ``_load_labels`` uses to tell a chosen
    list that classifies nothing apart from no choice at all.
    """
    labels = None
    label_metas: list[dict] = []
    # Did the caller (or the workspace) actually choose a list? Not the
    # same question as "did anything come back": a chosen list that
    # classifies nothing must be refused, while choosing nothing is what
    # the Tree-of-Life fallback is for.
    selected = False

    # ``load_merged_labels_with_metas`` returns the metadata of the sets it
    # actually opened and read, filtered in the same pass — so a file
    # deleted between the caller composing ``requested`` and the loader
    # reading it drops out of both ``labels`` and ``label_metas`` together.
    # Without this atomic capture, ``describe_label_source`` could name and
    # count a list that contributed no classes and
    # ``_record_labels_fingerprint`` would write the same false provenance
    # into ``labels_fingerprints`` (the Settings DELETE endpoint can retire
    # a label file while this background job is loading).

    if labels_files and isinstance(labels_files, list):
        requested = _saved_label_metas(labels_files)
        labels, label_metas = load_merged_labels_with_metas(requested)
        selected = _any_present(requested)
        log.info("Using %d merged labels from %d sets", len(labels), len(label_metas))
    elif labels_file and os.path.exists(labels_file):
        # One file, same normalization as a list of them. A hand-authored
        # .txt used to be passed to the classifier as written, which made
        # ``Robin`` and ``robin`` two classes here and one through
        # ``labels_files`` — so readiness, the embedding matrix and
        # precompute (which all report the merged list) described a run
        # this branch would not perform, and warmed an embedding identity
        # it would not look up. There is one answer to "what does this
        # file classify as" and it is the merged one.
        labels, label_metas = load_merged_labels_with_metas(
            _saved_label_metas([labels_file]),
        )
        selected = True
        if not os.path.exists(labels_file):
            # Racing DELETE between exists() above and the loader's read.
            log.warning(
                "Label file vanished between exists() and read, skipping: %s",
                labels_file,
            )
            selected = False
        else:
            log.info("Using %d labels from file: %s", len(labels), labels_file)
    else:
        # Try workspace-scoped active labels first
        ws_labels = workspace_config.get_workspace_active_labels(db) if db else None
        if ws_labels is not None:
            requested = _saved_label_metas(ws_labels)
            labels, label_metas = load_merged_labels_with_metas(requested)
            selected = _any_present(requested)
            names = [s.get("name", "?") for s in label_metas]
            log.info(
                "Using %d merged labels from workspace active sets: %s",
                len(labels),
                ", ".join(names),
            )
        else:
            active_sets = get_active_labels()
            if active_sets:
                labels, label_metas = load_merged_labels_with_metas(list(active_sets))
                selected = _any_present(active_sets)
                names = [s.get("name", "?") for s in label_metas]
                log.info(
                    "Using %d merged labels from global active sets: %s",
                    len(labels),
                    ", ".join(names),
                )
    return labels, label_metas, selected


def _load_labels(
    model_type: str, model_str: str, labels_file: str | None, labels_files: list[str] | None,
    db: Database | None = None, model_dir: str | None = None,
) -> tuple[list[str] | None, bool, list[LabelSetMeta]]:
    """Resolve labels for classification.

    ``model_dir`` — the on-disk directory of the active model. When given,
    the label-free ToL fallback checks that the model's ToL artifacts are
    actually installed (see `models.tree_of_life_ready`) before returning
    `use_tol=True`. Without this, a bioclip-2.5 install whose optional
    ToL files were skipped at download time would route to
    `Classifier(labels=None)` and crash with FileNotFoundError.

    Returns:
        (labels, use_tol, label_metas) — ``labels`` is a list of species
        strings or None; ``use_tol`` is True if Tree of Life mode should be
        used; ``label_metas`` is the list of label-set metadata dicts that
        actually produced ``labels`` (empty on the Tree of Life or timm
        paths). Downstream callers pass ``label_metas`` verbatim to
        ``describe_label_source`` and ``_record_labels_fingerprint`` so the
        displayed name and the fingerprint sources cannot disagree with the
        labels — even when the on-disk sources shift under our feet between
        this call and the display.
    """
    if model_type == "timm":
        log.info("Classification config: model=%s (timm) — no labels needed", model_str)
        return None, False, []

    labels, label_metas, selected = _select_labels(labels_file, labels_files, db)

    if labels:
        log.info(
            "Classification config: model=%s, labels=%d from %s",
            model_str,
            len(labels),
            labels_file or "active labels",
        )
    else:
        log.info("Classification config: model=%s, no labels selected", model_str)

    # A selected list that classifies nothing is not "no labels selected":
    # falling through would silently classify the whole catalog against
    # Tree of Life (all species) for a user who asked for one region. Why
    # it is empty — every name shared between species, or the file itself
    # holding none — changes the remedy, not the verdict, so both raise
    # and the message names the cause.
    if selected and not labels:
        skipped = len(getattr(labels, "dropped_ambiguous", ()))
        if skipped:
            raise UnusableLabelsError(
                f"Every name in the selected species list ({skipped:,}) is "
                f"shared by more than one species, so none of them can "
                f"identify a taxon. Go to Settings → Labels and download the "
                f"list again to split them by scientific name."
            )
        raise UnusableLabelsError(
            "The selected species list contains no species. Go to Settings → "
            "Labels and download one, or untick it there to classify against "
            "Tree of Life instead."
        )

    from models import supports_tree_of_life, tree_of_life_ready

    use_tol = False
    if not labels:
        label_metas = []
        if tree_of_life_ready(model_str, model_dir):
            log.info(
                "No regional labels available — using Tree of Life classifier (all species)"
            )
            use_tol = True
        elif supports_tree_of_life(model_str):
            # ToL-capable model but its optional artifacts weren't
            # installed on this host (e.g. bioclip-2.5 whose HF upload
            # of tol_embeddings.npy hasn't landed, or the optional
            # download was skipped). Surface the missing files
            # explicitly instead of letting Classifier(labels=None)
            # crash later with a FileNotFoundError.
            raise RuntimeError(
                f"No labels available and Tree of Life files "
                f"(tol_embeddings.npy, tol_classes.json) are not installed "
                f"for {model_str}. Go to Settings → Models and click "
                f"Repair, or Settings → Labels and download a species "
                f"list for your region."
            )
        else:
            raise RuntimeError(
                f"No labels available and Tree of Life mode is not supported "
                f"for {model_str}. Go to Settings > Labels and download "
                f"a species list for your region."
            )

    return labels, use_tol, label_metas


def _reuse_saved_label_embeddings(
    db: Database | None, model_str: str, model_dir: str, labels: list[str] | None,
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    """Recover columns from older set caches using verified source lists.

    Older manifests omit the actual label order. Saved lists and run
    provenance are only candidates: their complete ordered cache key must
    match an existing payload before any column can be reused.
    """
    from classifier import _embedding_cache_service, _embedding_identity
    from embedding_cache import LabelEmbeddingCache, canonicalize_labels, identity_digest

    cache = _embedding_cache_service()
    if not os.path.isdir(cache.cache_dir):
        return
    candidates = {name for name in os.listdir(cache.cache_dir) if name.endswith(".npy")}
    if not candidates:
        return
    try:
        identity = _embedding_identity(labels, model_str, model_dir)
    except (OSError, ValueError):
        return  # normal model loading will report missing/invalid artifacts
    saved = get_saved_labels()
    by_file = {s["labels_file"]: s for s in saved if s.get("labels_file")}
    groups = [[path] for path in by_file]
    if db is not None:
        groups.extend(row["sources"] for row in db.model_runs.get_labels_fingerprints() if row.get("sources"))
    checked = set()
    for sources in groups:
        if cancel_check and cancel_check():
            raise ClassificationCancelled("classification cancelled")
        try:
            sets = [by_file.get(path, {"labels_file": path}) for path in sources]
            variants = [load_merged_labels(sets)]
            if len(sources) == 1:
                variants.append(read_label_file(sources[0]))
            for variant in variants:
                candidate = {**identity, "labels": canonicalize_labels(variant)}
                digest = identity_digest(candidate)
                if digest in checked or f"{digest}.npy" not in candidates:
                    continue
                checked.add(digest)
                value = cache._load(digest, len(candidate["labels"]))
                LabelEmbeddingCache(cache.cache_dir, candidate, value.shape[0]).seed(candidate["labels"], value)
                log.info("Reused %d species labels from an existing set cache", len(candidate["labels"]))
        except (EOFError, OSError, ValueError):
            log.debug("Could not reuse a previous species label set", exc_info=True)


def _record_labels_fingerprint(
    db: Database, fingerprint: str, labels: list[str] | None, sources: list[str] | None,
    full_fingerprint: str | None = None,
) -> None:
    """Populate the labels_fingerprints sidecar. Cosmetic — powers UX lookups."""
    display = ", ".join(os.path.basename(s) for s in (sources or [])) or None
    with db.transaction():
        db.model_runs.upsert_labels_fingerprint(
            fingerprint=fingerprint,
            display_name=display,
            sources=sources,
            label_count=len(labels or []),
            full_fingerprint=full_fingerprint,
        )
