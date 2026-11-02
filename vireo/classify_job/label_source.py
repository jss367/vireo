"""Naming the label source a classify run used, for the Jobs page."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Protocol

from classify_job.cache_coverage import _cache_run_filter
from classify_job.common import log
from labels import get_active_labels, get_saved_labels
from services import workspace_config

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from classify_job.classify_types import LabelSetMeta
    from db import Database


class LabelSourceParams(Protocol):
    """The label-file fields a classify request carries.

    ``ClassifyParams`` (the classify job) and ``PipelineParams`` (the
    pipeline's model loader) both name their label sources this way.
    """

    @property
    def labels_file(self) -> str | None: ...

    @property
    def labels_files(self) -> list[str] | None: ...


def _resolve_label_set_metas(params: LabelSourceParams, db: Database | None) -> list[LabelSetMeta]:
    """Return the label-set metadata dicts _load_labels would merge, in order.

    Mirrors the lookup order in _load_labels — explicit files first, then the
    workspace's active sets, then the global ones — but resolves only the
    sources, not the species names. Callers use it for the fingerprint row's
    source paths and for naming the sets in the UI.

    Prefer passing the ``label_metas`` returned by ``_load_labels`` directly
    to consumers so the two cannot disagree. This helper is the fallback for
    callers that have not yet plumbed those through, and it MUST reproduce
    ``_load_labels``'s fallback branches — otherwise a missing
    ``params.labels_file`` names the deleted path on the Jobs page and in
    ``labels_fingerprints`` while the actual classifier ran against the
    workspace or global lists.
    """
    saved_by_file = {}
    for meta in get_saved_labels() or []:
        path = meta.get("labels_file")
        if path:
            saved_by_file[path] = meta

    def _metas(paths: Iterable[str]) -> list[LabelSetMeta]:
        return [saved_by_file.get(p, {"labels_file": p}) for p in paths]

    # ``os.path.exists`` mirrors ``_load_labels``, which reads through
    # ``load_merged_labels_with_metas`` — a configured path that has since
    # been deleted contributes nothing to ``labels`` there, so naming the
    # stale path here would misreport the label source. The plural branch
    # keeps every path that still exists; if all requested files are gone,
    # the caller sees the same empty metadata ``_load_labels`` would produce
    # (Tree of Life falls in one step below).
    if params.labels_files and isinstance(params.labels_files, list):
        return _metas([p for p in params.labels_files if os.path.exists(p)])
    if params.labels_file and os.path.exists(params.labels_file):
        return _metas([params.labels_file])
    ws_labels = workspace_config.get_workspace_active_labels(db) if db else None
    if ws_labels is not None:
        return _metas(list(ws_labels))
    return list(get_active_labels() or [])


def _sources_from_metas(label_metas: Iterable[Mapping[str, Any]] | None) -> list[str]:
    """Extract ``labels_file`` source paths from a list of metadata dicts."""
    return [
        path
        for meta in (label_metas or [])
        if (path := meta.get("labels_file"))
    ]


def _resolve_label_sources(params: LabelSourceParams, db: Database | None) -> list[str]:
    """Return the source file paths that build the active label set."""
    return _sources_from_metas(_resolve_label_set_metas(params, db))


def _label_set_name(meta: Mapping[str, Any]) -> str:
    """Display name for one label set — its saved name, else its filename."""
    name = (meta.get("name") or "").strip()
    if name:
        return name
    return os.path.basename(meta.get("labels_file") or "") or "unnamed list"


def _describe_cached_label_source(
    db: Database, photo_ids: Sequence[int], *, classifier_model: str | None = None,
    labels_fingerprint: str | None = None, detector_confidence: float = 0.0,
) -> str | None:
    """Name the label space of a reused-cache classify step when no peek loaded.

    Used only when ``_finalize_cached_only`` fires with ``peek_succeeded=False``
    — the label peek raised (missing files on a fresh install with an
    imported cache) so we have no live ``labels``/``use_tol``/``metas`` to
    hand ``describe_label_source``. The identity gate in
    ``_all_photos_cache_satisfied`` guarantees one canonical
    ``(classifier_model, labels_fingerprint)`` pair covers every reused row,
    so we can look up the corresponding ``labels_fingerprints`` sidecar and
    at least name the sources that originally produced these predictions.
    Returns ``None`` if we cannot recover any source — the caller then
    leaves the step unlabeled rather than inventing a description.

    ``detector_confidence`` — mirror the workspace threshold
    ``_all_photos_cache_satisfied`` used, so the fingerprint lookup only
    considers the same eligible detections. Without this, a photo also
    carrying runs on sub-threshold detections (or torn runs without any
    stored output) could contribute a fingerprint that is not the one
    actually reused, so the persisted ``label_source`` would name a list
    that did not produce these results.
    """
    if not photo_ids:
        return None
    try:
        found_fp = labels_fingerprint
        if found_fp is None:
            found_fp = _cached_labels_fingerprint(
                db, photo_ids, classifier_model, detector_confidence,
            )
        if not found_fp:
            return "Reused cached predictions (label source unavailable)"

        sidecar = None
        for entry in db.model_runs.get_labels_fingerprints() or []:
            if entry.get("fingerprint") == found_fp:
                sidecar = entry
                break
        if not sidecar:
            return "Reused cached predictions (label source unavailable)"
        return _cached_label_sidecar_text(sidecar)
    except Exception:  # broad: this only describes the summary's label source; a failure leaves it unnamed
        log.debug(
            "Could not describe cached label source", exc_info=True,
        )
        return None


def _cached_labels_fingerprint(
    db: Database, photo_ids: Sequence[int], classifier_model: str | None,
    detector_confidence: float,
) -> str | None:
    """The labels_fingerprint of the runs the cache-coverage check counted.

    Reads the same rows as ``_all_photos_cache_satisfied``'s identity gate
    (``covering_classifier_identities`` under ``_cache_run_filter``), so a
    run the coverage check accepts, including a completed run that matched
    nothing and left only a ``classifier_match_scores`` row, names its list
    here, and a run it skips (a RAW-recipe run, a torn run without output)
    cannot. The cached-only finalize only gets here when the label peek
    failed, so there is no full label fingerprint and the filter carries no
    runtime constraint, as in the coverage check. Returns ``None`` when no
    covering run carries a fingerprint.
    """
    from db import _chunks

    run_filter = _cache_run_filter(
        db, photo_ids, classifier_model, None, None, None, "no-tax",
    )
    for chunk in _chunks(photo_ids):
        for identity_row in db.model_runs.covering_classifier_identities(
            chunk, run_filter, detector_confidence,
        ):
            if identity_row["fp"]:
                return identity_row["fp"]
    return None


def _cached_label_sidecar_text(sidecar: Mapping[str, Any]) -> str:
    """Describe a reused label set from its ``labels_fingerprints`` row."""
    sources = sidecar.get("sources") or []
    names = [os.path.basename(s) for s in sources if s]
    count = sidecar.get("label_count") or 0
    count_text = f"{count:,} species" if count else "cached label set"
    if not names:
        display = (sidecar.get("display_name") or "").strip()
        if display:
            return (
                f"Reused cached predictions from "
                f"{count_text}: {display}"
            )
        return "Reused cached predictions (label source unavailable)"
    if len(names) > 3:
        shown = ", ".join(names[:3]) + f" and {len(names) - 3} more"
    else:
        shown = ", ".join(names)
    if len(names) == 1:
        return f"Reused cached predictions from {count_text}: {shown}"
    return (
        f"Reused cached predictions from {count_text} across "
        f"{len(names)} lists: {shown}"
    )


def describe_label_source(
    params: LabelSourceParams, db: Database | None, *, labels: list[str] | None, use_tol: bool,
    model_type: str, class_count: int | None = None, label_metas: list[LabelSetMeta] | None = None,
) -> str:
    """One line naming the label space a classify run compares photos against.

    The Jobs page names the model on each classify row, but the same model
    against a 1,300-species regional list and against the full Tree of Life
    are different classifiers as far as the results are concerned. The row has
    to say which list is in play, not just which weights are loaded.

    ``class_count`` is the constructed classifier's own label-space size, used
    for the two cases where the selected lists don't describe it: Tree of Life
    and a timm model's fixed head. Always returns a line — a run that reaches
    here has a label space, because _load_labels raises when it finds none.

    ``label_metas`` — pass the metadata list ``_load_labels`` returned to name
    the exact sets that produced ``labels``. Without it, this falls back to
    ``_resolve_label_set_metas`` which re-resolves from ``params`` and ``db``
    and can disagree with ``labels`` if the on-disk sources shifted (a
    configured file was deleted, or the workspace's active list was edited)
    between when the classifier loaded and when the row is rendered.
    """
    if model_type == "timm":
        if class_count:
            return (
                f"Model's own {class_count:,} built-in classes "
                "— species lists don't apply"
            )
        return "Model's own built-in classes — species lists don't apply"
    if use_tol or not labels:
        if class_count:
            return (
                f"Tree of Life: all {class_count:,} species "
                "(no species list active)"
            )
        return "Tree of Life: every species the model knows (no species list active)"

    if label_metas is None:
        metas = _resolve_label_set_metas(params, db)
    else:
        metas = label_metas
    names = [_label_set_name(m) for m in metas]
    count = f"{len(labels):,} species"
    if not names:
        return count
    if len(names) > 3:
        shown = ", ".join(names[:3]) + f" and {len(names) - 3} more"
    else:
        shown = ", ".join(names)
    if len(names) == 1:
        return f"{count} from {shown}"
    return f"{count} from {len(names)} lists: {shown}"
