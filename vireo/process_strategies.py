"""Seed data and flag helpers for user-editable "saved processes".

A saved process is a named snapshot of the process page's stage toggles,
stored in the ``saved_processes`` table (see ``db.py``). It is pure data: a
dict of PipelineParams skip-flags plus ``miss_enabled`` (misses are
config-gated, not param-gated) and ``review_mode`` (species-only review path).
The API layer resolves a process **id** into these flags server-side so the
import page, the process page, and import->process chaining all share one
vocabulary.

This module no longer owns the strategy *list* — that now lives in the DB and
is user-editable. What remains here is:

  * ``_BASE`` — the "everything on" baseline (also the column defaults for a
    brand-new process).
  * ``SEED_PROCESSES`` — the four processes inserted once on first run. They
    keep the semantics of the former hardcoded ``STRATEGIES`` dict. The default
    user-facing seed is "Identify birds": detect/classify birds and write
    lightweight species-review results, but skip the experimental
    quality/culling stack (SAM2 masks, DINO embeddings, eye focus, quality
    triage). The full quality pipeline remains available as the "Full" seed.
  * ``LEGACY_STRATEGY_NAMES`` — old strategy-name -> seed-name map, used by the
    one-shot ``default_strategy`` -> ``default_process_id`` migration.

Stage gate map (from run_pipeline_job in pipeline_job.py):

    stage          gated by
    -----          --------
    scan/ingest    skipped whenever collection_id is set (skip_scan)
    thumbnails     runs on collection photos; per-photo cache skip
    previews       runs on collection photos; per-photo cache skip
    model_loader   params.skip_classify
    detect         params.skip_classify (+ abort / no collection / no models)
    classify       params.skip_classify (+ same)
    extract_masks  params.skip_extract_masks
    eye_keypoints  params.skip_eye_keypoints OR params.skip_extract_masks
    regroup        params.skip_regroup
    misses         params.skip_regroup OR params.skip_classify OR
                   regroup-failed; then config ``miss_enabled`` inside the
                   stage body (per-run override wins)

Because detect/model_loader are gated by ``skip_classify``, "Quick look"
needs no separate ``skip_detect`` flag -- skipping classify already skips the
whole detect/classify chain, and (with ``skip_regroup``) misses too.

``review_mode`` distinguishes the identify preset's species-only review path
from a generic ``skip_regroup=True`` run (Custom on the Process page, or an API
caller that just wants to refresh classifications without touching grouping).
Without it, ``pipeline_job``'s regroup_stage would treat every classify-only run
as species-review and overwrite ``pipeline_results_ws*.json`` with all-REVIEW
output, silently downgrading the cache for callers who never asked for it.
"""

# The six fields a saved process carries, in addition to its name.
FLAG_FIELDS = (
    "skip_classify",
    "skip_extract_masks",
    "skip_eye_keypoints",
    "skip_regroup",
    "miss_enabled",
    "review_mode",
)

_BASE = {
    "skip_classify": False,
    "skip_extract_masks": False,
    "skip_eye_keypoints": False,
    "skip_regroup": False,
    "miss_enabled": True,
    "review_mode": None,
}

# Inserted once into ``saved_processes`` on first run (db_meta-guarded so a
# user who deletes all processes never gets them back). Order here is the
# ``sort_order`` used to render the process/import pickers.
SEED_PROCESSES = [
    {
        "name": "Identify birds",
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "skip_regroup": True,
        "miss_enabled": False,
        "review_mode": "species",
    },
    {"name": "Full"},
    {
        "name": "Cull-ready",
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "miss_enabled": False,
    },
    {
        "name": "Quick look",
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "skip_regroup": True,
        "miss_enabled": False,
    },
]

# The seed that fresh installs point the app-wide + per-workspace default at.
DEFAULT_SEED_NAME = "Identify birds"

# Old hardcoded strategy name -> seed name. Used only by the one-shot
# ``pipeline.default_strategy`` -> ``pipeline.default_process_id`` migration;
# unknown/removed names map to null (import only).
LEGACY_STRATEGY_NAMES = {
    "identify": "Identify birds",
    "full": "Full",
    "cull_ready": "Cull-ready",
    "quick_look": "Quick look",
}


def seed_flags(seed):
    """Expand a ``SEED_PROCESSES`` entry into a full flags dict over ``_BASE``.

    Ignores non-flag keys (e.g. ``name``) so a seed literal can carry display
    metadata alongside its overrides.
    """
    return {**_BASE, **{k: v for k, v in seed.items() if k in FLAG_FIELDS}}
