"""Named processing-stage presets ("process strategies").

A strategy is pure data: a dict of PipelineParams skip-flag overrides plus
``miss_enabled`` (misses are config-gated, not param-gated). The API layer
expands a strategy name server-side so the import page, the process page,
and import→process chaining all share one vocabulary.

The default user-facing preset is ``identify``: detect/classify birds and
write lightweight review results, but skip the experimental quality/culling
stack (SAM2 masks, DINO embeddings, eye focus, and quality triage). The full
quality pipeline remains available for Advanced Mode.

The "no processing at all" choice is deliberately NOT a strategy here: it
is a decision at the import→process boundary, represented as a null
workspace default (``pipeline.default_strategy``) and a short-circuit in
the import-completion chaining hook. ``/api/jobs/pipeline`` rejects both
``strategy: null`` and the literal string ``"none"``.

Stage gate map (recon 2026-07-04, run_pipeline_job in pipeline_job.py):

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
                   stage body (per-run override added alongside this module)
    archive        params.local_processing only (import runs; removed in PR 4)

Because detect/model_loader are gated by ``skip_classify``, quick_look
needs no separate ``skip_detect`` flag — skipping classify already skips
the whole detect/classify chain, and (with ``skip_regroup``) misses too.
"""

_BASE = {
    "skip_classify": False,
    "skip_extract_masks": False,
    "skip_eye_keypoints": False,
    "skip_regroup": False,
    "miss_enabled": True,
    # Distinguishes the identify preset's species-only review path from
    # a generic ``skip_regroup=True`` run (Advanced/Custom on the Process
    # page, or an API caller that just wants to refresh classifications
    # without touching grouping). Without this, ``pipeline_job``'s
    # regroup_stage would treat every classify-only run as species-review
    # and overwrite ``pipeline_results_ws*.json`` with all-REVIEW output,
    # silently downgrading the cache for callers who never asked for it.
    "review_mode": None,
}

STRATEGIES = {
    "identify": {
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "skip_regroup": True,
        "miss_enabled": False,
        "review_mode": "species",
    },
    "full": {},
    "cull_ready": {
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "miss_enabled": False,
    },
    "quick_look": {
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "skip_regroup": True,
        "miss_enabled": False,
    },
}


def resolve_strategy(name):
    """Expand a strategy name into stage-flag overrides. Raises ValueError.

    Non-string inputs (list, dict, int, bool, None) also raise ``ValueError``
    rather than ``TypeError``: callers that surface ValueError as 400 (both
    ``/api/jobs/pipeline`` and ``api_update_workspace``'s workspace-override
    validation) then handle malformed JSON bodies uniformly instead of one
    path 400ing and the other escaping as a 500.
    """
    if not isinstance(name, str):
        raise ValueError(
            f"strategy must be a string, got {type(name).__name__}"
        )
    if name not in STRATEGIES:
        raise ValueError(
            f"unknown strategy: {name!r} (expected one of {sorted(STRATEGIES)})"
        )
    return {**_BASE, **STRATEGIES[name]}
