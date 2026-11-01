"""Audited pause policies for every production job type.

Unknown jobs must explicitly supply ``pausable``; adding a launcher without
considering pause is an error, rather than a silently missing control.
"""

PAUSABLE_JOBS = frozenset({
    "pipeline", "scan", "thumbnails", "duplicate-scan", "verify-hashes",
    "capture-time", "verify-models", "sharpness", "cull", "regroup",
    "precompute-embeddings", "fetch-labels", "metadata-repair", "previews",
    "ingest", "move-photos", "offline-cache", "release-local-originals",
    "prepare-full-resolution", "classify", "develop", "extract-masks",
    "inat-export", "export", "publish-site", "export-site", "import",
    "import-full", "import-in-place", "staging-verify", "card-cleanup-scan",
    "card-cleanup-verify", "card-cleanup-delete", "send-to-nas", "move-folder",
})

DOWNLOAD_REASON = (
    "Pause is unavailable while this downloader or installer is running."
)
LOCAL_TRANSITION_REASON = (
    "Pause is unavailable while switching between local and remote files."
)
INTERNAL_REASON = "This automatic maintenance scan does not support pausing."
SNAPSHOT_REASON = (
    "Pause is unavailable while importing a saved photo list because the list "
    "is reserved until the import finishes."
)

UNPAUSABLE_JOBS = {
    "download-model": DOWNLOAD_REASON,
    "download-taxonomy": DOWNLOAD_REASON,
    "download-darktable": DOWNLOAD_REASON,
    "download-megadetector": DOWNLOAD_REASON,
    "sync": "Pause is unavailable while metadata changes are being written to sidecars.",
    "batch-delete": "Pause is unavailable while deleting photos and reconciling the library.",
    "work-locally-stage": LOCAL_TRANSITION_REASON,
    "work-locally-sync": LOCAL_TRANSITION_REASON,
    "work-locally-discard": LOCAL_TRANSITION_REASON,
    "work-locally-folder-stage": LOCAL_TRANSITION_REASON,
    "work-locally-folder-sync": LOCAL_TRANSITION_REASON,
    "work-locally-folder-discard": LOCAL_TRANSITION_REASON,
    "new_images_walk": INTERNAL_REASON,
    "missing_originals_scan": INTERNAL_REASON,
    "thumb_path_backfill": "Thumbnail rechecks support cancellation, but not pausing.",
    "preview-cache-maintenance": INTERNAL_REASON,
    "label-list-species-ids": INTERNAL_REASON,
    "exif_search_backfill": INTERNAL_REASON,
    "working_copy_backfill": INTERNAL_REASON,
    "retained-cache-maintenance": "Pause is unavailable while cache files and their records are reconciled.",
    "retained-cache-cleanup": "Pause is unavailable while cache files and their records are reconciled.",
    "panorama": "Pause is unavailable while the panorama tool is running.",
    "analyze-subjects": "Pause is unavailable during this subject analysis request.",
    "analysis-retry": "Pause is unavailable during this analysis retry.",
}


def resolve_pause_policy(job_type, pausable):
    """Return (supports_pause, unavailable_reason), rejecting omissions."""
    reason = UNPAUSABLE_JOBS.get(job_type)
    if pausable is None:
        if job_type in PAUSABLE_JOBS:
            return True, None
        if reason:
            return False, reason
        raise ValueError(f"Job {job_type!r} must declare a pause policy")
    if pausable:
        return True, None
    if job_type == "import-in-place":
        reason = SNAPSHOT_REASON
    if job_type.startswith("download-"):
        reason = DOWNLOAD_REASON
    return False, reason or "This operation does not support pausing."
