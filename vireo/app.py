"""Flask web app for the Vireo photo browser.

Usage:
    python vireo/app.py --db ~/.vireo/vireo.db [--port 8080]
"""

import argparse
import concurrent.futures
import contextlib
import copy
import json
import logging
import logging.handlers
import math
import os
import posixpath
import re
import secrets
import stat
import subprocess
import sys
import time
import uuid
import webbrowser
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import id_conflicts
import places
import remote_setup
from db import (
    Database,
    IncompatibleDatabaseError,
    MissingPhotosCancelled,
    text_search_match,
)
from flask import (
    Flask,
    g,
    jsonify,
    request,
)
from jobs import JobRunner, LogBroadcaster
from keyword_normalization import keyword_match_key, normalize_keyword_display
from photo_payload import (
    attach_edit_recipes,
    attach_nested_edit_recipes,
    attach_species_representatives,
    render_key_for_recipe,
)
from pipeline_results import auto_detach_burst_for_species
from preview_cache import (
    reconcile_preview_cache,
)
from proc import no_window_kwargs
from schema import ensure_schema
from services import prediction_decisions
from services.local_folder import (
    local_root_for_folder,
    local_root_under_folder,
    workspace_ids_for_folder_tree,
)
from services.local_workspace import (
    folder_has_local_workspace,
    stage_boundary_lock,
)
from services.pending_changes import (
    queue_keyword_add,
    queue_keyword_remove,
)
from services.pipeline_launch import PipelineChain
from services.visual_scope import (
    VISUAL_COLLECTION_MSG,
    VisualScope,
    inject_active_visual_model,
)
from volume_reachability import (  # noqa: F401  (re-exported for tests)
    _NETWORK_PROBE_LOCK,
    _NETWORK_PROBES,
)
from volume_reachability import (
    network_root_reachable as _network_root_reachable,
)
from web.audit import create_audit_blueprint
from web.background_jobs import make_background_job
from web.batch import create_batch_blueprint
from web.browse import create_browse_blueprint
from web.caches import create_caches_blueprint
from web.capture_time import create_capture_time_blueprint
from web.card_cleanup import create_card_cleanup_blueprint
from web.collections import create_collections_blueprint
from web.dashboard import create_dashboard_blueprint
from web.duplicates import create_duplicates_blueprint
from web.editing import create_editing_blueprint
from web.export import create_export_blueprint
from web.folders import create_folders_blueprint
from web.highlights import create_highlights_blueprint
from web.history import create_history_blueprint
from web.imports import create_imports_blueprint
from web.inat import InatTokenGeneration, create_inat_blueprint
from web.job_launchers import create_job_launchers_blueprint
from web.jobs import create_jobs_blueprint
from web.keywords import create_keywords_blueprint
from web.life_list import create_life_list_blueprint
from web.local_folder import LOCAL_FOLDER_JOB_TYPES, create_local_folder_blueprint
from web.local_workspace import LOCAL_WORKSPACE_JOB_TYPES, create_local_workspace_blueprint
from web.location_edits import LocationErrors
from web.locations import create_locations_blueprint
from web.media import create_media_blueprint
from web.misses import create_misses_blueprint
from web.models import create_models_blueprint
from web.moves import create_moves_blueprint
from web.pages import create_pages_blueprint
from web.photo_edit_recipes import create_photo_edit_recipes_blueprint
from web.photo_labels import create_photo_labels_blueprint
from web.photo_location_keywords import create_photo_location_keywords_blueprint
from web.photo_review import create_photo_review_blueprint
from web.photos import create_photos_blueprint
from web.pipeline import create_pipeline_blueprint
from web.remote_setup import create_remote_setup_blueprint
from web.request_args import (
    MAX_SELECTION_PHOTOS,
    coerce_collection_id,
    parse_selection_photo_ids,
    reject_visual_collection,
    request_rules_arg,
    request_visual_arg,
)
from web.settings import create_settings_blueprint
from web.species import create_species_blueprint
from web.storage import create_storage_blueprint
from web.sync import create_sync_blueprint
from web.system import create_system_blueprint
from web.workspaces import create_workspace_blueprint
from working_copy_cache import (
    evict_if_over_quota as evict_working_copy_cache_if_over_quota,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# How many planned capture-date folders a date-organized move job snapshots
# into its config for the jobs panel. The panel lists these and reports the
# real total separately, so the route stays readable (and the job row small)
# even when a source folder spans hundreds of dates.
MOVE_DATE_DEST_PREVIEW_LIMIT = 8


# Stable ordering and labels for the palette + nav rendering.
# The `id` is the nav-id used in `tabs`; `href` is the canonical
# route. Labels match what the navbar showed before the unification.
ALL_PAGES = [
    {"id": "import",          "label": "Import",          "href": "/import",
     "keywords": "import add photos card copy ingest new"},
    {"id": "pipeline",        "label": "Process",         "href": "/pipeline",
     "keywords": "process classify detect group stages"},
    {"id": "jobs",            "label": "Jobs",            "href": "/jobs"},
    {"id": "pipeline_review", "label": "Process Review",  "href": "/pipeline/review"},
    {"id": "pipeline_rapid_review", "label": "Rapid Review", "href": "/pipeline/rapid-review"},
    {"id": "review",          "label": "Review",          "href": "/review"},
    {"id": "cull",            "label": "Cull",            "href": "/cull"},
    {"id": "misses",          "label": "Misses",          "href": "/misses"},
    {"id": "highlights",      "label": "Highlights",      "href": "/highlights"},
    {"id": "life_list",       "label": "Life List",       "href": "/life-list"},
    {"id": "browse",          "label": "Browse",          "href": "/browse"},
    {"id": "edit",            "label": "Edit",            "href": "/edit"},
    {"id": "map",             "label": "Map",             "href": "/map"},
    {"id": "location_review", "label": "Review Photo Locations", "href": "/locations/review",
     "keywords": "location review map coordinates collections gps places"},
    {"id": "dashboard",       "label": "Dashboard",       "href": "/dashboard"},
    {"id": "storage",         "label": "Storage",         "href": "/storage"},
    {"id": "audit",           "label": "Audit",           "href": "/audit"},
    {"id": "card_cleanup",    "label": "Card cleanup",    "href": "/card-cleanup",
     "keywords": "card cleanup free space delete verified memory card format sd"},
    {"id": "move",            "label": "Move",            "href": "/move"},
    {"id": "id_conflicts",    "label": "ID Conflicts",    "href": "/id-conflicts",
     "keywords": "compare conflict prediction model disagreement species keyword classify review"},
    {"id": "settings",        "label": "Settings",        "href": "/settings"},
    {"id": "workspace",       "label": "Workspace",       "href": "/workspace"},
    {"id": "lightroom",       "label": "Lightroom",       "href": "/lightroom"},
    {"id": "shortcuts",       "label": "Shortcuts",       "href": "/shortcuts"},
    {"id": "keywords",        "label": "Keywords",        "href": "/keywords"},
    {"id": "duplicates",      "label": "Duplicates",      "href": "/duplicates"},
    {"id": "logs",            "label": "Logs",            "href": "/logs"},
]

# File logging is attached only when the server actually starts (see
# main() / _setup_file_logging). Importing this module — e.g. from pytest
# fixtures — must NOT touch ~/.vireo/vireo.log, or test tracebacks end up
# in the user's real log file.
def _setup_file_logging(log_dir=None):
    root = logging.getLogger()
    if any(getattr(h, "_vireo_file_handler", False) for h in root.handlers):
        return
    if log_dir is None:
        log_dir = os.path.expanduser("~/.vireo")
    os.makedirs(log_dir, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "vireo.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    handler._vireo_file_handler = True
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)


# Suppress noisy werkzeug request logs for polling endpoints
class _QuietRequestFilter(logging.Filter):
    """Filter out repetitive GET requests from werkzeug logs."""

    _quiet_paths = {"/api/jobs", "/api/logs/stream", "/api/logs/recent", "/thumbnails/"}

    def filter(self, record):
        msg = record.getMessage()
        if "200" in msg or "304" in msg:
            for path in self._quiet_paths:
                if f"GET {path}" in msg:
                    return False
        return True


logging.getLogger("werkzeug").addFilter(_QuietRequestFilter())


def _rank01(value, values):
    valid = [v for v in values if v is not None]
    if value is None:
        return 0.0
    if len(valid) <= 1:
        return 0.5 if valid else 0.0
    below = sum(1 for v in valid if v < value)
    equal = sum(1 for v in valid if v == value)
    return (below + 0.5 * equal) / len(valid)


def _highlight_exposure_score(photo):
    clip_high = photo.get("subject_clip_high") or 0.0
    clip_low = photo.get("subject_clip_low") or 0.0
    y_median = photo.get("subject_y_median")
    if y_median is None:
        return 0.5
    clip_penalty = math.exp(-6.0 * clip_high - 3.0 * clip_low)
    lum_penalty = math.exp(-abs((y_median / 255.0) - 0.45) / 0.30)
    return max(0.0, min(1.0, clip_penalty * lum_penalty))


def _highlight_area_score(subject_size):
    if subject_size is None or subject_size <= 0:
        return 0.0
    return min(1.0, math.sqrt(subject_size) * 2.0)


def _highlight_score_bucket(photos, picked_first=False):
    """Attach highlight scores and compact reason labels to one species bucket.

    picked_first promotes flagged photos above unflagged ones regardless of
    score — desired on the Highlights page. It is off by default so callers
    such as Life List can calculate the ranked baseline before applying their
    own Representative / Pick curation tiers.
    """
    subject_values = [p.get("subject_tenengrad") for p in photos]
    eye_values = [p.get("eye_tenengrad") for p in photos if p.get("eye_tenengrad") is not None]
    bg_values = [p.get("bg_tenengrad") for p in photos]
    bg_sep_values = [p.get("bg_separation") for p in photos if p.get("bg_separation") is not None]
    noise_values = [p.get("noise_estimate") for p in photos if p.get("noise_estimate") is not None]
    max_bg_sep = max(bg_sep_values) if bg_sep_values else None

    for p in photos:
        if p.get("eye_tenengrad") is not None and eye_values:
            focus = _rank01(p.get("eye_tenengrad"), eye_values)
            focus_label = "eye focus"
        else:
            subject_t = p.get("subject_tenengrad")
            rank = _rank01(subject_t, subject_values)
            if subject_t is not None and p.get("bg_tenengrad") is not None:
                ratio = math.log((subject_t + 1e-8) / (p.get("bg_tenengrad") + 1e-8))
                bg_term = 1.0 / (1.0 + math.exp(-ratio))
                focus = 0.70 * rank + 0.30 * bg_term
            elif subject_t is not None:
                focus = rank
            else:
                focus = p.get("quality_score") if p.get("quality_score") is not None else 0.0
            focus_label = "focus"

        exposure = _highlight_exposure_score(p)
        crop = p.get("crop_complete")
        crop_score = crop if crop is not None else 0.5
        if max_bg_sep and p.get("bg_separation") is not None:
            bg_sep = 1.0 - min(1.0, p.get("bg_separation") / max_bg_sep)
        else:
            bg_sep = 0.5
        composition = 0.55 * crop_score + 0.45 * bg_sep
        area = _highlight_area_score(p.get("subject_size"))
        if p.get("noise_estimate") is not None and noise_values:
            noise = 1.0 - _rank01(p.get("noise_estimate"), noise_values)
        else:
            noise = 1.0 - _rank01(p.get("bg_tenengrad"), bg_values) if bg_values else 0.5

        rich_available = any(
            p.get(k) is not None
            for k in (
                "subject_tenengrad", "eye_tenengrad", "crop_complete",
                "subject_clip_high", "subject_y_median", "noise_estimate",
            )
        )
        if rich_available:
            score = (
                0.42 * focus
                + 0.18 * exposure
                + 0.16 * composition
                + 0.10 * area
                + 0.08 * noise
                + 0.06 * (p.get("quality_score") or 0.0)
            )
        else:
            score = p.get("quality_score") if p.get("quality_score") is not None else 0.0

        rating = p.get("rating") or 0
        if rating >= 4:
            score += 0.04 + 0.02 * (rating - 4)
        elif rating == 3:
            score += 0.015
        # Baseline BEFORE the pick bonus, so the client can recompute
        # highlight_score on a lightbox pick/unpick via clamp(base + bonus)
        # instead of subtracting the bonus from the already-clamped value —
        # which loses precision when the raw score exceeded 1.0 pre-clamp
        # (e.g. base 0.97 → cached 1.0 → subtract 0.08 → 0.92, but the
        # correct unpicked value is 0.97).
        base_score_pre_pick = score
        if p.get("flag") == "flagged":
            score += 0.08

        reasons = []
        if p.get("flag") == "flagged":
            reasons.append("picked")
        if rating:
            reasons.append(f"{rating} star")
        if focus >= 0.72:
            reasons.append(f"strong {focus_label}")
        elif focus < 0.35 and rich_available:
            reasons.append("soft subject")
        if exposure >= 0.70:
            reasons.append("good exposure")
        elif p.get("subject_clip_high") is not None and p.get("subject_clip_high") > 0.30:
            reasons.append("highlight clipping")
        if crop is not None:
            if crop >= 0.90:
                reasons.append("clean crop")
            elif crop < 0.60:
                reasons.append("clipped subject")
        if area >= 0.50:
            reasons.append("large subject")
        if not reasons:
            reasons.append("legacy quality")

        p["highlight_score"] = round(max(0.0, min(1.0, score)), 4)
        p["highlight_base_score"] = round(
            max(0.0, min(1.0, base_score_pre_pick)), 4
        )
        p["score_parts"] = {
            "focus": round(focus, 3),
            "exposure": round(exposure, 3),
            "composition": round(composition, 3),
            "subject": round(area, 3),
            "noise": round(noise, 3),
        }
        p["reasons"] = reasons[:3]

    if picked_first:
        # Highlights page ordering: three contiguous regions —
        #   1. picks (flagged): analyzed first (by score desc), then
        #      unanalyzed picks in capture order;
        #   2. analyzed non-picks: by highlight_score desc;
        #   3. unanalyzed non-picks: capture order (earliest first).
        # The analyzed-before-unanalyzed tier separates the groups so the
        # score term only reorders analyzed rows and the timestamp term only
        # reorders unanalyzed rows — they never compete. Timestamps are ISO
        # text, so lexicographic order is chronological; missing timestamps
        # sort to the end of their group. This is an ascending sort (no
        # reverse), negating the score to rank analyzed rows high-first.
        def _bucket_sort_key(p):
            analyzed = p.get("quality_score") is not None
            ts = p.get("timestamp")
            return (
                0 if p.get("flag") == "flagged" else 1,
                0 if analyzed else 1,
                -(p.get("highlight_score") or 0) if analyzed else 0.0,
                (ts is None, ts),
                p.get("id", 0),
            )

        photos.sort(key=_bucket_sort_key)
    else:
        photos.sort(
            key=lambda p: (
                p.get("highlight_score") or 0,
                p.get("predicted_confidence") or 0,
                p.get("quality_score") or 0,
                p.get("rating") or 0,
                -p.get("id", 0),
            ),
            reverse=True,
        )


def _apply_preferred_photo(photos, preferred_photo_id, marker_key):
    """Move a valid preferred photo to the front of an already-ranked list."""
    for p in photos:
        p[marker_key] = False
    if preferred_photo_id is None:
        return False
    for idx, photo in enumerate(photos):
        if photo.get("id") == preferred_photo_id:
            photo[marker_key] = True
            if idx:
                photos.insert(0, photos.pop(idx))
            return True
    return False


def _sort_photos_with_representatives_first(photos, representative_order):
    """Promote representative photos while preserving ranked order otherwise."""
    ranked_position = {photo["id"]: idx for idx, photo in enumerate(photos)}
    photos.sort(key=lambda photo: (
        0 if photo["id"] in representative_order else 1,
        representative_order.get(photo["id"], 0),
        ranked_position.get(photo["id"], 0),
    ))


def _bucket_best_score(photos):
    """Return the highest highlight_score in a bucket, or None if empty.

    Used to rank buckets on the Highlights page. Reads the max across all
    photos so a pick-first (or user-preferred) reorder at photos[0] can't
    demote a species by anchoring the bucket score to a lower-scored photo.
    """
    scores = [p.get("highlight_score") for p in photos if p.get("highlight_score") is not None]
    return max(scores) if scores else None


def _bucket_unanalyzed_count(photos):
    """Length of the trailing "Not yet analyzed" tail of a bucket.

    Counts only the contiguous run of unscored non-pick photos at the END of
    ``photos``. This is what the divider labels, so callers can trust that a
    non-zero value marks a real tail below the divider.

    Only the trailing run is counted (not every unscored non-pick in the
    bucket) so that curated ordering — where
    :func:`_apply_ordered_highlights` or :func:`_apply_highlight_preferences`
    can promote an unscored, non-flagged photo to the front as a species
    highlight or representative — doesn't inflate the count with photos that
    now live above the divider. Unscored picks are excluded because they
    stay grouped with the other picks at the front regardless.
    """
    if not photos:
        return 0
    tail = 0
    for p in reversed(photos):
        if p.get("quality_score") is None and p.get("flag") != "flagged":
            tail += 1
        else:
            break
    return tail


def _apply_highlight_preferences(db, buckets):
    preferences = db.get_species_representative_lists(eligible_only=True)
    for bucket in buckets:
        representative_ids = preferences.get(bucket["species"]) or []
        representative_set = set(representative_ids)
        representative_order = {
            photo_id: idx for idx, photo_id in enumerate(representative_ids)
        }
        preferred_id = representative_ids[0] if representative_ids else None
        applied = False
        for photo in bucket.get("photos") or []:
            is_rep = photo.get("id") in representative_set
            photo["is_species_representative"] = is_rep
            applied = applied or is_rep
        # Promote representatives to the front so cross-workspace reps
        # (visible via species_representatives but without a workspace-scoped
        # species_highlights rank) become the bucket's primary photo.
        # _apply_ordered_highlights runs before this and only sorts when a
        # visible photo has a workspace highlight rank, so without this pass
        # a rep chosen in another workspace stays buried in score order.
        # Matches the Life List behavior which also promotes reps over
        # workspace highlights.
        if applied:
            _sort_photos_with_representatives_first(
                bucket["photos"], representative_order
            )
        top = bucket["photos"][0] if bucket["photos"] else {}
        bucket["preferred_photo_id"] = preferred_id
        # Species-level state stays true even when the representative photo is
        # outside the current folder or search result.
        bucket["has_species_representative"] = preferred_id is not None
        bucket["has_preferred_photo"] = applied
        bucket["best_quality"] = top.get("quality_score")
        # Rank by the highest-scored photo in the bucket, not photos[0].
        # A picked (or manually preferred) photo may sit at photos[0] with a
        # lower highlight_score; using its score for bucket ranking would
        # demote the whole species below buckets with worse actual best
        # photos.
        bucket["best_score"] = _bucket_best_score(bucket["photos"])
        bucket["best_timestamp"] = top.get("timestamp")
        # Run last so curated promotion (an unscored rep pushed to the front)
        # doesn't leave a stale tail count that anchors the divider above
        # analyzed content.
        bucket["unanalyzed_count"] = _bucket_unanalyzed_count(bucket.get("photos"))


def _apply_ordered_highlights(db, buckets):
    highlights = db.get_species_highlights(eligible_only=True)
    for bucket in buckets:
        ranks = highlights.get(bucket["species"], {})
        # Species-level state stays true even when every selected highlight is
        # outside the current folder or search result.
        bucket["has_highlight_selection"] = bool(ranks)
        photos = bucket.get("photos") or []
        matched = False
        for p in photos:
            rank = ranks.get(p.get("id"))
            p["is_highlighted"] = rank is not None
            p["highlight_rank"] = rank
            if rank is not None:
                matched = True
        # Only re-sort when at least one visible photo actually maps to a
        # stored highlight rank. `ranks` is workspace-scoped, so a folder
        # or search view of the same species can be truthy here while none
        # of its photos are highlights — re-sorting then discards the
        # picked-first order that _highlight_score_bucket already applied.
        if matched:
            bucket["photos"].sort(
                key=lambda p: (
                    0 if p.get("is_highlighted") else 1,
                    p.get("highlight_rank") if p.get("highlight_rank") is not None else 10**9,
                    -(p.get("highlight_score") or 0),
                    -(p.get("predicted_confidence") or 0),
                    -(p.get("quality_score") or 0),
                    -(p.get("rating") or 0),
                    p.get("id", 0),
                )
            )
        best = bucket["photos"][0] if bucket.get("photos") else {}
        bucket["highlight_count"] = sum(
            1 for p in bucket.get("photos") or [] if p.get("is_highlighted")
        )
        # unanalyzed_count is assigned in _apply_highlight_preferences instead:
        # that runs after this pass and can still reorder photos, so anchoring
        # the tail count here would be stale under curated ordering.
        bucket["best_quality"] = best.get("quality_score")
        bucket["best_score"] = best.get("highlight_score")
        bucket["best_timestamp"] = best.get("timestamp")


def _photo_highlight_entries(db, photo_id):
    """Return highlight-eligible species entries for one visible photo.

    Restricts both the candidate scan and the species-highlights lookup to
    the requested photo so the photo-detail endpoint doesn't rebuild every
    workspace bucket for each call (browse detail, lightbox, batch actions).
    """
    candidates = db.get_highlights_candidates(
        None, min_quality=0.0, photo_id=photo_id
    )
    buckets, _unidentified = _collect_highlight_buckets(
        candidates, confidence_threshold=0.0,
        canonicalize_species=_species_canonicalizer(db),
    )
    entries = []
    for bucket in buckets:
        species = bucket.get("species")
        if not species:
            continue
        ranks = db.get_species_highlights(species).get(species, {})
        for photo in bucket.get("photos") or []:
            if photo.get("id") != photo_id:
                continue
            rank = ranks.get(photo_id)
            entries.append({
                "species": species,
                "is_highlighted": rank is not None,
                "highlight_rank": rank,
                "is_confirmed": bool(photo.get("has_accepted_species")),
            })
            break
    return entries


def _highlight_confidence_label(confidence, is_accepted):
    if is_accepted:
        return "confirmed"
    if confidence is None:
        return "unknown"
    if confidence >= 0.85:
        return "likely"
    return "candidate"


def _normalize_highlight_confirmation_filter(value):
    value = (value or "all").strip().lower()
    if value not in {"all", "confirmed", "unconfirmed"}:
        return "all"
    return value


def _normalize_highlight_presence_filter(value):
    value = (value or "all").strip().lower()
    if value not in {"all", "yes", "no"}:
        return "all"
    return value


def _species_canonicalizer(db):
    """Memoized wrapper around db.resolve_species_display_name.

    Bucket collection resolves the same handful of species strings for
    thousands of candidate rows; caching avoids a keyword lookup per row.
    """
    cache = {}

    def canonicalize(name):
        if name not in cache:
            cache[name] = db.resolve_species_display_name(name)
        return cache[name]

    return canonicalize


def _collect_highlight_buckets(
    candidates,
    confidence_threshold,
    confirmation_filter="all",
    canonicalize_species=None,
):
    confirmation_filter = _normalize_highlight_confirmation_filter(
        confirmation_filter
    )
    bucket_map = {}
    unidentified_photos = []

    for row in candidates:
        r = dict(row)
        accepted = r.get("species")
        if confirmation_filter == "confirmed" and accepted is None:
            continue
        if confirmation_filter == "unconfirmed" and accepted is not None:
            continue
        predicted_conf = r.get("predicted_confidence")
        if accepted:
            species = accepted
            if canonicalize_species is not None:
                species = canonicalize_species(species) or species
            is_accepted = True
        elif (
            r.get("predicted_species")
            and predicted_conf is not None
            and predicted_conf >= confidence_threshold
        ):
            # Prediction labels are external vocabulary (classifier label
            # files) with their own casing. Bucket by the spelling
            # add_keyword would store so a predicted `Common Waxbill` and
            # photos already accepted as `Common waxbill` land in ONE
            # bucket, and so curation written from this bucket's label keys
            # on the string the keyword row and eligibility queries use.
            species = r["predicted_species"]
            if canonicalize_species is not None:
                species = canonicalize_species(species) or species
            is_accepted = False
        else:
            species = None
            is_accepted = False

        photo = {
            "id": r["id"],
            "filename": r["filename"],
            "timestamp": r.get("timestamp"),
            "folder_name": r.get("folder_name"),
            "folder_path": r.get("folder_path"),
            "keyword_names": r.get("keyword_names") or "",
            "rating": r.get("rating") or 0,
            "flag": r.get("flag") or "none",
            "quality_score": r.get("quality_score"),
            "is_analyzed": r.get("quality_score") is not None,
            "subject_sharpness": r.get("subject_sharpness"),
            "subject_size": r.get("subject_size"),
            "sharpness": r.get("sharpness"),
            "mask_path": r.get("mask_path"),
            "subject_tenengrad": r.get("subject_tenengrad"),
            "bg_tenengrad": r.get("bg_tenengrad"),
            "crop_complete": r.get("crop_complete"),
            "bg_separation": r.get("bg_separation"),
            "subject_clip_high": r.get("subject_clip_high"),
            "subject_clip_low": r.get("subject_clip_low"),
            "subject_y_median": r.get("subject_y_median"),
            "noise_estimate": r.get("noise_estimate"),
            "eye_tenengrad": r.get("eye_tenengrad"),
            "species": accepted,
            "prediction_id": r.get("prediction_id"),
            "predicted_species": r.get("predicted_species"),
            "predicted_confidence": predicted_conf,
            "has_accepted_species": accepted is not None,
            "is_unidentified": species is None,
            "is_confirmable_prediction": (
                accepted is None
                and species is not None
                and r.get("prediction_id") is not None
            ),
        }

        if species is None:
            unidentified_photos.append(photo)
        else:
            entry = bucket_map.setdefault(
                species, {"is_accepted": True, "photos": []}
            )
            entry["is_accepted"] = entry["is_accepted"] and is_accepted
            entry["photos"].append(photo)

    buckets = []
    for species, entry in bucket_map.items():
        photos = entry["photos"]
        _highlight_score_bucket(photos, picked_first=True)
        confidences = [
            p["predicted_confidence"]
            for p in photos
            if p.get("predicted_confidence") is not None
        ]
        avg_confidence = (
            round(sum(confidences) / len(confidences), 4)
            if confidences else None
        )
        top = photos[0] if photos else {}
        buckets.append({
            "species": species,
            "is_accepted": entry["is_accepted"],
            "certainty": _highlight_confidence_label(
                avg_confidence, entry["is_accepted"]
            ),
            "avg_confidence": avg_confidence,
            "photo_count": len(photos),
            "best_quality": top.get("quality_score"),
            "best_score": _bucket_best_score(photos),
            "best_timestamp": top.get("timestamp"),
            "photos": photos,
        })

    _highlight_score_bucket(unidentified_photos, picked_first=True)
    buckets.sort(
        key=lambda b: (
            b.get("best_score") or 0,
            b.get("avg_confidence") or 0,
            b.get("photo_count") or 0,
        ),
        reverse=True,
    )
    return buckets, unidentified_photos


def _highlight_search_fields(photo):
    return [
        photo.get("filename") or "",
        photo.get("folder_name") or "",
        photo.get("folder_path") or "",
        photo.get("keyword_names") or "",
        photo.get("species") or "",
        photo.get("predicted_species") or "",
        "unidentified" if photo.get("is_unidentified") else "",
    ]


def _highlight_photo_matches_query(
    photo,
    query,
    match_case=False,
    whole_word=False,
):
    tokens = str(query or "").split()
    if not tokens:
        return True
    fields = _highlight_search_fields(photo)
    return all(
        any(
            text_search_match(field, token, match_case, whole_word)
            for field in fields
        )
        for token in tokens
    )


def _refresh_highlight_bucket_metadata(bucket):
    photos = bucket.get("photos") or []
    confidences = [
        p.get("predicted_confidence")
        for p in photos
        if p.get("predicted_confidence") is not None
    ]
    avg_confidence = (
        round(sum(confidences) / len(confidences), 4)
        if confidences else None
    )
    top = photos[0] if photos else {}
    # Filtering can drop the unconfirmed photos from a mixed bucket, leaving
    # only confirmed ones — recompute so the "candidate" badge and the
    # `confirmed` sort match what the row now actually contains.
    bucket["is_accepted"] = bool(photos) and all(
        p.get("has_accepted_species") for p in photos
    )
    bucket["avg_confidence"] = avg_confidence
    bucket["certainty"] = _highlight_confidence_label(
        avg_confidence, bucket.get("is_accepted")
    )
    bucket["photo_count"] = len(photos)
    bucket["best_quality"] = top.get("quality_score")
    bucket["best_score"] = _bucket_best_score(photos)
    bucket["best_timestamp"] = top.get("timestamp")
    return bucket


def _filter_highlight_sections(
    buckets,
    unidentified_photos,
    query,
    match_case=False,
    whole_word=False,
):
    query = (query or "").strip()
    if not query:
        for bucket in buckets:
            _refresh_highlight_bucket_metadata(bucket)
        return buckets, unidentified_photos

    filtered_buckets = []
    for bucket in buckets:
        photos = [
            p for p in (bucket.get("photos") or [])
            if _highlight_photo_matches_query(p, query, match_case, whole_word)
        ]
        if photos:
            updated = {**bucket, "photos": photos}
            filtered_buckets.append(_refresh_highlight_bucket_metadata(updated))

    unidentified = [
        p for p in unidentified_photos
        if _highlight_photo_matches_query(p, query, match_case, whole_word)
    ]
    return filtered_buckets, unidentified


def _filter_highlight_curation_state(
    buckets,
    unidentified_photos,
    highlight_filter="all",
    representative_filter="all",
):
    highlight_filter = _normalize_highlight_presence_filter(highlight_filter)
    representative_filter = _normalize_highlight_presence_filter(
        representative_filter
    )

    def matches(bucket, filter_value, field):
        if filter_value == "all":
            return True
        return bool(bucket.get(field)) == (filter_value == "yes")

    filtered = [
        bucket for bucket in buckets
        if matches(bucket, highlight_filter, "has_highlight_selection")
        and matches(
            bucket,
            representative_filter,
            "has_species_representative",
        )
    ]
    # These two states only apply to named species. Unidentified photos have
    # neither a species highlight list nor a species representative assignment.
    if highlight_filter != "all" or representative_filter != "all":
        unidentified_photos = []
    return filtered, unidentified_photos


# Maximum number of bound parameters per SQL statement. SQLite's
# ``SQLITE_MAX_VARIABLE_NUMBER`` defaults to 32766 on builds since 3.32 but
# remains 999 on older builds (and on some packagers' default builds). Bulk
# duplicate-cleanup actions can hand us thousands of photo ids at once, so
# we chunk every IN-clause query under this cap to stay portable across
# SQLite versions. Sized below 999 to leave headroom for additional bound
# parameters in joined statements.
_SQL_PARAM_CHUNK = 900


def _chunked(seq, size=_SQL_PARAM_CHUNK):
    """Yield ``seq`` in successive lists of at most ``size`` items."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _filename_sequence_key(filename):
    """Return (prefix, number, width, ext) for names like DSC_3069.NEF."""
    stem, ext = os.path.splitext(filename or "")
    match = re.match(r"^(.*?)(\d+)$", stem)
    if not match:
        return None
    digits = match.group(2)
    return (match.group(1), int(digits), len(digits), ext.lower())


def _parse_capture_timestamp(value):
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _time_gap_seconds(a, b):
    ta = _parse_capture_timestamp(a)
    tb = _parse_capture_timestamp(b)
    if ta is None or tb is None:
        return None
    try:
        return abs((ta - tb).total_seconds())
    except TypeError:
        return None


def _best_batch_scope(db, seed_photo_id, max_gap_seconds=8.0, max_sequence_gap=2, max_photos=120):
    """Find a likely burst/batch around one seed photo.

    The first pass follows same-folder filename sequence numbers, which matches
    camera batches such as DSC_3069...DSC_3133. Capture time is used as a
    boundary when available. If a filename has no trailing number, fall back to
    neighboring capture-time rows in the folder.
    """
    seed = db.get_photo(seed_photo_id, verify_workspace=True)
    if not seed:
        return None, "Photo not found"

    folder_id = seed["folder_id"]
    seed_key = _filename_sequence_key(seed["filename"])
    seed_ext = (seed["extension"] or os.path.splitext(seed["filename"])[1]).lower()
    ws = db._ws_id()

    def _limited(rows):
        if len(rows) <= max_photos:
            return rows
        seed_idx = next(
            (i for i, row in enumerate(rows) if row["id"] == seed_photo_id),
            len(rows) // 2,
        )
        half = max_photos // 2
        start = max(0, seed_idx - half)
        end = min(len(rows), start + max_photos)
        start = max(0, end - max_photos)
        return rows[start:end]

    def _time_ok(left, right, require_timestamps=False):
        gap = _time_gap_seconds(left["timestamp"], right["timestamp"])
        if gap is None:
            return not require_timestamps
        return gap <= max_gap_seconds

    if seed_key:
        prefix, _, width, _ = seed_key
        rows = db.conn.execute(
            """SELECT p.id, p.folder_id, p.filename, p.extension, p.timestamp,
                      p.flag, p.rating, p.quality_score, p.sharpness
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ? AND p.folder_id = ? AND p.filename LIKE ?
               ORDER BY p.filename ASC, p.id ASC""",
            (ws, folder_id, f"{prefix}%"),
        ).fetchall()
        seq_rows = []
        for row in rows:
            key = _filename_sequence_key(row["filename"])
            row_ext = (row["extension"] or os.path.splitext(row["filename"])[1]).lower()
            if (
                key
                and key[0] == prefix
                and key[2] == width
                and row_ext == seed_ext
            ):
                seq_rows.append((key[1], row))
        seq_rows.sort(key=lambda item: (item[0], item[1]["filename"], item[1]["id"]))
        seed_idx = next(
            (i for i, (_, row) in enumerate(seq_rows) if row["id"] == seed_photo_id),
            None,
        )
        if seed_idx is not None:
            left = seed_idx
            while left > 0:
                prev_num, prev_row = seq_rows[left - 1]
                curr_num, curr_row = seq_rows[left]
                if curr_num - prev_num > max_sequence_gap or not _time_ok(prev_row, curr_row):
                    break
                left -= 1
            right = seed_idx
            while right < len(seq_rows) - 1:
                curr_num, curr_row = seq_rows[right]
                next_num, next_row = seq_rows[right + 1]
                if next_num - curr_num > max_sequence_gap or not _time_ok(curr_row, next_row):
                    break
                right += 1
            batch = [row for _, row in seq_rows[left:right + 1]]
            if len(batch) >= 2:
                return _limited(batch), "filename_sequence"

    rows = db.conn.execute(
        """SELECT p.id, p.folder_id, p.filename, p.extension, p.timestamp,
                  p.flag, p.rating, p.quality_score, p.sharpness
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
           WHERE wf.workspace_id = ? AND p.folder_id = ?
           ORDER BY p.timestamp IS NULL, p.timestamp ASC, p.filename ASC, p.id ASC""",
        (ws, folder_id),
    ).fetchall()
    seed_idx = next((i for i, row in enumerate(rows) if row["id"] == seed_photo_id), None)
    if seed_idx is None:
        return None, "Photo not found"
    left = seed_idx
    while (
        left > 0
        and _time_ok(rows[left - 1], rows[left], require_timestamps=True)
    ):
        left -= 1
    right = seed_idx
    while (
        right < len(rows) - 1
        and _time_ok(rows[right], rows[right + 1], require_timestamps=True)
    ):
        right += 1
    batch = rows[left:right + 1]
    if len(batch) < 2:
        return None, "No neighboring batch photos found"
    return _limited(batch), "capture_time"


def _quality_rank(photos, key, photo_id):
    values = [
        (p["id"], p.get(key))
        for p in photos
        if p.get(key) is not None
    ]
    if not values:
        return None
    values.sort(key=lambda item: item[1], reverse=True)
    for idx, (pid, _) in enumerate(values, start=1):
        if pid == photo_id:
            return idx
    return None


def _best_batch_reasons(photo, photos, is_best=False):
    reasons = []
    reject_reasons = photo.get("reject_reasons") or []
    if reject_reasons:
        return [str(r).replace("_", " ") for r in reject_reasons[:3]]

    q_rank = _quality_rank(photos, "quality_composite", photo["id"])
    focus_key = "eye_focus_score" if photo.get("eye_focus_score") is not None else "focus_score"
    focus_rank = _quality_rank(photos, focus_key, photo["id"])
    exposure_rank = _quality_rank(photos, "exposure_score", photo["id"])

    if q_rank == 1:
        reasons.append("highest overall quality")
    elif q_rank is not None and q_rank <= 3:
        reasons.append(f"quality rank #{q_rank}")
    if focus_rank == 1:
        reasons.append("sharpest eye" if focus_key == "eye_focus_score" else "sharpest subject")
    elif focus_rank is not None and focus_rank <= 3:
        reasons.append(f"focus rank #{focus_rank}")
    if exposure_rank == 1 and photo.get("exposure_score", 0) >= 0.7:
        reasons.append("cleanest exposure")
    if photo.get("crop_complete") is not None and photo.get("crop_complete") >= 0.9:
        reasons.append("full subject in frame")
    if is_best and not reasons:
        reasons.append("best available score in this batch")
    if not reasons:
        reasons.append("usable alternate")
    return reasons[:4]


def _build_best_batch_response(db, seed_photo_id, rows):
    import config as cfg
    from pipeline import (
        load_photo_features,
        run_selected_batch_review,
        serialize_results,
    )

    effective_cfg = db.get_effective_config(cfg.load())
    photo_ids = [row["id"] for row in rows]
    loaded = load_photo_features(db, config=effective_cfg, photo_ids=photo_ids)
    by_id = {p["id"]: p for p in loaded}
    photos = [by_id[pid] for pid in photo_ids if pid in by_id]
    if len(photos) < 2:
        return None, "At least two batch photos with pipeline features are required"
    if not any(
        p.get("mask_path")
        or p.get("subject_tenengrad") is not None
        or p.get("eye_tenengrad") is not None
        for p in photos
    ):
        return None, "Run the pipeline on these photos before using Best Batch"

    results = serialize_results(run_selected_batch_review(photos, config=effective_cfg))
    result_photos = results.get("photos", [])
    if len(result_photos) < 2:
        return None, "Could not score this batch"

    ranked = sorted(
        result_photos,
        key=lambda p: (
            p.get("label") != "REJECT",
            p.get("quality_composite") if p.get("quality_composite") is not None else -1,
            p.get("focus_score") if p.get("focus_score") is not None else -1,
        ),
        reverse=True,
    )
    best = ranked[0]
    alternate_ids = [p["id"] for p in ranked[1:5] if p.get("label") != "REJECT"]
    reject_ids = [p["id"] for p in result_photos if p["id"] != best["id"]]
    sequence_keys = [_filename_sequence_key(p.get("filename")) for p in result_photos]
    sequence_nums = [key[1] for key in sequence_keys if key]

    cards = []
    for idx, photo in enumerate(ranked, start=1):
        if photo["id"] == best["id"]:
            role = "best"
        elif photo["id"] in alternate_ids:
            role = "alternate"
        else:
            role = "reject"
        cards.append({
            "id": photo["id"],
            "filename": photo.get("filename"),
            "width": photo.get("width"),
            "height": photo.get("height"),
            "rank": idx,
            "role": role,
            "label": photo.get("label"),
            "quality": photo.get("quality_composite"),
            "quality_pct": (
                round(photo["quality_composite"] * 100)
                if photo.get("quality_composite") is not None else None
            ),
            "focus": photo.get("eye_focus_score", photo.get("focus_score")),
            "focus_basis": "eye" if photo.get("eye_focus_score") is not None else "subject",
            "sharpness": photo.get("subject_tenengrad"),
            "eye_x": photo.get("eye_x"),
            "eye_y": photo.get("eye_y"),
            "eye_conf": photo.get("eye_conf"),
            "eye_tenengrad": photo.get("eye_tenengrad"),
            "exposure": photo.get("exposure_score"),
            "flag": photo.get("flag") or "none",
            "rating": photo.get("rating"),
            "reasons": _best_batch_reasons(photo, result_photos, is_best=photo["id"] == best["id"]),
        })

    best_reasons = _best_batch_reasons(best, result_photos, is_best=True)
    return {
        "seed_photo_id": seed_photo_id,
        "photo_ids": photo_ids,
        "count": len(photo_ids),
        "sequence_range": (
            [min(sequence_nums), max(sequence_nums)] if sequence_nums else None
        ),
        "best_photo_id": best["id"],
        "best_filename": best.get("filename"),
        "best_reasons": best_reasons,
        "summary": results.get("summary", {}),
        "cards": cards,
        "alternate_ids": alternate_ids,
        "suggested_reject_ids": reject_ids,
    }, None


_FINDER_TRASH_TIMEOUT_SECS = 30
_FINDER_TRASH_BATCH_SIZE = 20
_MOUNT_QUERY_TIMEOUT_SECS = 5

# Distinct from ``None`` so ``_trash_paths`` can tell "caller didn't pass
# network_roots" (re-query is safe) apart from "caller's own mount query
# already failed and it is passing that fail-closed signal through"
# (re-querying would overwrite the caller's classification with a stale
# or empty set the moment the share detaches, reclassifying an
# already-known custom mount point as local and reintroducing the
# unbounded-I/O hang this routing exists to prevent).
_NETWORK_ROOTS_UNSET = object()


class _NetworkVolumeRoots(set):
    """Network roots plus the live /Volumes roots from one mount snapshot."""

    def __init__(self, network_roots=(), mounted_volume_roots=()):
        super().__init__(network_roots)
        self.mounted_volume_roots = frozenset(mounted_volume_roots)


def _mounted_volume_roots(mounts):
    """Return live top-level ``/Volumes/<name>`` roots from parsed mounts."""
    roots = set()
    for mount in mounts:
        normalized = posixpath.normpath(mount["mount_point"])
        parts = normalized.split("/")
        if len(parts) == 3 and parts[1] == "Volumes" and parts[2]:
            roots.add(normalized)
    return roots


def _volume_root_for_path(filepath):
    """Return ``/Volumes/<name>`` for a path on a macOS mounted volume."""
    try:
        normalized = os.path.normpath(os.path.abspath(filepath))
    except (OSError, TypeError, ValueError):
        return None
    parts = normalized.split(os.sep)
    if len(parts) < 4 or parts[0] != "" or parts[1] != "Volumes" or not parts[2]:
        return None
    return os.sep.join(parts[:3])


def _network_volume_roots(run=subprocess.run):
    """Return mounted macOS network-volume roots without touching the shares.

    ``mount`` reads the kernel mount table, so this stays responsive even when
    an SMB server is unhealthy.  ``None`` means the mount table could not be
    read; callers fail closed and treat ``/Volumes`` paths as network-backed
    rather than risking an unbounded in-process filesystem call.
    """
    if sys.platform != "darwin":
        return set()
    try:
        result = run(
            ["mount"], capture_output=True, text=True,
            timeout=_MOUNT_QUERY_TIMEOUT_SECS,
            **no_window_kwargs(),
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = result.stdout or ""
    mounts = remote_setup.parse_mount_table(output)
    return _NetworkVolumeRoots(
        # ``mount`` always reports macOS/POSIX paths.  Keep parsing independent
        # of the host running the test suite (notably Windows' ``ntpath``).
        (
            posixpath.normpath(mount["mount_point"])
            for mount in mounts
            if remote_setup.mount_type_is_network_or_unknown(
                mount["fs_type"],
            )
        ),
        _mounted_volume_roots(mounts),
    )


def _expand_first_symlink_prefix(filepath):
    """Expand one local symlink prefix without resolving its target.

    ``realpath`` follows the target and can block when that target is an
    unhealthy network share. Reading the first symlink itself only touches
    its local directory entry; the unvisited suffix is then appended
    lexically so network-volume classification stays free of share I/O.
    """
    try:
        normalized = os.path.normpath(os.path.abspath(filepath))
    except (OSError, TypeError, ValueError):
        return None
    drive, tail = os.path.splitdrive(normalized)
    parts = [part for part in tail.split(os.sep) if part]
    prefix = drive + os.sep
    for index, part in enumerate(parts):
        prefix = os.path.join(prefix, part)
        try:
            target = os.readlink(prefix)
        except OSError:
            continue
        if os.name == "nt":
            # Windows junctions commonly expose their substitution path
            # through os.readlink() with an extended-length prefix. Strip it
            # so comparisons against ordinary drive or UNC mount roots use
            # the same spelling.
            if target.startswith("\\\\?\\UNC\\"):
                target = "\\\\" + target[8:]
            elif target.startswith("\\\\?\\"):
                target = target[4:]
        if not os.path.isabs(target):
            target = os.path.join(os.path.dirname(prefix), target)
        return os.path.normpath(os.path.join(target, *parts[index + 1:]))
    return None


def _path_on_network_volume(filepath, network_roots):
    """Whether ``filepath`` should avoid in-process mounted-volume I/O."""
    normalized = os.path.normpath(os.path.abspath(filepath))
    if network_roots is None:
        # Discovery failed, so there is no trustworthy evidence that any
        # candidate is local. Route every macOS path through bounded Finder
        # handling instead of risking an in-process stat on a custom mount.
        # Preserve the explicit /Volumes fallback on non-macOS test hosts.
        return (
            sys.platform == "darwin"
            or _volume_root_for_path(normalized) is not None
        )
    for _depth in range(16):
        for root in network_roots:
            try:
                if os.path.commonpath((normalized, root)) == root:
                    return True
            except ValueError:
                continue
        if sys.platform == "darwin":
            volume_root = _volume_root_for_path(normalized)
            if volume_root is not None:
                mounted_roots = getattr(
                    network_roots, "mounted_volume_roots", None,
                )
                # A detached network mount disappears from the snapshot but
                # leaves its /Volumes directory behind. Conversely, a live
                # local USB/APFS root in the same snapshot must retain the
                # local-trash path. Plain sets from older callers/tests carry
                # no liveness evidence, so continue to fail closed for them.
                if mounted_roots is None or volume_root not in mounted_roots:
                    return True
        if sys.platform != "darwin":
            return False
        expanded = _expand_first_symlink_prefix(normalized)
        if expanded is None:
            return False
        normalized = expanded
    # A symlink loop or unusually deep chain cannot be classified safely.
    # Fail closed on macOS so no subsequent stat reaches a possible share.
    return True


def _deepest_network_root_for_path(filepath, network_roots):
    """Return the *deepest* ``network_roots`` entry ``filepath`` resolves into.

    ``_path_on_network_volume`` answers the yes/no membership question and
    short-circuits on the first matching root, which is enough for routing
    decisions but not for reachability probing.  When mounts are nested —
    e.g. a still-reachable ``/Volumes/NAS`` share with a detached
    ``/Volumes/NAS/archive`` share mounted underneath it — the caller must
    probe reachability of the *exact* mount the path depends on rather than
    any reachable ancestor; otherwise a healthy outer mount would vouch for
    a nested inner mount that is actually gone, and Finder's false
    ``missing`` result would prune catalog rows for photos that reappear on
    reconnect.  Returns ``None`` when no root matches (either directly or
    via symlink expansion, mirroring ``_path_on_network_volume``'s traversal).
    """
    if not network_roots:
        return None
    normalized = os.path.normpath(os.path.abspath(filepath))
    for _depth in range(16):
        best = None
        best_len = -1
        for root in network_roots:
            try:
                if (
                    os.path.commonpath((normalized, root)) == root
                    and len(root) > best_len
                ):
                    # Longest matching root wins so nested mounts probe the
                    # inner share rather than an outer one that happens to
                    # be iterated first from the roots set.
                    best = root
                    best_len = len(root)
            except ValueError:
                continue
        if best is not None:
            return best
        if sys.platform != "darwin":
            return None
        expanded = _expand_first_symlink_prefix(normalized)
        if expanded is None:
            return None
        normalized = expanded
    return None


def _missing_paths_via_finder(filepaths, timeout=_FINDER_TRASH_TIMEOUT_SECS):
    """Boundedly confirm which paths remain absent according to Finder."""
    filepaths = [os.fspath(path) for path in filepaths]
    if not filepaths:
        return set(), set(), []
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set statuses to {}",
            "-e", "repeat with posixPath in argv",
            "-e", "set statusValue to \"error\"",
            "-e", "try",
            "-e", "set fileRef to POSIX file (contents of posixPath)",
            "-e", "tell application \"Finder\"",
            "-e", "if exists fileRef then",
            "-e", "set statusValue to \"exists\"",
            "-e", "else",
            "-e", "set statusValue to \"missing\"",
            "-e", "end if",
            "-e", "end tell",
            "-e", "end try",
            "-e", "set end of statuses to statusValue",
            "-e", "end repeat",
            "-e", "set AppleScript's text item delimiters to linefeed",
            "-e", "return statuses as text",
            "-e", "end run",
            "--",
            *filepaths,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        **no_window_kwargs(),
    )
    if result.returncode != 0:
        raise OSError(
            result.stderr.strip()
            or f"Finder existence check failed ({result.returncode})"
        )
    statuses = result.stdout.splitlines()
    if len(statuses) != len(filepaths):
        raise OSError("Finder returned an invalid existence response")
    missing = set()
    existing = set()
    failures = []
    for filepath, outcome in zip(filepaths, statuses, strict=True):
        if outcome == "missing":
            missing.add(filepath)
        elif outcome == "exists":
            existing.add(filepath)
        else:
            failures.append({
                "path": filepath, "error": "Finder existence check failed",
            })
    return missing, existing, failures


def _ensure_volume_trashes_dir(filepath, ensured_volumes):
    """Make sure ``/Volumes/<X>/.Trashes/<uid>/`` exists when ``filepath`` is on
    an external/network mount. macOS ``send2trash`` legacy mode raises
    ``OSError: Directory not found`` on volumes where this directory was never
    created (fresh SMB shares, NAS mounts, USB drives) — without it the caller
    falls back to AppleScript Finder, which then times out under load (-1712)
    and stalls the whole bulk trash for hours.

    No-op for paths outside ``/Volumes/`` (the system Trash already handles
    those) and for volumes already ensured this request. ``OSError`` from
    ``makedirs`` (read-only mount, ACL block) is swallowed so the actual
    trash call surfaces a more specific error than "could not mkdir".
    """
    volume_root = _volume_root_for_path(filepath)
    if volume_root is None:
        return
    if volume_root in ensured_volumes:
        return
    ensured_volumes.add(volume_root)
    trashes_dir = os.path.join(volume_root, ".Trashes", str(os.getuid()))
    try:
        os.makedirs(trashes_dir, mode=0o700, exist_ok=True)
    except OSError as exc:
        log.debug("could not ensure %s: %s", trashes_dir, exc)


def _move_to_volume_trash(filepath):
    """Move one file directly into a local mounted volume's Trash directory.

    Finder ultimately performs a same-volume move into
    ``/Volumes/<name>/.Trashes/<uid>``. Doing that move directly avoids the
    legacy Carbon ``send2trash`` failure on removable drives. Network mounts
    must be filtered by :func:`_trash_paths` before calling this helper because
    their rename syscall can block indefinitely. Returns ``True`` only when
    the move completed; callers retain their normal trash fallbacks on
    ``False``.
    """
    if sys.platform != "darwin":
        return False
    volume_root = _volume_root_for_path(filepath)
    if volume_root is None:
        return False

    trash_dir = os.path.join(volume_root, ".Trashes", str(os.getuid()))
    try:
        os.makedirs(trash_dir, mode=0o700, exist_ok=True)
        # Refuse a redirected Trash directory. The destination must remain on
        # the same mounted volume as the source.
        real_trash = os.path.realpath(trash_dir)
        if os.path.commonpath((volume_root, real_trash)) != volume_root:
            raise OSError("volume Trash directory resolves outside its volume")

        basename = os.path.basename(filepath)
        stem, ext = os.path.splitext(basename)
        # Atomically reserve the destination name with O_CREAT|O_EXCL so two
        # concurrent moves for same-named files from different folders can't
        # both observe an empty slot and then rename to the same path — POSIX
        # rename would silently replace one file, permanently losing a photo
        # the user meant to send to Trash.
        candidate = os.path.join(trash_dir, basename)
        reserved = None
        reserved_stat = None
        for _ in range(32):
            try:
                fd = os.open(
                    candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600,
                )
                try:
                    reserved_stat = os.fstat(fd)
                finally:
                    os.close(fd)
                reserved = candidate
                break
            except FileExistsError:
                candidate = os.path.join(
                    trash_dir, f"{stem} {uuid.uuid4().hex[:8]}{ext}",
                )
        if reserved is None:
            raise OSError(
                f"could not reserve a unique Trash destination in {trash_dir}",
            )
        try:
            # ``os.replace`` (not ``os.rename``) so the O_EXCL placeholder we
            # just reserved is overwritten. POSIX rename replaces silently,
            # but Windows rename raises when the destination exists.
            os.replace(filepath, reserved)
        except OSError:
            # A network filesystem can commit a rename but lose the success
            # response. Never unlink ``reserved`` unless it is still the exact
            # placeholder we created; otherwise that path may now be the photo.
            try:
                current_stat = os.lstat(reserved)
                try:
                    os.lstat(filepath)
                    source_still_exists = True
                except FileNotFoundError:
                    source_still_exists = False
                if not source_still_exists:
                    return True
                placeholder_unchanged = (
                    reserved_stat.st_ino
                    and current_stat.st_dev == reserved_stat.st_dev
                    and current_stat.st_ino == reserved_stat.st_ino
                    and current_stat.st_size == reserved_stat.st_size
                )
                if placeholder_unchanged:
                    os.unlink(reserved)
            except OSError:
                pass
            raise
        return True
    except OSError as exc:
        log.debug("Direct volume Trash move failed for %s: %s", filepath, exc)
        return False


def _trash_via_finder(filepaths, timeout=_FINDER_TRASH_TIMEOUT_SECS):
    """Trash paths via one bounded Finder call with per-item outcomes.

    Fallback for when send2trash fails (e.g. external volumes where the
    legacy Carbon API can't locate .Trashes). macOS-only: on Linux/Windows
    ``send2trash`` already implements the platform trash spec, so there is no
    equivalent fallback. Raising here (instead of spawning a doomed
    ``osascript``) lets the caller surface the original send2trash failure.

    The script catches each path's error and continues so a retry containing
    files already moved by a timed-out earlier batch cannot abort before later
    files. Returns ``(moved_paths, missing_paths, failures)``. A "missing"
    outcome (Finder saw ``sourceExists=false`` but the parent directory still
    exists) is reported separately so the caller can revalidate mount
    identity before accepting it: an unmounted network volume leaves its
    mount-point directory in place on the underlying local FS, so Finder's
    ``parentExists`` check alone cannot distinguish a genuine delete from a
    silently detached mount.
    """
    if sys.platform != "darwin":
        raise OSError("Finder trash fallback is only available on macOS")
    if isinstance(filepaths, (str, bytes, os.PathLike)):
        filepaths = [os.fspath(filepaths)]
    else:
        filepaths = [os.fspath(path) for path in filepaths]
    if not filepaths:
        return set(), set(), []
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set statuses to {}",
            "-e", "repeat with posixPath in argv",
            "-e", "set statusValue to \"error\"",
            "-e", "set fileRef to POSIX file (contents of posixPath)",
            "-e", "try",
            "-e", "tell application \"Finder\" to delete fileRef",
            "-e", "set statusValue to \"moved\"",
            "-e", "on error",
            "-e", "try",
            "-e", (
                "set parentPath to do shell script \"/usr/bin/dirname \" & "
                "quoted form of (contents of posixPath)"
            ),
            "-e", "set parentRef to POSIX file parentPath",
            "-e", "tell application \"Finder\"",
            "-e", "set sourceExists to exists fileRef",
            "-e", "set parentExists to exists parentRef",
            "-e", "end tell",
            "-e", (
                "if (not sourceExists) and parentExists then "
                "set statusValue to \"missing\""
            ),
            "-e", "end try",
            "-e", "end try",
            "-e", "set end of statuses to statusValue",
            "-e", "end repeat",
            "-e", "set AppleScript's text item delimiters to linefeed",
            "-e", "return statuses as text",
            "-e", "end run",
            "--",
            *filepaths,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        **no_window_kwargs(),
    )
    if result.returncode != 0:
        raise OSError(result.stderr.strip() or f"Finder trash failed ({result.returncode})")
    statuses = result.stdout.splitlines()
    if len(statuses) != len(filepaths):
        raise OSError(
            "Finder trash returned an invalid per-file status response"
        )
    moved_paths = set()
    missing_paths = set()
    failures = []
    for filepath, outcome in zip(filepaths, statuses, strict=True):
        if outcome == "moved":
            moved_paths.add(filepath)
        elif outcome == "missing":
            missing_paths.add(filepath)
        else:
            failures.append({
                "path": filepath, "error": "Finder Trash failed",
            })
    return moved_paths, missing_paths, failures


def _snapshot_parent_device(filepath):
    """Return the parent directory's ``st_dev`` for later mount verification.

    Callers snapshot this before any deletion attempt and pass it to
    :func:`_path_confirmed_gone` afterwards. A network mount that
    disconnects mid-op leaves the mount-point directory visible on the
    underlying local filesystem, so ``os.path.isdir`` alone reports the
    parent as live even though the actual volume is gone. Comparing the
    pre-op and post-op ``st_dev`` catches that drop. ``None`` means no
    baseline could be captured, so the device check is skipped.
    """
    parent = os.path.dirname(filepath) or os.sep
    try:
        return os.stat(parent).st_dev
    except OSError:
        return None


def _path_confirmed_gone(filepath, expected_parent_dev=None):
    """Return True only when the file is verifiably absent from a live volume.

    ``os.path.exists`` returning False is ambiguous on network volumes: it
    also returns False when the underlying stat call errors out because the
    mount went away mid-operation. Treating that as "successfully moved to
    Trash" would prune the catalog row for a photo that reappears when the
    mount comes back. Confirm both that the file is missing *and* that the
    parent directory is still reachable so a genuine live-volume delete is
    accepted while a mid-flight disconnect is preserved as a failure.

    When ``expected_parent_dev`` is provided (from
    :func:`_snapshot_parent_device`), also require the parent's current
    ``st_dev`` to match — a network mount that vanishes can leave its
    mount-point directory in place on the underlying local filesystem, so
    the ``os.path.isdir`` check alone would incorrectly accept the file
    as gone.
    """
    if os.path.exists(filepath):
        return False
    parent = os.path.dirname(filepath) or os.sep
    try:
        parent_stat = os.stat(parent)
    except OSError:
        return False
    if not stat.S_ISDIR(parent_stat.st_mode):
        return False
    return not (
        expected_parent_dev is not None
        and parent_stat.st_dev != expected_parent_dev
    )


def _trash_paths(filepaths, progress_callback=None, already_missing_out=None,
                 network_roots=_NETWORK_ROOTS_UNSET):
    """Move paths to Trash and return ``(moved, successful, failures)``.

    Missing paths are successful (the requested end state already holds) but
    are not counted as moved. On macOS mounted volumes we try a direct,
    same-volume rename first. Network volumes skip all in-process move APIs:
    an SMB ``rename(2)`` can wait in the kernel indefinitely, so those paths
    go directly to the time-bounded Finder subprocess. Remaining paths use
    send2trash individually so failures can be attributed, then one Finder
    process per bounded batch.

    ``already_missing_out``, when a mutable set, is populated with every
    path treated as successful because the requested end state already
    held (local preflight found it absent, or Finder reported it missing
    on a still-mounted volume). Callers surface those to users as
    "already missing" rather than as trashed, so a duplicate-cleanup that
    finds every loser already gone can return an explicit terminal
    result instead of a silent ``{trashed: 0}``.

    ``network_roots`` lets a caller reuse a mount-table classification it
    already performed. When omitted we re-query ``_network_volume_roots()``.
    Callers whose own mount query already failed should pass ``None``
    explicitly: that preserves the fail-closed classification they made
    against ``/Volumes`` paths instead of us re-querying and — if the second
    query succeeds with the share now detached — silently reclassifying an
    already-known custom mount point (``/Users/me/mnt/photos``) as local,
    which is the exact case we routed through Finder in the first place.
    """
    ordered = list(dict.fromkeys(filepaths))
    successful = set()
    moved = 0
    fallback = []
    preflight_errors = {}
    finder_candidates = []
    network_finder_candidates = set()
    send_errors = {}
    if network_roots is _NETWORK_ROOTS_UNSET:
        network_roots = _network_volume_roots()
    processed = set()

    def report_processed(filepath):
        if filepath in processed:
            return
        processed.add(filepath)
        if progress_callback:
            progress_callback(
                len(processed), len(ordered), os.path.basename(filepath),
            )

    # Classify paths using the kernel mount table before any source or parent
    # stat.  Those metadata calls can themselves block indefinitely while an
    # unhealthy SMB mount is reconnecting, so network candidates must go
    # straight to the bounded Finder subprocess.
    local_paths = []
    for filepath in ordered:
        if _path_on_network_volume(filepath, network_roots):
            finder_candidates.append(filepath)
            network_finder_candidates.add(filepath)
            send_errors[filepath] = "Network volume Trash operation failed"
        else:
            local_paths.append(filepath)

    # Snapshot each local parent's st_dev before we touch anything. A network
    # mount that vanishes mid-batch can leave the mount-point directory
    # visible on the underlying local FS, so ``os.path.isdir`` alone would
    # accept the file as gone. Comparing pre-op vs post-op st_dev catches
    # the mount drop even when the directory still stats cleanly.
    parent_devs = {path: _snapshot_parent_device(path) for path in local_paths}

    for filepath in local_paths:
        if not os.path.isfile(filepath):
            # ``os.path.isfile`` returning False is ambiguous on network
            # volumes — it also happens when the underlying stat fails
            # because the mount is already disconnected. Only treat the
            # path as "already gone" when the parent directory is still
            # reachable AND its device matches the pre-op snapshot;
            # otherwise preserve as a failure so the caller doesn't prune
            # the catalog row for a photo that reappears when the mount
            # comes back.
            if _path_confirmed_gone(filepath, parent_devs.get(filepath)):
                log.warning("File already missing: %s", filepath)
                successful.add(filepath)
                if already_missing_out is not None:
                    already_missing_out.add(filepath)
            else:
                preflight_errors[filepath] = (
                    "Source path is unreachable"
                )
                log.warning(
                    "Trash preflight: source unreachable for %s", filepath,
                )
            report_processed(filepath)
            continue
        if _move_to_volume_trash(filepath):
            successful.add(filepath)
            moved += 1
            report_processed(filepath)
        else:
            fallback.append(filepath)

    if fallback:
        from send2trash import send2trash as _trash
        for filepath in fallback:
            try:
                _trash(filepath)
                successful.add(filepath)
                moved += 1
                report_processed(filepath)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
                    raise
                # Some platform Trash APIs can complete the move and still
                # report a post-operation error. Only trust the "not there"
                # signal when we can positively verify it — a disconnected
                # network volume also makes ``os.path.exists`` return False
                # (the underlying stat fails), which would otherwise mask a
                # real failure and prune the catalog row for a photo that
                # reappears when the mount comes back.
                if _path_confirmed_gone(filepath, parent_devs.get(filepath)):
                    successful.add(filepath)
                    moved += 1
                    report_processed(filepath)
                    continue
                send_errors[filepath] = str(exc)
                if sys.platform == "darwin":
                    finder_candidates.append(filepath)
                else:
                    report_processed(filepath)

    def _finder_missing_is_trustworthy(
        filepath, current_network_roots, reachable_network_roots,
        confirmed_network_missing, path_to_deepest_root,
    ):
        """Reject Finder's "missing" outcome when the underlying mount is gone.

        Finder reports "missing" when ``sourceExists=false`` but
        ``parentExists=true``. An unmounted network volume leaves its
        mount-point directory in place on the underlying local FS, so the
        parent-exists check alone cannot distinguish a genuine delete from a
        silently detached mount. Accepting "missing" in that case would
        prune the catalog row for a photo that reappears on remount.

        For network-classified paths we require three independent signals:
        (1) the mount table (already re-queried once per Finder batch by
        the caller and passed in via ``current_network_roots``) still
        lists a root the path resolves into; (2) an out-of-process
        ``stat`` probe on the *deepest* matching root the path resolves
        into — separate from Finder's cache — responds within its bounded
        timeout, so a still-listed but unreachable SMB server cannot
        masquerade as "empty" and, crucially, a reachable ancestor mount
        cannot vouch for a nested inner mount that is actually gone; and
        (3) Finder's second-look ``exists`` query also confirmed the path
        as missing. Doing the mount recheck and reachability probes per
        batch instead of per path keeps the worst-case cost bounded — a
        20-item retry of already-moved paths would otherwise spawn one
        ``mount`` (or ``stat``) subprocess per path. For local fallbacks
        we reuse the parent-device snapshot check that guards the
        send2trash path already.
        """
        if filepath in network_finder_candidates:
            if current_network_roots is None:
                # Mount discovery failed on the recheck — we cannot
                # confirm the volume is still mounted, so refuse to trust
                # "missing" and let the caller retry.
                return False
            if not _path_on_network_volume(filepath, current_network_roots):
                return False
            if filepath not in confirmed_network_missing:
                return False
            # Require the *exact* mount the path depends on — not just any
            # reachable ancestor — to respond to the out-of-process stat
            # probe. When a detached inner share is nested beneath a
            # reachable outer share (e.g. ``/Volumes/NAS/archive`` under a
            # still-live ``/Volumes/NAS``), only the deepest match tells us
            # whether the photo could reappear on reconnect.
            deepest_root = path_to_deepest_root.get(filepath)
            if deepest_root is None:
                return False
            return deepest_root in reachable_network_roots
        return _path_confirmed_gone(filepath, parent_devs.get(filepath))

    for finder_batch in _chunked(
        finder_candidates, size=_FINDER_TRASH_BATCH_SIZE,
    ):
        try:
            finder_moved_paths, finder_missing_paths, finder_failures = (
                _trash_via_finder(finder_batch)
            )
            moved += len(finder_moved_paths)
            successful.update(finder_moved_paths)
            # Query the mount table at most once per batch. A retry that
            # contains many paths already moved by an earlier timed-out
            # Finder call comes back with every path in ``missing`` — doing
            # a fresh ``mount`` subprocess per path could otherwise burn up
            # to ``_MOUNT_QUERY_TIMEOUT_SECS`` × ``len(batch)`` seconds and
            # undermine the bounded batch behaviour this code establishes.
            batch_network_missing = any(
                path in network_finder_candidates
                for path in finder_missing_paths
            )
            batch_network_roots = (
                _network_volume_roots() if batch_network_missing else None
            )
            # Probe each still-relevant mount root with a bounded
            # out-of-process ``stat`` — a signal independent of Finder's
            # exists-cache — so a still-listed but unreachable SMB server
            # cannot make ``missing`` outcomes look legitimate. Run the
            # probes concurrently so a Finder batch spanning many
            # unavailable shares completes within one probe timeout
            # rather than accumulating ``len(distinct_roots)`` ×
            # ``_MOUNT_QUERY_TIMEOUT_SECS`` serially — a full 20-item
            # batch across unreachable roots would otherwise add up to
            # ~100 seconds of hang time before the Finder recheck.
            reachable_network_roots = set()
            path_to_deepest_root = {}
            confirmed_network_missing = set()
            finder_recheck_errors = set()
            if batch_network_missing and batch_network_roots is not None:
                paths_still_on_network = {
                    path for path in finder_missing_paths
                    if path in network_finder_candidates
                    and _path_on_network_volume(path, batch_network_roots)
                }
                # Associate each path with the *deepest* mount root it
                # resolves into so nested mounts probe reachability of the
                # inner share rather than an outer one that happens to be
                # iterated first. Set iteration is order-independent, so
                # picking the first match could otherwise validate a
                # detached inner mount using a live outer one and prune
                # rows for photos that reappear on reconnect.
                for path in paths_still_on_network:
                    root = _deepest_network_root_for_path(
                        path, batch_network_roots,
                    )
                    if root is not None:
                        path_to_deepest_root[path] = root
                distinct_roots = list(set(path_to_deepest_root.values()))
                if distinct_roots:
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=len(distinct_roots),
                    ) as executor:
                        probe_results = executor.map(
                            _network_root_reachable, distinct_roots,
                        )
                        for root, is_reachable in zip(
                            distinct_roots, probe_results, strict=True,
                        ):
                            if is_reachable:
                                reachable_network_roots.add(root)
                if paths_still_on_network:
                    try:
                        (
                            confirmed_network_missing,
                            reappeared_paths,
                            recheck_failures,
                        ) = _missing_paths_via_finder(paths_still_on_network)
                        for path in reappeared_paths:
                            finder_recheck_errors.add(path)
                            send_errors[path] = (
                                "Source reappeared during Trash operation"
                            )
                        for failure in recheck_failures:
                            finder_recheck_errors.add(failure["path"])
                            send_errors[failure["path"]] = failure["error"]
                    except subprocess.TimeoutExpired:
                        for path in paths_still_on_network:
                            finder_recheck_errors.add(path)
                            send_errors[path] = (
                                "Finder existence check timed out"
                            )
                    except Exception as exc:
                        for path in paths_still_on_network:
                            finder_recheck_errors.add(path)
                            send_errors[path] = (
                                str(exc) or "Finder existence check failed"
                            )
            for missing_path in finder_missing_paths:
                if _finder_missing_is_trustworthy(
                    missing_path, batch_network_roots,
                    reachable_network_roots,
                    confirmed_network_missing,
                    path_to_deepest_root,
                ):
                    successful.add(missing_path)
                    if already_missing_out is not None:
                        already_missing_out.add(missing_path)
                else:
                    if missing_path not in finder_recheck_errors:
                        send_errors[missing_path] = "Source path is unreachable"
                    log.warning(
                        "Rejecting Finder 'missing' outcome for %s: "
                        "underlying mount appears to have detached",
                        missing_path,
                    )
            for failure in finder_failures:
                send_errors[failure["path"]] = failure["error"]
        except subprocess.TimeoutExpired:
            log.warning(
                "Finder Trash timed out after %ss for %d file(s)",
                _FINDER_TRASH_TIMEOUT_SECS, len(finder_batch),
            )
            for filepath in finder_batch:
                send_errors[filepath] = (
                    f"Finder Trash timed out after "
                    f"{_FINDER_TRASH_TIMEOUT_SECS}s"
                )
        except Exception as exc:
            log.warning("Finder Trash failed for a file batch", exc_info=True)
            for filepath in finder_batch:
                send_errors[filepath] = str(exc) or "Finder Trash failed"
        for filepath in finder_batch:
            report_processed(filepath)

    failures = []
    for filepath in ordered:
        if filepath in successful:
            continue
        error = (
            preflight_errors.get(filepath)
            or send_errors.get(filepath)
            or "Trash operation failed"
        )
        failures.append({"path": filepath, "error": error})
        log.warning("Trash failed for %s: %s", filepath, error)
    return moved, successful, failures


# The canonical implementation lives in ``new_images.py`` so non-Flask
# modules (e.g. ``pipeline_job.py``) can import it without pulling in the
# app module. Kept aliased here under the original private name for
# backward-compatibility with existing call sites and tests.
from new_images import invalidate_new_images_after_scan as _invalidate_new_images_after_scan  # noqa: E402


def _migrate_legacy_preview_cache(app):
    """One-shot migration of pre-refactor preview cache files.

    Two classes of pre-existing files are made visible to the LRU here:

    1. Unsized {id}.jpg from the old /full endpoint. These are renamed to
       {id}_<preview_max_size>.jpg and tracked.
    2. Sized {id}_{size}.jpg files written by an earlier /preview before
       preview_cache existed. These already match the new naming scheme,
       so we just insert a tracking row pointing at the file in place.

    Both classes were previously invisible to accounting and eviction —
    they sat on disk indefinitely unless the user hit Clear Cache. Runs
    once per process start; a no-op when nothing needs adopting.

    If preview_max_size=0 (meaning "full") we can't assign a size tier
    to unsized {id}.jpg, so those are left in place for Clear Cache to
    remove later. Sized files are still adopted in that case.
    """
    import re

    import config as cfg

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    if not os.path.isdir(preview_dir):
        return

    unsized_pat = re.compile(r"^(\d+)\.jpg$")
    sized_pat = re.compile(r"^(\d+)_(\d+)\.jpg$")
    try:
        all_files = os.listdir(preview_dir)
    except OSError:
        return
    unsized_files = [f for f in all_files if unsized_pat.match(f)]
    sized_files = [f for f in all_files if sized_pat.match(f)]
    if not unsized_files and not sized_files:
        return

    # Read preview_max_size explicitly so a configured 0 ("full res")
    # stays 0 and the tier-assignment guard below is reachable.
    raw_size = cfg.load().get("preview_max_size")
    target_size = 0 if raw_size == 0 else int(raw_size or 1920)

    db = Database(app.config["DB_PATH"])
    try:
        migrated = 0
        orphaned = 0
        adopted = 0
        if unsized_files and target_size == 0:
            log.info(
                "Leaving %d legacy preview files (preview_max_size=0 — can't assign tier)",
                len(unsized_files),
            )

        # Pass 1: rename unsized {id}.jpg → {id}_<target>.jpg + insert.
        if target_size:
            for fname in unsized_files:
                m = unsized_pat.match(fname)
                photo_id = int(m.group(1))
                src = os.path.join(preview_dir, fname)
                dst = os.path.join(preview_dir, f"{photo_id}_{target_size}.jpg")
                # Skip orphans: if the photo was deleted, inserting into
                # preview_cache would raise a FK error and rolling back the
                # already-performed os.rename is ugly. Unlink the orphan so
                # disk doesn't keep pointing at vanished photos.
                photo_row = db.conn.execute(
                    "SELECT 1 FROM photos WHERE id=?", (photo_id,)
                ).fetchone()
                if photo_row is None:
                    try:
                        os.remove(src)
                        orphaned += 1
                    except OSError:
                        pass
                    continue
                if os.path.exists(dst):
                    try:
                        os.remove(src)
                    except OSError:
                        pass
                    continue
                try:
                    os.rename(src, dst)
                    st = os.stat(dst)
                    db.preview_cache_insert(photo_id, target_size, st.st_size)
                    migrated += 1
                except OSError as e:
                    log.warning("Failed to migrate legacy preview %s: %s", src, e)

        # Pass 2: adopt pre-existing sized {id}_{size}.jpg files that
        # aren't tracked yet. These are produced by older /preview calls
        # that ran before preview_cache existed; without this pass they
        # stay invisible to accounting/eviction even though they already
        # match the new naming scheme.
        for fname in sized_files:
            m = sized_pat.match(fname)
            photo_id = int(m.group(1))
            size = int(m.group(2))
            path = os.path.join(preview_dir, fname)
            if db.preview_cache_get(photo_id, size):
                continue
            photo_row = db.conn.execute(
                "SELECT 1 FROM photos WHERE id=?", (photo_id,)
            ).fetchone()
            if photo_row is None:
                try:
                    os.remove(path)
                    orphaned += 1
                except OSError:
                    pass
                continue
            try:
                st = os.stat(path)
                db.preview_cache_insert(photo_id, size, st.st_size)
                adopted += 1
            except OSError as e:
                log.warning("Failed to adopt sized preview %s: %s", path, e)

        if migrated:
            log.info(
                "Migrated %d legacy preview cache files to size %d",
                migrated, target_size,
            )
        if adopted:
            log.info("Adopted %d untracked sized preview files into LRU", adopted)
        if orphaned:
            log.info("Removed %d orphaned legacy preview files", orphaned)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _migrate_edit_math_render_caches(app):
    """Invalidate rendered caches when the edit-math version has bumped.

    Cached previews/thumbnails are keyed by ``(photo_id, size)`` only — the
    edit recipe isn't part of the key. When the per-pixel rendering math in
    ``image_edits`` / ``tone`` changes, the old bytes on disk are no longer
    what we'd produce now, but a recipe-unchanged photo would otherwise keep
    serving them until the user manually cleared the cache.

    On each startup we compare ``db_meta["edit_math_version"]`` against
    ``image_edits.EDIT_MATH_VERSION``. If it lags, we drop:

      * every ``preview_cache`` row plus its on-disk JPEG
      * every per-photo thumbnail file plus its ``photos.thumb_path``

    for photos that have a non-null edit recipe (recipe-free photos render
    identically across math versions and don't need re-rendering). Then we
    write the new version so the migration is a no-op next boot.

    On a fresh DB with no recipes, the walk does nothing and we still bump
    the version so future deploys only act on real prior state.
    """
    from image_edits import EDIT_MATH_VERSION

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    preview_dir = os.path.join(vireo_dir, "previews")
    db = Database(app.config["DB_PATH"])
    try:
        stored = db.get_meta("edit_math_version")
        try:
            stored_version = int(stored) if stored is not None else 1
        except (TypeError, ValueError):
            stored_version = 1
        if stored_version >= EDIT_MATH_VERSION:
            return

        rows = db.conn.execute(
            "SELECT photo_id FROM photo_edit_recipes"
        ).fetchall()
        photo_ids = [row["photo_id"] for row in rows]

        invalidated_previews = 0
        invalidated_thumbs = 0
        # If any unlink fails (locked file on Windows, transient permission
        # error), we must NOT stamp the new version: a stale file/preview_cache
        # row could still be served, and bumping the version would make the
        # next boot skip the migration and never retry. Leaving the version
        # behind makes the migration idempotent and self-retrying.
        purge_failed = False

        # Scan preview_dir once and group untracked preview files by photo id.
        # The per-photo listdir would otherwise be O(N*M) (N edited photos x M
        # preview files) and can spend minutes just rescanning the same
        # directory on a large library before the server even starts.
        edited_set = set(photo_ids)
        untracked_previews_by_pid = {}
        try:
            preview_names = os.listdir(preview_dir)
        except FileNotFoundError:
            # Cache dir doesn't exist yet — nothing untracked to clean up.
            preview_names = ()
        except OSError:
            # Permissions / locked network volume / other transient read
            # failure: we can't see what's in there, so we don't know whether
            # there are stale orphans to purge. Skip the scan but leave the
            # version old so the next boot retries — matches the unlink-error
            # contract instead of taking the app down for a disposable cache
            # problem.
            log.warning(
                "Failed to list preview cache dir %s during edit-math "
                "migration; leaving version at %s to retry next boot",
                preview_dir, stored_version, exc_info=True,
            )
            preview_names = ()
            purge_failed = True

        for name in preview_names:
            if not name.endswith(".jpg"):
                continue
            underscore = name.find("_")
            if underscore <= 0:
                continue
            try:
                file_pid = int(name[:underscore])
            except ValueError:
                continue
            if file_pid in edited_set:
                untracked_previews_by_pid.setdefault(file_pid, []).append(name)
        for pid in photo_ids:
            for row in db.conn.execute(
                "SELECT size FROM preview_cache WHERE photo_id = ?", (pid,)
            ).fetchall():
                size_value = row["size"]
                path = os.path.join(preview_dir, f"{pid}_{size_value}.jpg")
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    log.warning(
                        "Failed to remove stale preview cache %s during "
                        "edit-math migration", path, exc_info=True,
                    )
                    purge_failed = True
                    continue
                db.conn.execute(
                    "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
                    (pid, size_value),
                )
                invalidated_previews += 1
            for name in untracked_previews_by_pid.get(pid, ()):
                path = os.path.join(preview_dir, name)
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    purge_failed = True
            thumb_cache = os.path.join(thumb_dir, f"{pid}.jpg")
            try:
                if os.path.exists(thumb_cache):
                    os.remove(thumb_cache)
                    invalidated_thumbs += 1
            except OSError:
                log.warning(
                    "Failed to remove stale thumbnail %s during "
                    "edit-math migration", thumb_cache, exc_info=True,
                )
                purge_failed = True
                continue
            for source in ("raw", "jpeg"):
                variant = os.path.join(thumb_dir, f"{pid}_{source}.jpg")
                try:
                    if os.path.exists(variant):
                        os.remove(variant)
                        invalidated_thumbs += 1
                except OSError:
                    log.warning(
                        "Failed to remove paired thumbnail %s during "
                        "edit-math migration", variant, exc_info=True,
                    )
                    purge_failed = True
            db.conn.execute(
                "UPDATE photos SET thumb_path = NULL WHERE id = ?", (pid,),
            )

        if purge_failed:
            # Commit the row deletions that did succeed, but leave the stored
            # version unchanged so the next boot re-runs and retries the
            # paths that couldn't be purged this time.
            db.conn.commit()
            log.warning(
                "edit_math_version %s -> %s: some cache purges failed; "
                "leaving version at %s so the migration retries next boot "
                "(invalidated %d preview-cache entries, %d thumbnails so far)",
                stored_version, EDIT_MATH_VERSION, stored_version,
                invalidated_previews, invalidated_thumbs,
            )
            return

        db.set_meta("edit_math_version", EDIT_MATH_VERSION, _commit=False)
        db.conn.commit()

        if photo_ids:
            log.info(
                "edit_math_version %s -> %s: invalidated %d preview-cache "
                "entries and %d thumbnails across %d edited photos",
                stored_version, EDIT_MATH_VERSION,
                invalidated_previews, invalidated_thumbs, len(photo_ids),
            )
        else:
            log.info(
                "edit_math_version %s -> %s: no edited photos, nothing to "
                "invalidate", stored_version, EDIT_MATH_VERSION,
            )
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _migrate_unedited_raw_preview_sources(app):
    """Drop previews that may have used an edit-quality RAW working copy.

    RAW working copies deliberately preserve highlight headroom and therefore
    look flatter/darker than the camera-rendered rendition used for browsing.
    Older preview routing treated that working copy as the canonical source
    even when a RAW had no edit recipe.  Those bytes are keyed only by
    ``(photo_id, size)``, so fixing source selection alone would keep serving
    already-cached dark tiers indefinitely.

    Purge only recipe-free RAWs that currently have a working copy, and gate
    the migration in ``db_meta`` so large libraries pay the directory scan
    once.  If any cache file cannot be inspected or removed, leave the marker
    unset so the next launch retries instead of permanently adopting stale
    pixels.
    """
    from image_loader import RAW_EXTENSIONS

    marker = "unedited_raw_camera_preview_source_v1"
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    db = Database(app.config["DB_PATH"])
    try:
        if db.get_meta(marker) == "1":
            return

        rows = db.conn.execute(
            """SELECT p.id, p.filename
               FROM photos p
               LEFT JOIN photo_edit_recipes r ON r.photo_id = p.id
               WHERE p.working_copy_path IS NOT NULL
                 AND r.photo_id IS NULL"""
        ).fetchall()
        affected = {
            int(row["id"])
            for row in rows
            if os.path.splitext(row["filename"] or "")[1].lower()
            in RAW_EXTENSIONS
        }

        purge_failed = False
        names_by_pid = {}
        try:
            preview_names = os.listdir(preview_dir)
        except FileNotFoundError:
            preview_names = ()
        except OSError:
            log.warning(
                "Failed to list preview cache dir %s while migrating "
                "unedited RAW preview sources; retrying next launch",
                preview_dir,
                exc_info=True,
            )
            preview_names = ()
            purge_failed = True

        for name in preview_names:
            if not name.endswith(".jpg"):
                continue
            underscore = name.find("_")
            if underscore <= 0:
                continue
            try:
                photo_id = int(name[:underscore])
            except ValueError:
                continue
            if photo_id in affected:
                names_by_pid.setdefault(photo_id, set()).add(name)

        invalidated = 0
        for photo_id in affected:
            tracked = db.conn.execute(
                "SELECT size FROM preview_cache WHERE photo_id=?",
                (photo_id,),
            ).fetchall()
            names = names_by_pid.get(photo_id, set())
            names.update(
                f"{photo_id}_{row['size']}.jpg" for row in tracked
            )
            photo_failed = False
            for name in names:
                path = os.path.join(preview_dir, name)
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    log.warning(
                        "Failed to remove stale unedited RAW preview %s",
                        path,
                        exc_info=True,
                    )
                    photo_failed = True
                    purge_failed = True
            if photo_failed:
                continue
            cursor = db.conn.execute(
                "DELETE FROM preview_cache WHERE photo_id=?", (photo_id,)
            )
            invalidated += max(cursor.rowcount, 0)

        if purge_failed:
            db.conn.commit()
            return

        db.set_meta(marker, "1", _commit=False)
        db.conn.commit()
        if affected:
            log.info(
                "Invalidated %d preview-cache entries across %d unedited "
                "RAW photos so camera-rendered previews can regenerate",
                invalidated,
                len(affected),
            )
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _enforce_preview_cache_quota_at_startup(app):
    """Reconcile and evict at startup so prior runs / external deletes
    can't leave the table out of sync or over quota.

    Reconcile first: if a previous session left ghost rows (files
    deleted while cache was under quota), eviction would see inflated
    totals and stay asleep when it shouldn't, or wake up and run a
    no-op pass over rows whose files don't exist.
    """
    from preview_cache import evict_if_over_quota

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    db = Database(app.config["DB_PATH"])
    try:
        reconcile_preview_cache(db, vireo_dir)
        evict_if_over_quota(db, vireo_dir)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _enforce_working_copy_cache_quota_at_startup(app):
    """Apply the persistent working-copy ceiling when the app starts.

    Quota eviction records a source-mtime marker on each removed row so later
    scans do not immediately regenerate files that were deliberately removed.

    Sweeps ``.<id>.render.*.jpg.tmp`` orphans in ``working/`` first so a
    process kill during a prior on-demand extraction does not permanently
    consume disk outside the configured ceiling (quota accounting skips these
    files by design, so the sweep is their only cleanup path). Passes
    ``startup=True`` so a legacy ``working/<id>.jpg`` whose mtime happens to
    fall inside the concurrent-writer grace window is still reclaimed on the
    first pass — no cache writer can be active this early.
    """
    from working_copy_cache import sweep_abandoned_render_tempfiles

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    sweep_abandoned_render_tempfiles(vireo_dir)
    db = Database(app.config["DB_PATH"])
    try:
        evict_working_copy_cache_if_over_quota(db, vireo_dir, startup=True)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _sweep_abandoned_transient_originals(app):
    """Reclaim non-cacheable ``/original`` renditions the process orphaned.

    When ``_serve_generated_original`` streams a rendition too large for the
    quota (or when the quota is zero), it moves the file into
    ``<vireo_dir>/originals/.<id>.transient.*.jpg`` and unlinks it in the
    generator's ``finally`` block after streaming. A process kill or crash
    during that stream leaves the ``.transient.*.jpg`` behind: working-copy
    eviction only scans ``working/`` and cannot see it, so repeated
    interrupted requests can accumulate arbitrary bytes outside the quota
    with no other cleanup path.
    """
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    originals_dir = os.path.join(vireo_dir, "originals")
    if not os.path.isdir(originals_dir):
        return
    try:
        with os.scandir(originals_dir) as entries:
            for entry in entries:
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                name = entry.name
                if not (
                    name.startswith(".")
                    and ".transient." in name
                    and name.endswith(".jpg")
                ):
                    continue
                try:
                    os.remove(entry.path)
                except OSError as exc:
                    log.warning(
                        "Could not remove abandoned transient rendition %s: %s",
                        entry.path, exc,
                    )
    except OSError as exc:
        log.warning(
            "Could not scan originals directory for transient renditions %s: %s",
            originals_dir, exc,
        )


def _scan_metadata_warning():
    """Thin wrapper over ``metadata.scan_metadata_warning`` for the scan paths.

    Kept as a module-level alias so callers don't import ``metadata`` directly
    at every call site; the implementation lives in ``metadata`` so the
    pipeline-job module can share it.
    """
    from metadata import scan_metadata_warning
    return scan_metadata_warning()


_LIFE_LIST_LEADING_APOSTROPHE_RE = re.compile(
    r"^['`´ʹʻʼ‘’‛]+"
)


def _life_list_alphabetical_key(name):
    """Sort key that ignores a leading Hawaiian ʻokina or apostrophe variant.

    Kept in sync with ``lifeListAlphabeticalName`` in ``life_list.html`` so
    server-side lifer numbering ties break under the same letter the client
    displays the species under.
    """
    return _LIFE_LIST_LEADING_APOSTROPHE_RE.sub("", str(name or "")).lower()


def create_app(db_path, thumb_cache_dir=None, api_token=None):
    """Create the Flask app for the Vireo photo browser.

    Args:
        db_path: path to the SQLite database
        thumb_cache_dir: path to thumbnail cache directory
        api_token: optional token required on /api/v1/* requests via the
            ``X-Vireo-Token`` header. When ``None`` (default), all /api/v1/*
            traffic is rejected with 401 — the token is expected to be
            supplied by ``main()`` after calling ``runtime.generate_token``.
    """
    app = Flask(
        __name__, template_folder=os.path.join(os.path.dirname(__file__), "templates")
    )
    app.config["DB_PATH"] = db_path
    app.config["COMPUTATION_CACHE_DIR"] = os.path.expanduser(
        "~/.vireo/computation-cache"
    )
    app.config["THUMB_CACHE_DIR"] = thumb_cache_dir or os.path.expanduser(
        "~/.vireo/thumbnails"
    )
    app.config["API_TOKEN"] = api_token
    app.config["TRUSTED_HOSTS"] = ["localhost", "127.0.0.1", "::1"]
    app.config["BROWSER_SESSION_COOKIE"] = "vireo_session"
    app.config["BROWSER_SESSION_TOKEN"] = secrets.token_urlsafe(32)
    app.config["BROWSER_AUTH_ENABLED"] = (
        os.environ.get("VIREO_DISABLE_BROWSER_AUTH") != "1"
    )
    app.config["REQUIRE_EXIFTOOL_FOR_IMPORT"] = (
        os.environ.get("VIREO_REQUIRE_EXIFTOOL_FOR_IMPORT", "1") != "0"
    )
    app.config["CARD_CLEANUP_DIR"] = os.path.join(
        os.path.dirname(os.path.abspath(db_path)), "card_cleanup"
    )
    # Built here rather than on first request: a check-then-set in the request
    # path lets two concurrent first requests each build their own store, and a
    # token handed out by whichever store loses the race is never found again.
    # The endpoint would then silently re-derive the whole comparison — the
    # multi-second cost this store exists to avoid.
    app.config["ID_CONFLICTS_SNAPSHOTS"] = id_conflicts.SnapshotStore()

    # Schema creation and migrations are startup work, never request work.
    # `:memory:` is the development exception because each SQLite connection
    # owns a distinct database and therefore must initialize itself.
    ensure_schema(db_path)

    _migrate_legacy_preview_cache(app)
    _migrate_edit_math_render_caches(app)
    _migrate_unedited_raw_preview_sources(app)
    _enforce_preview_cache_quota_at_startup(app)
    _sweep_abandoned_transient_originals(app)
    _enforce_working_copy_cache_quota_at_startup(app)

    # Request timing middleware — logs slow requests and user actions
    @app.before_request
    def _start_timer():
        request._start_time = time.time()
        supplied_request_id = request.headers.get("X-Request-ID", "")
        if re.fullmatch(r"[A-Za-z0-9._-]{1,64}", supplied_request_id):
            g.request_id = supplied_request_id
        else:
            g.request_id = uuid.uuid4().hex

    @app.before_request
    def _protect_browser_surface():
        """Keep hostile web pages away from Vireo's localhost interface.

        `/api/v1` remains token-authenticated for automation.  The internal
        browser API and photo responses use an HttpOnly, SameSite-strict
        session established by a Vireo HTML page.  Unsafe browser requests
        additionally carry a header that cross-origin forms cannot emit.
        """
        if not app.config["BROWSER_AUTH_ENABLED"]:
            return None

        path = request.path
        if path.startswith("/api/v1/") or path in {
            "/api/health",
            "/api/shutdown",
        }:
            return None

        protected = (
            path.startswith("/api/")
            or path.startswith("/photos/")
            or path.startswith("/thumbnails/")
        )
        if not protected:
            return None

        # Native desktop clients cannot use the HttpOnly browser cookie.
        # They authenticate with the same per-runtime secret as /api/v1.
        expected_api_token = app.config.get("API_TOKEN")
        supplied_api_token = request.headers.get("X-Vireo-Token", "")
        if (
            expected_api_token
            and supplied_api_token
            and secrets.compare_digest(
                supplied_api_token.encode("utf-8"),
                expected_api_token.encode("utf-8"),
            )
        ):
            return None

        fetch_site = request.headers.get("Sec-Fetch-Site", "").lower()
        if fetch_site and fetch_site not in {"same-origin", "none"}:
            return json_error(
                "Non-origin requests are not allowed",
                403,
                code="cross_site_request",
            )

        origin = request.headers.get("Origin")
        if origin:
            parsed = urlsplit(origin)
            expected = urlsplit(request.host_url)
            if (parsed.scheme, parsed.netloc) != (expected.scheme, expected.netloc):
                return json_error(
                    "Cross-origin requests are not allowed",
                    403,
                    code="cross_origin_request",
                )

        cookie_name = app.config["BROWSER_SESSION_COOKIE"]
        expected_token = app.config["BROWSER_SESSION_TOKEN"]
        if not secrets.compare_digest(
            request.cookies.get(cookie_name, "").encode("utf-8"),
            expected_token.encode("utf-8"),
        ):
            return json_error(
                "Browser session required",
                401,
                code="browser_session_required",
            )

        if (
            request.method not in {"GET", "HEAD", "OPTIONS"}
            and request.headers.get("X-Vireo-Client") != "browser"
        ):
            return json_error(
                "Missing browser request header",
                403,
                code="browser_header_required",
            )
        return None

    @app.after_request
    def _log_requests(response):
        if hasattr(request, "_start_time"):
            elapsed = time.time() - request._start_time
            if request.method in ("POST", "DELETE"):
                # Log user actions with details about what changed
                detail = ""
                path = request.path
                if path in ("/api/capture-time/preview", "/api/jobs/capture-time"):
                    body = {}
                else:
                    body = request.get_json(silent=True) or {}
                    if not isinstance(body, dict):
                        # Valid non-object JSON (5, "x", [..]) — the
                        # .get() calls below would 500 the response of
                        # any endpoint after it already ran.
                        body = {}
                if "/rating" in path:
                    detail = f" rating={body.get('rating')}"
                elif "/flag" in path:
                    detail = f" flag={body.get('flag')}"
                elif "/keywords" in path and request.method == "POST":
                    detail = f" keyword={body.get('name')}"
                elif "/accept" in path:
                    detail = " (accept prediction)"
                elif "/reject" in path:
                    detail = " (reject prediction)"
                elif "batch" in path:
                    ids = body.get("photo_ids", [])
                    detail = f" ({len(ids)} photos)"
                elif "/classify" in path:
                    detail = f" collection={body.get('collection_id')}"
                elif "/scan" in path:
                    detail = f" root={body.get('root', '')}"
                log.info(
                    "Action: %s %s → %s (%.1fs)%s request_id=%s",
                    request.method,
                    path,
                    response.status_code,
                    elapsed,
                    detail,
                    getattr(g, "request_id", "-"),
                )
            elif elapsed > 0.5:
                log.warning(
                    "Slow request: %s %s took %.1fs request_id=%s",
                    request.method,
                    request.path,
                    elapsed,
                    getattr(g, "request_id", "-"),
                )
            if request.path.startswith("/api/"):
                _quiet = request.method == "GET" and request.path == "/api/jobs"
                (log.debug if _quiet else log.info)(
                    "API: %s %s → %s (%.3fs) request_id=%s",
                    request.method,
                    request.path,
                    response.status_code,
                    elapsed,
                    getattr(g, "request_id", "-"),
                )
        request_id = getattr(g, "request_id", None)
        if request_id:
            response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy-Report-Only"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' data: blob: https:; "
            "connect-src 'self' https:; frame-ancestors 'none'; "
            "base-uri 'self'; form-action 'self'"
        )
        if (
            app.config["BROWSER_AUTH_ENABLED"]
            and response.mimetype == "text/html"
            and 200 <= response.status_code < 400
        ):
            response.set_cookie(
                app.config["BROWSER_SESSION_COOKIE"],
                app.config["BROWSER_SESSION_TOKEN"],
                httponly=True,
                samesite="Strict",
                secure=request.is_secure,
                path="/",
            )
        return response

    # Catch uncaught exceptions so they don't disappear silently
    @app.errorhandler(Exception)
    def _handle_error(e):
        from jobs import WorkspaceBusyError
        from werkzeug.exceptions import HTTPException
        if isinstance(e, WorkspaceBusyError):
            return json_error(str(e), 409)
        if isinstance(e, HTTPException):
            return e
        log.exception("Unhandled error: %s %s", request.method, request.path)
        return jsonify({
            "error": "Internal server error",
            "code": "internal_error",
            "message": "Something went wrong in Vireo. Try again.",
            "request_id": getattr(g, "request_id", None),
        }), 500

    _MAX_PER_PAGE = 500

    def json_error(msg, status=400, *, code=None, message=None):
        """Return a JSON error response with an optional user-facing message."""
        if code is None:
            code = {
                400: "invalid_request",
                401: "unauthorized",
                403: "forbidden",
                404: "not_found",
                409: "conflict",
            }.get(status, "request_failed")
        payload = {
            "error": msg,
            "code": code,
            "request_id": getattr(g, "request_id", None),
        }
        if message:
            payload["message"] = message
        return jsonify(payload), status

    def _photo_not_found_error(*, legacy_error="photo_not_found"):
        return json_error(
            legacy_error,
            404,
            message=(
                "This photo is no longer available in the active workspace. "
                "Refresh the page and try again."
            ),
        )

    # Location error responses shared by the location, place, and batch
    # routes (``web.location_edits``); built once around ``json_error``.
    location_errors = LocationErrors(
        json_error=json_error, photo_not_found_error=_photo_not_found_error,
    )

    def _get_db():
        """Get a Database instance. One connection per request via Flask g."""
        if "db" not in g:
            g.db = Database(
                db_path,
                initialize_schema=(db_path == ":memory:"),
            )
        return g.db

    # Shared prologue for routes that launch background jobs. See
    # web/background_jobs.py: the decorated view receives a ``JobLaunch``
    # (runner + active workspace + worker-thread db factory) as its first
    # argument and returns ``ctx.start(job_type, work, ...)``.
    background_job = make_background_job(
        lambda: app._job_runner, _get_db, db_path, Database
    )

    _invalid_preview_cache_paths = set()

    # Canonical definitions live in preview_cache so the recycled-rowid
    # purge (which runs in the scanner, outside this app factory) writes
    # the same marker web.media's _serve_preview lazy-adoption branch consults.
    def _ensure_preview_cache_invalidations_table(db):
        from preview_cache import ensure_preview_cache_invalidations_table
        ensure_preview_cache_invalidations_table(db)

    def _mark_preview_cache_invalid(db, photo_id, size, *, commit=True):
        from preview_cache import mark_preview_cache_invalid
        mark_preview_cache_invalid(db, photo_id, size, commit=commit)

    def _clear_preview_cache_invalid(db, photo_id, size, *, commit=True):
        _ensure_preview_cache_invalidations_table(db)
        db.conn.execute(
            "DELETE FROM preview_cache_invalidations WHERE photo_id=? AND size=?",
            (photo_id, size),
        )
        if commit:
            db.conn.commit()

    def _invalidate_photo_render_cache(db, photo_ids):
        """Drop cached rendered derivatives after an edit recipe changes."""
        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        thumb_dir = app.config["THUMB_CACHE_DIR"]
        preview_dir = os.path.join(vireo_dir, "previews")
        originals_dir = os.path.join(vireo_dir, "originals")
        external_edits_dir = os.path.join(vireo_dir, "external-edits")
        for pid in photo_ids:
            thumb_cache = os.path.join(thumb_dir, f"{pid}.jpg")
            clear_thumb_path = True
            try:
                if os.path.exists(thumb_cache):
                    os.remove(thumb_cache)
            except OSError:
                clear_thumb_path = not os.path.exists(thumb_cache)
                log.warning(
                    "Failed to remove stale thumbnail cache %s", thumb_cache,
                    exc_info=True,
                )
            for source in ("raw", "jpeg"):
                variant = os.path.join(thumb_dir, f"{pid}_{source}.jpg")
                try:
                    if os.path.exists(variant):
                        os.remove(variant)
                except OSError:
                    log.warning(
                        "Failed to remove stale paired-source thumbnail %s",
                        variant,
                        exc_info=True,
                    )
            # ``<pid>_regen.jpg`` / ``<pid>_raw_regen.jpg`` /
            # ``<pid>_jpeg_regen.jpg`` are the sidecars ``serve_thumbnail``
            # falls back to when the default couldn't be unlinked (a
            # persistent lock). Without this pass, editing the recipe
            # while the default stays locked leaves the sidecar carrying
            # the pre-edit pixels; the freshness gate there compares
            # against the unchanged source mtime and re-serves them.
            for stem in (f"{pid}", f"{pid}_raw", f"{pid}_jpeg"):
                sidecar = os.path.join(thumb_dir, f"{stem}_regen.jpg")
                try:
                    if os.path.exists(sidecar):
                        os.remove(sidecar)
                except OSError:
                    log.warning(
                        "Failed to remove stale regeneration sidecar %s",
                        sidecar,
                        exc_info=True,
                    )
            if clear_thumb_path:
                db.conn.execute(
                    "UPDATE photos SET thumb_path = NULL WHERE id = ?", (pid,),
                )
            tracked_sizes = set()
            removed_preview_rows = []
            for row in db.conn.execute(
                "SELECT size FROM preview_cache WHERE photo_id = ?",
                (pid,),
            ).fetchall():
                size_value = row["size"]
                tracked_sizes.add(str(size_value))
                path = os.path.join(preview_dir, f"{pid}_{size_value}.jpg")
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    if os.path.exists(path):
                        _invalid_preview_cache_paths.add(path)
                        _mark_preview_cache_invalid(
                            db, pid, size_value, commit=False,
                        )
                    log.warning(
                        "Failed to remove stale preview cache %s",
                        path, exc_info=True,
                    )
                else:
                    removed_preview_rows.append((pid, size_value))
                    _clear_preview_cache_invalid(
                        db, pid, size_value, commit=False,
                    )
            try:
                for name in os.listdir(preview_dir):
                    if not (name.startswith(f"{pid}_") and name.endswith(".jpg")):
                        continue
                    size_part = name[len(f"{pid}_"):-4]
                    if size_part in tracked_sizes:
                        continue
                    path = os.path.join(preview_dir, name)
                    try:
                        os.remove(path)
                    except OSError:
                        if os.path.exists(path):
                            _invalid_preview_cache_paths.add(path)
                            _mark_preview_cache_invalid(
                                db, pid, size_part, commit=False,
                            )
                        log.warning(
                            "Failed to remove stale preview cache %s",
                            path, exc_info=True,
                        )
                    else:
                        _clear_preview_cache_invalid(
                            db, pid, size_part, commit=False,
                        )
            except FileNotFoundError:
                pass
            if removed_preview_rows:
                db.conn.executemany(
                    "DELETE FROM preview_cache WHERE photo_id = ? AND size = ?",
                    removed_preview_rows,
                )
            original_paths = [
                os.path.join(originals_dir, f"{pid}.jpg"),
                *Path(originals_dir).glob(f"{pid}_*.jpg"),
            ]
            for original_path in original_paths:
                try:
                    if os.path.exists(original_path):
                        os.remove(original_path)
                except OSError:
                    log.warning(
                        "Failed to remove stale original cache %s",
                        original_path,
                    )
            external_cache = os.path.join(external_edits_dir, f"{pid}.jpg")
            external_meta = os.path.join(external_edits_dir, f"{pid}.json")
            for path in (external_cache, external_meta):
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    log.warning(
                        "Failed to remove stale external edit cache %s",
                        path, exc_info=True,
                    )
        db.conn.commit()

    def _queue_edit_recipe_sync(db, photo_id, recipe_json, *, _commit=True):
        """Queue the current non-destructive edit recipe for XMP sync."""
        db.remove_pending_changes(
            photo_id, "edit_recipe", workspace_id=db._ws_id(), _commit=False,
        )
        db.queue_change(
            photo_id, "edit_recipe", recipe_json or "",
            workspace_id=db._ws_id(), _commit=False,
        )
        if _commit:
            db.conn.commit()

    @app.teardown_appcontext
    def _close_db(exc):
        db = g.pop("db", None)
        if db is not None:
            db.conn.close()

    def _reraise_fatal_cleanup_error(exc):
        if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
            raise exc

    def _cleanup_cached_files_for_deleted_photos(files, progress_callback=None):
        try:
            from preview_cache import cleanup_cached_files_for_deleted_photos
            cleanup_cached_files_for_deleted_photos(
                app.config["THUMB_CACHE_DIR"], files, progress_callback=progress_callback,
            )
        except BaseException as exc:
            _reraise_fatal_cleanup_error(exc)
            log.exception("Failed to clean cached files after delete")

    @app.before_request
    def _enforce_api_v1_token():
        if not request.path.startswith("/api/v1/"):
            return None
        expected = app.config.get("API_TOKEN")
        if not expected:
            # No token configured → deny all v1 traffic.
            return json_error("API token not configured", 401)
        supplied = request.headers.get("X-Vireo-Token", "")
        # ``secrets.compare_digest`` raises ``TypeError`` when either str
        # operand contains a non-ASCII code point, which would surface as a
        # 500 for an attacker-supplied token — encode to bytes so a bogus
        # header is a plain 401 like any other wrong value.
        if not secrets.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
            return json_error("Invalid or missing X-Vireo-Token", 401)
        return None


    # Load user config (e.g. HF token) on startup
    import config as cfg

    startup_cfg = cfg.load()
    if startup_cfg.get("hf_token"):
        os.environ["HF_TOKEN"] = startup_cfg["hf_token"]

    # Initialize job runner, log broadcaster, and default collections
    _t0 = time.time()
    init_db = Database(
        db_path,
        initialize_schema=(db_path == ":memory:"),
    )
    log.info("Database init took %.2fs (workspace: %s)", time.time() - _t0,
             init_db.get_workspace(init_db._active_workspace_id)["name"])
    # File-backed startup skips Database's schema initialization. Repair old
    # move parentage here too, before Browse can serve the stale hierarchy.
    init_db.repair_stale_folder_parents()
    # Migrate the legacy 'Needs Classification' default collection BEFORE
    # seeding defaults — otherwise create_default_collections inserts
    # 'Needs Identification' first, then the migration skips renaming
    # because the target name already exists, leaving a duplicate.
    init_db.migrate_default_subject_collection()
    init_db.migrate_default_needs_identification_collection()
    init_db.migrate_default_location_collections()
    # One-shot keyword-name normalization backfill. Database.__init__ only
    # runs it when initialize_schema=True, and every file-backed connection
    # this app opens — including this startup init_db and every per-request
    # connection at `_get_db` — passes initialize_schema=False. Without an
    # explicit run here, an upgraded DB can serve requests with `‘apapane`-
    # style variant rows still present until some background job happens
    # to construct a full `Database()` (initialize_schema=True); in that
    # window an add/rename can miss the legacy row and create duplicate
    # tags or stale XMP. The method is idempotent (db_meta-gated) so
    # subsequent boots are a cheap SELECT.
    init_db.normalize_keyword_data()
    repaired_location_ancestors = init_db.repair_misclassified_location_ancestors()
    if repaired_location_ancestors:
        log.info(
            "Restored %d location hierarchy nodes misclassified as taxonomy",
            repaired_location_ancestors,
        )
    # Ungroup legacy bursts whose stored votes span more than one species.
    # Those rows display one species and, through accept_prediction's
    # vote-winner lookup, tag another; the repair makes them read as the
    # current classifier would have written them. Runs before the first
    # request so no page can render a row this is about to change, and
    # before any accept can act on one. Depends on no taxonomy or config,
    # so unlike the duplicate-species repair it needs no deferral: it is
    # db_meta-gated and self-logging, and later boots pay one marker
    # lookup. Method logs its own totals — the counts are the point, per
    # CORE_PHILOSOPHY.md.
    init_db.repair_mixed_species_prediction_groups()

    # Parsing taxonomy.json is expensive for a full iNaturalist download.
    # Cache the startup instance so overlapping one-time migrations and the
    # immediate background species pass do not each parse it independently.
    _taxonomy_not_loaded = object()
    _startup_taxonomy = _taxonomy_not_loaded

    def _load_startup_taxonomy():
        nonlocal _startup_taxonomy
        # Do not cache a miss: a concurrent first-run taxonomy download can
        # make the file available before the background retry starts.
        if (
            _startup_taxonomy is _taxonomy_not_loaded
            or _startup_taxonomy is None
        ):
            from taxonomy import load_local_taxonomy

            _startup_taxonomy = load_local_taxonomy()
        return _startup_taxonomy

    def _sync_mark_species_only(db, log_label):
        """Load taxonomy and run mark_species_keywords synchronously.

        Returns True when the pass ran (marking either updated rows or
        found nothing to update), False when taxonomy is missing or the
        pass raised. Callers use the return value to gate follow-up work
        that depends on hierarchy leaves being correctly typed as
        taxonomy/is_species.
        """
        tax = _load_startup_taxonomy()
        if tax is None:
            log.debug(
                "[%s] taxonomy not loaded; deferring species marking",
                log_label,
            )
            return False
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info(
                    "[%s] Marked %d keywords as species from taxonomy",
                    log_label, updated,
                )
            return True
        except Exception:
            log.debug(
                "[%s] mark_species_keywords failed",
                log_label, exc_info=True,
            )
            return False

    # Remove same-photo, same-taxon duplicate associations left by the old
    # hierarchy-import + top-level-confirmation interaction. Idempotent and
    # db_meta-gated, so later boots only pay for a single marker lookup.
    #
    # The repair identifies duplicates via
    # ``(is_species = 1 OR type = 'taxonomy') AND (rank = 'species' OR
    # taxon_id IS NULL)``. On upgraded databases a hierarchical species
    # leaf can still be a plain/general row until mark_species_keywords
    # retypes it, so run marking synchronously first — otherwise the
    # repair query cannot see the leaf, removes nothing, and still stamps
    # its one-shot marker; a subsequent background mark_species_keywords
    # pass could then make the leaf eligible while the redundant root
    # association remains permanently skipped. When taxonomy isn't
    # loaded yet (or marking fails), defer the repair to a later boot
    # rather than stamping the marker over an unmarked hierarchy.
    duplicate_repair_key = Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY
    duplicate_repair_pending = init_db.get_meta(duplicate_repair_key) != "1"
    if duplicate_repair_pending:
        # The legacy bug always left a typed top-level species/taxonomy tag
        # beside another tag on the same photo. If no such pair exists, the
        # repair is structurally impossible and it is safe to stamp the
        # one-shot marker without parsing a potentially huge taxonomy file.
        possible_duplicate = init_db.conn.execute(
            """SELECT 1
               FROM photo_keywords species_pk
               JOIN keywords species_k
                 ON species_k.id = species_pk.keyword_id
               WHERE (species_k.is_species = 1
                      OR species_k.type = 'taxonomy')
                 AND EXISTS (
                     SELECT 1
                     FROM photo_keywords other_pk
                     WHERE other_pk.photo_id = species_pk.photo_id
                       AND other_pk.keyword_id != species_pk.keyword_id
                 )
               LIMIT 1"""
        ).fetchone()
        if possible_duplicate is None:
            init_db.set_meta(duplicate_repair_key, "1")
            duplicate_repair_pending = False
            log.info(
                "Skipped duplicate-species startup repair: "
                "no possible duplicate associations"
            )

    if (
        duplicate_repair_pending
        and _sync_mark_species_only(init_db, "sync-startup-species-mark")
    ):
        init_db.repair_duplicate_photo_species()
    # One-time rewrite of the previous miss-threshold defaults (0.25 / 0.15)
    # to the new defaults (0.20 / 0.12) in both ~/.vireo/config.json and
    # workspace overrides. Gated by a marker so it runs once; re-saved
    # legacy values are preserved on subsequent boots.
    cfg.migrate_legacy_miss_thresholds(init_db)
    # One-time rewrite of the previous eye-focus detection default from on to
    # off in both global config and workspace overrides.
    cfg.migrate_eye_detect_default_off(init_db)
    # One-time resolution of the browse.toggle_ui="h" default clashing with
    # any pre-existing browse binding on ``h``. Writes an explicit "" so the
    # user's existing action keeps working; they can re-bind toggle_ui from
    # the shortcuts editor.
    cfg.migrate_toggle_ui_h_conflict()
    # Existing users commonly have the previous Browse card defaults persisted
    # verbatim. Add the new coordinate-source field only for that exact legacy
    # list; customized card layouts remain unchanged.
    cfg.migrate_browse_location_status_field()
    # One-time rename of the "compare" navigation shortcut to "id_conflicts"
    # after the Compare page became ID Conflicts, so a user's saved binding
    # follows the page instead of being orphaned.
    cfg.migrate_compare_nav_id_to_id_conflicts()
    # One-time rewrite of the previous encounter-grouping species weight
    # (0.10) to the new default (0.40) in both ~/.vireo/config.json and
    # workspace overrides. Without this, upgraded installs that had the
    # pipeline block persisted verbatim keep grouping distinct species into
    # one encounter — the intended split behavior only reaches fresh
    # configs. Only the exact legacy value is rewritten; re-saved values
    # are preserved on subsequent boots.
    cfg.migrate_legacy_w_species_default(init_db)
    # One-time rewrite of the global pipeline.default_strategy (legacy
    # hardcoded strategy name) to pipeline.default_process_id (saved_processes
    # id). The workspace-side rewrite happens inside Database(); this covers
    # the global config file so workspaces that inherit the global default
    # don't silently fall back to import-only after upgrade.
    #
    # init_db uses initialize_schema=False for boot perf, so on the first
    # boot after upgrade the saved_processes table isn't guaranteed to
    # exist yet on this connection — the migration would silently defer
    # and any import in this session that would inherit the legacy global
    # default falls back to import-only until the *next* boot. Open a
    # short-lived schema-initializing handle so the migration completes on
    # the very first boot instead. Only pay the schema-init cost if the
    # migration hasn't been stamped yet; subsequent boots short-circuit
    # inside the function and pass ``init_db`` (whose schema state is
    # irrelevant because the marker check runs first).
    if (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID
        not in cfg._migrations_applied(cfg._read_raw())
    ):
        _default_strategy_migration_db = Database(db_path)
        try:
            cfg.migrate_default_strategy_to_process_id(
                _default_strategy_migration_db,
            )
        finally:
            _default_strategy_migration_db.close()
    else:
        cfg.migrate_default_strategy_to_process_id(init_db)
    init_db.create_default_collections_for_all_workspaces()

    # Keep taxonomy typing and duplicate-species repair fresh in the
    # background. Wildlife classification eligibility is stored separately
    # on photos; species marking no longer materializes a Wildlife keyword.
    import threading

    def _retire_wildlife_genre():
        """Run the catalog-wide XMP migration outside startup readiness.

        Large upgraded catalogs can require tens of thousands of sidecar
        reads here. Keeping that work on create_app's calling thread prevents
        the HTTP listener from binding and makes the desktop launcher report
        a false startup failure when its readiness deadline expires.
        """
        retirement_db = None
        started_at = time.time()
        try:
            retirement_db = Database(db_path)
            retired = retirement_db.retire_builtin_wildlife_genre()
            if retired:
                log.info(
                    "Retired the built-in Wildlife genre from %d photo(s)",
                    retired,
                )
            log.info(
                "Wildlife genre retirement finished in %.2fs",
                time.time() - started_at,
            )
            return retired
        except Exception:
            log.exception("Wildlife genre retirement failed")
            return 0
        finally:
            if retirement_db is not None:
                retirement_db.close()

    # Tests and one-shot tools can invoke the pass deterministically without
    # enabling production timers. Production schedules it only after every
    # route has been registered, immediately before create_app returns.
    app._retire_wildlife_genre = _retire_wildlife_genre

    def _mark_species_and_repair(db, log_label):
        """Load taxonomy, mark species keywords, and repair duplicates."""
        tax = _load_startup_taxonomy()
        if tax is None:
            log.debug("[%s] taxonomy not loaded; deferring species marking", log_label)
            return
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info("[%s] Marked %d keywords as species from taxonomy",
                         log_label, updated)
            repaired = db.repair_duplicate_photo_species()
            if repaired:
                log.info(
                    "[%s] Removed %d duplicate root species associations",
                    log_label, repaired,
                )
        except Exception:
            log.debug(
                "[%s] species marking/repair failed", log_label, exc_info=True,
            )

    def _mark_species():
        bg_db = None
        try:
            bg_db = Database(db_path)
        except Exception:
            log.debug("Could not open background db for species marking", exc_info=True)
            return
        try:
            _mark_species_and_repair(bg_db, "background")
        finally:
            bg_db.close()

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        threading.Thread(target=_mark_species, daemon=True).start()

    def _folder_health_loop():
        """Periodically check folder health."""
        import time as _time
        _time.sleep(30)  # Initial delay
        while True:
            health_db = None
            try:
                health_db = Database(db_path)
                changed = health_db.check_folder_health()
                if changed:
                    log.info("Folder health check: %d folder(s) changed status", changed)
                    # A background ok↔missing flip would otherwise leave a
                    # ready /api/photos/missing cache serving the pre-flip
                    # photo list: the modal/banner could offer to delete
                    # rows whose folder just went offline, or hide ghosts
                    # from a folder that just came back, until a later
                    # rescan replaced the entry.
                    _invalidate_missing_originals_cache()
            except Exception:
                log.debug("Folder health check failed", exc_info=True)
            finally:
                if health_db is not None:
                    health_db.close()
            _time.sleep(600)  # 10 minutes

    # Suppressed in tests via ``VIREO_DISABLE_STARTUP_BACKFILL_TIMERS``: the
    # ``app_and_db`` fixture seeds folders at fictional paths like
    # ``/photos/2024`` that don't exist on disk. After the 30s grace period
    # this loop calls ``check_folder_health`` on the tmp_path DB, sees the
    # paths missing, and flips folder status to ``'missing'`` — which causes
    # ``get_photos`` to filter the seeded photos out and any subsequent
    # assertion against them to fail with ``IndexError``. On the slow
    # Windows CI runner the full suite takes ~48 min, so by the time the
    # later predictions/photos tests reach the fixture the timer has long
    # since fired.
    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        threading.Thread(target=_folder_health_loop, daemon=True).start()

    app._job_runner = JobRunner(db=init_db)

    # Sending and source cleanup establish their own exclusive reservations.
    # Control requests remain available while a transfer holds the workspace.
    # The /api/v1 alias loop below adds ``v1_<view>`` for every aliased
    # endpoint listed here, so headless clients get the same exemptions.
    _reservation_exempt_endpoints = {
        "imports.api_send_pending_archive", "workspaces.api_activate_workspace",
        "system.api_shutdown", "system.api_v1_shutdown",
        "jobs.api_job_cancel", "jobs.api_job_pause", "jobs.api_job_resume",
        "jobs.api_jobs_cancel_queued",
        "move_cleanup.source_cleanup",
    }

    @app.before_request
    def _reserve_workspace_mutation():
        if request.method not in {"POST", "PUT", "PATCH", "DELETE"} or not request.path.startswith("/api/"):
            return None
        if request.endpoint in _reservation_exempt_endpoints:
            return None
        target_ws = (request.view_args or {}).get("ws_id")
        # A request with no active workspace and no explicit target has no
        # workspace to reserve; endpoints that handle "no active workspace"
        # themselves (e.g. the offline-banner recheck no-op) must still reach
        # their view function instead of 500ing out of the before_request.
        # Background-job routes that capture ``ctx.workspace_id`` here and
        # hand it to a worker (scan, import-full, import-photos,
        # import-in-place) must reject the no-workspace case themselves so
        # they do not commit catalog rows invisible to every workspace.
        active_ws = _get_db()._active_workspace_id
        workspaces = set()
        if active_ws is not None:
            workspaces.add(active_ws)
        if target_ws is not None:
            workspaces.add(target_ws)
        if not workspaces:
            return None
        with contextlib.ExitStack() as reservation:
            for workspace_id in sorted(workspaces):
                reservation.enter_context(app._job_runner.workspace_mutation(
                    workspace_id,
                    exclusive=(
                        request.endpoint == "workspaces.api_delete_workspace"
                        and workspace_id == target_ws
                    ),
                ))
            g.nas_workspace_mutation = reservation.pop_all()
        return None

    @app.teardown_request
    def _release_workspace_mutation(exc):
        reservation = g.pop("nas_workspace_mutation", None)
        if reservation is not None:
            reservation.__exit__(None, None, None)
    # XMP sidecars are read-modify-written files; serialize sync jobs so
    # repeated clicks cannot race while touching the same sidecar.
    app._sync_job_lock = threading.Lock()
    app._log_broadcaster = LogBroadcaster(buffer_size=500)
    app._log_broadcaster.install()

    def _cleanup_app_resources(job_timeout=10.0):
        try:
            jobs_stopped = app._job_runner.shutdown(timeout=job_timeout)
        except Exception:
            jobs_stopped = False
            log.exception("Failed to shut down background jobs cleanly")
        try:
            app._log_broadcaster.uninstall()
        except Exception:
            log.exception("Failed to uninstall log broadcaster during cleanup")
        try:
            init_db.close()
        except Exception:
            log.exception("Failed to close database during cleanup")
        return jobs_stopped

    app._cleanup_app_resources = _cleanup_app_resources

    # Live progress of the most recent new-images walk, keyed by
    # (db_path, workspace_id). Written by the walk's progress callback and
    # read by the GET/POST endpoints so a ``pending`` response can say
    # "38,000 files checked, 2,100 new so far" instead of a bare spinner —
    # the transparency the banner-click path needs on multi-minute walks
    # over large network volumes. Values are per-spawn dicts; a new walk
    # replaces the key wholesale, so readers never see torn state.
    app._new_images_walk_progress = {}
    app._missing_originals_lock = threading.Lock()
    app._missing_originals_cache = {}
    app._missing_originals_inflight = {}
    app._missing_originals_errors = {}
    # Monotonic per-key counter bumped whenever the cache is invalidated
    # while a scan is in flight. Each scan snapshots this at start; if the
    # counter has advanced by the time it finishes, the scan's results are
    # from a pre-invalidation view of the library and must be discarded so
    # deleted photos don't reappear in the banner/modal.
    app._missing_originals_generation = {}

    # Working copies are generated by imports, scans, and on-demand reads.
    # Do not warm the library-wide cache at startup: it consumes disk and
    # CPU for photos the user has not requested.

    # ----- thumb_path self-healing backfill -----
    # The dashboard's coverage card counts thumbnails by ``thumb_path IS NOT
    # NULL``, but for a long stretch the column was never populated by
    # production code, so libraries with 40k JPEGs cached on disk reported
    # "0 thumbnails" forever. This pass aligns the column with disk reality
    # for legacy rows, and clears it for photos whose cached file has since
    # been deleted (drift correction).
    #
    # Same shape as the working-copy backfill above: ephemeral JobRunner
    # job (so it shows in the bottom panel), never written to job_history,
    # skipped entirely when a fast count check finds nothing to do.
    def _kickoff_thumb_path_backfill():
        from thumbnails import (
            backfill_thumb_paths,
            thumb_path_backfill_candidate_count,
        )

        tpdb = None
        try:
            tpdb = Database(db_path)
            candidate_count = thumb_path_backfill_candidate_count(
                tpdb, app.config["THUMB_CACHE_DIR"],
            )
        except Exception:
            log.exception("thumb_path backfill: candidate check failed")
            return
        finally:
            if tpdb is not None:
                tpdb.close()
        if candidate_count == 0:
            log.debug("thumb_path backfill: no candidates, skipping")
            return

        runner = app._job_runner
        cache_dir = app.config["THUMB_CACHE_DIR"]

        def work(job):
            thread_db = Database(db_path)
            try:
                active_ws = init_db._active_workspace_id
                if active_ws is not None:
                    thread_db.set_active_workspace(active_ws)

                def progress_cb(current, total):
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    runner.push_event(
                        job["id"],
                        "progress",
                        {
                            "current": current,
                            "total": total,
                            "phase": f"{current:,} / {total:,} photos reconciled",
                        },
                    )

                def status_cb(message, **_phase):
                    runner.push_event(job["id"], "progress", {
                        "phase": message,
                        "current": job["progress"].get("current", 0),
                        "total": job["progress"].get("total", 0),
                    })

                def cancel_check():
                    return runner.is_cancelled(job["id"])

                return backfill_thumb_paths(
                    thread_db, cache_dir,
                    progress_callback=progress_cb,
                    status_callback=status_cb,
                    cancel_check=cancel_check,
                )
            finally:
                thread_db.close()

        try:
            runner.start(
                "thumb_path_backfill", work,
                ephemeral=True,
                config={"trigger": "startup"},
            )
        except Exception:
            log.exception("Failed to start thumb_path backfill job")

    app._kickoff_thumb_path_backfill = _kickoff_thumb_path_backfill

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        _thumb_backfill_timer = threading.Timer(6.0, _kickoff_thumb_path_backfill)
        _thumb_backfill_timer.daemon = True
        _thumb_backfill_timer.start()

    # -- Page routes --

    app.register_blueprint(create_pages_blueprint(_get_db))

    # -- API routes --

    def _request_flag_filter():
        flag = request.args.get("flag", None)
        if flag in (None, ""):
            return None
        if flag not in ("none", "flagged", "rejected"):
            raise ValueError("flag must be 'none', 'flagged', or 'rejected'")
        return flag

    def _request_photo_ids_arg():
        """Parse the optional ``photo_ids`` query param (comma-separated ints).

        Returns None when absent so callers keep their unscoped behaviour, and
        raises ValueError on a malformed value so a typo surfaces as a 400
        rather than silently widening the query to the whole workspace.

        Capped at the same 1000 ids ``parse_selection_photo_ids`` allows:
        ``/api/predictions`` runs one ``_photo_in_workspace`` query per id, so
        an unbounded list turns a single GET into unbounded database work.
        """
        raw = request.args.get("photo_ids")
        if raw is None or raw.strip() == "":
            return None
        ids = []
        seen = set()
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                pid = int(part)
            except ValueError as exc:
                raise ValueError("photo_ids must be integers") from exc
            if pid not in seen:
                ids.append(pid)
                seen.add(pid)
        if not ids:
            raise ValueError("photo_ids must contain at least one id")
        if len(ids) > 1000:
            raise ValueError("too many photo_ids")
        return ids

    # Resolves visual-search clauses; owns the per-app query-text
    # embedding cache, so every route shares one instance.
    visual_scope = VisualScope()

    def _request_location_status_filter():
        value = (request.args.get("location_status") or "").strip().lower()
        if not value:
            return None
        if value not in {"exif", "assigned", "none"}:
            raise ValueError(
                "location_status must be 'exif', 'assigned', or 'none'"
            )
        return value



    _MISSING_ORIGINALS_STALE_SECONDS = 30 * 60
    _MISSING_ORIGINALS_BACKOFF_SECONDS = 30 * 60
    _MISSING_ORIGINALS_HEAVY_JOB_TYPES = {
        "scan",
        "pipeline",
        "thumbnails",
        "previews",
        "move-photos",
        "move-folder",
        "sync",
        "classify",
        "precompute-embeddings",
        "cull",
        "develop",
        "extract-masks",
        "regroup",
        "import",
        "import-full",
        "import-in-place",
        "ingest",
        "import-photos",
        "batch-delete",
        "duplicate-scan",
        "offline-cache",
        # Navbar's new-images probe walks the same folders a missing-originals
        # scan would; letting them run concurrently can double the filesystem
        # load on slow NAS/SMB libraries.
        "new_images_walk",
        # Folder-scoped and workspace-wide missing-originals scans have
        # distinct cache keys, so the same-key in-flight coalescing does
        # not catch a workspace scan started while a folder scan is
        # running (or vice versa). Treat any in-flight
        # missing_originals_scan as heavy work so automatic reruns
        # don't kick off a second filesystem walk over the same tree.
        "missing_originals_scan",
        # audit.verify_hashes walks every workspace source file and
        # hashes readable ones — the same NAS/SMB trees a Missing
        # Originals scan touches. Letting the 30-minute automatic
        # missing-originals timer fire during verification would
        # double the I/O on those slow volumes.
        "verify-hashes",
        # Card cleanup reads only archive copies that match one card, but
        # those reads still hit the same NAS/SMB trees.
        "card-cleanup-verify",
    }

    def _utc_iso_now():
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _parse_missing_originals_folder_id(db):
        folder_id = request.args.get("folder_id")
        if request.is_json:
            body = request.get_json(silent=True) or {}
            if "folder_id" in body:
                folder_id = body.get("folder_id")
        if folder_id in (None, ""):
            return None
        try:
            folder_id = int(folder_id)
        except (TypeError, ValueError):
            raise ValueError("folder_id must be an integer") from None
        linked = db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (db._active_workspace_id, folder_id),
        ).fetchone()
        if not linked:
            raise LookupError("folder not found")
        return folder_id

    def _missing_originals_key(db, folder_id):
        return (db._db_path, db._active_workspace_id, folder_id)

    def _missing_originals_payload(db, folder_id):
        key = _missing_originals_key(db, folder_id)
        now = time.monotonic()
        with app._missing_originals_lock:
            entry = app._missing_originals_cache.get(key)
            inflight = app._missing_originals_inflight.get(key)
            err = app._missing_originals_errors.get(key)
            # An error recorded after the last cached scan means a later
            # refresh failed. Returning the pre-refresh photo list as a
            # fresh "ready" result would hide the failure — and worse,
            # let the user delete rows whose originals may have been
            # restored between scans. When a scan is in flight the UI
            # already shows "pending", so still surface the stale
            # entry then; otherwise prefer the error state.
            cache_superseded_by_error = (
                entry is not None
                and err is not None
                and not inflight
                and err["set_at"] > entry["set_at"]
            )
            if entry is not None and not cache_superseded_by_error:
                status = "pending" if inflight else "ready"
                photos = entry["photos"]
                checked_at = entry["checked_at"]
                stale = now - entry["set_at"] > _MISSING_ORIGINALS_STALE_SECONDS
                error = None
            elif inflight:
                status = "pending"
                photos = []
                checked_at = None
                stale = False
                error = None
            elif err is not None:
                status = "error"
                photos = []
                checked_at = err["checked_at"]
                stale = False
                error = err["error"]
            else:
                status = "not_ready"
                photos = []
                checked_at = None
                stale = False
                error = None
            backoff_seconds = 0
            if err is not None:
                backoff_seconds = max(0, int(err["backoff_until"] - now))
            return {
                "status": status,
                "pending": bool(inflight),
                "checked_at": checked_at,
                "stale": stale,
                "error": error,
                "job_id": inflight if isinstance(inflight, str) else None,
                "photos": photos,
                "backoff_seconds": backoff_seconds,
                "workspace_id": db._active_workspace_id,
                "folder_id": folder_id,
            }

    def _build_missing_originals_rows(
        db,
        folder_id=None,
        progress_callback=None,
        cancel_callback=None,
    ):
        thumb_dir = app.config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_dir)
        preview_dir = os.path.join(vireo_dir, "previews")
        working_dir = os.path.join(vireo_dir, "working")

        def check_cancelled():
            if cancel_callback is not None and cancel_callback():
                raise MissingPhotosCancelled("missing originals scan cancelled")

        # Index preview cache once. The endpoint is polled from the navbar,
        # so per-photo `glob(preview_dir, f"{pid}_*.jpg")` was O(missing ×
        # cache_size) readdirs per request. Build {pid} from a single
        # listdir and check the set in O(1) per row.
        preview_pids: set[int] = set()
        try:
            check_cancelled()
            with os.scandir(preview_dir) as it:
                for entry in it:
                    check_cancelled()
                    name = entry.name
                    # Match `{id}.jpg` (legacy full preview) or `{id}_{size}.jpg`
                    # (sized variant). Anything else is not part of the per-photo
                    # cache and should be ignored.
                    if not name.endswith(".jpg"):
                        continue
                    head = name[:-4].split("_", 1)[0]
                    if head.isdigit():
                        preview_pids.add(int(head))
        except FileNotFoundError:
            pass  # cache dir hasn't been created yet — no previews

        out = []
        for row in db.get_missing_photos(
            folder_id=folder_id,
            progress_callback=progress_callback,
            cancel_callback=cancel_callback,
        ):
            check_cancelled()
            pid = row["id"]
            src = os.path.join(row["folder_path"], row["filename"])
            stem, _ext = os.path.splitext(src)
            # Working copy: the DB path wins when set, but legacy rows from
            # before working_copy_path was tracked can still have a file at
            # the default <vireo>/working/<id>.jpg location — and the batch
            # delete path cleans that up regardless of the DB column. If we
            # only consulted the column the badge would lie about what's
            # about to be removed.
            wc_rel = row["working_copy_path"]
            default_wc = os.path.join(working_dir, f"{pid}.jpg")
            if wc_rel:
                has_wc = os.path.isfile(os.path.join(vireo_dir, wc_rel))
            else:
                has_wc = os.path.isfile(default_wc)
            out.append({
                "id": pid,
                "filename": row["filename"],
                "extension": row["extension"],
                "folder_id": row["folder_id"],
                "folder_path": row["folder_path"],
                "timestamp": row["timestamp"],
                "file_size": row["file_size"],
                "has_thumb": os.path.isfile(os.path.join(thumb_dir, f"{pid}.jpg")),
                "has_preview": pid in preview_pids,
                "has_working_copy": has_wc,
                "has_xmp_sidecar": (
                    os.path.isfile(stem + ".xmp")
                    or os.path.isfile(stem + ".XMP")
                    or os.path.isfile(src + ".xmp")
                    or os.path.isfile(src + ".XMP")
                ),
            })
        attach_nested_edit_recipes(db, out)
        return out

    def _missing_originals_heavy_job_active():
        for job in app._job_runner.list_jobs():
            if job.get("status") not in (
                "running", "pausing", "paused", "queued",
            ):
                continue
            if job.get("type") in _MISSING_ORIGINALS_HEAVY_JOB_TYPES:
                return True
        return False

    def _pending_local_workspace_transition(workspace_id, db=None):
        """Return the queued/running local-workspace transition job, or None.

        ``db``: pass an explicit Database when calling off the request
        thread (job threads have no request context); defaults to the
        request-scoped db via ``_get_db()``.
        """
        # ``has_local_workspace`` only observes the ``local_workspaces`` row
        # a stage worker inserts once it actually runs; a stage/sync/discard
        # that has been enqueued but not yet reached that insert would leave
        # the row absent. A scan or move-folder enqueued in that window
        # passes its own guard and then rebases the catalog after the
        # transition worker later claims the workspace, so the folder /
        # workspace_folders / folders rows those jobs write end up outside
        # the manifest and local_workspace_folders. Detecting the pending
        # transition job in the runner queue closes that race at enqueue.
        if workspace_id is None:
            return None
        for job in app._job_runner.list_jobs():
            if job.get("status") not in (
                "queued", "running", "pausing", "paused",
            ):
                continue
            job_type = job.get("type")
            if job_type in LOCAL_WORKSPACE_JOB_TYPES and job.get("workspace_id") == workspace_id:
                return job
            if job_type in LOCAL_FOLDER_JOB_TYPES:
                if job.get("workspace_id") == workspace_id:
                    return job
                config = job.get("config") or {}
                root_ids = (config.get("root_folder_ids") or []) if isinstance(config, dict) else []
                if db is None:
                    db = _get_db()
                if any(
                    workspace_id in workspace_ids_for_folder_tree(db, int(root_id))
                    for root_id in root_ids
                ):
                    return job
        return None

    def _invalidate_missing_originals_cache(workspace_ids=None):
        """Drop cached Missing Originals results for this app's database.

        Photos are shared across workspaces (a folder can be linked into
        more than one), so a photo-row removal must clear every
        workspace cache that could still list it — scoping to the
        active workspace lets other workspaces keep serving stale
        ready payloads until their next scan.

        ``workspace_ids`` narrows the invalidation to those workspace
        ids. Use it on workspace create/delete to clear entries that
        could otherwise be served to a later workspace that reuses a
        SQLite rowid.
        """
        ws_filter = None if workspace_ids is None else {int(w) for w in workspace_ids}
        with app._missing_originals_lock:
            for store in (
                app._missing_originals_cache,
                app._missing_originals_errors,
            ):
                for key in list(store.keys()):
                    if key[0] != db_path:
                        continue
                    if ws_filter is not None and key[1] not in ws_filter:
                        continue
                    store.pop(key, None)
            # Bump generation for every in-flight scan under this DB so
            # its completion path refuses to write its stale
            # pre-invalidation snapshot back into the cache.
            for key in list(app._missing_originals_inflight.keys()):
                if key[0] != db_path:
                    continue
                if ws_filter is not None and key[1] not in ws_filter:
                    continue
                app._missing_originals_generation[key] = (
                    app._missing_originals_generation.get(key, 0) + 1
                )

    def _start_missing_originals_scan(db, folder_id=None, automatic=False):
        key = _missing_originals_key(db, folder_id)
        scan_started_at = time.monotonic()
        now = scan_started_at
        token = object()
        suppressed_reason = None
        reuse_existing = False
        fresh_cache = False
        with app._missing_originals_lock:
            inflight = app._missing_originals_inflight.get(key)
            if inflight:
                reuse_existing = True
            entry = app._missing_originals_cache.get(key)
            # Gate on when the last scan STARTED, not when it finished. The
            # navbar re-arms its 30-minute automatic timer from POST time,
            # so a scan that takes real wall-clock time to walk the disk
            # leaves ``set_at`` well under the threshold when the next tick
            # arrives — every other automatic scan would otherwise be
            # skipped, and deletions could stay undiscovered for nearly an
            # hour. Legacy entries without ``started_at`` fall back to
            # ``set_at``.
            if (
                not reuse_existing
                and automatic
                and entry is not None
                and now - entry.get("started_at", entry["set_at"])
                < _MISSING_ORIGINALS_STALE_SECONDS
            ):
                fresh_cache = True
            err = app._missing_originals_errors.get(key)
            if (
                not reuse_existing
                and not fresh_cache
                and automatic
                and err is not None
                and now < err["backoff_until"]
            ):
                suppressed_reason = "backoff"
        if reuse_existing:
            return _missing_originals_payload(db, folder_id)
        if fresh_cache:
            return _missing_originals_payload(db, folder_id)
        if automatic and suppressed_reason is None and _missing_originals_heavy_job_active():
            suppressed_reason = "heavy_job_active"
        if suppressed_reason is not None:
            payload = _missing_originals_payload(db, folder_id)
            payload["suppressed"] = True
            payload["reason"] = suppressed_reason
            if suppressed_reason == "heavy_job_active":
                payload["status"] = "skipped"
            return payload
        scan_generation = 0
        with app._missing_originals_lock:
            inflight = app._missing_originals_inflight.get(key)
            if inflight:
                reuse_existing = True
            else:
                app._missing_originals_inflight[key] = token
                scan_generation = app._missing_originals_generation.get(key, 0)
        if reuse_existing:
            return _missing_originals_payload(db, folder_id)

        runner = app._job_runner
        ws_id = db._active_workspace_id
        db_file = db._db_path
        scope_label = "workspace" if folder_id is None else f"folder #{folder_id}"

        def work(job):
            thread_db = None
            try:
                thread_db = Database(db_file)
                if ws_id is not None:
                    thread_db.set_active_workspace(ws_id)

                def progress(payload):
                    current = int(payload.get("photos_considered") or 0)
                    total = int(payload.get("total_photos") or 0)
                    missing_found = int(payload.get("missing_found") or 0)
                    folders_checked = int(payload.get("folders_checked") or 0)
                    current_folder = payload.get("current_folder") or ""
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    job["progress"]["current_file"] = current_folder
                    phase = (
                        f"{folders_checked:,} folders checked, "
                        f"{current:,} photos considered, "
                        f"{missing_found:,} missing"
                    )
                    runner.push_event(job["id"], "progress", {
                        "current": current,
                        "total": total,
                        "current_file": current_folder,
                        "folders_checked": folders_checked,
                        "missing_found": missing_found,
                        "phase": phase,
                    })

                def cancel_check():
                    return runner.is_cancelled(job["id"])

                photos = _build_missing_originals_rows(
                    thread_db,
                    folder_id=folder_id,
                    progress_callback=progress,
                    cancel_callback=cancel_check,
                )
                if cancel_check():
                    return {"cancelled": True, "scope": scope_label}
                checked_at = _utc_iso_now()
                stale = False
                with app._missing_originals_lock:
                    current_gen = app._missing_originals_generation.get(key, 0)
                    if cancel_check():
                        return {"cancelled": True, "scope": scope_label}
                    if current_gen != scan_generation:
                        # A batch delete (or other invalidation) fired
                        # while this scan was walking the disk. Its photo
                        # list reflects the pre-delete library, so writing
                        # it back would resurrect just-removed photos in
                        # the banner. Drop the result and let the next
                        # scan recompute.
                        stale = True
                    else:
                        app._missing_originals_cache[key] = {
                            "photos": photos,
                            "checked_at": checked_at,
                            "set_at": time.monotonic(),
                            "started_at": scan_started_at,
                        }
                        app._missing_originals_errors.pop(key, None)
                return {
                    "missing_count": len(photos),
                    "checked_at": checked_at,
                    "scope": scope_label,
                    "stale": stale,
                }
            except MissingPhotosCancelled:
                raise
            except Exception as exc:
                checked_at = _utc_iso_now()
                with app._missing_originals_lock:
                    current_gen = app._missing_originals_generation.get(key, 0)
                    if current_gen == scan_generation:
                        app._missing_originals_errors[key] = {
                            "error": str(exc) or exc.__class__.__name__,
                            "checked_at": checked_at,
                            "set_at": time.monotonic(),
                            "backoff_until": (
                                time.monotonic()
                                + _MISSING_ORIGINALS_BACKOFF_SECONDS
                            ),
                        }
                raise
            finally:
                if thread_db is not None:
                    thread_db.close()
                with app._missing_originals_lock:
                    if app._missing_originals_inflight.get(key) in (
                        token,
                        job["id"],
                    ):
                        app._missing_originals_inflight.pop(key, None)

        try:
            job_id = runner.start(
                "missing_originals_scan",
                work,
                workspace_id=ws_id,
                config={"scope": scope_label, "folder_id": folder_id},
                ephemeral=False,
                counts_for_badge=True,
            )
        except Exception:
            with app._missing_originals_lock:
                if app._missing_originals_inflight.get(key) is token:
                    app._missing_originals_inflight.pop(key, None)
            raise

        with app._missing_originals_lock:
            if app._missing_originals_inflight.get(key) is token:
                app._missing_originals_inflight[key] = job_id
        payload = _missing_originals_payload(db, folder_id)
        if payload.get("status") != "ready":
            payload["job_id"] = job_id
            payload["pending"] = True
            payload["status"] = "pending"
        return payload









    def _normalize_photo_id_list(raw_ids):
        """Validate and de-dupe a JSON ``photo_ids`` list, preserving order."""
        if not isinstance(raw_ids, list) or not raw_ids:
            return None, json_error("photo_ids required", 400)
        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return None, json_error("photo_ids must contain only integers", 400)
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        if not photo_ids:
            return None, json_error("photo_ids required", 400)
        return photo_ids, None

    def _gps_location_chunks(values, size=800):
        values = list(values)
        for idx in range(0, len(values), size):
            yield values[idx:idx + size]

    def _location_keyword_photo_ids(db, photo_ids):
        """Return ids that already have any linked location keyword."""
        if not photo_ids:
            return set()
        found = set()
        for chunk in _gps_location_chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                "SELECT DISTINCT pk.photo_id "
                "FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                f"WHERE k.type = 'location' AND pk.photo_id IN ({placeholders})",
                chunk,
            ).fetchall()
            found.update(row["photo_id"] for row in rows)
        return found

    _REVERSE_GEOCODE_CACHE_LANGUAGE_KEY = "_vireo_result_language"

    def _google_reverse_geocode(lat, lng, api_key, language):
        """Reverse-geocode while preserving the wrapper's default-on API."""
        if language == "en":
            return places.reverse_geocode(lat, lng, api_key)
        return places.reverse_geocode(lat, lng, api_key, language=None)

    _REVERSE_GEOCODE_CACHE_LEGACY = object()

    def _decode_cached_reverse_geocode(cached, language):
        """Return ``(matches_language, details)`` for a cached response.

        Rows written before the language field existed have no
        ``_vireo_result_language`` key at all; treat those as compatible
        with the current preference so a rollout does not silently
        invalidate every previously cached lookup — and, when no API
        key is configured, force the caller into the ``no_api_key``
        branch instead of reusing the cached result. Post-PR writes
        always include the key (``null`` for the opt-out preference,
        ``"en"`` for the default), so subsequent preference changes
        still invalidate correctly.
        """
        try:
            details = json.loads(cached["response"] or "{}")
        except (ValueError, TypeError):
            details = {}
        if not isinstance(details, dict):
            details = {}
        cached_language = details.pop(
            _REVERSE_GEOCODE_CACHE_LANGUAGE_KEY,
            _REVERSE_GEOCODE_CACHE_LEGACY,
        )
        if cached_language is _REVERSE_GEOCODE_CACHE_LEGACY:
            return True, details
        return cached_language == language, details

    def _encode_cached_reverse_geocode(details, language):
        """Serialize details with the requested language for cache matching.

        The language key is always written — including as ``null`` for
        the opt-out preference — so the decoder can distinguish a
        legacy untagged row from a row explicitly written under
        ``language=None``.
        """
        payload = dict(details) if isinstance(details, dict) else {}
        payload[_REVERSE_GEOCODE_CACHE_LANGUAGE_KEY] = language
        return json.dumps(payload)

    def _resolve_exif_place_for_photo(
        db, photo, api_key, language, grid_cache,
    ):
        """Resolve one photo's EXIF coordinates into normalized place details.

        Returns ``(details, reason)`` where ``details`` is the normalized
        Google-place dict on success and ``reason`` is a short unresolved code
        on failure. Results are de-duped by the DB's reverse-geocode grid so a
        burst in the same cell only performs/cache-checks one lookup.
        """
        lat = photo["latitude"]
        lng = photo["longitude"]
        if lat is None or lng is None:
            return None, "missing_gps"
        try:
            lat = float(lat)
            lng = float(lng)
        except (TypeError, ValueError):
            return None, "invalid_gps"
        if not (math.isfinite(lat) and math.isfinite(lng)):
            return None, "invalid_gps"

        grid = Database._reverse_geocode_grid(lat, lng)
        if grid in grid_cache:
            return grid_cache[grid]

        cached = db.reverse_geocode_cache_get(lat, lng)
        if cached is not None:
            language_matches, details = _decode_cached_reverse_geocode(
                cached, language,
            )
            if language_matches:
                if cached["place_id"] is None:
                    result = (None, "no_match")
                    grid_cache[grid] = result
                    return result
                if not details.get("place_id"):
                    details["place_id"] = cached["place_id"]
                if details.get("place_id"):
                    result = (details, None)
                else:
                    result = (None, "no_match")
                grid_cache[grid] = result
                return result

        if not api_key:
            result = (None, "no_api_key")
            grid_cache[grid] = result
            return result

        try:
            details = _google_reverse_geocode(
                lat, lng, api_key, language,
            )
        except places.PlacesTransientError:
            app.logger.warning(
                "bulk reverse_geocode transient failure for photo=%s lat=%s lng=%s",
                photo["id"],
                lat,
                lng,
            )
            result = (None, "transient_error")
            grid_cache[grid] = result
            return result

        cache_place_id = details.get("place_id") if details else None
        db.reverse_geocode_cache_put(
            lat,
            lng,
            place_id=cache_place_id,
            response_json=_encode_cached_reverse_geocode(details, language),
        )
        if not details or not details.get("place_id"):
            result = (None, "no_match")
        else:
            result = (details, None)
        grid_cache[grid] = result
        return result

    def _bulk_gps_location_source_ids(db, body):
        """Return source photo ids from either ``photo_ids`` or ``collection_id``."""
        raw_ids = body.get("photo_ids")
        if raw_ids:
            return _normalize_photo_id_list(raw_ids)

        collection_id = coerce_collection_id(body.get("collection_id"))
        if collection_id is False:
            return None, json_error("collection_id must be an integer", 400)
        if collection_id is None:
            return None, json_error("photo_ids or collection_id required", 400)

        row = db.conn.execute(
            "SELECT id, visual_json FROM collections "
            "WHERE id = ? AND workspace_id = ?",
            (collection_id, db._ws_id()),
        ).fetchone()
        if row is None:
            return None, json_error("collection not found", 404)
        # get_collection_photo_ids evaluates ``rules`` only; a visual-only
        # collection would silently expand to every metadata match. The
        # picker filters these out, but reject here as the boundary.
        if row["visual_json"] is not None:
            return None, json_error(VISUAL_COLLECTION_MSG, 400)
        return db.get_collection_photo_ids(collection_id), None

    def _bulk_gps_location_payload(db, body, cancel_check=None):
        """Build preview/apply data for resolving locations from EXIF GPS."""
        photo_ids, error = _bulk_gps_location_source_ids(db, body)
        if error is not None:
            return None, error
        if not photo_ids:
            return {
                "total": 0,
                "resolvable": 0,
                "updated": 0,
                "groups": [],
                "unresolved": [],
                "skipped": [],
            }, None
        photos_map = db.get_photos_by_ids(photo_ids)
        if len(photos_map) != len(photo_ids):
            return None, json_error("One or more photos were not found", 404)
        for pid in photo_ids:
            edit_error = location_errors.photo_location_edit_error(db, pid)
            if edit_error is not None:
                return None, edit_error

        assigned_ids = _location_keyword_photo_ids(db, photo_ids)
        import config as cfg
        maps_config = cfg.load()
        api_key = (maps_config.get("google_maps_api_key", "") or "").strip()
        language = places.result_language(maps_config)

        grid_cache = {}
        groups = {}
        unresolved = []
        skipped = []
        ordered_group_keys = []
        cancelled = False
        for pid in photo_ids:
            if cancel_check is not None and cancel_check():
                cancelled = True
                break
            photo = photos_map[pid]
            if pid in assigned_ids:
                skipped.append({
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "reason": "already_has_location",
                })
                continue
            details, reason = _resolve_exif_place_for_photo(
                db, photo, api_key, language, grid_cache,
            )
            if reason is not None:
                unresolved.append({
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "reason": reason,
                })
                continue

            place_id = details.get("place_id")
            if place_id not in groups:
                groups[place_id] = {
                    "place_id": place_id,
                    "summary": _summarize_details(details),
                    "name": details.get("name") or "",
                    "details": details,
                    "photo_ids": [],
                    "sample_filenames": [],
                }
                ordered_group_keys.append(place_id)
            group = groups[place_id]
            group["photo_ids"].append(pid)
            if len(group["sample_filenames"]) < 3:
                group["sample_filenames"].append(photo["filename"])

        group_list = []
        for place_id in ordered_group_keys:
            group = groups[place_id]
            group_list.append({
                "place_id": group["place_id"],
                "summary": group["summary"],
                "name": group["name"],
                "count": len(group["photo_ids"]),
                "photo_ids": group["photo_ids"],
                "sample_filenames": group["sample_filenames"],
            })

        result = {
            "total": len(photo_ids),
            "resolvable": sum(group["count"] for group in group_list),
            "updated": 0,
            "groups": group_list,
            "unresolved": unresolved,
            "skipped": skipped,
            "_details_by_place_id": {k: v["details"] for k, v in groups.items()},
        }
        if cancelled:
            result["cancelled"] = True
        return result, None

    # -- Edit API routes --

        return jsonify({"ok": True})

    def _effective_category_resolver(db, photo_ids):
        """Build ``(photo_id, species) -> category`` against *current* keywords.

        ``predictions.category`` is a snapshot of how the prediction compared
        to the photo's keywords at classification time, and nothing rewrites
        it when keywords change afterwards (the only writers are the classify
        path and duplicate merge). So a photo that gained a Robin keyword
        after a pending Sparrow prediction was stored as ``new`` still reads
        ``new`` — and Browse would offer a bare Accept that tags a species
        conflicting with what the photo already says. ``CORE_PHILOSOPHY.md``
        forbids exactly that: the button must mean what the user reads it as.

        Returns ``match``/``new``/``refinement``/``broader``/``conflict`` from
        ``compare_prediction_to_keywords`` — Compare's vocabulary, because
        this is Compare's computation, shared rather than reimplemented (see
        ``api_predictions_compare``). Callers treat
        ``refinement``/``broader``/``conflict`` as ambiguous, the same set
        Browse's ``predictionIsAmbiguous`` refuses to offer a bare Accept for.

        Two details are load-bearing and are the reason this goes through the
        same helpers Compare uses rather than a raw keyword query:

        * ``get_species_keywords_for_photos`` canonicalizes a hierarchy alias
          through its linked taxon's root, and ``resolve_species_display_name``
          does the same for the prediction label. Comparing raw
          ``keywords.name`` text would make a photo tagged with the leaf
          ``Desert Verdin`` read as *conflicting* with a ``Verdin``
          prediction whenever the taxonomy file is unavailable — inventing an
          ambiguity and sending a settled photo to Review.
        * the comparison runs on the species the accept path would actually
          apply (the burst consensus), not the row's own label.

        Returns None when no comparison is possible (compare or the photo set
        unavailable) so callers can fall back to the stored snapshot.
        """
        photo_ids = [pid for pid in dict.fromkeys(photo_ids) if pid is not None]
        if not photo_ids:
            return None
        try:
            from compare import compare_prediction_to_keywords
        except Exception:
            return None
        # Cached by mtime inside load_local_taxonomy, so this is a lookup on
        # the hot path rather than a re-parse per request. None degrades
        # compare_prediction_to_keywords to exact-text matching, which is
        # still current-state truth — better than a stale column either way,
        # and a missing or corrupt taxonomy file must never hard-fail the
        # endpoint.
        try:
            from taxonomy import load_local_taxonomy
            taxonomy = load_local_taxonomy()
        except Exception:
            taxonomy = None
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        species_by_photo = db.get_species_keywords_for_photos(photo_ids, include_identities=True)
        resolved = {}
        cache = {}

        def comparison_name(species, identity=None):
            identity = identity or resolver.display(species)
            if identity.scientific_name:
                return identity.scientific_name
            if species not in resolved:
                resolved[species] = db.resolve_species_display_name(identity.display_name)
            return resolved[species]

        keyword_names = {}
        for photo_id, entries in species_by_photo.items():
            names = []
            for entry in entries:
                source = {"taxon_id": int(entry["key"][6:])} if entry["key"].startswith("taxon:") else None
                identity = resolver.resolve(entry["name"], source=source) if source else resolver.display(entry["name"])
                names.append(comparison_name(entry["name"], identity))
            keyword_names[photo_id] = names

        def _category(photo_id, species, identity=None):
            if not species or photo_id is None:
                return None
            identity = identity or resolver.display(species)
            key = (photo_id, identity.key)
            if key not in cache:
                if any(entry["key"] == identity.key for entry in species_by_photo.get(photo_id, [])):
                    cache[key] = "match"
                else:
                    comparison = compare_prediction_to_keywords(
                        comparison_name(species, identity),
                        keyword_names.get(photo_id, []),
                        taxonomy,
                    )
                    cache[key] = (
                        comparison.get("category")
                        if isinstance(comparison, dict) else None
                    )
            return cache[key]

        return _category

    _EFFECTIVE_AMBIGUOUS_CATEGORIES = frozenset(
        {"refinement", "broader", "conflict"}
    )
    # Stored-snapshot categories that mean the same thing, used only when no
    # fresh comparison is available.
    _STORED_AMBIGUOUS_CATEGORIES = frozenset({"disagreement", "refinement"})

    def _prediction_is_ambiguous(effective_category, stored_category):
        """Would a bare Accept here be dishonest?

        The fresh comparison wins outright when there is one. ORing it with
        the stored snapshot would make ambiguity a one-way ratchet: a photo
        whose conflicting keyword has since been removed would keep being
        routed to Review forever, naming a conflict that no longer exists —
        the same staleness bug in the other direction. The snapshot is the
        fallback for when no fresh comparison could be made at all.
        """
        if effective_category is not None:
            return effective_category in _EFFECTIVE_AMBIGUOUS_CATEGORIES
        return stored_category in _STORED_AMBIGUOUS_CATEGORIES

    def _ambiguous_prediction_ids(db, rows):
        """Which of ``rows`` a bare Accept must not act on.

        The one definition of "ambiguous" for the pair of endpoints that
        need it: the selection panel, which splits its payload into
        ``acceptable_prediction_ids`` and ``ambiguous_prediction_ids``, and
        ``batch-accept``, which re-derives the same verdict before writing.
        Two conditions, both of which mean a bare Accept would decide
        something the user has not been shown:

        * an ``alternative`` sibling on the row's ``(detection, model)`` —
          the classifier offered a runner-up, so accepting picks a winner on
          the user's behalf;
        * a disagreement/refinement against the photo's species keywords,
          judged by ``_prediction_is_ambiguous`` on the *current* keywords
          (see ``_effective_category_resolver`` for why the stored
          ``category`` column cannot be trusted for this).

        ``batch-accept`` recomputes rather than trusting the payload because
        the panel's split is a snapshot: a keyword added from Review, a
        second Browse tab, or an XMP sync between render and click makes a
        row ambiguous while it is still ``pending``, so the decided-status
        precondition alone cannot catch it. The panel's own refresh handles
        mutations inside one document; only the server sees the rest. This
        lives here — not once per endpoint — for the reason rounds 7 and 8
        established for the status precondition and the accept scope: a rule
        with two implementations is a rule that drifts.

        ``rows`` are prediction rows carrying ``id``, ``photo_id``,
        ``detection_id``, ``model``, ``category``, ``species``, ``group_id``
        and ``individual``. Returns the ambiguous subset of their ids.
        """
        rows = list(rows)
        if not rows:
            return set()
        photo_ids = list(dict.fromkeys(
            row["photo_id"] for row in rows if row["photo_id"] is not None
        ))
        # Keyed by (detection, model) exactly as /api/predictions nests
        # alternatives, so "has alternatives" means the same thing in Browse,
        # in this check, and in Review.
        alt_keys = {
            (row["detection_id"], row["model"])
            for row in db.get_predictions(
                photo_ids=photo_ids, status="alternative",
            )
        }
        effective_category_of = _effective_category_resolver(db, photo_ids)
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        ambiguous = set()
        for row in rows:
            # Compared on the species the accept path would actually apply
            # (the burst consensus), not the row's own label.
            identity = resolver.consensus(row)
            species = identity.display_name
            effective_category = (
                effective_category_of(row["photo_id"], species, identity)
                if effective_category_of is not None and species else None
            )
            if (
                (row["detection_id"], row["model"]) in alt_keys
                or _prediction_is_ambiguous(effective_category, row["category"])
            ):
                ambiguous.add(row["id"])
        return ambiguous


    def _summarize_details(details):
        """Build a short human-friendly summary string from a Place Details dict.

        Format: ``"<leaf name> · <broadest 1-2 parents>"``. Google's
        ``address_components`` are ordered narrowest-first, so the broadest
        parents (country, state) sit at the END of the list. We pick at most
        the last two, dedupe against the leaf name, and join with " · ".

        Examples::

            "Central Park · New York · United States"
            "Some Lighthouse · Iceland"
            "JustALeaf"  # if no usable parent components
        """
        leaf = (details or {}).get("name", "") or ""
        components = (details or {}).get("address_components") or []

        # Broadest 1-2 parents = last two components (Google orders broad-last).
        tail = components[-2:] if len(components) >= 2 else components[-1:]
        # Walk in reverse so we render broadest-first to broader-second
        # ("New York · United States" reads better than "United States · New York"
        # given the leaf comes first; iNaturalist uses leaf-then-narrowest-up).
        # Actually: leaf · narrowest-parent · ... · broadest-parent reads most
        # naturally for breadcrumbs. So reverse the tail so the closest parent
        # is first.
        parts = [leaf] if leaf else []
        for comp in reversed(tail):
            name = (comp or {}).get("name") or (comp or {}).get("long_name") or ""
            if not name:
                continue
            if name == leaf or name in parts:
                continue
            parts.append(name)

        if not parts:
            return ""
        return " · ".join(parts)

    def _run_batch_delete(
        db, photo_ids, mode="vireo", include_companions=False, paths=None,
        progress_callback=None,
    ):
        """Delete photos using the same phases for sync and job endpoints."""
        paths = paths or []

        def emit(
            phase, current=0, total=0, current_file="", detail="", failed=0,
            stage_failures=None,
        ):
            if progress_callback:
                payload = {
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "current_file": current_file,
                    "detail": detail,
                    "failed": failed,
                }
                # ``stage_failures`` lets a single emit attribute failure counts
                # to specific stages so the frontend does not have to rely on
                # a specific per-stage emit having arrived first. The disk and
                # catalog phases each report their own count while Finishing
                # sends the merged map, so a dropped intermediate event cannot
                # silently promote a partial stage to green complete.
                if stage_failures:
                    payload["stage_failures"] = dict(stage_failures)
                progress_callback(payload)

        if mode == "disk_permanent" and paths:
            # Retry path: DB rows were already deleted by the initial
            # disk-mode call, so the photos table can't vouch for these
            # paths — but their parent directories still have folders rows.
            # Only delete files that live directly in a Vireo-managed
            # folder; anything else (a crafted request naming arbitrary
            # files) is refused, not removed.
            trashed = 0
            trash_failed = []
            total_paths = len(paths)
            emit("Deleting files permanently", 0, total_paths)
            for idx, p in enumerate(paths, start=1):
                if not isinstance(p, str) or not p:
                    emit("Deleting files permanently", idx, total_paths)
                    continue
                candidates = {os.path.dirname(p), os.path.dirname(os.path.realpath(p))}
                known = db.conn.execute(
                    f"SELECT 1 FROM folders WHERE path IN ({','.join('?' for _ in candidates)})",
                    list(candidates),
                ).fetchone()
                if not known:
                    log.warning(
                        "Refusing disk_permanent retry for path outside Vireo folders: %s", p
                    )
                    trash_failed.append({"path": p, "error": "not in a Vireo folder"})
                    emit("Deleting files permanently", idx, total_paths, os.path.basename(p))
                    continue
                if not os.path.isfile(p):
                    emit("Deleting files permanently", idx, total_paths, os.path.basename(p))
                    continue
                try:
                    os.remove(p)
                    trashed += 1
                except OSError:
                    log.warning("Permanent delete failed for %s", p, exc_info=True)
                    trash_failed.append({"path": p})
                emit("Deleting files permanently", idx, total_paths, os.path.basename(p))
            return {
                "ok": True, "deleted": 0, "trashed": trashed,
                "trash_failed": trash_failed,
            }

        if not photo_ids:
            raise ValueError("photo_ids required")
        if mode not in ("vireo", "disk", "disk_permanent"):
            raise ValueError("mode must be 'vireo', 'disk', or 'disk_permanent'")

        def remove_catalog_rows(ids, *, expand_companions, revalidate_identity=None):
            """Atomically remove resolved catalog rows, then clean caches.

            ``revalidate_identity`` is an optional ``{photo_id: (folder_id,
            filename, folder_path)}`` map captured at the start of the disk
            operation. When provided, each row is re-checked inside the delete
            transaction and skipped if any of those three fields has changed
            since — a concurrent ``/api/jobs/move-photos`` can commit a new
            ``folder_id`` while the disk-delete's stale-path
            ``os.path.isfile`` check is already reporting "gone", and a
            concurrent ``/api/jobs/move-folder`` can leave ``folder_id`` and
            ``filename`` unchanged while renaming the underlying
            ``folders.path``. Deleting by id alone in either case would
            discard the row for a photo that now lives at a completely
            different path. Skipped ids are returned as ``skipped_ids`` so
            the caller can surface them alongside filesystem failures.
            """
            result = {
                "deleted": 0, "ids": [], "files": [], "skipped_ids": [],
            }
            skipped_ids = []
            total = len(ids)
            prepared = 0
            emit(
                "Removing from Vireo", 0, total,
                detail="Preparing database changes; nothing is committed yet.",
            )
            revalidating = bool(revalidate_identity)
            # BEGIN IMMEDIATE takes the write lock up front so no other writer
            # (notably move-photos or move-folder) can commit an identity
            # change between the revalidation SELECT and the DELETE that
            # follows. Without it, a move committed after the SELECT but
            # before the DELETE would let us delete a row that no longer
            # matches the identity we verified.
            if revalidating:
                db.conn.execute("BEGIN IMMEDIATE")
            try:
                if revalidating:
                    verified_ids = []
                    for chunk in _chunked(ids):
                        placeholders = ",".join("?" for _ in chunk)
                        current = {
                            row["id"]: (
                                row["folder_id"],
                                row["filename"],
                                row["folder_path"],
                                row["companion_path"],
                            )
                            for row in db.conn.execute(
                                f"SELECT p.id, p.folder_id, p.filename, "
                                f"p.companion_path, f.path AS folder_path "
                                f"FROM photos p "
                                f"JOIN folders f ON f.id = p.folder_id "
                                f"WHERE p.id IN ({placeholders})",
                                list(chunk),
                            )
                        }
                        for photo_id in chunk:
                            expected = revalidate_identity.get(photo_id)
                            actual = current.get(photo_id)
                            if actual is None:
                                # Row already gone — a concurrent delete beat
                                # us to it. The requested end state already
                                # holds, so treat it as successfully deleted
                                # rather than a stale identity we couldn't
                                # verify. Reporting it in ``failed_photo_ids``
                                # here would leave the client showing a photo
                                # that is absent from both catalog and disk
                                # until reload.
                                continue
                            if expected is not None and actual == expected:
                                verified_ids.append(photo_id)
                            else:
                                skipped_ids.append(photo_id)
                    ids_to_delete = verified_ids
                else:
                    ids_to_delete = list(ids)

                for chunk in _chunked(ids_to_delete):
                    chunk_result = db.delete_photos(
                        chunk,
                        include_companions=expand_companions,
                        commit=False,
                    )
                    result["deleted"] += chunk_result["deleted"]
                    result["ids"].extend(chunk_result["ids"])
                    result["files"].extend(chunk_result["files"])
                    prepared += len(chunk)
                    emit(
                        "Removing from Vireo", min(prepared, total), total,
                        detail=(
                            "Preparing database changes; nothing is committed yet."
                        ),
                    )
                db.conn.commit()
            except Exception:
                db.conn.rollback()
                raise
            result["skipped_ids"] = skipped_ids

            emit(
                "Removed from Vireo", result["deleted"], result["deleted"],
                detail="Database changes committed.",
            )
            emit("Pruning pipeline cache", 0, 1)
            try:
                db.prune_pipeline_cache_for_ids(result["ids"])
            except BaseException as exc:
                _reraise_fatal_cleanup_error(exc)
                log.exception("Failed to prune pipeline cache after delete")
            emit("Pruning pipeline cache", 1, 1)

            def cache_progress(current, total_files, filename):
                emit("Cleaning cached files", current, total_files, filename)

            emit("Cleaning cached files", 0, len(result["files"]))
            _cleanup_cached_files_for_deleted_photos(
                result["files"], progress_callback=cache_progress,
            )
            return result

        # Database-only mode has no filesystem prerequisite and retains the
        # original all-or-nothing catalog transaction.
        if mode == "vireo":
            result = remove_catalog_rows(
                photo_ids, expand_companions=include_companions,
            )
            emit("Finishing", 1, 1)
            return {
                "ok": True,
                "deleted": result["deleted"],
                "trashed": 0,
                "trash_failed": [],
                "failed_photo_ids": [],
            }

        # Disk modes resolve paths without changing SQLite. A photo's catalog
        # row is removed only after its primary file reached the requested end
        # state. A companion without its own photo row is processed first; if
        # that fails, its primary is left untouched and its row remains
        # retryable.
        resolved = db.resolve_photos_for_delete(
            photo_ids, include_companions=include_companions,
        )
        files = resolved["files"]
        primary_paths = {
            f["photo_id"]: os.path.join(f["folder_path"], f["filename"])
            for f in files
        }
        # Snapshot each row's identity so the catalog-removal step can verify
        # the row still points at the same file it did when we resolved paths.
        # Without this, a concurrent /api/jobs/move-photos can commit a new
        # folder_id and remove the source file mid-run; our stale-path
        # os.path.isfile would then read "already gone" and we would delete
        # a row that now represents the moved file at a different location.
        # The folder_path is included so a concurrent /api/jobs/move-folder,
        # which keeps folder_id and filename unchanged while renaming
        # ``folders.path``, is also caught — otherwise the row would still be
        # deleted even though the copied file remains at the new path.
        # ``companion_path`` is included so a concurrent scan that pairs a
        # RAW with a JPEG mid-delete (scanner.py: ``UPDATE photos SET
        # companion_path`` on the primary, ``DELETE FROM photos`` on the
        # merged companion) is also caught — otherwise the primary's tuple
        # would still match, and we'd trash only the pre-pair paths and
        # remove the primary row, orphaning the newly-paired companion file
        # on disk with no catalog entry.
        resolved_identity = {
            f["photo_id"]: (
                f["folder_id"], f["filename"], f["folder_path"],
                f["companion_path"],
            )
            for f in files
        }
        catalog_primary_paths = set(primary_paths.values())
        extra_companions = {}
        if include_companions:
            for f in files:
                companion_path = f.get("companion_path")
                if not companion_path:
                    continue
                # ``companion_path`` is stored as either a relative filename
                # inside the same folder or an absolute path (see the same
                # resolution in new_images.py:35-40). Joining an absolute
                # path with the folder path would silently target the wrong
                # file — the disk op would either fail or, worse, trash the
                # wrong photo without ever pruning the correct catalog row.
                companion = (
                    companion_path if os.path.isabs(companion_path)
                    else os.path.join(f["folder_path"], companion_path)
                )
                if companion not in catalog_primary_paths:
                    extra_companions.setdefault(f["photo_id"], set()).add(companion)

        disk_phase = (
            "Moving files to Trash"
            if mode == "disk" else "Deleting files permanently"
        )
        all_disk_paths = list(dict.fromkeys(
            [path for paths_for_id in extra_companions.values() for path in paths_for_id]
            + list(primary_paths.values())
        ))
        emit(disk_phase, 0, len(all_disk_paths))

        disk_paths_finished = 0

        def operate(paths_to_change):
            nonlocal disk_paths_finished
            if not paths_to_change:
                return 0, set(), []
            if mode == "disk":
                progress_offset = disk_paths_finished

                def trash_progress(current, _total, filename):
                    emit(
                        disk_phase, progress_offset + current,
                        len(all_disk_paths), filename,
                    )

                result = _trash_paths(
                    paths_to_change, progress_callback=trash_progress,
                )
                disk_paths_finished += len(paths_to_change)
                return result
            successful = set()
            failures = []
            removed = 0
            # Snapshot each parent's st_dev before deletion so a mount that
            # vanishes mid-batch can be detected even when the mount point
            # remains visible on the underlying local FS (see
            # ``_snapshot_parent_device``).
            parent_devs = {
                path: _snapshot_parent_device(path)
                for path in paths_to_change
            }
            for filepath in paths_to_change:
                if not os.path.isfile(filepath):
                    # Same live-parent gate as ``_trash_paths`` — a
                    # disconnected mount also makes ``os.path.isfile``
                    # return False, and treating that as "already gone"
                    # would prune the catalog row for a photo that
                    # reappears when the volume comes back.
                    if _path_confirmed_gone(
                        filepath, parent_devs.get(filepath),
                    ):
                        log.warning("File already missing: %s", filepath)
                        successful.add(filepath)
                    else:
                        log.warning(
                            "Permanent delete preflight: source "
                            "unreachable for %s", filepath,
                        )
                        failures.append({
                            "path": filepath,
                            "error": "Source path is unreachable",
                        })
                    disk_paths_finished += 1
                    emit(
                        disk_phase, disk_paths_finished,
                        len(all_disk_paths), os.path.basename(filepath),
                    )
                    continue
                try:
                    os.remove(filepath)
                    successful.add(filepath)
                    removed += 1
                except OSError as exc:
                    log.warning(
                        "Permanent delete failed for %s", filepath,
                        exc_info=True,
                    )
                    failures.append({"path": filepath, "error": str(exc)})
                disk_paths_finished += 1
                emit(
                    disk_phase, disk_paths_finished,
                    len(all_disk_paths), os.path.basename(filepath),
                )
            return removed, successful, failures

        companion_paths = list(dict.fromkeys(
            path for paths_for_id in extra_companions.values()
            for path in paths_for_id
        ))
        trashed, companion_success, companion_failures = operate(companion_paths)
        eligible_ids = {
            photo_id for photo_id in resolved["ids"]
            if extra_companions.get(photo_id, set()) <= companion_success
        }
        eligible_primary_paths = [
            primary_paths[photo_id] for photo_id in resolved["ids"]
            if photo_id in eligible_ids and photo_id in primary_paths
        ]
        primary_moved, primary_success, primary_failures = operate(
            eligible_primary_paths,
        )
        trashed += primary_moved
        successful_ids = [
            photo_id for photo_id in resolved["ids"]
            if photo_id in eligible_ids
            and primary_paths.get(photo_id) in primary_success
        ]
        successful_id_set = set(successful_ids)
        failed_ids = [
            photo_id for photo_id in resolved["ids"]
            if photo_id not in successful_id_set
        ]

        failure_by_path = {
            failure["path"]: failure
            for failure in companion_failures + primary_failures
        }
        trash_failed = []
        for photo_id in failed_ids:
            failed_paths = [
                path for path in extra_companions.get(photo_id, set())
                if path not in companion_success
            ]
            primary = primary_paths.get(photo_id)
            if not failed_paths and primary not in primary_success:
                failed_paths.append(primary)
            for filepath in failed_paths:
                detail = dict(failure_by_path.get(filepath) or {
                    "path": filepath,
                    "error": "A companion file could not be removed",
                })
                detail["photo_id"] = photo_id
                trash_failed.append(detail)

        disk_failed_photos = len(failed_ids)
        emit(
            disk_phase, len(all_disk_paths), len(all_disk_paths),
            detail=(
                f"{len(successful_ids)} photo(s) ready for catalog removal; "
                f"{disk_failed_photos} retained after filesystem errors."
            ),
            failed=disk_failed_photos,
            stage_failures={"files": disk_failed_photos},
        )
        result = remove_catalog_rows(
            successful_ids,
            expand_companions=False,
            revalidate_identity=resolved_identity,
        )
        # Rows whose identity changed between resolve and catalog-removal
        # weren't deleted — surface them alongside filesystem failures so
        # the client keeps them visible and doesn't report them as trashed.
        skipped_ids = result.get("skipped_ids", []) or []
        catalog_failed_photos = len(skipped_ids)
        if skipped_ids:
            already_failed = set(failed_ids)
            for photo_id in skipped_ids:
                trash_failed.append({
                    "photo_id": photo_id,
                    "path": primary_paths.get(photo_id, ""),
                    "error": (
                        "Photo was moved to a new folder during this delete; "
                        "the catalog row was preserved"
                    ),
                })
            failed_ids = failed_ids + [
                photo_id for photo_id in skipped_ids
                if photo_id not in already_failed
            ]
            # Re-emit the catalog stage with the retained count so the frontend
            # transitions it from complete → partial. Without this, the initial
            # "Removed from Vireo" event marks the stage green and Finishing
            # later carries a higher failed count that the frontend can't
            # attribute to any stage.
            emit(
                "Removed from Vireo",
                result["deleted"], result["deleted"] + catalog_failed_photos,
                detail=(
                    f"{catalog_failed_photos} photo(s) retained "
                    "due to concurrent moves."
                ),
                failed=catalog_failed_photos,
                stage_failures={"catalog": catalog_failed_photos},
            )
        emit(
            "Finishing", 1, 1,
            failed=len(failed_ids),
            stage_failures={
                "files": disk_failed_photos,
                "catalog": catalog_failed_photos,
            },
        )
        return {
            "ok": True,
            "deleted": result["deleted"],
            "trashed": trashed,
            "trash_failed": trash_failed,
            "failed_photo_ids": failed_ids,
        }


    # -- Statistics --


    # -- Highlights --

    def _build_highlights_payload(
        db,
        scope="folder",
        folder_id=None,
        min_quality=0.0,
        confidence_threshold=0.70,
        limit_per_bucket=20,
        species_filter="",
        species_match_case=False,
        species_whole_word=False,
        search_query="",
        search_match_case=False,
        search_whole_word=False,
        confirmation_filter="all",
        highlight_filter="all",
        representative_filter="all",
    ):
        folders = db.get_folders_with_quality_data()
        if scope == "workspace":
            folder_id = None
        elif folder_id is None and folders:
            folder_id = folders[0]["id"]  # Most recent
        limit_per_bucket = max(1, min(int(limit_per_bucket), 100))
        species_filter = (species_filter or "").strip()
        confirmation_filter = _normalize_highlight_confirmation_filter(
            confirmation_filter
        )
        highlight_filter = _normalize_highlight_presence_filter(highlight_filter)
        representative_filter = _normalize_highlight_presence_filter(
            representative_filter
        )

        candidates = db.get_highlights_candidates(folder_id, min_quality=min_quality)
        total_in_scope = db.count_filtered_photos(folder_id=folder_id)

        buckets, unidentified_photos = _collect_highlight_buckets(
            candidates, confidence_threshold, confirmation_filter,
            canonicalize_species=_species_canonicalizer(db),
        )
        eligible_count = sum(b["photo_count"] for b in buckets) + len(
            unidentified_photos
        )
        if species_filter:
            buckets = [
                b for b in buckets
                if text_search_match(
                    b["species"],
                    species_filter,
                    species_match_case,
                    species_whole_word,
                )
            ]
            if not text_search_match(
                "unidentified",
                species_filter,
                species_match_case,
                species_whole_word,
            ):
                unidentified_photos = []

        buckets, unidentified_photos = _filter_highlight_sections(
            buckets,
            unidentified_photos,
            search_query,
            search_match_case,
            search_whole_word,
        )
        _apply_ordered_highlights(db, buckets)
        _apply_highlight_preferences(db, buckets)
        buckets, unidentified_photos = _filter_highlight_curation_state(
            buckets,
            unidentified_photos,
            highlight_filter,
            representative_filter,
        )

        def limited_bucket(bucket):
            photos = bucket["photos"]
            limited = photos[:limit_per_bucket]
            return {
                **bucket,
                "photos": limited,
                "loaded_count": len(limited),
                "has_more": len(photos) > len(limited),
            }

        limited_buckets = [limited_bucket(b) for b in buckets]
        unidentified_limited = unidentified_photos[:limit_per_bucket]
        visible_photos = []
        for bucket in limited_buckets:
            visible_photos.extend(bucket["photos"])
        visible_photos.extend(unidentified_limited)
        attach_edit_recipes(db, visible_photos)

        return {
            "buckets": limited_buckets,
            "unidentified": {
                "photo_count": len(unidentified_photos),
                "photos": unidentified_limited,
                "loaded_count": len(unidentified_limited),
                "has_more": len(unidentified_photos) > len(unidentified_limited),
                "unanalyzed_count": _bucket_unanalyzed_count(unidentified_photos),
            },
            "folders": [
                {"id": f["id"], "name": f["name"], "photo_count": f["photo_count"]}
                for f in folders
            ],
            "meta": {
                "total_in_scope": total_in_scope,
                "eligible": eligible_count,
                "limit_per_bucket": limit_per_bucket,
                "confirmation": confirmation_filter,
                "highlight_selection": highlight_filter,
                "species_representative": representative_filter,
                "search": (search_query or "").strip(),
            },
            "scope": "workspace" if folder_id is None else "folder",
        }

    def _build_life_list_payload(
        db,
        photos_per_species=12,
        photo_offset=0,
        species_filter=None,
    ):
        rows = db.get_life_list_candidates(species=species_filter)
        locations_by_species = db.get_life_list_locations(species=species_filter)
        class_by_taxon = db.get_class_ancestors_for_taxa({
            row["taxon_id"] for row in rows if row["taxon_id"] is not None
        })
        photo_offset = max(0, int(photo_offset))
        if photos_per_species is None:
            photo_limit = None
        else:
            photo_limit = max(
                1, min(int(photos_per_species), 100)
            )

        # Canonicalize each row's species to the root keyword's stored
        # spelling. After repair_duplicate_photo_species detaches a
        # redundant root row, a photo may only carry the hierarchy leaf
        # (``verdin``) whose spelling differs from the curation-keyed
        # root (``Verdin``); without canonicalization, that photo would
        # sit in its own ``verdin`` bucket, its curation on
        # ``Verdin`` would never apply, and single-species API loads
        # for ``Verdin`` would return an entry named ``verdin``.
        canonicalize = _species_canonicalizer(db)
        merged_locations = {}
        for raw_species, locations in locations_by_species.items():
            canonical = canonicalize(raw_species) or raw_species
            existing = merged_locations.setdefault(canonical, [])
            for loc in locations:
                if loc not in existing:
                    existing.append(loc)
        locations_by_species = merged_locations

        buckets = {}
        for row in rows:
            r = dict(row)
            r["species"] = canonicalize(r["species"]) or r["species"]
            entry = buckets.setdefault(r["species"], {
                "scientific_name": None,
                "common_name": None,
                "taxon_id": None,
                "taxon_rank": None,
                "taxonomic_class": None,
                "photos": [],
                "seen_ids": set(),
            })
            # Two keyword rows can share a name (different parents); take
            # the first linked taxon's names for the species entry.
            if entry["scientific_name"] is None:
                entry["scientific_name"] = r.get("scientific_name")
            if entry["common_name"] is None:
                entry["common_name"] = r.get("common_name")
            if entry["taxon_id"] is None and r.get("taxon_id") is not None:
                entry["taxon_id"] = r["taxon_id"]
                entry["taxon_rank"] = r.get("taxon_rank")
                entry["taxonomic_class"] = class_by_taxon.get(r["taxon_id"])
            # A photo tagged with two same-name species keywords would
            # otherwise be appended once per row, inflating photo_count
            # and duplicating cards in the lightbox.
            if r["id"] in entry["seen_ids"]:
                continue
            entry["seen_ids"].add(r["id"])
            entry["photos"].append({
                "id": r["id"],
                "filename": r["filename"],
                "timestamp": r.get("timestamp"),
                "rating": r.get("rating") or 0,
                "flag": r.get("flag") or "none",
                "quality_score": r.get("quality_score"),
                "subject_sharpness": r.get("subject_sharpness"),
                "subject_size": r.get("subject_size"),
                "sharpness": r.get("sharpness"),
                "subject_tenengrad": r.get("subject_tenengrad"),
                "bg_tenengrad": r.get("bg_tenengrad"),
                "crop_complete": r.get("crop_complete"),
                "bg_separation": r.get("bg_separation"),
                "subject_clip_high": r.get("subject_clip_high"),
                "subject_clip_low": r.get("subject_clip_low"),
                "subject_y_median": r.get("subject_y_median"),
                "noise_estimate": r.get("noise_estimate"),
                "eye_tenengrad": r.get("eye_tenengrad"),
            })

        def compact(photo):
            return {
                "id": photo["id"],
                "filename": photo["filename"],
                "timestamp": photo.get("timestamp"),
                "flag": photo.get("flag") or "none",
                "quality_score": photo.get("quality_score"),
                "highlight_score": photo.get("highlight_score"),
                "reasons": photo.get("reasons") or [],
                "is_species_representative": bool(
                    photo.get("is_species_representative")
                ),
                "is_life_list_photo": bool(photo.get("is_species_representative")),
                "is_highlighted": bool(photo.get("is_highlighted")),
                "highlight_rank": photo.get("highlight_rank"),
            }

        species_entries = []
        distinct_photo_ids = set()
        representatives = db.get_species_representative_lists(species=species_filter)
        highlights_by_species = db.get_species_highlights(species=species_filter)
        for species, entry in buckets.items():
            photos = entry["photos"]
            distinct_photo_ids.update(p["id"] for p in photos)
            timestamps = [p["timestamp"] for p in photos if p.get("timestamp")]
            _highlight_score_bucket(photos)
            representative_ids = representatives.get(species) or []
            representative_order = {
                photo_id: idx for idx, photo_id in enumerate(representative_ids)
            }
            preferred_id = representative_ids[0] if representative_ids else None
            highlight_ranks = highlights_by_species.get(species, {})
            for photo in photos:
                rank = highlight_ranks.get(photo["id"])
                photo["is_highlighted"] = rank is not None
                photo["highlight_rank"] = rank
                photo["is_species_representative"] = (
                    photo["id"] in representative_order
                )
            preferred_applied = any(
                photo.get("is_species_representative") for photo in photos
            )
            if not preferred_applied and highlight_ranks:
                valid_highlights = [
                    (highlight_ranks[p["id"]], p["id"])
                    for p in photos
                    if p["id"] in highlight_ranks
                ]
                if valid_highlights:
                    _, highlight_id = min(valid_highlights)
                    for idx, photo in enumerate(photos):
                        if photo["id"] == highlight_id:
                            if idx:
                                photos.insert(0, photos.pop(idx))
                            break
            # Life List curation order is explicit representative selection,
            # then Picks (the P flag), then the existing highlight/algorithmic
            # ranking. Keep ranked positions as the final tie-breaker so
            # multiple Picks retain their quality order and non-Picks behave
            # exactly as before.
            ranked_position = {
                photo["id"]: idx for idx, photo in enumerate(photos)
            }
            photos.sort(key=lambda photo: (
                0 if photo["id"] in representative_order else (
                    1 if photo.get("flag") == "flagged" else 2
                ),
                representative_order.get(photo["id"], 0),
                ranked_position.get(photo["id"], 0),
            ))
            best_photo = photos[0] if photos else {}
            if best_photo.get("is_species_representative"):
                best_source = "representative"
            elif best_photo.get("flag") == "flagged":
                best_source = "pick"
            elif best_photo.get("is_highlighted"):
                best_source = "highlight"
            else:
                best_source = "algorithm"
            if photo_limit is None:
                top = photos[photo_offset:]
            else:
                top = photos[photo_offset:photo_offset + photo_limit]
            species_entries.append({
                "species": species,
                "scientific_name": entry["scientific_name"],
                "common_name": entry["common_name"],
                "taxon_id": entry["taxon_id"],
                "taxon_rank": entry["taxon_rank"],
                "taxonomic_class": entry["taxonomic_class"],
                "photo_count": len(photos),
                "first_seen": min(timestamps) if timestamps else None,
                "last_seen": max(timestamps) if timestamps else None,
                "locations": locations_by_species.get(species, []),
                "preferred_photo_id": preferred_id,
                "has_preferred_photo": preferred_applied,
                "best_source": best_source,
                "best": compact(photos[0]) if photos else None,
                "photos": [compact(p) for p in top],
                "loaded_count": min(len(photos), photo_offset + len(top)),
                "has_more": photo_offset + len(top) < len(photos),
            })

        # Life-list numbering: chronological by first photographed date,
        # the way birders count lifers. Species with no capture time go
        # last, alphabetically, so they still get a stable number.
        species_entries.sort(key=lambda e: (
            e["first_seen"] is None,
            e["first_seen"] or "",
            _life_list_alphabetical_key(e["species"]),
            e["species"].lower(),
        ))
        for i, e in enumerate(species_entries, start=1):
            e["number"] = i
        life_list_photos = []
        for entry in species_entries:
            if entry.get("best"):
                life_list_photos.append(entry["best"])
            life_list_photos.extend(entry.get("photos") or [])
        attach_edit_recipes(db, life_list_photos)

        return {
            "species": species_entries,
            "meta": {
                "species_count": len(species_entries),
                "photo_count": len(distinct_photo_ids),
                "photos_per_species": photo_limit,
            },
        }

    app.register_blueprint(
        create_highlights_blueprint(
            _get_db,
            json_error,
            build_highlights_payload=_build_highlights_payload,
            chunked=_chunked,
            species_canonicalizer=_species_canonicalizer,
            collect_highlight_buckets=_collect_highlight_buckets,
            normalize_highlight_confirmation_filter=(
                _normalize_highlight_confirmation_filter
            ),
            filter_highlight_sections=_filter_highlight_sections,
            apply_ordered_highlights=_apply_ordered_highlights,
            apply_highlight_preferences=_apply_highlight_preferences,
            filter_highlight_curation_state=_filter_highlight_curation_state,
            bucket_best_score=_bucket_best_score,
        )
    )

    app.register_blueprint(
        create_local_workspace_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            os.path.dirname(app.config["THUMB_CACHE_DIR"]),
            invalidate_missing_originals=lambda ws_id: _invalidate_missing_originals_cache(
                workspace_ids=[ws_id]
            ),
        )
    )
    app.register_blueprint(
        create_local_folder_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            os.path.dirname(app.config["THUMB_CACHE_DIR"]),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(
        create_jobs_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            lambda: app.config["THUMB_CACHE_DIR"],
        )
    )

    # -- Prediction API routes --

    @app.route("/api/predictions")
    def api_predictions():
        db = _get_db()
        collection_id = request.args.get("collection_id", None, type=int)
        status = request.args.get("status", None)
        # Browse's detail panel asks for one photo's predictions. Reusing this
        # route rather than adding a per-photo one keeps a single definition of
        # "a prediction" — latest-fingerprint dedup, nested alternatives and
        # ``existing_species`` enrichment all come along, so the Browse panel
        # can never disagree with Review about what is pending.
        try:
            explicit_photo_ids = _request_photo_ids_arg()
        except ValueError as e:
            return json_error(str(e), 400)
        if explicit_photo_ids is not None:
            for pid in explicit_photo_ids:
                if not db._photo_in_workspace(pid):
                    return json_error(
                        f"Photo {pid} does not belong to the active workspace",
                        403,
                    )
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        try:
            rules = request_rules_arg()
            visual = request_visual_arg()
            # Fill in the active visual model on any UI-emitted
            # ``has_visual_index`` rule that omits it. /api/photos/query
            # already injects here; the Review GET path must do the same
            # so a workspace with stale embeddings from an inactive model
            # doesn't include predictions for photos that aren't indexed
            # by the currently-active visual model.
            rules = inject_active_visual_model(rules)
            # Forward ``collection_id`` so ``visual_info`` (matched /
            # candidates / indexed) describes the collection-scoped Review
            # queue the filter bar chip is showing — not a workspace-wide
            # proxy that misrepresents the actual queue and also wastes
            # embedding work on photos outside it.
            rules, visual_info = visual_scope.apply_to_rules(
                db, rules, visual, collection_id=collection_id,
            )
        except ValueError as e:
            return json_error(str(e), 400)
        try:
            if collection_id:
                photos = db.get_collection_photos(collection_id, per_page=999999)
                photo_ids = [p["id"] for p in photos]
                # An explicit ``photo_ids`` narrows the collection rather than
                # replacing it, so a Browse panel opened inside a collection
                # can't surface a photo the collection excludes.
                if explicit_photo_ids is not None:
                    allowed = set(photo_ids)
                    photo_ids = [
                        pid for pid in explicit_photo_ids if pid in allowed
                    ]
                preds = (
                    db.get_predictions(photo_ids=photo_ids, status=status, rules=rules)
                    if photo_ids
                    else []
                )
            else:
                preds = db.get_predictions(
                    photo_ids=explicit_photo_ids, status=status, rules=rules,
                )

            # Fetch alternatives to attach to their parent predictions.
            # Constrain by the returned parents' photo_ids and skip ``rules``
            # for this lookup: row-level parent predicates (e.g.
            # ``prediction_confidence >= 0.8`` or
            # ``prediction_status is pending``) evaluate against each row's
            # own values, so alternatives — whose status is
            # ``alternative`` and whose confidence/species usually differ
            # from the matching parent — would otherwise be dropped by
            # ``_filter_prediction_rows_by_rules`` before ``alts_by_key`` is
            # built. The parent would then render with an empty
            # ``alternatives`` list and the user could not accept an
            # alternate species in that filtered view. The
            # ``(detection_id, model)`` key in ``alts_by_key`` already
            # restricts attachment to alternatives whose parent is in
            # ``preds``, so no extra rows leak into the response.
            alt_preds = []
            if not status or status == "pending":
                parent_photo_ids = list({
                    p["photo_id"] for p in preds
                    if p["photo_id"] is not None
                })
                if parent_photo_ids:
                    alt_preds = db.get_predictions(
                        photo_ids=parent_photo_ids, status="alternative",
                    )
        except ValueError as e:
            return json_error(str(e), 400)

        # Index alternatives by (detection_id, model)
        alts_by_key = {}
        for a in alt_preds:
            ad = dict(a)
            key = (ad["detection_id"], ad["model"])
            alts_by_key.setdefault(key, []).append({
                "id": ad["id"],
                "species": ad["species"],
                "confidence": ad["confidence"],
                "taxonomy_kingdom": ad.get("taxonomy_kingdom"),
                "taxonomy_phylum": ad.get("taxonomy_phylum"),
                "taxonomy_class": ad.get("taxonomy_class"),
                "taxonomy_order": ad.get("taxonomy_order"),
                "taxonomy_family": ad.get("taxonomy_family"),
                "taxonomy_genus": ad.get("taxonomy_genus"),
                "scientific_name": ad.get("scientific_name"),
            })

        # Enrich predictions and attach alternatives
        results = []
        pred_dicts = [dict(p) for p in preds]
        attach_species_representatives(db, pred_dicts)
        recipes_by_photo = db.get_photo_edit_recipes({
            p.get("photo_id") for p in pred_dicts if p.get("photo_id") is not None
        })
        # Same recomputation as the selection aggregator: the stored
        # ``category`` is a classify-time snapshot, so a keyword added
        # after classification leaves it stale. Compare each pending
        # prediction against the CURRENT species keywords on its photo and
        # expose the fresh disposition as ``effective_category`` so
        # Browse's single-photo panel can route now-conflicting predictions
        # to Review instead of offering a bare Accept.
        pending_photo_ids = {
            d.get("photo_id") for d in pred_dicts
            if d.get("status") != "alternative" and d.get("photo_id") is not None
        }
        effective_category_of = _effective_category_resolver(
            db, pending_photo_ids,
        )
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        for d in pred_dicts:
            if d.get("status") == "alternative":
                continue  # alternatives are nested, not top-level
            d["edit_recipe"] = recipes_by_photo.get(d.get("photo_id"))
            d["render_key"] = render_key_for_recipe(d["edit_recipe"])
            # Species the accept path will actually apply. For an ordinary
            # prediction this is the row's own species; for a grouped/burst
            # prediction whose frames disagree, ``accept_prediction`` derives
            # the burst consensus from ``individual`` vote counts. The Browse
            # panel labels and groups rows by this so a Sparrow frame in a
            # majority-Robin burst never surfaces a Sparrow row whose Accept
            # actually tags Robin. Computed before the comparison below
            # because that species is the one that would land on the photo.
            identity = resolver.consensus(d)
            d["consensus_species"] = identity.display_name
            d["consensus_species_key"] = identity.key
            d["species_key"] = resolver.prediction(d).key
            effective_category = (
                effective_category_of(
                    d.get("photo_id"), d.get("consensus_species"), identity,
                )
                if effective_category_of is not None else None
            )
            d["effective_category"] = effective_category
            if _prediction_is_ambiguous(effective_category, d.get("category")):
                keywords = db.get_photo_keywords(d["photo_id"])
                d["existing_species"] = [
                    k["name"] for k in keywords
                    if db.is_keyword_species(k["id"])
                ]
            # Attach alternatives
            key = (d.get("detection_id"), d.get("model"))
            d["alternatives"] = alts_by_key.get(key, [])
            results.append(d)
        # Surface the visual clause's status so the Review filter bar's
        # visual chip can warn on fallback. Without this the chip would
        # advertise a visual search while Accept All / bulk operations ran
        # over the broadened metadata-only prediction set. Response is a
        # dict envelope so the field can travel alongside the list;
        # callers unwrap ``data.predictions``.
        response = {"predictions": results}
        if visual_info is not None:
            response["visual"] = visual_info
        # Only the per-photo callers (Browse's detail panel) need this, and
        # only they can afford it — Review asks for the whole workspace queue.
        # Without it an empty list is ambiguous: "never classified" and
        # "classified, found nothing" would render identically.
        if explicit_photo_ids is not None:
            response["photo_states"] = {
                str(pid): state
                for pid, state in db.get_prediction_states(explicit_photo_ids).items()
            }
            # Whether the best label in the list actually matched, per photo.
            # A prediction row carries a softmax confidence, which is a
            # ranking within the list and cannot distinguish "this is a chat"
            # from "nothing here is a chat, but this is closest". Sent with the
            # same per-photo gating as photo_states: Browse's panel shows one
            # photo, Review asks for the entire queue and must not pay for it.
            import config as cfg
            import match_confidence
            effective_cfg = db.get_effective_config(cfg.load())
            response["match_states"] = {
                str(pid): match_confidence.summarize_photo(
                    db.get_match_scores_for_photo(pid),
                    effective_cfg,
                    unscored_current_runs=(
                        db.get_unscored_current_prediction_runs(pid)
                    ),
                )
                for pid in explicit_photo_ids
            }
        return jsonify(response)

    _COMPARE_MAX_PER_PAGE = 200

    def _compare_photo_ids(name="photo_id"):
        """Parse repeated photo id args, or ``(None, None)`` when absent."""
        values = request.args.getlist(name)
        if not values:
            return None, None
        if len(values) > MAX_SELECTION_PHOTOS:
            return None, json_error(f"too many {name}s")
        try:
            photo_ids = [int(value) for value in values]
        except (TypeError, ValueError):
            return None, json_error(f"{name} must be an integer")
        if any(photo_id <= 0 for photo_id in photo_ids):
            return None, json_error(f"{name} must be a positive integer")
        return photo_ids, None

    def _attach_comparison_render_keys(db, photos):
        """Give ID Conflicts rows the fingerprint their thumbnail URLs need.

        The page builds ``/thumbnails/<id>.jpg`` through ``vireoThumbnailUrl``
        like every other grid, and that URL only carries an ``er`` fingerprint
        when the photo dict does. Thumbnails answer ``Cache-Control: public,
        max-age=86400``, so without one a browser that cached a row's
        thumbnail before an edit keeps showing the pre-edit image for a day.

        These rows are keyed by ``photo_id`` rather than ``id``, so they
        cannot go through ``attach_edit_recipes``.
        """
        if not photos:
            return photos
        recipes = db.get_photo_edit_recipes(
            [photo["photo_id"] for photo in photos],
        )
        for photo in photos:
            recipe = recipes.get(photo["photo_id"])
            photo["edit_recipe"] = recipe
            photo["render_key"] = render_key_for_recipe(recipe)
        return photos

    @app.route("/api/predictions/compare")
    def api_predictions_compare():
        """One page of the ID Conflicts comparison, plus every count it shows.

        Deriving what a row means — its status, whether the models disagree,
        whether it still needs review — takes a full pass over the
        collection's predictions and the taxonomy, so the result is kept as a
        snapshot (see id_conflicts) and the browser is handed a token to come
        back with. Filtering, sorting, searching and paging then run against
        that snapshot instead of re-deriving anything, and the rows on screen
        are rebuilt from the database so what the user acts on is current.

        Passing ``photo_id`` instead returns just those rows, with no
        snapshot: that is how a decision refreshes what it changed.
        """
        db = _get_db()
        collection_id = request.args.get("collection_id", None, type=int)
        if not collection_id:
            return json_error("collection_id required")
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        requested_photo_ids, err = _compare_photo_ids()
        if err is not None:
            return err

        requested_models = request.args.getlist("model")
        min_confidence = request.args.get("min_confidence", type=float)
        if min_confidence is None:
            min_confidence = id_conflicts.DEFAULT_MIN_CONFIDENCE
        min_confidence = min(1.0, max(0.0, min_confidence))

        if requested_photo_ids is not None:
            built = id_conflicts.build_comparison(
                db, collection_id, photo_ids=requested_photo_ids,
            )
            visible = id_conflicts.resolve_models(
                built["models"], requested_models,
            )
            for photo in built["photos"]:
                id_conflicts.attach_assessment(photo, visible, min_confidence)
            _attach_comparison_render_keys(db, built["photos"])
            return jsonify(built)

        refresh_ids, err = _compare_photo_ids("refresh_photo_id")
        if err is not None:
            return err

        workspace_id = db._ws_id()
        store = app.config["ID_CONFLICTS_SNAPSHOTS"]
        snapshot = store.get(request.args.get("token"))
        if snapshot is not None and not snapshot.matches(
            collection_id, workspace_id,
            id_conflicts.resolve_models(snapshot.all_models, requested_models),
            min_confidence,
        ):
            # The page changed something the snapshot was derived under —
            # collection, workspace, shown models or the conflict threshold —
            # so it has to be derived again.
            snapshot = None
        # A grouped decision can pull in a sibling carrying a model nothing in
        # the snapshot had. The rows, the columns and every count in there
        # were derived without it, so the snapshot is spent: serving from it
        # would put a column on the page whose numbers never considered the
        # model naming it. Derive the comparison again now, on this request.
        # That makes one decision click pay for a full rebuild, which on a
        # catalog-sized collection is seconds — but it takes a sibling
        # carrying a model no other photo in the collection has, and the
        # alternative is answering with numbers we know are wrong.
        if (
            snapshot is not None
            and refresh_ids
            and snapshot.patch(db, refresh_ids)
        ):
            store.discard(snapshot.token)
            snapshot = None
        if snapshot is None:
            snapshot = store.put(id_conflicts.build_snapshot(
                db, collection_id, workspace_id,
                models=requested_models, min_confidence=min_confidence,
            ))

        per_page = request.args.get("per_page", 60, type=int) or 60
        per_page = min(_COMPARE_MAX_PER_PAGE, max(1, per_page))
        selection = id_conflicts.select(
            snapshot.records,
            snapshot.models,
            filter_id=request.args.get("filter", "all"),
            excludes=request.args.getlist("exclude"),
            query=request.args.get("q", ""),
            match_case=request.args.get("match_case") == "1",
            whole_word=request.args.get("whole_word") == "1",
            sort=request.args.get("sort", "review_priority"),
            page=request.args.get("page", 1, type=int) or 1,
            per_page=per_page,
        )
        return jsonify({
            "token": snapshot.token,
            "models": snapshot.all_models,
            "visible_models": snapshot.models,
            "taxonomy_available": snapshot.taxonomy_available,
            "photos": _attach_comparison_render_keys(db, id_conflicts.page_rows(
                db, collection_id, selection.photo_ids,
                snapshot.models, snapshot.min_confidence,
            )),
            "page": selection.page,
            "per_page": per_page,
            "total": selection.total,
            "summary": selection.summary,
            "filter_counts": selection.filter_counts,
            "exclusion_counts": selection.exclusion_counts,
            "filters": [
                {"id": fid, "label": label} for fid, label in id_conflicts.FILTERS
            ],
            "sorts": [
                {"id": sid, "label": label} for sid, label in id_conflicts.SORTS
            ],
            "excludes": [
                {"id": eid, "label": label} for eid, label in id_conflicts.EXCLUDES
            ],
        })

    @app.route("/api/predictions/<int:pred_id>/reviewed", methods=["POST"])
    def api_mark_prediction_reviewed(pred_id):
        """Mark a pending prediction as reviewed, atomically.

        Under the same lock as the accept/reject routes. The existing
        pending-only precondition was correct but was read outside a
        transaction, so a batch-accept that landed between the read and the
        write could accept the row and then the reviewed write would overwrite
        the batch's ``accepted`` status with ``reviewed`` — losing the
        accept and the keyword together. Holding the writer lock across the
        read and the write is what makes the precondition actually mean what
        it says.
        """
        db = _get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            pred = db.conn.execute(
                """SELECT pr.id, pr.species, d.photo_id,
                          COALESCE(pr_rev.status, 'pending') AS status
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.id = ?""",
                (db._ws_id(), pred_id),
            ).fetchone()
            if pred is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            # Only pending predictions may transition to reviewed. Without
            # this guard a stale/double request or a direct API call against
            # an already accepted/rejected prediction would silently
            # overwrite the prior decision, corrupting review state and
            # audit history.
            if pred["status"] != "pending":
                db.conn.rollback()
                return json_error(
                    f'prediction already {pred["status"]}; cannot mark reviewed',
                    409,
                )
            db.update_prediction_status(pred_id, "reviewed", _commit=False)
            db.record_edit(
                "prediction_reviewed",
                f'Marked prediction "{pred["species"]}" reviewed',
                "reviewed",
                [{
                    "photo_id": pred["photo_id"],
                    "old_value": "pending",
                    "new_value": "reviewed",
                }],
                _commit=False,
            )
            db.conn.commit()
            return jsonify({"ok": True})
        except Exception:
            db.conn.rollback()
            raise

    @app.route("/api/predictions/<int:pred_id>/replace-keywords", methods=["POST"])
    def api_replace_species_keywords_with_prediction(pred_id):
        """Accept a prediction and replace existing species keywords atomically.

        Same lock as every other prediction-decision route. Without it, a
        replace-keywords request racing a batch-reject on the same row would
        strip conflicting species keywords under an accept while the reject
        commits the row as ``rejected`` — leaving the replaced photo tagged
        with a species now marked rejected, and the old species permanently
        lost even though undo cannot restore it.
        """
        db = _get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            current_status = _prediction_status(db, pred_id)
            if current_status is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if current_status in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f"prediction already {current_status}; cannot accept",
                    409,
                )
            # accept_prediction(replace_species=True) strips existing
            # species/taxonomy keywords from *every* photo it tags (the whole
            # group, not just this photo) inside one transaction, so grouped
            # photos are replaced consistently.
            result = db.accept_prediction(
                pred_id, replace_species=True, _commit=False,
            )
            if result is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            items = [
                {
                    "photo_id": a["photo_id"],
                    "old_value": ", ".join(a.get("old_species", [])),
                    "new_value": result["species"],
                }
                for a in result["affected"]
            ]
            is_batch = len(items) > 1
            desc = f'Replaced species keyword with "{result["species"]}"'
            if is_batch:
                desc += f" across {len(items)} photos"
            db.record_edit(
                "prediction_replace_species",
                desc,
                result["species"],
                items,
                is_batch=is_batch,
                _commit=False,
            )
            db.conn.commit()
            # Same reason as ``api_accept_prediction``: replace goes through
            # the same grouped expansion, so a looping caller needs the rows
            # this transaction decided rather than the one it asked about.
            return jsonify({
                "ok": True,
                "prediction_ids": result["accepted_prediction_ids"],
                "photo_ids": result["photo_ids"],
            })
        except Exception:
            db.conn.rollback()
            raise

    @app.route("/api/predictions/batch-accept", methods=["POST"])
    def api_batch_accept_predictions():
        """Accept many predictions of one species as a single action.

        Browse's selection panel accepts a species across the whole selection.
        This goes through ``accept_prediction`` rather than
        ``/api/batch/keyword`` because a prediction has two halves: the
        keyword tag AND the ``prediction_review`` status. Tagging alone would
        leave every photo still pending in Review, so the same work would have
        to be done a second time there.

        The whole batch lands as ONE ``prediction_accept`` edit so a single
        undo reverses it, matching the per-photo ``changed_tag`` / ``no_tag``
        encoding ``api_accept_prediction`` already uses.

        With ``photo_ids`` and ``expected_species``, Browse's "Accept on all"
        also adds that species to every selected photo missing it. Existing
        keywords are preserved. Prediction decisions still follow the checks
        below; photos without an acceptable prediction get keyword-only undo
        items. ``accepted`` counts photos changed by either part of the action.

        Contract: every row this endpoint accepts is one that is still
        undecided, still unambiguous, and still from the current label set *at
        the moment of the write* — judged against the database rather than
        against whatever the caller's panel believed. All three preconditions
        are re-derived here: statuses via ``_decided_prediction_ids``,
        staleness via ``_superseded_prediction_ids``, ambiguity via
        ``_ambiguous_prediction_ids`` (the same helper that produced the
        panel's ``acceptable_prediction_ids``). Rows failing any of them are
        skipped, never accepted, and counted back in ``already_decided``,
        ``skipped_superseded`` and ``skipped_ambiguous``. A stale payload can
        therefore accept less than the caller asked for, but never something
        the caller was not shown as acceptable.

        "At the moment of the write" is literal, not approximate: the checks
        and the writes run inside one ``BEGIN IMMEDIATE`` transaction
        (``prediction_decisions.begin_prediction_decision``), so no other
        connection can decide these rows in between. Two overlapping requests
        are serialized by SQLite's writer lock — the second reads what the
        first committed and skips accordingly, rather than acting on state it
        read before the first one wrote.
        """
        db = _get_db()
        body = request.get_json(silent=True) or {}
        # ``replace_species=True`` strips conflicting species keywords from
        # every tagged photo, but the batch endpoint records one
        # ``prediction_accept`` edit whose ``old_value`` carries only the
        # prediction id — no room for the removed keyword names. Undoing
        # that entry would restore prediction status but leave the
        # replaced species permanently gone. The single-photo replace
        # route (``/api/predictions/<id>/replace-keywords``) sidesteps this
        # by recording ``prediction_replace_species``, which is explicitly
        # non-undoable. Refuse the flag here rather than silently drop it
        # so a mis-wired caller learns immediately.
        if bool(body.get("replace_species")):
            return json_error(
                "replace_species is not supported on batch-accept because "
                "the batched undo entry cannot restore removed keywords; "
                "use /api/predictions/<pred_id>/replace-keywords for a "
                "single-photo replacement",
                400,
            )
        # An explicit selection extends the accept to photos without a
        # matching prediction. Their keyword additions share the same undo.
        all_photo_ids = None
        if "photo_ids" in body:
            all_photo_ids, err = parse_selection_photo_ids(db, body, json_error=json_error)
            if err is not None:
                return err
        if all_photo_ids is not None and body.get("prediction_ids") == []:
            pred_ids, err = [], None
        else:
            pred_ids, err = _parse_prediction_ids(db, body)
        if err is not None:
            return err

        # ``expected_species`` is the species the button in Browse names —
        # what "Accept on 38 Bald Eagle" would tag. Passed through so the
        # endpoint can refuse to accept a row whose grouping (and therefore
        # ``accept_prediction``'s applied species) drifted after the panel
        # rendered but before the lock. Optional so single-species callers
        # that already know they only submit one bucket at a time keep
        # working unchanged; when omitted, the drift check is skipped and
        # the endpoint's older contract holds.
        raw_expected = body.get("expected_species")
        expected_species = (
            raw_expected.strip() if isinstance(raw_expected, str) else None
        ) or None
        if all_photo_ids is not None and not expected_species:
            return json_error("expected_species required when accepting on all photos")

        # Everything from here to the commit is one transaction, taken with
        # the writer lock held from the first read (see
        # ``prediction_decisions.begin_prediction_decision``). The
        # preconditions below are only worth what their atomicity with the
        # write is worth: read them
        # outside the transaction and a second overlapping request can pass the
        # same checks against the same pre-write state.
        #
        # ``_parse_prediction_ids`` stays outside deliberately — it validates
        # the payload's shape and workspace ownership, which is not the state
        # these preconditions race against, and it can walk a 1,000-photo
        # selection. The lock is held for the decision, not for parsing. The
        # in-lock ``prediction_decisions.out_of_workspace_prediction_ids``
        # filter re-checks the workspace half so a folder detach that lands in
        # the window between parse and lock cannot tag a now-foreign photo.
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if all_photo_ids is not None:
                # Recheck ownership inside the write transaction, including
                # photos that have no prediction rows to validate below.
                all_photo_ids, err = parse_selection_photo_ids(db, body, json_error=json_error)
                if err is not None:
                    db.conn.rollback()
                    return err
                selected = set(all_photo_ids)
                if any(row["photo_id"] not in selected for row in _load_prediction_rows(db, pred_ids)):
                    db.conn.rollback()
                    return json_error("prediction_ids must belong to the selected photos")
            return _batch_accept_under_lock(db, pred_ids, expected_species, all_photo_ids)
        except Exception:
            db.conn.rollback()
            raise

    def _batch_accept_under_lock(db, pred_ids, expected_species=None, all_photo_ids=None):
        """The checks and writes of ``batch-accept``, inside its transaction.

        Split out only so the transaction's boundaries are impossible to
        misread: every statement here runs with the writer lock already held,
        and the single ``commit`` at the end is the moment any of it becomes
        visible to another request.
        """
        # Make a submission of an already-decided row a no-op rather than a
        # second accept. A double-clicked Accept button or a stale panel would
        # otherwise re-accept rows that are already accepted: the keyword now
        # exists, so the second pass records a status-only ``no_tag`` item
        # whose "previous" status is a fiction — undoing it would knock a
        # long-accepted prediction back to pending while keeping the keyword.
        # ``rejected`` rows are skipped for a sharper reason: when the user
        # accepts an alternative in Review or another tab, this row's sibling
        # wins and this row becomes the rejected loser. Accepting it from a
        # payload Browse rendered before that happened would tag the photo
        # with the species the user just rejected, and the batch's undo entry
        # would then reset the whole sibling scope to pending/alternative
        # instead of restoring the winner's accepted state.
        already_decided = _decided_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in already_decided]

        # Drop rows whose label set the catalog has moved past. Re-classifying
        # a detection after Browse rendered the panel leaves the old row
        # ``pending`` — nothing rewrites it — while every read path, including
        # the panel that produced this payload, has already switched to the
        # newest ``labels_fingerprint``. Accepting the old row would tag the
        # photo from a label set nothing displays and mark accepted a row the
        # user can no longer see, while the current prediction stayed pending.
        #
        # Before the ambiguity check so the two counts stay disjoint: a
        # superseded row is reported as superseded, not as a conflict the user
        # would go to Review to resolve and never find.
        superseded_ids = _superseded_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in superseded_ids]

        # Re-derive ambiguity instead of trusting the payload. The decided
        # filter above only catches rows whose *status* moved; a row can stay
        # ``pending`` and still stop being safe to bare-accept, because
        # ambiguity is a function of the photo's current species keywords.
        # Add a Golden Eagle keyword from Review or a second tab after Browse
        # rendered "Accept on 35", and the Bald Eagle row Browse listed as
        # acceptable is now a conflict — the panel would route it to Review,
        # but the button in the stale document still posts it. Skipping it
        # here is what makes "Accept" mean the same thing at click time as it
        # did at render time (``CORE_PHILOSOPHY.md``, no black boxes).
        #
        # Skipped rather than a 400 for the same reason ``already_decided``
        # is: the rest of the batch is still exactly what the user asked for,
        # and the panel refresh that follows re-renders the skipped rows in
        # their ambiguous form, with the Review hand-off. Failing the whole
        # call would strand 34 honest accepts on one row that moved.
        ambiguous_ids = _ambiguous_prediction_ids(
            db, _load_prediction_rows(db, pred_ids),
        )
        pred_ids = [pid for pid in pred_ids if pid not in ambiguous_ids]

        # A folder detach — itself a write — can happen between
        # ``_parse_prediction_ids`` and ``BEGIN IMMEDIATE``. Re-check
        # workspace ownership here so a batch cannot tag a photo that left
        # the workspace in that window (and cannot write workspace-scoped
        # ``prediction_review`` state for a row it no longer owns). Skipped
        # rather than 403 for the same reason ``already_decided`` is: the
        # rest of the batch is still exactly what the user asked for, and
        # failing the whole call would strand honest accepts on one row that
        # moved.
        out_of_workspace_ids = (
            prediction_decisions.out_of_workspace_prediction_ids(db, pred_ids)
        )
        pred_ids = [pid for pid in pred_ids if pid not in out_of_workspace_ids]

        # The button in Browse names one species and the endpoint should
        # apply exactly that species. Consensus can drift after render:
        # another tab ungrouping the burst clears ``individual`` votes so
        # ``accept_prediction`` falls back to the raw per-frame label, and
        # per-vote edits can shift the winner. Skip rows whose current
        # consensus no longer matches. No-op when the caller passes no
        # species (older tests, single-species callers that never render a
        # multi-species button).
        drifted_ids = _species_drifted_prediction_ids(
            db, _load_prediction_rows(db, pred_ids), expected_species,
        )
        pred_ids = [pid for pid in pred_ids if pid not in drifted_ids]

        # Confine the whole batch to the rows the caller actually submitted.
        # ``accept_prediction`` otherwise expands a grouped (burst) accept to
        # every row in the group, tagging photos the user never selected.
        #
        # The limit travels as prediction ids, not as any photo set derived
        # from them, because a photo is not a unique key for a prediction row:
        # one burst photo can carry a row per classifier model and a row per
        # detection. Under a photo-set limit — batch-wide or per
        # ``(group, model)`` bucket — submitting photo A's model-X row and
        # photo B's model-Y row lets A's grouped accept reach B's model-X row,
        # a row the panel deliberately omitted (below threshold, ambiguous, or
        # already accepted). Row identity has no such projection to get wrong,
        # whatever column next distinguishes two rows on one photo. Note this
        # set is built *after* both filters above, so a row that was decided
        # elsewhere, or that has since become ambiguous, cannot be re-accepted
        # through a sibling's group expansion either.
        submitted_pred_ids = set(pred_ids)
        # A grouped accept resolves every submitted sibling in its group in
        # one call — including each one's losing alternatives, which
        # ``accept_prediction`` now rejects per accepted row rather than only
        # for the entry row. So the remaining siblings in this loop are
        # already fully done; re-entering them was O(N^2) in group size and
        # appended a second, status-only history item per photo whose
        # recorded "previous" status is a fiction.
        handled = set()
        items = []
        keyword_id = None
        species = None
        species_key = None
        for pid in pred_ids:
            if pid in handled:
                continue
            result = db.accept_prediction(
                pid,
                prediction_ids=submitted_pred_ids,
                _commit=False,
            )
            handled.add(pid)
            if result is None:
                continue
            accepted_now = result.get("accepted_prediction_ids", ())
            handled.update(accepted_now)
            if not accepted_now:
                # A no-op accept (nothing in this row's scope) changed
                # nothing, so it carries no species to reconcile against
                # the batch's — and its ``keyword_id`` may be None because
                # no keyword was created. Folding it into the check below
                # would 400 a perfectly uniform batch.
                continue
            # Browse groups predictions by species identity, not keyword ID.
            # Different aliases can legitimately tag different keyword rows
            # of that species. Record the actual ID on each history item so
            # undo/redo reverses exactly that tag, while still rejecting a
            # batch that resolves to genuinely different species.
            if keyword_id is None:
                keyword_id, species = result["keyword_id"], result["species"]
                species_key = result["species_key"]
            elif result["species_key"] != species_key:
                db.conn.rollback()
                return json_error(
                    "prediction_ids must all resolve to one species", 400,
                )
            for a in result["affected"]:
                if a.get("changed_tag", True):
                    old_value = str(a["prediction_id"])
                else:
                    old_value = json.dumps({
                        "prediction_id": a["prediction_id"],
                        "no_tag": True,
                    })
                items.append({
                    "photo_id": a["photo_id"],
                    "old_value": old_value,
                    "new_value": str(result["keyword_id"]),
                })

        has_accepted_predictions = bool(items)
        if all_photo_ids is not None:
            if keyword_id is None:
                from species_identity import SpeciesResolver
                resolver = SpeciesResolver(db=db)
                identity = resolver.display(expected_species)
                # Only bind the resolved taxon when ``expected_species``
                # carried an explicit ``(scientific)``/``(taxon N)``
                # qualifier. A bare common name is name-only inference,
                # and routing it through ``_add_source_species_keyword``
                # refuses to reuse an unlinked same-name keyword the async
                # ``mark_species_keywords`` pass has not touched yet,
                # minting a suffixed duplicate such as
                # ``California Towhee (taxon 42)``. Mirrors the guard in
                # ``accept_prediction`` so both accept paths behave the
                # same for a legacy/name-only bucket.
                explicit = resolver.explicit_source(expected_species) is not None
                keyword_id = db.add_keyword(
                    identity.display_name, is_species=True, _commit=False,
                    source_taxon_id=identity.taxon_id if explicit else None,
                )
                species = db.conn.execute(
                    "SELECT name FROM keywords WHERE id = ?", (keyword_id,),
                ).fetchone()["name"]
            already_tagged = db.get_photos_with_equivalent_species(all_photo_ids, keyword_id)
            for photo_id in all_photo_ids:
                if photo_id in already_tagged:
                    continue
                db.tag_photo(photo_id, keyword_id, source="manual", _commit=False)
                items.append({
                    "photo_id": photo_id,
                    "old_value": json.dumps({"keyword_only": True}),
                    "new_value": str(keyword_id),
                })

            # Every new tag in Accept on all needs the same sidecar handling,
            # including tags already added by accept_prediction above.
            for item in items:
                old_value = item["old_value"]
                old_meta = (
                    json.loads(old_value) if old_value.startswith("{")
                    else {"prediction_id": int(old_value)}
                )
                if old_meta.get("no_tag"):
                    continue
                photo_id = item["photo_id"]
                item_species = db.conn.execute(
                    "SELECT name FROM keywords WHERE id = ?", (int(item["new_value"]),),
                ).fetchone()["name"]
                flat_removals = [dict(row) for row in db.conn.execute(
                    """SELECT workspace_id, value FROM pending_changes
                       WHERE photo_id = ? AND change_type = 'keyword_remove_flat'
                         AND value = ? COLLATE NOCASE""",
                    (photo_id, item_species),
                )]
                # accept_prediction queues an add directly. Reconcile it
                # with any pending removal before applying the shared helper.
                db.remove_pending_changes(photo_id, "keyword_add", item_species, _commit=False)
                queue_keyword_add(db, photo_id, item_species, _commit=False)
                # Keep the suppression records cleared by the add, including
                # those in other workspaces sharing this photo's sidecar.
                old_meta.update(symmetric_keyword_queue=True, flat_removals=flat_removals)
                item["old_value"] = json.dumps(old_meta)

        # History joins the same transaction rather than committing after it:
        # the accepted statuses and the entry that undoes them become visible
        # together, so no reader can see accepted rows with no way back.
        if items:
            photo_count = len({item["photo_id"] for item in items})
            desc = (
                f'Accepted prediction: added "{species}"' if has_accepted_predictions
                else f'Added species "{species}"'
            )
            if photo_count > 1:
                desc += f" to {photo_count} photos"
            db.record_edit(
                "prediction_accept", desc, str(keyword_id), items,
                is_batch=photo_count > 1, _commit=False,
            )
        db.conn.commit()
        if items:
            # ``record_edit`` skips its prune under ``_commit=False``; run it
            # once the decision is durable so history stays bounded.
            db._prune_edit_history()
        return jsonify({
            "ok": True,
            "accepted": len({item["photo_id"] for item in items}),
            # Reported rather than folded into ``accepted``: a caller that
            # resubmits should be able to tell "nothing to do, already
            # decided" from "nothing matched". Named ``already_decided``
            # rather than ``already_accepted`` because it counts rejected
            # rows too — a field whose name implies a narrower set than it
            # holds is the kind of quiet mis-description CORE_PHILOSOPHY.md
            # rules out, and this count is the only signal a caller gets for
            # rows the batch deliberately left alone.
            "already_decided": len(already_decided),
            # Rows still pending but no longer safe to bare-accept, because
            # the photo's keywords moved after the panel rendered. Reported
            # separately from ``already_decided`` because the user's next step
            # differs: a decided row needs nothing, an ambiguous one needs
            # Review. Browse turns this into a toast rather than letting the
            # count vanish between "Accept on 35" and 33 accepts.
            "skipped_ambiguous": len(ambiguous_ids),
            # Rows still pending, but from a label set a later classification
            # run replaced. Its own count for the same reason: the user's next
            # step is neither "nothing" nor "Review" — the refreshed panel
            # simply shows the current prediction in this row's place, and a
            # count folded into ``skipped_ambiguous`` would send them hunting
            # for a keyword conflict that does not exist.
            "skipped_superseded": len(superseded_ids),
            # Rows whose photo left the workspace between parse and lock. A
            # panel refresh drops the photo from view, so the user's next
            # step is neither Review nor a re-run — just the refresh — and
            # folding it into any of the other counts would misname it.
            "skipped_out_of_workspace": len(out_of_workspace_ids),
            # Rows whose current consensus species no longer matches the one
            # the button named — another tab ungrouped the burst, or per-vote
            # edits shifted the winner. Only computed when the caller passed
            # ``expected_species``; older callers see 0 here.
            "skipped_species_drifted": len(drifted_ids),
            "species": species,
        })

    def _load_prediction_rows(db, pred_ids):
        """Load ``pred_ids`` in the shape ``_ambiguous_prediction_ids`` wants.

        Selected by id rather than through ``get_predictions(photo_ids=...)``
        so a submitted row is judged on its own merits: the photo-scoped query
        also returns siblings the caller never submitted, and filters to the
        latest ``labels_fingerprint`` — which would silently drop a superseded
        row from the ambiguity check and leave the caller unable to tell "not
        ambiguous" from "not current". Staleness is judged explicitly instead,
        by ``_superseded_prediction_ids``, and reported under its own name.
        Workspace scoping is already settled by ``_parse_prediction_ids``,
        which runs first.

        Chunked for the same reason every other id query here is — a legal
        payload runs past the 999-variable limit older SQLite builds enforce.
        """
        if not pred_ids:
            return []
        ws = db._ws_id()
        rows = []
        for chunk in _chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(db.conn.execute(
                f"""SELECT pr.id, pr.species, pr.category, pr.detection_id,
                           pr.source_taxon_id, pr.scientific_name, pr.labels_fingerprint,
                           pr.classifier_model AS model, d.photo_id,
                           pr_rev.group_id AS group_id,
                           pr_rev.individual AS individual
                    FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    LEFT JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id
                     AND pr_rev.workspace_id = ?
                    WHERE pr.id IN ({placeholders})""",
                (ws, *chunk),
            ).fetchall())
        return rows

    # The one definition of "already decided" for every prediction-decision
    # endpoint (batch and single-row alike). ``pending`` rows are the normal
    # case; ``alternative`` is not a decision, so this list deliberately omits
    # it — but that only means the rows survive *this* filter. ``batch-reject``
    # then acts on them (a reject sweeps the runners-up down with the winner);
    # ``batch-accept`` still refuses every row on a ``(detection, model)`` that
    # carries one, via ``_ambiguous_prediction_ids`` below, because promoting a
    # runner-up picks a winner the user was never shown. Skipped there is
    # reported as ``skipped_ambiguous``, never ``already_decided``: the two
    # counts name different problems with different next steps.
    #
    # ``reviewed`` is included because it *is* a decision: the user pressed
    # "Reviewed" in Review to say "I looked at this and chose not to act". A
    # later accept or reject flip would overwrite that decision the same way a
    # stale accept overwrites a rejected row's sibling scope, and the history
    # entry the flip records reports a "previous" status of ``pending`` — a
    # fiction. Batch endpoints skip the row and report it as
    # ``already_decided``; single-row endpoints refuse with 409, mirroring
    # ``api_mark_prediction_reviewed``'s own precondition.
    # Owned by ``Database`` so ``db.py``'s grouped-accept expansion and this
    # module read the same tuple rather than two copies of it; Browse's panel
    # mirrors the same three values in ``PREDICTION_DECIDED_STATUSES``.
    _DECIDED_PREDICTION_STATUSES = Database.DECIDED_PREDICTION_STATUSES

    def _decided_prediction_ids(db, pred_ids):
        """Which of ``pred_ids`` already have a decision recorded.

        Both batch endpoints need the same precondition, so neither gets to
        pick its own status list: the panel that produced this payload listed
        the rows as pending, but a decision may have landed since — from a
        double-clicked button, a second tab, or Review. Acting on a row whose
        decision is already made writes a history item whose recorded
        "previous" status is a fiction, and the two directions fail in
        mirrored ways: a stale reject leaves an accepted row's species keyword
        attached to a row now marked ``rejected``, and a stale accept tags a
        photo with the loser the user just rejected by accepting its sibling.
        The statuses live here, not at the call sites, so the pair cannot
        drift apart again on what "still actionable" means.

        Chunked: a legal payload runs well past the 999-variable limit older
        SQLite builds enforce (see ``_SQL_PARAM_CHUNK``).
        """
        ws = db._ws_id()
        statuses = _DECIDED_PREDICTION_STATUSES
        if not pred_ids:
            return set()
        status_ph = ",".join("?" for _ in statuses)
        found = set()
        for chunk in _chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                row["prediction_id"] for row in db.conn.execute(
                    f"""SELECT prediction_id FROM prediction_review
                        WHERE workspace_id = ? AND status IN ({status_ph})
                          AND prediction_id IN ({placeholders})""",
                    (ws, *statuses, *chunk),
                )
            )
        return found

    def _prediction_status(db, pred_id):
        """Current review status of one prediction in the active workspace.

        Single-row decision endpoints (accept, reject, mark-reviewed,
        replace-keywords, accept-subject) use this inside
        ``prediction_decisions.begin_prediction_decision`` to enforce the same
        "still actionable" precondition the batch endpoints already enforce
        via ``_decided_prediction_ids``. Same reason the batch helper exists:
        the rule for which statuses are terminal lives in one place, so the
        single-row and batch flavors cannot drift on what "already decided"
        means, and neither flavor can be tightened on one side and forgotten
        on the other.

        Returns ``"pending"`` when no ``prediction_review`` row exists yet,
        the stored status string when one does, and ``None`` when the
        prediction id is unknown. Callers combine that with
        ``_DECIDED_PREDICTION_STATUSES`` to decide whether to 409.
        """
        row = db.conn.execute(
            """SELECT COALESCE(pr_rev.status, 'pending') AS status
               FROM predictions pr
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id
                AND pr_rev.workspace_id = ?
               WHERE pr.id = ?""",
            (db._ws_id(), pred_id),
        ).fetchone()
        return row["status"] if row else None

    def _superseded_prediction_ids(db, pred_ids):
        """Which of ``pred_ids`` belong to a label set the catalog moved past.

        The third precondition both batch endpoints share, alongside
        ``_decided_prediction_ids`` and ``_ambiguous_prediction_ids``, and the
        same *shape* as those two: a payload that was truthful when the panel
        rendered it and is not truthful any more. Re-classify a detection
        against a new label set between render and click and the old row stays
        ``pending`` — nothing rewrites it — while ``get_predictions`` (and so
        every panel, Review grid and summary) has already moved to the newest
        ``labels_fingerprint`` for that ``(detection, classifier_model)``.
        Accepting the old row would tag the photo with a species from a label
        set the catalog no longer shows, and mark a row accepted that no
        surface displays; rejecting it would report a dismissal while the
        current row stays pending in the panel the user is looking at.

        A peer helper rather than a branch inside ``_ambiguous_prediction_ids``
        because the two verdicts are not the same fact and do not lead the user
        to the same place: an ambiguous row needs a decision in Review, a
        superseded row needs nothing at all — the panel refresh simply shows
        the current row in its place. Folding it in would put superseded rows
        under a ``skipped_ambiguous`` count that names a conflict the user
        would go looking for and never find, which is the sort of quiet
        mis-description ``CORE_PHILOSOPHY.md`` rules out. What *is* shared is
        that the rule has one implementation for both endpoints, so accept and
        reject cannot drift on what "current" means.

        The latest-fingerprint expression is the one ``get_predictions`` and
        ``get_top_prediction_for_photo`` both use: newest ``created_at``, ties
        broken by ``id``.

        Chunked for the same reason every other id query here is (see
        ``_SQL_PARAM_CHUNK``).
        """
        if not pred_ids:
            return set()
        found = set()
        for chunk in _chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                row["id"] for row in db.conn.execute(
                    f"""SELECT pr.id FROM predictions pr
                        WHERE pr.id IN ({placeholders})
                          AND pr.labels_fingerprint != (
                              SELECT pr2.labels_fingerprint FROM predictions pr2
                              WHERE pr2.detection_id = pr.detection_id
                                AND pr2.classifier_model = pr.classifier_model
                              ORDER BY pr2.created_at DESC, pr2.id DESC
                              LIMIT 1)""",
                    chunk,
                )
            )
        return found

    def _parse_observed_statuses(body):
        """Validate ``/api/predictions/group/apply``'s render-time baseline.

        The burst modal sends ``observed``: the status it displayed for each
        group member when it loaded (``{prediction_id: status}``). Returns
        ``(observed, None)`` or ``(None, error_response)``.

        Absent or empty means "no baseline", and the route then applies
        unconditionally — the server can only refuse what the client claims to
        have seen. That is not a hole a caller can pick its way through so
        much as the honest limit of the check; the one caller
        (``review.html``'s ``grmApply``) always sends it, and
        ``test_group_apply_client_sends_the_observed_baseline`` fails if that
        stops being true.
        """
        raw = body.get("observed")
        if raw is None:
            return {}, None
        if not isinstance(raw, dict):
            return None, json_error(
                "observed must be an object mapping prediction id to status"
            )
        observed = {}
        for key, value in raw.items():
            try:
                pred_id = int(key)
            except (TypeError, ValueError):
                return None, json_error(
                    "observed keys must be prediction ids"
                )
            if not isinstance(value, str):
                return None, json_error(
                    "observed values must be status strings"
                )
            observed[pred_id] = value
        return observed, None

    def _stale_group_apply_photos(db, observed):
        """Photos whose group member was decided or regenerated since the modal rendered.

        Group apply is the one decision route where "already decided" is *not*
        the right precondition. The single-row and batch endpoints can use it
        because their buttons only exist on pending rows, so a decided row can
        only have been decided by someone else. The burst modal is different:
        it opens on any card carrying a ``group_id`` — including one whose
        members this same user accepted a minute ago — and
        ``loadGroupData`` re-derives picks/rejects from quality scores rather
        than from the stored statuses. Refusing every decided row would block
        a legitimate flow (re-open the burst, change the split, apply) *and*
        would describe the user's own prior decision as somebody else's.

        So the precondition is compare-and-swap against what the modal
        actually displayed: skip a photo only when the picture the modal saw
        has moved. Two shapes of "moved" invalidate a photo, in the same
        pass so the check cannot narrow to one and miss the other:

        1. **Status drift on an observed row.** An observed row's stored
           status is no longer the one the client displayed — a decision that
           landed from Browse or a second tab after the modal opened. A
           deliberate re-decision passes (observed ``accepted`` still matches
           current ``accepted``); a foreign decision does not.
        2. **Superseded label set.** An observed row is no longer the latest
           ``labels_fingerprint`` for its ``(detection, classifier_model)`` —
           classification reran between render and click and inserted new
           prediction rows for the same detections. The old row still exists
           with an unchanged status, so shape 1 alone would clear the check.
           But the write below (``update_predictions_status_by_photo``) does
           not respect the observed set: it rewrites every prediction on the
           photo, including the newly inserted rows the modal never saw, and
           applies the modal's stale species to them.

        Photo-level because the write is: ``update_predictions_status_by_photo``
        restates every prediction of the photo, so one stale member
        invalidates the whole photo's write, not just its own row.

        The latest-fingerprint expression is the one
        ``_superseded_prediction_ids``, ``get_predictions`` and
        ``get_top_prediction_for_photo`` all use: newest ``created_at``, ties
        broken by ``id``. Keeping one definition of "current" everywhere is
        the property the rest of this PR's precondition family relies on.
        """
        if not observed:
            return set()
        ws = db._ws_id()
        stale = set()
        for chunk in _chunked(list(observed)):
            placeholders = ",".join("?" for _ in chunk)
            for row in db.conn.execute(
                f"""SELECT pr.id AS prediction_id, d.photo_id AS photo_id,
                           COALESCE(pr_rev.status, 'pending') AS status,
                           (pr.labels_fingerprint != (
                               SELECT pr2.labels_fingerprint FROM predictions pr2
                               WHERE pr2.detection_id = pr.detection_id
                                 AND pr2.classifier_model = pr.classifier_model
                               ORDER BY pr2.created_at DESC, pr2.id DESC
                               LIMIT 1
                           )) AS is_superseded
                      FROM predictions pr
                      JOIN detections d ON d.id = pr.detection_id
                      LEFT JOIN prediction_review pr_rev
                        ON pr_rev.prediction_id = pr.id
                       AND pr_rev.workspace_id = ?
                     WHERE pr.id IN ({placeholders})""",
                (ws, *chunk),
            ):
                if (row["status"] != observed[row["prediction_id"]]
                        or row["is_superseded"]):
                    stale.add(row["photo_id"])
        return stale

    def _species_drifted_prediction_ids(db, rows, expected_species):
        """Which of ``rows`` no longer resolve to the species Browse rendered.

        Both accept panels group by species and label the button with the
        species they will apply — "Accept on 38 Bald Eagle". If the row's
        grouping changes between render and click, ``accept_prediction``
        will now apply a different species than the button named. Two known
        shapes:

        * ``/api/predictions/group/apply`` in another tab ungroups a burst
          member, so the row's ``individual`` votes are cleared and
          ``SpeciesResolver.consensus`` falls back to the raw per-frame
          label — a Robin-consensus row for a frame whose own species column
          reads Sparrow now accepts as Sparrow, not the Robin the button
          named.
        * Re-running consensus after a per-vote edit shifts the winner (Robin
          4 → Robin 2, Sparrow 4).

        Both let the endpoint tag the photo with a species the caller was
        never shown as the target of this click, and the shared "all resolve
        to one species" precondition below doesn't catch it when *every* row
        drifts to the same new species.

        No-ops when ``expected_species`` is falsy — a caller with no species
        in hand (Review's single-photo routes, older tests) is out of scope
        for this check. Compare resolved identities so common/scientific aliases
        do not register as drift. Unresolved labels retain text matching.

        Returns the drifted-row subset of ``rows`` ids.
        """
        rows = list(rows)
        if not rows or not expected_species:
            return set()
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        expected_key = resolver.display(expected_species).key
        drifted = set()
        for row in rows:
            current = resolver.consensus(row)
            if current.key != expected_key and keyword_match_key(current.display_name) != keyword_match_key(expected_species):
                drifted.add(row["id"])
        return drifted

    def _parse_prediction_ids(db, body):
        """Validate a batch payload's ``prediction_ids``.

        Returns ``(prediction_ids, None)`` or ``(None, error_response)``.
        Every id must exist and belong to a photo in the active workspace, so
        a batch can never reach across a workspace boundary.

        Size is bounded in the one unit the producer bounds: photos. These
        payloads come from ``/api/selection/prediction-suggestions``, which
        accepts at most ``MAX_SELECTION_PHOTOS`` photos and then emits every
        matching prediction row for them — a count it does not (and should
        not) cap, since a photo legitimately carries one row per detection per
        classifier model. Counting *ids* here therefore cannot be done without
        inventing a rows-per-photo guess, and three rounds of review found the
        guess wrong each time (1,000, then 25,000, then 200,000). Counting
        distinct photos instead makes the invariant hold by construction: any
        payload the suggestions endpoint can legally emit spans at most the
        photo selection it was given, so this endpoint accepts it — no margin
        to re-tune.
        """
        raw_ids = body.get("prediction_ids", [])
        if not isinstance(raw_ids, list) or not raw_ids:
            return None, json_error("prediction_ids required")
        pred_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return None, json_error("prediction_ids must be integers")
            if raw not in seen:
                pred_ids.append(raw)
                seen.add(raw)

        # Chunked because a legal payload runs to many thousands of ids, and a
        # single IN clause that wide exceeds the 999-variable limit older
        # SQLite builds enforce. ``_SQL_PARAM_CHUNK`` is the module-wide
        # convention for exactly this.
        photo_by_pred = {}
        for chunk in _chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            photo_by_pred.update({
                row["id"]: row["photo_id"] for row in db.conn.execute(
                    f"""SELECT pr.id, d.photo_id
                        FROM predictions pr
                        JOIN detections d ON d.id = pr.detection_id
                        WHERE pr.id IN ({placeholders})""",
                    chunk,
                ).fetchall()
            })
            # Bail as soon as the payload outgrows a legal selection rather
            # than resolving the rest of a runaway request.
            if len(set(photo_by_pred.values())) > MAX_SELECTION_PHOTOS:
                return None, json_error("too many photos in selection", 400)
        for pid in pred_ids:
            if pid not in photo_by_pred:
                return None, json_error(f"Prediction {pid} not found", 404)
        # One workspace query per distinct photo, not per prediction id:
        # several rows on one photo must not cost several round trips each.
        for photo_id in dict.fromkeys(photo_by_pred.values()):
            if not db._photo_in_workspace(photo_id):
                return None, json_error(
                    f"Photo {photo_id} does not belong to the "
                    "active workspace", 403,
                )
        return pred_ids, None

    @app.route("/api/predictions/batch-reject", methods=["POST"])
    def api_batch_reject_predictions():
        """Reject many predictions as a single undoable action.

        A photo with several detections carries one prediction row per
        detection, so Browse groups them into one species row. Dismissing that
        row must be one Cmd-Z, not one per detection.

        Contract, and the transaction that backs it, are ``batch-accept``'s:
        the preconditions and the writes share one ``BEGIN IMMEDIATE``
        transaction, so an Accept and a Reject fired before the panel reloads
        are serialized instead of interleaved. Rows already decided, or from a
        superseded label set, are skipped and counted back.
        """
        db = _get_db()
        body = request.get_json(silent=True) or {}
        pred_ids, err = _parse_prediction_ids(db, body)
        if err is not None:
            return err

        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            return _batch_reject_under_lock(db, pred_ids)
        except Exception:
            db.conn.rollback()
            raise

    def _batch_reject_under_lock(db, pred_ids):
        """The checks and writes of ``batch-reject``, inside its transaction."""
        # The same filter ``batch-accept`` applies, through the same helper —
        # not a parallel copy that can be tightened on one side only. Without
        # it, a stale panel — or an Accept then Reject before the panel
        # reloads — overwrites an ``accepted`` row with ``rejected`` while the
        # species keyword the accept added stays on the photo: keyword state
        # and review state then contradict each other, and nothing in the UI
        # points at the contradiction. Re-rejecting an already ``rejected``
        # row is skipped for the matching reason: it appends a history item
        # whose ``old_value`` of "pending" never happened.
        already_decided = _decided_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in already_decided]

        # Superseded rows are skipped on this side too, through the same
        # helper. A reject writes no keyword, so the damage is smaller than a
        # stale accept's — but it is the same misreport: the user dismisses a
        # species, the row that vanished from every panel is the one marked
        # ``rejected``, and the current prediction the panel *does* show stays
        # pending. One rule, one implementation, both endpoints — the lesson
        # the status precondition already taught here.
        superseded_ids = _superseded_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in superseded_ids]

        # Same shape as batch-accept: the workspace half of
        # ``_parse_prediction_ids`` runs outside the lock and a folder detach
        # can slip through in the parse→lock window. A reject writes no
        # keyword, but a workspace-scoped ``prediction_review`` row for a
        # now-foreign photo is the same class of leak the accept side
        # closes, and both endpoints filter identically for the same reason
        # they share ``_decided_prediction_ids``: a rule with two
        # implementations drifts.
        out_of_workspace_ids = (
            prediction_decisions.out_of_workspace_prediction_ids(db, pred_ids)
        )
        pred_ids = [pid for pid in pred_ids if pid not in out_of_workspace_ids]

        ws = db._ws_id()
        items = []
        species = None
        for pid in pred_ids:
            pred = db.conn.execute(
                """SELECT pr.id, pr.species, pr.detection_id,
                          pr.classifier_model AS model,
                          pr.labels_fingerprint, d.photo_id
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   WHERE pr.id = ?""",
                (pid,),
            ).fetchone()
            if pred is None:
                continue
            species = species or pred["species"]
            db.update_prediction_status(pid, "rejected", _commit=False)
            # Sibling alternatives go down with the parent, scoped by
            # fingerprint so a new label set can't rewrite an old one's
            # review state — same rule as the single-prediction reject.
            for row in db.conn.execute(
                """SELECT pr.id FROM predictions pr
                   JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ? AND pr.id != ?
                     AND pr_rev.status = 'alternative'""",
                (ws, pred["detection_id"], pred["model"],
                 pred["labels_fingerprint"], pid),
            ).fetchall():
                db.update_prediction_status(row["id"], "rejected", _commit=False)
            items.append({
                "photo_id": pred["photo_id"],
                "old_value": "pending",
                "new_value": "rejected",
            })

        # In the same transaction as the statuses it records, for the reason
        # ``batch-accept`` gives.
        if items:
            photo_count = len({item["photo_id"] for item in items})
            desc = f'Rejected prediction "{species}"'
            if photo_count > 1:
                desc += f" on {photo_count} photos"
            db.record_edit(
                "prediction_reject", desc, "rejected", items,
                is_batch=photo_count > 1, _commit=False,
            )
        db.conn.commit()
        if items:
            db._prune_edit_history()
        return jsonify({
            "ok": True,
            "rejected": len(items),
            # Reported rather than folded into ``rejected``, under the same
            # name batch-accept uses: a caller that resubmits should be able
            # to tell "nothing to do, already decided" from "nothing matched".
            "already_decided": len(already_decided),
            # Same name and meaning as on the accept side: rows a later
            # classification run replaced.
            "skipped_superseded": len(superseded_ids),
            # Same name and meaning as on the accept side: rows whose photo
            # left the workspace between parse and lock. Reported so a
            # panel-refresh caller can distinguish "detached mid-flight"
            # from "already rejected" or "superseded".
            "skipped_out_of_workspace": len(out_of_workspace_ids),
        })

    @app.route("/api/predictions/<int:pred_id>/accept", methods=["POST"])
    def api_accept_prediction(pred_id):
        """Accept a single prediction as one atomic decision.

        Serialized with every other prediction-decision route through
        ``prediction_decisions.begin_prediction_decision`` — batch-accept,
        batch-reject, single reject, replace-keywords, accept-subject,
        mark-reviewed. Without the lock, a double-clicked Accept, or an Accept
        fired while a batch-reject for the same row is in flight, can both
        pass their status precondition against the same pre-write state and
        both commit. The reject wins the write; the accept's keyword stays on
        the photo; keyword state and review state then contradict each other
        and undo restores a state that never existed. Holding SQLite's writer
        lock across the read *and* the write makes the check-then-write
        indivisible, exactly as the batch endpoints already do.
        """
        db = _get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            current_status = _prediction_status(db, pred_id)
            # Missing row falls through to ``accept_prediction`` returning None
            # — the endpoint's historical contract for unknown ids is a 200
            # no-op, not a 404.
            if current_status in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f"prediction already {current_status}; cannot accept",
                    409,
                )
            result = db.accept_prediction(pred_id, _commit=False)
            if result and result["affected"]:
                # ``changed_tag=False`` means the photo already carried an
                # equivalent species (hierarchical or root) so the accept only
                # flipped ``prediction_review.status``. Encode that as a JSON
                # ``no_tag`` payload so undo/redo can reverse the status change
                # without untagging (or re-tagging) a keyword the user
                # deliberately kept. Regular accepts still use the compact
                # ``str(prediction_id)`` form so existing edit-history rows
                # keep parsing unchanged.
                def _make_item(a):
                    if a.get('changed_tag', True):
                        old_value = str(a['prediction_id'])
                    else:
                        old_value = json.dumps({
                            'prediction_id': a['prediction_id'],
                            'no_tag': True,
                        })
                    return {
                        'photo_id': a['photo_id'],
                        'old_value': old_value,
                        'new_value': str(result['keyword_id']),
                    }
                items = [_make_item(a) for a in result['affected']]
                is_batch = len(result['affected']) > 1
                desc = f'Accepted prediction: added "{result["species"]}"'
                if is_batch:
                    desc += f' to {len(result["affected"])} photos'
                db.record_edit(
                    'prediction_accept', desc, str(result['keyword_id']),
                    items, is_batch=is_batch, _commit=False,
                )
            db.conn.commit()
            # Name every row this call decided, not just the one in the URL.
            # A grouped accept expands through the burst, so a caller looping
            # over a selection can have its next row already decided *by this
            # response* — and would otherwise meet the 409 above and report a
            # decision that did land as "not applied". Reporting the ids is
            # the only version of that answer the caller can trust: it is what
            # this transaction wrote, not an inference from a later status
            # read that cannot say who wrote it. Matches the shape
            # ``accept-subject`` already returns.
            return jsonify({
                "ok": True,
                "prediction_ids": (
                    result["accepted_prediction_ids"] if result else []
                ),
                "photo_ids": result["photo_ids"] if result else [],
            })
        except Exception:
            db.conn.rollback()
            raise

    @app.route("/api/predictions/<int:pred_id>/accept-subject", methods=["POST"])
    def api_accept_subject_species(pred_id):
        """Accept an additional-subject species from Compare, atomically.

        Under the same lock as every other prediction-decision route (see
        ``api_accept_prediction``). The precondition check is on the entry
        row's status: if the user marked it reviewed, or a race with a batch
        endpoint already accepted/rejected it, this endpoint must refuse
        rather than overwrite that decision.
        """
        db = _get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            current_status = _prediction_status(db, pred_id)
            if current_status is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if current_status in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f"prediction already {current_status}; cannot accept",
                    409,
                )
            result = db.accept_subject_species(pred_id, _commit=False)
            if result is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if result["affected"]:
                # Accept-subject can accept agreeing predictions from
                # multiple classifier models on one detection. Undo/redo
                # must reset every sibling scope, so always encode the full
                # ``prediction_ids`` list as JSON — the compact
                # comma-separated fallback cannot be parsed by
                # ``_edit_prediction_ids`` and would drop every sibling
                # status flip. ``no_tag`` is set only when every underlying
                # accept was a no-op (photo already carried the target via
                # an equivalent hierarchical or root row); in mixed batches
                # one accept actually tagged the species, so undo must
                # untag exactly once.
                _all_no_tag = all(
                    not a.get("changed_tag", True) for a in result["affected"]
                )
                payload = {
                    "prediction_ids": [
                        int(pid) for pid in result["prediction_ids"]
                    ],
                }
                if _all_no_tag:
                    payload["no_tag"] = True
                old_value = json.dumps(payload)
                db.record_edit(
                    "prediction_accept",
                    f'Accepted additional subject species: added "{result["species"]}"',
                    str(result["keyword_id"]),
                    [{
                        "photo_id": result["photo_id"],
                        "old_value": old_value,
                        "new_value": str(result["keyword_id"]),
                    }],
                    _commit=False,
                )
            db.conn.commit()
            return jsonify({
                "ok": True,
                "species": result["species"],
                "prediction_ids": result["prediction_ids"],
                "photo_ids": [result["photo_id"]],
            })
        except Exception:
            db.conn.rollback()
            raise

    @app.route("/api/predictions/<int:pred_id>/reject", methods=["POST"])
    def api_reject_prediction(pred_id):
        """Reject a single prediction as one atomic decision.

        Serialized with every other prediction-decision route through
        ``prediction_decisions.begin_prediction_decision`` — see
        ``api_accept_prediction`` for the full argument. Codex's fresh
        evidence beyond the batch-atomicity fix: a Browse batch accept
        overlapping a Review-side single reject on the same row would let the
        reject read while the batch held the writer lock and then overwrite
        the newly accepted status *after* the batch committed, leaving the
        species keyword attached to a row now marked rejected. Same failure
        mode as the inverse (single accept vs batch reject). The single-row
        routes now take the same lock and honour the same terminal-status
        precondition as their batch siblings.
        """
        db = _get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            # Review state lives in prediction_review now; predictions.model
            # is renamed to classifier_model. Sibling-alternative rejection
            # goes through the workspace-scoped review table.
            ws = db._ws_id()
            pred = db.conn.execute(
                """SELECT pr.id, pr.species, pr.detection_id,
                          pr.classifier_model AS model,
                          pr.labels_fingerprint, d.photo_id,
                          COALESCE(pr_rev.status, 'pending') AS status
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.id = ?""",
                (ws, pred_id),
            ).fetchone()
            # prediction_review has an FK on prediction_id; writing review
            # state for a missing pred would raise an IntegrityError and
            # return 500 where the legacy endpoint returned a harmless
            # no-op. Gate the write on existence so stale IDs stay a clean
            # 404.
            if pred is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if pred["status"] in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f'prediction already {pred["status"]}; cannot reject',
                    409,
                )
            db.update_prediction_status(pred_id, "rejected", _commit=False)
            # Also reject sibling alternative predictions for the same
            # (detection, classifier_model, labels_fingerprint) in this
            # workspace. Fingerprint scoping matters: without it, rejecting a
            # prediction from a new label set would rewrite review state for
            # prior fingerprints' alternatives on the same detection.
            sibling_ids = [row["id"] for row in db.conn.execute(
                """SELECT pr.id
                   FROM predictions pr
                   JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ?
                     AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ?
                     AND pr.id != ?
                     AND pr_rev.status = 'alternative'""",
                (ws, pred["detection_id"], pred["model"],
                 pred["labels_fingerprint"], pred_id),
            ).fetchall()]
            for sid in sibling_ids:
                db.update_prediction_status(sid, "rejected", _commit=False)
            db.record_edit(
                'prediction_reject',
                f'Rejected prediction "{pred["species"]}"',
                'rejected',
                [{
                    'photo_id': pred['photo_id'],
                    'old_value': 'pending',
                    'new_value': 'rejected',
                }],
                _commit=False,
            )
            db.conn.commit()
            return jsonify({"ok": True})
        except Exception:
            db.conn.rollback()
            raise

    @app.route("/api/predictions/group/<group_id>")
    def api_prediction_group(group_id):
        """Get all predictions and photo data for a burst group."""
        db = _get_db()
        preds = db.get_group_predictions(group_id)
        rows = [dict(p) for p in preds]
        attach_nested_edit_recipes(db, rows)
        return jsonify(rows)

    @app.route("/api/predictions/group/apply", methods=["POST"])
    def api_prediction_group_apply():
        """Apply pick/reject decisions and species to a burst group."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        picks = body.get("picks", [])  # list of photo_ids
        rejects = body.get("rejects", [])  # list of photo_ids
        removed = body.get("removed", [])  # list of prediction_ids to ungroup
        species = body.get("species", "")
        observed, observed_err = _parse_observed_statuses(body)
        if observed_err is not None:
            return observed_err

        # Pre-validate all photo IDs against workspace before any mutations
        for pid in picks + rejects:
            if not db._photo_in_workspace(pid):
                return json_error(f"Photo {pid} is not in the active workspace", 403)

        # Everything below — the stale filter, the keyword/flag writes, and
        # the prediction status writes — runs under the single decision lock
        # in one transaction. An earlier version filtered stale photos and
        # committed the flag/keyword writes *before* taking the lock and then
        # re-checked staleness inside it; a decision that landed in that
        # window would leave the photo flagged and keyworded while the
        # in-lock recheck refused to update its prediction status, so the
        # keyword-on-a-rejected-row split this precondition exists to
        # prevent came back as a narrower race. The fix is to hold the write
        # lock across the read that governs the writes.
        def _apply_group_decisions():
            # Stale filter under the lock: a decision that committed since
            # the modal rendered wins over this apply, so those photos are
            # dropped before any of *this* apply's writes land. Doing the
            # check inside ``BEGIN IMMEDIATE`` closes the check-then-write
            # window — nothing can decide between this read and the writes
            # below, so a photo we skip here cannot silently be flagged or
            # keyworded on the other side of the transaction.
            stale_photos = _stale_group_apply_photos(db, observed)
            actionable_picks = [pid for pid in picks if pid not in stale_photos]
            actionable_rejects = [
                pid for pid in rejects if pid not in stale_photos
            ]

            # Capture old flag values before mutation
            all_flag_pids = actionable_picks + actionable_rejects
            old_flags = {}
            for pid in all_flag_pids:
                old = db.get_photo(pid)
                if old:
                    old_flags[pid] = old["flag"] or "none"

            local_species = species
            # Flag picks and add species keyword — every write uses
            # ``_commit=False`` because the enclosing ``BEGIN IMMEDIATE`` owns
            # the transaction and an intermediate commit would release the
            # writer lock mid-decision.
            try:
                if local_species:
                    kid = db.add_keyword(
                        local_species, is_species=True, _commit=False,
                    )
                    # Queue/record the stored spelling (see api_add_keyword).
                    stored = db.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (kid,)
                    ).fetchone()
                    if stored and stored["name"]:
                        local_species = stored["name"]
                    already_has_species = db.get_photos_with_equivalent_species(
                        actionable_picks, kid
                    )
                    added_picks = []
                    for pid in actionable_picks:
                        db.update_photo_flag(pid, "flagged", _commit=False)
                        if pid in already_has_species:
                            continue
                        db.tag_photo(pid, kid, source="manual", _commit=False)
                        db.queue_change(
                            pid, "keyword_add", local_species, _commit=False,
                        )
                        added_picks.append(pid)

                    # Record keyword_add history for picks
                    kw_items = [
                        {'photo_id': pid, 'old_value': '', 'new_value': str(kid)}
                        for pid in added_picks
                    ]
                    if kw_items:
                        db.record_edit(
                            'keyword_add',
                            f'Added "{local_species}" to {len(added_picks)} photos (group prediction)',
                            str(kid), kw_items,
                            is_batch=len(added_picks) > 1,
                            _commit=False,
                        )
                else:
                    # No species — still flag picks
                    for pid in actionable_picks:
                        db.update_photo_flag(pid, "flagged", _commit=False)

                # Reject rejects
                for pid in actionable_rejects:
                    db.update_photo_flag(pid, "rejected", _commit=False)
            except ValueError as e:
                # ``prediction_decisions.under_prediction_decision_lock``'s
                # finally will roll back the still-open transaction; returning
                # here just short-circuits the rest of the writes.
                return json_error(str(e), 403)

            # Record flag history for all picks + rejects
            flag_items = []
            for pid in actionable_picks:
                if pid in old_flags:
                    flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'flagged'})
            for pid in actionable_rejects:
                if pid in old_flags:
                    flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'rejected'})
            if flag_items:
                for item in flag_items:
                    db.queue_flag_change_if_enabled(
                        item["photo_id"], item["new_value"], _commit=False
                    )
                desc = (
                    f'Group prediction: flagged {len(actionable_picks)}, '
                    f'rejected {len(actionable_rejects)}'
                )
                db.record_edit(
                    'flag', desc, 'group_apply', flag_items,
                    is_batch=True, _commit=False,
                )

            # Mark all predictions in this group as accepted/rejected
            for pid in actionable_picks:
                db.update_predictions_status_by_photo(
                    pid, 'accepted', _commit=False,
                )
            for pid in actionable_rejects:
                db.update_predictions_status_by_photo(
                    pid, 'rejected', _commit=False,
                )

            # Remove predictions from group. Ungrouping is not a decision — it
            # changes which rows the burst modal shows together, not what any
            # of them means — so it is not gated on the baseline.
            for pred_id in removed:
                db.ungroup_prediction(pred_id, _commit=False)
            db.conn.commit()
            # ``record_edit`` skips its prune under ``_commit=False``; run it
            # once the decision is durable so history stays bounded — same
            # shape the batch endpoints use.
            db._prune_edit_history()
            # Counted in photos, which is the unit the modal works in and the
            # unit the toast names. Reported even when zero so the client can
            # tell "nothing was stale" from an older server that never checked.
            return jsonify({"ok": True, "already_decided": len(stale_photos)})

        return prediction_decisions.under_prediction_decision_lock(
            db, _apply_group_decisions, json_error=json_error,
        )

    # -- Detection API routes --



    def _read_raw_config_file():
        """Return the parsed contents of ~/.vireo/config.json, or {}.

        Unlike cfg.load(), this does NOT merge DEFAULTS — so it contains
        only the keys the user has actually set. Used by write paths so the
        on-disk file stays minimal.

        Preserves a `.corrupt` backup on unreadable/non-dict content before
        returning `{}` — otherwise the very next PATCH/DELETE via the
        schema-driven settings routes would call `cfg.save()` on the empty
        dict and silently overwrite whatever the user had.
        """
        import config as cfg

        if not os.path.exists(cfg.CONFIG_PATH):
            return {}
        try:
            with open(cfg.CONFIG_PATH) as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg._preserve_corrupt_config()
            return {}
        if not isinstance(raw, dict):
            cfg._preserve_corrupt_config()
            return {}
        return raw

    # Serializes read-modify-write of ~/.vireo/config.json and the active
    # workspace's config_overrides across the schema-driven settings
    # endpoints (PATCH/DELETE/import). Without this, with per-field autosave
    # and `app.run(threaded=True)` two concurrent requests can read the same
    # snapshot and the later writer drops the earlier change.
    _settings_write_lock = threading.Lock()
    # Shared by the iNaturalist and settings blueprints so a settings write
    # that changes ``inat_token`` supersedes an in-flight modal validation.
    # Only touched while holding ``_settings_write_lock``.
    _inat_token_generation = InatTokenGeneration()




    # -- Scan status (kept, non-job) --

    # -- Model & Taxonomy API routes --


    # -- Job API routes --

    def _build_scan_work(
        roots, incremental, active_ws, repair_missing_metadata=False,
    ):
        """Build the background work function for a scan job.

        Shared by ``POST /api/jobs/scan`` and
        ``POST /api/folders/<id>/rescan`` so per-folder rescans reuse the
        same scan + thumbnail pipeline as a full scan.

        ``roots`` may be a single path string (back-compat, one root) or a
        list of paths. When multiple roots are given they are scanned
        **serially** inside this single job -- that's the whole point of
        this wrapper: parallel scan jobs used to fight for the SQLite
        writer lock, so we now process roots one after another. A failure
        on one root does not abort the others; the error is recorded and
        the job ends in ``"failed"`` (mixed-outcome rollup convention).
        """
        import config as cfg

        runner = app._job_runner

        # Back-compat: accept a bare string in addition to a list.
        if isinstance(roots, str):
            roots_list = [roots]
        else:
            roots_list = list(roots)

        def work(job):
            from scanner import ScanCancelled
            from scanner import scan as do_scan

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            # Check folder health before scanning to prevent duplicate imports
            if thread_db.check_folder_health():
                _invalidate_missing_originals_cache()

            # Accumulator so multi-root progress doesn't rewind at each
            # root boundary. scanner.scan() reports (current, total) local
            # to its invocation; we fold those into cumulative counters
            # that the SSE/status stream reads.
            # Track both the last reported *processed* count and the
            # last reported *total* for the current root. On root
            # boundary we advance the cumulative baseline by the
            # processed count (not the planned total) so a root that
            # fails mid-scan doesn't inflate the baseline with phantom
            # files the next root would start above.
            scan_acc = {"prior": 0, "last_current": 0, "last_total": 0}
            # Photos that actually reached the catalog, summed across roots
            # from each scan()'s counts sink. Kept separate from the
            # progress accumulator above: progress counts every file the
            # scan disposes of (including ones skipped because they
            # vanished under a dropped mount), which is what a progress bar
            # needs but overstates the result line.
            indexed_acc = {"n": 0}

            def progress_cb(current, total):
                scan_acc["last_current"] = current
                scan_acc["last_total"] = total
                cum_current = scan_acc["prior"] + current
                cum_total = scan_acc["prior"] + total
                job["progress"]["current"] = cum_current
                job["progress"]["total"] = cum_total
                runner.update_step(
                    job["id"], "scan",
                    progress={"current": cum_current, "total": cum_total},
                )
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": cum_current,
                        "total": cum_total,
                        "current_file": job["progress"].get("current_file", ""),
                        "rate": round(
                            cum_current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Scanning photos",
                    },
                )

            def advance_scan_acc():
                # Use processed count, not planned total — a root that
                # raised mid-scan will have last_current < last_total,
                # and starting the next root above the actual processed
                # count would overreport photos indexed.
                scan_acc["prior"] += scan_acc["last_current"]
                scan_acc["last_current"] = 0
                scan_acc["last_total"] = 0

            job["_start_time"] = time.time()
            runner.set_steps(job["id"], [
                {"id": "scan", "label": "Scan photos"},
                {"id": "thumbnails", "label": "Generate thumbnails"},
            ])
            runner.update_step(job["id"], "scan", status="running")
            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            def status_cb(message, phase_current=None, phase_total=None, phase_label=None):
                progress_payload = {
                    "phase": phase_label or message,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": message,
                    "rate": 0,
                    "phase_current": phase_current,
                    "phase_total": phase_total,
                    "phase_label": phase_label,
                }
                runner.update_step(job["id"], "scan", current_file=message)
                runner.push_event(job["id"], "progress", progress_payload)

            def cancel_check():
                return runner.is_cancelled(job["id"])

            def pause_check():
                return runner.pause_requested(job["id"])

            def cancel_only_check():
                return runner.cancellation_requested(job["id"])

            vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])

            # Per-root failures are caught and recorded rather than
            # re-raised so a failure on root A doesn't prevent root B
            # from scanning. Any failure flips the job to "failed" at
            # the end (mixed-outcome rollup).
            #
            # Track roots by failure class so the rollup below can
            # distinguish "this root's scan raised" (no photos indexed
            # — thumbnails can skip) from "this root's scan succeeded
            # but cache invalidation raised" (photos DID get indexed —
            # thumbnails must still run). Using len(root_errors) alone
            # double-counts roots that hit both failure classes and
            # misclassifies cache-only failures as scan failures.
            root_errors = []
            scan_failed_roots = set()
            cache_failed_roots = set()
            cancelled = False
            for idx, root in enumerate(roots_list, 1):
                if cancel_check():
                    cancelled = True
                    break
                phase = (
                    f"Scanning root {idx} of {len(roots_list)}: {root}"
                    if len(roots_list) > 1
                    else "Scanning photos"
                )
                runner.push_event(job["id"], "progress", {
                    "phase": phase,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": phase,
                    "rate": 0,
                })
                # Counts sink rather than the return value: scan() commits
                # rows as it goes and can raise (or be cancelled) after
                # thousands have landed. Reading the sink in `finally`
                # credits that work on every exit path — a root that dies
                # late would otherwise report zero photos indexed.
                root_counts = {}
                try:
                    do_scan(
                        root, thread_db,
                        progress_callback=progress_cb,
                        incremental=incremental,
                        extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                        status_callback=status_cb,
                        vireo_dir=vireo_dir,
                        thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                        cancel_check=cancel_check,
                        pause_check=pause_check,
                        cancel_only_check=cancel_only_check,
                        repair_missing_metadata=repair_missing_metadata,
                        counts=root_counts,
                    )
                except Exception as exc:
                    if isinstance(exc, ScanCancelled) and cancel_check():
                        log.info("Scan job %s cancelled during root %s", job["id"], root)
                        cancelled = True
                        break
                    log.exception("Scan failed for root %s", root)
                    scan_failed_roots.add(root)
                    msg = f"[{root}] {exc}"
                    root_errors.append(msg)
                    if msg not in job["errors"]:
                        job["errors"].append(msg)
                finally:
                    # Credit whatever this root indexed regardless of how
                    # it exited — completed, raised, or cancelled. The
                    # rows are committed either way, so the summary must
                    # count them either way.
                    indexed_acc["n"] += root_counts.get("indexed", 0)
                    # scanner.scan commits photo rows incrementally, so
                    # even a mid-scan failure can leave DB state that
                    # invalidates cached new-image counts. A failure
                    # here must surface: the shared cache has a 5-min
                    # TTL, so users would see stale "new images" counts
                    # with no job-level failure signal if we swallowed
                    # these errors. Keep the try/except so we still
                    # advance scan_acc and try the remaining roots,
                    # but record the failure into root_errors so the
                    # job is flagged failed at the rollup below.
                    try:
                        _invalidate_new_images_after_scan(thread_db, root)
                    except Exception as cache_exc:
                        log.exception(
                            "Failed to invalidate new-image cache for %s", root,
                        )
                        cache_failed_roots.add(root)
                        cache_msg = (
                            f"[{root}] cache invalidation failed "
                            f"after scan: {cache_exc}"
                        )
                        root_errors.append(cache_msg)
                        if cache_msg not in job["errors"]:
                            job["errors"].append(cache_msg)
                    # scanner.scan touches disk and may add or remove
                    # photo rows; a ready Missing Originals payload
                    # computed before the scan can now be stale (e.g.
                    # user restored an original before running "Rescan
                    # this Folder"). The pre-scan health-check
                    # invalidation only fires when a folder flips
                    # missing/ok, so also drop the cache once the scan
                    # itself has run — even on partial failure, since
                    # rows are committed incrementally.
                    try:
                        _invalidate_missing_originals_cache()
                    except Exception:
                        log.exception(
                            "Failed to invalidate missing-originals cache for %s",
                            root,
                        )
                    advance_scan_acc()

            if cancelled or cancel_check():
                # Same indexed count as the completed path, not the
                # progress counter. Otherwise "N photos" would mean two
                # different things depending on which branch produced it,
                # and a cancelled scan of a dropped share would report a
                # full house of photos it never cataloged.
                photo_count = indexed_acc["n"]
                runner.update_step(
                    job["id"], "scan", status="cancelled",
                    summary=f"{photo_count} photos (cancelled)",
                )
                runner.update_step(
                    job["id"], "thumbnails", status="skipped",
                    summary="skipped (cancelled)",
                )
                return {"photos_indexed": photo_count, "cancelled": True}

            # Count what the scans reported as indexed, not the progress
            # counter. On a clean run they agree; they diverge exactly when
            # the run went wrong — progress advances for files that were
            # discovered and then skipped (vanished under a mount that
            # dropped mid-scan), so "N photos" would read as a success
            # line for a scan that cataloged nothing.
            photo_count = indexed_acc["n"]
            # Unique roots that hit any failure class. Counting unique
            # roots (not error entries) avoids inflating the "N of M"
            # summary when a single root raises in both scan and cache
            # invalidation.
            failed_root_count = len(scan_failed_roots | cache_failed_roots)
            metadata_warning = _scan_metadata_warning()
            if root_errors:
                scan_summary = (
                    f"{photo_count} photos ({failed_root_count} of "
                    f"{len(roots_list)} root"
                    f"{'s' if len(roots_list) != 1 else ''} failed)"
                )
                if metadata_warning:
                    scan_summary += f" — {metadata_warning}"
                runner.update_step(
                    job["id"], "scan", status="failed", summary=scan_summary,
                    error=root_errors[0], error_count=len(root_errors),
                )
            else:
                scan_summary = f"{photo_count} photos"
                if metadata_warning:
                    scan_summary += f" — {metadata_warning}"
                runner.update_step(
                    job["id"], "scan", status="completed",
                    summary=scan_summary,
                )
            # Skip the thumbnail phase when EVERY requested root's scan
            # raised. generate_all() walks the whole library looking
            # for missing thumbnails — running it after a total scan
            # failure does a long, unrelated pass and delays the
            # failure feedback the user actually needs. When at least
            # one root's scan succeeded we still run thumbs so those
            # newly-indexed photos get covered. Cache-invalidation
            # failures do NOT gate this decision: the scan for that
            # root did produce indexed photos that need thumbnails.
            all_roots_failed = (
                bool(roots_list) and len(scan_failed_roots) == len(roots_list)
            )

            if all_roots_failed:
                log.info(
                    "All %d scan root(s) failed; skipping thumbnail phase",
                    len(roots_list),
                )
                runner.update_step(
                    job["id"], "thumbnails", status="skipped",
                    summary="skipped (all scan roots failed)",
                )
                thumb_result = None
            else:
                runner.update_step(job["id"], "thumbnails", status="running")

                # Auto-generate thumbnails for new photos only
                from thumbnails import generate_all

                log.info("Generating thumbnails...")
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": "Checking for new thumbnails...",
                        "rate": 0,
                        "phase": "Generating thumbnails",
                    },
                )

                def thumb_cb(current, total):
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    runner.push_event(
                        job["id"],
                        "progress",
                        {
                            "current": current,
                            "total": total,
                            "current_file": "",
                            "rate": round(
                                current / max(time.time() - job["_start_time"], 0.01), 1
                            ),
                            "phase": "Generating thumbnails",
                        },
                    )

                thumb_result = generate_all(
                    thread_db, app.config["THUMB_CACHE_DIR"], progress_callback=thumb_cb,
                    vireo_dir=vireo_dir,
                )
                from thumbnails import format_summary as thumb_summary
                runner.update_step(job["id"], "thumbnails", status="completed",
                                   summary=thumb_summary(thumb_result))

            # Mixed-outcome rollup: any failed root => job is "failed".
            # JobRunner._run_job dedupes job["errors"] by exact string
            # match. Raise the first per-root message (already recorded
            # above) so no extra aggregate entry is appended — that
            # would inflate error_count in job/history output. The
            # "N of M roots failed" context is already visible via the
            # scan step's summary and error_count set above.
            if root_errors:
                raise RuntimeError(root_errors[0])

            return {"photos_indexed": photo_count, "thumbnails": thumb_result}

        return work


    def _metadata_repair_count(db, workspace_id, root_paths=None):
        # Don't filter by ``folders.status``: that column is only refreshed
        # by ``check_folder_health`` (10-minute loop or the manual
        # "check missing folders" flow), so a workspace whose drive was
        # unplugged and is now reconnected still reads as ``status='missing'``
        # until then. The readiness endpoint fires as soon as the Import
        # page opens; if we filtered by ``status``, users would see 0
        # repairable photos and the repair route would 409 with "no
        # photos need metadata repair" even though ``os.path.isdir`` would
        # let the repair job scan them.
        #
        # ``root_paths`` scopes the count to real-time reachable roots
        # (based on ``os.path.isdir``). When a workspace mixes an offline
        # drive with photos missing EXIF and a separate reachable drive
        # with no repairable rows, an unscoped count combined with a
        # non-empty ``reachable_roots`` list would enable the Repair
        # button and start a job that finishes without ever touching the
        # offline photos — Codex's "repeating repair job" pathology. The
        # scoped count reflects only what the repair pass would actually
        # process. ``None`` preserves the unscoped legacy shape for any
        # future caller that wants a workspace-wide figure.
        params = [workspace_id]
        where_extra = ""
        if root_paths is not None:
            if not root_paths:
                return 0
            normalized_roots = [
                r.replace("\\", "/").rstrip("/") for r in root_paths if r
            ]
            if not normalized_roots:
                return 0
            clauses = []
            for norm in normalized_roots:
                prefix = norm + "/"
                # Match ``f.path`` normalized to forward slashes either
                # exactly against the root or as a boundary-preserving
                # prefix. ``substr(...)=prefix`` avoids the wildcard
                # collision LIKE would introduce (e.g. a folder called
                # ``photos_backup`` incorrectly matching a reachable
                # ``photos`` root because ``_`` matches any character
                # in LIKE without ESCAPE).
                clauses.append(
                    "(REPLACE(f.path, '\\', '/') = ? "
                    "OR substr(REPLACE(f.path, '\\', '/'), 1, ?) = ?)"
                )
                params.extend([norm, len(prefix), prefix])
            where_extra = " AND (" + " OR ".join(clauses) + ")"
        rows = db.conn.execute(
            "SELECT DISTINCT p.id, p.filename, "
            "f.id AS folder_id, f.path AS folder_path "
            "FROM photos p "
            "JOIN folders f ON f.id = p.folder_id "
            "JOIN workspace_folders wf ON wf.folder_id = f.id "
            "WHERE wf.workspace_id = ? "
            "AND p.exif_data IS NULL"
            + where_extra
            + " ORDER BY f.path, p.filename",
            params,
        ).fetchall()

        # A database row is only repairable when its original still exists.
        # The incremental repair scan discovers files from disk, so counting
        # a deleted/moved original here would leave the Repair button enabled
        # forever for a job that can never visit that row. Enumerate each
        # candidate folder once instead of statting every photo individually;
        # this keeps readiness responsive for large degraded imports.
        from image_loader import is_excluded_scan_path

        folder_files = {}
        repairable = 0
        for row in rows:
            folder_id = row["folder_id"]
            folder_path = row["folder_path"]
            if folder_id not in folder_files:
                if is_excluded_scan_path(folder_path):
                    folder_files[folder_id] = None
                else:
                    try:
                        with os.scandir(folder_path) as entries:
                            folder_files[folder_id] = {
                                entry.name for entry in entries if entry.is_file()
                            }
                    except OSError:
                        # The folder disappeared or became unreadable after
                        # root reachability was checked. Treat its rows as
                        # unavailable rather than offering a no-op repair.
                        folder_files[folder_id] = None

            names = folder_files[folder_id]
            if names is None:
                continue
            filename = row["filename"]
            if filename in names:
                repairable += 1
                continue
            # Preserve the filesystem's own case and Unicode matching rules
            # for a catalog name that did not compare byte-for-byte with the
            # directory entry (notably default APFS and NTFS volumes).
            if os.path.isfile(os.path.join(folder_path, filename)):
                repairable += 1
        return repairable


    # -- Export presets --

    def _move_folder_guard_error(guard_db, folder_id):
        """Return the error message blocking a folder move, or None.

        Context-free on purpose: takes the db explicitly and touches no
        Flask request/app context, so the chained completion hook can run
        it from a job thread with a thread db.
        """
        # A folder covered by any workspace's local_workspace_folders row has
        # its folders.path rebased into that workspace's managed copy; a
        # concurrent workspace-membership guard would refuse to touch the
        # folders row for exactly this reason. Moving it here (via
        # db.move_folder_path) would move or delete the managed copy and
        # rewrite the catalog while local_workspace_folders and the manifest
        # still expect the pre-move layout, so the owning workspace's next
        # status falls into missing-local recovery and sync/discard can no
        # longer restore. Reject the job before enqueue.
        with stage_boundary_lock():
            local_root_id = local_root_for_folder(guard_db, folder_id)
            if local_root_id is not None:
                return (
                    "Cannot move this folder while it has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first."
                )
            # A descendant local copy has already had its folders.path rebased
            # under local-folders/, so a folders.path subtree walk from this
            # ancestor no longer sees it — but local_folder_mappings.source_path
            # still records the original location. Without this check the move
            # job would move/delete the original source directory out from
            # under the manifest, leaving sync/discard unable to restore.
            descendant_root_id = local_root_under_folder(guard_db, folder_id)
            if descendant_root_id is not None:
                return (
                    "Cannot move this folder while a subfolder has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first."
                )
            staged_here, staged_owner_ws = folder_has_local_workspace(
                guard_db, folder_id,
            )
            if staged_here:
                return (
                    f"Cannot move this folder — workspace {staged_owner_ws} has it "
                    "staged locally. Switch to that workspace and sync or discard the "
                    "local copy first."
                )
            # ``folder_has_local_workspace`` only sees a completed stage
            # claim. A stage/sync/discard queued or running against a
            # workspace that contains this folder hasn't yet written
            # local_workspace_folders, so the read-only guard above passes
            # even though the transition worker is about to rebase this
            # folder's paths. Enqueueing the move now would rewrite the
            # ``folders`` row out from under the pending transition —
            # missing-local recovery on the next status. Refuse until the
            # transition finishes.
            row = guard_db.conn.execute(
                "SELECT workspace_id FROM workspace_folders WHERE folder_id = ?",
                (folder_id,),
            ).fetchall()
            for ws_row in row:
                pending = _pending_local_workspace_transition(
                    int(ws_row["workspace_id"]), db=guard_db)
                if pending:
                    return (
                        f"Wait for the {pending['type']} job on workspace "
                        f"{int(ws_row['workspace_id'])} to finish before moving this "
                        "folder; otherwise the move would run on paths that workspace "
                        "is about to claim."
                    )
        return None

    def _start_move_folder_job(runner, workspace_id, *, folder_id,
                               destination, display_dest, destination_name,
                               source_path, resolved_destination,
                               merge, remote, developed_dir, folder_template="",
                               date_destinations=None,
                               chained_from=None, serialize_lock=None,
                               allow_tracked_merge=False,
                               managed_staging_root=None, mount_baseline=None,
                               mount_identities=None):
        """Enqueue a move-folder job and return its job id.

        Shared by the move-folder endpoint and the chained
        process-completion hook (which runs on a job thread, so this
        must not touch Flask request/app context).

        ``serialize_lock``: optional ``threading.Lock`` shared by a batch
        of chained moves; when given, the transfer itself runs under the
        lock so batch-mates execute one at a time (see the why-comment at
        the acquire site). The job still enqueues — and its id exists —
        immediately.

        ``allow_tracked_merge``: passed through to ``move_folder``. The
        chained import→process→move hook opts in (see the why-comment in
        ``_enqueue_move_folder_job``); the manual move endpoint keeps the
        default refusal of tracked destinations.

        ``date_destinations``: the planned capture-date folders for a
        date-organized move (``path``/``relative_path``/``photo_count`` per
        entry, as produced by ``plan_folder_date_moves``). Snapshotted into
        the job config so the jobs panel can name the folders photos actually
        land in rather than only the selected root.
        """
        def work(job):
            from move import move_folder, move_folder_by_date

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            job["_start_time"] = time.time()

            last_phase = {"value": None}

            def progress_cb(current, total, filename, phase="Moving folder"):
                # Only update keys JobRunner pre-seeds in job["progress"]
                # (current/total/current_file). Do NOT insert "phase" here:
                # this runs on the worker thread outside the runner lock, and
                # adding a new key races _snapshot_job's locked dict() copy
                # ("dictionary changed size during iteration"). push_event
                # below mirrors phase onto job["progress"] under the lock.
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                # The copy phase fires once per file; on a large folder that
                # would flood the SSE stream and tie up Flask threads. Throttle
                # to every 10th file, but always emit on a phase change and on
                # the first/last file so the panel never looks stalled.
                phase_changed = phase != last_phase["value"]
                last_phase["value"] = phase
                if not phase_changed and current % 10 != 0 \
                        and current not in (1, total):
                    return
                runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "phase": phase,
                })

            # The chained moves from one import all rsync to the same NAS,
            # and runner.start gives each its own thread immediately — so
            # N folders would mean N concurrent rsyncs, each honoring
            # --bwlimit individually and together consuming N× the
            # configured bandwidth budget. The chain hands every job in
            # the batch one shared lock so the transfers run one at a
            # time while all N jobs (and their ids) still enqueue up
            # front.
            if serialize_lock is not None and not serialize_lock.acquire(blocking=False):
                # UI transparency: a job blocked on the batch lock
                # must say why it isn't moving anything yet.
                runner.push_event(job["id"], "progress", {
                    "current": 0,
                    "total": 0,
                    "current_file": "",
                    "phase": (
                        "Waiting for an earlier chained move to finish"
                    ),
                })
                # Poll instead of blocking outright: this wait is the
                # one boundary where Cancel can be honored without
                # touching mid-transfer semantics — a blocking acquire
                # would run the whole transfer anyway after the user
                # pressed Stop.
                while not serialize_lock.acquire(timeout=0.5):
                    if runner.is_cancelled(job["id"]):
                        # Returning with the lock NOT held, before the
                        # try below — so the release in its finally
                        # never fires on this path.
                        return {
                            "ok": False, "moved": 0, "errors": [],
                            "summary": (
                                "Cancelled before transfer started"
                            ),
                        }
            # Cancel check with the lock held, covering two paths:
            # (1) the waiter's ``acquire(timeout=0.5)`` returned True in
            # the same 0.5s window a cancel landed in, exiting the loop
            # without the in-loop check running; (2) a chained move whose
            # thread started AFTER the earlier holder already released
            # the batch lock — its non-blocking acquire succeeds so it
            # never enters the wait loop, but its ``/cancel`` may have
            # already been accepted while the thread was still queued.
            # Either way, don't start the transfer.
            if serialize_lock is not None and runner.is_cancelled(job["id"]):
                serialize_lock.release()
                return {
                    "ok": False, "moved": 0, "errors": [],
                    "summary": (
                        "Cancelled before transfer started"
                    ),
                }
            try:
                check_mount = None
                if managed_staging_root and not runner.begin_uncancellable(job["id"]):
                    return {"ok": False, "moved": 0, "errors": [], "summary": "Cancelled before transfer started"}
                if mount_baseline is not None:
                    from import_staging import check_staged_mount

                    def check_mount():
                        check_staged_mount(resolved_destination, mount_baseline, mount_identities)
                    check_mount()
                if folder_template:
                    result = move_folder_by_date(
                        db=thread_db,
                        folder_id=folder_id,
                        destination=destination,
                        folder_template=folder_template,
                        progress_cb=progress_cb,
                        developed_dir=developed_dir,
                    )
                else:
                    result = move_folder(
                        db=thread_db,
                        folder_id=folder_id,
                        destination=destination,
                        progress_cb=progress_cb,
                        developed_dir=developed_dir,
                        merge=merge,
                        remote=remote,
                        destination_name=destination_name,
                        allow_tracked_merge=allow_tracked_merge,
                        thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                        **({"verify_contents": True} if managed_staging_root and not remote else {}),
                        **({"pre_commit_check": check_mount} if check_mount else {}),
                    )
            finally:
                if serialize_lock is not None:
                    serialize_lock.release()

            # Tell the JobRunner whether the move actually succeeded. Without
            # this the runner marks any normal return "completed" — so a move
            # that copied nothing because rsync timed out used to read as
            # "completed, 0 errors" in the history. A `needs_merge` return is
            # NOT a failure: it's a soft signal that the destination already
            # exists and the UI should re-prompt for a merge/resume, so leave
            # it for the caller without flagging the job failed.
            if not result.get("needs_merge"):
                errors = result.get("errors") or []
                moved = result.get("moved", 0)
                if errors and moved == 0:
                    result["ok"] = False
                    result["summary"] = f"Move failed — {errors[0]}"
                else:
                    cleanup_error = result.get("cleanup_error")
                    result["ok"] = True
                    result["summary"] = (
                        f"Moved {moved} photo{'s' if moved != 1 else ''}"
                        + (f", {len(errors)} error(s)" if errors else "")
                        + (
                            f"; cleanup failed: {cleanup_error}"
                            if cleanup_error else ""
                        )
                    )
            if result.get("ok"):
                if managed_staging_root:
                    from path_guard import contains_resolved
                    # Remove only empty staging ancestors. Failed transfers and
                    # concurrent sibling moves keep their originals intact.
                    parent = os.path.dirname(source_path)
                    root = os.path.realpath(managed_staging_root)
                    while contains_resolved(root, parent):
                        try:
                            os.rmdir(parent)
                        except OSError:
                            break
                        if os.path.realpath(parent) == root:
                            break
                        parent = os.path.dirname(parent)
                try:
                    _invalidate_missing_originals_cache()
                except Exception:
                    log.exception(
                        "Failed to invalidate missing-originals cache "
                        "after move-folder job",
                    )
            return result

        job_config = {
            "folder_id": folder_id, "destination": display_dest, "merge": merge,
            # Snapshot both ends of the move when it is enqueued. The source
            # catalog row is rewritten after a successful move, so resolving
            # it later would make completed jobs misleading. Likewise,
            # ``destination`` above is only the selected parent; the jobs UI
            # needs the actual landing path (including a rename/source leaf).
            "source_path": source_path,
            "resolved_destination": resolved_destination,
        }
        if folder_template:
            job_config["folder_template"] = folder_template
        if date_destinations:
            # Cap the stored list: a multi-year source folder can plan
            # thousands of date folders, and the whole config is serialized
            # into the job row and every status poll. The panel shows the
            # first few and reports the true totals from the counts below,
            # which are computed over the full plan.
            job_config["date_destinations"] = [
                {
                    "path": item["path"],
                    "relative_path": item["relative_path"],
                    "photo_count": item["photo_count"],
                }
                for item in date_destinations[:MOVE_DATE_DEST_PREVIEW_LIMIT]
            ]
            job_config["date_destination_count"] = len(date_destinations)
            job_config["date_photo_count"] = sum(
                item["photo_count"] for item in date_destinations)
        if destination_name:
            job_config["destination_name"] = destination_name
        if remote:
            # Surface that this is an SSH transfer (and to where) so the job
            # panel can show it, per the UI-transparency rule.
            job_config["remote"] = {
                "host": remote["host"], "user": remote["user"],
                "ssh_dest_base": remote["ssh_dest_base"],
                "mount_dest_base": remote["mount_dest_base"],
                "bwlimit_kbps": remote["bwlimit_kbps"],
            }
        if chained_from:
            # Provenance for the jobs panel: this move was started by a
            # chained process run's completion hook, not by hand.
            job_config["chained_from"] = chained_from

        def staged_work(job):
            runner.push_event(job["id"], "progress", {
                "current": 0, "total": 0, "current_file": "",
                "phase": "Waiting for workspace jobs to finish before sending to NAS",
            })
            if not runner.wait_for_workspace_transfer(job["id"]):
                return {"ok": False, "moved": 0, "errors": [], "summary": "Cancelled before transfer started"}
            return work(job)

        return runner.start(
            "move-folder", staged_work if managed_staging_root else work,
            config=job_config,
            workspace_id=workspace_id,
            **({"workspace_transfer_batch": managed_staging_root} if managed_staging_root else {}),
        )





    # -- Image serving --



    # -- Pipeline: SAM2 Mask Extraction --


    def _enqueue_move_folder_job(thread_db, runner, workspace_id, *,
                                 folder_id, subpath, target,
                                 chained_from=None, serialize_lock=None):
        """Enqueue a chained remote move for one imported folder.

        Job-thread path into move-folder (no request context). ``target`` is
        the snapshot captured when the import was enqueued — deliberately NOT
        re-resolved from Settings here, so a mid-chain edit cannot redirect
        the move. Raises on any precondition failure — the caller records the
        failure per folder rather than aborting the batch.
        """
        import posixpath

        import config as cfg
        import move as move_mod

        mount_path = (target.get("mount_path") or "").strip()
        if not mount_path:
            raise RuntimeError(
                "remote target has no local mount path, so moved photos "
                "couldn't stay in your library — add one under Settings → "
                "Remote targets")
        if not os.path.isabs(mount_path):
            raise RuntimeError(
                f"remote target's local mount path isn't absolute "
                f"(\"{mount_path}\") — fix it under Settings → Remote targets")
        guard = _move_folder_guard_error(thread_db, folder_id)
        if guard:
            raise RuntimeError(guard)
        effective_cfg = thread_db.get_effective_config(cfg.load())
        if target.get("transport") == "mounted":
            folder = thread_db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (folder_id,),
            ).fetchone()
            if not folder:
                raise RuntimeError("folder no longer exists")
            destination = os.path.join(mount_path, *posixpath.dirname(subpath).split("/"))
            return _start_move_folder_job(
                runner, workspace_id, folder_id=folder_id,
                destination=destination, display_dest=destination,
                destination_name="", source_path=folder["path"],
                resolved_destination=os.path.join(destination, os.path.basename(folder["path"])),
                merge=True, remote=None,
                developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
                chained_from=chained_from, serialize_lock=serialize_lock,
                allow_tracked_merge=True,
                managed_staging_root=target.get("managed_staging_root"),
                mount_baseline=target.get("mount_baseline"),
                mount_identities=target.get("mount_identities"),
            )
        rsync_bin = move_mod.resolve_rsync_bin(
            effective_cfg.get("rsync_bin", "") or "")
        if not rsync_bin:
            raise RuntimeError("no usable GNU rsync for remote moves")
        ssh_bin = move_mod.resolve_ssh_bin(
            effective_cfg.get("ssh_bin", "") or "")
        if not ssh_bin:
            raise RuntimeError("OpenSSH client not found")
        # ``move_folder`` lands the source folder INSIDE the destination,
        # keeping the folder's own name — so to mirror the archive layout
        # (``<local_archive_root>/2026/trip`` → ``<remote_path>/2026/trip``)
        # the spec's subpath must be the folder's PARENT ("2026"), not the
        # full archive-relative subpath, or the leaf would double up
        # ("2026/trip/trip").
        remote = move_mod.build_remote_move_spec(
            target, posixpath.dirname(subpath), rsync_bin, ssh_bin)
        folder = thread_db.conn.execute(
            "SELECT path, name FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not folder:
            raise RuntimeError("folder no longer exists")
        landing_name = folder["name"] \
            or os.path.basename(folder["path"].rstrip("/\\"))
        resolved_destination = move_mod.rsync_dest_spec(
            target,
            posixpath.join(remote["ssh_dest_base"], landing_name),
        )
        return _start_move_folder_job(
            runner, workspace_id,
            folder_id=folder_id,
            destination=remote["mount_dest_base"],
            display_dest=move_mod.rsync_dest_spec(
                target, remote["ssh_dest_base"]),
            destination_name="",
            source_path=folder["path"],
            resolved_destination=resolved_destination,
            merge=True,
            remote=remote,
            developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
            chained_from=chained_from,
            serialize_lock=serialize_lock,
            # Re-importing more photos into an existing shoot folder is the
            # NORMAL flow, and its chained move lands exactly on the tracked
            # NAS copy created by the previous chain run. Without tracked-
            # merge the move would refuse ("Destination overlaps a folder
            # Vireo already manages") and strand the new photos locally.
            # Opting in uses move_folder's exact-overlap reconciliation
            # (fold the new rows into the existing archive rows); the
            # pre-copy content-conflict scan still refuses any same-name
            # file whose bytes differ, and manual moves keep the default
            # refusal.
            allow_tracked_merge=True,
            managed_staging_root=target.get("managed_staging_root"),
        )

    @app.route("/api/encounters/species", methods=["POST"])
    def api_encounter_species():
        """Confirm species for all photos in an encounter or a single burst.

        Expects JSON: {"species": "Blue Jay", "photo_ids": [1, 2, 3],
                       "burst_index": <int|null>,
                       "add": <bool>, "remove": <bool>,
                       "previous_species": <str|null>}

        Creates the species keyword, tags photos, and queues a sidecar add.
        If the encounter (or burst) was previously confirmed as a different
        species, also untags that species and queues a sidecar remove — or
        cancels the still-pending add if it hadn't synced yet — so the XMP
        doesn't accumulate stale species keywords.

        A burst can legitimately hold two species (two subjects), so the
        confirmation is a *set* edit with three modes:

        * replace (default): ``species`` swaps out ``previous_species`` —
          the burst's primary confirmed species unless the client names
          another entry of the current list.
        * ``add``: ``species`` joins the current list; nothing is untagged
          and the burst is never auto-detached.
        * ``remove``: ``species`` is untagged from the submitted photos and
          dropped from the list; no keyword is added.
        """
        from pipeline_locks import acquire_workspace_regroup

        db = _get_db()
        with acquire_workspace_regroup(db._ws_id()):
            return prediction_decisions.under_prediction_decision_lock(
                db, lambda: _confirm_encounter_species(db),
                json_error=json_error,
            )

    def _confirm_encounter_species(db):
        """Confirm species while holding the grouping and database writer locks."""
        body = request.get_json(silent=True) or {}
        species = body.get("species", "").strip()
        photo_ids = body.get("photo_ids", [])
        burst_index = body.get("burst_index")
        add_mode = bool(body.get("add"))
        remove_mode = bool(body.get("remove"))
        requested_previous = body.get("previous_species")
        if add_mode and remove_mode:
            return json_error("add and remove are mutually exclusive")
        if requested_previous is not None and (add_mode or remove_mode):
            return json_error("previous_species only applies to a replace")

        # Normalize up front: this route compares the requested species
        # against stored keyword rows and previous_species cache values
        # below, and add_keyword would normalize on insert anyway — keep one
        # spelling throughout. Rejects quote-only input as empty.
        species = normalize_keyword_display(species)
        if not species:
            return json_error("species is required")
        if not photo_ids:
            return json_error("photo_ids is required")

        # Validate all photo_ids exist before mutating. Chunked so the
        # IN-clause stays under SQLite's bound-parameter cap.
        found_ids = set()
        for chunk in _chunked(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"SELECT id FROM photos WHERE id IN ({placeholders})", chunk
            ).fetchall()
            found_ids.update(r["id"] for r in rows)
        missing = [pid for pid in photo_ids if pid not in found_ids]
        if missing:
            return json_error(f"Unknown photo_ids: {missing}")

        # Surface photos whose only detections are below the workspace's
        # detector_confidence threshold, but do not drop them. This endpoint
        # represents an explicit user confirmation, and that assertion should
        # win over a weak detector box. Fully automatic labeling paths should
        # do their own filtering before calling lower-level tag helpers.
        import config as cfg
        effective_cfg = db.get_effective_config(cfg.load())
        det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)
        det_rows = []
        for chunk in _chunked(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            det_rows.extend(db.conn.execute(
                f"""SELECT photo_id,
                           MAX(detector_confidence) AS max_conf,
                           COUNT(*) AS n
                    FROM detections WHERE photo_id IN ({placeholders})
                    GROUP BY photo_id""",
                chunk,
            ).fetchall())
        low_confidence_photo_ids = [
            r["photo_id"] for r in det_rows
            if r["n"] > 0 and (r["max_conf"] or 0) < det_conf_threshold
        ]
        from pipeline import load_results_raw, save_results_raw
        from pipeline_results import (
            build_species_override,
            burst_species_list,
            empty_species_override,
            encounter_confirmed_species_list,
            set_encounter_confirmed_species,
            species_key_set,
            updated_species_list,
        )

        cache_dir = os.path.dirname(db_path)
        cached = load_results_raw(cache_dir, db._active_workspace_id)
        before_cached = copy.deepcopy(cached)
        cache_saved = False
        previous_species = None
        current_species_list = []
        target_enc = None
        target_enc_idx = None
        if cached:
            photo_id_set = set(photo_ids)
            for enc_idx, enc in enumerate(cached.get("encounters", [])):
                enc_ids = set(enc.get("photo_ids", []))
                if not photo_id_set.issubset(enc_ids):
                    continue
                target_enc = enc
                target_enc_idx = enc_idx
                break

        # If this is a burst-scoped request, the burst must actually exist in
        # the cached encounter AND the submitted photo_ids must be a subset of
        # that burst's photos. Otherwise a stale client (e.g. one that still
        # holds a burst index from before a regrouping) could retag photos
        # that don't belong to this burst while the cache update below touches
        # the wrong override.
        if burst_index is not None:
            bursts = target_enc.get("bursts") if target_enc else None
            if not bursts or not (0 <= burst_index < len(bursts)):
                return json_error(
                    f"Unknown burst_index {burst_index} for submitted photos",
                )
            burst_photo_ids = set(bursts[burst_index].get("photo_ids", []))
            if not set(photo_ids).issubset(burst_photo_ids):
                return json_error(
                    f"photo_ids are not members of bursts[{burst_index}]",
                )

        if target_enc is not None:
            if burst_index is not None:
                # A burst override wins when recorded; otherwise the burst
                # inherits the encounter's confirmed species, which is what
                # those photos were actually tagged with.
                current_species_list = burst_species_list(
                    target_enc, target_enc["bursts"][burst_index],
                )
            else:
                current_species_list = encounter_confirmed_species_list(
                    target_enc,
                )

        # The cached list comes from the pipeline cache on disk, which the
        # v5 DB migration does not touch — a pre-normalization cache can
        # still carry a quoted spelling like `‘Apapane`. Normalize before
        # comparing/looking up against stored keyword rows, which are always
        # clean.
        current_species_list = [
            name for name in (
                normalize_keyword_display(s) for s in current_species_list
            ) if name
        ]

        if remove_mode:
            # Only a currently confirmed species can be removed; otherwise a
            # stale client could strip an unrelated keyword from the photos
            # while the cache list (which has nothing to drop) still reports
            # success.
            requested_key = keyword_match_key(species)
            previous_species = next(
                (
                    s for s in current_species_list
                    if keyword_match_key(s) == requested_key
                ),
                None,
            )
            if previous_species is None:
                return json_error(
                    f'"{species}" is not a confirmed species of the '
                    "submitted photos",
                )
            species = previous_species
        elif add_mode:
            previous_species = None
        elif requested_previous is not None:
            requested_previous = normalize_keyword_display(
                str(requested_previous),
            )
            if not requested_previous:
                return json_error("previous_species must not be empty")
            # The named species must be one the burst is currently confirmed
            # as — including when nothing is confirmed (or there is no
            # cache), otherwise a stale client could have an arbitrary
            # keyword untagged from the submitted photos.
            requested_key = keyword_match_key(requested_previous)
            previous_species = next(
                (
                    s for s in current_species_list
                    if keyword_match_key(s) == requested_key
                ),
                None,
            )
            if previous_species is None:
                return json_error(
                    f'"{requested_previous}" is not a confirmed species '
                    "of the submitted photos",
                )
        else:
            previous_species = (
                current_species_list[0] if current_species_list else None
            )

        ws_id = db._ws_id()

        old_kid_row = None
        # ``per_photo_old_row`` maps each submitted photo to the effective
        # old-species keyword row the removal loop should match against.
        # For root-lookup (parent_id IS NULL) hits every photo shares the
        # same row; for the hierarchy-leaf fallback, catalogs can hold the
        # same alias under different taxa across submitted photos (a
        # legitimate homonym; duplicate repair deliberately preserves such
        # rows), so each photo gets its own resolved row. A single
        # ``best_taxon`` used to select one row for the whole batch would
        # leave every photo on a different taxon with its old leaf still
        # attached — the removal loop matches by ``taxon_id``.
        per_photo_old_row = {}
        # Compare using the ASCII-only fold SQLite's `COLLATE NOCASE` (which
        # add_keyword's dedupe relies on) uses — Python's str.lower() folds
        # non-ASCII letters that SQLite treats as distinct. Without this, a
        # cache-recorded confirmed species `Éclair` and a user-submitted
        # `éclair` would match here, the replacement path would be skipped,
        # yet add_keyword's NOCASE lookup would still create/tag a separate
        # `éclair` row — leaving the photo with both taxonomy tags and no
        # remove queued for the old one.
        is_replacement = remove_mode or (
            previous_species is not None
            and keyword_match_key(previous_species) != keyword_match_key(species)
        )
        if is_replacement:
            # Match add_keyword's write path: species keywords live as root
            # keywords (parent_id IS NULL). Looking up by name alone could
            # collide with a non-species homonym nested under another keyword
            # (schema allows UNIQUE(name, parent_id)). Accept taxonomy-typed
            # rows with is_species=0 too: update_keyword with an explicit
            # type='taxonomy' (the Keywords type dropdown) doesn't set the
            # legacy is_species column, and the rest of the app treats
            # (is_species = 1 OR type = 'taxonomy') as species.
            old_kid_row = db.conn.execute(
                """SELECT id, name, taxon_id FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL
                     AND (is_species = 1 OR type = 'taxonomy')""",
                (previous_species,),
            ).fetchone()
            if old_kid_row is not None and old_kid_row["taxon_id"] is None:
                # Unlinked root: ``previous_species`` names a specific
                # legacy row whose identity is that row's own id, not a
                # taxon. Assign it to every submitted photo — the removal
                # loop below relies on this exact-row identity to route
                # through the ``eff_taxon_id is None`` branch of its
                # homonym-conflict guard, which distinguishes the unlinked
                # legacy species from any linked same-name row on the
                # photo (a distinct species that must be preserved). A
                # per-photo resolution would pick up the attached linked
                # homonym row and treat that linked species AS the
                # previous species, queueing it for removal.
                for pid in photo_ids:
                    per_photo_old_row[pid] = old_kid_row
            else:
                # Linked root (its identity IS the taxon, and taxon-based
                # matching in the removal loop is what actually decides
                # equivalence) OR no root at all: resolve per-photo from
                # the rows actually attached to each submitted photo. A
                # single catalog-wide root assignment would map every
                # submitted photo to that root's taxon; the removal loop
                # matches attached rows by ``taxon_id``, so any submitted
                # photo whose only same-name tag is a hierarchy leaf
                # under a different taxon (a legitimate homonym —
                # duplicate repair deliberately preserves these rows)
                # would keep its old leaf attached while the new species
                # is added on top, leaving a stale duplicate. The
                # per-photo query naturally covers both attached shapes:
                # its ``ORDER BY parent_id IS NULL`` puts an attached
                # root first (fast common case) and otherwise picks the
                # attached hierarchy leaf on the photo's own taxon.
                #
                # Filter to species-rank (or NULL-rank) taxonomy-typed
                # rows and accept ``is_species = 1`` rows too — for the
                # same taxonomy-typed vs. legacy-species reason as the
                # root lookup.
                #
                # When a linked root exists, ALSO match rows whose
                # ``taxon_id`` equals the root's taxon even if the leaf
                # display name differs from ``previous_species`` — this
                # is how repaired hierarchy leaves (e.g. ``Desert Verdin``
                # under root ``Verdin`` after duplicate repair detached
                # the redundant root) get resolved. Without it, the
                # removal loop can't find any old row on the photo and
                # the endpoint records a plain add, leaving the photo
                # tagged with both the old leaf and the new species.
                linked_root_taxon = (
                    old_kid_row["taxon_id"] if old_kid_row is not None else None
                )
                old_kid_row = None
                candidate_rows = []
                for chunk in _chunked(photo_ids):
                    placeholders_ids = ",".join("?" for _ in chunk)
                    if linked_root_taxon is not None:
                        # Match by the linked root's taxon OR by
                        # ``previous_species`` name. The taxon arm catches
                        # repaired aliases (e.g. ``Desert Verdin`` sharing
                        # the root's taxon); the name arm preserves the
                        # existing behavior for attached same-name rows
                        # under a different taxon (a legitimate homonym
                        # duplicate repair leaves alone).
                        candidate_rows.extend(
                            db.conn.execute(
                                f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                                    FROM photo_keywords pk
                                    JOIN keywords k ON k.id = pk.keyword_id
                                    LEFT JOIN taxa t ON t.id = k.taxon_id
                                    WHERE pk.photo_id IN ({placeholders_ids})
                                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                                      AND (t.rank = 'species' OR t.rank IS NULL)
                                      AND (k.taxon_id = ?
                                           OR k.name = ? COLLATE NOCASE)
                                    ORDER BY CASE WHEN k.parent_id IS NULL
                                                  THEN 0 ELSE 1 END,
                                             k.id""",
                                [*chunk, linked_root_taxon, previous_species],
                            ).fetchall()
                        )
                    else:
                        candidate_rows.extend(
                            db.conn.execute(
                                f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                                    FROM photo_keywords pk
                                    JOIN keywords k ON k.id = pk.keyword_id
                                    LEFT JOIN taxa t ON t.id = k.taxon_id
                                    WHERE pk.photo_id IN ({placeholders_ids})
                                      AND k.name = ? COLLATE NOCASE
                                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                                      AND (t.rank = 'species' OR t.rank IS NULL)
                                    ORDER BY CASE WHEN k.parent_id IS NULL
                                                  THEN 0 ELSE 1 END,
                                             k.id""",
                                [*chunk, previous_species],
                            ).fetchall()
                        )
                for row in candidate_rows:
                    # SQL orders root rows first, then by id — first
                    # candidate per photo is the most canonical.
                    per_photo_old_row.setdefault(row["photo_id"], row)
                if per_photo_old_row:
                    # ``old_kid_row`` gates the removal block below.
                    # ``old_target_key`` is derived from
                    # ``previous_species`` (not this representative row)
                    # so the NULL-taxon fallback in the removal loop
                    # stays keyed to the requested display name even
                    # when the resolved row is a differently-named
                    # hierarchy alias.
                    old_kid_row = next(iter(per_photo_old_row.values()))

        # Run all mutations in a single transaction so that a mid-loop failure
        # (SQLite lock, disk error, etc.) can't leave half the photos retagged
        # while the other half still carry the old species.
        try:
            # Resolve the target species keyword id up front so we can precheck
            # which photos already carry it. add_keyword is idempotent and
            # returns the existing id when the species already exists.
            if remove_mode:
                # Nothing is being added: never create a keyword row just to
                # remove it. Report the existing root's id when one exists.
                stored = db.conn.execute(
                    """SELECT id, name FROM keywords
                       WHERE name = ? COLLATE NOCASE
                         AND parent_id IS NULL
                         AND (is_species = 1 OR type = 'taxonomy')
                       ORDER BY (type = 'taxonomy') DESC, id""",
                    (species,),
                ).fetchone()
                kid = stored["id"] if stored else None
                already_has_new = set()
                newly_tagged = []
            else:
                kid = db.add_keyword(species, is_species=True, _commit=False)
                # Queue/record the stored spelling (see api_add_keyword).
                stored = db.conn.execute(
                    "SELECT name FROM keywords WHERE id = ?", (kid,)
                ).fetchone()
                if stored and stored["name"]:
                    species = stored["name"]

                # A species can already be attached through a hierarchical
                # keyword row with a different id/casing. Compare by taxon_id
                # (or normalized name for taxonomy-less legacy rows), otherwise
                # a confirmation creates a redundant top-level association.
                already_has_new = db.get_photos_with_equivalent_species(
                    photo_ids, kid,
                )
                newly_tagged = [
                    pid for pid in photo_ids if pid not in already_has_new
                ]

            # Resolve every attached keyword row equivalent to the previous
            # species, including nested hierarchy leaves. Multiple hierarchy
            # placements are deliberate and survive duplicate repair, so a
            # replacement must remove and record all of them for undo/redo.
            old_rows_by_photo = {}
            if is_replacement and old_kid_row is not None:
                # ``old_target_key`` keys the NULL-taxon fallback in the
                # removal loop below. Derive it from ``previous_species``
                # (the requested display name) rather than the resolved
                # representative row's stored name — with the taxon-based
                # per-photo resolution above, that row can be a hierarchy
                # alias whose leaf name differs from what the user typed
                # (e.g. resolved ``Desert Verdin`` for requested
                # ``Verdin``), and the NULL-taxon fallback should still
                # match a legacy ``Verdin`` row on the photo.
                old_target_key = keyword_match_key(previous_species)
                # When the previous species is linked to a taxon and another
                # taxonomy row anywhere in the catalog shares the same
                # normalized name but points at a different taxon (e.g.
                # legacy ``Robin`` alongside taxonomy ``robin``), an
                # unlinked NULL-taxon row could belong to either species.
                # Treating it as the old species would queue a legacy
                # homonym tag for removal. Mirror the guard in
                # get_photos_with_equivalent_species.
                #
                # The same guard applies when the *previous species* is
                # unlinked: any distinct linked same-key row is a different
                # species, and folding it in would let encounter replacement
                # delete a taxonomy species from the photo.
                #
                # Cache the check per (taxon_id, kid_id) — hierarchy fallback
                # can resolve different taxa across submitted photos, and
                # each unique taxon gets its own homonym conflict answer.
                homonym_conflict_cache = {}

                def _homonym_conflict(target_taxon_id, target_kid_id):
                    cache_key = (
                        target_taxon_id,
                        None if target_taxon_id is not None else target_kid_id,
                    )
                    if cache_key in homonym_conflict_cache:
                        return homonym_conflict_cache[cache_key]
                    conflict = False
                    if target_taxon_id is not None:
                        hrows = db.conn.execute(
                            """SELECT name FROM keywords
                               WHERE (is_species = 1 OR type = 'taxonomy')
                                 AND taxon_id IS NOT NULL
                                 AND taxon_id != ?""",
                            (target_taxon_id,),
                        ).fetchall()
                    else:
                        hrows = db.conn.execute(
                            """SELECT name FROM keywords
                               WHERE (is_species = 1 OR type = 'taxonomy')
                                 AND taxon_id IS NOT NULL
                                 AND id != ?""",
                            (target_kid_id,),
                        ).fetchall()
                    for hrow in hrows:
                        if keyword_match_key(hrow["name"]) == old_target_key:
                            conflict = True
                            break
                    homonym_conflict_cache[cache_key] = conflict
                    return conflict

                for chunk in _chunked(photo_ids):
                    placeholders_ids = ",".join("?" for _ in chunk)
                    rows = db.conn.execute(
                        f"""SELECT pk.photo_id, k.id, k.name, k.taxon_id
                            FROM photo_keywords pk
                            JOIN keywords k ON k.id = pk.keyword_id
                            LEFT JOIN taxa t ON t.id = k.taxon_id
                            WHERE pk.photo_id IN ({placeholders_ids})
                              AND (k.is_species = 1 OR k.type = 'taxonomy')
                              AND (t.rank = 'species' OR t.rank IS NULL)
                            ORDER BY pk.photo_id,
                                     CASE WHEN k.parent_id IS NULL THEN 0 ELSE 1 END,
                                     k.id""",
                        list(chunk),
                    ).fetchall()
                    for row in rows:
                        photo_old = per_photo_old_row.get(row["photo_id"])
                        if photo_old is None:
                            # ``previous_species`` did not resolve to any
                            # row attached to this photo (no root-lookup
                            # hit, no candidate leaf); nothing to remove.
                            continue
                        eff_taxon_id = photo_old["taxon_id"]
                        eff_kid_id = photo_old["id"]
                        eff_homonym_conflict = _homonym_conflict(
                            eff_taxon_id, eff_kid_id,
                        )
                        if eff_taxon_id is None:
                            if eff_homonym_conflict:
                                # Unlinked previous species with a linked
                                # same-key homonym in the catalog: only the
                                # exact resolved old row is safe to remove.
                                same_species = row["id"] == eff_kid_id
                            else:
                                same_species = (
                                    keyword_match_key(row["name"]) == old_target_key
                                )
                        else:
                            same_species = (
                                row["taxon_id"] == eff_taxon_id
                            ) or (
                                not eff_homonym_conflict
                                and row["taxon_id"] is None
                                and keyword_match_key(row["name"]) == old_target_key
                            )
                        if same_species:
                            old_rows_by_photo.setdefault(row["photo_id"], []).append(row)

                # Same-taxon replacements (e.g., renaming to a scientific-name
                # alias) make the earlier taxon-equivalence precheck stale:
                # every equivalent row is scheduled for removal, so the photo
                # would end up carrying no species keyword. Recompute
                # equivalence while ignoring the rows about to be untagged and
                # move any now-uncovered photo back into ``newly_tagged`` so
                # the tag_photo/keyword_add loop below still writes ``kid``.
                excluded_ids = {
                    row["id"]
                    for rows in old_rows_by_photo.values()
                    for row in rows
                }
                if excluded_ids:
                    replacement_covered = list(
                        already_has_new & set(old_rows_by_photo)
                    )
                    if replacement_covered:
                        survivors = db.get_photos_with_equivalent_species(
                            replacement_covered, kid,
                            exclude_keyword_ids=excluded_ids,
                        )
                        lost_equivalence = set(replacement_covered) - survivors
                        if lost_equivalence:
                            already_has_new -= lost_equivalence
                            newly_tagged = [
                                pid for pid in photo_ids
                                if pid not in already_has_new
                            ]

                for pid, old_rows in old_rows_by_photo.items():
                    remove_names = []
                    for old in old_rows:
                        db.untag_photo(pid, old["id"], _commit=False)
                        # A same-taxon alias replace re-adds ``kid`` right
                        # after, so its sidecar remove is skipped; a remove
                        # must queue every untagged row.
                        if (remove_mode or old["id"] != kid) and (
                            old["name"] not in remove_names
                        ):
                            remove_names.append(old["name"])
                    for old_name in remove_names:
                        queue_keyword_remove(
                            db, pid, old_name, workspace_id=ws_id, _commit=False,
                        )

            had_old = set(old_rows_by_photo)

            for pid in newly_tagged:
                db.tag_photo(pid, kid, source="manual", _commit=False)
                queue_keyword_add(
                    db, pid, species, workspace_id=ws_id, _commit=False,
                )

            photo_edit_id = None
            if is_replacement and had_old:
                # Photos that actually changed: had the old keyword (so the
                # remove side fired) and/or newly gained the new one. Use the
                # union so undo restores the exact state we mutated.
                newly_set = set(newly_tagged)
                changed = [
                    pid for pid in photo_ids if pid in had_old or pid in newly_set
                ]
                items = []
                for pid in changed:
                    old_ids = [row["id"] for row in old_rows_by_photo.get(pid, [])]
                    if len(old_ids) > 1:
                        old_value = json.dumps({
                            "keyword_id": old_ids[0],
                            "keyword_ids": old_ids,
                        }, sort_keys=True)
                    else:
                        old_value = str(old_ids[0]) if old_ids else ""
                    items.append({
                        "photo_id": pid,
                        "old_value": old_value,
                        "new_value": str(kid) if pid in newly_set else "",
                    })
                if remove_mode:
                    description = (
                        f'Removed species "{species}" from {len(changed)} photos'
                    )
                else:
                    description = (
                        f'Replaced species "{previous_species}" with '
                        f'"{species}" on {len(changed)} photos'
                    )
                photo_edit_id = db.record_edit(
                    "species_replace",
                    description,
                    str(kid) if kid is not None else "",
                    items,
                    is_batch=len(changed) > 1,
                    _commit=False,
                )
            elif newly_tagged:
                items = [
                    {"photo_id": pid, "old_value": "", "new_value": str(kid)}
                    for pid in newly_tagged
                ]
                photo_edit_id = db.record_edit(
                    "keyword_add",
                    f'Confirmed species "{species}" on {len(newly_tagged)} photos',
                    str(kid),
                    items,
                    is_batch=len(newly_tagged) > 1,
                    _commit=False,
                )
            # The confirmed set after this edit, for the cache, the history
            # entry and the response.
            new_species_list = updated_species_list(
                current_species_list, species, previous_species,
                add=add_mode, remove=remove_mode,
            )
            if remove_mode:
                cache_description = f'Removed species "{species}" from '
            elif add_mode:
                cache_description = f'Added species "{species}" on '
            else:
                cache_description = f'Confirmed species "{species}" on '
            new_burst_override = None
            will_auto_detach = False
            new_encounter_state = None
            if cached and target_enc is not None:
                # The override's confirmed state comes from the database (read
                # inside this transaction, so it sees the tags just written),
                # the same rule serialize_results applies on regroup: a burst
                # is confirmed as the species every frame carries, and only
                # when every entry of the edited list is on every frame. Per-
                # frame extras ([A, C] beside [A]) do not unconfirm it — the
                # user just confirmed the set on all of it — and the list is
                # the cache's ordering plus any extra the frames all share
                # that the cache had not recorded. An edit that leaves some
                # frame without the full set records the list unconfirmed;
                # it still stays authoritative (burst_species_list), because
                # None would make the burst inherit the encounter's stale
                # list and resurrect a species this edit removed. An emptied
                # burst keeps an explicit empty override for the same reason.
                actual_by_photo = db.get_species_keywords_for_photos(photo_ids)
                actual_sets = [
                    species_key_set(actual_by_photo.get(pid, []))
                    for pid in photo_ids
                ]
                shared_keys = (
                    set.intersection(*actual_sets) if actual_sets else set()
                )
                recorded = species_key_set(new_species_list)
                frames_share = (
                    bool(actual_sets)
                    and all(actual_sets)
                    and bool(shared_keys)
                    and recorded <= shared_keys
                )
                if burst_index is not None:
                    if not new_species_list:
                        new_burst_override = empty_species_override()
                    elif frames_share:
                        extras = [
                            s for s in actual_by_photo.get(photo_ids[0], [])
                            if keyword_match_key(s) in shared_keys
                            and keyword_match_key(s) not in recorded
                        ]
                        new_burst_override = build_species_override(
                            new_species_list + extras,
                        )
                    else:
                        new_burst_override = build_species_override(
                            new_species_list, confirmed=False,
                        )
                    # Auto-detach if the burst's species no longer overlap
                    # its encounter's — splits it out and merges into an
                    # adjacent encounter of the same confirmed species when
                    # one exists. Compare with keyword_match_key so a cached
                    # pre-normalization spelling (e.g. ‘Apapane) does not
                    # trigger a needless split against the stored species
                    # (Apapane); the DB write already normalized to the
                    # canonical form. Adding a second species never
                    # detaches (it can only widen overlap); a remove
                    # detaches only when it leaves a non-empty set that
                    # shares nothing with the encounter. An emptied burst
                    # stays put: there is nothing to file it under.
                    enc_species_list = encounter_confirmed_species_list(target_enc)
                    if not enc_species_list and target_enc.get("species"):
                        enc_species_list = [target_enc["species"][0]]
                    will_auto_detach = (
                        not add_mode
                        and bool(new_species_list)
                        and bool(enc_species_list)
                        and not (
                            species_key_set(new_species_list)
                            & species_key_set(enc_species_list)
                        )
                        and len(target_enc["bursts"]) > 1
                    )
                else:
                    new_encounter_state = {
                        "confirmed_species": (
                            new_species_list[0] if new_species_list else None
                        ),
                        "confirmed_species_list": list(new_species_list),
                        "species_confirmed": bool(new_species_list) and frames_share,
                    }

            cache_only_write = (
                not (is_replacement and had_old)
                and not newly_tagged
                and cached
                and target_enc is not None
            )
            if cache_only_write:
                # No keyword row was recorded (every photo already carries
                # this species), but the cache mutation below will still
                # write ``confirmed_species`` / ``species_confirmed`` (or a
                # burst ``species_override``). Without a matching history
                # entry, a preceding grouping edit would remain the newest
                # undoable row and its undo would silently discard this
                # confirmation because grouping signatures ignore these
                # species fields by design. Record the cache-only write so
                # LIFO undo reverts it first. When auto-detach will fire
                # the structural change and confirmation share one grouping
                # history entry further down instead.
                from services.grouping_history import (
                    record_species_confirm_cache,
                )
                if burst_index is not None:
                    burst_target = target_enc["bursts"][burst_index]
                    current_override = burst_target.get("species_override")
                    if current_override != new_burst_override and not will_auto_detach:
                        record_species_confirm_cache(
                            db, species=species, target_enc=target_enc,
                            burst_index=burst_index,
                            submitted_photo_ids=photo_ids,
                            new_override=new_burst_override,
                            description=cache_description + "1 burst",
                        )
                else:
                    current_state = {
                        "confirmed_species": target_enc.get("confirmed_species"),
                        "confirmed_species_list": list(
                            encounter_confirmed_species_list(target_enc)
                        ),
                        "species_confirmed": bool(target_enc.get("species_confirmed")),
                    }
                    if current_state != new_encounter_state:
                        record_species_confirm_cache(
                            db, species=species, target_enc=target_enc,
                            burst_index=None,
                            submitted_photo_ids=photo_ids,
                            new_encounter_state=new_encounter_state,
                            description=(
                                cache_description + f"{len(photo_ids)} photos"
                            ),
                        )

            # Apply the pipeline-cache mutation and persist inside the same
            # transaction so a failed ``save_results_raw`` rolls back the
            # species/keyword edits that would otherwise leave undo pointing
            # at a change that never landed on disk. ``burst_index`` was
            # validated above, so the branch here is unambiguous: burst-
            # scoped requests only touch the burst override, encounter-
            # scoped requests only touch the encounter.
            if cached and target_enc is not None:
                if burst_index is not None:
                    target_enc["bursts"][burst_index]["species_override"] = (
                        new_burst_override
                    )
                    if will_auto_detach:
                        auto_detach_burst_for_species(
                            cached, target_enc_idx, burst_index,
                            new_species_list[0],
                        )
                        change = {
                            "before": before_cached["encounters"],
                            "after": cached["encounters"],
                        }
                        if photo_edit_id is None:
                            from services.grouping_history import record_grouping_edit
                            record_grouping_edit(
                                db, cache_description + "1 burst", change, [],
                            )
                else:
                    set_encounter_confirmed_species(target_enc, new_species_list)
                    target_enc["species_confirmed"] = bool(
                        new_encounter_state["species_confirmed"]
                    )
                    # Child bursts may carry overrides serialize_results (or
                    # an earlier burst edit) materialized for the pre-edit
                    # species. Both review pages read those before the
                    # encounter, so leaving them would show the old species
                    # on every burst and let a flag-only Rapid apply re-tag
                    # it. Rebuild each fully-submitted burst's override from
                    # what its frames carry now (read inside this
                    # transaction); bursts without an override keep
                    # inheriting the encounter's new list.
                    from pipeline import derive_burst_override
                    submitted = set(photo_ids)
                    for child in target_enc.get("bursts") or []:
                        if child.get("species_override") is None:
                            continue
                        child_ids = child.get("photo_ids") or []
                        if not child_ids or not set(child_ids) <= submitted:
                            continue
                        child["species_override"] = derive_burst_override(
                            [
                                {"confirmed_species_list": actual_by_photo.get(pid, [])}
                                for pid in child_ids
                            ],
                            preferred_order=new_species_list,
                        )
                if photo_edit_id is not None and before_cached["encounters"] != cached["encounters"]:
                    # Labels and confirmation counts are part of the same user
                    # action even when no burst moves to another encounter.
                    photo_edit = db.conn.execute(
                        "SELECT action_type, new_value FROM edit_history WHERE id = ?",
                        (photo_edit_id,),
                    ).fetchone()
                    change = {
                        "before": before_cached["encounters"],
                        "after": cached["encounters"],
                        "photo_edit": dict(photo_edit),
                    }
                    from services.grouping_history import convert_to_grouping_edit
                    convert_to_grouping_edit(db, photo_edit_id, change)
                save_results_raw(cached, cache_dir, db._active_workspace_id)
                cache_saved = True

            db.conn.commit()
        except Exception:
            db.conn.rollback()
            if cache_saved:
                try:
                    save_results_raw(before_cached, cache_dir, db._active_workspace_id)
                except Exception:
                    log.exception("Failed to restore pipeline cache after species confirmation failed")
            raise
        # Prune oldest edit-history rows now that the transaction has landed.
        db._prune_edit_history()

        # Report `replaced` consistent with the actual replacement decision
        # (is_replacement, which uses keyword_match_key to match SQLite's
        # ASCII-only NOCASE). Python's `.lower()` folds non-ASCII pairs like
        # `Éclair`/`éclair` — which SQLite/add_keyword keep as distinct
        # keyword rows — so a `.lower()` comparison here would report
        # replaced=None on a request that actually untagged the previous
        # species row and tagged a new one.
        replaced = (
            previous_species if is_replacement and not remove_mode else None
        )
        response = {
            "ok": True,
            "species": species,
            "keyword_id": kid,
            "photo_count": len(photo_ids),
            "previous_species": replaced,
            "mode": "remove" if remove_mode else "add" if add_mode else "replace",
            "species_list": new_species_list,
            "low_confidence_photo_ids": low_confidence_photo_ids,
            # Kept for older clients/tests that checked this field; explicit
            # confirmations no longer skip submitted photos.
            "skipped_photo_ids": [],
        }
        if cached:
            response["encounters"] = cached.get("encounters", [])
            response["summary"] = cached.get("summary", {})
        return jsonify(response)

    app.register_blueprint(
        create_media_blueprint(
            _get_db,
            json_error,
            db_path,
            app.config,
            invalid_preview_cache_paths=_invalid_preview_cache_paths,
            clear_preview_cache_invalid=_clear_preview_cache_invalid,
            photo_not_found_error=_photo_not_found_error,
        )
    )
    # The prepare-full-resolution job calls the /original view directly so
    # its RAW/companion/edit fallbacks cannot drift from the lightbox's.
    serve_original_photo = app.view_functions["media.serve_original_photo"]

    app.register_blueprint(
        create_photo_labels_blueprint(
            _get_db, json_error, settings_write_lock=_settings_write_lock
        )
    )
    app.register_blueprint(create_photo_review_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_life_list_blueprint(
            _get_db,
            json_error,
            build_life_list_payload=_build_life_list_payload,
        )
    )
    app.register_blueprint(
        create_audit_blueprint(
            _get_db,
            json_error,
            app.config,
            cleanup_cached_files_for_deleted_photos=(
                _cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(
        create_settings_blueprint(
            _get_db,
            json_error,
            app.config,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            advance_inat_token_generation=_inat_token_generation.advance,
        )
    )
    app.register_blueprint(
        create_inat_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            token_generation=_inat_token_generation,
            settings_write_lock=_settings_write_lock,
            read_raw_config_file=_read_raw_config_file,
            max_selection_photos=MAX_SELECTION_PHOTOS,
        )
    )
    app.register_blueprint(
        create_storage_blueprint(
            _get_db, json_error, db_path, app.config, chunked=_chunked,
        )
    )
    app.register_blueprint(
        create_workspace_blueprint(
            _get_db,
            json_error,
            ALL_PAGES,
            get_runner=lambda: app._job_runner,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            settings_write_lock=_settings_write_lock,
            new_images_walk_progress=app._new_images_walk_progress,
            missing_originals_heavy_job_types=_MISSING_ORIGINALS_HEAVY_JOB_TYPES,
        )
    )
    app.register_blueprint(
        create_folders_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            build_scan_work=_build_scan_work,
            cleanup_cached_files_for_deleted_photos=(
                _cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(create_capture_time_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_remote_setup_blueprint(_get_db, json_error, app.config)
    )
    app.register_blueprint(
        create_editing_blueprint(
            _get_db,
            json_error,
            settings_write_lock=_settings_write_lock,
            read_raw_config_file=_read_raw_config_file,
        )
    )
    app.register_blueprint(create_species_blueprint(_get_db))
    app.register_blueprint(
        create_system_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            get_log_broadcaster=lambda: app._log_broadcaster,
            settings_write_lock=_settings_write_lock,
            read_raw_config_file=_read_raw_config_file,
        )
    )
    app.register_blueprint(
        create_misses_blueprint(
            _get_db,
            json_error,
            settings_write_lock=_settings_write_lock,
            resolve_visual=visual_scope.resolve,
        )
    )
    app.register_blueprint(
        create_browse_blueprint(
            _get_db,
            json_error,
            visual_scope=visual_scope,
            max_per_page=_MAX_PER_PAGE,
            ambiguous_prediction_ids=_ambiguous_prediction_ids,
        )
    )
    # Registered before the /api/v1 alias loop below, which aliases two of
    # these endpoints by their ``collections.``-qualified names.
    app.register_blueprint(
        create_collections_blueprint(
            _get_db, json_error, max_per_page=_MAX_PER_PAGE,
        )
    )
    app.register_blueprint(create_dashboard_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_sync_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
        )
    )
    from web.move_cleanup import create_move_cleanup_blueprint
    app.register_blueprint(create_move_cleanup_blueprint(
        _get_db, lambda: app._job_runner, json_error,
        lambda paths: _trash_paths(paths), _move_folder_guard_error,
    ))
    app.register_blueprint(create_moves_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_duplicates_blueprint(
            _get_db,
            json_error,
            chunked=_chunked,
            # Late-bound through the module globals, like the move-cleanup
            # blueprint's trash hook above, so a patched ``app._trash_paths``
            # or ``app._network_volume_roots`` still reaches these routes.
            trash_paths=lambda *args, **kwargs: _trash_paths(*args, **kwargs),
            network_volume_roots=lambda: _network_volume_roots(),
            path_on_network_volume=_path_on_network_volume,
            cleanup_cached_files_for_deleted_photos=(
                _cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(
        create_card_cleanup_blueprint(
            _get_db, json_error, lambda: app._job_runner, db_path, app.config,
        )
    )
    app.register_blueprint(
        create_models_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            count_keywords=init_db.count_keywords,
        )
    )
    app.register_blueprint(
        create_caches_blueprint(_get_db, json_error, db_path, app.config)
    )
    app.register_blueprint(
        create_export_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            build_life_list_payload=_build_life_list_payload,
            build_highlights_payload=_build_highlights_payload,
            resolve_visual=visual_scope.resolve,
        )
    )
    app.register_blueprint(
        create_pipeline_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
        )
    )
    # Built here rather than at the top: the after-process NAS move hands
    # off to _enqueue_move_folder_job, which is defined late in create_app
    # and passed by value.
    pipeline_chain = PipelineChain(
        get_runner=lambda: app._job_runner,
        db_path=db_path,
        config=app.config,
        invalidate_missing_originals=_invalidate_missing_originals_cache,
        enqueue_move_folder_job=_enqueue_move_folder_job,
    )
    app.register_blueprint(
        create_imports_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            metadata_repair_count=_metadata_repair_count,
            enqueue_process_job=pipeline_chain.enqueue_process_job,
            chain_after_move=pipeline_chain.chain_after_move,
            bulk_gps_location_payload=_bulk_gps_location_payload,
            guard_move_folder=_move_folder_guard_error,
            sync_job_lock=app._sync_job_lock,
        )
    )
    app.register_blueprint(create_keywords_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_locations_blueprint(
            _get_db,
            json_error,
            location_errors=location_errors,
            normalize_photo_id_list=_normalize_photo_id_list,
            bulk_gps_location_source_ids=_bulk_gps_location_source_ids,
            location_keyword_photo_ids=_location_keyword_photo_ids,
            google_reverse_geocode=_google_reverse_geocode,
            decode_cached_reverse_geocode=_decode_cached_reverse_geocode,
            encode_cached_reverse_geocode=_encode_cached_reverse_geocode,
            summarize_details=_summarize_details,
        )
    )
    app.register_blueprint(
        create_batch_blueprint(
            _get_db,
            json_error,
            location_errors=location_errors,
            bulk_gps_location_payload=_bulk_gps_location_payload,
            run_batch_delete=_run_batch_delete,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    # Registered before the /api/v1 alias loop below, which aliases
    # ``photos.api_photos`` and ``photos.api_photo_detail``.
    app.register_blueprint(
        create_photos_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            visual_scope=visual_scope,
            photo_not_found_error=_photo_not_found_error,
            max_per_page=_MAX_PER_PAGE,
            request_flag_filter=_request_flag_filter,
            request_location_status_filter=_request_location_status_filter,
            parse_missing_originals_folder_id=_parse_missing_originals_folder_id,
            missing_originals_payload=_missing_originals_payload,
            start_missing_originals_scan=_start_missing_originals_scan,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            run_batch_delete=_run_batch_delete,
            photo_highlight_entries=_photo_highlight_entries,
            best_batch_scope=_best_batch_scope,
            build_best_batch_response=_build_best_batch_response,
        )
    )
    app.register_blueprint(
        create_photo_edit_recipes_blueprint(
            _get_db,
            json_error,
            app.config,
            photo_not_found_error=_photo_not_found_error,
            invalidate_photo_render_cache=_invalidate_photo_render_cache,
            queue_edit_recipe_sync=_queue_edit_recipe_sync,
        )
    )
    app.register_blueprint(
        create_history_blueprint(
            _get_db,
            json_error,
            db_path,
            invalidate_photo_render_cache=_invalidate_photo_render_cache,
            queue_edit_recipe_sync=_queue_edit_recipe_sync,
        )
    )
    app.register_blueprint(
        create_photo_location_keywords_blueprint(
            _get_db,
            json_error,
            photo_not_found_error=_photo_not_found_error,
            location_errors=location_errors,
        )
    )

    app.register_blueprint(
        create_job_launchers_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            chunked=_chunked,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            run_batch_delete=_run_batch_delete,
            build_scan_work=_build_scan_work,
            pending_local_workspace_transition=_pending_local_workspace_transition,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            metadata_repair_count=_metadata_repair_count,
            guard_move_folder=_move_folder_guard_error,
            start_move_folder_job=_start_move_folder_job,
            # Late-bound so it resolves whichever ``serve_original_photo``
            # create_app holds when a job runs, not when the app is built.
            serve_original_photo=lambda *args, **kwargs: serve_original_photo(
                *args, **kwargs
            ),
            sync_job_lock=app._sync_job_lock,
        )
    )

    # --- /api/v1/* aliases over the stable subset of /api/* ---
    # These are the endpoints advertised to external callers in docs/headless-api.md.
    # Keep this list tight — expanding it locks the surface.
    _V1_ALIASES = [
        # (v1 path, existing endpoint name, methods)
        ("/api/v1/photos", "photos.api_photos", ["GET"]),
        ("/api/v1/photos/<int:photo_id>", "photos.api_photo_detail", ["GET"]),
        ("/api/v1/collections", "collections.api_collections", ["GET"]),
        ("/api/v1/collections/<int:collection_id>/photos",
         "collections.api_collection_photos", ["GET"]),
        ("/api/v1/workspaces", "workspaces.api_get_workspaces", ["GET"]),
        ("/api/v1/workspaces/<int:ws_id>/activate",
         "workspaces.api_activate_workspace", ["POST"]),
        ("/api/v1/keywords", "keywords.api_keywords", ["GET"]),
    ]

    for v1_path, endpoint_name, methods in _V1_ALIASES:
        view = app.view_functions.get(endpoint_name)
        if view is None:
            raise RuntimeError(
                f"Cannot alias {v1_path}: endpoint '{endpoint_name}' not registered"
            )
        # Blueprint endpoints are aliased under their bare view name so the
        # v1 endpoint names stay ``v1_<view>`` whichever module owns the route.
        v1_endpoint = f"v1_{endpoint_name.rpartition('.')[2]}"
        app.add_url_rule(
            v1_path,
            endpoint=v1_endpoint,
            view_func=view,
            methods=methods,
        )
        # An alias is exempt from the workspace mutation reservation exactly
        # when the view it aliases is, so the two surfaces can't drift.
        if endpoint_name in _reservation_exempt_endpoints:
            _reservation_exempt_endpoints.add(v1_endpoint)

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        # Give the main thread enough time to return from create_app and bind
        # the HTTP listener before this potentially multi-minute XMP scan
        # starts competing for filesystem and interpreter time.
        _wildlife_retirement_timer = threading.Timer(
            1.0, _retire_wildlife_genre,
        )
        _wildlife_retirement_timer.daemon = True
        _wildlife_retirement_timer.start()

    return app


def _emit_incompatible_database_exit(e):
    # Both --load-taxonomy and create_app open the catalog; either can trip
    # ensure_schema's newer-DB guard, and both need the same guided exit so
    # the desktop launcher gets a structured signal instead of a raw
    # traceback.
    import sys as _sys
    if getattr(e, "newer", False):
        log.error(
            "Cannot open database at %s: it was created by a newer "
            "version of Vireo than this build supports. Update Vireo to "
            "its latest version to open this catalog. Underlying error: %s",
            e.db_path, e.cause,
        )
    else:
        log.error(
            "Cannot open database at %s: it is from an incompatible older "
            "version of Vireo. Back it up and remove it to start fresh "
            "(e.g. `mv %s %s.bak`), then relaunch. Underlying error: %s",
            e.db_path, e.db_path, e.db_path, e.cause,
        )
    _sys.stderr.write(json.dumps({
        "error": "incompatible_database",
        "db_path": e.db_path,
        "reason": str(e),
        "newer": getattr(e, "newer", False),
    }) + "\n")
    raise SystemExit(3) from e


def main():
    _setup_file_logging()

    parser = argparse.ArgumentParser(description="Vireo Photo Browser")
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/.vireo/vireo.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--thumb-dir",
        default=os.path.expanduser("~/.vireo/thumbnails"),
        help="Path to thumbnail cache directory",
    )
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run without opening a browser; write runtime.json and enable "
             "the /api/v1 API. Use this when invoking the sidecar directly "
             "from scripts or agents.",
    )
    parser.add_argument(
        "--load-taxonomy",
        action="store_true",
        help="Download and import the iNaturalist taxonomy, then exit",
    )
    parser.add_argument(
        "--check-exiftool",
        action="store_true",
        help="Verify the bundled/system ExifTool, print its version, and exit",
    )
    args = parser.parse_args()

    if args.check_exiftool:
        from metadata import exiftool_status

        status = exiftool_status()
        if status["available"]:
            print(status["version"] or "unknown")
            raise SystemExit(0)
        print(status.get("error") or status.get("hint") or "ExifTool unavailable", file=sys.stderr)
        raise SystemExit(1)

    if args.headless:
        args.no_browser = True

    if args.load_taxonomy:
        from db import Database
        from taxonomy import fetch_common_names, load_taxonomy, seed_informal_groups
        # Run the newer-schema guard before Database(args.db) executes any
        # legacy DDL/ALTERs against a catalog stamped by a future Vireo build.
        # create_app takes this same check through ensure_schema; keep the two
        # entry points in sync so `--load-taxonomy` can't corrupt a newer DB.
        try:
            ensure_schema(args.db)
        except IncompatibleDatabaseError as e:
            _emit_incompatible_database_exit(e)
        db = Database(args.db)
        log.info("Loading taxonomy tree from iNaturalist...")
        stats = load_taxonomy(db)
        log.info("  Taxonomy: %d taxa loaded, %d skipped", stats['loaded'], stats['skipped'])
        log.info("Fetching common names from iNat API (this may take a few minutes)...")
        cn_stats = fetch_common_names(db)
        log.info("  Common names: %d taxa updated", cn_stats['updated'])
        log.info("Seeding informal groups...")
        ig_stats = seed_informal_groups(db)
        log.info("  Informal groups: %d groups created", ig_stats['groups_created'])
        log.info("Done.")
        raise SystemExit(0)

    # Resolve port: --port 0 means pick a random free port
    port = args.port
    if port == 0:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

    from runtime import (
        acquire_single_instance,
        delete_runtime_json,
        generate_token,
        release_single_instance,
        write_runtime_json,
    )

    # Atomically reserve the single-instance slot BEFORE any heavy
    # initialization. Reserving up-front (rather than writing runtime.json
    # at the end of startup) closes the race where two near-simultaneous
    # launches both see an empty slot and both start serving.
    try:
        status, info = acquire_single_instance(pid=os.getpid())
    except OSError as e:
        # Filesystem fault opening the lock file (unreadable ~/.vireo,
        # permission denied, etc). Surface the real cause rather than
        # misreporting as already_running — the two need different
        # remediation.
        import sys as _sys
        _sys.stderr.write(json.dumps({
            "error": "startup_failed",
            "reason": str(e),
        }) + "\n")
        raise SystemExit(2) from e
    if status == "conflict":
        import sys as _sys
        _sys.stderr.write(json.dumps({
            "error": "already_running",
            "port": info.get("port"),
            "pid": info.get("pid"),
        }) + "\n")
        raise SystemExit(1)

    # Register cleanup immediately after acquiring the slot so a crash
    # during initialization still releases the reservation lock and any
    # runtime.json we may have written.
    import atexit
    import signal as _signal

    def _cleanup_runtime_state():
        delete_runtime_json()
        release_single_instance()

    atexit.register(_cleanup_runtime_state)
    _signal.signal(_signal.SIGTERM, lambda *_: (_cleanup_runtime_state(), os._exit(0)))

    api_token = generate_token()
    mode = "headless" if args.headless else "gui"

    try:
        app = create_app(
            db_path=args.db, thumb_cache_dir=args.thumb_dir, api_token=api_token,
        )
    except IncompatibleDatabaseError as e:
        # The database file predates a schema change this build can't migrate.
        # Fail fast with actionable guidance instead of letting a raw
        # OperationalError traceback escape (which the sidecar host only sees
        # as "did not become healthy within 30s"). The atexit/SIGTERM cleanup
        # registered above releases the single-instance lock and runtime.json
        # on this exit, so a retry isn't blocked by a stale reservation.
        _emit_incompatible_database_exit(e)
    except Exception as e:
        # Any other failure to build the app is still a fatal startup error
        # (corrupt-but-not-stale DB, missing/locked resource, an unexpected
        # bug, ...). Emit the same structured signal the lock-fault path uses
        # so the desktop launcher can surface an actionable dialog instead of
        # leaving the user with a blank window or a generic 30s health-check
        # timeout. The full traceback still goes to the log for diagnosis.
        import sys as _sys
        log.exception("Vireo failed to start while initializing the app")
        _sys.stderr.write(json.dumps({
            "error": "startup_failed",
            "reason": str(e) or e.__class__.__name__,
        }) + "\n")
        raise SystemExit(2) from e

    # Startup banner
    import config as cfg
    startup_cfg = cfg.load()
    log.info("=" * 50)
    log.info("Vireo starting on http://localhost:%d", port)
    log.info("  Database: %s", args.db)
    log.info("  Thumbnails: %s", args.thumb_dir)
    log.info("  Threshold: %.0f%%  Grouping: %ds  Similarity: %.0f%%",
             startup_cfg.get("classification_threshold", 0.4) * 100,
             startup_cfg.get("grouping_window_seconds", 10),
             startup_cfg.get("similarity_threshold", 0.85) * 100)
    if startup_cfg.get("hf_token"):
        log.info("  HuggingFace token: configured")
    log.info("=" * 50)

    # Open browser after server is ready, not before
    if not args.no_browser:
        import threading
        import urllib.request

        def _open_browser():
            url = f"http://localhost:{port}"
            for _ in range(50):  # try for up to 5 seconds
                try:
                    urllib.request.urlopen(url, timeout=0.1)
                    webbrowser.open(url)
                    return
                except Exception:
                    time.sleep(0.1)

        threading.Thread(target=_open_browser, daemon=True).start()

    # Look up the running version using the same fallback chain as
    # /api/version: package metadata, then pyproject.toml, then "0.0.0".
    # In source/dev runs where importlib.metadata is missing but
    # pyproject.toml is present, runtime.json must agree with
    # /api/v1/version — external callers use it to make compatibility
    # decisions and a bare "0.0.0" would mislead them.
    try:
        from importlib.metadata import version as pkg_version
        ver = pkg_version("vireo")
    except Exception:
        import tomllib
        try:
            with open(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"), "rb") as f:
                ver = tomllib.load(f)["project"]["version"]
        except Exception:
            ver = "0.0.0"

    # Finalize runtime.json, replacing the reservation marker with the full
    # payload now that the port and token are known. Cleanup handlers were
    # registered immediately after `acquire_single_instance` above.
    write_runtime_json(
        port=port, pid=os.getpid(), version=ver, db_path=args.db,
        token=api_token, mode=mode,
    )

    # Waitress uses a fixed thread pool (default 4). Each open SSE stream
    # (bottom-panel logs, job progress, import duplicate check) pins one
    # thread for its whole lifetime, so the pool must be sized well above
    # the plausible number of concurrent streams or page loads queue
    # behind them and the app appears frozen.
    from waitress import serve as waitress_serve
    waitress_serve(app, host="127.0.0.1", port=port, threads=16)


if __name__ == "__main__":
    # In a PyInstaller bundle, multiprocessing workers re-execute this binary.
    # Without freeze_support, the child runs main() — argparse rejects the
    # `--multiprocessing-fork ...` argv (or the `-c` bootstrap), the child
    # exits, and the parent gets EOFError on the handshake socket. The
    # PyInstaller runtime hook installs a freeze_support that intercepts
    # those argv shapes and runs the worker bootstrap instead, but only
    # when we actually call it.
    import multiprocessing
    multiprocessing.freeze_support()
    # --pty-spawn-helper: pty setup shim dispatched by remote_setup's
    # install-key path. In the packaged (PyInstaller --onefile) build
    # sys.executable is this same binary, so remote_setup can't shell
    # out via `[python, "-c", helper]` and instead re-executes us with
    # this flag. Must run BEFORE argparse — the wrapped ssh argv follows
    # and would otherwise get rejected as unknown options.
    if len(sys.argv) >= 3 and sys.argv[1] == "--pty-spawn-helper":
        import fcntl
        import termios
        os.setsid()
        fcntl.ioctl(0, termios.TIOCSCTTY, 0)
        a = termios.tcgetattr(0)
        a[3] &= ~(termios.ECHO | termios.ECHOE | termios.ECHOK
                  | termios.ECHONL)
        termios.tcsetattr(0, termios.TCSANOW, a)
        os.execvp(sys.argv[2], sys.argv[2:])
    main()
