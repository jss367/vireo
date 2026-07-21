"""User configuration for Vireo (persisted to ~/.vireo/config.json)."""

import contextlib
import copy
import json
import logging
import os
import sys
import tempfile
import threading
import time

log = logging.getLogger(__name__)

CONFIG_PATH = os.path.expanduser("~/.vireo/config.json")

_lock = threading.Lock()

DEFAULTS = {
    "classification_threshold": 0.4,
    "grouping_window_seconds": 10,
    "similarity_threshold": 0.85,
    "preview_max_size": 1920,
    "keyword_case": "auto",
    "sync_flags_to_xmp": True,
    "write_assigned_location_to_xmp": False,
    "max_edit_history": 1000,
    "inat_token": "",
    "hf_token": "",
    "google_maps_api_key": "",
    "scan_roots": [],
    "scan_workers": 0,
    "setup_complete": False,
    # Path to a GNU rsync binary used for remote (SSH) folder moves. macOS
    # ships Apple's `openrsync`, which cannot drive rsync-over-SSH, so a
    # remote move needs real GNU rsync. Empty = auto-resolve (bundled binary,
    # then a few known install paths) via move.resolve_rsync_bin(). Local
    # moves are unaffected and keep using whatever `rsync` is on PATH.
    "rsync_bin": "",
    # Optional explicit Windows OpenSSH path. Empty uses PATH and then the
    # standard Windows optional-feature location.
    "ssh_bin": "",
    # Saved remote (NAS) destinations for folder moves over SSH. Each entry:
    #   {"id", "name", "host", "user", "port", "ssh_key",
    #    "remote_path", "mount_path", "bwlimit_kbps"}
    # `remote_path` is the NAS-side filesystem path used for the rsync-over-SSH
    # transfer; `mount_path` is the local path (e.g. an SMB mount) where Vireo
    # can read those same files afterward, and is what the catalog points at
    # once a move completes. `local_archive_root` (optional) is the local
    # directory that mirrors `remote_path` for chained import→process→move
    # runs — see `_coerce_remote_target`. Custom settings UI (like
    # external_editors), so it's excluded from SCHEMA.
    "remote_targets": [],
    "darktable_bin": "",
    # Legacy single-editor field. Kept for one-cycle migration: if
    # `external_editors` is empty and this is set, get_editors() synthesizes
    # a one-element list from it. Hidden from the schema-rendered settings.
    "external_editor": "",
    # List of {"name": str, "path": str} dicts. Source of truth for the
    # multi-editor "Open in Editor" picker.
    "external_editors": [],
    "report_url": "https://script.google.com/macros/s/AKfycbwqjy8KaB0X04b9R614PWkikRmEsbarXXdarl0S0QC6thT9Uoyn8F74Gku-5z9h-TTf/exec",
    "darktable_style": "",
    "darktable_output_format": "jpg",
    "darktable_output_dir": "",
    "darktable_auto_convert_dng": True,
    "dng_converter_bin": "",
    # When true, the Tauri desktop wrapper opens this UI in the user's
    # default web browser on launch instead of creating its WKWebView
    # window. The Flask sidecar and tray icon still run as usual.
    # Read by `src-tauri/src/lib.rs` at startup; takes effect after restart.
    "open_in_browser": False,
    # --- Subject identification ---
    # Keyword types that count as "identifying" a photo for queue/classifier
    # purposes. Photos with at least one keyword of one of these types drop
    # out of "Needs Identification" and are skipped by the classifier.
    "subject_types": ["taxonomy", "individual", "genre"],
    # --- Display ---
    "browse_card_fields": [
        "filename", "location_status", "rating", "flag", "sharpness"
    ],
    "photos_per_page": 50,
    "thumbnail_size": 400,
    "thumbnail_quality": 85,
    "working_copy_max_size": 4096,
    "working_copy_quality": 92,
    "preview_quality": 90,
    "preview_cache_max_mb": 20480,
    "browse_thumb_default": 220,
    # --- Detection ---
    "detector_confidence": 0.2,
    "detection_padding": 0.2,
    "top_k_predictions": 5,
    "redundancy_threshold": 0.88,
    # --- Culling defaults ---
    "cull_time_window": 60,
    "cull_phash_threshold": 19,
    # --- Pipeline (nested — flows through effective_cfg.get("pipeline")) ---
    "pipeline": {
        # Saved process to run after an import. None = no automatic
        # processing (the "import only" choice); otherwise a saved_processes
        # id. Per-workspace via config_overrides. Global default stays None
        # so a fresh workspace is import-only until the user picks a process.
        "default_process_id": None,
        "w_focus": 0.45,
        "w_exposure": 0.20,
        "w_composition": 0.15,
        "w_area": 0.10,
        "w_noise": 0.10,
        "reject_crop_complete": 0.60,
        "reject_focus": 0.35,
        "reject_clip_high": 0.30,
        "reject_composite": 0.40,
        # Miss detection
        "sam2_variant": "sam2-small",
        "dinov2_variant": "vit-b14",
        "proxy_longest_edge": 1536,
        "miss_enabled": True,
        "miss_det_confidence": 0.20,
        "miss_det_confidence_burst": 0.12,
        "miss_bbox_area_min": 0.005,
        "miss_bbox_area_min_singleton": 0.002,
        "miss_oof_ratio": 0.5,
        # If any stored classifier prediction on a photo's detections has
        # confidence >= this, the photo cannot be flagged no_subject — the
        # classifier saw something even when the detector's confidence was
        # below the workspace `detector_confidence` cutoff. Set to 1.01 to
        # disable the override.
        "miss_classifier_override_conf": 0.8,
        # Contextual weak-detection rescue. The normal detector threshold
        # remains authoritative everywhere else; boxes in this lower band are
        # considered only when a short run is bracketed by strong detections.
        "weak_detection_rescue_enabled": True,
        "weak_detection_confidence": 0.12,
        # Eye-focus detection
        "eye_detect_enabled": False,
        "eye_classifier_conf_gate": 0.50,
        "eye_detection_conf_gate": 0.50,
        "eye_window_k": 0.08,
        "reject_eye_focus": 0.35,
        "burst_time_gap": 3.0,
        "burst_embedding_threshold": 0.40,
        "burst_lambda": 0.85,
        "burst_max_keep": 3,
        "encounter_lambda": 0.70,
        "encounter_max_keep": 5,
        "w_time": 0.35,
        "w_subj": 0.35,
        "w_global": 0.15,
        # Kept in sync with encounters.DEFAULTS["w_species"]; raised from 0.10
        # so a species mismatch resists merging distinct species into one
        # encounter. See the note in encounters.py for the rationale.
        "w_species": 0.40,
        "w_meta": 0.05,
        "tau_enc": 40.0,
        "hard_cut_time": 180.0,
        "hard_cut_score": 0.42,
        "soft_cut_score": 0.52,
        "species_hard_cut_confidence": 0.80,
        "species_hard_cut_margin": 0.60,
        "merge_score": 0.62,
        "merge_max_gap": 60.0,
        "merge_tau": 20.0,
        "extract_full_metadata": True,
    },
    # --- Ingest (import from external source) ---
    "ingest": {
        "folder_template": "%Y/%Y-%m-%d",
        "skip_duplicates": True,
        "file_types": "both",
        "recent_destinations": [],
    },
    "keyboard_shortcuts": {
        "navigation": {
            "import": "",
            "pipeline": "",
            "pipeline_review": "",
            "review": "",
            "cull": "",
            "browse": "",
            "map": "",
            "variants": "",
            "dashboard": "",
            "storage": "",
            "audit": "",
            "id_conflicts": "",
            "workspace": "",
            "shortcuts": "",
            "settings": "",
            "keywords": "",
            "lightroom": "",
        },
        "review": {
            "accept": "a",
            "skip": "s",
        },
        "pipeline_rapid_review": {
            "pick": "p",
            "reject": "x",
            "next": "arrowright",
            "back": "arrowleft",
            "clear": "u",
            "apply": "enter",
            "exit": "escape",
            "zoom": "z",
        },
        "browse": {
            "rate_0": "0",
            "rate_1": "1",
            "rate_2": "2",
            "rate_3": "3",
            "rate_4": "4",
            "rate_5": "5",
            "flag": "p",
            "reject": "x",
            "unflag": "u",
            "undo": "ctrl+z",
            "select_all": "ctrl+a",
            "compare": "c",
            "zoom": "z",
            "toggle_boxes": "b",
            "toggle_ui": "h",
        },
    },
}


def _deep_merge(base, override):
    """Merge override into base recursively so nested dicts are merged, not replaced."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load():
    """Load config, returning defaults for any missing keys."""
    config = copy.deepcopy(DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                config = _deep_merge(config, json.load(f))
        except Exception:
            log.warning("Failed to read config, using defaults")
    return config


def _replace_with_windows_retry(src, dst):
    # On Windows, ``os.replace`` can transiently raise ``PermissionError``
    # ([WinError 5] / [WinError 32]) when Defender or the Search indexer
    # holds the destination open for a moment after a previous write. GitHub's
    # Windows runners can hold temp config files for several seconds, so keep
    # retrying with bounded backoff before giving up.
    if sys.platform != "win32":
        os.replace(src, dst)
        return
    delays = (0.0, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2)
    last_exc = None
    for delay in delays:
        if delay:
            time.sleep(delay)
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            last_exc = exc
    raise last_exc


def save(config):
    """Save config to disk atomically (write to temp file, then replace)."""
    config_dir = os.path.dirname(CONFIG_PATH)
    os.makedirs(config_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=config_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, indent=2)
        _replace_with_windows_retry(tmp_path, CONFIG_PATH)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


def get(key):
    """Get a single config value."""
    return load().get(key, DEFAULTS.get(key))


def set(key, value):
    """Set a single config value (thread-safe)."""
    with _lock:
        config = load()
        config[key] = value
        save(config)


# --- One-time migrations ---------------------------------------------------
#
# When a DEFAULTS value changes, existing installs that have persisted the
# previous default in ``~/.vireo/config.json`` or in a workspace's
# ``config_overrides`` would otherwise keep running with the old value —
# ``load()`` and ``Database.get_effective_config()`` deep-merge persisted
# values on top of DEFAULTS, so a change to DEFAULTS alone is invisible to
# upgraded users. Migrations rewrite the *exact* legacy default value to
# the new one, leaving any user-customized value alone. They run **once**
# per install, gated by an entry in ``_migrations_applied`` in the config
# file, so a user who later explicitly re-saves the legacy value (e.g.
# 25% via the slider) keeps that setting on future loads.

MIGRATION_MISS_THRESHOLDS = "miss_thresholds_2026_05"
MIGRATION_TOGGLE_UI_H_CONFLICT = "toggle_ui_h_conflict_2026_07"
MIGRATION_EYE_DETECT_DEFAULT_OFF = "eye_detect_default_off_2026_07"
MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID = "default_strategy_to_process_id_2026_07"
MIGRATION_BROWSE_LOCATION_STATUS = "browse_location_status_2026_07"
MIGRATION_W_SPECIES_DEFAULT = "w_species_default_2026_07"

_LEGACY_MISS_DET_CONFIDENCE = 0.25
_LEGACY_MISS_DET_CONFIDENCE_BURST = 0.15
_NEW_MISS_DET_CONFIDENCE = 0.20
_NEW_MISS_DET_CONFIDENCE_BURST = 0.12

_LEGACY_W_SPECIES = 0.10
_NEW_W_SPECIES = 0.40


def _read_raw():
    """Return the raw on-disk config (no DEFAULTS merge), or ``{}``."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _migrations_applied(raw):
    applied = raw.get("_migrations_applied")
    return list(applied) if isinstance(applied, list) else []


def migrate_legacy_miss_thresholds(db=None):
    """One-time rewrite of the legacy miss-threshold default pair.

    Rewrites ``pipeline.miss_det_confidence`` / ``...burst`` from the
    previous legacy default pair (0.25 / 0.15) to the new default pair
    (0.20 / 0.12) in both the global config file and every workspace's
    ``config_overrides``. Only the **exact** legacy pair is touched —
    customized values (e.g. 0.30 / 0.18 or a partial pair) are left alone.
    Gated by a marker so it runs at most once per install; a user who
    later explicitly re-saves 25%/15% will keep that setting.
    """
    with _lock:
        raw = _read_raw()
        applied = _migrations_applied(raw)
        if MIGRATION_MISS_THRESHOLDS in applied:
            return False
        rewrote = False
        pipeline = raw.get("pipeline")
        if isinstance(pipeline, dict):
            det = pipeline.get("miss_det_confidence")
            burst = pipeline.get("miss_det_confidence_burst")
            if (
                det == _LEGACY_MISS_DET_CONFIDENCE
                and burst == _LEGACY_MISS_DET_CONFIDENCE_BURST
            ):
                pipeline["miss_det_confidence"] = _NEW_MISS_DET_CONFIDENCE
                pipeline["miss_det_confidence_burst"] = _NEW_MISS_DET_CONFIDENCE_BURST
                rewrote = True
        if db is not None:
            ws_rewrites = db.rewrite_legacy_miss_thresholds_in_workspaces(
                _LEGACY_MISS_DET_CONFIDENCE,
                _LEGACY_MISS_DET_CONFIDENCE_BURST,
                _NEW_MISS_DET_CONFIDENCE,
                _NEW_MISS_DET_CONFIDENCE_BURST,
            )
            if ws_rewrites:
                rewrote = True
        applied.append(MIGRATION_MISS_THRESHOLDS)
        raw["_migrations_applied"] = applied
        save(raw)
        return rewrote


def migrate_toggle_ui_h_conflict():
    """One-time resolution of the browse.toggle_ui="h" default vs. a user
    binding that already uses ``h``.

    ``h`` was newly added as the default binding for the lightbox
    "toggle UI controls" action. For an install that had already customized
    another browse action to ``h`` (for example ``browse.flag: "h"``),
    ``load()`` would deep-merge the new default in and produce two browse
    bindings on the same key. The lightbox key handler checks ``toggle_ui``
    before flag/reject/unflag, so ``h`` would silently start toggling the
    chrome instead of firing the user's existing binding.

    This migration inspects the raw on-disk config. If the user has a
    ``keyboard_shortcuts.browse`` block that already binds ``h`` to some
    other action AND does NOT already have an explicit ``toggle_ui``
    setting, it writes ``browse.toggle_ui: ""`` to disk so the effective
    config leaves the user's existing binding alone. Users can pick a
    non-conflicting key from the shortcuts editor to re-enable it.
    Gated by a marker so it runs at most once per install; a user who
    later explicitly rebinds ``toggle_ui`` keeps that setting.
    """
    with _lock:
        raw = _read_raw()
        applied = _migrations_applied(raw)
        if MIGRATION_TOGGLE_UI_H_CONFLICT in applied:
            return False
        rewrote = False
        shortcuts = raw.get("keyboard_shortcuts")
        if isinstance(shortcuts, dict):
            browse = shortcuts.get("browse")
            if isinstance(browse, dict) and "toggle_ui" not in browse:
                for action, key in browse.items():
                    if action == "toggle_ui":
                        continue
                    if isinstance(key, str) and key.lower() == "h":
                        browse["toggle_ui"] = ""
                        rewrote = True
                        break
        applied.append(MIGRATION_TOGGLE_UI_H_CONFLICT)
        raw["_migrations_applied"] = applied
        save(raw)
        return rewrote


def migrate_browse_location_status_field():
    """Add coordinate source to the exact legacy default Browse card fields.

    Customized card-field lists stay untouched. This makes the newly explicit
    GPS/assigned/none state visible after upgrade for users who were still on
    the previous default rather than only for fresh installs.
    """
    legacy_default = ["filename", "rating", "flag", "sharpness"]
    new_default = [
        "filename", "location_status", "rating", "flag", "sharpness"
    ]
    with _lock:
        raw = _read_raw()
        applied = _migrations_applied(raw)
        if MIGRATION_BROWSE_LOCATION_STATUS in applied:
            return False
        rewrote = raw.get("browse_card_fields") == legacy_default
        if rewrote:
            raw["browse_card_fields"] = new_default
        applied.append(MIGRATION_BROWSE_LOCATION_STATUS)
        raw["_migrations_applied"] = applied
        save(raw)
        return rewrote


def migrate_eye_detect_default_off(db=None):
    """One-time rewrite of the previous eye-detection default.

    Older versions defaulted ``pipeline.eye_detect_enabled`` to ``True``.
    Because legacy save paths may have persisted the full merged config,
    upgraded users would otherwise keep the old default even after DEFAULTS
    changes. Rewrite only the exact legacy default value; an already-false
    user setting remains false, and future explicit true settings are kept.
    """
    with _lock:
        raw = _read_raw()
        applied = _migrations_applied(raw)
        if MIGRATION_EYE_DETECT_DEFAULT_OFF in applied:
            return False
        rewrote = False
        pipeline = raw.get("pipeline")
        global_was_true = (
            isinstance(pipeline, dict) and pipeline.get("eye_detect_enabled") is True
        )
        # An absent ``pipeline.eye_detect_enabled`` key means the config was
        # relying on the previous DEFAULTS value, which was ``True`` — i.e.
        # the same effective pre-migration state as an explicit ``True``.
        # Only an explicit ``False`` in the raw config means the user (or
        # DEFAULTS-persisting save code) had already committed to eye-off
        # before this migration ran.
        global_was_explicit_false = (
            isinstance(pipeline, dict) and pipeline.get("eye_detect_enabled") is False
        )
        if global_was_true:
            pipeline["eye_detect_enabled"] = False
            rewrote = True
        if db is not None:
            # Only rewrite workspace ``eye_detect_enabled=True`` overrides
            # when the global default they were mirroring was also True (or
            # absent — the old DEFAULTS value). If the global was already
            # explicit False, a workspace ``True`` override is an
            # intentional per-workspace opt-in that intentionally differs
            # from Settings; flipping it here would silently disable eye
            # detection for that workspace after the upgrade.
            if not global_was_explicit_false:
                ws_rewrites = db.rewrite_legacy_eye_detect_default_in_workspaces()
                if ws_rewrites:
                    rewrote = True
            # A workspace's cached ``pipeline_results_ws*.json`` and stamped
            # ``last_group_fingerprint`` were produced with whatever
            # ``eye_detect_enabled`` was effective at the time. Flipping the
            # default changes scoring/KEEP/REJECT decisions, but
            # ``compute_group_fingerprint`` only reads encounter/burst
            # settings — so without invalidation the Process page keeps
            # reporting the prior cache as fresh. Any global state other than
            # explicit ``False`` means workspaces without their own explicit
            # ``False`` override were scoring with eye detection on (either
            # via an explicit ``True`` global or the old ``True`` DEFAULTS
            # value); clear their fingerprints so the plan reports Group &
            # Score as needing to re-run.
            if not global_was_explicit_false:
                db.invalidate_group_fingerprints_without_explicit_eye_false()
        applied.append(MIGRATION_EYE_DETECT_DEFAULT_OFF)
        raw["_migrations_applied"] = applied
        save(raw)
        return rewrote


def migrate_default_strategy_to_process_id(db):
    """One-time rewrite of the legacy global ``pipeline.default_strategy``
    (a hardcoded strategy name) to ``pipeline.default_process_id`` (a
    ``saved_processes.id``).

    The workspace-side rewrite runs inside :class:`db.Database` at handle
    creation (guarded by ``db_meta``). This function does the corresponding
    rewrite for the *global* ``~/.vireo/config.json``: without it, an
    upgraded install that had a global after-import default set via the old
    ``pipeline.default_strategy`` silently falls back to import-only,
    because the import endpoints and ``get_effective_config`` now read only
    ``pipeline.default_process_id``. Workspaces that inherit the global
    setting (i.e. have no per-workspace override) would stop auto-processing
    on import until the user re-picks a default in Settings.

    Unknown/removed legacy names map to null (import only). Gated by
    ``_migrations_applied`` so it runs at most once per install; a user who
    later manually adds ``default_strategy`` back is not silently rewritten.
    """
    import sqlite3

    import process_strategies as ps
    with _lock:
        raw = _read_raw()
        applied = _migrations_applied(raw)
        if MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID in applied:
            return False
        pipeline = raw.get("pipeline")
        if not (isinstance(pipeline, dict) and "default_strategy" in pipeline):
            # Nothing legacy to migrate — stamp so we don't re-check every
            # boot. No saved_processes access needed on this (possibly
            # schema-uninitialized) connection.
            applied.append(MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID)
            raw["_migrations_applied"] = applied
            save(raw)
            return False
        # A legacy key is present and must be resolved against the seeded
        # saved_processes table. create_app opens its startup ``init_db`` with
        # ``initialize_schema=False``, so on the first boot after an upgrade
        # this can run before any schema-initializing handle created/seeded
        # the table. Defer WITHOUT stamping (and without dropping the legacy
        # key) so a later boot completes the migration once a request-path
        # Database has seeded the table — instead of crashing startup with
        # "no such table: saved_processes".
        try:
            seeded = db.conn.execute(
                "SELECT id FROM saved_processes WHERE name = ?",
                (ps.DEFAULT_SEED_NAME,),
            ).fetchone()
        except sqlite3.OperationalError:
            return False
        if seeded is None:
            return False
        old = pipeline.pop("default_strategy")
        seed_name = (
            ps.LEGACY_STRATEGY_NAMES.get(old)
            if isinstance(old, str) else None
        )
        pid = None
        if seed_name is not None:
            rows = db.conn.execute(
                "SELECT id FROM saved_processes WHERE name = ?",
                (seed_name,),
            ).fetchall()
            if rows:
                pid = rows[0]["id"]
        if pid is not None:
            pipeline["default_process_id"] = pid
        applied.append(MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID)
        raw["_migrations_applied"] = applied
        save(raw)
        return True


def migrate_legacy_w_species_default(db=None):
    """One-time rewrite of the previous ``pipeline.w_species`` default (0.10)
    to the new default (0.40) in both the global config file and every
    workspace's ``config_overrides``.

    Without this migration, an upgraded install that had the pipeline block
    persisted verbatim keeps ``w_species: 0.10`` and its encounter grouping
    stays on the old species weight — the intended split-mismatched-species
    behavior only reaches fresh configs. Only the *exact* legacy value is
    rewritten; a user who has explicitly tuned ``w_species`` (e.g. 0.15,
    0.25) is left alone. Gated by a marker so it runs at most once per
    install; a user who later re-saves 0.10 via the algorithm slider keeps
    that setting on future loads.

    The effective ``compute_group_fingerprint`` includes ``w_species``, so
    any workspace whose value actually changes here will naturally show as
    outdated on the Process page — no explicit fingerprint invalidation
    needed.
    """
    with _lock:
        raw = _read_raw()
        applied = _migrations_applied(raw)
        if MIGRATION_W_SPECIES_DEFAULT in applied:
            return False
        rewrote = False
        pipeline = raw.get("pipeline")
        if isinstance(pipeline, dict) and pipeline.get("w_species") == _LEGACY_W_SPECIES:
            pipeline["w_species"] = _NEW_W_SPECIES
            rewrote = True
        if db is not None:
            ws_rewrites = db.rewrite_legacy_w_species_default_in_workspaces(
                _LEGACY_W_SPECIES,
                _NEW_W_SPECIES,
            )
            if ws_rewrites:
                rewrote = True
        applied.append(MIGRATION_W_SPECIES_DEFAULT)
        raw["_migrations_applied"] = applied
        save(raw)
        return rewrote


def get_editors():
    """Return the configured external editors as a list of {name, path} dicts.

    Source of truth is ``external_editors``. If that's empty and the legacy
    ``external_editor`` string is set, a one-element list is synthesized
    (no on-disk migration — the file is left alone). Malformed entries
    (missing path, non-string fields) are filtered out so callers can trust
    the shape.
    """
    config = load()
    raw = config.get("external_editors") or []
    editors = []
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            path = entry.get("path")
            if not isinstance(path, str) or not path.strip():
                continue
            name = entry.get("name")
            if not isinstance(name, str) or not name.strip():
                name = os.path.basename(path.rstrip("/")) or "Editor"
            editors.append({"name": name.strip(), "path": path.strip()})
    if not editors:
        legacy = config.get("external_editor")
        if isinstance(legacy, str) and legacy.strip():
            editors.append({"name": "Editor", "path": legacy.strip()})
    return editors


def _coerce_remote_target(entry):
    """Validate/normalize one remote-target dict, or return None if unusable.

    A usable target needs at least a host, user, and an *absolute POSIX*
    remote_path (the rest have sane fallbacks). A relative remote_path like
    "Photos" would be sent unchanged to ``user@host:Photos/<folder>`` —
    rsync and the checksum verify would then operate under the SSH user's
    remote cwd, while the catalog gets repointed to the absolute local
    mount_path. The original source would be deleted on a verified copy
    that lives at a different remote location than the path Vireo records,
    so reject relative remote paths at the entry boundary rather than
    after a move is already in flight. ``mount_path`` may be empty — a
    target with no local mount simply can't keep its photos catalogued
    after a move (the move route enforces that where it matters), but the
    entry is still valid for transfer. Numeric fields are coerced; junk
    falls back to defaults.
    """
    if not isinstance(entry, dict):
        return None
    host = (entry.get("host") or "").strip()
    user = (entry.get("user") or "").strip()
    remote_path = (entry.get("remote_path") or "").strip()
    if not host or not user or not remote_path:
        return None
    # POSIX-absolute only: rsync ships this path to the NAS-side shell, so
    # Windows drive forms / backslashes are also non-starters (the NAS is
    # POSIX). Comparing to "/" handles every absolute form that survives a
    # POSIX shell intact.
    if not remote_path.startswith("/"):
        return None
    name = (entry.get("name") or "").strip() or f"{user}@{host}"
    try:
        port = int(entry.get("port") or 22)
    except (TypeError, ValueError):
        port = 22
    try:
        bwlimit = int(entry.get("bwlimit_kbps") or 0)
    except (TypeError, ValueError):
        bwlimit = 0
    tid = (entry.get("id") or "").strip()
    if not tid:
        # Stable-ish id derived from the connection tuple so the UI can key
        # rows even for legacy entries saved before ids existed.
        tid = f"{user}@{host}:{remote_path}"
    mount_path = (entry.get("mount_path") or "").strip()
    # Local directory that mirrors remote_path for chained import→process→
    # move runs. Empty = target never offers the chained move. Must be an
    # absolute local path and must not live inside mount_path (the mount is
    # the *destination* view of the NAS; the archive root is the local
    # staging side — pointing it at the mount would "move" files onto
    # themselves). Invalid values are blanked rather than dropping the
    # whole target.
    local_archive_root = (entry.get("local_archive_root") or "").strip()
    if local_archive_root:
        if not os.path.isabs(local_archive_root):
            local_archive_root = ""
        elif mount_path and os.path.isabs(mount_path):
            # A relative mount_path would realpath against the server's
            # CWD here, making this containment check depend on where the
            # server happened to be launched — it could blank a perfectly
            # valid archive root. Relative mounts are unusable for
            # transfers anyway, so skip the check instead of resolving.
            #
            # Containment goes through move._path_equal_or_descends so
            # case-only aliases on case-insensitive volumes (default macOS
            # APFS, Windows NTFS: "/Volumes/Photos" vs "/volumes/photos")
            # are recognized as the same directory. A byte-wise
            # commonpath would miss this and leave a target eligible for
            # chained moves whose archive root is really the mount, and
            # the accepted chain would later fail as a source/destination
            # overlap. The same helper the move guards use is authoritative.
            try:
                from move import _path_equal_or_descends
                if _path_equal_or_descends(local_archive_root, mount_path):
                    local_archive_root = ""
            except (OSError, ValueError):
                # Different drives on Windows / unreadable realpath: cannot
                # be inside. Blanking the archive root here would drop a
                # perfectly valid config on a transient FS hiccup, so leave
                # it as saved and let the runtime move guards catch a real
                # overlap.
                pass
    return {
        "id": tid,
        "name": name,
        "host": host,
        "user": user,
        "port": port,
        "ssh_key": (entry.get("ssh_key") or "").strip(),
        "remote_path": remote_path,
        "mount_path": mount_path,
        "bwlimit_kbps": max(0, bwlimit),
        "local_archive_root": local_archive_root,
    }


def get_remote_targets():
    """Return configured remote (SSH) move targets, validated/normalized.

    Malformed entries (missing host/user/remote_path) are dropped so callers
    can trust the shape. See DEFAULTS["remote_targets"] for the field set.
    """
    raw = load().get("remote_targets") or []
    if not isinstance(raw, list):
        return []
    targets = []
    for entry in raw:
        coerced = _coerce_remote_target(entry)
        if coerced is not None:
            targets.append(coerced)
    return targets


def get_remote_target(target_id):
    """Return the validated remote target with the given id, or None."""
    if not target_id:
        return None
    for t in get_remote_targets():
        if t["id"] == target_id:
            return t
    return None
