"""User configuration for Vireo (persisted to ~/.vireo/config.json)."""

import contextlib
import copy
import filecmp
import json
import logging
import os
import shutil
import tempfile
import threading

from file_replace import replace_file
from filter_shortcuts import DEFAULT_SHORTCUTS

log = logging.getLogger(__name__)

CONFIG_PATH = os.path.expanduser("~/.vireo/config.json")

_lock = threading.Lock()

# Serializes read-modify-write of config.json and the active workspace's
# config_overrides across the schema-driven settings endpoints (PATCH/DELETE/
# import) and every other route that rewrites a settings block. Without it,
# with per-field autosave and ``app.run(threaded=True)``, two concurrent
# requests can read the same snapshot and the later writer drops the earlier
# change. config.json is process-global, so one module-level lock covers every
# app instance. Distinct from ``_lock`` (held inside ``set`` and the
# migrations) so a holder of this lock can still call those.
settings_write_lock = threading.Lock()

DEFAULTS = {
    "move_keep_visible_in_other_workspaces": True,
    "classification_threshold": 0.4,
    # Per-model floor on the raw, pre-softmax match score, below which the best
    # label in the list is reported as not matching the image at all. Shaped
    # ``{model_name: {"threshold": float, "score_kind": "cosine"|"logit"}}``.
    #
    # Empty by default, and deliberately so: a cosine floor and a logit floor
    # are different scales, and neither can be guessed from the model name. An
    # unconfigured model records its scores and declines to judge them, which
    # is the honest state. Derive a real floor from your own confirmed
    # identifications with ``scripts/calibrate_match_threshold.py``.
    "match_thresholds": {},
    "grouping_window_seconds": 10,
    "similarity_threshold": 0.85,
    "preview_max_size": 1920,
    "keyword_case": "auto",
    "sync_flags_to_xmp": True,
    "write_assigned_location_to_xmp": False,
    "write_location_keywords_to_xmp": False,
    "max_edit_history": 1000,
    "inat_token": "",
    "hf_token": "",
    "google_maps_api_key": "",
    "google_maps_prefer_english": True,
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
    # Saved export-dialog presets. Each entry: {"name": str, "settings": dict}
    # where settings snapshots the full export dialog (destination,
    # export_to_subfolder, subfolder_name, reveal_after_export, format,
    # max_size, quality, naming_template, metadata_fields). Written only by
    # the /api/export/presets endpoints, which validate via
    # export.normalize_export_preset_settings. Custom UI (the export modal),
    # so excluded from SCHEMA like remote_targets.
    "export_presets": [],
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
    # Buttons in the always-visible quick-filter row of the universal filter
    # bar, in render order. Each entry is {"id", "label", "group", "rules"}
    # where `rules` is an ordinary filter-rule node; `filter_shortcuts.py`
    # owns the shape, the defaults (the row the bar shipped with), and the
    # validation. Custom UI in Settings, so it stays out of SCHEMA like
    # external_editors. An explicit empty list means "no quick filters".
    "filter_shortcuts": copy.deepcopy(DEFAULT_SHORTCUTS),
    "photos_per_page": 50,
    "thumbnail_size": 400,
    "thumbnail_quality": 85,
    "working_copy_max_size": 4096,
    "working_copy_quality": 92,
    "working_copy_cache_max_mb": 20480,
    "preview_quality": 90,
    "preview_cache_max_mb": 20480,
    "browse_thumb_default": 220,
    # --- Browse stacks ---
    # Browse's Stacks toggle collapses exact duplicates and camera bursts.
    # A burst is a run of frames from one folder whose consecutive capture
    # times are no further apart than ``browse_stack_time_gap`` seconds and
    # which carry the same species and location keywords. Keyword matching
    # is deliberate: an untagged frame inside a tagged run breaks out as its
    # own item so the gap in tagging stays visible instead of hiding behind
    # a tagged cover.
    "browse_stack_time_gap": 3.0,
    # How a keyword change inside a time run is resolved.
    #   "break"     - start a new stack at every change, so an A/B/A run
    #                 yields three stacks in shooting order.
    #   "partition" - one stack per distinct keyword set in the run, so the
    #                 same A/B/A run yields two.
    "browse_stack_split_mode": "break",
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


def _preserve_corrupt_config():
    """Copy an unreadable config file to ``<path>.corrupt`` before callers
    fall back to defaults — the next ``save()`` overwrites the original,
    which would otherwise silently destroy whatever the user had."""
    backup = CONFIG_PATH + ".corrupt"
    try:
        # Skip only when the current corrupt file is byte-for-byte the same
        # as the existing backup. mtime comparison isn't safe here: a
        # restored-from-history file, a coarse-resolution filesystem, or an
        # editor that resets mtime can leave the current corruption with an
        # equal-or-older timestamp than an unrelated older backup, and the
        # current bytes would then be discarded.
        if os.path.exists(backup) and filecmp.cmp(backup, CONFIG_PATH, shallow=False):
            return
        shutil.copy2(CONFIG_PATH, backup)
        log.warning("Config file is unreadable; preserved a copy at %s", backup)
    except OSError:
        log.warning("Could not back up the unreadable config file to %s", backup, exc_info=True)


def load():
    """Load config, returning defaults for any missing keys."""
    config = copy.deepcopy(DEFAULTS)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                config = _deep_merge(config, json.load(f))
        except Exception:
            # Broad on purpose: config.load() runs on nearly every request and
            # at startup, and a corrupt file must fall back to defaults rather
            # than take the app down (the original is preserved first).
            log.warning("Failed to read config, using defaults", exc_info=True)
            _preserve_corrupt_config()
    return _repair_types(config)


def _repair_types(config):
    """Fall back to the default for any stored number or bool of the wrong type.

    See ``config_schema.repair_types``: a ``null`` or ``"abc"`` stored by an
    older, unvalidated write path would otherwise fail every reader that
    does arithmetic on it, until the file was edited by hand.
    """
    import config_schema

    return config_schema.repair_types(config, DEFAULTS)


def load_strict():
    """Load config the same as :func:`load`, but raise on read failure.

    ``load()`` catches parse/IO errors and returns ``DEFAULTS`` so callers
    that only need a value can keep going. That silent fallback is unsafe
    for destructive cleanup that is gated on a setting: an off-by-default
    key would come back False from a corrupt config, and a caller reading
    it as an explicit off would strip previously-written metadata and
    clear the pending row -- a later config repair would not requeue
    anything. Those callers use ``load_strict`` and treat the exception
    as "unknown; leave state alone."
    """
    config = copy.deepcopy(DEFAULTS)
    if not os.path.exists(CONFIG_PATH):
        return config
    try:
        with open(CONFIG_PATH) as f:
            data = json.load(f)
    except Exception:
        _preserve_corrupt_config()
        raise
    return _repair_types(_deep_merge(config, data))


def save(config):
    """Save config to disk atomically (write to temp file, then replace)."""
    config_dir = os.path.dirname(CONFIG_PATH)
    os.makedirs(config_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=config_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, indent=2)
        replace_file(tmp_path, CONFIG_PATH)
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


def _read_raw():
    """Return the raw on-disk config (no DEFAULTS merge), or ``{}``."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        # Write paths save() right after this read, so an unpreserved
        # corrupt file would be clobbered with near-defaults.
        _preserve_corrupt_config()
        return {}
    if not isinstance(raw, dict):
        _preserve_corrupt_config()
        return {}
    return raw


def read_raw_config_file():
    """Return the parsed contents of config.json, or ``{}``.

    Unlike ``load()``, this does NOT merge DEFAULTS — so it contains only the
    keys the user has actually set. Write paths (under
    ``settings_write_lock``) use it so the on-disk file stays minimal.

    Preserves a ``.corrupt`` backup on unreadable/non-dict content before
    returning ``{}`` — otherwise the very next PATCH/DELETE via the
    schema-driven settings routes would call ``save()`` on the empty dict
    and silently overwrite whatever the user had.
    """
    return _read_raw()


# Config migrations rewrite a persisted legacy default in
# ``~/.vireo/config.json`` once per install, gated by an entry in the file's
# ``_migrations_applied`` list (settings export omits it; import keeps this
# install's entries). Every past migration has been applied and retired.
def _migrations_applied(raw):
    applied = raw.get("_migrations_applied")
    return list(applied) if isinstance(applied, list) else []


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


def _coerce_remote_target(entry, *, check_filesystem=True):
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
                if check_filesystem:
                    from move import _path_equal_or_descends
                    overlaps = _path_equal_or_descends(local_archive_root, mount_path)
                else:
                    # Reading saved settings must never stat an offline share.
                    # Save/transfer validation still resolves filesystem aliases.
                    archive = os.path.normcase(os.path.normpath(local_archive_root))
                    mount = os.path.normcase(os.path.normpath(mount_path))
                    overlaps = os.path.commonpath((archive, mount)) == mount
                if overlaps:
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
        coerced = _coerce_remote_target(entry, check_filesystem=False)
        if coerced is not None:
            targets.append(coerced)
    return targets


def get_export_presets():
    """Return saved export presets with a trusted shape, sorted by name.

    Entries with a malformed name or settings payload are dropped so callers
    can rely on {"name": str, "settings": dict}, mirroring
    get_remote_targets(). Settings contents were validated on write.
    """
    raw = load().get("export_presets") or []
    if not isinstance(raw, list):
        return []
    presets = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        settings = entry.get("settings")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(settings, dict):
            continue
        presets.append({"name": name, "settings": settings})
    presets.sort(key=lambda preset: preset["name"].casefold())
    return presets


def get_remote_target(target_id):
    """Return the validated remote target with the given id, or None."""
    if not target_id:
        return None
    for t in get_remote_targets():
        if t["id"] == target_id:
            return t
    return None
