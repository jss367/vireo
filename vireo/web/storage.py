"""Storage: the Storage page's disk-usage report and cache housekeeping.

``/api/storage`` measures every Vireo-managed location (catalog, generated
caches, working copies, offline originals, masks, models, Hugging Face cache)
and the volumes backing them. The ``/api/storage/*`` routes list, delete and
clear regenerable cache files, reclaim SAM mask variants and stale masks, and
open a storage folder in the platform file browser.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys

from db import Database
from flask import Blueprint, jsonify, request
from proc import no_window_kwargs
from web.background_jobs import make_background_job
from working_copy_cache import working_copy_stats

log = logging.getLogger(__name__)


def create_storage_blueprint(get_db, json_error, db_path, config, *, get_runner):
    """Build the storage blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``,
    ``CARD_CLEANUP_DIR``, ``COMPUTATION_CACHE_DIR``), read when a request runs
    rather than when the app is built.
    """
    blueprint = Blueprint("storage", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    def _retained_config():
        import config as cfg
        return {**cfg.load(), "THUMB_CACHE_DIR": config["THUMB_CACHE_DIR"],
                "COMPUTATION_CACHE_DIR": config["COMPUTATION_CACHE_DIR"]}

    @blueprint.route("/api/storage/retention", methods=["POST"])
    @background_job
    def api_storage_retention(ctx):
        import config as cfg
        import config_schema as schema
        from services.retained_cache import cleanup

        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("Expected a JSON object")
        keys = ("retained_cache_max_mb", "retained_cache_max_age_days", "grouping_history_max_mb")
        try:
            values = {key: schema.validate_value(key, body.get(key)) for key in keys}
        except schema.ValidationError as exc:
            return json_error(str(exc))
        with cfg.settings_write_lock:
            raw = cfg.read_raw_config_file()
            raw.update(values)
            cfg.save(raw)
        settings = _retained_config()

        def work(job):
            with ctx.thread_db() as db:
                result = cleanup(settings, db.storage.source_folder_paths(),
                                 checkpoint=lambda: ctx.checkpoint(job))
                for workspace in db.workspaces.list_all():
                    ctx.checkpoint(job)
                    db.set_active_workspace(workspace["id"])
                    retired = db._prune_edit_history()
                    for key, count in retired.items():
                        result[key] = result.get(key, 0) + count
                return result
        return ctx.start("retained-cache-maintenance", work, config=values)

    @blueprint.route("/api/storage/retained")
    def api_storage_retained():
        from services.retained_cache import summary
        settings = _retained_config()
        return jsonify({
            "caches": summary(settings),
            "grouping_history": get_db().storage.grouping_history(),
            "maintenance": get_db().storage.latest_maintenance(),
            "policy": {key: settings[key] for key in (
                "retained_cache_max_mb", "retained_cache_max_age_days", "grouping_history_max_mb",
            )},
        })

    @blueprint.route("/api/storage/retained/clear", methods=["POST"])
    @background_job
    def api_storage_retained_clear(ctx):
        from services.retained_cache import FAMILIES, cleanup
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("Expected a JSON object")
        family = body.get("type")
        if not isinstance(family, str) or (family not in FAMILIES and family != "grouping-history"):
            return json_error("Unknown retained cache type")
        if family == "grouping-history" and body.get("confirm_history_loss") is not True:
            return json_error("Confirm that grouping undo and redo snapshots will be removed")
        settings = _retained_config()

        def work(job):
            with ctx.thread_db() as db:
                if family == "grouping-history":
                    db.begin_immediate()
                    ids = db.storage.grouping_payload_ids()
                    for edit_id in ids:
                        ctx.checkpoint(job)
                        db.edit_history.retire_stale_grouping_entry(edit_id)
                    db.commit()
                    return {"deleted": len(ids), "summary":
                            f"Removed {len(ids)} grouping undo and redo snapshots; photo edits were retained."}

                def progress(current, total, path):
                    ctx.runner.push_event(job["id"], "progress", {
                        "current": current, "total": total,
                        "current_file": os.path.basename(path), "phase": "Removing retained generated files",
                    })
                result = cleanup(settings, db.storage.source_folder_paths(), family=family,
                                 clear=True, checkpoint=lambda: ctx.checkpoint(job), progress=progress)
                return result

        return ctx.start("retained-cache-cleanup", work, config={"cache_type": family})

    def _storage_masks_data(db):
        """Build the shared mask-storage payload once per storage refresh."""
        import config as cfg

        # This is a global view, so use the most permissive confidence floor
        # across workspaces. The stale set must not change with active workspace.
        min_detector_conf = db.min_detector_confidence_across_workspaces(
            cfg.load()
        )
        variants = db.masks_features.variants_summary()
        stale = db.masks_features.find_stale(detector_confidence=min_detector_conf)
        return {
            "variants": variants,
            "total_bytes": sum(v["bytes"] for v in variants),
            "stale_count": len(stale),
            "path": os.path.join(os.path.dirname(db_path), "masks"),
        }

    @blueprint.route("/api/storage")
    def api_storage():
        """Comprehensive storage info for the storage management panel."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR
        from models import DEFAULT_MODELS_DIR
        from storage_breakdown import group_auxiliary_storage

        def _dir_stats(path):
            count = 0
            total = 0
            if os.path.isdir(path):
                for f in os.listdir(path):
                    fp = os.path.join(path, f)
                    if os.path.isfile(fp):
                        count += 1
                        total += os.path.getsize(fp)
            return {"count": count, "size": total, "path": path}

        from services.storage_paths import (
            auxiliary_paths as owned_auxiliary_paths,
        )
        from services.storage_paths import (
            directory_bytes as _dir_size_recursive,
        )
        from services.storage_paths import (
            huggingface_cache_dir,
            huggingface_repositories,
        )

        db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        thumb = _dir_stats(config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(
            os.path.dirname(config["THUMB_CACHE_DIR"]), "previews"
        )
        preview = _dir_stats(preview_dir)
        paired = _dir_stats(os.path.join(preview_dir, "paired"))
        preview["count"] += paired["count"]
        preview["size"] += paired["size"]
        working = working_copy_stats(
            os.path.dirname(config["THUMB_CACHE_DIR"])
        )
        emb = _dir_stats(EMB_CACHE_DIR)
        models_size = _dir_size_recursive(DEFAULT_MODELS_DIR)
        db = get_db()
        offline_size = db.caches.offline_original_total_bytes()
        offline_count = db.caches.offline_original_cached_count()
        masks = _storage_masks_data(db)
        masks_size = masks["total_bytes"]
        storage_root = os.path.dirname(config["THUMB_CACHE_DIR"])

        # HuggingFace cache — only count Vireo-relevant models
        hf_cache = huggingface_cache_dir()
        hf_size = 0
        hf_models = []
        hf_repositories = huggingface_repositories()
        if os.path.isdir(hf_cache):
            for d in os.listdir(hf_cache):
                if d in hf_repositories or d.startswith((
                    "models--imageomics--", "models--bioclip--",
                )):
                    dp = os.path.join(hf_cache, d)
                    size = _dir_size_recursive(dp)
                    hf_size += size
                    hf_models.append(
                        {
                            "name": d.replace("models--", "").replace("--", "/"),
                            "size": size,
                        }
                    )

        def _is_within(path, root):
            try:
                return os.path.commonpath(
                    [os.path.abspath(path), os.path.abspath(root)]
                ) == os.path.abspath(root)
            except (OSError, ValueError):
                return False

        named_storage = [
            (db_path, db_size),
            (thumb["path"], thumb["size"]),
            (preview["path"], preview["size"]),
            (working["path"], working["size"]),
            (EMB_CACHE_DIR, emb["size"]),
            (DEFAULT_MODELS_DIR, models_size),
            (os.path.join(storage_root, "offline"), offline_size),
            (masks["path"], masks_size),
            (hf_cache, hf_size),
        ]
        auxiliary_paths = owned_auxiliary_paths(
            db_path, config["THUMB_CACHE_DIR"], config, db,
        )
        other_entries = []
        measured_paths = []
        # Measure each top-level auxiliary path once, then remove the bytes
        # already represented by a named category. This also exposes files
        # nested in cache folders that their flat/DB-backed counters omit.
        for path in sorted(set(map(os.path.abspath, auxiliary_paths)), key=len):
            if any(_is_within(path, parent) for parent in measured_paths):
                continue
            # As with os.walk(storage_root), do not follow directory links
            # into unrelated data or back into an already-counted cache.
            if os.path.islink(path) and os.path.isdir(path):
                continue
            measured_paths.append(path)
            try:
                size = (
                    os.path.getsize(path) if os.path.isfile(path)
                    else _dir_size_recursive(path)
                )
            except OSError:
                continue
            accounted = sum(
                named_size for named_path, named_size in named_storage
                if _is_within(named_path, path)
            )
            remaining = max(0, size - accounted)
            if remaining:
                other_entries.append({"path": path, "size": remaining})
        other_categories = group_auxiliary_storage(other_entries, os.path.abspath(db_path))
        other_size = sum(category["size"] for category in other_categories)
        total = sum(size for _path, size in named_storage) + other_size
        reclaimable = thumb["size"] + preview["size"] + emb["size"]

        def _volume_for_path(path):
            usage_path = os.path.abspath(path)
            while not os.path.exists(usage_path):
                parent = os.path.dirname(usage_path)
                if parent == usage_path:
                    break
                usage_path = parent
            try:
                usage = shutil.disk_usage(usage_path)
                free_bytes = usage.free
                capacity_bytes = usage.total
            except OSError:
                free_bytes = None
                capacity_bytes = None

            mount_path = usage_path
            while not os.path.ismount(mount_path):
                parent = os.path.dirname(mount_path)
                if parent == mount_path:
                    break
                mount_path = parent
            if sys.platform.startswith("win"):
                name = os.path.splitdrive(mount_path)[0] or mount_path
            elif mount_path == os.path.sep:
                name = "System volume"
            else:
                name = os.path.basename(mount_path.rstrip(os.path.sep)) or mount_path
            return {
                "name": name,
                "mount_path": mount_path,
                "free": free_bytes,
                "capacity": capacity_bytes,
            }

        raw_locations = [
            ("catalog", "Catalog and masks", os.path.dirname(db_path)),
            (
                "generated",
                "Thumbnails, previews, working copies, and offline originals",
                storage_root,
            ),
        ]
        if emb["size"]:
            raw_locations.append(("embeddings", "Label embeddings", EMB_CACHE_DIR))
        if models_size:
            raw_locations.append(("models", "Downloaded models", DEFAULT_MODELS_DIR))
        if hf_size:
            raw_locations.append(("hf_cache", "Hugging Face cache", hf_cache))
        from services.storage_paths import location_id as managed_location_id
        for entry in other_entries:
            path = entry["path"] if os.path.isdir(entry["path"]) else os.path.dirname(entry["path"])
            if not any(_is_within(path, existing) for _id, _label, existing in raw_locations):
                raw_locations.append((managed_location_id(path), "Additional managed storage", path))

        locations_by_path = {}
        for location_id, label, path in raw_locations:
            normalized = os.path.abspath(path)
            if normalized not in locations_by_path:
                locations_by_path[normalized] = {
                    "id": location_id,
                    "path": normalized,
                    "labels": [],
                    "volume": _volume_for_path(normalized),
                }
            locations_by_path[normalized]["labels"].append(label)
        locations = list(locations_by_path.values())

        volumes_by_mount = {}
        for location in locations:
            volume = location["volume"]
            mount = volume["mount_path"]
            if mount not in volumes_by_mount:
                volumes_by_mount[mount] = dict(volume)
                volumes_by_mount[mount]["location_ids"] = []
            volumes_by_mount[mount]["location_ids"].append(location["id"])

        return jsonify(
            {
                "total": total,
                "reclaimable": reclaimable,
                "storage_root": storage_root,
                "locations": locations,
                "volumes": list(volumes_by_mount.values()),
                "database": {"size": db_size, "path": db_path},
                "thumbnails": thumb,
                "previews": preview,
                "working_copies": working,
                "other": {
                    "size": other_size, "path": storage_root,
                    "categories": other_categories,
                },
                "embeddings": emb,
                "models": {"size": models_size, "path": DEFAULT_MODELS_DIR},
                "hf_cache": {"size": hf_size, "path": hf_cache, "models": hf_models},
                "offline_originals": {
                    "count": offline_count,
                    "size": offline_size,
                    "path": os.path.join(os.path.dirname(config["THUMB_CACHE_DIR"]), "offline"),
                },
                "masks": {"size": masks_size, **masks},
            }
        )

    @blueprint.route("/api/storage/masks")
    def api_storage_masks():
        """Per-variant SAM mask summary (counts, bytes, active counts)
        plus stale-mask count for the storage dashboard."""
        return jsonify(_storage_masks_data(get_db()))

    @blueprint.route("/api/storage/masks/delete-variant", methods=["POST"])
    def api_storage_masks_delete_variant():
        """Delete all photo_masks rows + files for the given variant.
        Returns 400 when the variant is currently active for any photo.
        """
        body = request.get_json(silent=True) or {}
        variant = body.get("variant", "")
        if not variant:
            return json_error("variant required")
        db = get_db()
        try:
            n = db.masks_features.delete_for_variant(variant)
        except ValueError as e:
            return json_error(str(e), 400)
        log.info("Deleted %d masks for variant %s", n, variant)
        return jsonify({"ok": True, "deleted": n})

    @blueprint.route("/api/storage/masks/delete-inactive", methods=["POST"])
    def api_storage_masks_delete_inactive():
        """Delete all non-active variant masks across all photos."""
        db = get_db()
        n = db.masks_features.delete_inactive()
        log.info("Deleted %d inactive-variant masks", n)
        return jsonify({"ok": True, "deleted": n})

    @blueprint.route("/api/storage/masks/delete-stale", methods=["POST"])
    def api_storage_masks_delete_stale():
        """Delete masks whose stored prompt no longer matches the current
        primary detection. Skips active variants."""
        import config as cfg

        db = get_db()
        # Mirror /api/storage/masks: the deletion set must be a function
        # of the data, not of which workspace happens to be active.
        # Using the cross-workspace minimum keeps the count and the
        # delete in sync and prevents deleting masks that another
        # workspace's lower detector_confidence would still consider
        # fresh.
        min_detector_conf = db.min_detector_confidence_across_workspaces(
            cfg.load()
        )
        n = db.delete_stale_masks(detector_confidence=min_detector_conf)
        log.info("Deleted %d stale masks", n)
        # Edit-mask snapshots piggyback on the same sweep: unreferenced,
        # aged-out snapshot files (no current recipe or edit-history entry
        # points at them) are reclaimed alongside stale live masks.
        import local_masks
        # Snapshots live under dirname(THUMB_CACHE_DIR) — the same root the
        # snapshot POST endpoint writes to and every render call site reads
        # from — which is not always dirname(db_path) when create_app is
        # given a custom thumb-dir under a different parent.
        gc = local_masks.gc_edit_masks(
            db, os.path.dirname(config["THUMB_CACHE_DIR"])
        )
        if gc["deleted"]:
            log.info(
                "Deleted %d unreferenced edit-mask snapshots", gc["deleted"]
            )
        return jsonify({
            "ok": True, "deleted": n, "snapshots_deleted": gc["deleted"],
        })

    @blueprint.route("/api/storage/files")
    def api_storage_files():
        """List individual files in a cache directory."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR

        cache_type = request.args.get("type", "")
        dirs = {
            "thumbnails": config["THUMB_CACHE_DIR"],
            "previews": os.path.join(
                os.path.dirname(config["THUMB_CACHE_DIR"]), "previews"
            ),
            "embeddings": EMB_CACHE_DIR,
        }
        cache_dir = dirs.get(cache_type)
        if not cache_dir:
            return json_error("Unknown cache type")

        raw_limit = request.args.get("limit", type=int)
        limit = raw_limit if raw_limit and raw_limit > 0 else None

        # Load embedding manifest for display names
        manifest = {}
        if cache_type == "embeddings":
            from classifier import _load_manifest
            manifest = _load_manifest()

        files = []
        truncated = False
        directories = [(cache_dir, "")]
        if cache_type == "previews":
            directories.append((os.path.join(cache_dir, "paired"), "paired/"))
        from contextlib import ExitStack
        from itertools import chain, zip_longest

        def named_files(entries, prefix):
            for info in entries:
                if info.is_file() and info.name != "manifest.json":
                    yield info, prefix

        with ExitStack() as stack:
            iterators = []
            for directory, prefix in directories:
                if os.path.isdir(directory):
                    entries = stack.enter_context(os.scandir(directory))
                    ordered = sorted(entries, key=lambda e: e.name) if limit is None else entries
                    iterators.append(named_files(ordered, prefix))
            # Share bounded listings between cache families so a large ordinary
            # cache cannot hide all paired previews from the Storage control.
            combined = (chain.from_iterable(zip_longest(*iterators)) if limit is not None
                        else chain.from_iterable(iterators))
            for item in combined:
                if item is None:
                    continue
                if limit is not None and len(files) >= limit:
                    truncated = True
                    break
                info, prefix = item
                entry = {"name": prefix + info.name, "size": info.stat().st_size}
                if info.name in manifest:
                    entry["meta"] = manifest[info.name]
                files.append(entry)
        return jsonify({
            "type": cache_type,
            "path": cache_dir,
            "files": files,
            "truncated": truncated,
            "limit": limit,
        })

    def _clear_storage_cache(cache_type):
        if cache_type == "previews":
            preview_dir = os.path.join(
                os.path.dirname(config["THUMB_CACHE_DIR"]), "previews"
            )
            if os.path.isdir(preview_dir):
                shutil.rmtree(preview_dir)
                log.info("Preview cache cleared")
            # Keep preview_cache table in sync with the filesystem so
            # Settings "Current usage" and eviction don't see phantoms.
            db = get_db()
            db.caches.preview_clear_all()
            return jsonify({"ok": True})
        elif cache_type == "thumbnails":
            thumb_dir = config["THUMB_CACHE_DIR"]
            if os.path.isdir(thumb_dir):
                shutil.rmtree(thumb_dir)
                log.info("Thumbnail cache cleared")
            # Keep photos.thumb_path in sync with disk so the dashboard
            # coverage card and the pipeline plan don't see phantoms —
            # the column is the fast proxy that gates "thumbnail done"
            # in count_photos_missing_thumb, while the actual stage
            # gates on os.path.exists. Leaving thumb_path populated
            # after wiping the cache would make the Previews pill
            # report "Already done" even though the next run would
            # regenerate every thumbnail.
            db = get_db()
            db.clear_all_photo_thumb_paths()
            return jsonify({"ok": True})
        elif cache_type == "embeddings":
            from classifier import CACHE_DIR

            if os.path.isdir(CACHE_DIR):
                shutil.rmtree(CACHE_DIR)
                log.info("Embedding cache cleared")
            return jsonify({"ok": True})
        else:
            return json_error("Unknown cache type")

    @blueprint.route("/api/storage/clear", methods=["POST"])
    def api_storage_clear():
        """Clear a specific cache."""
        body = request.get_json(silent=True) or {}
        return _clear_storage_cache(body.get("type", ""))

    @blueprint.route("/api/storage/clear-safe", methods=["POST"])
    def api_storage_clear_safe():
        """Clear every regenerable cache without touching models or originals."""
        for cache_type in ("thumbnails", "previews", "embeddings"):
            _clear_storage_cache(cache_type)
        return jsonify({
            "ok": True,
            "cleared": ["thumbnails", "previews", "embeddings"],
        })

    @blueprint.route("/api/storage/open-folder", methods=["POST"])
    def api_storage_open_folder():
        """Open one of Vireo's server-selected storage paths."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR
        from models import DEFAULT_MODELS_DIR
        from services.storage_paths import auxiliary_paths, huggingface_cache_dir
        from services.storage_paths import location_id as managed_location_id

        location_id = (request.get_json(silent=True) or {}).get(
            "location", "generated"
        )
        locations = {
            "catalog": os.path.dirname(db_path),
            "generated": os.path.dirname(config["THUMB_CACHE_DIR"]),
            "embeddings": EMB_CACHE_DIR,
            "models": DEFAULT_MODELS_DIR,
            "hf_cache": huggingface_cache_dir(),
        }
        from services.retained_cache import roots
        locations.update({"retained-" + family: path for family, path in roots(config).items()})
        for owned_path in auxiliary_paths(db_path, config["THUMB_CACHE_DIR"], config, get_db()):
            directory = owned_path if os.path.isdir(owned_path) else os.path.dirname(owned_path)
            locations[managed_location_id(directory)] = directory
        path = locations.get(location_id)
        if path is None:
            return json_error("Unknown storage location")
        path = os.path.abspath(path)
        if not os.path.isdir(path):
            return jsonify({"ok": False, "reason": "storage folder not found"})
        try:
            if sys.platform == "darwin":
                proc = subprocess.run(
                    ["open", "--", path], timeout=5, check=False,
                    capture_output=True, text=True, **no_window_kwargs(),
                )
            elif sys.platform.startswith("win"):
                proc = subprocess.run(
                    ["explorer", path], timeout=5, check=False,
                    capture_output=True, text=True, **no_window_kwargs(),
                )
            else:
                proc = subprocess.run(
                    ["xdg-open", path], timeout=5, check=False,
                    capture_output=True, text=True, **no_window_kwargs(),
                )
            if not sys.platform.startswith("win") and proc.returncode != 0:
                reason = (proc.stderr or proc.stdout or "").strip()
                return jsonify({
                    "ok": False,
                    "reason": reason or f"open command exited {proc.returncode}",
                })
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
            return jsonify({"ok": False, "reason": str(exc)})
        return jsonify({"ok": True})

    @blueprint.route("/api/storage/delete-files", methods=["POST"])
    def api_storage_delete_files():
        """Delete specific files from a cache directory."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR

        body = request.get_json(silent=True) or {}
        cache_type = body.get("type", "")
        filenames = body.get("files", [])
        dirs = {
            "thumbnails": config["THUMB_CACHE_DIR"],
            "previews": os.path.join(
                os.path.dirname(config["THUMB_CACHE_DIR"]), "previews"
            ),
            "embeddings": EMB_CACHE_DIR,
        }
        cache_dir = dirs.get(cache_type)
        if not cache_dir:
            return json_error("Unknown cache type")
        if not filenames:
            return json_error("No files specified")

        deleted = 0
        # Keep preview_cache rows in sync when previews are deleted directly
        # via this endpoint (stats page). Matches {pid}_{size}.jpg only;
        # legacy {pid}.jpg files have no tracking row to remove.
        preview_rows_removed = 0
        # Keep photos.thumb_path in sync when individual thumbnail files
        # are removed — same reason as the storage/clear path: the column
        # is the planner's fast proxy and would otherwise report phantoms.
        thumb_ids_cleared = []
        if cache_type in ("previews", "thumbnails"):
            import re
            sized_pat = re.compile(r"^(\d+)_(\d+)\.jpg$")
            thumb_pat = re.compile(r"^(\d+)\.jpg$")
            db = get_db()
        for fname in filenames:
            # Prevent path traversal
            safe = os.path.basename(fname)
            is_paired = cache_type == "previews" and fname == f"paired/{safe}"
            fp = os.path.join(cache_dir, "paired" if is_paired else "", safe)
            if os.path.isfile(fp):
                os.remove(fp)
                deleted += 1
                if is_paired:
                    db.caches.paired_preview_delete(safe)
                elif cache_type == "previews":
                    m = sized_pat.match(safe)
                    if m:
                        db.caches.preview_delete(int(m.group(1)), int(m.group(2)))
                        preview_rows_removed += 1
                elif cache_type == "thumbnails":
                    m = thumb_pat.match(safe)
                    if m:
                        thumb_ids_cleared.append(int(m.group(1)))
        if cache_type == "previews" and preview_rows_removed:
            log.info("Removed %d preview_cache rows alongside files", preview_rows_removed)
        if cache_type == "thumbnails" and thumb_ids_cleared:
            # The write is chunked under SQLite's SQLITE_MAX_VARIABLE_NUMBER
            # cap (999 on older builds). The storage UI's "delete selected"
            # can send thousands of files at once; a single statement with
            # one bind per id would raise OperationalError after the files
            # have already been removed, leaving photos.thumb_path out of
            # sync with disk.
            db.clear_photo_thumb_paths(thumb_ids_cleared)
            log.info(
                "Cleared photos.thumb_path for %d photos alongside files",
                len(thumb_ids_cleared),
            )
        log.info("Deleted %d files from %s cache", deleted, cache_type)
        return jsonify({"ok": True, "deleted": deleted})

    return blueprint
