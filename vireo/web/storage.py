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
import re
import shutil
import subprocess
import sys

from flask import Blueprint, jsonify, request
from proc import no_window_kwargs
from working_copy_cache import working_copy_stats

log = logging.getLogger(__name__)


def create_storage_blueprint(get_db, json_error, db_path, config, *, chunked):
    """Build the storage blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``,
    ``CARD_CLEANUP_DIR``, ``COMPUTATION_CACHE_DIR``), read when a request runs
    rather than when the app is built. ``chunked`` is app.py's ``_chunked``,
    which splits an id list under SQLite's bind-variable cap; it stays in
    app.py because the batch-delete routes share it.
    """
    blueprint = Blueprint("storage", __name__)

    def _storage_masks_data(db):
        """Build the shared mask-storage payload once per storage refresh."""
        import config as cfg

        # This is a global view, so use the most permissive confidence floor
        # across workspaces. The stale set must not change with active workspace.
        min_detector_conf = db.min_detector_confidence_across_workspaces(
            cfg.load()
        )
        variants = db.mask_variants_summary()
        stale = db.find_stale_masks(detector_confidence=min_detector_conf)
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

        def _dir_size_recursive(path):
            total = 0
            if os.path.isdir(path):
                for dirpath, _dirnames, filenames in os.walk(path):
                    for f in filenames:
                        try:
                            total += os.path.getsize(os.path.join(dirpath, f))
                        except OSError:
                            # Cache eviction and generation can race a storage
                            # refresh. A disappearing file should make this
                            # snapshot slightly stale, not fail the endpoint.
                            continue
            return total

        db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        thumb = _dir_stats(config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(
            os.path.dirname(config["THUMB_CACHE_DIR"]), "previews"
        )
        preview = _dir_stats(preview_dir)
        working = working_copy_stats(
            os.path.dirname(config["THUMB_CACHE_DIR"])
        )
        emb = _dir_stats(EMB_CACHE_DIR)
        models_size = _dir_size_recursive(DEFAULT_MODELS_DIR)
        db = get_db()
        offline_size = db.offline_original_total_bytes()
        offline_count_row = db.conn.execute(
            "SELECT COUNT(*) AS c FROM offline_originals WHERE status='cached'"
        ).fetchone()
        masks = _storage_masks_data(db)
        masks_size = masks["total_bytes"]
        storage_root = os.path.dirname(config["THUMB_CACHE_DIR"])

        # HuggingFace cache — only count Vireo-relevant models
        hf_cache = os.path.expanduser("~/.cache/huggingface/hub")
        hf_size = 0
        hf_models = []
        if os.path.isdir(hf_cache):
            for d in os.listdir(hf_cache):
                if d.startswith("models--imageomics") or d.startswith(
                    "models--bioclip"
                ):
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
        catalog_root = os.path.dirname(os.path.abspath(db_path))
        if os.path.abspath(storage_root) == catalog_root:
            # In the conventional layout this is Vireo's dedicated data
            # directory. Include taxonomy, logs, SQLite sidecars/backups, and
            # other managed files that do not have their own category.
            try:
                with os.scandir(storage_root) as entries:
                    auxiliary_paths = [entry.path for entry in entries]
            except OSError:
                auxiliary_paths = []
        else:
            # ``--thumb-dir`` may be any directory (for example
            # /data/thumbs). Its parent is not necessarily owned by Vireo, so
            # never recurse across that shared parent and classify siblings as
            # Vireo data. Count only explicit auxiliary paths Vireo creates.
            auxiliary_paths = [
                os.path.join(storage_root, dirname)
                for dirname in (
                    "originals",
                    "external-edits",
                    "external-dng",
                    "inat-uploads",
                    "inat-exports",
                    "edit-masks",
                    # ``staging_recovery`` treats <vireo_dir>/staging as
                    # Vireo-managed storage that may hold the only remaining
                    # copies of photos from failed or cancelled imports. Omit
                    # and the Storage page under a custom ``--thumb-dir``
                    # layout can hide a very large staging tree.
                    "staging",
                )
            ]
            auxiliary_paths.extend([
                config["CARD_CLEANUP_DIR"],
                config["COMPUTATION_CACHE_DIR"],
                # Taxonomy assets remain beside the catalog even when
                # generated image caches use a custom thumbnail root.
                os.path.join(catalog_root, "taxonomy.json"),
                os.path.join(catalog_root, "taxonomy"),
                f"{db_path}-wal",
                f"{db_path}-shm",
                f"{db_path}-journal",
            ])
            # Schema migrations retain one full catalog snapshot beside the
            # database as ``<db>.pre-v<N>.bak``. In a split custom-thumbnail
            # layout the catalog parent is deliberately not recursed, so add
            # only this exact Vireo-owned filename pattern rather than
            # sweeping unrelated siblings into the Storage total.
            backup_name_re = re.compile(
                re.escape(os.path.basename(db_path))
                + r"\.pre-v\d+\.bak\Z"
            )
            try:
                with os.scandir(catalog_root) as entries:
                    auxiliary_paths.extend(
                        entry.path
                        for entry in entries
                        if entry.is_file()
                        and backup_name_re.fullmatch(entry.name)
                    )
            except OSError:
                pass
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
                    "count": offline_count_row["c"],
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
            n = db.delete_masks_for_variant(variant)
        except ValueError as e:
            return json_error(str(e), 400)
        log.info("Deleted %d masks for variant %s", n, variant)
        return jsonify({"ok": True, "deleted": n})

    @blueprint.route("/api/storage/masks/delete-inactive", methods=["POST"])
    def api_storage_masks_delete_inactive():
        """Delete all non-active variant masks across all photos."""
        db = get_db()
        n = db.delete_inactive_masks()
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
        if os.path.isdir(cache_dir):
            if limit is None:
                names = sorted(os.listdir(cache_dir))
                for f in names:
                    fp = os.path.join(cache_dir, f)
                    if os.path.isfile(fp) and f != "manifest.json":
                        entry = {"name": f, "size": os.path.getsize(fp)}
                        if f in manifest:
                            entry["meta"] = manifest[f]
                        files.append(entry)
            else:
                with os.scandir(cache_dir) as entries:
                    for entry_info in entries:
                        if entry_info.name == "manifest.json":
                            continue
                        if not entry_info.is_file():
                            continue
                        if len(files) >= limit:
                            truncated = True
                            break
                        stat = entry_info.stat()
                        entry = {"name": entry_info.name, "size": stat.st_size}
                        if entry_info.name in manifest:
                            entry["meta"] = manifest[entry_info.name]
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
            db.conn.execute("DELETE FROM preview_cache")
            db.conn.commit()
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
            db.conn.execute(
                "UPDATE photos SET thumb_path = NULL "
                "WHERE thumb_path IS NOT NULL"
            )
            db.conn.commit()
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

        location_id = (request.get_json(silent=True) or {}).get(
            "location", "generated"
        )
        locations = {
            "catalog": os.path.dirname(db_path),
            "generated": os.path.dirname(config["THUMB_CACHE_DIR"]),
            "embeddings": EMB_CACHE_DIR,
            "models": DEFAULT_MODELS_DIR,
            "hf_cache": os.path.expanduser("~/.cache/huggingface/hub"),
        }
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
            fp = os.path.join(cache_dir, safe)
            if os.path.isfile(fp):
                os.remove(fp)
                deleted += 1
                if cache_type == "previews":
                    m = sized_pat.match(safe)
                    if m:
                        db.preview_cache_delete(int(m.group(1)), int(m.group(2)))
                        preview_rows_removed += 1
                elif cache_type == "thumbnails":
                    m = thumb_pat.match(safe)
                    if m:
                        thumb_ids_cleared.append(int(m.group(1)))
        if cache_type == "previews" and preview_rows_removed:
            log.info("Removed %d preview_cache rows alongside files", preview_rows_removed)
        if cache_type == "thumbnails" and thumb_ids_cleared:
            # Chunk the IN-list under SQLite's SQLITE_MAX_VARIABLE_NUMBER cap
            # (999 on older builds). The storage UI's "delete selected" can
            # send thousands of files at once; a single statement with one
            # bind per id would raise OperationalError after the files have
            # already been removed, leaving photos.thumb_path out of sync
            # with disk.
            for chunk in chunked(thumb_ids_cleared):
                placeholders = ",".join("?" for _ in chunk)
                db.conn.execute(
                    f"UPDATE photos SET thumb_path = NULL "
                    f"WHERE id IN ({placeholders})",
                    chunk,
                )
            db.conn.commit()
            log.info(
                "Cleared photos.thumb_path for %d photos alongside files",
                len(thumb_ids_cleared),
            )
        log.info("Deleted %d files from %s cache", deleted, cache_type)
        return jsonify({"ok": True, "deleted": deleted})

    return blueprint
