"""Export presets, photo and complete-site exports, and website publishing."""

from __future__ import annotations

import os
import time

import config as cfg
from db import Database, _chunks
from flask import Blueprint, jsonify, request
from web.background_jobs import make_background_job


def create_export_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    read_raw_config_file,
    settings_write_lock,
    build_life_list_payload,
    build_highlights_payload,
    resolve_visual,
):
    """Build the export blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``).
    Presets live in the settings file, hence the raw reader + write lock;
    site publishing renders the same Life List / Highlights payloads the
    pages use, so those builders are injected until their domains move.
    """
    blueprint = Blueprint("export", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    @blueprint.route("/api/jobs/panorama", methods=["POST"])
    @background_job
    def api_job_panorama(ctx):
        """Create a panorama from every selected photo in this workspace."""
        from panorama import create_panorama, validate_options

        try:
            options = validate_options(request.get_json(silent=True))
        except ValueError as exc:
            return json_error(str(exc))
        db = get_db()
        ids = options["photo_ids"]
        placeholders = ",".join("?" for _ in ids)
        visible = db.conn.execute(
            f"""SELECT p.id FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
            [ctx.workspace_id, *ids],
        ).fetchall()
        if {row["id"] for row in visible} != set(ids):
            return json_error("Every selected photo must be available in the current workspace")
        if options["destination"] and not os.path.isdir(options["destination"]):
            return json_error("Choose an existing destination folder")
        effective_cfg = db.get_effective_config(cfg.load())
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        reveal = options.pop("reveal")

        def work(job):
            from export import reveal_exported_files

            def progress(current, total, phase):
                event = {"current": current, "total": total, "phase": phase, "current_file": phase}
                job["progress"].update(event)
                ctx.runner.push_event(job["id"], "progress", event)

            thread_db = ctx.thread_db()
            try:
                result = create_panorama(
                    thread_db, vireo_dir, **options, config=effective_cfg,
                    checkpoint=lambda: ctx.checkpoint(job), progress=progress,
                )
                result["revealed"] = bool(
                    reveal and not ctx.runner.cancellation_requested(job["id"])
                    and reveal_exported_files([result["path"]])
                )
                return result
            finally:
                thread_db.close()

        return ctx.start("panorama", work, config={**options, "reveal": reveal})

    @blueprint.route("/api/export/presets")
    def api_export_presets_list():
        """List saved export presets (global; shared by every workspace)."""
        import config as cfg

        return jsonify({"presets": cfg.get_export_presets()})

    @blueprint.route("/api/export/presets", methods=["POST"])
    def api_export_presets_save():
        """Create or replace a saved export preset by name."""
        import config as cfg
        from export import (
            normalize_export_preset_name,
            normalize_export_preset_settings,
        )

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        try:
            name = normalize_export_preset_name(body.get("name"))
            settings = normalize_export_preset_settings(body.get("settings"))
        except ValueError as exc:
            return json_error(str(exc))
        replace = body.get("replace", False)
        if not isinstance(replace, bool):
            return json_error("replace must be a boolean")

        preset = {"name": name, "settings": settings}
        with settings_write_lock:
            raw = read_raw_config_file()
            existing = raw.get("export_presets")
            existing = existing if isinstance(existing, list) else []
            replaced = any(
                isinstance(entry, dict) and entry.get("name") == name
                for entry in existing
            )
            if replaced and not replace:
                return json_error(
                    f"export preset {name!r} already exists",
                    status=409,
                    code="export_preset_exists",
                )
            presets = [
                entry for entry in existing
                if isinstance(entry, dict) and entry.get("name") != name
            ]
            presets.append(preset)
            presets.sort(key=lambda entry: str(entry.get("name", "")).casefold())
            raw["export_presets"] = presets
            cfg.save(raw)
        return jsonify({"ok": True, "replaced": replaced, "preset": preset})

    @blueprint.route("/api/export/presets/<name>", methods=["DELETE"])
    def api_export_presets_delete(name):
        """Delete a saved export preset."""
        import config as cfg

        with settings_write_lock:
            raw = read_raw_config_file()
            existing = raw.get("export_presets")
            existing = existing if isinstance(existing, list) else []
            remaining = [
                entry for entry in existing
                if not (isinstance(entry, dict) and entry.get("name") == name)
            ]
            if len(remaining) == len(existing):
                return json_error(f"unknown export preset {name!r}", status=404)
            raw["export_presets"] = remaining
            cfg.save(raw)
        return jsonify({"ok": True})

    @blueprint.route("/api/jobs/export/preflight", methods=["POST"])
    def api_job_export_preflight():
        """Preview collision renames before starting a photo export."""
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        destination = body.get("destination", "")
        export_to_subfolder = body.get("export_to_subfolder", False)
        naming_template = body.get("naming_template", "{original}")
        output_format = body.get("format", body.get("output_format", "jpg"))
        max_size = body.get("max_size")

        if not raw_ids:
            return json_error("photo_ids required")
        try:
            photo_ids = [int(pid) for pid in raw_ids]
        except (ValueError, TypeError):
            return json_error("photo_ids must be integers")
        if not isinstance(destination, str):
            return json_error("destination must be a string")
        destination = destination.strip()
        if destination and not os.path.isabs(destination):
            return json_error("destination must be an absolute path")
        if not isinstance(export_to_subfolder, bool):
            return json_error("export_to_subfolder must be a boolean")
        if not isinstance(naming_template, str):
            return json_error("naming_template must be a string")

        try:
            from export import (
                ExportPreflightError,
                normalize_max_size,
                normalize_output_format,
                normalize_subfolder_name,
                preview_export_renames,
            )
            output_format = normalize_output_format(output_format)["extension"]
            max_size = normalize_max_size(max_size)
            subfolder_name = (
                normalize_subfolder_name(body.get("subfolder_name"))
                if export_to_subfolder else None
            )
        except ValueError as exc:
            return json_error(str(exc))

        db = get_db()
        active_ws = db._active_workspace_id
        visible_set = set()
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            visible = db.conn.execute(
                f"""SELECT p.id FROM photos p
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
                [active_ws] + list(chunk),
            ).fetchall()
            visible_set.update(row["id"] for row in visible)
        photo_ids = [pid for pid in photo_ids if pid in visible_set]
        if not photo_ids:
            return json_error("no exportable photos in current workspace")

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        effective_cfg = db.get_effective_config(cfg.load())

        try:
            renames = preview_export_renames(
                db=db,
                photo_ids=photo_ids,
                destination=destination,
                options={
                    "naming_template": naming_template,
                    "format": output_format,
                    "export_to_subfolder": export_to_subfolder,
                    "subfolder_name": subfolder_name,
                    "max_size": max_size,
                    "working_copy_max_size": effective_cfg.get(
                        "working_copy_max_size", 4096,
                    ),
                    "vireo_dir": vireo_dir,
                    "developed_dir": (
                        effective_cfg.get("darktable_output_dir", "") or ""
                    ),
                },
            )
        except ExportPreflightError as exc:
            return json_error(str(exc), status=409)
        return jsonify({
            "rename_count": len(renames),
            "renames": renames[:20],
            "truncated": len(renames) > 20,
        })

    @blueprint.route("/api/jobs/export", methods=["POST"])
    @background_job
    def api_job_export(ctx):
        """Export selected photos to a destination directory."""
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        destination = body.get("destination", "")
        export_to_subfolder = body.get("export_to_subfolder", False)
        naming_template = body.get("naming_template", "{original}")
        max_size = body.get("max_size")
        quality = body.get("quality", 92)
        output_format = body.get("format", body.get("output_format", "jpg"))
        metadata_fields = body.get("metadata_fields", [])
        reveal_after_export = body.get("reveal_after_export", False)

        if not raw_ids:
            return json_error("photo_ids required")
        try:
            photo_ids = [int(pid) for pid in raw_ids]
        except (ValueError, TypeError):
            return json_error("photo_ids must be integers")
        if not isinstance(destination, str):
            return json_error("destination must be a string")
        destination = destination.strip()
        if destination and not os.path.isabs(destination):
            return json_error("destination must be an absolute path")
        if not isinstance(export_to_subfolder, bool):
            return json_error("export_to_subfolder must be a boolean")
        if not isinstance(reveal_after_export, bool):
            return json_error("reveal_after_export must be a boolean")
        try:
            from export import (
                normalize_max_size,
                normalize_metadata_fields,
                normalize_output_format,
                normalize_quality,
                normalize_subfolder_name,
            )
            output_format_info = normalize_output_format(output_format)
            output_format = output_format_info["extension"]
            quality = normalize_quality(quality)
            metadata_fields = normalize_metadata_fields(metadata_fields)
            max_size = normalize_max_size(max_size)
            subfolder_name = (
                normalize_subfolder_name(body.get("subfolder_name"))
                if export_to_subfolder else None
            )
        except ValueError as exc:
            return json_error(str(exc))

        db = get_db()

        # Filter to only photos visible in the active workspace,
        # preserving the caller's original ordering. Chunked so a large
        # selection doesn't exceed SQLite's bound-parameter cap.
        visible_set = set()
        for chunk in _chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            visible = db.conn.execute(
                f"""SELECT p.id FROM photos p
                    JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
                [ctx.workspace_id] + list(chunk),
            ).fetchall()
            visible_set.update(r["id"] for r in visible)
        photo_ids = [pid for pid in photo_ids if pid in visible_set]
        if not photo_ids:
            return json_error("no exportable photos in current workspace")
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        effective_cfg = db.get_effective_config(cfg.load())
        wc_max_size = effective_cfg.get("working_copy_max_size", 4096)
        # Pass the configured darktable output dir so export prefers the
        # perfected render over a fresh libraw decode of the RAW.
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        def work(job):
            from export import export_photos, reveal_exported_files

            thread_db = ctx.thread_db()

            job["_start_time"] = time.time()
            job["progress"]["total"] = len(photo_ids)

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                ctx.runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "rate": round(
                        current / max(time.time() - job["_start_time"], 0.01), 1
                    ),
                    "phase": "Exporting photos",
                })

            result = export_photos(
                db=thread_db,
                vireo_dir=vireo_dir,
                photo_ids=photo_ids,
                destination=destination,
                options={
                    "naming_template": naming_template,
                    "max_size": max_size,
                    "quality": quality,
                    "format": output_format,
                    "working_copy_max_size": wc_max_size,
                    "developed_dir": developed_dir,
                    "metadata_fields": metadata_fields,
                    "export_to_subfolder": export_to_subfolder,
                    "subfolder_name": subfolder_name,
                    "collect_files": reveal_after_export,
                },
                progress_cb=progress_cb,
                cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
                cancel_only_check=lambda: ctx.runner.cancellation_requested(job["id"]),
            )
            exported_files = result.pop("files", [])
            result["revealed"] = bool(
                reveal_after_export
                and exported_files
                and not ctx.runner.is_cancelled(job["id"])
                and reveal_exported_files(exported_files)
            )
            return result

        return ctx.start(
            "export", work, pausable=True,
            config={
                "photo_ids": photo_ids,
                "destination": destination,
                "destination_mode": "custom" if destination else "original",
                "export_to_subfolder": export_to_subfolder,
                "subfolder_name": subfolder_name,
                "naming_template": naming_template,
                "format": output_format,
                "metadata_fields": metadata_fields,
                "reveal_after_export": reveal_after_export,
            },
        )

    def _publish_site_bool(body, key, default):
        raw = body.get(key, default)
        if isinstance(raw, str):
            normalized = raw.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True, None
            if normalized in {"0", "false", "no", "off"}:
                return False, None
            return None, f"{key} must be a boolean"
        if not isinstance(raw, bool):
            return None, f"{key} must be a boolean"
        return raw, None

    def _parse_publish_site_options(body, require_destination=False):
        if not isinstance(body, dict):
            return None, "request body must be a JSON object"

        destination = body.get("destination", "")
        if not isinstance(destination, str):
            return None, "destination must be a string"
        destination = destination.strip()
        if require_destination and not destination:
            return None, "destination required"
        if destination and not os.path.isabs(destination):
            return None, "destination must be an absolute path"

        try:
            photos_per_species = int(body.get("photos_per_species", 1))
            limit_per_bucket = int(body.get("limit_per_bucket", 3))
        except (TypeError, ValueError):
            return None, "photos_per_species and limit_per_bucket must be numbers"
        photos_per_species = max(1, min(photos_per_species, 100))
        limit_per_bucket = max(1, min(limit_per_bucket, 100))

        include_life_list, error = _publish_site_bool(
            body, "include_life_list", True,
        )
        if error:
            return None, error
        include_highlights, error = _publish_site_bool(
            body, "include_highlights", False,
        )
        if error:
            return None, error
        include_locations, error = _publish_site_bool(
            body, "include_locations", False,
        )
        if error:
            return None, error
        if not include_life_list and not include_highlights:
            return None, "select Life List, Highlights, or both"

        max_size = body.get("max_size", 2400)
        if max_size in ("", None):
            max_size = None
        elif isinstance(max_size, bool):
            return None, "max_size must be a number"
        else:
            try:
                max_size = int(max_size)
            except (TypeError, ValueError):
                return None, "max_size must be a number"
        quality = body.get("quality", 88)
        if isinstance(quality, bool):
            return None, "quality must be a number"
        try:
            quality = int(quality)
        except (TypeError, ValueError):
            return None, "quality must be a number"
        quality = max(1, min(quality, 100))

        return {
            "destination": destination,
            "include_life_list": include_life_list,
            "photos_per_species": photos_per_species,
            "include_highlights": include_highlights,
            "limit_per_bucket": limit_per_bucket,
            "include_locations": include_locations,
            "max_size": max_size,
            "quality": quality,
        }, None

    def _empty_published_life_list(photos_per_species):
        return {
            "species": [],
            "meta": {
                "species_count": 0,
                "photo_count": 0,
                "photos_per_species": photos_per_species,
            },
        }

    def _empty_published_highlights(limit_per_bucket):
        return {
            "buckets": [],
            "unidentified": {
                "photo_count": 0,
                "photos": [],
                "loaded_count": 0,
                "has_more": False,
            },
            "folders": [],
            "meta": {"total_in_scope": 0, "eligible": 0,
                     "limit_per_bucket": limit_per_bucket},
            "scope": "workspace",
        }

    def _build_publish_site_payloads(db, options):
        if options["include_life_list"]:
            life_list = build_life_list_payload(
                db,
                photos_per_species=options["photos_per_species"],
            )
        else:
            life_list = _empty_published_life_list(
                options["photos_per_species"],
            )
        if options["include_highlights"]:
            highlights = build_highlights_payload(
                db,
                scope="workspace",
                min_quality=0.0,
                limit_per_bucket=options["limit_per_bucket"],
            )
        else:
            highlights = _empty_published_highlights(
                options["limit_per_bucket"],
            )
        return life_list, highlights

    def _publish_site_photo_ids(life_list, highlights):
        photo_ids = {
            p["id"]
            for entry in life_list.get("species", [])
            for p in ([entry.get("best")] + (entry.get("photos") or []))
            if p and p.get("id")
        }
        for bucket in highlights.get("buckets", []):
            photo_ids.update(
                p["id"] for p in (bucket.get("photos") or []) if p.get("id")
            )
        unidentified = highlights.get("unidentified") or {}
        photo_ids.update(
            p["id"] for p in (unidentified.get("photos") or []) if p.get("id")
        )
        return photo_ids

    @blueprint.route("/api/jobs/publish-site/preflight", methods=["POST"])
    def api_job_publish_site_preflight():
        """Return exact content and unique-photo counts for a site publish."""
        options, error = _parse_publish_site_options(
            request.get_json(silent=True),
        )
        if error:
            return json_error(error)
        life_list, highlights = _build_publish_site_payloads(get_db(), options)
        return jsonify({
            "life_list_species": life_list.get("meta", {}).get(
                "species_count", 0,
            ),
            "highlight_buckets": len(highlights.get("buckets", [])),
            "unidentified_photos": len(
                (highlights.get("unidentified") or {}).get("photos") or []
            ),
            "image_count": len(_publish_site_photo_ids(life_list, highlights)),
            "data_file_count": 3,
        })

    @blueprint.route("/api/jobs/publish-site", methods=["POST"])
    @background_job
    def api_job_publish_site(ctx):
        """Publish selected workspace website data and optimized photos."""
        options, error = _parse_publish_site_options(
            request.get_json(silent=True),
            require_destination=True,
        )
        if error:
            return json_error(error)

        destination = options["destination"]
        photos_per_species = options["photos_per_species"]
        limit_per_bucket = options["limit_per_bucket"]
        max_size = options["max_size"]
        quality = options["quality"]
        include_locations = options["include_locations"]

        import config as cfg
        db = get_db()
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        effective_cfg = db.get_effective_config(cfg.load())
        wc_max_size = effective_cfg.get("working_copy_max_size", 4096)
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        def work(job):
            from site_publish import publish_site

            thread_db = ctx.thread_db()
            job["_start_time"] = time.time()

            life_list, highlights = _build_publish_site_payloads(
                thread_db, options,
            )
            photo_ids = _publish_site_photo_ids(life_list, highlights)
            job["progress"]["total"] = len(photo_ids)

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                ctx.runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "rate": round(
                        current / max(time.time() - job["_start_time"], 0.01), 1
                    ),
                    "phase": "Publishing website",
                })

            return publish_site(
                db=thread_db,
                vireo_dir=vireo_dir,
                destination=destination,
                life_list=life_list,
                highlights=highlights,
                options={
                    "max_size": max_size,
                    "quality": quality,
                    "working_copy_max_size": wc_max_size,
                    "developed_dir": developed_dir,
                    "include_locations": include_locations,
                },
                progress_cb=progress_cb,
                cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
                begin_commit=lambda: ctx.runner.begin_uncancellable(job["id"]),
            )

        return ctx.start(
            "publish-site",
            work,
            pausable=True,
            config={
                "destination": destination,
                "include_life_list": options["include_life_list"],
                "photos_per_species": photos_per_species,
                "include_highlights": options["include_highlights"],
                "limit_per_bucket": limit_per_bucket,
                "max_size": max_size,
                "quality": quality,
                "include_locations": include_locations,
            },
        )

    @blueprint.route("/api/jobs/export-site", methods=["POST"])
    @background_job
    def api_job_export_site(ctx):
        """Export every workspace photo, albums, metadata, and life list."""
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        destination = body.get("destination")
        if not isinstance(destination, str) or not destination.strip():
            return json_error("destination required")
        destination = destination.strip()
        if not os.path.isabs(destination) or "\x00" in destination:
            return json_error("destination must be an absolute folder path")
        include_locations = body.get("include_locations", False)
        if not isinstance(include_locations, bool):
            return json_error("include_locations must be a boolean")
        effective = get_db().get_effective_config(cfg.load())
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])

        def work(job):
            from site_export import export_site

            thread_db = ctx.thread_db()
            started = time.time()

            def progress(current, total, filename, phase):
                payload = {
                    "current": current, "total": total,
                    "current_file": filename, "phase": phase,
                    "rate": round(current / max(time.time() - started, 0.01), 1),
                }
                job["progress"].update(payload)
                ctx.runner.push_event(job["id"], "progress", payload)

            try:
                return export_site(
                    thread_db, vireo_dir, destination,
                    build_life_list=build_life_list_payload,
                    resolve_visual=resolve_visual,
                    options={
                        "include_locations": include_locations,
                        "working_copy_max_size": effective.get("working_copy_max_size", 4096),
                        "developed_dir": effective.get("darktable_output_dir", "") or "",
                    },
                    progress_cb=progress,
                    checkpoint=lambda: ctx.checkpoint(job),
                    cancel_check=lambda: ctx.runner.cancellation_requested(job["id"]),
                    begin_commit=lambda: ctx.runner.begin_uncancellable(job["id"]),
                )
            finally:
                thread_db.close()

        return ctx.start(
            "export-site", work, pausable=True,
            config={"destination": destination, "include_locations": include_locations},
        )

    return blueprint
