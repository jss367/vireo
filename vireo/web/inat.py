"""iNaturalist: submission prep, token save, direct upload and JPEG export.

``/api/inat/prepare`` prefills the submission modal, ``/api/inat/token``
validates and saves a token from that modal, ``/api/inat/submit`` and
``/api/inat/submit-batch`` upload observations (rendering the photo's edit
recipe first), and ``/api/inat/export`` writes edited JPEGs carrying only
the metadata the user ticked.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
import tempfile

from camera_denoise import render_cache_fields as _camera_render_cache_fields
from db import Database
from flask import Blueprint, jsonify, request
from render_source import (
    companion_image_can_replace_raw_result as _companion_image_can_replace_raw_result,
)
from render_source import (
    image_is_smaller_than_expected as _image_is_smaller_than_expected,
)
from render_source import path_satisfies_recipe_render as _path_satisfies_recipe_render
from render_source import (
    recipe_render_source as _recipe_render_source,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from web.background_jobs import make_background_job
from working_copy_cache import working_copy_publication_guard

log = logging.getLogger(__name__)


class InatTokenGeneration:
    """Per-app counter that lets only the newest token write win.

    ``/api/inat/token`` validates a token against iNaturalist before saving
    it, and that remote call can finish out of order. Every token write --
    a modal save starting validation, or a settings write that changes
    ``inat_token`` -- calls :meth:`advance`; a modal save persists only if
    :attr:`current` still equals the value it saw when it started.

    One instance is created per app and shared by the iNaturalist and
    settings blueprints. It has no lock of its own: every read of
    :attr:`current` and every :meth:`advance` call must happen while
    holding ``settings_write_lock``, which also serializes the config-file
    writes the counter guards.
    """

    def __init__(self):
        self.current = 0

    def advance(self):
        """Invalidate in-flight modal validations after any token write."""
        self.current += 1


def create_inat_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    token_generation,
    settings_write_lock,
    read_raw_config_file,
    max_selection_photos,
):
    """Build the iNaturalist blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``).
    The token route reads and writes the settings file, so it shares
    ``settings_write_lock`` and ``read_raw_config_file`` with the settings
    blueprint, and ``token_generation`` (an :class:`InatTokenGeneration`)
    with the settings writes that change ``inat_token``; all three stay
    owned by ``create_app``. ``max_selection_photos`` is the app-wide cap
    on photos in one selection request, still used by routes in app.py.
    """
    blueprint = Blueprint("inat", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    @blueprint.route("/api/inat/prepare/<int:photo_id>")
    def api_inat_prepare(photo_id):
        """Prepare iNaturalist submission data for a photo."""
        import config as cfg

        db = get_db()
        photo = db.conn.execute(
            """SELECT p.*, f.path as folder_path FROM photos p
               JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()
        if not photo:
            return json_error("Photo not found", 404)
        if not db._photo_in_workspace(photo_id):
            return json_error(
                f"Photo {photo_id} does not belong to the active workspace", 403,
            )

        # Use the current-fingerprint helper so a photo with cached
        # predictions from multiple label sets doesn't prefill iNat with
        # a species from a stale label set. Apply the workspace-effective
        # detector_confidence floor so we never prefill a taxon from a
        # detection the UI threshold hides.
        min_conf = db.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        pred = db.get_top_prediction_for_photo(
            photo_id, min_detector_confidence=min_conf,
        )

        species = pred["species"] if pred else ""
        scientific = pred["scientific_name"] if pred else ""

        loc = db.get_effective_photo_location(photo_id)
        lat = loc["latitude"] if loc else None
        lng = loc["longitude"] if loc else None
        # The browser uploader does not document query parameters for
        # pre-filling an observation. In particular, taxon_name is an
        # observation-search parameter, not an uploader parameter. Open the
        # generic uploader and keep the prepared metadata in this response for
        # Vireo's authenticated direct-upload flow.
        upload_url = "https://www.inaturalist.org/observations/upload"

        # Check submission history
        subs = db.get_inat_submissions([photo_id])
        already = photo_id in subs

        user_cfg = cfg.load()
        mode = "direct" if user_cfg.get("inat_token") else "quick"

        return jsonify({
            "species": species,
            "scientific_name": scientific,
            "confidence": pred["confidence"] if pred else 0,
            "timestamp": photo["timestamp"],
            "latitude": lat,
            "longitude": lng,
            "filename": photo["filename"],
            "edit_recipe": db.get_photo_edit_recipe(photo_id),
            "upload_url": upload_url,
            "mode": mode,
            "direct_upload_enabled": mode == "direct",
            "photo_upload_requires_token": mode != "direct",
            "already_submitted": already,
            "existing_observation_url": subs[photo_id]["observation_url"] if already else None,
        })

    @blueprint.route("/api/inat/validate-token", methods=["POST"])
    def api_inat_validate_token():
        """Validate an iNaturalist API token."""
        import inat
        body = request.json or {}
        token = body.get("token", "")
        if not token:
            return json_error("Token is required")
        try:
            result = inat.validate_token(token)
        except inat.InatApiError as exc:
            return json_error(str(exc), 502)
        if result is None:
            return json_error("Invalid or expired token", 401)
        return jsonify(result)

    @blueprint.route("/api/inat/token", methods=["POST"])
    def api_inat_save_token():
        """Validate and save an iNaturalist token from the submission modal."""
        import config as cfg
        import inat

        body = request.get_json(silent=True) or {}
        token = str(body.get("token") or "").strip()
        if not token:
            return json_error("Token is required")
        with settings_write_lock:
            token_generation.advance()
            request_generation = token_generation.current
            token_at_validation_start = read_raw_config_file().get(
                "inat_token", "",
            )
        try:
            user = inat.validate_token(token)
        except inat.InatApiError as exc:
            return json_error(str(exc), 502)
        if user is None:
            return json_error("Invalid or expired token", 401)

        # Preserve the raw config shape instead of writing cfg.load(), which
        # would pin every default into config.json.  Invalid tokens never reach
        # disk; validation and persistence are one user action in this flow.
        # Only the newest validation request may persist: remote validation can
        # finish out of order when a modal is closed and reopened, and an older
        # response must not overwrite the token chosen by the newer flow.
        with settings_write_lock:
            if request_generation != token_generation.current:
                return json_error(
                    "A newer token validation superseded this request", 409,
                )
            raw = read_raw_config_file()
            if raw.get("inat_token", "") != token_at_validation_start:
                return json_error(
                    "The iNaturalist token changed while this request was "
                    "being validated", 409,
                )
            raw["inat_token"] = token
            cfg.save(raw)
        return jsonify({"ok": True, "login": user.get("login")})

    @blueprint.route("/api/inat/export", methods=["POST"])
    @background_job
    def api_inat_export(ctx):
        """Export edited JPEGs with only the selected iNaturalist metadata."""
        import config as cfg
        from inat_export import (
            InatExportError,
            export_inat_photo,
            reveal_inat_exports,
        )

        body = request.get_json(silent=True) or {}
        destination = str(body.get("destination") or "").strip()
        submissions = body.get("submissions")
        if not destination:
            return json_error("destination is required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")
        if not isinstance(submissions, list) or not submissions:
            return json_error("submissions array is required")
        if len(submissions) > max_selection_photos:
            return json_error("too many photos in selection", 400)
        normalized_submissions = []
        for item in submissions:
            if not isinstance(item, dict):
                return json_error("each submission must be an object")
            raw_photo_id = item.get("photo_id")
            if isinstance(raw_photo_id, (bool, float)):
                return json_error("photo_id must be an integer")
            try:
                photo_id = int(raw_photo_id)
            except (TypeError, ValueError):
                return json_error("photo_id must be an integer")
            normalized = dict(item)
            normalized["photo_id"] = photo_id
            if normalized.get("include_location", False):
                latitude = normalized.get("latitude")
                longitude = normalized.get("longitude")
                if isinstance(latitude, bool) or isinstance(longitude, bool):
                    return json_error(
                        "latitude and longitude must be finite numbers",
                    )
                try:
                    latitude = float(latitude)
                    longitude = float(longitude)
                except (TypeError, ValueError):
                    return json_error(
                        "latitude and longitude must be finite numbers",
                    )
                if not math.isfinite(latitude) or not math.isfinite(longitude):
                    return json_error(
                        "latitude and longitude must be finite numbers",
                    )
                if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
                    return json_error(
                        "latitude or longitude is out of range",
                    )
                normalized["latitude"] = latitude
                normalized["longitude"] = longitude
            normalized_submissions.append(normalized)
        submissions = normalized_submissions

        db = get_db()
        effective_cfg = db.get_effective_config(cfg.load())
        quality = effective_cfg.get("working_copy_quality", 92)
        wc_max_size = effective_cfg.get("working_copy_max_size", 4096)
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        reveal = bool(body.get("reveal", False))

        def work(job):
            thread_db = ctx.thread_db()
            exported = []
            errors = []
            total = len(submissions)
            job["progress"]["total"] = total
            try:
                for index, item in enumerate(submissions, start=1):
                    if ctx.runner.is_cancelled(job["id"]):
                        break
                    photo_id = item["photo_id"]
                    photo = thread_db.get_photo(
                        photo_id, verify_workspace=True,
                    )
                    if not photo:
                        errors.append({
                            "photo_id": photo_id,
                            "error": "Photo not found",
                        })
                        job["progress"]["current"] = index
                        ctx.runner.push_event(job["id"], "progress", {
                            "current": index,
                            "total": total,
                            "current_file": "",
                            "phase": "Exporting for iNaturalist",
                        })
                        continue

                    metadata = {}
                    if item.get("include_taxon", False):
                        taxon_name = str(
                            item.get("taxon_name") or ""
                        ).strip()
                        if taxon_name:
                            metadata["taxon_name"] = taxon_name
                    if item.get("include_date", False):
                        observed_on = str(
                            item.get("observed_on") or ""
                        ).strip()
                        if observed_on:
                            # The modal discloses a calendar date, not capture
                            # time or timezone, so export only that snapshot.
                            metadata["timestamp"] = observed_on[:10]
                    if item.get("include_location", False):
                        latitude = item.get("latitude")
                        longitude = item.get("longitude")
                        if (
                            latitude not in (None, "")
                            and longitude not in (None, "")
                        ):
                            metadata["latitude"] = latitude
                            metadata["longitude"] = longitude
                    if item.get("include_description", False):
                        description = str(
                            item.get("description") or ""
                        ).strip()
                        if description:
                            metadata["description"] = description[:10000]

                    try:
                        path = export_inat_photo(
                            thread_db,
                            vireo_dir,
                            photo_id,
                            destination,
                            metadata,
                            quality=quality,
                            working_copy_max_size=wc_max_size,
                            developed_dir=developed_dir,
                        )
                        exported.append({
                            "photo_id": photo_id,
                            "path": path,
                            "filename": os.path.basename(path),
                        })
                    except (InatExportError, OSError, ValueError) as exc:
                        errors.append({
                            "photo_id": photo_id,
                            "error": str(exc),
                        })
                    finally:
                        job["progress"]["current"] = index
                        job["progress"]["current_file"] = photo["filename"]
                        ctx.runner.push_event(job["id"], "progress", {
                            "current": index,
                            "total": total,
                            "current_file": photo["filename"],
                            "phase": "Exporting for iNaturalist",
                        })

                revealed = False
                cancelled = ctx.runner.is_cancelled(job["id"])
                if exported and reveal and not cancelled:
                    revealed = reveal_inat_exports(
                        [item["path"] for item in exported], destination,
                    )
                return {
                    "ok": bool(exported),
                    "exported": exported,
                    "errors": errors,
                    "destination": destination,
                    "revealed": revealed,
                }
            finally:
                thread_db.close()

        return ctx.start(
            "inat-export",
            work,
            pausable=True,
            config={
                "photo_ids": [item["photo_id"] for item in submissions],
                "destination": destination,
            },
        )

    def _inat_edit_recipe_source(photo, recipe, fallback_path):
        from image_loader import RAW_EXTENSIONS

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        folders = {photo["folder_id"]: photo["folder_path"]}
        primary_is_raw = (
            os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
        )
        if recipe.get("crop"):
            source_path, using_working_copy = _recipe_render_source(
                photo, recipe, 0, vireo_dir, folders,
            )
            return source_path or fallback_path, bool(using_working_copy)

        # For RAW primaries, skip the working-copy short-circuit while the
        # RAW source is available: legacy working copies predate the
        # highlight-preserving RAW decode and would feed iNat a clipped JPEG
        # to apply the recipe to. If the RAW source is offline/missing, the
        # working copy is the only local fallback.
        wc_rel = photo["working_copy_path"]
        if wc_rel and (
            not primary_is_raw or not os.path.exists(fallback_path)
        ):
            wc_path = (
                wc_rel if os.path.isabs(wc_rel)
                else os.path.join(vireo_dir, wc_rel)
            )
            if (
                os.path.exists(wc_path)
                and _path_satisfies_recipe_render(wc_path, photo, recipe, 0)
            ):
                return wc_path, True

        companion_path = photo["companion_path"]
        raw_source_available = os.path.exists(fallback_path)
        # RAW primaries are decoded with RAW_DECODE_LINEAR later;
        # only use the clipped camera JPEG when the RAW source is offline.
        if companion_path and (not primary_is_raw or not raw_source_available):
            companion = os.path.join(photo["folder_path"], companion_path)
            if (
                os.path.exists(companion)
                and _path_satisfies_recipe_render(companion, photo, recipe, 0)
            ):
                return companion, False
        return fallback_path, False

    def _inat_upload_photo_path(db, photo, fallback_path):
        import config as cfg

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        recipe = db.get_photo_edit_recipe(photo["id"])
        if not recipe:
            return fallback_path, None

        import local_masks as _local_masks
        from image_edits import (
            EDIT_MATH_VERSION,
            apply_recipe_to_loaded_image,
            recipe_to_json,
        )
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        source_path, using_working_copy = _inat_edit_recipe_source(
            photo, recipe, fallback_path,
        )
        fallback_available = bool(
            fallback_path and os.path.isfile(fallback_path)
        )
        # When the original is offline the working copy is the only usable
        # local source; the retry-from-original recovery below cannot save us.
        # Hold the publication/eviction guard through the exists → getmtime →
        # decode window so quota enforcement cannot unlink the working copy
        # mid-upload. Guard is an RLock, so nested acquisitions inside
        # load_image are safe.
        pin_working_copy = using_working_copy and not fallback_available
        source_pin_cm = (
            working_copy_publication_guard()
            if pin_working_copy else contextlib.nullcontext()
        )
        with source_pin_cm:
            if not source_path or not os.path.isfile(source_path):
                if using_working_copy and fallback_available:
                    # Quota enforcement can unlink the working copy between
                    # recipe-source resolution and this existence check.
                    source_path = fallback_path
                    using_working_copy = False
                else:
                    return None, f"{photo['filename']}: source file missing"

            out_dir = os.path.join(vireo_dir, "inat-uploads")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, f"{photo['id']}.jpg")
            meta_path = os.path.join(out_dir, f"{photo['id']}.json")
            try:
                source_mtime = os.path.getmtime(source_path)
            except FileNotFoundError:
                if using_working_copy and fallback_available:
                    source_path = fallback_path
                    using_working_copy = False
                    source_mtime = os.path.getmtime(source_path)
                else:
                    return None, f"{photo['filename']}: source file missing"
            recipe_json = recipe_to_json(recipe) or ""
            # Include the edit-math version so a math bump invalidates this
            # cached render — the JPEG is keyed by recipe/source/mtime, none
            # of which change when only the per-pixel rendering math does, so
            # without this we'd keep submitting stale renders to iNaturalist
            # after a deploy.
            expected_meta = {
                "recipe": recipe_json,
                "source_path": source_path,
                "source_mtime": source_mtime,
                "edit_math_version": EDIT_MATH_VERSION,
                **_camera_render_cache_fields(photo, recipe),
            }
            try:
                if os.path.isfile(out_path) and os.path.isfile(meta_path):
                    with open(meta_path, encoding="utf-8") as f:
                        cached_meta = json.load(f)
                    if cached_meta == expected_meta:
                        return out_path, None
            except (OSError, ValueError, TypeError):
                pass

            # Derive the decode mode from the primary photo's extension rather
            # than source_path so a future change to source_path resolution
            # (working copy, companion JPEG fallback, etc.) cannot silently
            # bypass RAW_DECODE_LINEAR for a RAW primary.
            primary_is_raw = (
                os.path.splitext(photo["filename"])[1].lower()
                in RAW_EXTENSIONS
            )
            raw_decode = (
                RAW_DECODE_LINEAR if primary_is_raw else None
            )
            load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
            img = load_image(source_path, max_size=None, **load_kwargs)
            if img is None and using_working_copy and fallback_path and (
                os.path.isfile(fallback_path)
                and os.path.abspath(fallback_path)
                != os.path.abspath(source_path)
            ):
                # Working copy was evicted between validation and decode;
                # retry from the original before the companion fallback so
                # RAW primaries still use the highlight-preserving decode.
                retry_img = load_image(
                    fallback_path, max_size=None, **load_kwargs,
                )
                if retry_img is not None:
                    img = retry_img
                    source_path = fallback_path
                    using_working_copy = False
                    with contextlib.suppress(OSError):
                        source_mtime = os.path.getmtime(source_path)
                    expected_meta["source_path"] = source_path
                    expected_meta["source_mtime"] = source_mtime
        expected_w, expected_h = 0, 0
        needs_companion = False
        if primary_is_raw:
            # libraw may return the embedded JPEG when demosaic fails — see
            # _external_edit_handoff_path for the same gate. Without this an
            # unsupported RAW would upload clipped/undersized pixels to
            # iNaturalist even when a usable sidecar JPEG exists. Both axes
            # checked via the shared helper.
            expected_w, expected_h = _recipe_source_dimensions(photo)
            needs_companion = img is None or _image_is_smaller_than_expected(
                img, expected_w, expected_h,
            )
        if needs_companion:
            # Mirror _external_edit_handoff_path: libraw may fail to decode
            # this RAW variant, but the full-size companion JPEG can still
            # carry the recipe for iNaturalist. Without this fallback the
            # upload silently fails for RAW+JPEG pairs whose RAW is
            # unsupported. Cache key stays on the RAW source — see comment
            # in _external_edit_handoff_path for the contract trade-off.
            companion_path = photo["companion_path"]
            folder_path = photo["folder_path"]
            if folder_path and companion_path:
                companion_abs = os.path.join(folder_path, companion_path)
                if (
                    os.path.exists(companion_abs)
                    and os.path.abspath(companion_abs)
                    != os.path.abspath(source_path)
                ):
                    companion_img = load_image(companion_abs, max_size=None)
                    # Prefer companion when it covers the expected size on
                    # both axes — a long-edge-only check misses cases like a
                    # 6000x3376 embedded preview "tying" a 6000x4000 sidecar
                    # and losing the short-edge content.
                    if _companion_image_can_replace_raw_result(
                        companion_img, img, expected_w, expected_h,
                    ):
                        if img is None:
                            log.info(
                                "iNat upload RAW decode failed for photo %s; "
                                "falling back to companion JPEG %s",
                                photo["id"], companion_abs,
                            )
                        else:
                            log.info(
                                "iNat upload RAW decode fell back to "
                                "undersized embedded JPEG (%dx%d) for "
                                "photo %s; using companion JPEG %s (%dx%d)",
                                img.size[0], img.size[1], photo["id"],
                                companion_abs,
                                companion_img.size[0],
                                companion_img.size[1],
                            )
                            img.close()
                        img = companion_img
                    elif companion_img is not None:
                        companion_img.close()
        if img is None:
            return None, f"{photo['filename']}: failed to load image"
        rendered = None
        tmp_path = None
        fd = None
        try:
            rendered = apply_recipe_to_loaded_image(
                img, recipe,
                camera_metadata=photo,
                native_size=_recipe_source_dimensions(photo),
                local_mask=_local_masks.load_snapshot(
                    vireo_dir, photo["id"], recipe,
                ),
            )
            quality = cfg.load().get("working_copy_quality", 92)
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{photo['id']}.", suffix=".jpg.tmp", dir=out_dir,
            )
            os.close(fd)
            fd = None
            rendered.save(tmp_path, format="JPEG", quality=quality)
            os.replace(tmp_path, out_path)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(expected_meta, f, sort_keys=True)
        except Exception:
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.close(fd)
            with contextlib.suppress(OSError):
                if tmp_path:
                    os.unlink(tmp_path)
            raise
        finally:
            if rendered is not None:
                rendered.close()
            if rendered is not img:
                img.close()
        return out_path, None

    @blueprint.route("/api/inat/submit", methods=["POST"])
    def api_inat_submit():
        """Submit a single observation to iNaturalist."""
        import config as cfg
        import inat

        user_cfg = cfg.load()
        token = user_cfg.get("inat_token")
        if not token:
            return json_error("iNaturalist token not configured. Add it in Settings.")

        data = request.json or {}
        photo_id = data.get("photo_id")
        if not photo_id:
            return json_error("photo_id is required")

        db = get_db()
        photo = db.conn.execute(
            """SELECT p.*, f.path as folder_path FROM photos p
               JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()
        if not photo:
            return json_error("Photo not found", 404)
        if not db._photo_in_workspace(photo_id):
            return json_error(
                f"Photo {photo_id} does not belong to the active workspace", 403,
            )

        photo_path = os.path.join(photo["folder_path"], photo["filename"])
        if not os.path.isfile(photo_path):
            recipe = db.get_photo_edit_recipe(photo_id)
            wc_rel = photo["working_copy_path"]
            wc_abs = (
                wc_rel if wc_rel and os.path.isabs(wc_rel)
                else os.path.join(
                    os.path.dirname(config["THUMB_CACHE_DIR"]), wc_rel,
                )
                if wc_rel else None
            )
            companion_abs = (
                os.path.join(photo["folder_path"], photo["companion_path"])
                if photo["companion_path"] else None
            )
            if not (
                recipe
                and (
                    (wc_abs and os.path.isfile(wc_abs))
                    or (companion_abs and os.path.isfile(companion_abs))
                )
            ):
                return json_error("Photo file not found on disk", 404)

        upload_path, upload_error = _inat_upload_photo_path(db, photo, photo_path)
        if upload_error:
            return json_error(upload_error, 500)

        # Use overrides from request, or fall back to DB data. The helper
        # picks the highest-confidence prediction from the CURRENT
        # fingerprint, scoped to the active workspace, and respecting the
        # workspace-effective detector_confidence floor — submitting a
        # stale or below-threshold taxon to iNaturalist is permanent and
        # not easily reversible.
        min_conf = db.get_effective_config(user_cfg).get(
            "detector_confidence", 0.2
        )
        pred = db.get_top_prediction_for_photo(
            photo_id, min_detector_confidence=min_conf,
        )

        default_taxon = (
            (pred["scientific_name"] or pred["species"]) if pred else None
        )
        taxon = data.get("taxon_name", default_taxon)
        observed_on = (
            data["observed_on"]
            if "observed_on" in data
            else (photo["timestamp"][:10] if photo["timestamp"] else None)
        )
        loc = db.get_effective_photo_location(photo_id)
        photo_lat = loc["latitude"] if loc else None
        photo_lng = loc["longitude"] if loc else None
        has_latitude = "latitude" in data
        has_longitude = "longitude" in data
        if has_latitude != has_longitude:
            return json_error(
                "latitude and longitude must be provided together", 400,
            )
        if has_latitude:
            lat = data["latitude"]
            lng = data["longitude"]
        else:
            lat = photo_lat
            lng = photo_lng

        try:
            obs_id, obs_url = inat.submit_observation(
                token=token,
                photo_path=upload_path,
                taxon_name=taxon,
                observed_on=observed_on,
                latitude=lat,
                longitude=lng,
                description=data.get("description"),
                geoprivacy=data.get("geoprivacy", "open"),
            )
        except inat.InatAuthError as e:
            return json_error(str(e), 401)
        except inat.InatPartialUploadError as e:
            return jsonify({
                "error": str(e),
                "partial": True,
                "observation_id": e.observation_id,
                "observation_url": e.observation_url,
            }), 502
        except inat.InatApiError as e:
            return json_error(str(e), 502)

        db.record_inat_submission(photo_id, obs_id, obs_url)
        return jsonify({"observation_id": obs_id, "observation_url": obs_url})

    @blueprint.route("/api/inat/submit-batch", methods=["POST"])
    def api_inat_submit_batch():
        """Submit multiple observations to iNaturalist."""
        import config as cfg
        import inat

        user_cfg = cfg.load()
        token = user_cfg.get("inat_token")
        if not token:
            return json_error("iNaturalist token not configured. Add it in Settings.")

        submissions = (request.json or {}).get("submissions", [])
        if not submissions:
            return json_error("submissions array is required")

        db = get_db()
        # Resolve the workspace-effective detector_confidence floor once for
        # the whole batch — read-time thresholding means below-threshold
        # detections should never seed an iNat submission.
        min_conf = db.get_effective_config(user_cfg).get(
            "detector_confidence", 0.2
        )
        results = []
        for sub in submissions:
            photo_id = sub.get("photo_id")
            has_latitude = "latitude" in sub
            has_longitude = "longitude" in sub
            if has_latitude != has_longitude:
                results.append({
                    "photo_id": photo_id,
                    "error": "latitude and longitude must be provided together",
                })
                continue
            photo = db.conn.execute(
                """SELECT p.*, f.path as folder_path FROM photos p
                   JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
                (photo_id,),
            ).fetchone()
            if not photo:
                results.append({"photo_id": photo_id, "error": "Photo not found"})
                continue
            if not db._photo_in_workspace(photo_id):
                results.append({
                    "photo_id": photo_id,
                    "error": (
                        f"Photo {photo_id} does not belong to the active workspace"
                    ),
                })
                continue

            photo_path = os.path.join(photo["folder_path"], photo["filename"])
            if not os.path.isfile(photo_path):
                results.append({"photo_id": photo_id, "error": "Photo file not found on disk"})
                continue

            upload_path, upload_error = _inat_upload_photo_path(
                db, photo, photo_path,
            )
            if upload_error:
                results.append({"photo_id": photo_id, "error": upload_error})
                continue

            # Current-fingerprint + workspace-scoped top prediction,
            # respecting the active detector_confidence floor — avoids
            # submitting a stale-label-set or now-hidden taxon to iNaturalist.
            pred = db.get_top_prediction_for_photo(
                photo_id, min_detector_confidence=min_conf,
            )

            default_taxon = (
                (pred["scientific_name"] or pred["species"])
                if pred else None
            )
            taxon = sub.get("taxon_name", default_taxon)
            observed_on = (
                sub["observed_on"]
                if "observed_on" in sub
                else (
                    photo["timestamp"][:10]
                    if photo["timestamp"] else None
                )
            )
            loc = db.get_effective_photo_location(photo_id)
            photo_lat = loc["latitude"] if loc else None
            photo_lng = loc["longitude"] if loc else None
            if has_latitude:
                lat = sub["latitude"]
                lng = sub["longitude"]
            else:
                lat = photo_lat
                lng = photo_lng

            try:
                obs_id, obs_url = inat.submit_observation(
                    token=token,
                    photo_path=upload_path,
                    taxon_name=taxon,
                    observed_on=observed_on,
                    latitude=lat,
                    longitude=lng,
                    description=sub.get("description"),
                    geoprivacy=sub.get("geoprivacy", "open"),
                )
                db.record_inat_submission(photo_id, obs_id, obs_url)
                results.append({"photo_id": photo_id, "observation_id": obs_id, "observation_url": obs_url})
            except inat.InatPartialUploadError as e:
                results.append({
                    "photo_id": photo_id,
                    "error": str(e),
                    "partial": True,
                    "observation_id": e.observation_id,
                    "observation_url": e.observation_url,
                })
            except (inat.InatAuthError, inat.InatApiError) as e:
                results.append({"photo_id": photo_id, "error": str(e)})

        return jsonify({"results": results})

    @blueprint.route("/api/inat/submissions")
    def api_inat_submissions():
        """Return submission records for a set of photo IDs."""
        raw = request.args.get("photo_ids", "")
        if not raw:
            return json_error("photo_ids parameter is required")
        try:
            photo_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            return json_error("photo_ids must be comma-separated integers")

        db = get_db()
        subs = db.get_inat_submissions(photo_ids)
        # Convert keys to strings for JSON
        return jsonify({str(k): v for k, v in subs.items()})

    return blueprint
