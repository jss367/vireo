"""Models, darktable, taxonomy info, species-label and classifier endpoints.

Also the classifier's model-side readiness (``/api/classify/readiness``,
``/api/classify/config``), the MegaDetector download/delete, and the label
embedding cache (``/api/embedding-cache`` list/clear, ``/api/embedding-matrix``)
— the per-model text embeddings that belong to a downloaded classifier.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import uuid

from classification_readiness import classification_readiness
from config import read_raw_config_file, settings_write_lock
from db import Database
from flask import Blueprint, jsonify, request
from web.background_jobs import make_background_job

log = logging.getLogger(__name__)

# Pipeline models (ONNX) by ID: the HF subfolder under ``jss367/vireo-onnx-models``
# and the local directory name under ``~/.vireo/models``, plus the files each
# needs. Download and delete both key off this table, so an ID that isn't
# listed here can neither fetch nor remove anything on disk.
PIPELINE_MODELS = {
    "megadetector-v6": {
        "subfolder": "megadetector-v6",
        "files": ["model.onnx"],
    },
    "sam2-tiny": {
        "subfolder": "sam2-tiny",
        "files": ["image_encoder.onnx", "mask_decoder.onnx"],
    },
    "sam2-small": {
        "subfolder": "sam2-small",
        "files": ["image_encoder.onnx", "mask_decoder.onnx"],
    },
    "sam2-base-plus": {
        "subfolder": "sam2-base-plus",
        "files": ["image_encoder.onnx", "mask_decoder.onnx"],
    },
    "sam2-large": {
        "subfolder": "sam2-large",
        "files": ["image_encoder.onnx", "mask_decoder.onnx"],
    },
    "vit-s14": {
        "subfolder": "dinov2-vit-s14",
        "files": ["model.onnx", "model.onnx.data"],
    },
    "vit-b14": {
        "subfolder": "dinov2-vit-b14",
        "files": ["model.onnx", "model.onnx.data"],
    },
    "vit-l14": {
        "subfolder": "dinov2-vit-l14",
        "files": ["model.onnx", "model.onnx.data"],
    },
}


def pipeline_models_dir():
    """Directory holding downloaded pipeline model weights."""
    return os.path.expanduser("~/.vireo/models")



def create_models_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    *,
    count_keywords,
):
    """Build the models blueprint.

    The darktable download records the installed binary through
    ``config.read_raw_config_file`` under ``config.settings_write_lock``.
    ``count_keywords`` is the startup
    database's ``count_keywords`` (``init_db`` in ``create_app``), which
    ``/api/classify/config`` reports as the taxonomy species count.
    """
    blueprint = Blueprint("models", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    @blueprint.route("/api/models/status")
    def api_models_status():
        """Lightweight model readiness check for first-launch detection."""
        from models import get_models

        r = classification_readiness(get_db())
        active = r["active"]

        all_models = get_models()

        return jsonify({
            "needs_setup": not r["ready"],
            "classification": {
                "ready": r["ready"],
                "model_ready": r["model_downloaded"],
                "labels_ready": r["labels_ready"],
                "model_name": active["name"] if active else None,
                "model_id": active["id"] if active else None,
            },
            "available_models": [
                {
                    "id": m["id"],
                    "name": m["name"],
                    "description": m.get("description", ""),
                    "size_mb": m.get("size_mb", 0),
                    "downloaded": m.get("downloaded", False),
                    "model_type": m.get("model_type", "bioclip"),
                }
                for m in all_models
            ],
        })

    @blueprint.route("/api/darktable/status")
    def api_darktable_status():
        import config as cfg
        from develop import (
            darktable_search_paths,
            darktable_tools_dir,
            darktable_uses_tools_dir,
            find_darktable,
            find_dng_converter,
        )

        configured = cfg.get("darktable_bin")
        binary = find_darktable(configured)
        dng_configured = cfg.get("dng_converter_bin")
        dng_binary = find_dng_converter(dng_configured)

        # What we tell the user we checked has to match what find_darktable
        # actually probes: shutil.which("darktable-cli") first, then the
        # filesystem candidates. darktable_search_paths() covers only the
        # latter, and on Linux it lists just the AppImages already present in
        # our tools dir — so a box with none yet would get an empty "we checked
        # here" list, on exactly the platform the download targets. Hence
        # compose here rather than returning that list raw.
        # darktable_uses_tools_dir() is the same predicate darktable_search_paths()
        # branches on, so the directory is named on, and only on, the platform
        # that probes it. It cannot already be in the list: the detector only
        # ever emits files *inside* the directory, never the directory itself.
        checked_paths = ["$PATH (darktable-cli)"]
        checked_paths.extend(darktable_search_paths())
        if darktable_uses_tools_dir():
            checked_paths.append(darktable_tools_dir())

        return jsonify({
            "available": binary is not None,
            "bin": binary or "",
            "configured_bin": configured,
            "dng_available": dng_binary is not None,
            "dng_bin": dng_binary or "",
            "configured_dng_bin": dng_configured,
            "auto_convert_dng": cfg.get("darktable_auto_convert_dng"),
            "style": cfg.get("darktable_style"),
            "output_format": cfg.get("darktable_output_format"),
            "output_dir": cfg.get("darktable_output_dir"),
            "checked_paths": checked_paths,
        })

    @blueprint.route("/api/darktable/install/available")
    def api_darktable_install_available():
        """What a download would fetch, for the confirmation dialog.

        Always 200: when this cannot answer, the panel shows a plain
        "Get darktable" link and the reason, rather than a dead button.

        Includes the Flask host's ``platform`` (linux/darwin/win32) so the
        UI's "Download installer" vs "Download and set up" label and its
        Gatekeeper/SmartScreen warnings describe the machine that will
        actually run the installer — not the browser device, which can be
        a phone or a different desktop on the LAN.
        """
        import sys

        import darktable_install

        try:
            release, reason = darktable_install.resolve_release_cached()
        except Exception as e:
            # resolve_release is documented never to raise; this is belt and
            # braces so an unexpected bug still degrades to a link, not a 500.
            log.warning("darktable release lookup failed: %s", e)
            return jsonify({
                "available": False,
                "reason": darktable_install.REASON_UNREACHABLE,
                "platform": sys.platform,
            })

        if not release:
            # Pass the reason through verbatim. Do NOT substitute a generic
            # message: "we could not reach GitHub" and "no build exists for
            # your platform" are different facts and users act on them
            # differently.
            return jsonify({
                "available": False,
                "reason": reason,
                "platform": sys.platform,
            })

        return jsonify({"available": True, "platform": sys.platform, **release})

    @blueprint.route("/api/models")
    def api_models():
        from models import get_active_model, get_models

        active = get_active_model()
        return jsonify(
            {
                "models": get_models(),
                "active_id": active["id"] if active else None,
            }
        )

    @blueprint.route("/api/models/<path:model_id>", methods=["DELETE"])
    def api_remove_model(model_id):
        """Remove a model's weights from disk and unregister it."""
        from models import remove_model

        removed = remove_model(model_id)
        if removed is None:
            return json_error("Model not found", 404)
        log.info("Removed model: %s", model_id)
        return jsonify({"ok": True, **removed})

    @blueprint.route("/api/models/active", methods=["POST"])
    def api_set_active_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")
        from models import set_active_model

        set_active_model(model_id)
        return jsonify({"ok": True})

    @blueprint.route("/api/models/custom", methods=["POST"])
    def api_add_custom_model():
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        if not isinstance(body.get("name", ""), str) or not isinstance(body.get("weights_path", ""), str):
            return json_error("name and weights_path must be strings")
        name = body.get("name", "").strip()
        weights_path = body.get("weights_path", "").strip()
        model_str = body.get("model_str", "ViT-B-16")
        if not name or not weights_path:
            return json_error("name and weights_path required")
        from models import register_model

        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "model"
        model_id = f"custom-{slug}-{uuid.uuid4().hex[:12]}"
        register_model(model_id, name, model_str, weights_path, "Custom model")
        return jsonify({"ok": True, "model_id": model_id})

    @blueprint.route("/api/taxonomy/info")
    def api_taxonomy_info():
        from models import get_taxonomy_info

        return jsonify(get_taxonomy_info())

    @blueprint.route("/api/jobs/download-darktable", methods=["POST"])
    @background_job
    def api_job_download_darktable(ctx):
        """Download the latest darktable build and hand it to the platform."""
        import darktable_install
        from job_contract import progress_event

        # Read the identity the user confirmed BEFORE any other check.  If a
        # download is already running and we are about to join it, the join
        # must be conditional on this matching the running job's stored
        # artifact — otherwise a second tab that saw a fresh release would
        # join an old, stale download and receive different bytes than the
        # dialog confirmed.
        body = request.get_json(silent=True) or {}
        expected_name = body.get("expected_name")
        expected_version = body.get("expected_version")
        # digest is compared verbatim (the API sends "sha256:<hex>"); None on
        # either side means the client did not send one, not that they match.
        expected_digest = body.get("expected_digest")
        # Size is the fallback identity for digestless releases: if GitHub
        # deletes and re-uploads an asset under the same tag+filename during
        # the availability cache's TTL, name/version match but bytes differ,
        # and without a digest the name/version check alone would silently
        # accept the swap.  Compare when the client sent one — an int on the
        # wire, coerced defensively so a stray string does not throw here.
        try:
            expected_size = int(body["expected_size"]) if body.get("expected_size") is not None else None
        except (TypeError, ValueError):
            expected_size = None

        def _artifact_matches(stored):
            """True when the stored artifact identity matches expected_*."""
            if not stored:
                return not expected_name
            if expected_name and expected_name != stored.get("asset_name"):
                return False
            if expected_version and expected_version != stored.get("asset_version"):
                return False
            if expected_digest and expected_digest != stored.get("asset_digest"):
                return False
            return not (
                expected_size is not None
                and stored.get("asset_size") is not None
                and expected_size != stored.get("asset_size")
            )

        # resolve_release(), never the cached variant: this re-resolves
        # server-side so the URL we actually download can't be a ten-minute-old
        # value from whatever /install/available last showed.
        try:
            asset, reason = darktable_install.resolve_release()
        except Exception as e:
            # resolve_release is documented never to raise; belt and braces so
            # an unexpected bug degrades to a message, not a 500.
            log.warning("darktable release lookup failed: %s", e)
            asset, reason = None, darktable_install.REASON_UNREACHABLE
        if not asset:
            # Surface the specific reason. resolve_release distinguishes
            # "could not reach GitHub" from "no build for your platform";
            # collapsing them here would tell a user behind a proxy that their
            # platform is unsupported, which is false.
            return json_error(reason or darktable_install.REASON_UNREACHABLE)

        # Bind the download to the exact artifact the user confirmed.  Between
        # /install/available populating its 10-minute cache and this POST,
        # darktable-org can publish a new release; the confirmation dialog
        # still names the cached version and digest, but resolve_release() just
        # returned the newer asset.  Downloading it here without another
        # confirmation would silently swap the artifact the user OK'd for a
        # different one.  code=darktable_asset_changed lets the client re-fetch
        # availability and re-prompt with the fresh identity.
        if expected_name and (
            expected_name != asset.get("name")
            or (expected_version and expected_version != asset.get("version"))
            or (expected_digest and expected_digest != asset.get("digest"))
            or (
                expected_size is not None
                and asset.get("size") is not None
                and expected_size != asset.get("size")
            )
        ):
            # Refresh the availability cache with the release we just resolved.
            # Without this, the UI's Re-check would fetch /install/available,
            # get the same ten-minute-old cached asset back, re-confirm it, and
            # get bounced by the same 409 in a loop until the TTL expires.
            darktable_install.update_release_cache(asset)
            return json_error(
                (
                    "The darktable release changed since this dialog opened. "
                    f"Confirmation showed {expected_name}; the current release "
                    f"is {asset.get('name')}. Re-check to see the new build "
                    "before downloading."
                ),
                status=409,
                code="darktable_asset_changed",
            )

        # Refuse before spending 87MB, and say what is needed vs what is free.
        # 2x the asset size, per the design's error table: the artifact is kept
        # after hand-off and the installer still has to unpack alongside it, so
        # a check for exactly the download size would succeed and then strand
        # the user on a full disk.
        target = darktable_install.install_dir()
        try:
            free = darktable_install.free_space_bytes(target)
        except OSError as e:
            # free_space_bytes creates the directory, so this is where a
            # read-only or otherwise unusable ~/.vireo surfaces. Name the
            # directory and the OS error: a 500 would tell the user nothing.
            log.warning("Cannot use the darktable download directory %s: %s", target, e)
            return json_error(
                f"Cannot use the download directory {target}: {e.strerror or e}"
            )
        asset_size = asset.get("size") or 0
        # A previous cancelled-during-verify attempt leaves the full artifact
        # already sitting at the final destination — download() fast-paths to
        # verify in that case and spends zero download bytes.  Counting those
        # bytes as still-to-be-downloaded blocks the promised retry with a
        # "not enough space" error even though the retry only needs unpacking
        # headroom.  Gate on the API-supplied size (same rule download() uses)
        # so an unrelated file the user dropped in the tools directory does
        # not get credited against the check.
        #
        # A cancelled- or connection-lost-during-transfer attempt leaves a
        # ``.partial`` sibling that _download_with_resume will resume from,
        # so those bytes are also already on disk and the retry only needs
        # ``asset_size - partial_size`` more download bytes plus the unpack
        # headroom.  Cap the credit at ``asset_size`` — a ``.partial`` bigger
        # than the API-published size is a sign of a stale file from a
        # different release, and crediting more than we can actually reuse
        # would let the retry through only for the disk to fill mid-transfer.
        reusable_bytes = 0
        asset_name = asset.get("name") or ""
        if asset_name:
            existing_dest = os.path.join(target, os.path.basename(asset_name))
            existing_partial = existing_dest + ".partial"
            try:
                if os.path.isfile(existing_dest) and os.path.getsize(existing_dest) == asset_size:
                    reusable_bytes = asset_size
                elif os.path.isfile(existing_partial) and asset_size:
                    reusable_bytes = min(os.path.getsize(existing_partial), asset_size)
            except OSError:
                reusable_bytes = 0
        needed = max(0, asset_size * 2 - reusable_bytes)
        if free < needed:
            return json_error(
                f"Not enough disk space: {needed // (1024 * 1024)} MB needed in "
                f"{target}, {free // (1024 * 1024)} MB free."
            )

        def work(job):
            try:
                from .taxonomy import DownloadCancelled
            except ImportError:
                from taxonomy import DownloadCancelled

            # byte_callback runs on the download thread inside the write loop,
            # so this must stay cheap — a queue put, never a DB write or
            # anything else that can stall the transfer.
            def on_bytes(done, total):
                ctx.runner.push_event(job["id"], "progress", progress_event(
                    phase="Downloading darktable",
                    current=done,
                    total=total or asset.get("size") or 0,
                    current_file=asset["name"],
                ))

            path, verify_detail = darktable_install.download(
                asset,
                byte_callback=on_bytes,
                should_cancel=lambda: ctx.runner.is_cancelled(job["id"]),
            )

            # total=0 on purpose: the UI treats a non-zero total as a byte
            # count and would render "Verifying: 0 of 0 MB" instead of the
            # verification detail. That detail is passed through verbatim —
            # it is the only thing that distinguishes "the digest matched"
            # from "GitHub published no digest to check against".
            ctx.runner.push_event(job["id"], "progress", progress_event(
                phase="Verifying", current=0, total=0, current_file=verify_detail,
            ))

            # A Stop press after verification and before hand_off would open
            # the installer anyway (Linux would also chmod the AppImage), then
            # jobs.py would still label the job "cancelled" — the user would
            # see cancellation but the side effect they cancelled happened.
            # begin_uncancellable is the atomic version of that check: it
            # returns False (leaving the cancel flag intact so _run_job
            # records the job as cancelled) if a cancel is already pending,
            # otherwise it enters the uninterruptible phase so a Cancel that
            # arrives during hand_off is rejected rather than flipping the
            # terminal status once the side effect has already committed.
            if not ctx.runner.begin_uncancellable(job["id"]):
                raise DownloadCancelled("Download cancelled")

            result = darktable_install.hand_off(path)

            # The single place config is mutated, right next to the field that
            # tells the user it happened. hand_off deliberately does not write
            # it. bin_path is only set where the download IS the binary
            # (Linux AppImage); after an installer hand-off the user chooses
            # where the app lands, so there is nothing truthful to record.
            #
            # Route the write through settings_write_lock and the raw
            # read-modify-write pattern the settings endpoints use.  cfg.set()
            # takes only config._lock, so a concurrent /api/config save (or an
            # autosave from the All-settings region) could otherwise interleave
            # its own read-modify-write with this one and either lose
            # darktable_bin or overwrite whatever field the user just changed.
            config_written = False
            if result.get("bin_path"):
                import config as cfg

                with settings_write_lock:
                    raw = read_raw_config_file()
                    raw["darktable_bin"] = result["bin_path"]
                    cfg.save(raw)
                config_written = True

            return {
                "version": asset["version"],
                "downloaded_to": path,
                "verified": verify_detail,
                "action": result["action"],
                "bin_path": result.get("bin_path"),
                "config_written": config_written,
                "quarantined": darktable_install.is_quarantined(path),
            }

        # Atomic singleton: the earlier check-then-start pattern (list_jobs()
        # followed by runner.start()) let two concurrent POSTs both see "no
        # existing job" and both start a worker on the same .partial.
        # start_singleton does the existence check and the registration under
        # one lock acquisition, so a second POST arriving between the check
        # and the start joins the first instead of racing it.
        #
        # The stored config carries the artifact identity so a later POST for
        # a DIFFERENT artifact (e.g. a new release published while an older
        # download is still in flight) can be rejected instead of quietly
        # joining a download of the wrong bytes.
        job_id, joined, existing_snapshot = ctx.runner.start_singleton(
            "download-darktable", work,
            singleton_key="darktable-download",
            config={
                "asset_name": asset["name"],
                "asset_version": asset["version"],
                "asset_digest": asset.get("digest"),
                "asset_size": asset.get("size"),
            },
            workspace_id=ctx.workspace_id,
        )
        if joined:
            # Only join a running download whose artifact matches what THIS
            # request confirmed. Without this check, a fresh POST for a new
            # release would silently receive an old release's bytes from an
            # already-running worker.
            existing_config = (existing_snapshot or {}).get("config") or {}
            if not _artifact_matches(existing_config):
                # Same reason as above: the cache may hold whatever asset the
                # first tab's download targeted, so a Re-check without this
                # refresh would re-render the stale identity and the user
                # would re-confirm it into the same 409.
                darktable_install.update_release_cache(asset)
                return json_error(
                    (
                        "A different darktable download is already in progress "
                        f"({existing_config.get('asset_name') or 'unknown'}). "
                        f"Confirmation showed {asset.get('name')}. Wait for the "
                        "in-flight download to finish (or cancel it) before "
                        "starting this one."
                    ),
                    status=409,
                    code="darktable_asset_changed",
                )
            return jsonify({"job_id": job_id, "joined_existing": True})
        return jsonify({"job_id": job_id})

    @blueprint.route("/api/labels/search-places")
    def api_labels_search_places():
        q = request.args.get("q", "")
        if len(q) < 2:
            return jsonify([])
        from labels import search_places

        return jsonify(search_places(q))

    @blueprint.route("/api/labels/taxon-groups")
    def api_labels_taxon_groups():
        from labels import TAXON_GROUPS

        return jsonify([{"key": k, "name": v["name"]} for k, v in TAXON_GROUPS.items()])

    @blueprint.route("/api/labels/observation-filters")
    def api_labels_observation_filters():
        from labels import OBSERVATION_FILTERS

        return jsonify([
            {"key": k, "name": v["name"], "description": v["description"]}
            for k, v in OBSERVATION_FILTERS.items()
        ])

    @blueprint.route("/api/labels")
    def api_labels_list():
        from labels import get_active_labels as get_global_active_labels
        from labels import get_saved_labels, label_set_summary

        def summarize(meta):
            """Drop the per-label identity map, keep what it implies.

            ``label_identities`` is megabytes on a regional list and the
            page never reads it — but the page must still say how many
            species this set contributes and how many of its names the
            classifier cannot use, so both counts travel in its place.
            ``species_count`` is what the file holds; ``usable_count`` is
            what a run receives, and they differ exactly when names are
            shared between species.
            """
            trimmed = {k: v for k, v in meta.items() if k != "label_identities"}
            path = meta.get("labels_file")
            if path and os.path.exists(path):
                try:
                    # Memoized on the files' size+mtime: this endpoint
                    # answers for every saved set on each Settings and
                    # Pipeline load, and normalizing a regional list is
                    # ~0.5s of work that only changes when the files do.
                    usable, skipped = label_set_summary(meta)
                    trimmed["usable_count"] = usable
                    trimmed["ambiguous_count"] = skipped
                except Exception:
                    log.warning(
                        "Could not inspect %s for ambiguous labels", path,
                        exc_info=True,
                    )
            return trimmed

        db = get_db()
        saved = [summarize(meta) for meta in get_saved_labels()]
        saved_by_file = {s["labels_file"]: s for s in saved if s.get("labels_file")}
        ws_labels = db.get_workspace_active_labels()
        if ws_labels is not None:
            # Resolve workspace labels to metadata
            active = []
            for p in ws_labels:
                if os.path.exists(p):
                    meta = saved_by_file.get(p, {"labels_file": p})
                    active.append(meta)
        else:
            # Same trimmed objects as ``saved`` so the identity map is not
            # re-attached through the active list. Look the entry up rather
            # than passing ``summarize(meta)`` as a ``get`` default: that
            # default is evaluated eagerly, so every active set would be
            # re-read and re-merged even when its trimmed copy is in hand.
            active = []
            for meta in get_global_active_labels():
                known = saved_by_file.get(meta.get("labels_file"))
                active.append(known if known is not None else summarize(meta))
        return jsonify(
            {
                "labels": saved,
                "active": active,
            }
        )

    @blueprint.route("/api/labels", methods=["DELETE"])
    def api_delete_labels():
        from labels import delete_labels

        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        labels_file = body.get("labels_file")
        if not labels_file:
            return json_error("labels_file required")
        try:
            delete_labels(labels_file)
        except ValueError as exc:
            return json_error(str(exc))
        # Every workspace that had this set selected, not just the active
        # one: a selection naming a deleted file blocks classification and
        # no checkbox can clear it, because the UI lists only files it can
        # find.
        get_db().forget_label_file(labels_file)
        return jsonify({"ok": True})

    @blueprint.route("/api/labels/active", methods=["POST"])
    def api_set_active_labels():
        body = request.get_json(silent=True) or {}
        # Accept new list format or old single-path format
        labels_files = body.get("labels_files")
        if labels_files is None:
            single = body.get("labels_file")
            if not single:
                return json_error("labels_files or labels_file required")
            labels_files = [single]
        db = get_db()
        db.set_workspace_active_labels(labels_files)
        return jsonify({"ok": True})

    @blueprint.route("/api/models/pipeline")
    def api_models_pipeline():
        """Return download status of all pipeline models (MegaDetector, SAM2, DINOv2)."""
        models_dir = pipeline_models_dir()
        models = []

        # MegaDetector — check for ONNX model in ~/.vireo/models/megadetector-v6/
        md_dir = os.path.join(models_dir, "megadetector-v6")
        md_onnx = os.path.join(md_dir, "model.onnx")
        md_status = "not downloaded"
        md_size = None
        if os.path.isfile(md_onnx):
            md_size = round(os.path.getsize(md_onnx) / 1024 / 1024, 1)
            md_status = "downloaded"
        models.append({
            "id": "megadetector-v6",
            "name": "MegaDetector V6",
            "role": "Detection",
            "description": "YOLOv9-c animal detector (ONNX)",
            "size_estimate": "~50 MB",
            "status": md_status,
            "size": f"{md_size} MB" if md_size else None,
        })

        # SAM2 variants — check for ONNX models in ~/.vireo/models/sam2-{variant}/
        sam2_variants = [
            ("sam2-tiny", "SAM2 Tiny", "~40 MB"),
            ("sam2-small", "SAM2 Small", "~150 MB"),
            ("sam2-base-plus", "SAM2 Base+", "~320 MB"),
            ("sam2-large", "SAM2 Large", "~900 MB"),
        ]
        for variant_id, name, size_est in sam2_variants:
            variant_dir = os.path.join(models_dir, variant_id)
            encoder_path = os.path.join(variant_dir, "image_encoder.onnx")
            decoder_path = os.path.join(variant_dir, "mask_decoder.onnx")
            status = "not downloaded"
            size = None
            if os.path.isfile(encoder_path) and os.path.isfile(decoder_path):
                total_size = os.path.getsize(encoder_path) + os.path.getsize(decoder_path)
                size = round(total_size / 1024 / 1024, 1)
                status = "downloaded"
            elif os.path.isfile(encoder_path) or os.path.isfile(decoder_path):
                status = "incomplete"
            models.append({
                "id": variant_id,
                "name": name,
                "role": "Segmentation",
                "description": f"SAM2 mask generation ({variant_id}, ONNX)",
                "size_estimate": size_est,
                "status": status,
                "size": f"{size} MB" if size else None,
            })

        # DINOv2 variants — check for ONNX models in ~/.vireo/models/dinov2-{variant}/
        dinov2_variants = [
            ("vit-s14", "DINOv2 ViT-S/14", "384-dim", "~85 MB"),
            ("vit-b14", "DINOv2 ViT-B/14", "768-dim", "~350 MB"),
            ("vit-l14", "DINOv2 ViT-L/14", "1024-dim", "~1.2 GB"),
        ]
        for variant_id, name, dims, size_est in dinov2_variants:
            variant_dir = os.path.join(models_dir, f"dinov2-{variant_id}")
            model_path = os.path.join(variant_dir, "model.onnx")
            data_path = model_path + ".data"
            status = "not downloaded"
            size = None
            # DINOv2 uses external-data ONNX: model.onnx is just the ~1 MB graph;
            # the real weights live in a model.onnx.data sidecar. Both must be
            # present for the model to load.
            if os.path.isfile(model_path) and os.path.isfile(data_path):
                total_bytes = os.path.getsize(model_path) + os.path.getsize(data_path)
                size = round(total_bytes / 1024 / 1024, 1)
                status = "downloaded"
            elif os.path.isfile(model_path) or os.path.isfile(data_path):
                status = "incomplete"
            models.append({
                "id": variant_id,
                "name": name,
                "role": "Embeddings",
                "description": f"{dims} embeddings for grouping (ONNX)",
                "size_estimate": size_est,
                "status": status,
                "size": f"{size} MB" if size else None,
            })

        return jsonify({"models": models})

    @blueprint.route("/api/models/pipeline/download", methods=["POST"])
    @background_job
    def api_models_pipeline_download(ctx):
        """Download a pipeline model (ONNX) by ID from jss367/vireo-onnx-models."""
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")

        if model_id not in PIPELINE_MODELS:
            return json_error(f"Unknown pipeline model: {model_id}")

        def work(job):
            import shutil

            from huggingface_hub import hf_hub_download
            from models import ONNX_REPO

            spec = PIPELINE_MODELS[model_id]
            subfolder = spec["subfolder"]
            files = spec["files"]
            model_dir = os.path.join(pipeline_models_dir(), subfolder)
            os.makedirs(model_dir, exist_ok=True)

            total = len(files)
            for fi, filename in enumerate(files):
                ctx.runner.push_event(job["id"], "progress", {
                    "phase": f"Downloading {fi + 1}/{total}: {filename}...",
                    "current": fi,
                    "total": total,
                })

                MAX_RETRIES = 3
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        cached = hf_hub_download(
                            repo_id=ONNX_REPO,
                            filename=filename,
                            subfolder=subfolder,
                        )
                        dest = os.path.join(model_dir, filename)
                        if cached != dest:
                            shutil.copy2(cached, dest)
                        break
                    except Exception as exc:
                        if attempt == MAX_RETRIES:
                            raise
                        wait = 2 ** attempt
                        import time
                        log.warning(
                            "Download attempt %d/%d for %s/%s failed (%s), "
                            "retrying in %ds...",
                            attempt, MAX_RETRIES, subfolder, filename, exc, wait,
                        )
                        ctx.runner.push_event(job["id"], "progress", {
                            "phase": f"Retrying {filename} in {wait}s "
                                     f"(attempt {attempt}/{MAX_RETRIES})...",
                            "current": fi, "total": total,
                        })
                        time.sleep(wait)

            ctx.runner.push_event(job["id"], "progress", {
                "phase": "Download complete", "current": total, "total": total,
            })
            return {"status": "downloaded", "model_id": model_id}

        # The job type embeds the model id, so it cannot be enumerated in
        # CATALOG_INDEPENDENT_JOB_TYPES. Downloading ONNX weights into
        # ~/.vireo/models touches no catalog row, so it must not hold Work
        # Locally hostage.
        return ctx.start(
            f"download-{model_id}", work, config={"model_id": model_id},
            blocks_local_transitions=False,
        )

    @blueprint.route("/api/models/pipeline/delete", methods=["POST"])
    def api_models_pipeline_delete():
        """Delete a pipeline model's ONNX files from ~/.vireo/models/.

        ``model_id`` must be a key of :data:`PIPELINE_MODELS`; the directory
        removed is that entry's ``subfolder``, never a path built from the
        request. A prefix check alone let ``sam2-large/../..`` resolve to the
        parent of the models directory and rmtree the whole Vireo data dir.
        """
        import shutil

        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id or not isinstance(model_id, str):
            return json_error("model_id required")
        spec = PIPELINE_MODELS.get(model_id)
        if spec is None:
            return json_error(f"Unknown pipeline model: {model_id}", status=404)

        model_dir = os.path.join(pipeline_models_dir(), spec["subfolder"])
        removed = []
        if os.path.isdir(model_dir):
            shutil.rmtree(model_dir)
            removed.append(model_dir)

        # Clear the cached session singletons so the next use reloads (or
        # reports the model as missing) instead of serving stale weights.
        if model_id == "megadetector-v6":
            import detector
            detector._session = None
        elif model_id.startswith("sam2-"):
            import masking
            masking._encoder_session = None
            masking._decoder_session = None
            masking._sam2_variant_loaded = None
        elif model_id.startswith("vit-"):
            import dino_embed as dinov2_mod
            dinov2_mod._session = None
            dinov2_mod._variant_loaded = None

        return jsonify({"deleted": removed, "count": len(removed), "model_id": model_id})

    @blueprint.route("/api/classify/readiness")
    def api_classify_readiness():
        """Check what's ready for classification and what will need work."""
        from classifier import _embedding_is_cached, _resolve_model_dir
        from labels import get_active_labels, get_saved_labels, load_label_set, load_merged_labels
        from models import get_active_model, get_models

        model_id = request.args.get("model_id", "")
        labels_file = request.args.get("labels_file", "")
        labels_files = request.args.getlist("labels_files")

        # Resolve model
        models = get_models()
        model = None
        if model_id:
            model = next((m for m in models if m["id"] == model_id), None)
        if not model:
            model = get_active_model()

        model_ready = bool(model and model.get("downloaded"))
        model_name = model["name"] if model else "None"
        model_size = model.get("size_mb", 0) if model else 0
        model_source = model.get("source", "") if model else ""
        model_type = model.get("model_type", "bioclip") if model else "bioclip"
        needs_download = not model_ready and (
            model_source.startswith("hf-hub:") or model_source == "timm"
        )

        from metadata import exiftool_status as get_exiftool_status
        from metadata import find_homebrew

        exiftool_probe = get_exiftool_status()
        exiftool_status = {
            "installed": exiftool_probe["available"],
            "version": exiftool_probe["version"],
            "bundled": bool(
                getattr(sys, "_MEIPASS", None) and exiftool_probe["available"]
            ),
            "brew_available": find_homebrew() is not None,
        }

        # timm models have a fixed class set — no labels needed
        if model_type == "timm":
            return jsonify(
                {
                    "model_name": model_name,
                    "model_ready": model_ready,
                    "model_size_mb": model_size,
                    "needs_download": needs_download,
                    "labels_name": "Built-in (10K iNat21 species)",
                    "labels_count": 10000,
                    "use_tol": False,
                    "embeddings_cached": True,  # no embeddings to compute
                    "exiftool": exiftool_status,
                }
            )

        # Resolve labels (BioCLIP path)
        use_tol = False
        label_count = 0
        label_name = ""
        labels = []
        labels_selected = False

        if labels_file:
            # Single file override from query param (classify page picker)
            if os.path.exists(labels_file):
                saved_meta = next(
                    (ls for ls in get_saved_labels()
                     if ls.get("labels_file") == labels_file), None,
                )
                labels = load_label_set(labels_file, saved_meta)
                label_count = len(labels)
                labels_selected = True
                if saved_meta:
                    label_name = saved_meta.get("name", labels_file)
        elif labels_files:
            # Multiple files override from query param
            active_sets = []
            saved = get_saved_labels()
            saved_by_file = {s["labels_file"]: s for s in saved}
            for p in labels_files:
                meta = saved_by_file.get(p, {"labels_file": p})
                active_sets.append(meta)
            labels = load_merged_labels(active_sets)
            label_count = len(labels)
            # Same rule as classify_job._any_present: an open tab can send
            # a path deleted since it rendered, and that is a fallback, not
            # a refusal — readiness must not promise a block the job will
            # not perform.
            labels_selected = any(
                os.path.exists(ls.get("labels_file", "")) for ls in active_sets
            )
            names = [s.get("name", os.path.basename(s["labels_file"])) for s in active_sets]
            label_name = ", ".join(names)
        else:
            db = get_db()
            ws_labels = db.get_workspace_active_labels()
            if ws_labels is not None:
                saved_by_file = {s["labels_file"]: s for s in get_saved_labels()}
                active_sets = [saved_by_file.get(p, {"labels_file": p}) for p in ws_labels if os.path.exists(p)]
            else:
                active_sets = get_active_labels()
            if active_sets:
                labels = load_merged_labels(active_sets)
                label_count = len(labels)
                labels_selected = True
                names = [s.get("name", os.path.basename(s["labels_file"])) for s in active_sets]
                label_name = ", ".join(names)
            else:
                from models import supports_tree_of_life, tree_of_life_ready
                model_str_check = model.get("model_str", "") if model else ""
                model_dir_check = model.get("weights_path") if model else None
                if tree_of_life_ready(model_str_check, model_dir_check):
                    use_tol = True
                    label_name = "Tree of Life (all species)"
                elif supports_tree_of_life(model_str_check):
                    # ToL-capable model whose artifacts aren't installed
                    # yet (e.g. bioclip-2.5 before its HF upload lands).
                    # Tell the user what's missing rather than falsely
                    # advertising ToL-ready and crashing at classify time.
                    label_name = (
                        "Tree of Life files not installed — click Repair "
                        "in Settings → Models, or download a species list"
                    )
                else:
                    label_name = "No labels — download a species list in Settings"

        # A selection that classifies nothing is not "no labels":
        # classify_job raises and the planner marks it blocked, so the
        # preflight panel has to say so too rather than rendering nothing.
        # ``labels_selected`` separates that from the genuine no-selection
        # case, which still falls back to Tree of Life.
        labels_skipped = len(getattr(labels, "dropped_ambiguous", ()))
        labels_blocked = bool(labels_selected and not labels and not use_tol)

        # Check embedding cache
        embeddings_cached = False
        if model and not use_tol and labels:
            model_dir = _resolve_model_dir(
                model.get("model_str", ""), model.get("weights_path")
            )
            embeddings_cached = _embedding_is_cached(
                labels, model.get("model_str", ""), model_dir
            )

        return jsonify(
            {
                "model_name": model_name,
                "model_ready": model_ready,
                "model_size_mb": model_size,
                "needs_download": needs_download,
                "labels_name": label_name,
                "labels_count": label_count,
                "labels_blocked": labels_blocked,
                "labels_skipped": labels_skipped,
                "use_tol": use_tol,
                "embeddings_cached": embeddings_cached,
                "exiftool": exiftool_status,
            }
        )

    @blueprint.route("/api/classify/config")
    def api_classify_config():
        """Return classifier configuration from model registry."""
        import config as cfg
        from models import get_active_model, get_taxonomy_info

        active = get_active_model()
        tax = get_taxonomy_info()
        user_cfg = get_db().get_effective_config(cfg.load())
        return jsonify(
            {
                "model_name": active["name"] if active else "No model",
                "model_str": active["model_str"] if active else "",
                "weights_path": active["weights_path"] if active else "",
                "weights_available": active["downloaded"] if active else False,
                "taxonomy_available": tax["available"],
                "taxonomy_species_count": count_keywords(),
                "default_threshold": user_cfg["classification_threshold"],
                "default_grouping_window": user_cfg["grouping_window_seconds"],
                "default_similarity_threshold": user_cfg.get(
                    "similarity_threshold", 0.85
                ),
            }
        )

    @blueprint.route("/api/embedding-cache")
    def api_embedding_cache():
        """Return info about cached label embeddings."""
        from classifier import CACHE_DIR

        if not os.path.isdir(CACHE_DIR):
            return jsonify({"entries": [], "total_size": 0})
        from embedding_cache import CHECKPOINT_SUFFIX

        entries = []
        total_size = 0
        for f in sorted(os.listdir(CACHE_DIR)):
            if f.endswith(CHECKPOINT_SUFFIX):
                # Partial progress from a paused or interrupted
                # computation, not a usable embedding set.
                continue
            if f.endswith(".npy") or f.endswith(".pt"):
                fp = os.path.join(CACHE_DIR, f)
                size = os.path.getsize(fp)
                total_size += size
                entries.append({"file": f, "size": size})
        # Reusable individual labels live below their encoder identity.
        # Aggregate them instead of shipping thousands of filenames to Settings.
        label_size = 0
        label_count = 0
        for root, _dirs, files in os.walk(os.path.join(CACHE_DIR, "labels")):
            for name in files:
                if name.endswith(".npy") and not name.endswith(CHECKPOINT_SUFFIX):
                    try:
                        label_size += os.path.getsize(os.path.join(root, name))
                        label_count += 1
                    except FileNotFoundError:
                        pass  # a concurrent cache clear won
        if label_count:
            entries.append({"file": "Reusable species labels", "size": label_size, "label_count": label_count})
            total_size += label_size
        return jsonify({"entries": entries, "total_size": total_size})

    @blueprint.route("/api/embedding-matrix")
    def api_embedding_matrix():
        """Return which model+labels combinations have cached embeddings."""
        from classifier import _embedding_is_cached, _resolve_model_dir
        from labels import get_saved_labels, normalized_label_set
        from models import get_models

        # Only BioCLIP-style models use per-label text embeddings. timm models
        # have a fixed class head and never need embedding precomputation, so
        # excluding them here prevents the Settings UI from offering a
        # "Compute" button that would fail with a missing-file error.
        models = [
            m for m in get_models()
            if m["downloaded"] and m.get("model_type", "bioclip") != "timm"
        ]
        label_sets = get_saved_labels()

        matrix = []
        for ls in label_sets:
            labels_file = ls.get("labels_file", "")
            if not labels_file or not os.path.exists(labels_file):
                continue
            # Cached on the files' stamps: both Settings and Storage load
            # this matrix, and it needs every saved set's full list to ask
            # whether its embeddings are cached.
            labels = normalized_label_set(ls)
            row = {
                "labels_name": ls.get("name", ""),
                "labels_file": labels_file,
                "species_count": len(labels),
                # No prompt survived, so there is nothing to embed: the row
                # stays visible (the set is on disk and the user is looking
                # for it) but says why instead of offering a Compute button
                # whose job can only fail.
                "unusable": not labels,
                "skipped": len(getattr(labels, "dropped_ambiguous", ())),
                "models": {},
            }
            for m in models:
                model_dir = _resolve_model_dir(m["model_str"], m.get("weights_path"))
                row["models"][m["id"]] = {
                    "cached": _embedding_is_cached(
                        labels, m["model_str"], model_dir
                    ),
                    "model_name": m["name"],
                }
            matrix.append(row)

        return jsonify(
            {
                "models": [{"id": m["id"], "name": m["name"]} for m in models],
                "matrix": matrix,
            }
        )

    @blueprint.route("/api/embedding-cache", methods=["DELETE"])
    def api_embedding_cache_clear():
        """Clear all cached label embeddings."""
        import shutil

        from classifier import CACHE_DIR

        if os.path.isdir(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
            log.info("Embedding cache cleared")
        return jsonify({"ok": True})

    @blueprint.route("/api/megadetector/download", methods=["POST"])
    @background_job
    def api_megadetector_download(ctx):
        """Download MegaDetector ONNX model as a background job."""

        def work(job):
            from detector import MEGADETECTOR_ONNX_DIR, MEGADETECTOR_ONNX_PATH
            from huggingface_hub import hf_hub_download
            from models import ONNX_REPO

            os.makedirs(MEGADETECTOR_ONNX_DIR, exist_ok=True)

            ctx.runner.push_event(job["id"], "progress", {
                "phase": "Downloading MegaDetector ONNX model...",
                "current": 0, "total": 1,
            })

            import shutil

            cached_path = hf_hub_download(
                repo_id=ONNX_REPO,
                filename="model.onnx",
                subfolder="megadetector-v6",
            )

            dest = MEGADETECTOR_ONNX_PATH
            if cached_path != dest:
                shutil.copy2(cached_path, dest)

            if not os.path.isfile(dest):
                raise RuntimeError("Download completed but ONNX file not found")

            size_mb = round(os.path.getsize(dest) / 1024 / 1024, 1)
            return {"status": "downloaded", "size": f"{size_mb} MB", "path": dest}

        return ctx.start("download-megadetector", work)

    @blueprint.route("/api/megadetector/delete", methods=["POST"])
    def api_megadetector_delete():
        """Delete MegaDetector ONNX model from disk."""
        import shutil

        import detector
        from detector import MEGADETECTOR_ONNX_DIR

        removed = []
        if os.path.isdir(MEGADETECTOR_ONNX_DIR):
            shutil.rmtree(MEGADETECTOR_ONNX_DIR)
            removed.append(MEGADETECTOR_ONNX_DIR)

        # Clear the cached singleton so it reloads next time
        detector._session = None

        return jsonify({"deleted": removed, "count": len(removed)})

    return blueprint
