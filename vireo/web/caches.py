"""Derived-data caches and results.

The preview cache (``/api/preview-cache``), the portable computation cache
(``/api/computation-cache`` status, export and import), the global detector
cache stats (``/api/detection-cache/stats``), a photo's stored detections
(``/api/detections/<id>``), and the cached culling analysis with its apply
step (``/api/culling/*``). Everything here reads or clears data Vireo derived
from photos; nothing here downloads or configures a model (that is
``web.models``).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile

from flask import Blueprint, jsonify, make_response, request
from photo_payload import attach_nested_edit_recipes

log = logging.getLogger(__name__)


def create_caches_blueprint(get_db, json_error, db_path, config):
    """Build the derived-caches blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR`` for the
    previews directory, ``COMPUTATION_CACHE_DIR`` for the artifact store),
    read when a request runs rather than when the app is built. ``db_path``
    locates the per-workspace culling results file beside the catalog.
    """
    blueprint = Blueprint("caches", __name__)

    @blueprint.route("/api/detections/<int:photo_id>")
    def api_detections(photo_id):
        """Get all detections for a photo."""
        db = get_db()
        # Detections are global, but exposing them for photos outside the
        # active workspace leaks data the workspace deliberately hides
        # (mirrors serve_thumbnail's workspace gate).
        if db.get_photo(photo_id, verify_workspace=True) is None:
            return json_error("not found", 404)
        dets = db.get_detections(photo_id)
        return jsonify([dict(d) for d in dets])

    def _computation_store():
        from computation_cache import ArtifactStore

        return ArtifactStore(config["COMPUTATION_CACHE_DIR"])

    @blueprint.route("/api/computation-cache")
    def api_computation_cache_status():
        """Return local portable-object and currently exportable-run counts."""
        from computation_cache import exportable_run_counts

        export_summary = exportable_run_counts(get_db())
        return jsonify({
            **_computation_store().stats(),
            "exportable": export_summary,
            "experimental": True,
            "artifact_schema": 1,
        })

    @blueprint.route("/api/computation-cache/export")
    def api_computation_cache_export():
        """Download portable detector/classifier results as a cache bundle."""
        import datetime as _datetime

        from computation_cache import exportable_artifacts, write_bundle

        requested = request.args.get("types", "detection,classification")
        artifact_types = {part.strip() for part in requested.split(",") if part.strip()}
        # exportable_artifacts dependency-closes the copy set — a classifier
        # export always drags its detector dependencies along.  Mirror that
        # expansion here so detector artifacts forwarded from the local
        # object store (e.g. imports whose photos were not yet cataloged
        # when they landed) travel with the classifications that reference
        # them; otherwise the destination has to reproduce detection from
        # model weights it may not have.
        if "classification" in artifact_types:
            artifact_types = artifact_types | {"detection"}
        try:
            database_artifacts, summary = exportable_artifacts(
                get_db(), artifact_types=artifact_types,
            )
            stored_artifacts = [
                artifact for _digest, artifact
                in (_computation_store().iter_artifacts() or ())
                if artifact["type"] in artifact_types
            ]
            fd, temp_path = tempfile.mkstemp(suffix=".vireo-cache")
            os.close(fd)
            try:
                manifest = write_bundle(
                    temp_path,
                    [*database_artifacts, *stored_artifacts],
                    device_label=request.args.get("device_label") or None,
                )
                with open(temp_path, "rb") as handle:
                    body = handle.read()
            finally:
                with contextlib.suppress(OSError):
                    os.unlink(temp_path)
        except ValueError as exc:
            return json_error(str(exc), status=400)

        today = _datetime.date.today().isoformat()
        response = make_response(body)
        response.headers["Content-Type"] = "application/vnd.vireo.cache+zip"
        response.headers["Content-Disposition"] = (
            f'attachment; filename="vireo-results-{today}.vireo-cache"'
        )
        response.headers["X-Vireo-Cache-Objects"] = str(manifest["object_count"])
        response.headers["X-Vireo-Cache-Skipped-Legacy"] = str(
            summary["skipped_legacy"]
        )
        return response

    @blueprint.route("/api/computation-cache/import", methods=["POST"])
    def api_computation_cache_import():
        """Validate, store, and immediately apply an uploaded cache bundle."""
        import zipfile

        from computation_cache import (
            CacheFormatError,
            import_bundle,
            materialize_local_store,
        )

        upload = request.files.get("file")
        if upload is None or not upload.filename:
            return json_error("a .vireo-cache file is required", status=400)
        try:
            store = _computation_store()
            # Stream the upload through the store so a large bundle never
            # accumulates every parsed artifact in Python at once — the
            # returned "artifacts" field is intentionally empty and
            # materialization reads back from the store's on-disk objects.
            imported = import_bundle(upload.stream, store)
            # An explicit user-initiated import is a trust action for the
            # runtimes this bundle carries: quarantine gates for unknown
            # detector/classifier runtimes exist to keep drive-by
            # materialize calls from surfacing foreign inference, but
            # here the user chose the source. Whitelist the artifact-
            # supplied fingerprints so the objects they just uploaded
            # actually plant instead of getting stranded in the store.
            detector_rts = imported["detector_runtimes"]
            classifier_rts = imported["classifier_runtimes"]
            # Persist the trust so later materialize_local_store calls
            # from run_classify_job / the pipeline accept these runtimes
            # too. Without persistence, a bundle imported before its
            # matching photos are cataloged would leave those artifacts
            # quarantined forever — the one-shot whitelist below only
            # covers this HTTP call.
            store.record_trusted_runtimes(
                detector_runtimes=detector_rts,
                classifier_runtimes=classifier_rts,
            )
            applied = materialize_local_store(
                get_db(), store=store,
                known_runtimes=detector_rts,
                known_classifier_runtimes=classifier_rts,
            )
        except (CacheFormatError, zipfile.BadZipFile) as exc:
            return json_error(str(exc), status=400)
        return jsonify({
            "ok": True,
            "objects": imported["manifest"]["object_count"],
            "added": imported["added"],
            "already_present": imported["already_present"],
            **applied,
        })

    def _recommended_preview_cache_mb(db):
        """Recommend a quota that fits one preview per photo across the
        whole library.

        Uses the measured average bytes-per-preview from the existing
        ``preview_cache`` rows when available, otherwise falls back to
        500 KB (a typical 1920px JPEG at quality 90). Photos are global
        across workspaces so this is a library-wide count, not
        workspace-scoped.

        Returns 0 when the photos table is empty (avoids a "recommended
        0 MB" surprise on a fresh install).
        """
        photo_count = db.conn.execute(
            "SELECT COUNT(*) FROM photos"
        ).fetchone()[0]
        if not photo_count:
            return 0
        avg_row = db.conn.execute(
            "SELECT AVG(bytes) AS a FROM preview_cache WHERE bytes > 0"
        ).fetchone()
        avg_bytes = (avg_row["a"] if avg_row and avg_row["a"] else 500 * 1024)
        recommended_bytes = photo_count * avg_bytes
        return max(1, int(recommended_bytes / 1024 / 1024) + 1)

    @blueprint.route("/api/preview-cache")
    def api_preview_cache():
        """Return counts and totals from the preview_cache table, plus quota."""
        import config as cfg
        db = get_db()
        count_row = db.conn.execute(
            "SELECT COUNT(*) AS c FROM preview_cache"
        ).fetchone()
        total = db.preview_cache_total_bytes()
        quota_mb = cfg.load().get("preview_cache_max_mb", 20480)
        return jsonify({
            "count": count_row["c"],
            "total_size": total,
            "quota_bytes": int(quota_mb) * 1024 * 1024,
            "recommended_mb": _recommended_preview_cache_mb(db),
        })

    @blueprint.route("/api/preview-cache/clear", methods=["POST"])
    def api_preview_cache_clear():
        """Delete every preview_cache file and row, including legacy and
        untracked files in the previews directory.

        Tracked rows whose on-disk files couldn't be unlinked (e.g. a
        locked or permission-restricted file) are kept so accounting
        and future eviction still reflect the leaked bytes — otherwise
        /api/preview-cache would under-report and eviction would stop
        targeting them.
        """
        import re

        db = get_db()
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(vireo_dir, "previews")

        count_row = db.conn.execute(
            "SELECT COUNT(*) AS c FROM preview_cache"
        ).fetchone()
        tracked = count_row["c"]

        # Matches {id}.jpg (legacy /full cache) and {id}_{size}.jpg (current).
        pattern = re.compile(r"^(\d+)(?:_(\d+))?\.jpg$")
        sized_pat = re.compile(r"^(\d+)_(\d+)\.jpg$")
        files_removed = 0
        failed_tracked = []  # (photo_id, size) tuples for sized files we couldn't unlink
        if os.path.isdir(preview_dir):
            for fname in os.listdir(preview_dir):
                if not pattern.match(fname):
                    continue
                try:
                    os.remove(os.path.join(preview_dir, fname))
                    files_removed += 1
                except OSError:
                    m = sized_pat.match(fname)
                    if m:
                        failed_tracked.append((int(m.group(1)), int(m.group(2))))

        if failed_tracked:
            # Stage failed keys in a temp table so the DELETE isn't a giant
            # NOT IN clause that blows past SQLite's default variable limit
            # (~999) with a few hundred unlinkable files. Insert in chunks
            # of 400 pairs (800 bind parameters) for the same reason.
            db.conn.execute(
                "CREATE TEMP TABLE _pc_failed (photo_id INTEGER, size INTEGER)"
            )
            try:
                CHUNK = 400
                for i in range(0, len(failed_tracked), CHUNK):
                    batch = failed_tracked[i:i + CHUNK]
                    placeholders = ",".join(["(?,?)"] * len(batch))
                    flat = [v for pair in batch for v in pair]
                    db.conn.execute(
                        f"INSERT INTO _pc_failed (photo_id, size) VALUES {placeholders}",
                        flat,
                    )
                db.conn.execute(
                    "DELETE FROM preview_cache WHERE (photo_id, size) NOT IN "
                    "(SELECT photo_id, size FROM _pc_failed)"
                )
            finally:
                db.conn.execute("DROP TABLE _pc_failed")
        else:
            db.conn.execute("DELETE FROM preview_cache")
        db.conn.commit()

        remaining = db.conn.execute(
            "SELECT COUNT(*) AS c FROM preview_cache"
        ).fetchone()["c"]
        cleared = tracked - remaining

        return jsonify({
            "cleared": cleared,
            "files_removed": files_removed,
            "failed": len(failed_tracked),
        })

    @blueprint.route("/api/detection-cache/stats")
    def api_detection_cache_stats():
        """Return global detector-cache stats for the settings page.

        `detector_runs` is shared across workspaces, so the numbers do
        not depend on the active workspace.
        """
        db = get_db()
        return jsonify(db.get_global_detection_stats())

    @blueprint.route("/api/culling/results")
    def api_culling_results():
        """Return the most recent culling analysis results for the active workspace."""
        db = get_db()
        cache_path = os.path.join(os.path.dirname(db_path), f"culling_results_ws{db._active_workspace_id}.json")
        if not os.path.exists(cache_path):
            return json_error("No culling analysis found. Run one first.", 404)

        with open(cache_path) as f:
            results = json.load(f)

        # Enrich with photo metadata for the UI
        db = get_db()
        for sg in results["species_groups"]:
            for pg in sg.get("scene_groups", sg.get("pose_groups", [])):
                for photo in pg["photos"]:
                    p = db.get_photo(photo["photo_id"])
                    if p:
                        photo["filename"] = p["filename"]
                        photo["sharpness"] = p["sharpness"]
                        photo["subject_sharpness"] = p["subject_sharpness"]
                        photo["quality_score"] = p["quality_score"]
        attach_nested_edit_recipes(db, results)

        return jsonify(results)

    @blueprint.route("/api/culling/apply", methods=["POST"])
    def api_culling_apply():
        """Apply culling decisions — flag keepers and reject others.

        ``unflag`` carries the photos the user explicitly moved back to
        Review on the cull page. Without it those photos would keep a stale
        "flagged"/"rejected" flag from an earlier apply, and the cull page
        would show that old flag back on the card the moment the decision
        stopped being a session override — a silently reverted decision.
        Only ids the client sends are touched, never every REVIEW photo.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        keepers = body.get("keepers", [])
        rejects = body.get("rejects", [])
        unflag = body.get("unflag", [])

        for name, value in (("keepers", keepers), ("rejects", rejects),
                            ("unflag", unflag)):
            if not isinstance(value, list):
                return json_error(f"{name} must be a list")

        # A photo listed in more than one action would otherwise land on
        # whichever mutation ran last (or whichever kept its old flag), so
        # the endpoint's answer would depend on prior state. Reject the
        # payload before touching anything.
        overlaps = (
            (set(keepers) & set(rejects), "keepers", "rejects"),
            (set(keepers) & set(unflag), "keepers", "unflag"),
            (set(rejects) & set(unflag), "rejects", "unflag"),
        )
        for shared, a, b in overlaps:
            if shared:
                pid = next(iter(sorted(shared)))
                return json_error(
                    f"Photo {pid} listed in both {a} and {b}", 400
                )

        # Pre-validate all photo IDs against workspace before any mutations
        for pid in keepers + rejects + unflag:
            if not db._photo_in_workspace(pid):
                return json_error(f"Photo {pid} is not in the active workspace", 403)

        # Capture old flags before mutation
        old_flags = {}
        for pid in keepers + rejects + unflag:
            old = db.get_photo(pid)
            if old:
                old_flags[pid] = old["flag"] or "none"

        # Clearing a flag that is already "none" would write a no-op history
        # entry, so only the photos that actually carry a flag are cleared.
        cleared = [pid for pid in unflag if old_flags.get(pid, "none") != "none"]

        try:
            for pid in keepers:
                db.update_photo_flag(pid, "flagged")
            for pid in rejects:
                db.update_photo_flag(pid, "rejected")
            for pid in cleared:
                db.update_photo_flag(pid, "none")
        except ValueError as e:
            return json_error(str(e), 403)

        # Record flag history
        flag_items = []
        for pid in keepers:
            if pid in old_flags:
                flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'flagged'})
        for pid in rejects:
            if pid in old_flags:
                flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'rejected'})
        for pid in cleared:
            flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'none'})
        if flag_items:
            for item in flag_items:
                db.queue_flag_change_if_enabled(
                    item["photo_id"], item["new_value"], _commit=False
                )
            db.conn.commit()
            summary = f'Culling: flagged {len(keepers)}, rejected {len(rejects)}'
            if cleared:
                summary += f', cleared {len(cleared)}'
            db.record_edit('flag', summary, 'culling_apply', flag_items, is_batch=True)

        log.info(
            "Culling applied: %d keepers, %d rejects, %d cleared",
            len(keepers), len(rejects), len(cleared),
        )
        return jsonify({
            "ok": True,
            "keepers": len(keepers),
            "rejects": len(rejects),
            "cleared": len(cleared),
        })

    return blueprint
