"""Settings: the global config file, per-workspace overrides, backup and restore.

``/api/config`` is the curated settings form's full-snapshot endpoint; the
``/api/settings/*`` routes back the schema-rendered settings UI (one key per
request) and the settings export/import. Every global-config write holds
``settings_write_lock`` and ends in the same post-save side effects, so the
routes stay behaviorally identical whichever path saved the value.
"""

from __future__ import annotations

import json
import logging
import os

import filter_shortcuts
from config import read_raw_config_file, settings_write_lock
from db import commit_with_retry
from flask import Blueprint, g, jsonify, make_response, request
from preview_cache import (
    evict_if_over_quota as evict_preview_cache_if_over_quota,
)
from working_copy_cache import (
    arrange_deferred_over_quota_retry as arrange_deferred_working_copy_quota_retry,
)
from working_copy_cache import (
    evict_if_over_quota as evict_working_copy_cache_if_over_quota,
)
from working_copy_cache import working_copy_publication_guard, working_copy_stats

log = logging.getLogger(__name__)

LOCATION_KEYWORDS_SETTING = "write_location_keywords_to_xmp"


def queue_location_keyword_cleanup_for_workspace(db, workspace_id):
    """Queue ``location`` changes for every located photo in one workspace.

    Runs the workspace-scoped backfill under an explicit active-workspace
    switch so the caller's active workspace is left untouched.
    """
    saved_active = db._active_workspace_id
    try:
        db._active_workspace_id = int(workspace_id)
        db.queue_location_changes_for_tagged_photos()
    except Exception:
        log.warning(
            "Failed to queue location cleanup for workspace %s",
            workspace_id, exc_info=True,
        )
    finally:
        db._active_workspace_id = saved_active


def workspace_effective_setting(raw_override, global_cfg, key):
    """Resolve a workspace's effective boolean setting.

    ``raw_override`` is the ``workspaces.config_overrides`` column value
    (JSON string, dict, or None). If the workspace defines its own value
    for ``key``, that wins; otherwise the global config value is used.
    Kept boolean-only because the location-keywords cleanup transition
    check is boolean-valued; a future generalization would need to widen
    the return type.
    """
    overrides = None
    if raw_override:
        try:
            parsed = (
                json.loads(raw_override) if isinstance(raw_override, str)
                else raw_override
            )
            if isinstance(parsed, dict):
                overrides = parsed
        except (json.JSONDecodeError, TypeError):
            overrides = None
    if overrides is not None and key in overrides:
        return bool(overrides[key])
    return bool((global_cfg or {}).get(key, False))


def queue_location_keyword_cleanup_on_global_off(
    db, previous_global, current_global,
):
    """Queue cleanup in every workspace whose effective setting flipped off.

    ``write_location_keywords_to_xmp`` promises in its own description
    that turning it off removes the keywords Vireo wrote on the next
    sync. But ``sync_to_xmp`` only visits photos with a queued row --
    after a successful write the previous run's ``location`` row is
    gone, so a bare setting flip would strand the sidecars until the
    user reassigned each place by hand or ran the Settings backfill.
    Detect the True → False transition per workspace here (workspace
    overrides win over the global) and queue the affected photos.
    """
    key = LOCATION_KEYWORDS_SETTING
    prev_global_val = bool((previous_global or {}).get(key, False))
    cur_global_val = bool((current_global or {}).get(key, False))
    # A workspace with its own override for this key is not affected
    # by a global toggle -- its effective value doesn't change.
    if prev_global_val == cur_global_val:
        return
    try:
        workspaces = db.get_workspaces()
    except Exception:
        log.warning(
            "Failed to enumerate workspaces for %s cleanup check",
            key, exc_info=True,
        )
        return
    for ws in workspaces:
        override = None
        raw_override = ws["config_overrides"]
        if raw_override:
            try:
                parsed = (
                    json.loads(raw_override) if isinstance(raw_override, str)
                    else raw_override
                )
                if isinstance(parsed, dict) and key in parsed:
                    override = bool(parsed[key])
            except (json.JSONDecodeError, TypeError):
                pass
        prev_effective = (
            override if override is not None else prev_global_val
        )
        cur_effective = (
            override if override is not None else cur_global_val
        )
        if prev_effective and not cur_effective:
            queue_location_keyword_cleanup_for_workspace(db, ws["id"])


def read_workspace_overrides(db):
    """Return the active workspace's config_overrides as a dict (or {}).

    Coerces non-dict payloads (possible via legacy workspace
    create/update APIs) to ``{}`` so dotted-key mutation in the schema
    write paths can't crash on a malformed override.
    """
    ws = db.get_workspace(db._active_workspace_id)
    if not ws or not ws["config_overrides"]:
        return {}
    try:
        raw = ws["config_overrides"]
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}

def write_workspace_overrides(db, overrides):
    db.update_workspace(
        db._active_workspace_id,
        config_overrides=overrides if overrides else None,
    )


def create_settings_blueprint(
    get_db,
    json_error,
    config,
    *,
    advance_inat_token_generation,
):
    """Build the settings blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``,
    ``DB_PATH``), read when a request runs rather than when the app is built.
    ``config.read_raw_config_file`` / ``config.settings_write_lock`` are
    shared with every other route that writes ``config.json``.
    ``advance_inat_token_generation`` invalidates
    in-flight iNaturalist token validations whenever a settings write changes
    the token. It is the bound ``advance`` of the app's one
    ``web.inat.InatTokenGeneration``, shared with the iNaturalist routes,
    and is only called while holding ``settings_write_lock``.
    """
    blueprint = Blueprint("settings", __name__)

    def _working_copy_quota_confirmation_required(
        previous, requested_quota_mb, confirmed=False,
    ):
        """Return a 409 response for an unconfirmed quota reduction.

        Every global-config mutation path calls this while holding
        ``settings_write_lock`` and before writing the new value. Keeping the
        check here prevents schema PATCH/DELETE and settings import from
        bypassing the Storage page's eviction warning.

        A legacy config with an invalid ``working_copy_cache_max_mb`` (possibly
        left by a hand-edit or an older, unvalidated ``/api/config`` write)
        must not skip the gate: ``_settings_post_save_side_effects`` interprets
        an unparseable stored value as the 20480 MB runtime default, so a
        lower request would still evict working copies. Fall back to the same
        default here so the confirmation invariant holds against those legacy
        configs.
        """
        try:
            previous_quota_mb = int(
                previous.get("working_copy_cache_max_mb", 20480)
            )
        except (TypeError, ValueError):
            previous_quota_mb = 20480
        try:
            requested_quota_mb = int(requested_quota_mb)
        except (TypeError, ValueError):
            return None
        if requested_quota_mb >= previous_quota_mb or confirmed is True:
            return None

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        # Measure usage under the publication guard so the value returned to
        # the client is a snapshot no concurrent publisher is mutating.
        with working_copy_publication_guard():
            usage = working_copy_stats(vireo_dir, quota_mb=requested_quota_mb)
        return jsonify({
            "ok": False,
            "error": "working-copy quota reduction requires confirmation",
            "code": "working_copy_eviction_confirmation_required",
            "request_id": getattr(g, "request_id", None),
            "previous_working_copy_quota_mb": previous_quota_mb,
            "requested_working_copy_quota_mb": requested_quota_mb,
            "current_working_copy_usage_bytes": usage["size"],
        }), 409

    def _settings_post_save_side_effects(current, previous=None):
        """Side effects mirrored from the legacy /api/config POST handler.

        Keeps every global settings write behaviorally identical to the old
        curated-form save: HF_TOKEN stays in sync, cache budgets take effect
        immediately, and raising the working-copy ceiling makes deliberately
        evicted rows eligible for a future backfill again.
        """
        hf_token = current.get("hf_token", "")
        if hf_token:
            os.environ["HF_TOKEN"] = hf_token
        elif "HF_TOKEN" in os.environ:
            del os.environ["HF_TOKEN"]

        previous = previous or {}
        try:
            previous_working_copy_quota = int(
                previous.get("working_copy_cache_max_mb", 20480)
            )
        except (TypeError, ValueError):
            previous_working_copy_quota = 20480
        try:
            current_working_copy_quota = int(
                current.get("working_copy_cache_max_mb", 20480)
            )
        except (TypeError, ValueError):
            current_working_copy_quota = 20480
        quota_db = get_db()
        if current_working_copy_quota > previous_working_copy_quota:
            # The next scoped scan or startup backfill can use the newly
            # available space; settings writes never perform RAW decoding.
            # Acquire the publication guard so this clear serializes with
            # concurrent on-demand extractions that decide cacheability
            # under the same guard. Without it, an in-flight non-cacheable
            # commit that reads the still-stale request-start budget could
            # stamp a fresh ``working_copy_evicted_mtime`` after this
            # UPDATE runs, leaving the row permanently ineligible for
            # backfill under the raised ceiling.
            with working_copy_publication_guard():
                quota_db.conn.execute(
                    "UPDATE photos SET working_copy_evicted_mtime=NULL, "
                    "working_copy_failed_at=CASE WHEN "
                    "working_copy_failed_source='source' "
                    "AND companion_path IS NOT NULL THEN NULL "
                    "ELSE working_copy_failed_at END, "
                    "working_copy_failed_mtime=CASE WHEN "
                    "working_copy_failed_source='source' "
                    "AND companion_path IS NOT NULL THEN NULL "
                    "ELSE working_copy_failed_mtime END, "
                    "working_copy_failed_source=CASE WHEN "
                    "working_copy_failed_source='source' "
                    "AND companion_path IS NOT NULL THEN NULL "
                    "ELSE working_copy_failed_source END "
                    "WHERE working_copy_evicted_mtime IS NOT NULL"
                )
                commit_with_retry(quota_db.conn)

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        evict_preview_cache_if_over_quota(quota_db, vireo_dir)
        if current_working_copy_quota < previous_working_copy_quota:
            # Cache writers and startup already enforce an unchanged quota.
            # Avoid scanning every photo and working-copy file while holding
            # the publication lock for unrelated settings saves.
            eviction_result = evict_working_copy_cache_if_over_quota(
                quota_db, vireo_dir,
            )
            if eviction_result.get("deferred"):
                # Snapshot retries exhausted under catalog contention.
                # Without a scheduled follow-up, the cache would sit above
                # the new ceiling until an unrelated cache write or
                # restart eventually re-ran enforcement. Spawn a bounded
                # background retry that opens its own SQLite connection
                # (Database handles are thread-affine).
                arrange_deferred_working_copy_quota_retry(
                    config["DB_PATH"], vireo_dir,
                )

        # A global toggle from on → off must honor the setting's own
        # description: "Turning this off removes the keywords Vireo wrote
        # on the next sync." Sync only visits queued photos, so queue them
        # here for every workspace whose effective value transitioned off.
        queue_location_keyword_cleanup_on_global_off(
            quota_db, previous, current,
        )

    @blueprint.route("/api/config")
    def api_config_get():
        import config as cfg

        return jsonify(cfg.load())

    @blueprint.route("/api/config", methods=["POST"])
    def api_config_set():
        import config as cfg
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        # Share the schema-driven settings write lock so an autosave in the
        # All-settings region can't race with the curated form's full-snapshot
        # save and silently overwrite a recently-saved schema value.
        with settings_write_lock:
            previous = cfg.load()
            current = dict(previous)

            # Validate the working-copy quota against the schema up front.
            # Without this, a non-integer or explicit ``null`` value used to
            # slip past the confirmation gate (int() raised, ``new_quota_mb``
            # stayed None) but was still persisted verbatim; the post-save
            # side effect then fell back to the 20 GB default when parsing
            # the stored value — a silent reduction that could evict working
            # copies without a confirmation prompt whenever the previously
            # stored quota exceeded 20 GB.
            #
            # After validation the confirmation gate delegates to the same
            # helper every other global-config write path uses, so a stale
            # client cache still can't bypass the Storage-page warning.
            if "working_copy_cache_max_mb" in body:
                try:
                    new_quota_mb = schema.validate_value(
                        "working_copy_cache_max_mb",
                        body["working_copy_cache_max_mb"],
                    )
                except schema.ValidationError as e:
                    return json_error(str(e), status=400)
                # Persist the validated integer so the ``for key in body``
                # loop below can't write a raw payload value that later
                # parses to a different fallback default.
                body["working_copy_cache_max_mb"] = new_quota_mb
                confirmation = _working_copy_quota_confirmation_required(
                    previous,
                    new_quota_mb,
                    body.get("_confirm_working_copy_eviction"),
                )
                if confirmation is not None:
                    return confirmation

            # Handle keyboard_shortcuts with validation
            if "keyboard_shortcuts" in body:
                shortcuts = body["keyboard_shortcuts"]
                if isinstance(shortcuts, dict):
                    valid_contexts = cfg.DEFAULTS["keyboard_shortcuts"]
                    validated = {}
                    for ctx_name, actions in shortcuts.items():
                        if ctx_name in valid_contexts and isinstance(actions, dict):
                            validated[ctx_name] = {}
                            for action, key_str in actions.items():
                                if action in valid_contexts[ctx_name] and isinstance(key_str, str):
                                    validated[ctx_name][action] = key_str.strip().lower()
                    current["keyboard_shortcuts"] = cfg._deep_merge(
                        cfg.DEFAULTS["keyboard_shortcuts"], validated
                    )

            # Normalize remote_targets on save (assign stable ids, coerce
            # field types, drop unusable entries) so stored entries always
            # have the shape get_remote_targets() guarantees on read.
            if "remote_targets" in body:
                raw_targets = body["remote_targets"]
                normalized = []
                if isinstance(raw_targets, list):
                    import uuid
                    for entry in raw_targets:
                        coerced = cfg._coerce_remote_target(entry)
                        if coerced is None:
                            continue
                        # A client-supplied id wins (stable across edits);
                        # otherwise mint one so future edits can key the row.
                        if not (isinstance(entry, dict) and (entry.get("id") or "").strip()):
                            coerced["id"] = uuid.uuid4().hex
                        normalized.append(coerced)
                current["remote_targets"] = normalized

            # Quick-filter buttons are rule expressions, so validate them
            # against the field registry here rather than rendering a button
            # that could never match anything.
            if "filter_shortcuts" in body:
                raw_shortcuts = body["filter_shortcuts"]
                if isinstance(raw_shortcuts, list):
                    # Over the cap is refused, not trimmed: a silently dropped
                    # button looks saved until the page is reloaded.
                    if len(raw_shortcuts) > filter_shortcuts.MAX_SHORTCUTS:
                        return json_error(
                            "filter_shortcuts: at most "
                            f"{filter_shortcuts.MAX_SHORTCUTS} quick filters",
                            status=400,
                        )
                    # Two buttons applying one expression cannot be told
                    # apart on the bar, so refuse rather than store a pair
                    # where clicking either lights both.
                    twins = filter_shortcuts.find_duplicate(raw_shortcuts)
                    if twins:
                        return json_error(
                            f"filter_shortcuts: {twins[0]!r} and {twins[1]!r} "
                            "apply the same rule",
                            status=400,
                        )
                    current["filter_shortcuts"] = filter_shortcuts.for_storage(
                        filter_shortcuts.normalize(raw_shortcuts)
                    )

            for key in body:
                # export_presets is validated and written only by the
                # /api/export/presets endpoints; skip it here so a full-config
                # snapshot save can't overwrite presets unvalidated.
                if key in ("keyboard_shortcuts", "remote_targets", "export_presets",
                           "filter_shortcuts"):
                    continue
                if key in cfg.DEFAULTS:
                    # Deep-merge for nested-dict config sections so a curated
                    # payload that only ships a subset of keys (settings.html's
                    # saveConfig posts a ``pipeline`` block with just the
                    # weights/miss/eye sliders it renders) doesn't wipe the
                    # keys it didn't include. Without this, the global
                    # ``pipeline.default_process_id`` would silently reset to
                    # null on every autosave and workspaces inheriting it
                    # would go back to import-only.
                    if (
                        isinstance(cfg.DEFAULTS[key], dict)
                        and isinstance(body[key], dict)
                        and isinstance(current.get(key), dict)
                    ):
                        current[key] = cfg._deep_merge(current[key], body[key])
                    else:
                        current[key] = body[key]
            # default_process_id points into saved_processes; the deep-merge
            # above can carry a stale or foreign id into the global config.
            # Reject it here like the settings PATCH/import paths, else
            # workspaces inheriting this global default hit "unknown process
            # id" in _validate_after_import on their next import instead of
            # starting it.
            pipeline_current = current.get("pipeline")
            if isinstance(pipeline_current, dict):
                pid = pipeline_current.get("default_process_id")
                if pid is not None:
                    if not isinstance(pid, int) or isinstance(pid, bool):
                        return json_error(
                            "pipeline.default_process_id must be an integer "
                            "or null", status=400,
                        )
                    if get_db().get_saved_process(pid) is None:
                        return json_error(
                            f"unknown process id: {pid}", status=400
                        )
            cfg.save(current)
            if "inat_token" in body:
                advance_inat_token_generation()
            _settings_post_save_side_effects(current, previous)
        return jsonify({"ok": True})

    @blueprint.route("/api/settings/schema")
    def api_settings_schema():
        """Return the SCHEMA dict and the ordered category list.

        Consumed by the schema-rendered settings UI to generate widgets.

        ``pipeline.default_process_id`` is stored/validated as an ``int``
        (config_schema is DB-agnostic), but the widget should be a picker over
        the live saved-process list. Present it here as a dynamic ``enum``
        without mutating the module-level SCHEMA: shallow-copy the dict, swap
        that one entry, and inject the current processes as enum/enum_labels.
        """
        import config_schema as schema

        db = get_db()
        out = dict(schema.SCHEMA)
        spec = dict(out.get("pipeline.default_process_id", {}))
        if spec:
            procs = db.get_saved_processes()
            spec["type"] = "enum"
            # Return numeric ids so the settings renderer, which selects the
            # active option via strict equality against the effective value,
            # matches. The stored/effective value is an ``int`` (validated by
            # the static ``int`` spec), so string-typed enum values would
            # never match and the picker would silently fall through to the
            # nullable "Import only" option, misrepresenting an active
            # default and resetting it on the next save. <option> values in
            # the DOM still serialize to strings; the PATCH endpoint coerces
            # them back to int via the static int spec on the way in.
            spec["enum"] = [p["id"] for p in procs]
            spec["enum_labels"] = {p["id"]: p["name"] for p in procs}
            out["pipeline.default_process_id"] = spec
        return jsonify({
            "schema": out,
            "categories": list(schema.CATEGORIES),
        })

    @blueprint.route("/api/settings/values")
    def api_settings_values():
        """Return values across all four layers (default / global / workspace / effective).

        Each layer is a dotted-flat dict, restricted to keys present in SCHEMA.
        Keys hand-edited into config.json or stored as workspace metadata
        (e.g. active_labels) that are not declared in SCHEMA are intentionally
        omitted from the response so they don't show up as "overridden" in
        the UI; they are preserved on disk and visible in the raw-JSON tab.
        """
        import config as cfg
        import config_schema as schema

        schema_keys = set(schema.SCHEMA.keys())

        # Default layer: flatten DEFAULTS, restrict to schema keys.
        default_flat = schema.flatten(cfg.DEFAULTS)
        default_layer = {k: default_flat[k] for k in schema_keys if k in default_flat}

        # Global layer: read the raw file (not cfg.load(), which deep-merges
        # with DEFAULTS — we want only what the user explicitly set). Also
        # filter out keys whose value equals the default: legacy save paths
        # write the entire deep-merged config, but a value matching the
        # default is not really a user override and should not show as one.
        global_flat = schema.flatten(read_raw_config_file())
        global_layer = {
            k: v for k, v in global_flat.items()
            if k in schema_keys and default_layer.get(k) != v
        }

        # Workspace layer: parse config_overrides for the active workspace.
        workspace_layer = {}
        db = get_db()
        ws = db.get_workspace(db._active_workspace_id)
        if ws and ws["config_overrides"]:
            try:
                overrides = (
                    json.loads(ws["config_overrides"])
                    if isinstance(ws["config_overrides"], str)
                    else ws["config_overrides"]
                )
                ws_flat = schema.flatten(overrides if isinstance(overrides, dict) else {})
                # Filter out global-only schema keys: workspace create/update
                # APIs can persist arbitrary override payloads, so a workspace
                # may contain entries for keys whose scope is "global".
                # Runtime paths for those keys read global config only, so
                # surfacing the workspace value here would mislead the UI
                # into showing a workspace-effective value that is never
                # actually applied.
                workspace_layer = {
                    k: v for k, v in ws_flat.items()
                    if k in schema_keys
                    and schema.SCHEMA[k].get("scope") != "global"
                }
            except (json.JSONDecodeError, TypeError):
                workspace_layer = {}

        # Effective layer: workspace > global > default for every schema key.
        effective_layer = {}
        for k in schema_keys:
            if k in workspace_layer:
                effective_layer[k] = workspace_layer[k]
            elif k in global_layer:
                effective_layer[k] = global_layer[k]
            elif k in default_layer:
                effective_layer[k] = default_layer[k]

        return jsonify({
            "default": default_layer,
            "global": global_layer,
            "workspace": workspace_layer,
            "effective": effective_layer,
        })

    @blueprint.route("/api/settings/global", methods=["PATCH"])
    def api_settings_global_patch():
        """Set a single global config value (validated against SCHEMA)."""
        import config as cfg
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        key = body.get("key")
        if not isinstance(key, str) or key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)
        try:
            value = schema.validate_value(key, body.get("value"))
        except schema.ValidationError as e:
            return json_error(str(e), status=400)

        # default_process_id points into saved_processes; config_schema only
        # int-coerces it, so validate existence here too (mirrors the
        # workspace PATCH and _validate_workspace_config_overrides). Without
        # this, a stale global default lets workspaces that inherit it hit the
        # import endpoints' "unknown process id" wall and fail to auto-process.
        if (
            key == "pipeline.default_process_id"
            and value is not None
            and get_db().get_saved_process(value) is None
        ):
            return json_error(f"unknown process id: {value}", status=400)

        with settings_write_lock:
            previous = cfg.load()
            if key == "working_copy_cache_max_mb":
                confirmation = _working_copy_quota_confirmation_required(
                    previous,
                    value,
                    body.get("_confirm_working_copy_eviction"),
                )
                if confirmation is not None:
                    return confirmation
            raw = read_raw_config_file()
            schema.set_dotted(raw, key, value)
            cfg.save(raw)
            if key == "inat_token":
                advance_inat_token_generation()
            _settings_post_save_side_effects(cfg.load(), previous)
        return jsonify({"ok": True, "key": key, "value": value})

    @blueprint.route("/api/settings/global/<path:key>", methods=["DELETE"])
    def api_settings_global_delete(key):
        """Remove a key from the global config file (reverts to default)."""
        import config as cfg
        import config_schema as schema

        if key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)

        body = request.get_json(silent=True) or {}
        with settings_write_lock:
            previous = cfg.load()
            if key == "working_copy_cache_max_mb":
                confirmation = _working_copy_quota_confirmation_required(
                    previous,
                    cfg.DEFAULTS["working_copy_cache_max_mb"],
                    body.get("_confirm_working_copy_eviction"),
                )
                if confirmation is not None:
                    return confirmation
            raw = read_raw_config_file()
            schema.delete_dotted(raw, key)
            cfg.save(raw)
            if key == "inat_token":
                advance_inat_token_generation()
            _settings_post_save_side_effects(cfg.load(), previous)
        return jsonify({"ok": True, "key": key})

    @blueprint.route("/api/settings/workspace", methods=["PATCH"])
    def api_settings_workspace_patch():
        """Set a single per-workspace override (validated against SCHEMA)."""
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        key = body.get("key")
        if not isinstance(key, str) or key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)
        if schema.SCHEMA[key].get("scope") == "global":
            return json_error(
                f"{key!r} is global-only and cannot be overridden per workspace",
                status=400,
            )
        try:
            value = schema.validate_value(key, body.get("value"))
        except schema.ValidationError as e:
            return json_error(str(e), status=400)

        db = get_db()
        # default_process_id is a pointer into saved_processes: config_schema
        # only coerces it to int/null, so existence is checked here against the
        # DB (mirrors _validate_workspace_config_overrides).
        if (
            key == "pipeline.default_process_id"
            and value is not None
            and db.get_saved_process(value) is None
        ):
            return json_error(f"unknown process id: {value}", status=400)
        with settings_write_lock:
            import config as cfg
            key_transition_check = (key == LOCATION_KEYWORDS_SETTING)
            prev_effective_val = (
                bool(db.get_effective_config(cfg.load()).get(key, False))
                if key_transition_check else None
            )
            overrides = read_workspace_overrides(db)
            schema.set_dotted(overrides, key, value)
            write_workspace_overrides(db, overrides)
            if key_transition_check:
                new_effective_val = bool(
                    db.get_effective_config(cfg.load()).get(key, False),
                )
                if prev_effective_val and not new_effective_val:
                    queue_location_keyword_cleanup_for_workspace(
                        db, db._active_workspace_id,
                    )
        return jsonify({"ok": True, "key": key, "value": value})

    @blueprint.route("/api/settings/workspace/<path:key>", methods=["DELETE"])
    def api_settings_workspace_delete(key):
        """Remove a per-workspace override (the key falls back to global/default)."""
        import config_schema as schema

        if key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)
        db = get_db()
        with settings_write_lock:
            import config as cfg
            key_transition_check = (key == LOCATION_KEYWORDS_SETTING)
            prev_effective_val = (
                bool(db.get_effective_config(cfg.load()).get(key, False))
                if key_transition_check else None
            )
            overrides = read_workspace_overrides(db)
            schema.delete_dotted(overrides, key)
            write_workspace_overrides(db, overrides)
            if key_transition_check:
                new_effective_val = bool(
                    db.get_effective_config(cfg.load()).get(key, False),
                )
                if prev_effective_val and not new_effective_val:
                    queue_location_keyword_cleanup_for_workspace(
                        db, db._active_workspace_id,
                    )
        return jsonify({"ok": True, "key": key})

    @blueprint.route("/api/settings/export")
    def api_settings_export():
        """Download ~/.vireo/config.json as an attachment.

        Returns the raw user-overrides file (or "{}" if absent), pretty-printed.
        Workspace overrides are not included — they're per-workspace state, not
        global config.
        """
        import datetime as _datetime

        import config_schema as schema

        raw = read_raw_config_file()
        raw.pop("_migrations_applied", None)
        # Never ship secret values in a settings backup — these files get
        # attached to bug reports and shared machines. Import leaves this
        # machine's stored secrets alone when the payload omits them, and
        # ``_secrets_omitted`` tells the user what to re-enter after
        # importing on a fresh machine.
        _ABSENT = object()
        omitted = []
        for secret_key in schema.secret_keys():
            val = schema.get_dotted(raw, secret_key, default=_ABSENT)
            if val is _ABSENT:
                continue
            schema.delete_dotted(raw, secret_key)
            if val:
                omitted.append(secret_key)
        if omitted:
            raw["_secrets_omitted"] = sorted(omitted)
        # ``pipeline.default_process_id`` points into ``saved_processes``,
        # whose rows are DB-local — the ids don't transfer across databases.
        # If the same integer id happens to exist in the target DB it points
        # at a different process; the seed ids 1-4 make this collision
        # common. Emit the current process name AND its full flag snapshot
        # alongside the id as a portable identity so ``/api/settings/import``
        # can verify the target DB's same-named row means the same thing.
        # Name alone is not enough — process names are user-editable and two
        # databases can end up with matching names on rows that differ in
        # flags (renamed customs, edited seeds, unrelated customs that just
        # happen to share a label). Importing a foreign backup would then
        # silently repoint the after-import default at a divergent process.
        # The extra keys live in the exported JSON only; the raw config file
        # itself is never rewritten with them.
        pipeline_raw = raw.get("pipeline")
        if isinstance(pipeline_raw, dict):
            pid = pipeline_raw.get("default_process_id")
            if isinstance(pid, int):
                match = get_db().get_saved_process(pid)
                if match is not None:
                    import process_strategies as ps

                    pipeline_raw["default_process_name"] = match["name"]
                    pipeline_raw["default_process_flags"] = {
                        k: match[k] for k in ps.FLAG_FIELDS
                    }
        body = json.dumps(raw, indent=2)
        today = _datetime.date.today().isoformat()
        resp = make_response(body)
        resp.headers["Content-Type"] = "application/json"
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="vireo-config-{today}.json"'
        )
        return resp

    @blueprint.route("/api/settings/import", methods=["POST"])
    def api_settings_import():
        """Replace ~/.vireo/config.json with the supplied JSON payload.

        Validates every schema-known leaf key in the payload before writing;
        on any validation failure, returns 400 with a per-key error map and
        leaves the file untouched. Non-schema keys (setup_complete, the
        keyboard_shortcuts subtree, etc.) pass through unchanged so that
        backups round-trip cleanly. Workspace overrides are untouched —
        backups capture global state only.
        """
        import config as cfg
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        raw_text = body.get("json", "")
        if not isinstance(raw_text, str):
            return json_error("body.json must be a string", status=400)
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as e:
            return json_error(f"invalid JSON: {e}", status=400)
        if not isinstance(payload, dict):
            return json_error("payload must be a JSON object", status=400)

        # Bookkeeping marker written by /api/settings/export; never persist it.
        payload.pop("_secrets_omitted", None)

        # Translate the legacy ``pipeline.default_strategy`` (hardcoded strategy
        # name) to the current ``pipeline.default_process_id`` before schema
        # validation runs. Startup migration does the same for
        # ~/.vireo/config.json, but importing an older settings backup would
        # otherwise write the legacy key through as a non-schema value (the
        # import endpoints only read ``default_process_id``) and silently fall
        # back to import-only until a restart. Mirror the migration's mapping
        # exactly: unknown/removed legacy names -> null (import only). If both
        # keys are present (e.g. a hand-edited backup), the new key wins and
        # the legacy key is dropped so it can't reappear on a later export.
        pipeline_payload = payload.get("pipeline")
        if (
            isinstance(pipeline_payload, dict)
            and "default_strategy" in pipeline_payload
        ):
            import process_strategies as ps

            legacy = pipeline_payload.pop("default_strategy")
            if "default_process_id" not in pipeline_payload:
                seed_name = (
                    ps.LEGACY_STRATEGY_NAMES.get(legacy)
                    if isinstance(legacy, str) else None
                )
                translated_pid = None
                if seed_name is not None:
                    match = next(
                        (
                            p for p in get_db().get_saved_processes()
                            if p["name"] == seed_name
                        ),
                        None,
                    )
                    if match is not None:
                        translated_pid = match["id"]
                pipeline_payload["default_process_id"] = translated_pid

        # Translate the portable ``pipeline.default_process_name`` (emitted by
        # /api/settings/export alongside the id) to the target DB's id.
        # ``saved_processes`` rows are DB-local — the raw id doesn't transfer
        # across databases (seed ids 1-4 collide, custom ids collide by
        # chance), so a foreign backup that only carried ``default_process_id``
        # would silently point at whatever unrelated row happens to share that
        # id. Match by name AND by full flag snapshot when the exporter emits
        # ``default_process_flags``: the name alone is not a stable identity
        # (users rename processes, unrelated custom processes across DBs can
        # share a label, and even a seed named "Full" may have been edited on
        # one side). If the exporter didn't ship flags (older backup), fall
        # back to name-only match. Any mismatch — no name row, no flag-equal
        # row, or the flags entry is malformed — resolves to null (import
        # only) instead of silently activating a divergent process. The name
        # and flag fields are stripped from the payload so they don't persist
        # to ~/.vireo/config.json as non-schema keys.
        if isinstance(pipeline_payload, dict) and (
            "default_process_name" in pipeline_payload
            or "default_process_flags" in pipeline_payload
        ):
            import process_strategies as ps

            name_val = pipeline_payload.pop("default_process_name", None)
            flags_val = pipeline_payload.pop("default_process_flags", None)
            if name_val is None:
                pipeline_payload["default_process_id"] = None
            elif isinstance(name_val, str):
                candidates = [
                    p for p in get_db().get_saved_processes()
                    if p["name"] == name_val
                ]
                translated_pid = None
                if isinstance(flags_val, dict):
                    for cand in candidates:
                        if all(
                            cand.get(k) == flags_val.get(k)
                            for k in ps.FLAG_FIELDS
                        ):
                            translated_pid = cand["id"]
                            break
                elif candidates:
                    translated_pid = candidates[0]["id"]
                pipeline_payload["default_process_id"] = translated_pid
            else:
                pipeline_payload["default_process_id"] = None

        # Iterate the schema directly rather than relying on flatten() — empty
        # objects at schema leaves (e.g. {"classification_threshold": {}}) flatten
        # to nothing and would otherwise be written as-is, replacing a numeric
        # leaf with {} on disk and breaking downstream consumers.
        _MISSING = object()
        errors = {}

        # 1. Reject scalars where a schema-backed object subtree is expected
        #    (e.g. {"pipeline": 5}).
        for prefix in schema.schema_parent_prefixes():
            val = schema.get_dotted(payload, prefix, default=_MISSING)
            if val is _MISSING or isinstance(val, dict):
                continue
            errors[prefix] = f"{prefix} must be a JSON object"

        # 2. For every schema leaf actually present in the payload, reject any
        #    object (empty or otherwise) and run the usual value validation.
        for schema_key in schema.SCHEMA:
            val = schema.get_dotted(payload, schema_key, default=_MISSING)
            if val is _MISSING:
                continue
            if isinstance(val, dict):
                errors[schema_key] = f"{schema_key} must be a JSON scalar, not an object"
                continue
            try:
                coerced = schema.validate_value(schema_key, val)
                schema.set_dotted(payload, schema_key, coerced)
            except schema.ValidationError as e:
                errors[schema_key] = str(e)

        # default_process_id points into saved_processes; config_schema only
        # coerces it to int/null. An imported config carrying an id from
        # another DB — or a stale id after a process was deleted — would
        # silently write through here and then hit the "unknown process id"
        # 400 at every import that inherits this global default. Mirror the
        # global PATCH check so the failure surfaces at the import layer.
        pid_key = "pipeline.default_process_id"
        if pid_key not in errors:
            pid_val = schema.get_dotted(payload, pid_key, default=_MISSING)
            if (
                pid_val is not _MISSING
                and pid_val is not None
                and get_db().get_saved_process(pid_val) is None
            ):
                errors[pid_key] = f"unknown process id: {pid_val}"

        # Structured non-schema keys still need shape validation, otherwise a
        # malformed payload would write through to the file and crash
        # downstream UI consumers that assume a specific shape.
        if "keyboard_shortcuts" in payload:
            # shortcuts.html dereferences `cfg.keyboard_shortcuts.<ctx>.<action>`
            # and assumes a dict tree.
            ks = payload["keyboard_shortcuts"]
            if not isinstance(ks, dict):
                errors["keyboard_shortcuts"] = "keyboard_shortcuts must be a JSON object"
            else:
                for ctx_name, actions in ks.items():
                    if not isinstance(actions, dict):
                        errors[f"keyboard_shortcuts.{ctx_name}"] = (
                            f"keyboard_shortcuts.{ctx_name} must be a JSON object"
                        )
                        continue
                    for action, key_str in actions.items():
                        if not isinstance(key_str, str):
                            errors[f"keyboard_shortcuts.{ctx_name}.{action}"] = (
                                "must be a string"
                            )

        # ingest.recent_destinations is also EXCLUDED from SCHEMA but is a
        # structured value (list[str]). pipeline.html calls
        # `recents.forEach(...)` on it, so a non-list value would crash the
        # pipeline page after a bad import.
        ingest_section = payload.get("ingest")
        if isinstance(ingest_section, dict) and "recent_destinations" in ingest_section:
            recents = ingest_section["recent_destinations"]
            if not isinstance(recents, list):
                errors["ingest.recent_destinations"] = (
                    "ingest.recent_destinations must be a JSON array"
                )
            else:
                for i, item in enumerate(recents):
                    if not isinstance(item, str):
                        errors[f"ingest.recent_destinations[{i}]"] = (
                            "must be a string"
                        )
                        break

        if "export_presets" in payload:
            from export import (
                normalize_export_preset_name,
                normalize_export_preset_settings,
            )

            raw_presets = payload["export_presets"]
            normalized_presets = []
            seen_preset_names = set()
            if not isinstance(raw_presets, list):
                errors["export_presets"] = "export_presets must be a JSON array"
            else:
                for i, entry in enumerate(raw_presets):
                    entry_key = f"export_presets[{i}]"
                    if not isinstance(entry, dict):
                        errors[entry_key] = "export preset must be a JSON object"
                        continue
                    try:
                        name = normalize_export_preset_name(entry.get("name"))
                    except ValueError as exc:
                        errors[f"{entry_key}.name"] = str(exc)
                        name = None
                    if name is not None:
                        if name in seen_preset_names:
                            errors[f"{entry_key}.name"] = (
                                f"duplicate export preset name {name!r}"
                            )
                            name = None
                        else:
                            seen_preset_names.add(name)
                    try:
                        settings = normalize_export_preset_settings(
                            entry.get("settings")
                        )
                    except ValueError as exc:
                        errors[f"{entry_key}.settings"] = str(exc)
                        settings = None
                    if name is not None and settings is not None:
                        normalized_presets.append({"name": name, "settings": settings})
                normalized_presets.sort(
                    key=lambda preset: preset["name"].casefold()
                )
                payload["export_presets"] = normalized_presets

        # filter_shortcuts is EXCLUDED from SCHEMA (custom Settings UI) but
        # the filter bar renders it on five pages, so a malformed import must
        # not reach the template. Normalize like /api/config does: unusable
        # entries are dropped rather than rendered as dead buttons.
        if "filter_shortcuts" in payload:
            raw_shortcuts = payload["filter_shortcuts"]
            if not isinstance(raw_shortcuts, list):
                errors["filter_shortcuts"] = "filter_shortcuts must be a JSON array"
            elif len(raw_shortcuts) > filter_shortcuts.MAX_SHORTCUTS:
                errors["filter_shortcuts"] = (
                    f"at most {filter_shortcuts.MAX_SHORTCUTS} quick filters"
                )
            elif filter_shortcuts.find_duplicate(raw_shortcuts):
                twins = filter_shortcuts.find_duplicate(raw_shortcuts)
                errors["filter_shortcuts"] = (
                    f"{twins[0]!r} and {twins[1]!r} apply the same rule"
                )
            else:
                payload["filter_shortcuts"] = filter_shortcuts.for_storage(
                    filter_shortcuts.normalize(raw_shortcuts)
                )

        if errors:
            return jsonify({"error": "validation failed", "errors": errors}), 400

        with settings_write_lock:
            previous = cfg.load()
            # Exports omit secret values (see /api/settings/export), so a
            # restored backup must not wipe the tokens already configured on
            # this machine: absent secret keys keep their current on-disk
            # value; explicitly supplied ones write through.
            current_raw = read_raw_config_file()
            # Export deliberately omits install-local migration markers.
            # Restore must not make already completed migrations run again.
            payload["_migrations_applied"] = list(dict.fromkeys(
                cfg._migrations_applied(current_raw) + cfg._migrations_applied(payload)
            ))
            for secret_key in schema.secret_keys():
                if schema.get_dotted(payload, secret_key, default=_MISSING) is not _MISSING:
                    continue
                existing = schema.get_dotted(current_raw, secret_key, default=_MISSING)
                if existing is not _MISSING:
                    schema.set_dotted(payload, secret_key, existing)
            confirmation = _working_copy_quota_confirmation_required(
                previous,
                payload.get(
                    "working_copy_cache_max_mb",
                    cfg.DEFAULTS["working_copy_cache_max_mb"],
                ),
                body.get("_confirm_working_copy_eviction"),
            )
            if confirmation is not None:
                return confirmation
            cfg.save(payload)
            if "inat_token" in payload:
                advance_inat_token_generation()
            _settings_post_save_side_effects(cfg.load(), previous)
        return jsonify({"ok": True})

    return blueprint
