"""Move rules and the folder-move destination preflight.

``/api/move-rules`` is the CRUD (plus match preview) for saved move rules;
``/api/move-folder/preflight`` resolves where a folder move would land and
whether that destination already exists, so the Move page can offer a
merge/resume confirmation before the ``move-folder`` job starts. Source
cleanup after a completed move lives in ``web/move_cleanup.py``.
"""

from __future__ import annotations

import os

from flask import Blueprint, jsonify, request


def _scan_dir_file_count(root_path, file_limit=None, dir_limit=None):
    """Count files (non-directory entries) under ``root_path`` with a lazy
    ``os.scandir`` walk. ``os.scandir`` yields entries one at a time, so with
    ``file_limit``/``dir_limit`` set we bail the moment either cap is reached
    rather than waiting for the OS to enumerate a directory with millions of
    entries. Pass ``None`` for both to count the whole tree exactly.

    Returns ``(file_count, truncated)`` where ``truncated`` is True iff a cap
    stopped the walk early.
    """
    file_count = 0
    dirs_seen = 0
    truncated = False
    stack = [root_path]
    # `not truncated` in the outer condition stops the walk the moment the
    # inner loop trips a cap. Without it, we'd keep popping queued sibling
    # directories until `dirs_seen` mechanically caught up to `dir_limit` —
    # which on a flat fanout means opening every already-queued child, the
    # exact worker-stalling case the cap is supposed to prevent.
    while stack and not truncated:
        if file_limit is not None and file_count >= file_limit:
            truncated = True
            break
        if dir_limit is not None and dirs_seen >= dir_limit:
            truncated = True
            break
        current = stack.pop()
        dirs_seen += 1
        try:
            scanner = os.scandir(current)
        except OSError:
            continue
        with scanner:
            for entry in scanner:
                # Check the caps inside the inner loop so a single directory
                # with a huge number of children cannot blow past either limit
                # before we bail out.
                if file_limit is not None and file_count >= file_limit:
                    truncated = True
                    break
                if dir_limit is not None and dirs_seen + len(stack) >= dir_limit:
                    truncated = True
                    break
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                except OSError:
                    is_dir = False
                if is_dir:
                    stack.append(entry.path)
                else:
                    file_count += 1
    return file_count, truncated


def create_moves_blueprint(get_db, json_error):
    """Build the move-rules and move-folder preflight blueprint.

    These routes need only the database and the JSON error helper; the
    destination walk they use, ``_scan_dir_file_count``, is module-level
    here because nothing else calls it.
    """
    blueprint = Blueprint("moves", __name__)

    @blueprint.route("/api/move-rules", methods=["GET"])
    def api_list_move_rules():
        db = get_db()
        rules = db.list_move_rules()
        return jsonify([dict(r) for r in rules])

    @blueprint.route("/api/move-rules", methods=["POST"])
    def api_create_move_rule():
        db = get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        destination = body.get("destination", "").strip()
        criteria = body.get("criteria", {})
        if not name or not destination:
            return json_error("name and destination required")
        rule_id = db.create_move_rule(name, destination, criteria)
        return jsonify({"ok": True, "id": rule_id})

    @blueprint.route("/api/move-rules/<int:rule_id>", methods=["PUT"])
    def api_update_move_rule(rule_id):
        db = get_db()
        body = request.get_json(silent=True) or {}
        kwargs = {}
        if "name" in body:
            kwargs["name"] = body["name"]
        if "destination" in body:
            kwargs["destination"] = body["destination"]
        if "criteria" in body:
            kwargs["criteria"] = body["criteria"]
        db.update_move_rule(rule_id, **kwargs)
        return jsonify({"ok": True})

    @blueprint.route("/api/move-rules/<int:rule_id>", methods=["DELETE"])
    def api_delete_move_rule(rule_id):
        db = get_db()
        db.delete_move_rule(rule_id)
        return jsonify({"ok": True})

    @blueprint.route("/api/move-rules/preview", methods=["POST"])
    def api_move_rule_preview():
        db = get_db()
        body = request.get_json(silent=True) or {}
        criteria = body.get("criteria", {})
        photo_ids = db.query_move_rule_matches(criteria)
        return jsonify({"count": len(photo_ids), "photo_ids": photo_ids})

    @blueprint.route("/api/move-folder/preflight", methods=["POST"])
    def api_move_folder_preflight():
        """Report the resolved destination for a folder move and whether it
        already exists, so the UI can offer a merge/resume confirmation
        instead of silently failing on an existing destination. Handles both
        local-path and remote-target (SSH) destinations.

        ``mode`` controls how much work the endpoint does (local paths only;
        a remote target returns its SSH-probed result and ignores ``mode``):

        * ``"quick"`` (default) — caps the destination file count at 1000 so
          the live, keystroke-debounced status line stays instant even when
          the destination holds millions of files.
        * ``"exact"`` — walks the destination with a much larger cap
          (100k files / 50k dirs) than ``quick`` so the true count is
          reported for any realistic photo library, while still bounding
          the Flask worker thread on pathological NAS targets (millions
          of unrelated files). The UI fires this in the background to
          replace the capped "at least 1000" line once the fast check
          reported a truncation, and still renders "at least N" if the
          exact walk itself truncated.
        * ``"preview"`` — keeps the destination count on the capped fast
          path (same as ``quick``) and adds a ``preview_merge`` block
          reporting how many source files would actually copy vs. be
          skipped as already present. The deliberate merge-confirm click
          fires this; the source-tree walk for ``preview_merge`` is
          acceptable there, but a second uncapped destination walk is not
          — the dialog displays the preview's copy/skip counts, not the
          raw destination count, so capping the destination here keeps the
          Flask worker free even when the resume target is a NAS folder
          with millions of unrelated files.
        """
        import move as move_mod
        from move import preview_merge, resolve_folder_dest

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("JSON body must be an object")
        folder_id = body.get("folder_id")
        destination = body.get("destination", "")
        destination_name_raw = body.get("destination_name", "")
        folder_template_raw = body.get("folder_template", "")
        mode = body.get("mode", "quick")
        if mode not in ("quick", "exact", "preview"):
            mode = "quick"
        remote_target_id = (body.get("remote_target_id") or "").strip()
        subpath = body.get("subpath", "")

        if not folder_id:
            return json_error("folder_id required")
        if not isinstance(folder_template_raw, str):
            return json_error("folder_template must be a string")
        folder_template = folder_template_raw.strip()
        if folder_template and destination_name_raw:
            return json_error(
                "destination_name cannot be combined with folder_template"
            )
        try:
            destination_name = move_mod.normalize_destination_name(
                destination_name_raw)
        except ValueError as exc:
            return json_error(str(exc))

        folder = get_db().conn.execute(
            "SELECT path, name FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not folder:
            return json_error("Folder not found", status=404)

        # Remote target: resolve the NAS-side dest and probe it over SSH.
        if remote_target_id:
            if folder_template:
                return json_error(
                    "Organizing one folder into capture-date folders is only "
                    "available for local or mounted-drive destinations."
                )
            import posixpath

            import config as cfg
            target = cfg.get_remote_target(remote_target_id)
            if not target:
                return json_error("Remote target not found", status=404)
            effective_cfg = get_db().get_effective_config(cfg.load())
            ssh_bin = move_mod.resolve_ssh_bin(
                effective_cfg.get("ssh_bin", "") or "")
            if not ssh_bin:
                return json_error(
                    "OpenSSH Client was not found. Install it or configure ssh.exe in Settings."
                )
            target = dict(target)
            target["ssh_bin"] = ssh_bin
            try:
                spec = move_mod.build_remote_move_spec(target, subpath, "", ssh_bin)
            except ValueError as exc:
                return json_error(str(exc))
            # The NAS path is POSIX, so the preview/probe path must join with
            # '/' even when this server runs on Windows — resolve_folder_dest
            # uses os.path.join and would produce ``/volume1/Photo\trip``,
            # which the SSH ``test -d`` probe would then look up at a
            # different remote path than the actual transfer (move_folder
            # uses posixpath.join), so an existing destination would be
            # reported as new and the first non-merge move would fail.
            landing_name = destination_name or folder["name"] \
                or os.path.basename(folder["path"].rstrip("/\\"))
            resolved = posixpath.join(spec["ssh_dest_base"], landing_name)
            exists, fcount, truncated, reachable, err = \
                move_mod.remote_preflight(target, resolved)
            return jsonify({
                "resolved_dest": move_mod.rsync_dest_spec(target, resolved),
                "exists": exists,
                "file_count": fcount,
                "file_count_truncated": truncated,
                "remote": True,
                "reachable": reachable,
                "error": err,
                "mount_path_set": bool(target.get("mount_path")),
            })

        if not isinstance(destination, str):
            return json_error("destination must be a string")
        if not destination:
            return json_error("destination required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        if folder_template:
            from ingest import folder_template_samples
            try:
                plan, capture_dts = \
                    move_mod.plan_folder_date_moves_with_capture_dates(
                        get_db(), folder_id, destination, folder_template,
                    )
            except ValueError as exc:
                return json_error(str(exc))
            destinations = [
                {
                    "path": item["destination"],
                    "relative_path": item["relative_path"],
                    "photo_count": item["photo_count"],
                    "exists": os.path.isdir(item["destination"]),
                }
                for item in plan
            ]
            return jsonify({
                "resolved_dest": destination,
                "date_organized": True,
                "folder_template": folder_template,
                "destinations": destinations,
                "destination_count": len(destinations),
                "photo_count": sum(item["photo_count"] for item in plan),
                "exists": any(item["exists"] for item in destinations),
                "file_count": 0,
                "file_count_truncated": False,
                # Example folder names for the format dropdown, rendered from
                # the very capture dates that produced ``destinations`` above.
                # Same scan, same code path — so the label a user reads as
                # "my folder will be called this" cannot disagree with the
                # folder list it sits beside.
                "template_samples": folder_template_samples(capture_dts),
            })

        resolved = resolve_folder_dest(
            folder["path"], folder["name"], destination, destination_name)
        exists = os.path.isdir(resolved)
        file_count = 0
        file_count_truncated = False
        if exists:
            if mode == "exact":
                # Generous cap, not uncapped: requestExactDestCount fires from
                # the keystroke-driven path, and an unbounded walk on a NAS
                # target with millions of files would pin a Flask worker for
                # minutes (the UI seq guard only discards stale replies, not
                # server-side work). 100k/50k is large enough that any
                # realistic photo library reports its true count, and small
                # enough that the worst case is seconds, not minutes.
                file_count, file_count_truncated = _scan_dir_file_count(
                    resolved, file_limit=100000, dir_limit=50000)
            else:
                # Both "quick" and "preview" cap the destination scan. The
                # merge dialog uses preview_merge's copy/skip counts, not
                # this number, so walking the destination uncapped here
                # would block a Flask worker for no UI benefit.
                file_count, file_count_truncated = _scan_dir_file_count(
                    resolved, file_limit=1000, dir_limit=2000)

        result = {
            "resolved_dest": resolved,
            "exists": exists,
            "file_count": file_count,
            "file_count_truncated": file_count_truncated,
        }
        if mode == "preview" and exists:
            result["preview"] = preview_merge(folder["path"], resolved)
        return jsonify(result)

    return blueprint
