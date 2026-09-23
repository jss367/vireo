"""Remote setup: the synchronous endpoints behind the Settings-page NAS wizard.

The ``/api/remote-setup/*`` routes list mounted network shares, check SSH
reachability and key auth, install the Vireo key on the NAS with a one-time
password, locate a mounted share's NAS-side path, browse remote directories,
and validate the local archive root
(docs/superpowers/specs/2026-07-26-nas-setup-wizard-design.md). All of them
are loopback-only: the app already binds 127.0.0.1 via waitress, but the
install-key route carries a NAS password, so defense in depth is cheap and
the consistent rule is simplest.

The configured remote targets the wizard produces are served here too:
``/api/remote-targets`` lists them for the move/import pickers (with rsync/ssh
availability and each local archive root's state), and
``/api/remote-targets/test`` checks one saved or in-progress target. These
two are not loopback-guarded; they never were.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import subprocess

from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)


def create_remote_setup_blueprint(get_db, json_error, config):
    """Build the remote-setup blueprint.

    Beyond the database accessor and the JSON error helper, only ``config``
    (``app.config``) is injected: the remote-targets list reads
    ``REMOTE_TARGET_PROBE_BUDGET_SECS`` from it at request time. The loopback
    guard, the host/user/port validation, the ssh binary lookup and the
    archive-root probes are used only by these routes, so they live here.
    """
    blueprint = Blueprint("remote_setup", __name__)

    def _remote_setup_forbidden():
        if request.remote_addr not in ("127.0.0.1", "::1"):
            return json_error(
                "remote setup is only available from this machine", 403)
        return None

    def _remote_setup_conn_args(body):
        """Validate and extract {host, user, port} shared by the wizard's
        SSH-touching endpoints. Returns (args, None) or (None, response)."""
        host = (body.get("host") or "").strip()
        user = (body.get("user") or "").strip()
        port = body.get("port", 22)
        if not host or not user:
            return None, json_error("host and user are required")
        try:
            port = int(port)
        except (TypeError, ValueError):
            return None, json_error("port must be an integer")
        if not 1 <= port <= 65535:
            return None, json_error("port must be between 1 and 65535")
        return {"host": host, "user": user, "port": port}, None

    def _remote_setup_ssh_bin():
        import config as cfg
        import move as move_mod

        effective_cfg = get_db().get_effective_config(cfg.load())
        return move_mod.resolve_ssh_bin(effective_cfg.get("ssh_bin", "") or "")

    @blueprint.route("/api/remote-setup/mounts")
    def api_remote_setup_mounts():
        """Mounted network shares the wizard can offer as NAS candidates."""
        import remote_setup

        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        if not remote_setup.platform_supported():
            return jsonify({"mounts": [], "unsupported_platform": True})
        return jsonify({"mounts": remote_setup.list_network_mounts()})

    @blueprint.route("/api/remote-setup/ssh-check", methods=["POST"])
    def api_remote_setup_ssh_check():
        """Port reachability + current key-auth status for a candidate NAS.

        Also ensures the Vireo keypair exists (idempotent, local-only) and
        returns its public line: the wizard's Terminal-fallback expander
        builds its authorized_keys one-liner from ``pub_key_line`` and must
        work without install-key (no password) ever being called.
        """
        import remote_setup

        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        conn, err = _remote_setup_conn_args(request.get_json(silent=True) or {})
        if err:
            return err
        try:
            _priv, pub = remote_setup.ensure_vireo_key()
            with open(pub) as f:
                pub_key_line = f.read().strip()
        except (RuntimeError, OSError) as exc:
            return json_error(f"could not prepare the Vireo SSH key: {exc}")
        result = {
            "port_open": remote_setup.port_reachable(conn["host"], conn["port"]),
            "pub_key_line": pub_key_line,
            "key_path": _priv,
            "key_auth_ok": False,
        }
        ssh_bin = _remote_setup_ssh_bin()
        if not ssh_bin:
            result["ssh_missing"] = True
        elif result["port_open"]:
            result["key_auth_ok"] = remote_setup.key_auth_works(
                host=conn["host"], user=conn["user"], port=conn["port"],
                key=_priv, ssh_bin=ssh_bin)
        return jsonify(result)

    @blueprint.route("/api/remote-setup/install-key", methods=["POST"])
    def api_remote_setup_install_key():
        """Authorize this Mac's Vireo key on the NAS using a password, once.

        The password is request-scoped only: written to the ssh pty and
        nowhere else — never config, DB, logs, or this response.
        """
        import remote_setup

        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        body = request.get_json(silent=True) or {}
        conn, err = _remote_setup_conn_args(body)
        if err:
            return err
        password = body.get("password") or ""
        if not password:
            return json_error("password is required")
        ssh_bin = _remote_setup_ssh_bin()
        if not ssh_bin:
            return json_error("no OpenSSH client found — set its path under "
                              "Settings > Paths")
        try:
            priv, pub = remote_setup.ensure_vireo_key()
            with open(pub) as f:
                pub_key_line = f.read().strip()
        except (RuntimeError, OSError) as exc:
            return json_error(f"could not prepare the Vireo SSH key: {exc}")
        argv = remote_setup.build_install_argv(
            host=conn["host"], user=conn["user"], port=conn["port"],
            key_pub_line=pub_key_line, ssh_bin=ssh_bin)
        res = remote_setup.install_key_with_password(
            spawn_argv=argv, password=password)
        out = {"ok": bool(res.get("ok")), "error": res.get("error")}
        if res.get("detail"):
            out["detail"] = res["detail"]
        if out["ok"]:
            out["key_auth_ok"] = remote_setup.key_auth_works(
                host=conn["host"], user=conn["user"], port=conn["port"],
                key=priv, ssh_bin=ssh_bin)
            with contextlib.suppress(OSError, subprocess.SubprocessError):
                fp = subprocess.run(
                    ["ssh-keygen", "-lf", pub], capture_output=True,
                    text=True, timeout=10)
                if fp.returncode == 0:
                    out["fingerprint"] = fp.stdout.strip()
        return jsonify(out)

    @blueprint.route("/api/remote-setup/locate-share", methods=["POST"])
    def api_remote_setup_locate_share():
        """Nonce-verified NAS-side path for a mounted share. Proof, not a
        guess — the chained move deletes local originals, so a same-named
        share on the wrong volume must be impossible to configure here."""
        import remote_setup

        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        body = request.get_json(silent=True) or {}
        conn, err = _remote_setup_conn_args(body)
        if err:
            return err
        mount_path = (body.get("mount_path") or "").strip()
        share = (body.get("share") or "").strip()
        if not share:
            return json_error("share is required")
        if not os.path.isabs(mount_path) or not os.path.isdir(mount_path):
            return json_error("mount_path must be an existing absolute path")
        ssh_bin = _remote_setup_ssh_bin()
        if not ssh_bin:
            return json_error("no OpenSSH client found — set its path under "
                              "Settings > Paths")
        priv, _pub = remote_setup.vireo_key_paths()
        try:
            remote_path = remote_setup.locate_share(
                mount_point=mount_path, share=share, host=conn["host"],
                user=conn["user"], port=conn["port"], key=priv,
                ssh_bin=ssh_bin)
        except remote_setup.MountNotWritable:
            return json_error(
                "the mounted volume is not writable — the wizard (and the "
                "move workflow it sets up) needs write access to the share")
        return jsonify({"remote_path": remote_path})

    @blueprint.route("/api/remote-setup/list-remote-dirs", methods=["POST"])
    def api_remote_setup_list_remote_dirs():
        """SSH-backed directory listing for the wizard's fallback browser."""
        import remote_setup

        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        body = request.get_json(silent=True) or {}
        conn, err = _remote_setup_conn_args(body)
        if err:
            return err
        path = (body.get("path") or "").strip()
        if not path.startswith("/"):
            return json_error("path must be absolute")
        ssh_bin = _remote_setup_ssh_bin()
        if not ssh_bin:
            return json_error("no OpenSSH client found — set its path under "
                              "Settings > Paths")
        priv, _pub = remote_setup.vireo_key_paths()
        return jsonify({"dirs": remote_setup.list_remote_dirs(
            path=path, host=conn["host"], user=conn["user"],
            port=conn["port"], key=priv, ssh_bin=ssh_bin)})

    @blueprint.route("/api/remote-setup/disk-free")
    def api_remote_setup_disk_free():
        """Free bytes on the volume containing ``path`` (archive-root step)."""
        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        path = (request.args.get("path") or "").strip()
        if not os.path.isabs(path) or not os.path.isdir(path):
            return json_error("path must be an existing absolute path")
        return jsonify({"free_bytes": shutil.disk_usage(path).free})

    @blueprint.route("/api/remote-setup/check-archive-root", methods=["POST"])
    def api_remote_setup_check_archive_root():
        """True if ``path`` resolves (through symlinks or case-only aliases)
        into ``mount_path``. Exposes the same filesystem-aware containment
        check ``_coerce_remote_target`` runs at save time so the wizard's
        archive-root step can reject aliased folders up front instead of
        letting the save silently blank ``local_archive_root``, which would
        leave the target unable to offer the chained move the wizard was
        meant to configure. A purely-lexical check on the client can't see
        symlinks, so this has to be server-side."""
        import move as move_mod

        forbidden = _remote_setup_forbidden()
        if forbidden:
            return forbidden
        body = request.get_json(silent=True) or {}
        path = (body.get("path") or "").strip()
        mount_path = (body.get("mount_path") or "").strip()
        if not path or not mount_path:
            return json_error("path and mount_path are required")
        if not os.path.isabs(path) or not os.path.isabs(mount_path):
            return json_error("both paths must be absolute")
        try:
            inside = bool(move_mod._path_equal_or_descends(path, mount_path))
        except (OSError, ValueError):
            # realpath failed (unreadable / cross-device on Windows) — the
            # save-time validator is the authoritative check, so let the
            # user proceed rather than block on a transient FS hiccup.
            inside = False
        return jsonify({"inside_mount": inside})

    def _archive_root_state(target):
        """``(present, volume_offline)`` for the target's local archive root.

        ``present`` is None when no root is configured or when it cannot be
        determined, else whether the directory exists. ``volume_offline`` is
        True when the root sits on a mount-shaped volume that failed the
        bounded reachability probe. The filesystem is only touched after
        that probe passes: a plain ``os.path.isdir`` on a stale SMB/NFS
        share can block a Flask worker indefinitely, and this runs on
        every ``/api/remote-targets`` call (page loads and the Import
        page's after-move refresh), so one dead root must not be able to
        hang the endpoint. Ordinary local folders have no mount-shaped
        prefix and skip the probe entirely.
        """
        import volume_reachability

        root = (target.get("local_archive_root") or "").strip()
        if not root:
            return None, False
        _, reachable = volume_reachability.get_shared().check(root)
        if not reachable:
            return None, True
        return os.path.isdir(root), False

    # Aggregate budget for probing every target's archive root in one
    # /api/remote-targets call. Each individual probe is bounded (see
    # volume_reachability), but with several roots on distinct dead
    # volumes the bounded probes would add up serially past the Import
    # page's 10s client abort, and the page would then report every SSH
    # destination as unavailable. Probe concurrently and stop waiting at
    # the budget; a root still unanswered by then is reported the same way
    # volume_reachability reports "could not be inspected in time" —
    # unreachable — rather than guessed.
    config.setdefault("REMOTE_TARGET_PROBE_BUDGET_SECS", 6.0)

    def _archive_root_states(targets):
        """``[(present, volume_offline), ...]`` aligned with ``targets``,
        probed concurrently under ``REMOTE_TARGET_PROBE_BUDGET_SECS``."""
        from concurrent.futures import ThreadPoolExecutor, wait

        indexed = [(i, t) for i, t in enumerate(targets)
                   if (t.get("local_archive_root") or "").strip()]
        states = [(None, False)] * len(targets)
        if not indexed:
            return states
        budget = float(config.get("REMOTE_TARGET_PROBE_BUDGET_SECS", 6.0))
        pool = ThreadPoolExecutor(
            max_workers=min(len(indexed), 8),
            thread_name_prefix="archive-root-probe")
        futures = {pool.submit(_archive_root_state, t): i for i, t in indexed}
        done, _ = wait(futures, timeout=budget)
        # Don't block on stragglers: their probes are bounded and reaped by
        # volume_reachability, so the worker threads exit on their own.
        pool.shutdown(wait=False, cancel_futures=True)
        for fut, i in futures.items():
            if fut in done and fut.exception() is None:
                states[i] = fut.result()
            else:
                if fut in done:
                    log.warning("archive-root probe raised for %s",
                                targets[i].get("local_archive_root"),
                                exc_info=fut.exception())
                else:
                    log.warning(
                        "archive-root probe for %s did not finish within "
                        "%.1fs; reporting the volume as unreachable",
                        targets[i].get("local_archive_root"), budget)
                states[i] = (None, True)
        return states

    @blueprint.route("/api/remote-targets")
    def api_remote_targets_list():
        """List configured remote (SSH) move targets for the move-form picker,
        plus whether a usable GNU rsync is available for the transfer."""
        import config as cfg
        import move as move_mod

        effective_cfg = get_db().get_effective_config(cfg.load())
        rsync_bin = move_mod.resolve_rsync_bin(
            effective_cfg.get("rsync_bin", "") or "")
        usable = bool(rsync_bin and move_mod.is_gnu_rsync(rsync_bin))
        ssh_bin = move_mod.resolve_ssh_bin(
            effective_cfg.get("ssh_bin", "") or "")
        targets = cfg.get_remote_targets()
        for t, (present, offline) in zip(
                targets, _archive_root_states(targets), strict=True):
            t["local_archive_root_present"] = present
            t["local_archive_root_volume_offline"] = offline
        return jsonify({
            "targets": targets,
            "rsync_available": usable,
            "rsync_bin": rsync_bin if usable else None,
            "ssh_available": bool(ssh_bin),
            "ssh_bin": ssh_bin,
            "remote_available": bool(usable and ssh_bin),
        })

    @blueprint.route("/api/remote-targets/test", methods=["POST"])
    def api_remote_target_test():
        """Test connectivity for a remote target (saved or in-progress edit):
        SSH reachability, remote-path writability, GNU rsync availability,
        whether the local mount path is currently present, and whether the
        local archive root exists.

        The archive-root check is the one that catches a typo'd
        ``local_archive_root`` (issue #1377): the connection itself is fine,
        so ``ok`` stays true, but a bare "Connection OK" would let the user
        walk away from Settings believing the chained move is configured
        and only find out on the Import page, where the hint can't name
        the field that is wrong. ``archive_root_present`` is None when no
        root is configured (nothing to check), so the UI can tell "not
        set" from "set but missing"."""
        import config as cfg
        import move as move_mod

        body = request.get_json(silent=True) or {}
        target = cfg._coerce_remote_target(body)
        if target is None:
            return json_error("Host, user, and remote path are required.")

        effective_cfg = get_db().get_effective_config(cfg.load())
        rsync_bin = move_mod.resolve_rsync_bin(
            effective_cfg.get("rsync_bin", "") or "")
        # Apple openrsync resolves but can't drive SSH — treat as unusable.
        if rsync_bin and not move_mod.is_gnu_rsync(rsync_bin):
            rsync_bin = ""
        ssh_bin = move_mod.resolve_ssh_bin(
            effective_cfg.get("ssh_bin", "") or "")
        target["ssh_bin"] = ssh_bin
        res = move_mod.test_remote_connection(target, rsync_bin)
        import volume_reachability

        mount = target.get("mount_path")
        res["mount_path"] = mount
        # Same bounded gate as the archive root: the mount is the NAS share
        # itself, the most likely path to be stale, so probe before isdir.
        res["mount_present"] = bool(
            mount and volume_reachability.get_shared().check(mount)[1]
            and os.path.isdir(mount))
        # _coerce_remote_target blanks an invalid archive root (relative,
        # or inside mount_path) rather than rejecting the target, and the
        # save path does the same. Compare against what was actually
        # submitted so a rejected root is reported as such instead of
        # reading as "not configured" and getting a green result.
        submitted_root = (body.get("local_archive_root") or "").strip()
        archive_root = target.get("local_archive_root") or ""
        archive_root_invalid = bool(submitted_root and not archive_root)
        res["archive_root"] = (archive_root or submitted_root) or None
        res["archive_root_invalid"] = archive_root_invalid
        if archive_root_invalid:
            present, volume_offline = False, False
        else:
            present, volume_offline = _archive_root_state(target)
        res["archive_root_present"] = present
        res["archive_root_volume_offline"] = volume_offline
        res["rsync_bin"] = rsync_bin or None
        res["ssh_bin"] = ssh_bin
        if res.get("ok") and volume_offline:
            res["message"] = (
                f"Connection OK, but the volume holding the local archive "
                f"root '{archive_root}' is not reachable right now, so "
                f"whether the folder exists can't be checked. The Import "
                f"page won't offer \"Then move to NAS\" for this target "
                f"until it is.")
        elif res.get("ok") and archive_root_invalid:
            res["message"] = (
                f"Connection OK, but the local archive root "
                f"'{submitted_root}' is not valid: it must be an absolute "
                f"path on this machine and must not be inside the mount "
                f"path. Saving will clear it, and the Import page won't "
                f"offer \"Then move to NAS\" for this target.")
        elif res.get("ok") and res["archive_root_present"] is False:
            res["message"] = (
                f"Connection OK, but the local archive root '{archive_root}' "
                f"does not exist on this machine \u2014 the Import page won't "
                f"offer \"Then move to NAS\" for this target until it does. "
                f"Check the path for a typo, or create the folder.")
        if not ssh_bin:
            res["ok"] = False
            res["message"] = (
                "OpenSSH Client was not found. Install the Windows optional "
                "feature or configure ssh.exe under Settings → Paths."
            )
        return jsonify(res)

    return blueprint
