"""Remote setup: the synchronous endpoints behind the Settings-page NAS wizard.

The ``/api/remote-setup/*`` routes list mounted network shares, check SSH
reachability and key auth, install the Vireo key on the NAS with a one-time
password, locate a mounted share's NAS-side path, browse remote directories,
and validate the local archive root
(docs/superpowers/specs/2026-07-26-nas-setup-wizard-design.md). All of them
are loopback-only: the app already binds 127.0.0.1 via waitress, but the
install-key route carries a NAS password, so defense in depth is cheap and
the consistent rule is simplest.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess

from flask import Blueprint, jsonify, request


def create_remote_setup_blueprint(get_db, json_error):
    """Build the remote-setup blueprint.

    Nothing is injected beyond the database accessor and the JSON error
    helper: the loopback guard, the host/user/port validation and the ssh
    binary lookup are used only by these routes, so they move here with them.
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

    return blueprint
