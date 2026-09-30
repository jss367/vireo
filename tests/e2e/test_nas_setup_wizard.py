"""E2E coverage for the Settings-page NAS setup wizard.

The live server runs in-process, so remote_setup's discovery/SSH functions
are monkeypatched at the module level — no test spawns a real ssh. The
wizard JS, the /api/remote-setup endpoints, the config save path, and the
remote-target list rendering are all real.
"""

import json
import os
import time

import pytest
from playwright.sync_api import expect


@pytest.fixture()
def nas_env(live_server, tmp_path, monkeypatch):
    """Patch remote_setup for a one-NAS world with controllable key auth."""
    import move
    import remote_setup

    mount_dir = tmp_path / "mnt-photography"
    mount_dir.mkdir()

    key_dir = tmp_path / ".vireo" / "ssh"
    os.makedirs(key_dir, exist_ok=True)
    priv = key_dir / "vireo_ed25519"
    pub = key_dir / "vireo_ed25519.pub"
    priv.write_text("FAKE KEY\n")
    pub.write_text("ssh-ed25519 AAAATESTKEY vireo\n")

    state = {"auth": False, "installs": 0}

    # The full E2E suite runs on Ubuntu, but the wizard is macOS-only in
    # production — force platform_supported() true so /api/remote-setup/mounts
    # doesn't short-circuit to `unsupported_platform` before returning the
    # seeded mount below.
    monkeypatch.setattr(remote_setup, "platform_supported", lambda: True)
    monkeypatch.setattr(remote_setup, "list_network_mounts", lambda **kw: [{
        "fs_type": "smbfs", "host": "100.80.236.59",
        "friendly_host": "synology-nas.ts.net", "display_name": "synology-nas",
        "share": "Photography", "mount_point": str(mount_dir),
        "user": "julius_admin",
    }])
    monkeypatch.setattr(remote_setup, "port_reachable",
                        lambda host, port, timeout=5: True)
    monkeypatch.setattr(remote_setup, "ensure_vireo_key",
                        lambda **kw: (str(priv), str(pub)))

    def fake_key_auth(**kw):
        return state["auth"]

    def fake_install(**kw):
        state["installs"] += 1
        state["auth"] = True
        return {"ok": True, "error": None}

    monkeypatch.setattr(remote_setup, "key_auth_works", fake_key_auth)
    monkeypatch.setattr(remote_setup, "install_key_with_password", fake_install)
    monkeypatch.setattr(remote_setup, "build_install_argv", lambda **kw: ["ssh"])
    monkeypatch.setattr(remote_setup, "locate_share",
                        lambda **kw: "/volume1/Photography")
    monkeypatch.setattr(move, "test_remote_connection", lambda remote, rsync_bin: {
        "ok": True, "ssh": True, "remote_path_writable": True,
        "rsync_ok": True, "remote_rsync_ok": True,
        "message": "Connection OK",
    })
    return {"state": state, "mount_dir": str(mount_dir), "priv": str(priv)}


def _walk_to_ssh_step(page, live_server):
    page.goto(f"{live_server['url']}/settings")
    page.get_by_role("button", name="Set up from mounted volume…").click()
    page.locator("#nwTitle").wait_for(state="visible")
    # Step 1: the seeded mount is preselected.
    assert "Pick your NAS volume" in page.locator("#nwTitle").text_content()
    page.locator("#nwBody").get_by_text("Photography on synology-nas").wait_for()
    page.locator("#nwNext").click()
    assert "Connect over SSH" in page.locator("#nwTitle").text_content()


def _finish_from_share_step(page, live_server, nas_env):
    # Step 3: nonce-verified share path.
    page.locator("#nwBody").get_by_text("/volume1/Photography").wait_for()
    page.locator("#nwNext").click()
    # Step 4: create & use the suggested archive folder.
    assert "local archive folder" in page.locator("#nwTitle").text_content()
    page.get_by_role("button", name="Create & use").click()
    page.locator("#nwBody").get_by_text("Archive folder:").wait_for()
    page.locator("#nwNext").click()
    # Step 5: review auto-runs the connection test.
    page.locator("#nwBody").get_by_text("Connection OK").wait_for()
    page.locator("#nwNext").click()  # Save
    page.locator("#nasWizard").wait_for(state="hidden")
    # The new target card appears in the manual editor list.
    page.locator("#cfgRemoteTargetsList").get_by_text("Test connection").first.wait_for()

    # Saved config (500ms debounce) contains the assembled target.
    deadline = time.time() + 5
    target = None
    while time.time() < deadline:
        cfg = json.loads(page.evaluate(
            "async () => JSON.stringify(await (await fetch('/api/config')).json())"))
        targets = cfg.get("remote_targets") or []
        if targets:
            target = targets[0]
            break
        time.sleep(0.2)
    assert target, "remote target was not saved to config"
    assert target["host"] == "100.80.236.59"  # pinned to the verified mount IP
    assert target["user"] == "julius_admin"
    assert target["remote_path"] == "/volume1/Photography"
    assert target["mount_path"] == nas_env["mount_dir"]
    assert target["local_archive_root"].endswith("Vireo Archive")
    assert target["ssh_key"] == nas_env["priv"]
    assert target["bwlimit_kbps"] == 0


def test_wizard_deep_link_initializes_settings(nas_env, live_server, page):
    """The deep link opens the wizard after all page scripts initialize."""
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))

    page.goto(f"{live_server['url']}/settings#nas-setup")

    expect(page.locator("#nasWizard")).to_be_visible()
    expect(page.locator("#nwBody").get_by_text("Photography on synology-nas")).to_be_visible()
    expect(page.locator("body")).to_have_attribute("data-settings-ready", "true")
    assert not errors


def test_wizard_happy_path_with_password(nas_env, live_server, page):
    _walk_to_ssh_step(page, live_server)
    # Step 2: not yet authorized -> password form.
    pw = page.locator("#nwBody input[type=password]")
    pw.wait_for(state="visible")
    pw.fill("sekret")
    page.get_by_role("button", name="Authorize").click()
    page.locator("#nwBody").get_by_text("Authorized").wait_for()
    assert nas_env["state"]["installs"] == 1
    page.locator("#nwNext").click()
    _finish_from_share_step(page, live_server, nas_env)


def test_wizard_terminal_fallback_never_sends_password(nas_env, live_server, page):
    _walk_to_ssh_step(page, live_server)
    page.locator("#nwBody input[type=password]").wait_for(state="visible")
    # Expand the Terminal fallback: the one-liner embeds the real public key.
    page.get_by_text("Prefer to do this yourself in Terminal?").click()
    assert "AAAATESTKEY" in page.locator("#nwBody pre").text_content()
    # User runs the command out-of-band; simulate its effect, then Verify.
    nas_env["state"]["auth"] = True
    page.get_by_role("button", name="Verify", exact=True).click()
    page.locator("#nwBody").get_by_text("already authorized").wait_for()
    assert nas_env["state"]["installs"] == 0   # password path never used
    page.locator("#nwNext").click()
    _finish_from_share_step(page, live_server, nas_env)
