from pathlib import Path

from playwright.sync_api import expect
from services import workspace_folders


def test_work_locally_full_cycle(live_server, page, tmp_path):
    """Stage a workspace locally, edit a file, and sync it back — driving
    only the UI: buttons, confirm dialogs, and the live progress panel."""
    db = live_server["db"]
    source = tmp_path / "nas-src"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original-bytes")

    ws_id = db.create_workspace("Local Cycle")
    db.commit()
    db.set_active_workspace(ws_id)
    folder_id = workspace_folders.add_folder(db, str(source), name="nas-src")

    activate = page.request.post(f"{live_server['url']}/api/workspaces/{ws_id}/activate")
    assert activate.ok

    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(f"{live_server['url']}/workspace", timeout=5000)

    work_locally = page.get_by_role("button", name="Work Locally", exact=True)
    expect(work_locally).to_be_visible(timeout=5000)
    work_locally.click()
    modal = page.locator("#stageLocalFoldersModal")
    expect(modal).to_have_class("modal-overlay open")
    chosen_parent = tmp_path / "chosen-local-storage"
    modal.locator("[data-local-destination-base]").fill(str(chosen_parent))
    expect(modal.locator("[data-local-destination-preview]")).to_contain_text(
        str(chosen_parent / "nas-src")
    )
    modal.get_by_role("button", name="Copy Locally", exact=True).click()

    panel = page.locator("#localWorkspaceContent")
    expect(panel).to_contain_text("using local storage", timeout=15000)

    local_path = db.conn.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()["path"]
    assert local_path == str(chosen_parent / "nas-src")
    Path(local_path, "bird.jpg").write_bytes(b"edited-locally")

    page.get_by_role("button", name="Sync Back", exact=True).click()
    expect(page.get_by_role("button", name="Work Locally", exact=True)).to_be_visible(timeout=15000)

    assert (source / "bird.jpg").read_bytes() == b"edited-locally"
    restored = db.conn.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()["path"]
    assert restored == str(source)


def test_sync_back_replaces_folder_actions_with_live_progress(
    live_server, page, tmp_path, monkeypatch
):
    """The folder row acknowledges a sync immediately and tracks its job."""
    import threading

    import web.local_folder as local_folder_web

    db = live_server["db"]
    source = tmp_path / "inline-progress-source"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original")
    workspace_id = db.create_workspace("Inline Sync Progress")
    db.commit()
    db.set_active_workspace(workspace_id)
    folder_id = workspace_folders.add_folder(db, str(source), name="inline-progress-source")
    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok

    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(f"{live_server['url']}/workspace", timeout=5000)
    page.get_by_role("button", name="Work Locally", exact=True).click()
    modal = page.locator("#stageLocalFoldersModal")
    destination = tmp_path / "inline-progress-local"
    modal.locator("[data-local-destination-base]").fill(str(destination))
    modal.get_by_role("button", name="Copy Locally", exact=True).click()
    expect(page.get_by_text("Local", exact=True)).to_be_visible(timeout=15000)

    local_path = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (folder_id,)
    ).fetchone()["path"]
    Path(local_path, "bird.jpg").write_bytes(b"edited")

    real_sync_folder = local_folder_web.sync_folder
    progress_sent = threading.Event()
    release_sync = threading.Event()

    def paused_sync_folder(*args, **kwargs):
        kwargs["progress"](3, 10, "bird.jpg")
        progress_sent.set()
        assert release_sync.wait(timeout=10)
        return real_sync_folder(*args, **kwargs)

    monkeypatch.setattr(local_folder_web, "sync_folder", paused_sync_folder)

    try:
        page.get_by_role("button", name="Sync Back", exact=True).click()

        row = page.locator(".workspace-folder-row-stacked")
        status = row.locator(".workspace-folder-job").first
        expect(status).to_be_visible(timeout=5000)
        expect(status).to_have_attribute("data-local-folder-root-id", str(folder_id))
        expect(status).to_contain_text("Syncing inline-progress-source to source")
        expect(status).to_contain_text("3 / 10 files")
        panel = page.locator("#localWorkspaceContent")
        expect(panel).to_contain_text("Syncing inline-progress-source to source")
        expect(panel).to_contain_text("3 / 10 files")
        assert progress_sent.is_set()
        expect(status.get_by_role("link", name="View Jobs")).to_have_attribute(
            "href", "/jobs"
        )
        expect(row.get_by_role("button", name="Sync Back", exact=True)).to_have_count(0)
        expect(row.get_by_role("button", name="Discard", exact=True)).to_have_count(0)

        # A bulk job gives several rows the same job ID. Progress for one root
        # must not overwrite another root's status.
        page.evaluate(
            """([jobId, rootId]) => {
                const current = document.querySelector('.workspace-folder-job');
                const other = current.cloneNode(true);
                other.id = 'other-folder-job';
                other.dataset.localFolderRootId = String(Number(rootId) + 1);
                other.querySelector('.workspace-folder-job-phase').textContent = 'Waiting for its turn';
                current.parentElement.appendChild(other);
                window.dispatchEvent(new CustomEvent('vireo:local-folder-job-progress', {
                    detail: {
                        jobId,
                        progress: {
                            root_folder_id: rootId,
                            phase: 'Publishing this folder',
                            current: 4,
                            total: 10
                        }
                    }
                }));
            }""",
            [status.get_attribute("data-local-folder-job-id"), folder_id],
        )
        expect(status).to_contain_text("Publishing this folder")
        expect(page.locator("#other-folder-job")).to_contain_text("Waiting for its turn")
    finally:
        release_sync.set()

    expect(
        page.get_by_role("button", name="Work Locally", exact=True)
    ).to_be_visible(timeout=15000)


def test_bulk_sync_snapshot_marks_an_already_completed_folder(
    live_server, page, tmp_path
):
    """A late subscriber reconstructs completed per-folder progress from steps."""
    import os
    import threading

    from services.local_folder import stage_folder

    db = live_server["db"]
    workspace_id = db.create_workspace("Bulk Sync Progress")
    db.commit()
    db.set_active_workspace(workspace_id)
    folder_ids = []
    for name in ("first-source", "second-source"):
        source = tmp_path / name
        source.mkdir()
        (source / "bird.jpg").write_bytes(b"original")
        folder_ids.append(workspace_folders.add_folder(db, str(source), name=name))

    vireo_dir = os.path.dirname(live_server["app"].config["THUMB_CACHE_DIR"])
    local_base = str(tmp_path / "bulk-local")
    for folder_id in folder_ids:
        stage_folder(db, folder_id, vireo_dir, local_base=local_base)

    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok

    started = threading.Event()
    release = threading.Event()
    runner = live_server["app"]._job_runner

    def paused_bulk_sync(job):
        runner.set_steps(
            job["id"],
            [
                {"id": f"folder-{folder_ids[0]}", "label": "Sync first-source to source"},
                {"id": f"folder-{folder_ids[1]}", "label": "Sync second-source to source"},
            ],
        )
        runner.update_step(job["id"], f"folder-{folder_ids[0]}", status="running")
        runner.update_step(
            job["id"],
            f"folder-{folder_ids[0]}",
            status="completed",
            progress={"current": 1, "total": 1},
        )
        runner.update_step(job["id"], f"folder-{folder_ids[1]}", status="running")
        job["progress"].update(
            {
                "root_folder_id": folder_ids[1],
                "phase": "Checking second-source for conflicts",
            }
        )
        # Publish before the page subscribes. The browser must reconstruct the
        # first folder from the job snapshot's completed step.
        runner.push_event(job["id"], "progress", dict(job["progress"]))
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    job_id = runner.start(
        "work-locally-folder-sync",
        paused_bulk_sync,
        workspace_id=workspace_id,
        config={"root_folder_ids": folder_ids},
    )
    try:
        assert started.wait(timeout=2)
        page.goto(f"{live_server['url']}/workspace", timeout=5000)
        first = page.locator(
            f'.workspace-folder-job[data-local-folder-job-id="{job_id}"]'
            f'[data-local-folder-root-id="{folder_ids[0]}"]'
        )
        second = page.locator(
            f'.workspace-folder-job[data-local-folder-job-id="{job_id}"]'
            f'[data-local-folder-root-id="{folder_ids[1]}"]'
        )
        expect(first).to_contain_text("Sync first-source to source complete", timeout=5000)
        expect(first).to_contain_text("1 / 1 files")
        expect(second).to_contain_text("Checking second-source for conflicts")
    finally:
        release.set()


def test_work_locally_explains_processing_blocker(live_server, page, tmp_path):
    """A running pipeline disables local-copy controls with a visible reason."""
    import threading

    from services.local_folder import stage_folder

    db = live_server["db"]
    source = tmp_path / "nas-blocked"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original-bytes")
    recovery_source = tmp_path / "nas-recovery"
    recovery_source.mkdir()
    (recovery_source / "fox.jpg").write_bytes(b"original-bytes")
    workspace_id = db.create_workspace("Processing")
    db.commit()
    db.set_active_workspace(workspace_id)
    workspace_folders.add_folder(db, str(source), name="nas-blocked")
    recovery_folder_id = workspace_folders.add_folder(
        db, str(recovery_source), name="nas-recovery"
    )
    stage_folder(db, recovery_folder_id, str(tmp_path))
    db.conn.execute(
        "UPDATE local_folders SET state='staging' WHERE root_folder_id=?",
        (recovery_folder_id,),
    )
    db.conn.commit()

    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok

    started = threading.Event()
    release = threading.Event()

    def processing(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    job_id = live_server["app"]._job_runner.start(
        "pipeline", processing, workspace_id=workspace_id
    )
    try:
        assert started.wait(timeout=2)
        page.goto(f"{live_server['url']}/workspace", timeout=5000)

        panel = page.locator("#localWorkspaceContent")
        expect(panel).to_contain_text(
            "Pipeline is running. Work Locally will be available when it finishes or is cancelled.",
            timeout=5000,
        )
        expect(panel.get_by_role("link", name="View Jobs")).to_be_visible()
        folder_action = page.get_by_role("button", name="Work Locally", exact=True)
        cleanup_action = page.get_by_role("button", name="Clean Up", exact=True)
        expect(folder_action).to_be_disabled()
        expect(cleanup_action).to_be_disabled()

        release.set()
        expect(folder_action).to_be_enabled(timeout=10000)
        expect(cleanup_action).to_be_enabled(timeout=10000)
        expect(panel).not_to_contain_text("Pipeline is running", timeout=10000)
    finally:
        release.set()
        live_server["app"]._job_runner.cancel_job(job_id)


def test_open_work_locally_dialog_disables_when_processing_starts(
    live_server, page, tmp_path
):
    """A blocker discovered after preflight disables the open copy dialog."""
    import threading

    db = live_server["db"]
    source = tmp_path / "nas-late-blocker"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original-bytes")
    workspace_id = db.create_workspace("Late Processing")
    db.commit()
    db.set_active_workspace(workspace_id)
    workspace_folders.add_folder(db, str(source), name="nas-late-blocker")
    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok

    page.goto(f"{live_server['url']}/workspace", timeout=5000)
    page.get_by_role("button", name="Work Locally", exact=True).click()
    modal = page.locator("#stageLocalFoldersModal")
    copy_button = modal.get_by_role("button", name="Copy Locally", exact=True)
    expect(copy_button).to_be_enabled(timeout=5000)

    started = threading.Event()
    release = threading.Event()

    def processing(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    job_id = live_server["app"]._job_runner.start(
        "pipeline", processing, workspace_id=workspace_id
    )
    try:
        assert started.wait(timeout=2)
        page.evaluate("() => window.vireoLocalFolders.load()")
        expect(copy_button).to_be_disabled()
        expect(modal.locator("#stageLocalFoldersError")).to_contain_text(
            "Pipeline is running. Work Locally will be available when it finishes or is cancelled."
        )

        release.set()
        expect(copy_button).to_be_enabled(timeout=10000)
    finally:
        release.set()
        live_server["app"]._job_runner.cancel_job(job_id)


def test_workspace_page_lists_all_workspaces(live_server, page):
    """Workspace page shows both Default and Field Work workspaces."""
    url = live_server["url"]
    page.goto(f"{url}/workspace", timeout=5000)

    container = page.locator("#workspacesContent")

    # Workspace names are rendered as <input> elements inside the container.
    # Wait for the async JS fetch to populate the list.
    default_input = container.locator("input[value='Default']")
    field_work_input = container.locator("input[value='Field Work']")

    expect(default_input).to_be_visible(timeout=5000)
    expect(field_work_input).to_be_visible(timeout=5000)
    expect(page.get_by_role("button", name="Work Locally", exact=True).first).to_be_visible()


def test_shared_folder_local_status_follows_workspace_switch(live_server, page, tmp_path):
    """One managed copy is visible and controllable from every linked workspace."""
    db = live_server["db"]
    source = tmp_path / "shared-source"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original")
    first = db.create_workspace("Shared First")
    second = db.create_workspace("Shared Second")
    db.commit()
    folder_id = workspace_folders.add_folder(db, str(source), name="shared-source", link_to_workspace=False)
    workspace_folders.add_workspace_folder(db, first, folder_id)
    workspace_folders.add_workspace_folder(db, second, folder_id)

    page.on("dialog", lambda dialog: dialog.accept())
    assert page.request.post(f"{live_server['url']}/api/workspaces/{first}/activate").ok
    page.goto(f"{live_server['url']}/workspace", timeout=5000)
    page.get_by_role("button", name="Work Locally", exact=True).click()
    page.locator("#stageLocalFoldersModal").get_by_role(
        "button", name="Copy Locally", exact=True
    ).click()
    expect(page.get_by_text("Local · 2 workspaces", exact=True)).to_be_visible(timeout=15000)

    local_row = page.locator(".workspace-folder-row-stacked")
    expect(local_row).to_have_count(1)
    main_box = local_row.locator(".workspace-folder-main").bounding_box()
    actions_box = local_row.locator(".workspace-folder-actions").bounding_box()
    assert main_box is not None
    assert actions_box is not None
    assert actions_box["y"] >= main_box["y"] + main_box["height"]

    assert page.request.post(f"{live_server['url']}/api/workspaces/{second}/activate").ok
    page.reload(timeout=5000)
    expect(page.get_by_text("Local · 2 workspaces", exact=True)).to_be_visible(timeout=5000)
    page.get_by_role("button", name="Discard", exact=True).click()
    expect(page.get_by_role("button", name="Work Locally", exact=True)).to_be_visible(timeout=15000)
    assert (source / "bird.jpg").read_bytes() == b"original"
