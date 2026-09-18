from playwright.sync_api import expect


def test_keep_awake_reminder_opens_with_keyboard_and_click(live_server, page):
    keeping_awake = True
    page.route(
        "**/api/jobs",
        lambda route: route.fulfill(
            json={"active": [], "keeping_awake": keeping_awake},
        ),
    )
    page.route("**/api/jobs/history?*", lambda route: route.fulfill(json=[]))
    page.goto(f"{live_server['url']}/jobs")

    note = page.locator("#keepingAwakeNote")
    summary = note.locator("summary")
    reminder = note.get_by_text("Keep your laptop open while jobs run", exact=True)
    expect(summary).to_be_visible()
    expect(reminder).to_be_hidden()

    summary.focus()
    summary.press("Enter")
    expect(reminder).to_be_visible()
    summary.press("Space")
    expect(reminder).to_be_hidden()
    summary.click()
    expect(reminder).to_be_visible()

    keeping_awake = False
    page.reload()
    expect(note).to_be_hidden()


def _move_folder_job(live_server, config):
    base = {
        "folder_id": 42,
        "destination": "/Volumes/Photos/Archive",
        "source_path": "/Volumes/Camera/Paris",
        "resolved_destination": "/Volumes/Photos/Archive",
        "merge": False,
    }
    base.update(config)
    return {
        "id": "move-folder-route-test",
        "type": "move-folder",
        "status": "running",
        "started_at": "2026-08-16T21:34:53",
        "finished_at": None,
        "duration": None,
        "progress": {
            "current": 31,
            "total": 503,
            "current_file": "DSC_5656.NEF",
            "phase": "Organizing by capture date",
        },
        "result": None,
        "errors": [],
        "config": base,
        "workspace_id": live_server["db"]._active_workspace_id,
        "steps": [],
        "pausable": False,
    }


def _serve_jobs_page(live_server, page, job, *, history=False):
    """Serve one job either as an active job (default) or as a history row.

    History jobs need a click flow to reach the detail card, so this helper
    also opens the detail pane for that case.
    """
    active_jobs = [] if history else [job]
    history_jobs = [job] if history else []
    page.route(
        "**/api/jobs",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            json={
                "active": active_jobs,
                "active_workspace_id": live_server["db"]._active_workspace_id,
                "workspace_names": {},
                "keeping_awake": True,
            },
        ),
    )
    page.route(
        "**/api/jobs/history?*",
        lambda route: route.fulfill(
            status=200, content_type="application/json", json=history_jobs
        ),
    )
    page.goto(f"{live_server['url']}/jobs")
    if history:
        # Wait for the history row to render, then open its detail card.
        page.locator(
            '.job-list-item[data-job-id="' + job["id"] + '"]'
        ).click()


def test_move_folder_job_shows_source_and_destination(live_server, page):
    """History entries predating the plan snapshot keep the generic note."""
    job = _move_folder_job(live_server, {"folder_template": "%Y/%Y-%m-%d"})
    _serve_jobs_page(live_server, page, job)

    move_route = page.locator(".job-move-route")
    expect(move_route).to_be_visible()
    expect(move_route.locator(".job-move-route-label")).to_have_text(
        ["From", "To"]
    )
    expect(move_route.locator(".job-move-route-path")).to_have_text(
        ["/Volumes/Camera/Paris", "/Volumes/Photos/Archive"]
    )
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "Organizing photos into capture-date folders using %Y/%Y-%m-%d"
    )


def test_move_folder_job_shows_the_single_capture_date_folder(
    live_server, page,
):
    """One capture date: "To" is the date folder itself, and says so.

    While the move is live, the counts are the pre-move plan, so the note
    labels them as planned rather than claiming every photo already landed.
    """
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "resolved_destination": "/Volumes/Photos/Archive/2026-09-12",
        "date_destinations": [{
            "path": "/Volumes/Photos/Archive/2026-09-12",
            "relative_path": "2026-09-12",
            "photo_count": 499,
        }],
        "date_destination_count": 1,
        "date_photo_count": 499,
    })
    _serve_jobs_page(live_server, page, job)

    move_route = page.locator(".job-move-route")
    expect(move_route.locator(".job-move-route-path")).to_have_text(
        ["/Volumes/Camera/Paris", "/Volumes/Photos/Archive/2026-09-12"]
    )
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "All 499 photos planned to land in this single folder"
    )
    expect(move_route.locator(".job-move-route-dates")).to_have_count(0)


def test_move_folder_job_lists_the_capture_date_folders(live_server, page):
    """Several capture dates: "To" is the root, with the fan-out listed.

    Live counts are the plan, so the header says "planned" — not that the
    photos have already been split.
    """
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "date_destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "relative_path": "2026-09-12",
                "photo_count": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "relative_path": "2026-09-13",
                "photo_count": 199,
            },
        ],
        "date_destination_count": 5,
        "date_photo_count": 700,
    })
    _serve_jobs_page(live_server, page, job)

    move_route = page.locator(".job-move-route")
    expect(move_route.locator(".job-move-route-path")).to_have_text(
        ["/Volumes/Camera/Paris", "/Volumes/Photos/Archive"]
    )
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "700 photos planned across 5 capture-date folders under this path"
    )
    expect(move_route.locator(".job-move-route-dates li")).to_have_text([
        "2026-09-12 · 300 photos",
        "2026-09-13 · 199 photos",
        "+ 3 more folders",
    ])


def test_move_folder_job_reports_partial_single_landing_after_completion(
    live_server, page,
):
    """Completed history uses ``result`` to show what actually landed.

    ``move_folder_by_date`` skips missing sources and destination collisions,
    so a "planned 499, moved 497" split is real. The note must not claim all
    499 landed when only 497 did.
    """
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "resolved_destination": "/Volumes/Photos/Archive/2026-09-12",
        "date_destinations": [{
            "path": "/Volumes/Photos/Archive/2026-09-12",
            "relative_path": "2026-09-12",
            "photo_count": 499,
        }],
        "date_destination_count": 1,
        "date_photo_count": 499,
    })
    job["status"] = "completed"
    job["finished_at"] = "2026-08-16T21:39:12"
    job["result"] = {
        "moved": 497,
        "errors": ["skipped 2 photos"],
        "destinations": [{
            "path": "/Volumes/Photos/Archive/2026-09-12",
            "planned": 499,
            "moved": 497,
        }],
        "destination_count": 1,
    }
    _serve_jobs_page(live_server, page, job, history=True)

    move_route = page.locator(".job-move-route")
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "497 of 499 photos landed in this single folder"
    )


def test_move_folder_job_reports_partial_fanout_after_completion(
    live_server, page,
):
    """Completed fan-out: header shows moved of planned, per-folder rows
    say how many actually moved when it differs from the plan."""
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "date_destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "relative_path": "2026-09-12",
                "photo_count": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "relative_path": "2026-09-13",
                "photo_count": 199,
            },
        ],
        "date_destination_count": 2,
        "date_photo_count": 499,
    })
    job["status"] = "completed"
    job["finished_at"] = "2026-08-16T21:39:12"
    job["result"] = {
        "moved": 495,
        "errors": ["skipped 4 photos"],
        "destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "planned": 300,
                "moved": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "planned": 199,
                "moved": 195,
            },
        ],
        "destination_count": 2,
    }
    _serve_jobs_page(live_server, page, job, history=True)

    move_route = page.locator(".job-move-route")
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "495 of 499 photos landed in 2 capture-date folders"
    )
    expect(move_route.locator(".job-move-route-dates li")).to_have_text([
        "2026-09-12 · 300 photos",
        "2026-09-13 · 195 of 199 photos moved",
    ])


def test_move_folder_job_prefers_the_result_over_the_stale_plan(
    live_server, page,
):
    """A re-plan between enqueue and run must not leave history lying.

    The worker re-plans when it starts, so a capture-time edit landing in
    between can send every photo somewhere the enqueue-time snapshot never
    named. The finished route is drawn from ``result.destinations``, so "To"
    names the folder that actually received the photos — and the plan's own
    folder count is stated as a plain fact rather than dropped.
    """
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "date_destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "relative_path": "2026-09-12",
                "photo_count": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "relative_path": "2026-09-13",
                "photo_count": 199,
            },
        ],
        "date_destination_count": 2,
        "date_photo_count": 499,
    })
    job["status"] = "completed"
    job["finished_at"] = "2026-08-16T21:39:12"
    job["result"] = {
        "moved": 499,
        "errors": [],
        # The worker put everything in one corrected date folder.
        "destinations": [{
            "path": "/Volumes/Photos/Archive/2026-09-11",
            "planned": 499,
            "moved": 499,
        }],
        "destination_count": 1,
    }
    _serve_jobs_page(live_server, page, job, history=True)

    move_route = page.locator(".job-move-route")
    expect(move_route.locator(".job-move-route-path")).to_have_text(
        ["/Volumes/Camera/Paris", "/Volumes/Photos/Archive/2026-09-11"]
    )
    note = move_route.locator(".job-move-route-note")
    expect(note).to_contain_text("All 499 photos landed in this single folder")
    # One folder here is the run's outcome, not proof the template collapses
    # to one path, so the note must not explain it that way.
    expect(note).to_contain_text("the plan at start had 2 folders")
    expect(note).not_to_contain_text("resolves to one path")


def test_move_folder_job_names_every_folder_the_move_actually_used(
    live_server, page,
):
    """A finished fan-out lists the result's folders, not the plan's."""
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "resolved_destination": "/Volumes/Photos/Archive/2026-09-12",
        "date_destinations": [{
            "path": "/Volumes/Photos/Archive/2026-09-12",
            "relative_path": "2026-09-12",
            "photo_count": 499,
        }],
        "date_destination_count": 1,
        "date_photo_count": 499,
    })
    job["status"] = "completed"
    job["finished_at"] = "2026-08-16T21:39:12"
    job["result"] = {
        "moved": 499,
        "errors": [],
        "destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "planned": 300,
                "moved": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "planned": 199,
                "moved": 199,
            },
        ],
        "destination_count": 2,
    }
    _serve_jobs_page(live_server, page, job, history=True)

    move_route = page.locator(".job-move-route")
    # Two landing folders means there is no single "To" path; the selected
    # root is the honest answer even though the plan named one folder.
    expect(move_route.locator(".job-move-route-path")).to_have_text(
        ["/Volumes/Camera/Paris", "/Volumes/Photos/Archive"]
    )
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "All 499 photos landed in 2 capture-date folders"
    )
    expect(move_route.locator(".job-move-route-dates li")).to_have_text([
        "2026-09-12 · 300 photos",
        "2026-09-13 · 199 photos",
        "Planned at start: 1 folder",
    ])


def test_move_folder_job_does_not_count_a_folder_that_got_nothing(
    live_server, page,
):
    """A group whose photos were all skipped is not a landing folder.

    ``move_folder_by_date`` still reports that destination with ``moved: 0``
    (a missing source or a same-name file at the destination skips the photo
    rather than failing the job), so the header counts only the folders that
    received photos while the list still shows the empty one for what it is.
    """
    job = _move_folder_job(live_server, {
        "folder_template": "%Y-%m-%d",
        "date_destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "relative_path": "2026-09-12",
                "photo_count": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "relative_path": "2026-09-13",
                "photo_count": 199,
            },
        ],
        "date_destination_count": 2,
        "date_photo_count": 499,
    })
    job["status"] = "completed"
    job["finished_at"] = "2026-08-16T21:39:12"
    job["result"] = {
        "moved": 300,
        "errors": ["199 photos already exist at the destination"],
        "destinations": [
            {
                "path": "/Volumes/Photos/Archive/2026-09-12",
                "planned": 300,
                "moved": 300,
            },
            {
                "path": "/Volumes/Photos/Archive/2026-09-13",
                "planned": 199,
                "moved": 0,
            },
        ],
        "destination_count": 2,
    }
    _serve_jobs_page(live_server, page, job, history=True)

    move_route = page.locator(".job-move-route")
    expect(move_route.locator(".job-move-route-note")).to_contain_text(
        "300 of 499 photos landed in 1 capture-date folder"
    )
    expect(move_route.locator(".job-move-route-dates li")).to_have_text([
        "2026-09-12 · 300 photos",
        "2026-09-13 · 0 of 199 photos moved",
    ])


def test_label_preparation_shows_progress_and_one_estimate(live_server, page):
    from datetime import datetime, timedelta

    started_at = (datetime.now() - timedelta(minutes=10)).isoformat()
    job = {
        "id": "pipeline-label-preparation",
        "type": "pipeline", "status": "running", "started_at": started_at,
        "workspace_id": live_server["db"]._active_workspace_id,
        "config": {"collection_name": "One photo"}, "errors": [],
        "progress": {
            "phase": "Preparing species labels for BioCLIP-2.5",
            "current": 16, "total": 104,
            "phase_current": 100, "phase_total": 1419, "phase_label": "Species labels",
        },
        "steps": [{
            "id": "model_loader", "label": "Load models", "status": "running",
            "started_at": started_at,
            "progress": {"current": 100, "total": 1419, "unit": "labels"},
            "current_file": "100 / 1,419 labels ready · about 127 min remaining",
        }],
    }
    page.route("**/api/jobs", lambda route: route.fulfill(json={"active": [job], "history": []}))
    page.route("**/api/jobs/history?*", lambda route: route.fulfill(json=[]))
    page.goto(f"{live_server['url']}/jobs")
    step = page.locator('[data-step-id="model_loader"]')
    expect(step).to_be_visible()
    expect(step.locator('.tree-step-current-file')).to_have_text(job["steps"][0]["current_file"])
    expect(step.locator('.tree-step-progress-text').first).to_have_text('100 / 1,419')
    # The browser must not invent a second estimate from total elapsed time,
    # which includes cached labels and time spent paused.
    expect(step.locator('.tree-step-throughput')).to_have_count(0)
