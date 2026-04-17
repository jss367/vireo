"""Scenario: visit the workspace management page.

Verifies that /workspace renders, the active workspace name appears,
folder and workspace sections are present, and the "New Workspace" button
exists.
"""

import contextlib


def run(session):
    session.goto("/workspace")

    # Wait for loadWorkspaces()'s /api/workspaces fetch to resolve and
    # replace the initial "Loading..." placeholder in #workspacesContent
    # rather than using a fixed sleep.  On loaded CI the async fetch can
    # exceed a 1s wait, leaving the page in its loading state and
    # producing false BUGs on the workspace-input assertion below.  Fall
    # through on timeout so assertions surface the stale state.
    with contextlib.suppress(Exception):
        session.page.wait_for_function(
            """() => {
                const c = document.getElementById('workspacesContent');
                return c && !c.textContent.includes('Loading');
            }""",
            timeout=10000,
        )

    session.screenshot("workspace-initial")

    # The workspace name heading should exist and show "Default"
    ws_name = session.eval(
        "(document.getElementById('wsPageName') || {}).textContent || ''"
    )
    session.assert_that(
        ws_name.strip() == "Default",
        f"expected workspace name 'Default', got {ws_name!r}",
    )

    # The Folders section should exist
    has_folders = session.eval("!!document.getElementById('wsFoldersContent')")
    session.assert_that(has_folders, "expected folders section on workspace page")

    # The Recent Jobs section should exist
    has_history = session.eval("!!document.getElementById('historyContent')")
    session.assert_that(has_history, "expected recent jobs section on workspace page")

    # The All Workspaces section should exist
    has_workspaces = session.eval("!!document.getElementById('workspacesContent')")
    session.assert_that(has_workspaces, "expected all workspaces section on workspace page")

    # The "+ New Workspace" button should exist
    has_new_ws_btn = session.eval(
        """!!document.querySelector('button[onclick*="showCreateWorkspaceModal"]')"""
    )
    session.assert_that(has_new_ws_btn, "expected '+ New Workspace' button")

    # The "+ Add Folder" link should exist and point to /pipeline
    has_add_folder = session.eval(
        """!!document.querySelector('a.btn[href="/pipeline"]')"""
    )
    session.assert_that(has_add_folder, "expected '+ Add Folder' link to /pipeline")

    # The workspaces section should show at least the Default workspace
    # (rendered dynamically by loadWorkspaces JS)
    ws_inputs = session.eval(
        """document.querySelectorAll('#workspacesContent input[type="text"]').length"""
    )
    session.assert_that(ws_inputs >= 1, f"expected at least 1 workspace entry, got {ws_inputs}")

    session.screenshot("workspace-loaded")
