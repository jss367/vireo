"""Scenario: visit the workspace management page.

Verifies that /workspace renders, the active workspace name appears,
folder and workspace sections are present, and the "New Workspace" button
exists.
"""


def run(session):
    session.goto("/workspace")

    # Wait for the workspace page JS to load data
    session.page.wait_for_timeout(1000)

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

    # The "+ New Workspace" button should exist inside the page-local
    # `.content` wrapper.  The navbar (_navbar.html) also has a button
    # with the same `onclick`, so scoping to `.content` ensures we fail
    # if the workspace page's own button is removed even while the
    # navbar action remains.
    has_new_ws_btn = session.eval(
        """!!document.querySelector('.content button[onclick*="showCreateWorkspaceModal"]')"""
    )
    session.assert_that(has_new_ws_btn, "expected '+ New Workspace' button on workspace page")

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
