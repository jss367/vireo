from playwright.sync_api import expect


def test_navbar_links_navigate_to_pipeline(live_server, page):
    """Clicking Pipeline in navbar navigates to the pipeline page."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='nav-pipeline']")
    expect(page).to_have_url(f"{url}/pipeline")


def test_navbar_links_navigate_to_jobs(live_server, page):
    """Clicking Jobs in navbar navigates to the jobs page."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='nav-jobs']")
    expect(page).to_have_url(f"{url}/jobs")


def test_navbar_links_navigate_to_browse(live_server, page):
    """Clicking Browse in navbar navigates to the browse page."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("[data-testid='nav-browse']")
    expect(page).to_have_url(f"{url}/browse")


def test_workspace_dropdown_shows_current(live_server, page):
    """Workspace dropdown displays the current workspace name."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    dropdown = page.locator("[data-testid='workspace-dropdown']")
    expect(dropdown).to_contain_text("Default")


def test_workspace_switch(live_server, page):
    """Switching workspace updates the dropdown to show the new workspace."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='workspace-dropdown']")
    # Wait for workspace menu items to load (fetched via JS)
    page.locator(".ws-menu-item", has_text="Field Work").wait_for(state="visible")
    page.locator(".ws-menu-item", has_text="Field Work").click()
    page.wait_for_load_state("networkidle")
    dropdown = page.locator("[data-testid='workspace-dropdown']")
    expect(dropdown).to_contain_text("Field Work")


def test_workspace_persists_across_navigation(live_server, page):
    """After switching workspace, navigating to another page keeps the workspace."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='workspace-dropdown']")
    # Wait for workspace menu items to load (fetched via JS)
    page.locator(".ws-menu-item", has_text="Field Work").wait_for(state="visible")
    page.locator(".ws-menu-item", has_text="Field Work").click()
    page.wait_for_load_state("networkidle")
    page.click("[data-testid='nav-pipeline']")
    page.wait_for_load_state("networkidle")
    dropdown = page.locator("[data-testid='workspace-dropdown']")
    expect(dropdown).to_contain_text("Field Work")
