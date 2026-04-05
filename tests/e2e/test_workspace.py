from playwright.sync_api import expect


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
