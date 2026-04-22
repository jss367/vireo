"""Regression test for the /highlights initial-load scope alignment.

Codex P1 on PR 617: on first load, the page's fetch was sent before the
folder dropdown was populated, so no scope/folder_id was passed and the
backend defaulted to a single (most recent) folder. The dropdown was then
populated with ``All folders in this workspace`` as the default-selected
option, so the UI claimed workspace-wide results while the data was in fact
single-folder. This test locks in the fix by asserting that on first render,
the photos shown reflect workspace scope (blending every folder with quality
data) and the dropdown selection matches.
"""
from playwright.sync_api import expect


def _seed_quality_scores_and_species(db, data):
    """Give every seeded photo a quality_score and a species keyword.

    The default seeder tags only one photo per folder with a species; we tag
    all of them here so the species label on the rendered card uniquely
    identifies the source folder (hawks -> park, robins -> yard). That gives
    the test a clean signal for which folders actually contributed photos.
    """
    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    robin_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("American Robin",)
    ).fetchone()["id"]
    # Seed order: park photos (hawks) at indices 0-2, yard (robins) at 3-4.
    species_map = {0: hawk_kid, 1: hawk_kid, 2: hawk_kid, 3: robin_kid, 4: robin_kid}
    for i, pid in enumerate(data["photos"]):
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?",
            (0.9 - i * 0.05, pid),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, species_map[i]),
        )
    db.conn.commit()


def test_initial_load_matches_default_workspace_scope(live_server, page):
    """First fetch must use workspace scope, matching the default selection.

    Seeded data has two folders: ``park`` (3 hawks, 2024-03) and ``yard``
    (2 robins, 2024-06). ``yard`` is the most-recent folder, so without the
    fix the initial fetch — which sends no scope/folder_id — returns only
    robins (2 photos). With the fix, the initial fetch explicitly requests
    ``scope=workspace`` and returns photos from both folders.
    """
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    url = live_server["url"]
    page.goto(f"{url}/highlights", timeout=5000)

    # Wait for the grid to populate (async fetch → render).
    cards = page.locator(".highlights-card")
    expect(cards.first).to_be_visible(timeout=5000)

    # The default-selected dropdown option must be the workspace sentinel,
    # matching the scope the page actually fetched.
    folder_select = page.locator("#folderSelect")
    expect(folder_select).to_have_value("__workspace__")

    # If the initial fetch used workspace scope, both species (Hawk + Robin)
    # are represented. If it only fetched the most-recent folder, we'd only
    # see ``American Robin`` (from ``yard``, the newest folder).
    species_text = set(page.locator(".card-species").all_inner_texts())
    assert "Red-tailed Hawk" in species_text and "American Robin" in species_text, (
        f"Expected both species on initial load (workspace scope), "
        f"got {species_text!r}. This likely means the first fetch used "
        f"folder scope and the UI/data are out of sync."
    )
