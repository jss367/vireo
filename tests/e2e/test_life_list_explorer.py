"""End-to-end coverage for the zoomable Life List explorer sunburst."""

from playwright.sync_api import expect


def _seed_hummingbird_tree(db):
    rows = [
        ("Aves", "Birds", "class", None),
        ("Apodiformes", "Swifts and Hummingbirds", "order", "Aves"),
        ("Trochilidae", "Hummingbirds", "family", "Apodiformes"),
        ("Archilochus", None, "genus", "Trochilidae"),
        ("Archilochus colubris", "Ruby-throated Hummingbird", "species", "Archilochus"),
        ("Selasphorus", None, "genus", "Trochilidae"),
        ("Selasphorus rufus", "Rufous Hummingbird", "species", "Selasphorus"),
        ("Incertae sedis", "Reference gap", "family", "Apodiformes"),
        ("Unplaced hummingbird", None, "genus", "Incertae sedis"),
    ]
    ids = {}
    for name, common_name, rank, parent_name in rows:
        ids[name] = db.conn.execute(
            "INSERT INTO taxa (name, common_name, rank, parent_id) VALUES (?, ?, ?, ?)",
            (name, common_name, rank, ids.get(parent_name)),
        ).lastrowid

    # Mark one species found so both the completeness color and empty branch
    # remain represented in the focused family view.
    db.conn.execute(
        "UPDATE keywords SET taxon_id = ?, type = 'taxonomy' WHERE name = ?",
        (ids["Archilochus colubris"], "Red-tailed Hawk"),
    )
    db.conn.commit()


def test_sunburst_expands_selected_taxon(live_server, page):
    _seed_hummingbird_tree(live_server["db"])
    page.goto(f"{live_server['url']}/life-list?view=explorer")

    center = page.locator("#explorerSunburstCenter")
    expect(center).to_have_attribute("data-name", "Birds")

    page.locator(".ll-card", has_text="Swifts and Hummingbirds").click()
    expect(center).to_have_attribute("data-name", "Swifts and Hummingbirds")

    page.locator(".ll-card", has_text="Hummingbirds").click()
    expect(center).to_have_attribute("data-name", "Hummingbirds")

    # The family now owns the full circle: only its genera are rendered as
    # arcs, rather than retaining the tiny whole-class order/family rings.
    expect(page.locator(".ll-sb-arc")).to_have_count(2)
    assert set(page.locator(".ll-sb-arc").evaluate_all(
        "els => els.map(el => el.dataset.name)"
    )) == {"Archilochus", "Selasphorus"}

    # The center acts as a one-level-up control and keeps chart + cards synced.
    center.click()
    expect(center).to_have_attribute("data-name", "Swifts and Hummingbirds")
    expect(page.locator(".ll-card", has_text="Hummingbirds")).to_be_visible()

    # Zero-total reference branches still retain the selected center, their
    # equal-width child arcs, and therefore the one-level-up control.
    page.locator(".ll-card", has_text="Reference gap").click()
    expect(center).to_have_attribute("data-name", "Reference gap")
    expect(page.locator(".ll-sb-arc")).to_have_count(1)
    expect(page.locator(".ll-sb-arc")).to_have_attribute(
        "data-name", "Unplaced hummingbird"
    )
