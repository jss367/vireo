"""End-to-end coverage for the zoomable Life List explorer sunburst."""

import json
from urllib.parse import parse_qs, urlparse

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
    return ids


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


def test_uncounted_identifications_are_reasoned_and_actionable(live_server, page):
    db = live_server["db"]
    ids = _seed_hummingbird_tree(db)
    existing_photo = db.conn.execute(
        "SELECT pk.photo_id FROM photo_keywords pk "
        "JOIN keywords k ON k.id=pk.keyword_id "
        "WHERE k.name='Red-tailed Hawk' LIMIT 1"
    ).fetchone()["photo_id"]
    folder_id = db.conn.execute(
        "SELECT folder_id FROM photos WHERE id=?", (existing_photo,)
    ).fetchone()["folder_id"]

    # A family keyword on a photo that also has a descendant species is
    # redundant and must not appear in the disclosure.
    redundant_id = db.add_keyword("Hummingbirds", kw_type="taxonomy")
    db.conn.execute(
        "UPDATE keywords SET taxon_id=?, is_species=1 WHERE id=?",
        (ids["Trochilidae"], redundant_id),
    )
    db.tag_photo(existing_photo, redundant_id)

    broad_photo = db.add_photo(
        folder_id=folder_id, filename="broad-only.jpg", extension=".jpg",
        file_size=1, file_mtime=10.0,
    )
    broad_id = db.add_keyword("Reference gap", kw_type="taxonomy")
    db.conn.execute(
        "UPDATE keywords SET taxon_id=?, is_species=1 WHERE id=?",
        (ids["Incertae sedis"], broad_id),
    )
    db.tag_photo(broad_photo, broad_id)

    unmatched_photo = db.add_photo(
        folder_id=folder_id, filename="hybrid.jpg", extension=".jpg",
        file_size=1, file_mtime=11.0,
    )
    unmatched_id = db.add_keyword("Mystery hybrid", kw_type="taxonomy")
    db.tag_photo(unmatched_photo, unmatched_id)
    db.conn.commit()

    page.goto(f"{live_server['url']}/life-list?view=explorer")
    disclosures = page.locator(".ll-unmatched")
    expect(disclosures).to_have_count(2)
    expect(disclosures.nth(0)).to_contain_text(
        "1 identification label in Birds isn't included in species totals."
    )
    expect(disclosures.nth(1)).to_contain_text(
        "identification labels elsewhere in this workspace"
    )
    expect(disclosures.nth(1)).to_contain_text("Mystery Hybrid")
    assert "Hummingbirds" not in " ".join(disclosures.all_inner_texts())

    disclosures.nth(0).locator("summary").click()
    expect(disclosures.nth(0)).to_contain_text("Reference Gap")
    expect(disclosures.nth(0)).to_contain_text("Identified at family rank")
    expect(disclosures.nth(0)).to_contain_text("1 photo")

    href = disclosures.nth(0).get_by_role("link", name="View photos").get_attribute("href")
    filters = json.loads(parse_qs(urlparse(href).query)["filters"][0])
    assert filters == {
        "root": {
            "mode": "all",
            "rules": [
                {
                    "field": "life_list_uncounted",
                    "op": "is",
                    "value": json.dumps(
                        {
                            "name": "Reference Gap",
                            "taxon_id": ids["Incertae sedis"],
                        },
                        separators=(",", ":"),
                    ),
                }
            ],
        }
    }


def _hold_genus_species(page):
    """Hold the next species-leaf request until the test releases it."""
    held = []
    page.route(
        lambda url: "/api/life-list/explorer/species" in url,
        lambda route: held.append(route),
        times=1,
    )
    return held


def _open_hummingbird_family(live_server, page):
    _seed_hummingbird_tree(live_server["db"])
    page.goto(f"{live_server['url']}/life-list?view=explorer")
    page.locator(".ll-card", has_text="Swifts and Hummingbirds").click()
    page.locator(".ll-card", has_text="Hummingbirds").click()
    expect(page.locator("#explorerSunburstCenter")).to_have_attribute(
        "data-name", "Hummingbirds"
    )


def _release(page, held):
    with page.expect_response(lambda r: "/api/life-list/explorer/species" in r.url):
        for route in held:
            route.continue_()
    # Let the resolved fetch run its continuation before asserting.
    page.wait_for_timeout(200)


def test_late_genus_species_response_does_not_replace_breadcrumb_navigation(
    live_server, page
):
    _open_hummingbird_family(live_server, page)
    held = _hold_genus_species(page)
    with page.expect_request(lambda r: "/api/life-list/explorer/species" in r.url):
        page.locator(".ll-card", has_text="Archilochus").click()
    expect(page.locator("#explorerBody")).to_contain_text("Loading species")
    assert held

    # Leave the genus before its species arrive.
    page.locator(".ll-crumb[data-depth='2']").click()
    expect(page.locator(".ll-card", has_text="Selasphorus")).to_be_visible()

    _release(page, held)
    expect(page.locator(".ll-leaf-head")).to_have_count(0)
    expect(page.locator(".ll-card", has_text="Archilochus")).to_be_visible()
    expect(page.locator(".ll-card", has_text="Selasphorus")).to_be_visible()
    expect(page.locator(".ll-crumb.current")).to_have_text("Hummingbirds")


def test_late_genus_species_response_does_not_replace_another_genus(live_server, page):
    _open_hummingbird_family(live_server, page)
    held = _hold_genus_species(page)
    with page.expect_request(lambda r: "/api/life-list/explorer/species" in r.url):
        page.locator(".ll-card", has_text="Archilochus").click()
    assert held

    page.locator(".ll-crumb[data-depth='2']").click()
    page.locator(".ll-card", has_text="Selasphorus").click()
    expect(page.locator(".ll-leaf-head")).to_contain_text("0/1 species in Selasphorus")
    expect(page.locator(".ll-sp", has_text="Selasphorus rufus")).to_be_visible()

    _release(page, held)
    expect(page.locator(".ll-leaf-head")).to_contain_text("0/1 species in Selasphorus")
    expect(page.locator(".ll-sp", has_text="Selasphorus rufus")).to_be_visible()
    expect(page.locator(".ll-sp", has_text="Archilochus colubris")).to_have_count(0)


def test_rank_view_over_pending_genus_returns_to_that_genus(live_server, page):
    _open_hummingbird_family(live_server, page)
    # Load one genus fully so a leaf payload is cached.
    page.locator(".ll-card", has_text="Archilochus").click()
    expect(page.locator(".ll-sp", has_text="Archilochus colubris")).to_be_visible()
    page.locator(".ll-crumb[data-depth='2']").click()

    held = _hold_genus_species(page)
    with page.expect_request(lambda r: "/api/life-list/explorer/species" in r.url):
        page.locator(".ll-card", has_text="Selasphorus").click()
    assert held

    # Open a flat rank view while Selasphorus is still loading.
    page.locator(".ll-sumchip[data-rank='family']").click()
    expect(page.locator("#rankBackBtn")).to_be_visible()
    _release(page, held)
    expect(page.locator("#rankBackBtn")).to_be_visible()

    # Back to cards returns to the genus the user opened, with its own species
    # rather than the previously cached genus's.
    page.locator("#rankBackBtn").click()
    expect(page.locator(".ll-leaf-head")).to_contain_text("0/1 species in Selasphorus")
    expect(page.locator(".ll-sp", has_text="Selasphorus rufus")).to_be_visible()
    expect(page.locator(".ll-sp", has_text="Archilochus colubris")).to_have_count(0)
