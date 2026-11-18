"""The iNaturalist dialog for a photo that shows more than one species."""

from playwright.sync_api import expect

OPTIONS = [
    {"species": "House Finch", "scientific_name": "Haemorhous mexicanus",
     "source": "confirmed", "confidence": None, "primary": False},
    {"species": "Northern Cardinal", "scientific_name": "Cardinalis cardinalis",
     "source": "prediction", "confidence": 0.95, "primary": True},
]


def _prepared(mode, options):
    return {
        "species": "Northern Cardinal", "scientific_name": "Cardinalis cardinalis",
        "confidence": 0.95, "species_options": options,
        "timestamp": "2024-06-01T10:00:00", "latitude": 32.7, "longitude": -117.1,
        "filename": "bird.jpg", "edit_recipe": None,
        "upload_url": "https://www.inaturalist.org/observations/upload",
        "mode": mode, "already_submitted": False, "existing_observation_url": None,
    }


def _open(live_server, page, mode, options):
    page.route("**/api/inat/prepare/1", lambda route: route.fulfill(json=_prepared(mode, options)))
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("submitToInat(1)")
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")


def test_quick_dialog_says_the_photo_has_two_species_and_sends_the_chosen_one(live_server, page):
    _open(live_server, page, "quick", OPTIONS)
    expect(page.locator("#inatSpeciesNote0")).to_have_text(
        "This photo has 2 species: House Finch, Northern Cardinal. An iNaturalist "
        "observation records one taxon, so choose which one to send."
    )
    picker = page.locator("#inatSpecies0")
    # The default stays the prefilled primary taxon.
    expect(picker.locator("option:checked")).to_have_text(
        "Northern Cardinal (Cardinalis cardinalis) · 95% prediction")
    expect(page.locator("#inatTaxonText0")).to_have_text("Cardinalis cardinalis")
    picker.select_option(label="House Finch (Haemorhous mexicanus) · confirmed")
    expect(page.locator("#inatTaxonText0")).to_have_text("Haemorhous mexicanus")

    # The choice carries into direct submission after a token is saved.
    page.route("**/api/inat/token", lambda route: route.fulfill(json={"ok": True, "login": "birder42"}))
    page.locator("#inatQuickToken").fill("valid-token")
    page.locator("#inatQuickTokenBtn").click()
    expect(page.locator("#inatTaxon0")).to_have_value("Haemorhous mexicanus")
    expect(page.locator("#inatSpecies0 option:checked")).to_have_text(
        "House Finch (Haemorhous mexicanus) · confirmed")


def test_direct_dialog_picker_fills_the_taxon_field(live_server, page):
    _open(live_server, page, "direct", OPTIONS)
    expect(page.locator("#inatSpeciesNote0")).to_contain_text("This photo has 2 species")
    expect(page.locator("#inatTaxon0")).to_have_value("Cardinalis cardinalis")
    page.locator("#inatSpecies0").select_option(label="House Finch (Haemorhous mexicanus) · confirmed")
    expect(page.locator("#inatTaxon0")).to_have_value("Haemorhous mexicanus")


def test_single_species_photo_has_no_picker(live_server, page):
    _open(live_server, page, "quick", OPTIONS[1:])
    expect(page.locator("#inatSpecies0")).to_have_count(0)
    expect(page.locator("#inatTaxonText0")).to_have_text("Cardinalis cardinalis")


def test_species_queue_compares_account_and_sends_selected_candidate(live_server, page):
    def entry(name, scientific, remote_id, local_id):
        return {"species": name, "scientific_name": scientific, "taxon_rank": "species",
                "taxon_id": local_id, "inat_id": remote_id, "photo_count": 2,
                "best": {"id": 1, "filename": "chosen.jpg"}}

    entries = [entry("House Finch", "Haemorhous mexicanus", 100, 99),
               entry("Northern Cardinal", "Cardinalis cardinalis", 99, 100),
               entry("Unlinked", None, None, None)]
    page.route("**/api/life-list?*", lambda r: r.fulfill(json={"species": entries}))
    page.route("**/api/inat/account-species?*", lambda r: r.fulfill(json={
        "login": "birder", "taxon_ids": [99], "refreshed_at": "2026-10-01T10:00:00Z",
    }))
    page.route("**/api/life-list/species?*", lambda r: r.fulfill(json={
        "photos": [{"id": 2, "filename": "alternate.jpg"}], "photo_count": 2,
    }))
    page.route("**/api/inat/prepare/2", lambda r: r.fulfill(json=_prepared("direct", OPTIONS)))
    submissions = []

    def submit(route):
        submissions.append(route.request.post_data_json)
        route.fulfill(json={"observation_url": "https://www.inaturalist.org/observations/1"})

    page.route("**/api/inat/submit", submit)
    page.goto(f"{live_server['url']}/inaturalist")
    page.get_by_label("iNaturalist username").fill("birder")
    page.get_by_role("button", name="Refresh").click()
    expect(page.locator("article")).to_have_count(1)
    expect(page.locator("article")).to_contain_text("House Finch")
    expect(page.locator("article")).to_contain_text("chosen.jpg")
    page.get_by_role("button", name="Next").click()
    expect(page.locator("article")).to_contain_text("alternate.jpg")
    page.get_by_role("button", name="Send to iNaturalist").click()
    expect(page.locator("#inatTaxon0")).to_have_value("Haemorhous mexicanus")
    page.locator("#inatSubmitBtn").click()
    expect(page.locator("article")).to_have_count(0)
    assert submissions[0]["expected_login"] == "birder"
    assert submissions[0]["taxon_id"] == 100
    assert submissions[0]["photo_id"] == 2
    expect(page.locator("#receipt a")).to_have_attribute("href", "https://www.inaturalist.org/observations/1")


def test_submission_preserves_pending_account_switch(live_server, page):
    def snapshot(login):
        return {"login": login, "taxon_ids": [], "refreshed_at": "2026-10-01T10:00:00Z"}

    pending = []
    page.route("**/api/life-list?*", lambda r: r.fulfill(json={"species": []}))
    page.route("**/api/inat/account-species?login=first", lambda r: r.fulfill(json=snapshot("first")))
    page.route("**/api/inat/account-species?login=second", lambda route: pending.append(route))
    page.goto(f"{live_server['url']}/inaturalist")
    login = page.get_by_label("iNaturalist username")
    refresh = page.get_by_role("button", name="Refresh")
    login.fill("first")
    refresh.click()
    expect(page.locator("#summary")).to_contain_text("first ·")
    login.fill("second")
    with page.expect_request("**/api/inat/account-species?login=second"):
        refresh.click()
    page.evaluate("""document.dispatchEvent(new CustomEvent('inat:species-submitted', {
        detail: {login: 'first', id: 100, url: 'https://www.inaturalist.org/observations/1'}
    }))""")
    expect(page.locator("#status")).to_have_text("Checking observations…")
    cached = page.evaluate("JSON.parse(localStorage.getItem('vireo.inaturalist.observer'))")
    assert cached["login"] == "first" and cached["taxon_ids"] == [100]
    pending[0].fulfill(json=snapshot("second"))
    expect(page.locator("#summary")).to_contain_text("second ·")
    expect(page.locator("#status")).to_be_empty()
