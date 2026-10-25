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
