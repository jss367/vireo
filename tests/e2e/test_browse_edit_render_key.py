"""Grid thumbnail URLs must change when any part of the edit recipe does.

``serve_thumbnail`` answers ``Cache-Control: public, max-age=86400``. Saving an
edit deletes the cached JPEG server-side, but a browser holding a day-old copy
of the same URL never asks — so the query string is the only thing that tells
it to refetch. The fingerprint used to be built from a hand-listed subset of
recipe fields (rotation/straighten/flip/crop/adjustments) that omitted the
mask-weighted ``local`` section, which left Browse showing pre-edit pixels for
24h after a subject/background adjustment.
"""

from urllib.parse import parse_qs, urlparse

import pytest


def _local_only_recipe(subject_exposure):
    return {
        "local": {
            "mask": {"ref": "a1b2c3d4e5f6", "source_digest": "sha1:0011223344556677"},
            "regions": [
                {"region": "subject", "adjustments": {"exposure": subject_exposure}},
            ],
        },
    }


def _grid_thumbnail_url(page, photo_id):
    """The URL Browse's card for ``photo_id`` would fetch."""
    locator = page.locator(f"img[data-thumbnail-src*='/thumbnails/{photo_id}.jpg']")
    locator.first.wait_for(state="attached", timeout=30000)
    return locator.first.get_attribute("data-thumbnail-src")


def _render_fingerprint(url):
    return parse_qs(urlparse(url).query).get("er", [None])[0]


@pytest.mark.e2e
def test_local_only_edit_changes_the_grid_thumbnail_url(live_server, page):
    photo_id = live_server["data"]["photos"][0]
    db = live_server["db"]

    page.goto(live_server["url"] + "/browse")
    unedited = _grid_thumbnail_url(page, photo_id)
    assert _render_fingerprint(unedited) is None, (
        "an unedited photo should keep the bare thumbnail URL so the browser "
        f"cache is usable, got {unedited!r}"
    )

    db.set_photo_edit_recipe(photo_id, _local_only_recipe(1.0))
    page.goto(live_server["url"] + "/browse")
    first = _grid_thumbnail_url(page, photo_id)
    assert _render_fingerprint(first), (
        "a local-only edit left the thumbnail URL unchanged; the browser "
        f"would keep serving the pre-edit image for 24h ({first!r})"
    )

    db.set_photo_edit_recipe(photo_id, _local_only_recipe(-2.0))
    page.goto(live_server["url"] + "/browse")
    second = _grid_thumbnail_url(page, photo_id)
    assert _render_fingerprint(second) != _render_fingerprint(first), (
        "changing only the local section reused the previous fingerprint "
        f"({second!r})"
    )


@pytest.mark.e2e
def test_thumbnail_url_falls_back_to_the_recipe_when_no_render_key(live_server, page):
    """A payload carrying only ``edit_recipe`` still busts the cache.

    ``render_key`` comes from the server, but the client keeps its own
    fingerprint for photo dicts that predate it and for recipes this page just
    wrote itself. That fallback has to cover the whole recipe too, or it
    reintroduces the bug for exactly the recipes the server key would have
    fixed.
    """
    page.goto(live_server["url"] + "/browse")
    urls = page.evaluate(
        """(recipes) => ({
            none: window.vireoThumbnailUrl({id: 900001, edit_recipe: null}),
            localA: window.vireoThumbnailUrl({id: 900002, edit_recipe: recipes[0]}),
            localB: window.vireoThumbnailUrl({id: 900003, edit_recipe: recipes[1]}),
            mixedA: window.vireoThumbnailUrl({
                id: 900004, edit_recipe: Object.assign({adjustments: {exposure: 0.5}}, recipes[0]),
            }),
            mixedB: window.vireoThumbnailUrl({
                id: 900005, edit_recipe: Object.assign({adjustments: {exposure: 0.5}}, recipes[1]),
            }),
        })""",
        [_local_only_recipe(1.0), _local_only_recipe(-2.0)],
    )

    assert _render_fingerprint(urls["none"]) is None
    assert _render_fingerprint(urls["localA"]), (
        f"local-only recipe produced no fingerprint: {urls['localA']!r}"
    )
    assert _render_fingerprint(urls["localA"]) != _render_fingerprint(urls["localB"])
    assert _render_fingerprint(urls["mixedA"]) != _render_fingerprint(urls["mixedB"]), (
        "identical global adjustments masked a change in the local section"
    )
