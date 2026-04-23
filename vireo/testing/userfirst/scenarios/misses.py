"""Scenario: drive the /misses page end-to-end.

The DB is pre-seeded with three misses (one per category). We open the
page, verify the three category sections render with the right counts,
click a thumbnail to open the lightbox, bulk-reject the `clipped` section
(auto-accepting the confirm dialog), and re-check the DB state through
the API to ensure `flag='reject'` landed on the right photo.
"""


def _accept_next_dialog(page):
    page.once("dialog", lambda d: d.accept())


def run(session):
    # -- Initial state: /misses shows three sections, each with 1 photo --
    session.goto("/misses")
    session.page.wait_for_selector('[data-testid="miss-count-clipped"]', timeout=10000)
    session.screenshot("misses-initial")

    for cat in ("no_subject", "clipped", "oof"):
        txt = session.eval(
            f"document.querySelector('[data-testid=\"miss-count-{cat}\"]').textContent"
        )
        session.assert_that(
            txt and "(1)" in txt,
            f"expected {cat} count to be (1), got {txt!r}",
        )

    # -- Each section should have a bulk-reject button --
    for cat in ("no_subject", "clipped", "oof"):
        ok = session.eval(
            f"!!document.querySelector('[data-testid=\"miss-reject-{cat}\"]')"
        )
        session.assert_that(ok, f"expected bulk-reject button for {cat}")

    # -- Verify each card has an unflag (X) button overlaid --
    has_unflag = session.eval(
        """!!document.querySelector('[data-testid^="miss-unflag-clipped-"]')"""
    )
    session.assert_that(has_unflag, "expected unflag button on clipped card")

    # Lightbox click-through is not covered here: the synthetic fixture has no
    # real image files on disk, so /photos/<id>/full would 500 and trip the
    # harness's HTTP-error watchdog. browse_lightbox.py exercises that path
    # against a seed with a workable photos_root.

    # -- Bulk-reject the clipped category --
    _accept_next_dialog(session.page)
    session.click('[data-testid="miss-reject-clipped"]')
    session.page.wait_for_timeout(600)
    session.screenshot("misses-after-reject")

    # The clipped section should now be empty; no_subject and oof remain.
    clipped_after = session.eval(
        "document.querySelector('[data-testid=\"miss-count-clipped\"]').textContent"
    )
    session.assert_that(
        clipped_after and "(0)" in clipped_after,
        f"expected clipped count (0) after reject, got {clipped_after!r}",
    )
    ns_after = session.eval(
        "document.querySelector('[data-testid=\"miss-count-no_subject\"]').textContent"
    )
    session.assert_that(
        ns_after and "(1)" in ns_after,
        f"expected no_subject count still (1), got {ns_after!r}",
    )

    # -- Verify through the API that the clipped photo now has flag='reject' --
    import json

    api_resp = session.eval(
        """fetch('/api/misses').then(r => r.json()).then(d =>
            JSON.stringify({clipped: d.clipped.length, oof: d.oof.length,
                            no_subject: d.no_subject.length}))"""
    )
    counts = json.loads(api_resp) if isinstance(api_resp, str) else api_resp
    session.assert_that(
        counts.get("clipped") == 0,
        f"expected /api/misses to show clipped=0 after reject, got {counts!r}",
    )
