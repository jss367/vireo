"""End-to-end tests for the pending-changes sync overlay."""

from playwright.sync_api import expect


def _make_photo_folders_accessible(live_server, tmp_path, photo_ids):
    """Point selected fixture photos at real, empty folders for XMP writes."""
    db = live_server["db"]
    folder_ids = {
        db.get_photo(photo_id)["folder_id"] for photo_id in photo_ids
    }
    for folder_id in folder_ids:
        folder_path = tmp_path / f"sync-photo-folder-{folder_id}"
        folder_path.mkdir()
        db.conn.execute(
            "UPDATE folders SET path = ? WHERE id = ?",
            (str(folder_path), folder_id),
        )
    db.conn.commit()


def test_escape_closes_pending_changes_overlay(live_server, page):
    """Escape dismisses the Review Pending Changes overlay without leaking."""
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """() => {
            window.__syncEscapeLeaked = 0;
            document.body.addEventListener('keydown', (event) => {
                if (event.key === 'Escape') window.__syncEscapeLeaked += 1;
            });
            openSyncPreview();
        }"""
    )

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay).to_be_visible()

    page.keyboard.press("Escape")

    expect(overlay).to_be_hidden()
    assert page.evaluate("window._syncModalOpen") is False
    assert page.evaluate("window.__syncEscapeLeaked") == 0


def test_escape_closes_lightbox_before_pending_changes_overlay(live_server, page):
    """Stacked popups unwind one at a time, with the newest closing first."""
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """(photoId) => {
            openSyncPreview();
            openLightbox(photoId, 'hawk1.jpg', [
                {id: photoId, filename: 'hawk1.jpg'},
            ]);
        }""",
        photo_id,
    )

    sync_overlay = page.locator("#syncPreviewOverlay")
    lightbox = page.locator("#lightboxOverlay")
    expect(sync_overlay).to_be_visible()
    expect(lightbox).to_have_class("lightbox-overlay active")

    page.keyboard.press("Escape")

    expect(lightbox).not_to_have_class("lightbox-overlay active")
    expect(sync_overlay).to_be_visible()

    page.keyboard.press("Escape")

    expect(sync_overlay).to_be_hidden()


def test_progressive_preview_keeps_first_results_visible_after_later_failure(
    live_server, page,
):
    """Completed batches remain inspectable if a later page cannot load."""
    photo_id = live_server["data"]["photos"][0]

    def route_preview(route):
        if "offset=0" not in route.request.url:
            route.abort()
            return
        route.fulfill(
            status=200,
            content_type="application/json",
            body=(
                '{'
                f'"photos":[{{"photo_id":{photo_id},"filename":"hawk1.jpg",'
                '"folder":"/photos","changes":[{"id":991,"type":"rating",'
                '"value":"5","presentation":{"field":"XMP rating",'
                '"action":"updated","before":"3 stars","after":"5 stars"}}]}],'
                '"total_changes":2,"total_photos":2,'
                '"change_type_counts":{"rating":2},"offset":0,'
                '"next_offset":1,"has_more":true,"revision":"test-revision",'
                '"location_sync_enabled":false}'
            ),
        )

    page.route("**/api/sync/preview?**", route_preview)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("void openSyncPreview()")

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay.locator(".sync-preview-thumb")).to_have_count(1)
    expect(overlay.locator("#syncPreviewProgressText")).to_contain_text(
        "Could not finish"
    )
    expect(overlay.locator(".sync-review-group-title")).to_contain_text(
        "1 photo reviewed so far"
    )
    expect(overlay.locator("#syncPreviewSyncButton")).to_be_disabled()


def test_large_preview_caps_rendered_thumbnails_until_show_more(live_server, page):
    """A large loaded result set does not mount every thumbnail at once."""
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """() => {
            _syncPreviewLoading = false;
            _syncModalOpen = true;
            _syncPreviewDisplayLimit = 300;
            _syncActiveFilters = new Set(['rating']);
            _syncPreviewData = {
                total_changes: 301,
                total_photos: 301,
                change_type_counts: {rating: 301},
                location_sync_enabled: false,
                photos: Array.from({length: 301}, (_, index) => ({
                    photo_id: 100000 + index,
                    filename: 'photo-' + index + '.jpg',
                    folder: '/photos',
                    changes: [{
                        id: 200000 + index,
                        type: 'rating',
                        value: '5',
                        presentation: {
                            field: 'XMP rating',
                            action: 'updated',
                            before: '3 stars',
                            after: '5 stars',
                        },
                    }],
                })),
            };
            document.getElementById('syncPreviewOverlay').style.display = 'block';
            renderSyncPreview();
        }"""
    )

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay.locator(".sync-preview-thumb")).to_have_count(300)
    expect(overlay.locator(".sync-preview-thumb").first).to_have_attribute(
        "loading", "lazy"
    )
    overlay.get_by_role("button", name="Show 1 more").click()
    expect(overlay.locator(".sync-preview-thumb")).to_have_count(301)


def test_location_changes_are_grouped_with_plain_language_delta(
    live_server, page, tmp_path,
):
    """Identical location writes render once with thumbnails, not raw tokens."""
    import config as cfg

    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:2]
    _make_photo_folders_accessible(live_server, tmp_path, photo_ids)
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    florida_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('Florida', 'location')"
    ).lastrowid
    tallahassee_id = db.conn.execute(
        "INSERT INTO keywords "
        "(name, parent_id, type, latitude, longitude) "
        "VALUES ('Tallahassee', ?, 'location', 30.4383, -84.2807)",
        (florida_id,),
    ).lastrowid
    db.conn.commit()
    for photo_id in photo_ids:
        db.set_photo_location(photo_id, tallahassee_id)
        db.queue_change(photo_id, "location", "effective")

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("openSyncPreview()")

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay).to_be_visible()
    expect(overlay.locator(".sync-review-group")).to_have_count(1)
    expect(overlay.locator(".sync-review-group-title")).to_have_text(
        "Location updated on 2 photos"
    )
    expect(overlay.locator(".sync-review-delta")).to_contain_text(
        "No XMP sidecar"
    )
    expect(overlay.locator(".sync-review-delta")).to_contain_text(
        "Tallahassee, Florida"
    )
    expect(overlay.locator(".sync-review-note")).to_contain_text(
        "written to XMP as GPS metadata"
    )
    expect(overlay.locator(".sync-preview-thumb")).to_have_count(2)
    expect(overlay).not_to_contain_text("effective")

    # The Dashboard's compact pending-change detail consumes the same API;
    # keep the internal token out of that secondary review surface too.
    page.goto(f"{live_server['url']}/dashboard")
    pending_card = page.locator("#pendingCard")
    expect(pending_card).to_be_visible()
    pending_card.click()
    pending_detail = page.locator("#pendingDetail")
    expect(pending_detail).to_contain_text(
        "Location: No XMP sidecar → Tallahassee, Florida"
    )
    expect(pending_detail).not_to_contain_text("effective")


def test_location_without_gps_is_shown_as_added(live_server, page, tmp_path):
    """A coordinate-less assignment must not look like an unchanged location."""
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    _make_photo_folders_accessible(live_server, tmp_path, [photo_id])

    reserve_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'location')",
        ("Pu'u Wa'awa'a Forest Reserve",),
    ).lastrowid
    db.conn.commit()
    db.set_photo_location(photo_id, reserve_id)
    db.queue_change(photo_id, "location", "effective")

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("openSyncPreview()")

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay.locator(".sync-review-group-title")).to_have_text(
        "Location added to 1 photo"
    )
    expect(overlay.locator(".sync-review-delta")).to_contain_text(
        "Pu'u Wa'awa'a Forest Reserve"
    )
    expect(overlay.locator(".sync-review-detail")).to_contain_text(
        "is assigned in Vireo"
    )
    expect(overlay.locator(".sync-review-detail")).to_contain_text(
        "has no GPS coordinates to write to XMP"
    )


def test_rating_preview_responds_to_selected_sidecar_creator(
    live_server, page, tmp_path,
):
    """Filtering out the change that creates XMP updates the rating promise."""
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    _make_photo_folders_accessible(live_server, tmp_path, [photo_id])
    db.queue_change(photo_id, "rating", "5")
    db.queue_change(photo_id, "keyword_add", "Raptor")

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("openSyncPreview()")

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay).to_be_visible()
    rating_group = overlay.locator(".sync-review-group").filter(
        has_text="Rating updated"
    )
    expect(rating_group.locator(".sync-review-before")).to_have_text(
        "No XMP sidecar"
    )
    expect(rating_group.locator(".sync-review-after")).to_have_text("5 stars")
    expect(rating_group).to_contain_text(
        "Another selected change creates the XMP sidecar first"
    )

    overlay.get_by_text("Keyword additions").locator("input").uncheck()

    rating_group = overlay.locator(".sync-review-group").filter(
        has_text="XMP rating unchanged"
    )
    expect(rating_group.locator(".sync-review-before")).to_have_text(
        "No XMP sidecar"
    )
    expect(rating_group.locator(".sync-review-after")).to_have_text(
        "No XMP sidecar"
    )
    expect(rating_group).to_contain_text("5 stars stays in Vireo")


def test_rating_preview_accounts_for_auto_included_keyword_pair(
    live_server, page, tmp_path,
):
    """A selected rename removal auto-includes its hidden sidecar-creating add."""
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    _make_photo_folders_accessible(live_server, tmp_path, [photo_id])
    db.queue_change(photo_id, "rating", "5")
    db.queue_change(photo_id, "keyword_add", "Raptor")
    db.queue_change(photo_id, "keyword_remove", "Raptor")

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("openSyncPreview()")

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay).to_be_visible()
    overlay.get_by_text("Keyword additions").locator("input").uncheck()

    rating_group = overlay.locator(".sync-review-group").filter(
        has_text="Rating updated"
    )
    expect(rating_group.locator(".sync-review-after")).to_have_text("5 stars")
    expect(rating_group).to_contain_text(
        "Another selected change creates the XMP sidecar first"
    )

    overlay.get_by_text("Keyword removals").locator("input").uncheck()

    rating_group = overlay.locator(".sync-review-group").filter(
        has_text="XMP rating unchanged"
    )
    expect(rating_group).to_contain_text("5 stars stays in Vireo")


def test_open_review_during_sync_shows_progress_then_loads_remaining(live_server, page):
    """Reloaded pages discover active syncs without loading a changing preview."""
    state = {"active": True, "current": 20, "pending": 80}
    previews = []

    def status(route):
        route.fulfill(json={
            "pending_count": state["pending"], "pending_photo_count": state["pending"],
            "change_type_counts": {"rating": state["pending"]},
            "active_job": {
                "id": "sync-test", "status": "running",
                "progress": {"current": state["current"], "total": 100,
                             "synced": state["current"] - 1, "failed": 1, "checkpoint": 2},
            } if state["active"] else None,
        })

    def preview(route):
        previews.append(route.request.url)
        route.continue_()

    page.route("**/api/sync/status", status)
    page.route("**/api/sync/preview?**", preview)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("void openSyncPreview()")
    expect(page.locator('#syncPreviewProgressText')).to_have_text(
        'Processed 20 of 100 photos · 19 synced · 1 with errors'
    )
    expect(page.locator('#syncPreviewSyncButton')).to_be_disabled()
    expect(page.locator('#syncPreviewDiscardAllButton')).to_be_disabled()
    assert previews == []

    state.update(current=40, pending=60)
    expect(page.locator('#syncPreviewProgressText')).to_contain_text('Processed 40 of 100')
    expect(page.locator('#syncBannerText')).to_contain_text('60 changes across 60 photos')
    assert previews == []

    state.update(active=False, pending=0)
    expect(page.locator('#syncPreviewProgressText')).to_have_text('No pending changes')
    assert previews
    assert page.evaluate('window._syncReviewWaiting') is False
