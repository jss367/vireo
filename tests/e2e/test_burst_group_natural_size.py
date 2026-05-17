"""Verify burst-loupe strip imgs are laid out at natural source-pixel size.

This guards the WKWebView quality regression: when a strip <img> is laid
out at 180x120 and then scaled up via `transform: scale(N)`, browsers
rasterize the layer at 180x120 first and bilinearly upscale. The fix is
to give each <img> inline width/height equal to its served source-pixel
dimensions and use transform-scale only to shrink it into the viewport.

These tests assert the layout invariant (img CSS width == naturalWidth)
rather than visual pixel quality, since the latter is hard to test
deterministically across browser engines.
"""
import os
import re

import pytest
from PIL import Image
from playwright.sync_api import expect


def _seed_burst_with_real_photos(db, thumb_dir, group_id="grp-natural-1",
                                  model="BioCLIP-2"):
    """Seed three Red-tailed Hawk predictions with on-disk photos that have
    real width/height metadata. Photos are 600x400 jpeg so /photos/<id>/original
    yields a small but real image whose naturalWidth/Height we can assert.
    """
    rows = db.conn.execute(
        """SELECT pr.id, d.photo_id, p.folder_id, p.filename
             FROM predictions pr
             JOIN detections d ON d.id = pr.detection_id
             JOIN photos p ON p.id = d.photo_id
            WHERE pr.species = 'Red-tailed Hawk'
              AND pr.classifier_model = ?""",
        (model,),
    ).fetchall()
    if not rows:
        return 0
    total = len(rows)
    ws_id = db._active_workspace_id
    for i, row in enumerate(rows):
        db.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status,
                  group_id, vote_count, total_votes)
               VALUES (?, ?, 'pending', ?, ?, ?)
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET group_id = excluded.group_id,
                             vote_count = excluded.vote_count,
                             total_votes = excluded.total_votes""",
            (row["id"], ws_id, group_id, total, total),
        )
        db.conn.execute(
            """UPDATE photos
                 SET quality_score = ?, subject_sharpness = ?,
                     width = ?, height = ?
               WHERE id = ?""",
            (0.5 + 0.1 * i, 100 + 10 * i, 600, 400, row["photo_id"]),
        )
    db.conn.commit()
    return total


def _ensure_photo_files_on_disk(db, tmp_path):
    """The /photos/<id>/original endpoint reads from disk, so we need real
    files at the photo paths. Write 600x400 JPEGs.
    """
    rows = db.conn.execute(
        """SELECT p.id, p.filename, f.path
             FROM photos p
             JOIN folders f ON f.id = p.folder_id
            WHERE p.filename LIKE 'hawk%'"""
    ).fetchall()
    base = str(tmp_path / "hawk_photos")
    os.makedirs(base, exist_ok=True)
    for row in rows:
        # The folder.path in the seed is /photos/park, but that doesn't exist
        # on the test machine. Patch it to a real tmp dir.
        new_dir = base
        new_path = os.path.join(new_dir, row["filename"])
        Image.new("RGB", (600, 400), color=(80, 120, 160)).save(new_path, "JPEG")
        db.conn.execute(
            "UPDATE folders SET path = ? WHERE id = (SELECT folder_id FROM photos WHERE id = ?)",
            (new_dir, row["id"]),
        )
    db.conn.commit()


def _open_burst_modal(page):
    trigger = page.locator("button[data-group-id]").first
    expect(trigger).to_be_visible()
    trigger.click()
    page.locator("#grmOverlay .grm-card[data-photo-id]").first.wait_for(
        state="visible", timeout=2000
    )


def test_burst_card_img_laid_out_at_natural_size(live_server, page, tmp_path):
    """After upgrading the strip to original (via wheel zoom), each <img>
    should have inline CSS width/height equal to the server's served image
    dimensions. This proves the rasterized layer is at full source size
    instead of being upscaled from a tiny tile.
    """
    db = live_server["db"]
    n = _seed_burst_with_real_photos(db, tmp_path)
    if n < 1:
        pytest.skip("could not seed burst group")
    _ensure_photo_files_on_disk(db, tmp_path)

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    # Trigger a wheel zoom on the loupe so the strip upgrades to /original.
    loupe = page.locator("#grmLoupeImg")
    loupe.dispatch_event("wheel", {"deltaY": -100, "clientX": 200, "clientY": 200})

    # Wait for at least one card img to load at the original.
    page.wait_for_function(
        """() => {
          const imgs = document.querySelectorAll('.grm-card img');
          return Array.from(imgs).some(img => img.complete && img.naturalWidth > 100);
        }""",
        timeout=4000,
    )

    # Assert: the card img's inline style.width matches its naturalWidth.
    result = page.evaluate(
        """() => {
          const imgs = document.querySelectorAll('.grm-card img');
          return Array.from(imgs).map(img => ({
            inlineW: img.style.width,
            inlineH: img.style.height,
            naturalW: img.naturalWidth,
            naturalH: img.naturalHeight,
            complete: img.complete,
          }));
        }"""
    )
    assert result, "no .grm-card imgs found"
    # At least one fully-loaded img should have inline dims matching naturalWidth.
    matched = [
        r for r in result
        if r["complete"] and r["naturalW"] > 0
        and r["inlineW"] == f"{r['naturalW']}px"
        and r["inlineH"] == f"{r['naturalH']}px"
    ]
    assert matched, (
        f"no card img has inline width matching naturalWidth — got {result}. "
        "This means the layer is being rasterized at the wrong size, which "
        "is the WKWebView upscaling bug this fix is meant to prevent."
    )


def test_burst_card_box_is_180x120(live_server, page, tmp_path):
    """The 180x120 viewport stays the visible card box; the natural-size
    <img> inside is clipped via overflow:hidden on .grm-card-img-box.
    """
    db = live_server["db"]
    n = _seed_burst_with_real_photos(db, tmp_path)
    if n < 1:
        pytest.skip("could not seed burst group")
    _ensure_photo_files_on_disk(db, tmp_path)

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    # The image-box wrapper should be exactly 180x120.
    box = page.locator(".grm-card .grm-card-img-box").first
    expect(box).to_be_visible()
    bbox = box.bounding_box()
    assert bbox is not None
    assert abs(bbox["width"] - 180) < 1, f"box width {bbox['width']} != 180"
    assert abs(bbox["height"] - 120) < 1, f"box height {bbox['height']} != 120"


def test_burst_modal_thumb_slider_resizes_wrapped_grid(live_server, page, tmp_path):
    """The burst modal should expose the same kind of thumbnail-size control as
    other review grids, and the strips should wrap instead of requiring
    horizontal scrolling for large bursts.
    """
    db = live_server["db"]
    n = _seed_burst_with_real_photos(db, tmp_path)
    if n < 1:
        pytest.skip("could not seed burst group")
    _ensure_photo_files_on_disk(db, tmp_path)

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    assert page.locator("#grmThumbSizeSlider").is_visible()
    flex_wrap = page.locator("#grmCandidates").evaluate(
        "el => getComputedStyle(el).flexWrap"
    )
    assert flex_wrap == "wrap"

    page.locator("#grmThumbSizeSlider").evaluate(
        """el => {
          el.value = 220;
          el.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )

    box = page.locator(".grm-card .grm-card-img-box").first
    bbox = box.bounding_box()
    assert bbox is not None
    assert abs(bbox["width"] - 220) < 1, f"box width {bbox['width']} != 220"
    assert abs(bbox["height"] - 147) < 1, f"box height {bbox['height']} != 147"


def test_burst_modal_resolution_and_right_zoom_controls(live_server, page, tmp_path):
    """The burst modal exposes image resolution and right-side loupe zoom controls."""
    db = live_server["db"]
    n = _seed_burst_with_real_photos(db, tmp_path)
    if n < 1:
        pytest.skip("could not seed burst group")
    _ensure_photo_files_on_disk(db, tmp_path)

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    expect(page.locator("#grmResSlider")).to_be_visible()
    expect(page.locator("#grmLoupeZoomSlider")).to_be_visible()

    page.locator("#grmResSlider").evaluate(
        """el => {
          el.value = 3;
          el.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )
    expect(page.locator("#grmResLabel")).to_contain_text("original")

    page.locator("#grmLoupeZoomSlider").evaluate(
        """el => {
          el.value = 200;
          el.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )
    transform = page.locator("#grmLoupePhoto").evaluate("el => el.style.transform")
    match = re.search(r"scale\(([-0-9.]+)\)", transform)
    assert match, f"scale transform missing from {transform!r}"
    assert abs(float(match.group(1)) - 2.0) < 0.01


def test_burst_modal_scores_visible_box_sharpness(live_server, page, tmp_path):
    """Box sharpness scoring should render results without changing selection."""
    db = live_server["db"]
    n = _seed_burst_with_real_photos(db, tmp_path)
    if n < 1:
        pytest.skip("could not seed burst group")
    _ensure_photo_files_on_disk(db, tmp_path)

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    if cards.count() >= 2:
        first_pid = cards.nth(0).get_attribute("data-photo-id")
        second_pid = cards.nth(1).get_attribute("data-photo-id")
        page.evaluate(
            """([first, second]) => {
              const a = parseInt(first, 10);
              const b = parseInt(second, 10);
              grmState.selected = b;
              grmState.selectedIds = new Set([a, b]);
              grmState.selectionAnchor = a;
              renderGroupModal();
            }""",
            [first_pid, second_pid],
        )
        before = page.evaluate("Array.from(grmState.selectedIds).map(String).sort()")
        assert before == sorted([first_pid, second_pid])

    expect(page.locator("#grmBoxSharpnessBtn")).to_be_visible()
    page.locator("#grmBoxSharpnessBtn").click()
    page.locator(".grm-card-scores", has_text="Box:").first.wait_for(
        state="visible", timeout=5000
    )
    expect(page.locator(".grm-card-scores", has_text="Box:")).to_have_count(n)

    if cards.count() >= 2:
        after = page.evaluate("Array.from(grmState.selectedIds).map(String).sort()")
        assert after == before


def test_region_sharpness_endpoint_accepts_large_batches(live_server, page, tmp_path):
    """The region sharpness API accepts expected batch sizes and rejects bad payloads."""
    db = live_server["db"]
    n = _seed_burst_with_real_photos(db, tmp_path)
    if n < 1:
        pytest.skip("could not seed burst group")
    _ensure_photo_files_on_disk(db, tmp_path)

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    pid = int(page.locator("#grmOverlay .grm-card[data-photo-id]").first.get_attribute("data-photo-id"))
    regions = [{
        "photo_id": pid,
        "x": 0,
        "y": 0,
        "w": 120,
        "h": 80,
        "source_w": 600,
        "source_h": 400,
    } for _ in range(101)]
    response = page.request.post(
        f"{url}/api/photos/sharpness/regions",
        data={"regions": regions},
    )
    assert response.status == 200
    body = response.json()
    assert len(body["results"]) == 101

    non_object_response = page.request.post(
        f"{url}/api/photos/sharpness/regions",
        data=[],
    )
    assert non_object_response.status == 400

    oversized_response = page.request.post(
        f"{url}/api/photos/sharpness/regions",
        data={"regions": regions * 2},
    )
    assert oversized_response.status == 400
