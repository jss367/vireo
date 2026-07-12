"""E2E coverage for the Review Burst Group confirm/apply split.

The burst-group modal footer (in `pipeline_review.html`) carries two
checkboxes — "Confirm species" (#grmConfirmSpeciesChk) and "Apply
picks/rejects" (#grmApplyFlagsChk) — and a single "Apply and close" button
(#grmApplyBtn). These tests drive a real browser against a real Flask server
with a real on-disk pipeline cache, so the apply paths exercise the live
`/api/pipeline/group/apply` (flags-only) and `/api/encounters/species`
endpoints and the assertions read genuinely persisted state (the photos.flag
column, the photo_keywords table, and the pipeline cache file on disk).

We reuse the predictionless-cache helper shape from
`test_pipeline_review_species.py` but build a SINGLE multi-frame burst so we
can move/remove individual frames and confirm that species tagging covers
every frame (not just picks).

Why a new file rather than extending test_pipeline_review_species.py: that
file's helpers seed separate single-photo encounters for the species-widget
flow; these tests need one multi-frame burst opened through the burst-group
modal (grmApply), a distinct setup and surface area. They share the same
live_server/page fixtures and the same real-cache + real-DB assertion style.
"""
import json
import os
import re
import time

from playwright.sync_api import expect

# Regression guard for a fixed product bug: grmOnToggleChange() used to pin
# BOTH overrides (confirmSpeciesOverride AND applyFlagsOverride) to their
# current DOM state on EVERY checkbox toggle, instead of recording an override
# only for the box the user actually clicked. So the realistic species-only
# flow — uncheck "Apply picks/rejects" first, THEN type a new species — left
# "Confirm species" stuck unchecked even though a real, pending species change
# existed. The fix records an override only for the box that fired (per-box
# override intent), so the two tests below now exercise the intended flow.


def _write_single_burst_cache(
    live_server,
    photo_ids,
    *,
    confirmed_species=None,
    burst_override_species=None,
    burst_override_confirmed=True,
):
    """Write a pipeline cache with one encounter containing one multi-frame
    burst over `photo_ids`. Optionally pre-confirm the burst's species so we
    can test the "same species → no-op, stays unchecked" smart default.

    By default the per-burst `species_override` mirrors `confirmed_species`
    (encounter and burst agree). Pass `burst_override_species` to give the
    burst an override species that DIFFERS from the encounter
    `confirmed_species` — this models a burst confirmed/overridden as X while
    the encounter label is Y, the case the confirm/apply split must not clobber
    when applying flags only. `burst_override_confirmed` toggles whether that
    override is confirmed (defaults True).
    """
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    photos = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": "REVIEW",
            "quality_composite": 0.5,
            "flag": "none",
            "rating": 0,
            "confirmed_species": confirmed_species,
        }
        for row in rows
    ]
    ids = [p["id"] for p in photos]
    if burst_override_species is not None:
        override = {
            "species": burst_override_species,
            "confirmed": burst_override_confirmed,
        }
    elif confirmed_species:
        override = {"species": confirmed_species, "confirmed": True}
    else:
        override = None
    burst = {
        "photo_ids": ids,
        "species_predictions": [],
        "species_override": override,
    }
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": 1,
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": [confirmed_species] if confirmed_species else [],
                "species_predictions": [],
                "species_confirmed": bool(confirmed_species),
                "confirmed_species": confirmed_species,
                "bursts": [burst],
            }
        ],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": 0,
            "review_count": len(ids),
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    path = _cache_path(live_server)
    with open(path, "w") as f:
        json.dump(cache, f)


def _cache_path(live_server):
    db = live_server["db"]
    return os.path.join(
        os.path.dirname(db._db_path),
        f"pipeline_results_ws{db._active_workspace_id}.json",
    )


def _read_cache(live_server):
    with open(_cache_path(live_server)) as f:
        return json.load(f)


def _open_burst_modal(page, live_server):
    """Navigate to the pipeline review page and open the burst-group modal by
    clicking the first photo card (routes through openInspect → openGroupReview).
    Waits for the modal to seed so Apply is enabled.
    """
    # The burst modal is a full-screen overlay built for large pixel-peeping
    # displays; its footer (species field + sliders + the two commit
    # checkboxes + Apply button) is wider than the default 1280px test
    # viewport, which pushes Apply off-screen. Use a realistically wide
    # viewport so every footer control is clickable.
    page.set_viewport_size({"width": 1600, "height": 900})
    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator(".photo-card img").first.click()
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    # Seed completes when Apply leaves the "Loading…" state and re-enables.
    expect(page.locator("#grmApplyBtn")).to_be_enabled()


def _dispatch_contextmenu(locator):
    locator.evaluate(
        "el => el.dispatchEvent(new MouseEvent('contextmenu', "
        "{clientX: 100, clientY: 100, bubbles: true, cancelable: true}))"
    )


def _photo_flags(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, flag FROM photos WHERE id IN ({placeholders})", photo_ids
    ).fetchall()
    return {r["id"]: (r["flag"] or "none") for r in rows}


def _photos_with_species(live_server, photo_ids, species):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"""
        SELECT p.id
        FROM photos p
        JOIN photo_keywords pk ON pk.photo_id = p.id
        JOIN keywords k ON k.id = pk.keyword_id
        WHERE p.id IN ({placeholders}) AND k.name = ? AND k.is_species = 1
        """,
        (*photo_ids, species),
    ).fetchall()
    return {r["id"] for r in rows}


def _tag_all(live_server, photo_ids, species):
    """Tag every photo with `species` as a species keyword directly in the DB,
    so /group/state reports has_species_keyword=True for the whole burst."""
    db = live_server["db"]
    kid = db.add_keyword(species, is_species=True)
    for pid in photo_ids:
        db.tag_photo(pid, kid)
    return kid


def test_pipeline_burst_context_open_browse_falls_back_to_current_window(live_server, page):
    """Open in Browse Mode must still navigate when window.open is ignored."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)
    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    assert cards.count() >= 2
    target = cards.nth(1)
    target_pid = target.get_attribute("data-photo-id")
    assert target_pid

    page.evaluate("() => { window.open = () => null; }")
    _dispatch_contextmenu(target)

    selected = page.evaluate("String(grmState.selected)")
    assert selected == target_pid

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Open in Browse Mode").click()

    page.wait_for_function(
        "expectedPid => location.pathname === '/browse'"
        " && new URLSearchParams(location.search).get('photo_id') === expectedPid",
        arg=target_pid,
        timeout=5000,
    )


def test_pipeline_burst_loupe_context_open_browse_uses_selected_photo(live_server, page):
    """Right-clicking the Process Review loupe should target its selected photo."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)
    selected_pid = page.evaluate("String(grmState.selected)")
    assert selected_pid and selected_pid != "null"

    page.evaluate("() => { window.open = () => null; }")
    _dispatch_contextmenu(page.locator("#grmLoupePhoto"))

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Open in Browse Mode").click()

    page.wait_for_function(
        "expectedPid => location.pathname === '/browse'"
        " && new URLSearchParams(location.search).get('photo_id') === expectedPid",
        arg=selected_pid,
        timeout=5000,
    )


def test_smart_default_flags_checked_when_moves_pending(live_server, page):
    """Moving a frame to rejects pre-checks "Apply picks/rejects" via the smart
    default; "Confirm species" stays unchecked because the species field is
    empty (no pending species change)."""
    photo_ids = live_server["data"]["photos"][0:3]  # hawk1/2/3, one burst
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Reject the currently-selected frame.
    page.keyboard.press("x")
    expect(page.locator("#grmCount")).to_contain_text("1 rejects")

    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()


def test_apply_and_close_is_single_flight_while_request_is_pending(live_server, page):
    """Repeated Apply invocations while the first write is in flight must not
    post duplicate group/apply requests."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    apply_requests = []

    def delay_apply(route):
        apply_requests.append(route.request)
        time.sleep(0.2)
        route.continue_()

    page.route("**/api/pipeline/group/apply", delay_apply)
    _open_burst_modal(page, live_server)

    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.evaluate(
            """() => {
                grmApply();
                grmApply();
                grmApply();
            }"""
        )

    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))
    assert len(apply_requests) == 1


def test_smart_default_species_checked_on_new_species(live_server, page):
    """Typing a species that differs from the burst's confirmed species
    pre-checks "Confirm species"."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids, confirmed_species="Red-tailed Hawk")
    # Tag every frame with the confirmed species so there is no outstanding
    # keyword work — this isolates the "species text unchanged → unchecked"
    # smart default from the missing-keyword gate (covered separately).
    _tag_all(live_server, photo_ids, "Red-tailed Hawk")

    _open_burst_modal(page, live_server)

    # Field opens pre-filled with the confirmed species → no change → unchecked.
    expect(page.locator("#grmSpecies")).to_have_value("Red-tailed Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    page.locator("#grmSpecies").fill("Cooper's Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    # No flag moves → flags box stays unchecked.
    expect(page.locator("#grmApplyFlagsChk")).not_to_be_checked()


def test_unchecking_dirty_box_shows_amber_hint(live_server, page):
    """Unchecking a checked+dirty box reveals its amber .grm-dirty-hint with
    non-empty text."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    page.keyboard.press("x")  # one pending reject → flags box auto-checks
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmFlagsDirtyHint")).to_be_hidden()

    # Uncheck it (real click on the checkbox fires grmOnToggleChange).
    page.locator("#grmApplyFlagsChk").uncheck()

    hint = page.locator("#grmFlagsDirtyHint")
    expect(hint).to_be_visible()
    assert hint.inner_text().strip(), "amber hint text should be non-empty"
    expect(hint).to_contain_text("won't be saved")


def test_species_only_apply_leaves_flags_untouched_and_tags_all_frames(live_server, page):
    """Species-only apply (flags unchecked): no photo's flag changes, EVERY
    burst frame gets the species keyword, and the burst is marked confirmed.

    Intended flow: stage a flag move, uncheck "Apply picks/rejects", THEN type
    a new species so "Confirm species" auto-checks."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Stage a flag move so flags would be dirty — then uncheck flags to prove
    # the move is NOT persisted while species is.
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    page.locator("#grmApplyFlagsChk").uncheck()

    page.locator("#grmSpecies").fill("Northern Goshawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()

    with page.expect_response("**/api/encounters/species"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # No flag changed: the staged reject was discarded with the unchecked box.
    flags = _photo_flags(live_server, photo_ids)
    assert flags == {pid: "none" for pid in photo_ids}, flags

    # Every frame carries the species keyword — not just a pick.
    tagged = _photos_with_species(live_server, photo_ids, "Northern Goshawk")
    assert tagged == set(photo_ids), tagged

    # Burst is marked confirmed in the persisted cache.
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert burst.get("species_override", {}).get("confirmed") is True
    assert burst["species_override"]["species"] == "Northern Goshawk"


def test_smart_default_species_checked_when_frame_missing_keyword(live_server, page):
    """Burst already confirmed as the current species, but one frame still
    lacks that species keyword (e.g. legacy data that only tagged picks). The
    species field is unchanged, so speciesChanged is false — but outstanding
    tag work must still flip the "Confirm species" smart default ON, and Apply
    must POST /api/encounters/species so the missing keyword gets written.

    Regression for the classic burst-group modal analogue of the rapid-review
    missing-keyword gate: /api/pipeline/group/apply is flags-only, so without
    this gate the unwritten keyword would be silently dropped.
    """
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][0:3]
    species = "Red-tailed Hawk"

    # Confirm the burst as the species in the cache, but only tag TWO of the
    # three frames in the DB — the third is missing the keyword.
    _write_single_burst_cache(live_server, photo_ids, confirmed_species=species)
    kid = db.add_keyword(species, is_species=True)
    db.tag_photo(photo_ids[0], kid)
    db.tag_photo(photo_ids[1], kid)
    # photo_ids[2] intentionally left untagged.

    _open_burst_modal(page, live_server)

    # Species field reflects the confirmed species; do NOT change it.
    expect(page.locator("#grmSpecies")).to_have_value(species)
    # Smart default flips ON because a frame still needs the keyword, even
    # though the confirmed species itself is unchanged.
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()

    with page.expect_response("**/api/encounters/species"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # All three frames now carry the species keyword — the missing one got it.
    tagged = _photos_with_species(live_server, photo_ids, species)
    assert tagged == set(photo_ids), tagged


def test_flags_only_apply_leaves_species_unconfirmed(live_server, page):
    """Flags-only apply (species unchecked): flags persist, but no species
    keyword is added and the burst is not marked confirmed."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Reject the selected frame → flags auto-checks.
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()

    # Type a species but explicitly leave it unconfirmed.
    page.locator("#grmSpecies").fill("Sharp-shinned Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    page.locator("#grmConfirmSpeciesChk").uncheck()
    expect(page.locator("#grmSpeciesDirtyHint")).to_be_visible()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # Exactly one frame was rejected; the rest untouched.
    flags = _photo_flags(live_server, photo_ids)
    assert sorted(flags.values()) == ["none", "none", "rejected"], flags

    # No species keyword added anywhere.
    tagged = _photos_with_species(live_server, photo_ids, "Sharp-shinned Hawk")
    assert tagged == set(), tagged

    # Burst NOT marked confirmed.
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert not (burst.get("species_override") or {}).get("confirmed")


def test_flags_only_apply_preserves_differing_burst_override(live_server, page):
    """Regression: a burst CONFIRMED as override species X while the encounter
    label is a different species Y must NOT have X replaced by Y when the user
    applies pick/reject edits only.

    On open the species field must reflect the burst's actual override (X), not
    the encounter label (Y), so "Confirm species" stays UNchecked. Moving a
    frame to rejects and clicking Apply then runs the flags-only path and leaves
    the override species untouched — it does not silently post Y to
    /api/encounters/species (which would untag X, tag Y).
    """
    # Pick species that the conftest fixture does NOT pre-tag onto these frames
    # (it only tags photo[0]="Red-tailed Hawk", photo[3]="American Robin"), so a
    # post-apply keyword check cleanly reflects what THIS apply did.
    override_species = "Cooper's Hawk"      # X — what the burst is confirmed as
    encounter_species = "Northern Harrier"  # Y — the encounter label/prediction
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(
        live_server,
        photo_ids,
        confirmed_species=encounter_species,
        burst_override_species=override_species,
        burst_override_confirmed=True,
    )
    # Tag every frame with the override species X so there is no outstanding
    # keyword work — this keeps "Confirm species" OFF by the smart default,
    # isolating the override-preservation behavior from the missing-keyword gate.
    _tag_all(live_server, photo_ids, override_species)

    _open_burst_modal(page, live_server)

    # Field reflects the BURST override (X), not the encounter label (Y), and
    # "Confirm species" defaults OFF because field == already-confirmed species.
    expect(page.locator("#grmSpecies")).to_have_value(override_species)
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    # Reject the selected frame; flags auto-checks. Apply runs flags-only.
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # The reject persisted.
    flags = _photo_flags(live_server, photo_ids)
    assert sorted(flags.values()) == ["none", "none", "rejected"], flags

    # The burst's override species is STILL X (not replaced by Y).
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert burst["species_override"]["species"] == override_species, burst["species_override"]
    assert burst["species_override"]["confirmed"] is True

    # No frame was tagged with the encounter species Y by a stray species call.
    tagged_y = _photos_with_species(live_server, photo_ids, encounter_species)
    assert tagged_y == set(), tagged_y


def test_detached_burst_preserves_differing_confirmed_override(live_server, page):
    """Regression: detaching a frame from a burst that is CONFIRMED as override
    species X (while the encounter label is a different species Y) must carry X
    onto the new single-photo burst — not drop it to null (which silently
    reverts that frame to the encounter species Y on a flags-only apply).

    "Remove from group" one frame, leave "Apply picks/rejects" CHECKED (the
    flags-only detach path runs), Apply → the new single-photo burst in the
    saved cache must retain species_override = {species: X, confirmed: True},
    and the shortened source burst must still carry X too.
    """
    override_species = "Cooper's Hawk"      # X — what the burst is confirmed as
    encounter_species = "Northern Harrier"  # Y — the encounter label/prediction
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(
        live_server,
        photo_ids,
        confirmed_species=encounter_species,
        burst_override_species=override_species,
        burst_override_confirmed=True,
    )
    # Tag every frame with the override species X so the burst has no
    # outstanding keyword work — confirm stays OFF, isolating the detach-path
    # override-preservation from the missing-keyword gate.
    _tag_all(live_server, photo_ids, override_species)

    _open_burst_modal(page, live_server)

    # Field reflects the burst override (X); confirm defaults OFF (no change).
    expect(page.locator("#grmSpecies")).to_have_value(override_species)
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    # Remove the second card from the group; keep "Apply picks/rejects" CHECKED
    # so grmApply runs the flags-only detach path that splits it into its own
    # single-photo burst.
    second_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{second_pid}']").click()
    page.locator("#grmRemoveBtn").click()
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    cache = _read_cache(live_server)
    bursts = cache["encounters"][0]["bursts"]
    # The detach produced a second burst (the removed frame on its own).
    assert len(bursts) == 2, [b["photo_ids"] for b in bursts]
    detached = next(b for b in bursts if b["photo_ids"] == [second_pid])
    source = next(b for b in bursts if second_pid not in b["photo_ids"])

    # The detached single-photo burst KEEPS the burst-specific confirmed override
    # X — it is not null and not the encounter species Y.
    assert detached["species_override"] is not None, detached
    assert detached["species_override"]["species"] == override_species, detached["species_override"]
    assert detached["species_override"]["confirmed"] is True, detached["species_override"]

    # The shortened source burst still carries X for its remaining frames.
    assert source["species_override"]["species"] == override_species, source["species_override"]
    assert source["species_override"]["confirmed"] is True

    # No frame was tagged with the encounter species Y by a stray species call.
    tagged_y = _photos_with_species(live_server, photo_ids, encounter_species)
    assert tagged_y == set(), tagged_y


def test_species_only_with_removed_photo_keeps_member_and_tags_it(live_server, page):
    """Truthfulness guard: mark a photo "Remove from group", leave flags
    UNCHECKED, confirm species → the removal is discarded (photo stays a burst
    member) AND it still receives the species keyword.

    If grmApply detached the removed photo while flags were unchecked, the
    photo would no longer be a burst member and the burst-scoped
    /api/encounters/species call (validated against the on-disk burst) would
    either omit it or fail — so this asserts both the cache structure and the
    keyword landing on the removed frame.
    """
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Select the second card explicitly, then remove it from the group.
    second_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{second_pid}']").click()
    page.locator("#grmRemoveBtn").click()

    # Removal makes flags dirty (detachNew>0) so the box auto-checks; uncheck it
    # to discard the removal, mirroring a user who only wants to confirm species.
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    page.locator("#grmApplyFlagsChk").uncheck()
    expect(page.locator("#grmFlagsDirtyHint")).to_be_visible()

    page.locator("#grmSpecies").fill("Broad-winged Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()

    with page.expect_response("**/api/encounters/species"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # Removal discarded: the burst still holds all three frames.
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert sorted(burst["photo_ids"]) == sorted(photo_ids), burst["photo_ids"]
    assert len(cache["encounters"][0]["bursts"]) == 1, "no detach should have happened"

    # The "removed" photo still got tagged because it remained a member.
    tagged = _photos_with_species(live_server, photo_ids, "Broad-winged Hawk")
    assert tagged == set(photo_ids), tagged
    assert second_pid in tagged
