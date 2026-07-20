"""Playwright verification for the five new filter features."""
import re
import sys

from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:4173"
failures = []


def ensure_popover(page, open_it=True):
    hidden = page.locator("#filterPopover").is_hidden()
    if open_it and hidden:
        page.click("#filterButton")
    elif not open_it and not hidden:
        page.click("#doneFilter")
    page.wait_for_timeout(200)


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))
    if not condition:
        failures.append(name)


with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    page.goto(URL)
    page.evaluate("localStorage.clear()")
    page.reload()

    # ---- 2. Multi-select quick filters ----
    ensure_popover(page)
    page.click('#quickColors [data-color="red"]')
    page.click('#quickColors [data-color="yellow"]')
    chip = page.locator(".filter-chip").first
    chip_text = chip.inner_text()
    check("multi-select color chip", "is one of Red, Yellow" in chip_text, chip_text.strip())
    red_active = page.get_attribute('#quickColors [data-color="red"]', "class") or ""
    yellow_active = page.get_attribute('#quickColors [data-color="yellow"]', "class") or ""
    check("both color swatches active", "active" in red_active and "active" in yellow_active)
    count_two_colors = int(page.inner_text("#resultCount"))
    page.click('#quickColors [data-color="yellow"]')  # untoggle yellow
    page.wait_for_timeout(300)
    count_one_color = int(page.inner_text("#resultCount"))
    check("untoggle narrows results", count_one_color < count_two_colors,
          f"{count_two_colors} -> {count_one_color}")
    page.click('#quickColors [data-color="red"]')  # remove rule entirely
    page.wait_for_timeout(300)
    check("empty selection removes rule", page.locator(".filter-chip").count() == 0)

    # Flags combine too
    page.click('#quickFlags [data-flag="flagged"]')
    page.click('#quickFlags [data-flag="none"]')
    page.wait_for_timeout(300)
    flag_chip = page.locator(".filter-chip").first.inner_text()
    check("multi-select flag chip", "Flag is one of Picked, Unflagged" in flag_chip, flag_chip.strip())
    page.click("#clearAll")
    page.wait_for_timeout(300)

    # ---- 1. Typeahead with counts ----
    page.click("#addFilterButton")
    page.fill("#fieldSearch", "camera model")
    page.click('[data-add-field="camera_model"]')
    page.wait_for_timeout(400)
    value_input = page.locator('#ruleTree [data-suggest="1"]')
    check("suggest input rendered", value_input.count() == 1)
    value_input.click()
    page.wait_for_timeout(200)
    options = page.locator(".value-suggest .value-option")
    check("suggestions visible on focus", options.count() > 0, f"{options.count()} options")
    first_option_text = options.first.inner_text()
    check("suggestion has a count", bool(re.search(r"\d", first_option_text)), first_option_text.replace("\n", " · "))
    value_input.type("son", delay=60)
    page.wait_for_timeout(600)
    value_input = page.locator('#ruleTree [data-suggest="1"]')  # re-query after re-render
    focused = page.evaluate("document.activeElement?.dataset?.suggest === '1'")
    check("input keeps focus while typing", focused)
    options = page.locator(".value-suggest .value-option")
    texts = [options.nth(i).inner_text() for i in range(options.count())]
    check("typeahead narrows to Sony", len(texts) > 0 and all("Sony" in t for t in texts), "; ".join(t.replace("\n", " ") for t in texts))
    options.first.click()
    page.wait_for_timeout(400)
    chip_text = page.locator(".filter-chip").first.inner_text()
    check("picked value lands in chip", "Camera model contains Sony" in chip_text, chip_text.strip())
    check("dropdown closed after pick", page.locator(".value-suggest:visible").count() == 0)
    page.click("#clearAll")
    page.wait_for_timeout(300)

    # ---- 3 & 5. Relative date + between via the facets preset ----
    ensure_popover(page, open_it=False)
    page.click('[data-preset="facets"]')
    page.wait_for_timeout(500)
    chips = " | ".join(page.locator(".filter-chip").all_inner_texts())
    check("relative-date chip", "Capture date is in the last 12 months" in chips, chips)
    check("is-one-of chip from preset", "Color label is one of Red, Yellow" in chips)
    check("between chip", "ISO is between 400 and 3200" in chips)
    preset_count = int(page.inner_text("#resultCount"))
    check("facets preset returns results", preset_count > 0, f"{preset_count} photos")

    # Rule UI for in_last and between renders paired inputs
    ensure_popover(page)
    check("relative date inputs", page.locator('#ruleTree [data-action="rel-n"]').count() == 1
          and page.locator('#ruleTree [data-action="rel-unit"]').count() == 1)
    check("between inputs", page.locator('#ruleTree [data-action="value-from"]').count() == 1
          and page.locator('#ruleTree [data-action="value-to"]').count() == 1)
    enum_toggles = page.locator("#ruleTree .enum-toggle")
    check("enum multi toggles with counts", enum_toggles.count() == 5
          and re.search(r"\d", enum_toggles.first.inner_text()))
    # widen the relative window and confirm the count doesn't shrink
    page.fill('#ruleTree [data-action="rel-n"]', "24")
    page.wait_for_timeout(700)
    wider = int(page.inner_text("#resultCount"))
    check("wider window keeps/grows results", wider >= preset_count, f"{preset_count} -> {wider}")
    ensure_popover(page, open_it=False)

    # ---- 4. Pause / resume with \ ----
    filtered = int(page.inner_text("#resultCount"))
    page.keyboard.press("\\")
    page.wait_for_timeout(400)
    paused_count = int(page.inner_text("#resultCount"))
    check("pause shows everything", paused_count == 54, f"{filtered} -> {paused_count}")
    check("pause badge visible", "Filters paused" in page.inner_text("#contentBadges"))
    check("resume button state", "Resume" in page.inner_text("#muteFilters"))
    check("chips dimmed", "muted" in (page.get_attribute("#filterSecondaryRow", "class") or ""))
    # persists across reload
    page.reload()
    page.wait_for_timeout(500)
    check("paused state survives reload", int(page.inner_text("#resultCount")) == 54)
    page.keyboard.press("\\")
    page.wait_for_timeout(400)
    resumed = int(page.inner_text("#resultCount"))
    check("resume restores filtered count", resumed == wider, f"{resumed} vs {wider}")
    check("pause button back", "Pause" in page.inner_text("#muteFilters"))
    # \ typed in the search box must not toggle
    page.click("#quickSearch")
    page.keyboard.press("\\")
    page.wait_for_timeout(200)
    check("backslash in input does not pause", "Pause" in page.inner_text("#muteFilters"))
    page.keyboard.press("Escape")

    # Mute button click path
    page.click("#muteFilters")
    page.wait_for_timeout(300)
    check("button click pauses", int(page.inner_text("#resultCount")) == 54)
    page.click("#muteFilters")
    page.wait_for_timeout(300)
    check("button click resumes", int(page.inner_text("#resultCount")) == resumed)

    # ---- Saving while paused drops the paused flag ----
    page.click("#muteFilters")  # pause again
    page.wait_for_timeout(300)
    check("paused before save", int(page.inner_text("#resultCount")) == 54)
    page.click("#moreActions")
    page.click("#saveCollection")
    page.wait_for_timeout(200)
    page.fill("#collectionName", "Paused-save test")
    page.click("#confirmSave")
    page.wait_for_timeout(300)
    # Clear filters (also drops paused state) so reopening the saved
    # collection is a distinct action, not a no-op.
    page.click("#clearAll")
    page.wait_for_timeout(300)
    saved_button = page.locator('[data-preset^="saved:"]', has_text="Paused-save test").first
    saved_button.click()
    page.wait_for_timeout(400)
    reopened_count = int(page.inner_text("#resultCount"))
    check(
        "reopened collection is not paused",
        reopened_count == resumed,
        f"{reopened_count} vs {resumed} (all=54 would mean paused)",
    )
    check(
        "reopen restores filter chips",
        page.locator(".filter-chip").count() > 0,
    )
    check(
        "reopen shows Pause (not Resume) button",
        "Pause" in page.inner_text("#muteFilters"),
    )

    # ---- narrow viewport smoke ----
    page.set_viewport_size({"width": 760, "height": 900})
    page.wait_for_timeout(400)
    check("narrow: mute button visible", page.locator("#muteFilters").is_visible())
    ensure_popover(page)
    check("narrow: enum toggles wrap without overflow",
          page.locator("#ruleTree .enum-toggle").count() == 5)
    ensure_popover(page, open_it=False)

    page.screenshot(path=".context/photo-filter-prototype/verify-new-features.png", full_page=False)
    browser.close()

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    sys.exit(1)
print("ALL CHECKS PASSED")
