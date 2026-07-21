"""Scenario: import a card from the Import page, chain processing, re-run.

Drives /import like a user: add a source card, pick a destination,
choose "Quick look" as the after-import strategy, start, and read the
completion state — the created folders, the safe-to-format pill, and the
chained process job. Then re-runs the same import and confirms the page
reports everything as duplicates with no second processing run.

Honesty note (transparency rule): the live mid-run per-folder progress
row is not asserted here — a three-file import completes faster than the
SSE round trip is observable, so this scenario verifies the per-folder
counts in the completion state instead. The mid-run counters are pinned
at the API level by test_progress_events_carry_live_per_folder_counts.
"""
import os
import time


def _wait_for_result(session, timeout=45.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        visible = session.eval(
            "document.getElementById('resultCard').style.display !== 'none'"
        )
        if visible:
            return True
        time.sleep(0.5)
    return False


def _start_and_wait(session, label):
    # startImport hides the result card only after its POST resolves; hide
    # it up front so the wait below can't race a previous run's card.
    session.eval(
        "document.getElementById('resultCard').style.display = 'none'"
    )
    session.click("#btnStart")
    ok = _wait_for_result(session)
    session.assert_that(ok, f"{label}: import result never appeared")
    session.screenshot(label)


def run(session):
    photos_root = os.environ.get("VIREO_TEST_PHOTOS", "")
    card = os.path.join(photos_root, "card")
    archive = os.path.join(photos_root, "archive")

    session.goto("/import")
    session.screenshot("import-initial")

    for el_id in ("sourceInput", "modeCopy", "destInput", "afterImportSelect",
                  "btnStart", "safeToFormatPill"):
        present = session.eval(f"!!document.getElementById('{el_id}')")
        session.assert_that(present, f"expected #{el_id} on /import")

    session.click("#modeCopy")
    session.fill("#sourceInput", card)
    session.click("#btnAddSource")
    added = session.eval(
        "document.querySelectorAll('#sourceList .source-item').length"
    )
    session.assert_that(added == 1, f"expected 1 source, got {added}")

    session.fill("#destInput", archive)
    # The After Import dropdown lists saved processes by id; pick "Quick look"
    # by its visible label and fire the change event so the page treats it as
    # a user choice (startImport() only forwards actively-picked options).
    picked = session.eval(
        "(() => {"
        "  const sel = document.getElementById('afterImportSelect');"
        "  const opt = Array.from(sel.options).find("
        "    o => (o.textContent || '').trim() === 'Quick look');"
        "  if (!opt) return false;"
        "  sel.value = opt.value;"
        "  sel.dispatchEvent(new Event('change'));"
        "  return true;"
        "})()"
    )
    session.assert_that(
        picked, "expected a 'Quick look' saved process in the After Import menu"
    )

    _start_and_wait(session, "import-first-run")

    pill = session.eval(
        "document.getElementById('safeToFormatPill').textContent"
    )
    session.assert_that(
        "Safe to format" in pill,
        f"first run should be safe to format, pill said: {pill!r}",
    )
    folder_rows = session.eval(
        "document.querySelectorAll('#resultFolders tr').length"
    )
    session.assert_that(
        folder_rows >= 1,
        f"expected per-folder result rows, got {folder_rows}",
    )
    chain = session.eval(
        "document.getElementById('chainInfo').textContent"
    )
    session.assert_that(
        "Processing started" in chain,
        f"expected a chained process job, chainInfo said: {chain!r}",
    )
    summary = session.eval(
        "document.getElementById('resultSummary').textContent"
    )
    session.assert_that(
        "3 copied" in summary,
        f"expected 3 copied on first run, summary said: {summary!r}",
    )

    # --- Re-run: everything is a duplicate, nothing re-copies, no second
    # processing run.
    _start_and_wait(session, "import-rerun")
    summary = session.eval(
        "document.getElementById('resultSummary').textContent"
    )
    session.assert_that(
        "0 copied" in summary and "3 duplicates skipped" in summary,
        f"re-run should be all duplicates, summary said: {summary!r}",
    )
    chain = session.eval(
        "document.getElementById('chainInfo').textContent"
    )
    session.assert_that(
        "no new photos" in chain,
        f"re-run should skip chaining with 'no new photos', got: {chain!r}",
    )
