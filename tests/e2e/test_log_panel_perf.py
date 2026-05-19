"""Regression: the bottom-panel log stream must not do O(n) work per line.

Root cause of the "UI frozen while a long job runs" report: every live SSE
log line ran the full ``lpApplyFilter()`` re-scan (querySelectorAll over all
~200 line nodes + a style write on each) plus a forced auto-scroll reflow.
At the pipeline's log rate, sustained for hours, this saturates the renderer
main thread even though the Flask backend stays fast.

The live (non-bulk) path must coalesce rendering so a burst of N lines does
NOT trigger N full re-filters. ``addLpLine`` is exercised directly because
the SSE handler calls it verbatim: ``addLpLine(JSON.parse(e.data))``.
"""


def _drive_log_burst(page, live_server, n):
    """Load /browse, wrap lpApplyFilter to count, fire N live log lines."""
    page.goto(f"{live_server['url']}/browse")
    # Navbar IIFE defines window.addLpLine / window.lpApplyFilter at load.
    page.wait_for_function("typeof window.addLpLine === 'function'")
    page.wait_for_function("typeof window.lpApplyFilter === 'function'")

    page.evaluate(
        """(n) => {
            // Count full re-filter passes. addLpLine calls the bare
            // identifier `lpApplyFilter`, which resolves to this global,
            // so wrapping here intercepts the hot-path invocation.
            window.__filterCalls = 0;
            const orig = window.lpApplyFilter;
            window.lpApplyFilter = function () {
                window.__filterCalls++;
                return orig.apply(this, arguments);
            };
            const now = Date.now() / 1000;
            for (let i = 0; i < n; i++) {
                // Exactly what the SSE 'log' handler passes: single arg,
                // non-bulk -> the live render path under test.
                window.addLpLine({
                    time: now + i * 0.001,
                    level: 'INFO',
                    message: 'burst line ' + i,
                });
            }
        }""",
        n,
    )


def test_live_log_burst_does_not_refilter_per_line(live_server, page):
    """300 streamed lines must not cause 300 full lpApplyFilter re-scans."""
    n = 300
    _drive_log_burst(page, live_server, n)

    # Wait for the coalesced render to settle. Checking the cap alone is
    # racy: the navbar backfills startup logs via the bulk path on load,
    # so lpCount can already read '200' before the injected burst's rAF
    # flush runs. Also require the newest injected line to be present —
    # that can only be true after the burst flushed.
    page.wait_for_function(
        """() => {
            const countOk =
                document.getElementById('lpCount').textContent === '200';
            const els = document.querySelectorAll('#lpContent .lp-line');
            const last = els.length ? els[els.length - 1].textContent : '';
            return countOk && last.includes('burst line 299');
        }"""
    )

    filter_calls = page.evaluate("window.__filterCalls")
    line_count = page.evaluate(
        "document.querySelectorAll('#lpContent .lp-line').length"
    )
    count_text = page.evaluate(
        "document.getElementById('lpCount').textContent"
    )
    last_visible = page.evaluate(
        """() => {
            const els = document.querySelectorAll('#lpContent .lp-line');
            const last = els[els.length - 1];
            return last ? last.textContent : '';
        }"""
    )

    # Bound + correctness preserved by the fix.
    assert line_count == 200, f"DOM not capped at 200: {line_count}"
    assert count_text == "200", f"lpCount wrong: {count_text!r}"
    assert "burst line 299" in last_visible, (
        f"newest line not rendered: {last_visible!r}"
    )

    # The actual regression assertion: the expensive O(n) re-filter must be
    # coalesced, not run once per streamed line. A few rAF-batched flushes
    # are fine; one-per-line (== n) is the bug.
    assert filter_calls <= 5, (
        f"lpApplyFilter ran {filter_calls} times for {n} streamed lines "
        f"— per-line O(n) re-render (renderer-freeze bug) is present"
    )


def test_backgrounded_tab_burst_stays_bounded(live_server, page):
    """A long job logging into a hidden tab must not blow up.

    requestAnimationFrame is paused while a tab is hidden/minimized, but
    the EventSource keeps delivering, so addLpLine keeps enqueuing. The
    pending queue must stay bounded (capped on enqueue) and must schedule
    exactly one flush no matter how many lines arrive while paused — then
    resume correctly (DOM capped, newest kept, oldest dropped) when the
    tab becomes visible again.
    """
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("typeof window.addLpLine === 'function'")

    result = page.evaluate(
        """() => {
            // Simulate a hidden tab: rAF never auto-fires; capture the
            // single scheduled callback and count how many were scheduled.
            const cbs = [];
            window.__rafScheduled = 0;
            window.requestAnimationFrame = function (cb) {
                window.__rafScheduled++;
                cbs.push(cb);
                return cbs.length;
            };
            const now = Date.now() / 1000;
            // Far more than the 200 retention cap.
            for (let i = 0; i < 1000; i++) {
                window.addLpLine({
                    time: now + i * 0.001,
                    level: 'INFO',
                    message: 'bg line ' + i,
                });
            }
            const scheduledWhilePaused = window.__rafScheduled;
            // Tab visible again: drain the one captured flush.
            cbs.forEach((cb) => cb());
            const els = document.querySelectorAll('#lpContent .lp-line');
            return {
                scheduledWhilePaused,
                lineCount: els.length,
                countText: document.getElementById('lpCount').textContent,
                newest: els.length ? els[els.length - 1].textContent : '',
                oldest: els.length ? els[0].textContent : '',
            };
        }"""
    )

    # One flush scheduled for the whole 1000-line paused burst — not a
    # per-line rAF storm waiting to stampede on resume.
    assert result["scheduledWhilePaused"] == 1, (
        f"expected 1 scheduled flush for the paused burst, got "
        f"{result['scheduledWhilePaused']}"
    )
    # Resume is bounded and correct.
    assert result["lineCount"] == 200, (
        f"DOM not capped after resume: {result['lineCount']}"
    )
    assert result["countText"] == "200", f"lpCount wrong: {result['countText']!r}"
    assert "bg line 999" in result["newest"], (
        f"newest line missing after resume: {result['newest']!r}"
    )
    assert "bg line 0" not in result["oldest"], (
        f"oldest line should have been dropped: {result['oldest']!r}"
    )
