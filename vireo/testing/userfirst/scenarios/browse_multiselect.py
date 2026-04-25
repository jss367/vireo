"""Regression scenario for #601: browse keyboard shortcuts act on the full multi-selection.

Before the fix, the flag (P/X/U), rating (0-5), and color-label (R/Y/G/B)
shortcuts on /browse only acted on ``selectedPhotoId`` (the single photo
in the detail pane). Multi-select via Cmd/Ctrl-click was silently ignored.

This scenario normal-clicks one card, Ctrl-clicks two more, presses the
reject shortcut (``x``), reloads, and verifies all three photos show the
rejected badge — not just the last-clicked one.
"""


def run(session):
    session.goto("/browse")
    session.page.wait_for_selector(".grid-card[data-id]", state="visible", timeout=5000)
    # Keyboard handler bails out when _shortcuts hasn't been fetched yet.
    session.page.wait_for_function("() => window._shortcuts !== null", timeout=5000)

    ids = session.eval(
        """(() => Array.from(document.querySelectorAll('.grid-card[data-id]'))
            .slice(0, 3).map(c => parseInt(c.dataset.id, 10)))()"""
    )
    session.assert_that(
        len(ids) == 3,
        f"need at least 3 grid cards for multi-select; got {len(ids)}",
    )
    if len(ids) < 3:
        return

    # Normal-click the first card, Cmd-click the next two. On the first
    # Cmd-click the handler folds the focused photo into selectedPhotos, so
    # the Set ends up containing all three ids.
    # Use Meta (Cmd) rather than Control: on macOS Ctrl+click is intercepted
    # by the OS as a right-click and opens the context menu instead of
    # dispatching a multi-select click. The JS handler accepts metaKey OR
    # ctrlKey, so Meta works on all platforms.
    session.page.click(f'.grid-card[data-id="{ids[0]}"]')
    session.page.click(
        f'.grid-card[data-id="{ids[1]}"]', modifiers=["Meta"]
    )
    session.page.click(
        f'.grid-card[data-id="{ids[2]}"]', modifiers=["Meta"]
    )
    session.screenshot("after-multiselect")

    sel_size = session.eval("selectedPhotos.size")
    session.assert_that(
        sel_size == 3,
        f"selectedPhotos should contain all three clicked photos; got size={sel_size}",
    )

    # Dispatch the reject shortcut as a synthetic keydown on document so it
    # bypasses whatever element currently holds focus but still trips the
    # listener (which is bound on document and ignores INPUT/TEXTAREA).
    with session.page.expect_response(
        lambda r: "/api/batch/flag" in r.url and r.request.method == "POST",
        timeout=5000,
    ) as resp_info:
        session.page.evaluate(
            """() => {
                const evt = new KeyboardEvent('keydown', {
                    key: 'x', code: 'KeyX', bubbles: true, cancelable: true,
                });
                document.dispatchEvent(evt);
            }"""
        )

    resp = resp_info.value
    session.assert_that(
        resp.status == 200,
        f"batch flag request should return 200; got {resp.status}",
    )
    session.screenshot("after-reject-shortcut")

    session.goto("/browse")
    session.page.wait_for_selector(".grid-card[data-id]", state="visible", timeout=5000)

    flagged = session.eval(
        f"""(() => {{
            const wanted = {ids};
            return wanted.map(id => {{
                const card = document.querySelector(`.grid-card[data-id="${{id}}"]`);
                if (!card) return [id, 'card-missing'];
                return [id, card.querySelector('.grid-card-flag.flag-rejected') ? 'rejected' : 'not-rejected'];
            }});
        }})()"""
    )
    for pid, state in flagged:
        session.assert_that(
            state == "rejected",
            f"photo {pid} should show rejected badge after batch shortcut; got {state!r}",
        )
