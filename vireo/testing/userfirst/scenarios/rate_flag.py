"""Scenario: rate and flag photos via the API, verify the browse grid reflects changes.

Uses fetch() calls through page.evaluate to hit the rating and flag API
endpoints, then reloads the browse page to confirm the UI reflects the
persisted state.
"""


def run(session):
    session.goto("/browse")
    session.screenshot("before-rating")

    # Grab the first photo's data-id from the grid
    first_id = session.eval(
        """(() => {
            const card = document.querySelector('.grid-card[data-id]');
            return card ? parseInt(card.getAttribute('data-id'), 10) : null;
        })()"""
    )
    session.assert_that(first_id is not None, "expected at least one photo card")
    if first_id is None:
        return  # nothing to test

    # Set rating to 4 via API — use XMLHttpRequest synchronously to avoid
    # race conditions with Playwright's network listeners on navigation.
    rate_status = session.eval(
        f"""(() => {{
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/photos/{first_id}/rating', false);
            xhr.setRequestHeader('Content-Type', 'application/json');
            xhr.send(JSON.stringify({{rating: 4}}));
            return xhr.status;
        }})()"""
    )
    session.assert_that(rate_status == 200, f"expected rating API 200, got {rate_status}")

    # Flag the photo via API (synchronous XHR)
    flag_status = session.eval(
        f"""(() => {{
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/photos/{first_id}/flag', false);
            xhr.setRequestHeader('Content-Type', 'application/json');
            xhr.send(JSON.stringify({{flag: 'flagged'}}));
            return xhr.status;
        }})()"""
    )
    session.assert_that(flag_status == 200, f"expected flag API 200, got {flag_status}")

    # Reload browse to see the updated state
    session.goto("/browse")
    session.screenshot("after-rating-and-flag")

    # Verify the flagged photo shows the flag badge somewhere in the grid
    flag_badges = session.eval(
        "document.querySelectorAll('.grid-card-flag.flag-flagged').length"
    )
    session.assert_that(flag_badges > 0, "expected at least one flagged badge after flagging")

    # Verify ratings are displayed (at least one card should have stars)
    star_spans = session.eval(
        "document.querySelectorAll('.grid-card-rating').length"
    )
    session.assert_that(star_spans > 0, "expected rating stars after setting rating")
