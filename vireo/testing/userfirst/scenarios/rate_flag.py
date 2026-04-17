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

    # Verify the *specific* photo we targeted shows the flag badge. A grid-wide
    # check would pass even if the endpoint failed, because browse_seed already
    # includes pre-flagged photos.
    target_flag = session.eval(
        f"""(() => {{
            const card = document.querySelector('.grid-card[data-id="{first_id}"]');
            if (!card) return 'card-missing';
            return card.querySelector('.grid-card-flag.flag-flagged') ? 'flagged' : 'not-flagged';
        }})()"""
    )
    session.assert_that(
        target_flag == "flagged",
        f"expected photo {first_id} to show flagged badge, got {target_flag!r}",
    )

    # Verify the *specific* photo's rating badge shows 4 stars. Same reasoning
    # as above: the seed includes pre-rated photos, so a grid-wide check is
    # insufficient.
    target_stars = session.eval(
        f"""(() => {{
            const card = document.querySelector('.grid-card[data-id="{first_id}"]');
            if (!card) return null;
            const el = card.querySelector('.grid-card-rating');
            return el ? el.textContent : '';
        }})()"""
    )
    star_count = target_stars.count("\u2605") if isinstance(target_stars, str) else -1
    session.assert_that(
        star_count == 4,
        f"expected photo {first_id} to show 4 stars, got {target_stars!r}",
    )
