"""Recover one failed initial connection to the browser suite's local server."""

import warnings
from urllib.parse import urlsplit

from playwright.sync_api import Error


def _is_test_server_url(url):
    try:
        parsed = urlsplit(url)
        return parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and bool(parsed.port)
    except ValueError:
        return False


def install_initial_navigation_retry(page):
    """Retry only an initial local goto rejected with ERR_ADDRESS_INVALID.

    Chromium can report this before the first page opens, even when the
    server logs a 200 response. On POSIX it maps EADDRNOTAVAIL to this code;
    the traceback alone does not identify why the address was unavailable.
    Keep this mitigation visible and bounded: one warning and one retry,
    with later navigations, reloads and other failures left untouched.

    Install before module exposure so its callbacks run only after success.
    """
    goto = page.goto

    def goto_with_retry(url, **kwargs):
        initial = page.url == "about:blank"
        try:
            return goto(url, **kwargs)
        except Error as exc:
            if (
                not initial
                or not _is_test_server_url(url)
                or str(exc).partition("\n")[0] != f"Page.goto: net::ERR_ADDRESS_INVALID at {url}"
            ):
                raise
            warnings.warn(
                f"Initial E2E navigation to {url} failed with net::ERR_ADDRESS_INVALID; retrying once.",
                RuntimeWarning,
                stacklevel=2,
            )
            return goto(url, **kwargs)

    page.goto = goto_with_retry
    return page
