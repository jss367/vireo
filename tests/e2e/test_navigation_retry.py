"""Keep the initial-navigation workaround narrow and module exposure intact."""

import warnings
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from playwright.sync_api import Error, TimeoutError

from e2e.module_pages import install_page_module_exposure
from e2e.navbar_modules import install_navbar_exposure
from e2e.navigation import install_initial_navigation_retry

URL = "http://127.0.0.1:57718/browse"


def _address_error(url=URL):
    return Error(f'Page.goto: net::ERR_ADDRESS_INVALID at {url}\nCall log:\n  - navigating to "{url}"')


def _page(*results, current_url="about:blank"):
    goto = Mock(side_effect=results)
    page = SimpleNamespace(url=current_url, goto=goto, reload=Mock())
    return install_initial_navigation_retry(page), goto


@pytest.mark.parametrize("keyword_url", [False, True])
def test_initial_address_error_retries_once_with_original_options(keyword_url):
    response = object()
    page, goto = _page(_address_error(), response)
    options = {"wait_until": "domcontentloaded", "timeout": 1234, "referer": URL}
    with pytest.warns(RuntimeWarning, match="ERR_ADDRESS_INVALID; retrying once") as recorded:
        result = page.goto(url=URL, **options) if keyword_url else page.goto(URL, **options)
    assert result is response
    assert len(recorded) == 1
    assert goto.call_count == 2
    assert all(call.args == (URL,) and call.kwargs == options for call in goto.call_args_list)


def test_success_does_not_retry_or_warn():
    response = object()
    page, goto = _page(response)
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        assert page.goto(URL) is response
    assert not recorded
    goto.assert_called_once_with(URL)


@pytest.mark.parametrize("failure", [_address_error(), TimeoutError("Page.goto: Timeout 1234ms exceeded")])
def test_second_failure_propagates(failure):
    page, goto = _page(_address_error(), failure)
    with pytest.warns(RuntimeWarning, match="retrying once"), pytest.raises(Error) as caught:
        page.goto(URL)
    assert caught.value is failure
    assert goto.call_count == 2


@pytest.mark.parametrize("failure", [
    Error("Page.goto: net::ERR_CONNECTION_REFUSED"),
    Error("Page.goto: net::ERR_ABORTED"),
    TimeoutError("Page.goto: Timeout 1234ms exceeded"),
    Error("Target page, context or browser has been closed"),
    _address_error("https://example.com/redirect-target"),
    Error(""),
])
def test_other_failures_propagate_without_retry(failure):
    page, goto = _page(failure)
    with pytest.raises(Error) as caught:
        page.goto(URL)
    assert caught.value is failure
    goto.assert_called_once_with(URL)


@pytest.mark.parametrize("url", [
    "https://example.com/browse",
    "http://127.0.0.1.example.com:57718/browse",
    "http://192.0.2.1:57718/browse",
    "http://127.0.0.1:0/browse",
    "http://127.0.0.1:99999/browse",
    "http://127.0.0.1/browse",
])
def test_non_test_server_urls_are_not_retried(url):
    failure = _address_error(url)
    page, goto = _page(failure)
    with pytest.raises(Error) as caught:
        page.goto(url)
    assert caught.value is failure
    goto.assert_called_once_with(url)


def test_later_navigation_and_reload_are_not_retried():
    failure = _address_error()
    page, goto = _page(failure, current_url=URL)
    with pytest.raises(Error) as caught:
        page.goto(URL)
    assert caught.value is failure
    goto.assert_called_once_with(URL)
    page.reload.side_effect = failure
    with pytest.raises(Error):
        page.reload()
    page.reload.assert_called_once_with()


def test_retry_still_exposes_modules_after_success(context, live_server):
    page = context.new_page()
    real_goto = page.goto
    url = live_server["url"] + "/browse"
    attempts = []

    def fail_first_goto(target, **kwargs):
        attempts.append(target)
        if len(attempts) == 1:
            raise _address_error(target)
        return real_goto(target, **kwargs)

    page.goto = fail_first_goto
    install_initial_navigation_retry(page)
    install_navbar_exposure(page)
    install_page_module_exposure(page, "browse", "browseState")
    after_navigation = Mock()
    page.after_navigation.append(after_navigation)

    with pytest.warns(RuntimeWarning, match="retrying once"):
        response = page.goto(url)

    assert response.status == 200
    assert attempts == [url, url]
    after_navigation.assert_called_once_with(page)
    assert page.evaluate("browseState === pageModules.state.state")
    assert page.evaluate("typeof vireoLightboxViewport") == "object"
    page.wait_for_selector("#grid .grid-card")
