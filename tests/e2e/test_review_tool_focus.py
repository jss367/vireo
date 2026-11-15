"""Focused-state regressions for independently published Review ordering."""
import pytest
from playwright.sync_api import expect


@pytest.mark.parametrize('tool', ['alternatives', 'queue', 'scores'])
def test_opened_tool_retains_focus_after_dom_move(live_server, page, tool):
    page.set_viewport_size({'width': 768, 'height': 900})
    page.goto(f"{live_server['url']}/review")
    page.locator('[data-pred-id]').first.wait_for(state='visible')
    entries = {'alternatives':'#identityAlternativesPanel > summary', 'queue':'#rqEnabled', 'scores':'.evidence-explanation > summary'}
    targets = {'alternatives':'#identityAlternativesPanel', 'queue':'#reviewQueue', 'scores':'.evidence-explanation'}
    entry = page.locator(entries[tool])
    entry.focus()
    page.keyboard.press('Space' if tool == 'queue' else 'Enter')
    page.wait_for_function("selector => document.querySelector('.review-tools').lastElementChild === document.querySelector(selector)", arg=targets[tool])
    expect(entry).to_be_focused()

def test_closing_queue_retains_focus_and_restores_original_order(live_server, page):
    page.set_viewport_size({'width': 768, 'height': 900})
    page.goto(f"{live_server['url']}/review")
    page.locator('[data-pred-id]').first.wait_for(state='visible')
    checkbox = page.locator('#rqEnabled')
    checkbox.check()
    page.wait_for_function("document.querySelector('.review-tools').lastElementChild.id === 'reviewQueue'")
    checkbox.focus()
    page.keyboard.press('Space')
    page.wait_for_function("document.querySelector('.review-tools').children[1].id === 'reviewQueue'")
    expect(checkbox).not_to_be_checked()
    expect(checkbox).to_be_focused()
