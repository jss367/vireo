"""Delayed startup state must not replace newer edits in the shared filter bar."""

from pathlib import Path

import pytest
from playwright.sync_api import expect

ROOT = Path(__file__).resolve().parents[2]


def start_filter_bar(page, delayed_endpoint, stored_scope=None):
    page.set_content((ROOT / 'vireo/templates/_filterbar.html').read_text())
    page.evaluate('''endpoint => {
      window.savedFilter = {
        root: {mode: 'all', rules: [{mode: 'all', _qs: true,
          _qs_text: 'kingf', _qs_version: 2,
          rules: [{field: 'metadata', op: 'contains', value: 'kingf'}]}]},
        muted: false, visual: null
      };
      window.savedWrites = [];
      window.fetch = async (url, options = {}) => {
        if (url === endpoint && !window.releaseStartup) {
          await new Promise(resolve => { window.releaseStartup = resolve; });
        }
        let data = {};
        if (url === '/api/filters/fields') data = {fields: []};
        if (url === '/api/filters/shortcuts') data = {shortcuts: [], groups: []};
        if (url === '/api/workspaces/active') {
          data = {id: 5, ui_state: {universal_filters: {browse: window.savedFilter}}};
        }
        if (options.method === 'PUT') {
          const body = JSON.parse(options.body);
          window.savedWrites.push(body.ui_state.universal_filters.browse);
        }
        return {ok: true, json: async () => data};
      };
    }''', delayed_endpoint)
    if stored_scope is not None:
        page.evaluate("""scope => {
          Object.defineProperty(window, 'localStorage', {configurable: true, value: {
            getItem: () => scope, setItem: () => {},
          }});
        }""", stored_scope)
    for name in ['vireo-search.js', 'vireo-filter.js']:
        page.add_script_tag(path=str(ROOT / 'vireo/static' / name))
    page.evaluate('''() => {
      window.filterInit = VireoFilter.init({root: '#vireoFilterBar', page: 'browse'});
    }''')
    page.wait_for_function('typeof window.releaseStartup === "function"')
    assert not page.evaluate('VireoFilter.isReady()')
    return page.locator('.vf-search input')


@pytest.mark.parametrize('delayed_endpoint', ['/api/filters/fields', '/api/workspaces/active'])
@pytest.mark.parametrize('query', ['kes', '', 'kes OR ('])
def test_new_search_survives_delayed_startup(page, delayed_endpoint, query):
    search = start_filter_bar(page, delayed_endpoint)
    search.fill('kes')
    if query != 'kes':
        search.fill(query)
    search.press('Tab')
    # Release immediately: init must also preserve text whose debounce has
    # not run yet, including on blur when focus is outside the search box.
    page.evaluate('''async () => {
      window.releaseStartup();
      await window.filterInit;
    }''')
    expect(search).to_have_value(query)
    if query == 'kes OR (':
        expect(search).to_have_attribute('aria-invalid', 'true')
    elif query:
        page.wait_for_function('VireoFilter.getUserRules().rules[0]?._qs_text === "kes"')
    else:
        assert page.evaluate('VireoFilter.getUserRules().rules') == []
    assert 'kingf' not in str(page.evaluate('VireoFilter.getRules()'))
    page.wait_for_function('window.savedWrites.length > 0')
    saved = page.evaluate('window.savedWrites.at(-1)')
    assert 'kingf' not in str(saved)
    if query == 'kes':
        assert saved['root']['rules'][0]['_qs_text'] == 'kes'


def test_applied_search_survives_delayed_restore(page):
    search = start_filter_bar(page, '/api/workspaces/active')
    search.fill('kes')
    page.wait_for_function('VireoFilter.getUserRules().rules[0]?._qs_text === "kes"')
    search.press('Tab')
    page.evaluate('''async () => {
      window.releaseStartup();
      await window.filterInit;
    }''')
    expect(search).to_have_value('kes')
    assert page.evaluate('VireoFilter.getUserRules().rules[0]._qs_text') == 'kes'
    page.wait_for_function('window.savedWrites.at(-1)?.root.rules[0]?._qs_text === "kes"')


def test_untouched_search_restores_saved_filters(page):
    search = start_filter_bar(page, '/api/workspaces/active')
    page.evaluate('''async () => {
      window.releaseStartup();
      await window.filterInit;
    }''')
    expect(search).to_have_value('kingf')
    assert page.evaluate('VireoFilter.getUserRules().rules[0]._qs_text') == 'kingf'


def test_scope_toggle_survives_delayed_restore_with_empty_search(page):
    search = start_filter_bar(page, '/api/workspaces/active')
    # Also exercise mute preservation: picking a scope must not discard
    # unrelated saved state (saved rules, mute, visual clause).
    page.evaluate('''() => {
      window.savedFilter = {
        root: window.savedFilter.root,
        muted: true,
        visual: null,
      };
    }''')
    toggle = page.locator('.vf-search-scope')
    # Choose a different scope while the input is empty and restore is
    # pending. Returning to the input's default scope is a newer choice too.
    before = toggle.get_attribute('aria-pressed')
    toggle.click()
    chosen = toggle.get_attribute('aria-pressed')
    assert chosen != before
    page.evaluate('''async () => {
      window.releaseStartup();
      await window.filterInit;
    }''')
    # The explicit scope choice survives the restore ...
    expect(toggle).to_have_attribute('aria-pressed', chosen)
    # ... and the saved filter tree still restores, so saved search text,
    # saved rules, and mute state are not discarded merely because the
    # user toggled the scope.
    expect(search).to_have_value('kingf')
    assert page.evaluate('VireoFilter.getUserRules().rules[0]._qs_text') == 'kingf'
    assert page.evaluate('VireoFilter.isMuted()') is True
    # A newly typed search uses the toggle's current scope, overriding the
    # restored chip's own scope.
    search.fill('hawk')
    search.press('Enter')
    field = 'keyword' if chosen == 'true' else 'metadata'
    assert page.evaluate('VireoFilter.getUserRules().rules[0].rules[0].field') == field


@pytest.mark.parametrize('saved_scope', ['all', 'keyword'])
def test_focused_input_restores_saved_scope_without_replacing_text(page, saved_scope):
    search = start_filter_bar(
        page, '/api/workspaces/active',
        stored_scope='all' if saved_scope == 'keyword' else 'keyword',
    )
    page.evaluate("""scope => {
      const group = window.savedFilter.root.rules[0];
      group._qs_scope = scope;
      group.rules[0].field = scope === 'keyword' ? 'keyword' : 'metadata';
    }""", saved_scope)
    search.focus()
    expect(search).to_be_focused()
    page.evaluate("""async () => {
      window.releaseStartup();
      await window.filterInit;
    }""")
    expect(search).to_be_focused()
    expect(search).to_have_value('')
    toggle = page.locator('.vf-search-scope')
    expect(toggle).to_have_attribute('aria-pressed', 'true' if saved_scope == 'keyword' else 'false')
    search.fill('hawk')
    search.press('Enter')
    field = 'keyword' if saved_scope == 'keyword' else 'metadata'
    assert page.evaluate('VireoFilter.getUserRules().rules[0].rules[0].field') == field
