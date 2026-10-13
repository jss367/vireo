import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../vireo/static/navbar-command-palette-open.js', import.meta.url), 'utf8');
const pages = [
  ['import', '/import'], ['browse', '/browse'], ['pipeline', '/pipeline'],
  ['pipeline_review', '/pipeline/review'], ['review', '/review'], ['cull', '/cull'],
  ['jobs', '/jobs'], ['highlights', '/highlights'], ['misses', '/misses'],
  ['keywords', '/keywords'], ['storage', '/storage'],
].map(([id, href]) => ({id, href}));

function harness(invoke) {
  let tabs = ['keywords', 'storage', 'browse'];
  let keydown;
  const window = {
    location: {href: ''},
    _navTabs: {getTabs: () => tabs, getAllPages: () => pages},
    vireoResolveNavigationHref: path => path === '/browse' ? '/browse?photo_id=42' : path,
  };
  if (invoke) window.__TAURI_INTERNALS__ = {invoke};
  vm.runInNewContext(source, {
    window, navigator: {platform: 'MacIntel'}, console,
    document: {addEventListener: (type, callback) => { if (type === 'keydown') keydown = callback; }},
  });
  return {
    window,
    reorder: order => { tabs = order; },
    press: (key, modifiers = {}) => {
      let prevented = false;
      keydown({key, metaKey: true, shiftKey: false, altKey: false,
        preventDefault: () => { prevented = true; }, ...modifiers});
      return prevented;
    },
  };
}

test('tab order follows reordering; fixed pages remain stable; both preserve Browse context', () => {
  const h = harness();
  assert(h.press('1'));
  assert.equal(h.window.location.href, '/keywords');
  h.reorder(['storage', 'browse', 'keywords']);
  h.press('1');
  assert.equal(h.window.location.href, '/storage');
  h.press('2');
  assert.equal(h.window.location.href, '/browse?photo_id=42');

  h.window.vireoSetNumberedNavigationMode('fixed_pages');
  for (const [index, page] of pages.slice(0, 9).entries()) {
    assert(h.press(String(index + 1)));
    assert.equal(h.window.location.href, h.window.vireoResolveNavigationHref(page.href));
  }
  h.reorder(['keywords', 'browse']);
  h.press('1');
  assert.equal(h.window.location.href, '/import');
  h.window.vireoSetNumberedNavigationMode('tab_order');
  h.press('1');
  assert.equal(h.window.location.href, '/keywords');
});

test('unavailable tab positions and modified numbers do not navigate', () => {
  const h = harness();
  assert.equal(h.press('9'), false);
  assert.equal(h.press('1', {altKey: true}), false);
  assert.equal(h.press('1', {shiftKey: true}), false);
  assert.equal(h.press('1', {metaKey: false}), false);
  assert.equal(h.window.location.href, '');
});

test('native menu updates use the same destinations and finish in order', async () => {
  const calls = [];
  let release;
  const first = new Promise(resolve => { release = resolve; });
  const h = harness(async (command, args) => {
    calls.push({command, paths: Array.from(args.paths)});
    if (calls.length === 1) await first;
  });
  const initial = h.window.vireoSyncNumberedNavigation();
  await Promise.resolve();
  assert.deepEqual(calls, [{command: 'sync_navigation_shortcuts', paths: ['/keywords', '/storage', '/browse']}]);
  const fixed = h.window.vireoSetNumberedNavigationMode('fixed_pages');
  assert.equal(calls.length, 1);
  release();
  await initial;
  await fixed;
  assert.deepEqual(calls[1].paths, pages.slice(0, 9).map(page => page.href));
  h.reorder(['storage', 'keywords']);
  await h.window.vireoSetNumberedNavigationMode('tab_order');
  assert.deepEqual(calls[2].paths, ['/storage', '/keywords']);
  assert(h.press('J', {shiftKey: true}));
  assert.equal(h.window.location.href, '/jobs');
});
