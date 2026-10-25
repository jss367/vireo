import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

function page() {
  const elements = new Map(), toasts = [], reloads = [];
  function element() {
    return {style: {}, children: [], textContent: '', disabled: false,
      appendChild(child) { this.children.push(child); },
      addEventListener() { throw new Error('Retained entries must not bind a remove action'); }};
  }
  const c = vm.createContext({
    document: {createElement: element, getElementById(id) {
      if (!elements.has(id)) elements.set(id, element());
      return elements.get(id);
    }},
    confirm: () => true,
    showToast: (message, kind) => toasts.push({message, kind}),
    storageFormatBytes: bytes => `${bytes} B`,
    safeFetch() { throw new Error('Unexpected transport'); },
    // storage/hf-cache.js is an ES module: its imports come from this context.
    state: {hfCacheInventory: null},
  });
  for (const file of ['settings/models.js', 'storage/hf-cache.js']) {
    vm.runInContext(moduleAsScript(readFileSync(new URL(`../../vireo/static/${file}`, import.meta.url), 'utf8')), c);
  }
  c.loadModels = () => reloads.push('models');
  c.loadPipelineModels = () => reloads.push('pipeline');
  return {c, elements, toasts, reloads};
}

test('both model removal actions show partial cache cleanup instead of a success toast', async () => {
  const {c, toasts, reloads} = page();
  c.safeFetch = async () => ({ok: false, kept_path: '/invented/custom', cache_cleanup: {
    freed: 7, failed: [{error: 'invented permission failure'}],
  }});
  await c.removeModel('invented');
  await c.deletePipelineModel('invented', 'Invented model');
  assert.equal(toasts.length, 2);
  assert(toasts.every(t => t.kind === 'error' && /cleanup was incomplete.*permission failure/.test(t.message)));
  assert.deepEqual(reloads, ['models', 'pipeline']);
});

test('ordinary custom-model unregister still reports intentionally kept weights', async () => {
  const {c, toasts, reloads} = page();
  c.safeFetch = async () => ({ok: true, kept_path: '/invented/custom'});
  await c.removeModel('invented');
  assert.equal(toasts.length, 1);
  assert.equal(toasts[0].kind, 'success');
  assert.match(toasts[0].message, /left in place at \/invented\/custom/);
  assert.deepEqual(reloads, ['models']);
});

test('retained-only entries show retained bytes and cannot bind a remove action', () => {
  const {c} = page();
  const row = c._hfCacheRow({name: 'Copies', key: 'copies', size: 0,
    retained_size: 99, removable: false, installed: false, file_count: 1});
  const button = row.children[1];
  assert.equal(button.disabled, true);
  assert.match(button.title, /copies are retained/);
  const bytes = row.children[0].children[1].children[0];
  assert.match(bytes.textContent, /0 B.*99 B retained/);
  assert.equal(c.hfCacheCardNote({size: 99, unused_size: 0, retained_size: 99}), '99 B retained cache payloads');
  assert.equal(c.hfCacheCardNote({size: 1, unused_size: 0, installed_size: 0}), 'No removable payloads');
});

test('bulk removal counts removable entries and does nothing for retained-only bytes', async () => {
  const {c} = page();
  const confirms = [], posts = [];
  c.confirm = text => { confirms.push(text); return true; };
  c._postHfCacheRemove = body => posts.push(body);
  c.state.hfCacheInventory = {unused_size: 10, entries: [
    {installed: false, removable: false}, {installed: false, removable: true}, {installed: true},
  ]};
  await c.removeUnusedHfCache();
  assert.match(confirms[0], /Remove 1 entry/);
  assert.equal(posts.length, 1);
  c.state.hfCacheInventory = {unused_size: 0, entries: [{installed: false, removable: false}]};
  await c.removeUnusedHfCache();
  assert.equal(posts.length, 1);
  assert.equal(confirms.length, 1);
});

test('unmanaged inventory rows retain their ownership explanation even with an external install', () => {
  const {c} = page();
  for (const installed of [false, true]) {
    const row = c._hfCacheRow({name: 'External cache', key: 'outside/client', size: 0,
      retained_size: 23, removable: false, installed, file_count: 1,
      retained_reason: 'Unmanaged registration: Vireo does not own this cache cleanup.'});
    assert.equal(row.children[1].disabled, true);
    assert.match(row.children[1].title, /Unmanaged registration/);
    assert.match(row.children[0].children[1].children[0].textContent, /0 B.*23 B retained/);
  }
});
