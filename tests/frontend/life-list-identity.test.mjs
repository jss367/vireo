import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

const cards = readFileSync('vireo/static/life-list/cards.js', 'utf8');

function harness() {
  const entries = [11, 12].map((taxon_id, i) => ({
    species: 'Shared Bird', taxon_id, photos: [{id: i + 1}], has_more: true,
    locations: [], photo_count: 2,
  }));
  const requests = [];
  const state = {currentData: {species: entries}, lifeListLoadPromises: {}};
  const ctx = vm.createContext({
    state, URLSearchParams,
    safeFetch: url => new Promise(resolve => requests.push({url, resolve})),
    escapeAttr: String, formatDate: () => '',
  });
  vm.runInContext(moduleAsScript(cards) + '\nrender = function() {};', ctx);
  return {ctx, entries, requests, state};
}

test('same-named taxa select and page independently, sharing only their own pending request', async () => {
  const {ctx, entries, requests, state} = harness();
  assert.equal(ctx.lifeListEntry('Shared Bird', 12), entries[1]);
  assert.equal(ctx.lifeListEntry('Shared Bird', null), undefined);
  const first = ctx.loadMoreLifeListPhotos(entries[0]);
  assert.equal(ctx.loadMoreLifeListPhotos(entries[0]), first);
  const second = ctx.loadMoreLifeListPhotos(entries[1]);
  assert.notEqual(first, second);
  assert.equal(requests.length, 2);
  for (const [i, request] of requests.entries()) {
    const params = new URL(request.url, 'https://vireo.test').searchParams;
    assert.equal(params.get('taxon_id'), String(entries[i].taxon_id));
    assert.equal(params.get('offset'), '1');
    request.resolve({photos: [{id: i + 3}], loaded_count: 2, has_more: false});
  }
  await Promise.all([first, second]);
  assert.deepEqual(entries[0].photos.map(photo => photo.id), [1, 3]);
  assert.deepEqual(entries[1].photos.map(photo => photo.id), [2, 4]);
  assert.equal(Object.keys(state.lifeListLoadPromises).length, 0);
});

test('cards and paging buttons carry taxon identity alongside the display name', () => {
  const {ctx, entries} = harness();
  const html = ctx.renderCard(entries[1], 2);
  assert.equal((html.match(/data-taxon-id="12"/g) || []).length, 2);
});
