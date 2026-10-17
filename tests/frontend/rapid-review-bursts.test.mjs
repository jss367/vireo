import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function page() {
  const elements = new Map();
  const context = vm.createContext({
    document: {getElementById(id) {
      if (!elements.has(id)) elements.set(id, {innerHTML: '', textContent: '', value: '', checked: false});
      return elements.get(id);
    }},
    window: {location: {search: ''}},
    escapeHtml: String,
    escapeAttr: String,
    safeFetch() { throw new Error('Unexpected transport in inert burst test'); },
  });
  for (const file of ['state', 'photo-urls', 'species-list', 'sessions', 'queue-filters', 'queue', 'render', 'decisions', 'species-keywords', 'apply-diff', 'apply']) {
    vm.runInContext(readFileSync(new URL(`../../vireo/static/pipeline-rapid-review/${file}.js`, import.meta.url), 'utf8'), context);
  }
  return {context, elements};
}

const plain = value => JSON.parse(JSON.stringify(value));

test('Rapid Review builds and renders legacy and object burst sessions together', () => {
  const {context: c, elements} = page();
  c.rapid.photoMap = {1: {id: 1}, 2: {id: 2}, 3: {id: 3}};
  c.rapid.results = {encounters: [{
    confirmed_species: 'Wigeon',
    bursts: [[1, 2, 999], {photo_ids: [3], species_override: {species: 'Mallard', confirmed: true}}],
  }]};
  c.rapid.allSessions = c.buildSessions(c.rapid.results);
  c.rebuildFilteredSessions();
  assert.deepEqual(plain(c.rapid.sessions.map(s => ({ids: s.ids, species: s.species, override: s.hasBurstOverride}))), [
    {ids: [1, 2], species: 'Wigeon', override: false},
    {ids: [3], species: 'Mallard', override: true},
  ]);
  c.renderBurstList();
  assert.match(elements.get('burstList').innerHTML, /Encounter 1, Burst 1/);
  assert.match(elements.get('burstList').innerHTML, /Encounter 1, Burst 2/);
  assert.match(elements.get('queueCount').textContent, /2 of 2 bursts/);
});

test('Rapid Review locates legacy and modern bursts after server restructuring', () => {
  const {context: c} = page();
  const encounters = [{bursts: [{photo_ids: [9]}, [1, 2]]}, {bursts: [{photo_ids: [3, 4]}]}];
  assert.deepEqual(plain(c.locateBurstByPhotoIds(encounters, [1, 2])), {encIdx: 0, burstIdx: 1});
  assert.deepEqual(plain(c.locateBurstByPhotoIds(encounters, [4])), {encIdx: 1, burstIdx: 0});
  assert.equal(c.locateBurstByPhotoIds(encounters, [1, 3]), null);
  assert.equal(c.locateBurstByPhotoIds(encounters, []), null);
});

test('legacy bursts inherit encounter species while object overrides preserve their precedence', () => {
  const {context: c} = page();
  const encounter = {confirmed_species_list: ['Wigeon'], species: ['prediction']};
  assert.deepEqual(plain(c.burstSpeciesList(encounter, [1])), ['Wigeon']);
  assert.deepEqual(plain(c.burstSpeciesList(encounter, {photo_ids: [1], species_override: {species_list: []}})), []);
  assert.deepEqual(plain(c.burstSpeciesList(encounter, {photo_ids: [1], species_override: {species: 'candidate', confirmed: false}})), ['Wigeon']);
  assert.equal(c.hasBurstSpeciesOverride([1]), false);
  assert.deepEqual(plain(c.burstIds([1, 2])), [1, 2]);
  assert.deepEqual(plain(c.burstIds({photo_ids: [1, 2]})), [1, 2]);
  assert.deepEqual(plain(c.burstIds(null)), []);
});

for (const [name, burst] of [
  ['legacy', [1, 2]],
  ['object', {photo_ids: [1, 2]}],
  ['confirmed object', {photo_ids: [1, 2], species_override: {species: 'Wigeon', confirmed: true}}],
]) {
  test(`flag-only Apply on a ${name} burst does not repost its confirmed species`, async () => {
    const {context: c} = page();
    c.rapid.results = {
      photos: [{id: 1, confirmed_species: 'Wigeon'}, {id: 2, confirmed_species: 'Wigeon'}],
      encounters: [{confirmed_species: 'Wigeon', bursts: [burst]}],
    };
    c.rapid.photoMap = {1: c.rapid.results.photos[0], 2: c.rapid.results.photos[1]};
    c.rapid.items = c.rapid.results.photos;
    c.rapid.allSessions = c.buildSessions(c.rapid.results);
    c.rapid.sessions = c.rapid.allSessions;
    c.rapid.sessionIndex = 0;
    c.rapid.seeded = true;
    c.rapid.keywordStateSpecies = 'Wigeon';
    c.rapid.initialSpeciesKeywords = {1: {wigeon: true}, 2: {wigeon: true}};
    c.document.getElementById('speciesInput').value = 'Wigeon';
    c.renderApplyState = () => {};
    c.openSession = () => {};
    c.showEmptyQueue = () => {};
    const requests = [];
    c.safeFetch = async (url, init) => {
      requests.push([url, JSON.parse(init.body)]);
      return {};
    };
    assert.equal(await c.applyCurrent(false), true);
    assert.deepEqual(requests.map(([url]) => url), ['/api/pipeline/group/apply', '/api/pipeline/save-cache']);
    assert.deepEqual(plain(c.rapid.items.map(p => p.confirmed_species)), ['Wigeon', 'Wigeon']);
  });
}
