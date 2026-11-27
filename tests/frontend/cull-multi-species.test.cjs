// Cull with photos that show two species: species lists, per-species and
// page-wide tallies, the species filter and Apply, run against the page's
// real classic scripts with a fake DOM.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');

const CULL = path.join(__dirname, '../../vireo/static/cull');
const FILES = ['state', 'decisions', 'keepers', 'data', 'focus', 'actions', 'favorites'];

function page({renderSpecies = false} = {}) {
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, {id, value: '', textContent: '', innerHTML: '', style: {}, disabled: false});
    return nodes.get(id);
  };
  const posted = [];
  const ctx = vm.createContext({
    document: {getElementById: node, querySelector: () => null, querySelectorAll: () => []},
    window: {VireoTime: {captureTime: value => value}},
    VireoBrowseCompare: {bindControls() {}},
    escapeHtml: value => String(value),
    escapeAttr: value => String(value),
    Vireo: {confirm: async () => true},
    safeFetch: async (url, options) => { posted.push([url, JSON.parse(options.body)]); return {ok: true}; },
    showToast() {},
    console,
  });
  for (const file of FILES) {
    vm.runInContext(readFileSync(path.join(CULL, file + '.js'), 'utf8'), ctx, {filename: file + '.js'});
  }
  // Rendering is covered by the browser tests; these cover the data.
  vm.runInContext('renderCulling = function() {};', ctx);
  if (!renderSpecies) ctx.renderCullSpeciesOptions = () => {};
  return {ctx, node, posted};
}

const plain = value => JSON.parse(JSON.stringify(value));

function photo(id, extra) {
  return Object.assign({id, filename: `p${id}.jpg`, timestamp: '2024-05-01T08:00:0' + id, flag: 'none', label: 'REVIEW'}, extra);
}

function results(photos) {
  return {photos, encounters: [{photo_ids: photos.map(p => p.id), bursts: [{photo_ids: photos.map(p => p.id)}]}]};
}

test('a photo lists every confirmed species, then each subject top, then the photo top', () => {
  const {ctx} = page();
  function namesForPhoto(p, encounter = {}) {
    const names = new Map();
    const keys = ctx.cullPhotoSpecies(p, encounter, {}, {}, names);
    return keys.map(key => names.get(key));
  }
  assert.deepEqual(plain(namesForPhoto(photo(1, {
    confirmed_species: 'Mallard', confirmed_species_list: ['Mallard', 'Wood Duck'],
  }))), ['Mallard', 'Wood Duck']);
  assert.deepEqual(plain(namesForPhoto(photo(1, {confirmed_species: ['Mallard', 'Wood Duck', 'Mallard']}))),
    ['Mallard', 'Wood Duck']);
  assert.deepEqual(plain(namesForPhoto(photo(1, {
    species_top5: [['Wood Duck', 0.9], ['Mallard', 0.7]],
    subjects: [
      {predictions: [['Mallard', 0.7, 'm'], ['Gadwall', 0.2, 'm']]},
      {predictions: [['Wood Duck', 0.9, 'm']]},
      {predictions: []},
    ],
  }))), ['Wood Duck', 'Mallard']);
  // Two boxes on one animal that agree are still one species.
  assert.deepEqual(plain(namesForPhoto(photo(1, {
    species_top5: [['Mallard', 0.9]],
    subjects: [{predictions: [['Mallard', 0.9]]}, {predictions: [['Mallard', 0.6]]}],
  }))), ['Mallard']);
  // No per-subject evidence: today's single top species.
  assert.deepEqual(plain(namesForPhoto(photo(1, {species_top5: [['Mallard', 0.9], ['Gadwall', 0.4]]}))),
    ['Mallard']);
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {}))), []);
  // A photo without species falls back to its encounter's confirmed set.
  assert.deepEqual(plain(namesForPhoto(photo(1, {}), {photo_ids: [1], confirmed_species: 'Mallard',
    confirmed_species_list: ['Mallard', 'Wood Duck']}, {})), ['Mallard', 'Wood Duck']);
  assert.deepEqual(plain(namesForPhoto(photo(1, {}), {photo_ids: [1], species: ['Mallard', 0.8]}, {})),
    ['Mallard']);
});

test('single-species photos group exactly as before', () => {
  const {ctx} = page();
  ctx.cullManualDecisions = {3: 'reject'};
  const data = ctx.pipelineToCullData(results([
    photo(1, {confirmed_species: 'Mallard', confirmed_species_list: ['Mallard'], flag: 'flagged', flag_confirmation_status: 'confirmed'}),
    photo(2, {species_top5: [['Mallard', 0.9]]}),
    photo(3, {species_top5: [['Gadwall', 0.8]]}),
  ]));
  assert.deepEqual(plain(data.species_groups.map(sg => [sg.species, sg.photo_count, sg.keepers, sg.reviews, sg.rejects])),
    [['Mallard', 2, 1, 1, 0], ['Gadwall', 1, 0, 0, 1]]);
  assert.equal(data.total_photos, 3);
  assert.equal(data.suggested_keepers, 1);
  assert.equal(data.suggested_reviews, 1);
  assert.equal(data.suggested_rejects, 1);
  assert.equal(data.multi_species_photos, 0);
  assert.equal(data.species_groups[0].scene_groups[0].photos[0].also_species, undefined);
});

test('a two-species photo counts under each species but once in the page totals', () => {
  const {ctx} = page();
  ctx.cullManualDecisions = {1: 'reject'};
  const data = ctx.pipelineToCullData(results([
    photo(1, {confirmed_species: 'Mallard', confirmed_species_list: ['Mallard', 'Wood Duck']}),
    photo(2, {species_top5: [['Mallard', 0.9]]}),
    photo(3, {species_top5: [['Wood Duck', 0.9], ['Mallard', 0.6]],
      subjects: [{predictions: [['Wood Duck', 0.9]]}, {predictions: [['Mallard', 0.6]]}]}),
  ]));
  assert.deepEqual(plain(data.species_groups.map(sg => [sg.species, sg.photo_count, sg.rejects])),
    [['Mallard', 3, 1], ['Wood Duck', 2, 1]]);
  assert.equal(data.total_photos, 3);
  assert.equal(data.suggested_rejects, 1);
  assert.equal(data.suggested_reviews, 2);
  assert.equal(data.pinned_count, 1);
  assert.equal(data.multi_species_photos, 2);
  const mallard = data.species_groups[0].scene_groups[0].photos;
  assert.deepEqual(plain(mallard.map(p => [p.photo_id, p.also_species || []])),
    [[1, ['Wood Duck']], [2, []], [3, ['Wood Duck']]]);
});

test('a favorite suggested under one species shows as one under the other', () => {
  const {ctx} = page();
  const data = ctx.pipelineToCullData(results([
    photo(1, {label: 'KEEP', automatic_keep_eligible: true, quality_composite: 0.9, confirmed_species_list: ['Mallard', 'Wood Duck']}),
  ]));
  const cards = data.species_groups.map(sg => plain(sg.scene_groups[0].photos[0]));
  assert.deepEqual(plain(cards.map(p => [p.favorite, p.action, p.suggested_for])),
    [[true, 'keep', 'Mallard'], [true, 'keep', 'Mallard']]);
  assert.deepEqual(plain(data.species_groups.map(sg => sg.keepers)), [1, 1]);
  assert.equal(data.suggested_keepers, 1);
});

test('reported wildlife winners remain undecided alone and together in Cull', () => {
  const {ctx} = page();
  const fixture = require('../../vireo/tests/fixtures/keep_safety_review_examples.json');
  const failures = [213640, 214325, 216521].map(id => fixture.photos.find(p => p.id === id));
  for (const group of [...failures.map(p => [p]), failures]) {
    const data = ctx.pipelineToCullData(results(group));
    assert.equal(data.suggested_reviews, group.length);
    assert.equal(data.suggested_keepers, 0);
    assert.deepEqual(plain(ctx.collectCullingApplyPayload(data).keepers), []);
  }
});

test('a stale KEEP stays out of favorites and Apply after confirmation is lost', () => {
  const {ctx} = page();
  for (const eligibility of [undefined, false]) {
    const source = results([photo(1, {
      label: 'KEEP', flag: 'flagged', flag_confirmation_status: 'confirmed',
      automatic_keep_eligible: eligibility, confirmed_species_list: ['Mallard', 'Wood Duck'],
    })]);
    ctx.cullSourceResults = source;
    assert.equal(ctx.pipelineToCullData(source).suggested_keepers, 1);
    ctx.syncCullSourceFlags(results([photo(1, {flag: 'flagged', flag_confirmation_status: 'unverified'})]));
    const data = ctx.pipelineToCullData(source);
    assert.equal(data.suggested_reviews, 1);
    assert.equal(data.suggested_keepers, 0);
    for (const species of data.species_groups) {
      assert.equal(species.scene_groups[0].photos[0].favorite, false);
    }
    assert.deepEqual(plain(ctx.collectCullingApplyPayload(data).keepers), []);
  }
});

test('the species filter keeps a two-species photo under the chosen species only', () => {
  const {ctx} = page();
  ctx.cullSourceResults = results([
    photo(1, {confirmed_species_list: ['Mallard', 'Wood Duck']}),
    photo(2, {confirmed_species_list: ['Wood Duck']}),
    photo(3, {confirmed_species_list: ['Gadwall']}),
  ]);
  ctx.cullSelectedSpecies.set('name:mallard', 'Mallard');
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [1]);
  const data = ctx.pipelineToCullData(ctx.cullSourceResults);
  assert.deepEqual(plain(data.species_groups.map(sg => [sg.species, sg.photo_count])), [['Mallard', 1]]);
  ctx.cullSelectedSpecies.set('name:wood duck', 'Wood Duck');
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [1, 2]);
  const both = ctx.pipelineToCullData(ctx.cullSourceResults);
  assert.deepEqual(plain(both.species_groups.map(sg => [sg.species, sg.photo_count])), [['Wood Duck', 2], ['Mallard', 1]]);
  assert.equal(both.total_photos, 2);
});

test('case variants share a dropdown count, filter and group across encounters', () => {
  const {ctx, node} = page({renderSpecies: true});
  const options = [];
  Object.assign(node('cullSpeciesOptions'), {
    childNodes: options,
    replaceChildren() { options.length = 0; },
    appendChild(label) { options.push(label); },
  });
  ctx.document.createElement = () => ({
    append(checkbox, text) { Object.assign(this, {checkbox, text}); },
    addEventListener(event, callback) { this[event] = callback; },
  });
  ctx.document.createTextNode = text => text;
  const source = results([
    photo(1, {confirmed_species_list: ['Great blue heron', 'Great Blue Heron']}),
    photo(2, {species_top5: [['Great Blue Heron', 0.9]]}),
    photo(3, {confirmed_species: 'Mallard'}),
  ]);
  source.encounters = [{photo_ids: [1]}, {photo_ids: [2, 3]}];
  ctx.cullSourceResults = source;
  ctx.renderCullSpeciesOptions();
  assert.deepEqual(options.map(o => o.text), ['Great blue heron (2)', 'Mallard (1)']);
  ctx.onCullScopeChange = () => {};
  options[0].checkbox.checked = true;
  options[0].checkbox.change();
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [1, 2]);
  const data = ctx.pipelineToCullData(source);
  assert.deepEqual(plain(data.species_groups.map(g => [g.species, g.photo_count])), [['Great blue heron', 2]]);
  assert.equal(data.multi_species_photos, 0);
  assert.equal(ctx.pipelineToCullData(results([source.photos[1]])).total_photos, 1);
});

test('Cull groups aliases by taxon and preserves distinct taxa sharing a subject label', () => {
  const {ctx} = page();
  const key = 'taxon:4956';
  const source = results([
    photo(1, {confirmed_species: 'Great blue heron', confirmed_species_identities: [{name: 'Great blue heron', key}]}),
    photo(2, {species_top5: [['Ardea herodias', 0.9, 'model', key]]}),
    photo(3, {subjects: [
      {predictions: [['Heron', 0.8, 'model', key]]},
      {predictions: [['Heron', 0.9, 'model', 'taxon:999']]},
    ]}),
  ]);
  ctx.cullSourceResults = source;
  const data = ctx.pipelineToCullData(source);
  assert.deepEqual(plain(data.species_groups.map(g => [g.species_key, g.photo_count])), [[key, 3], ['taxon:999', 1]]);
  assert.equal(data.multi_species_photos, 1);
  ctx.cullSelectedSpecies.set(key, 'Great blue heron');
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [1, 2, 3]);
  ctx.cullSelectedSpecies.clear();
  ctx.cullSelectedSpecies.set('taxon:999', 'Heron');
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [3]);
});

test('Apply sends a two-species photo once', async () => {
  const {ctx, posted} = page();
  ctx.cullManualDecisions = {1: 'keep', 2: 'reject'};
  ctx.pipelineResults = results([
    photo(1, {confirmed_species_list: ['Mallard', 'Wood Duck']}),
    photo(2, {species_top5: [['Mallard', 0.9]], subjects: [{predictions: [['Mallard', 0.9]]}, {predictions: [['Wood Duck', 0.5]]}]}),
  ]);
  ctx.cullData = ctx.pipelineToCullData(ctx.pipelineResults);
  ctx.cullOperationPending = () => false;
  ctx.updateCullApplyButton = () => {};
  ctx.syncCullSourceFlags = () => {};
  ctx.rebuildCullDataFromPipeline = () => {};
  await ctx.applyCulling();
  // The reject is recorded in favor of the kept photo of its encounter.
  assert.deepEqual(posted, [['/api/culling/apply', {
    keepers: [1], rejects: [2], unflag: [],
    rejections: [{photo_id: 2, keeper_photo_id: 1, comparison: null}],
  }]]);
});
