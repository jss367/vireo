// Cull with photos that show two species: species lists, per-species and
// page-wide tallies, the species filter and Apply, run against the page's
// real classic scripts with a fake DOM.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');

const CULL = path.join(__dirname, '../../vireo/static/cull');
const FILES = ['state', 'decisions', 'data', 'focus', 'actions', 'favorites'];

function page() {
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
    confirm: () => true,
    safeFetch: async (url, options) => { posted.push([url, JSON.parse(options.body)]); return {ok: true}; },
    showToast() {},
    console,
  });
  for (const file of FILES) {
    vm.runInContext(readFileSync(path.join(CULL, file + '.js'), 'utf8'), ctx, {filename: file + '.js'});
  }
  // Rendering is covered by the browser tests; these cover the data.
  vm.runInContext('renderCulling = function() {}; renderCullSpeciesOptions = function() {};', ctx);
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
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {
    confirmed_species: 'Mallard', confirmed_species_list: ['Mallard', 'Wood Duck'],
  }))), ['Mallard', 'Wood Duck']);
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {confirmed_species: ['Mallard', 'Wood Duck', 'Mallard']}))),
    ['Mallard', 'Wood Duck']);
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {
    species_top5: [['Wood Duck', 0.9], ['Mallard', 0.7]],
    subjects: [
      {predictions: [['Mallard', 0.7, 'm'], ['Gadwall', 0.2, 'm']]},
      {predictions: [['Wood Duck', 0.9, 'm']]},
      {predictions: []},
    ],
  }))), ['Wood Duck', 'Mallard']);
  // Two boxes on one animal that agree are still one species.
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {
    species_top5: [['Mallard', 0.9]],
    subjects: [{predictions: [['Mallard', 0.9]]}, {predictions: [['Mallard', 0.6]]}],
  }))), ['Mallard']);
  // No per-subject evidence: today's single top species.
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {species_top5: [['Mallard', 0.9], ['Gadwall', 0.4]]}))),
    ['Mallard']);
  assert.deepEqual(plain(ctx.speciesListForPhoto(photo(1, {}))), []);
  // A photo without species falls back to its encounter's confirmed set.
  assert.deepEqual(plain(ctx.cullPhotoSpecies(photo(1, {}), {photo_ids: [1], confirmed_species: 'Mallard',
    confirmed_species_list: ['Mallard', 'Wood Duck']}, {})), ['Mallard', 'Wood Duck']);
  assert.deepEqual(plain(ctx.cullPhotoSpecies(photo(1, {}), {photo_ids: [1], species: ['Mallard', 0.8]}, {})),
    ['Mallard']);
});

test('single-species photos group exactly as before', () => {
  const {ctx} = page();
  ctx.cullManualDecisions = {3: 'reject'};
  const data = ctx.pipelineToCullData(results([
    photo(1, {confirmed_species: 'Mallard', confirmed_species_list: ['Mallard'], flag: 'flagged'}),
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
    photo(1, {label: 'KEEP', quality_composite: 0.9, confirmed_species_list: ['Mallard', 'Wood Duck']}),
  ]));
  const cards = data.species_groups.map(sg => plain(sg.scene_groups[0].photos[0]));
  assert.deepEqual(plain(cards.map(p => [p.favorite, p.action, p.suggested_for])),
    [[true, 'keep', 'Mallard'], [true, 'keep', 'Mallard']]);
  assert.deepEqual(plain(data.species_groups.map(sg => sg.keepers)), [1, 1]);
  assert.equal(data.suggested_keepers, 1);
});

test('the species filter keeps a two-species photo under the chosen species only', () => {
  const {ctx} = page();
  ctx.cullSourceResults = results([
    photo(1, {confirmed_species_list: ['Mallard', 'Wood Duck']}),
    photo(2, {confirmed_species_list: ['Wood Duck']}),
    photo(3, {confirmed_species_list: ['Gadwall']}),
  ]);
  ctx.cullSelectedSpecies.add('Mallard');
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [1]);
  const data = ctx.pipelineToCullData(ctx.cullSourceResults);
  assert.deepEqual(plain(data.species_groups.map(sg => [sg.species, sg.photo_count])), [['Mallard', 1]]);
  ctx.cullSelectedSpecies.add('Wood Duck');
  assert.deepEqual(plain(ctx.cullScopePhotoIds()), [1, 2]);
  const both = ctx.pipelineToCullData(ctx.cullSourceResults);
  assert.deepEqual(plain(both.species_groups.map(sg => [sg.species, sg.photo_count])), [['Wood Duck', 2], ['Mallard', 1]]);
  assert.equal(both.total_photos, 2);
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
  assert.deepEqual(posted, [['/api/culling/apply', {keepers: [1], rejects: [2], unflag: []}]]);
});
