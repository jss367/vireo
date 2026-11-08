// Relational rejects on the Cull page: which kept photo a reject loses to,
// the measurement the two share, and what Apply sends, run against the
// page's real classic scripts with a fake DOM.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');

const CULL = path.join(__dirname, '../../vireo/static/cull');
const FILES = ['state', 'analysis', 'decisions', 'keepers', 'data', 'focus', 'actions', 'favorites'];

function page() {
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, {id, value: '', textContent: '', innerHTML: '', style: {}, disabled: false, hidden: false});
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
    safeFetch: async (url, options) => {
      if (options && options.body) posted.push([url, JSON.parse(options.body)]);
      return {ok: true, orphans: []};
    },
    showToast() {},
    console,
  });
  for (const file of FILES) {
    vm.runInContext(readFileSync(path.join(CULL, file + '.js'), 'utf8'), ctx, {filename: file + '.js'});
  }
  vm.runInContext('renderCulling = function() {}; renderCullSpeciesOptions = function() {}; updateCullApplyButton = function() {};', ctx);
  return {ctx, node, posted};
}

const plain = value => JSON.parse(JSON.stringify(value));

function photo(id, extra) {
  return Object.assign({
    id, filename: `p${id}.jpg`, timestamp: '2024-05-01T08:00:0' + id, flag: 'none', label: 'REVIEW',
    species_top5: [['Black Phoebe', 0.9]],
  }, extra);
}

/** Two encounters of one species: 1 and 2 in the first, 3 and 4 in the second. */
function results() {
  const photos = [
    photo(1, {quality_composite: 0.9, focus_evidence: {basis: 'eye', measurement: 1840.4, measurement_name: 'Tenengrad'}}),
    photo(2, {quality_composite: 0.4, focus_evidence: {basis: 'eye', measurement: 610.2, measurement_name: 'Tenengrad'}}),
    photo(3, {quality_composite: 0.8, focus_evidence: {basis: 'subject', measurement: 300, measurement_name: 'Tenengrad'}}),
    photo(4, {quality_composite: 0.3, focus_evidence: null}),
  ];
  return {photos, encounters: [
    {photo_ids: [1, 2], bursts: [{photo_ids: [1, 2]}]},
    {photo_ids: [3, 4], bursts: [{photo_ids: [3, 4]}]},
  ]};
}

test('a reject loses to the best kept photo of its encounter, with the measurement both share', () => {
  const {ctx, posted} = page();
  ctx.setPipelineResults(results());
  ctx.setCullAction(1, 'keep');
  ctx.setCullAction(2, 'reject');
  const payload = plain(ctx.collectCullingApplyPayload(ctx.cullData));
  assert.deepEqual(payload.rejections, [{
    photo_id: 2, keeper_photo_id: 1,
    comparison: {basis: 'eye', measurement_name: 'Tenengrad', photo: 610, keeper: 1840, scope: 'encounter'},
  }]);
  return ctx.applyCulling().then(() => {
    assert.deepEqual(posted[0][1].rejections, payload.rejections);
    // The saved keeper now rides on the source photo for the card to show.
    assert.deepEqual(plain(ctx.pipelineResults.photos[1].reject_keeper),
      {photo_id: 1, filename: 'p1.jpg', comparison: payload.rejections[0].comparison});
  });
});

test('a reject with nothing kept in its encounter loses to the species reference favorite', () => {
  const {ctx} = page();
  ctx.setPipelineResults(results());
  ctx.setCullAction(1, 'keep');
  ctx.setCullAction(4, 'reject');
  const [rejection] = plain(ctx.collectCullingApplyPayload(ctx.cullData).rejections);
  assert.equal(rejection.keeper_photo_id, 1);
  // Photo 4 has no focus evidence and sits in another encounter: no shared measurement.
  assert.equal(rejection.comparison, null);
});

test('composite quality compares only inside one encounter', () => {
  const {ctx} = page();
  ctx.setPipelineResults(results());
  // Same encounter, bases differ (subject vs none): the quality rank still applies.
  assert.deepEqual(plain(ctx.cullComparison(4, 3, 'encounter')),
    {basis: 'quality', measurement_name: 'quality rank in encounter', photo: 0.3, keeper: 0.8, scope: 'encounter'});
  // Different encounters and different bases: nothing honest to record.
  assert.equal(ctx.cullComparison(3, 1, 'species'), null);
  // Different encounters, same basis: the raw measurements are recorded as such.
  assert.equal(plain(ctx.cullComparison(2, 1, 'species')).scope, 'species');
});

test('a reject with nothing kept anywhere is sent without a keeper', () => {
  const {ctx} = page();
  ctx.setPipelineResults(results());
  ctx.setCullAction(2, 'reject');
  const payload = plain(ctx.collectCullingApplyPayload(ctx.cullData));
  assert.deepEqual(payload.rejects, [2]);
  assert.deepEqual(payload.rejections, []);
});

test('rejecting in Compare records the reference on screen, until the decision changes', () => {
  const {ctx} = page();
  ctx.setPipelineResults(results());
  ctx.setCullAction(1, 'keep');
  ctx.setCullAction(3, 'keep');
  ctx.cullFavoriteReferences['Black Phoebe'] = 3;
  ctx.browseCompare = {close() {}, isOpen: () => true, controller: {open: async () => {}}};
  ctx.cullCompareState = {species: 'Black Phoebe', photoId: 2};
  ctx.renderCullComparison = async () => {};
  ctx.decideCullComparison('reject');
  assert.equal(ctx.cullRejectKeepers[2], 3);
  // Compared against 3, so the record names 3 even though 1 shares its encounter.
  let [rejection] = plain(ctx.collectCullingApplyPayload(ctx.cullData).rejections);
  assert.equal(rejection.keeper_photo_id, 3);
  assert.equal(rejection.comparison, null);
  ctx.setCullAction(2, 'review');
  assert.equal(ctx.cullRejectKeepers[2], undefined);
  ctx.setCullAction(2, 'reject');
  [rejection] = plain(ctx.collectCullingApplyPayload(ctx.cullData).rejections);
  assert.equal(rejection.keeper_photo_id, 1);
});

test('the card text says what a reject lost to and by what measure', () => {
  const {ctx} = page();
  ctx.setPipelineResults(results());
  ctx.setCullAction(1, 'keep');
  ctx.setCullAction(2, 'reject');
  const sg = ctx.cullData.species_groups[0];
  const group = sg.scene_groups[0];
  const reject = group.photos.find(p => p.photo_id === 2);
  const pending = ctx.cullCardKeeper(reject, group, sg);
  assert.equal(pending.saved, false);
  assert.equal(pending.text, 'Instead of p1.jpg · eye Tenengrad 610 vs 1840 (same encounter)');
  ctx.pipelineResults.photos[1].reject_keeper = {photo_id: 1, filename: null, comparison: null};
  ctx.pipelineResults.photos[1].flag = 'rejected';
  ctx.pipelineResults.photos[1].flag_confirmation_status = 'confirmed';
  delete ctx.cullManualDecisions[2];
  ctx.rebuildCullDataFromPipeline();
  const saved = ctx.cullCardKeeper(
    ctx.cullData.species_groups[0].scene_groups[0].photos.find(p => p.photo_id === 2),
    ctx.cullData.species_groups[0].scene_groups[0], ctx.cullData.species_groups[0]);
  assert.equal(saved.saved, true);
  assert.equal(saved.text, 'Instead of a photo since deleted (no shared measurement)');
});

test('the orphan banner counts saved rejects whose keeper is gone', async () => {
  const {ctx, node} = page();
  ctx.safeFetch = async () => ({orphans: [
    {photo_id: 2, filename: 'p2.jpg', keeper_photo_id: 1, keeper_filename: null, comparison: null, orphan_reason: 'keeper_deleted'},
  ]});
  await ctx.loadCullOrphans();
  assert.equal(node('cullOrphans').hidden, false);
  assert.equal(node('cullOrphansText').textContent, '1 rejected photo lost the photo it was rejected for.');
  assert.equal(ctx.cullOrphanReason(ctx.cullOrphans[0]),
    'Instead of a photo since deleted (no shared measurement) — that photo was deleted.');
});
