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
    photo(3, {quality_composite: 0.8, focus_evidence: {basis: 'eye', measurement: 300, measurement_name: 'Tenengrad'}}),
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
  // Same encounter, one photo unmeasured: the quality rank still applies.
  assert.deepEqual(plain(ctx.cullComparison(4, 3, 'encounter')),
    {basis: 'quality', measurement_name: 'quality rank in encounter', photo: 0.3, keeper: 0.8, scope: 'encounter'});
  // Different encounters: nothing honest to record, even on a shared basis.
  assert.equal(ctx.cullComparison(2, 3, 'species'), null);
  assert.equal(ctx.cullComparison(2, 3, 'compared'), null);
  // Compared side by side inside one encounter: the focus evidence still applies.
  assert.equal(plain(ctx.cullComparison(2, 1, 'compared')).basis, 'eye');
});

test('a Compare reject keeps its reference when the photo is listed under another species first', () => {
  const {ctx} = page();
  const data = results();
  // Photo 2 shows two species; its keeper 3 is kept only under the second one.
  data.photos[1].confirmed_species_list = ['Black Phoebe', "Say's Phoebe"];
  data.photos[2].species_top5 = [["Say's Phoebe", 0.9]];
  ctx.setPipelineResults(data);
  ctx.setCullAction(3, 'keep');
  ctx.cullRejectKeepers[2] = 3;
  ctx.setCullAction(2, 'reject');
  assert.equal(ctx.cullData.species_groups[0].species, 'Black Phoebe');
  const [rejection] = plain(ctx.collectCullingApplyPayload(ctx.cullData).rejections);
  assert.equal(rejection.keeper_photo_id, 3);
});

test('a reject saved earlier is sent as a flag alone, so its record is kept', () => {
  const {ctx} = page();
  const data = results();
  data.photos[1].flag = 'rejected';
  data.photos[1].flag_confirmation_status = 'confirmed';
  data.photos[1].reject_keeper = {photo_id: 3, filename: 'p3.jpg', comparison: null};
  ctx.setPipelineResults(data);
  ctx.setCullAction(1, 'keep');
  const payload = plain(ctx.collectCullingApplyPayload(ctx.cullData));
  assert.deepEqual(payload.rejects, [2]);
  assert.deepEqual(payload.rejections, []);
});

test('a reject with nothing kept anywhere is sent with an explicit null keeper', () => {
  const {ctx} = page();
  ctx.setPipelineResults(results());
  ctx.setCullAction(2, 'reject');
  const payload = plain(ctx.collectCullingApplyPayload(ctx.cullData));
  assert.deepEqual(payload.rejects, [2]);
  assert.deepEqual(payload.rejections, [{photo_id: 2, keeper_photo_id: null, comparison: null}]);
});

test('the confirm prompt counts keepers, no-keeper rejects and unchanged saved rejects apart', () => {
  const {ctx} = page();
  assert.equal(ctx.describeRejections([2], [{photo_id: 2, keeper_photo_id: null, comparison: null}]),
    '1 with nothing kept to compare');
  assert.equal(ctx.describeRejections([2, 4, 5], [{photo_id: 2, keeper_photo_id: 1, comparison: null}]),
    '1 recorded with the photo kept instead; 2 already saved, unchanged');
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
  assert.equal(saved.photo_id, null);
  assert.equal(saved.text, 'Instead of a photo since deleted (no shared measurement)');
  // A keeper this workspace cannot see keeps its name but gets no thumbnail.
  ctx.pipelineResults.photos[1].reject_keeper = {photo_id: 1, filename: 'p1.jpg', comparison: null, visible: false};
  ctx.rebuildCullDataFromPipeline();
  const hidden = ctx.cullCardKeeper(
    ctx.cullData.species_groups[0].scene_groups[0].photos.find(p => p.photo_id === 2),
    ctx.cullData.species_groups[0].scene_groups[0], ctx.cullData.species_groups[0]);
  assert.equal(hidden.photo_id, null);
  assert.equal(hidden.name, 'p1.jpg');
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

/** The two-encounter fixture with the fixed-scale detail measured on photos 1 to 3. */
function resultsWithDetail() {
  const data = results();
  const detail = (value, score) => ({
    detail: value, detail_px: 900, scale_px: 512, score,
    measurement_name: 'Tenengrad at 512 px subject scale', basis: 'subject',
  });
  data.photos[0].comparable_quality = detail(18200, 0.81);
  data.photos[1].comparable_quality = detail(6100, 0.52);
  data.photos[2].comparable_quality = detail(9400, 0.63);
  return data;
}

test('across encounters a reject is compared by the fixed-scale subject detail', () => {
  const {ctx} = page();
  ctx.setPipelineResults(resultsWithDetail());
  // Inside one encounter the scorer's own evidence still wins.
  assert.equal(plain(ctx.cullComparison(2, 1, 'encounter')).basis, 'eye');
  // Across encounters the detail carries each photo's cross-encounter quality beside it.
  assert.deepEqual(plain(ctx.cullComparison(2, 3, 'species')), {
    basis: 'detail', measurement_name: 'Tenengrad at 512 px subject scale', scale_px: 512,
    photo: 6100, keeper: 9400, scope: 'species', photo_quality: 0.52, keeper_quality: 0.63,
  });
  // An unmeasured photo still has nothing honest to compare across encounters.
  assert.equal(ctx.cullComparison(4, 1, 'species'), null);
  assert.equal(ctx.cullKeeperText('p3.jpg', ctx.cullComparison(2, 3, 'species')),
    'Instead of p3.jpg · subject detail 6100 vs 9400 at 512 px (across encounters)');
  assert.equal(ctx.cullKeeperText('p3.jpg', ctx.cullComparison(2, 3, 'compared')),
    'Instead of p3.jpg · subject detail 6100 vs 9400 at 512 px (compared side by side)');
});

test('the Apply record carries the detail comparison for a cross-encounter keeper', () => {
  const {ctx} = page();
  ctx.setPipelineResults(resultsWithDetail());
  ctx.setCullAction(3, 'keep');
  ctx.setCullAction(2, 'reject');
  const [rejection] = plain(ctx.collectCullingApplyPayload(ctx.cullData).rejections);
  // Nothing is kept in photo 2's encounter, so it loses to the species reference (photo 3).
  assert.equal(rejection.keeper_photo_id, 3);
  assert.equal(rejection.comparison.basis, 'detail');
  assert.deepEqual([rejection.comparison.photo, rejection.comparison.keeper], [6100, 9400]);
});

test('cards show the detail and the cross-encounter quality once measured', () => {
  const {ctx} = page();
  ctx.setPipelineResults(resultsWithDetail());
  assert.deepEqual(plain(ctx.cullDetailLine(1)).text, 'Detail 18,200 · quality 81% across encounters');
  assert.equal(ctx.cullDetailLine(4), null);
  const first = ctx.cullData.species_groups[0].scene_groups[0].photos[0];
  assert.equal(first.comparable, 0.81);
});

test('favorite suggestions on one day are ordered by the cross-encounter quality when measured', () => {
  const {ctx} = page();
  const data = resultsWithDetail();
  // Photo 3 ranks top of its encounter (0.8) but photo 2 has more measured detail;
  // photo 4 is unmeasured and falls back to its encounter rank.
  data.photos[1].comparable_quality.score = 0.7;
  data.photos[2].comparable_quality.score = 0.6;
  const a = {photo_id: 2, quality: 0.4, comparable: 0.7};
  const b = {photo_id: 3, quality: 0.8, comparable: 0.6};
  const c = {photo_id: 4, quality: 0.3, comparable: null};
  assert.deepEqual([b, c, a].sort(ctx.cullQualityOrder).map(p => p.photo_id), [2, 3, 4]);
});

test('mixing measured and unmeasured photos sorts transitively', () => {
  // Pre-fix: the comparator used comparable for measured/measured pairs
  // and encounter quality whenever either side was unmeasured, which
  // formed cycles — A beats B on comparable, C beats A on encounter,
  // B beats C on encounter — so ``Array.sort`` returned a different
  // leader depending on input order. Partitioning measured first keeps
  // every pairwise call consistent with the final order.
  const {ctx} = page();
  const A = {photo_id: 1, quality: 0.3, comparable: 0.9};
  const B = {photo_id: 2, quality: 0.6, comparable: 0.8};
  const C = {photo_id: 3, quality: 0.5, comparable: null};
  const permutations = [
    [A, B, C], [A, C, B], [B, A, C], [B, C, A], [C, A, B], [C, B, A],
  ];
  const sorted = permutations.map(p => p.slice().sort(ctx.cullQualityOrder).map(x => x.photo_id));
  for (const result of sorted) assert.deepEqual(result, sorted[0]);
  // Measured photos lead unmeasured ones regardless of encounter rank.
  assert.deepEqual(sorted[0], [1, 2, 3]);
});
