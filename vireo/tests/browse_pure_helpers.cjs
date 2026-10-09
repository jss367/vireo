// Unit tests for the pure helpers in Browse's page scripts (vireo/static/browse).
// Each helper is lifted out of its file by name and run in a fresh VM context
// holding only the declarations it needs, so a helper that starts reaching for
// the DOM or page state fails here instead of passing by accident.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const DIR = 'vireo/static/browse/';

function declaration(file, name) {
  const src = fs.readFileSync(DIR + file, 'utf8');
  const fn = src.match(new RegExp('(?:^|\\n)((?:async )?function ' + name + '\\([^]*?\\n\\})'));
  if (fn) return fn[1];
  const v = src.match(new RegExp('(?:^|\\n)(var ' + name + ' = [^]*?;)(?=\\n)'));
  assert(v, name + ' is not declared in ' + file);
  return v[1];
}

// load({'cards.js': ['formatFileSize']}, {extra: globals}) -> context
function load(spec, globals) {
  const ctx = vm.createContext(Object.assign({}, globals));
  for (const [file, names] of Object.entries(spec)) {
    for (const name of names) vm.runInContext(declaration(file, name), ctx);
  }
  return ctx;
}

// Values created inside a VM context have that context's prototypes, which
// deepStrictEqual treats as different from this realm's. Compare by JSON.
function same(actual, expected) {
  assert.deepEqual(JSON.parse(JSON.stringify(actual)), expected);
}

const tests = [];
function test(name, body) { tests.push([name, body]); }

test('formatFileSize picks the largest unit below the size', () => {
  const {formatFileSize} = load({'cards.js': ['formatFileSize']});
  assert.equal(formatFileSize(null), '');
  assert.equal(formatFileSize(undefined), '');
  assert.equal(formatFileSize(0), '0 B');
  assert.equal(formatFileSize(1023), '1023 B');
  assert.equal(formatFileSize(1024), '1.0 KB');
  assert.equal(formatFileSize(1536), '1.5 KB');
  assert.equal(formatFileSize(1048576), '1.0 MB');
  assert.equal(formatFileSize(25 * 1048576 + 524288), '25.5 MB');
  assert.equal(formatFileSize(5 * 1073741824), '5.0 GB');
});

test('cardExtensionLabel names both files of a RAW+JPEG pair', () => {
  const {cardExtensionLabel} = load({'cards.js': ['cardExtensionLabel']});
  assert.equal(cardExtensionLabel({extension: '.nef'}), 'NEF');
  assert.equal(cardExtensionLabel({extension: '.NEF', companion_path: '_D854674.jpg'}), 'NEF + JPG');
  assert.equal(cardExtensionLabel({extension: '.dng', companion_path: 'a.b.JPEG'}), 'DNG + JPEG');
  // Same format twice is one label.
  assert.equal(cardExtensionLabel({extension: '.jpg', companion_path: 'x.JPG'}), 'JPG');
  // Only the basename's extension counts, and a leading-dot name has none.
  assert.equal(cardExtensionLabel({extension: '.nef', companion_path: '/archive.v1/sidecar'}), 'NEF');
  assert.equal(cardExtensionLabel({extension: '.nef', companion_path: 'C:\\a.b\\x.jpg'}), 'NEF + JPG');
  assert.equal(cardExtensionLabel({extension: '.nef', companion_path: '.hidden'}), 'NEF');
  assert.equal(cardExtensionLabel({extension: '.nef', companion_path: ''}), 'NEF');
  assert.equal(cardExtensionLabel({extension: '', companion_path: 'x.jpg'}), 'JPG');
  assert.equal(cardExtensionLabel({}), '');
});

test('keywordMatchScore ranks exact, prefix, word-prefix, then substring', () => {
  const {keywordMatchScore} = load({'keyword-autocomplete.js': ['keywordMatchScore']});
  assert.equal(keywordMatchScore('hawk', 'hawk'), 0);
  assert.equal(keywordMatchScore('hawk owl', 'hawk'), 1);
  assert.equal(keywordMatchScore('red-tailed hawk', 'hawk'), 2);
  assert.equal(keywordMatchScore('great_blue heron', 'blue'), 2);
  assert.equal(keywordMatchScore('places/yosemite', 'yose'), 2);
  assert.equal(keywordMatchScore('nighthawk', 'hawk'), 3 + 5 / 1000);
  assert.equal(keywordMatchScore('sparrow', 'hawk'), 99);
  // An earlier substring match outranks a later one.
  assert(keywordMatchScore('xhawk', 'hawk') < keywordMatchScore('nighthawk', 'hawk'));
  const ranked = ['nighthawk', 'red-tailed hawk', 'hawk owl', 'hawk', 'sparrow']
    .sort((a, b) => keywordMatchScore(a, 'hawk') - keywordMatchScore(b, 'hawk'));
  assert.deepEqual(ranked, ['hawk', 'hawk owl', 'red-tailed hawk', 'nighthawk', 'sparrow']);
});

test('browseStackCoverCompare prefers flagged, then quality, then sharpness', () => {
  const ctx = load({'stacks.js': ['browseStackFlagRank', 'browseStackCoverCompare']});
  assert.equal(ctx.browseStackFlagRank('flagged'), 2);
  assert.equal(ctx.browseStackFlagRank(null), 1);
  assert.equal(ctx.browseStackFlagRank('none'), 1);
  assert.equal(ctx.browseStackFlagRank('rejected'), 0);
  const cover = members => members.slice().sort(ctx.browseStackCoverCompare)[0].id;
  // Flag beats every score.
  assert.equal(cover([
    {id: 1, flag: 'none', quality_score: 0.99},
    {id: 2, flag: 'flagged', quality_score: 0.10},
    {id: 3, flag: 'rejected', quality_score: 1.00},
  ]), 2);
  // A rejected frame never covers an unflagged one.
  assert.equal(cover([
    {id: 4, flag: 'rejected', quality_score: 0.9},
    {id: 5, flag: null, quality_score: 0.1},
  ]), 5);
  // A missing score sorts below any present score.
  assert.equal(cover([
    {id: 6, flag: null, quality_score: null, subject_sharpness: 50},
    {id: 7, flag: null, quality_score: 0.01},
  ]), 7);
  // Equal quality falls through to subject sharpness, then overall sharpness.
  assert.equal(cover([
    {id: 8, quality_score: 0.5, subject_sharpness: 10, sharpness: 900},
    {id: 9, quality_score: 0.5, subject_sharpness: 20, sharpness: 1},
  ]), 9);
  // Any rating beats no rating, even a zero one; higher ratings win after that.
  assert.equal(cover([{id: 10, rating: null}, {id: 11, rating: 0}]), 11);
  assert.equal(cover([{id: 12, rating: 2}, {id: 13, rating: 4}]), 13);
  // Then resolution and file size; a full tie keeps the lowest id.
  assert.equal(cover([{id: 14, width: 10, height: 10}, {id: 15, width: 20, height: 10}]), 15);
  assert.equal(cover([{id: 16, file_size: 5}, {id: 17, file_size: 9}]), 17);
  assert.equal(cover([{id: 19}, {id: 18}]), 18);
});

test('asciiCaseFoldKey lowercases ASCII letters only', () => {
  const {asciiCaseFoldKey} = load({'prediction-panels.js': ['asciiCaseFoldKey']});
  assert.equal(asciiCaseFoldKey("Say's Phoebe"), "say's phoebe");
  assert.equal(asciiCaseFoldKey('ÉCLAIR'), 'Éclair');
  assert.equal(asciiCaseFoldKey(null), '');
  assert.equal(asciiCaseFoldKey(undefined), '');
  assert.equal(asciiCaseFoldKey(42), '42');
});

test('formatPredictionConfidence rounds to a percentage', () => {
  const {formatPredictionConfidence} = load({'prediction-panels.js': ['formatPredictionConfidence']});
  assert.equal(formatPredictionConfidence(null), 'confidence unknown');
  assert.equal(formatPredictionConfidence(undefined), 'confidence unknown');
  assert.equal(formatPredictionConfidence(0), '0%');
  assert.equal(formatPredictionConfidence(0.876), '88%');
  assert.equal(formatPredictionConfidence(1), '100%');
});

test('predictionIsDecided covers accepted, rejected and reviewed', () => {
  const {predictionIsDecided} = load({
    'prediction-panels.js': ['PREDICTION_DECIDED_STATUSES', 'predictionIsDecided'],
  });
  for (const status of ['accepted', 'rejected', 'reviewed']) {
    assert.equal(predictionIsDecided({status}), true, status);
  }
  assert.equal(predictionIsDecided({status: 'pending'}), false);
  assert.equal(predictionIsDecided({}), false);
  assert.equal(predictionIsDecided(null), false);
});

test('prediction ambiguity trusts the fresh effective_category over the stored one', () => {
  const ctx = load({'prediction-panels.js': ['predictionIsAmbiguous', 'predictionAmbiguityReason']});
  // Alternatives are always ambiguous.
  assert.equal(ctx.predictionIsAmbiguous({alternatives: [{}], effective_category: 'match'}), true);
  // A stale stored conflict is cleared by a fresh comparison that found none.
  assert.equal(ctx.predictionIsAmbiguous({category: 'disagreement', effective_category: 'new'}), false);
  assert.equal(ctx.predictionIsAmbiguous({category: 'new', effective_category: 'conflict'}), true);
  assert.equal(ctx.predictionIsAmbiguous({effective_category: 'broader'}), true);
  // Without a fresh comparison the stored category decides.
  assert.equal(ctx.predictionIsAmbiguous({category: 'disagreement'}), true);
  assert.equal(ctx.predictionIsAmbiguous({category: 'refinement'}), true);
  assert.equal(ctx.predictionIsAmbiguous({category: 'new'}), false);

  assert.equal(ctx.predictionAmbiguityReason({
    alternatives: [{}, {}], effective_category: 'conflict', existing_species: ['American Robin'],
  }), '2 alternatives · conflicts with keyworded American Robin');
  assert.equal(ctx.predictionAmbiguityReason({alternatives: [{}]}), '1 alternative');
  assert.equal(ctx.predictionAmbiguityReason({category: 'refinement'}),
    'refines an existing keyword');
  assert.equal(ctx.predictionAmbiguityReason({effective_category: 'broader', existing_species: ['A', 'B']}),
    'broader than keyworded A, B');
  assert.equal(ctx.predictionAmbiguityReason({category: 'disagreement', effective_category: 'new'}), '');
});

test('unlisted-run helpers index by detection and model and count detections', () => {
  const ctx = load({'prediction-panels.js': ['buildUnlistedRunIndex', 'unlistedGroupReason']});
  const index = ctx.buildUnlistedRunIndex({unlisted_runs: [
    {state: 'unlisted', detection_id: 7, classifier_model: 'bioclip'},
    {state: 'listed', detection_id: 8, classifier_model: 'bioclip'},
    {state: 'unlisted', detection_id: null, classifier_model: 'bioclip'},
    {state: 'unlisted', detection_id: 9},
  ]});
  assert.deepEqual(Object.keys(index).sort(), ['7|bioclip', '9|']);
  same(ctx.buildUnlistedRunIndex(null), {});

  assert.equal(ctx.unlistedGroupReason({
    unlisted: [{classifier_model: 'bioclip'}],
    detections: {7: true}, unlistedDetections: {7: true},
  }), 'This detection matched nothing in the label list bioclip ran — the species '
    + 'above is the closest available label, not a match.');
  assert.match(ctx.unlistedGroupReason({
    unlisted: [{classifier_model: 'a'}, {classifier_model: 'a'}, {model: 'b'}],
    detections: {1: 1, 2: 1, 3: 1}, unlistedDetections: {1: 1, 2: 1},
  }), /^2 of the 3 detections behind this row matched nothing in the label list a and b ran/);
  assert.match(ctx.unlistedGroupReason({
    unlisted: [{}], detections: {1: 1, 2: 1}, unlistedDetections: {1: 1, 2: 1},
  }), /^Every detection behind this row .* this model ran/);
});

test('formatShiftMinutes signs the offset and names whole hours', () => {
  const {formatShiftMinutes} = load({'batch-actions.js': ['formatShiftMinutes']});
  assert.equal(formatShiftMinutes(60), '+60 minutes (+1 hours)');
  assert.equal(formatShiftMinutes(-120), '-120 minutes (-2 hours)');
  assert.equal(formatShiftMinutes(-90), '-90 minutes');
  assert.equal(formatShiftMinutes(45), '+45 minutes');
  assert.equal(formatShiftMinutes(0), '+0 minutes (+0 hours)');
});

test('formatCoordinatePair prints five decimals and needs both halves', () => {
  const {formatCoordinatePair} = load({'location.js': ['formatCoordinatePair']});
  assert.equal(formatCoordinatePair(40.7127753, -74.0059728), '40.71278, -74.00597');
  assert.equal(formatCoordinatePair('1.5', '2'), '1.50000, 2.00000');
  assert.equal(formatCoordinatePair(0, 0), '0.00000, 0.00000');
  assert.equal(formatCoordinatePair(null, 1), '');
  assert.equal(formatCoordinatePair(1, undefined), '');
});

test('normalizeGooglePlaceForSubmit flattens a Places result', () => {
  const {normalizeGooglePlaceForSubmit} = load({'location.js': ['normalizeGooglePlaceForSubmit']});
  same(normalizeGooglePlaceForSubmit({
    place_id: 'abc',
    formatted_address: 'Point Reyes, CA',
    types: ['park'],
    geometry: {location: {lat: () => 38.07, lng: () => -122.88}},
    address_components: [{long_name: 'California', short_name: 'CA', types: ['administrative_area_level_1']}, {}],
  }), {
    place_id: 'abc', name: 'Point Reyes, CA', types: ['park'], lat: 38.07, lng: -122.88,
    address_components: [
      {name: 'California', short_name: 'CA', types: ['administrative_area_level_1']},
      {name: '', short_name: '', types: []},
    ],
  });
  // Plain-number coordinates (the new Places API shape) work too.
  same(normalizeGooglePlaceForSubmit({place_id: 'p', name: 'N', geometry: {location: {lat: 1, lng: 2}}}),
    {place_id: 'p', name: 'N', types: [], lat: 1, lng: 2, address_components: []});
  assert.equal(normalizeGooglePlaceForSubmit(null), null);
  assert.equal(normalizeGooglePlaceForSubmit({name: 'no id'}), null);
  assert.equal(normalizeGooglePlaceForSubmit({place_id: 'p'}), null);
  assert.equal(normalizeGooglePlaceForSubmit({place_id: 'p', geometry: {location: {lat: 1, lng: null}}}), null);
});

test('exportExtensionForFormat defaults to jpg', () => {
  const {exportExtensionForFormat} = load({'export.js': ['exportExtensionForFormat']});
  assert.equal(exportExtensionForFormat('png'), 'png');
  assert.equal(exportExtensionForFormat('tiff'), 'tiff');
  assert.equal(exportExtensionForFormat('jpeg'), 'jpg');
  assert.equal(exportExtensionForFormat(undefined), 'jpg');
});

test('best-batch labels and score text', () => {
  const ctx = load({'best-batch.js': ['bestBatchRoleLabel', 'bestBatchScoreText']});
  assert.equal(ctx.bestBatchRoleLabel('best'), 'Best');
  assert.equal(ctx.bestBatchRoleLabel('alternate'), 'Alt');
  assert.equal(ctx.bestBatchRoleLabel('reject'), 'Reject');
  assert.equal(ctx.bestBatchScoreText({quality_pct: 87, focus: 0.456, sharpness: 312.6}), 'Q 87 · F 46 · S 313');
  assert.equal(ctx.bestBatchScoreText({quality_pct: 0}), 'Q 0');
  assert.equal(ctx.bestBatchScoreText({}), 'No score');
});

test('selectionIdsKey is order-independent and leaves its input alone', () => {
  const {selectionIdsKey} = load({'selection.js': ['selectionIdsKey']});
  const ids = [10, 2, 33];
  assert.equal(selectionIdsKey(ids), '2,10,33');
  assert.equal(selectionIdsKey([33, 10, 2]), selectionIdsKey(ids));
  assert.deepEqual(ids, [10, 2, 33]);
  assert.equal(selectionIdsKey([]), '');
});

test('browseFocusCandidateChunks keeps each focused lookup under the server cap', () => {
  const ctx = load({
    'state.js': ['BROWSE_MAX_FOCUS_CANDIDATES'],
    'loading.js': ['browseFocusCandidateChunks'],
  });
  assert.equal(ctx.BROWSE_MAX_FOCUS_CANDIDATES, 200);
  same(ctx.browseFocusCandidateChunks(null, true), []);
  same(ctx.browseFocusCandidateChunks({photoId: 5}, false), []);
  same(ctx.browseFocusCandidateChunks({photoId: 5}, true), [[5]]);
  // The anchor leads; stack members follow once, without nulls or a repeat.
  same(ctx.browseFocusCandidateChunks(
    {photoId: 5, stackSelection: true, stackIds: [7, 5, null, 8]}, true), [[5, 7, 8]]);
  const stackIds = Array.from({length: 450}, (_, i) => i + 1000);
  const chunks = ctx.browseFocusCandidateChunks({photoId: 1, stackSelection: true, stackIds}, true);
  same(chunks.map(c => c.length), [200, 200, 51]);
  assert.equal(chunks[0][0], 1);
  assert.equal(chunks[2][50], 1449);
});

test('normalizeCollectionRules accepts every stored rule shape', () => {
  const {normalizeCollectionRules} = load({'collection-editor.js': ['normalizeCollectionRules']});
  const leaf = {field: 'rating', op: '>=', value: 3};
  same(normalizeCollectionRules([leaf]), {mode: 'all', rules: [leaf]});
  same(normalizeCollectionRules(JSON.stringify([leaf])), {mode: 'all', rules: [leaf]});
  same(normalizeCollectionRules({mode: 'any', rules: [leaf]}), {mode: 'any', rules: [leaf]});
  same(normalizeCollectionRules({mode: 'bogus', rules: []}), {mode: 'all', rules: []});
  same(normalizeCollectionRules('{not json'), {mode: 'all', rules: []});
  same(normalizeCollectionRules(null), {mode: 'all', rules: []});
  same(normalizeCollectionRules({rules: 'nope'}), {mode: 'all', rules: []});
});

test('walkRuleTree visits every leaf through nested groups', () => {
  const {walkRuleTree} = load({'collection-editor.js': ['walkRuleTree']});
  const seen = [];
  walkRuleTree({mode: 'all', rules: [
    {field: 'a'},
    {mode: 'any', rules: [{field: 'b'}, {mode: 'none', rules: [{field: 'c'}]}]},
    [{field: 'd'}],
  ]}, leaf => seen.push(leaf.field));
  assert.deepEqual(seen, ['a', 'b', 'c', 'd']);
  walkRuleTree(null, () => assert.fail('visited a null tree'));
});

test('coerceRuleForSave types each value the way the backend expects', () => {
  const ctx = load({'collection-editor.js': ['NUMERIC_RULE_FIELDS', 'coerceRuleForSave']});
  const save = rule => JSON.parse(JSON.stringify(ctx.coerceRuleForSave(rule)));
  const numeric = ctx.NUMERIC_RULE_FIELDS[0];
  assert.deepEqual(save({field: numeric, op: '>=', value: '2.5'}), {field: numeric, op: '>=', value: 2.5});
  assert.deepEqual(save({field: numeric, op: '>=', value: 'x'}), {field: numeric, op: '>=', value: 0});
  // A numeric ``between`` keeps both ends instead of collapsing to one scalar.
  assert.deepEqual(save({field: numeric, op: 'between', value: ['1', '4']}),
    {field: numeric, op: 'between', value: [1, 4]});
  assert.deepEqual(save({field: numeric, op: 'between', value: '3'}),
    {field: numeric, op: 'between', value: [3, 3]});
  assert.deepEqual(save({field: 'has_gps', op: 'is', value: 'true'}), {field: 'has_gps', op: 'is', value: 1});
  assert.deepEqual(save({field: 'has_gps', op: 'is', value: 'no'}), {field: 'has_gps', op: 'is', value: 0});
  assert.deepEqual(save({field: 'timestamp', op: 'recent_days', value: '14'}),
    {field: 'timestamp', op: 'recent_days', value: 14});
  assert.deepEqual(save({field: 'timestamp', op: 'between', value: ['2024-01-01']}),
    {field: 'timestamp', op: 'between', value: ['2024-01-01', '']});
  assert.deepEqual(save({field: 'keyword', op: 'contains', value: 'hawk'}),
    {field: 'keyword', op: 'contains', value: 'hawk'});
  // Groups recurse and fall back to "all" for an unknown mode.
  assert.deepEqual(save({mode: 'weird', rules: [{field: 'has_gps', op: 'is', value: true}]}),
    {mode: 'all', rules: [{field: 'has_gps', op: 'is', value: 1}]});
});

test('folderRowsContainScope walks parents and survives cycles', () => {
  const {folderRowsContainScope} = load({'folder-health.js': ['folderRowsContainScope']});
  const rows = [
    {id: 1, parent_id: null}, {id: 2, parent_id: 1}, {id: 3, parent_id: 2},
    {id: 8, parent_id: 9}, {id: 9, parent_id: 8},
  ];
  assert.equal(folderRowsContainScope(rows, 3, 1), true);
  assert.equal(folderRowsContainScope(rows, 3, 3), true);
  assert.equal(folderRowsContainScope(rows, 1, 3), false);
  assert.equal(folderRowsContainScope(rows, 8, 1), false);
  assert.equal(folderRowsContainScope(null, 4, 4), true);
  assert.equal(folderRowsContainScope(null, 4, 5), false);
});

test('_batchUnanimous reports a shared value only when every id agrees', () => {
  const {_batchUnanimous} = load({'edit-actions.js': ['_batchUnanimous']});
  const rating = {1: 3, 2: 3, 3: 5};
  same(_batchUnanimous([1, 2], id => rating[id]), {unanimous: true, value: 3});
  same(_batchUnanimous([1, 2, 3], id => rating[id]), {unanimous: false, value: null});
  same(_batchUnanimous([], id => rating[id]), {unanimous: false, value: null});
  // Photos with no value yet agree on having none.
  const unset = _batchUnanimous([4, 5], id => rating[id]);
  assert.equal(unset.unanimous, true);
  assert.equal(unset.value, undefined);
});

let failed = 0;
for (const [name, body] of tests) {
  try {
    body();
  } catch (err) {
    failed++;
    console.error('FAIL ' + name + '\n' + (err && err.stack || err));
  }
}
console.log((tests.length - failed) + '/' + tests.length + ' browse helper tests passed');
if (failed) process.exitCode = 1;
