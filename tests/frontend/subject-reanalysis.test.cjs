// Exercise the lightbox's real retry handler without running model inference.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');
const {parse} = require('acorn');

const source = readFileSync('vireo/static/navbar-lightbox-subjects.js', 'utf8');
const tree = parse(source, {ecmaVersion: 'latest', sourceType: 'module'});
const initializer = tree.body.find(node => node.type === 'ExpressionStatement').expression.callee;
const analyze = initializer.body.body.find(node => node.type === 'FunctionDeclaration' && node.id.name === 'analyze');

function harness(outcome, navigate = false) {
  const requests = [], warnings = [], loaded = [], refreshed = [], events = [];
  const context = vm.createContext({
    data: {photo_id: 230871, workspace_id: 5}, generation: 1, busy: false,
    status: {}, render() {}, _lbGuardReadOnly: () => false,
    vireoLightboxSession: {requestedPhotoId: () => context.currentPhoto}, currentPhoto: 230871,
    safeFetch: async (url, options = {}) => {
      requests.push({url, options});
      if (url === '/api/photos/analysis-retry') return {job_id: 'one-photo'};
      assert.equal(url, '/api/jobs/one-photo');
      if (navigate) { context.generation++; context.currentPhoto = 999; }
      return {status: 'completed', result: {photos: [outcome]}};
    },
    load: async id => { context.generation++; loaded.push(id); },
    _lbResetMaskOverlay: () => refreshed.push('mask'),
    _lbLoadDetections: id => refreshed.push(id), _lbLoadMaskVariants() {},
    showToast: (...args) => warnings.push(args),
    document: {dispatchEvent: event => events.push(event.type)},
    CustomEvent: class { constructor(type) { this.type = type; } },
  });
  vm.runInContext(source.slice(analyze.start, analyze.end), context);
  return {run: () => context.analyze(3422044149613778), requests, warnings, loaded, refreshed, events};
}

test('retry rebuilds full analysis for exactly the current photo, workspace, and clicked subject', async () => {
  const h = harness({status: 'complete'});
  await h.run();
  assert.equal(h.requests[0].url, '/api/photos/analysis-retry');
  assert.deepEqual(JSON.parse(h.requests[0].options.body), {
    photo_ids: [230871], expected_workspace_id: 5, expected_primary_detection_id: 3422044149613778,
  });
  assert.deepEqual(h.loaded, [230871]);
  assert.deepEqual(h.refreshed, ['mask', 230871]);
  assert.deepEqual(h.events, ['vireo:primary-subject-changed']);
  assert.deepEqual(h.warnings, []);
});

for (const outcome of [{status: 'review'}, {status: 'failed', error: 'Mask unavailable'}]) {
  test('a completed job with photo outcome ' + outcome.status + ' still warns', async () => {
    const h = harness(outcome);
    await h.run();
    assert.equal(h.warnings.length, 1);
    assert.equal(h.warnings[0][1], 'warning');
    if (outcome.error) assert.equal(h.warnings[0][0], outcome.error);
  });
}

test('a retry finishing after navigation cannot reload or warn on the newly opened photo', async () => {
  const h = harness({status: 'failed', error: 'Previous photo failed'}, true);
  await h.run();
  assert.deepEqual(h.loaded, []);
  assert.deepEqual(h.refreshed, []);
  assert.deepEqual(h.warnings, []);
});
