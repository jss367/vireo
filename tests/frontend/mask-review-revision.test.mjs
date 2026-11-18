import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

function harness() {
  const nodes = new Map(), writes = [], reloads = [];
  const byId = id => {
    if (!nodes.has(id)) nodes.set(id, {handlers: {}, disabled: false,
      replaceChildren() {}, setAttribute() {}, append() {},
      addEventListener(name, handler) { this.handlers[name] = handler; }});
    return nodes.get(id);
  };
  const state = {inspectedId: 1, bestBatchData: {comparison: {}}};
  const context = vm.createContext({byId, state, AbortController, Error,
    fetchJson: (url, options) => new Promise((resolve, reject) => writes.push({url, body: JSON.parse(options.body), resolve, reject})),
    loadBatch: async () => reloads.push(true),
  });
  vm.runInContext(moduleAsScript(readFileSync('vireo/static/best-batch/subjects.js', 'utf8')), context);
  context.renderMaskReview({id: 1, subject: {detection_id: 2, analysis_key: 'key'},
    mask_review: {verdict: 'unreviewed', revision: 7, mask_digest: 'pixels', history: []}});
  return {byId, writes, reloads, state, render: card => context.renderMaskReview(card),
    analyze: photoId => context.analyzeSubjects(photoId), setFetch: callback => { context.fetchJson = callback; }};
}

test('mask review sends its displayed revision and blocks opposing/repeated clicks until refreshed', async () => {
  const {byId, writes, reloads} = harness();
  const pending = byId('invalidMask').handlers.click();
  assert.equal(byId('validMask').disabled, true);
  assert.equal(byId('invalidMask').disabled, true);
  await byId('validMask').handlers.click();
  await byId('invalidMask').handlers.click();
  assert.equal(writes.length, 1);
  assert.equal(writes[0].body.expected_revision, 7);
  writes[0].resolve({verdict: 'invalid', revision: 8});
  await pending;
  assert.equal(reloads.length, 1);
});

for (const navigate of [false, true]) {
  test(`a failed or stale review stays read-only without overwriting another card (${navigate})`, async () => {
    const {byId, writes, reloads, state} = harness();
    const pending = byId('validMask').handlers.click();
    if (navigate) state.inspectedId = 3;
    writes[0].reject(new Error('Mask review changed.'));
    await pending;
    assert.equal(reloads.length, 0);
    assert.equal(byId('validMask').disabled, true);
    assert.equal(byId('invalidMask').disabled, true);
    if (navigate) assert.ok(!byId('maskVerdict').textContent.includes('not confirmed'));
    else assert.match(byId('maskVerdict').textContent, /Reload the comparison/);
  });
}


test('a late failure cannot replace the status of a freshly rendered review of the same photo', async () => {
  const {byId, writes, render} = harness();
  const pending = byId('validMask').handlers.click();
  render({id: 1, subject: {detection_id: 2, analysis_key: 'key'},
    mask_review: {verdict: 'valid', revision: 8, mask_digest: 'pixels', history: []}});
  const refreshed = byId('maskVerdict').textContent;
  writes[0].reject(new Error('delayed old failure'));
  await pending;
  assert.equal(byId('maskVerdict').textContent, refreshed);
  assert.equal(byId('invalidMask').disabled, false);
});

for (const refined of [false, true]) {
  test(`analysis preserves missing-only bulk work and a newly refined single subject (${refined})`, async () => {
    const {state, analyze, setFetch, reloads} = harness();
    state.requestSequence = 0;
    state.subjectOverrides = {'1': 99};
    state.bestBatchData = {photo_ids: [1, 2, 3], comparison: {photo_ids: [1, 2, 3],
      subject_overrides: {'1': 10, '2': 20, '3': 30}}, cards: [
      {id: 1, subject: {detection_id: 10}, evidence_status: 'ready'},
      {id: 2, subject: {detection_id: 20}, evidence_status: 'subject_analysis_missing'},
      {id: 3, subject: {detection_id: 30}, evidence_status: 'ready'},
    ]};
    const requests = [];
    setFetch(async (url, options) => {
      if (options) {
        requests.push(JSON.parse(options.body));
        return {job_id: 'synthetic'};
      }
      return {status: 'completed', result: {analyzed: 1}};
    });
    await analyze(refined ? 1 : undefined);
    assert.equal(requests.length, 1);
    assert.deepEqual(requests[0].photo_ids, refined ? [1] : [2]);
    assert.deepEqual(requests[0].subject_overrides, refined ? {'1': 99} : {'2': 20});
    assert.equal(reloads.length, 1);
  });
}
