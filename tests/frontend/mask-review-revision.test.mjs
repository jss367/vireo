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
  return {byId, writes, reloads, state, render: card => context.renderMaskReview(card)};
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
