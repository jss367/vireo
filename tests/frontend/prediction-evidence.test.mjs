import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function harness() {
  const ctx = vm.createContext({window: {}, Set, Map, Number, JSON, console,
    predictions: [], allPredictions: [], currentModel: 'all', currentTab: 'all',
    currentSort: 'filename', minConfidence: 0, currentLabelsFingerprint: null,
    _predictionsReloading: false, _loadPredictionsEpoch: 1,
    _predictionEpochStale: epoch => epoch !== ctx._loadPredictionsEpoch,
    renderAll() {}, reviewQueue: {workspace: 2},
    reviewErrorMessage: (error, fallback) => error?.message || fallback,
    showToast() {},
  });
  for (const file of ['cards', 'evidence', 'grid', 'decisions']) {
    vm.runInContext(readFileSync(`vireo/static/review/${file}.js`, 'utf8'), ctx);
  }
  return ctx;
}

function prediction(id, detection, decision = 'unreviewed', group_id = null) {
  return {id, detection_id: detection, photo_id: 1, status: 'pending', filename: 'DSC_2542.NEF',
    confidence: .9994, group_id, evidence: {subject: {decision, revision: 0}, reasons: ['High relative score — audit']}};
}

test('relative score preserves the reported plover value instead of displaying 100% confidence', () => {
  const ctx = harness();
  assert.equal(ctx.reviewRelativeScore(.9994), '99.94');
  assert.equal(ctx.reviewRelativeScore(null), 'unknown');
  assert.equal(ctx.reviewRelativeScore(NaN), 'unknown');
});

test('questionable queue keeps separate animals, merges models on one region, and retains burst members', () => {
  const ctx = harness();
  ctx.predictions = [prediction(1, 10), prediction(2, 10), prediction(3, 11)];
  ctx.reviewEvidenceFilter = 'questionable';
  assert.deepEqual(Array.from(ctx.getVisibleItems(), p => p.id), [1, 3]);
  assert.deepEqual(Array.from(ctx.getVisibleItems(true), p => p.id), [1, 2, 3]);
  ctx.predictions[0].evidence.reasons = [];
  assert.deepEqual(Array.from(ctx.getVisibleItems(), p => p.id), [2, 3]);
});

test('deferred subjects and every affected burst are excluded from Accept All and direct accepts', async () => {
  const ctx = harness();
  ctx.predictions = [prediction(1, 10, 'not_enough_evidence', 'burst'),
    prediction(2, 11, 'unreviewed', 'burst'), prediction(3, 12), prediction(4, 13, 'invalid_subject')];
  assert.deepEqual(Array.from(ctx.visiblePendingCards(), p => p.id), [3]);
  let sent = 0;
  ctx.safeFetch = async () => {sent++;};
  await ctx.acceptPrediction(1);
  await ctx.acceptPrediction(2);
  await ctx.acceptAlternative(15, 1);
  assert.equal(sent, 0);
  ctx.reviewEvidenceFilter = 'deferred';
  assert.deepEqual(Array.from(ctx.getVisibleItems(), p => p.id), [1, 4]);
});

test('saving uncertainty carries observed workspace and revision, double clicks send once', async () => {
  const ctx = harness();
  ctx.predictions = [prediction(1, 10)];
  let done, sent, reloaded = 0, lastRenderedSaving = false;
  ctx.renderAll = () => {lastRenderedSaving = ctx.reviewEvidenceSaving.has(10);};
  ctx.safeFetch = async (url, options) => {sent = {url, body: JSON.parse(options.body)};return new Promise(resolve => {done = resolve;});};
  ctx.loadPredictions = async () => {reloaded++;ctx._loadPredictionsEpoch++;};
  const pending = ctx.saveReviewEvidence(1, 'not_enough_evidence');
  assert.deepEqual(Array.from(ctx.visiblePendingCards(), p => p.id), []);
  await ctx.saveReviewEvidence(1, 'invalid_subject');
  assert.deepEqual(sent, {url:'/api/detections/10/evidence-review', body:{decision:'not_enough_evidence',observed_revision:0,observed_workspace_id:2}});
  done({ok:true,decision:'not_enough_evidence',revision:1});
  await pending;
  assert.equal(reloaded, 1);
  assert.equal(ctx.reviewEvidenceSaving.size, 0);
  assert.equal(lastRenderedSaving, false, 'render after releasing the save guard, even after a reload advances the epoch');
});

test('a stale save response cannot overwrite the newly loaded scope', async () => {
  const ctx = harness();
  ctx.predictions = [prediction(1, 10)];
  let done, reloaded = 0;
  ctx.safeFetch = async () => new Promise(resolve => {done = resolve;});
  ctx.loadPredictions = async () => {reloaded++;};
  const pending = ctx.saveReviewEvidence(1, 'not_enough_evidence');
  ctx._loadPredictionsEpoch++;
  done({ok:true,decision:'not_enough_evidence',revision:1});
  await pending;
  assert.equal(reloaded, 0);
});

test('rejecting a region posts the verdict, reloads the queue once, and blocks a double click', async () => {
  const ctx = harness();
  ctx.predictions = [prediction(1, 10)];
  let done, sent = [], reloaded = 0, toasts = [];
  ctx.safeFetch = async (url, options) => {sent.push({url, body: JSON.parse(options.body)});return new Promise(resolve => {done = resolve;});};
  ctx.loadPredictions = async () => {reloaded++;ctx._loadPredictionsEpoch++;};
  ctx.showToast = (message, kind) => {toasts.push([message, kind]);};
  const pending = ctx.rejectReviewRegion(1);
  assert.equal(ctx.reviewEvidenceSaving.has(10), true);
  await ctx.rejectReviewRegion(1);
  assert.deepEqual(sent, [{url: '/api/detections/10/subject-verdict', body: {verdict: 'not_animal'}}]);
  done({ok: true, changed: true, verdict: 'not_animal'});
  await pending;
  assert.equal(reloaded, 1);
  assert.equal(ctx.reviewEvidenceSaving.size, 0);
  assert.deepEqual(toasts, [['Region rejected as not an animal. Undo restores it.', 'success']]);
  // The evidence buttons route the region action to the verdict, not to evidence-review.
  const full = prediction(1, 10);
  Object.assign(full.evidence, {
    score: {model: 'bioclip', explanation: 'relative', raw_value: null, raw_kind: null, label_count: 3},
    match: {explanation: 'listed'}, geography: {explanation: 'unknown'},
    subject: {decision: 'unreviewed', revision: 0, detector_model: 'megadetector-v6', detector_score: .9, category: 'animal', explanation: 'box'},
  });
  ctx.escapeHtml = text => String(text);
  ctx.escapeAttr = text => String(text);
  const html = ctx.reviewEvidenceHtml(full);
  assert.match(html, /data-evidence-decision="reject_region"[^>]*>Not an animal: reject region/);
  assert.doesNotMatch(html, /data-evidence-decision="invalid_subject"/);
});

test('a rejection the server did not confirm is reported and keeps the queue', async () => {
  const ctx = harness();
  ctx.predictions = [prediction(1, 10)];
  let reloaded = 0, toasts = [];
  ctx.safeFetch = async () => ({ok: false});
  ctx.loadPredictions = async () => {reloaded++;};
  ctx.showToast = (message, kind) => {toasts.push([message, kind]);};
  await ctx.rejectReviewRegion(1);
  assert.equal(reloaded, 0);
  assert.equal(toasts.length, 1);
  assert.equal(toasts[0][1], 'error');
  assert.match(toasts[0][0], /^Region was not rejected: /);
});
