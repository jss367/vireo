import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function harness() {
  const ctx = vm.createContext({window: {}, Set, Map, JSON, Number,
    predictions: [], allPredictions: [], currentModel: 'all', currentTab: 'pending',
    currentSort: 'confidence_desc', minConfidence: 0, currentLabelsFingerprint: null,
    _predictionsReloading: false, _loadPredictionsEpoch: 1,
    _predictionEpochStale: epoch => epoch !== ctx._loadPredictionsEpoch,
    renderAll() {}, reviewQueue: {workspace: 1},
    escapeHtml: value => String(value).replaceAll('<', '&lt;'),
    escapeAttr: value => String(value),
    Vireo: {predictions: {decidedPredictionIds: applied => applied.prediction_ids || []}},
  });
  for (const file of ['cards', 'evidence', 'grid', 'decisions']) {
    vm.runInContext(readFileSync(`vireo/static/review/${file}.js`, 'utf8'), ctx);
  }
  ctx.predictions = [
    row(1, 10, 'bio', 'bio', .95), row(2, 11, 'bio', 'bio', .6),
    row(3, 11, 'inat', 'inat', .9), row(4, 12, 'inat', 'inat', .85),
    row(5, 12, 'third', null, .8),
  ];
  ctx.allPredictions = ctx.predictions;
  return ctx;
}

function row(id, photo, model, group, confidence) {
  return {id, photo_id: photo, detection_id: photo, model, group_id: group, confidence,
    filename: `${photo}.jpg`, status: 'pending', species: 'Blue Tit',
    consensus_species: 'Blue Tit', consensus_species_key: 'taxon:1',
    labels_fingerprint: model, card_id: 'merged-blue', vote_count: 2, total_votes: 2};
}

const ids = rows => Array.from(rows, p => p.id).sort();

test('one merged card exposes every model and counts distinct photos and cards', () => {
  const ctx = harness();
  assert.equal(ctx.getVisibleItems().length, 1);
  const pred = ctx.predictions[0];
  assert.deepEqual(ids(ctx.reviewCardMembers(pred)), [1, 2, 3, 4, 5]);
  assert.equal(ctx.reviewCardPhotoCount(pred), 3);
  assert.equal(ctx.reviewCardCounts().pending, 1);
  const html = ctx.reviewCardModelsHtml(pred);
  for (const name of ['bio', 'inat', 'third']) assert.ok(html.includes(name));
  assert.ok(html.includes('2/2 votes'));
  assert.ok(html.includes('95.00% best relative score'));
  assert.match(ctx.reviewPredictionActions(pred, 'Blue Tit'), /Tag 3 photos/);
  assert.match(ctx.groupMembersHtml(pred), /data-group-id="bio"/);
  assert.match(ctx.groupMembersHtml(pred), /data-group-id="inat"/);
});

test('model and fingerprint filters retain per-model cards and exact request scope', () => {
  const ctx = harness();
  ctx.currentModel = 'inat';
  assert.deepEqual(ids(ctx.getVisibleItems()), [3]);
  assert.deepEqual(JSON.parse(ctx.reviewCardRequest(ctx.predictions[2]).body), {
    review_prediction_ids: [3, 4], observed_workspace_id: 1,
  });
  ctx.currentModel = 'all';
  ctx.currentLabelsFingerprint = 'third';
  assert.deepEqual(ids(ctx.getVisibleItems()), [5]);
  assert.deepEqual(JSON.parse(ctx.reviewCardRequest(ctx.predictions[4]).body).review_prediction_ids, [5]);
});

test('model chips use evidence for the consensus species and never borrow a minority score', () => {
  const ctx = harness();
  Object.assign(ctx.predictions[0], {species: 'Other bird', species_key: 'taxon:other', confidence: .99});
  ctx.predictions[1].species_key = 'taxon:1';
  let html = ctx.reviewCardModelsHtml(ctx.predictions[0]);
  assert.ok(html.includes('bio · 60.00% best relative score'));
  assert.equal(html.includes('99.00%'), false);
  ctx.predictions[1].species_key = 'taxon:other';
  html = ctx.reviewCardModelsHtml(ctx.predictions[0]);
  assert.ok(html.includes('bio · Relative score unavailable for this species'));
});

test('hiding a connecting frame splits cards instead of submitting a disconnected selection', () => {
  const ctx = harness();
  // Remove both models on the connecting photo through the confidence filter.
  ctx.predictions[2].confidence = .6;
  ctx.minConfidence = .75;
  assert.deepEqual(ids(ctx.getVisibleItems()), [1, 4]);
  assert.deepEqual(ids(ctx.reviewCardMembers(ctx.predictions[0])), [1]);
  assert.deepEqual(ids(ctx.reviewCardMembers(ctx.predictions[3])), [4, 5]);
});

test('different species, disjoint cards and previous decisions stay separate', () => {
  const ctx = harness();
  const other = {...row(6, 11, 'other', null, .7), card_id: 'other-species', species: 'Other', consensus_species_key: 'taxon:2'};
  const disjoint = {...row(7, 13, 'bio', 'far', .7), card_id: 'disjoint-blue'};
  const decided = {...row(8, 10, 'old', null, .7), card_id: 'accepted-blue', status: 'accepted'};
  ctx.predictions.push(other, disjoint, decided);
  assert.equal(ctx.getVisibleItems().length, 3);
  assert.equal(ctx.reviewCardCounts().accepted, 1);
  assert.equal(ctx.reviewCardCounts().pending, 3);
  ctx.currentTab = 'all';
  assert.equal(ctx.getVisibleItems().length, 4);
});

test('a deferred member in another model blocks the entire merged card', () => {
  const ctx = harness();
  ctx.predictions[4].evidence = {subject: {decision: 'not_enough_evidence'}};
  assert.equal(ctx.reviewEvidenceBlocksAccept(1), true);
  assert.equal(ctx.visiblePendingCards().length, 0);
  ctx.currentModel = 'bio';
  assert.equal(ctx.reviewEvidenceBlocksAccept(1), false, 'a filtered action does not reach the deferred model');
});

test('accept and reject submit all members and consume the confirmed IDs', async () => {
  for (const decision of ['accept', 'reject']) {
    const ctx = harness();
    let sent;
    ctx.safeFetch = async (url, options) => {
      sent = {url, body: JSON.parse(options.body)};
      return {[decision === 'accept' ? 'prediction_ids' : 'rejected_prediction_ids']: [1, 2, 3, 4, 5]};
    };
    await ctx[decision === 'accept' ? 'acceptPrediction' : 'rejectPrediction'](1);
    assert.equal(sent.url, `/api/predictions/1/${decision}`);
    assert.deepEqual(sent.body.review_prediction_ids.sort(), [1, 2, 3, 4, 5]);
    assert.equal(ctx.predictions.every(p => p.status === (decision === 'accept' ? 'accepted' : 'rejected')), true);
  }
});

test('Accept All visits a merged card once and continues to unrelated cards', async () => {
  const ctx = harness();
  ctx.predictions.push({...row(6, 13, 'bio', null, .7), card_id: 'other-blue'});
  ctx.Vireo = {predictions: {groupedDecisionTracker() {
    const done = new Set();
    return {alreadyDecided: id => done.has(id), record: applied => {
      applied.prediction_ids.forEach(id => done.add(id));
      return applied.prediction_ids;
    }};
  }}};
  const sent = [];
  ctx.safeFetch = async (url, options) => {
    const ids = JSON.parse(options.body).review_prediction_ids;
    sent.push(ids);
    return {prediction_ids: ids};
  };
  await ctx.acceptAllPending();
  assert.equal(sent.length, 2);
  assert.deepEqual(sent.flat().sort(), [1, 2, 3, 4, 5, 6]);
});

test('Accept All stops when a local model filter changes during a request', async () => {
  const ctx = harness();
  ctx.predictions.push({...row(6, 13, 'bio', null, .7), card_id: 'other-blue'});
  ctx.Vireo = {predictions: {groupedDecisionTracker: () => ({
    alreadyDecided: () => false, record: applied => applied.prediction_ids,
  })}};
  let calls = 0;
  ctx.safeFetch = async () => {
    calls++;
    ctx.currentModel = 'bio';
    return {prediction_ids: [1, 2, 3, 4, 5]};
  };
  await ctx.acceptAllPending();
  assert.equal(calls, 1);
  assert.equal(ctx.predictions[5].status, 'pending');
});
