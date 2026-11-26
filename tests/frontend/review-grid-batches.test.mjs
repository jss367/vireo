import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

// The Review grid builds its cards in batches, and one render shares the
// filtered rows. These run the page's own scripts against a minimal DOM.

function element() {
  return {innerHTML: '', textContent: '', hidden: false, style: {setProperty() {}},
    insertAdjacentHTML(_where, html) { this.innerHTML += html; },
    addEventListener() {}};
}

function harness(count) {
  const elements = {};
  const ctx = vm.createContext({window: {}, Set, Map, JSON, Number, Math, Object, Array,
    document: {getElementById: id => elements[id] || (elements[id] = element())},
    predictions: [], allPredictions: [], currentModel: 'all', currentTab: 'all',
    currentCollection: 'all', currentPhotoIdFilter: null, thumbSize: 400,
    currentSort: 'filename', minConfidence: 0, currentLabelsFingerprint: null,
    _predictionsReloading: false, _loadPredictionsEpoch: 1,
    _predictionEpochStale: epoch => epoch !== ctx._loadPredictionsEpoch,
    renderAll() {}, reviewQueue: {workspace: 1}, reviewDetectionBoxesVisible: true,
    escapeHtml: value => String(value).replaceAll('<', '&lt;'),
    escapeAttr: value => String(value),
    Vireo: {predictions: {decidedPredictionIds: applied => applied.prediction_ids || []}},
  });
  for (const file of ['cards', 'evidence', 'grid', 'decisions']) {
    vm.runInContext(readFileSync(`vireo/static/review/${file}.js`, 'utf8'), ctx);
  }
  ctx.predictions = Array.from({length: count}, (_, i) => row(i + 1));
  ctx.allPredictions = ctx.predictions;
  return {ctx, elements};
}

function row(id) {
  const name = String(id).padStart(4, '0');
  return {id, photo_id: 1000 + id, detection_id: 2000 + id, model: 'bio', group_id: null,
    confidence: .9, filename: `${name}.jpg`, status: 'pending', species: 'Blue Tit',
    consensus_species: 'Blue Tit', consensus_species_key: 'taxon:1',
    labels_fingerprint: 'bio', card_id: `card-${id}`};
}

const built = el => (el.innerHTML.match(/data-pred-id="/g) || []).length;

test('the grid builds one batch, says how many cards it shows, and adds a batch on request', () => {
  const {ctx, elements} = harness(150);
  ctx.renderGrid();
  assert.equal(built(elements.grid), 60);
  assert.equal(elements.gridMore.hidden, false);
  assert.equal(elements.gridMoreStatus.textContent, 'Showing 60 of 150 cards. More appear as you scroll.');
  assert.equal(elements.gridMoreBtn.textContent, 'Show 60 more');
  ctx.showMoreReviewCards();
  assert.equal(built(elements.grid), 120);
  assert.equal(elements.gridMoreBtn.textContent, 'Show 30 more');
  ctx.showMoreReviewCards();
  assert.equal(built(elements.grid), 150);
  assert.equal(elements.gridMore.hidden, true);
  ctx.showMoreReviewCards();
  assert.equal(built(elements.grid), 150, 'nothing is left to add');
});

test('a short queue is built whole and offers nothing more', () => {
  const {ctx, elements} = harness(12);
  ctx.renderGrid();
  assert.equal(built(elements.grid), 12);
  assert.equal(elements.gridMore.hidden, true);
});

test('re-rendering the same view keeps the cards already built; another view starts over', () => {
  const {ctx, elements} = harness(150);
  ctx.renderGrid();
  ctx.showMoreReviewCards();
  ctx.predictions[0].status = 'accepted';
  ctx.renderGrid();
  assert.equal(built(elements.grid), 120, 'a decision does not send the reader back to the first batch');
  ctx.currentSort = 'confidence_desc';
  ctx.renderGrid();
  assert.equal(built(elements.grid), 60);
  ctx.showMoreReviewCards();
  ctx.currentTab = 'pending';
  ctx.renderGrid();
  assert.equal(built(elements.grid), 60);
  assert.equal(elements.gridMoreStatus.textContent, 'Showing 60 of 149 cards. More appear as you scroll.');
});

test('the lightbox list covers every photo of the view, not only the cards built so far', () => {
  const {ctx, elements} = harness(150);
  ctx.predictions.push({...row(151), photo_id: 1001, card_id: 'second-subject'});
  ctx.renderGrid();
  assert.equal(built(elements.grid), 60);
  const photos = ctx.reviewGridPhotoList();
  assert.equal(photos.length, 150, 'two cards of one photo list it once');
  assert.deepEqual(JSON.parse(JSON.stringify(photos[0])), {id: 1001, filename: '0001.jpg'});
  assert.equal(photos[149].id, 1150);
});

test('an empty view clears the grid and the count line', () => {
  const {ctx, elements} = harness(150);
  ctx.renderGrid();
  ctx.currentTab = 'rejected';
  ctx.renderGrid();
  assert.equal(elements.grid.innerHTML, '');
  assert.equal(elements.gridMore.hidden, true);
  assert.equal(ctx.reviewGridPhotoList().length, 0);
});

test('the accept check finds a prediction by id after the list is replaced', () => {
  const {ctx} = harness(3);
  ctx.predictions[2].evidence = {subject: {decision: 'not_enough_evidence'}};
  assert.equal(ctx.reviewEvidenceBlocksAccept(3), true);
  assert.equal(ctx.reviewEvidenceBlocksAccept(99), false, 'an unknown id blocks nothing');
  ctx.predictions = [row(7), {...row(8), evidence: {subject: {decision: 'invalid_subject'}}}];
  assert.equal(ctx.reviewEvidenceBlocksAccept(3), false, 'the replaced list no longer holds it');
  assert.equal(ctx.reviewEvidenceBlocksAccept(8), true);
  assert.equal(ctx.reviewEvidenceBlocksAccept(7), false);
});

test('one render shares the filtered rows and each status view keeps its own card index', () => {
  const {ctx} = harness(4);
  // Rows 1 and 2 are one burst, so one card while both are pending.
  ctx.predictions[1].card_id = 'card-1';
  ctx.predictions[0].group_id = ctx.predictions[1].group_id = 'burst';
  ctx.predictions[3].status = 'accepted';
  ctx.currentTab = 'pending';
  ctx.reviewVisibleRowsCache = new Map();
  const all = ctx.getVisibleItems(true, true);
  assert.equal(ctx.getVisibleItems(true, true), all, 'the second request reuses the rows');
  const pending = ctx.getVisibleItems(true);
  assert.equal(pending.length, 3);
  assert.equal(ctx.reviewCardCounts().pending, 2);
  assert.equal(ctx.reviewCardCounts().accepted, 1);
  assert.equal(ctx.getVisibleItems().length, 2);
  assert.equal(ctx.reviewCardMembers(ctx.predictions[0]).length, 2);
  ctx.reviewVisibleRowsCache = null;
  // Outside a render every request reads the rows again.
  ctx.predictions[1].status = 'accepted';
  assert.equal(ctx.getVisibleItems(true).length, 2);
  assert.equal(ctx.reviewCardCounts().pending, 2);
  assert.equal(ctx.reviewCardMembers(ctx.predictions[0]).length, 1);
});
