// Exercise the actual Browse controllers with deferred network responses.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const {test} = require('node:test');

function setup() {
  const elements = new Map();
  const pending = [];
  const rendered = [];
  const opened = [];
  const ctx = vm.createContext({
    document: {getElementById(id) {
      if (!elements.has(id)) {
        const classes = new Set(['hidden']);
        elements.set(id, {
          innerHTML: '', textContent: '',
          classList: {
            add: name => classes.add(name),
            remove: name => classes.delete(name),
            contains: name => classes.has(name),
          },
        });
      }
      return elements.get(id);
    }},
    VireoBrowseCompare: {create: () => ({})},
    safeFetch: (url, options) => new Promise((resolve, reject) => {
      pending.push({url, options, resolve, reject});
    }),
    showToast() {},
    selectionIdsKey: ids => ids.slice().sort((a, b) => a - b).join(','),
    selectionPredictionPhotoIdsByIdx: {},
    _batchToggleMixed() {},
    browseSelectionStackNote: () => '',
    openLightbox: (...args) => opened.push(args),
  });
  ctx.window = ctx;
  for (const file of ['panel-requests.js', 'selection-panel.js', 'prediction-panels.js']) {
    vm.runInContext(fs.readFileSync('vireo/static/browse/' + file, 'utf8'), ctx);
  }
  // Rendering is independent of network ownership; record which responses
  // reach it while executing the real loaders and their failure paths.
  ctx.renderSelectionKeywordSuggestions = rows => rendered.push(rows);
  ctx.renderSelectionPredictions = rows => rendered.push(rows);
  ctx.renderDetailPredictions = rows => rendered.push(rows);
  return {ctx, elements, pending, rendered, opened, lanes: ctx.Vireo.browse.panelRequests};
}

for (const [loader, responseField, elementId] of [
  ['loadSelectionKeywordSuggestions', 'keywords', 'selectionKeywordSuggestions'],
  ['loadSelectionPredictions', 'predictions', 'selectionPredictions'],
]) {
  test(loader + ': newest selection wins, including late failures', async () => {
    const {ctx, elements, pending, rendered} = setup();
    const old = ctx[loader]([1, 2]);
    const fresh = ctx[loader]([3, 4]);
    pending[1].resolve({[responseField]: ['fresh']});
    await fresh;
    const markup = elements.get(elementId).innerHTML;
    pending[0].reject(new Error('late failure'));
    await old;
    assert.deepEqual(rendered, [['fresh']]);
    assert.equal(elements.get(elementId).innerHTML, markup);
    // A stale failure must not clear the newer selection's cache.
    await ctx[loader]([4, 3]);
    assert.equal(pending.length, 2);
  });

  test(loader + ': late success cannot replace newer data', async () => {
    const {ctx, pending, rendered} = setup();
    const old = ctx[loader]([1, 2]);
    const fresh = ctx[loader]([3, 4]);
    pending[1].resolve({[responseField]: ['fresh']});
    await fresh;
    pending[0].resolve({[responseField]: ['old']});
    await old;
    assert.deepEqual(rendered, [['fresh']]);
  });

  test(loader + ': failed selection can retry without changing photos', async () => {
    const {ctx, elements, pending, rendered} = setup();
    const failed = ctx[loader]([1, 2]);
    pending[0].reject(new Error('offline'));
    await failed;
    assert.match(elements.get(elementId).innerHTML, /Could not load/);
    const retry = ctx[loader]([2, 1]);
    assert.equal(pending.length, 2);
    pending[1].resolve({[responseField]: ['recovered']});
    await retry;
    assert.deepEqual(rendered, [['recovered']]);
  });

  test(loader + ': duplicate in-flight requests and oversized selections are skipped', async () => {
    const {ctx, pending} = setup();
    const first = ctx[loader]([1, 2]);
    await ctx[loader]([2, 1]);
    await ctx[loader](Array.from({length: 1001}, (_, i) => i + 1));
    assert.equal(pending.length, 1);
    pending[0].resolve({[responseField]: []});
    await first;
  });
}

test('invalidating a selection drops its pending data and permits the same key again', async () => {
  const {ctx, lanes, pending, rendered} = setup();
  const old = ctx.loadSelectionKeywordSuggestions([1, 2]);
  lanes.keywords.invalidate();
  const fresh = ctx.loadSelectionKeywordSuggestions([1, 2]);
  pending[0].resolve({keywords: ['old']});
  await old;
  assert.deepEqual(rendered, []);
  pending[1].resolve({keywords: ['fresh']});
  await fresh;
  assert.deepEqual(rendered, [['fresh']]);
});

test('returning to a single photo invalidates the pending batch panels', async () => {
  const {ctx, elements, pending, rendered} = setup();
  const keywords = ctx.loadSelectionKeywordSuggestions([1, 2]);
  const predictions = ctx.loadSelectionPredictions([1, 2]);
  ctx.updateSelectionPanel([1]);
  pending[0].resolve({keywords: ['old']});
  pending[1].resolve({predictions: ['old']});
  await Promise.all([keywords, predictions]);
  assert.deepEqual(rendered, []);
  assert.equal(elements.get('selectionKeywordSuggestions').innerHTML, '');
  assert.equal(elements.get('selectionPredictions').innerHTML, '');
});

test('an oversized selection retires pending suggestions and preserves the cap message', async () => {
  const {ctx, elements, pending, rendered} = setup();
  const keywords = ctx.loadSelectionKeywordSuggestions([1, 2]);
  const predictions = ctx.loadSelectionPredictions([1, 2]);
  ctx.updatePasteEditSection = () => {};
  ctx.renderBatchInspector = () => {};
  ctx.renderSelectionWildlifeState = () => {};
  ctx.updateSelectionPanel(Array.from({length: 1001}, (_, i) => i + 1));
  pending[0].resolve({keywords: ['old']});
  pending[1].resolve({predictions: ['old']});
  await Promise.all([keywords, predictions]);
  assert.deepEqual(rendered, []);
  assert.match(elements.get('selectionKeywordSuggestions').innerHTML, /1,000 photos or fewer/);
  assert.match(elements.get('selectionPredictions').innerHTML, /1,000 photos or fewer/);
});

test('request lanes do not supersede unrelated panels', () => {
  const {lanes} = setup();
  const keyword = lanes.keywords.begin('1,2');
  lanes.predictions.begin('3,4');
  lanes.predictions.invalidate();
  assert.equal(keyword.isCurrent(), true);
});

test('an observation is invalidated even before the first request', () => {
  const {lanes} = setup();
  const current = lanes.predictions.observe();
  lanes.predictions.invalidate();
  assert.equal(current(), false);
});

test('Show photos stops batching when its selection is replaced', async () => {
  const {ctx, lanes, pending, opened} = setup();
  ctx.selectionPredictionPhotoIdsByIdx[0] = Array.from({length: 600}, (_, i) => i + 1);
  const button = {disabled: false, textContent: 'Show 600 photos'};
  const showing = ctx.showSelectionPredictionPhotos(0, button);
  lanes.predictions.invalidate();
  pending[0].resolve({photos: [{id: 1}]});
  await showing;
  assert.equal(pending.length, 1);
  assert.deepEqual(opened, []);
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, 'Show 600 photos');
});

test('only the latest Show click opens the lightbox', async () => {
  const {ctx, pending, opened} = setup();
  ctx.selectionPredictionPhotoIdsByIdx = {0: [1, 2], 1: [3, 4]};
  const old = ctx.showSelectionPredictionPhotos(0);
  const fresh = ctx.showSelectionPredictionPhotos(1);
  pending[1].resolve({photos: [{id: 3}, {id: 4}]});
  await fresh;
  pending[0].resolve({photos: [{id: 1}, {id: 2}]});
  await old;
  assert.equal(opened.length, 1);
  assert.equal(opened[0][0], 3);
});

for (const failure of [false, true]) {
  test('departed detail prediction ' + (failure ? 'failure' : 'success') + ' cannot repaint', async () => {
    const {ctx, elements, pending, rendered} = setup();
    ctx._detailPhotoId = 1;
    const old = ctx.loadDetailPredictions(1);
    ctx._detailPhotoId = 2;
    elements.get('detailPredictions').innerHTML = 'Photo 2';
    if (failure) pending[0].reject(new Error('late failure'));
    else pending[0].resolve(['photo 1']);
    await old;
    assert.equal(elements.get('detailPredictions').innerHTML, 'Photo 2');
    assert.deepEqual(rendered, []);
  });
}

test('clearing wildlife state prevents a late response from restoring batch controls', async () => {
  const {ctx, elements, pending} = setup();
  const old = ctx.renderSelectionWildlifeState([1, 2]);
  await ctx.renderSelectionWildlifeState([]);
  pending[0].resolve({selected_count: 2, included_count: 2});
  await old;
  assert.equal(elements.get('selectionWildlifeStatus').textContent, '');
  assert.equal(elements.get('selectionWildlifeActions').innerHTML, '');
});
