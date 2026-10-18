// Exercise the actual Browse controllers with deferred network responses.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const {test} = require('node:test');

// The browse files are ES modules. Run each one's body as a script, as
// tests/frontend/module-source.mjs does: drop its imports (the context supplies
// those names) and its `export` keywords. Page state is `state.<field>`, and
// the context stands in for `state`.
function moduleAsScript(source) {
  return source
    .replace(/^import\s[^;]*?\sfrom\s+'[^']+';\n/gm, '')
    .replace(/(^|\*\/ )export (?=(?:async\s+)?function|const |let |var |class )/gm, '$1');
}

function setup() {
  const elements = new Map();
  const pending = [];
  const rendered = [];
  const renderedCounts = [];
  const opened = [];
  const document = {activeElement: null};
  class Element {
    constructor(tag = 'div') {
      this.tag = tag;
      this.children = [];
      this.attributes = new Map();
      this.listeners = [];
      this._html = '';
      this._text = '';
    }
    set innerHTML(value) { this._html = value; this._text = ''; this.children = []; }
    get innerHTML() { return this._html + this._text + this.children.map(child => child.outerHTML).join(''); }
    get outerHTML() { return '<' + this.tag + '>' + this.innerHTML + '</' + this.tag + '>'; }
    set textContent(value) { this._html = ''; this._text = value; this.children = []; }
    get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
    setAttribute(key, value) { this.attributes.set(key, value); }
    appendChild(child) { child.parent = this; this.children.push(child); }
    contains(element) { return element === this || this.children.some(child => child.contains(element)); }
    addEventListener(name, handler) { this.listeners.push(handler); }
    closest() { return this instanceof HTMLButtonElement && this.attributes.has('data-panel-retry') ? this : null; }
    focus() { document.activeElement = this; }
  }
  class HTMLButtonElement extends Element {
    constructor() { super('button'); this.disabled = false; }
  }
  document.createElement = tag => tag === 'button' ? new HTMLButtonElement() : new Element(tag);
  document.getElementById = function(id) {
    if (!elements.has(id)) {
      const classes = new Set(['hidden']);
      const element = new Element();
      element.classList = {
        add: name => classes.add(name), remove: name => classes.delete(name), contains: name => classes.has(name),
      };
      elements.set(id, element);
    }
    return elements.get(id);
  };
  const ctx = vm.createContext({
    document, Element, HTMLButtonElement,
    VireoBrowseCompare: {create: () => ({})},
    safeFetch: (url, options) => new Promise((resolve, reject) => {
      pending.push({url, options, resolve, reject});
    }),
    showToast() {},
    selectionIdsKey: ids => ids.slice().sort((a, b) => a - b).join(','),
    _batchToggleMixed() {},
    browseSelectionStackNote: () => '',
    openLightbox: (...args) => opened.push(args),
    getBrowseSelection: () => ctx._activeSelection || [],
    getActiveSelection: () => ctx._vireoNativeMenuPhotoIdsOverride || ctx._activeSelection || [],
  });
  ctx.window = ctx;
  ctx.state = ctx;
  vm.runInContext(fs.readFileSync('vireo/static/vireo-panel-loader.js', 'utf8'), ctx);
  ctx.VireoPanelLoader = ctx.window.VireoPanelLoader;
  for (const file of ['panel-requests.js', 'selection-panel-state.js', 'selection-panel.js', 'prediction-panels.js', 'panel-loaders.js']) {
    vm.runInContext(moduleAsScript(fs.readFileSync('vireo/static/browse/' + file, 'utf8')), ctx);
  }
  // Rendering is independent of network ownership; record which responses
  // reach it while executing the real loaders and their failure paths.
  ctx.renderSelectionKeywordSuggestions = (rows, count) => { rendered.push(rows); renderedCounts.push(count); };
  ctx.renderSelectionPredictions = (rows, count) => { rendered.push(rows); renderedCounts.push(count); };
  ctx.renderDetailPredictions = rows => rendered.push(rows);
  for (const name of ['loadSelectionKeywordSuggestions', 'loadSelectionPredictions']) {
    const load = ctx[name];
    ctx[name] = ids => { ctx._activeSelection = ids.slice(); return load(ids); };
  }
  return {ctx, elements, pending, rendered, renderedCounts, opened, lanes: ctx.panelRequests};
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

  test(loader + ': Retry recovers repeatedly without duplicate requests', async () => {
    const {ctx, elements, pending, rendered} = setup();
    const kind = responseField === 'keywords' ? 'keywords' : 'predictions';
    const controller = ctx.panelLoaders[kind];
    const first = ctx[loader]([1, 2]);
    pending[0].reject(new Error('offline'));
    await first;
    const panel = elements.get(elementId);
    const button = panel.children[0].children[0];
    assert.equal(button.textContent, 'Retry');
    assert.match(button.attributes.get('aria-label'), /Retry loading/);
    button.focus();
    const retry = controller.retry();
    await controller.retry();
    assert.equal(pending.length, 2);
    assert.equal(ctx.document.activeElement, panel);
    assert.match(panel.textContent, /Checking/);
    // A click on the retired button cannot bubble into the new request.
    for (const listener of panel.listeners) listener({target: button});
    assert.equal(pending.length, 2);
    pending[1].reject(new Error('still offline'));
    await retry;
    assert.match(panel.textContent, /Could not load/);
    const recovered = controller.retry();
    pending[2].resolve({[responseField]: ['recovered']});
    await recovered;
    assert.deepEqual(rendered, [['recovered']]);
  });

  test(loader + ': an invalidated failure cannot retry the departed selection', async () => {
    const {ctx, lanes, pending} = setup();
    const kind = responseField === 'keywords' ? 'keywords' : 'predictions';
    const first = ctx[loader]([1, 2]);
    pending[0].reject(new Error('offline'));
    await first;
    lanes[kind].invalidate();
    await ctx.panelLoaders[kind].retry();
    assert.equal(pending.length, 1);
  });

  test(loader + ': Retry checks the active selection even without another request', async () => {
    const {ctx, pending} = setup();
    const kind = responseField === 'keywords' ? 'keywords' : 'predictions';
    const first = ctx[loader]([1, 2]);
    pending[0].reject(new Error('offline'));
    await first;
    ctx._activeSelection = [3, 4];
    await ctx.panelLoaders[kind].retry();
    assert.equal(pending.length, 1);
  });

  test(loader + ': retry input is a private snapshot', async () => {
    const {ctx, pending, rendered} = setup();
    const ids = [1, 2];
    const first = ctx[loader](ids);
    ids.splice(0, 2, 3, 4);
    pending[0].reject(new Error('offline'));
    await first;
    const kind = responseField === 'keywords' ? 'keywords' : 'predictions';
    const retry = ctx.panelLoaders[kind].retry();
    assert.deepEqual(JSON.parse(pending[1].options.body).photo_ids, [1, 2]);
    pending[1].resolve({[responseField]: ['original selection']});
    await retry;
    assert.deepEqual(rendered, [['original selection']]);
  });

  test(loader + ': server-reported zero selection count is preserved', async () => {
    const {ctx, pending, renderedCounts} = setup();
    const first = ctx[loader]([1, 2]);
    pending[0].resolve({[responseField]: [], selected_count: 0});
    await first;
    assert.deepEqual(renderedCounts, [0]);
  });

  test(loader + ': a late caller cannot claim the current panel for old ids', async () => {
    const {ctx, pending, rendered} = setup();
    const current = ctx[loader]([3, 4]);
    const kind = responseField === 'keywords' ? 'keywords' : 'predictions';
    const stale = ctx.panelLoaders[kind].load([1, 2]);
    assert.equal(pending.length, 1);
    pending[0].resolve({[responseField]: ['current']});
    await Promise.all([current, stale]);
    assert.deepEqual(rendered, [['current']]);
  });

  test(loader + ': a native-menu action override cannot discard the visible selection response', async () => {
    const {ctx, pending, rendered} = setup();
    const current = ctx[loader]([1, 2]);
    ctx._vireoNativeMenuPhotoIdsOverride = [9];
    pending[0].resolve({[responseField]: ['visible selection']});
    await current;
    assert.deepEqual(rendered, [['visible selection']]);
    await ctx[loader]([2, 1]);
    assert.equal(pending.length, 1);
  });

  test(loader + ': failures and Retry stay with the grid while menu actions target another photo', async () => {
    const {ctx, pending, rendered} = setup();
    ctx._vireoNativeMenuPhotoIdsOverride = [9];
    const current = ctx[loader]([1, 2]);
    pending[0].reject(new Error('offline'));
    await current;
    const kind = responseField === 'keywords' ? 'keywords' : 'predictions';
    const retry = ctx.panelLoaders[kind].retry();
    assert.deepEqual(JSON.parse(pending[1].options.body).photo_ids, [1, 2]);
    pending[1].resolve({[responseField]: ['recovered']});
    await retry;
    assert.deepEqual(rendered, [['recovered']]);
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
  ctx.selectionPanel.predictions.replaceRows([
    {predicted_photo_ids: Array.from({length: 600}, (_, i) => i + 1)},
  ]);
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
  ctx.selectionPanel.predictions.replaceRows([
    {predicted_photo_ids: [1, 2]}, {predicted_photo_ids: [3, 4]},
  ]);
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
    ctx.detailPhotoId = 1;
    const old = ctx.loadDetailPredictions(1);
    ctx.detailPhotoId = 2;
    elements.get('detailPredictions').innerHTML = 'Photo 2';
    if (failure) pending[0].reject(new Error('late failure'));
    else pending[0].resolve(['photo 1']);
    await old;
    assert.equal(elements.get('detailPredictions').innerHTML, 'Photo 2');
    assert.deepEqual(rendered, []);
  });
}

test('detail prediction Retry works before the first successful render and retires on departure', async () => {
  const {ctx, pending, rendered} = setup();
  ctx.detailPhotoId = 1;
  const first = ctx.loadDetailPredictions(1);
  pending[0].reject(new Error('offline'));
  await first;
  const retry = ctx.panelLoaders.detailPredictions.retry();
  assert.match(pending[1].url, /photo_ids=1/);
  ctx.detailPhotoId = 2;
  pending[1].resolve({predictions: ['old photo']});
  await retry;
  assert.deepEqual(rendered, []);
  await ctx.panelLoaders.detailPredictions.retry();
  assert.equal(pending.length, 2);
});

test('clearing wildlife state prevents a late response from restoring batch controls', async () => {
  const {ctx, elements, pending} = setup();
  const old = ctx.renderSelectionWildlifeState([1, 2]);
  await ctx.renderSelectionWildlifeState([]);
  pending[0].resolve({selected_count: 2, included_count: 2});
  await old;
  assert.equal(elements.get('selectionWildlifeStatus').textContent, '');
  assert.equal(elements.get('selectionWildlifeActions').innerHTML, '');
});

test('retiring panel rows prevents keyword writes, prediction acceptance, and stale toggles', async () => {
  const {ctx, pending, rendered} = setup();
  const state = ctx.selectionPanel;
  state.keywords.replace([{id: 7, name: "Cooper's Hawk", missing_photo_ids: [1], present_photo_ids: [2]}]);
  state.predictions.remember([{species: "Say's Phoebe"}], 2, {});
  state.predictions.replaceRows([{species: "Say's Phoebe", acceptable_prediction_ids: [100]}]);
  state.keywords.reset();
  state.predictions.reset();
  await ctx.applySelectionKeyword(7);
  await ctx.removeSelectionKeyword(7);
  await ctx.acceptSelectionPrediction(0, true);
  ctx.toggleSelectionPredictions();
  assert.deepEqual(pending, []);
  assert.deepEqual(rendered, []);
});

test('row actions retain a snapshot even when the source arrays change', () => {
  const {ctx} = setup();
  const state = ctx.selectionPanel;
  const keyword = {id: 7, missing_photo_ids: [1], present_photo_ids: [2]};
  const prediction = {species: "Say's Phoebe", acceptable_prediction_ids: [100], predicted_photo_ids: [1, 2]};
  state.keywords.replace([keyword]);
  state.predictions.replaceRows([prediction]);
  keyword.missing_photo_ids.push(3);
  prediction.acceptable_prediction_ids.push(101);
  prediction.predicted_photo_ids.length = 0;
  assert.deepEqual(Array.from(state.keywords.get(7).missingPhotoIds), [1]);
  const row = state.predictions.getRow(0);
  assert.deepEqual(Array.from(row.acceptableIds), [100]);
  assert.deepEqual(Array.from(row.photoIds), [1, 2]);
  assert.throws(() => row.acceptableIds.push(102), TypeError);
});

test('loading a different selection immediately retires the old actionable rows', async () => {
  const {ctx, pending} = setup();
  const state = ctx.selectionPanel;
  state.keywords.replace([{id: 7, missing_photo_ids: [1]}]);
  state.predictions.replaceRows([{species: "Say's Phoebe", acceptable_prediction_ids: [100]}]);
  const keywords = ctx.loadSelectionKeywordSuggestions([3, 4]);
  const predictions = ctx.loadSelectionPredictions([3, 4]);
  assert.equal(state.keywords.get(7), undefined);
  assert.equal(state.predictions.getRow(0), undefined);
  pending[0].resolve({keywords: []});
  pending[1].resolve({predictions: []});
  await Promise.all([keywords, predictions]);
});

function setupActions() {
  const listeners = [];
  const calls = [];
  const panel = {
    addEventListener: (name, handler) => listeners.push(handler),
    contains: button => button.inPanel,
  };
  class Element {}
  class HTMLButtonElement extends Element {}
  const ctx = vm.createContext({
    document: {getElementById: () => panel}, Element, HTMLButtonElement,
  });
  ctx.window = ctx;
  for (const file of ['selection-panel-state.js', 'selection-panel-events.js']) {
    vm.runInContext(moduleAsScript(fs.readFileSync('vireo/static/browse/' + file, 'utf8')), ctx);
  }
  for (const name of ['openBatchDevelopmentEditor', 'pasteEditSettingsToSelection',
    'setSelectionWildlifeExcluded', 'applySelectionKeyword', 'removeSelectionKeyword',
    'toggleSelectionPredictions', 'acceptSelectionPrediction', 'showSelectionPredictionPhotos',
    'openPredictionInReview']) {
    ctx[name] = (...args) => calls.push([name, ...args]);
  }
  const state = ctx.selectionPanel;
  state.predictions.replaceRows([{species: "Say's Phoebe", ambiguous_photo_ids: [12]}]);
  const bindActions = ctx.bindSelectionPanelActions;
  bindActions();
  function click(action, attrs = {}, options = {}) {
    const attributes = {'data-selection-action': action, ...attrs};
    const button = Object.assign(new HTMLButtonElement(), {
      inPanel: true, disabled: false, ...options,
      getAttribute: name => Object.hasOwn(attributes, name) ? attributes[name] : null,
    });
    // The click target can be a nested icon rather than the button itself.
    const target = Object.assign(new Element(), {closest: () => button});
    const event = {target};
    listeners.forEach(handler => handler(event));
    return button;
  }
  return {state, bindActions, listeners, calls, click, Element};
}

test('delegated actions bind once and preserve each button argument', () => {
  const {bindActions, listeners, calls, click} = setupActions();
  bindActions();
  assert.equal(listeners.length, 1);
  click('edit');
  click('paste');
  click('wildlife-exclude');
  click('wildlife-include');
  click('keyword-add', {'data-keyword-id': '7'});
  click('keyword-remove', {'data-keyword-id': '7'});
  click('prediction-toggle');
  const subset = click('prediction-accept', {'data-prediction-row': '0'});
  const all = click('prediction-accept-all', {'data-prediction-row': '0'});
  const show = click('prediction-show', {'data-prediction-row': '0'});
  click('prediction-review', {'data-prediction-row': '0'});
  assert.deepEqual(calls, [
    ['openBatchDevelopmentEditor'], ['pasteEditSettingsToSelection'],
    ['setSelectionWildlifeExcluded', true], ['setSelectionWildlifeExcluded', false],
    ['applySelectionKeyword', 7], ['removeSelectionKeyword', 7], ['toggleSelectionPredictions'],
    ['acceptSelectionPrediction', 0, false, subset],
    ['acceptSelectionPrediction', 0, true, all],
    ['showSelectionPredictionPhotos', 0, show], ['openPredictionInReview', 12],
  ]);
});

test('delegated actions ignore disabled, detached, invalid, and retired rows', () => {
  const {state, calls, click} = setupActions();
  click('edit', {}, {disabled: true});
  click('paste', {}, {inPanel: false});
  for (const key of ['', '-1', '1.5', 'not-a-number']) {
    click('keyword-add', {'data-keyword-id': key});
    click('prediction-accept-all', {'data-prediction-row': key});
  }
  click('prediction-accept-all');
  click('prediction-accept-all', {'data-prediction-row': '2'});
  state.predictions.reset();
  click('prediction-show', {'data-prediction-row': '0'});
  click('prediction-review', {'data-prediction-row': '0'});
  assert.deepEqual(calls, []);
});

test('delegated actions ignore non-element targets and non-button matches', () => {
  const {listeners, calls, Element} = setupActions();
  const nonButton = Object.assign(new Element(), {closest: () => new Element()});
  const emptyMatch = Object.assign(new Element(), {closest: () => null});
  for (const target of [null, {}, nonButton, emptyMatch]) {
    for (const listener of listeners) listener({target});
  }
  assert.deepEqual(calls, []);
});
