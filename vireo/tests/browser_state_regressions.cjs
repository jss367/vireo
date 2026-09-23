// Behavioral regressions using actual template functions in a simulated UI.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
function fn(file, name) {
  const src = fs.readFileSync(file, 'utf8');
  const found = src.match(new RegExp('(?:async )?function ' + name + '\\([^]*?\\n\\}'));
  assert(found, name);
  return found[0];
}
const browse = 'vireo/templates/browse.html';
const review = 'vireo/templates/pipeline_review.html';
async function main() {
  let complete;
  const ctx = vm.createContext({
    selectedPhotos: new Set(), selectedPhotoId: null, selectedIndex: -1,
    anchorRestoreEpoch: 0, selectAllRequestSeq: 0, loadEpoch: 0,
    window: {}, buildBrowseIdsRequest: () => ({}),
    safeFetch: () => new Promise(resolve => { complete = resolve; }),
    showToast() {}, renderGrid() {}, updateBatchBar() {},
    browseSelectionIdsForClick: id => [id], loadDetail() {},
    refreshCardSelectionVisuals() {}, noteFocusedCardVisibility() {},
  });
  for (const name of ['observeBrowseWindow', 'selectAllMatchingPhotos', 'selectPhoto', 'getActiveSelection']) {
    vm.runInContext(fn(browse, name), ctx);
  }
  const pending = ctx.selectAllMatchingPhotos();
  ctx.selectPhoto({shiftKey: false, metaKey: false, ctrlKey: false}, 2, 1);
  assert.equal(ctx.selectedPhotoId, 2);
  complete({photo_ids: [1, 2, 3, 4]});
  await pending;
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [2]);
  

  const flags = [];
  const inspect = vm.createContext({
    inspectPhotoId: 10,
    setPipelineReviewFlag: (...args) => flags.push(args),
  });
  for (const name of ['pipelineReviewBareKey', 'inspectKeyHandler']) vm.runInContext(fn(review, name), inspect);
  inspect.inspectKeyHandler({key: 'p', target: {tagName: 'INPUT'}, preventDefault() {}, stopPropagation() {}});
  assert.deepEqual(flags, []);
  inspect.inspectKeyHandler({key: 'p', target: {tagName: 'DIV'}, preventDefault() {}, stopPropagation() {}});
  assert.deepEqual(flags, [[10, 'flagged']]);
  

  const sent = [];
  const burst = vm.createContext({
    reviewScopeMode: 'cache', pipelineResults: {source: 'browse-selection'},
    safeFetch: async (url, opts) => { sent.push([url, JSON.parse(opts.body)]); return {ok: false}; },
    notifyReadOnlyScopedView() {},
  });
  for (const name of ['isScopedReviewView', 'detachBurst']) vm.runInContext(fn(review, name), burst);
  await burst.detachBurst(0, 0);
  assert.equal(sent.length, 0);
}
main().catch(err => { console.error(err); process.exitCode = 1; });

async function testAsyncModalOwnership() {
  const navbar = 'vireo/templates/_navbar.html';
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) elements.set(id, {value: '', style: {}, textContent: '', classList: {remove() {}}});
    return elements.get(id);
  }
  let finish;
  const ctx = vm.createContext({
    inatQueue: [{photo_id: 1}], _inatModalGeneration: 1, _inatSubmitOwner: 0,
    _inatSubmitting: false, _inatCancelled: false,
    _lbGuardReadOnly: () => false,
    document: {getElementById: element, querySelector: element},
    safeFetch: () => new Promise(resolve => {finish = resolve;}),
    isTauri: () => false, showToast() {}, escapeAttr: s => s,
  });
  vm.runInContext(fn(navbar, 'inatDoSubmit'), ctx);
  const pending = ctx.inatDoSubmit();
  // Close the old modal and open another while its request is in flight.
  ctx._inatModalGeneration++;
  ctx._inatCancelled = true;
  const newQueue = [{photo_id: 2}];
  ctx.inatQueue = newQueue;
  element('inatStatus0').textContent = 'New observation';
  finish({observation_url: 'https://example.invalid/observation'});
  await pending;
  assert.equal(ctx.inatQueue, newQueue);
  assert.equal(element('inatStatus0').textContent, 'New observation');
}

async function testReadinessAndRegroupRaces() {
  let selected = [{value: 'model-a'}, {value: 'model-b'}];
  const panel = {innerHTML: '', style: {}};
  const requests = [];
  const ctx = vm.createContext({
    readinessRequestSequence: 0, escapeHtml: s => s,
    document: {getElementById: () => panel, querySelectorAll: s => s.includes('model-checkbox') ? selected : []},
    safeFetch: url => new Promise(resolve => requests.push({url, resolve})),
  });
  vm.runInContext(fn('vireo/templates/pipeline.html', 'updateReadiness'), ctx);
  const old = ctx.updateReadiness();
  assert.equal(requests.length, 2);
  assert(requests[0].url.includes('model-a'));
  assert(requests[1].url.includes('model-b'));
  selected = [{value: 'model-c'}];
  const current = ctx.updateReadiness();
  requests[2].resolve({model_ready: true, model_name: 'Current model', use_tol: true});
  await current;
  requests[0].resolve({model_ready: true, model_name: 'Old model A', use_tol: true});
  requests[1].resolve({model_ready: true, model_name: 'Old model B', use_tol: true});
  await old;
  assert(panel.innerHTML.includes('Current model'));
  assert(!panel.innerHTML.includes('Old model'));

  const indicator = {style: {display: 'block'}};
  const pending = [];
  let applied = 0;
  const reviewCtx = vm.createContext({
    reviewScopeRequestSeq: 0, getGroupingConfig: () => ({}), getScoringConfig: () => ({}),
    reviewScopePayload: s => s, scopedCacheInfoFor: () => ({}),
    applyReviewResults: () => applied++, document: {getElementById: () => indicator},
    safeFetch: () => new Promise(resolve => pending.push(resolve)),
  });
  vm.runInContext(fn(review, 'doRegroupLive'), reviewCtx);
  reviewCtx.doRegroupLive();
  reviewCtx.doRegroupLive();
  pending[0]({});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(indicator.style.display, 'block');
  assert.equal(applied, 0);
  pending[1]({});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(indicator.style.display, 'none');
  assert.equal(applied, 1);
}

async function testSavedCollectionPreviewScope() {
  const source = fs.readFileSync('vireo/static/vireo-filter.js', 'utf8');
  const openSaveModal = source.match(/  function openSaveModal\(\) \{[^]*?\n  \}/)[0];
  for (const scope of [{folder_id: 7}, {collection_id: 9}]) {
    const elements = new Map();
    const element = selector => {
      if (!elements.has(selector)) elements.set(selector, {focus() {}});
      return elements.get(selector);
    };
    const rules = [{field: 'rating', operator: 'gte', value: 3}];
    const visual = {prompt: 'owl', strength: 'medium'};
    let request;
    const ctx = vm.createContext({
      $: element, state: {getScope: () => scope, visual},
      userRules: () => rules, expressionSummary: () => 'Saved filter',
      setTimeout: fn => fn(),
      fetchJson: async (url, options) => {
        request = JSON.parse(options.body);
        return {total: request.folder_id || request.collection_id ? 1 : 9};
      },
    });
    vm.runInContext(openSaveModal, ctx);
    ctx.openSaveModal();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(element('.vf-save-preview').textContent, '9 matching photos — Saved filter');
    assert.deepEqual(request.rules, rules);
    assert.deepEqual(request.visual, visual);
  }
}

Promise.all([testAsyncModalOwnership(), testReadinessAndRegroupRaces(), testSavedCollectionPreviewScope()])
  .catch(err => { console.error(err); process.exitCode = 1; });
