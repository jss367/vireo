// Behavioral regressions using actual template functions in a simulated UI.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
function fn(file, name) {
  const src = [].concat(file).map(f => fs.readFileSync(f, 'utf8')).join('\n');
  const found = src.match(new RegExp('(?:async )?function ' + name + '\\([^]*?\\n\\}'));
  assert(found, name);
  // The extracted review functions use the same state guards as the page.
  // Evaluate those real helpers in each isolated VM as well.
  if ([].concat(file).includes('vireo/static/pipeline-review/state.js') &&
      !['requireReviewValue', 'currentReviewResults'].includes(name)) {
    return fn(file, 'requireReviewValue') + '\n' + fn(file, 'currentReviewResults') + '\n' + found[0];
  }
  return found[0];
}
const browse = ['vireo/static/browse/loading.js', 'vireo/static/browse/selection.js'];
const review = Array.from(
  fs.readFileSync('vireo/templates/pipeline_review.html', 'utf8')
    .matchAll(/<script(?: defer)? src="\/static\/(pipeline-review\/[^"]+\.js)"><\/script>/g),
  match => 'vireo/static/' + match[1],
);
assert(review.length > 0, 'Pipeline Review scripts must be loaded by the template');
async function main() {
  let complete;
  const ctx = vm.createContext({
    selectedPhotos: new Set(), selectedPhotoId: null, selectedIndex: -1,
    anchorRestoreEpoch: 0, selectAllRequestSeq: 0,
    window: {}, buildBrowseIdsRequest: () => ({}),
    safeFetch: () => new Promise(resolve => { complete = resolve; }),
    showToast() {}, renderGrid() {}, updateBatchBar() {},
    browseSelectionIdsForClick: id => [id], loadDetail() {},
    refreshCardSelectionVisuals() {}, noteFocusedCardVisibility() {},
    // shared.js's reader of the navbar's native-menu override.
    nativeMenuPhotoIdsOverride: () => ctx.window._vireoNativeMenuPhotoIdsOverride,
  });
  // Browse's functions are ES module code that reads page state as
  // `state.<field>`; the context stands in for that object.
  ctx.state = ctx;
  vm.runInContext(fs.readFileSync('vireo/static/browse/window-requests.js', 'utf8').replace(/^export /gm, ''), ctx);
  for (const name of ['selectAllMatchingPhotos', 'selectPhoto', 'getBrowseSelection', 'getActiveSelection']) {
    vm.runInContext(fn(browse, name), ctx);
  }
  const pending = ctx.selectAllMatchingPhotos();
  ctx.selectPhoto({shiftKey: false, metaKey: false, ctrlKey: false}, 2, 1);
  assert.equal(ctx.selectedPhotoId, 2);
  complete({photo_ids: [1, 2, 3, 4]});
  await pending;
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [2]);
  ctx.window._vireoNativeMenuPhotoIdsOverride = [99];
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [99]);
  assert.deepEqual(Array.from(ctx.getBrowseSelection()), [2]);
  ctx.selectedPhotos.add(2);
  ctx.selectedPhotos.add(3);
  assert.deepEqual(Array.from(ctx.getBrowseSelection()), [2, 3]);
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [99]);
  

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
    reviewScopeMode: 'cache', reviewScopeRegrouped: false, pipelineResults: {source: 'browse-selection'},
    safeFetch: async (url, opts) => { sent.push([url, JSON.parse(opts.body)]); return {ok: false}; },
    notifyReadOnlyScopedView() {},
  });
  for (const name of ['isScopedReviewView', 'detachBurst']) vm.runInContext(fn(review, name), burst);
  await burst.detachBurst(0, 0);
  assert.equal(sent.length, 0);


  // A scope change clears the right-click context selection so the next
  // native-menu keyboard action doesn't resolve to a photo now hidden by
  // the newly selected Folder/Collection scope.
  const scopeChange = vm.createContext({
    window: {_vireoNativeMenuPhotoIdsOverride: [10]},
    pipelineReviewContextPhotoIds: [10],
    reviewScopeMode: 'cache',
    reviewScopeFolderId: null, reviewScopeCollectionId: null,
    reviewScopeRegrouped: false, reviewScopeRequestSeq: 0,
    reviewScopeMembershipSeq: 0,
    reviewScopePhotoIds: null, reviewScopeTotal: null,
    pipelineResults: null, resultsCacheInfo: null,
    cachedPipelineResults: null, cachedResultsCacheInfo: null,
    refreshLatestScopeSnapshotIfCurrent() {},
    updateReviewScopeControls() {},
    setReviewScopeStatus() {},
    showSavedReview() {},
    applyReviewResults() {},
    loadRegroupedScopeResults() {},
    reviewScopeSelectionId: () => 'f1',
    safeFetch: () => new Promise(() => {}),
  });
  for (const name of [
    'clearPipelineReviewContextSelection',
    'loadReviewScopeResults',
    'onReviewScopeFolderChange',
  ]) vm.runInContext(fn(review, name), scopeChange);
  scopeChange.onReviewScopeFolderChange('f1');
  assert.equal(scopeChange.pipelineReviewContextPhotoIds.length, 0);
  assert.equal(scopeChange.window._vireoNativeMenuPhotoIdsOverride, null);


  // A smart-collection membership refresh that drops the right-clicked
  // photo must scope-filter the stale native-menu selection. Filter mode
  // stays writable (isScopedReviewView() is false), so leaving the dropped
  // id in pipelineReviewContextPhotoIds would let the next keyboard action
  // mutate a now-hidden photo. A multi-id override keeps the still-in-scope
  // ids and drops only the ones that fell out.
  const membershipDrop = vm.createContext({
    window: {_vireoNativeMenuPhotoIdsOverride: [10, 20]},
    pipelineReviewContextPhotoIds: [10],
    reviewScopeMode: 'collection',
    reviewScopeCollectionId: 'c1',
    reviewScopeRegrouped: false,
    reviewScopePhotoIds: new Set([10, 20]),
    reviewScopeTotal: 2,
    reviewScopeMembershipSeq: 0,
    _reviewScopeMembershipRefreshTimer: null,
    _reviewScopeMembershipRefreshSeq: 0,
    reviewScopeMembershipSeq: 0,
    pipelineResults: {summary: {}},
    resultsCacheInfo: null,
    renderResults() {},
    updateSummaryBar() {},
    updateReviewScopeStatusFromResults() {},
    renderCacheScopeBanner() {},
    setTimeout: (fn) => { fn(); return 0; },
    clearTimeout() {},
    safeFetch: async () => ({photo_ids: [20]}),
  });
  for (const name of [
    'clearPipelineReviewContextSelection',
    'filterPipelineReviewContextSelectionToScope',
    'isReviewScopeFilterMode', 'activeReviewScopeFilter', 'photoInOpenGroupReview',
    'isPhotoActionableInReviewScope', 'photoIdsInReviewScope', 'reconcileReviewScopeTargets',
    'maybeRefreshCollectionScopeMembership',
  ]) vm.runInContext(fn(review, name), membershipDrop);
  membershipDrop.maybeRefreshCollectionScopeMembership();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(membershipDrop.pipelineReviewContextPhotoIds, []);
  assert.deepEqual(membershipDrop.window._vireoNativeMenuPhotoIdsOverride, [20]);


  // A smart-collection membership refresh that drops the currently inspected
  // photo must close the legacy inspector. getActiveSelection() prioritizes
  // inspectPhotoId and the inspector's P/X shortcut writes against it, so
  // leaving the inspector open on a photo that just left the collection
  // would let the next keyboard action mutate a now-hidden target.
  let inspectorClosed = 0;
  const inspectorDrop = vm.createContext({
    window: {},
    pipelineReviewContextPhotoIds: [],
    reviewScopeMode: 'collection',
    reviewScopeCollectionId: 'c1',
    reviewScopeRegrouped: false,
    reviewScopePhotoIds: new Set([10, 20]),
    reviewScopeTotal: 2,
    reviewScopeMembershipSeq: 0,
    _reviewScopeMembershipRefreshTimer: null,
    _reviewScopeMembershipRefreshSeq: 0,
    reviewScopeMembershipSeq: 0,
    inspectPhotoId: 10,
    pipelineResults: {summary: {}},
    resultsCacheInfo: null,
    renderResults() {},
    updateSummaryBar() {},
    updateReviewScopeStatusFromResults() {},
    renderCacheScopeBanner() {},
    setTimeout: (fn) => { fn(); return 0; },
    clearTimeout() {},
    safeFetch: async () => ({photo_ids: [20]}),
    closeInspect() { inspectorClosed++; inspectorDropHolder.inspectPhotoId = null; },
    openInspect(id) { inspectorDropHolder.openInspectCalls.push(id); },
    openInspectCalls: [],
  });
  const inspectorDropHolder = inspectorDrop;
  for (const name of [
    'clearPipelineReviewContextSelection',
    'filterPipelineReviewContextSelectionToScope',
    'isReviewScopeFilterMode', 'activeReviewScopeFilter', 'photoInOpenGroupReview',
    'isPhotoActionableInReviewScope', 'photoIdsInReviewScope', 'reconcileReviewScopeTargets',
    'maybeRefreshCollectionScopeMembership',
  ]) vm.runInContext(fn(review, name), inspectorDrop);
  inspectorDrop.maybeRefreshCollectionScopeMembership();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(inspectorClosed, 1,
    'inspector must close when its photo leaves the collection');
  assert.equal(inspectorDrop.inspectPhotoId, null);
  assert.deepEqual(inspectorDrop.openInspectCalls, [],
    'inspector must not reopen on a dropped inspected photo');

  // A stale membership refresh response — one that was in flight when a
  // switch to cache/workspace and back to the same collection bumped
  // reviewScopeMembershipSeq without touching _reviewScopeMembershipRefreshSeq
  // — must not overwrite the freshly-loaded membership from the scope's own
  // fetch. Capturing reviewScopeMembershipSeq at refresh start and rejecting
  // on change is what guards this: without it the stale response's local
  // seq, mode and collection-id checks would still pass.
  let resolveStale;
  const staleRace = vm.createContext({
    window: {},
    pipelineReviewContextPhotoIds: [],
    reviewScopeMode: 'collection',
    reviewScopeCollectionId: 'c1',
    reviewScopeRegrouped: false,
    reviewScopePhotoIds: new Set([10, 20]),
    reviewScopeTotal: 2,
    reviewScopeMembershipSeq: 7,
    _reviewScopeMembershipRefreshTimer: null,
    _reviewScopeMembershipRefreshSeq: 0,
    inspectPhotoId: null,
    pipelineResults: {summary: {}},
    resultsCacheInfo: null,
    renderResults() { throw new Error('render must not run for a stale response'); },
    updateSummaryBar() { throw new Error('summary must not refresh for a stale response'); },
    updateReviewScopeStatusFromResults() {},
    renderCacheScopeBanner() {},
    setTimeout: (fn) => { fn(); return 0; },
    clearTimeout() {},
    safeFetch: () => new Promise(resolve => { resolveStale = resolve; }),
  });
  for (const name of [
    'clearPipelineReviewContextSelection',
    'filterPipelineReviewContextSelectionToScope',
    'maybeRefreshCollectionScopeMembership',
  ]) vm.runInContext(fn(review, name), staleRace);
  staleRace.maybeRefreshCollectionScopeMembership();
  // Simulate the user switching scope away and back to the same collection
  // while the refresh fetch is in flight — loadReviewScopeResults bumps
  // reviewScopeMembershipSeq but not _reviewScopeMembershipRefreshSeq.
  staleRace.reviewScopeMembershipSeq++;
  resolveStale({photo_ids: [99]});
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(Array.from(staleRace.reviewScopePhotoIds), [10, 20],
    'stale membership response must not overwrite the fresh scope-load set');
  assert.equal(staleRace.reviewScopeTotal, 2);


  // A membership refresh that keeps the inspected photo in scope must not
  // close the inspector. The user has not changed scope and the photo is
  // still visible, so dismissing the open panel would be a regression.
  // When another member of the inspected encounter is dropped, the panel
  // must be rebuilt so its filmstrip's scope filter re-runs
  // and the hidden photo's clickable thumbnail disappears.
  let keepClosed = 0;
  const keepOpened = [];
  const inspectorKeep = vm.createContext({
    window: {},
    pipelineReviewContextPhotoIds: [],
    reviewScopeMode: 'collection',
    reviewScopeCollectionId: 'c1',
    reviewScopeRegrouped: false,
    reviewScopePhotoIds: new Set([10, 20]),
    reviewScopeTotal: 2,
    reviewScopeMembershipSeq: 0,
    _reviewScopeMembershipRefreshTimer: null,
    _reviewScopeMembershipRefreshSeq: 0,
    reviewScopeMembershipSeq: 0,
    inspectPhotoId: 10,
    pipelineResults: {summary: {}},
    resultsCacheInfo: null,
    renderResults() {},
    updateSummaryBar() {},
    updateReviewScopeStatusFromResults() {},
    renderCacheScopeBanner() {},
    setTimeout: (fn) => { fn(); return 0; },
    clearTimeout() {},
    safeFetch: async () => ({photo_ids: [10]}),
    closeInspect() { keepClosed++; },
    openInspect(id) { keepOpened.push(id); },
  });
  for (const name of [
    'clearPipelineReviewContextSelection',
    'filterPipelineReviewContextSelectionToScope',
    'isReviewScopeFilterMode', 'activeReviewScopeFilter', 'photoInOpenGroupReview',
    'isPhotoActionableInReviewScope', 'photoIdsInReviewScope', 'reconcileReviewScopeTargets',
    'maybeRefreshCollectionScopeMembership',
  ]) vm.runInContext(fn(review, name), inspectorKeep);
  inspectorKeep.maybeRefreshCollectionScopeMembership();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(keepClosed, 0,
    'inspector must stay open when its photo remains in the collection');
  assert.equal(inspectorKeep.inspectPhotoId, 10);
  assert.deepEqual(keepOpened, [10],
    'inspector must rebuild its panel so the filmstrip drops out-of-scope photos');


  // renderAlgorithmTrace must suppress openInspect links on pair photos
  // outside the scope. openInspect routes a flat encounter member to the
  // legacy inspector, and its P/X shortcut writes a flag against
  // inspectPhotoId with no scoped-view read-only guard. An in-scope pair
  // photo stays clickable.
  const traceCtx = vm.createContext({
    window: {},
    pipelineResults: {
      photos: [
        {id: 1, filename: 'a.jpg'},
        {id: 2, filename: 'b.jpg'},
        {id: 3, filename: 'c.jpg'},
      ],
      encounters: [{
        photo_ids: [1, 2, 3],
        photo_count: 3,
        burst_count: 0,
        trace: [{decision: 'kept', dt_seconds: 1, score: 0.5}, {decision: 'cut_time', dt_seconds: 10, score: 0.1}],
      }],
    },
    reviewScopeMode: 'folder',
    reviewScopeRegrouped: false,
    reviewScopePhotoIds: new Set([1, 3]),
    _focusedEncounterIdx: 0,
    document: {getElementById: (id) => {
      if (id === 'algorithmTrace') return {set innerHTML(v) { tracePanelHtml = v; }};
      if (id === 'traceFocusLabel') return {textContent: ''};
      return null;
    }},
    escapeHtml: s => String(s), escapeAttr: s => String(s),
  });
  let tracePanelHtml = '';
  for (const name of [
    'isReviewScopeFilterMode', 'activeReviewScopeFilter', 'photoInOpenGroupReview',
    'isPhotoActionableInReviewScope', 'renderAlgorithmTrace',
  ]) vm.runInContext(fn(review, name), traceCtx);
  traceCtx.renderAlgorithmTrace();
  assert(tracePanelHtml.includes('openInspect(1)'), 'photo 1 is in scope and must be clickable');
  assert(tracePanelHtml.includes('openInspect(3)'), 'photo 3 is in scope and must be clickable');
  assert(!tracePanelHtml.includes('openInspect(2)'),
    'out-of-scope photo 2 must not render an openInspect link in the trace');


  // The legacy inspector's filmstrip must drop out-of-scope photos: clicking
  // a hidden thumbnail would navigate inspectPhotoId to an out-of-scope
  // photo, and the P/X shortcut writes a flag against it with no scoped
  // read-only guard. Only the in-scope encounter members should render.
  const filmstripCtx = vm.createContext({
    window: {},
    inspectPhotoId: null,
    pipelineResults: {
      photos: [{id: 1}, {id: 2}, {id: 3}],
      encounters: [{photo_ids: [1, 2, 3], photo_count: 3, burst_count: 0, species: ['Owl']}],
    },
    reviewScopeMode: 'folder',
    reviewScopeRegrouped: false,
    reviewScopePhotoIds: new Set([1, 3]),
    minConfidence: 50,
    document: {
      getElementById: (id) => {
        if (id === 'inspectPanel') return {innerHTML: ''};
        if (id === 'inspectOverlay') return {classList: {add() {}, remove() {}, contains: () => false}};
        return null;
      },
      removeEventListener() {}, addEventListener() {},
    },
    escapeHtml: s => String(s), escapeAttr: s => String(s),
    hasQualityScore: () => false,
    buildSpeciesPredictionsHtml: () => '',
    buildBurstConsensus: () => null,
    findBurstForPhoto: () => null,
    closeInspect() {}, openGroupReview() {},
    inspectKeyHandler() {},
  });
  for (const name of [
    'findPhotoInResults', 'findEncounterForPhoto',
    'isReviewScopeFilterMode', 'activeReviewScopeFilter', 'photoInOpenGroupReview',
    'isPhotoActionableInReviewScope', 'photoIdsInReviewScope', 'openInspect',
    'buildQualityEvidenceHtml', 'buildRelativeFocusHtml', 'buildFocusCohortHtml',
    'describeReviewRejectReason',
  ]) vm.runInContext(fn(review, name), filmstripCtx);
  // Capture the panel's rendered HTML by swapping getElementById to catch
  // the write.
  let panelHtml = '';
  filmstripCtx.document.getElementById = (id) => {
    if (id === 'inspectPanel') return {set innerHTML(v) { panelHtml = v; }};
    if (id === 'inspectOverlay') return {classList: {add() {}, remove() {}, contains: () => false}};
    return null;
  };
  filmstripCtx.openInspect(1);
  assert(panelHtml.includes('Absolute sharpness: unvalidated.'), 'inspector renders real quality evidence');
  const filmstrip = panelHtml.match(/<div class="context-filmstrip">[^]*?<\/div>/);
  assert(filmstrip, 'filmstrip markup rendered');
  assert(filmstrip[0].includes('openInspect(1)'));
  assert(filmstrip[0].includes('openInspect(3)'));
  assert(!filmstrip[0].includes('openInspect(2)'),
    'out-of-scope photo 2 must not appear in the filmstrip');


  // A scope-photos membership load must not be invalidated by a slider
  // move that bumps reviewScopeRequestSeq. Otherwise the tuning response
  // renders through the pre-fetch empty scope set and the view stays at
  // zero photos until the user reselects the scope.
  let resolveScope;
  const raceCtx = vm.createContext({
    window: {},
    pipelineReviewContextPhotoIds: [],
    reviewScopeMode: 'folder',
    reviewScopeFolderId: 'f1', reviewScopeCollectionId: null,
    reviewScopeRegrouped: false, reviewScopeRequestSeq: 0,
    reviewScopeMembershipSeq: 0,
    reviewScopePhotoIds: null, reviewScopeTotal: null,
    pipelineResults: {source: 'cache'}, resultsCacheInfo: null,
    cachedPipelineResults: null, cachedResultsCacheInfo: null,
    refreshLatestScopeSnapshotIfCurrent() {},
    updateReviewScopeControls() {},
    setReviewScopeStatus() {},
    showSavedReview() {},
    applyReviewResults() {},
    loadRegroupedScopeResults() {},
    reviewScopeSelectionId: () => 'f1',
    safeFetch: () => new Promise(resolve => { resolveScope = resolve; }),
  });
  for (const name of [
    'clearPipelineReviewContextSelection',
    'loadReviewScopeResults',
  ]) vm.runInContext(fn(review, name), raceCtx);
  raceCtx.loadReviewScopeResults();
  // Slider move bumps the request seq while the membership fetch is in flight.
  raceCtx.reviewScopeRequestSeq++;
  resolveScope({photo_ids: [1, 2, 3]});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(raceCtx.reviewScopeTotal, 3);
  assert.deepEqual(Array.from(raceCtx.reviewScopePhotoIds), [1, 2, 3]);
}
main().catch(err => { console.error(err); process.exitCode = 1; });

async function testAsyncModalOwnership() {
  const navbar = 'vireo/static/lightbox/inat.js';
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
  vm.runInContext(fn('vireo/static/pipeline/readiness.js', 'updateReadiness'), ctx);
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

async function testDeepLinkFailureReleasesLoading() {
  const source = fn('vireo/static/browse/boot.js', '_runPhotoDeepLink');
  for (const failure of ['config', 'photo', 'superseded']) {
    const context = vm.createContext({
      document: {getElementById: () => ({checked: true})},
      folderHealthRefreshSeq: 1,
      loading: false,
      _cfgPromise: failure === 'config' ? Promise.reject(new Error('offline')) : Promise.resolve({}),
      applyBrowseConfig() {},
      safeFetch: async () => {
        if (failure === 'superseded') context.folderHealthRefreshSeq++;
        throw new Error('offline');
      },
    });
    context.state = context;
    vm.runInContext(source, context);
    await context._runPhotoDeepLink(1);
    // An early failure releases the grid; a newer health refresh retains
    // ownership so cleanup cannot allow competing page requests.
    assert.equal(context.loading, failure === 'superseded', failure);
  }
}

Promise.all([testAsyncModalOwnership(), testReadinessAndRegroupRaces(), testSavedCollectionPreviewScope(), testDeepLinkFailureReleasesLoading()])
  .catch(err => { console.error(err); process.exitCode = 1; });

const noteFilter = vm.createContext({});
vm.runInContext(fn('vireo/static/pipeline/completion.js', '_pipelineFailureErrors'), noteFilter);
const note = '[extract_masks] No detections to mask';
assert.equal(noteFilter._pipelineFailureErrors({errors: [note], notes: [note]}).length, 0);
assert.deepEqual(Array.from(noteFilter._pipelineFailureErrors({errors: [note, '[scan] failed'], notes: [note]})), ['[scan] failed']);

const dateViewCode = fn('vireo/static/jobs/move-route.js', 'moveDateView');
const dateView = vm.createContext({
  isLiveStatus: status => ['running', 'queued'].includes(status),
  DATE_FOLDER_ROWS: 10,
  pathUnderRoot: path => path,
  plural: (count, word) => word + (count === 1 ? '' : 's'),
});
vm.runInContext(dateViewCode, dateView);
const alreadyPlaced = dateView.moveDateView({destination: '/archive'}, {
  status: 'completed', result: {moved: 0, already_in_place: 2,
    destinations: [{path: '/archive/2026-07-12', planned: 2, moved: 0, already_in_place: 2}]},
});
assert.equal(alreadyPlaced.folderCount, 1);
assert.equal(alreadyPlaced.photoCount, 2);
assert.match(alreadyPlaced.folders[0].text, /2 already in place/);
