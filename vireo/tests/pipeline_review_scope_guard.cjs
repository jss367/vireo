// Pipeline Review's Folder/Collection scope hides photos but keeps the saved
// review editable. This loads every Pipeline Review script the template loads
// (except boot.js, which starts requests) into one context, the way the page
// does, then drives every entry point that can turn a photo ID into a write
// target with the ID of a photo the scope hides. No request may name it.
//
// A new entry point that skips the scope guard in scope.js fails here, not
// one Codex review round later.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const scripts = Array.from(
  fs.readFileSync('vireo/templates/pipeline_review.html', 'utf8')
    .matchAll(/<script src="\/static\/(pipeline-review\/[^"]+\.js)"><\/script>/g),
  match => 'vireo/static/' + match[1],
).filter(file => !file.endsWith('/boot.js'));
assert(scripts.length > 10, 'Pipeline Review scripts must be loaded by the template');

const IN_SCOPE = 1;
const HIDDEN = 2;

function classList(initial) {
  const set = new Set(initial || []);
  return {
    add: c => set.add(c), remove: c => set.delete(c),
    contains: c => set.has(c), toggle: c => (set.has(c) ? set.delete(c) : set.add(c)),
  };
}

function makePage() {
  const elements = new Map();
  const element = id => {
    if (!elements.has(id)) {
      elements.set(id, {
        id, innerHTML: '', textContent: '', value: '', style: {}, dataset: {},
        classList: classList(), focus() {}, setAttribute() {}, appendChild() {},
        querySelector: () => null, querySelectorAll: () => [],
      });
    }
    return elements.get(id);
  };
  const requests = [];
  const listeners = {};
  const toasts = [];
  const lightboxOpens = [];
  const location = {href: '/pipeline/review'};
  const ctx = vm.createContext({
    console, setTimeout: fn => { fn(); return 0; }, clearTimeout() {},
    window: {location},
    location,
    document: {
      getElementById: element,
      querySelector: () => null, querySelectorAll: () => [],
      addEventListener(type, fn) { listeners[type] = fn; }, removeEventListener() {},
      createElement: () => element('created-' + Math.random()),
    },
    localStorage: {getItem: () => null, setItem() {}},
    sessionStorage: {getItem: () => null, setItem() {}},
    navigator: {clipboard: {writeText: async () => {}}},
    escapeHtml: s => String(s == null ? '' : s),
    escapeAttr: s => String(s == null ? '' : s),
    showToast: (msg, kind) => toasts.push([msg, kind]),
    openLightbox: (...args) => lightboxOpens.push(args),
    safeFetch: async (url, opts) => {
      const method = (opts && opts.method) || 'GET';
      const body = opts && opts.body ? JSON.parse(opts.body) : null;
      requests.push({url, method, body});
      if (url === '/api/collections') return [];
      if (url === '/api/pipeline/group/state') {
        const photos = {};
        (body.photo_ids || []).forEach(id => { photos[id] = {flag: 'none'}; });
        return {photos};
      }
      return {ok: true};
    },
  });
  for (const file of scripts) {
    vm.runInContext(fs.readFileSync(file, 'utf8'), ctx, {filename: file});
  }
  // The grid renderer needs the full page DOM; these checks are about which
  // requests go out, so stub the redraws.
  ctx.renderResults = () => {};
  ctx.updateSummaryBar = () => {};
  ctx.renderGroupModal = () => {};
  ctx.grmSyncZoneCards = () => {};
  // The shared navbar's native-menu resolver (navbar-shared-helpers.js) asks
  // the page for its selection through getActiveSelection().
  ctx.nativeMenuActivePhotoIds = () => ctx.getActiveSelection();

  vm.runInContext(`
    pipelineResults = {
      source: 'cache',
      photos: [
        {id: 1, filename: 'a.jpg', label: 'REVIEW', flag: 'none', rating: 0},
        {id: 2, filename: 'b.jpg', label: 'REVIEW', flag: 'none', rating: 0},
        {id: 3, filename: 'c.jpg', label: 'REVIEW', flag: 'none', rating: 0},
      ],
      encounters: [{
        photo_ids: [1, 2, 3], photo_count: 3, burst_count: 0, bursts: [],
        species: ['Owl'],
        trace: [{decision: 'kept', dt_seconds: 1, score: 0.5},
                {decision: 'kept', dt_seconds: 1, score: 0.5}],
      }],
      summary: {total_photos: 3},
    };
    reviewScopeMode = 'folder';
    reviewScopeFolderId = 'f1';
    reviewScopeRegrouped = false;
    reviewScopePhotoIds = new Set([${IN_SCOPE}]);
    reviewScopeTotal = 1;
  `, ctx);
  return {ctx, requests, toasts, lightboxOpens, element, location, listeners};
}

function run(ctx, code) { return vm.runInContext(code, ctx); }

function namesHidden(req) {
  const text = req.url + ' ' + JSON.stringify(req.body);
  return new RegExp('(^|\\D)' + HIDDEN + '(\\D|$)').test(text);
}

function writes(requests) {
  return requests.filter(r => r.method !== 'GET');
}

async function settle() {
  for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve));
}

async function hiddenPhotoIsNeverAWriteTarget() {
  const {ctx, requests, lightboxOpens, element, location} = makePage();

  // Selection sources the native Photo menu reads.
  run(ctx, `pipelineReviewContextPhotoIds = [${HIDDEN}]`);
  assert.deepEqual(Array.from(ctx.getActiveSelection()), []);
  run(ctx, `window._vireoNativeMenuPhotoIdsOverride = [${HIDDEN}, ${IN_SCOPE}]`);
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [IN_SCOPE]);
  run(ctx, `window._vireoNativeMenuPhotoIdsOverride = null; inspectPhotoId = ${HIDDEN}`);
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [],
    'a stale inspectPhotoId must not become the native menu target');

  // Native menu commands that resolve through that selection.
  run(ctx, `pipelineReviewContextPhotoIds = [${HIDDEN}]; inspectPhotoId = null`);
  await ctx.batchSetRating(4);
  await ctx.batchSetFlag('rejected');
  await ctx.addToCollection();
  ctx.batchAddKeyword();
  assert(!element('pipelineKeywordModal').classList.contains('open'));
  assert(!element('pipelineCollectionModal').classList.contains('open'));

  // The inspector: opening it (card click, filmstrip, trace link) and its
  // P/X shortcuts on a stale inspectPhotoId.
  ctx.openInspect(HIDDEN);
  assert.equal(run(ctx, 'inspectPhotoId'), null);
  assert(!element('inspectOverlay').classList.contains('open'));
  run(ctx, `inspectPhotoId = ${HIDDEN}`);
  ctx.inspectKeyHandler({key: 'x', target: {tagName: 'DIV'}, preventDefault() {}, stopPropagation() {}});
  run(ctx, 'inspectPhotoId = null');

  // Algorithm trace links render no openInspect() for the hidden photo.
  run(ctx, '_focusedEncounterIdx = 0');
  ctx.renderAlgorithmTrace();
  assert(element('algorithmTrace').innerHTML.includes('openInspect(' + IN_SCOPE + ')'));
  assert(!element('algorithmTrace').innerHTML.includes('openInspect(' + HIDDEN + ')'));

  // Direct writers, as the shared lightbox and native menu call them.
  assert.equal(await ctx.setPipelineReviewFlag(HIDDEN, 'flagged'), false);
  assert.equal(await ctx.setRatingFor(HIDDEN, 3), false);
  assert.equal(await ctx.setWildlifeExcludedFor(HIDDEN, true), false);
  await ctx.detachPhoto(0, null, HIDDEN);
  await ctx.setPipelineReviewGroupFlag(
    {key: 'encounter:0', noun: 'encounter', photoIds: [IN_SCOPE, HIDDEN]}, 'rejected');

  // Organization dialogs whose photos left scope while they were open.
  run(ctx, `pipelineReviewKeywordPhotoIds = [${HIDDEN}]`);
  element('pipelineKeywordInput').value = 'tag';
  await ctx.confirmPipelineKeyword();
  run(ctx, `pipelineReviewCollectionPhotoIds = [${HIDDEN}]; pipelineReviewSelectedCollectionId = 9`);
  element('pipelineCollectionNewName').value = '';
  await ctx.confirmPipelineCollection();

  // Lightbox and editor.
  assert.equal(ctx.openPipelineLightbox(HIDDEN), false);
  assert.equal(lightboxOpens.length, 0);
  assert.equal(ctx.pipelineReviewLightboxOptions(HIDDEN).readOnly, true,
    'Find Similar must open a hidden photo read-only');
  assert.equal(ctx.openPipelinePhotoEditor(HIDDEN), false);
  assert.equal(location.href, '/pipeline/review');

  // Every enabled action of a context menu built for the hidden photo.
  const items = ctx.buildPipelinePhotoContextMenu([HIDDEN], HIDDEN, false);
  for (const item of items) {
    if (item.chips) item.chips.forEach(chip => !chip.disabled && chip.onClick());
    else if (item.onClick && !item.disabled) await item.onClick();
  }

  await settle();
  const leaked = requests.filter(namesHidden);
  assert.deepEqual(leaked, [], 'requests named the hidden photo: ' + JSON.stringify(leaked));
  // The only write that went out is the group reject, narrowed to the
  // in-scope photo.
  assert.deepEqual(writes(requests).map(r => r.url),
    ['/api/pipeline/group/state', '/api/batch/flag']);
  assert.deepEqual(writes(requests)[1].body.photo_ids, [IN_SCOPE]);
}

async function inScopePhotosStayWritable() {
  const {ctx, requests} = makePage();
  run(ctx, `pipelineReviewContextPhotoIds = [${IN_SCOPE}]`);
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [IN_SCOPE]);
  await ctx.setPipelineReviewFlag(IN_SCOPE, 'flagged');
  await ctx.setRatingFor(IN_SCOPE, 2);
  ctx.openInspect(IN_SCOPE);
  assert.equal(run(ctx, 'inspectPhotoId'), IN_SCOPE);
  await settle();
  assert.deepEqual(writes(requests).map(r => r.url),
    ['/api/photos/' + IN_SCOPE + '/flag', '/api/batch/rating']);
}

async function groupReviewStillEditsTheWholeBurst() {
  // Group Review deliberately shows and edits the whole burst, including
  // members outside the scope.
  const {ctx, requests, element} = makePage();
  element('grmOverlay').classList.add('open');
  run(ctx, `grmState.items = [{id: ${IN_SCOPE}}, {id: ${HIDDEN}}];
            grmState.selectedIds = new Set([${HIDDEN}]); grmState.selected = ${HIDDEN};`);
  run(ctx, `window._vireoNativeMenuPhotoIdsOverride = [${HIDDEN}]`);
  assert.deepEqual(Array.from(ctx.getActiveSelection()), [HIDDEN]);
  run(ctx, 'window._vireoNativeMenuPhotoIdsOverride = null');
  const staged = await ctx.setPipelineReviewFlag(HIDDEN, 'rejected');
  assert.equal(staged && staged.provisional, true);
  assert(run(ctx, `grmState.rejects.has(${HIDDEN})`));
  await ctx.setPipelineReviewRating([HIDDEN], 5);
  await settle();
  assert.deepEqual(writes(requests).map(r => [r.url, r.body.photo_ids]),
    [['/api/batch/rating', [HIDDEN]]]);
}

async function allReviewedPhotosIsUnfiltered() {
  const {ctx, requests} = makePage();
  run(ctx, "reviewScopeMode = 'cache'; reviewScopePhotoIds = null;");
  await ctx.setPipelineReviewFlag(HIDDEN, 'flagged');
  await settle();
  assert.deepEqual(writes(requests).map(r => r.url), ['/api/photos/' + HIDDEN + '/flag']);
}

async function membershipRefreshDropsTheInspectedPhoto() {
  const {ctx, element} = makePage();
  run(ctx, `reviewScopeMode = 'collection'; reviewScopeCollectionId = 'c1';
            reviewScopePhotoIds = new Set([${IN_SCOPE}, ${HIDDEN}]);
            pipelineReviewContextPhotoIds = [${HIDDEN}];`);
  ctx.openInspect(HIDDEN);
  assert.equal(run(ctx, 'inspectPhotoId'), HIDDEN);
  ctx.safeFetch = async () => ({photo_ids: [IN_SCOPE]});
  ctx.maybeRefreshCollectionScopeMembership();
  await settle();
  assert.equal(run(ctx, 'inspectPhotoId'), null);
  assert(!element('inspectOverlay').classList.contains('open'));
  assert.deepEqual(Array.from(run(ctx, 'pipelineReviewContextPhotoIds')), []);
}

async function staleMembershipRefreshIsDropped() {
  // A refresh that started before a scope load (cache and back to the same
  // collection) must not overwrite the membership that load brought back.
  const {ctx} = makePage();
  run(ctx, `reviewScopeMode = 'collection'; reviewScopeCollectionId = 'c1';
            reviewScopePhotoIds = new Set([${IN_SCOPE}, ${HIDDEN}]);`);
  let answerRefresh;
  ctx.safeFetch = () => new Promise(resolve => { answerRefresh = resolve; });
  ctx.maybeRefreshCollectionScopeMembership();
  // The newer scope load lands first.
  run(ctx, `++reviewScopeMembershipSeq; reviewScopePhotoIds = new Set([${IN_SCOPE}]);`);
  answerRefresh({photo_ids: [IN_SCOPE, HIDDEN, 3]});
  await settle();
  assert.deepEqual(Array.from(run(ctx, 'reviewScopePhotoIds')), [IN_SCOPE]);
}

(async () => {
  await hiddenPhotoIsNeverAWriteTarget();
  await inScopePhotosStayWritable();
  await groupReviewStillEditsTheWholeBurst();
  await allReviewedPhotosIsUnfiltered();
  await membershipRefreshDropsTheInspectedPhoto();
  await staleMembershipRefreshIsDropped();
})().catch(err => { console.error(err); process.exitCode = 1; });
