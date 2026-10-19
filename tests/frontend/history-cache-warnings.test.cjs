// Selected real declarations with fake DOM, HTTP, history responses and cache refresh.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');
const {parse} = require('acorn');

function selected(file, accept, sourceType = 'script') {
  const source = readFileSync(file, 'utf8');
  return parse(source, {ecmaVersion: 'latest', sourceType}).body
    .filter(accept).map(node => source.slice(node.start, node.end)).join('\n');
}

function harness() {
  const banner = {style: {display: 'none'}}, text = {innerHTML: ''}, badge = {textContent: ''};
  const nodes = {historyCacheBanner: banner, historyCacheBannerText: text, historyBadge: badge};
  const hooks = {}, requests = [], toasts = [], applied = [];
  const replies = [], state = {refreshError: null};
  const ctx = vm.createContext({
    document: {getElementById: id => nodes[id], querySelector: () => null, dispatchEvent() {}},
    escapeHtml: value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    Vireo: {registerPageHooks: value => Object.assign(hooks, value), pageHook: key => hooks[key]},
    reviewScopeRequestSeq: 0, pipelineReviewGroupFlagInFlight: {},
    cloneReviewData: value => JSON.parse(JSON.stringify(value)),
    isScopedReviewView: () => false, applyReviewResults: (...args) => applied.push(args),
    loadReviewScopeResults() { throw new Error('unexpected scope change'); },
    safeFetch: async (url, options) => {
      requests.push([url, options]);
      if (url === '/api/pipeline/page-init') {
        if (state.refreshError) throw state.refreshError;
        return {results: {invented: true}, results_cache_info: {invented: true}};
      }
      assert.ok(url === '/api/undo' || url === '/api/redo');
      assert.equal(options.method, 'POST');
      assert.ok(replies.length, 'missing invented history response');
      return replies.shift();
    },
    showToast: (...args) => toasts.push(args), historyBusy: false, historyPendingWrites: 0,
    renderHistoryControls() {}, refreshHistoryControls: async () => {},
    _lbRefreshEditRecipeCache() {}, lbState: {flagEditSeq: 0}, _historySessionCount: 2,
    loadEditHistory() {}, refreshHistoryLightbox: async () => {},
    CustomEvent: class { constructor(type, init) { this.type = type; this.detail = init.detail; } },
  });
  vm.runInContext(selected('vireo/static/pipeline-review/scope.js', node =>
    node.type === 'FunctionDeclaration' && node.id.name === 'renderHistoryCacheStaleBanner' ||
    node.type === 'VariableDeclaration' && node.declarations.some(item => item.id.name.startsWith('_historyCacheStaleEntryId'))), ctx);
  vm.runInContext(selected('vireo/static/pipeline-review/species-editor.js', node =>
    node.type === 'FunctionDeclaration' && node.id.name === 'registerPipelineReviewHistoryHooks'), ctx);
  ctx.registerPipelineReviewHistoryHooks();
  vm.runInContext(selected('vireo/static/navbar-shared-helpers.js', node =>
    node.type === 'FunctionDeclaration' && node.id.name === 'changeHistory', 'module'), ctx);
  return {
    banner, text, requests, toasts, applied, state,
    change: body => hooks.afterHistoryChange(body),
    history: async (operation, body) => { replies.push(body); return ctx.changeHistory(operation); },
  };
}

function response(id, failed = false, operation = 'undone') {
  return {ok: true, [operation]: 'invented edit ' + String(id),
    ...(id == null ? {} : {entry_id: id}), ...(failed ? {pipeline_cache_stale: true, warning: 'invented stale cache'} : {})};
}

for (const failures of [[2, 1], [1, 2]]) {
  for (const repairs of [[1, 2], [2, 1]]) {
    for (const operation of ['undone', 'redone']) {
      test('all outstanding IDs survive unrelated success; failures ' + failures + ', repairs ' + repairs + ', ' + operation, async () => {
        const h = harness();
        for (const id of failures) await h.change(response(id, true, operation));
        assert.equal(h.banner.style.display, 'flex');
        await h.change(response(99, false, operation));
        assert.equal(h.banner.style.display, 'flex');
        await h.change(response(repairs[0], false, operation === 'undone' ? 'redone' : 'undone'));
        assert.equal(h.banner.style.display, 'flex');
        await h.change(response(repairs[1], false, operation === 'undone' ? 'redone' : 'undone'));
        assert.equal(h.banner.style.display, 'none');
        assert.equal(h.applied.length, 5);
      });
    }
  }
}

test('repeated failures for one entry require only its successful repair', async () => {
  const h = harness();
  await h.change(response(1, true));
  await h.change(response(1, true, 'redone'));
  await h.change(response(99));
  assert.equal(h.banner.style.display, 'flex');
  await h.change(response(1));
  assert.equal(h.banner.style.display, 'none');
});

for (const missingFirst of [false, true]) {
  test('unidentified failure survives known repairs; missing first=' + missingFirst, async () => {
    const h = harness();
    for (const id of missingFirst ? [undefined, 1, 2] : [1, 2, undefined]) await h.change(response(id, true));
    await h.change(response(1));
    await h.change(response(2));
    await h.change(response(undefined));
    assert.equal(h.banner.style.display, 'flex');
  });
}

test('success without an ID cannot repair a known entry', async () => {
  const h = harness();
  await h.change(response(1, true));
  await h.change(response(undefined));
  assert.equal(h.banner.style.display, 'flex');
  await h.change(response(1));
  assert.equal(h.banner.style.display, 'none');
});

test('null-ID failure remains conservative after a known inverse', async () => {
  const h = harness();
  await h.change(response(1, true));
  await h.change({...response(undefined, true), entry_id: null});
  await h.change(response(1));
  assert.equal(h.banner.style.display, 'flex');
});

test('failed inverse and repeated failures never acknowledge another entry', async () => {
  const h = harness();
  await h.change(response(2, true));
  await h.change(response(1, true));
  await h.change(response(1, true, 'redone'));
  await h.change(response(1, false, 'undone'));
  assert.equal(h.banner.style.display, 'flex');
  await h.change(response(2, false, 'redone'));
  assert.equal(h.banner.style.display, 'none');
});

test('page-init HTTP failure preserves the warning already recorded', async () => {
  const h = harness(), failure = new Error('invented page-init failure');
  h.state.refreshError = failure;
  await assert.rejects(h.change(response(1, true)), error => error === failure);
  assert.equal(h.banner.style.display, 'flex');
  h.state.refreshError = null;
  await h.change(response(2));
  assert.equal(h.banner.style.display, 'flex');
  await h.change(response(1, false, 'redone'));
  assert.equal(h.banner.style.display, 'none');
});

test('real history hook forwards HTTP entry IDs without dropping outstanding warnings', async () => {
  const h = harness();
  assert.equal(await h.history('undo', response(2, true)), true);
  assert.equal(await h.history('undo', response(1, true)), true);
  assert.equal(await h.history('redo', response(1, false, 'redone')), true);
  assert.equal(h.banner.style.display, 'flex');
  assert.equal(await h.history('redo', response(2, false, 'redone')), true);
  assert.equal(h.banner.style.display, 'none');
  assert.equal(h.requests.filter(([url]) => url === '/api/pipeline/page-init').length, 4);
  assert.equal(h.toasts.filter(([, type]) => type === 'warning').length, 2);
});

test('no-change and unrelated successful history do not invent a warning', async () => {
  const h = harness();
  await h.change(null);
  await h.change(undefined);
  await h.change(response(77));
  assert.equal(h.banner.style.display, 'none');
});
