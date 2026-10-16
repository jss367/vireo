const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const scripts = ['state.js', 'format.js', 'import-retry.js', 'job-card.js', 'actions.js'];
let clickHandler;
let fetchCount = 0;
let resolveRequest;
let request;
const toasts = [];
const ctx = vm.createContext({
  console,
  document: {
    getElementById() { return {addEventListener(_type, handler) { clickHandler = handler; }}; },
    createElement() {
      return {get innerHTML() {
        return String(this.textContent).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
      }};
    },
  },
  // Stand-ins for the shared helpers in vireo-utils.js, which the page loads
  // from the navbar before any jobs/ script.
  escapeHtml: value => String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  escapeAttr: value => String(value).replaceAll('"', '&quot;'),
  formatJobType: () => 'Import In Place',
  renderMoveRoute: () => '', renderSourceCleanup: () => '',
  renderRichStep: () => '', renderStep: () => '', renderResultDetails: () => '',
  showToast: (...args) => toasts.push(args),
  renderHistoryDetail: () => {},
  fetchJobs: () => {}, fetchHistory: () => {},
  fetch: (url, options) => {
    fetchCount++;
    request = {url, options};
    return new Promise(resolve => { resolveRequest = resolve; });
  },
});
ctx.window = ctx;
vm.runInContext(fs.readFileSync('vireo/static/import-recovery.js', 'utf8'), ctx);
for (const file of scripts) {
  vm.runInContext(fs.readFileSync('vireo/static/jobs/' + file, 'utf8'), ctx);
}
const parent = {
  id: 'old-job', type: 'import-in-place', status: 'failed',
  config: {sources: ['/photos'], recursive: false, after_import: null},
  result: {interrupted: true}, tree: [], progress: {current: 27365, total: 94747},
};
ctx.parent = parent;
ctx.historyJobs = [parent];
assert(ctx.canRestartInPlace(parent));
assert(ctx.canRestartInPlace({...parent, status: 'cancelled'}));
assert(!ctx.canRestartInPlace({...parent, status: 'completed'}));
assert(!ctx.canRestartInPlace({...parent, config: {}}));
assert(!ctx.canRestartInPlace({...parent, type: 'pipeline'}));
assert(ctx.canRestartInPlace({...parent, config: {source_snapshot_id: 7}}));
const card = ctx.renderJobCard(parent, false);
assert.match(card, /Restart import/);
assert.match(card, /reusing unchanged catalog records/);
assert.match(card, /Progress starts over/);

ctx.bindDetailPaneActions();
const button = {
  disabled: false, textContent: 'Restart import',
  getAttribute() { return parent.id; },
};
const event = {target: {closest(selector) { return selector === '[data-restart-in-place]' ? button : null; }}};

async function flush() { for (let i = 0; i < 10; i++) await Promise.resolve(); }
async function main() {
  clickHandler(event);
  assert(button.disabled);
  assert.equal(button.textContent, 'Restarting…');
  assert.match(ctx.renderJobCard(parent, false), /disabled aria-disabled="true"/);
  clickHandler(event);
  assert.equal(fetchCount, 1);
  assert.equal(request.url, '/api/jobs/import-in-place/old-job/restart');
  assert.equal(request.options.method, 'POST');
  // No frontend reconstruction of settings: the server owns the saved scope.
  assert.deepEqual(JSON.parse(request.options.body), {});

  resolveRequest({ok: false, json: () => Promise.resolve({error: 'Source is missing'})});
  await flush();
  assert(!button.disabled);
  assert.equal(button.textContent, 'Restart import');
  assert(!ctx.inPlaceRestarts[parent.id]);
  assert.equal(toasts.at(-1)[0], 'Source is missing');

  clickHandler(event);
  // A stale tab gets the existing child from the server and follows it.
  resolveRequest({ok: false, json: () => Promise.resolve({restart_job_id: 'new-job'})});
  await flush();
  assert.equal(ctx.selectedJobId, 'new-job');
  assert.equal(ctx.inPlaceRestarts[parent.id].jobId, 'new-job');
  assert.match(ctx.renderJobCard(parent, false), /Show restarted import/);
  assert(!ctx.renderJobCard(parent, false).includes('data-restart-in-place'));

  ctx.inPlaceRestarts = {};
  ctx.activeJobs = [{id: 'persisted-child', type: 'import-in-place', status: 'running',
    config: {restarted_from: parent.id}}];
  assert.equal(ctx.inPlaceRestartJobId(parent), 'persisted-child');
  assert.match(ctx.renderJobCard(parent, false), /Show restarted import/);
  ctx.activeJobs = [];
  ctx.historyJobs.push({id: 'finished-child', type: 'import-in-place', status: 'completed',
    config: {restarted_from: parent.id}});
  assert.equal(ctx.inPlaceRestartJobId(parent), 'finished-child');
}
main().catch(error => { console.error(error); process.exitCode = 1; });
