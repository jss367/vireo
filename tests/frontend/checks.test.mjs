import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import {ESLint} from 'eslint';
import vm from 'node:vm';
import {parse} from 'acorn';
import {moduleAsScript} from './module-source.mjs';

const eslint = new ESLint();

async function lint(code, filePath) {
  return (await eslint.lintText(code, {filePath}))[0].messages;
}

for (const file of ['browse/panel-requests.js', 'browse/panel-loaders.js', 'vireo-panel-loader.js',
  'import-recovery.js', 'pipeline-messages.js', 'map-focus.js', 'vireo-export-job.js',
  'vireo-visible-poll.js', 'vireo-thumbnail-queue.js', 'vireo-predictions.js']) {
  test(file + ': strict controllers reject undefined names and unused variables', async () => {
    const messages = await lint(
      '(function() { const unused = 1; misspelledPanelAction(); })();',
      'vireo/static/' + file,
    );
    assert.deepEqual(messages.map(message => message.ruleId).sort(), ['no-undef', 'no-unused-vars']);
  });
}

test('classic scripts accept shared globals but still reject correctness errors', async () => {
  const messages = await lint(
    'renderGrid(); switch (status) { case 1: break; case 1: break; }',
    'vireo/static/review/grid.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-duplicate-case']);
});

test('strict controllers recognize browser globals and reject undeclared names', async () => {
  assert.deepEqual(await lint(
    'window.addEventListener("click", function() {});',
    'vireo/static/vireo-panel-loader.js',
  ), []);
  const messages = await lint('acceptSelectionPrediction(0, true);', 'vireo/static/vireo-panel-loader.js');
  assert.deepEqual(messages.map(message => message.ruleId), ['no-undef']);
});

test('third-party scripts are excluded from application lint', async () => {
  assert.equal(await eslint.isPathIgnored('vireo/static/vendor/fuse.min.js'), true);
  assert.equal(await eslint.isPathIgnored('vireo/static/review/grid.js'), false);
});

// The shared navbar and lightbox are ES modules too: explicit imports, the
// classic shared scripts' globals declared, no window writes outside the
// classic bridge, and no inline handler markup.
for (const file of ['navbar-tabs.js', 'lightbox/navigation.js']) {
  test(`${file}: navbar modules reject undeclared names, window writes and inline handlers`, async () => {
    const messages = await lint(
      'import { a } from "./a.js"; export const b = a + Keymap.getScope(); misspelledHelper();'
        + ' window.x = 1; globalThis.y = 2; export const c = \'<div onclick="go()">\';'
        + ' document.body.setAttribute("onclick", "x");',
      `vireo/static/${file}`,
    );
    assert.deepEqual(messages.map(message => message.ruleId),
      ['no-undef', 'no-restricted-syntax', 'no-restricted-syntax', 'no-restricted-syntax', 'no-restricted-syntax']);
  });
}

test('navbar-globals.js is the one navbar module that may write window', async () => {
  const messages = await lint(
    'import { a } from "./a.js"; window.a = a; Object.assign(window, {a}); export const c = \'<b onclick="x">\';',
    'vireo/static/navbar-globals.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-restricted-syntax']);
});

// Module pages (frontend-module-pages.json): ES modules that share nothing
// through globals. Every listed page gets these rules from eslint.config.mjs.
const modulePages = JSON.parse(readFileSync(new URL('../../frontend-module-pages.json', import.meta.url), 'utf8'));

for (const [page, contract] of Object.entries(modulePages)) {
  const pageFile = `vireo/static/${page}/render.js`;
  const shim = `vireo/static/${page}/shared.js`;

  test(`${page}: module page files parse as modules and reject undeclared globals`, async () => {
    const messages = await lint('import { a } from "./a.js"; export const b = a; escapeHtml(b);', pageFile);
    assert.deepEqual(messages.map(message => message.ruleId), ['no-undef']);
  });

  test(`${page}: module page files may not assign globals`, async () => {
    const messages = await lint(
      'window.x = 1; window["y"] = 2; globalThis.z = 3; Object.assign(window, {});'
        + ' window.location.href = "/"; document.title = "t";',
      pageFile,
    );
    assert.deepEqual(messages.map(message => message.ruleId), Array(4).fill('no-restricted-syntax'));
  });

  test(`${page}: module page files may not use inline event handlers`, async () => {
    const messages = await lint(
      'export const a = \'<div onclick="go()">\'; export const b = `<b ondblclick="x">`;'
        + ' document.body.onclick = null; document.body.setAttribute("onclick", "x");'
        + ' export const c = "<button type=button>";',
      pageFile,
    );
    assert.deepEqual(messages.map(message => message.ruleId), Array(4).fill('no-restricted-syntax'));
  });

  test(`${page}: shared.js writes only the listed shared latches, never its page hooks`, async () => {
    const allowed = contract.sharedWrites.map(name => `window.${name} = null;`).join(' ');
    assert.deepEqual(await lint(allowed, shim), []);
    const hook = contract.pageHooks[0] || 'afterHistoryChange';
    const messages = await lint(
      `window.notAHook = 1; window["${hook}"] = 2; window.${hook} = 3;`
        + ' window.Vireo.registerPageHooks({});',
      shim,
    );
    assert.deepEqual(messages.map(message => message.ruleId), Array(3).fill('no-restricted-syntax'));
  });
}


for (const page of Object.keys(modulePages)) {
  for (const file of ['render.js', 'shared.js']) {
    test(`${page}/${file}: global mutation forms are rejected`, async () => {
      const messages = await lint(
        'window.x++; --globalThis.y; delete self.z; Reflect.set(window, "x", 1);'
          + ' Reflect.defineProperty(self, "y", {}); Reflect.deleteProperty(globalThis, "z");',
        `vireo/static/${page}/${file}`,
      );
      assert.deepEqual(messages.map(message => message.ruleId), Array(6).fill('no-restricted-syntax'));
    });
    test(`${page}/${file}: mixed-case inline handler forms are rejected`, async () => {
      const messages = await lint(
        'export const a = \'<div ONCLICK="go()">\'; export const b = `<b OnDblClick="x">`;'
          + ' document.body.OnClick = null; document.body["ONCLICK"] = null;'
          + ' document.body.setAttribute("OnClick", "x");',
        `vireo/static/${page}/${file}`,
      );
      assert.deepEqual(messages.map(message => message.ruleId), Array(5).fill('no-restricted-syntax'));
    });
  }
}

// Subject audit checks use controlled responses to exercise their concurrent callers.
function requestQueue() {
  const requests = [];
  return {requests, fetch: () => new Promise((resolve, reject) => requests.push({resolve, reject}))};
}

function subjectHarness() {
  const queue = requestQueue();
  const elements = {btnSubjects: {disabled: false}, subjectStatus: {style: {}},
    subjectContent: {replaceChildren() {}, append() {}}, btnRunAll: {}, summaryNote: {}};
  const phases = [];
  const context = vm.createContext({
    byId: id => elements[id], safeFetch: queue.fetch,
    loadSummary: async () => phases.push('summary'),
    showCheckError: (status, error) => { status.textContent = String(error); status.style.color = 'var(--danger)'; },
    document: {createElement: () => ({addEventListener() {}, append() {}})},
  });
  vm.runInContext(moduleAsScript(readFileSync('vireo/static/audit/subjects.js', 'utf8')), context);
  const summary = readFileSync('vireo/static/audit/summary.js', 'utf8');
  const tree = parse(summary, {ecmaVersion: 'latest', sourceType: 'module'});
  const runAll = tree.body.find(n => n.declaration?.id?.name === 'runAllChecks').declaration;
  for (const name of ['runDriftCheck', 'runOrphanCheck', 'runUntrackedCheck', 'runSidecarCheck',
    'runClockCheck', 'runGpsCheck', 'runHashVerify']) context[name] = async () => phases.push(name);
  vm.runInContext(summary.slice(runAll.start, runAll.end), context);
  return {context, elements, phases, requests: queue.requests};
}

const emptyReport = {findings: [], review: [], references: []};

test('Run All awaits an already running subject check and a later click starts a fresh check', async () => {
  const h = subjectHarness();
  const manual = h.context.runSubjectCheck();
  let complete = false;
  const all = h.context.runAllChecks().then(() => { complete = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(h.requests.length, 1);
  assert.equal(complete, false);
  assert.equal(h.phases.includes('runClockCheck'), false);
  h.requests[0].resolve(emptyReport);
  await Promise.all([manual, all]);
  assert.equal(complete, true);
  assert.equal(h.elements.btnSubjects.disabled, false);
  const next = h.context.runSubjectCheck();
  assert.equal(h.requests.length, 2);
  h.requests[1].resolve(emptyReport);
  await next;
});

test('retry clears the earlier error color both during loading and after success', async () => {
  const h = subjectHarness();
  const first = h.context.runSubjectCheck();
  h.requests[0].reject(new Error('synthetic failure'));
  await first;
  assert.equal(h.elements.subjectStatus.style.color, 'var(--danger)');
  const retry = h.context.runSubjectCheck();
  assert.equal(h.elements.subjectStatus.style.color, '');
  h.requests[1].resolve(emptyReport);
  await retry;
  assert.equal(h.elements.subjectStatus.style.color, '');
});

function countsHarness() {
  const queue = requestQueue();
  class Details { open = false; addEventListener(_type, callback) { this.toggle = callback; } }
  const details = new Details();
  const result = {};
  const context = {safeFetch: queue.fetch, HTMLDetailsElement: Details,
    Vireo: {registerPageHooks() {}}, window: {addEventListener() {}},
    document: {addEventListener() {}, getElementById: id => id === 'subjectCounts' ? details : result}};
  for (const name of ['startPipeline', 'initPipelinePage', 'refreshPipelineUI', 'updateAdvancedPipelineOptions',
    'bindSlotPolling']) context[name] = () => {};
  vm.runInNewContext(readFileSync('vireo/static/pipeline/boot.js', 'utf8'), context);
  return {requests: queue.requests, result, toggle(open) { details.open = open; return details.toggle({target: details}); }};
}

for (const lateFailure of [false, true]) {
  test(`an older counts ${lateFailure ? 'failure' : 'success'} cannot replace newer evidence`, async () => {
    const h = countsHarness();
    const old = h.toggle(true);
    await h.toggle(false);
    const latest = h.toggle(true);
    h.requests[1].resolve({unnamed: 7, stale_mask: 8, stale_score: 9});
    await latest;
    const text = h.result.textContent;
    if (lateFailure) h.requests[0].reject(new Error('old failure'));
    else h.requests[0].resolve({unnamed: 1, stale_mask: 2, stale_score: 3});
    await old;
    assert.equal(h.result.textContent, text);
    assert.match(text, /^7 unclassified/);
  });
}
