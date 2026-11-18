import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';
import {moduleAsScript} from './module-source.mjs';

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
