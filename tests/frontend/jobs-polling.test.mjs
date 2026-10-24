import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function harness() {
  const requests = [];
  const timers = new Map();
  const elements = Object.fromEntries(['jobsPollWarning', 'jobsPollMessage', 'retryJobsPolling', 'keepingAwakeNote']
    .map(id => [id, {hidden: true, textContent: '', style: {}}]));
  const renders = [];
  const context = vm.createContext({
    AbortController,
    document: {getElementById: id => elements[id]},
    setTimeout: callback => { const id = Symbol(); timers.set(id, callback); return id; },
    clearTimeout: id => timers.delete(id),
    // Deliberately allow retired requests to finish: ownership cannot depend
    // on abort being honored by every transport/body decoder.
    fetch: (url, options) => new Promise((resolve, reject) => requests.push({url, options, resolve, reject})),
    updateList() {}, updateRunningBadge() {}, populateTypeFilter() {},
    renderDetail: job => renders.push(['active', job.id]),
    renderHistoryDetail: job => renders.push(['history', job.id]),
    renderActiveOverview: () => renders.push(['active-overview']),
    renderHistoryOverview: () => renders.push(['history-overview']),
    canRetryImportDirectToNas: () => false,
  });
  for (const name of ['state', 'polling']) {
    vm.runInContext(readFileSync(`vireo/static/jobs/${name}.js`, 'utf8'), context);
  }
  function respond(index, data, ok = true) {
    requests[index].resolve({ok, json: async () => data});
  }
  return {context, requests, timers, elements, renders, respond};
}

const row = (id, extra = {}) => ({id, type: 'scan', status: 'running', ...extra});
const payload = rows => ({active: rows, active_workspace_id: 0, workspace_names: {}, keeping_awake: false});

for (const lane of ['active', 'history']) {
  const method = lane === 'active' ? 'fetchJobs' : 'fetchHistory';
  const pack = rows => lane === 'active' ? payload(rows) : rows;
  const key = lane === 'active' ? 'activeJobs' : 'historyJobs';

  test(`${lane}: latest refresh owns data, metadata, errors and pending state`, async () => {
    const h = harness();
    const old = h.context[method]();
    const current = h.context[method]();
    assert.equal(h.requests[0].options.signal.aborted, true);
    h.respond(1, pack([row('new')]));
    await current;
    h.respond(0, pack([row('old')]));
    await old;
    assert.equal(h.context[key][0].id, 'new');
    assert.equal(h.context.jobsPollState[lane].error, false);
    assert.equal(h.elements.jobsPollWarning.hidden, true);
    if (lane === 'active') assert.equal(h.context.activeWsId, 0);

    const stale = h.context[method]();
    const pending = h.context[method]();
    h.requests[2].reject(new Error('old offline response'));
    await stale;
    assert.equal(h.context.jobsPollState[lane].pending, true);
    assert.equal(h.elements.jobsPollWarning.hidden, true);
    h.respond(3, pack([row('latest')]));
    await pending;
    assert.equal(h.context[key][0].id, 'latest');
    assert.equal(h.timers.size, 0);
  });

  for (const failure of ['network', 'http', 'json', 'shape', 'row']) {
    test(`${lane}: ${failure} failure preserves the last list and recovers`, async () => {
      const h = harness();
      const first = h.context[method]();
      h.respond(0, pack([row('kept')]));
      await first;
      const failed = h.context[method]();
      if (failure === 'network') h.requests[1].reject(new Error('offline'));
      if (failure === 'http') h.respond(1, pack([]), false);
      if (failure === 'json') h.requests[1].resolve({ok: true, json: async () => { throw new Error('bad JSON'); }});
      if (failure === 'shape') h.respond(1, {});
      if (failure === 'row') h.respond(1, pack([null]));
      assert.equal(await failed, false);
      assert.equal(h.context[key][0].id, 'kept');
      assert.equal(h.elements.jobsPollWarning.hidden, false);
      assert.match(h.elements.jobsPollMessage.textContent, /Showing the last successful list/);
      h.context.retryJobsPolling();
      h.context.retryJobsPolling();
      assert.equal(h.requests.length, 3);
      assert.equal(h.elements.retryJobsPolling.disabled, true);
      h.respond(2, pack([]));
      // retryJobsPolling is a click handler; drain its fetch/body/finally work.
      await new Promise(resolve => setImmediate(resolve));
      assert.equal(h.context[key].length, 0);
      assert.equal(h.elements.jobsPollWarning.hidden, true);
      assert.equal(h.timers.size, 0);
    });
  }

  test(`${lane}: scheduled polls wait for a slow request; timeout permits recovery`, async () => {
    const h = harness();
    const pending = h.context[method]();
    const skipped = lane === 'active' ? h.context.fetchJobs(true) : h.context.fetchHistory(false, true);
    assert.equal(await skipped, false);
    assert.equal(h.requests.length, 1);
    h.requests[0].options.signal.addEventListener('abort', () => h.requests[0].reject(new Error('timeout')));
    [...h.timers.values()][0]();
    await pending;
    assert.equal(h.context.jobsPollState[lane].pending, false);
    assert.match(h.elements.jobsPollMessage.textContent, /could not be loaded/);
    const retry = h.context[method]();
    h.respond(1, pack([]));
    await retry;
    assert.equal(h.elements.jobsPollWarning.hidden, true);
  });
}

test('successful active polls cannot hide a history error (and vice versa)', async () => {
  const h = harness();
  const active = h.context.fetchJobs();
  const history = h.context.fetchHistory();
  h.requests[0].reject(new Error('offline'));
  h.requests[1].reject(new Error('offline'));
  await Promise.all([active, history]);
  assert.match(h.elements.jobsPollMessage.textContent, /Active jobs could not be loaded/);
  assert.match(h.elements.jobsPollMessage.textContent, /Job history could not be loaded/);
  const retry = h.context.fetchJobs();
  h.respond(2, payload([]));
  await retry;
  assert.equal(h.elements.jobsPollWarning.hidden, false);
  assert.doesNotMatch(h.elements.jobsPollMessage.textContent, /Active jobs/);
  assert.match(h.elements.jobsPollMessage.textContent, /Job history/);
});

test('a superseded success cannot erase the latest failure', async () => {
  const h = harness();
  const old = h.context.fetchJobs();
  const latest = h.context.fetchJobs();
  h.requests[1].reject(new Error('offline'));
  await latest;
  h.respond(0, payload([row('stale')]));
  await old;
  assert.equal(h.context.activeJobs.length, 0);
  assert.equal(h.elements.jobsPollWarning.hidden, false);
});

for (const action of ['retry', 'changed-selection', 'active-again', 'missing']) {
  test(`history handoff survives replacement/failure without stealing selection: ${action}`, async () => {
    const h = harness();
    h.context.selectedJobId = 'finished';
    h.context.selectedSource = 'active';
    h.context.currentView = 'finished';
    const old = h.context.fetchHistory(true);
    const failed = h.context.fetchHistory();
    h.requests[1].reject(new Error('offline'));
    await failed;
    h.respond(0, []);
    await old;
    const retry = h.context.fetchHistory();
    if (action === 'changed-selection') h.context.selectedJobId = 'other';
    if (action === 'active-again') h.context.activeJobs = [row('finished')];
    h.respond(2, action === 'missing' ? [] : [row('finished', {status: 'completed', tree: '[]', result: '{"ok":true}'})]);
    await retry;
    if (action === 'retry') {
      assert.equal(h.context.selectedSource, 'history');
      assert.equal(h.context.historyJobs[0].result.ok, true);
      assert.deepEqual(h.renders, [['history', 'finished']]);
    } else if (action === 'missing') {
      assert.equal(h.context.selectedJobId, null);
      assert.deepEqual(h.renders, [['active-overview']]);
    } else {
      assert.equal(h.context.selectedSource, 'active');
      assert.equal(h.renders.length, 0);
    }
  });
}

test('a disappearing active job gets a fresh history snapshot without repeated cancellation', async () => {
  const h = harness();
  h.context.selectedJobId = 'finished';
  h.context.selectedSource = 'active';
  const staleHistory = h.context.fetchHistory();
  const active = h.context.fetchJobs();
  h.respond(1, payload([]));
  await active;
  assert.equal(h.requests.length, 3);
  assert.equal(h.requests[0].options.signal.aborted, true);
  const nextActive = h.context.fetchJobs();
  h.respond(3, payload([]));
  await nextActive;
  assert.equal(h.requests.length, 4);
  assert.equal(h.requests[2].options.signal.aborted, false);
  h.respond(0, []);
  await staleHistory;
  assert.equal(h.context.selectedJobId, 'finished');
  h.respond(2, [row('finished', {status: 'completed'})]);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(h.context.selectedSource, 'history');
  assert.deepEqual(h.renders, [['history', 'finished']]);
});

test('timeout during body decoding cannot apply a late success', async () => {
  const h = harness();
  const pending = h.context.fetchJobs();
  let finishBody;
  h.requests[0].resolve({ok: true, json: () => new Promise(resolve => { finishBody = resolve; })});
  await new Promise(resolve => setImmediate(resolve));
  [...h.timers.values()][0]();
  finishBody(payload([row('late')]));
  await pending;
  assert.equal(h.context.activeJobs.length, 0);
  assert.equal(h.context.jobsPollState.active.error, true);
});

test('an unreadable history says so in the server\'s words, not "no jobs"', async () => {
  const h = harness();
  const reason = 'Vireo could not read the job history from its catalog (database is locked).';
  const failed = h.context.fetchHistory();
  h.respond(0, {error: reason, code: 'job_history_unavailable'}, false);
  assert.equal(await failed, false);
  assert.equal(h.context.jobsPollState.history.loaded, false);
  assert.equal(h.context.jobsPollState.history.errorDetail, reason);
  assert.equal(h.elements.jobsPollMessage.textContent,
    'Job history could not be loaded. ' + reason + ' Retrying automatically.');
  const recovered = h.context.fetchHistory();
  h.respond(1, []);
  await recovered;
  assert.equal(h.context.jobsPollState.history.errorDetail, '');
  assert.equal(h.elements.jobsPollWarning.hidden, true);
});
