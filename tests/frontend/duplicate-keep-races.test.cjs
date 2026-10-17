// Selected functions, invented cards and deferred transport: no app/browser I/O.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const {test} = require('node:test');
const {parse} = require('acorn');
const root = path.resolve(__dirname, '../..');

function page() {
  const proposal = {file_hash: 'H', status: 'resolved',
    winner: {id: 1, filename: 'one', exists: true},
    losers: [{id: 2, filename: 'two', exists: true, rejected: true},
      {id: 3, filename: 'three', exists: true, rejected: true}]};
  const requests = [], renders = [], toasts = [], cards = new Map(), elements = new Map();
  const element = id => {
    if (!elements.has(id)) elements.set(id, {style: {}, disabled: false, innerHTML: ''});
    return elements.get(id);
  };
  for (const id of [1, 2, 3]) {
    const controls = {'.keep-btn': {}, '.trash-btn': {}, '[data-trash-status]': {}};
    cards.set(id, {controls, querySelector: selector => controls[selector],
      closest: () => ({querySelectorAll: () => Array.from(cards.values()).flatMap(
        card => [card.controls['.keep-btn'], card.controls['.trash-btn']])})});
  }
  const c = vm.createContext({
    _proposals: [proposal], _lastScanResult: {proposals: [proposal]},
    _scanInProgress: false, _duplicateKeepPending: new Set(),
    _duplicateFileTrashState: new Map(), _duplicateTrashActive: false,
    KEPT_ANOTHER_COPY_REASON: 'kept another copy instead',
    _duplicateLoadEpoch: 0, _excludedDuplicateHashes: new Set(),
    _filterReady: Promise.resolve(), _pendingFilterRefresh: null,
    window: {location: {search: ''}}, URLSearchParams,
    clearDuplicateTrashProgress() {}, hideRestoredBanner() {}, setEmptyVisible() {},
    showProgress() {}, hideProgress() {}, safeEventSource() {},
    document: {
      getElementById: element,
      querySelector(selector) {
        if (selector.startsWith('.dup-card')) return cards.get(Number(selector.match(/"(\d+)"/)[1]));
        assert(selector.startsWith('.dup-group'));
        return {parentNode: {}, set outerHTML(value) { renders.push(value); }};
      },
      querySelectorAll() { throw new Error('Unexpected DOM enumeration'); },
    },
    safeFetch(url, opts) {
      return new Promise((resolve, reject) => requests.push({url, body: opts && JSON.parse(opts.body), resolve, reject}));
    },
    showToast: (...args) => toasts.push(args),
    groupMatchesFilter: () => true,
    renderResolvedGroup: p => String(p.winner.id),
    refreshDuplicateSummaries() {},
    renderResults() { throw new Error('Stale result cannot own the live page'); },
  });
  const source = fs.readFileSync(path.join(root, 'vireo/static/duplicates-cleanup.js'), 'utf8');
  const names = ['duplicateFileTrashIsPending', 'updateDuplicateFileTrashCard',
    'prepareDuplicateTrashBatch', 'applyKeptDuplicateCopy', 'keepDuplicateCopy'];
  for (const n of parse(source, {ecmaVersion: 'latest'}).body) {
    if (n.type === 'FunctionDeclaration' && names.includes(n.id.name)) {
      vm.runInContext(source.slice(n.start, n.end), c);
    }
  }
  const restore = fs.readFileSync(path.join(root, 'vireo/static/duplicates-restore.js'), 'utf8');
  const template = fs.readFileSync(path.join(root, 'vireo/templates/duplicates.html'), 'utf8');
  const inline = template.match(/<script>([\s\S]*?)<\/script>/)[1];
  for (const [text, wanted] of [[restore, ['cancelDuplicateLoad', 'prepareDuplicateScan',
    'duplicateResultsReloadBlocked', 'tryRestoreLastScan']], [inline, ['startScan']]]) {
    for (const n of parse(text, {ecmaVersion: 'latest'}).body) {
      if (n.type === 'FunctionDeclaration' && wanted.includes(n.id.name)) {
        vm.runInContext(text.slice(n.start, n.end), c);
      }
    }
  }
  return {c, proposal, requests, renders, toasts, cards};
}

function response(winner, losers) {
  return {file_hash: 'H', winner_id: winner, loser_ids: losers, rating: 4, flag: 'flagged'};
}

test('same-group keep requests serialize and disable both keep and Trash controls', async () => {
  const p = page();
  const a = p.c.keepDuplicateCopy(2), b = p.c.keepDuplicateCopy(3);
  assert.equal(p.requests.length, 1);
  for (const card of p.cards.values()) {
    assert.equal(card.controls['.keep-btn'].disabled, true);
    assert.equal(card.controls['.trash-btn'].disabled, true);
  }
  assert.throws(() => p.c.prepareDuplicateTrashBatch([3], {}), /Wait for that cleanup/);
  assert.equal(p.c._duplicateFileTrashState.size, 0);
  p.requests[0].resolve(response(2, [1]));
  await Promise.all([a, b]);
  assert.equal(p.proposal.winner.id, 2);
  assert.equal(p.c._duplicateKeepPending.size, 0);
  const next = p.c.keepDuplicateCopy(3);
  assert.equal(p.requests.length, 2);
  p.requests[1].resolve(response(3, [2]));
  await next;
  assert.equal(p.proposal.winner.id, 3);
});

test('same-copy double clicks send a single request', async () => {
  const p = page();
  const a = p.c.keepDuplicateCopy(2), b = p.c.keepDuplicateCopy(2);
  assert.equal(p.requests.length, 1);
  p.requests[0].resolve(response(2, [1]));
  await Promise.all([a, b]);
  assert.equal(p.proposal.winner.id, 2);
});

test('replacement scans own the page when a previous keep response arrives', async () => {
  const p = page();
  const call = p.c.keepDuplicateCopy(2);
  const current = {file_hash: 'NEW', winner: {id: 9}, losers: [], status: 'resolved'};
  p.c._proposals = [current];
  p.c._lastScanResult = {proposals: [current]};
  p.requests[0].resolve(response(2, [1]));
  await call;
  assert.equal(current.winner.id, 9);
  assert.equal(p.proposal.winner.id, 1);
  assert.equal(p.renders.length, 0);
  assert.equal(p.toasts.length, 0);
  assert.equal(p.c._duplicateKeepPending.size, 0);
  for (const card of p.cards.values()) assert.equal(card.controls['.keep-btn'].disabled, false);
});

test('failed transport releases group state and preserves the old keeper', async () => {
  const p = page();
  const call = p.c.keepDuplicateCopy(2);
  p.requests[0].reject(new Error('invented transport failure'));
  await call;
  assert.equal(p.proposal.winner.id, 1);
  assert.equal(p.toasts.length, 0);
  assert.equal(p.c._duplicateKeepPending.size, 0);
  for (const card of p.cards.values()) assert.equal(card.controls['.trash-btn'].disabled, false);
});


test('a pending keep prevents a scan from replacing its pre-switch snapshot', async () => {
  const p = page(), snapshot = p.c._lastScanResult;
  const keep = p.c.keepDuplicateCopy(2);
  const scan = p.c.startScan();
  assert.equal(p.requests.length, 1, 'scan transport must wait until the keep lands');
  assert.equal(p.c._scanInProgress, false);
  assert.equal(p.c._lastScanResult, snapshot);
  p.requests[0].resolve(response(2, [1]));
  await Promise.all([keep, scan]);
  assert.equal(p.proposal.winner.id, 2);
  assert.equal(p.renders.at(-1), '2');
  const nextScan = p.c.startScan();
  assert.equal(p.requests[1].url, '/api/duplicates/scan');
  p.requests[1].resolve({job_id: 'invented'});
  await nextScan;
});

test('a pending keep prevents restore or cleanup refresh from replacing its snapshot', async () => {
  const p = page(), snapshot = p.c._lastScanResult;
  const keep = p.c.keepDuplicateCopy(2);
  const restore = p.c.tryRestoreLastScan(true);
  assert.equal(p.requests.length, 1, 'reload must not request a pre-switch catalog');
  assert.equal(p.c._lastScanResult, snapshot);
  assert.equal(p.c._proposals[0], p.proposal);
  p.requests[0].resolve(response(2, [1]));
  await Promise.all([keep, restore]);
  assert.equal(p.proposal.winner.id, 2);
  assert.equal(p.renders.at(-1), '2');
});

test('keep refuses a scan already running or a restore with no current snapshot', async () => {
  const p = page();
  p.c._scanInProgress = true;
  const duringScan = p.c.keepDuplicateCopy(2);
  assert.equal(p.requests.length, 0);
  await duringScan;
  p.c._scanInProgress = false;
  p.c._lastScanResult = null;
  const duringRestore = p.c.keepDuplicateCopy(2);
  assert.equal(p.requests.length, 0);
  await duringRestore;
  assert.equal(p.c._duplicateKeepPending.size, 0);
});
