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
  const requests = [], renders = [], toasts = [], cards = new Map();
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
    document: {
      querySelector(selector) {
        if (selector.startsWith('.dup-card')) return cards.get(Number(selector.match(/"(\d+)"/)[1]));
        assert(selector.startsWith('.dup-group'));
        return {parentNode: {}, set outerHTML(value) { renders.push(value); }};
      },
      querySelectorAll() { throw new Error('Unexpected DOM enumeration'); },
    },
    safeFetch(url, opts) {
      assert.equal(url, '/api/duplicates/keep');
      return new Promise((resolve, reject) => requests.push({body: JSON.parse(opts.body), resolve, reject}));
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
