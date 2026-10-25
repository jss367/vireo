import assert from 'node:assert/strict';
import {readFileSync, readdirSync} from 'node:fs';
import {test} from 'node:test';
import {parse} from 'acorn';

async function implementation(file) {
  const source = readFileSync(new URL('../../vireo/static/browse/' + file, import.meta.url), 'utf8');
  return import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
}
const {createBrowseSelection} = await implementation('selection-state.js');
const {createBrowsePagination} = await implementation('pagination.js');

test('single and batch transitions keep focus, membership and range anchor together', () => {
  const selection = createBrowseSelection();
  selection.selectBatch([7, 8, 7], 4);
  assert.deepEqual(selection.ids(), [7, 8]);
  assert.equal(selection.focusedPhotoId, null);
  assert.equal(selection.anchorIndex, 4);
  selection.selectSingle(9, 6);
  assert.deepEqual(selection.ids(), [9]);
  assert.equal(selection.batchIds.size, 0);
  assert.equal(selection.anchorIndex, 6);
  selection.clear();
  assert.deepEqual(selection.ids(), []);
  assert.equal(selection.focusedPhotoId, null);
  assert.equal(selection.anchorIndex, -1);
});

test('the live membership view exposes no mutable Set, including through callbacks', () => {
  const selection = createBrowseSelection();
  const view = selection.batchIds;
  selection.selectBatch([1, 2], 0);
  assert.equal(view.size, 2);
  assert.equal(view.has(2), true);
  assert.equal(view.add, undefined);
  assert.equal(view.clear, undefined);
  assert.equal(view.read, undefined);
  view.forEach((...args) => { assert.equal(args.length, 1); });
  assert.throws(() => { selection.focusedPhotoId = 99; }, TypeError);
  assert.throws(() => { selection.batchIds = new Set([99]); }, TypeError);
  selection.replaceBatch([3]);
  assert.deepEqual(Array.from(view), [3]);
});

test('incoming arrays and outgoing action snapshots cannot mutate selection', () => {
  const selection = createBrowseSelection();
  const input = [1, 2];
  selection.selectBatch(input, 0);
  input.push(3);
  const action = selection.ids();
  action.splice(0, 2, 99);
  assert.deepEqual(selection.ids(), [1, 2]);
});

test('later selection mutations invalidate pending Select all even without a page reload', () => {
  const selection = createBrowseSelection();
  selection.selectSingle(1, 0);
  const pending = selection.beginSelectAll();
  assert.equal(selection.focusedPhotoId, null);
  assert.equal(pending(), true);
  selection.selectBatch([5, 6], 2);
  assert.equal(pending(), false);
  const newer = selection.beginSelectAll();
  assert.equal(pending(), false);
  selection.addToBatch(7);
  assert.equal(newer(), false);
});

test('selection reads and pagination reanchoring preserve a pending membership response', () => {
  const selection = createBrowseSelection();
  const pending = selection.beginSelectAll();
  selection.ids();
  selection.batchIds.has(1);
  selection.reanchor(20);
  assert.equal(pending(), true);
});

test('double-click snapshots survive later membership changes and are consumed once', () => {
  const selection = createBrowseSelection();
  selection.selectBatch([1, 2], 0);
  selection.captureClick();
  selection.selectBatch([3, 4], 1);
  assert.deepEqual(Array.from(selection.takeClickSnapshot()), [1, 2]);
  assert.equal(selection.takeClickSnapshot(), null);
});

test('Cmd-click folds a focused photo into the batch, then drops deselected focus', () => {
  const selection = createBrowseSelection();
  selection.selectSingle(1, 0);
  assert.equal(selection.toggle([2]), false);
  assert.deepEqual(selection.ids(), [1, 2]);
  assert.equal(selection.toggle([1]), true);
  assert.deepEqual(selection.ids(), [2]);
  assert.equal(selection.focusedPhotoId, null);
  assert.equal(selection.anchorIndex, -1);
});

test('deselecting a whole stack cannot resurrect its focused cover', () => {
  const selection = createBrowseSelection();
  selection.selectBatch([1, 2, 3], 0);
  selection.focus(1, 0);
  assert.equal(selection.toggle([1, 2, 3]), true);
  assert.deepEqual(selection.ids(), []);
});

test('a partially selected stack expands as a unit and range targets retain action order', () => {
  const selection = createBrowseSelection();
  selection.selectBatch([2], 0);
  selection.toggle([1, 2, 3]);
  assert.deepEqual(selection.ids(), [2, 1, 3]);
  selection.selectSingle(99, 0);
  selection.extendRange([1, 2], [88]);
  assert.deepEqual(selection.ids(), [1, 2, 99, 88]);
  assert.equal(selection.focusedPhotoId, 99);
});

test('focused windows complete at the dataset end, using their offset', () => {
  const pages = createBrowsePagination();
  pages.configure(10);
  pages.setTotals({total: 95, underlying_total: 120, stack_count: 8});
  pages.place(9, 10, 10);
  assert.equal(pages.offset, 80);
  assert.equal(pages.allLoaded, false);
  pages.append(15, 1);
  assert.equal(pages.currentPage, 11);
  assert.equal(pages.allLoaded, true);
  assert.equal(pages.underlyingTotal, 120);
  assert.equal(pages.stackCount, 8);
});

test('focused append and prepend keep a coherent contiguous window', () => {
  const pages = createBrowsePagination();
  pages.configure(10);
  pages.setTotals({total: 100});
  pages.append(10, 1, 5);
  assert.equal(pages.earliestPage, 5);
  assert.equal(pages.currentPage, 6);
  pages.prepend(4, 20);
  assert.equal(pages.earliestPage, 4);
  assert.equal(pages.currentPage, 6);
  assert.equal(pages.offset, 30);
  assert.equal(pages.ready, true);
});

test('an empty response finishes pagination without advancing past the requested page', () => {
  const pages = createBrowsePagination();
  pages.setTotals({total: 100});
  pages.place(1, 2, 50);
  pages.append(50, 1, null, true);
  assert.equal(pages.allLoaded, true);
  assert.equal(pages.currentPage, 2);
  assert.equal(pages.ready, true);
});

test('a failed older request cannot release the loading flag of a newer request', () => {
  const pages = createBrowsePagination();
  const oldFinish = pages.beginLoad();
  const newFinish = pages.beginLoad();
  assert.equal(oldFinish(), false);
  assert.equal(pages.loading, true);
  assert.equal(newFinish(), true);
  assert.equal(newFinish(), false);
  assert.equal(pages.loading, false);
});

test('cancelling a load retires its completion, while resetting metadata preserves setup ownership', () => {
  const pages = createBrowsePagination();
  const finish = pages.beginLoad();
  pages.setTotals({total: 200});
  pages.place(3, 5, 100);
  pages.reset();
  assert.equal(pages.ready, false);
  assert.equal(pages.earliestPage, 1);
  assert.equal(pages.currentPage, 1);
  assert.equal(pages.loading, true);
  pages.cancelLoad();
  const replacement = pages.beginLoad();
  assert.equal(finish(), false);
  assert.equal(pages.loading, true);
  assert.equal(replacement(), true);
});

test('owners are independent and their public counters cannot be assigned', () => {
  const first = createBrowsePagination();
  const second = createBrowsePagination();
  first.setTotals({total: 200});
  first.place(3, 5, 50);
  first.beginLoad();
  assert.equal(second.loading, false);
  assert.equal(second.total, 0);
  assert.throws(() => { first.currentPage = 99; }, TypeError);
  assert.throws(() => { first.loading = false; }, TypeError);
});

test('Browse modules cannot retain selection or pagination fields in shared page state', () => {
  const moved = new Set(['selectedPhotoId', 'selectedIndex', 'selectedPhotos', 'lastClickedPhotoId',
    'selectAllRequestSeq', 'browseSelectionBeforeStackDblClickStart', 'currentPage', 'earliestPage',
    'perPage', 'allLoaded', 'loading', 'browseDatasetReady', 'totalPhotos', 'totalUnderlyingPhotos', 'totalBrowseStacks']);
  const directory = new URL('../../vireo/static/browse/', import.meta.url);
  function walk(node, visit) {
    if (!node || typeof node !== 'object') return;
    visit(node);
    for (const value of Object.values(node)) {
      if (Array.isArray(value)) value.forEach(child => walk(child, visit));
      else walk(value, visit);
    }
  }
  for (const file of readdirSync(directory).filter(name => name.endsWith('.js'))) {
    const tree = parse(readFileSync(new URL(file, directory), 'utf8'), {sourceType: 'module', ecmaVersion: 'latest'});
    walk(tree, node => {
      if (node.type === 'MemberExpression' && node.object?.name === 'state') {
        const field = node.computed ? node.property.value : node.property.name;
        assert.ok(!moved.has(field), file + ' reaches moved state.' + field);
      }
      if (file === 'state.js' && node.type === 'Property') {
        assert.ok(!moved.has(node.key.name), 'state.js still declares ' + node.key.name);
      }
    });
  }
});
