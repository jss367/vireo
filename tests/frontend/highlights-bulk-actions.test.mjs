import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

function harness(safeFetch, isUnidentified = false) {
  const bucket = {species: 'Hawk', photos: [{id: 1}], has_more: true};
  const data = {buckets: isUnidentified ? [] : [bucket], unidentified: isUnidentified ? bucket : null};
  const state = {currentData: data, loadedHighlightsScope: 'folder_id=1', highlightsLoadSeq: 1, bucketRefetchSeq: {}, bucketRefetchInFlight: {}};
  let folderId = 1;
  const toasts = [];
  const context = vm.createContext({
    state,
    safeFetch,
    showToast: (message, kind) => toasts.push({message, kind}),
    highlightsScopeParams: () => new URLSearchParams({folder_id: folderId}),
    requestScopeParams: () => new URLSearchParams({folder_id: folderId}),
    appendHighlightFilterParams: () => {},
  });
  for (const file of ['render.js', 'species.js', 'ordering.js']) {
    const url = new URL('../../vireo/static/highlights/' + file, import.meta.url);
    vm.runInContext(moduleAsScript(readFileSync(url, 'utf8')), context);
  }
  return {
    bucket, state, toasts, getIds: () => isUnidentified ? context.unidPhotoIds() : context.bucketPhotoIds(bucket),
    refetch: () => context.refetchBucketFromZero(bucket, isUnidentified),
    selectFolder: id => { folderId = id; },
  };
}

function assertToldNothingChanged(toasts) {
  assert.equal(toasts.length, 1);
  assert.equal(toasts[0].kind, 'error');
  assert.match(toasts[0].message, /^Nothing was changed/);
}

test('changing folders during a bulk action drops the old page and cancels the action', async () => {
  let finishPage;
  const pendingPage = new Promise(resolve => { finishPage = resolve; });
  const requests = [];
  const {bucket, state, toasts, getIds, selectFolder} = harness(url => {
    requests.push(url);
    return pendingPage;
  });

  const action = getIds();
  assert.equal(requests.length, 1);
  selectFolder(2);
  const replacement = {species: 'Hawk', photos: [{id: 3}], has_more: false};
  state.currentData = {buckets: [replacement], unidentified: null};
  state.loadedHighlightsScope = 'folder_id=2';
  state.highlightsLoadSeq++;
  finishPage({photos: [{id: 2}], loaded_count: 2, has_more: true});

  assert.equal(await action, null);
  assert.equal(requests.length, 1);
  assert.deepEqual(bucket.photos, [{id: 1}]);
  assert.deepEqual(replacement.photos, [{id: 3}]);
  assertToldNothingChanged(toasts);
});

test('a pending scope change cannot start an action on the displayed bucket', async () => {
  const {bucket, toasts, getIds, selectFolder} = harness(() => assert.fail('unexpected page request'));
  bucket.has_more = false;

  selectFolder(2);

  assert.equal(await getIds(), null);
  assertToldNothingChanged(toasts);
});

test('a bulk action loads every page within the same folder', async () => {
  const {toasts, getIds} = harness(async url => {
    const params = new URL(url, 'http://localhost').searchParams;
    assert.equal(params.get('folder_id'), '1');
    const offset = Number(params.get('offset'));
    return {photos: [{id: offset + 1}], loaded_count: offset + 1, has_more: offset < 2};
  });

  assert.deepEqual(Array.from(await getIds()), [1, 2, 3]);
  assert.deepEqual(toasts, []);
});

test('a page that claims more photos but makes no progress ends the bulk action', async () => {
  const {toasts, getIds} = harness(async () => ({photos: [], loaded_count: 1, has_more: true}));

  assert.equal(await getIds(), null);
  assertToldNothingChanged(toasts);
});

for (const isUnidentified of [false, true]) {
  for (const refetchFirst of [false, true]) {
    test(`a ${isUnidentified ? 'unidentified' : 'species'} refetch invalidates pending pagination (${refetchFirst ? 'refetch' : 'page'} resolves first)`, async () => {
      const pending = [];
      const {bucket, getIds, refetch} = harness(url => new Promise(resolve => {
        pending.push({params: new URL(url, 'http://localhost').searchParams, resolve});
      }), isUnidentified);
      const action = getIds();
      const refresh = refetch();
      assert.equal(pending.length, 2);
      assert.equal(pending[0].params.get('offset'), '1');
      assert.equal(pending[1].params.get('offset'), '0');
      assert.equal(pending[0].params.get('species'), isUnidentified ? '__unidentified__' : 'Hawk');
      const finishRefresh = () => pending[1].resolve({photos: [{id: 2}], loaded_count: 1, has_more: true});
      if (refetchFirst) { finishRefresh(); assert.equal(await refresh, true); }
      pending[0].resolve({photos: [{id: 2}], loaded_count: 2, has_more: false});
      assert.equal(await action, null);
      if (!refetchFirst) { finishRefresh(); assert.equal(await refresh, true); }
      assert.deepEqual(Array.from(bucket.photos, photo => photo.id), [2]);
      assert.equal(bucket.loaded_count, 1);
      assert.equal(bucket.has_more, true);
      assert.equal(pending.length, 2);
    });
  }
}

test('refetching a different bucket does not invalidate pagination', async () => {
  let finishPage;
  const {getIds, state} = harness(() => new Promise(resolve => { finishPage = resolve; }));
  const action = getIds();
  state.bucketRefetchSeq.Owl = 1;
  finishPage({photos: [{id: 2}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await action), [1, 2]);
});

// A Pick/Unpick that fires refetchBucketFromZero before the user clicks
// Confirm/Change leaves the refetch in flight when pagination starts. Seq is
// bumped at the refetch's start, so pagination captures the already-bumped
// value and the equality check alone would still pass. The bulk action must
// cancel instead of returning the old client prefix combined with the newly
// reordered backend tail.
for (const isUnidentified of [false, true]) {
  test(`a ${isUnidentified ? 'unidentified' : 'species'} refetch in flight when pagination starts cancels the bulk action`, async () => {
    const pending = [];
    const {bucket, toasts, getIds, refetch} = harness(url => new Promise(resolve => {
      pending.push({params: new URL(url, 'http://localhost').searchParams, resolve});
    }), isUnidentified);
    const refresh = refetch();
    assert.equal(pending.length, 1);
    assert.equal(pending[0].params.get('offset'), '0');
    const action = getIds();
    // Pagination must bail immediately on the in-flight refetch, before
    // issuing its own /api/highlights/bucket request.
    assert.equal(pending.length, 1);
    assert.equal(await action, null);
    assertToldNothingChanged(toasts);
    pending[0].resolve({photos: [{id: 2}], loaded_count: 1, has_more: true});
    assert.equal(await refresh, true);
    assert.deepEqual(Array.from(bucket.photos, photo => photo.id), [2]);
  });
}

test('a settled refetch does not block later pagination', async () => {
  const pending = [];
  const {bucket, getIds, refetch} = harness(url => new Promise(resolve => {
    pending.push({params: new URL(url, 'http://localhost').searchParams, resolve});
  }));
  const refresh = refetch();
  assert.equal(pending.length, 1);
  // Refetch leaves has_more true so pagination still runs afterward.
  pending[0].resolve({photos: [{id: 9}], loaded_count: 1, has_more: true});
  assert.equal(await refresh, true);
  // Pagination now sees no refetch in flight and proceeds normally.
  const action = getIds();
  assert.equal(pending.length, 2);
  assert.equal(pending[1].params.get('offset'), '1');
  pending[1].resolve({photos: [{id: 10}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await action), [9, 10]);
  assert.deepEqual(Array.from(bucket.photos, photo => photo.id), [9, 10]);
});
