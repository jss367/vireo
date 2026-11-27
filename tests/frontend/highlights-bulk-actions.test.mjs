import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

function harness(safeFetch, isUnidentified = false) {
  const bucket = {species: 'Hawk', photos: [{id: 1}], has_more: true};
  const data = {buckets: isUnidentified ? [] : [bucket], unidentified: isUnidentified ? bucket : null};
  const state = {
    currentData: data, loadedHighlightsScope: 'folder_id=1', highlightsLoadSeq: 1,
    bucketRefetchSeq: {}, bucketRefetchDone: {}, bucketRefetchFailed: {},
  };
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
    getIdsFor: target => isUnidentified ? context.ensureFullBucketLoaded(target, null) : context.bucketPhotoIds(target),
    selectFolder: id => { folderId = id; },
    reload: () => {
      const url = new URL('../../vireo/static/highlights/loading.js', import.meta.url);
      vm.runInContext(moduleAsScript(readFileSync(url, 'utf8')), context);
      context.document = {getElementById: id => id === 'perRowSlider'
        ? {value: '20'} : {options: [{textContent: 'Folder'}], selectedIndex: 0}};
      context.render = () => {};
      context.VireoLoadingStatus = {create: () => ({start() {}, done() {}, fail() {}})};
      state.rejectedFromGrid = new Set();
      return context.loadHighlights();
    },
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

// The Pick/Unpick's server mutation already applied before safeFetch rejects.
// target.photos still holds the pre-pick prefix while the backend's tail has
// been reordered; pagination must stay disabled for this bucket until a
// successful refetch or a full reload rebases it on the backend's new order.
test('a failed refetch keeps pagination disabled against the stale prefix', async () => {
  const pending = [];
  const {bucket, toasts, getIds, refetch} = harness(url => new Promise((resolve, reject) => {
    pending.push({params: new URL(url, 'http://localhost').searchParams, resolve, reject});
  }));
  const refresh = refetch();
  assert.equal(pending.length, 1);
  pending[0].reject(new Error('network'));
  await refresh.catch(() => {});
  // Even though no refetch is now outstanding, the bucket's loaded window is
  // stale against the reordered backend tail; the bulk action must cancel.
  const action = getIds();
  assert.equal(pending.length, 1);
  assert.equal(await action, null);
  assertToldNothingChanged(toasts);
  // target.photos was never overwritten by the failed refetch.
  assert.deepEqual(Array.from(bucket.photos, photo => photo.id), [1]);
  // A successful refetch replaces the prefix, so pagination may resume.
  const retry = refetch();
  assert.equal(pending.length, 2);
  pending[1].resolve({photos: [{id: 9}], loaded_count: 1, has_more: true});
  assert.equal(await retry, true);
  const resumed = getIds();
  assert.equal(pending.length, 3);
  assert.equal(pending[2].params.get('offset'), '1');
  pending[2].resolve({photos: [{id: 10}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await resumed), [9, 10]);
});

// A pick that triggers refetch A; a quick unpick before A resolves starts
// refetch B. B resolves first and writes target.photos. A is still pending
// (slow or hung). target already reflects the backend's current order, so
// pagination must not be stranded by A — the guard is based on the latest
// settled seq, not an outstanding count.
test('a hung older refetch does not block pagination after the newer one settles', async () => {
  const pending = [];
  const {bucket, getIds, refetch} = harness(url => new Promise(resolve => {
    pending.push({params: new URL(url, 'http://localhost').searchParams, resolve});
  }));
  const refreshA = refetch();
  const refreshB = refetch();
  assert.equal(pending.length, 2);
  // B (the newer refetch) resolves first and writes target.photos.
  pending[1].resolve({photos: [{id: 42}], loaded_count: 1, has_more: true});
  assert.equal(await refreshB, true);
  assert.deepEqual(Array.from(bucket.photos, photo => photo.id), [42]);
  // A is still pending (hung). Pagination must proceed against target.
  const action = getIds();
  assert.equal(pending.length, 3);
  assert.equal(pending[2].params.get('offset'), '1');
  pending[2].resolve({photos: [{id: 43}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await action), [42, 43]);
  // A finally resolves but its seq check short-circuits before mutating.
  pending[0].resolve({photos: [{id: 99}], loaded_count: 1, has_more: true});
  assert.equal(await refreshA, false);
  assert.deepEqual(Array.from(bucket.photos, photo => photo.id), [42, 43]);
});

function deferredHarness(isUnidentified = false) {
  const pending = [];
  const result = harness(url => new Promise((resolve, reject) => {
    pending.push({params: new URL(url, 'http://localhost').searchParams, resolve, reject});
  }), isUnidentified);
  return {...result, pending};
}

async function failRefetch(request, refresh, mode) {
  if (mode === 'reject') {
    const rejection = assert.rejects(refresh, /network/);
    request.reject(new Error('network'));
    await rejection;
  } else {
    request.resolve(mode === 'null' ? null : {photos: null});
    await refresh;
  }
}

for (const isUnidentified of [false, true]) {
  for (const mode of ['reject', 'null', 'malformed']) {
    test(`a ${isUnidentified ? 'unidentified' : 'species'} ${mode} refetch blocks stale IDs until a real retry succeeds`, async () => {
      const {bucket, pending, getIds, refetch} = deferredHarness(isUnidentified);
      const refresh = refetch();
      await failRefetch(pending[0], refresh, mode);
      const action = getIds();
      // The backend order is now [2, 1], while the client still holds [1].
      // If pagination is attempted, its offset-one tail repeats 1 and omits 2.
      if (pending.length > 1) pending[1].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
      assert.equal(await action, null);
      assert.equal(pending.length, 1);
      assert.deepEqual(Array.from(bucket.photos, p => p.id), [1]);
      const retry = refetch();
      pending[1].resolve({photos: [{id: 2}], loaded_count: 1, has_more: true});
      assert.equal(await retry, true);
      const resumed = getIds();
      assert.equal(pending[2].params.get('offset'), '1');
      pending[2].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
      assert.deepEqual(Array.from(await resumed), [2, 1]);
    });
  }

  test(`an older ${isUnidentified ? 'unidentified' : 'species'} rejection cannot invalidate a newer successful refetch`, async () => {
    const {pending, getIds, refetch} = deferredHarness(isUnidentified);
    const old = refetch();
    const latest = refetch();
    pending[1].resolve({photos: [{id: 2}], loaded_count: 1, has_more: true});
    assert.equal(await latest, true);
    await failRefetch(pending[0], old, 'reject');
    const action = getIds();
    if (pending.length > 2) pending[2].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
    assert.deepEqual(Array.from(await action || []), [2, 1]);
  });

  test(`a newer failed ${isUnidentified ? 'unidentified' : 'species'} refetch cannot be rescued by an older successful response`, async () => {
    const {pending, getIds, refetch} = deferredHarness(isUnidentified);
    const old = refetch();
    const latest = refetch();
    await failRefetch(pending[1], latest, 'reject');
    pending[0].resolve({photos: [{id: 3}], loaded_count: 1, has_more: true});
    assert.equal(await old, false);
    assert.equal(await getIds(), null);
    assert.equal(pending.length, 2);
  });
}

test('a failed bucket does not disable pagination in another bucket', async () => {
  const {pending, getIds, state} = deferredHarness();
  state.bucketRefetchFailed.Owl = true;
  const action = getIds();
  assert.equal(pending.length, 1);
  pending[0].resolve({photos: [{id: 2}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await action), [1, 2]);
});

test('a full reload permits pagination after a failed refetch', async () => {
  const {pending, getIds, getIdsFor, refetch, reload, state} = deferredHarness();
  const refresh = refetch();
  await failRefetch(pending[0], refresh, 'reject');
  assert.equal(await getIds(), null);
  const replacement = {species: 'Hawk', photos: [{id: 2}], has_more: true};
  const loaded = reload();
  pending[1].resolve({buckets: [replacement], unidentified: null, folders: []});
  await loaded;
  assert.equal(state.bucketRefetchFailed.Hawk, undefined);
  // The old row reference is rejected; the replacement can paginate normally.
  assert.equal(await getIds(), null);
  const resumed = getIdsFor(replacement);
  assert.equal(pending[2].params.get('offset'), '1');
  pending[2].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await resumed), [2, 1]);
});

test('a null later refetch page cannot validate a partially refreshed window', async () => {
  const pending = [];
  let secondRequested;
  const secondRequest = new Promise(resolve => { secondRequested = resolve; });
  const {bucket, getIds, refetch} = harness(() => new Promise(resolve => {
    pending.push({resolve});
    if (pending.length === 2) secondRequested();
  }));
  bucket.photos = Array.from({length: 501}, (_, id) => ({id: id + 1}));
  const refresh = refetch();
  pending[0].resolve({photos: Array.from({length: 500}, (_, id) => ({id: id + 2})), has_more: true});
  await secondRequest;
  assert.equal(pending.length, 2);
  pending[1].resolve(null);
  await refresh;
  const action = getIds();
  if (pending.length > 2) pending[2].resolve({photos: [{id: 1}], loaded_count: 502, has_more: false});
  assert.equal(await action, null);
  assert.equal(pending.length, 2);
  assert.equal(bucket.photos.length, 501);
  assert.equal(bucket.photos[0].id, 1);
});

// A Pick that triggers refetchBucketFromZero for Hawk; before its /bucket
// request resolves, the user changes scope and the full reload completes with
// a fresh Hawk bucket. The reload rebases every bucket on the backend's
// current order, so the still-pending (or hung) refetch no longer governs
// pagination — the newly loaded bucket must accept Load more and bulk
// Confirm/Change immediately, not wait for the obsolete request to settle.
test('a hung older refetch does not block pagination after a full reload', async () => {
  const {bucket, pending, getIdsFor, refetch, reload, state} = deferredHarness();
  const old = refetch();
  assert.equal(pending.length, 1);
  const replacement = {species: 'Hawk', photos: [{id: 2}], has_more: true};
  const loaded = reload();
  pending[1].resolve({buckets: [replacement], unidentified: null, folders: []});
  await loaded;
  // The reload cleared the Hawk failure flag and marked the pending refetch
  // as settled, so pagination on the newly loaded bucket proceeds immediately
  // even though the old /bucket response hasn't arrived.
  assert.equal(state.bucketRefetchFailed.Hawk, undefined);
  const action = getIdsFor(replacement);
  assert.equal(pending[2].params.get('offset'), '1');
  pending[2].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await action || []), [2, 1]);
  // The old refetch eventually resolves and writes to its captured (now
  // orphaned) bucket target, but the replacement the user sees is untouched.
  pending[0].resolve({photos: [{id: 99}], loaded_count: 1, has_more: true});
  await old;
  assert.deepEqual(Array.from(replacement.photos, p => p.id), [2, 1]);
  assert.deepEqual(Array.from(bucket.photos, p => p.id), [99]);
});

test('an old refetch rejection cannot invalidate a completed full reload', async () => {
  const {pending, getIdsFor, refetch, reload} = deferredHarness();
  const old = refetch();
  const replacement = {species: 'Hawk', photos: [{id: 2}], has_more: true};
  const loaded = reload();
  pending[1].resolve({buckets: [replacement], unidentified: null, folders: []});
  await loaded;
  await failRefetch(pending[0], old, 'reject');
  const action = getIdsFor(replacement);
  if (pending.length > 2) pending[2].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
  assert.deepEqual(Array.from(await action || []), [2, 1]);
});

for (const isUnidentified of [false, true]) {
  for (const outcome of ['pending', 'success', 'reject', 'null']) {
    test(`a reload issued before a ${isUnidentified ? 'unidentified' : 'species'} refetch cannot rebase its ${outcome} result`, async () => {
      const {pending, getIdsFor, refetch, reload, state} = deferredHarness(isUnidentified);
      const loaded = reload();
      const refresh = refetch();
      if (outcome === 'success') {
        pending[1].resolve({photos: [{id: 2}], loaded_count: 1, has_more: true});
        await refresh;
      } else if (outcome !== 'pending') {
        await failRefetch(pending[1], refresh, outcome);
      }
      const stale = {species: 'Hawk', photos: [{id: 1}], has_more: true};
      const staleData = {buckets: isUnidentified ? [] : [stale], unidentified: isUnidentified ? stale : null, folders: []};
      pending[0].resolve(staleData);
      // All responses are controlled promises; drain their continuations before
      // checking that the stale reload was discarded and a fresh one requested.
      await new Promise(setImmediate);
      assert.notEqual(state.currentData, staleData);
      assert.equal(pending.length, 3);
      assert.equal(pending[2].params.has('limit_per_bucket'), true);
      const fresh = {species: 'Hawk', photos: [{id: 2}], has_more: true};
      pending[2].resolve({buckets: isUnidentified ? [] : [fresh], unidentified: isUnidentified ? fresh : null, folders: []});
      await loaded;
      const action = getIdsFor(fresh);
      assert.equal(pending[3].params.get('offset'), '1');
      pending[3].resolve({photos: [{id: 1}], loaded_count: 2, has_more: false});
      assert.deepEqual(Array.from(await action), [2, 1]);
      if (outcome === 'pending') {
        pending[1].resolve({photos: [{id: 99}], loaded_count: 1, has_more: true});
        await refresh;
      }
      assert.deepEqual(Array.from(fresh.photos, p => p.id), [2, 1]);
    });
  }
}
