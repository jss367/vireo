import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

function harness(safeFetch) {
  const bucket = {species: 'Hawk', photos: [{id: 1}], has_more: true};
  const data = {buckets: [bucket], unidentified: null};
  const state = {currentData: data, loadedHighlightsScope: 'folder_id=1', highlightsLoadSeq: 1};
  let folderId = 1;
  const toasts = [];
  const context = vm.createContext({
    state,
    safeFetch,
    showToast: (message, kind) => toasts.push({message, kind}),
    highlightsScopeParams: () => new URLSearchParams({folder_id: folderId}),
  });
  for (const file of ['render.js', 'species.js']) {
    const url = new URL('../../vireo/static/highlights/' + file, import.meta.url);
    vm.runInContext(moduleAsScript(readFileSync(url, 'utf8')), context);
  }
  return {
    bucket, state, toasts, getIds: () => context.bucketPhotoIds(bucket),
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
