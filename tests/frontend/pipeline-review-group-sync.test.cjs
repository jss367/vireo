// setPipelineReviewGroupFlag's sync block writes the live snapshot through
// to the local cache before the batch decision runs. Legacy 'picked' flags
// normalize to 'none' in the batch-decision comparison but must not flatten
// a picked photo's cached flag when only the confirmation status changed.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');

function harness({photo, live}) {
  const requests = [];
  const ctx = vm.createContext({
    isScopedReviewView: () => false,
    notifyReadOnlyScopedView: () => {},
    findPhotoInResults: id => (id === photo.id ? photo : null),
    isPhotoActionableInReviewScope: () => true,
    showToast: () => {},
    renderResults: () => {},
    refreshLatestScopeSnapshotIfCurrent: () => {},
    requireReviewValue: value => value,
    safeFetch: (url, options) => {
      requests.push({url, body: JSON.parse(options.body)});
      if (url === '/api/pipeline/group/state') {
        return Promise.resolve({photos: {[photo.id]: live}});
      }
      return Promise.resolve({ok: true});
    },
  });
  vm.runInContext(readFileSync('vireo/static/pipeline-review/flags.js', 'utf8'), ctx);
  return {ctx, requests};
}

test('picked keeper survives a sync whose only change is confirmation status', async () => {
  const photo = {id: 7, flag: 'picked', flag_confirmation_status: 'unverified'};
  const h = harness({
    photo,
    live: {flag: 'picked', flag_confirmation_status: 'confirmed'},
  });
  await h.ctx.setPipelineReviewGroupFlag(
    {key: 'burst:0:0', noun: 'burst', photoIds: [photo.id]},
    'rejected',
  );
  // The /api/batch/flag POST still runs because 'picked' normalizes to 'none'
  // and the user asked to reject. The cache sync, though, must keep the
  // picked label visible until that batch write completes.
  const batchWrite = h.requests.find(r => r.url === '/api/batch/flag');
  assert.ok(batchWrite, 'batch write should run for non-rejected live flags');
  assert.deepEqual(batchWrite.body, {photo_ids: [photo.id], flag: 'rejected'});
  // The post-batch write path sets flag='rejected' at the end, so to isolate
  // the sync behaviour, rerun the clear-rejects path where no batch fires.
  const clearPhoto = {id: 8, flag: 'picked', flag_confirmation_status: 'unverified'};
  const clear = harness({
    photo: clearPhoto,
    live: {flag: 'picked', flag_confirmation_status: 'confirmed'},
  });
  await clear.ctx.setPipelineReviewGroupFlag(
    {key: 'burst:0:0', noun: 'burst', photoIds: [clearPhoto.id]},
    'none',
  );
  assert.equal(clearPhoto.flag, 'picked', 'clear-rejects must not overwrite a live picked flag');
  assert.equal(clearPhoto.flag_confirmation_status, 'confirmed');
  assert.ok(!clear.requests.some(r => r.url === '/api/batch/flag'),
    'clear-rejects must not touch a live picked photo');
});

test('live rejected flag still overwrites a cached picked photo during sync', async () => {
  const photo = {id: 9, flag: 'picked', flag_confirmation_status: 'unverified'};
  const h = harness({
    photo,
    live: {flag: 'rejected', flag_confirmation_status: 'confirmed'},
  });
  await h.ctx.setPipelineReviewGroupFlag(
    {key: 'burst:0:0', noun: 'burst', photoIds: [photo.id]},
    'none',
  );
  // Clear-rejects overwrites a live rejected flag back to none via the batch
  // write; the earlier sync step must have accepted the live rejected label
  // first so the changedIds filter runs on the truth and not on 'picked'.
  const batchWrite = h.requests.find(r => r.url === '/api/batch/flag');
  assert.ok(batchWrite, 'clear-rejects must run the batch write for a live rejected photo');
  assert.deepEqual(batchWrite.body, {photo_ids: [photo.id], flag: 'none'});
  assert.equal(photo.flag, 'none');
  assert.equal(photo.flag_confirmation_status, 'confirmed');
});
