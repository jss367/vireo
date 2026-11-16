import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

const source = moduleAsScript(
  readFileSync(new URL('../../vireo/static/highlights/rejects.js', import.meta.url), 'utf8'),
);

function run(state) {
  const context = vm.createContext({state, document: {getElementById: () => ({value: 'all'})}});
  vm.runInContext(source, context);
  return context;
}

test('rejecting a tied contender clears the bucket\'s stale tie decision', () => {
  const state = {
    currentData: {
      buckets: [{
        species: 'Red-tailed Hawk',
        photos: [{id: 1}, {id: 2}],
        photo_count: 2,
        best_decision: {
          kind: 'tie',
          photo_id: 1,
          contender_ids: [1, 2],
          tied_photo_ids: [1, 2],
        },
      }],
      unidentified: null,
    },
  };
  const ctx = run(state);
  assert.equal(ctx.removeRejectedPhotoFromModel(2), true);
  assert.equal(state.currentData.buckets[0].best_decision, null);
  assert.deepEqual(state.currentData.buckets[0].photos, [{id: 1}]);
});

test('rejecting a photo that isn\'t a tied contender keeps the decision', () => {
  const state = {
    currentData: {
      buckets: [{
        species: 'Red-tailed Hawk',
        photos: [{id: 1}, {id: 7}, {id: 2}],
        photo_count: 3,
        best_decision: {
          kind: 'tie',
          photo_id: 1,
          contender_ids: [1, 2],
          tied_photo_ids: [1, 2],
        },
      }],
      unidentified: null,
    },
  };
  const ctx = run(state);
  assert.equal(ctx.removeRejectedPhotoFromModel(7), true);
  assert.equal(state.currentData.buckets[0].best_decision.kind, 'tie');
  assert.deepEqual(state.currentData.buckets[0].best_decision.tied_photo_ids, [1, 2]);
});

test('non-tie decisions aren\'t cleared by a prune, so unaffected rows keep their line', () => {
  const state = {
    currentData: {
      buckets: [{
        species: 'Red-tailed Hawk',
        photos: [{id: 1}, {id: 2}],
        photo_count: 2,
        best_decision: {
          kind: 'chosen',
          photo_id: 1,
          contender_ids: [1],
          tied_photo_ids: [],
        },
      }],
      unidentified: null,
    },
  };
  const ctx = run(state);
  assert.equal(ctx.removeRejectedPhotoFromModel(2), true);
  assert.equal(state.currentData.buckets[0].best_decision.kind, 'chosen');
});
