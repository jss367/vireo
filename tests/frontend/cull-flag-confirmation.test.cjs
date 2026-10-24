const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
function page() {
  const context = vm.createContext({
    cullManualDecisions: {}, cullAppliedDecisions: {}, document: {},
    VireoBrowseCompare: {bindControls() {}},
  });
  for (const file of ['decisions', 'favorites']) {
    vm.runInContext(fs.readFileSync(path.join(root, `vireo/static/cull/${file}.js`), 'utf8'), context);
  }
  return context;
}
test('bare flags and ranking labels never become photographer decisions', () => {
  const ctx = page();
  for (const flag of ['flagged', 'rejected', 'none']) {
    for (const status of [undefined, 'unverified']) {
      const decision = ctx.resolveCullDecision({id: 1, flag, label: 'KEEP', flag_confirmation_status: status});
      assert.equal(decision.action, 'review');
      assert.equal(decision.source, 'auto');
      assert.equal(decision.suggested, 'keep');
    }
  }
});
test('live confirmed decisions and explicit session choices retain their priority', () => {
  const ctx = page();
  assert.equal(ctx.resolveCullDecision({id: 1, flag: 'flagged', flag_confirmation_status: 'confirmed'}).action, 'keep');
  assert.equal(ctx.resolveCullDecision({id: 1, flag: 'rejected', flag_confirmation_status: 'confirmed'}).action, 'reject');
  ctx.cullManualDecisions[1] = 'review';
  assert.equal(ctx.resolveCullDecision({id: 1, flag: 'flagged', flag_confirmation_status: 'confirmed'}).action, 'review');
});
test('source synchronization copies confirmation and discards a stale confirmation claim', () => {
  const ctx = page();
  ctx.cullSourceResults = {photos: [{id: 1, flag: 'flagged', flag_confirmation_status: 'confirmed'}]};
  ctx.syncCullSourceFlags({photos: [{id: 1, flag: 'flagged'}]});
  assert.equal(ctx.cullSourceResults.photos[0].flag_confirmation_status, 'unverified');
  ctx.syncCullSourceFlags({photos: [{id: 1, flag: 'rejected', flag_confirmation_status: 'confirmed'}]});
  assert.equal(ctx.resolveCullDecision(ctx.cullSourceResults.photos[0]).action, 'reject');
});
test('manual Apply persists Review on a clear flag and retains confirmed UI state', async () => {
  const ctx = page();
  vm.runInContext(fs.readFileSync(path.join(root, 'vireo/static/cull/actions.js'), 'utf8'), ctx);
  let payload;
  Object.assign(ctx, {
    cullData: {species_groups: [{scene_groups: [{photos: [
      {photo_id: 1, action: 'keep', decision_source: 'manual'},
      {photo_id: 2, action: 'review', decision_source: 'manual'},
    ]}]}]},
    pipelineResults: {photos: [{id: 1, flag: 'none'}, {id: 2, flag: 'none'}]},
    cullOperationPending: () => false, confirm: () => true,
    safeFetch: async (_url, options) => { payload = JSON.parse(options.body); },
    document: {getElementById: () => ({style: {}})},
    rebuildCullDataFromPipeline() {}, updateCullApplyButton() {},
  });
  ctx.cullManualDecisions[1] = 'keep';
  ctx.cullManualDecisions[2] = 'review';
  await ctx.applyCulling();
  assert.deepEqual(payload, {keepers: [1], rejects: [], unflag: [2]});
  assert.equal(ctx.resolveCullDecision(ctx.pipelineResults.photos[0]).action, 'keep');
  assert.equal(ctx.pipelineResults.photos[0].flag_confirmation_status, 'confirmed');
  assert.equal(ctx.cullAppliedDecisions[2], 'review');
});
