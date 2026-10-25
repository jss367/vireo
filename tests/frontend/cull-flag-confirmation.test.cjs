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

test('Apply status distinguishes photos actually cleared from already-none Reviews', async () => {
  const ctx = page();
  vm.runInContext(fs.readFileSync(path.join(root, 'vireo/static/cull/actions.js'), 'utf8'), ctx);
  const prompts = [];
  const statusEl = {textContent: '', style: {}};
  const applyBtn = {disabled: false};
  Object.assign(ctx, {
    cullData: {species_groups: [{scene_groups: [{photos: [
      {photo_id: 1, action: 'review', decision_source: 'manual'},
      {photo_id: 2, action: 'review', decision_source: 'manual'},
    ]}]}]},
    pipelineResults: {photos: [
      {id: 1, flag: 'flagged'},  // Will actually clear.
      {id: 2, flag: 'none'},     // Already clear; saved as explicit Review only.
    ]},
    cullOperationPending: () => false,
    confirm: (msg) => { prompts.push(msg); return true; },
    safeFetch: async () => ({ok: true, keepers: 0, rejects: 0, cleared: 1}),
    document: {getElementById: (id) => id === 'applyBtn' ? applyBtn : statusEl},
    rebuildCullDataFromPipeline() {}, updateCullApplyButton() {},
    syncCullSourceFlags() {},
  });
  await ctx.applyCulling();
  assert.equal(prompts.length, 1);
  assert.match(prompts[0], /1 photos cleared back to no flag/);
  assert.match(prompts[0], /1 photos saved as Review/);
  assert.match(statusEl.textContent, /1 cleared/);
  assert.match(statusEl.textContent, /1 saved as Review/);
});

test('confirmed clear reloads as saved Review, unlike an unattributed raw none', () => {
  const ctx = page();
  const saved = ctx.resolveCullDecision({id: 1, flag: 'none', flag_confirmation_status: 'confirmed'});
  assert.equal(saved.action, 'review');
  assert.equal(saved.source, 'confirmed');
  const bare = ctx.resolveCullDecision({id: 2, flag: 'none', flag_confirmation_status: 'unverified'});
  assert.equal(bare.action, 'review');
  assert.equal(bare.source, 'auto');
});

test('Undo releases Review attribution pins and later Apply does not clear a newer flag', async () => {
  const ctx = page();
  for (const file of ['history', 'actions']) {
    vm.runInContext(fs.readFileSync(path.join(root, `vireo/static/cull/${file}.js`), 'utf8'), ctx);
  }
  let payload;
  let saved = {flag: 'none', flag_confirmation_status: 'unverified'};
  Object.assign(ctx, {
    pipelineResults: {photos: [{id: 1, flag: 'none', flag_confirmation_status: 'confirmed'}]},
    cullSourceResults: {photos: [{id: 1, flag: 'none', flag_confirmation_status: 'confirmed'}]},
    cullOperationPending: () => false, confirm: () => true,
    safeFetch: async (url, options) => {
      if (url === '/api/pipeline/group/state') return {photos: {1: saved}};
      payload = JSON.parse(options.body);
    },
    document: {getElementById: () => ({style: {}})},
    hasCullFilters: () => false,
    setPipelineResults(results) { ctx.pipelineResults = results; },
    rebuildCullDataFromPipeline() {}, updateCullApplyButton() {},
  });
  for (saved of [
    {flag: 'none', flag_confirmation_status: 'unverified'},
    {flag: 'flagged', flag_confirmation_status: 'confirmed'},
    {flag: 'rejected', flag_confirmation_status: 'confirmed'},
  ]) {
    ctx.cullManualDecisions[1] = ctx.cullAppliedDecisions[1] = 'review';
    await ctx.cullAfterHistoryChange({undone: 'Apply'});
    assert.equal(ctx.cullManualDecisions[1], undefined);
    assert.equal(ctx.cullAppliedDecisions[1], undefined);
  }
  // A newer automatic flag has no manual confirmation and must not be cleared
  // by applying a different photo's pin after the earlier Review was undone.
  const decision = ctx.resolveCullDecision({id: 1, flag: 'flagged', flag_confirmation_status: 'unverified'});
  ctx.cullData = {species_groups: [{scene_groups: [{photos: [
    {photo_id: 1, action: decision.action, decision_source: decision.source},
    {photo_id: 2, action: 'keep', decision_source: 'manual'},
  ]}]}]};
  ctx.cullManualDecisions[2] = 'keep';
  await ctx.applyCulling();
  assert.deepEqual(payload, {keepers: [2], rejects: [], unflag: []});
  // Redo's restored attribution renders as saved Review even without a pin.
  saved = {flag: 'none', flag_confirmation_status: 'confirmed'};
  await ctx.cullAfterHistoryChange({redone: 'Apply'});
  assert.equal(ctx.resolveCullDecision(ctx.pipelineResults.photos[0]).source, 'confirmed');
  ctx.cullManualDecisions[1] = ctx.cullAppliedDecisions[1] = 'review';
  await ctx.cullAfterHistoryChange({redone: 'unrelated edit'});
  assert.equal(ctx.cullManualDecisions[1], 'review');
  assert.equal(ctx.cullAppliedDecisions[1], 'review');
});
