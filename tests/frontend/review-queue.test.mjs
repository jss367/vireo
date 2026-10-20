import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

function harness() {
  const storage = new Map(), nodes = new Map();
  const element = id => {
    if (!nodes.has(id)) nodes.set(id, {textContent: '', style: {}, dataset: {}, hidden: false, removeAttribute(name) {delete this[name];}});
    return nodes.get(id);
  };
  const ctx = vm.createContext({
    localStorage: {getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value)},
    document: {getElementById: element, querySelectorAll: () => []},
    URL, URLSearchParams, console, Number, JSON, Set, Map,
    window: {location: {search: ''}},
    currentCollection: 'all', currentTab: 'all', currentModel: 'all', currentSort: 'filename',
    minConfidence: 0, currentLabelsFingerprint: null, currentPhotoIdFilter: null,
    VireoFilter: {getRules: () => ({mode:'all',rules:[]}), getVisual: () => null},
    getVisibleItems: () => [], allPredictions: [], _predictionsReloading: false,
    reviewErrorMessage: (e, fallback) => e?.message || fallback,
  });
  for (const file of ['queue-state', 'queue-focus', 'queue']) {
    vm.runInContext(readFileSync('vireo/static/review/' + file + '.js', 'utf8'), ctx);
  }
  return {ctx, storage, element};
}

test('session reload preserves inspection, selected subject, center and unresolved; never restores flags', () => {
  const {ctx} = harness();
  const state = {photoId: 7, followUpOnly: true, items: {'7': {inspected:true,unresolved:true,subject:22,center:[.7,.2],flag:'rejected'}}};
  const restored = JSON.parse(JSON.stringify(ctx.rqSession(state)));
  assert.deepEqual(restored, {photoId:7,followUpOnly:true,items:{'7':{inspected:true,unresolved:true,subject:22,center:[.7,.2]}}});
  assert.equal(ctx.rqSession({items:{'7':{center:[Infinity, 2]}}}).items['7'].center, null);
});

test('workspace and exact filter scopes have separate resumes, sort preserves the photo anchor', () => {
  const {ctx} = harness();
  ctx.reviewQueue.workspace = 1;
  const first = ctx.rqScopeKey();
  ctx.currentSort = 'date';
  assert.equal(ctx.rqScopeKey(), first);
  for (const [field, value] of [['currentCollection','4'],['currentModel','owl'],['currentTab','pending'],['minConfidence',.5],['currentPhotoIdFilter',8]]) {
    ctx[field] = value;
    assert.notEqual(ctx.rqScopeKey(), first);
    ctx[field] = ({currentCollection:'all',currentModel:'all',currentTab:'all',minConfidence:0,currentPhotoIdFilter:null})[field];
  }
  ctx.reviewQueue.workspace = 2;
  assert.notEqual(ctx.rqScopeKey(), first);
});

test('progress counts unique photographs, keeps burst members, and distinguishes inspection and follow-up', () => {
  const {ctx} = harness();
  ctx.getVisibleItems = includeMembers => {
    assert.equal(includeMembers, true);
    return [{photo_id:1,group_id:'burst',flag:'flagged'},{photo_id:1,flag:'flagged'},
      {photo_id:2,group_id:'burst',flag:'rejected'},{photo_id:3,flag:'none'}];
  };
  ctx.reviewQueue.session.items = {'2':{inspected:true,unresolved:true}};
  assert.deepEqual(JSON.parse(JSON.stringify(ctx.rqProgress())), {total:3,inspected:1,decided:2,unresolved:1});
});

test('late crop pixels cannot replace the current photo or create an object URL', async () => {
  const {ctx, element} = harness();
  ctx.reviewQueue.current = {id:1};
  ctx.reviewQueue.session.items = {'1':{center:[.2,.3]}};
  let resolve;
  ctx.fetch = () => new Promise(done => {resolve = done;});
  let created = 0;
  ctx.URL = {revokeObjectURL(){}, createObjectURL(){created++;return 'blob:new';}};
  const loading = ctx.rqLoadCrop();
  ctx.reviewQueue.cropSequence++;
  ctx.reviewQueue.current = {id:2};
  element('rqCropStatus').textContent = 'Photo 2 is loading';
  resolve(new Response('pixels', {headers:{'Content-Type':'image/jpeg'}}));
  await loading;
  assert.equal(created, 0);
  assert.equal(element('rqCropStatus').textContent, 'Photo 2 is loading');
});

test('a failed flag compare disables changes and preserves the stored inspection session', async () => {
  const {ctx, storage, element} = harness();
  ctx.reviewQueue.workspace = 1;
  ctx.reviewQueue.key = 'session';
  ctx.reviewQueue.current = {id:1,flag:'flagged'};
  ctx.reviewQueue.items = [{photo_id:1}];
  ctx.reviewQueue.session.photoId = 1;
  ctx.rqItem(1).unresolved = true;
  ctx.rqSave();
  const before = storage.get('session');
  let sent;
  ctx.safeFetch = async (url, options) => {sent = JSON.parse(options.body);throw new Error('Flag changed elsewhere');};
  await ctx.rqSetFlag('rejected');
  assert.deepEqual(sent, {flag:'rejected',observed_flag:'flagged',observed_workspace_id:1});
  assert.equal(ctx.reviewQueue.current, null);
  assert.match(element('rqFlag').textContent, /changed elsewhere/);
  assert.equal(storage.get('session'), before);
});


test('resume restores the captured expression, while explicit URL filter handoffs win', () => {
  const {ctx} = harness();
  ctx.reviewQueue.savedRules = {mode:'all',rules:[{field:'filename',op:'contains',value:'owl'}]};
  let calls = 0;
  ctx.VireoFilter.loadExpression = rules => {calls++;assert.equal(rules, ctx.reviewQueue.savedRules);};
  ctx.rqRestoreFilters();
  assert.equal(calls, 1);
  ctx.window.location.search = '?photo_id=7';
  ctx.rqRestoreFilters();
  assert.equal(calls, 2);
  ctx.window.location.search = '?filters=explicit';
  ctx.rqRestoreFilters();
  assert.equal(calls, 2);
});

test('missing or invalid chosen subjects require manual centering without using an unrelated animal', () => {
  const {ctx} = harness();
  ctx.reviewQueue.current = {id:1};
  ctx.rqItem(1).subject = 22;
  ctx.reviewQueue.subjects = [{id:11,box_x:.1,box_y:.1,box_w:.2,box_h:.2}];
  assert.equal(ctx.rqSubjectCenter(), null);
  ctx.reviewQueue.subjects.push({id:22,box_x:-.1,box_y:0,box_w:.5,box_h:.5});
  assert.equal(ctx.rqSubjectCenter(), null);
  ctx.rqItem(1).center = [.7,.3];
  assert.deepEqual([...ctx.rqSubjectCenter()], [.7,.3]);
});
