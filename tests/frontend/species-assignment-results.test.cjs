const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');
const {parse} = require('acorn');

function declarations(file, names) {
  const source = readFileSync(file, 'utf8');
  return parse(source, {ecmaVersion: 'latest'}).body
    .filter(node => node.type === 'FunctionDeclaration' && names.includes(node.id.name))
    .map(node => source.slice(node.start, node.end)).join('\n');
}

function harness() {
  const results = {encounters: Array.from({length: 530}, (_, i) => ({photo_ids: [i + 1], bursts: [{photo_ids: [i + 1]}]}))};
  const requests = [], refreshes = [];
  let reply;
  const ctx = vm.createContext({
    currentReviewResults: () => results, speciesConfirmInFlight: {},
    isScopedReviewView: () => false, speciesConfirmPhotoKey: ids => ids.join(','),
    rerenderSpeciesWidgets() {}, checkPendingSync() {}, refreshLatestScopeSnapshotIfCurrent() {},
    showToast() {}, refreshAfterSpeciesConfirm: (...args) => refreshes.push(args),
    grmState: {source: 'pipeline', burstIdx: null}, reviewEncounterSpecies: async body => body,
    safeFetch: async (url, init) => {
      requests.push({url, body: JSON.parse(init.body)});
      assert.equal(url, '/api/encounters/species');
      assert.equal(requests.at(-1).body.response_mode, 'compact');
      return reply;
    },
  });
  vm.runInContext(declarations('vireo/static/pipeline-review/species-editor.js', [
    'encounterStructureSignature', 'applySpeciesResponse', 'applyReviewedSpecies',
  ]), ctx);
  vm.runInContext(declarations('vireo/static/pipeline-review/group-changes.js', ['grmConfirmSpeciesCall']), ctx);
  return {ctx, results, requests, refreshes, setReply: value => { reply = value; }};
}

function compact(encounter, index) {
  return {ok: true, response_mode: 'compact',
    encounter_changes: {removed: [encounter.photo_ids], upserts: [{index, encounter}]},
    summary: {confirmed_count: index + 1}};
}

test('30 species additions patch selected encounters and summary without fetching or saving a full tree', async () => {
  const h = harness();
  const unrelated = h.results.encounters.slice(30);
  for (let i = 0; i < 30; i++) {
    const enc = {...h.results.encounters[i], confirmed_species: 'Invented species ' + i, species_confirmed: true};
    h.setReply(compact(enc, i));
    await h.ctx.applyReviewedSpecies({species: enc.confirmed_species, photo_ids: enc.photo_ids}, i, null);
    assert.equal(h.results.encounters[i], enc);
    assert.equal(h.results.summary.confirmed_count, i + 1);
    assert.equal(h.refreshes[i][2], false, 'label-only edit does not force a structural render');
  }
  assert.equal(h.requests.length, 30);
  unrelated.forEach((enc, i) => assert.equal(h.results.encounters[i + 30], enc));
});

test('a split and an adjacent merge splice at final indices while retaining unrelated encounters', () => {
  const h = harness();
  const keep = h.results.encounters[2];
  h.results.encounters[0].photo_ids = [1, 1000];
  const signature = h.ctx.encounterStructureSignature(h.results.encounters);
  const detached = {photo_ids: [1000, 2], bursts: [{photo_ids: [1000]}, {photo_ids: [2]}]};
  const remaining = {photo_ids: [1], bursts: [{photo_ids: [1]}]};
  const changed = h.ctx.applySpeciesResponse({response_mode: 'compact', encounter_changes: {
    removed: [[1, 1000], [2]], upserts: [{index: 0, encounter: remaining}, {index: 1, encounter: detached}],
  }}, signature);
  assert.equal(changed, true);
  assert.equal(h.results.encounters[0], remaining);
  assert.equal(h.results.encounters[1], detached);
  assert.equal(h.results.encounters[2], keep);
});

test('Group Review consumes the same compact changes and saved flags', async () => {
  const h = harness();
  const enc = {...h.results.encounters[0], confirmed_species: 'Blue Jay'};
  const reply = {...compact(enc, 0), group_photos: {'1': {flag: 'flagged'}}};
  h.setReply(reply);
  assert.equal(await h.ctx.grmConfirmSpeciesCall('Blue Jay', [1]), reply);
  assert.equal(h.results.encounters[0], enc);
  assert.equal(h.requests.length, 1);
});

test('older full responses remain readable during an explicit client migration', () => {
  const h = harness();
  const encounters = [{photo_ids: [1, 2], bursts: []}];
  h.ctx.applySpeciesResponse({encounters, summary: {confirmed_count: 1}}, 'old');
  assert.equal(h.results.encounters, encounters);
  assert.equal(h.results.summary.confirmed_count, 1);
});
