const assert = require('node:assert/strict');
const {createHash, webcrypto} = require('node:crypto');
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
  let reply, reload;
  const reloaded = [], toasts = [];
  const ctx = vm.createContext({
    currentReviewResults: () => results, speciesConfirmInFlight: {},
    crypto: webcrypto, TextEncoder, reviewScopeRequestSeq: 0, pipelineReviewWorkspaceId: 1,
    cloneReviewData: value => JSON.parse(JSON.stringify(value)),
    applyReviewResults: data => { reloaded.push(data); Object.assign(results, data); },
    isScopedReviewView: () => false, speciesConfirmPhotoKey: ids => ids.join(','),
    rerenderSpeciesWidgets() {}, checkPendingSync() {}, refreshLatestScopeSnapshotIfCurrent() {},
    showToast: (...args) => toasts.push(args), refreshAfterSpeciesConfirm: (...args) => refreshes.push(args),
    grmState: {source: 'pipeline', burstIdx: null}, reviewEncounterSpecies: async body => body,
    safeFetch: async (url, init) => {
      requests.push({url, body: init.body ? JSON.parse(init.body) : null});
      if (url === '/api/pipeline/page-init') {
        if (reload instanceof Error) throw reload;
        assert.ok(reload, 'Unexpected full-tree reload');
        return typeof reload === 'function' ? reload() : reload;
      }
      assert.equal(url, '/api/encounters/species');
      assert.equal(requests.at(-1).body.response_mode, 'compact');
      return reply;
    },
  });
  vm.runInContext(declarations('vireo/static/pipeline-review/species-editor.js', [
    'encounterStructureSignature', 'applySpeciesResponse', 'applyReviewedSpecies',
    'speciesStructureDigest', 'reloadReviewAfterSpeciesChange',
  ]), ctx);
  vm.runInContext(declarations('vireo/static/pipeline-review/group-changes.js', ['grmConfirmSpeciesCall']), ctx);
  return {ctx, results, requests, refreshes, reloaded, toasts,
    setReply: value => { reply = value; }, setReload: value => { reload = value; }};
}

function digest(signature) {
  return createHash('sha256').update(signature).digest('hex');
}

function compact(encounter, index, h) {
  return {ok: true, response_mode: 'compact',
    encounter_changes: {base_structure_digest: digest(h.ctx.encounterStructureSignature(h.results.encounters)),
      removed: [encounter.photo_ids], upserts: [{index, encounter}]},
    summary: {confirmed_count: index + 1}};
}

test('30 species additions patch selected encounters and summary without fetching or saving a full tree', async () => {
  const h = harness();
  const unrelated = h.results.encounters.slice(30);
  for (let i = 0; i < 30; i++) {
    const enc = {...h.results.encounters[i], confirmed_species: 'Invented species ' + i, species_confirmed: true};
    h.setReply(compact(enc, i, h));
    await h.ctx.applyReviewedSpecies({species: enc.confirmed_species, photo_ids: enc.photo_ids}, i, null);
    assert.equal(h.results.encounters[i], enc);
    assert.equal(h.results.summary.confirmed_count, i + 1);
    assert.equal(h.refreshes[i][2], false, 'label-only edit does not force a structural render');
  }
  assert.equal(h.requests.length, 30);
  unrelated.forEach((enc, i) => assert.equal(h.results.encounters[i + 30], enc));
});

test('a split and an adjacent merge splice at final indices while retaining unrelated encounters', async () => {
  const h = harness();
  const keep = h.results.encounters[2];
  h.results.encounters[0].photo_ids = [1, 1000];
  const signature = h.ctx.encounterStructureSignature(h.results.encounters);
  const detached = {photo_ids: [1000, 2], bursts: [{photo_ids: [1000]}, {photo_ids: [2]}]};
  const remaining = {photo_ids: [1], bursts: [{photo_ids: [1]}]};
  const changed = await h.ctx.applySpeciesResponse({response_mode: 'compact', encounter_changes: {
    base_structure_digest: digest(signature), removed: [[1, 1000], [2]], upserts: [{index: 0, encounter: remaining}, {index: 1, encounter: detached}],
  }}, signature);
  assert.equal(changed, true);
  assert.equal(h.results.encounters[0], remaining);
  assert.equal(h.results.encounters[1], detached);
  assert.equal(h.results.encounters[2], keep);
});

test('Group Review consumes the same compact changes and saved flags', async () => {
  const h = harness();
  const enc = {...h.results.encounters[0], confirmed_species: 'Blue Jay'};
  const reply = {...compact(enc, 0, h), group_photos: {'1': {flag: 'flagged'}}};
  h.setReply(reply);
  assert.equal(await h.ctx.grmConfirmSpeciesCall('Blue Jay', [1]), reply);
  assert.equal(h.results.encounters[0], enc);
  assert.equal(h.requests.length, 1);
});

test('older full responses remain readable during an explicit client migration', async () => {
  const h = harness();
  const encounters = [{photo_ids: [1, 2], bursts: []}];
  await h.ctx.applySpeciesResponse({encounters, summary: {confirmed_count: 1}}, 'old');
  assert.equal(h.results.encounters, encounters);
  assert.equal(h.results.summary.confirmed_count, 1);
});

for (const stale of ['selected split', 'unrelated reorder', 'unmatched removal']) {
  test('reloads authoritative groups for ' + stale + ' without partially applying a patch', async () => {
    const h = harness();
    const server = JSON.parse(JSON.stringify(h.results));
    if (stale === 'selected split') {
      h.results.encounters[0] = {photo_ids: [1, 2], bursts: [{photo_ids: [1, 2]}]};
      h.results.encounters.splice(1, 1);
    } else if (stale === 'unrelated reorder') {
      [server.encounters[1], server.encounters[2]] = [server.encounters[2], server.encounters[1]];
    }
    const signature = h.ctx.encounterStructureSignature(server.encounters);
    const response = {response_mode: 'compact', encounter_changes: {
      base_structure_digest: digest(signature), removed: [stale === 'unmatched removal' ? [99999] : [1]],
      upserts: [{index: 0, encounter: {...server.encounters[0], confirmed_species: 'Blue Jay'}}],
    }, summary: {confirmed_count: 99}};
    h.setReload({workspace_id: 1, results: server});
    assert.equal(await h.ctx.applySpeciesResponse(response, 'old'), true);
    assert.equal(h.reloaded.length, 1);
    assert.equal(h.requests[0].url, '/api/pipeline/page-init');
    assert.equal(h.results.encounters[0].confirmed_species, undefined, 'patch was not applied before reload');
    const ids = h.results.encounters.flatMap(enc => enc.photo_ids);
    assert.equal(new Set(ids).size, ids.length, 'no duplicate photos after stale-tree recovery');
    assert.notEqual(h.results.summary?.confirmed_count, 99, 'no partial summary update');
  });
}

test('a regroup during digest calculation reloads instead of applying to the changed local tree', async () => {
  const h = harness();
  const response = compact({...h.results.encounters[0], confirmed_species: 'Blue Jay'}, 0, h);
  const server = JSON.parse(JSON.stringify(h.results));
  h.ctx.crypto = {subtle: {digest: async (...args) => {
    h.results.encounters.reverse();
    return webcrypto.subtle.digest(...args);
  }}};
  h.setReload({workspace_id: 1, results: server});
  await h.ctx.applySpeciesResponse(response, 'old');
  assert.equal(h.reloaded.length, 1);
  assert.equal(h.results.encounters[0].photo_ids[0], 1);
});

test('a failed stale-tree reload leaves old groups untouched and explains that species were saved', async () => {
  const h = harness();
  const old = h.results.encounters;
  h.setReload(new Error('invented transport failure'));
  const response = compact({...old[0], confirmed_species: 'Blue Jay'}, 0, h);
  response.encounter_changes.base_structure_digest = 'stale';
  assert.equal(await h.ctx.applySpeciesResponse(response, 'old'), false);
  assert.equal(h.results.encounters, old);
  assert.match(h.toasts[0][0], /Species saved.*could not be loaded/);
});

for (const navigation of ['scope', 'workspace']) {
  test('stale-tree recovery does not overwrite a newer ' + navigation + ' navigation', async () => {
    const h = harness();
    const old = h.results.encounters;
    const response = compact({...old[0], confirmed_species: 'Blue Jay'}, 0, h);
    response.encounter_changes.base_structure_digest = 'stale';
    h.setReload(() => Promise.resolve().then(() => {
      if (navigation === 'scope') h.ctx.reviewScopeRequestSeq++;
      else h.ctx.pipelineReviewWorkspaceId = 2;
      return {workspace_id: 1, results: {encounters: []}};
    }));
    assert.equal(await h.ctx.applySpeciesResponse(response, 'old'), false);
    assert.equal(h.reloaded.length, 0);
    assert.equal(h.results.encounters, old);
  });
}
