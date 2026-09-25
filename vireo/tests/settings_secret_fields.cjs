// The curated settings autosave sends a secret field only when it was edited.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const settings = 'vireo/templates/settings.html';
const src = fs.readFileSync(settings, 'utf8');
function fn(name) {
  const found = src.match(new RegExp('(?:async )?function ' + name + '\\([^]*?\\n\\}'));
  assert(found, name);
  return found[0];
}
function varDecl(name) {
  const found = src.match(new RegExp('var ' + name + ' = [^]*?;\\n'));
  assert(found, name);
  return found[0];
}

async function main() {
  const elements = {};
  const element = id => {
    if (!elements[id]) elements[id] = {value: '1', checked: false};
    return elements[id];
  };
  const posts = [];
  const ctx = vm.createContext({
    document: {getElementById: element},
    window: {},
    safeFetch: async (url, opts) => { posts.push(JSON.parse(opts.body)); return {ok: true}; },
    _saveStatusMark() {},
    collectExternalEditors: () => [],
    collectRemoteTargets: () => [],
    getSelectedCardFields: () => [],
    collectFilterShortcuts: () => [],
    VireoPipelineConfig: {embeddingDistancePercentToThreshold: v => v},
  });
  vm.runInContext(varDecl('_SECRET_FIELDS') + varDecl('_savedSecrets'), ctx);
  for (const name of ['_readSecretFields', '_editedSecrets', '_rememberSavedSecrets', '_postConfigSnapshot']) {
    vm.runInContext(fn(name), ctx);
  }

  // What loadConfig does after filling the form from /api/config.
  element('cfgHfToken').value = 'hf_loaded';
  element('cfgInatToken').value = 'inat_loaded';
  element('cfgGoogleMapsApiKey').value = '';
  ctx._rememberSavedSecrets(ctx._readSecretFields());

  // An unrelated setting changes: no secret goes out, so a token saved from
  // another tab since this page loaded is not written back over.
  await ctx._postConfigSnapshot(1);
  assert.equal(posts.length, 1);
  for (const key of ['hf_token', 'inat_token', 'google_maps_api_key']) {
    assert(!(key in posts[0]), key + ' sent without being edited');
  }
  assert('photos_per_page' in posts[0]);

  // The user edits the iNat token: only that secret is sent.
  element('cfgInatToken').value = ' inat_new ';
  await ctx._postConfigSnapshot(2);
  assert.equal(posts[1].inat_token, 'inat_new');
  assert(!('hf_token' in posts[1]));
  assert(!('google_maps_api_key' in posts[1]));

  // Once saved it is the baseline, so the next autosave leaves it out.
  await ctx._postConfigSnapshot(3);
  assert(!('inat_token' in posts[2]));

  // Clearing a field is an edit too.
  element('cfgHfToken').value = '';
  await ctx._postConfigSnapshot(4);
  assert.equal(posts[3].hf_token, '');

  // A failed save keeps the field unsaved, so the retry sends it again.
  element('cfgGoogleMapsApiKey').value = 'AIza_new';
  ctx.safeFetch = async () => { throw new Error('offline'); };
  await assert.rejects(ctx._postConfigSnapshot(5));
  ctx.safeFetch = async (url, opts) => { posts.push(JSON.parse(opts.body)); return {ok: true}; };
  await ctx._postConfigSnapshot(6);
  assert.equal(posts[4].google_maps_api_key, 'AIza_new');
}

main().catch(err => { console.error(err); process.exitCode = 1; });
