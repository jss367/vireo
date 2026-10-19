// Selected declarations with invented DOM, HTTP responses, file text and timers.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');
const {parse} = require('acorn');
const source = readFileSync('vireo/static/settings/all-settings.js', 'utf8');
const key = 'write_location_keywords_to_xmp';
const warning = 'Invented cleanup warning <b>literal</b>';

function harness() {
  const rows = new Map(), toasts = [], alerts = [], timers = [], writes = [];
  const escape = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
  const decode = value => value.replaceAll('&lt;', '<').replaceAll('&gt;', '>').replaceAll('&quot;', '"').replaceAll('&amp;', '&');
  function rowFrom(html) {
    const rowKey = html.match(/data-key="([^"]+)"/)[1];
    const status = {textContent: decode(html.match(/class="setting-row-status"[^>]*>([^<]*)</)[1]), style: {}};
    const match = html.match(/class="setting-row-warning"([^>]*)>([\s\S]*?)<\/div>/);
    const notice = {hidden: /\bhidden\b/.test(match[1]), textContent: decode(match[2])};
    return {key: rowKey, status, notice, contains: () => false,
      querySelector: selector => selector === '.setting-row-warning' ? notice : status,
      replaceWith(fresh) { rows.set(rowKey, fresh); }};
  }
  const container = {};
  Object.defineProperty(container, 'innerHTML', {set(html) {
    rows.clear();
    for (const match of html.matchAll(/<div class="setting-row-card"[\s\S]*?(?=<div class="setting-row-card"|$)/g)) {
      const row = rowFrom(match[0]); rows.set(row.key, row);
    }
  }});
  const values = {workspace_id: 17, default: {[key]: false}, global: {}, workspace: {}};
  const control = {valuesError: null, valuesJsonError: false, loadError: null, loadResult: true, responseJsonError: false, response: {ok: true, warning}};
  const ctx = vm.createContext({
    document: {activeElement: null, getElementById: () => container,
      querySelector: selector => rows.get(selector.match(/data-key="([^"]+)"/)[1]),
      querySelectorAll: () => [], createElement: () => {
        const tmp = {};
        Object.defineProperty(tmp, 'innerHTML', {set(html) { tmp.firstElementChild = rowFrom(html); }});
        return tmp;
      }},
    HTMLElement: class {}, HTMLInputElement: class {}, window: {},
    escapeHtml: escape, escapeAttr: escape, filterAllSettings() {},
    fetch: async (url, options) => {
      if (url === '/api/settings/values') {
        if (control.valuesError) throw control.valuesError;
        return {ok: true, json: async () => {
          if (control.valuesJsonError) throw new SyntaxError('invented values JSON');
          return values;
        }};
      }
      writes.push([url, options]);
      return {ok: true, status: 200, json: async () => {
        if (control.responseJsonError) throw new SyntaxError('invented response JSON');
        return control.response;
      }};
    },
    loadConfig: async () => { if (control.loadError) throw control.loadError; return control.loadResult; },
    loadWsOverrides: async () => { if (control.loadError) throw control.loadError; return control.loadResult; },
    showToast: (...args) => toasts.push(args), setTimeout: fn => { timers.push(fn); return timers.length; }, clearTimeout() {},
    confirm: () => true, alert: text => alerts.push(text), location: {reload() {}},
    _saveStatus: {pending: {}}, _saveTimer: null, _autosaveSuspended: false, _editedWhileSuspended: false,
    _cancelQueuedSaves: async () => {}, _saveStatusMark() {}, _nextSaveGen: () => 1,
    settingsErrorMessage: error => error.message, saveConfig() {},
  });
  const functions = new Set(['setSettingsScope', 'refreshAllSettingsValues', 'renderAllSettings', 'renderSettingRow', 'renderSettingWidget',
    '_settingControlSelector', 'rerenderSettingRow', 'cssEscape', '_settingsScopeBaseFor', 'saveSchemaSetting', 'resetSchemaSetting',
    'showSettingSaveWarning', 'setRowStatus', 'formatSettingValue', 'settingsImportedMessage', 'importSettingsFile',
    'settingWarningToken', 'claimSettingSave', 'reportSettingsImported', 'rememberSettingSaveWarning', 'restoreSettingSaveWarning', 'settingSaveRefreshFailed']);
  const variables = new Set(['ALL_SETTINGS_CACHE', 'ALL_SETTINGS_DEBOUNCE', 'ALL_SETTINGS_SCOPE', 'ALL_SETTINGS_WARNINGS', 'ALL_SETTINGS_SAVE_GENERATIONS']);
  const nodes = parse(source, {ecmaVersion: 'latest'}).body.filter(node =>
    node.type === 'FunctionDeclaration' && functions.has(node.id.name) ||
    node.type === 'VariableDeclaration' && node.declarations.some(d => variables.has(d.id.name)));
  vm.runInContext(nodes.map(node => source.slice(node.start, node.end)).join('\n'), ctx);
  ctx.ALL_SETTINGS_CACHE.schema = {[key]: {type: 'bool', label: 'Invented', desc: '', category: 'Metadata'}};
  ctx.ALL_SETTINGS_CACHE.categories = ['Metadata']; ctx.ALL_SETTINGS_CACHE.values = values;
  ctx.renderAllSettings(container);
  return {ctx, rows, toasts, alerts, timers, control, values, container, writes};
}

for (const method of ['save', 'reset']) {
  for (const failure of ['transport', 'values-json', 'curated']) {
    test(method + ' retains warning through ' + failure + ' refresh failure', async () => {
      const h = harness();
      if (failure === 'transport') h.control.valuesError = new Error('invented refresh transport');
      if (failure === 'values-json') h.control.valuesJsonError = true;
      if (failure === 'curated') h.control.loadError = new Error('invented curated refresh');
      if (method === 'save') await h.ctx.saveSchemaSetting(key, false, 'global');
      else await h.ctx.resetSchemaSetting(key);
      assert.equal(h.rows.get(key).notice.textContent, warning);
      assert.equal(h.rows.get(key).notice.hidden, false);
      assert(h.toasts.some(args => args[0] === warning && args[1] === 'warning'));
      assert.notEqual(h.rows.get(key).status.textContent, 'Network error');
    });
  }
}

test('warnings survive scope renders and stay scoped to workspace and key until a successful write', async () => {
  const h = harness();
  await h.ctx.saveSchemaSetting(key, false, 'global');
  h.ctx.setSettingsScope('workspace');
  assert.equal(h.rows.get(key).notice.hidden, true);
  await h.ctx.saveSchemaSetting(key, false, 'workspace');
  h.values.workspace_id = 18; h.ctx.renderAllSettings(h.container);
  assert.equal(h.rows.get(key).notice.hidden, true);
  h.values.workspace_id = 17; h.ctx.renderAllSettings(h.container);
  assert.equal(h.rows.get(key).notice.textContent, warning);
  h.ctx.setSettingsScope('global');
  assert.equal(h.rows.get(key).notice.textContent, warning);
  h.control.response = {ok: true};
  await h.ctx.saveSchemaSetting(key, true, 'global');
  assert.equal(h.rows.get(key).notice.hidden, true);
  h.ctx.setSettingsScope('workspace');
  assert.equal(h.rows.get(key).notice.textContent, warning);
  h.timers.forEach(fn => fn());
  assert.equal(h.rows.get(key).status.textContent, 'Saved with a warning');
});

for (const failure of ['transport', 'values-json', 'curated', 'not-reloaded']) {
  test('import reports warning before ' + failure + ' refresh failure', async () => {
    const h = harness();
    if (failure === 'transport') h.control.valuesError = new Error('invented refresh transport');
    if (failure === 'values-json') h.control.valuesJsonError = true;
    if (failure === 'curated') h.control.loadError = new Error('invented curated refresh');
    if (failure === 'not-reloaded') h.control.loadResult = false;
    await h.ctx.importSettingsFile({name: 'invented.json', text: async () => '{}'});
    assert(h.toasts.some(args => args[0] === warning && args[1] === 'warning'));
    assert(h.alerts.some(text => text.includes(warning)));
    assert.equal(h.rows.get(key).notice.textContent, warning);
    assert.equal(h.ctx._autosaveSuspended, true);
  });
}

test('unreadable successful response preserves an earlier warning and acknowledges the save', async () => {
  const h = harness();
  await h.ctx.saveSchemaSetting(key, false, 'global');
  h.control.responseJsonError = true;
  await h.ctx.saveSchemaSetting(key, false, 'global');
  h.ctx.setSettingsScope('workspace'); h.ctx.setSettingsScope('global');
  assert.equal(h.rows.get(key).notice.textContent, warning);
  assert.notEqual(h.rows.get(key).status.textContent, 'Network error');
});

test('a saved warning is not attached to another scope when its response arrives late', async () => {
  const h = harness();
  let release;
  const waiting = new Promise(resolve => { release = resolve; });
  h.ctx.fetch = async url => {
    if (url === '/api/settings/values') return {ok: true, json: async () => h.values};
    await waiting;
    return {ok: true, json: async () => ({ok: true, warning})};
  };
  const saved = h.ctx.saveSchemaSetting(key, false, 'global');
  h.ctx.setSettingsScope('workspace');
  release(); await saved;
  assert.equal(h.rows.get(key).notice.hidden, true);
  h.ctx.setSettingsScope('global');
  assert.equal(h.rows.get(key).notice.textContent, warning);
});

test('a different key write cannot clear the location warning', async () => {
  const h = harness();
  h.ctx.ALL_SETTINGS_CACHE.schema.invented = {type: 'bool', label: 'Invented', desc: '', category: 'Metadata'};
  h.values.default.invented = false;
  h.ctx.renderAllSettings(h.container);
  await h.ctx.saveSchemaSetting(key, false, 'global');
  h.control.response = {ok: true};
  await h.ctx.saveSchemaSetting('invented', true, 'global');
  h.ctx.setSettingsScope('workspace'); h.ctx.setSettingsScope('global');
  assert.equal(h.rows.get(key).notice.textContent, warning);
  assert.equal(h.rows.get('invented').notice.hidden, true);
});

test('an unreadable successful import response never re-posts stale form values', async () => {
  const h = harness();
  let posted = 0;
  h.ctx.saveConfig = () => { posted++; };
  h.ctx._saveStatus.pending.config = 1;
  h.control.responseJsonError = true;
  await h.ctx.importSettingsFile({name: 'invented.json', text: async () => '{}'});
  assert.equal(posted, 0);
  assert.equal(h.ctx._autosaveSuspended, true);
  assert(h.alerts.some(text => text.startsWith('Settings imported, but')));
});

test('a later failed save keeps the old warning while reporting the new transport failure', async () => {
  const h = harness();
  await h.ctx.saveSchemaSetting(key, false, 'global');
  h.ctx.fetch = async () => { throw new Error('invented save transport'); };
  await h.ctx.saveSchemaSetting(key, true, 'global');
  assert.equal(h.rows.get(key).notice.textContent, warning);
  assert.equal(h.rows.get(key).status.textContent, 'Network error');
});

function delayedWrites(h) {
  const pending = [];
  h.ctx.fetch = async (url, options) => {
    if (url === '/api/settings/values') return {ok: true, json: async () => h.values};
    return new Promise(resolve => pending.push({url, options, resolve}));
  };
  return pending;
}
function finishWrite(pending, index, payload) {
  pending[index].resolve({ok: true, status: 200, json: async () => payload});
}
function write(h, method) {
  return method === 'save' ? h.ctx.saveSchemaSetting(key, false, h.ctx.ALL_SETTINGS_SCOPE) : h.ctx.resetSchemaSetting(key);
}

for (const olderMethod of ['save', 'reset']) {
  for (const newerMethod of ['save', 'reset']) {
    for (const [olderPayload, newerPayload] of [
      [{ok: true}, {ok: true, warning: 'new warning'}],
      [{ok: true, warning: 'old warning'}, {ok: true}],
      [{ok: true, warning: 'old warning'}, {ok: true, warning: 'new warning'}],
    ]) {
      test('superseded ' + olderMethod + ' response cannot change newer ' + newerMethod + ' warning: ' + JSON.stringify([olderPayload, newerPayload]), async () => {
        const h = harness(), pending = delayedWrites(h);
        const older = write(h, olderMethod), newer = write(h, newerMethod);
        finishWrite(pending, 1, newerPayload); await newer;
        const toastCount = h.toasts.length;
        finishWrite(pending, 0, olderPayload); await older;
        assert.equal(h.rows.get(key).notice.textContent, newerPayload.warning || '');
        assert.equal(h.rows.get(key).notice.hidden, !newerPayload.warning);
        assert.equal(h.toasts.length, toastCount);
        h.ctx.setSettingsScope('workspace'); h.ctx.setSettingsScope('global');
        assert.equal(h.rows.get(key).notice.textContent, newerPayload.warning || '');
      });
    }
  }
}

test('save ownership is independent for different workspace tokens', async () => {
  const h = harness(); h.ctx.setSettingsScope('workspace');
  const pending = delayedWrites(h), first = write(h, 'save');
  h.values.workspace_id = 18; h.ctx.renderAllSettings(h.container);
  const second = write(h, 'save');
  finishWrite(pending, 1, {ok: true, warning: 'workspace 18 warning'}); await second;
  finishWrite(pending, 0, {ok: true, warning: 'workspace 17 warning'}); await first;
  assert.equal(h.rows.get(key).notice.textContent, 'workspace 18 warning');
  h.values.workspace_id = 17; h.ctx.renderAllSettings(h.container);
  assert.equal(h.rows.get(key).notice.textContent, 'workspace 17 warning');
});

test('save ownership is independent for global and workspace scopes', async () => {
  const h = harness(), pending = delayedWrites(h), global = write(h, 'save');
  h.ctx.setSettingsScope('workspace'); const workspace = write(h, 'save');
  finishWrite(pending, 1, {ok: true, warning: 'workspace warning'}); await workspace;
  finishWrite(pending, 0, {ok: true, warning: 'global warning'}); await global;
  assert.equal(h.rows.get(key).notice.textContent, 'workspace warning');
  h.ctx.setSettingsScope('global'); assert.equal(h.rows.get(key).notice.textContent, 'global warning');
});

for (const method of ['save', 'reset']) {
  test('superseded ' + method + ' JSON response is ignored after a newer warning arrives', async () => {
    const h = harness(), pending = delayedWrites(h);
    let releaseJson;
    const json = new Promise(resolve => { releaseJson = resolve; });
    const older = write(h, method);
    pending[0].resolve({ok: true, json: () => json});
    await Promise.resolve(); await Promise.resolve();
    const newer = write(h, 'save');
    finishWrite(pending, 1, {ok: true, warning: 'new warning'}); await newer;
    const count = h.toasts.length;
    releaseJson({ok: true, warning: 'old warning'}); await older;
    assert.equal(h.rows.get(key).notice.textContent, 'new warning');
    assert.equal(h.toasts.length, count);
  });
}

test('superseded refresh failure cannot overwrite the newer warning or publish a stale failure toast', async () => {
  const h = harness();
  let rejectOldRefresh, refreshes = 0;
  const waiting = new Promise((_resolve, reject) => { rejectOldRefresh = reject; });
  h.ctx.fetch = async url => {
    if (url === '/api/settings/values') {
      if (++refreshes === 1) return waiting;
      return {ok: true, json: async () => h.values};
    }
    return {ok: true, json: async () => ({ok: true, warning: refreshes ? 'new warning' : 'old warning'})};
  };
  const older = write(h, 'save');
  while (refreshes === 0) await Promise.resolve();
  await write(h, 'save'); const count = h.toasts.length;
  rejectOldRefresh(new Error('invented old refresh failure')); await older;
  assert.equal(h.rows.get(key).notice.textContent, 'new warning');
  assert.equal(h.rows.get(key).status.textContent, 'Saved with a warning');
  assert.equal(h.toasts.length, count);
});

test('an older successful save timer cannot clear the newer pending save status', async () => {
  const h = harness();
  h.control.response = {ok: true}; await write(h, 'save');
  const pending = delayedWrites(h), newer = write(h, 'save');
  assert.equal(h.rows.get(key).status.textContent, 'Saving…');
  h.timers.forEach(fn => fn());
  assert.equal(h.rows.get(key).status.textContent, 'Saving…');
  finishWrite(pending, 0, {ok: true, warning: 'new warning'}); await newer;
});

for (const olderPayload of [{ok: true}, {ok: true, warning: 'old warning'}]) {
  test('the newer warning owns its state despite refresh failure and stale response ' + JSON.stringify(olderPayload), async () => {
    const h = harness(), pending = delayedWrites(h);
    h.ctx.fetch = async (url, options) => {
      if (url === '/api/settings/values') throw new Error('invented latest refresh failure');
      return new Promise(resolve => pending.push({url, options, resolve}));
    };
    const older = write(h, 'save'), newer = write(h, 'save');
    finishWrite(pending, 1, {ok: true, warning: 'new warning'}); await newer;
    const count = h.toasts.length;
    finishWrite(pending, 0, olderPayload); await older;
    assert.equal(h.rows.get(key).notice.textContent, 'new warning');
    assert.equal(h.rows.get(key).status.textContent, 'Saved with a warning');
    assert.equal(h.toasts.length, count);
  });
}

test('a successful import supersedes an older schema response for its global setting token', async () => {
  const h = harness(), pending = delayedWrites(h), older = write(h, 'save');
  const imported = h.ctx.importSettingsFile({name: 'invented.json', text: async () => '{}'});
  while (pending.length < 2) await Promise.resolve();
  finishWrite(pending, 1, {ok: true, warning: 'import warning'}); await imported;
  const count = h.toasts.length;
  finishWrite(pending, 0, {ok: true}); await older;
  assert.equal(h.rows.get(key).notice.textContent, 'import warning');
  assert.equal(h.toasts.length, count);
});

test('an older import response cannot overwrite a later schema warning', async () => {
  const h = harness(), pending = delayedWrites(h);
  const imported = h.ctx.importSettingsFile({name: 'invented.json', text: async () => '{}'});
  while (pending.length < 1) await Promise.resolve();
  const newer = write(h, 'save');
  finishWrite(pending, 1, {ok: true, warning: 'new warning'}); await newer;
  const count = h.toasts.length;
  finishWrite(pending, 0, {ok: true, warning: 'old import warning'}); await imported;
  assert.equal(h.rows.get(key).notice.textContent, 'new warning');
  assert.equal(h.toasts.length, count);
  assert(!h.alerts.some(text => text.includes('old import warning')));
});
