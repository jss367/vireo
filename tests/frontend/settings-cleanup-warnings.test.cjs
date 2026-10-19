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
    'settingWarningToken', 'rememberSettingSaveWarning', 'restoreSettingSaveWarning', 'settingSaveRefreshFailed']);
  const variables = new Set(['ALL_SETTINGS_CACHE', 'ALL_SETTINGS_DEBOUNCE', 'ALL_SETTINGS_SCOPE', 'ALL_SETTINGS_WARNINGS']);
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
