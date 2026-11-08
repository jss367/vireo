import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const storeSource = readFileSync('vireo/static/navbar-notification-store.js', 'utf8').replaceAll('export ', '');
const uiSource = readFileSync('vireo/static/navbar-notifications.js', 'utf8').replace(/^import .*;\n/, '').replaceAll('export ', '');
function storage() {
  const data = new Map();
  return {getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value)};
}
function makeStore(disk) {
  const ctx = vm.createContext({disk});
  vm.runInContext(storeSource + '\nthis.store = createNotificationStore(() => disk);', ctx);
  return ctx.store;
}
const entry = n => ({id: String(n), message: 'Photo exported', type: 'warning', time: n, read: false, details: 'GPS written', jobId: 'export-1'});

test('history survives reload, retains the newest 100, and persists read and clear actions', () => {
  const disk = storage();
  const first = makeStore(disk);
  for (let n = 0; n < 105; n++) first.add(entry(n));
  const second = makeStore(disk);
  assert.equal(second.read().length, 100);
  assert.equal(second.read()[0].id, '5');
  second.markRead(['104']);
  assert.equal(first.read().at(-1).read, true);
  second.clear();
  assert.equal(first.read().length, 0);
});

test('invalid storage and denied access do not prevent in-memory notifications', () => {
  const disk = storage();
  disk.setItem('vireo.notifications.v1', '{broken');
  const store = makeStore(disk);
  store.add(entry(1));
  assert.equal(store.read().length, 1);
  disk.setItem('vireo.notifications.v1', JSON.stringify([null, {}, entry(2)]));
  assert.equal(store.read().length, 1);
  assert.equal(store.read()[0].id, '2');
  const denied = makeStore({getItem() { throw Error('denied'); }, setItem() { throw Error('denied'); }});
  denied.add(entry(3));
  assert.equal(denied.read()[0].id, '3');
});

class Element {
  constructor() { this.children = []; this.dataset = {}; this.attributes = {}; this.handlers = {}; this.hidden = true; }
  append(...elements) { elements.forEach(element => this.appendChild(element)); }
  appendChild(element) { this.children.push(element); element.parent = this; return element; }
  replaceChildren() { this.children = []; }
  setAttribute(name, value) { this.attributes[name] = value; }
  addEventListener(name, handler) { this.handlers[name] = handler; }
  remove() { this.parent.children = this.parent.children.filter(child => child !== this); }
  focus() { this.focused = true; }
  click() { this.handlers.click(); }
}
function ui(disk = storage()) {
  const elements = Object.fromEntries(['notificationsToggle', 'notificationHistory', 'notificationList', 'notificationBadge', 'notificationClose', 'notificationReadAll', 'notificationClear'].map(id => [id, new Element()]));
  const body = new Element();
  const timers = [];
  let uuid = 0;
  const context = vm.createContext({localStorage: disk, crypto: {randomUUID: () => String(++uuid)},
    document: {body, createElement: () => new Element(), getElementById: id => elements[id]},
    window: {addEventListener() {}}, Keymap: {pushEsc: () => 1, popEsc() {}},
    setTimeout: (callback, delay) => timers.push({callback, delay})});
  vm.runInContext(storeSource + '\n' + uiSource, context);
  return {context, elements, timers, container: body.children[0]};
}

test('only routine feedback expires; warnings remain dismissible and recoverable', () => {
  const h = ui();
  h.context.showToast('Saved', 'success');
  h.context.showToast('Missing species', 'warning', {details: 'GPS written', jobId: 'job & 1'});
  h.context.showToast('Disk full', 'error');
  assert.equal(h.timers.length, 1);
  assert.equal(h.timers[0].delay, 5000);
  assert.equal(h.elements.notificationBadge.textContent, '2');
  h.timers[0].callback();
  assert.equal(h.container.children.length, 2);
  const warning = h.container.children[0];
  assert.equal(warning.children[1].href, '/jobs?job=job%20%26%201');
  warning.children.at(-1).click();
  assert.equal(h.elements.notificationBadge.textContent, '1');
  h.elements.notificationsToggle.click();
  assert.equal(h.elements.notificationHistory.hidden, false);
  assert.equal(h.elements.notificationList.children.length, 3);
  h.elements.notificationReadAll.click();
  assert.equal(h.elements.notificationBadge.hidden, true);
  h.elements.notificationClear.click();
  assert.equal(h.elements.notificationList.textContent, 'No notifications yet.');
});

test('notification text is rendered as text and stored details survive page reload', () => {
  const disk = storage();
  const first = ui(disk);
  first.context.showToast('<img onerror=alert(1)>', 'warning', {details: '<script>bad</script>'});
  assert.equal(first.container.children[0].children[0].textContent, '<img onerror=alert(1)>');
  const second = ui(disk);
  second.elements.notificationsToggle.click();
  const row = second.elements.notificationList.children[0];
  assert.equal(row.children[1].textContent, '<img onerror=alert(1)>');
  assert.equal(row.children[2].children[1].textContent, '<script>bad</script>');
});

test('job links select the requested job after both snapshots finish', async () => {
  const source = readFileSync('vireo/static/jobs/boot.js', 'utf8').replace(/^import .*;\n/gm, '');
  for (const location of ['active', 'history', 'missing']) {
    const selected = [];
    const warnings = [];
    let resolveJobs;
    const pending = new Promise(resolve => { resolveJobs = resolve; });
    const state = {activeJobs: [], historyJobs: []};
    const context = vm.createContext({state, URLSearchParams, window: {location: {search: '?job=export-1'}},
      HTMLButtonElement: Element, HTMLSelectElement: Element, jobsElement: () => new Element(),
      bindJobListClicks() {}, bindSourceCleanupConfirm() {}, bindStepRetryButtons() {}, bindDetailPaneActions() {},
      retryJobsPolling() {}, applyFilters() {}, applyOriginFilter() {}, setInterval() {},
      fetchJobs: () => pending, fetchHistory: () => Promise.resolve(),
      selectJob: (...args) => selected.push(args), showToast: (...args) => warnings.push(args)});
    vm.runInContext(source, context);
    assert.equal(selected.length, 0);
    if (location !== 'missing') state[location === 'active' ? 'activeJobs' : 'historyJobs'].push({id: 'export-1'});
    resolveJobs();
    await new Promise(resolve => setImmediate(resolve));
    if (location === 'missing') assert.match(warnings[0][0], /not in the recent job history/);
    else assert.deepEqual(selected, [['export-1', location]]);
  }
});
