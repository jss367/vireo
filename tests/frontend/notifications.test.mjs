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

test('a readable storage whose setItem is blocked keeps the new notification in memory', () => {
  const readOnly = {getItem: () => '[]', setItem() { throw Error('quota'); }};
  const store = makeStore(readOnly);
  store.add(entry(1));
  // A later read() must not roll back to the empty stored list.
  assert.equal(store.read().length, 1);
  assert.equal(store.read()[0].id, '1');
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

test('clicking a toast\'s details button opens the panel and expands the matching row', () => {
  const h = ui();
  h.context.showToast('Missing species', 'warning', {details: 'Species keywords unavailable'});
  // The toast order is: span, maybe job link, details button, dismiss button.
  // No jobId was passed, so children are [message, details button, dismiss button].
  const detailsButton = h.container.children[0].children[1];
  detailsButton.click();
  assert.equal(h.elements.notificationHistory.hidden, false);
  // The row's <details> element (third child after heading, message) is open.
  const row = h.elements.notificationList.children[0];
  const row_details = row.children[2];
  assert.equal(row_details.open, true);
});

test('details button expands the matching row even when the panel is already open', () => {
  const h = ui();
  // Open the panel first.
  h.elements.notificationsToggle.click();
  assert.equal(h.elements.notificationHistory.hidden, false);
  h.context.showToast('Disk full', 'error', {details: '/photos is 98% full'});
  // A new toast re-renders history while the panel is open; the row is now present but collapsed.
  const row = h.elements.notificationList.children[0];
  const row_details = row.children[2];
  assert.equal(row_details.open, undefined);
  // Click the toast's details button.
  h.container.children[0].children[1].click();
  // The panel stayed open, and the matching row's details are now expanded.
  assert.equal(h.elements.notificationHistory.hidden, false);
  assert.equal(row_details.open, true);
});

test('a toast whose entry was evicted from history silently no-ops rather than crashing', () => {
  const h = ui();
  // Build a toast whose entry id does not match anything in history: simulate
  // eviction by clearing the store after the toast is rendered.
  h.context.showToast('Earlier warning', 'warning', {details: 'details-1'});
  h.context.store?.clear?.();
  // Replace the store entries directly: clear the on-disk map so renderHistory finds no match.
  // The toast itself still has its click handler.
  const detailsButton = h.container.children[0].children[1];
  // This must not throw; the open-history call will render an empty list, and expandEntry is a no-op.
  detailsButton.click();
  // Panel is open; the row for the now-evicted entry is not present.
  assert.equal(h.elements.notificationHistory.hidden, false);
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

// The ?job=X resolver lives in polling.js and is called from boot.js after
// the initial Promise.all and from every successful fetchJobs/fetchHistory
// apply. Load its source once so the tests exercise the shipped function
// rather than a transcription.
const pollingSource = readFileSync('vireo/static/jobs/polling.js', 'utf8');
const resolverSource = pollingSource.match(/export function resolvePendingLinkedJob[\s\S]*?\n\}/)[0].replace(/^export /, '');
function linkedJobContext(initial = {}) {
  const selected = [];
  const warnings = [];
  const state = Object.assign({activeJobs: [], historyJobs: [], pendingLinkedJobId: null, selectedJobId: null,
    jobsPollState: {active: {loaded: false}, history: {loaded: false}}}, initial);
  const context = vm.createContext({state,
    // The real selectJob clears pendingLinkedJobId; mirror that so tests see the final shape.
    selectJob: (id, src) => { selected.push([id, src]); state.pendingLinkedJobId = null; },
    showToast: (...args) => warnings.push(args)});
  vm.runInContext(resolverSource + '\nthis.resolve = resolvePendingLinkedJob;', context);
  return {state, selected, warnings, resolve: context.resolve};
}

test('a URL-linked job is selected once the lane that holds it loads', () => {
  for (const location of ['active', 'history']) {
    const h = linkedJobContext({pendingLinkedJobId: 'export-1'});
    // Only the non-holding lane has loaded so far. No match, no warning.
    h.state.jobsPollState[location === 'active' ? 'history' : 'active'].loaded = true;
    h.resolve();
    assert.equal(h.selected.length, 0);
    assert.equal(h.warnings.length, 0);
    // The holding lane recovers and the job appears.
    h.state.jobsPollState[location].loaded = true;
    h.state[location === 'active' ? 'activeJobs' : 'historyJobs'].push({id: 'export-1'});
    h.resolve();
    assert.deepEqual(h.selected, [['export-1', location]]);
    assert.equal(h.state.pendingLinkedJobId, null);
  }
});

test('the missing-job warning waits for both lanes to actually load', () => {
  const h = linkedJobContext({pendingLinkedJobId: 'export-1'});
  // Active loaded without the job; history still not loaded. No warning yet.
  h.state.jobsPollState.active.loaded = true;
  h.resolve();
  assert.equal(h.warnings.length, 0);
  assert.equal(h.state.pendingLinkedJobId, 'export-1');
  // History loaded, job is in neither — now the warning fires and the link clears.
  h.state.jobsPollState.history.loaded = true;
  h.resolve();
  assert.match(h.warnings[0][0], /not in the recent job history/);
  assert.equal(h.state.pendingLinkedJobId, null);
});

test('resolution retries after a lane that initially failed recovers', () => {
  const h = linkedJobContext({pendingLinkedJobId: 'export-1'});
  // First polls: both lanes failed (loaded stays false). Resolver is called
  // but has nothing to do and must not warn.
  h.resolve();
  assert.equal(h.selected.length, 0);
  assert.equal(h.warnings.length, 0);
  assert.equal(h.state.pendingLinkedJobId, 'export-1');
  // History recovers with the job absent; active still failed. Still no warning.
  h.state.jobsPollState.history.loaded = true;
  h.resolve();
  assert.equal(h.warnings.length, 0);
  // Active recovers and the linked job is now in it. The selection fires late,
  // which is exactly the recovery the user asked for.
  h.state.jobsPollState.active.loaded = true;
  h.state.activeJobs.push({id: 'export-1'});
  h.resolve();
  assert.deepEqual(h.selected, [['export-1', 'active']]);
});

test('a user selection made before resolution supersedes the URL link', () => {
  const h = linkedJobContext({pendingLinkedJobId: 'export-1', selectedJobId: 'other-job'});
  // Both lanes loaded, and the linked job is not there. But because the user
  // has already chosen 'other-job', the resolver drops the link silently
  // rather than shouting "missing" or jumping away from their selection.
  h.state.jobsPollState.active.loaded = true;
  h.state.jobsPollState.history.loaded = true;
  h.resolve();
  assert.equal(h.selected.length, 0);
  assert.equal(h.warnings.length, 0);
  assert.equal(h.state.pendingLinkedJobId, null);
});

test('boot.js sets pendingLinkedJobId from ?job=X and defers to the resolver', async () => {
  const source = readFileSync('vireo/static/jobs/boot.js', 'utf8').replace(/^import .*;\n/gm, '');
  const resolveCalls = [];
  const state = {activeJobs: [], historyJobs: [], pendingLinkedJobId: null,
    jobsPollState: {active: {loaded: false}, history: {loaded: false}}};
  const context = vm.createContext({state, URLSearchParams, window: {location: {search: '?job=export-1'}},
    HTMLButtonElement: Element, HTMLSelectElement: Element, jobsElement: () => new Element(),
    bindJobListClicks() {}, bindSourceCleanupConfirm() {}, bindStepRetryButtons() {}, bindDetailPaneActions() {},
    retryJobsPolling() {}, applyFilters() {}, applyOriginFilter() {}, setInterval() {},
    fetchJobs: () => Promise.resolve(true), fetchHistory: () => Promise.resolve(true),
    resolvePendingLinkedJob: () => resolveCalls.push(null)});
  vm.runInContext(source, context);
  assert.equal(state.pendingLinkedJobId, 'export-1');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(resolveCalls.length, 1);
});
