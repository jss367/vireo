// Exercise the real drawing controller with synthetic pixels and transport.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');

const source = readFileSync('vireo/static/lightbox/subject-box.js', 'utf8')
  .replace(/^import .*;$/m, '').replace('export function openSubjectBox', 'function openSubjectBox');
const tick = () => new Promise(resolve => setImmediate(resolve));

function harness(options = {}) {
  const puts = [], created = [], revoked = [], saved = [], documentEvents = {};
  const status = {textContent: ''}, save = {disabled: true}, cancel = {}, remove = {};
  const canvas = {events: {}, getContext: () => ({drawImage() {}, strokeRect() {}}),
    addEventListener(name, callback) { this.events[name] = callback; }, setPointerCapture() {},
    getBoundingClientRect: () => ({left: 0, top: 0, width: 100, height: 100})};
  save.addEventListener = (_name, callback) => { save.click = callback; };
  remove.addEventListener = (_name, callback) => { remove.click = callback; };
  cancel.addEventListener = () => {};
  const dialog = {isConnected: false, events: {},
    querySelector(selector) { return selector === 'canvas' ? canvas : selector === '[role="status"]' ? status : selector === '[data-save]' ? save : selector === '[data-remove]' ? remove : cancel; },
    addEventListener(name, callback) { this.events[name] = callback; }, setAttribute() {}, showModal() {},
    close() { this.isConnected = false; this.events.close?.(); }, remove() { this.isConnected = false; }};
  class SyntheticImage {
    constructor() { this.complete = false; this.naturalWidth = 0; }
    set src(_value) { this.complete = true; this.naturalWidth = 200; this.naturalHeight = 100; queueMicrotask(() => this.onload()); }
  }
  const response = {ok: true, headers: {get: () => options.token === undefined ? 'issued-frame-proof' : options.token},
    blob: async () => new Blob(['synthetic raster']), text: async () => 'Original unavailable; reconnect it'};
  const controller = vm.runInNewContext(source + '\n({openSubjectBox, loadSubjectFrame})', {
    Image: SyntheticImage, Error, queueMicrotask,
    URL: {createObjectURL() { created.push(1); return 'blob:synthetic'; }, revokeObjectURL(url) { revoked.push(url); }},
    document: {createElement: () => dialog, body: {append() { dialog.isConnected = true; }},
      addEventListener(name, callback) { documentEvents[name] = callback; }, removeEventListener() {}},
    fetchResponse: options.fetch || (async () => ({...response, ok: options.ok !== false})),
    safeFetch: async (_url, init) => { puts.push(JSON.parse(init.body)); if (options.saveError) throw new Error(options.saveError); return {photo_id: 7}; },
  });
  function draw() {
    canvas.events.pointerdown({clientX: 20, clientY: 30, pointerId: 1, preventDefault() {}});
    canvas.events.pointermove({clientX: 60, clientY: 70});
    canvas.events.pointerup();
  }
  controller.openSubjectBox(7, data => saved.push(data), options.subject);
  return {puts, created, revoked, saved, status, save, remove, dialog, draw, response,
    reset: () => documentEvents['lightbox:subjectreset']()};
}

test('saving submits the proof delivered with the displayed raster and releases its object URL', async () => {
  const h = harness(); await tick(); h.draw(); await h.save.click();
  assert.equal(h.puts[0].frame_token, 'issued-frame-proof');
  assert.equal(h.puts[0].frame, undefined);
  for (const [key, value] of Object.entries({x: .2, y: .3, w: .4, h: .4})) assert.ok(Math.abs(h.puts[0].box[key] - value) < 1e-12);
  assert.equal(h.saved.length, 1); assert.deepEqual(h.revoked, ['blob:synthetic']);
});

test('a raster without frame proof cannot enable drawing or saving', async () => {
  const h = harness({token: null}); await tick(); h.draw();
  assert.equal(h.save.disabled, true); assert.equal(h.puts.length, 0); assert.equal(h.created.length, 0);
  assert.match(h.status.textContent, /could not be verified/);
});

test('a response arriving after navigation cannot attach a raster or revive the closed dialog', async () => {
  let resolve;
  const h = harness({fetch: () => new Promise(done => { resolve = done; })});
  h.reset(); resolve(h.response); await tick();
  assert.equal(h.dialog.isConnected, false); assert.equal(h.created.length, 0); assert.equal(h.puts.length, 0);
});

test('offline originals remain unavailable to the drawing flow', async () => {
  const h = harness({ok: false}); await tick();
  assert.match(h.status.textContent, /Original unavailable/); assert.equal(h.save.disabled, true);
});

test('a stale confirmation reports the server error without minting a replacement proof', async () => {
  const h = harness({saveError: 'Original frame changed; reload it'}); await tick(); h.draw(); await h.save.click();
  assert.match(h.status.textContent, /Original frame changed/); assert.equal(h.dialog.isConnected, true);
  assert.equal(h.saved.length, 0); assert.equal(h.puts[0].frame_token, 'issued-frame-proof');
});

test('edit and remove target the region version that opened the dialog', async () => {
  const subject = {id: 19, revision: 'old-box', box_x: .1, box_y: .1, box_w: .4, box_h: .5};
  for (const operation of ['edit', 'remove']) {
    const h = harness({subject}); await tick();
    if (operation === 'edit') { h.draw(); await h.save.click(); }
    else await h.remove.click();
    assert.equal(h.puts[0].operation, operation);
    assert.equal(h.puts[0].detection_id, subject.id);
    assert.equal(h.puts[0].revision, subject.revision);
  }
});

test('confirming the displayed box submits its unchanged coordinates and signed frame', async () => {
  const subject = {id: 19, revision: 'legacy', box_x: .1, box_y: .2, box_w: .3, box_h: .4};
  const h = harness({subject});
  assert.equal(h.save.disabled, true);
  await tick();
  assert.equal(h.save.disabled, false);
  assert.equal(h.save.textContent, 'Confirm existing box');
  await h.save.click();
  assert.deepEqual(h.puts, [{operation: 'edit', detection_id: 19, revision: 'legacy',
    box: {x: .1, y: .2, w: .3, h: .4}, frame_token: 'issued-frame-proof'}]);
});
