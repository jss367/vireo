// Load the complete controller, with deterministic browser dependencies.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function mount(overrides = {}) {
  const timers = new Map();
  const images = [];
  let timerId = 0;
  class Image {
    constructor() {
      this.listeners = new Map();
      this.naturalWidth = 1920;
      this.naturalHeight = 1280;
      this.complete = true;
      images.push(this);
    }
    addEventListener(type, fn) {
      if (!this.listeners.has(type)) this.listeners.set(type, new Set());
      this.listeners.get(type).add(fn);
    }
    removeEventListener(type, fn) { this.listeners.get(type)?.delete(fn); }
    removeAttribute(name) { if (name === 'src') this.src = ''; }
    emit(type) {
      this['on' + type]?.();
      [...(this.listeners.get(type) || [])].forEach(fn => fn());
    }
  }
  const visible = new Image();
  const window = {
    Image,
    document: {getElementById: id => id === 'lightboxImg' ? visible : {
      classList: {contains: () => true}
    }},
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, {fn, delay}); return id; },
    clearTimeout(id) { timers.delete(id); },
  };
  const context = vm.createContext({window});
  vm.runInContext(fs.readFileSync('vireo/static/lightbox/session.js', 'utf8'), context);
  assert.equal(timers.size, 0, 'loading the factory is inert');
  const photos = [1, 2, 3, 4].map(id => ({id, width: 6000, height: 4000}));
  const view = {currentSrcKey: 'full', fullUsesOriginal: false, originalUnavailable: false,
    zoom: 1, nativeZoom: 4, photoW: 6000, photoH: 4000, visualTransitionPending: false};
  const controller = window.VireoLightboxSession.create({
    window, photos: () => photos, view: () => view, photoData: () => null,
    sourceUrl: (id, key) => `/photos/${id}/${key}`,
    sourceRank: key => ({full: 0, original: 3})[key],
    fullPreviewLimit: () => 1920, pickSourceKey: () => 'original',
    rememberEditRecipe() {}, rememberRenderKey() {}, ...overrides
  });
  assert.equal(timers.size, 0, 'creating a session is inert');
  function tick(delay) {
    // Execute the current timer batch, including cancelled callbacks captured by
    // the event loop only in tests that explicitly retain their function below.
    for (const [id, timer] of [...timers]) {
      if (delay !== undefined && timer.delay !== delay) continue;
      if (!timers.delete(id)) continue;
      timer.fn();
    }
  }
  return {controller, window, timers, images, visible, photos, view, tick};
}

{
  const {controller: c} = mount();
  const first = c.begin(1);
  assert(c.commit(first));
  const second = c.begin(2);
  assert.equal(c.requestedPhotoId(), 2);
  assert.equal(c.displayedPhotoId(), 1, 'outgoing bitmap keeps its identity');
  assert.equal(c.commit(first), false);
  assert.equal(c.close(), 1, 'close reconciles to the visible photo');
  assert.equal(c.requestedPhotoId(), null);
  assert.equal(c.displayedPhotoId(), null);
  c.begin(2);
  assert.equal(c.isCurrent(second), false, 'same-photo reopen rejects old work');
  assert.equal(c.commit(second), false);
  assert(Object.isFrozen(c));
}
{
  const {controller: c, images, visible, timers, tick} = mount();
  let commits = 0;
  c.begin(1);
  c.scheduleSwap(() => commits++, 150);
  const queued = [...timers.values()][0].fn;
  c.loadSource('/old', () => commits++, () => commits++);
  const stale = images.at(-1).onload;
  c.watchImage(visible, () => commits++);
  c.setInitialLoad(() => commits++, () => commits++);
  c.close();
  assert.equal(timers.size, 0);
  assert.equal(images.at(-1).onload, null);
  assert.equal(visible.listeners.get('load').size, 0);
  assert.equal(c.finishInitialLoad(), false);
  c.begin(1);
  c.watchImage(visible, () => commits++);
  c.scheduleSwap(() => commits++, 150);
  queued(); stale();
  assert.equal(c.hasScheduledSwap(), true, 'a queued old timer cannot clear its replacement');
  c.cancelSwap();
  assert.equal(commits, 0, 'callbacks already queued cannot affect a reopened session');
  visible.emit('load');
  visible.emit('load');
  assert.equal(commits, 1, 'reopen installs exactly one listener');
  c.scheduleSwap(() => commits++, 150);
  tick(150);
  assert.equal(commits, 2);
}
{
  const {controller: c, visible} = mount();
  let committed = 0, abandoned = 0;
  c.begin(1);
  c.setInitialLoad(() => committed++, () => abandoned++);
  assert(c.finishInitialLoad());
  assert.equal(c.finishInitialLoad(), false);
  c.setInitialLoad(() => committed++, () => abandoned++);
  assert(c.abandonInitialLoad());
  assert.equal(committed, 1);
  assert.equal(abandoned, 1);
  c.watchInitialImage(visible, () => committed++, () => {});
  const stale = visible.onload;
  c.begin(2);
  c.watchInitialImage(visible, () => committed++, () => {});
  const current = visible.onload;
  stale();
  assert.equal(visible.onload, current, 'old completion cannot clear new image handlers');
  visible.emit('error');
  visible.emit('load');
  assert.equal(committed, 2, 'fallback retains its load callback');
  c.close();
  assert.equal(visible.onload, null);
  assert.equal(visible.onerror, null);
}
{
  const {controller: c, images, tick, timers} = mount({preloadConcurrency: 1});
  c.begin(1);
  c.scheduleAdjacent('full'); tick(0);
  const retired = images.at(-1);
  assert.equal(c.preloadStatus().activeCount, 1);
  const reserved = c.preloadStatus().bytes;
  c.close();
  assert.equal(c.preloadStatus().adjacent.length, 0);
  assert.equal(c.preloadStatus().bytes, reserved, 'retired request retains its memory reservation');
  c.begin(3);
  c.scheduleAdjacent('full'); tick(0);
  assert.equal(images.at(-1), retired, 'reopen cannot reuse an occupied preload slot');
  retired.emit('load');
  assert.equal(retired.src, '', 'retired bitmap is released after completion');
  assert.equal(c.preloadStatus().activeCount, 0);
  tick(0);
  assert.notEqual(images.at(-1), retired, 'new session resumes when the slot is free');
  c.close();
  images.at(-1).emit('error');
  assert.equal(timers.size, 0, 'late completion after close never restarts the queue');
  assert.equal(c.preloadStatus().bytes, 0);
}
{
  const {controller: c, images, tick} = mount({preloadConcurrency: 1});
  c.begin(1); c.scheduleAdjacent('full'); tick(0);
  images.at(-1).emit('load');
  const status = c.preloadStatus();
  assert(Object.isFrozen(status.adjacent));
  assert(Object.isFrozen(status.adjacent[0]));
  assert.equal(status.adjacent[0].img, undefined);
  const preview = c.decodedPreview(2, 'original');
  assert.equal(preview.sourceKey, 'full');
  assert(Object.isFrozen(preview));
  assert.equal(preview.img, undefined);
  c.close();
}
console.log('Lightbox session lifecycle checks passed');
