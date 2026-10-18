import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';
import {moduleAsScript} from './module-source.mjs';

const read = path => readFileSync(path, 'utf8');
const navbar = read(process.env.VIREO_NAVBAR_TEMPLATE || 'vireo/templates/_navbar.html');
const page = read('vireo/templates/browse.html').replace(/\{%\s*include\s+'_navbar\.html'\s*%\}/, navbar);
const paths = ['/static/navbar-boot.js', '/static/help.js', '/static/browse/boot.js'];
const tags = [...page.matchAll(/<script\b([^>]*)>/g)].map(match => {
  const attrs = match[1];
  return {src: attrs.match(/\bsrc="([^"]+)"/)?.[1], deferred: /\bdefer\b|\btype="module"/.test(attrs)};
}).filter(tag => paths.includes(tag.src));
assert.equal(tags.length, 3, 'derive one navbar, Help and page entry from actual template tags');
const schedule = [...tags.filter(tag => !tag.deferred), ...tags.filter(tag => tag.deferred)].map(tag => tag.src);
const boot = read('vireo/static/navbar-boot.js');
const binderNames = ['bindCommandPaletteKeys', 'bindShortcutsSheetKeys', 'bindLightboxKeyboard'];
const bootTree = parse(boot, {ecmaVersion: 'latest', sourceType: 'module'});
const binders = bootTree.body.filter(node => node.type === 'ExpressionStatement'
  && node.expression.type === 'CallExpression' && binderNames.includes(node.expression.callee.name))
  .map(node => node.expression.callee.name);
assert.equal(binders.length, 3, 'derive listener registration from actual module boot calls');

function functionSource(file, name) {
  const source = read(file);
  const node = parse(source, {ecmaVersion: 'latest', sourceType: 'module'}).body
    .map(node => node.declaration || node).find(node => node.type === 'FunctionDeclaration' && node.id.name === name);
  assert.ok(node, name);
  return source.slice(node.start, node.end);
}

function harness({sheet = false, lightbox = false} = {}) {
  const listeners = [], flags = [], fetches = [];
  let pageCalls = 0;
  class Element {
    constructor(tagName = 'DIV') {
      this.tagName = tagName; this.value = ''; this.hidden = false; this.style = {};
      const classes = new Set();
      this.classList = {contains: name => classes.has(name), add: name => classes.add(name),
        remove: name => classes.delete(name)};
    }
    addEventListener() {}
    focus() {}
  }
  const elements = Object.fromEntries(['helpModal', 'helpSearchInput', 'helpResults',
    'shortcutsCheatSheet', 'lightboxOverlay', 'commandPalette'].map(id => [id, new Element()]));
  elements.commandPalette.hidden = true;
  if (sheet) elements.shortcutsCheatSheet.classList.add('open');
  if (lightbox) elements.lightboxOverlay.classList.add('active');
  const ctx = vm.createContext({
    HTMLElement: Element, HTMLInputElement: class extends Element {},
    navigator: {platform: 'MacIntel'},
    document: {body: new Element('BODY'), getElementById: id => elements[id] || null,
      createElement: () => new Element(), querySelector: selector => {
        if (selector.includes('.help-overlay.active') && elements.helpModal.classList.contains('active')) return elements.helpModal;
        if (selector.includes('.shortcuts-overlay.open') && elements.shortcutsCheatSheet.classList.contains('open')) return elements.shortcutsCheatSheet;
        return null;
      }, addEventListener: (type, fn) => { if (type === 'keydown') listeners.push(fn); }},
    Keymap: {pushEsc: () => 1, popEsc() {}, lockBodyScroll() {}, unlockBodyScroll() {}},
    navbarState: {shortcuts: {browse: {flag: 'F1'}}}, lbState: {visualTransitionPending: false},
    vireoLightboxSession: {requestedPhotoId: () => 77}, Vireo: {pageHook: () => null},
    lightboxKeyMatchesConfiguredBrowseShortcut: () => false,
    _lbApplyFlag: (...args) => flags.push(args),
    fetch: url => { assert.equal(url, '/static/help.json'); fetches.push(url);
      return Promise.resolve({json: async () => []}); },
    Fuse: class { search() { return []; } },
  });
  ctx.window = ctx;
  for (const src of schedule) {
    if (src === '/static/navbar-boot.js') {
      vm.runInContext(functionSource('vireo/static/navbar-command-palette-open.js', 'bindCommandPaletteKeys')
        + '\n' + functionSource('vireo/static/navbar-tab-reorder-hotkeys.js', 'bindShortcutsSheetKeys')
        + '\n' + moduleAsScript(read('vireo/static/lightbox/keyboard.js')), ctx);
      for (const binder of binders) vm.runInContext(binder + '();', ctx);
    } else if (src === '/static/help.js') {
      vm.runInContext(read('vireo/static/help.js'), ctx);
    } else {
      // A page listener is intentionally inert; the real navbar/Help handlers
      // above must consume F1 before any page handler gets it.
      listeners.push(() => { pageCalls += 1; });
    }
  }
  function dispatch() {
    let stopped = false, prevented = false;
    const event = {key: 'F1', target: new Element('BODY'), ctrlKey: false, metaKey: false,
      altKey: false, shiftKey: false, preventDefault() { prevented = true; },
      stopImmediatePropagation() { stopped = true; }};
    for (const fn of listeners) { fn(event); if (stopped) break; }
    return {stopped, prevented, help: elements.helpModal.classList.contains('active'),
      flags, pageCalls, fetches};
  }
  return {dispatch};
}

test('actual template defers Help after navbar and before page boot', () => {
  assert.deepEqual(schedule, paths);
});
test('sheet-open F1 is consumed without opening Help or reaching the page', () => {
  const result = harness({sheet: true}).dispatch();
  assert.equal(result.help, false);
  assert.equal(result.stopped, true);
  assert.equal(result.prevented, true);
  assert.equal(result.pageCalls, 0);
});
test('normal F1 opens Help through its production listener and fake fetch', () => {
  const result = harness().dispatch();
  assert.equal(result.help, true);
  assert.equal(result.prevented, true);
  assert.deepEqual(result.fetches, ['/static/help.json']);
});
test('lightbox flag remapped to F1 runs exactly once without Help', () => {
  const result = harness({lightbox: true}).dispatch();
  assert.deepEqual(result.flags, [[77, 'flagged']]);
  assert.equal(result.help, false);
  assert.equal(result.stopped, true);
  assert.equal(result.pageCalls, 0);
});
