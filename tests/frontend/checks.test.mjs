import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import {ESLint} from 'eslint';

const eslint = new ESLint();

async function lint(code, filePath) {
  return (await eslint.lintText(code, {filePath}))[0].messages;
}

for (const file of ['browse/panel-requests.js', 'browse/panel-loaders.js', 'vireo-panel-loader.js',
  'import-recovery.js', 'pipeline-messages.js', 'map-focus.js', 'vireo-export-job.js',
  'vireo-visible-poll.js', 'vireo-thumbnail-queue.js', 'vireo-predictions.js']) {
  test(file + ': strict controllers reject undefined names and unused variables', async () => {
    const messages = await lint(
      '(function() { const unused = 1; misspelledPanelAction(); })();',
      'vireo/static/' + file,
    );
    assert.deepEqual(messages.map(message => message.ruleId).sort(), ['no-undef', 'no-unused-vars']);
  });
}

test('classic scripts accept shared globals but still reject correctness errors', async () => {
  const messages = await lint(
    'refreshGridCards(); switch (status) { case 1: break; case 1: break; }',
    'vireo/static/browse/grid.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-duplicate-case']);
});

test('strict controllers recognize browser globals and the explicit action bridge', async () => {
  const messages = await lint(
    'window.addEventListener("click", function() { acceptSelectionPrediction(0, true); });',
    'vireo/static/browse/selection-panel-events.js',
  );
  assert.deepEqual(messages, []);
});

test('legacy actions are only declared for the event bridge', async () => {
  const messages = await lint(
    'acceptSelectionPrediction(0, true);',
    'vireo/static/browse/panel-requests.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-undef']);
});

test('third-party scripts are excluded from application lint', async () => {
  assert.equal(await eslint.isPathIgnored('vireo/static/vendor/fuse.min.js'), true);
  assert.equal(await eslint.isPathIgnored('vireo/static/browse/grid.js'), false);
});

// The shared navbar and lightbox are ES modules too: explicit imports, the
// classic shared scripts' globals declared, no window writes outside the
// classic bridge, and no inline handler markup.
for (const file of ['navbar-tabs.js', 'lightbox/navigation.js']) {
  test(`${file}: navbar modules reject undeclared names, window writes and inline handlers`, async () => {
    const messages = await lint(
      'import { a } from "./a.js"; export const b = a + Keymap.getScope(); misspelledHelper();'
        + ' window.x = 1; globalThis.y = 2; export const c = \'<div onclick="go()">\';'
        + ' document.body.setAttribute("onclick", "x");',
      `vireo/static/${file}`,
    );
    assert.deepEqual(messages.map(message => message.ruleId),
      ['no-undef', 'no-restricted-syntax', 'no-restricted-syntax', 'no-restricted-syntax', 'no-restricted-syntax']);
  });
}

test('navbar-globals.js is the one navbar module that may write window', async () => {
  const messages = await lint(
    'import { a } from "./a.js"; window.a = a; Object.assign(window, {a}); export const c = \'<b onclick="x">\';',
    'vireo/static/navbar-globals.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-restricted-syntax']);
});

// Module pages (frontend-module-pages.json): ES modules that share nothing
// through globals. Every listed page gets these rules from eslint.config.mjs.
const modulePages = JSON.parse(readFileSync(new URL('../../frontend-module-pages.json', import.meta.url), 'utf8'));

for (const [page, contract] of Object.entries(modulePages)) {
  const pageFile = `vireo/static/${page}/render.js`;
  const shim = `vireo/static/${page}/shared.js`;

  test(`${page}: module page files parse as modules and reject undeclared globals`, async () => {
    const messages = await lint('import { a } from "./a.js"; export const b = a; escapeHtml(b);', pageFile);
    assert.deepEqual(messages.map(message => message.ruleId), ['no-undef']);
  });

  test(`${page}: module page files may not assign globals`, async () => {
    const messages = await lint(
      'window.x = 1; window["y"] = 2; globalThis.z = 3; Object.assign(window, {});'
        + ' window.location.href = "/"; document.title = "t";',
      pageFile,
    );
    assert.deepEqual(messages.map(message => message.ruleId), Array(4).fill('no-restricted-syntax'));
  });

  test(`${page}: module page files may not use inline event handlers`, async () => {
    const messages = await lint(
      'export const a = \'<div onclick="go()">\'; export const b = `<b ondblclick="x">`;'
        + ' document.body.onclick = null; document.body.setAttribute("onclick", "x");'
        + ' export const c = "<button type=button>";',
      pageFile,
    );
    assert.deepEqual(messages.map(message => message.ruleId), Array(4).fill('no-restricted-syntax'));
  });

  test(`${page}: shared.js writes only the listed shared latches, never its page hooks`, async () => {
    const allowed = contract.sharedWrites.map(name => `window.${name} = null;`).join(' ');
    assert.deepEqual(await lint(allowed, shim), []);
    const hook = contract.pageHooks[0] || 'afterHistoryChange';
    const messages = await lint(
      `window.notAHook = 1; window["${hook}"] = 2; window.${hook} = 3;`
        + ' window.Vireo.registerPageHooks({});',
      shim,
    );
    assert.deepEqual(messages.map(message => message.ruleId), Array(3).fill('no-restricted-syntax'));
  });
}


for (const page of Object.keys(modulePages)) {
  for (const file of ['render.js', 'shared.js']) {
    test(`${page}/${file}: global mutation forms are rejected`, async () => {
      const messages = await lint(
        'window.x++; --globalThis.y; delete self.z; Reflect.set(window, "x", 1);'
          + ' Reflect.defineProperty(self, "y", {}); Reflect.deleteProperty(globalThis, "z");',
        `vireo/static/${page}/${file}`,
      );
      assert.deepEqual(messages.map(message => message.ruleId), Array(6).fill('no-restricted-syntax'));
    });
    test(`${page}/${file}: mixed-case inline handler forms are rejected`, async () => {
      const messages = await lint(
        'export const a = \'<div ONCLICK="go()">\'; export const b = `<b OnDblClick="x">`;'
          + ' document.body.OnClick = null; document.body["ONCLICK"] = null;'
          + ' document.body.setAttribute("OnClick", "x");',
        `vireo/static/${page}/${file}`,
      );
      assert.deepEqual(messages.map(message => message.ruleId), Array(5).fill('no-restricted-syntax'));
    });
  }
}
