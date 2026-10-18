import assert from 'node:assert/strict';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {test} from 'node:test';
import {checkFrontendTypes} from '../../scripts/check_frontend_types.mjs';

function fixture(t, {source = '/** @type {number} */ const count = 1;', options = {}} = {}) {
  const root = mkdtempSync(path.join(os.tmpdir(), 'vireo-strict-types-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  mkdirSync(path.join(root, 'tsconfig.frontend-pages'));
  mkdirSync(path.join(root, 'vireo/static'), {recursive: true});
  writeFileSync(path.join(root, 'vireo/static/example.js'), source);
  writeFileSync(path.join(root, 'tsconfig.frontend.json'), JSON.stringify({
    compilerOptions: {allowJs: true, checkJs: true, strict: true,
      noUncheckedIndexedAccess: true, noEmit: true, target: 'ES2022', types: [], ...options},
    files: ['vireo/static/example.js'],
  }));
  return root;
}

test('coverage gate checks actual implementations and excludes vendor/minified assets', t => {
  const root = fixture(t);
  mkdirSync(path.join(root, 'vireo/static/vendor'));
  writeFileSync(path.join(root, 'vireo/static/vendor/vendor.js'), 'thirdParty();');
  writeFileSync(path.join(root, 'vireo/static/bundle.min.js'), 'thirdParty();');
  assert.deepEqual(checkFrontendTypes(root), {projects: 1, scripts: 1});
});

test('coverage gate rejects an unlisted first-party script', t => {
  const root = fixture(t);
  writeFileSync(path.join(root, 'vireo/static/forgotten.js'), 'const forgotten = true;');
  assert.throws(() => checkFrontendTypes(root), /omit 1 scripts/);
});

test('coverage gate fails for real implementation errors', t => {
  const root = fixture(t, {source: '/** @type {number} */ const count = "wrong";'});
  assert.throws(() => checkFrontendTypes(root), /Command failed/);
});

for (const options of [{checkJs: false}, {strict: false}, {noUncheckedIndexedAccess: false},
  {noEmit: false}, {noImplicitAny: false}, {strictNullChecks: false}, {strictBuiltinIteratorReturn: false}]) {
  test(`coverage gate rejects weakened options ${JSON.stringify(options)}`, t => {
    assert.throws(() => checkFrontendTypes(fixture(t, {options})), /must be true|cannot be disabled/);
  });
}

test('iterator results retain their value and completion types', t => {
  const source = '/** @type {string} */ const value = [123].values().next().value;';
  assert.throws(() => checkFrontendTypes(fixture(t, {source})), /Command failed/);
  assert.throws(() => checkFrontendTypes(fixture(t, {
    source, options: {strictBuiltinIteratorReturn: false},
  })), /cannot be disabled/);
});

test('coverage gate rejects skipping semantic checking', t => {
  assert.throws(() => checkFrontendTypes(fixture(t, {options: {noCheck: true}})), /cannot be enabled/);
});

for (const directive of ['@ts-nocheck', '@ts-ignore', '@ts-expect-error']) {
  test(`coverage gate rejects implementation suppression ${directive}`, t => {
    const root = fixture(t, {source: `// ${directive}\nconst count = 1;`});
    assert.throws(() => checkFrontendTypes(root), /must not suppress/);
  });

  test(`coverage gate rejects implementation suppression ${directive} in an ES module`, t => {
    const root = fixture(t, {source: `// ${directive}\nexport const count = 1;`});
    assert.throws(() => checkFrontendTypes(root), /must not suppress/);
  });
}

test('coverage gate checks ES module pages, including types across imports', t => {
  const root = fixture(t, {source: 'import {count} from "./state.js";\n/** @type {number} */ export const total = count;'});
  writeFileSync(path.join(root, 'vireo/static/state.js'), 'export const count = 1;');
  const project = path.join(root, 'tsconfig.frontend.json');
  const config = JSON.parse(readFileSync(project, 'utf8'));
  writeFileSync(project, JSON.stringify({...config, files: [...config.files, 'vireo/static/state.js']}));
  assert.deepEqual(checkFrontendTypes(root), {projects: 1, scripts: 2});
  writeFileSync(path.join(root, 'vireo/static/state.js'), 'export const count = "one";');
  assert.throws(() => checkFrontendTypes(root), /Command failed/);
});
