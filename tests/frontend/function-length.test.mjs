// JavaScript function length only shrinks.
//
// The JS twin of vireo/tests/test_function_length_ratchet.py. Every function
// in a first-party script under vireo/static (vendor and *.min.js excluded)
// is capped at MAX_FUNCTION_LINES, counted from the line the function starts
// on to the line its body closes on, comments included. The functions already
// over the cap are listed in function_length_baseline.json with their exact
// current length: an entry that no longer matches fails the test (grown, or
// shrunk without lowering the entry, so a shrink cannot quietly be undone),
// an unlisted function may not cross the cap, and a stale entry (deleted,
// renamed, moved, or now under the cap) must be removed. Split a long
// function instead of listing it.
//
// Rewrite the baseline from the tree after shortening a function:
//
//     node tests/frontend/function-length.test.mjs --write-baseline
//
// The writer refuses to raise an entry or to add one, with two exceptions it
// can tell apart from growth: a function that moved between files (same
// name and same length as a stale entry) keeps its entry under the new key,
// and `--allow-new <path-prefix>` admits the functions of a page whose
// inline template script was just extracted to vireo/static/<page>/ (they
// were long before; the ratchet simply could not see inline JS, which Jinja
// expressions keep unparsable).
//
// Immediately invoked functions are exempt: they are scope wrappers (a file's
// IIFE, a page's boot block), and the functions inside them are measured on
// their own. A function's key is "<path>:<qualname>", where qualname nests
// the names of the enclosing functions with dots. An anonymous function is
// named by what it is assigned to or passed to: `const render = function`
// and `obj.render = () =>` are `render`; a callback is
// `<callee>(<first string argument>)`, so `addEventListener('click', ...)`
// is `addEventListener('click')`. When two functions in the same scope would
// share a key, the second and later ones get an ordinal suffix (`render#2`),
// in source order, so each keeps its own length; literal `%`, `#` and `.` in a
// source name are percent-escaped before adding ordinals, so an embedded `.`
// (a callback whose callee is a member chain like `document.addEventListener`)
// cannot collide with the structural `.` that joins stack elements. Callee member
// components are escaped before joining, retaining computed literal boundaries.
// A short sibling inserted before a long function may shift its ordinal. The
// writer accepts this only with an identical tracked HEAD implementation; without
// that proof, surviving keys keep their budgets and the ambiguous shift fails.

import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {parse} from 'acorn';

export const MAX_FUNCTION_LINES = 100;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const STATIC = path.join(ROOT, 'vireo', 'static');
export const BASELINE_PATH = path.join(ROOT, 'tests', 'frontend', 'function_length_baseline.json');

const FUNCTION_TYPES = new Set(['FunctionDeclaration', 'FunctionExpression', 'ArrowFunctionExpression']);

function* sourceFiles(dir = STATIC) {
  for (const entry of fs.readdirSync(dir, {withFileTypes: true}).sort((a, b) => a.name.localeCompare(b.name))) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== 'vendor') yield* sourceFiles(full);
    } else if (entry.name.endsWith('.js') && !entry.name.endsWith('.min.js')) {
      yield full;
    }
  }
}

function parseScript(source, file) {
  const options = {ecmaVersion: 'latest', locations: true, allowHashBang: true};
  try {
    return parse(source, {...options, sourceType: 'script'});
  } catch (scriptError) {
    try {
      return parse(source, {...options, sourceType: 'module'});
    } catch {
      throw new Error(`${file}: ${scriptError.message}`);
    }
  }
}

function keyName(node) {
  if (node.type === 'Identifier') return node.name;
  if (node.type === 'Literal') return String(node.value);
  if (node.type === 'MemberExpression') return keyName(node.property);
  return null;
}

function escapeName(name) {
  return name.replace(/[%#.]/g, char => char === '%' ? '%25' : char === '#' ? '%23' : '%2E');
}

// Reserve source-component boundaries before joining member chains. The walk's
// final escape then distinguishes a literal dot (%252E) from a member dot (%2E).
function calleeName(callee) {
  if (callee.type === 'Identifier') return escapeName(callee.name);
  if (callee.type === 'MemberExpression') {
    const object = calleeName(callee.object);
    const rawProperty = keyName(callee.property);
    const property = rawProperty === null ? null : escapeName(rawProperty);
    return object && property ? `${object}.${property}` : property;
  }
  return null;
}

// The name an anonymous function gets from its syntactic context.
function contextName(node, parent) {
  if (node.id) return node.id.name;
  if (!parent) return '<anonymous>';
  switch (parent.type) {
    case 'VariableDeclarator':
      return parent.id.type === 'Identifier' ? parent.id.name : '<anonymous>';
    case 'AssignmentExpression':
      return keyName(parent.left) || '<anonymous>';
    case 'Property':
    case 'PropertyDefinition':
    case 'MethodDefinition':
      return keyName(parent.key) || '<anonymous>';
    case 'CallExpression':
    case 'NewExpression': {
      const callee = calleeName(parent.callee) || '<call>';
      const literal = parent.arguments.find(arg => arg.type === 'Literal' && typeof arg.value === 'string');
      return literal ? `${callee}('${literal.value}')` : `${callee}()`;
    }
    case 'ReturnStatement':
      return '<returned>';
    default:
      return '<anonymous>';
  }
}

function isIife(node, parent) {
  return parent && parent.type === 'CallExpression' && parent.callee === node;
}

function walk(node, parent, stack, rel, lengths, seen, identities, source) {
  if (!node || typeof node.type !== 'string') return;
  let pushed = false;
  if (FUNCTION_TYPES.has(node.type)) {
    if (!isIife(node, parent)) {
      const name = escapeName(contextName(node, parent));
      const base = [...stack, name].join('.');
      const ordinal = (seen.get(base) || 0) + 1;
      seen.set(base, ordinal);
      stack.push(ordinal === 1 ? name : `${name}#${ordinal}`);
      pushed = true;
      const key = `${rel}:${stack.join('.')}`;
      lengths.set(key, node.loc.end.line - node.loc.start.line + 1);
      identities?.set(key, source.slice(node.start, node.end).replace(/\r\n/g, '\n'));
    }
  }
  for (const value of Object.values(node)) {
    if (Array.isArray(value)) {
      for (const child of value) if (child && typeof child.type === 'string') walk(child, node, stack, rel, lengths, seen, identities, source);
    } else if (value && typeof value.type === 'string' && value !== node.loc) {
      walk(value, node, stack, rel, lengths, seen, identities, source);
    }
  }
  if (pushed) stack.pop();
}

function sourceLengths(source, rel = 'fixture.js', identities = null) {
  const lengths = new Map();
  walk(parseScript(source, rel), null, [], rel, lengths, new Map(), identities, source);
  return lengths;
}

export function functionLengths(identities = null) {
  const lengths = new Map();
  for (const file of sourceFiles()) {
    const rel = path.relative(ROOT, file).split(path.sep).join('/');
    for (const [key, length] of sourceLengths(fs.readFileSync(file, 'utf8'), rel, identities)) lengths.set(key, length);
  }
  return lengths;
}

export function overCap(lengths) {
  const over = {};
  for (const [key, length] of lengths) if (length > MAX_FUNCTION_LINES) over[key] = length;
  return over;
}

export function loadBaseline() {
  return JSON.parse(fs.readFileSync(BASELINE_PATH, 'utf8'));
}

// The qualified name without its file, and without collision ordinals, so a
// function can match a moved entry or a separately verified ordinal shift.
function nameOf(key) {
  return key.slice(key.indexOf(':') + 1).replace(/#\d+(?=\.|$)/g, '');
}

function sortedObject(object) {
  return Object.fromEntries(Object.entries(object).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
}

// Which baseline entries each current over-cap function answers for. Exact
// matches first; then, within one file and name, a set of same-named
// functions whose lengths are the previous lengths in another order is a
// reordering (ordinals follow source order), not growth; then a key that is
// new to the baseline may continue a stale entry with the same name and
// length from another file (a move) — only if that stale key is actually
// gone from the current tree, so a surviving same-key function cannot lend
// its old length to a new over-cap sibling that just happens to match it.
// `surviving` is every current function key, including those under the cap,
// so a key that shrank below the cap is still recognised as present and
// cannot be consumed as a move without separate tracked-source identity proof.
// A function whose key is already in the
// baseline and whose group is not a pure reordering is compared to its own
// entry, so a deleted sibling's budget can never transfer to a growing
// function. Each old entry is consumed at most once.
function reconcileBaseline(current, previous, allowNew = [], bootstrapping = false, surviving = null, verifiedMoves = new Map()) {
  const remaining = new Set(Object.keys(previous));
  const matched = new Set();
  const moves = [];
  const keys = Object.keys(current).sort();
  const survivingKeys = surviving ?? new Set(Object.keys(current));
  const groupOf = key => `${key.slice(0, key.indexOf(':'))}:${nameOf(key)}`;
  const sortedLengths = (object, list) => list.map(key => object[key]).sort((a, b) => a - b);

  for (const key of keys) {
    if (current[key] === previous[key]) {
      remaining.delete(key);
      matched.add(key);
    }
  }
  const groups = new Map();
  for (const key of keys.filter(key => !matched.has(key) && key in previous)) {
    const group = groupOf(key);
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group).push(key);
  }
  for (const [group, currentKeys] of groups) {
    const oldKeys = [...remaining].filter(old => groupOf(old) === group);
    const reordered = oldKeys.length === currentKeys.length
      && sortedLengths(previous, oldKeys).join() === sortedLengths(current, currentKeys).join();
    if (!reordered) continue;
    for (const key of currentKeys) {
      const from = oldKeys.find(old => remaining.has(old) && previous[old] === current[key]);
      remaining.delete(from);
      matched.add(key);
      moves.push([from, key]);
    }
  }
  for (const key of keys.filter(key => !matched.has(key) && !(key in previous))) {
    const from = [...remaining].find(old => (!survivingKeys.has(old) || verifiedMoves.get(key)?.has(old))
      && nameOf(old) === nameOf(key) && previous[old] === current[key]);
    if (from !== undefined) {
      remaining.delete(from);
      matched.add(key);
      moves.push([from, key]);
    }
  }
  const grown = [];
  const added = [];
  for (const key of keys.filter(key => !matched.has(key))) {
    if (key in previous) {
      remaining.delete(key);
      if (current[key] > previous[key]) grown.push(key);
    } else if (!bootstrapping && !allowNew.some(prefix => key.startsWith(prefix))) {
      added.push(key);
    }
  }
  return {grown, added, moves};
}

// Lengths cannot identify an inserted short sibling versus a shrunk survivor.
// Only override that ambiguity for an unchanged implementation proved against
// this checkout's tracked HEAD. Missing Git/source/identity evidence fails closed.
function verifiedOrdinalMoves(current, previous, lengths, identities) {
  const verified = new Map();
  const tracked = new Map();
  const candidates = Object.keys(current).filter(key => !(key in previous) && identities.has(key));
  const sameFile = (a, b) => a.slice(0, a.indexOf(':')) === b.slice(0, b.indexOf(':'));
  let trackedHead = null;
  for (const key of candidates) {
    const oldKeys = Object.keys(previous).filter(old => lengths.has(old) && lengths.get(old) <= MAX_FUNCTION_LINES
      && previous[old] === current[key] && sameFile(old, key) && nameOf(old) === nameOf(key));
    if (!oldKeys.length) continue;
    if (trackedHead === null) {
      const repo = spawnSync('git', ['-C', ROOT, 'rev-parse', '--show-toplevel'], {encoding: 'utf8'});
      if (repo.status !== 0 || fs.realpathSync(repo.stdout.trim()) !== fs.realpathSync(ROOT)) return verified;
      const head = spawnSync('git', ['-C', ROOT, 'rev-parse', 'HEAD'], {encoding: 'utf8'});
      if (head.status !== 0 || !/^[0-9a-f]{40,64}$/.test(head.stdout.trim())) return verified;
      trackedHead = head.stdout.trim();
    }
    const file = key.slice(0, key.indexOf(':'));
    if (!tracked.has(file)) {
      const result = spawnSync('git', ['-C', ROOT, 'cat-file', 'blob', `${trackedHead}:${file}`], {encoding: 'utf8', maxBuffer: 8 * 1024 * 1024});
      const priorIdentities = new Map();
      let priorLengths = new Map();
      if (result.status === 0) {
        try { priorLengths = sourceLengths(result.stdout, file, priorIdentities); } catch { /* No reliable prior parse. */ }
      }
      tracked.set(file, {priorLengths, priorIdentities});
    }
    const {priorLengths, priorIdentities} = tracked.get(file);
    for (const old of oldKeys) {
      if (priorLengths.get(old) === previous[old] && priorIdentities.get(old) === identities.get(key)) {
        if (!verified.has(key)) verified.set(key, new Set());
        verified.get(key).add(old);
      }
    }
  }
  return verified;
}

export function writeBaseline(allowNew = [], {lengths = null, baselinePath = BASELINE_PATH} = {}) {
  const identities = new Map();
  const effectiveLengths = lengths ?? functionLengths(identities);
  const current = overCap(effectiveLengths);
  const bootstrapping = !fs.existsSync(baselinePath);
  const previous = bootstrapping ? {} : JSON.parse(fs.readFileSync(baselinePath, 'utf8'));
  const surviving = new Set(effectiveLengths.keys());
  const verifiedMoves = verifiedOrdinalMoves(current, previous, effectiveLengths, identities);
  const {grown, added, moves} = reconcileBaseline(current, previous, allowNew, bootstrapping, surviving, verifiedMoves);
  if (grown.length) {
    console.log('Refusing to raise baseline entries; shorten these functions instead:');
    for (const key of grown) console.log(`  ${key}: ${previous[key]} -> ${current[key]} lines`);
    return 1;
  }
  for (const [from, key] of moves) console.log(`  moved: ${from} -> ${key}`);
  if (added.length) {
    console.log(
      'Refusing to add baseline entries; split these functions instead '
      + '(a function extracted from an inline template script is admitted with '
      + '--allow-new <path-prefix>):',
    );
    for (const key of added) console.log(`  ${key} (${current[key]} lines)`);
    return 1;
  }
  fs.writeFileSync(baselinePath, JSON.stringify(sortedObject(current), null, 2) + '\n');
  console.log(`wrote ${Object.keys(current).length} entries to ${path.relative(ROOT, baselinePath)}`);
  return 0;
}

const argv = process.argv.slice(2);
if (argv[0] === '--write-baseline') {
  const allowNew = [];
  for (let i = 1; i < argv.length; i += 1) {
    if (argv[i] === '--allow-new' && argv[i + 1]) allowNew.push(argv[++i]);
    else {
      console.error('usage: node tests/frontend/function-length.test.mjs --write-baseline [--allow-new <path-prefix>]...');
      process.exit(2);
    }
  }
  process.exit(writeBaseline(allowNew));
} else {
  const {test} = await import('node:test');

  test('literal ordinal-like names cannot hide an over-cap function', () => {
    const longBody = '\n'.repeat(104);
    const lengths = sourceLengths(`const actions = {
      'render#2'() {${longBody}},
      render() {${longBody}},
      render() {},
      'render%232'() {},
    };`);
    assert.equal(lengths.size, 4);
    assert.equal(lengths.get('fixture.js:render#2'), 1);
    assert.equal(lengths.get('fixture.js:render%25232'), 1);
    assert.ok(overCap(lengths)['fixture.js:render']);
    assert.ok(overCap(lengths)['fixture.js:render%232']);
    assert.notEqual(nameOf('fixture.js:render#2'), nameOf('fixture.js:render%232'));
  });

  test('callee components preserve literal dots, percent escapes and member boundaries', () => {
    const callback = lines => `function () {${'\n'.repeat(lines - 1)}}`;
    const lengths = sourceLengths(`
      obj['outer.inner'](${callback(105)});
      obj.outer.inner(${callback(110)});
      obj['outer%2Einner'](${callback(106)});
      obj['outer#2.inner'](${callback(107)});
    `);
    assert.equal(lengths.size, 4);
    assert.equal(lengths.get('fixture.js:obj%2Eouter%252Einner()'), 105);
    assert.equal(lengths.get('fixture.js:obj%2Eouter%2Einner()'), 110);
    assert.equal(lengths.get('fixture.js:obj%2Eouter%25252Einner()'), 106);
    assert.equal(lengths.get('fixture.js:obj%2Eouter%25232%252Einner()'), 107);
    assert.notEqual(nameOf('fixture.js:obj%2Eouter%252Einner()'), nameOf('fixture.js:obj%2Eouter%2Einner()'));
  });

  test('embedded dots in source names cannot collide with structural qualification', () => {
    // A literal method named `outer.inner` and a function `inner` nested
    // inside a function `outer` would both land at base `outer.inner` without
    // escaping the embedded dot. The second would then be ordinalized as a
    // same-name sibling, and a closed length permutation between the two
    // would silently permit the literal method to grow. Escape the dot in
    // source names so the two keep distinct keys.
    const longBody = '\n'.repeat(104);
    const lengths = sourceLengths(`
      const obj = { 'outer.inner'() {${longBody}} };
      function outer() { function inner() {${longBody}} }
    `);
    assert.ok(lengths.has('fixture.js:outer%2Einner'), [...lengths.keys()].join('\n'));
    assert.ok(lengths.has('fixture.js:outer.inner'), [...lengths.keys()].join('\n'));
    assert.equal(lengths.get('fixture.js:outer%2Einner'), 105);
    assert.equal(lengths.get('fixture.js:outer.inner'), 105);
    assert.notEqual(nameOf('fixture.js:outer%2Einner'), nameOf('fixture.js:outer.inner'));
    const fixture = (literal, nested) => `
      const obj = { 'outer.inner'() {${'\n'.repeat(literal - 1)}} };
      function outer() { function inner() {${'\n'.repeat(nested - 1)}} }
    `;
    const previous = overCap(sourceLengths(fixture(105, 110)));
    const current = overCap(sourceLengths(fixture(110, 105)));
    assert.deepEqual(reconcileBaseline(current, previous).grown, ['fixture.js:outer%2Einner']);
  });

  test('same-named sibling permutations retain their individual length budgets', () => {
    const fixture = lengths => lengths.map(length => `{ function render() {${'\n'.repeat(length - 1)}} }`).join('\n');
    const previous = overCap(sourceLengths(fixture([105, 110]), 'a.js'));
    const current = overCap(sourceLengths(fixture([110, 105]), 'a.js'));
    assert.deepEqual(previous, {'a.js:render': 105, 'a.js:render#2': 110});
    assert.deepEqual(current, {'a.js:render': 110, 'a.js:render#2': 105});
    const result = reconcileBaseline(current, previous);
    assert.deepEqual(result.grown, []);
    assert.deepEqual(result.added, []);
    assert.equal(result.moves.length, 2);
  });

  test('reconciliation refuses growth, new entries and reuse of one old entry', () => {
    const previous = {'a.js:render': 105, 'a.js:render#2': 110};
    assert.deepEqual(reconcileBaseline({'a.js:render': 111}, previous).grown, ['a.js:render']);
    assert.deepEqual(reconcileBaseline({'a.js:render': 106, 'a.js:render#2': 110}, previous).grown, ['a.js:render']);
    assert.deepEqual(reconcileBaseline({...previous, 'a.js:render#3': 105}, previous).added, ['a.js:render#3']);
    assert.deepEqual(reconcileBaseline({'b.js:newName': 105}, previous).added, ['b.js:newName']);
    assert.deepEqual(reconcileBaseline({
      'b.js:render': 105, 'b.js:render#2': 105,
    }, {'a.js:render': 105}).added, ['b.js:render#2']);
    assert.deepEqual(reconcileBaseline({'a.js:render': 111}, previous, ['a.js:']).grown, ['a.js:render']);
  });

  test('a surviving same-key function cannot lend its length to a new over-cap sibling', () => {
    // previous has one over-cap `a.js:render` (110). It shrinks to 105 and
    // a new `b.js:render` appears at 110. The move rule must not consume
    // the still-present `a.js:render` as the source for the new key: the
    // new key is an addition.
    const previous = {'a.js:render': 110};
    assert.deepEqual(
      reconcileBaseline({'a.js:render': 105, 'b.js:render': 110}, previous),
      {grown: [], added: ['b.js:render'], moves: []},
    );
    // Same shape with an ordinal: `a.js:render#2` is still present at a
    // smaller length and the new `b.js:render` at its former length is an
    // addition, not a move.
    assert.deepEqual(
      reconcileBaseline(
        {'a.js:render': 105, 'a.js:render#2': 101, 'b.js:render': 110},
        {'a.js:render': 105, 'a.js:render#2': 110},
      ),
      {grown: [], added: ['b.js:render'], moves: []},
    );
  });

  test('a surviving function that shrank below the cap cannot lend its length to a new over-cap sibling', () => {
    // previous had one over-cap `a.js:render` (110). It shrinks to 100
    // (now under the cap, so overCap drops it from `current`) and a new
    // `b.js:render` appears at 110. The move rule must consult surviving
    // keys, not just the over-cap subset, or it would consume the still
    // present `a.js:render` as the source for the new key.
    const previous = {'a.js:render': 110};
    const surviving = new Set(['a.js:render', 'b.js:render']);
    assert.deepEqual(
      reconcileBaseline({'b.js:render': 110}, previous, [], false, surviving),
      {grown: [], added: ['b.js:render'], moves: []},
    );
    // Same shape with an ordinal that shrank below the cap and so is
    // missing from the over-cap `current`.
    assert.deepEqual(
      reconcileBaseline(
        {'b.js:render': 110},
        {'a.js:render#2': 110},
        [], false,
        new Set(['a.js:render', 'a.js:render#2', 'b.js:render']),
      ),
      {grown: [], added: ['b.js:render'], moves: []},
    );
    // The move rule still fires when the old key is truly gone from the tree.
    assert.deepEqual(
      reconcileBaseline(
        {'b.js:render': 110}, previous, [], false, new Set(['b.js:render']),
      ),
      {grown: [], added: [], moves: [['a.js:render', 'b.js:render']]},
    );
  });

  test('a deleted sibling cannot lend its budget to a same-named function that grew', () => {
    const previous = {'a.js:render': 105, 'a.js:render#2': 110};
    // The 110-line sibling is gone and the 105-line one grew to exactly 110:
    // not a reordering (lengths {110} vs {105, 110}), so render answers for
    // its own entry.
    assert.deepEqual(reconcileBaseline({'a.js:render': 110}, previous), {grown: ['a.js:render'], added: [], moves: []});
    // Same across files: the moved-looking key is new, but the surviving
    // same-key function is still measured against its own entry.
    assert.deepEqual(reconcileBaseline({'a.js:render': 110, 'b.js:render': 105}, {'a.js:render': 105}).grown, ['a.js:render']);
    // A genuine reordering of three siblings is still accepted whole.
    const three = {'a.js:render': 101, 'a.js:render#2': 102, 'a.js:render#3': 103};
    const rotated = {'a.js:render': 103, 'a.js:render#2': 101, 'a.js:render#3': 102};
    assert.deepEqual(reconcileBaseline(rotated, three).grown, []);
    assert.equal(reconcileBaseline(rotated, three).moves.length, 3);
  });

  test('writer reconciliation still permits shrinking, removal, moves and approved extraction', () => {
    const previous = {'a.js:render': 110, 'a.js:obsolete': 120};
    for (const current of [{}, {'a.js:render': 105}, {'b.js:render': 110}]) {
      const result = reconcileBaseline(current, previous);
      assert.deepEqual(result.grown, []);
      assert.deepEqual(result.added, []);
    }
    assert.deepEqual(reconcileBaseline({'b.js:newName': 105}, previous, ['b.js:']).added, []);
    assert.deepEqual(reconcileBaseline({'b.js:newName': 105}, {}, [], true).added, []);
  });

  test('CLI writer preserves budgets through source scanning and member-name encoding', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ratchet-cli-'));
    try {
      const tests = path.join(tmp, 'tests', 'frontend');
      const staticDir = path.join(tmp, 'vireo', 'static');
      fs.mkdirSync(tests, {recursive: true});
      fs.mkdirSync(staticDir, {recursive: true});
      const script = path.join(tests, 'function-length.test.mjs');
      fs.copyFileSync(fileURLToPath(import.meta.url), script);
      // Copy the installed parser so the real CLI can resolve it on every OS,
      // including Windows without privileged directory symlinks or a network.
      const acorn = path.dirname(fileURLToPath(import.meta.resolve('acorn/package.json')));
      fs.cpSync(acorn, path.join(tmp, 'node_modules', 'acorn'), {recursive: true});
      const baselinePath = path.join(tests, 'function_length_baseline.json');
      const declaration = lines => `function render() {${'\n'.repeat(lines - 1)}}\n`;
      fs.writeFileSync(path.join(staticDir, 'b.js'), declaration(110));
      for (const [lines, ordinal] of [[100, false], [99, false], [100, true]]) {
        const key = `vireo/static/a.js:render${ordinal ? '#2' : ''}`;
        const original = JSON.stringify({[key]: 110}, null, 2) + '\n';
        fs.writeFileSync(baselinePath, original);
        fs.writeFileSync(path.join(staticDir, 'a.js'), ordinal
          ? `function render() {}\n{${declaration(lines)}}` : declaration(lines));
        const result = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
        assert.equal(result.status, 1, `${result.error || ''}\n${result.stdout}\n${result.stderr}`);
        assert.match(result.stdout, /Refusing to add baseline entries/);
        assert.match(result.stdout, /vireo\/static\/b\.js:render/);
        assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      }
      // A genuinely vacated key still supports the existing move exception.
      fs.unlinkSync(path.join(staticDir, 'a.js'));
      const moved = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
      assert.equal(moved.status, 0, `${moved.error || ''}\n${moved.stdout}\n${moved.stderr}`);
      assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')), {'vireo/static/b.js:render': 110});
      // Run the full parser-to-writer path: these different callees must not
      // become ordinal siblings whose exchanged lengths hide callback growth.
      fs.unlinkSync(path.join(staticDir, 'b.js'));
      const callbacks = (literal, chain) => `
        obj['outer.inner'](function () {${'\n'.repeat(literal - 1)}});
        obj.outer.inner(function () {${'\n'.repeat(chain - 1)}});
      `;
      const starting = JSON.stringify(overCap(sourceLengths(callbacks(105, 110), 'vireo/static/callbacks.js')), null, 2) + '\n';
      fs.writeFileSync(baselinePath, starting);
      fs.writeFileSync(path.join(staticDir, 'callbacks.js'), callbacks(110, 105));
      const grown = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
      assert.equal(grown.status, 1, `${grown.error || ''}\n${grown.stdout}\n${grown.stderr}`);
      assert.match(grown.stdout, /Refusing to raise baseline entries/);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), starting);
    } finally {
      fs.rmSync(tmp, {recursive: true, force: true});
    }
  });

  test('CLI writer requires tracked source identity for an ordinal shift', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ratchet-identity-'));
    try {
      const tests = path.join(tmp, 'tests', 'frontend');
      const staticDir = path.join(tmp, 'vireo', 'static');
      const empty = path.join(tmp, 'empty');
      for (const dir of [tests, staticDir, empty]) fs.mkdirSync(dir, {recursive: true});
      const script = path.join(tests, 'function-length.test.mjs');
      fs.copyFileSync(fileURLToPath(import.meta.url), script);
      fs.cpSync(path.dirname(fileURLToPath(import.meta.resolve('acorn/package.json'))),
        path.join(tmp, 'node_modules', 'acorn'), {recursive: true});
      const baselinePath = path.join(tests, 'function_length_baseline.json');
      const sourcePath = path.join(staticDir, 'a.js');
      const original = JSON.stringify({'vireo/static/a.js:render': 110}, null, 2) + '\n';
      const long = `function render() {${'\n'.repeat(109)}}\n`;
      const short = 'function render() {}\n';
      fs.writeFileSync(sourcePath, long);
      fs.writeFileSync(baselinePath, original);
      const globalConfig = path.join(tmp, 'git-global-config');
      fs.writeFileSync(globalConfig, '');
      const git = args => {
        const result = spawnSync('git', ['-c', `core.hooksPath=${empty}`, '-c', 'commit.gpgsign=false',
          '-c', 'user.name=Ratchet fixture', '-c', 'user.email=ratchet@example.invalid', ...args], {
          cwd: tmp, encoding: 'utf8',
          env: {...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: globalConfig},
        });
        assert.equal(result.status, 0, `${result.error || ''}\n${result.stdout}\n${result.stderr}`);
      };
      git(['init', '--quiet', `--template=${empty}`]);
      git(['add', 'vireo/static/a.js', 'tests/frontend/function_length_baseline.json']);
      git(['commit', '--quiet', '-m', 'Synthetic original function']);
      fs.writeFileSync(sourcePath, short + long);
      const accepted = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
      assert.equal(accepted.status, 0, `${accepted.stdout}\n${accepted.stderr}`);
      assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')), {'vireo/static/a.js:render#2': 110});
      // A changed new body, duplicate copies, and a still-long old key cannot
      // reuse the old entry even when the names and lengths would match.
      for (const source of [short + long.replace('{', '{/* changed */'), short + long + long, long + long]) {
        fs.writeFileSync(baselinePath, original);
        fs.writeFileSync(sourcePath, source);
        const rejected = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
        assert.equal(rejected.status, 1, `${rejected.stdout}\n${rejected.stderr}`);
        assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      }
      // Identical code in a different file must not bypass the survivor guard.
      fs.writeFileSync(sourcePath, short);
      fs.writeFileSync(path.join(staticDir, 'b.js'), long);
      const crossFile = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
      assert.equal(crossFile.status, 1, `${crossFile.stdout}\n${crossFile.stderr}`);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      fs.unlinkSync(path.join(staticDir, 'b.js'));
      // Without the tracked identity proof, even the harmless case fails closed.
      fs.renameSync(path.join(tmp, '.git'), path.join(tmp, 'saved-git'));
      fs.writeFileSync(sourcePath, short + long);
      const unproved = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
      assert.equal(unproved.status, 1, `${unproved.stdout}\n${unproved.stderr}`);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
    } finally {
      fs.rmSync(tmp, {recursive: true, force: true});
    }
  });

  test(`no static script function crosses or grows past ${MAX_FUNCTION_LINES} lines`, () => {
    const baseline = loadBaseline();
    const current = overCap(functionLengths());
    const describe = keys => keys.map(key => `  ${key} (${current[key]} lines)`).join('\n');

    const added = Object.keys(current).filter(key => !(key in baseline)).sort();
    assert.equal(added.length, 0,
      `These functions are longer than ${MAX_FUNCTION_LINES} lines and not in `
      + 'function_length_baseline.json; split them rather than listing them '
      + '(for a function just extracted from a template, run the writer with --allow-new):\n'
      + describe(added));

    const grown = Object.keys(current).filter(key => key in baseline && current[key] > baseline[key]).sort();
    assert.equal(grown.length, 0,
      'These functions grew; a function over the cap may only get shorter:\n'
      + grown.map(key => `  ${key}: ${baseline[key]} -> ${current[key]} lines`).join('\n'));

    const shrunk = Object.keys(current).filter(key => key in baseline && current[key] < baseline[key]).sort();
    assert.equal(shrunk.length, 0,
      'These functions got shorter but their baseline entries still hold the old '
      + 'length, which would let them grow back; run this file with --write-baseline:\n'
      + shrunk.map(key => `  ${key}: ${baseline[key]} -> ${current[key]} lines`).join('\n'));
  });

  test('the baseline has no stale entries', () => {
    const baseline = loadBaseline();
    const current = overCap(functionLengths());
    const stale = Object.keys(baseline).filter(key => !(key in current)).sort();
    assert.equal(stale.length, 0,
      'These function_length_baseline.json entries no longer match a function over '
      + 'the cap (deleted, renamed, moved, or now short enough); delete them, or run '
      + 'this file with --write-baseline:\n' + stale.map(key => `  ${key}`).join('\n'));
  });
}
