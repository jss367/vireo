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
// in source order, so each keeps its own length; literal `%` and `#` in a
// source name are percent-escaped before adding ordinals. The writer's move rule
// ignores the ordinal, so a sibling inserted before an over-cap function
// does not count as growth.

import assert from 'node:assert/strict';
import fs from 'node:fs';
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

function calleeName(callee) {
  if (callee.type === 'Identifier') return callee.name;
  if (callee.type === 'MemberExpression') {
    const object = calleeName(callee.object);
    const property = keyName(callee.property);
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

function walk(node, parent, stack, rel, lengths, seen) {
  if (!node || typeof node.type !== 'string') return;
  let pushed = false;
  if (FUNCTION_TYPES.has(node.type)) {
    if (!isIife(node, parent)) {
      const name = contextName(node, parent).replace(/[%#]/g, char => char === '%' ? '%25' : '%23');
      const base = [...stack, name].join('.');
      const ordinal = (seen.get(base) || 0) + 1;
      seen.set(base, ordinal);
      stack.push(ordinal === 1 ? name : `${name}#${ordinal}`);
      pushed = true;
      lengths.set(`${rel}:${stack.join('.')}`, node.loc.end.line - node.loc.start.line + 1);
    }
  }
  for (const value of Object.values(node)) {
    if (Array.isArray(value)) {
      for (const child of value) if (child && typeof child.type === 'string') walk(child, node, stack, rel, lengths, seen);
    } else if (value && typeof value.type === 'string' && value !== node.loc) {
      walk(value, node, stack, rel, lengths, seen);
    }
  }
  if (pushed) stack.pop();
}

function sourceLengths(source, rel = 'fixture.js') {
  const lengths = new Map();
  walk(parseScript(source, rel), null, [], rel, lengths, new Map());
  return lengths;
}

export function functionLengths() {
  const lengths = new Map();
  for (const file of sourceFiles()) {
    const rel = path.relative(ROOT, file).split(path.sep).join('/');
    for (const [key, length] of sourceLengths(fs.readFileSync(file, 'utf8'), rel)) lengths.set(key, length);
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
// function that moved files or gained an earlier same-named sibling still
// matches its old entry.
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
// length from another file (a move). A function whose key is already in the
// baseline and whose group is not a pure reordering is compared to its own
// entry, so a deleted sibling's budget can never transfer to a growing
// function. Each old entry is consumed at most once.
function reconcileBaseline(current, previous, allowNew = [], bootstrapping = false) {
  const remaining = new Set(Object.keys(previous));
  const matched = new Set();
  const moves = [];
  const keys = Object.keys(current).sort();
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
    const from = [...remaining].find(old => nameOf(old) === nameOf(key) && previous[old] === current[key]);
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

export function writeBaseline(allowNew = []) {
  const current = overCap(functionLengths());
  const bootstrapping = !fs.existsSync(BASELINE_PATH);
  const previous = bootstrapping ? {} : loadBaseline();
  const {grown, added, moves} = reconcileBaseline(current, previous, allowNew, bootstrapping);
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
  fs.writeFileSync(BASELINE_PATH, JSON.stringify(sortedObject(current), null, 2) + '\n');
  console.log(`wrote ${Object.keys(current).length} entries to ${path.relative(ROOT, BASELINE_PATH)}`);
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
