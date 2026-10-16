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
// name, length, and tracked body as a stale entry) keeps its entry under the new key,
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
// and `obj.render = () =>` are `render`. Synthesized names use reserved
// @call, @new, @returned and @anonymous tags with structured escaped payloads,
// preserving expression/member and argument boundaries. Source names escape
// %, #, . and @, so they cannot imitate tags, nested qualification or ordinals.
// When functions in the same scope share a key, later ones receive #2, #3, ...
// in source order; the suffix lives outside the encoded source context.
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
  return name.replace(/[%#.@]/g, char => ({'%': '%25', '#': '%23', '.': '%2E', '@': '%40'})[char]);
}

// Tagged arrays retain expression/member boundaries; source positions and raw
// quoting are excluded so inserting siblings or changing quote style is stable.
function syntaxIdentity(node) {
  return JSON.stringify(node, (key, value) => {
    if (['start', 'end', 'loc', 'raw'].includes(key)) return undefined;
    return typeof value === 'bigint' ? {bigint: String(value)} : value;
  });
}

function calleeName(callee) {
  if (callee.type === 'Identifier') return ['identifier', callee.name];
  if (callee.type === 'MemberExpression') {
    const property = callee.computed ? ['computed', syntaxIdentity(callee.property)]
      : ['property', callee.property.type, callee.property.name];
    return ['member', calleeName(callee.object), property, Boolean(callee.optional)];
  }
  return ['expression', syntaxIdentity(callee)];
}

// The name an anonymous function gets from its syntactic context.
function contextName(node, parent) {
  if (node.id) return node.id.name;
  if (!parent) return {kind: 'anonymous', value: null};
  switch (parent.type) {
    case 'VariableDeclarator':
      return parent.id.type === 'Identifier' ? parent.id.name : {kind: 'anonymous', value: syntaxIdentity(parent.id)};
    case 'AssignmentExpression':
      return keyName(parent.left) ?? {kind: 'anonymous', value: syntaxIdentity(parent.left)};
    case 'Property':
    case 'PropertyDefinition':
    case 'MethodDefinition':
      return keyName(parent.key) ?? {kind: 'anonymous', value: syntaxIdentity(parent.key)};
    case 'CallExpression':
    case 'NewExpression': {
      const callee = calleeName(parent.callee);
      const literal = parent.arguments.find(arg => arg.type === 'Literal' && typeof arg.value === 'string');
      return {kind: parent.type === 'NewExpression' ? 'new' : 'call', value: [callee, literal ? literal.value : null]};
    }
    case 'ReturnStatement':
      return {kind: 'returned', value: null};
    default:
      return {kind: 'anonymous', value: null};
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
      const context = contextName(node, parent);
      const name = typeof context === 'string' ? escapeName(context)
        : `@${context.kind}:${escapeName(JSON.stringify(context.value))}`;
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
// functions whose lengths are the previous lengths in another order may be a
// reordering. The writer requires complete tracked identity proof for it; then a key that is
// new to the baseline may continue a stale entry with the same name and
// length and tracked body from another file (a move), only if the stale key is
// gone from the current tree, so a surviving same-key function cannot lend
// its old length to a new over-cap sibling that just happens to match it.
// `surviving` is every current function key, including those under the cap,
// so a key that shrank below the cap is still recognised as present and
// cannot be consumed as a move without separate tracked-source identity proof.
// A function whose key is already in the
// baseline and whose group is not a pure reordering is compared to its own
// entry, so a deleted sibling's budget can never transfer to a growing
// function. Each old entry is consumed at most once.
function reconcileBaseline(current, previous, allowNew = [], bootstrapping = false, surviving = null, verifiedMoves = new Map(), requireIdentity = false) {
  const remaining = new Set(Object.keys(previous));
  const matched = new Set();
  const moves = [];
  const keys = Object.keys(current).sort();
  const survivingKeys = surviving ?? new Set(Object.keys(current));
  const groupOf = key => `${key.slice(0, key.indexOf(':'))}:${nameOf(key)}`;
  const sortedLengths = (object, list) => list.map(key => object[key]).sort((a, b) => a - b);

  // Complete tracked groups take precedence over coincidentally equal ordinal
  // keys, which may now belong to a different sibling of the same length.
  for (const [key, old] of verifiedMoves) {
    if (!remaining.has(old) || previous[old] !== current[key]) continue;
    const differentFile = old.slice(0, old.indexOf(':')) !== key.slice(0, key.indexOf(':'));
    if (differentFile && survivingKeys.has(old)) continue;
    remaining.delete(old);
    matched.add(key);
    if (key !== old) moves.push([old, key]);
  }
  for (const key of keys) {
    if (!matched.has(key) && remaining.has(key) && current[key] === previous[key]) {
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
  for (const [group, currentKeys] of requireIdentity ? [] : groups) {
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
  // No name/length-only move fallback: every transferred budget needs a proof.
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

// Share one pinned HEAD and parse cache across all move proofs. Missing Git,
// missing files or an unreliable prior parse cannot authorize a transfer.
function trackedFunctionReader() {
  let head;
  const files = new Map();
  return file => {
    if (head === undefined) {
      const repo = spawnSync('git', ['-C', ROOT, 'rev-parse', '--show-toplevel'], {encoding: 'utf8'});
      const revision = spawnSync('git', ['-C', ROOT, 'rev-parse', 'HEAD'], {encoding: 'utf8'});
      if (repo.status !== 0 || revision.status !== 0 || !/^[0-9a-f]{40,64}$/.test(revision.stdout.trim())
        || fs.realpathSync(repo.stdout.trim()) !== fs.realpathSync(ROOT)) head = null;
      else head = revision.stdout.trim();
    }
    if (head === null) return null;
    if (!files.has(file)) {
      const result = spawnSync('git', ['-C', ROOT, 'cat-file', 'blob', `${head}:${file}`], {encoding: 'utf8', maxBuffer: 8 * 1024 * 1024});
      const identities = new Map();
      let lengths = new Map();
      if (result.status === 0) {
        try { lengths = sourceLengths(result.stdout, file, identities); } catch { /* No reliable prior parse. */ }
      }
      files.set(file, {lengths, identities});
    }
    return files.get(file);
  };
}

// Every accepted transfer preserves a tracked body and consumes its budget
// once. Same-file group permutations do not pin occupied slots or AST owners.
function verifiedBodyMoves(current, previous, lengths, identities) {
  const verified = new Map();
  const readTracked = trackedFunctionReader();
  const groupOf = key => `${key.slice(0, key.indexOf(':'))}:${nameOf(key)}`;
  const groups = new Map();
  for (const key of Object.keys(current)) {
    const group = groupOf(key);
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group).push(key);
  }
  for (const [group, keys] of groups) {
    if (!keys.some(key => current[key] !== previous[key])) continue;
    const oldKeys = Object.keys(previous).filter(old => groupOf(old) === group);
    if (!oldKeys.length || oldKeys.length !== keys.length || !keys.every(key => identities.has(key))) continue;
    const prior = readTracked(keys[0].slice(0, keys[0].indexOf(':')));
    if (!prior) continue;
    const available = new Set(oldKeys);
    const proof = new Map();
    for (const key of keys) {
      const old = [...available].find(old => previous[old] === current[key]
        && prior.lengths.get(old) === previous[old] && prior.identities.get(old) === identities.get(key));
      if (old === undefined) break;
      available.delete(old);
      proof.set(key, old);
    }
    if (proof.size === keys.length) for (const [key, old] of proof) verified.set(key, old);
  }
  const used = new Set(verified.values());
  const stale = new Set(Object.keys(previous).filter(old => !lengths.has(old) && !used.has(old)));
  for (const key of Object.keys(current).filter(key => !(key in previous) && !verified.has(key) && identities.has(key))) {
    for (const old of stale) {
      if (nameOf(old) !== nameOf(key) || previous[old] !== current[key]) continue;
      const prior = readTracked(old.slice(0, old.indexOf(':')));
      if (prior?.lengths.get(old) === previous[old] && prior.identities.get(old) === identities.get(key)) {
        verified.set(key, old);
        stale.delete(old);
        break;
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
  const verifiedMoves = verifiedBodyMoves(current, previous, effectiveLengths, identities);
  const {grown, added, moves} = reconcileBaseline(current, previous, allowNew, bootstrapping, surviving, verifiedMoves, true);
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
    assert.deepEqual([...lengths.values()].sort((a, b) => a - b), [105, 106, 107, 110]);
    assert.equal(new Set([...lengths.keys()].map(nameOf)).size, 4);
    assert.ok([...lengths.keys()].every(key => key.startsWith('fixture.js:@call:')));
  });

  test('literal names cannot imitate tagged structural contexts', () => {
    const callback = lines => `function () {true;${'\n'.repeat(lines - 1)}}`;
    for (const form of [fn => `obj.foo(${fn});`, fn => `new Widget(${fn});`, fn => `[${fn}];`]) {
      const [key] = sourceLengths(form(callback(110))).keys();
      const forged = key.slice(key.indexOf(':') + 1);
      const source = `const x = {${JSON.stringify(forged)}() {true;${'\n'.repeat(104)}}};` + form(callback(110));
      assert.equal(new Set([...sourceLengths(source).keys()].map(nameOf)).size, 2);
    }
    const returned = `function outer() {
      const x = {'@returned:null'() {true;${'\n'.repeat(104)}}};
      return ${callback(110)};
    }`;
    assert.equal(new Set([...sourceLengths(returned).keys()].map(nameOf)).size, 3);
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
    }, {'a.js:render': 105}).added, ['b.js:render', 'b.js:render#2']);
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
    assert.deepEqual(
      reconcileBaseline({'b.js:render': 110}, previous, [], false, surviving,
        new Map([['b.js:render', 'a.js:render']])),
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
        new Map([['b.js:render', 'a.js:render']]),
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
    for (const current of [{}, {'a.js:render': 105}]) {
      const result = reconcileBaseline(current, previous);
      assert.deepEqual(result.grown, []);
      assert.deepEqual(result.added, []);
    }
    const moved = reconcileBaseline({'b.js:render': 110}, previous, [], false, null,
      new Map([['b.js:render', 'a.js:render']]));
    assert.deepEqual(moved.added, []);
    assert.deepEqual(moved.moves, [['a.js:render', 'b.js:render']]);
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
      // A vacated key without tracked source is insufficient proof of a move.
      const untrackedBaseline = fs.readFileSync(baselinePath, 'utf8');
      fs.unlinkSync(path.join(staticDir, 'a.js'));
      const moved = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
      assert.equal(moved.status, 1, `${moved.error || ''}\n${moved.stdout}\n${moved.stderr}`);
      assert.match(moved.stdout, /Refusing to add baseline entries/);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), untrackedBaseline);
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
      fs.unlinkSync(path.join(staticDir, 'callbacks.js'));
      const body = lines => 'true;' + '\n'.repeat(lines - 1);
      const method = (name, lines) => `const holder = {${JSON.stringify(name)}() {${body(lines)}}};`;
      const callback = lines => `function () {${body(lines)}}`;
      const contextCases = [
        (a, b) => method('obj.foo()', a) + `obj.foo(${callback(b)});`,
        (a, b) => method("obj.foo('x')", a) + `obj.foo('x', ${callback(b)});`,
        (a, b) => method('Widget()', a) + `new Widget(${callback(b)});`,
        (a, b) => method('<anonymous>', a) + `[${callback(b)}];`,
        (a, b) => `function outer() {${method('<returned>', a)} return ${callback(b)};}`,
        (a, b) => `Widget(${callback(a)}); new Widget(${callback(b)});`,
      ];
      for (const fixture of contextCases) {
        const original = JSON.stringify(overCap(sourceLengths(fixture(105, 110), 'vireo/static/contexts.js')), null, 2) + '\n';
        fs.writeFileSync(baselinePath, original);
        fs.writeFileSync(path.join(staticDir, 'contexts.js'), fixture(110, 105));
        const result = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8'});
        assert.equal(result.status, 1, `${result.stdout}\n${result.stderr}`);
        assert.match(result.stdout, /Refusing to raise baseline entries/);
        assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      }
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
      const gitEnv = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('GIT_')));
      Object.assign(gitEnv, {GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: globalConfig});
      const git = args => {
        const result = spawnSync('git', ['-c', `core.hooksPath=${empty}`, '-c', 'commit.gpgsign=false',
          '-c', 'user.name=Ratchet fixture', '-c', 'user.email=ratchet@example.invalid', ...args], {
          cwd: tmp, encoding: 'utf8',
          env: gitEnv,
        });
        assert.equal(result.status, 0, `${result.error || ''}\n${result.stdout}\n${result.stderr}`);
      };
      git(['init', '--quiet', `--template=${empty}`]);
      git(['add', 'vireo/static/a.js', 'tests/frontend/function_length_baseline.json']);
      git(['commit', '--quiet', '-m', 'Synthetic original function']);
      fs.writeFileSync(sourcePath, short + long);
      const accepted = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
      assert.equal(accepted.status, 0, `${accepted.stdout}\n${accepted.stderr}`);
      assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')), {'vireo/static/a.js:render#2': 110});
      // A changed new body, duplicate copies, and a still-long old key cannot
      // reuse the old entry even when the names and lengths would match.
      for (const source of [short + long.replace('{', '{/* changed */'), short + long + long, long + long]) {
        fs.writeFileSync(baselinePath, original);
        fs.writeFileSync(sourcePath, source);
        const rejected = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
        assert.equal(rejected.status, 1, `${rejected.stdout}\n${rejected.stderr}`);
        assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      }
      // Identical code in a different file must not bypass the survivor guard.
      fs.writeFileSync(sourcePath, short);
      fs.writeFileSync(path.join(staticDir, 'b.js'), long);
      const crossFile = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
      assert.equal(crossFile.status, 1, `${crossFile.stdout}\n${crossFile.stderr}`);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      fs.unlinkSync(sourcePath);
      for (const replacement of [long.replace('{', '{/* unrelated */'), long + long]) {
        fs.writeFileSync(baselinePath, original);
        fs.writeFileSync(path.join(staticDir, 'b.js'), replacement);
        const rejected = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
        assert.equal(rejected.status, 1, `${rejected.stdout}\n${rejected.stderr}`);
        assert.equal(fs.readFileSync(baselinePath, 'utf8'), original);
      }
      for (const movedBody of [long, long.replace(/\n/g, '\r\n')]) {
        fs.writeFileSync(baselinePath, original);
        fs.writeFileSync(path.join(staticDir, 'b.js'), movedBody);
        const moved = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
        assert.equal(moved.status, 0, `${moved.stdout}\n${moved.stderr}`);
        assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')), {'vireo/static/b.js:render': 110});
      }
      fs.unlinkSync(path.join(staticDir, 'b.js'));
      const declaration = (lines, label) => `function render() {/*${label}*/${'\n'.repeat(lines - 1)}}\n`;
      for (const sizes of [[105, 110], [110, 110]]) {
        const a = declaration(sizes[0], 'A');
        const b = declaration(sizes[1], 'B');
        const groupSource = a + b;
        const groupBaseline = JSON.stringify(overCap(sourceLengths(groupSource, 'vireo/static/a.js')), null, 2) + '\n';
        fs.writeFileSync(sourcePath, groupSource);
        fs.writeFileSync(baselinePath, groupBaseline);
        git(['add', 'vireo/static/a.js', 'tests/frontend/function_length_baseline.json']);
        git(['commit', '--quiet', '-m', `Synthetic sibling group ${sizes.join('-')}`]);
        fs.unlinkSync(sourcePath);
        for (const [source, exit] of [[groupSource, 0], [groupSource.replace('/*A*/', '/*replacement*/'), 1], [groupSource + a, 1]]) {
          fs.writeFileSync(baselinePath, groupBaseline);
          fs.writeFileSync(path.join(staticDir, 'b.js'), source);
          const result = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
          assert.equal(result.status, exit, `${result.stdout}\n${result.stderr}`);
          if (exit) assert.equal(fs.readFileSync(baselinePath, 'utf8'), groupBaseline);
          else assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')), overCap(sourceLengths(source, 'vireo/static/b.js')));
        }
        fs.unlinkSync(path.join(staticDir, 'b.js'));

        for (const source of [b + a, short + a + b, short + short + a + b, a + short + b, short + b + a]) {
          fs.writeFileSync(sourcePath, source);
          fs.writeFileSync(baselinePath, groupBaseline);
          const result = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
          assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
          assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')),
            overCap(sourceLengths(source, 'vireo/static/a.js')));
        }
        const invalid = [short + a.replace('/*A*/', '/*changed*/') + b, short + a + b + b];
        if (sizes[0] !== sizes[1]) invalid.push(b, declaration(sizes[1], 'A') + declaration(sizes[0], 'B'));
        for (const source of invalid) {
          fs.writeFileSync(sourcePath, source);
          fs.writeFileSync(baselinePath, groupBaseline);
          const result = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
          assert.equal(result.status, 1, `${result.stdout}\n${result.stderr}`);
          assert.equal(fs.readFileSync(baselinePath, 'utf8'), groupBaseline);
        }
        fs.writeFileSync(sourcePath, short + a + b);
        fs.writeFileSync(baselinePath, JSON.stringify(overCap(sourceLengths(short + a + b, 'vireo/static/a.js')), null, 2) + '\n');
        git(['add', 'vireo/static/a.js', 'tests/frontend/function_length_baseline.json']);
        git(['commit', '--quiet', '-m', 'Synthetic short sibling to remove']);
        fs.writeFileSync(sourcePath, groupSource);
        const removed = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
        assert.equal(removed.status, 0, `${removed.stdout}\n${removed.stderr}`);
        assert.equal(fs.readFileSync(baselinePath, 'utf8'), groupBaseline);
      }
      // Preserve the owner's body-based contract: unchanged bodies may exchange
      // occupied same-file slots, even under different enclosing conditions.
      const a = declaration(105, 'A');
      const b = declaration(110, 'B');
      const branches = (left, right) => `if (featureA) {${left}} if (featureB) {${right}}`;
      fs.writeFileSync(sourcePath, branches(a, b));
      fs.writeFileSync(baselinePath, JSON.stringify(overCap(sourceLengths(branches(a, b), 'vireo/static/a.js')), null, 2) + '\n');
      git(['add', 'vireo/static/a.js', 'tests/frontend/function_length_baseline.json']);
      git(['commit', '--quiet', '-m', 'Synthetic unchanged body slots']);
      fs.writeFileSync(sourcePath, branches(b, a));
      const swapped = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
      assert.equal(swapped.status, 0, `${swapped.stdout}\n${swapped.stderr}`);
      assert.deepEqual(JSON.parse(fs.readFileSync(baselinePath, 'utf8')), overCap(sourceLengths(branches(b, a), 'vireo/static/a.js')));
      // Without the tracked identity proof, even the harmless case fails closed.
      fs.renameSync(path.join(tmp, '.git'), path.join(tmp, 'saved-git'));
      fs.writeFileSync(baselinePath, original);
      fs.writeFileSync(sourcePath, short + long);
      const unproved = spawnSync(process.execPath, [script, '--write-baseline'], {encoding: 'utf8', env: gitEnv});
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
