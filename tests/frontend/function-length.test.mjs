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
// in source order, so each keeps its own length; the writer's move rule
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
      const base = [...stack, contextName(node, parent)].join('.');
      const ordinal = (seen.get(base) || 0) + 1;
      seen.set(base, ordinal);
      stack.push(ordinal === 1 ? contextName(node, parent) : `${contextName(node, parent)}#${ordinal}`);
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

export function functionLengths() {
  const lengths = new Map();
  for (const file of sourceFiles()) {
    const rel = path.relative(ROOT, file).split(path.sep).join('/');
    const tree = parseScript(fs.readFileSync(file, 'utf8'), rel);
    walk(tree, null, [], rel, lengths, new Map());
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

export function writeBaseline(allowNew = []) {
  const current = overCap(functionLengths());
  const bootstrapping = !fs.existsSync(BASELINE_PATH);
  const previous = bootstrapping ? {} : loadBaseline();
  const grown = Object.keys(current).filter(key => key in previous && current[key] > previous[key]).sort();
  if (grown.length) {
    console.log('Refusing to raise baseline entries; shorten these functions instead:');
    for (const key of grown) console.log(`  ${key}: ${previous[key]} -> ${current[key]} lines`);
    return 1;
  }
  const stale = Object.keys(previous).filter(key => !(key in current));
  const moved = new Set();
  const added = [];
  for (const key of Object.keys(current).filter(key => !(key in previous)).sort()) {
    const from = stale.find(old => !moved.has(old) && nameOf(old) === nameOf(key) && previous[old] === current[key]);
    if (from) {
      moved.add(from);
      console.log(`  moved: ${from} -> ${key}`);
    } else if (!bootstrapping && !allowNew.some(prefix => key.startsWith(prefix))) {
      added.push(key);
    }
  }
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
