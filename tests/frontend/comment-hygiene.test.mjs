// JavaScript comments describe the code as it is, not how it got here.
//
// The JS half of vireo/tests/test_comment_hygiene.py, which explains the
// rule. Both read their patterns from vireo/tests/comment_hygiene_patterns.json
// and share vireo/tests/comment_hygiene_baseline.json: this file owns the
// `.js` entries (first-party scripts under vireo/static, vendor and *.min.js
// excluded), the Python test owns the `.py` ones, and each writer leaves the
// other's entries untouched. Comments come from acorn's onComment, so strings,
// template literals and regular expressions are never mistaken for comments.
//
// The counts must match exactly. A new match fails: reword the comment. A
// removed one fails until its entry is lowered, so a cleanup cannot quietly
// be undone. Rewrite the `.js` entries from the tree with
//
//     node tests/frontend/comment-hygiene.test.mjs --write-baseline
//
// which refuses, exiting nonzero without writing, to raise a count or add one.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {parse} from 'acorn';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const VIREO = path.join(ROOT, 'vireo');
export const BASELINE_PATH = path.join(VIREO, 'tests', 'comment_hygiene_baseline.json');
const PATTERNS_PATH = path.join(VIREO, 'tests', 'comment_hygiene_patterns.json');
const WRITER = 'node tests/frontend/comment-hygiene.test.mjs --write-baseline';

const PATTERNS = Object.entries(JSON.parse(fs.readFileSync(PATTERNS_PATH, 'utf8')))
  .map(([name, spec]) => [name, new RegExp(spec.source, `${spec.flags}g`)]);

const isOwned = module => module.endsWith('.js');

function* sourceFiles(dir) {
  for (const entry of fs.readdirSync(dir, {withFileTypes: true}).sort((a, b) => a.name.localeCompare(b.name))) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== 'vendor') yield* sourceFiles(full);
    } else if (entry.name.endsWith('.js') && !entry.name.endsWith('.min.js')) {
      yield full;
    }
  }
}

/** Comment bodies in `source`, in order, without their delimiters. */
export function commentTexts(source, file = 'fixture.js') {
  const comments = [];
  const options = {ecmaVersion: 'latest', allowHashBang: true, onComment: (block, text) => comments.push(text)};
  try {
    parse(source, {...options, sourceType: 'script'});
  } catch (scriptError) {
    comments.length = 0;
    try {
      parse(source, {...options, sourceType: 'module'});
    } catch {
      throw new Error(`${file}: ${scriptError.message}`);
    }
  }
  return comments;
}

export function countMatches(texts) {
  const counts = {};
  for (const [name, pattern] of PATTERNS) {
    const total = texts.reduce((sum, text) => sum + (text.match(pattern)?.length ?? 0), 0);
    if (total) counts[name] = total;
  }
  return counts;
}

/** `{"static/<path>.js": {pattern: matches}}`, nonzero only, keyed relative to `vireo/`. */
export function moduleCounts(vireoDir = VIREO) {
  const result = {};
  for (const file of sourceFiles(path.join(vireoDir, 'static'))) {
    const counts = countMatches(commentTexts(fs.readFileSync(file, 'utf8'), file));
    if (Object.keys(counts).length) result[path.relative(vireoDir, file).split(path.sep).join('/')] = counts;
  }
  return result;
}

const loadBaseline = (baselinePath = BASELINE_PATH) => JSON.parse(fs.readFileSync(baselinePath, 'utf8'));

const ownedEntries = counts => Object.fromEntries(Object.entries(counts).filter(([module]) => isOwned(module)));

function flatten(counts) {
  const flat = new Map();
  for (const [module, byName] of Object.entries(counts)) {
    for (const [name, n] of Object.entries(byName)) flat.set(`${module} [${name}]`, n);
  }
  return flat;
}

const describe = (keys, before, after) =>
  keys.map(key => `  ${key}: ${before.get(key) ?? 0} -> ${after.get(key) ?? 0}`).join('\n');

// The same layout as the Python writer's json.dumps output, so either writer
// can rewrite the file without reformatting the other's lines.
function dump(counts) {
  const lines = Object.keys(counts).sort().map(module => {
    const byName = Object.keys(counts[module]).sort().map(name => `${JSON.stringify(name)}: ${counts[module][name]}`);
    return `  ${JSON.stringify(module)}: {${byName.join(', ')}}`;
  });
  return `{\n${lines.join(',\n')}${lines.length ? '\n' : ''}}\n`;
}

export function writeBaseline({vireoDir = VIREO, baselinePath = BASELINE_PATH} = {}) {
  const current = moduleCounts(vireoDir);
  let others = {};
  if (fs.existsSync(baselinePath)) {
    const existing = loadBaseline(baselinePath);
    others = Object.fromEntries(Object.entries(existing).filter(([module]) => !isOwned(module)));
    const before = flatten(ownedEntries(existing));
    const after = flatten(current);
    const raised = [...after.keys()].filter(key => after.get(key) > (before.get(key) ?? 0)).sort();
    if (raised.length) {
      console.error(`Refusing to raise or add baseline counts; reword these comments instead:\n${describe(raised, before, after)}`);
      return 1;
    }
  }
  fs.writeFileSync(baselinePath, dump({...others, ...current}));
  const total = [...flatten(current).values()].reduce((sum, n) => sum + n, 0);
  console.log(`Wrote the JavaScript entries of ${path.basename(baselinePath)}: ${total} matches in ${Object.keys(current).length} modules`);
  return 0;
}

const argv = process.argv.slice(2);
if (argv[0] === '--write-baseline') {
  if (argv.length !== 1) {
    console.error(`usage: ${WRITER}`);
    process.exit(2);
  }
  process.exit(writeBaseline());
} else {
  const {test} = await import('node:test');

  test('JavaScript comment counts match the baseline exactly', () => {
    const baseline = flatten(ownedEntries(loadBaseline()));
    const current = flatten(moduleCounts());

    const grown = [...current.keys()].filter(key => current.get(key) > (baseline.get(key) ?? 0)).sort();
    assert.equal(grown.length, 0,
      'These comments cite a review, a PR, or the code\'s history. Say what the code '
      + `does and why, in the present tense, instead:\n${describe(grown, baseline, current)}`);

    const shrunk = [...baseline.keys()].filter(key => (current.get(key) ?? 0) < baseline.get(key)).sort();
    assert.equal(shrunk.length, 0,
      `These counts dropped but ${path.basename(BASELINE_PATH)} still holds the old ones, which `
      + `would let them grow back. Run \`${WRITER}\` and commit the result:\n${describe(shrunk, baseline, current)}`);
  });

  test('patterns match the same text as in the Python ratchet', () => {
    assert.deepEqual(countMatches([
      '# Codex #1388 P1 r3663278142: the old shape',
      '# moved verbatim from app.py (PR #2133)',
      '// this used to raise; the key is used to look it up',
      '/* &#10003; check mark, see issues/1234, color #333abc */',
    ]), {codex: 1, review_id: 1, pr_ref: 2, moved_verbatim: 1, used_to: 1});
  });

  test('only comments are read, never strings, templates or regular expressions', () => {
    const source = [
      "const u = 'http://x/#1234'; // Codex",
      'const t = `a ${\'b\' /* PR #12 */} //c`;',
      'const r = /\\/\\//g; /* used to */',
      'for (const x of /re/) { /* keep */ }',
      'function f(of) { return of / 2; } // tail',
    ].join('\n');
    assert.deepEqual(commentTexts(source), [' Codex', ' PR #12 ', ' used to ', ' keep ', ' tail']);
  });

  test('module scripts are parsed as modules', () => {
    assert.deepEqual(commentTexts("import {a} from './a.js'; // Codex\nexport const b = a;\n"), [' Codex']);
    assert.throws(() => commentTexts('const = ;', 'broken.js'), /broken\.js/);
  });

  test('the writer rewrites only the .js entries and refuses to raise or add one', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'comment-hygiene-'));
    try {
      const vireoDir = path.join(tmp, 'vireo');
      fs.mkdirSync(path.join(vireoDir, 'static', 'vendor'), {recursive: true});
      const baselinePath = path.join(tmp, 'baseline.json');
      const script = path.join(vireoDir, 'static', 'a.js');
      const write = () => writeBaseline({vireoDir, baselinePath});
      fs.writeFileSync(path.join(vireoDir, 'static', 'vendor', 'lib.js'), '// Codex\n');
      fs.writeFileSync(path.join(vireoDir, 'static', 'lib.min.js'), '// Codex\n');

      fs.writeFileSync(script, '// Codex: one\n// Codex: two\n');
      assert.equal(write(), 0); // no baseline yet: written in full
      assert.deepEqual(loadBaseline(baselinePath), {'static/a.js': {codex: 2}});

      fs.writeFileSync(baselinePath, '{\n  "m.py": {"codex": 4, "used_to": 1},\n  "static/a.js": {"codex": 2}\n}\n');
      const before = fs.readFileSync(baselinePath, 'utf8');
      fs.writeFileSync(script, '// Codex: one\n// Codex: two\n// Codex: three\n');
      assert.equal(write(), 1);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), before);
      fs.writeFileSync(script, '// Codex: one\n// used to\n');
      assert.equal(write(), 1);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'), before);

      fs.writeFileSync(script, '// Codex: one\n');
      assert.equal(write(), 0);
      assert.equal(fs.readFileSync(baselinePath, 'utf8'),
        '{\n  "m.py": {"codex": 4, "used_to": 1},\n  "static/a.js": {"codex": 1}\n}\n');
    } finally {
      fs.rmSync(tmp, {recursive: true, force: true});
    }
  });
}
