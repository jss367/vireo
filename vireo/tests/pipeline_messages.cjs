// Unit tests for splitPipelineMessages (vireo/static/pipeline-messages.js),
// which sorts a finished Process run's messages into failures and notes for
// the Process page. The file runs in a fresh VM context with no DOM, so a
// helper that starts reaching for page state fails here.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const ctx = vm.createContext({});
vm.runInContext(fs.readFileSync('vireo/static/pipeline-messages.js', 'utf8'), ctx);
const {splitPipelineMessages} = ctx;

// Values created inside a VM context have that context's prototypes, which
// deepStrictEqual treats as different from this realm's. Compare by JSON.
function same(actual, expected) {
  assert.deepEqual(JSON.parse(JSON.stringify(actual)), expected);
}

const tests = [];
function test(name, body) { tests.push([name, body]); }

const SUBTHRESHOLD = '[extract_masks] 3 photo(s) have detections but every ' +
  'detection is below the current detector_confidence threshold (0.2).';
const DOWNLOAD = '[eye_keypoints] Failed to download superanimal-bird weights';
const DENIED = '[scan] PERMISSION_DENIED: /Volumes/Card';

test('notes are not errors', () => {
  const out = splitPipelineMessages([SUBTHRESHOLD, DOWNLOAD], [SUBTHRESHOLD, DOWNLOAD]);
  same(out.errors, {});
  same(out.notes, {extract_masks: SUBTHRESHOLD, eye_keypoints: DOWNLOAD});
  same(out.noteList, [SUBTHRESHOLD, DOWNLOAD]);
  same(out.errorList, []);
});

test('a real error stays an error next to notes', () => {
  const out = splitPipelineMessages([DENIED, DOWNLOAD], [DOWNLOAD]);
  same(out.errors, {scan: DENIED});
  same(out.notes, {eye_keypoints: DOWNLOAD});
  same(out.errorList, [DENIED]);
});

test('older runs without a notes key treat every entry as an error', () => {
  for (const notes of [undefined, null, 'not a list']) {
    const out = splitPipelineMessages([SUBTHRESHOLD], notes);
    same(out.errors, {extract_masks: SUBTHRESHOLD});
    same(out.notes, {});
    same(out.noteList, []);
  }
});

test('missing or empty errors yield nothing', () => {
  for (const errors of [undefined, null, []]) {
    same(splitPipelineMessages(errors, undefined),
      {errors: {}, notes: {}, errorList: [], noteList: []});
  }
});

test('a stage with a real error is a failure, not a note, on its card', () => {
  const fatal = '[extract_masks] Fatal: 2 of 5 photos unreachable.';
  const out = splitPipelineMessages([SUBTHRESHOLD, fatal], [SUBTHRESHOLD]);
  same(out.errors, {extract_masks: fatal});
  same(out.notes, {});
  // The note is still listed for the notes banner.
  same(out.noteList, [SUBTHRESHOLD]);
});

test('a Fatal entry wins over an earlier generic error for its stage', () => {
  const generic = '[extract_masks] 1 of 2 photos failed mask extraction';
  const fatal = '[extract_masks] Fatal: 1 of 2 photos unreachable.';
  same(splitPipelineMessages([generic, fatal], []).errors, {extract_masks: fatal});
  same(splitPipelineMessages([fatal, generic], []).errors, {extract_masks: fatal});
});

test('an unprefixed message files under "unknown"', () => {
  same(splitPipelineMessages(['boom'], []).errors, {unknown: 'boom'});
});

let failed = 0;
for (const [name, body] of tests) {
  try {
    body();
  } catch (err) {
    failed++;
    console.error('FAIL ' + name + '\n' + (err && err.stack || err));
  }
}
console.log((tests.length - failed) + '/' + tests.length + ' pipeline message tests passed');
if (failed) process.exitCode = 1;
