// Unit tests for vireo/static/vireo-export-job.js: the toast the export modal
// shows once the export job ends. The shipped file runs in a fresh VM context
// with only the globals it may use, so a change to it is what gets tested.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const SOURCE = fs.readFileSync('vireo/static/vireo-export-job.js', 'utf8');

function load(globals) {
  const ctx = vm.createContext(Object.assign({}, globals));
  vm.runInContext(SOURCE, ctx);
  return ctx.VireoExportJob;
}

function same(actual, expected) {
  assert.deepEqual(JSON.parse(JSON.stringify(actual)), expected);
}

const tests = [];
function test(name, body) { tests.push([name, body]); }

const {outcome} = load({});

test('a successful export names the count and the folder', () => {
  same(outcome({status: 'completed', result: {
    exported: 1, errors: [], destination: '/Volumes/Photos/2026',
    destinations: ['/Volumes/Photos/2026'],
  }}), {message: 'Exported 1 photo to /Volumes/Photos/2026', type: 'success'});
  same(outcome({status: 'completed', result: {
    exported: 3, errors: [], destination: '', destinations: ['/a', '/b'],
  }}), {message: 'Exported 3 photos to 2 folders', type: 'success'});
});

test('numbered names are counted', () => {
  same(outcome({status: 'completed', result: {
    exported: 2, renamed: 1, errors: [], destinations: ['/out'],
  }}), {message: 'Exported 2 photos to /out (1 saved with a numbered name)', type: 'success'});
});

test('an export where every photo failed says nothing was exported and why', () => {
  same(outcome({status: 'failed', result: {
    exported: 0, renamed: 0, destinations: [],
    errors: ['_D850071.NEF: original folder is not reachable (/Volumes/Photography/2026)'],
  }}), {
    message: 'Nothing was exported. 1 photo failed: '
      + '_D850071.NEF: original folder is not reachable (/Volumes/Photography/2026)',
    type: 'error',
  });
});

test('a partial export reports both counts and caps the listed errors', () => {
  same(outcome({status: 'failed', result: {
    exported: 4, errors: ['a.NEF: x', 'b.NEF: y', 'c.NEF: z', 'd.NEF: w'],
    destinations: ['/out'],
  }}), {
    message: 'Exported 4 photos to /out. 4 photos failed: a.NEF: x; b.NEF: y; and 2 more',
    type: 'error',
  });
});

test('a job that crashed before returning a result shows its error', () => {
  same(outcome({status: 'failed', result: null, errors: ['disk full']}),
    {message: 'Export failed: disk full', type: 'error'});
  same(outcome({status: 'failed', result: null, errors: []}),
    {message: 'Export failed: the job ended before producing a result.', type: 'error'});
});

test('a stopped export says how far it got', () => {
  same(outcome({status: 'cancelled', result: null}),
    {message: 'Export stopped before any photo was exported.', type: 'warning'});
  same(outcome({status: 'cancelled', result: {exported: 2, errors: [], destinations: ['/out']}}),
    {message: 'Export stopped after exporting 2 photos to /out', type: 'warning'});
});

test('an expired job does not claim success', () => {
  assert.equal(outcome({status: 'expired', result: null}).type, 'warning');
});

test('watch toasts the outcome when the job stream completes', () => {
  const toasts = [];
  let streamed;
  const job = load({
    safeEventSource(url, callbacks) { streamed = url; callbacks.onComplete({
      status: 'completed', result: {exported: 1, errors: [], destinations: ['/out']},
    }); },
    showToast(message, type) { toasts.push([message, type]); },
  });
  job.watch('export-1');
  assert.equal(streamed, '/api/jobs/export-1/stream');
  same(toasts, [['Exported 1 photo to /out', 'success']]);
});

let failed = 0;
for (const [name, body] of tests) {
  try { body(); console.log('ok - ' + name); }
  catch (err) { failed++; console.log('not ok - ' + name + '\n' + err.stack); }
}
if (failed) process.exit(1);
