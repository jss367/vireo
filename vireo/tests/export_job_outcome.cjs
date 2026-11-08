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

test('only folders that received a file are counted', () => {
  same(outcome({status: 'failed', result: {
    exported: 1, errors: ['b.NEF: decode failed'], destination: '',
    destinations: ['/a', '/b'], written_destinations: ['/a'],
  }}), {message: 'Exported 1 photo to /a. 1 photo failed: b.NEF: decode failed', type: 'error'});
  same(outcome({status: 'completed', result: {
    exported: 2, errors: [], destinations: ['/a', '/b'], written_destinations: ['/a', '/b'],
  }}), {message: 'Exported 2 photos to 2 folders', type: 'success'});
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
    {message: 'Export stopped before any photo was exported', type: 'warning'});
  // Stopped mid-run before the first file finished: the job still returns a
  // result, with nothing exported.
  same(outcome({status: 'cancelled', result: {exported: 0, errors: [], destinations: ['/out']}}),
    {message: 'Export stopped before any photo was exported', type: 'warning'});
  same(outcome({status: 'cancelled', result: {exported: 2, errors: [], destinations: ['/out']}}),
    {message: 'Export stopped after exporting 2 photos to /out', type: 'warning'});
});

test('an expired job does not claim success', () => {
  assert.equal(outcome({status: 'expired', result: null}).type, 'warning');
});

test('a cancelled export with a zero-photo result reads the same in toast and history', () => {
  const toasts = [];
  const job = load({
    safeEventSource(url, callbacks) { callbacks.onComplete({
      status: 'cancelled', result: {exported: 0, errors: [], destinations: ['/out']},
    }); },
    showToast(message, type, options) { toasts.push([message, type, options]); },
  });
  job.watch('export-cancel');
  // The persistent summary (first argument) and the expanded details must agree,
  // not read "Export stopped after exporting 0 photos" against "stopped before any photo".
  assert.equal(toasts[0][0], 'Export stopped before any photo was exported.');
  assert.equal(toasts[0][2].details, 'Export stopped before any photo was exported');
});

test('watch toasts the outcome when the job stream completes', () => {
  const toasts = [];
  let streamed;
  const job = load({
    safeEventSource(url, callbacks) { streamed = url; callbacks.onComplete({
      status: 'completed', result: {exported: 1, errors: [], destinations: ['/out']},
    }); },
    showToast(message, type, options) { toasts.push([message, type, options]); },
  });
  job.watch('export-1');
  assert.equal(streamed, '/api/jobs/export-1/stream');
  same(toasts, [['Exported 1 photo.', 'success', {details: 'Exported 1 photo to /out', jobId: 'export-1'}]]);
});

test('watch points to the jobs panel when the stream drops', () => {
  const toasts = [];
  const job = load({
    safeEventSource(url, callbacks) {
      assert.equal(callbacks.quietError, true, 'the generic Connection lost toast must be suppressed');
      callbacks.onError();
    },
    showToast(message, type, options) { toasts.push([message, type, options]); },
  });
  job.watch('export-2');
  same(toasts, [['Could not follow the export to the end. Check job details.', 'warning', {jobId: 'export-2'}]]);
});

test('metadata-free exports are explicitly identified', () => {
  const result = outcome({status: 'completed', result: {exported: 1,
    metadata: {metadata_free: true, categories: {}}}});
  assert.match(result.message, /Metadata: none selected \(metadata-free export\)/);
  assert.equal(result.type, 'success');
});

test('metadata counts distinguish writes, unavailable values and failures', () => {
  const result = outcome({status: 'completed', result: {exported: 2,
    metadata: {metadata_free: false, categories: {
      location: {requested: 4, written: 1, unavailable: 1, failed: 1, not_attempted: 1},
    }}}});
  assert.match(result.message, /Location \(GPS\) 1\/4 written, 1 unavailable, 1 failed, 1 not attempted/);
  assert.equal(result.type, 'warning');
});

test('successful metadata writes are shown separately from image counts', () => {
  const result = outcome({status: 'completed', result: {exported: 2,
    metadata: {metadata_free: false, categories: {
      species: {requested: 2, written: 2, unavailable: 0, failed: 0, not_attempted: 0},
    }}}});
  assert.match(result.message, /Species keywords 2\/2 written/);
  assert.equal(result.type, 'success');
});

test('metadata warnings stay concise while retaining the complete report and job link', () => {
  const toasts = [];
  const job = load({
    safeEventSource(url, callbacks) { callbacks.onComplete({status: 'completed', result: {
      exported: 1, destination: '/photos', metadata: {metadata_free: false, categories: {
        species: {requested: 1, written: 0, unavailable: 1, failed: 0, not_attempted: 0},
        location: {requested: 1, written: 1, unavailable: 0, failed: 0, not_attempted: 0},
      }},
    }}); },
    showToast(...args) { toasts.push(args); },
  });
  job.watch('export-3');
  assert.equal(toasts[0][0], 'Exported 1 photo. Species keywords unavailable.');
  assert.equal(toasts[0][1], 'warning');
  assert.equal(toasts[0][2].jobId, 'export-3');
  assert.match(toasts[0][2].details, /Location \(GPS\) 1\/1 written/);
});

let failed = 0;
for (const [name, body] of tests) {
  try { body(); console.log('ok - ' + name); }
  catch (err) { failed++; console.log('not ok - ' + name + '\n' + err.stack); }
}
if (failed) process.exit(1);
