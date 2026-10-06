// Runs the Jobs page's import Resume gate (vireo/static/jobs/) over the
// scenarios in the JSON file named by argv[2] and prints one result per
// scenario, so test_import_resume_takeover.py can hold it equal to the
// server's ``import_resume_takeover``.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const src = ['format.js', 'import-retry.js']
  .map(file => fs.readFileSync('vireo/static/jobs/' + file, 'utf8')).join('\n');
function fn(name) {
  // Page functions are declared at the top level of their classic script.
  const found = src.match(new RegExp('(?:^|\\n)function ' + name + '\\([^]*?\\n\\}\\n'));
  assert(found, name);
  return found[0];
}

const ctx = vm.createContext({});
for (const name of ['jobConfig', 'hasFailedImportFiles', 'isResumableImport', 'importResumeTakeover', 'importTakeoverNote']) {
  vm.runInContext(fn(name), ctx);
}
ctx.scenarios = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const out = vm.runInContext(`scenarios.map(function(s) {
  var takeover = importResumeTakeover(s.parent, s.rows);
  var offersResume = isResumableImport(s.parent) && !takeover.by;
  return {
    tags_applied: takeover.tagsApplied,
    chained: takeover.chained,
    by: takeover.by,
    kind: takeover.kind,
    // What the page offers on the parent row (renderJobCard).
    offers_resume: offersResume,
    offers_retry: Number(s.parent.result.failed || 0) > 0 && !offersResume && !takeover.by,
    note: takeover.by ? importTakeoverNote(takeover) : null,
  };
})`, ctx);
process.stdout.write(JSON.stringify(out));
