const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id, {textContent: '', style: {}});
  return elements.get(id);
}
const timers = [];
const requests = [];
const page = vm.createContext({
  document: {getElementById: element},
  reviewScopeMode: 'collection', reviewScopeCollectionId: 1,
  setTimeout: callback => {timers.push(callback); return timers.length;},
  clearTimeout: id => {timers[id - 1] = null;},
  safeFetch: url => new Promise((resolve, reject) => requests.push({url, resolve, reject})),
});
vm.runInContext(fs.readFileSync('vireo/static/pipeline-review/coverage.js', 'utf8'), page);

function response(original, catalog) {
  return {
    original_import: {count: original, recorded_at: null},
    current_membership: {catalog_count: catalog, online_folder_count: catalog, offline_folder_count: 0},
    original_files: {known_missing_count: null, definition: 'Files are checked separately'},
    snapshot_at: '2026-10-20T12:00:00Z',
    review: {denominator: catalog, prediction_decision_count: 0, with_species_count: 0,
      without_species_count: catalog, unresolved_identity_count: 2, unresolved_definition: 'Explicit unresolved alternatives',
      denominator_definition: 'Current membership',
      decision_definition: 'Recorded predictions', identification_definition: 'Species keywords',
      inspection_definition: 'Result loading does not prove inspection'},
  };
}

async function main() {
  page.refreshCollectionCoverage();
  timers[0]();
  page.reviewScopeCollectionId = 2;
  page.refreshCollectionCoverage();
  timers[1]();
  requests[1].resolve(response(3303, 3159));
  await new Promise(setImmediate);
  assert.match(element('collectionCoverageCounts').textContent, /3,303 originally imported/);
  assert.match(element('collectionCoverageCounts').textContent, /3,159 current catalog members/);
  assert.match(element('collectionCoverageReview').textContent, /0 \/ 3159 with recorded prediction decisions/);
  assert.match(element('collectionCoverageReview').textContent, /Inspected: unknown/);
  assert.match(element('collectionCoverageReview').textContent, /2 \/ 3159 explicitly unresolved/);
  assert.match(element('collectionCoverageDefinitions').textContent, /does not establish deletion or data loss/);
  assert.match(element('collectionCoverageDefinitions').textContent, /accessibility is unknown/);

  requests[0].resolve(response(10, 10));
  await new Promise(setImmediate);
  assert.match(element('collectionCoverageCounts').textContent, /3,159 current catalog members/,
    'an old collection response cannot overwrite the new scope');

  page.refreshCollectionCoverage();
  timers[2]();
  page.reviewScopeMode = 'folder';
  page.refreshCollectionCoverage();
  requests[2].resolve(response(500, 500));
  await new Promise(setImmediate);
  assert.equal(element('collectionCoverage').style.display, 'none');

  page.renderCollectionCoverage(response(null, 0));
  assert.match(element('collectionCoverageCounts').textContent, /Original import size unknown/);
  assert.match(element('collectionCoverageCounts').textContent, /0 current catalog members/);
  assert.doesNotMatch(element('collectionCoverageCounts').textContent, /0 originally imported/);
  console.log('Collection coverage: definitions, unknown totals and stale scopes passed');
}
main().catch(error => {console.error(error); process.exitCode = 1;});
