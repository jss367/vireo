import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

// The Jobs page's import throughput line comes from the backend's own
// estimate, which names the phase it describes (eta_phase). steps.js is an
// ES module; its imports are stubbed as context globals and format.js's
// formatElapsed is the real one.
function importEtaStatusLine(progress) {
  const context = vm.createContext({
    jobsElement() {}, LEAF_MAX: 10, state: {}, escapeHtml: s => s,
  });
  for (const name of ['format', 'steps']) {
    vm.runInContext(moduleAsScript(readFileSync(`vireo/static/jobs/${name}.js`, 'utf8')), context);
  }
  return vm.runInContext('importEtaStatusLine', context)(progress);
}

test('the copy phase renders as before, from completed batches', () => {
  assert.equal(importEtaStatusLine({eta_phase: 'copy', eta_state: 'estimating', eta_settled: 200}),
    'Estimating after the first transfer batch…');
  assert.equal(
    importEtaStatusLine({eta_phase: 'copy', eta_state: 'ready', eta_seconds: 90, eta_rate_per_min: 600}),
    '~600/min from completed batches &middot; ETA 1m 30s');
  assert.equal(importEtaStatusLine({eta_phase: 'copy', eta_state: 'ready', eta_seconds: 0}),
    'Copy batches complete &middot; finishing import…');
  // Older progress dicts without a phase label are the copy phase.
  assert.equal(importEtaStatusLine({eta_state: 'ready', eta_seconds: 0}),
    'Copy batches complete &middot; finishing import…');
});

test('the working-copy phase never reads as finishing while it is estimating', () => {
  assert.equal(importEtaStatusLine({eta_phase: 'working_copies', eta_state: 'estimating', eta_settled: 27}),
    'Estimating after the first working copies…');
});

test('the working-copy phase shows its own rate and ETA', () => {
  assert.equal(
    importEtaStatusLine({
      eta_phase: 'working_copies', eta_state: 'ready', eta_settled: 27,
      eta_seconds: 11562, eta_rate_per_min: 30,
    }),
    '~30/min working copies &middot; ETA 3h 12m');
  assert.equal(importEtaStatusLine({eta_phase: 'working_copies', eta_state: 'ready', eta_seconds: 0}),
    'Working copies complete &middot; finishing import…');
});
