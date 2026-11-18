import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

function harness() {
  const status = {};
  const context = vm.createContext({
    state: {},
    document: {getElementById: () => status, addEventListener: () => {}},
    escapeAttr: String,
  });
  for (const name of ['cards', 'location']) {
    const path = new URL(`../../vireo/static/browse/${name}.js`, import.meta.url);
    vm.runInContext(moduleAsScript(readFileSync(path, 'utf8')), context);
  }
  return {context, status};
}

for (const [source, label] of [
  ['reviewed_gps', 'Reviewed GPS'], ['exif', 'EXIF GPS'],
  ['assigned', 'Assigned map location'], ['none', 'No coordinates'],
]) {
  test(`the ${source} card labels its actual coordinate source`, () => {
    const {context} = harness();
    const html = context.renderCardField('location_status', {location_status: source});
    assert.ok(html.includes(label));
    if (source === 'reviewed_gps') {
      assert.ok(html.includes('Catalog GPS correction'));
      assert.ok(!html.includes('Embedded GPS'));
    }
  });
}

test('reviewed GPS detail does not claim corrected coordinates are embedded', () => {
  const {context, status} = harness();
  context.renderCoordinateStatus({location_status: 'reviewed_gps', latitude: 3, longitude: 4});
  assert.ok(status.textContent.includes('Reviewed GPS correction'));
  assert.ok(status.textContent.includes('Original photo GPS is unchanged'));
  assert.ok(!status.textContent.includes('3.00000'));
  assert.equal(status.className, 'coordinate-status reviewed_gps');
});

test('raw EXIF, assigned and absent detail statuses retain their behavior', () => {
  const {context, status} = harness();
  context.renderCoordinateStatus({location_status: 'exif', latitude: 0, longitude: 0});
  assert.ok(status.textContent.includes('EXIF GPS — 0.00000, 0.00000'));
  context.renderCoordinateStatus({location_status: 'assigned', location: {latitude: 5, longitude: 6}});
  assert.ok(status.textContent.includes('Assigned map location — 5.00000, 6.00000'));
  context.renderCoordinateStatus({location_status: 'none'});
  assert.ok(status.textContent.includes('No coordinates'));
});
