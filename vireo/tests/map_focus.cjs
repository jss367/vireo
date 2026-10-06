// Unit tests for describeUnplottableFocus (vireo/static/map-focus.js): the
// notice the Map page shows when "View on Map" lands on a photo it cannot
// place. The file runs in a fresh VM context with no DOM.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const ctx = vm.createContext({encodeURIComponent});
vm.runInContext(fs.readFileSync('vireo/static/map-focus.js', 'utf8'), ctx);
const {describeUnplottableFocus} = ctx;

function same(actual, expected) {
  assert.deepEqual(JSON.parse(JSON.stringify(actual)), expected);
}

const tests = [];
function test(name, body) { tests.push([name, body]); }

test('a name-only location offers to link it to a place', () => {
  const view = describeUnplottableFocus({
    id: 21, filename: '_D851106.NEF', reason: 'no_coordinates',
    location_keywords: [{id: 1, name: 'Laguna Lake'}],
  });
  assert.equal(view.title, '_D851106.NEF has no map location');
  assert.match(view.detail, /“Laguna Lake” is a name only/);
  same(view.actions, [
    {label: 'Link “Laguna Lake” to a place', href: '/keywords?link_place=1'},
    {label: 'Open in Browse', href: '/browse?photo_id=21'},
  ]);
});

test('several name-only locations get one link each', () => {
  const view = describeUnplottableFocus({
    id: 5, filename: 'a.jpg', reason: 'no_coordinates',
    location_keywords: [{id: 1, name: 'Laguna Lake'}, {id: 2, name: 'SLO'}],
  });
  assert.match(view.detail, /locations “Laguna Lake”, “SLO” are names only/);
  assert.equal(view.actions.length, 3);
});

test('no location at all points at assigning one', () => {
  const view = describeUnplottableFocus({
    id: 5, filename: 'a.jpg', reason: 'no_coordinates', location_keywords: [],
  });
  assert.equal(view.detail, 'It has no EXIF GPS coordinates and no assigned location.');
  same(view.actions, [{label: 'Assign a location in Browse', href: '/browse?photo_id=5'}]);
});

test('coordinates in an offline folder are not called missing coordinates', () => {
  const view = describeUnplottableFocus({id: 5, filename: 'a.jpg', reason: 'unavailable'});
  assert.equal(view.title, 'a.jpg is not on the map');
  assert.match(view.detail, /has map coordinates, but its folder is offline or missing/);
});

test('an unknown photo says so', () => {
  const view = describeUnplottableFocus({id: 999, reason: 'not_found'});
  assert.equal(view.title, 'Photo not found');
  same(view.actions, []);
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
console.log((tests.length - failed) + '/' + tests.length + ' map focus tests passed');
if (failed) process.exitCode = 1;
