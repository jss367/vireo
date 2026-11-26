import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';

function functionSource(file, name) {
  const source = readFileSync(file, 'utf8');
  const node = parse(source, {ecmaVersion: 'latest', sourceType: 'module'}).body
    .map(node => node.declaration || node)
    .find(node => node.type === 'FunctionDeclaration' && node.id.name === name);
  assert.ok(node, name);
  return source.slice(node.start, node.end);
}

function rateText(progress, showingPhaseCounts = false) {
  const context = vm.createContext({});
  vm.runInContext(functionSource('vireo/static/navbar-shared-helpers.js', 'bpRateText'), context);
  return context.bpRateText(progress, showingPhaseCounts);
}

test('a standalone job rate is items per second, shown per minute, never as bytes', () => {
  assert.equal(rateText({current: 40, total: 100, rate: 3.2}), ' · 192/min');
  assert.equal(rateText({current: 4, total: 100, rate: 0.1}), ' · 6.0/min');
  assert.doesNotMatch(rateText({rate: 3.2}), /MB/);
});

test('a standalone rate is left out while the counts shown are a sub-phase\'s', () => {
  assert.equal(rateText({current: 40, total: 100, phase_current: 2, phase_total: 9, rate: 3.2}, true), '');
});

test('a pipeline rate is already files per minute and names its files', () => {
  assert.equal(
    rateText({current: 42, total: 100, rate: 190.4, rate_unit: 'per_minute'}),
    ' · 190 files/min',
  );
  assert.equal(rateText({rate: 190, rate_unit: 'per_minute'}, true), ' · 190 files/min');
});

test('no rate, a zero rate or no progress shows nothing', () => {
  assert.equal(rateText({current: 1, total: 2}), '');
  assert.equal(rateText({current: 1, total: 2, rate: 0}), '');
  assert.equal(rateText(undefined), '');
  assert.equal(rateText(null), '');
});
