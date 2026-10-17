import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

const source = readFileSync('vireo/static/vireo-predictions.js', 'utf8');

for (const browser of [false, true]) {
  function load() {
    const host = {Vireo: {browse: {retained: true}, predictions: {retained: true}}};
    const context = vm.createContext(browser ? {window: host} : host);
    vm.runInContext(source, context);
    return browser ? host.Vireo : context.Vireo;
  }
  const name = browser ? 'browser namespace' : 'Node globalThis';

  test(`${name}: installation preserves other namespace owners`, () => {
    const namespace = load();
    assert.equal(namespace.browse.retained, true);
    assert.equal(namespace.predictions.retained, true);
    assert.equal(typeof namespace.predictions.groupedDecisionTracker, 'function');
  });

  test(`${name}: malformed payloads cannot create credited prediction ids`, () => {
    const api = load().predictions;
    for (const response of [null, undefined, true, 7, 'ids', {}, [], {prediction_ids: '7,8'}, {prediction_ids: {id: 7}}]) {
      assert.deepEqual(Array.from(api.decidedPredictionIds(response)), []);
    }
    const ids = api.decidedPredictionIds({prediction_ids: [7, '8', 9, null, false, {}, [], -1, 0, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]});
    assert.deepEqual(Array.from(ids), [7, 9]);
  });

  test(`${name}: only confirmed ids count and each run owns its decisions`, () => {
    const api = load().predictions;
    const tracker = api.groupedDecisionTracker();
    const other = api.groupedDecisionTracker();
    const payload = {prediction_ids: [11, 12, 11, '13']};
    const recorded = tracker.record(payload);
    payload.prediction_ids.push(14);
    recorded.push(15);
    tracker.record({prediction_ids: [12, 16]});
    tracker.record({prediction_ids: null});
    assert.equal(tracker.size(), 3);
    assert.equal(tracker.alreadyDecided(11), true);
    assert.equal(tracker.alreadyDecided(12), true);
    assert.equal(tracker.alreadyDecided(13), false);
    assert.equal(tracker.alreadyDecided(14), false);
    assert.equal(tracker.alreadyDecided(15), false);
    assert.equal(tracker.alreadyDecided(16), true);
    assert.equal(other.size(), 0);
  });
}
