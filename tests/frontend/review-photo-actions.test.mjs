import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';

const actions = readFileSync('vireo/static/review/photo-actions.js', 'utf8');
const navbar = readFileSync('vireo/static/navbar-shared-helpers.js', 'utf8');
const nativeActions = parse(navbar, {ecmaVersion: 'latest'}).body
  .filter(node => ['nativeMenuSetRating', 'nativeMenuSetFlag'].includes(node.id?.name))
  .map(node => navbar.slice(node.start, node.end)).join('\n');
const flush = () => new Promise(resolve => setImmediate(resolve));

function harness() {
  const requests = [], copies = [], toasts = [], renders = [];
  const ctx = vm.createContext({
    navigator: {clipboard: {writeText: async path => { copies.push(path); }}},
    safeFetch: (url, options, policy) => new Promise((resolve, reject) => requests.push({url, options, policy, resolve, reject})),
    showToast: (message, type) => toasts.push({message, type}),
    renderAll: () => renders.push(true),
    allPredictions: [{photo_id: 1, is_species_representative: true, life_list: [
      {species: 'Hawk', is_current_photo: true, is_species_representative: true},
    ]}, {photo_id: 2, is_species_representative: true}],
    nativeMenuPhotoIdsForWrite: () => [1],
    nativeMenuLightboxOpen: () => false,
    nativeMenuInfo: message => toasts.push({message, type: 'info'}),
  });
  ctx.window = ctx;
  vm.runInContext(actions + '\n' + nativeActions, ctx);
  function confirm(index) { requests[index].resolve({ok: true, updated: 1}); }
  return {ctx, requests, copies, toasts, renders, confirm};
}

for (const [field, value] of [['rating', 0], ['flag', 'none']]) {
  const method = field === 'rating' ? 'setReviewRating' : 'setReviewFlag';
  test(`${field}: duplicate pending clicks share one failure; a new click retries`, async () => {
    const h = harness();
    const first = h.ctx[method](1, value);
    assert.equal(h.ctx[method](1, value), first);
    await flush();
    assert.equal(h.requests.length, 1);
    assert.deepEqual(JSON.parse(h.requests[0].options.body), {photo_ids: [1], [field]: value});
    assert.equal(h.requests[0].policy.toast, false);
    assert.equal(h.toasts.length, 0);
    h.requests[0].reject(new Error('disk unavailable'));
    assert.equal(await first, false);
    assert.equal(h.toasts.length, 1);
    assert.equal(h.toasts[0].type, 'error');
    assert.match(h.toasts[0].message, /disk unavailable/);
    const retry = h.ctx[method](1, value);
    await flush();
    assert.equal(h.requests.length, 2);
    h.confirm(1);
    assert.equal(await retry, true);
    assert.equal(h.toasts[1].type, 'success');
  });

  test(`${field}: successive choices run in click order even after failure`, async () => {
    const h = harness();
    const nextValue = field === 'rating' ? 5 : 'flagged';
    const first = h.ctx[method](1, value);
    const next = h.ctx[method](1, nextValue);
    assert.equal(h.ctx[method](1, nextValue), next);
    await flush();
    assert.equal(h.requests.length, 1);
    h.requests[0].reject(new Error('offline'));
    await first;
    await flush();
    assert.equal(h.requests.length, 2);
    assert.equal(JSON.parse(h.requests[1].options.body)[field], nextValue);
    h.confirm(1);
    assert.equal(await next, true);
    const reset = h.ctx[method](1, value);
    await flush();
    h.confirm(2);
    assert.equal(await reset, true);
    assert.equal(JSON.parse(h.requests[2].options.body)[field], value);
  });
}

test('different photos and fields remain independent', async () => {
  const h = harness();
  const pending = [h.ctx.setReviewRating(1, 3), h.ctx.setReviewRating(2, 5), h.ctx.setReviewFlag(1, 'none')];
  await flush();
  assert.equal(h.requests.length, 3);
  for (let i = 0; i < 3; i++) h.confirm(i);
  assert.deepEqual(await Promise.all(pending), [true, true, true]);
});

for (const ack of [null, {}, {ok: false, updated: 1}, {ok: true, updated: 0}, {ok: true, updated: 2}]) {
  test(`unconfirmed flag saves keep representative state: ${JSON.stringify(ack)}`, async () => {
    const h = harness();
    const pending = h.ctx.setReviewFlag(1, 'rejected');
    await flush();
    assert.equal(h.ctx.allPredictions[0].is_species_representative, true);
    h.requests[0].resolve(ack);
    assert.equal(await pending, false);
    assert.equal(h.ctx.allPredictions[0].is_species_representative, true);
    assert.equal(h.ctx.allPredictions[0].life_list[0].is_current_photo, true);
    assert.equal(h.renders.length, 0);
    assert.equal(h.toasts.length, 1);
    assert.equal(h.toasts[0].type, 'error');
  });
}

test('a confirmed reject alone clears representative badges for its photo', async () => {
  const h = harness();
  const pending = h.ctx.setReviewFlag(1, 'rejected');
  await flush();
  h.confirm(0);
  assert.equal(await pending, true);
  assert.equal(h.ctx.allPredictions[0].is_species_representative, false);
  assert.equal(h.ctx.allPredictions[0].life_list[0].is_current_photo, false);
  assert.equal(h.ctx.allPredictions[1].is_species_representative, true);
  assert.equal(h.renders.length, 1);
});

test('history-busy rejection keeps the transport warning without adding another', async () => {
  const h = harness();
  const pending = h.ctx.setReviewRating(1, 4);
  await flush();
  h.toasts.push({message: 'Undo is still finishing', type: 'warning'});
  h.requests[0].reject(Object.assign(new Error('Undo is still finishing'), {code: 'history_busy'}));
  assert.equal(await pending, false);
  assert.equal(h.toasts.length, 1);
});

for (const failure of ['lookup', 'missing-path', 'blank-path', 'invalid-path', 'unavailable', 'denied']) {
  test(`Copy Path reports ${failure} and allows retry`, async () => {
    const h = harness();
    const first = h.ctx.copyReviewPhotoPath(1);
    assert.equal(h.ctx.copyReviewPhotoPath(1), first);
    await flush();
    if (failure === 'lookup') h.requests[0].reject(new Error('Photo could not be loaded'));
    else {
      if (failure === 'unavailable') h.ctx.navigator.clipboard = undefined;
      if (failure === 'denied') h.ctx.navigator.clipboard.writeText = async () => { throw new Error('Permission denied'); };
      const data = failure === 'missing-path' ? {} : {path: failure === 'blank-path' ? ' ' : failure === 'invalid-path' ? 42 : '/photos/hawk.jpg'};
      h.requests[0].resolve(data);
    }
    assert.equal(await first, false);
    assert.equal(h.toasts.length, 1);
    assert.equal(h.toasts[0].type, 'error');
    assert.match(h.toasts[0].message, /Could not copy path/);
    h.ctx.navigator.clipboard = {writeText: async path => { h.copies.push(path); }};
    const retry = h.ctx.copyReviewPhotoPath(1);
    await flush();
    h.requests[1].resolve({path: '/photos/hawk.jpg'});
    assert.equal(await retry, true);
    assert.deepEqual(h.copies, ['/photos/hawk.jpg']);
    assert.deepEqual(h.toasts[1], {message: 'Path copied', type: 'success'});
  });
}

test('Copy Path waits for clipboard completion and preserves order between photos', async () => {
  const h = harness();
  let finishCopy;
  h.ctx.navigator.clipboard.writeText = path => new Promise(resolve => {
    h.copies.push(path);
    finishCopy = resolve;
  });
  const first = h.ctx.copyReviewPhotoPath(1);
  const next = h.ctx.copyReviewPhotoPath(2);
  await flush();
  h.requests[0].resolve({path: '/photos/first.jpg'});
  await flush();
  assert.equal(h.requests.length, 1);
  assert.equal(h.toasts.length, 0);
  finishCopy();
  await first;
  await flush();
  h.requests[1].resolve({path: '/photos/second.jpg'});
  await flush();
  assert.equal(h.toasts.length, 1);
  finishCopy();
  await next;
  assert.deepEqual(h.copies, ['/photos/first.jpg', '/photos/second.jpg']);
  assert.equal(h.toasts.length, 2);
});

for (const lightbox of [false, true]) {
  for (const [method, value] of [['nativeMenuSetRating', 4], ['nativeMenuSetFlag', 'flagged']]) {
    for (const success of [false, true]) {
      test(`${method}: ${lightbox ? 'lightbox' : 'grid'} reports only confirmed outcome (${success})`, async () => {
        const h = harness();
        h.ctx.nativeMenuLightboxOpen = () => lightbox;
        const pending = h.ctx[method](value);
        await flush();
        assert.equal(h.toasts.length, 0);
        if (success) h.confirm(0);
        else h.requests[0].reject(new Error('offline'));
        await pending;
        assert.equal(h.toasts.length, 1);
        assert.equal(h.toasts[0].type, success ? 'success' : 'error');
      });
    }
  }
}

for (const [method, values, field] of [
  ['setReviewRating', [1, 5, 1], 'rating'],
  ['setReviewFlag', ['rejected', 'flagged', 'rejected'], 'flag'],
]) {
  test(`${field}: returning to the initial choice while pending still writes it last`, async () => {
    const h = harness();
    const promises = values.map(value => h.ctx[method](1, value));
    assert.notEqual(promises[0], promises[2]);
    for (let i = 0; i < values.length; i++) {
      await flush();
      assert.equal(h.requests.length, i + 1);
      assert.equal(JSON.parse(h.requests[i].options.body)[field], values[i]);
      h.confirm(i);
      assert.equal(await promises[i], true);
    }
  });
}
