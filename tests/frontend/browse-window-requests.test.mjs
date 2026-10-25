import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';

const source = readFileSync(new URL('../../vireo/static/browse/window-requests.js', import.meta.url), 'utf8');
let instance = 0;
function freshOwner() {
  return import('data:text/javascript;base64,' + Buffer.from(source).toString('base64') + '#' + instance++);
}

test('observing a window lets its grid, selection and stack requests finish together', async () => {
  const owner = await freshOwner();
  const grid = owner.claimBrowseWindow();
  const selection = owner.observeBrowseWindow();
  const stacks = owner.observeBrowseWindow();
  assert.equal(owner.browseWindowSequence(), 1);
  assert.ok(grid() && selection() && stacks());
});

test('a scope change retires every earlier callback permanently', async () => {
  const owner = await freshOwner();
  const bootstrap = owner.claimBrowseWindow();
  const oldStack = owner.observeBrowseWindow();
  const filter = owner.claimBrowseWindow();
  assert.equal(bootstrap(), false);
  assert.equal(oldStack(), false);
  assert.equal(filter(), true);
  const restoredScope = owner.claimBrowseWindow();
  assert.equal(bootstrap(), false, 'returning to the original scope cannot revive its callbacks');
  assert.equal(filter(), false);
  assert.equal(restoredScope(), true);
  assert.equal(owner.browseWindowSequence(), 3);
});

test('a late async completion cannot paint into a newer window', async () => {
  const owner = await freshOwner();
  let resolve;
  const response = new Promise(done => { resolve = done; });
  const isCurrent = owner.claimBrowseWindow();
  const painted = [];
  const pending = response.then(value => { if (isCurrent()) painted.push(value); });
  owner.claimBrowseWindow();
  resolve('old rows');
  await pending;
  assert.deepEqual(painted, []);
});

test('reading server sequence numbers cannot invalidate callbacks or another page instance', async () => {
  const first = await freshOwner();
  const second = await freshOwner();
  const current = first.claimBrowseWindow();
  const otherPage = second.claimBrowseWindow();
  assert.equal(first.browseWindowSequence(), first.browseWindowSequence());
  second.claimBrowseWindow();
  assert.equal(otherPage(), false);
  assert.equal(current(), true);
  assert.equal(first.browseWindowSequence(), 1);
});
