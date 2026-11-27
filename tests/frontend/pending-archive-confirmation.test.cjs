const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const test = require('node:test');
const vm = require('node:vm');

const source = readFileSync('vireo/static/vireo-pending-archives.js', 'utf8');
const start = source.indexOf('  async function startSend(');
const end = source.indexOf('  function appendMissingSource(', start);
const implementation = source.slice(start, end);

for (const outcome of ['accept', 'cancel', 'stale']) {
  test(`NAS identity confirmation: ${outcome}`, async () => {
    const requests = [];
    const confirmation = {'/Volumes/Photos': ['network-share', 'opaque-share-key', '/']};
    let prompt;
    const context = vm.createContext({
      sending: false, error: {textContent: ''}, actionError: '', signature: '', refresh() {},
      Vireo: {async confirm(options) {
        prompt = [options.title, options.message].concat(options.details).join('\n');
        return outcome !== 'cancel';
      }},
      async fetch(url, options) {
        requests.push({url, body: JSON.parse(options.body)});
        if (requests.length === 1) return {
          ok: false, status: 409, async json() { return {mount_confirmation: confirmation}; },
        };
        return {ok: outcome !== 'stale', status: outcome === 'stale' ? 409 : 200,
          async json() { return {error: 'Destination changed; review again'}; }};
      },
    });
    vm.runInContext(implementation, context);
    await context.startSend({id: 'import-1', destination: '/Volumes/Photos/Trip'}, true, []);
    assert.ok(prompt.includes('/Volumes/Photos/Trip'));
    assert.ok(prompt.includes('Mounted at: /Volumes/Photos'));
    assert.ok(!prompt.includes('opaque-share-key'));
    assert.equal(requests.length, outcome === 'cancel' ? 1 : 2);
    if (outcome !== 'cancel') assert.deepEqual(requests[1].body, {
      sync_first: true, confirmed_mount_identities: confirmation,
    });
    assert.equal(context.sending, false);
    assert.equal(context.error.textContent, outcome === 'stale' ? 'Destination changed; review again' : '');
  });
}
