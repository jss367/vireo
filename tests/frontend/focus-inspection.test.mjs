import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {moduleAsScript} from './module-source.mjs';

for (const [status, contentType, body, expected] of [
  [404, 'text/plain', 'Not found', 'Focus evidence unavailable (HTTP 404).'],
  [500, 'text/plain', 'Could not load image', 'Focus evidence unavailable (HTTP 500).'],
  [409, 'application/json', '{broken', 'Focus evidence unavailable (HTTP 409).'],
  [404, 'application/json', '{"error":"Selected subject analysis is stale"}', 'Selected subject analysis is stale'],
  [500, 'application/json', '{"error":{}}', 'Focus evidence unavailable (HTTP 500).'],
]) {
  test('focus crop HTTP ' + status + ' with ' + contentType + ' exposes a useful unavailable state', async () => {
    const nodes = {
      inspectedFocusImg: {src: 'blob:old', hidden: false, removeAttribute(name) { delete this[name]; }},
      focusRasterStatus: {textContent: ''}, focusCenter: {hidden: false},
    };
    const context = vm.createContext({
      AbortController, Error, byId: id => nodes[id],
      fetch: async () => new Response(body, {status, headers: {'Content-Type': contentType}}),
    });
    vm.runInContext(moduleAsScript(readFileSync('vireo/static/best-batch/focus.js', 'utf8')), context);
    await vm.runInContext('loadFocus("/photos/1/focus-crop")', context);
    assert.ok(nodes.focusRasterStatus.textContent.startsWith(expected), nodes.focusRasterStatus.textContent);
    assert.equal(nodes.inspectedFocusImg.hidden, true);
    assert.equal(nodes.inspectedFocusImg.src, undefined);
    assert.equal(nodes.focusCenter.hidden, true);
  });
}
