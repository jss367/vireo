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
      focusEvidenceStatus: {textContent: ''},
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

function focusHarness(headers = {}) {
  const nodes = new Map(), requests = [];
  const byId = id => {
    if (!nodes.has(id)) nodes.set(id, {
      textContent: '', style: {}, dataset: {}, hidden: false, handlers: {},
      setAttribute(name, value) { this[name] = value; },
      removeAttribute(name) { delete this[name]; },
      addEventListener(name, handler) { this.handlers[name] = handler; },
    });
    return nodes.get(id);
  };
  const ctx = vm.createContext({
    byId, AbortController, URL: class extends URL {
      static createObjectURL() { return 'blob:crop'; }
      static revokeObjectURL() {}
    },
    window: {location: {origin: 'http://vireo.local'}},
    state: {inspectedId: 1},
    fetch: async url => {
      requests.push(url);
      return new Response('pixels', {headers: {
        'X-Vireo-Focus-Basis': 'subject', 'X-Vireo-Focus-Center': '.5,.5',
        'X-Vireo-Focus-Eye-Status': 'unbound',
        'X-Vireo-Focus-Detection-Id': '42', 'X-Vireo-Focus-Detection-Confidence': '.052',
        'X-Vireo-Focus-Subject-Box': '.1,.2,.3,.4', ...headers,
      }});
    },
  });
  vm.runInContext(moduleAsScript(readFileSync('vireo/static/best-batch/focus.js', 'utf8')), ctx);
  return {ctx, byId, requests};
}

test('automatic crop exposes the actual subject and missing eye evidence, then offers manual head placement', async () => {
  const {ctx, byId, requests} = focusHarness();
  ctx.bindFocusControls();
  await ctx.loadFocus('/photos/1/focus-crop?size=512');
  assert.match(byId('focusEvidenceStatus').textContent, /Subject-box-centered.*head or eye may be outside/);
  assert.match(byId('focusEvidenceStatus').textContent, /Outlined model subject.*confidence 5%.*No subject-bound eye evidence/);
  assert.match(byId('focusEvidenceStatus').textContent, /before rejecting the photo/);
  assert.equal(byId('focusSubjectBox').style.left, '10%');
  assert.equal(byId('focusSubjectBox').style.height, '40%');
  byId('headFocusBtn').handlers.click();
  assert.match(byId('focusSelectionStatus').textContent, /Click the intended animal.*head or eye/);
  assert.equal(byId('manualFocusBtn')['aria-pressed'], 'true');
  assert.equal(requests.length, 1, 'No guessed head location or unrelated eye is fetched');
});

test('head inspection keeps the selected subject and generation when using a verified eye', async () => {
  const {ctx, byId, requests} = focusHarness({'X-Vireo-Focus-Eye-Status': 'located'});
  ctx.bindFocusControls();
  await ctx.loadFocus('/photos/1/focus-crop?size=512&source=original&detection_id=42&analysis_key=generation');
  byId('headFocusBtn').handlers.click();
  const url = new URL(requests[1], 'http://vireo.local');
  assert.equal(url.searchParams.get('basis'), 'eye');
  assert.equal(url.searchParams.get('detection_id'), '42');
  assert.equal(url.searchParams.get('analysis_key'), 'generation');
  assert.equal(url.searchParams.get('source'), 'original');
});

test('legacy eye scoring never labels a body crop as eye-centered or draws a false center crosshair', () => {
  const {ctx} = focusHarness();
  Object.assign(ctx, {escapeAttr: String, escapeHtml: String, rankLabel: () => '', scoreText: () => '',
    previewUrl: () => '', hasEyeKeypoint: () => true});
  vm.runInContext(moduleAsScript(readFileSync('vireo/static/best-batch/render.js', 'utf8')), ctx);
  const html = ctx.focusCardHtml({id: 1, focus_basis: 'eye', eye_x: .8, eye_y: .2, focus_crop_url: '/crop'});
  assert.match(html, /head may be outside crop/);
  assert.match(html, /No verified eye evidence/);
  assert.doesNotMatch(html, /Eye-centered|crosshair/);
});
