import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

class Element {
  constructor() { this.textContent = ''; this.style = {}; this.children = []; }
  appendChild(child) { this.children.push(child); }
  replaceChildren() { this.children = []; }
  text() { return [this.textContent, ...this.children.map(child => child.text())].join(' '); }
}

const problem = {
  camera: 'Synthetic Camera', summary: 'Clock moved back', evidence: ['Two distinct capture times'],
  fixes: [{count: 2, first: 'DSC_0001.jpg', last: 'DSC_0002.jpg', label: 'Check the clock', explanation: 'Synthetic evidence'}],
};

function harness(skipDuplicates, {stale = null, clearWarning = false} = {}) {
  const elements = new Map(['previewSummary', 'btnPreview', 'importClockProblems', 'chkSkipDuplicates',
    'chkRecursive', 'chkVerifyByHash', 'chkTrustLikelyDuplicates'].map(id => [id, new Element()]));
  elements.get('chkSkipDuplicates').checked = skipDuplicates;
  elements.get('chkRecursive').checked = true;
  const files = [1, 2, 3].map(id => ({path: `/synthetic/card/DSC_000${id}.jpg`}));
  const frames = [
    {capture_dates: {[files[0].path]: '2026-05-30'}},
    {recovered: [files[1].path], ...(skipDuplicates ? {duplicates: [files[0].path]} : {})},
    {clock_problems: [problem]},
    ...(clearWarning ? [{clock_problems: []}] : []),
    {checked: 3, total: 3}, {done: true},
  ];
  const bytes = new TextEncoder().encode(frames.map(frame => 'data: ' + JSON.stringify(frame) + '\n\n').join(''));
  const renders = [];
  const errors = [];
  const requests = [];
  const exclusions = [];
  const ctx = vm.createContext({
    AbortController, TextDecoder, sources: ['/synthetic/card'], newImagesSnapshotId: null,
    importPreviewTimer: null, importPreviewAbort: null, importPreviewSeq: 0,
    selectedImportMode: () => 'copy', selectedFileTypes: () => ['jpg'],
    selectedFolderTemplate: () => '%Y/%m/%d', resolvedCopyDestination: () => '/synthetic/archive',
    newImagesExcludedFolderList: () => [],
    document: {getElementById: id => elements.get(id), createElement: () => new Element()},
    showError: message => { if (message) errors.push(message); },
    hideDestStructure() {},
    clearImportPreviewGrid() {
      elements.get('importClockProblems').replaceChildren();
      elements.get('importClockProblems').style.display = 'none';
    },
    updateStartGate() {},
    retireHideDuplicatesOptIn() {}, updateImportSelectionUI() {}, applyDoneFrameSourceCounts() {},
    failActiveSourceScans() {},
    readFolderPreviewStream: async () => ({files}),
    renderImportPreviewGrid: (_files, duplicates) => renders.push(Array.from(duplicates)),
    renderDestStructure: async excluded => { exclusions.push(Array.from(excluded)); return null; },
    fetch: async (url, options) => {
      requests.push({url, body: JSON.parse(options.body)});
      assert.ok(['/api/import/folder-preview-stream', '/api/import/check-duplicates'].includes(url));
      let offset = 0;
      return {ok: true, body: {getReader: () => ({read: async () => {
        if (offset === 0) {
          if (stale === 'sequence') ctx.importPreviewSeq++;
          if (stale === 'signature') elements.get('chkRecursive').checked = false;
        }
        if (offset >= bytes.length) return {done: true};
        // Split field names and frame boundaries across reads.
        const value = bytes.slice(offset, offset + 7);
        offset += 7;
        return {done: false, value};
      }})}};
    },
  });
  for (const name of ['preview', 'clock-check']) {
    const source = readFileSync(new URL(`../../vireo/static/import/${name}.js`, import.meta.url), 'utf8');
    vm.runInContext(source, ctx);
  }
  return {ctx, elements, files, renders, errors, requests, exclusions, run: () => ctx.previewImport()};
}

for (const skipDuplicates of [false, true]) {
  const mode = skipDuplicates ? 'duplicate-check' : 'recovery-only';
  test(`${mode} renders fragmented clock frames and preserves import verdicts`, async () => {
    const h = harness(skipDuplicates);
    await h.run();
    assert.deepEqual(h.errors, []);
    assert.equal(h.requests[1].body.skip_duplicates, skipDuplicates);
    assert.equal(h.requests[1].body.destination, '/synthetic/archive');
    const panel = h.elements.get('importClockProblems');
    assert.equal(panel.style.display, '');
    assert.match(panel.text(), /Synthetic Camera: Clock moved back/);
    assert.match(panel.text(), /DSC_0001.jpg–DSC_0002.jpg/);
    assert.deepEqual(Array.from(h.ctx.importRecoveredPaths), [h.files[1].path]);
    assert.deepEqual(h.renders.at(-1), skipDuplicates ? [h.files[0].path] : []);
    assert.deepEqual(h.exclusions.at(-1), skipDuplicates ? [h.files[0].path] : []);
    assert.equal(h.files[0].capture_date, '2026-05-30');
    assert.match(h.elements.get('previewSummary').textContent, skipDuplicates ? /1 to copy/ : /2 to copy · duplicates will be copied/);
    assert.equal(h.ctx.importDupStreamPending, false);
  });

  for (const stale of ['sequence', 'signature']) {
    test(`${mode} ignores clock frames from a stale ${stale}`, async () => {
      const h = harness(skipDuplicates, {stale});
      await h.run();
      assert.deepEqual(h.errors, []);
      assert.equal(h.elements.get('importClockProblems').children.length, 0);
      assert.deepEqual(h.renders, []);
    });
  }

  test(`${mode} clears warnings when the stream emits an empty clock frame`, async () => {
    const h = harness(skipDuplicates, {clearWarning: true});
    await h.run();
    assert.deepEqual(h.errors, []);
    assert.equal(h.elements.get('importClockProblems').style.display, 'none');
    assert.equal(h.elements.get('importClockProblems').children.length, 0);
  });
}
