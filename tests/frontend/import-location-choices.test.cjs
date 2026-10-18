const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {test} = require('node:test');
const vm = require('node:vm');

const fixture = process.env.VIREO_LOCATION_GROUP_FIXTURE
  ? JSON.parse(readFileSync(process.env.VIREO_LOCATION_GROUP_FIXTURE, 'utf8'))
  : {
    groups: ['kw:1', 'kw:2', 'none'].map((key, i) => ({key, cells: [i + ',0'],
      sample_paths: ['/invented/' + i], suggestion: null, latitude: i, longitude: 0})),
    file_cells: {'/invented/0': '0,0', '/invented/1': '1,0', '/invented/2': '2,0'},
    location_options: [{id: 20, name: 'Exact', label: 'Exact · Root [#20]'},
      {id: 22, name: 'Exact', label: 'Exact · Root [#22]'},
      {id: 34, name: 'Beach · City', label: 'Beach · City'}],
  };

function harness({uniqueKeys = false} = {}) {
  class Element {
    constructor() { this.children = []; this.dataset = {}; this.handlers = {}; this.value = ''; }
    setAttribute() {}
    append(...items) { this.children.push(...items); }
    appendChild(item) { this.children.push(item); }
    addEventListener(name, fn) { this.handlers[name] = fn; }
  }
  const ctx = vm.createContext({
    document: {getElementById: () => ({checked: true}), createElement: () => new Element()},
    importPreviewedPaths: Object.keys(fixture.file_cells), importSelectionEnabled: () => false,
    updateStartGate: () => {},
    fetch: () => { throw new Error('HTTP forbidden in fake-DOM control'); },
  });
  vm.runInContext(readFileSync('vireo/static/import/locations.js', 'utf8'), ctx);
  ctx.data = structuredClone(fixture);
  if (uniqueKeys) ctx.data.groups.forEach((group, i) => { group.key = "isolated:" + i; });
  vm.runInContext('importLocations.data = data;', ctx);
  const run = code => vm.runInContext(code, ctx);
  function edit(index, name) {
    ctx.index = index;
    ctx.name = name;
    run(`globalThis.row = renderImportLocationGroup(data.groups[index]);
      globalThis.input = row.children[2].children[1];
      input.value = name; input.handlers.input();`);
  }
  const choices = () => JSON.parse(run('JSON.stringify(importLocationChoices())'));
  return {ctx, run, edit, choices};
}

test('vanished and unnamed groups retain independent edited choices', () => {
  const h = harness();
  h.edit(0, 'First typed location');
  assert.equal(h.choices().length, 1);
  assert.deepEqual(h.choices()[0].cells, fixture.groups[0].cells);
  h.edit(1, 'Second typed location');
  assert.equal(h.choices().length, 2);
  assert.deepEqual(h.choices().map(c => c.name), ['First typed location', 'Second typed location']);
  h.run('row.children[0].checked = false; row.children[0].handlers.change();');
  assert.deepEqual(h.choices().map(c => c.name), ['First typed location']);
});

test('every displayed picker label resolves to its own saved keyword ID', () => {
  for (const option of fixture.location_options) {
    const h = harness({uniqueKeys: true});
    h.edit(0, option.label);
    assert.equal(h.choices().length, 1);
    assert.equal(h.choices()[0].keyword_id, option.id, option.label);
  }
});

test('an ambiguous bare saved-place name never selects the first keyword', () => {
  const names = new Map();
  for (const option of fixture.location_options) {
    const key = option.name.toLocaleLowerCase();
    names.set(key, [...(names.get(key) || []), option]);
  }
  const ambiguous = [...names.values()].find(options => options.length > 1
    && !fixture.location_options.some(option => option.label.toLocaleLowerCase()
      === options[0].name.toLocaleLowerCase()));
  assert.ok(ambiguous);
  const h = harness({uniqueKeys: true});
  h.edit(0, ambiguous[0].name);
  assert.deepEqual(h.choices(), []);
});

test('location admission counts only selected choices and distinct cells at backend limits', () => {
  const h = harness();
  h.ctx.selected = [];
  h.run('importLocationChoices = () => selected;');
  const problem = () => h.run('importLocationChoicesProblem()');
  h.ctx.selected = Array.from({length: 1000}, (_, i) => ({cells: [String(i)]}));
  assert.equal(problem(), null);
  h.ctx.selected.push({cells: ['extra']});
  assert.match(problem(), /1,000.*Uncheck/);
  h.ctx.selected = [{cells: Array.from({length: 100000}, (_, i) => String(i))}];
  assert.equal(problem(), null);
  h.ctx.selected.push({cells: ['0']});
  assert.equal(problem(), null, 'count distinct cells');
  h.ctx.selected[1].cells.push('extra');
  assert.match(problem(), /100,000.*Uncheck/);
  h.ctx.selected = [];
  assert.equal(problem(), null);
  const backend = readFileSync('vireo/services/import_locations.py', 'utf8');
  assert.match(backend, /MAX_SUGGESTION_PATHS = 100_000\b/);
  assert.match(backend, /MAX_CHOICES = 1000\b/);
  assert.match(backend, /MAX_CHOICE_CELLS = MAX_SUGGESTION_PATHS\b/);
});

test('legacy GPS intent survives both copy retry builders without invented choices', () => {
  for (const [file, name] of [['jobs/import-retry.js', 'importRetryBody'],
    ['import/jobs.js', 'retryBodyFromFinishedJob']]) {
    const ctx = vm.createContext({jobConfig: job => job.config});
    vm.runInContext(readFileSync('vireo/static/' + file, 'utf8'), ctx);
    for (const legacy of [true, false, undefined]) {
      ctx.job = {id: 17, type: 'import', config: {sources: ['/invented'],
        destination: '/invented-output', location_from_gps: legacy}};
      const body = vm.runInContext(name + '(job)', ctx);
      assert.equal(body.location_from_gps, !!legacy, file);
      assert.equal(body.location_choices.length, 0);
    }
  }
});

test('form admission blocks excessive location selections in both import modes', () => {
  const ctx = vm.createContext({sources: ['/invented'], newImagesSnapshotId: null,
    importExiftoolRequired: false, document: {getElementById: () => ({checked: false})},
    importLocationChoicesProblem: () => 'Select at most 1,000 location groups.',
    selectedImportMode: () => { throw new Error('must reject before mode-specific admission'); }});
  vm.runInContext(readFileSync('vireo/static/import/form.js', 'utf8'), ctx);
  assert.match(vm.runInContext('importFormProblem().message', ctx), /1,000/);
  ctx.newImagesSnapshotId = 123;
  ctx.newImagesFileCount = 0;
  assert.match(vm.runInContext('importFormProblem().message', ctx), /1,000/);
});

test('preview work-limit refusal blocks GPS Start until a new preview or opt-out', () => {
  const h = harness();
  h.ctx.data = {groups: [], file_cells: {}, no_gps_paths: [], location_options: [],
    work_limit_error: 'GPS locations support at most 1,000 groups per import. Preview fewer files, or turn off GPS location suggestions.'};
  h.run('importLocations.data = data;');
  assert.match(h.run('importLocationChoicesProblem()'), /1,000.*Preview fewer files/);
  h.ctx.document.getElementById = () => ({checked: false});
  assert.equal(h.run('importLocationChoicesProblem()'), null);
  assert.deepEqual(h.choices(), []);
  h.ctx.document.getElementById = () => ({checked: true});
  h.run('delete importLocations.data.work_limit_error;');
  assert.equal(h.run('importLocationChoicesProblem()'), null);
});

test('large choices and keystroke validation read candidate cells only linearly', () => {
  for (const size of [10000, 20000]) {
    const h = harness();
    let reads = 0;
    const paths = Array.from({length: size}, (_, i) => '/invented/' + i);
    const fileCells = Object.fromEntries(paths.map((path, i) => [path, String(i % 1000)]));
    h.ctx.large = {groups: Array.from({length: 1000}, (_, i) => ({
      key: String(i), cells: [String(i)], suggestion: null, sample_paths: [],
      latitude: 0, longitude: 0,
    })), file_cells: new Proxy(fileCells, {get(target, key) {
      if (++reads > size * 8) throw new Error('quadratic candidate-path scan');
      return target[key];
    }}), no_gps_paths: [], location_options: []};
    h.ctx.importPreviewedPaths = paths;
    h.run(`importLocations.data = large;
      large.groups.forEach(group => importLocations.choices.set(group.key,
        {enabled: true, value: 'Invented ' + group.key}));`);
    assert.equal(h.choices().length, 1000);
    assert.equal(reads, size, 'one path lookup per candidate, across every group');
    reads = 0;
    h.run('updateStartGate = () => importLocationChoicesProblem();');
    h.edit(0, 'Edited invented location');
    assert.ok(reads <= size * 4, 'keystroke status and Start validation use constant linear passes');
    reads = 0;
    h.ctx.importSelectionEnabled = () => true;
    h.ctx.importGridCards = () => [];
    h.ctx.importSelectableCardPaths = () => [paths[0]];
    h.ctx.importDeselected = new Set();
    assert.equal(h.choices().length, 1, 'selection replacement takes effect immediately');
    assert.equal(reads, 1);
    h.ctx.importDeselected.add(paths[0]);
    assert.deepEqual(h.choices(), [], 'deselection is never cached');
    h.ctx.importPreviewedPaths = ['/invented/replacement'];
    h.ctx.importSelectionEnabled = () => false;
    h.ctx.large.file_cells = {'/invented/replacement': '1'};
    assert.deepEqual(h.choices()[0].cells, ['1'], 'new preview mapping replaces old counts');
  }
});
