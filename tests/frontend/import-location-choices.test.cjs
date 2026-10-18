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
