import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';
import {moduleAsScript} from './module-source.mjs';

test('keyword chip and evidence heading explain the same alias and escape names', () => {
  const nodes = [];
  const container = {innerHTML: '', appendChild(node) { nodes.push(node); }};
  const ctx = vm.createContext({
    document: {
      getElementById: () => container,
      createElement: () => ({style: {}, children: [], appendChild(node) { this.children.push(node); }}),
    },
    escapeHtml: text => String(text).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    _rememberKeywordNames() {}, bindBrowseMarkup() {},
  });
  vm.runInContext(moduleAsScript(readFileSync('vireo/static/browse/species-provenance.js', 'utf8')), ctx);
  vm.runInContext('evidenceForm = () => ({form: true})', ctx);
  const source = readFileSync('vireo/static/browse/detail.js', 'utf8');
  const fn = parse(source, {ecmaVersion: 'latest', sourceType: 'module'}).body
    .find(node => node.type === 'FunctionDeclaration' && node.id.name === 'renderDetailKeywords');
  vm.runInContext(source.slice(fn.start, fn.end), ctx);
  const names = {display_name: 'European Starling', synonyms: ['Common starling', '<alias>'],
    taxon_id: 6424, scientific_name: 'Sturnus vulgaris'};
  ctx.renderDetailKeywords({id: 1,
    keywords: [{id: 1194, name: 'European starling', type: 'taxonomy', ...names}],
    species_assignments: [{keyword_id: 1194, species: 'European starling', method: 'direct_visual', ...names}],
  });
  const visible = 'European Starling (also known as Common starling, &lt;alias&gt;)';
  assert.ok(container.innerHTML.includes(visible));
  assert.ok(container.innerHTML.includes('data-kw-id="1194"'));
  assert.equal(nodes.length, 1, 'one evidence panel, not an assignment per synonym');
  assert.ok(nodes[0].innerHTML.includes(visible));
  assert.equal(nodes[0].children[0].children[0].textContent, 'Taxon: Sturnus vulgaris (catalog ID 6424)');
  assert.equal(ctx.speciesDisplayLabel({}, 'Unresolved bird'), 'Unresolved bird');
});
