import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';
import {moduleAsScript} from './module-source.mjs';

function functionSource(file, name) {
  const source = readFileSync(file, 'utf8');
  const node = parse(source, {ecmaVersion: 'latest', sourceType: 'module'}).body
    .map(node => node.declaration || node)
    .find(node => node.type === 'FunctionDeclaration' && node.id.name === name);
  assert.ok(node, name);
  return source.slice(node.start, node.end);
}

function harness() {
  const context = vm.createContext({formatJobType: type => type});
  for (const name of ['escapeHtml', 'escapeAttr']) {
    vm.runInContext(functionSource('vireo/static/vireo-utils.js', name), context);
  }
  context.bpEscape = context.escapeHtml;
  for (const name of ['state', 'format', 'lineage', 'job-card', 'list']) {
    vm.runInContext(moduleAsScript(readFileSync(`vireo/static/jobs/${name}.js`, 'utf8')), context);
  }
  for (const name of ['bpActiveProgress', 'bpProgressPct', 'isWaitingJob', 'renderJobList']) {
    vm.runInContext(functionSource('vireo/static/navbar-shared-helpers.js', name), context);
  }
  return {context, state: vm.runInContext('state', context)};
}

const row = extra => ({id: 'job', type: 'send-to-nas', status: 'completed', ...extra});

for (const [name, extra, label, title] of [
  ['user root', {origin: 'user'}, 'User', 'Started by you'],
  ['user root with null parent', {origin: 'user', parent_job_id: null}, 'User', 'Started by you'],
  ['user follow-up', {origin: 'user', parent_job_id: 'import'}, 'User', 'Part of a user-started operation'],
  ['system', {origin: 'system', reason: 'Check "new" photos & folders'}, 'Vireo', 'Check &quot;new&quot; photos &amp; folders'],
]) {
  test(`${name}: list and status panel show the origin and accurate tooltip`, () => {
    const {context} = harness();
    for (const html of [context.renderListItem(row(extra), 'history'), context.renderJobList([row(extra)])]) {
      assert.match(html, new RegExp('>' + label + '</span>'));
      assert.ok(html.includes('title="' + title + '"'));
    }
  });
}

test('unknown origins remain unlabeled in the list, card and status panel', () => {
  const {context} = harness();
  const job = row({});
  assert.doesNotMatch(context.renderListItem(job, 'history'), /job-origin-chip/);
  assert.doesNotMatch(context.renderJobList([job]), />User<|>Vireo</);
  assert.equal(context.renderProvenance(job), '');
});

test('cards label user roots and preserve follow-up context and parent links', () => {
  const {context, state} = harness();
  const parent = row({id: 'import', type: 'Import', origin: 'user'});
  const child = row({origin: 'user', parent_job_id: parent.id});
  state.historyJobs = [parent, child];
  assert.match(context.renderProvenance(parent), /Started by User/);
  assert.match(context.renderProvenance(child), /Following your Import/);
  assert.match(context.renderProvenance(child), /data-show-job="import"/);
  assert.doesNotMatch(context.renderListItem(child, 'history', {followUp: true}), /job-origin-chip/);
});
