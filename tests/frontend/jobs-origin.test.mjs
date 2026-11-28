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
  for (const name of ['bpActiveProgress', 'bpProgressPct', 'bpPhaseStep', 'bpPhaseCounts', 'bpCountText', 'bpErrorCount', 'bpErrorText', 'isWaitingJob', 'renderJobList']) {
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

test('the status panel distinguishes phase counts from overall progress', () => {
  const {context} = harness();
  const progress = {current: 16, total: 104, phase_current: 0, phase_total: 20};
  assert.match(context.renderJobList([row({progress})]), /This phase: 0\/20 \(0%\) · Overall: 16\/104/);
  delete progress.phase_total;
  assert.match(context.renderJobList([row({progress})]), /Overall: 16\/104 \(15%\)/);
});

test('the status panel shows a Process run in photos, a percent and its errors', () => {
  const {context} = harness();
  const html = context.renderJobList([row({
    type: 'pipeline', status: 'running',
    progress: {phase: 'Extracting features', stage_id: 'extract_masks', current: 74, total: 104},
    steps: [{id: 'extract_masks', status: 'running', progress: {current: 641, total: 1965}, error_count: 193}],
  })]);
  assert.match(html, />This phase: 641\/1,965 \(33%\) · Overall: 71%</);
  assert.match(html, /class="bp-job-errors"[^>]*>193 errors</);
  assert.doesNotMatch(html, /weighted/);
});

test('the job card gives a Process run\'s overall progress as a percent', () => {
  const {context} = harness();
  const progress = {current: 16, total: 104, phase_current: 100, phase_total: 1419, phase_label: 'Species labels'};
  assert.equal(
    context.renderProgressMeta(row({type: 'pipeline', progress})),
    '<span>Overall: 15%</span><span>This phase (Species labels): 7%</span>',
  );
  assert.match(context.renderProgressMeta(row({progress})), /^<span>Overall: 16 \/ 104<\/span>/);
  assert.equal(context.overallText(row({type: 'pipeline', progress: {current: 74, total: 104}})), '71%');
  assert.equal(context.overallText(row({progress: {current: 1200, total: 5000}})), '1,200 / 5,000');
  const pipeline = row({type: 'pipeline', progress: {current: 74, total: 104}});
  assert.equal(context.progressCountsText(pipeline, {current: 74, total: 104, phase: false}), '71%');
  assert.equal(context.progressCountsText(pipeline, {current: 100, total: 1419, phase: true}), '100 / 1,419');
});
