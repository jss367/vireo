import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';
import {parse} from 'acorn';

const FILE = 'vireo/static/navbar-shared-helpers.js';

function functionSource(name) {
  const source = readFileSync(FILE, 'utf8');
  const node = parse(source, {ecmaVersion: 'latest', sourceType: 'module'}).body
    .map(node => node.declaration || node)
    .find(node => node.type === 'FunctionDeclaration' && node.id.name === name);
  assert.ok(node, name);
  return source.slice(node.start, node.end);
}

// The status line element: text set directly, or text and spans set together.
function summaryElement() {
  let children = [];
  const classes = new Set(['idle']);
  return {
    classList: {add: name => classes.add(name), remove: name => classes.delete(name)},
    set textContent(value) { children = [String(value)]; },
    get textContent() { return children.map(child => typeof child === 'string' ? child : child.textContent).join(''); },
    replaceChildren(...nodes) { children = nodes; },
    spans() { return children.filter(child => typeof child !== 'string'); },
  };
}

function harness(jobs) {
  const summary = summaryElement();
  const context = vm.createContext({
    activeJobs: jobs,
    formatJobType: type => type === 'pipeline' ? 'Pipeline' : type,
    document: {
      getElementById: () => summary,
      createElement: () => ({className: '', style: {}, textContent: ''}),
    },
  });
  for (const name of [
    'isLiveJob', 'countsForBadge', 'isWaitingJob', 'bpActiveProgress', 'bpRateText', 'bpProgressPct',
    'bpPhaseStep', 'bpPhaseCounts', 'bpCountText', 'bpErrorCount', 'bpErrorText', 'updateSummary',
  ]) {
    vm.runInContext(functionSource(name), context);
  }
  return {context, summary};
}

function statusLine(job) {
  const {context, summary} = harness([job]);
  context.updateSummary();
  return summary;
}

const step = (id, status, current, total, extra = {}) => ({id, status, progress: {current, total}, error_count: 0, ...extra});

// A Process run in its mask stage, as /api/jobs reports it.
const processRun = (extra = {}) => ({
  id: 'p1', type: 'pipeline', status: 'running',
  progress: {
    phase: 'Extracting features (SAM2 + DINOv2)', stage_id: 'extract_masks',
    current: 74, total: 104, rate: 87.2, rate_unit: 'per_minute',
  },
  steps: [
    step('scan', 'completed', 1965, 1965),
    step('extract_masks', 'running', 641, 1965, {error_count: 193}),
    step('eye_keypoints', 'pending', 0, 0),
  ],
  errors: [],
  ...extra,
});

test('a Process run shows its stage in photos and its overall progress as a percent', () => {
  const line = statusLine(processRun()).textContent;
  assert.equal(
    line,
    'Pipeline — Extracting features (SAM2 + DINOv2) · This phase: 641/1,965 (33%) · Overall: 71% · 193 errors · 87 files/min',
  );
  assert.doesNotMatch(line, /weighted|74\/104/);
});

test('the error count is its own element in the danger color', () => {
  const summary = statusLine(processRun());
  const [errors] = summary.spans();
  assert.equal(errors.textContent, '193 errors');
  assert.equal(errors.className, 'job-summary-errors');
  assert.equal(errors.style.color, 'var(--danger)');
});

test('a run with no errors says nothing about errors', () => {
  const job = processRun();
  job.steps[1].error_count = 0;
  const summary = statusLine(job);
  assert.equal(summary.spans().length, 0);
  assert.equal(
    summary.textContent,
    'Pipeline — Extracting features (SAM2 + DINOv2) · This phase: 641/1,965 (33%) · Overall: 71% · 87 files/min',
  );
});

test('errors count every step, and one error is singular', () => {
  const job = processRun();
  job.steps[0].error_count = 2;
  assert.match(statusLine(job).textContent, / · 195 errors · /);
  job.steps[0].error_count = 0;
  job.steps[1].error_count = 0;
  job.steps[1].error = 'SAM2 failed to load';
  assert.match(statusLine(job).textContent, / · 1 error · /);
});

test('a running step\'s errors count before the step reports them', () => {
  const job = processRun();
  job.steps[0].status = 'running';
  job.steps[1].error_count = 0;
  job.errors = ['[scan] a.xmp: unreadable', '[scan] PERMISSION_DENIED: /Volumes/card'];
  assert.match(statusLine(job).textContent, / · 2 errors · /);
  // Once the step's count names the same failures they are not counted twice.
  job.steps[0].error_count = 1;
  assert.match(statusLine(job).textContent, / · 2 errors · /);
});

test('a Process run\'s notes are not counted as errors', () => {
  const job = processRun();
  job.steps[1].error_count = 0;
  job.steps.push(step('classify:inat21', 'completed', 900, 900, {error: 'iNat21 has no common names'}));
  job.errors = [
    '[extract_masks] no detections qualify',
    '[common_names] iNat21 has no common names',
  ];
  job.notes = [...job.errors];
  const summary = statusLine(job);
  assert.equal(summary.spans().length, 0);
  assert.doesNotMatch(summary.textContent, /error/);
  // A real failure alongside the notes is still counted.
  job.errors.push('[scan] PERMISSION_DENIED: /Volumes/card');
  assert.match(statusLine(job).textContent, / · 1 error · /);
});

test('a job without steps counts its error list', () => {
  const job = {
    id: 't1', type: 'thumbnails', status: 'running',
    progress: {phase: 'Generating', current: 300, total: 2000},
    errors: ['a.nef: unreadable', 'b.nef: unreadable'],
  };
  assert.equal(statusLine(job).textContent, 'thumbnails — Generating · Overall: 300/2,000 (15%) · 2 errors');
});

test('a Process run between stages shows only the overall percent', () => {
  const job = processRun();
  job.steps[1].status = 'completed';
  job.steps[1].error_count = 0;
  job.progress.rate = null;
  assert.equal(statusLine(job).textContent, 'Pipeline — Extracting features (SAM2 + DINOv2) · Overall: 71%');
});

test('a stage that has not counted its photos yet shows only the overall percent', () => {
  const job = processRun({steps: [step('extract_masks', 'running', 0, 0)]});
  assert.match(statusLine(job).textContent, /SAM2 \+ DINOv2\) · Overall: 71% · 87 files\/min$/);
});

test('classification counts the step of the model that is running', () => {
  const job = processRun({
    progress: {phase: 'Classifying with iNat21', stage_id: 'classify', step_id: 'classify:inat21', current: 40, total: 104},
    steps: [
      step('classify:bioclip', 'completed', 900, 900),
      step('classify:inat21', 'running', 120, 900),
    ],
  });
  assert.equal(statusLine(job).textContent, 'Pipeline — Classifying with iNat21 · This phase: 120/900 (13%) · Overall: 38%');
});

test('a step id left over from an earlier stage is not counted as this stage', () => {
  const job = processRun({
    progress: {phase: 'Extracting features', stage_id: 'extract_masks', step_id: 'classify:inat21', current: 74, total: 104},
    steps: [
      step('classify:inat21', 'running', 120, 900),
      step('extract_masks', 'running', 10, 50),
    ],
  });
  assert.match(statusLine(job).textContent, /This phase: 10\/50 \(20%\)/);
});

test('a sub-phase a Process stage reports itself wins over the step', () => {
  const job = processRun({
    progress: {
      phase: 'Preparing species labels', stage_id: 'model_loader', current: 16, total: 104,
      phase_current: 100, phase_total: 1419, phase_label: 'Species labels',
    },
    steps: [step('model_loader', 'running', 100, 1419)],
  });
  assert.equal(statusLine(job).textContent, 'Pipeline — Preparing species labels · This phase: 100/1,419 (7%) · Overall: 15%');
});

test('other jobs keep real counts for the phase and the whole job', () => {
  const job = {
    id: 'm1', type: 'move-folder', status: 'running',
    progress: {phase: 'Copying', current: 16, total: 104, phase_current: 0, phase_total: 20, rate: 3},
  };
  assert.equal(statusLine(job).textContent, 'move-folder — Copying · This phase: 0/20 (0%) · Overall: 16/104');
});

test('queued jobs are counted after the errors and the speed', () => {
  const {context, summary} = harness([processRun(), {id: 'p2', type: 'pipeline', status: 'queued'}]);
  context.updateSummary();
  assert.match(summary.textContent, / · 193 errors · 87 files\/min \+ 1 more$/);
});
