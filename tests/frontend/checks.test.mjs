import assert from 'node:assert/strict';
import {test} from 'node:test';
import {ESLint} from 'eslint';

const eslint = new ESLint();

async function lint(code, filePath) {
  return (await eslint.lintText(code, {filePath}))[0].messages;
}

test('strict controllers reject undefined names and unused variables', async () => {
  const messages = await lint(
    '(function() { const unused = 1; misspelledPanelAction(); })();',
    'vireo/static/browse/panel-requests.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId).sort(), ['no-undef', 'no-unused-vars']);
});

test('classic scripts accept shared globals but still reject correctness errors', async () => {
  const messages = await lint(
    'refreshGridCards(); switch (status) { case 1: break; case 1: break; }',
    'vireo/static/browse/grid.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-duplicate-case']);
});

test('strict controllers recognize browser globals and the explicit action bridge', async () => {
  const messages = await lint(
    'window.addEventListener("click", function() { acceptSelectionPrediction(0, true); });',
    'vireo/static/browse/selection-panel-events.js',
  );
  assert.deepEqual(messages, []);
});

test('legacy actions are only declared for the event bridge', async () => {
  const messages = await lint(
    'acceptSelectionPrediction(0, true);',
    'vireo/static/browse/panel-requests.js',
  );
  assert.deepEqual(messages.map(message => message.ruleId), ['no-undef']);
});

test('third-party scripts are excluded from application lint', async () => {
  assert.equal(await eslint.isPathIgnored('vireo/static/vendor/fuse.min.js'), true);
  assert.equal(await eslint.isPathIgnored('vireo/static/browse/grid.js'), false);
});
