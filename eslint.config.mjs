import {readFileSync} from 'node:fs';
import js from '@eslint/js';
import globals from 'globals';

// Pages whose scripts are ES modules (see CLAUDE.md and
// vireo/tests/test_module_pages.py). Their files share nothing through
// globals: every name is imported, declared, or a browser global. The page
// hands the navbar and lightbox its hooks through Vireo.registerPageHooks
// (vireo-page-hooks.js), not window, so the only window writes allowed are
// the shared-script latches the list names (`sharedWrites`), made from the
// page's shared.js.
const modulePages = JSON.parse(readFileSync(new URL('./frontend-module-pages.json', import.meta.url), 'utf8'));
const GLOBAL_OBJECTS = '/^(window|globalThis|self)$/';
const inlineHandlerRestrictions = [
  {
    selector: 'Literal[value=/\\bon[a-z]+\\s*=/i]',
    message: 'Module pages bind events with addEventListener, not inline on*= markup.',
  },
  {
    selector: 'TemplateElement[value.raw=/\\bon[a-z]+\\s*=/i]',
    message: 'Module pages bind events with addEventListener, not inline on*= markup.',
  },
  {
    selector: 'AssignmentExpression > MemberExpression.left[property.name=/^on[a-z]+$/i]',
    message: 'Module pages bind events with addEventListener, not on* properties.',
  },
  {
    selector: 'AssignmentExpression > MemberExpression.left[computed=true][property.value=/^on[a-z]+$/i]',
    message: 'Module pages bind events with addEventListener, not on* properties.',
  },
  {
    selector: `UpdateExpression > MemberExpression.argument[object.name=${GLOBAL_OBJECTS}], UnaryExpression[operator='delete'] > MemberExpression.argument[object.name=${GLOBAL_OBJECTS}]`,
    message: 'Module pages own no globals: export the value instead.',
  },
  {
    selector: `CallExpression[callee.object.name='Reflect'][callee.property.name=/^(set|defineProperty|deleteProperty)$/][arguments.0.name=${GLOBAL_OBJECTS}]`,
    message: 'Module pages own no globals: export the value instead.',
  },
  {
    selector: "CallExpression[callee.property.name='setAttribute'][arguments.0.value=/^on[a-z]+$/i]",
    message: 'Module pages bind events with addEventListener, not on* attributes.',
  },
  {
    selector: `CallExpression[callee.object.name='Object'][callee.property.name=/^(assign|defineProperty|defineProperties)$/][arguments.0.name=${GLOBAL_OBJECTS}]`,
    message: 'Module pages own no globals: export the value instead.',
  },
];

/** @param {string} page @param {{pageHooks: string[], sharedWrites: string[]}} contract */
function modulePageConfig(page, contract) {
  const allowed = contract.sharedWrites.join('|');
  const sharedWriteSelector = `AssignmentExpression > MemberExpression.left[object.name=${GLOBAL_OBJECTS}]`
    + (allowed ? `:not([computed=false][property.name=/^(${allowed})$/])` : '');
  const pageFiles = `vireo/static/${page}/**/*.js`;
  const shim = `vireo/static/${page}/shared.js`;
  const rules = {
    'no-undef': 'error',
    'no-unused-vars': ['error', {caughtErrors: 'none'}],
  };
  return [
    {
      files: [pageFiles],
      languageOptions: {sourceType: 'module', globals: globals.browser},
      rules: {
        ...rules,
        'no-restricted-syntax': ['error', ...inlineHandlerRestrictions, {
          selector: `AssignmentExpression > MemberExpression.left[object.name=${GLOBAL_OBJECTS}]`,
          message: 'Module pages own no globals: export the value, or register a page hook from shared.js '
            + 'with Vireo.registerPageHooks (listed in frontend-module-pages.json).',
        }],
      },
    },
    {
      files: [shim],
      rules: {
        'no-restricted-syntax': ['error', ...inlineHandlerRestrictions, {
          selector: sharedWriteSelector,
          message: `${shim} may only write the shared latches listed for "${page}" (sharedWrites) `
            + 'in frontend-module-pages.json; register page hooks with Vireo.registerPageHooks.',
        }],
      },
    },
  ];
}

const checkedControllers = [
  'vireo/static/browse/panel-requests.js',
  'vireo/static/browse/selection-panel-state.js',
  'vireo/static/browse/selection-panel-events.js',
  'vireo/static/vireo-dialog.js',
  'vireo/static/vireo-panel-loader.js',
  'vireo/static/browse/panel-loaders.js',
  // Small shared classic scripts: each names the page globals it exports in
  // an `exported` directive, and the navbar globals it calls are declared
  // for it below.
  'vireo/static/import-recovery.js',
  'vireo/static/pipeline-messages.js',
  'vireo/static/map-focus.js',
  'vireo/static/vireo-export-job.js',
  'vireo/static/vireo-visible-poll.js',
  'vireo/static/vireo-thumbnail-queue.js',
  'vireo/static/vireo-predictions.js',
];

export default [
  {ignores: ['vireo/static/vendor/**', '**/*.min.js']},
  {
    files: ['vireo/static/**/*.js'],
    languageOptions: {
      sourceType: 'script',
      globals: globals.browser,
    },
    rules: {
      ...js.configs.recommended.rules,
      // Legacy classic scripts share page globals and expose functions to
      // other scripts/inline handlers. Tighten these checks as their state
      // moves into controllers with explicit dependencies (see below).
      'no-undef': 'off',
      'no-unused-vars': 'off',
      'no-empty': ['error', {allowEmptyCatch: true}],
    },
  },
  {
    files: checkedControllers,
    rules: {
      'no-undef': 'error',
      'no-unused-vars': 'error',
    },
  },
  {
    files: ['vireo/static/browse/panel-loaders.js'],
    languageOptions: {
      globals: {
        getBrowseSelection: 'readonly',
        selectionIdsKey: 'readonly',
        safeFetch: 'readonly',
        renderSelectionKeywordSuggestions: 'readonly',
        renderSelectionPredictions: 'readonly',
        renderDetailPredictions: 'readonly',
      },
    },
  },
  {
    files: ['vireo/static/vireo-export-job.js'],
    languageOptions: {
      globals: {
        safeEventSource: 'readonly',
        showToast: 'readonly',
      },
    },
  },
  {
    files: ['vireo/static/browse/selection-panel-events.js'],
    languageOptions: {
      globals: {
        openBatchDevelopmentEditor: 'readonly',
        pasteEditSettingsToSelection: 'readonly',
        setSelectionWildlifeExcluded: 'readonly',
        applySelectionKeyword: 'readonly',
        removeSelectionKeyword: 'readonly',
        toggleSelectionPredictions: 'readonly',
        acceptSelectionPrediction: 'readonly',
        showSelectionPredictionPhotos: 'readonly',
        openPredictionInReview: 'readonly',
      },
    },
  },
  ...Object.entries(modulePages).flatMap(([page, contract]) => modulePageConfig(page, contract)),
];
