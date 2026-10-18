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
// Markup a script builds binds its events with addEventListener: no inline
// on*= handlers in HTML strings and no setAttribute('on…').
const inlineMarkupRestrictions = [
  {
    selector: 'Literal[value=/\\bon[a-z]+\\s*=/i]',
    message: 'Bind events with addEventListener, not inline on*= markup.',
  },
  {
    selector: 'TemplateElement[value.raw=/\\bon[a-z]+\\s*=/i]',
    message: 'Bind events with addEventListener, not inline on*= markup.',
  },
  {
    selector: "CallExpression[callee.property.name='setAttribute'][arguments.0.value=/^on[a-z]+$/i]",
    message: 'Bind events with addEventListener, not on* attributes.',
  },
];
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

// The shared navbar and lightbox (see navbar-boot.js and CLAUDE.md): ES
// modules that import each other's functions and reach page code only through
// the page-hook registry. The globals below are the classic shared scripts
// they still call, which load before them. navbar-globals.js is the only file
// here that may write to window: it publishes what classic pages still use.
const navbarModules = ['vireo/static/navbar-*.js', 'vireo/static/lightbox/*.js'];
const navbarBridge = 'vireo/static/navbar-globals.js';
const classicSharedGlobals = {
  // vireo-api.js, vireo-utils.js, vireo-page-hooks.js, vireo-visible-poll.js
  Vireo: 'readonly',
  escapeHtml: 'readonly',
  escapeAttr: 'readonly',
  formatBytes: 'readonly',
  VireoViewPreferences: 'readonly',
  VireoTextSearch: 'readonly',
  // keymap.js, vireo-color-labels.js, vireo-workspace-switcher.js, help.js
  Keymap: 'readonly',
  VireoColorLabels: 'readonly',
  VireoWorkspaceSwitcher: 'readonly',
  openHelpModal: 'readonly',
  // tauri-bridge.js
  isTauri: 'readonly',
  openExternal: 'readonly',
  openExternalWithRecovery: 'readonly',
  copyExternalUrl: 'readonly',
  pickDirectory: 'readonly',
  showExternalOpenFailure: 'readonly',
  // vendor/fuse.min.js
  Fuse: 'readonly',
};
const globalWrite = `AssignmentExpression > MemberExpression.left[object.name=${GLOBAL_OBJECTS}]`;

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
  {
    files: navbarModules,
    languageOptions: {sourceType: 'module', globals: {...globals.browser, ...classicSharedGlobals}},
    rules: {
      'no-undef': 'error',
      'no-unused-vars': ['error', {caughtErrors: 'none'}],
      'no-restricted-syntax': ['error', ...inlineMarkupRestrictions, {
        selector: `${globalWrite}, UpdateExpression > MemberExpression.argument[object.name=${GLOBAL_OBJECTS}]`,
        message: 'Navbar modules own no globals: export the value, and publish it from navbar-globals.js '
          + 'only if a classic page still needs it.',
      }],
    },
  },
  {
    files: [navbarBridge],
    rules: {'no-restricted-syntax': ['error', ...inlineMarkupRestrictions]},
  },
  ...Object.entries(modulePages).flatMap(([page, contract]) => modulePageConfig(page, contract)),
];
