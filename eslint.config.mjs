import js from '@eslint/js';
import globals from 'globals';

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
];
