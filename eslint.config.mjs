import js from '@eslint/js';
import globals from 'globals';

const checkedBrowseControllers = [
  'vireo/static/browse/panel-requests.js',
  'vireo/static/browse/selection-panel-state.js',
  'vireo/static/browse/selection-panel-events.js',
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
    files: checkedBrowseControllers,
    rules: {
      'no-undef': 'error',
      'no-unused-vars': 'error',
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
