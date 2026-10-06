// Shared editor state, loading freeze, dirty tracking, and status text.
// Classic page script; load boot.js after all definitions.

var editorState = {
  photoId: null,
  photo: null,
  savedRecipe: {},
  savedRecipeSeq: 0,
  recipe: {},
  presets: [],
  localDraft: {subject: {}, background: {}, feather: 0},
  localMask: null,
  localMaskPromise: null,
  localMaskUpdateSeq: 0,
  localAvailable: false,
  localStale: false,
  savedLocalStale: false,
  maskOverlay: false,
  maskOverlaySeq: 0,
  maskOverlayTimer: null,
  previewSeq: 0,
  previewTimer: null,
  drag: null,
  pan: null,
  spacePan: false,
  showBefore: false,
  // Crop handles live on the uncropped source while a crop is being edited.
  // Once saved, the editor switches to the actual cropped render and fits it
  // in the stage; Edit Crop reopens the full source when another change is
  // needed.
  cropEditing: true,
  zoomMode: 'fit',
  zoomPercent: 100,
  cropAspect: null,
  navIds: [],
  baseNavIds: [],
  loadSeq: 0,
  loading: false,
  histogramTimer: null,
  searchTimer: null,
  searchSeq: 0,
  searchQuery: '',
  // Seq under which the current `searchQuery` was dispatched. When the input
  // is retyped back to `searchQuery` after an intermediate keystroke bumped
  // `searchSeq`, this mismatch tells us the in-flight response was already
  // invalidated so a replacement fetch is required.
  searchQuerySeq: 0,
  savingPhotoIds: {},
  suppressUnloadPrompt: false,
};

function setEditorLoading(loading) {
  // While a new photo is fetching, freeze the editing surface so a slider
  // tweak or crop drag can't mutate state that ends up applied to (and saved
  // onto) the photo currently loading. markChanged() and the crop pointerdown
  // handler also gate on editorState.loading as belt-and-braces.
  editorState.loading = !!loading;
  var shell = document.querySelector('.editor-shell');
  if (shell) shell.classList.toggle('editor-loading', !!loading);
  var resetAll = document.getElementById('resetAllBtn');
  if (resetAll) resetAll.disabled = !!loading;
  var copyBtn = document.getElementById('copySettingsBtn');
  if (copyBtn) copyBtn.disabled = !!loading;
  var exportBtn = document.getElementById('exportBtn');
  if (exportBtn) exportBtn.disabled = !!loading || !editorState.photoId;
  updateEditorZoomControl();
  updateAspectButtons();
  if (window.renderHistoryControls) window.renderHistoryControls();
}

function isEditorDirty() {
  return recipeKey(editorState.recipe) !== recipeKey(editorState.savedRecipe);
}

function cloneRecipe(recipe) {
  if (!recipe || typeof recipe !== 'object') return {};
  try { return JSON.parse(JSON.stringify(recipe)); }
  catch (_) { return {}; }
}

function setStatus(text, isError) {
  var el = document.getElementById('editorStatus');
  if (!el) return;
  el.textContent = text || '';
  el.classList.toggle('error', !!isError);
}

function setButtonActive(id, active) {
  var btn = document.getElementById(id);
  if (btn) btn.classList.toggle('active', !!active);
}

function setButtonDisabled(id, disabled) {
  var btn = document.getElementById(id);
  if (btn) btn.disabled = !!disabled;
}

function updateFeedbackControls() {
  setButtonActive('beforeBtn', editorState.showBefore);
  var beforeBtn = document.getElementById('beforeBtn');
  if (beforeBtn) beforeBtn.textContent = editorState.showBefore ? 'After' : 'Before';
  var canvas = document.getElementById('editorCanvas');
  if (canvas) canvas.classList.toggle('show-before', !!editorState.showBefore);
  var note = document.getElementById('feedbackNote');
  if (note) {
    note.textContent = editorState.showBefore ? 'Approximate saved preview' : 'Approximate current preview';
  }
  updateEditorZoomControl();
  updateAspectButtons();
}
