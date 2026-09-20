/* Undo working photo edits without requiring a saved checkpoint. */
var editorHistory = {undo: [], redo: [], current: null, gesture: null, recordedGesture: null};

function editorHistorySnapshot() {
  return {
    recipe: cloneRecipe(editorState.recipe),
    cropAspect: editorState.cropAspect,
    cropEditing: editorState.cropEditing,
    localStale: editorState.localStale,
  };
}

function resetEditorHistory() {
  editorHistory.undo = [];
  editorHistory.redo = [];
  editorHistory.current = editorHistorySnapshot();
  editorHistory.gesture = null;
  editorHistory.recordedGesture = null;
  if (window.renderHistoryControls) window.renderHistoryControls();
}

function refreshEditorHistoryLocalStaleness() {
  var savedMask = (editorState.savedRecipe.local || {}).mask;
  if (!savedMask) return;
  // Staleness describes the mask snapshot, not an editing step. A delayed
  // status read must update every matching undo/redo state without changing
  // the status of a newer mask selected while that read was in flight.
  var gesture = editorHistory.gesture || {};
  editorHistory.undo.concat(editorHistory.redo, gesture.undoBefore || [], gesture.redoBefore || [],
    [editorHistory.current, editorState]).forEach(function(snapshot) {
    var mask = snapshot && (snapshot.recipe.local || {}).mask;
    if (mask && mask.ref === savedMask.ref && mask.source_digest === savedMask.source_digest) {
      snapshot.localStale = editorState.savedLocalStale;
    }
  });
}

function recordEditorHistory() {
  var next = editorHistorySnapshot();
  var previous = editorHistory.current;
  if (!previous || recipeKey(previous.recipe) === recipeKey(next.recipe)) {
    editorHistory.current = next;
    return;
  }
  if (!editorHistory.gesture || editorHistory.recordedGesture !== editorHistory.gesture) {
    if (editorHistory.gesture) {
      // Keep the branch intact until a drag has made a lasting change. These
      // shallow copies also retain any oldest entry evicted by the size cap.
      editorHistory.gesture.undoBefore = editorHistory.undo.slice();
      editorHistory.gesture.redoBefore = editorHistory.redo.slice();
    }
    editorHistory.undo.push(previous);
    // Bound memory for long editing sessions; recipes contain no image pixels.
    if (editorHistory.undo.length > 100) editorHistory.undo.shift();
  }
  editorHistory.recordedGesture = editorHistory.gesture;
  editorHistory.current = next;
  editorHistory.redo = [];
  if (window.renderHistoryControls) window.renderHistoryControls();
}

function finishEditorHistoryGesture() {
  // A drag that returns to its starting value is not an edit.
  var previous = editorHistory.undo[editorHistory.undo.length - 1];
  if (editorHistory.gesture && editorHistory.recordedGesture === editorHistory.gesture &&
      previous && recipeKey(previous.recipe) === recipeKey(editorState.recipe)) {
    editorHistory.undo = editorHistory.gesture.undoBefore;
    editorHistory.redo = editorHistory.gesture.redoBefore;
  }
  editorHistory.gesture = null;
  editorHistory.recordedGesture = null;
  if (window.renderHistoryControls) window.renderHistoryControls();
}

function editorHistoryBlocked() {
  return editorState.loading || !editorState.photoId ||
    (editorState.localMaskPromise && !editorState.localMask) ||
    Object.keys(editorState.savingPhotoIds).length > 0;
}

function hasEditorHistory() {
  return editorHistory.undo.length > 0 || editorHistory.redo.length > 0 || isEditorDirty();
}

// The shared history buttons and shortcuts use the working history first.
// Saving starts a fresh session; the existing persisted undo then applies.
window.localHistoryStatus = function(operation) {
  if (!editorHistoryBlocked() && !hasEditorHistory()) return null;
  return {
    available: !editorHistoryBlocked() && editorHistory[operation].length > 0,
    description: 'Photo adjustment',
  };
};

window.changeLocalHistory = function(operation) {
  if (editorHistoryBlocked()) return false;
  finishEditorHistoryGesture();
  if (!hasEditorHistory()) return null;
  var source = editorHistory[operation];
  if (!source.length) return false;
  editorHistory[operation === 'undo' ? 'redo' : 'undo'].push(editorHistorySnapshot());
  var snapshot = source.pop();
  // Invalidate pending mask/picker work before restoring the working recipe.
  editorState.localMaskUpdateSeq++;
  editorState.localMaskPromise = null;
  cancelPointColorPicker();
  colorEditor.dragging = null;
  editorState.drag = null;
  editorState.recipe = cloneRecipe(snapshot.recipe);
  ensureCrop(editorState.recipe);
  editorState.cropAspect = snapshot.cropAspect;
  editorState.cropEditing = snapshot.cropEditing;
  editorState.localStale = snapshot.localStale;
  editorState.showBefore = false;
  editorHistory.current = editorHistorySnapshot();
  syncControls();
  updateFeedbackControls();
  updatePreview();
  if (window.renderHistoryControls) window.renderHistoryControls();
  return true;
};

function editorHistoryOwnsEvent(event) {
  var target = event.target;
  // Text fields retain their own native text undo; sliders still edit photos.
  if (target && (target.isContentEditable || target.closest('textarea, select') ||
      (target.tagName === 'INPUT' && !['range', 'checkbox', 'radio', 'button'].includes(target.type)))) return false;
  return !document.querySelector('.modal-overlay.open, .grm-overlay.open, .inspect-overlay.open, .help-overlay.active, .folder-browser-overlay.open, .shortcuts-overlay.open, .export-preset-dialog-overlay.open');
}

document.addEventListener('keydown', function(event) {
  if (event.defaultPrevented || !editorHistoryOwnsEvent(event)) return;
  if (!(event.metaKey || event.ctrlKey) || event.altKey || event.key.toLowerCase() !== 'z') return;
  event.preventDefault();
  event.stopImmediatePropagation();
  if (event.shiftKey) window.doRedo();
  else window.doUndo();
}, true);

// Native Edit > Undo/Redo can arrive as an editing event in a WebView.
document.addEventListener('beforeinput', function(event) {
  if (!['historyUndo', 'historyRedo'].includes(event.inputType) || !editorHistoryOwnsEvent(event)) return;
  event.preventDefault();
  if (event.inputType === 'historyRedo') window.doRedo();
  else window.doUndo();
});

document.addEventListener('pointerdown', function(event) {
  finishEditorHistoryGesture();
  if (event.button === 0 && event.target.closest('.editor-shell')) {
    // Opening the crop or receiving mask staleness can change UI state without
    // changing the recipe. Capture that state at the start of the next gesture.
    if (editorHistory.current && recipeKey(editorHistory.current.recipe) === recipeKey(editorState.recipe)) {
      editorHistory.current = editorHistorySnapshot();
    }
    editorHistory.gesture = {pointerId: event.pointerId};
  }
}, true);
['pointerup', 'pointercancel', 'lostpointercapture'].forEach(function(type) {
  document.addEventListener(type, function(event) {
    if (editorHistory.gesture && editorHistory.gesture.pointerId === event.pointerId) {
      finishEditorHistoryGesture();
    }
  });
});
window.addEventListener('blur', finishEditorHistoryGesture);
