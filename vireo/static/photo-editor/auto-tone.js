// Auto Tone requests and their result messages.
// Classic page script; load boot.js after all definitions.

// --- Auto Tone -------------------------------------------------------------
// The server fits Auto Tone by rendering candidates through the real tone
// pipeline (see vireo/auto_tone.py), metering on the subject when the photo
// has a mask or detection. It sets exposure, highlights, shadows, contrast,
// whites, blacks, vibrance and saturation. White balance and the presence
// controls stay as the user set them so a wanted cast such as golden-hour
// warmth is never neutralised; the fit renders them, so it balances the photo
// as it will look with them.

var AUTO_TONE_KEYS = ['exposure', 'highlights', 'shadows', 'contrast', 'whites', 'blacks', 'vibrance', 'saturation'];

function autoToneMessage(result, changedControls) {
  var notes = (result && result.notes) || [];
  var metering = '';
  if (result && result.metering === 'subject') {
    metering = result.subject_source === 'detection'
      ? ' (metered on the detected subject)'
      : ' (metered on the subject mask)';
  }
  if (!changedControls) return 'Auto Tone' + metering + ': already balanced, nothing changed';
  if (!notes.length) {
    return 'Auto Tone' + metering + ': Reset previous tone adjustments; source already balanced. White balance left unchanged.';
  }
  var text = notes.join(', ');
  return 'Auto Tone' + metering + ': ' + text.charAt(0).toUpperCase() + text.slice(1) +
    '. White balance left unchanged.';
}

function _loadImage(src) {
  return new Promise(function(resolve, reject) {
    var im = new Image();
    im.onload = function() { resolve(im); };
    im.onerror = function() { reject(new Error('image load failed')); };
    im.src = src;
  });
}

async function autoTone() {
  if (!editorState.photoId) return;
  var photoId = editorState.photoId;
  var loadSeq = editorState.loadSeq;
  var btn = document.getElementById('autoToneBtn');
  if (btn && btn.disabled) return;
  var prevLabel = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Analyzing...';
  }
  try {
    // The fit reads the frame (geometry and crop), white balance and
    // presence from this recipe; its tonal values do not affect the result,
    // so repeated clicks are idempotent. Snapshot the recipe: if the user
    // edits while the fit runs, the result describes a stale frame and
    // applying it would overwrite newer edits.
    var startKey = recipeKey(editorState.recipe);
    var analysed = previewRecipeFor(editorState.recipe, true);
    delete analysed.local;
    var result = await safeFetch('/api/photos/' + photoId + '/auto-tone?recipe=' +
      encodeURIComponent(JSON.stringify(analysed)), {}, {toast: false});
    if (editorState.photoId !== photoId || editorState.loadSeq !== loadSeq ||
        recipeKey(editorState.recipe) !== startKey) return;
    var auto = result.adjustments || {};

    // Replace the fitted controls; keep every other choice (white balance,
    // presence, detail, curves, HSL, grading, denoise mode) as it was.
    var adj = cloneRecipe(editorState.recipe.adjustments || {});
    var changedControls = false;
    AUTO_TONE_KEYS.forEach(function(k) {
      var value = Number(auto[k] || 0);
      if (Math.abs(Number(adj[k] || 0) - value) > 0.000001) changedControls = true;
      if (Math.abs(value) > 0.000001) adj[k] = value;
      else delete adj[k];
    });
    if (Object.keys(adj).length) editorState.recipe.adjustments = adj;
    else delete editorState.recipe.adjustments;

    syncControls();
    markChanged(true);
    if (typeof showToast === 'function') showToast(autoToneMessage(result, changedControls), 'success');
  } catch (e) {
    if (editorState.photoId !== photoId || editorState.loadSeq !== loadSeq) return;
    if (typeof showToast === 'function') {
      showToast('Could not analyze this photo for Auto Tone' +
        (e && e.message ? ': ' + e.message : ''), 'error');
    }
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = prevLabel || 'Auto Tone';
    }
  }
}
