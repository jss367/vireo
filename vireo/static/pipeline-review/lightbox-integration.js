// Pipeline Review options and guards for the shared lightbox.
// Classic page script; shared globals are initialized before boot.js runs.

function pipelineReviewVisiblePhotoList(photoId) {
  var photo = findPhotoInResults(photoId);
  if (!photo) return [];
  var groupOverlay = document.getElementById('grmOverlay');
  var groupCard = groupOverlay && groupOverlay.classList.contains('open')
    ? groupOverlay.querySelector('.grm-card[data-photo-id="' + photoId + '"]')
    : null;
  var root = groupCard ? groupOverlay : document.getElementById('encountersContainer');
  var selector = groupCard ? '.grm-card[data-photo-id]' : '.photo-card[data-photo-id]';
  var seen = {};
  var list = [];
  if (root) {
    root.querySelectorAll(selector).forEach(function(card) {
      // Collapsed encounters remain in the DOM but are not part of the visible
      // review sequence. Group Review cards are all visible in its overlay.
      if (!groupCard && card.offsetParent === null) return;
      var id = parseInt(card.dataset.photoId, 10);
      if (!id || seen[id]) return;
      var item = findPhotoInResults(id);
      if (!item) return;
      seen[id] = true;
      list.push(item);
    });
  }
  if (!seen[photoId]) list = [photo];
  return list;
}

// photoId is passed by callers that open the lightbox on a photo that may not
// be on screen (Find Similar). Under a Folder/Collection scope a photo the
// scope hides opens read-only, matching the scope guard on the writers.
function pipelineReviewLightboxOptions(photoId) {
  var groupOverlay = document.getElementById('grmOverlay');
  var groupApplying = !!(
    groupOverlay && groupOverlay.classList.contains('open') &&
    grmState && grmState.applying
  );
  var scoped = isScopedReviewView();
  var outOfScope = !scoped && photoId != null && !isPhotoActionableInReviewScope(photoId);
  var readOnly = scoped || groupApplying || outOfScope;
  var readOnlyMessage;
  if (groupApplying) readOnlyMessage = 'Group Review is applying changes. Wait for it to finish.';
  else if (scoped) readOnlyMessage = scopedViewReadOnlyMessage();
  else if (outOfScope) {
    readOnlyMessage = 'This photo is outside the selected '
      + (reviewScopeMode === 'folder' ? 'folder' : 'collection')
      + '. Switch Scope to All reviewed photos to edit it.';
  }
  return {readOnly: readOnly, readOnlyMessage: readOnlyMessage};
}

function openPipelineLightbox(photoId) {
  var groupOverlay = document.getElementById('grmOverlay');
  if (groupOverlay && groupOverlay.classList.contains('open') &&
      grmState && grmState.applying) {
    showToast('Group Review is applying changes — wait for it to finish', 'warning');
    return false;
  }
  if (!isPhotoActionableInReviewScope(photoId)) return false;
  var photo = findPhotoInResults(photoId);
  if (!photo || typeof openLightbox !== 'function') return false;
  var list = pipelineReviewVisiblePhotoList(photoId);
  openLightbox(photoId, photo.filename || '', list, pipelineReviewLightboxOptions(photoId));
  return true;
}

function grmHasPendingUserEdits() {
  if (!grmState) return false;
  // Movement and species controls remain usable while the DB seed request is
  // in flight. We cannot diff those choices against an unknown snapshot yet,
  // but they are still user work that same-window navigation must preserve.
  if (!grmState.seeded) {
    return !!((grmState.touched && grmState.touched.size) || grmState.speciesFieldTouched);
  }
  var diff = grmComputeDiff();
  // An unconfirmed classifier prediction pre-fills the species field and
  // makes diff.speciesChanged true before the user has touched anything.
  // That is a proposed Apply action, not unsaved user work, so it must not
  // block same-window navigation in Tauri. Only protect a species change
  // after the user has actually edited the field, and only while that edit
  // still produces a real confirmation change.
  return grmFlagsDirty(diff) ||
    (!!grmState.speciesFieldTouched && diff.speciesChanged);
}

function registerPipelineReviewLightboxBrowseHook() {
    window.getLightboxBrowseDisabledHint = function(photoId, willNavigateInPlace) {
      var overlay = document.getElementById('grmOverlay');
      var groupOpen = overlay && overlay.classList.contains('open');
      var sameWindowNavigation = !!willNavigateInPlace ||
        (typeof isTauri === 'function' && isTauri());
      if (groupOpen && sameWindowNavigation && grmHasPendingUserEdits()) {
        return 'Apply or close Group Review before leaving this page';
      }
      return null;
    };
}
