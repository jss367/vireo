// Detection-box overlays: the Show/Hide Boxes toggle, species colors, and re-syncing boxes after an edit.
// Classic page script; load boot.js after all definitions.

function _reviewStoredBool(key, fallback) {
  try {
    var value = localStorage.getItem(key);
    if (value === '0') return false;
    if (value === '1') return true;
  } catch (e) {}
  return fallback;
}
function _reviewPersistBool(key, value) {
  try { localStorage.setItem(key, value ? '1' : '0'); } catch (e) {}
}
var REVIEW_BOXES_STORAGE_KEY = 'vireo.review.detectionBoxesVisible';
var reviewDetectionBoxesVisible = _reviewStoredBool(REVIEW_BOXES_STORAGE_KEY, false);

function _syncReviewDetectionBoxesBtn() {
  var btn = document.getElementById('toggleDetectionBoxesBtn');
  if (btn) btn.textContent = reviewDetectionBoxesVisible ? 'Hide Boxes' : 'Show Boxes';
}

function _syncReviewDetectionBoxes() {
  document.querySelectorAll('.detection-box').forEach(function(box) {
    var pid = parseInt(box.getAttribute('data-photo-id'), 10);
    var hideDetectionOverlay = (
      typeof window.vireoPhotoHasOrientationEdit === 'function' &&
      !isNaN(pid) &&
      window.vireoPhotoHasOrientationEdit(pid)
    );
    box.style.display = (reviewDetectionBoxesVisible && !hideDetectionOverlay) ? '' : 'none';
  });
}

function toggleReviewDetectionBoxes() {
  reviewDetectionBoxesVisible = !reviewDetectionBoxesVisible;
  _reviewPersistBool(REVIEW_BOXES_STORAGE_KEY, reviewDetectionBoxesVisible);
  _syncReviewDetectionBoxesBtn();
  _syncReviewDetectionBoxes();
}

/* Species color palette for bounding boxes */
var _speciesColors = {};
var _colorPalette = [
  '#24E5CA', '#f0c040', '#e74c3c', '#3498db', '#9b59b6',
  '#1abc9c', '#e67e22', '#2ecc71', '#e84393', '#00cec9'
];
var _nextColorIdx = 0;

function getSpeciesColor(species) {
  if (!_speciesColors[species]) {
    _speciesColors[species] = _colorPalette[_nextColorIdx % _colorPalette.length];
    _nextColorIdx++;
  }
  return _speciesColors[species];
}

function bindReviewDetectionBoxRenderSync() {
  document.addEventListener('lightbox:renderchanged', function(e) {
    var ids = e && e.detail && Array.isArray(e.detail.photoIds) ? e.detail.photoIds : [];
    ids.forEach(function(id) {
      var hideDetectionOverlay = (
        typeof window.vireoPhotoHasOrientationEdit === 'function' &&
        window.vireoPhotoHasOrientationEdit(id)
      );
      document.querySelectorAll('.detection-box[data-photo-id="' + id + '"]').forEach(function(box) {
        box.style.display = (reviewDetectionBoxesVisible && !hideDetectionOverlay) ? '' : 'none';
      });
    });
  });
}
