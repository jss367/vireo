// Photo thumbnails, the time-review sample and split controls, and the lightbox preview.
// Classic page script; load boot.js after all definitions.
'use strict';

function openPhotoPreview(photo) {
  var group = currentGroup();
  if (!photo || !group || typeof openLightbox !== 'function') return;
  // While an assignment is in flight or a partial batch is waiting for
  // retry, opening the lightbox lets the user delete a photo; the
  // 'lightbox:photodeleted' handler then runs renderCurrentGroup() which
  // nulls state.assignment and unlocks navigation, silently orphaning the
  // committed chunks. Keep the preview closed until the pending
  // assignment resolves.
  if (hasPartialAssignmentProgress()) {
    showToast('Finish or retry the pending assignment before opening a photo — ' +
      formatNumber(state.assignment.completed) + ' of ' +
      formatNumber(state.assignment.total) + ' photos already got “' +
      state.selectedChoice.name + '”. Reload to abandon.', 'error');
    return;
  }
  if (state.isAssigning) {
    showToast('Assignment in progress — finish or wait before opening a photo.', 'error');
    return;
  }
  openLightbox(photo.id, photo.filename || '', group.photos.slice());
}

function renderThumbnails(group) {
  var container = document.getElementById('locationReviewThumbnails');
  if (state.mode === 'time') { renderTimeThumbnails(group); return; }
  var visible = group.photos.slice(0, 24);
  container.innerHTML = visible.map(function(photo) {
    return '<button class="location-review-thumb" type="button" data-photo-id="' + photo.id + '" title="Open ' + escapeAttr(photo.filename) + '">' +
      '<img src="/thumbnails/' + photo.id + '.jpg" alt="" loading="lazy" onerror="this.style.visibility=\'hidden\'">' +
      '<span>' + escapeHtml(photo.filename) + '</span></button>';
  }).join('') + (group.photos.length > visible.length
    ? '<div class="location-review-more">+' + formatNumber(group.photos.length - visible.length) + ' more</div>' : '');
  container.querySelectorAll('[data-photo-id]').forEach(function(button) {
    button.addEventListener('click', function() {
      var photoId = parseInt(button.dataset.photoId, 10);
      var photo = group.photos.find(function(item) { return item.id === photoId; });
      openPhotoPreview(photo);
    });
  });
}

function renderTimeThumbnails(group) {
  var pageSize = 36;
  state.photoPage = Math.min(state.photoPage, Math.floor((group.count - 1) / pageSize));
  var visible;
  if (state.inspectAll) {
    visible = group.photos.slice(state.photoPage * pageSize, (state.photoPage + 1) * pageSize);
  } else {
    var sampleCount = Math.min(6, group.count);
    visible = Array.from({length: sampleCount}, function(_, index) {
      return group.photos[sampleCount === 1 ? 0 : Math.round(index * (group.count - 1) / (sampleCount - 1))];
    });
  }
  document.getElementById('locationReviewShowAll').textContent = state.inspectAll ? 'Show examples' : 'Inspect all ' + group.count + ' photos';
  document.getElementById('locationReviewSampleLabel').textContent = state.inspectAll
    ? 'Photos ' + (state.photoPage * pageSize + 1) + '–' + Math.min((state.photoPage + 1) * pageSize, group.count) + ' of ' + group.count
    : 'Examples from beginning to end · ' + visible.length + ' of ' + group.count;
  document.getElementById('locationReviewPagePrevious').hidden = !state.inspectAll || state.photoPage === 0;
  document.getElementById('locationReviewPageNext').hidden = !state.inspectAll || (state.photoPage + 1) * pageSize >= group.count;
  var container = document.getElementById('locationReviewThumbnails');
  container.innerHTML = visible.map(function(photo) {
    var index = group.photo_ids.indexOf(photo.id);
    return '<div class="location-review-photo-card"><button class="location-review-thumb" type="button" data-photo-id="' + photo.id + '" title="Open ' + escapeAttr(photo.filename) + '">' +
      '<img src="/thumbnails/' + photo.id + '.jpg" alt="Preview of ' + escapeAttr(photo.filename) + '" loading="lazy">' +
      '<span>' + escapeHtml(photo.filename) + '</span><span>' + escapeHtml(formatDateTime(photo.timestamp)) + '</span></button>' +
      (group.count > 1 ? '<div class="location-review-photo-actions">' +
        (index ? '<button class="location-review-button" type="button" data-split-before="' + photo.id + '">Split before this</button>' : '') +
        '<button class="location-review-button" type="button" data-review-separately="' + photo.id + '">Review separately</button></div>' : '') + '</div>';
  }).join('');
  container.querySelectorAll('[data-photo-id]').forEach(function(button) {
    button.addEventListener('click', function() {
      openPhotoPreview(group.photos.find(function(photo) { return photo.id === Number(button.dataset.photoId); }));
    });
  });
  container.querySelectorAll('[data-split-before], [data-review-separately]').forEach(function(button) {
    button.addEventListener('click', function() {
      if (state.isAssigning || hasPartialAssignmentProgress()) return;
      var split = !!button.dataset.splitBefore;
      var id = Number(button.dataset.splitBefore || button.dataset.reviewSeparately);
      var index = group.photo_ids.indexOf(id);
      if (index < 0 || group.count < 2 || (split && index === 0)) return;
      var moved = group.photos.splice(index, split ? group.count - index : 1);
      var next = Object.assign({}, group, {id: group.id + '-split-' + id, photos: moved});
      [group, next].forEach(function(part) {
        part.photo_ids = part.photos.map(function(photo) { return photo.id; });
        part.count = part.photos.length;
        refreshGroupMetadata(part);
      });
      if (split) state.groups.splice(state.currentIndex + 1, 0, next);
      else state.groups.push(next);
      state.initialGroupCount += 1;
      renderCurrentGroup();
      showToast(split ? 'Group split. The remaining photos are next in the queue.' : 'Photo moved to its own group at the end of the queue.');
    });
  });
}

function lightboxIsOpen() {
  var overlay = document.getElementById('lightboxOverlay');
  return !!(overlay && overlay.classList.contains('active'));
}
