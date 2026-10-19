// Rendering the review: change-type labels and styles, grouping changes,
// and the dialog's filters, summary, groups and photos.
// Classic script.

/** @param {string} type */
function _syncTypeLabel(type) {
  return /** @type {Record<string, string>} */ ({
    keyword_add: 'Keyword additions',
    keyword_merge: 'Keyword merges',
    keyword_remove: 'Keyword removals',
    keyword_remove_flat: 'Keyword removals',
    rating: 'Ratings',
    flag: 'Flags',
    location: 'Locations',
    edit_recipe: 'Photo edits'
  })[type] || type.replace(/_/g, ' ');
}

/** @param {{type: string}} change */
function _syncChangeStyle(change) {
  if (change.type === 'keyword_add') return { icon: '+', color: 'var(--accent)' };
  if (change.type === 'keyword_remove' || change.type === 'keyword_remove_flat') {
    return { icon: '\u2212', color: 'var(--danger)' };
  }
  return { icon: '~', color: 'var(--warning)' };
}

/**
 * @param {SyncChange} change @param {SyncPhoto | null} photo @param {Set<string> | null} activeFilters
 * @returns {SyncPresentation}
 */
function _syncChangePresentation(change, photo, activeFilters) {
  if (change.rating_requires_sidecar && photo) {
    var sidecarWillExist = photo.changes.some(function(other) {
      return other.id !== change.id &&
        other.creates_xmp_sidecar &&
        (!activeFilters || activeFilters.has(other.type));
    });
    if (sidecarWillExist && change.presentation_with_sidecar) {
      return change.presentation_with_sidecar;
    }
    if (!sidecarWillExist && change.presentation_without_sidecar) {
      return change.presentation_without_sidecar;
    }
  }
  if (change.presentation) return change.presentation;
  return {
    field: change.type.replace(/_/g, ' '),
    action: 'updated',
    before: 'Current XMP value',
    after: change.value || 'Cleared'
  };
}

/** @param {SyncPreviewData} data @param {Set<string>} activeFilters */
function _syncGroupedChanges(data, activeFilters) {
  /** @type {SyncChangeGroup[]} */
  var groups = [];
  /** @type {Record<string, SyncChangeGroup>} */
  var groupsByKey = {};
  data.photos.forEach(function(photo) {
    photo.changes.forEach(function(change) {
      if (!activeFilters.has(change.type)) return;
      var presentation = _syncChangePresentation(change, photo, activeFilters);
      var key = JSON.stringify([
        change.type,
        presentation.field,
        presentation.action,
        presentation.before,
        presentation.after,
        presentation.before_detail || '',
        presentation.after_detail || ''
      ]);
      var group = groupsByKey[key];
      if (!group) {
        group = {
          type: change.type,
          presentation: presentation,
          entries: []
        };
        groupsByKey[key] = group;
        groups.push(group);
      }
      group.entries.push({ photo: photo, change: change });
    });
  });
  return groups;
}

/** @param {SyncChangeGroup} group @param {boolean} partial */
function _syncGroupTitle(group, partial) {
  var count = group.entries.length;
  var photoWord = count === 1 ? 'photo' : 'photos';
  var presentation = group.presentation;
  var connector = /** @type {Record<string, string>} */ ({
    added: 'added to',
    removed: 'removed from',
    updated: 'updated on',
    cleared: 'cleared from',
    checked: 'checked on',
    unchanged: 'unchanged on'
  })[presentation.action] || presentation.action + ' on';
  return presentation.field + ' ' + connector + ' ' + count + ' ' + photoWord +
    (partial ? ' reviewed so far' : '');
}

/** The change types present: from the summary counts, else from the loaded photos. @param {SyncPreviewData} data */
function _syncPresentTypes(data) {
  /** @type {Record<string, boolean>} */
  var allTypes = {};
  Object.keys(data.change_type_counts || {}).forEach(function(type) {
    allTypes[type] = true;
  });
  if (Object.keys(allTypes).length === 0) {
    data.photos.forEach(function(photo) {
      photo.changes.forEach(function(change) { allTypes[change.type] = true; });
    });
  }
  return allTypes;
}

/** The Include: checkbox per change type. @param {string[]} typeList @param {Set<string>} filters */
function _syncFilterBarHtml(typeList, filters) {
  var html = '<div style="display:flex;gap:12px;align-items:center;margin-bottom:12px;flex-wrap:wrap;">';
  html += '<span style="font-size:12px;color:var(--text-dim);">Include:</span>';
  typeList.forEach(function(type) {
    var style = _syncChangeStyle({ type: type });
    var checked = filters.has(type) ? ' checked' : '';
    html += '<label style="display:flex;align-items:center;gap:4px;font-size:12px;color:' + style.color + ';cursor:pointer;">' +
      '<input type="checkbox"' + checked + ' onchange="toggleSyncFilter(\'' + type + '\')" style="accent-color:' + style.color + ';"> ' +
      escapeHtml(_syncTypeLabel(type)) + '</label>';
  });
  return html + '</div>';
}

/** The visible counts and the Discard All / Sync to XMP buttons. @param {SyncPreviewData} data @param {Set<string>} filters @param {string} actionsDisabled */
function _syncSummaryRowHtml(data, filters, actionsDisabled) {
  var visibleCount = 0;
  var visiblePhotos = 0;
  data.photos.forEach(function(photo) {
    var visible = photo.changes.filter(function(change) {
      return filters.has(change.type);
    });
    if (visible.length > 0) visiblePhotos++;
    visibleCount += visible.length;
  });

  var changeWord = visibleCount === 1 ? 'change' : 'changes';
  var photoWord = visiblePhotos === 1 ? 'photo' : 'photos';
  var reviewedSuffix = _syncPreviewLoading ? ' reviewed so far' : '';
  return '<div style="display:flex;gap:8px;margin-bottom:12px;align-items:center;">' +
    '<span style="font-size:12px;color:var(--text-dim);flex:1;">' + visibleCount.toLocaleString() + ' ' + changeWord + ' across ' + visiblePhotos.toLocaleString() + ' ' + photoWord + reviewedSuffix + '</span>' +
    '<button class="sync-preview-action" onclick="discardAllChanges()"' + actionsDisabled + ' style="background:none;color:var(--danger);border:1px solid var(--danger);border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;">Discard All</button>' +
    '<button class="sync-preview-action" onclick="runVisibleSync()"' + actionsDisabled + ' style="background:var(--accent);color:var(--accent-text);border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:500;">Sync to XMP</button>' +
  '</div>';
}

/** What a location change writes, given the Settings toggles. @param {SyncPreviewData} data */
function _syncLocationNoteHtml(data) {
  var locationNotes = [];
  locationNotes.push(data.location_sync_enabled
    ? 'A location keyword with coordinates is written to XMP as GPS metadata.'
    : 'Writing assigned GPS to XMP is turned off in Settings; sync only removes GPS previously written by Vireo.');
  locationNotes.push(data.location_keyword_sync_enabled
    ? 'The place and its parent places are written as XMP keywords (the place in dc:subject, the full path in lr:hierarchicalSubject), so a catalog app reading the sidecar lists it as a keyword.'
    : 'Writing location keywords to XMP is turned off in Settings, so place names stay in Vireo; sync only removes location keywords Vireo wrote earlier.');
  return '<div class="sync-review-note"><strong style="color:var(--text-primary);">About location sync:</strong> '
    + locationNotes.join(' ') + '</div>';
}

/** One photo of a group, with its per-change discard. @param {SyncGroupEntry} entry @param {string} actionsDisabled */
function _syncGroupPhotoHtml(entry, actionsDisabled) {
  var photo = entry.photo;
  var change = entry.change;
  var thumbUrl = window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(photo)
    : '/thumbnails/' + photo.photo_id + '.jpg';
  var html = '<div class="sync-preview-photo" style="width:' + _syncThumbSize + 'px;">';
  html += '<img class="sync-preview-thumb" src="' + escapeAttr(thumbUrl) + '" loading="lazy" decoding="async" style="width:' + _syncThumbSize + 'px;height:' + Math.round(_syncThumbSize * 2 / 3) + 'px;object-fit:cover;border-radius:4px;cursor:pointer;display:block;" data-photo-id="' + photo.photo_id + '" data-filename="' + escapeAttr(photo.filename) + '">';
  html += '<div class="sync-preview-filename" title="' + escapeAttr(photo.filename) + '">' + escapeHtml(photo.filename) + '</div>';
  html += '<button class="sync-preview-discard sync-preview-action" onclick="event.stopPropagation();discardChange(' + change.id + ')"' + actionsDisabled + ' title="Discard this change" aria-label="Discard change for ' + escapeAttr(photo.filename) + '">&times;</button>';
  return html + '</div>';
}

/** A group's header (before/after and Discard group) and its first displayed photos. @param {SyncChangeGroup} group @param {SyncGroupEntry[]} displayedEntries @param {string} actionsDisabled */
function _syncGroupHtml(group, displayedEntries, actionsDisabled) {
  var presentation = group.presentation;
  var style = _syncChangeStyle({ type: group.type });
  var ids = group.entries.map(function(entry) { return entry.change.id; });
  var html = '<section class="sync-review-group">';
  html += '<div class="sync-review-group-header">';
  html += '<span style="color:' + style.color + ';font-weight:700;font-size:17px;width:16px;text-align:center;">' + style.icon + '</span>';
  html += '<div style="flex:1;min-width:0;">';
  html += '<div class="sync-review-group-title">' + escapeHtml(_syncGroupTitle(group, _syncPreviewLoading)) + '</div>';
  html += '<div class="sync-review-delta"><span class="sync-review-before">' + escapeHtml(presentation.before) + '</span><span class="sync-review-arrow">→</span><span class="sync-review-after">' + escapeHtml(presentation.after) + '</span></div>';
  if (presentation.before_detail) {
    html += '<div class="sync-review-detail">Before: ' + escapeHtml(presentation.before_detail) + '</div>';
  }
  if (presentation.after_detail) {
    html += '<div class="sync-review-detail">' + escapeHtml(presentation.after_detail) + '</div>';
  }
  html += '</div>';
  html += '<button class="sync-preview-action sync-review-group-discard" onclick="discardSyncChanges([' + ids.join(',') + '])"' + actionsDisabled + ' title="Discard this group">Discard group</button>';
  html += '</div>';
  html += '<div class="sync-review-photos">';
  displayedEntries.forEach(function(entry) { html += _syncGroupPhotoHtml(entry, actionsDisabled); });
  return html + '</div></section>';
}

/** Every group, up to the display limit, then a Show more button for the rest. @param {SyncPreviewData} data @param {Set<string>} filters @param {string} actionsDisabled */
function _syncGroupsHtml(data, filters, actionsDisabled) {
  var groups = _syncGroupedChanges(data, filters);
  var html = '<div class="sync-review-groups">';
  var displayRemaining = _syncPreviewDisplayLimit;
  var hiddenEntries = 0;
  groups.forEach(function(group) {
    var displayedEntries = group.entries.slice(0, Math.max(0, displayRemaining));
    displayRemaining -= displayedEntries.length;
    hiddenEntries += group.entries.length - displayedEntries.length;
    if (displayedEntries.length === 0) return;
    html += _syncGroupHtml(group, displayedEntries, actionsDisabled);
  });
  html += '</div>';
  if (hiddenEntries > 0) {
    html += '<div class="sync-preview-more">Showing the first ' +
      (_syncPreviewDisplayLimit - displayRemaining).toLocaleString() + ' matching results to keep this review responsive. ' +
      '<button onclick="showMoreSyncPreview()" style="margin-left:8px;padding:4px 10px;border:1px solid var(--border-secondary);border-radius:4px;background:var(--bg-secondary);color:var(--text-primary);cursor:pointer;">Show ' +
      Math.min(300, hiddenEntries).toLocaleString() + ' more</button></div>';
  }
  return html;
}

function renderSyncPreview() {
  var data = _syncPreviewData;
  var content = /** @type {HTMLElement} */ (document.getElementById('syncPreviewContent'));
  if (!data) return;
  if (data.photos.length === 0 && _syncPreviewLoading) {
    content.innerHTML = '<p style="color:var(--text-dim);padding:20px;">Waiting for the first results...</p>';
    return;
  }
  if (data.photos.length === 0) {
    content.innerHTML = '<p style="color:var(--text-dim);padding:20px;">No pending changes.</p>';
    return;
  }

  var allTypes = _syncPresentTypes(data);
  var typeList = Object.keys(allTypes).sort();
  if (_syncActiveFilters === null) _syncActiveFilters = new Set(typeList);
  var filters = _syncActiveFilters;
  var actionsDisabled = _syncPreviewComplete ? '' : ' disabled';

  var html = _syncFilterBarHtml(typeList, filters) + _syncSummaryRowHtml(data, filters, actionsDisabled);
  if (filters.has('location') && allTypes.location) html += _syncLocationNoteHtml(data);
  html += _syncGroupsHtml(data, filters, actionsDisabled);

  content.innerHTML = html;
  _syncSetPreviewActionsEnabled(
    _syncPreviewComplete && (data.total_changes || 0) > 0
  );
  if (_syncLightboxPanelEl && typeof vireoLightboxSession !== 'undefined') {
    _syncRenderLightboxPanel(vireoLightboxSession.requestedPhotoId());
  }
}

function showMoreSyncPreview() {
  _syncPreviewDisplayLimit += 300;
  renderSyncPreview();
}
