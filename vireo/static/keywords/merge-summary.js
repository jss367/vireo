// renderMergeSummary(): what the merge will do, in prose.
// Classic page script; load boot.js after all definitions.

/** @param {KeywordMergePreview} data */ function renderMergeSummary(data) {
  const resolved = data.resolved;
  /** @param {KeywordMergeChild} child */ const childLine = child => {
    const where = child.to_path ? escapeHtml(child.to_path.join(' → ')) : '';
    if (child.outcome === 'merge') {
      return escapeHtml(child.name) + ' — merges into the keyword already at ' + where;
    }
    if (child.outcome === 'rename') {
      return escapeHtml(child.name) + ' — kept separately as ' + where +
        ' (a different keyword already uses that name there)';
    }
    return escapeHtml(child.name) + ' — moves to ' + where;
  };
  let html = '<div class="kw-merge-result">' +
    '<p><strong>' + escapeHtml(resolved.path.join(' → ')) + '</strong> · ' +
    escapeHtml(resolved.type) + '</p>' +
    '<p>' + data.combined_count + ' distinct photo' +
    (data.combined_count === 1 ? '' : 's') + ' after merging · ' +
    data.removed_count + ' keyword record' + (data.removed_count === 1 ? '' : 's') +
    ' removed</p>';
  if (resolved.place_id) {
    html += '<p>Keeps the Google place link' +
      (resolved.latitude != null && resolved.longitude != null
        ? ' · ' + osmLink(resolved.latitude, resolved.longitude,
            formatCoords(resolved.latitude, resolved.longitude))
        : '') + '</p>';
  } else if (resolved.latitude != null && resolved.longitude != null) {
    html += '<p>Coordinates: ' +
      osmLink(resolved.latitude, resolved.longitude,
              formatCoords(resolved.latitude, resolved.longitude)) + '</p>';
  }
  if (data.children.length) {
    html += '<p>' + (data.children.length === 1
      ? '1 child keyword moves under it:'
      : data.children.length + ' child keywords move under it:') + '</p><ul>' +
      data.children.map(child => '<li>' + childLine(child) +
        (child.photo_count ? ' · ' + child.photo_count + ' photos in this branch' : '') +
        '</li>').join('') + '</ul>';
  }
  (data.notes || []).forEach(note => {
    html += '<p class="kw-merge-note">' + escapeHtml(note) + '</p>';
  });
  html += '<p class="kw-merge-note">Imported names and paths will resolve to the ' +
    'combined keyword on future imports.</p></div>';
  mergePreview.innerHTML = html;
}
