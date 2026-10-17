// render(): the keyword table, its empty state, stats and the unlinked count.
// Classic page script; load boot.js after all definitions.

function render() {
  const filtered = sortList(getFiltered());
  const tbody = /** @type {HTMLElement} */ (document.getElementById('kwBody'));
  const empty = /** @type {HTMLElement} */ (document.getElementById('kwEmpty'));
  const stats = /** @type {HTMLElement} */ (document.getElementById('kwStats'));

  stats.textContent = filtered.length + ' of ' + (showRecords ? allKeywords.length : identityGroups.length) + ' keywords';
  const unlinkedEl = /** @type {HTMLElement} */ (document.getElementById('kwUnlinkedCount'));
  if (unlinkedEl) unlinkedEl.textContent = String(unlinkedLocationCount());

  if (!filtered.length) {
    tbody.innerHTML = '';
    empty.style.display = 'block';
    empty.textContent = searchTerm || filterType !== 'all' ? 'No matching keywords.' : 'No keywords found.';
    updateBulkBar();
    updateSelectAllCheckbox();
    return;
  }
  empty.style.display = 'none';

  const rows = filtered.map(k => {
    const t = k.type || 'general';
    const grouped = !showRecords && k.members && k.members.length > 1;
    const icon = TYPE_ICONS[t] || TYPE_ICONS.general;
    const checked = selectedIds.has(k.id) ? 'checked' : '';
    const parentName = k.parent_id ? (parentMap[k.parent_id] || '#' + k.parent_id) : '';
    const taxonStr = k.taxon_common_name ? (k.taxon_common_name + (k.taxon_name ? ' (' + k.taxon_name + ')' : ''))
                     : (k.taxon_name || '');
    const coordStr = (k.latitude != null && k.longitude != null)
                     ? k.latitude.toFixed(4) + ', ' + k.longitude.toFixed(4) : '';

    // Per-row Link cell:
    //   * linked location                 -> 📍 linked badge
    //   * directly tagged unlinked place  -> 📍 Link... button
    //   * parent-only chain rows          -> empty
    //   * other types                     -> empty
    let linkCell = '';
    if (t === 'location') {
      if (k.place_id) {
        linkCell = '<span class="kw-linked-badge" title="Linked to Google place">📍 linked</span>';
      } else if (isActionableUnlinkedLocation(k)) {
        linkCell = '<button class="kw-link-btn" data-link-id="' + k.id + '" type="button">' +
          '📍 Link…</button>';
      }
    }

    const details = !showRecords && k.paths && k.paths.length > 1
      ? '<details><summary>' + k.paths.length + ' names and paths</summary>' +
        k.paths.map(path => '<div class="kw-parent">' + escapeHtml(path.join(' → ')) + '</div>').join('') +
        '<button type="button" class="kw-link-btn" data-edit-records="1">Edit individual records</button></details>' : '';
    return '<tr data-id="' + k.id + '">' +
      '<td class="cb-cell">' + (grouped ? '' : '<input type="checkbox" class="kw-cb" data-id="' + k.id + '" ' + checked + '>') + '</td>' +
      '<td class="kw-name-cell"><span class="' + (grouped ? '' : 'name-text') + '" data-id="' + k.id + '">' + escapeHtml(k.name) + '</span>' + details + '</td>' +
      '<td><span class="kw-type-badge type-' + t + '" ' + (grouped ? '' : 'data-id="' + k.id + '"') + ' data-type="' + t + '">' + icon + ' ' + t + '</span></td>' +
      '<td><span class="kw-parent">' + escapeHtml(parentName) + '</span></td>' +
      '<td><span class="kw-taxon">' + escapeHtml(taxonStr) + '</span></td>' +
      '<td>' + (k.photo_count || 0) + '</td>' +
      '<td><span class="kw-coords">' + coordStr + '</span></td>' +
      '<td>' + linkCell + '</td>' +
      '</tr>';
  });
  tbody.innerHTML = rows.join('');
  updateBulkBar();
  updateSelectAllCheckbox();
}
