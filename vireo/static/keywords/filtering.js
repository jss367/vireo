// Filtering and sorting the loaded list: getFiltered, the unlinked-location
// chip count, isActionableUnlinkedLocation, and sortList.
// Classic page script; load boot.js after all definitions.

function getFiltered() {
  let list = showRecords ? allKeywords : identityGroups;
  if (filterType === 'unlinked-location') {
    list = list.filter(k => isActionableUnlinkedLocation(k));
  } else if (filterType !== 'all') {
    list = list.filter(k => (k.type || 'general') === filterType);
  }
  if (searchTerm) {
    const options = VireoTextSearch.readOptions('kwSearch');
    list = list.filter(k => VireoTextSearch.matchesFields(
      [k.name].concat((k.paths || (k.path ? [k.path] : [])).map(path => path.join(' → '))).join('\n'), searchTerm, options));
  }
  return list;
}

function unlinkedLocationCount() {
  return allKeywords.reduce(function(n, k) {
    return n + (isActionableUnlinkedLocation(k) ? 1 : 0);
  }, 0);
}

function isActionableUnlinkedLocation(k) {
  return (
    (k.type || 'general') === 'location' &&
    !k.place_id &&
    (k.direct_photo_count || 0) > 0
  );
}

function sortList(list) {
  return list.slice().sort((a, b) => {
    let va, vb;
    switch(sortCol) {
      case 'name': va = (a.name||'').toLowerCase(); vb = (b.name||'').toLowerCase(); break;
      case 'type': va = a.type||'general'; vb = b.type||'general'; break;
      case 'parent':
        va = (a.parent_id ? (parentMap[a.parent_id]||'').toLowerCase() : '');
        vb = (b.parent_id ? (parentMap[b.parent_id]||'').toLowerCase() : '');
        break;
      case 'taxon':
        va = (a.taxon_common_name || a.taxon_name || '').toLowerCase();
        vb = (b.taxon_common_name || b.taxon_name || '').toLowerCase();
        break;
      case 'photo_count': va = a.photo_count||0; vb = b.photo_count||0; break;
      case 'coords':
        va = (a.latitude != null ? 1 : 0); vb = (b.latitude != null ? 1 : 0);
        break;
      default: va = ''; vb = '';
    }
    if (va < vb) return -1 * sortDir;
    if (va > vb) return 1 * sortDir;
    return 0;
  });
}
