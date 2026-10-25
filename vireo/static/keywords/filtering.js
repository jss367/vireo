// Filtering and sorting the loaded list: getFiltered, the unlinked-location
// chip count, isActionableUnlinkedLocation, and sortList.

import { state } from './state.js';
import { VireoTextSearch } from './shared.js';

export function getFiltered() {
  let list = state.showRecords ? state.allKeywords : state.identityGroups;
  if (state.filterType === 'unlinked-location') {
    list = list.filter(k => isActionableUnlinkedLocation(k));
  } else if (state.filterType !== 'all') {
    list = list.filter(k => (k.type || 'general') === state.filterType);
  }
  if (state.searchTerm) {
    const options = VireoTextSearch.readOptions('kwSearch');
    list = list.filter(k => VireoTextSearch.matchesFields(
      [k.name].concat((k.paths || (k.path ? [k.path] : [])).map(path => path.join(' → '))).join('\n'), state.searchTerm, options));
  }
  return list;
}

export function unlinkedLocationCount() {
  return state.allKeywords.reduce(function(n, k) {
    return n + (isActionableUnlinkedLocation(k) ? 1 : 0);
  }, 0);
}

/** @param {KeywordRecord} k */ export function isActionableUnlinkedLocation(k) {
  return (
    (k.type || 'general') === 'location' &&
    !k.place_id &&
    (k.direct_photo_count || 0) > 0
  );
}

/** @param {KeywordRecord[]} list */ export function sortList(list) {
  return list.slice().sort((a, b) => {
    let va, vb;
    switch(state.sortCol) {
      case 'name': va = (a.name||'').toLowerCase(); vb = (b.name||'').toLowerCase(); break;
      case 'type': va = a.type||'general'; vb = b.type||'general'; break;
      case 'parent':
        va = (a.parent_id ? (state.parentMap[a.parent_id]||'').toLowerCase() : '');
        vb = (b.parent_id ? (state.parentMap[b.parent_id]||'').toLowerCase() : '');
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
    if (va < vb) return -1 * state.sortDir;
    if (va > vb) return 1 * state.sortDir;
    return 0;
  });
}
