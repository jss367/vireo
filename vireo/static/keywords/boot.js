// Page startup: the module the template loads. Binds every listener in the
// old inline script's order, then the first load and the ?link_place= deep
// link.

import { state } from './state.js';
import { bindKeywordBulkApply, deleteSelectedKeywords } from './bulk.js';
import { bindKeywordContextMenu } from './context-menu.js';
import {
  bindKeywordFilterButtons, bindKeywordRecordToggles, bindKeywordSearch, bindKeywordSorting,
} from './filters.js';
import { bindLinkPlaceControls, openLinkPlaceModal } from './link-place.js';
import { loadKeywords } from './loading.js';
import { bindLocationReview } from './location-review.js';
import { bindMergeDialog } from './merge-dialog.js';
import { bindMergeFieldDelegates } from './merge-fields.js';
import { mergeDialog } from './merge-state.js';
import { bindKeywordRename } from './rename.js';
import { render } from './render.js';
import { bindKeywordSelection } from './selection.js';
import { bindKeywordTypeBadges, closeDropdown } from './type-dropdown.js';
import { openExternalLink, openLinkPlaceFromUrl, VireoTextSearch } from './shared.js';

// Preserve the inline script's order: the listeners it attached at load, then
// the first load (and the ?link_place= deep link it answers).
bindKeywordRecordToggles();
bindLocationReview();
bindKeywordSearch();
// The search options used to carry inline onclick handlers.
/** @type {HTMLButtonElement} */ (document.getElementById('kwSearchMatchCaseBtn')).addEventListener('click', function() {
  VireoTextSearch.toggle('kwSearch', 'match_case', render);
});
/** @type {HTMLButtonElement} */ (document.getElementById('kwSearchWholeWordBtn')).addEventListener('click', function() {
  VireoTextSearch.toggle('kwSearch', 'whole_word', render);
});
bindKeywordFilterButtons();
bindKeywordSorting();
bindKeywordSelection();
bindMergeFieldDelegates();
bindMergeDialog();
// The merge dialog's OpenStreetMap links (merge-helpers.js) open externally.
mergeDialog.addEventListener('click', function(event) {
  const link = event.target instanceof Element && event.target.closest('a[data-external-link]');
  if (link) openExternalLink(event, /** @type {HTMLAnchorElement} */ (link).href);
});
bindKeywordTypeBadges();
document.addEventListener('click', closeDropdown);
bindKeywordRename();
bindKeywordBulkApply();
/** @type {HTMLButtonElement} */ (document.getElementById('kwBulkDelete')).addEventListener('click', deleteSelectedKeywords);
bindKeywordContextMenu();
bindLinkPlaceControls();

// -- Init --
loadKeywords().then(() => openLinkPlaceFromUrl(state.allKeywords, openLinkPlaceModal));
