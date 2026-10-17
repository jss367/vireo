// Page startup.
// Classic page script; loads after every other Keywords page definition.

// Preserve the inline script's order: the listeners it attached at load, then
// the first load (and the ?link_place= deep link it answers).
bindKeywordRecordToggles();
bindLocationReview();
bindKeywordSearch();
bindKeywordFilterButtons();
bindKeywordSorting();
bindKeywordSelection();
bindMergeFieldDelegates();
bindMergeDialog();
bindKeywordTypeBadges();
document.addEventListener('click', closeDropdown);
bindKeywordRename();
bindKeywordBulkApply();
/** @type {HTMLButtonElement} */ (document.getElementById('kwBulkDelete')).addEventListener('click', deleteSelectedKeywords);
bindKeywordContextMenu();
bindLinkPlaceControls();

// -- Init --
loadKeywords().then(() => openLinkPlaceFromUrl(allKeywords, openLinkPlaceModal));
