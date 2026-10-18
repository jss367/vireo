// All Pipeline Review state and functions are loaded before startup.
// Keep registration and startup effects in their original order.

loadPipelineReviewTuningDefaults();

// The shared navbar and lightbox call these through Vireo.pageHook (see
// vireo-page-hooks.js); the register* calls below add the rest.
Vireo.registerPageHooks({
  getActiveSelection: getActiveSelection,
  getGroupModalSelection: grmNativeMenuSelection,
  batchSetRating: batchSetRating,
  batchSetFlag: batchSetFlag,
  setRatingFor: setRatingFor,
  setFlagFor: setPipelineReviewFlag,
  revealPhoto: revealPhoto,
  copyPhotoPaths: copyPhotoPaths,
  batchAddKeyword: batchAddKeyword,
  addToCollection: addToCollection,
  openPipelineLightbox: openPipelineLightbox,
});
registerPipelineReviewScopeHook();
bindPipelineReviewSpeciesDropdown();
registerPipelineReviewHistoryHooks();
restoreGroupReviewThumbSize();
bindGroupReviewResize();
bindGroupReviewKeyboard();
Vireo.registerPageHooks({
  getLightboxOpenOptions: pipelineReviewLightboxOptions,
  setWildlifeExcludedFor: setWildlifeExcludedFor,
});
registerPipelineReviewLightboxBrowseHook();
bindPipelineReviewOrganizationKeyboard();
bindPipelineReviewContextMenu();
bindPipelineReviewPhotoEvents();

initPipelineReviewPage();
