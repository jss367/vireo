// All Pipeline Review state and functions are loaded before startup.
// Keep registration and startup effects in their original order.

loadPipelineReviewTuningDefaults();

window.setFlagFor = setPipelineReviewFlag;
registerPipelineReviewScopeHook();
bindPipelineReviewSpeciesDropdown();
registerPipelineReviewHistoryHooks();
restoreGroupReviewThumbSize();
bindGroupReviewResize();
bindGroupReviewKeyboard();
window.getLightboxOpenOptions = pipelineReviewLightboxOptions;
window.setWildlifeExcludedFor = setWildlifeExcludedFor;
registerPipelineReviewLightboxBrowseHook();
bindPipelineReviewOrganizationKeyboard();
bindPipelineReviewContextMenu();
bindPipelineReviewPhotoEvents();

initPipelineReviewPage();
