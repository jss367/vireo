// Page startup.
// Classic page script; loads after every other Photo Editor definition.

// The shared navbar's undo/redo and native Export command (vireo-page-hooks.js).
Vireo.registerPageHooks({
  beforeHistoryChange: beforeHistoryChange,
  afterHistoryChange: afterHistoryChange,
  openExportModal: openExportModal,
  // The export modal's shared controllers.
  applyExportPreset: applyExportPreset,
  markExportCustom: markExportCustom,
  updateExportFormatControls: updateExportFormatControls,
  updateExportPreview: updateExportPreview,
  selectedExportMetadataFields: selectedExportMetadataFields,
  buildExportPreflightRequest: buildExportPreflightRequest,
});

// Preserve setup order: edit-history busy listener, export folder browser and
// controls, then the editor itself once the DOM is ready.
bindEditHistoryBusyEvents();
const exportFolderBrowser = createExportFolderBrowser();
bindExportControls();
document.addEventListener('DOMContentLoaded', initEditor);
