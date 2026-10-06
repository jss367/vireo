// Page startup.
// Classic page script; loads after every other Photo Editor definition.

// Preserve setup order: edit-history busy listener, export folder browser and
// controls, then the editor itself once the DOM is ready.
bindEditHistoryBusyEvents();
const exportFolderBrowser = createExportFolderBrowser();
bindExportControls();
document.addEventListener('DOMContentLoaded', initEditor);
