// The native menu's Verify Models command (vireo-page-hooks.js).
Vireo.registerPageHooks({verifyAllModels: verifyAllModels});

loadModels();
loadTaxonomy();
loadSystemInfo();
loadMcpConnectInfo();
loadConfig();
loadWsOverrides();
loadScanRoots();
loadVersion();
loadThemePicker();
loadAllSettings();
loadComputationCacheStatus();
loadLocationWriteStatus();

setupSettingsFind();
setupSettingsFolderBrowser();

if (location.hash === '#nas-setup') {
  // Deep link from the Import page's "Move to NAS unavailable" hint.
  setTimeout(openNasWizard, 0);
}

loadLabels();
loadTaxonGroups();
loadObservationFilters();

setupSettingsNativeFilePickers();
