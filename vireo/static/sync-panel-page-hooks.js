/* Sync panel: the native menu's Sync Metadata command (vireo-page-hooks.js).
 * runSync lives in _sync_panel.html's inline script, which may only shrink
 * (test_inline_page_scripts.py), so it is registered from this classic script,
 * loaded right after that one. */
Vireo.registerPageHooks({runSync: runSync});
