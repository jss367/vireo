/* Duplicates: the native menu's Find Duplicates command (vireo-page-hooks.js).
 * startScan lives in duplicates.html's inline script, which may only shrink
 * (test_inline_page_scripts.py), so it is registered from this classic script,
 * loaded right after that one. */
Vireo.registerPageHooks({startScan: startScan});
