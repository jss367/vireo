// Initial configuration, form wiring, workspace defaults, and deep-link dispatch.
// Classic page script; load boot.js after all definitions.

async function initImportPage() {
  // Install destination-structure invalidation before the first await —
  // the form controls it listens on are all static template markup, so
  // they're already in the DOM. Wiring after the /api/volumes,
  // /api/config, and /api/workspaces/active fetches would leave a gap
  // where a user can render a preview and then edit the destination,
  // template, or file-type controls without the rendered structure
  // being invalidated.
  wireDestStructureInvalidation();
  wireImportSelectAll();
  // Remote targets are independent of import readiness.  In a large catalog
  // the metadata-repair count below can take a long time, and starting the
  // target request afterward used to leave the destination picker with only
  // its static local-path option for the entire scan.
  loadImportRemoteTargets();
  // Metadata readiness is advisory startup work: the import endpoint still
  // verifies ExifTool when a job starts, and loadImportReadiness() owns the
  // eventual form-gate update.  Do not hold snapshot deep links (or the rest
  // of the wizard) behind a catalog-wide repair count that can take minutes.
  loadImportReadiness();
  // Native menu deep link: File > Import Folder... routes here as
  // /import?mode=copy&pick=source so it lands in Copy-to-archive with the
  // source picker open, while File > Import Photos... gets the page
  // default (Add in place).
  const menuParams = new URLSearchParams(window.location.search);
  if (menuParams.get('mode') === 'copy') {
    const copyRadio = document.getElementById('modeCopy');
    if (copyRadio) copyRadio.checked = true;
  }
  // Volume suggestions for the source input.
  try {
    const resp = await fetch('/api/volumes');
    if (resp.ok) {
      const volumes = await resp.json();
      const dl = document.getElementById('volumeDatalist');
      (volumes || []).forEach((v) => {
        const opt = document.createElement('option');
        opt.value = v.path || v;
        dl.appendChild(opt);
      });
    }
  } catch (e) { /* suggestions only */ }
  // Effective config = global config with the active workspace's
  // config_overrides merged over it (templates are Jinja-free by
  // convention, so the merge happens here, matching
  // db.get_effective_config's pipeline/ingest sub-dict behavior).
  let cfg = {};
  let overrides = {};
  let cfgOk = false;
  let overridesOk = false;
  try {
    const resp = await fetch('/api/config');
    if (resp.ok) { cfg = await resp.json(); cfgOk = true; }
  } catch (e) { /* defaults below */ }
  try {
    const resp = await fetch('/api/workspaces/active');
    if (resp.ok) {
      const ws = await resp.json();
      const activeName = document.getElementById('activeWorkspaceName');
      if (activeName && ws.name) activeName.textContent = '(' + ws.name + ')';
      const raw = ws.config_overrides;
      overrides = typeof raw === 'string' ? JSON.parse(raw) : (raw || {});
      overridesOk = true;
    }
  } catch (e) { /* defaults below */ }
  const ingestCfg = Object.assign(
    {}, cfg.ingest || {}, (overrides || {}).ingest || {},
  );
  const pipelineCfg = Object.assign(
    {}, cfg.pipeline || {}, (overrides || {}).pipeline || {},
  );
  const gpsCheckbox = document.getElementById('chkLocationFromGps');
  const gpsHint = document.getElementById('locationGpsHint');
  if (cfgOk && !String(cfg.google_maps_api_key || '').trim()) {
    gpsCheckbox.disabled = true;
    gpsHint.textContent =
      'Add a Google Maps API key in Settings to resolve GPS coordinates into location tags.';
  }

  const recents = Array.isArray(ingestCfg.recent_destinations)
    ? ingestCfg.recent_destinations.filter((destination) => typeof destination === 'string')
    : [];
  const dl = document.getElementById('recentDestinationOptions');
  recents.forEach((d) => {
    const opt = document.createElement('option');
    opt.value = d;
    dl.appendChild(opt);
  });
  renderRecentDestinations(recents);
  if (typeof ingestCfg.folder_template === 'string') {
    setFolderTemplate(ingestCfg.folder_template);
  }

  // Populate the dropdown from the saved-process library before resolving
  // any default id into a selected option.
  await loadAfterImportProcesses();

  // Preselect the workspace's default process (null -> import only).
  const sel = document.getElementById('afterImportSelect');
  const def = pipelineCfg.default_process_id;
  sel.value = (def != null) ? String(def) : '__none__';
  if (sel.selectedIndex === -1) sel.value = '__none__';
  // Capture the unscoped global default so the "New workspace default"
  // placeholder can name the process the server will actually apply
  // to a freshly-created workspace (which starts with no overrides).
  _globalDefaultProcessId =
    (cfg && cfg.pipeline && cfg.pipeline.default_process_id != null)
      ? cfg.pipeline.default_process_id : null;
  // Capture the effective (workspace-scoped) default so switching out of
  // "New workspace" mode without touching the dropdown restores this
  // value rather than sticking on the placeholder.
  _workspaceDefaultProcessId = (def != null) ? def : null;
  // updateAdvancedImportOptions() may flip _afterImportHidingDefault
  // when the resolved default is an advanced-only strategy the current
  // mode hides. Wire the change listener first so a user-driven change
  // afterwards (advanced toggle keeps it, explicit dropdown pick clears
  // it) is always tracked.
  sel.addEventListener('change', () => {
    _afterImportHidingDefault = false;
    _afterImportUserTouched = true;
    // Once the user actively picks a real option, the "Workspace
    // default (hidden)" placeholder is no longer relevant — retire it
    // so it doesn't clutter the dropdown on the next open.
    if (sel.value !== '__hidden_default__') retireHiddenDefaultOption(sel);
    updateAfterMoveUI();
  });
  _afterImportHidingDefault = false;
  // Only mark the resolved default as trustworthy when BOTH config
  // fetches actually succeeded. Otherwise the select value reflects the
  // HTML placeholder rather than the workspace default, and startImport
  // should omit the key so the server applies pipeline.default_process_id.
  // Set this BEFORE the initial updateAdvancedImportOptions() call so
  // that call can use _workspaceDefaultProcessId to derive the visible
  // dropdown value.
  _afterImportConfigLoaded = cfgOk && overridesOk;
  updateAdvancedImportOptions();
  loadOrphanedStaging();
  updateImportMode();
  const newImagesDeepLink = menuParams.get('new_images');
  if (newImagesDeepLink) {
    await initNewImagesImport(newImagesDeepLink);
  } else if (menuParams.get('pick') === 'source') {
    // One-shot trigger: strip it from the URL so a manual reload doesn't
    // reopen the picker, then open it (native dialog in Tauri, in-page
    // folder browser otherwise).
    menuParams.delete('pick');
    const qs = menuParams.toString();
    history.replaceState(
      null, '', window.location.pathname + (qs ? '?' + qs : ''));
    browseForSource();
  }
}
