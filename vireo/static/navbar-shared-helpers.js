/* ---------- Toast Notifications ---------- */
(function() {
  var container = document.createElement('div');
  container.id = 'toastContainer';
  container.style.cssText = 'position:fixed;top:16px;right:16px;z-index:100000;display:flex;flex-direction:column;gap:8px;pointer-events:none;';
  document.body.appendChild(container);
})();

function showToast(msg, type) {
  var colors = {
    success: 'var(--success)',
    info: 'var(--info)',
    warning: 'var(--warning)',
    error: 'var(--danger)'
  };
  var textColors = {
    success: 'var(--success-text)',
    info: 'var(--info-text)',
    warning: 'var(--warning-text)',
    error: 'var(--danger-text)'
  };
  // A missing or unknown type is neutral information. Callers that need an
  // assertive error must opt into it instead of making routine confirmations
  // look like failures.
  type = Object.prototype.hasOwnProperty.call(colors, type) ? type : 'info';
  var container = document.getElementById('toastContainer');
  if (!container) return;
  var toast = document.createElement('div');
  var bg = colors[type];
  var color = textColors[type];
  toast.dataset.type = type;
  toast.setAttribute('role', type === 'error' ? 'alert' : 'status');
  toast.setAttribute('aria-live', type === 'error' ? 'assertive' : 'polite');
  toast.style.cssText = 'pointer-events:auto;padding:10px 16px;border-radius:6px;font-size:14px;max-width:400px;word-break:break-word;background:' + bg + ';color:' + color + ';box-shadow:0 2px 8px rgba(0,0,0,0.3);opacity:0;transition:opacity 0.2s;';
  toast.textContent = msg;
  container.appendChild(toast);
  requestAnimationFrame(function() { toast.style.opacity = '1'; });
  setTimeout(function() {
    toast.style.opacity = '0';
    setTimeout(function() { toast.remove(); }, 200);
  }, 5000);
}

/* ---------- External links ---------- */
// Click handler for external (http/https) links. Inside the desktop webview a
// plain `target="_blank"` anchor does nothing, so route the open through the
// OS browser via openExternal (tauri-bridge.js) and prevent the dead in-app
// navigation. Use on <a> tags as: onclick="return openExternalLink(event, this.href)".
function openExternalLink(event, url) {
  if (event) event.preventDefault();
  if (typeof openExternalWithRecovery !== 'function') {
    showToast('Could not open link: ' + url, 'error');
    return false;
  }
  openExternalWithRecovery(url);
  return false;
}

/* ---------- Safe Fetch ---------- */
async function safeFetch(url, opts, options) {
  return window.Vireo.api.json(url, opts, options);
}

/* ---------- Native Menu Commands ----------
 * The Tauri menu bar dispatches command IDs here. Keep this layer thin:
 * call the same page helpers, modals, and API endpoints used by visible UI.
 */
function nativeMenuInfo(msg) {
  if (typeof showToast === 'function') showToast(msg, 'info');
  else console.info(msg);
}

function nativeMenuError(msg) {
  if (typeof showToast === 'function') showToast(msg, 'error');
  else console.error(msg);
}

function revealFeedbackMessage(data) {
  if (data && data.ok === false) {
    return 'Reveal failed: ' + (data.reason || 'unknown error');
  }
  return 'Revealed in ' + (window.VIREO_FILE_MANAGER_NAME || 'file manager');
}

function showRevealFeedback(data) {
  var isError = data && data.ok === false;
  var msg = revealFeedbackMessage(data);
  if (typeof showToast === 'function') showToast(msg, isError ? 'error' : 'success');
  else if (isError) console.error(msg);
  else console.info(msg);
}

function nativeMenuRoute(path) {
  window.location.href = window.vireoResolveNavigationHref(path);
}

function nativeMenuLightboxOpen() {
  var overlay = document.getElementById('lightboxOverlay');
  return !!(overlay && overlay.classList.contains('active'));
}

function nativeMenuActivePhotoIds() {
  if (nativeMenuLightboxOpen()) {
    // Navigation advances the internal id before the incoming bitmap is
    // visible. Do not let native Photo menu commands target that hidden photo.
    if (typeof _lbVisualTransitionPending !== 'undefined' && _lbVisualTransitionPending) {
      return [];
    }
    if (typeof vireoLightboxSession === 'undefined' || vireoLightboxSession.requestedPhotoId() == null) {
      return [];
    }
    return [vireoLightboxSession.requestedPhotoId()];
  }
  if (
    typeof grmState !== 'undefined' &&
    document.getElementById('grmOverlay') &&
    document.getElementById('grmOverlay').classList.contains('open')
  ) {
    if (grmState.selectedIds && grmState.selectedIds.size) {
      return Array.from(grmState.selectedIds);
    }
    if (grmState.selected != null) return [grmState.selected];
  }
  if (typeof getActiveSelection === 'function') {
    var ids = getActiveSelection();
    if (ids && ids.length) return ids;
  }
  if (typeof selectedPhotoId !== 'undefined' && selectedPhotoId != null) {
    return [selectedPhotoId];
  }
  return [];
}

function nativeMenuRequirePhotos(commandName) {
  var ids = nativeMenuActivePhotoIds();
  if (!ids.length) {
    nativeMenuError(commandName + ' needs a selected photo.');
    return null;
  }
  return ids;
}

async function nativeMenuBatch(endpoint, payload, success) {
  await safeFetch(endpoint, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload || {}),
  });
  if (success) nativeMenuInfo(success);
}

function nativeMenuOpenLightbox() {
  if (typeof openBrowseShortcutPhoto === 'function' && openBrowseShortcutPhoto(false)) return;
  var ids = nativeMenuRequirePhotos('Open in Lightbox');
  if (!ids) return;
  if (typeof window.openPipelineLightbox === 'function') {
    window.openPipelineLightbox(ids[0]);
    return;
  }
  if (typeof openLightbox === 'function' && typeof photos !== 'undefined') {
    var p = photos.find(function(x) { return x.id === ids[0]; });
    if (p) {
      openLightbox(p.id, p.filename, photos);
      return;
    }
  }
  nativeMenuError('Open in Lightbox is available from photo grids.');
}

function nativeMenuOpenBrowse() {
  var ids = nativeMenuRequirePhotos('Open in Browse');
  if (!ids) return;
  if (ids.length !== 1) {
    nativeMenuError('Open in Browse needs one selected photo.');
    return;
  }
  var disabledHint = typeof window.getLightboxBrowseDisabledHint === 'function'
    ? window.getLightboxBrowseDisabledHint(ids[0], true)
    : null;
  if (disabledHint) {
    nativeMenuError(disabledHint);
    return false;
  }
  nativeMenuRoute('/browse?photo_id=' + encodeURIComponent(ids[0]));
  return true;
}

function nativeMenuPhotoIdsForWrite() {
  var ids = nativeMenuActivePhotoIds();
  if (ids.length) return ids;
  nativeMenuError('Select one or more photos first.');
  return null;
}

function nativeMenuSetPhotoIdsOverride(ids) {
  window._vireoNativeMenuPhotoIdsOverride = ids ? ids.slice() : null;
}

function nativeMenuCloseLightboxForModal() {
  if (nativeMenuLightboxOpen() && typeof closeLightbox === 'function') {
    closeLightbox();
  }
}

async function nativeMenuSetRating(rating) {
  var ids = nativeMenuPhotoIdsForWrite();
  if (!ids) return;
  if (ids.length === 1 && nativeMenuLightboxOpen()) {
    if (typeof setRatingFor === 'function') {
      await setRatingFor(ids[0], rating);
      return;
    }
    if (typeof setReviewRating === 'function') {
      setReviewRating(ids[0], rating);
      nativeMenuInfo('Rating updated');
      return;
    }
  }
  if (typeof batchSetRating === 'function') {
    await batchSetRating(rating);
    return;
  }
  if (ids.length === 1 && typeof setReviewRating === 'function') {
    setReviewRating(ids[0], rating);
    nativeMenuInfo('Rating updated');
    return;
  }
  await nativeMenuBatch('/api/batch/rating', {photo_ids: ids, rating: rating}, 'Rating updated');
}

async function nativeMenuSetFlag(flag) {
  var ids = nativeMenuPhotoIdsForWrite();
  if (!ids) return;
  if (ids.length === 1 && nativeMenuLightboxOpen()) {
    if (typeof _lbApplyFlag === 'function' && ids[0] === vireoLightboxSession.requestedPhotoId()) {
      _lbApplyFlag(ids[0], flag);
      return;
    }
    if (typeof setReviewFlag === 'function') {
      await setReviewFlag(ids[0], flag);
      nativeMenuInfo('Flag updated');
      return;
    }
  }
  if (typeof batchSetFlag === 'function') {
    await batchSetFlag(flag);
    return;
  }
  if (ids.length === 1 && typeof setReviewFlag === 'function') {
    await setReviewFlag(ids[0], flag);
    nativeMenuInfo('Flag updated');
    return;
  }
  await nativeMenuBatch('/api/batch/flag', {photo_ids: ids, flag: flag}, 'Flag updated');
}

function nativeMenuRefreshWildlifeExcludedState(ids, excluded) {
  if (typeof photos !== 'undefined' && Array.isArray(photos)) {
    ids.forEach(function(id) {
      var p = photos.find(function(x) { return x.id === id; });
      if (p) p.wildlife_excluded = excluded ? 1 : 0;
    });
    if (typeof refreshGridCards === 'function') refreshGridCards(ids);
    else if (typeof renderGrid === 'function') renderGrid();
    if (typeof selectedPhotoId !== 'undefined' && ids.indexOf(selectedPhotoId) !== -1 && typeof loadDetail === 'function') {
      loadDetail(selectedPhotoId);
    }
  }
}

async function nativeMenuSetWildlifeExcluded(excluded) {
  var ids = nativeMenuPhotoIdsForWrite();
  if (!ids) return;
  var usePageHelper = typeof window.setWildlifeExcludedFor === 'function';
  var results = await Promise.all(ids.map(function(id) {
    if (usePageHelper) {
      return window.setWildlifeExcludedFor(id, excluded);
    }
    return safeFetch('/api/photos/' + id + '/wildlife_excluded', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({excluded: excluded}),
    }, {toast: false});
  }));
  if (usePageHelper && results.some(function(result) { return result === false; })) {
    return false;
  }
  if (!usePageHelper) nativeMenuRefreshWildlifeExcludedState(ids, excluded);
  if (typeof vireoLightboxSession !== 'undefined' && ids.indexOf(vireoLightboxSession.requestedPhotoId()) !== -1) {
    _lbCurrentWildlifeExcluded = excluded;
  }
  nativeMenuInfo(excluded ? 'Excluded from wildlife classification' : 'Marked as wildlife');
}

function nativeMenuPendingPredictions() {
  if (typeof predictions === 'undefined') return [];
  return predictions.filter(function(p) { return p.status === 'pending'; });
}

async function nativeMenuJob(endpoint, body, label) {
  var data = await safeFetch(endpoint, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  });
  var suffix = data && data.job_id ? ' (' + data.job_id + ')' : '';
  nativeMenuInfo(label + suffix);
}

function isLiveJob(job) {
  return !!job && (
    job.status === 'running' ||
    job.status === 'pausing' ||
    job.status === 'paused' ||
    job.status === 'queued' ||
    job.status === 'pending'
  );
}

function countsForBadge(job) {
  return !job || job.counts_for_badge !== false;
}

function isAttentionJob(job) {
  return isLiveJob(job) && countsForBadge(job);
}

function isWaitingJob(job) {
  return !!job && (job.status === 'queued' || job.status === 'pending');
}

async function nativeMenuCancelCurrentJob() {
  var jobs = await safeFetch('/api/jobs', {}, {toast: false});
  var list = Array.isArray(jobs) ? jobs : (jobs.active || jobs.jobs || []);
  // Queued pipelines are cancellable too (runner.cancel_job handles
  // the queued path atomically), so the native menu's "cancel current
  // job" should be able to reach them — otherwise a waiting pipeline
  // is stuck if the user closes the pipeline page.
  //
  // /api/jobs lists every workspace's jobs, oldest first. "Current" means the
  // active workspace's (workspace-agnostic jobs count, as in the bottom
  // panel), and the job actually doing work beats one that is paused or
  // waiting; ties go to the most recently started.
  var wsId = (jobs && !Array.isArray(jobs)) ? jobs.active_workspace_id : null;
  var attention = list.filter(isAttentionJob);
  var inWorkspace = attention.filter(function(j) {
    return wsId == null || j.workspace_id == null || j.workspace_id === wsId;
  });
  if (!inWorkspace.length) {
    var elsewhere = attention.length;
    nativeMenuInfo(elsewhere
      ? 'No active job in this workspace (' + elsewhere + ' in other workspaces; cancel from the Jobs panel)'
      : 'No active job to cancel');
    return;
  }
  function rank(j) {
    if (j.status === 'running' || j.status === 'pausing') return 0;
    if (j.status === 'paused') return 1;
    return 2;
  }
  inWorkspace.sort(function(a, b) {
    var r = rank(a) - rank(b);
    if (r) return r;
    var sa = a.started_at || '', sb = b.started_at || '';
    return sa < sb ? 1 : sa > sb ? -1 : 0;
  });
  var target = inWorkspace[0];
  var name = window.formatJobType ? window.formatJobType(target.type) : (target.type || 'job');
  await safeFetch('/api/jobs/' + target.id + '/cancel', {method: 'POST'});
  nativeMenuInfo('Cancel requested: ' + name);
}

async function nativeMenuCopyDiagnostics() {
  var parts = [];
  try { parts.push({version: await safeFetch('/api/version', {}, {toast: false})}); } catch(e) {}
  try { parts.push({system: await safeFetch('/api/system/info', {}, {toast: false})}); } catch(e) {}
  try { parts.push({jobs: await safeFetch('/api/jobs', {}, {toast: false})}); } catch(e) {}
  try { parts.push({logs: await safeFetch('/api/logs/recent?count=50', {}, {toast: false})}); } catch(e) {}
  var text = JSON.stringify({generated_at: new Date().toISOString(), diagnostics: parts}, null, 2);
  if (!navigator.clipboard) {
    nativeMenuError('Clipboard access is unavailable.');
    return;
  }
  await navigator.clipboard.writeText(text);
  nativeMenuInfo('Diagnostics copied');
}

window.handleNativeMenuCommand = async function(command) {
  try {
    switch (command) {
      case 'new_workspace':
        if (window.vireoWorkspaceSwitcher) vireoWorkspaceSwitcher.showCreate();
        else nativeMenuRoute('/workspace');
        break;
      case 'open_workspace':
        if (window.vireoWorkspaceSwitcher) vireoWorkspaceSwitcher.toggle();
        else nativeMenuRoute('/workspace');
        break;
      case 'import_photos':
        nativeMenuRoute('/import');
        break;
      case 'import_folder':
        // Unlike import_photos (which lands on the Import page default),
        // Import Folder... deep-links into Copy-to-archive with the source
        // folder picker open — the two File-menu commands must stay
        // distinct actions.
        nativeMenuRoute('/import?mode=copy&pick=source');
        break;
      case 'export_selected':
        if (typeof openExportModal === 'function') openExportModal();
        else nativeMenuError('Export is available from Browse after selecting photos.');
        break;
      case 'photo_open_lightbox':
        nativeMenuOpenLightbox();
        break;
      case 'photo_open_browse':
        nativeMenuOpenBrowse();
        break;
      case 'photo_reveal': {
        var revealIds = nativeMenuRequirePhotos(window.VIREO_REVEAL_LABEL);
        if (!revealIds) break;
        if (typeof revealPhoto === 'function') revealPhoto(revealIds[0]);
        else if (typeof revealReviewPhoto === 'function') revealReviewPhoto(revealIds[0]);
        else await nativeMenuBatch('/api/files/reveal', {photo_id: revealIds[0]});
        break;
      }
      case 'photo_open_editor': {
        var editorIds = nativeMenuRequirePhotos('Open in External Editor');
        if (!editorIds) break;
        if (typeof openInEditor === 'function') await openInEditor(editorIds);
        else nativeMenuError('No external editor command is available here.');
        break;
      }
      case 'photo_copy_paths': {
        var copyIds = nativeMenuRequirePhotos('Copy Path');
        if (!copyIds) break;
        if (typeof copyPhotoPaths === 'function') await copyPhotoPaths(copyIds);
        else nativeMenuError('Copy Path is available from Browse.');
        break;
      }
      case 'photo_find_similar': {
        var similarIds = nativeMenuRequirePhotos('Find Similar');
        if (!similarIds) break;
        if (similarIds.length !== 1) nativeMenuError('Find Similar needs one selected photo.');
        else if (typeof findSimilar === 'function') findSimilar(similarIds[0]);
        else nativeMenuError('Find Similar is available from Browse and Lightbox.');
        break;
      }
      case 'photo_compare':
        if (typeof openBrowseCompare === 'function') openBrowseCompare();
        else nativeMenuRoute('/id-conflicts');
        break;
      case 'photo_add_keyword':
        if (typeof batchAddKeyword === 'function') {
          var keywordIds = nativeMenuRequirePhotos('Add Keyword');
          if (!keywordIds) break;
          nativeMenuSetPhotoIdsOverride(keywordIds);
          nativeMenuCloseLightboxForModal();
          batchAddKeyword();
        } else {
          nativeMenuError('Add Keyword is available from Browse after selecting photos.');
        }
        break;
      case 'photo_add_collection':
        if (typeof addToCollection === 'function') {
          var collectionIds = nativeMenuRequirePhotos('Add to Collection');
          if (!collectionIds) break;
          nativeMenuSetPhotoIdsOverride(collectionIds);
          nativeMenuCloseLightboxForModal();
          addToCollection();
        } else {
          nativeMenuError('Add to Collection is available from Browse after selecting photos.');
        }
        break;
      case 'photo_adjust_capture_time':
        if (typeof openCaptureTimeModal === 'function') {
          var captureTimeIds = nativeMenuRequirePhotos('Adjust Capture Time');
          if (!captureTimeIds) break;
          nativeMenuSetPhotoIdsOverride(captureTimeIds);
          nativeMenuCloseLightboxForModal();
          openCaptureTimeModal();
        } else {
          nativeMenuError('Adjust Capture Time is available from Browse after selecting photos.');
        }
        break;
      case 'photo_delete':
        if (nativeMenuLightboxOpen() && typeof lightboxDelete === 'function') {
          lightboxDelete();
        } else if (typeof batchDelete === 'function') {
          batchDelete();
        } else {
          nativeMenuError('Delete is available from Browse or an open Lightbox.');
        }
        break;
      case 'photo_rate_0': await nativeMenuSetRating(0); break;
      case 'photo_rate_1': await nativeMenuSetRating(1); break;
      case 'photo_rate_2': await nativeMenuSetRating(2); break;
      case 'photo_rate_3': await nativeMenuSetRating(3); break;
      case 'photo_rate_4': await nativeMenuSetRating(4); break;
      case 'photo_rate_5': await nativeMenuSetRating(5); break;
      case 'photo_flag_pick': await nativeMenuSetFlag('flagged'); break;
      case 'photo_flag_reject': await nativeMenuSetFlag('rejected'); break;
      case 'photo_flag_clear': await nativeMenuSetFlag('none'); break;
      case 'review_accept': {
        var acceptPending = nativeMenuPendingPredictions();
        if (acceptPending.length && typeof acceptPrediction === 'function') await acceptPrediction(acceptPending[0].id);
        else nativeMenuError('No pending prediction to accept.');
        break;
      }
      case 'review_reject': {
        var rejectPending = nativeMenuPendingPredictions();
        if (rejectPending.length && typeof rejectPrediction === 'function') await rejectPrediction(rejectPending[0].id);
        else nativeMenuError('No pending prediction to reject.');
        break;
      }
      case 'review_accept_all':
        if (typeof acceptAllPending === 'function') await acceptAllPending();
        else nativeMenuRoute('/review');
        break;
      case 'review_previous':
        if (typeof lightboxNav === 'function' && typeof vireoLightboxSession !== 'undefined' && vireoLightboxSession.requestedPhotoId() != null) lightboxNav(-1);
        else if (typeof moveBrowseSelection === 'function') moveBrowseSelection(-1, {});
        break;
      case 'review_next':
        if (typeof lightboxNav === 'function' && typeof vireoLightboxSession !== 'undefined' && vireoLightboxSession.requestedPhotoId() != null) lightboxNav(1);
        else if (typeof moveBrowseSelection === 'function') moveBrowseSelection(1, {});
        break;
      case 'review_mark_wildlife':
        await nativeMenuSetWildlifeExcluded(false);
        break;
      case 'review_exclude_wildlife':
        await nativeMenuSetWildlifeExcluded(true);
        break;
      case 'tools_run_pipeline':
        if (typeof startPipeline === 'function') await startPipeline();
        else nativeMenuRoute('/pipeline');
        break;
      case 'tools_scan_library':
        nativeMenuRoute('/workspace');
        nativeMenuInfo('Choose a folder or workspace root to scan.');
        break;
      case 'tools_rescan':
        if (typeof openRescanModal === 'function') openRescanModal();
        else nativeMenuRoute('/workspace');
        break;
      case 'tools_find_duplicates':
        if (typeof startScan === 'function') await startScan();
        else nativeMenuRoute('/duplicates');
        break;
      case 'tools_build_previews':
        await nativeMenuJob('/api/jobs/previews', {}, 'Preview job started');
        break;
      case 'tools_sync_metadata':
        if (typeof runSync === 'function') await runSync();
        else await nativeMenuJob('/api/jobs/sync', {}, 'Metadata sync started');
        break;
      case 'tools_verify_models':
        if (typeof verifyAllModels === 'function') await verifyAllModels();
        else await nativeMenuJob('/api/jobs/verify-all-models', {}, 'Model verification started');
        break;
      case 'tools_cancel_job':
        await nativeMenuCancelCurrentJob();
        break;
      case 'help_open_help':
        if (typeof openHelpModal === 'function') openHelpModal();
        break;
      case 'help_copy_diagnostics':
        await nativeMenuCopyDiagnostics();
        break;
      default:
        console.warn('Unknown native menu command:', command);
    }
  } catch (e) {
    nativeMenuError(e && e.message ? e.message : 'Menu command failed');
  }
};

/* ---------- Open in External Editor ----------
 * Two entry points for opening photos in an external editor:
 *   - openInEditor(ids, editorIndex)         — direct call, posts to backend
 *   - getExternalEditorsSync()               — for context-menu builders that
 *                                              need to inline editor entries
 *                                              synchronously; reads the cache.
 *
 * Editors are cached on first read of /api/config; settings.html invalidates
 * via window.invalidateEditorsCache() after saving so the next action picks
 * up changes.
 */
var _externalEditorsCache = null;
var _externalEditorsLoading = null;

async function getExternalEditors() {
  if (_externalEditorsCache !== null) return _externalEditorsCache;
  if (_externalEditorsLoading) return _externalEditorsLoading;
  _externalEditorsLoading = (async function() {
    var arr = [];
    try {
      var cfg = await safeFetch('/api/config', {}, { toast: false });
      arr = Array.isArray(cfg.external_editors) ? cfg.external_editors : [];
      if (arr.length === 0 && (cfg.external_editor || '').trim()) {
        arr = [{ name: 'Editor', path: cfg.external_editor.trim() }];
      }
    } catch(_) {
      arr = [];
    }
    // Mirror cfg.get_editors() shape-filtering on the server: hand-edited
    // config.json (or any client posting to /api/config, which doesn't
    // shape-validate this key) can yield {"path": 123} or similar. Without
    // the typeof guard, path.replace() would throw TypeError inside this
    // async IIFE, leaving _externalEditorsLoading as a rejected promise
    // that every later "Open in Editor" action resolves against.
    _externalEditorsCache = [];
    arr.forEach(function(e) {
      if (!e || typeof e !== 'object') return;
      var path = e.path;
      if (typeof path !== 'string') return;
      path = path.trim();
      if (!path) return;
      var name = e.name;
      if (typeof name !== 'string' || !name.trim()) {
        var parts = path.replace(/\/+$/, '').split('/');
        name = parts[parts.length - 1] || 'Editor';
      } else {
        name = name.trim();
      }
      _externalEditorsCache.push({ name: name, path: path });
    });
    _externalEditorsLoading = null;
    return _externalEditorsCache;
  })();
  return _externalEditorsLoading;
}

// Synchronous accessor for context-menu builders. Returns the cached list,
// or an empty array if the cache hasn't been warmed yet. Pages that build
// menus call getExternalEditors() at init so this is normally populated by
// the time the user opens a menu.
window.getExternalEditorsSync = function() {
  return _externalEditorsCache || [];
};

window.invalidateEditorsCache = function() {
  _externalEditorsCache = null;
  _externalEditorsLoading = null;
};

function photoExternalEditDisabledHint() {
  if (_lbReadOnly && typeof nativeMenuLightboxOpen === 'function' &&
      nativeMenuLightboxOpen()) {
    return _lbReadOnlyMessage;
  }
  return typeof window.getPhotoExternalEditDisabledHint === 'function'
    ? window.getPhotoExternalEditDisabledHint()
    : null;
}

async function openInEditor(photoIds, editorIndex) {
  if (!photoIds || !photoIds.length) return;
  var disabledHint = photoExternalEditDisabledHint();
  if (disabledHint) {
    showToast(disabledHint, 'warning');
    return false;
  }
  var body = { photo_ids: photoIds };
  if (typeof editorIndex === 'number') body.editor_index = editorIndex;
  try {
    var data = await safeFetch('/api/photos/open-external', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    showToast('Opened ' + data.opened + ' photo(s) in editor', 'success');
  } catch(e) {
    showToast(e.message || 'Failed to open in editor', 'error');
  }
}

// Build the "Open in Editor" entries for a context menu. With 0 or 1 editors
// configured, this is a single "Open in Editor" item (backend picks default).
// With 2+, it returns one "Open in <name>" item per editor so the user picks
// inline without a nested submenu.
window.buildOpenInEditorMenuItems = function(photoIds) {
  var editors = window.getExternalEditorsSync();
  var disabledHint = photoExternalEditDisabledHint();
  if (editors.length > 1) {
    return editors.map(function(ed, idx) {
      return {
        label: 'Open in ' + ed.name,
        disabled: !!disabledHint,
        disabledHint: disabledHint || undefined,
        onClick: function() { window.openInEditor(photoIds, idx); },
      };
    });
  }
  return [{
    label: 'Open in Editor',
    disabled: !!disabledHint,
    disabledHint: disabledHint || undefined,
    onClick: function() { window.openInEditor(photoIds); },
  }];
};

// Warm the cache so context-menu builders can render editor entries
// synchronously the first time the user opens a menu.
(function() {
  var run = function() { getExternalEditors().catch(function() {}); };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', run, { once: true });
  } else {
    run();
  }
})();

/* ---------- Develop with darktable ----------
 * Lifted out of browse.html so review.html (burst-review modal) and any
 * other surface can dispatch a darktable develop job for an arbitrary list
 * of photo IDs. Checks availability, confirms, posts the job, and streams
 * progress via SSE.
 */
async function developPhotos(photoIds) {
  if (!photoIds || !photoIds.length) return;
  var disabledHint = photoExternalEditDisabledHint();
  if (disabledHint) {
    showToast(disabledHint, 'warning');
    return false;
  }
  try {
    var status = await safeFetch('/api/darktable/status', {}, { toast: false });
    if (!status.available) {
      showToast('darktable-cli not found. Configure it in Settings.', 'warning');
      return;
    }
  } catch(_) {
    showToast('Could not check darktable availability.', 'error');
    return;
  }
  if (!confirm('Develop ' + photoIds.length + ' photo(s) with darktable?')) return;
  try {
    var data = await safeFetch('/api/jobs/develop', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: photoIds}),
    });
    showToast('Developing ' + photoIds.length + ' photo(s)...', 'info');
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(prog) {
        showToast('Developing: ' + prog.current + '/' + prog.total + ' — ' + (prog.current_file || ''), 'info');
      },
      onComplete: function(event) {
        var result = (event && event.result) || {};
        var developedCount = result.developed || 0;
        var errorCount = result.errors || 0;
        var completed = event && event.status === 'completed';
        showToast(
          (completed ? 'Development complete: ' : 'Development failed: ') +
            developedCount +
            ' developed, ' + errorCount + ' errors',
          completed ? (errorCount ? 'warning' : 'success') : 'error'
        );
      },
      onError: function() {}
    });
  } catch(e) {
    showToast(e.message || 'Error starting develop job', 'error');
  }
}

/* ---------- Safe EventSource ---------- */
// A dropped stream toasts "Connection lost" unless the caller passes
// quietError: true because its onError shows a more specific message.
function safeEventSource(url, callbacks) {
  callbacks = callbacks || {};
  var source = new EventSource(url);
  source.addEventListener('progress', function(e) {
    try {
      if (callbacks.onProgress) callbacks.onProgress(JSON.parse(e.data));
    } catch(ex) { /* ignore malformed SSE data */ }
  });
  source.addEventListener('complete', function(e) {
    source.close();
    try {
      if (callbacks.onComplete) callbacks.onComplete(JSON.parse(e.data));
    } catch(ex) { /* ignore malformed SSE data */ }
  });
  source.onerror = function() {
    source.close();
    if (!callbacks.quietError) showToast('Connection lost', 'error');
    if (callbacks.onError) callbacks.onError();
  };
  return source;
}

/* ---------- Bottom Panel ---------- */
function formatDuration(seconds) {
  if (seconds == null) return '-';
  if (seconds < 60) return seconds.toFixed(1) + 's';
  if (seconds < 3600) {
    var m = Math.floor(seconds / 60);
    var s = Math.round(seconds % 60);
    return m + 'm ' + s + 's';
  }
  var h = Math.floor(seconds / 3600);
  var m = Math.floor((seconds % 3600) / 60);
  return h + 'h ' + m + 'm';
}

(function() {
  var LEVEL_ORDER = { 'DEBUG': 0, 'INFO': 1, 'WARNING': 2, 'ERROR': 3, 'CRITICAL': 4 };
  var lpLines = [];
  var lpSource = null;
  var lpMaxLines = 200;
  var activeJobs = [];
  var _activeWsId = null;
  var currentBpTab = 'jobs';
  var _jobPollTimer = null;
  var _navJobPollTimer = null;

  // Job polls pause while the window is hidden, except while a job is live:
  // the dock/taskbar progress is read from a minimized window. (``active``
  // also carries jobs that finished within the last hour.)
  function startJobPoll(intervalMs) {
    return Vireo.pollWhileVisible(pollJobs, intervalMs, {
      runWhileHidden: function() { return activeJobs.some(isLiveJob); },
    });
  }
  function stopJobPoll(poll) {
    if (poll) poll.stop();
    return null;
  }

  function runtimeWarningDismissKey(id) {
    return 'vireo_runtime_warning_dismissed_' + String(id || '');
  }

  function findRuntimeWarning() {
    for (var i = 0; i < activeJobs.length; i++) {
      var j = activeJobs[i];
      if (j.status === 'running' && j.runtime_warning) return j.runtime_warning;
    }
    return null;
  }

  function updateRuntimeWarningBanner() {
    var banner = document.getElementById('runtimeWarningBanner');
    var text = document.getElementById('runtimeWarningText');
    if (!banner || !text) return;

    var warning = findRuntimeWarning();
    if (!warning || localStorage.getItem(runtimeWarningDismissKey(warning.id)) === '1') {
      banner.style.display = 'none';
      banner.dataset.warningId = '';
      return;
    }

    banner.dataset.warningId = warning.id || '';
    text.innerHTML =
      '<strong>' + bpEscape(warning.title || 'Using CPU only') + '.</strong> ' +
      bpEscape(warning.message || 'This job may be much slower than expected.') +
      ' ' + bpEscape(warning.detail || '') +
      ' ' + bpEscape(warning.next_action || '');
    banner.style.display = 'flex';
  }

  window.dismissRuntimeWarning = function() {
    var banner = document.getElementById('runtimeWarningBanner');
    if (!banner) return;
    var id = banner.dataset.warningId || 'cpu-only-ml';
    localStorage.setItem(runtimeWarningDismissKey(id), '1');
    banner.style.display = 'none';
    fetch('/api/jobs/runtime-warning/dismiss', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: id})
    }).catch(function() {});
  };

  // Theme system
  var THEMES = [
    {id: 'vireo-dark', name: 'Vireo Dark', icon: '\u2600'},
    {id: 'vireo-light', name: 'Vireo Light', icon: '\u263D'},
    {id: 'classic-dark', name: 'Classic Dark', icon: '\u2605'},
    {id: 'vireo-gold', name: 'Vireo Gold', icon: '\u2736'},
    {id: 'high-contrast', name: 'High Contrast', icon: '\u25C9'},
  ];

  window.getThemeList = function() { return THEMES; };

  // Restore theme on load (before init to avoid flash)
  (function() {
    var saved = localStorage.getItem('vireo_theme') || 'vireo-gold';
    // Migrate old values
    if (saved === 'dark') saved = 'vireo-dark';
    if (saved === 'light') saved = 'vireo-light';
    applyTheme(saved, true);
  })();

  function applyTheme(themeId, skipSave) {
    document.documentElement.setAttribute('data-theme', themeId);
    if (!skipSave) localStorage.setItem('vireo_theme', themeId);
    updateThemeIcon();
  }

  window.setTheme = function(themeId) {
    applyTheme(themeId);
  };

  window.toggleTheme = function() {
    var current = document.documentElement.getAttribute('data-theme') || 'vireo-dark';
    var idx = THEMES.findIndex(function(t) { return t.id === current; });
    var next = THEMES[(idx + 1) % THEMES.length];
    applyTheme(next.id);
  };

  function updateThemeIcon() {
    var btn = document.getElementById('themeToggle');
    if (!btn) return;
    var current = document.documentElement.getAttribute('data-theme') || 'vireo-dark';
    var theme = THEMES.find(function(t) { return t.id === current; }) || THEMES[0];
    btn.textContent = theme.icon;
    btn.title = 'Theme: ' + theme.name + ' (click to cycle)';
  }

  // Advanced mode system. Keep the old dev-mode names as aliases so pages
  // and tests that still use them continue to work.
  (function() {
    var stored = localStorage.getItem('vireo_advanced_mode');
    if (stored == null) stored = localStorage.getItem('vireo_dev_mode');
    var on = stored === 'true';
    document.documentElement.setAttribute('data-advanced-mode', on ? 'true' : 'false');
    document.documentElement.setAttribute('data-dev-mode', on ? 'true' : 'false');
    var btn = document.getElementById('devModeToggle');
    if (btn) { btn.style.opacity = on ? '1' : '0.4'; btn.title = 'Advanced mode: ' + (on ? 'ON' : 'OFF'); }
  })();

  window.isAdvancedMode = function() {
    return document.documentElement.getAttribute('data-advanced-mode') === 'true';
  };
  window.isDevMode = window.isAdvancedMode;

  window.toggleAdvancedMode = function() {
    var on = !isAdvancedMode();
    document.documentElement.setAttribute('data-advanced-mode', on ? 'true' : 'false');
    document.documentElement.setAttribute('data-dev-mode', on ? 'true' : 'false');
    localStorage.setItem('vireo_advanced_mode', on ? 'true' : 'false');
    localStorage.setItem('vireo_dev_mode', on ? 'true' : 'false');
    var btn = document.getElementById('devModeToggle');
    if (btn) { btn.style.opacity = on ? '1' : '0.4'; btn.title = 'Advanced mode: ' + (on ? 'ON' : 'OFF'); }
    // Notify pages that may want to re-render
    window.dispatchEvent(new Event('advancedmodechange'));
    window.dispatchEvent(new Event('devmodechange'));
  };
  window.toggleDevMode = window.toggleAdvancedMode;

  function init() {
    // Restore panel state and height
    if (localStorage.getItem('vireo_panel_open') === 'true') {
      var savedH = parseInt(localStorage.getItem('vireo_panel_height')) || 240;
      var panel = document.getElementById('bottomPanel');
      panel.classList.add('open');
      panel.style.height = savedH + 'px';
      document.getElementById('bottomToggle').classList.add('open');
      document.body.style.paddingBottom = (savedH + 28) + 'px';
      document.body.style.setProperty('--bottom-offset', (savedH + 28) + 'px');
    }
    var savedTab = localStorage.getItem('vireo_panel_tab') || 'jobs';
    switchBpTab(savedTab);

    // Infinite scroll for history pane
    document.getElementById('bpHistoryList').addEventListener('scroll', function() {
      var el = this;
      if (el.scrollTop + el.clientHeight >= el.scrollHeight - 20) {
        loadEditHistory(true);
      }
    });

    // SSE for logs — only connect when panel is open
    var panelOpen = localStorage.getItem('vireo_panel_open') === 'true';
    if (panelOpen) { startLogStream(); }
    // Close SSE and stop polling immediately on link click (before unload)
    var isInternalNav = false;
    document.addEventListener('click', function(e) {
      var link = e.target.closest('a[href]');
      if (link && !e.defaultPrevented && !e.ctrlKey && !e.metaKey && !e.shiftKey && !e.altKey &&
          (!e.button || e.button === 0) && link.target !== '_blank' && !link.hasAttribute('download') &&
          link.hostname === location.hostname && !link.getAttribute('href').startsWith('#') &&
          !(link.pathname === location.pathname && link.search === location.search && link.hash)) {
        isInternalNav = true;
        // Kill SSE and polling NOW so threads are freed for the next page
        if (lpSource) { lpSource.close(); lpSource = null; }
        _jobPollTimer = stopJobPoll(_jobPollTimer);
        _navJobPollTimer = stopJobPoll(_navJobPollTimer);
      }
    });

    window.addEventListener('beforeunload', function(e) {
      // Save logs to sessionStorage so they persist across page navigations
      try {
        sessionStorage.setItem('vireo_logs', JSON.stringify(lpLines.slice(-lpMaxLines)));
      } catch(ex) {}
      if (lpSource) { lpSource.close(); lpSource = null; }
      // Only warn on actual tab close, not internal navigation
      if (isInternalNav) { isInternalNav = false; return; }
      // Warn on tab close when user-attention work exists, including queued
      // or pending jobs. Ambient system jobs should not block tab close.
      var running = activeJobs.filter(isAttentionJob);
      if (running.length > 0) {
        e.preventDefault();
        e.returnValue = '';
      }
    });

    // Restore logs from sessionStorage (persisted across page navigations)
    var savedLogs = [];
    try {
      var raw = sessionStorage.getItem('vireo_logs');
      if (raw) savedLogs = JSON.parse(raw);
    } catch(ex) {}

    if (savedLogs.length > 0) {
      // Use saved logs and merge with any newer server logs
      savedLogs.forEach(function(l) { addLpLine(l, true); });
      var lastTime = savedLogs[savedLogs.length - 1].time || 0;
      fetch('/api/logs/recent?count=200')
        .then(function(r) { return r.json(); })
        .then(function(logs) {
          logs.forEach(function(l) {
            if (l.time > lastTime) addLpLine(l, true);
          });
          lpApplyFilter();
        });
    } else {
      fetch('/api/logs/recent?count=200')
        .then(function(r) { return r.json(); })
        .then(function(logs) {
          logs.forEach(function(l) { addLpLine(l, true); });
          lpApplyFilter();
        });
    }

    refreshHistoryControls();

    // Always poll once for navbar badge
    pollJobs();

    // Clear any existing timers before starting new ones
    _jobPollTimer = stopJobPoll(_jobPollTimer);
    _navJobPollTimer = stopJobPoll(_navJobPollTimer);

    if (panelOpen) {
      // Fast poll when panel is open; no slow poll needed
      _jobPollTimer = startJobPoll(2000);
    } else {
      // Slow background poll keeps navbar badge current
      _navJobPollTimer = startJobPoll(15000);
    }
  }

  /* ---------- Tab switching ---------- */
  window.switchBpTab = function(tab) {
    currentBpTab = tab;
    var tabNames = ['jobs', 'history', 'logs'];
    var paneIds = {'jobs': 'bpJobs', 'history': 'bpHistory', 'logs': 'bpLogs'};
    document.querySelectorAll('.bp-tab').forEach(function(t, i) {
      t.classList.toggle('active', tabNames[i] === tab);
    });
    document.querySelectorAll('.bp-pane').forEach(function(p) {
      p.classList.toggle('active', p.id === paneIds[tab]);
    });
    localStorage.setItem('vireo_panel_tab', tab);
    if (tab === 'history') loadEditHistory();
  };

  /* ---------- History tab ---------- */
  var _historySessionCount = 0;
  var _historyLoadSeq = 0;
  var _historyAppendPending = false;

  function loadEditHistory(append) {
    if (append && _historyAppendPending) return;
    if (!append) { _historyLoadSeq++; _historyAppendPending = false; }
    var sequence = _historyLoadSeq;
    var list = document.getElementById('bpHistoryList');
    if (!list) return;
    _historyAppendPending = true;
    var offset = append ? list.querySelectorAll('.bp-history-row').length : 0;
    var historyPromise = safeFetch('/api/edit-history?limit=50&offset=' + offset, {}, {toast: false});
    var redoPromise = !append ? safeFetch('/api/redo/status', {}, {toast: false}).catch(function() { return null; }) : Promise.resolve(null);
    var undoPromise = safeFetch('/api/undo/status', {}, {toast: false});
    Promise.all([historyPromise, redoPromise, undoPromise])
      .then(function(results) {
        if (sequence !== _historyLoadSeq) return;
        var data = results[0];
        var redoStatus = results[1];
        if (!append) list.innerHTML = '';
        // Show redo banner if there's something to redo
        if (!append && redoStatus && redoStatus.available) {
          var redoRow = document.createElement('div');
          redoRow.className = 'bp-history-redo';
          redoRow.style.cssText = 'display:flex;align-items:center;gap:8px;padding:4px 8px;font-size:12px;border-bottom:1px solid var(--border-subtle);color:var(--text-muted);font-style:italic;';
          redoRow.innerHTML = '<span style="opacity:0.5;min-width:70px;">redo</span>'
            + '<span style="min-width:16px;text-align:center;">\u21B6</span>'
            + '<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + _escapeHtml(redoStatus.description) + '</span>'
            + '<button onclick="doRedo(this)" style="background:none;border:1px solid var(--border-primary);color:var(--text-secondary);border-radius:4px;padding:2px 8px;cursor:pointer;font-size:11px;margin-left:4px;">Redo</button>';
          list.appendChild(redoRow);
        }
        if (!data || data.length === 0) {
          if (!append && !list.querySelector('.bp-history-redo')) list.innerHTML = '<div class="bp-history-empty" style="color:var(--text-muted);padding:12px;font-size:12px;">No edits yet</div>';
          return;
        }
        data.forEach(function(entry) {
          if (list.querySelector('[data-edit-id="' + entry.id + '"]')) return;
          var row = document.createElement('div');
          row.className = 'bp-history-row';
          row.dataset.editId = entry.id;
          row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:4px 8px;font-size:12px;border-bottom:1px solid var(--border-subtle);';

          var icon = entry.action_type === 'rating' ? '\u2605' :
                     entry.action_type === 'flag' ? '\u2691' : '\uD83C\uDFF7';

          var ago = _timeAgo(entry.created_at);
          var isFirst = results[2] && entry.id === results[2].id;

          row.innerHTML = '<span style="opacity:0.5;min-width:70px;">' + ago + '</span>'
            + '<span style="min-width:16px;text-align:center;">' + icon + '</span>'
            + '<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + _escapeHtml(entry.description) + '</span>'
            + (isFirst ? '<button onclick="doUndo(this)" style="background:none;border:1px solid var(--border-primary);color:var(--text-secondary);border-radius:4px;padding:2px 8px;cursor:pointer;font-size:11px;margin-left:4px;">Undo</button>'
                       : '');
          list.appendChild(row);
        });
      })
      .catch(function() {}).finally(function() {
        if (sequence === _historyLoadSeq) _historyAppendPending = false;
      });
  }

  function _escapeHtml(s) {
    if (typeof escapeHtml === 'function') return escapeHtml(s);
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  var historyBusy = false;
  var historyPendingWrites = 0;
  var historyStatusSeq = 0;
  var historyStatus = {undo: {available: false}, redo: {available: false}};

  function renderHistoryControls() {
    var blocked = historyBusy || historyPendingWrites > 0;
    ['undo', 'redo'].forEach(function(operation) {
      var button = document.getElementById(operation === 'undo' ? 'historyUndoBtn' : 'historyRedoBtn');
      var status = (typeof window.localHistoryStatus === 'function' && window.localHistoryStatus(operation)) || historyStatus[operation];
      button.disabled = blocked || !status.available;
      var label = operation === 'undo' ? 'Undo' : 'Redo';
      button.title = status.available ? label + ': ' + status.description : 'Nothing to ' + operation;
      var modifier = /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘' : 'Ctrl+';
      button.title += ' (' + modifier + (operation === 'undo' ? 'Z' : 'Shift+Z') + ')';
      button.setAttribute('aria-label', button.title);
      button.setAttribute('aria-keyshortcuts', operation === 'undo' ? 'Control+z Meta+z' : 'Control+Shift+z Meta+Shift+z');
    });
    var status = (typeof window.localHistoryStatus === 'function' && window.localHistoryStatus('undo')) || historyStatus.undo;
    document.getElementById('undoMsg').textContent = historyBusy ? 'Updating…' : (status.description || '');
    document.getElementById('undoToast').setAttribute('aria-busy', String(blocked));
  }

  function refreshHistoryControls() {
    var seq = ++historyStatusSeq;
    return Promise.all([
      safeFetch('/api/undo/status', {}, {toast: false}),
      safeFetch('/api/redo/status', {}, {toast: false})
    ]).then(function(results) {
      if (seq !== historyStatusSeq) return;
      historyStatus = {undo: results[0], redo: results[1]};
      renderHistoryControls();
    }).catch(function() {
      // Keep the last known action retryable after a transient status failure.
      if (seq === historyStatusSeq) renderHistoryControls();
    });
  }
  window.refreshHistoryControls = refreshHistoryControls;
  // Local editor steps change synchronously and need no server status poll.
  window.renderHistoryControls = renderHistoryControls;
  window.addEventListener('focus', refreshHistoryControls);
  window.vireoHistoryBusy = function() { return historyBusy; };

  async function changeHistory(operation, btn) {
    if (historyBusy || historyPendingWrites) return false;
    if (typeof window.changeLocalHistory === 'function') {
      var localResult = window.changeLocalHistory(operation);
      if (localResult !== null) return localResult;
    }
    if (typeof window.beforeHistoryChange === 'function' && window.beforeHistoryChange() === false) return false;
    historyBusy = true;
    document.dispatchEvent(new CustomEvent('vireo:edit-history-busy', {detail: {busy: true}}));
    if (btn) btn.disabled = true;
    renderHistoryControls();
    try {
      var data = await safeFetch('/api/' + operation, {method: 'POST'});
      if (!data || !data.ok) return false;
      _lbRefreshEditRecipeCache(data.edit_recipes);
      // Invalidate older in-flight lightbox flag reads before refreshing it.
      _lbFlagEditSeq++;
      var description = data[operation === 'undo' ? 'undone' : 'redone'];
      showToast((operation === 'undo' ? 'Undone: ' : 'Redone: ') + description, 'success');
      _historySessionCount = Math.max(0, _historySessionCount + (operation === 'undo' ? -1 : 1));
      document.getElementById('historyBadge').textContent = _historySessionCount || '';
      loadEditHistory();
      document.dispatchEvent(new CustomEvent('vireo:edit-history-changed', {
        detail: { operation: operation, result: data }
      }));
      if (typeof window.afterHistoryChange === 'function') {
        try { await window.afterHistoryChange(data); }
        catch (e) { showToast('The edit was saved, but the view could not refresh. Reload to see the current state.', 'error'); }
      }
      await refreshHistoryLightbox();
      return true;
    } catch (e) {
      // safeFetch reports the error. The server retains the history entry.
      return false;
    } finally {
      await refreshHistoryControls();
      historyBusy = false;
      document.dispatchEvent(new CustomEvent('vireo:edit-history-busy', {detail: {busy: false}}));
      if (btn) btn.disabled = false;
      renderHistoryControls();
    }
  }

  async function refreshHistoryLightbox() {
    var photoId = vireoLightboxSession.requestedPhotoId();
    var openToken = vireoLightboxSession.capture();
    if (photoId == null || !document.getElementById('lightboxOverlay').classList.contains('active')) return;
    try {
      var photo = await safeFetch('/api/photos/' + photoId, {}, {toast: false});
      if (!vireoLightboxSession.isCurrent(openToken)) return;
      _lbPhotoDataByPhoto[String(photoId)] = photo;
      _lbRecordFlag(photoId, photo.flag);
      _lbRenderKeywords(photo.keywords);
      _lbRenderLifeListPanel(photoId);
    } catch (e) {
      showToast('The edit was saved, but the photo details could not refresh. Reopen the photo to see its current state.', 'error');
    }
  }

  window.doUndo = function(btn) { return changeHistory('undo', btn); };
  window.doRedo = function(btn) { return changeHistory('redo', btn); };

  // Browse keeps its configurable shortcut bindings; fields retain native
  // text undo, and staged modal edits must not undo the catalog behind them.
  Keymap.register('global', {name: 'undo_history', key: 'ctrl+z', action: function(e) {
    return historyShortcut(e, 'undo');
  }});
  Keymap.register('global', {name: 'redo_history', key: 'ctrl+shift+z', action: function(e) {
    return historyShortcut(e, 'redo');
  }});
  function historyShortcut(e, operation) {
    if (location.pathname === '/browse' || location.pathname.indexOf('/edit') === 0) return false;
    if (document.querySelector('.modal-overlay.open, .grm-overlay.open, .inspect-overlay.open, .help-overlay.active, .folder-browser-overlay.open, .shortcuts-overlay.open')) return false;
    e.stopImmediatePropagation();
    changeHistory(operation);
    return true;
  }

  function _refreshEditRecipeHistoryChanges(data) {
    if (!data || data.action_type !== 'edit_recipe' || !Array.isArray(data.edit_recipes)) return;
    var photoIds = [];
    data.edit_recipes.forEach(function(update) {
      if (!update || update.photo_id == null) return;
      var photoId = Number(update.photo_id);
      if (!Number.isFinite(photoId)) return;
      photoIds.push(photoId);
      _lbMarkEditRecipeWrite(photoId);
      _lbRememberEditRecipe(photoId, update.recipe);
      if (_lbPhotoDataByPhoto[String(photoId)]) {
        _lbPhotoDataByPhoto[String(photoId)].edit_recipe = update.recipe || null;
      }
      _vireoBumpRenderVersion(photoId);
    });
    if (!photoIds.length) return;
    if (typeof window.vireoRefreshPhotoRenders === 'function') {
      window.vireoRefreshPhotoRenders(photoIds);
    }
    if (vireoLightboxSession.requestedPhotoId() != null && photoIds.indexOf(Number(vireoLightboxSession.requestedPhotoId())) !== -1) {
      _lbReloadCurrentRenderAfterEdit(Number(vireoLightboxSession.requestedPhotoId()));
    }
  }

  function _bumpHistoryBadge() {
    _historySessionCount++;
    var badge = document.getElementById('historyBadge');
    if (badge) badge.textContent = _historySessionCount;
    if (currentBpTab === 'history') loadEditHistory();
  }

  // Track at the shared transport so fetch, Request objects, and safeFetch
  // all participate in the same pending-write guard exactly once.
  Vireo.api.trackHistoryRequest = function(input, opts, send) {
    var url = input instanceof Request ? input.url : String(input);
    var method = (opts && opts.method) || (input instanceof Request ? input.method : 'GET');
    var mutation = /^(POST|PUT|PATCH|DELETE)$/i.test(method);
    var historyWrite = mutation && /\/(rating|flag|color_label|keywords|edit-recipe|batch\/|predictions\/|culling\/|misses\/reject|highlights\/(confirm|relabel)|species\/label-cluster|encounters\/species|sync\/discard|pipeline\/(detach-|group\/apply|save-cache)|photos\/\d+\/(wildlife_excluded|location))/.test(url);
    if (historyWrite && historyBusy) {
      var error = new Error('Undo or redo is still finishing — try again in a moment');
      error.code = 'history_busy';
      showToast(error.message, 'warning');
      return Promise.reject(error);
    }
    if (historyWrite) { historyPendingWrites++; renderHistoryControls(); }
    return Promise.resolve().then(send).then(function(response) {
      if (historyWrite && response.ok) _bumpHistoryBadge();
      return response;
    }).finally(function() {
      if (historyWrite) {
        historyPendingWrites--;
        refreshHistoryControls();
        renderHistoryControls();
      }
    });
  };

  function _timeAgo(isoStr) {
    var d = new Date(isoStr + 'Z');
    var now = new Date();
    var sec = Math.floor((now - d) / 1000);
    if (sec < 60) return 'just now';
    if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
    if (sec < 86400) return Math.floor(sec / 3600) + 'h ago';
    return d.toLocaleDateString();
  }

  window.toggleBottomPanel = function() {
    var panel = document.getElementById('bottomPanel');
    var toggle = document.getElementById('bottomToggle');
    panel.classList.add('animating');
    panel.classList.toggle('open');
    toggle.classList.toggle('open');
    var isOpen = panel.classList.contains('open');
    localStorage.setItem('vireo_panel_open', isOpen);
    var h = isOpen ? (parseInt(localStorage.getItem('vireo_panel_height')) || 240) : 0;
    panel.style.height = h + 'px';
    var newPad = isOpen ? (h + 28) + 'px' : '28px';
    document.body.style.paddingBottom = newPad;
    document.body.style.setProperty('--bottom-offset', newPad);
    // Start/stop SSE and polling based on panel visibility
    if (isOpen) {
      startLogStream();
      // Fast poll while panel open; pause slow nav poll to avoid double-polling
      _navJobPollTimer = stopJobPoll(_navJobPollTimer);
      if (!_jobPollTimer) { pollJobs(); _jobPollTimer = startJobPoll(2000); }
    } else {
      stopLogStream();
      _jobPollTimer = stopJobPoll(_jobPollTimer);
      // Resume slow poll for navbar badge
      if (!_navJobPollTimer) { _navJobPollTimer = startJobPoll(15000); }
    }
    setTimeout(function() { panel.classList.remove('animating'); }, 200);
  };

  /* ---------- Panel drag resize ---------- */
  (function() {
    var handle = document.getElementById('bpDragHandle');
    var panel = document.getElementById('bottomPanel');
    var dragging = false;
    var startY, startH;

    handle.addEventListener('mousedown', function(e) {
      dragging = true;
      startY = e.clientY;
      startH = panel.offsetHeight;
      handle.classList.add('dragging');
      document.body.style.userSelect = 'none';
      e.preventDefault();
    });

    document.addEventListener('mousemove', function(e) {
      if (!dragging) return;
      var newH = Math.max(100, Math.min(window.innerHeight - 100, startH + (startY - e.clientY)));
      panel.style.height = newH + 'px';
      document.body.style.paddingBottom = (newH + 28) + 'px';
      document.body.style.setProperty('--bottom-offset', (newH + 28) + 'px');
    });

    document.addEventListener('mouseup', function() {
      if (!dragging) return;
      dragging = false;
      handle.classList.remove('dragging');
      document.body.style.userSelect = '';
      localStorage.setItem('vireo_panel_height', panel.offsetHeight);
    });
  })();

  /* ---------- Log stream lifecycle ---------- */
  function startLogStream() {
    if (lpSource) return;
    lpSource = new EventSource('/api/logs/stream');
    lpSource.addEventListener('log', function(e) {
      try { addLpLine(JSON.parse(e.data)); } catch(ex) { /* ignore malformed SSE data */ }
    });
  }

  function stopLogStream() {
    if (lpSource) { lpSource.close(); lpSource = null; }
  }

  /* ---------- Jobs polling ---------- */
  // Pages signal job completion via this event so activeJobs stays current
  window.addEventListener('vireo-job-done', function(e) {
    refreshHistoryControls();
    var jobId = e.detail && e.detail.job_id;
    if (jobId) {
      activeJobs = activeJobs.filter(function(j) { return j.id !== jobId; });
      renderJobs();
      updateSummary();
      updateActivityDot();
      updateDockProgress();
      updateRuntimeWarningBanner();
    }
  });

  function pollJobs() {
    fetch('/api/jobs')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        // Import tags and other worker edits commit after their start request
        // returns. Reconcile even if a short job finished between two polls.
        refreshHistoryControls();
        activeJobs = data.active || [];
        _activeWsId = data.active_workspace_id || null;
        renderJobs();
        updateSummary();
        updateActivityDot();
        updateDockProgress();
        updateRuntimeWarningBanner();
      })
      .catch(function() {});
  }

  // Show OS-level progress on the dock/taskbar icon for the longest-running
  // job so users can see progress when the window is minimized. No-op outside
  // Tauri. With multiple concurrent jobs we pick the one that started first
  // (earliest started_at); phases without a known total pulse instead.
  var _lastDockProgress = { state: null, value: -1 };
  function bpActiveProgress(progress) {
    if (!progress) return null;
    if (typeof progress.phase_current === 'number' && progress.phase_total > 0) {
      return {
        current: progress.phase_current,
        total: progress.phase_total,
        label: progress.phase_label || progress.phase || 'Current phase',
        phase: true,
      };
    }
    if (progress.total > 0) {
      return {
        current: progress.current || 0,
        total: progress.total,
        label: 'Overall',
        phase: false,
      };
    }
    return null;
  }

  function bpProgressPct(info) {
    if (!info || !info.total) return 0;
    return Math.max(0, Math.min(100, Math.round((info.current / info.total) * 100)));
  }

  function updateDockProgress() {
    if (!window.__TAURI_INTERNALS__) return;
    var running = activeJobs.filter(function(j) {
      return j.status === 'running' && countsForBadge(j);
    });
    var state, value;
    if (running.length === 0) {
      state = 'none'; value = null;
    } else {
      var longest = running[0];
      for (var i = 1; i < running.length; i++) {
        var a = running[i].started_at || '';
        var b = longest.started_at || '';
        if (a && (!b || a < b)) longest = running[i];
      }
      var progressInfo = bpActiveProgress(longest.progress);
      if (progressInfo) {
        var pct = bpProgressPct(progressInfo);
        state = 'normal'; value = pct;
      } else {
        state = 'indeterminate'; value = null;
      }
    }
    if (_lastDockProgress.state === state && _lastDockProgress.value === value) return;
    _lastDockProgress = { state: state, value: value };
    try {
      window.__TAURI_INTERNALS__.invoke('set_job_progress', {
        progress: value,
        indeterminate: state === 'indeterminate',
      }).catch(function() {});
    } catch (e) { /* ignore */ }
  }

  function renderJobs() {
    var container = document.getElementById('bpJobsList');
    // "Active" includes queued/pending work waiting behind a busy slot,
    // not just running ones. Otherwise the bottom panel shows nothing
    // when the user has queued work and the slot is held by a sibling
    // pipeline — there'd be no way to find or cancel the queued run
    // from this surface.
    var allRunning = activeJobs.filter(isLiveJob);
    var attentionJobs = allRunning.filter(countsForBadge);
    // Workspace-agnostic jobs (null workspace_id — the startup backfills
    // run over globally-shared photos) list with the current workspace's
    // jobs: they are running for this workspace too, so filing them under
    // "Other Workspaces" would tell the user the opposite of the truth.
    var thisWs = allRunning.filter(function(j) {
      return j.workspace_id == null || j.workspace_id === _activeWsId;
    });
    var otherWs = allRunning.filter(function(j) {
      return j.workspace_id != null && j.workspace_id !== _activeWsId;
    });

    // Badges count attention-worthy work across workspaces. Ambient system
    // jobs stay visible in the panel without asking for attention.
    document.getElementById('jobsBadge').textContent = attentionJobs.length;
    var navBadge = document.getElementById('navJobBadge');
    if (navBadge) navBadge.textContent = attentionJobs.length > 0 ? ' (' + attentionJobs.length + ')' : '';

    if (allRunning.length === 0) {
      // "Last run" fallback must exclude live state — running,
      // queued, and pending. A queued job that hasn't started is not a "last
      // run" any more than a running one is.
      var lastDone = activeJobs.filter(function(j) { return !isLiveJob(j); }).sort(function(a, b) {
        return String(b.finished_at || b.created_at || '').localeCompare(String(a.finished_at || a.created_at || ''));
      })[0];
      if (lastDone) {
        container.innerHTML = '<div style="font-size:12px;color:var(--text-muted);padding:4px 0;">No active jobs. Last run: ' +
          bpEscape(window.formatJobType(lastDone.type)) + ' ' + (lastDone.status === 'completed' ? '\u2713' : '\u2717') +
          ' <a href="/jobs" style="color:var(--accent);text-decoration:none;">View history \u2192</a></div>';
      } else {
        container.innerHTML = '<div style="font-size:12px;color:var(--text-muted);padding:4px 0;">No active jobs. ' +
          '<a href="/jobs" style="color:var(--accent);text-decoration:none;">View history \u2192</a></div>';
      }
      return;
    }

    var html = '';

    // Current workspace jobs
    if (thisWs.length > 0) {
      html += renderJobList(thisWs);
    } else if (otherWs.length > 0) {
      html += '<div style="font-size:12px;color:var(--text-muted);padding:4px 0;">No jobs in this workspace.</div>';
    }

    // Other workspace jobs
    if (otherWs.length > 0) {
      html += '<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border-color);">';
      html += '<div style="font-size:11px;color:var(--text-invisible);margin-bottom:4px;">Other Workspaces</div>';
      html += '<div style="font-size:11px;color:var(--text-muted);margin-bottom:6px;">Jobs can be long-running, so Vireo lets you change workspaces while your jobs run. You can still view them here.</div>';
      html += renderJobList(otherWs);
      html += '</div>';
    }

    container.innerHTML = html;
  }

  function renderJobList(jobs) {
    var html = '';
    jobs.forEach(function(j) {
      var progressInfo = bpActiveProgress(j.progress);
      var pct = progressInfo ? bpProgressPct(progressInfo) : 0;
      var phase = j.progress && j.progress.phase ? j.progress.phase : '';
      var detail = '';
      if (progressInfo) {
        detail = progressInfo.current.toLocaleString() + '/' + progressInfo.total.toLocaleString();
        if (pct > 0) detail += ' (' + pct + '%)';
      }
      // Queued/pending work gets a muted dot so the user can tell it apart
      // from jobs actually consuming the GPU.
      var isWaiting = isWaitingJob(j);
      var isPaused = j.status === 'pausing' || j.status === 'paused';
      var waitingLabel = isPaused ? j.status : 'queued';
      var dotBg = isPaused ? 'var(--warning, #d99a24)' :
        (isWaiting ? 'var(--text-muted)' : 'var(--accent)');
      html += '<div class="bp-compact-job" style="display:flex;align-items:center;gap:8px;padding:4px 0;font-size:12px;">';
      html += '<span style="width:6px;height:6px;border-radius:50%;background:' + dotBg + ';flex-shrink:0;"></span>';
      html += '<span style="font-weight:600;">' + bpEscape(window.formatJobType(j.type)) + '</span>';
      if (isWaiting || isPaused) html += '<span style="color:var(--text-muted);">(' + waitingLabel + ')</span>';
      if (phase) html += '<span style="color:var(--text-muted);">\u2014 ' + bpEscape(phase) + '</span>';
      if (detail) html += '<span style="color:var(--text-muted);font-variant-numeric:tabular-nums;">' + detail + '</span>';
      html += '<a href="/jobs" style="margin-left:auto;color:var(--accent);text-decoration:none;font-size:11px;white-space:nowrap;">View \u2192</a>';
      html += '</div>';
    });
    return html;
  }

  function updateSummary() {
    var summary = document.getElementById('jobSummary');
    var liveJobs = activeJobs.filter(isLiveJob);
    var attentionJobs = liveJobs.filter(countsForBadge);
    var running = attentionJobs.filter(function(j) { return j.status === 'running'; });
    var waiting = attentionJobs.filter(isWaitingJob);
    var paused = attentionJobs.filter(function(j) {
      return j.status === 'pausing' || j.status === 'paused';
    });

    if (running.length === 0 && waiting.length === 0 && paused.length > 0) {
      summary.textContent = paused.length === 1
        ? window.formatJobType(paused[0].type) + ' ' + paused[0].status
        : paused.length + ' jobs paused';
      summary.classList.add('idle');
      summary.classList.remove('active');
      return;
    }

    if (running.length === 0 && waiting.length === 0) {
      if (liveJobs.length > 0) {
        summary.textContent = liveJobs.length === 1
          ? 'Background: ' + window.formatJobType(liveJobs[0].type)
          : liveJobs.length + ' background jobs';
      } else {
        summary.textContent = 'No active jobs';
      }
      summary.classList.add('idle');
      summary.classList.remove('active');
      return;
    }

    summary.classList.remove('idle');

    // When the slot is empty and only waiting jobs exist (shouldn't
    // happen with the current scheduler — promotion is immediate on
    // enqueue when a slot is free — but cheap to handle), render a
    // short waiting-state line. Waiting jobs have no progress to drive
    // the running-job render path below.
    if (running.length === 0) {
      summary.textContent = waiting.length === 1
        ? 'Processing queued'
        : waiting.length + ' processing jobs queued';
      return;
    }

    var j = running[0];
    var text = window.formatJobType(j.type);
    if (j.progress && j.progress.phase) {
      text += ' — ' + j.progress.phase;
    }
    var progressInfo = bpActiveProgress(j.progress);
    if (progressInfo) {
      var pct = bpProgressPct(progressInfo);
      var isBytes = j.type && j.type.indexOf('download') !== -1 && progressInfo.total > 1000000;
      if (isBytes) {
        text += ' ' + (progressInfo.current / (1024*1024)).toFixed(0) + '/' + (progressInfo.total / (1024*1024)).toFixed(0) + ' MB (' + pct + '%)';
      } else {
        text += ' ' + progressInfo.current.toLocaleString() + '/' + progressInfo.total.toLocaleString() + ' (' + pct + '%)';
      }
      if (j.progress.rate > 0) {
        text += ' ' + (j.progress.rate / (1024*1024)).toFixed(1) + ' MB/s';
      }
    } else if (j.progress && j.progress.current_file) {
      text += ' ' + j.progress.current_file;
    }
    // The "+ N more" suffix counts running siblings AND any waiting jobs
    // so the user sees their total backlog at a glance.
    var extras = (running.length - 1) + waiting.length;
    if (extras > 0) {
      text += ' + ' + extras + ' more';
    }
    summary.textContent = text;
  }

  function updateActivityDot() {
    // The dot signals attention-worthy work; ambient system jobs remain in
    // the Jobs panel without lighting up the navbar.
    var running = activeJobs.filter(isAttentionJob);
    var dot = document.getElementById('navActivityDot');
    if (dot) dot.classList.toggle('active', running.length > 0);
  }

  function formatElapsed(secs) {
    if (secs < 60) return Math.round(secs) + 's';
    if (secs < 3600) return Math.floor(secs / 60) + 'm ' + Math.round(secs % 60) + 's';
    return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
  }

  /* ---------- Log lines ---------- */
  // Live log lines arrive over SSE at the job's log rate (can be many per
  // second for hours). Doing per-line DOM work — appending, a forced
  // auto-scroll reflow, and a full lpApplyFilter() re-scan over every
  // retained node — saturates the renderer main thread and freezes the UI
  // even while the backend stays responsive. So the live (non-bulk) path
  // only enqueues; a single requestAnimationFrame flush coalesces a whole
  // frame's worth of lines into one append + one scroll + one count write,
  // with the level filter read once and applied inline per new node (no
  // O(n) re-scan). lpApplyFilter() stays for the filter <select> onchange,
  // where a full re-scan of existing lines is genuinely required.
  var _lpPending = [];
  var _lpFlushScheduled = false;

  function _lpMinOrder() {
    var sel = document.getElementById('lpLevelFilter');
    return LEVEL_ORDER[sel ? sel.value : 'DEBUG'] || 0;
  }

  function _lpMakeLineEl(record, minOrder) {
    var time = new Date(record.time * 1000);
    var timeStr = time.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    var div = document.createElement('div');
    div.className = 'lp-line lp-' + record.level;
    div.dataset.level = record.level;
    div.innerHTML = '<span class="lp-time">' + timeStr + '</span>' +
      '<span class="lp-level">' + record.level + '</span>' +
      bpEscape(record.message || '');
    // Apply the active level filter to just this node — cheap, and avoids
    // re-scanning every existing line on each new line.
    if ((LEVEL_ORDER[record.level] || 0) < minOrder) div.style.display = 'none';
    return div;
  }

  function _lpFlush() {
    _lpFlushScheduled = false;
    if (_lpPending.length === 0) return;
    var batch = _lpPending;
    _lpPending = [];
    var container = document.getElementById('lpContent');
    if (!container) return;
    var minOrder = _lpMinOrder();

    for (var i = 0; i < batch.length; i++) lpLines.push(batch[i]);
    var overflow = lpLines.length - lpMaxLines;
    if (overflow > 0) lpLines.splice(0, overflow);

    // Only build nodes we'll actually keep — a single burst larger than
    // the cap would otherwise create (and immediately discard) thousands.
    var keepFrom = Math.max(0, batch.length - lpMaxLines);
    var frag = document.createDocumentFragment();
    for (var j = keepFrom; j < batch.length; j++) {
      frag.appendChild(_lpMakeLineEl(batch[j], minOrder));
    }
    container.appendChild(frag);
    while (container.childNodes.length > lpMaxLines && container.firstChild) {
      container.firstChild.remove();
    }

    var autoScroll = document.getElementById('lpAutoScroll');
    if (autoScroll && autoScroll.checked) {
      container.scrollTop = container.scrollHeight;
    }
    document.getElementById('lpCount').textContent = lpLines.length;
    document.getElementById('logsBadge').textContent = lpLines.length;
  }

  window.addLpLine = function(record, bulk) {
    if (bulk) {
      // Restore/backfill path: synchronous so callers that immediately
      // run lpApplyFilter() afterwards see the final DOM. Still bounded,
      // still no per-line scroll.
      if (lpLines.length >= lpMaxLines) {
        lpLines.shift();
        var firstB = document.getElementById('lpContent').firstChild;
        if (firstB) firstB.remove();
      }
      lpLines.push(record);
      document.getElementById('lpContent').appendChild(
        _lpMakeLineEl(record, _lpMinOrder())
      );
      document.getElementById('lpCount').textContent = lpLines.length;
      document.getElementById('logsBadge').textContent = lpLines.length;
      return;
    }
    _lpPending.push(record);
    // Bound the queue independent of rAF. A hidden/minimized tab pauses
    // requestAnimationFrame while the EventSource keeps delivering, so
    // without this the pending array would grow unbounded for the entire
    // life of a long job and then process one giant batch on resume.
    // Only the last lpMaxLines can ever be shown (the DOM cap in
    // _lpFlush), so dropping older pending records here is lossless and
    // mirrors the lpLines cap.
    if (_lpPending.length > lpMaxLines) {
      _lpPending.splice(0, _lpPending.length - lpMaxLines);
    }
    if (!_lpFlushScheduled) {
      _lpFlushScheduled = true;
      requestAnimationFrame(_lpFlush);
    }
  };

  window.lpApplyFilter = function() {
    var minLevel = document.getElementById('lpLevelFilter').value;
    var minOrder = LEVEL_ORDER[minLevel] || 0;
    document.querySelectorAll('#lpContent .lp-line').forEach(function(el) {
      var ok = (LEVEL_ORDER[el.dataset.level] || 0) >= minOrder;
      el.style.display = ok ? '' : 'none';
    });
  };

  function bpEscape(str) {
    if (str == null) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
  }

  // Convert internal job-type identifiers like "new_images_walk" or
  // "duplicate-scan" into a user-facing label ("New Images Walk", "Duplicate
  // Scan"). Exposed on window so per-page scripts (e.g. jobs.html) can reuse it.
  window.formatJobType = function(type) {
    if (!type) return '';
    return String(type)
      .split(/[-_]+/)
      .filter(function(w) { return w.length > 0; })
      .map(function(w) { return w.charAt(0).toUpperCase() + w.slice(1); })
      .join(' ');
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
