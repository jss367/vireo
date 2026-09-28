(function initFolderScopedWorkLocally() {
  'use strict';

  var data = null;
  var activeJob = null;
  var actionInFlight = false;
  var loadGeneration = 0;
  var pendingStageItems = [];
  var stagePreflightGeneration = 0;
  var stagePreflightTimer = null;
  var stagePreflightSignature = null;
  var stagePreflightAbort = null;
  var stagePreflightId = null;
  var stagePreflightWorkspaceId = null;
  var stagePreflightStorageKey = preflightClientStorageKey();
  var stagePreflightClientState = loadPreflightClientState();
  var stagePreflightClientId = stagePreflightClientState.id;
  // The per-history-entry token and counter survive reloads and Back/Forward.
  // If a pagehide keepalive is dropped, a restored page can still supersede
  // its old scan without sharing cancellation state with an unrelated tab.
  var stagePreflightSeq = stagePreflightClientState.seq;

  function newPreflightId() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
      return window.crypto.randomUUID();
    }
    return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
  }

  function loadPreflightClientState() {
    try {
      var stored = JSON.parse(window.sessionStorage.getItem(stagePreflightStorageKey));
      if (
        stored && typeof stored.id === 'string' && stored.id &&
        Number.isSafeInteger(stored.seq) && stored.seq >= 0
      ) {
        return stored;
      }
    } catch (_error) {
      // Storage can be disabled by browser privacy settings. The in-memory
      // state below still preserves normal same-page supersession.
    }
    var state = {id: newPreflightId(), seq: 0};
    persistPreflightClientState(state);
    return state;
  }

  function preflightClientStorageKey() {
    var stateKey = 'vireoWorkLocallyPreflightEntryId';
    var keyPrefix = 'vireoWorkLocallyPreflightClient:';
    try {
      var historyState = window.history.state;
      var entryId = isRestoredNavigation() && historyState &&
        typeof historyState[stateKey] === 'string'
        ? historyState[stateKey]
        : null;
      if (!entryId) {
        entryId = newPreflightId();
        var nextState = historyState && typeof historyState === 'object'
          ? Object.assign({}, historyState)
          : {};
        nextState[stateKey] = entryId;
        window.history.replaceState(nextState, document.title);
      }
      return keyPrefix + entryId;
    } catch (_error) {
      // History state can be unavailable in hardened browsing contexts.
      return keyPrefix + newPreflightId();
    }
  }

  function isRestoredNavigation() {
    try {
      if (window.performance && typeof window.performance.getEntriesByType === 'function') {
        var navigations = window.performance.getEntriesByType('navigation');
        if (navigations.length) {
          return ['reload', 'back_forward'].indexOf(navigations[0].type) >= 0;
        }
      }
      // Navigation Timing Level 1 fallback for older WebKit versions.
      return Boolean(
        window.performance && window.performance.navigation &&
        [1, 2].indexOf(window.performance.navigation.type) >= 0
      );
    } catch (_error) {
      return false;
    }
  }

  function persistPreflightClientState(state) {
    try {
      window.sessionStorage.setItem(stagePreflightStorageKey, JSON.stringify(state));
    } catch (_error) {
      // Best effort: blocked or full storage should not prevent preflighting.
    }
  }

  function newPreflightSeq() {
    stagePreflightSeq += 1;
    stagePreflightClientState.seq = stagePreflightSeq;
    persistPreflightClientState(stagePreflightClientState);
    return stagePreflightSeq;
  }

  function cancelStagePreflight(notifyServer) {
    var preflightId = stagePreflightId;
    var workspaceId = stagePreflightWorkspaceId;
    if (stagePreflightAbort) stagePreflightAbort.abort();
    stagePreflightAbort = null;
    stagePreflightId = null;
    stagePreflightWorkspaceId = null;
    if (!notifyServer || !preflightId) return;
    // Aborting fetch stops the browser from waiting, but a WSGI handler may
    // already be blocked in SMB I/O. Notify the server so it stops walking as
    // soon as that filesystem call returns. A later preflight also supersedes
    // the earlier one server-side if this best-effort request cannot arrive.
    Vireo.api.fetch('/api/workspaces/active/local-folders/preflight/cancel', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        preflight_id: preflightId,
        workspace_id: workspaceId
      }),
      keepalive: true
    }).catch(function() {});
  }

  var blockingJobTimer = null;
  var blockingJobRefreshDue = false;
  var stageBlockedByJob = false;

  function blockingJobMessage(job) {
    job = job || (data && data.blocking_job);
    if (!job) return '';
    var label = typeof window.formatJobType === 'function'
      ? window.formatJobType(job.type)
      : String(job.type || 'Processing').replace(/[-_]+/g, ' ');
    var status = ['queued', 'running', 'pausing', 'paused'].indexOf(job.status) >= 0
      ? job.status
      : 'running';
    return label + ' is ' + status + '. Work Locally will be available when it finishes or is cancelled.';
  }

  function blockingJobForFolder(folderId) {
    var jobs = (data && data.folder_blocking_jobs) || {};
    return jobs[String(Number(folderId))] || null;
  }

  function blockingJobForSelection(folderIds) {
    if (!data) return null;
    if (!folderIds || !folderIds.length) return data.blocking_job || null;
    for (var index = 0; index < folderIds.length; index += 1) {
      var job = blockingJobForFolder(folderIds[index]);
      if (job) return job;
    }
    return null;
  }

  function blockerSignature(payload) {
    if (!payload) return '';
    // ``residency_fingerprint`` is a short server-side digest of the
    // ``local_folders`` rows visible to this workspace (state, activated_at,
    // created_at per covering root). Including it here means a
    // stage/sync/discard started and finished in another tab between two
    // polls — both of which see no active job — still trips the signature
    // and re-runs load(), so the folder-tree badge doesn't sit at its
    // pre-job state until reload.
    return JSON.stringify({
      blocking_job: payload.blocking_job || null,
      folder_blocking_jobs: payload.folder_blocking_jobs || {},
      residency_fingerprint: payload.residency_fingerprint || ''
    });
  }

  async function refreshBlockingJob() {
    // Poll only the lightweight blocker endpoint. The full /local-folders
    // payload walks every managed local tree, so hitting it on a keep-alive
    // timer while nothing is happening would keep the disk busy for large
    // libraries. Trigger a full load() only when the blocker state actually
    // changes so the panel re-renders enabled/disabled controls.
    if (!data) return;
    try {
      var payload = await Vireo.api.json(
        '/api/workspaces/active/local-folders/blocker', {}, {toast: false}
      );
      var previous = blockerSignature(data);
      var next = blockerSignature(payload);
      if (previous !== next) {
        await load();
      }
    } catch (_error) {
      // Swallow — the next tick will retry, and any hard error already
      // surfaces through the full-load code path.
    } finally {
      scheduleBlockingJobRefresh();
    }
  }

  function scheduleBlockingJobRefresh() {
    if (blockingJobTimer) clearTimeout(blockingJobTimer);
    blockingJobTimer = null;
    blockingJobRefreshDue = false;
    if (!data) return;
    // Fast refresh while a block is active so the UI re-enables promptly,
    // slow keep-alive otherwise so a scan/pipeline started in another tab
    // becomes visible without a page reload.
    var delay = data.blocking_job ? 3000 : 15000;
    blockingJobTimer = setTimeout(function() {
      blockingJobTimer = null;
      // Nobody reads the controls of a hidden window: wait for it to come
      // back, then refresh at once.
      if (document.hidden) {
        blockingJobRefreshDue = true;
        return;
      }
      refreshBlockingJob();
    }, delay);
  }

  document.addEventListener('visibilitychange', function() {
    if (document.hidden || !blockingJobRefreshDue) return;
    blockingJobRefreshDue = false;
    refreshBlockingJob();
  });

  function selectedItems(folderIds, localOnly) {
    var wanted = folderIds && folderIds.length
      ? new Set(folderIds.map(Number))
      : null;
    return ((data && data.folders) || []).filter(function(item) {
      if (wanted && !wanted.has(Number(item.requested_folder_id))) return false;
      return !localOnly || item.state !== 'remote';
    });
  }

  function statusFor(folderId) {
    return ((data && data.folders) || []).find(function(item) {
      return Number(item.requested_folder_id) === Number(folderId);
    }) || null;
  }

  function jobFor(folderId) {
    var item = statusFor(folderId);
    var rootId = Number(item && item.root_folder_id != null
      ? item.root_folder_id
      : folderId);
    var job = ((data && data.jobs) || []).find(function(candidate) {
      return (candidate.folder_ids || []).map(Number).indexOf(rootId) >= 0;
    }) || null;
    if (!job || !activeJob || activeJob.id !== job.id) return job;
    return Object.assign({}, job, {
      progress: activeJob.progressByRoot[String(rootId)] || null
    });
  }

  function folderName(path) {
    var parts = String(path || '').replace(/[\\/]+$/, '').split(/[\\/]/);
    return parts[parts.length - 1] || 'Folder';
  }

  function joinPath(parent, name) {
    var base = String(parent || '').replace(/[\\/]+$/, '');
    if (!base) return name;
    return base + (base.indexOf('\\') >= 0 ? '\\' : '/') + name;
  }

  function updateStageDestinationPreview(input) {
    var preview = document.querySelector(
      '[data-local-destination-preview="' + input.dataset.localDestinationBase + '"]'
    );
    if (!preview) return;
    preview.textContent = joinPath(input.value.trim(), input.dataset.localFolderName);
  }

  function stageRequestBody() {
    var destinationBases = {};
    var complete = pendingStageItems.length > 0;
    pendingStageItems.forEach(function(item) {
      var id = Number(item.requested_folder_id);
      var input = document.querySelector('[data-local-destination-base="' + id + '"]');
      var destination = input ? input.value.trim() : '';
      if (!destination) complete = false;
      destinationBases[String(id)] = destination;
    });
    return {
      complete: complete,
      body: {
        workspace_id: data && data.workspace_id,
        folder_ids: pendingStageItems.map(function(item) { return item.requested_folder_id; }),
        destination_bases: destinationBases
      }
    };
  }

  function updateStageDialogBlocker() {
    var modal = document.getElementById('stageLocalFoldersModal');
    if (!modal || !modal.classList.contains('open') || !pendingStageItems.length) return;
    var folderIds = pendingStageItems.map(function(item) {
      return item.requested_folder_id;
    });
    var blocker = blockingJobForSelection(folderIds);
    var button = document.getElementById('confirmStageLocalFolders');
    var error = document.getElementById('stageLocalFoldersError');
    if (blocker) {
      stageBlockedByJob = true;
      stagePreflightGeneration += 1;
      stagePreflightSignature = null;
      cancelStagePreflight(true);
      if (stagePreflightTimer) clearTimeout(stagePreflightTimer);
      stagePreflightTimer = null;
      if (button) button.disabled = true;
      if (error) error.textContent = blockingJobMessage(blocker);
      return;
    }
    if (!stageBlockedByJob) return;
    stageBlockedByJob = false;
    if (error) error.textContent = '';
    scheduleStagePreflight(0);
  }

  function renderStagePreflight(preflight) {
    var capacity = document.getElementById('stageLocalFoldersCapacity');
    var button = document.getElementById('confirmStageLocalFolders');
    var byFolder = {};
    (preflight.folders || []).forEach(function(folder) {
      byFolder[String(folder.folder_id)] = folder;
    });
    pendingStageItems.forEach(function(item) {
      var id = String(item.requested_folder_id);
      var line = document.querySelector('[data-local-capacity-folder="' + id + '"]');
      var folder = byFolder[id];
      if (!line || !folder) return;
      line.style.color = 'var(--text-secondary)';
      line.textContent = formatBytesNav(folder.total_bytes || 0) + ' to copy · ' +
        Number(folder.total_files || 0).toLocaleString() + ' file' +
        (Number(folder.total_files || 0) === 1 ? '' : 's') + ' · ' +
        formatBytesNav(folder.free_bytes || 0) + ' free on destination';
    });

    var blocked = (preflight.volumes || []).filter(function(volume) {
      return !volume.can_copy;
    });
    if (capacity) {
      capacity.style.color = blocked.length ? 'var(--danger)' : 'var(--text-secondary)';
      if (blocked.length) {
        capacity.innerHTML = blocked.map(function(volume) {
          return '<div><strong>Not enough space.</strong> ' +
            escapeHtml(formatBytesNav(volume.copy_bytes || 0)) + ' would be copied to this disk, ' +
            escapeHtml(formatBytesNav(volume.free_bytes || 0)) + ' is free, and Vireo keeps ' +
            escapeHtml(formatBytesNav(volume.reserve_bytes || 0)) + ' free for safety.</div>';
        }).join('');
      } else {
        var volumeCount = (preflight.volumes || []).length;
        var summary = formatBytesNav(preflight.total_bytes || 0) + ' total to copy' +
          (volumeCount === 1
            ? ' · ' + formatBytesNav(preflight.volumes[0].after_copy_bytes || 0) + ' will remain free'
            : ' across ' + volumeCount + ' destination disks');
        var closeToReserve = (preflight.volumes || []).filter(function(volume) {
          var reserve = Number(volume.reserve_bytes || 0);
          var afterCopy = Number(volume.after_copy_bytes || 0);
          // Routine successful copies do not need a safety warning. Explain
          // the reserve only when the remaining headroom is no larger than
          // the reserve itself (for example, the copy leaves less than 10%
          // free on a disk with the standard 5% reserve).
          return reserve > 0 && afterCopy - reserve <= reserve;
        });
        if (volumeCount === 1 && closeToReserve.length === 1) {
          var volume = closeToReserve[0];
          var headroom = Math.max(
            0,
            Number(volume.after_copy_bytes || 0) - Number(volume.reserve_bytes || 0)
          );
          summary += headroom > 0
            ? '. That leaves ' + formatBytesNav(headroom) +
              " above Vireo's " + formatBytesNav(volume.reserve_bytes || 0) +
              ' safety reserve'
            : ". That is exactly Vireo's " + formatBytesNav(volume.reserve_bytes || 0) +
              ' safety reserve, with no extra headroom';
        } else if (closeToReserve.length) {
          summary += '. ' + closeToReserve.length + ' destination disk' +
            (closeToReserve.length === 1
              ? ' is close to its ' + formatBytesNav(closeToReserve[0].reserve_bytes || 0) +
                ' safety reserve'
              : 's are close to their safety reserves (' + closeToReserve.map(function(volume) {
                return formatBytesNav(volume.reserve_bytes || 0);
              }).join(', ') + ')');
        }
        capacity.textContent = summary + '.';
      }
    }
    if (button) button.disabled = !preflight.can_copy;
  }

  async function runStagePreflight() {
    var request = stageRequestBody();
    var capacity = document.getElementById('stageLocalFoldersCapacity');
    var button = document.getElementById('confirmStageLocalFolders');
    var generation = ++stagePreflightGeneration;
    stagePreflightSignature = null;
    if (button) button.disabled = true;
    if (!request.complete) {
      if (capacity) {
        capacity.style.color = 'var(--danger)';
        capacity.textContent = 'Choose a destination for every local copy.';
      }
      return;
    }
    var controller = new AbortController();
    var preflightId = newPreflightId();
    var preflightSeq = newPreflightSeq();
    stagePreflightAbort = controller;
    stagePreflightId = preflightId;
    stagePreflightWorkspaceId = data && data.workspace_id != null
      ? Number(data.workspace_id)
      : null;
    if (capacity) {
      capacity.style.color = 'var(--text-dim)';
      capacity.innerHTML = '<span class="btn-spinner" style="display:inline-block;margin-right:6px;"></span>Calculating folder sizes and available space...';
    }
    try {
      var result = await Vireo.api.json('/api/workspaces/active/local-folders/preflight', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(Object.assign({}, request.body, {
          preflight_id: preflightId,
          preflight_client_id: stagePreflightClientId,
          preflight_seq: preflightSeq
        })),
        signal: controller.signal
      }, {toast: false});
      if (generation !== stagePreflightGeneration || !pendingStageItems.length) return;
      stagePreflightSignature = JSON.stringify(request.body);
      renderStagePreflight(result);
    } catch (requestError) {
      if (controller.signal.aborted || generation !== stagePreflightGeneration || !pendingStageItems.length) return;
      if (capacity) {
        capacity.style.color = 'var(--danger)';
        capacity.textContent = requestError.message || 'Could not check folder sizes and free space.';
      }
    } finally {
      if (stagePreflightAbort === controller) stagePreflightAbort = null;
      if (stagePreflightId === preflightId) {
        stagePreflightId = null;
        stagePreflightWorkspaceId = null;
      }
    }
  }

  function scheduleStagePreflight(delay) {
    if (stagePreflightTimer) clearTimeout(stagePreflightTimer);
    cancelStagePreflight(true);
    stagePreflightGeneration += 1;
    stagePreflightSignature = null;
    var button = document.getElementById('confirmStageLocalFolders');
    if (button) button.disabled = true;
    stagePreflightTimer = setTimeout(runStagePreflight, delay == null ? 500 : delay);
  }

  function closeStageDialog() {
    if (actionInFlight) return;
    var modal = document.getElementById('stageLocalFoldersModal');
    if (modal) modal.classList.remove('open');
    stagePreflightGeneration += 1;
    stagePreflightSignature = null;
    cancelStagePreflight(true);
    stageBlockedByJob = false;
    if (stagePreflightTimer) clearTimeout(stagePreflightTimer);
    stagePreflightTimer = null;
    pendingStageItems = [];
  }

  async function openStageDialog(folderIds) {
    if (actionInFlight || activeJob) return;
    await load();
    if (actionInFlight || activeJob) return;
    var blocker = blockingJobForSelection(folderIds);
    if (blocker) {
      showToast(blockingJobMessage(blocker), 'warning');
      return;
    }
    var items = selectedItems(folderIds, false).filter(function(item) {
      return item.state === 'remote';
    });
    if (!items.length) return;
    pendingStageItems = items;
    var container = document.getElementById('stageLocalFoldersModalItems');
    var error = document.getElementById('stageLocalFoldersError');
    var modal = document.getElementById('stageLocalFoldersModal');
    if (!container || !modal) return;
    stageBlockedByJob = false;
    if (error) error.textContent = '';
    var workspaceNamesById = {};
    ((data && data.linked_workspaces) || []).forEach(function(workspace) {
      workspaceNamesById[String(workspace.id)] = workspace.name;
    });
    container.innerHTML = items.map(function(item) {
      var id = Number(item.requested_folder_id);
      var source = item.source_path || item.display_path || '';
      var name = folderName(source);
      var localName = item.local_folder_name || name;
      var base = item.default_local_base || '';
      var finalPath = item.default_local_path || joinPath(base, localName);
      var shared = (item.workspace_ids || []).length;
      var sharedNames = (item.workspace_ids || []).map(function(workspaceId) {
        return workspaceNamesById[String(workspaceId)];
      }).filter(Boolean).map(escapeHtml);
      var sharingMessage = 'This copy will be shared by ' + shared + ' linked workspaces' +
        (sharedNames.length === shared ? ': ' + sharedNames.join(', ') : '') + '.';
      return '<div class="setting-row" style="display:block;padding:12px 0;">' +
        '<div style="font-size:13px;font-weight:600;color:var(--text-primary);margin-bottom:3px;">' + escapeHtml(name) + '</div>' +
        '<div style="font-family:monospace;font-size:10px;color:var(--text-dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-bottom:9px;" title="' + escapeAttr(source) + '">' + escapeHtml(source) + '</div>' +
        '<label class="modal-label" for="localDestinationBase-' + id + '">Create the local copy in</label>' +
        '<div style="display:flex;gap:7px;align-items:center;">' +
          '<input class="modal-input" id="localDestinationBase-' + id + '" data-local-destination-base="' + id + '" data-folder-name="' + escapeAttr(name) + '" data-local-folder-name="' + escapeAttr(localName) + '" value="' + escapeAttr(base) + '" style="flex:1;font-family:monospace;font-size:12px;">' +
          '<button class="modal-btn" data-browse-local-destination="' + id + '" style="white-space:nowrap;">Browse...</button>' +
        '</div>' +
        '<div style="font-size:10px;color:var(--text-dim);margin-top:5px;">Local folder: <span data-local-destination-preview="' + id + '" style="font-family:monospace;">' + escapeHtml(finalPath) + '</span></div>' +
        '<div data-local-capacity-folder="' + id + '" style="font-size:11px;color:var(--text-dim);margin-top:6px;">Calculating size and free space...</div>' +
        (shared > 1 ? '<div style="font-size:10px;color:var(--accent);margin-top:4px;">' + sharingMessage + '</div>' : '') +
      '</div>';
    }).join('');
    modal.classList.add('open');
    scheduleStagePreflight(0);
  }

  function progressMarkup() {
    var p = activeJob && activeJob.progress;
    var detail = '';
    if (p && p.total) {
      detail = Number(p.current || 0).toLocaleString() + ' / ' +
        Number(p.total).toLocaleString() + ' files';
      if (p.bytes_total) {
        detail += ' · ' + formatBytesNav(p.bytes_current || 0) + ' of ' +
          formatBytesNav(p.bytes_total);
      }
    }
    return '<div style="font-size:13px;color:var(--text-secondary);">' +
      '<span class="btn-spinner" style="display:inline-block;margin-right:7px;"></span>' +
      escapeHtml((p && p.phase) || 'Updating local folders...') +
      (detail ? '<div style="font-size:11px;color:var(--text-dim);margin-top:6px;">' + escapeHtml(detail) + '</div>' : '') +
      (p && p.current_file ? '<div style="font-family:monospace;font-size:10px;color:var(--text-ghost);margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + escapeHtml(p.current_file) + '</div>' : '') +
      '<div style="font-size:11px;color:var(--text-dim);margin-top:6px;">Progress is also available in Jobs.</div></div>';
  }

  function renderLegacy(legacy) {
    var container = document.getElementById('localWorkspaceContent');
    if (!container) return;
    if (activeJob) {
      container.innerHTML = progressMarkup();
      return;
    }
    if (!legacy || legacy.state === 'remote') {
      container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">Migrating the previous local session...</span>';
      return;
    }
    var blocked = !!(data && data.blocking_job);
    var blockerHtml = blocked
      ? '<div role="status" style="font-size:12px;color:var(--warning);line-height:1.5;margin-bottom:12px;">' +
          escapeHtml(blockingJobMessage()) + ' <a href="/jobs" style="color:inherit;text-decoration:underline;">View Jobs</a></div>'
      : '';
    var disabledAttrs = blocked ? ' disabled style="opacity:.5;"' : '';
    if (legacy.state === 'staging') {
      container.innerHTML =
        '<div style="font-size:13px;color:var(--warning);margin-bottom:10px;">An older workspace copy was interrupted before activation.</div>' +
        blockerHtml +
        '<button class="btn btn-secondary" data-legacy-action="discard"' + disabledAttrs + '>Clean Up Incomplete Copy</button>';
      return;
    }
    var changes = legacy.changes || {created: 0, modified: 0, deleted: 0};
    var recovery = legacy.state === 'recovery';
    container.innerHTML =
      '<div style="font-size:13px;color:var(--warning);margin-bottom:8px;">' +
        (recovery ? 'Finish the local session created by the previous Vireo version.' : 'This workspace is using a local copy created by the previous Vireo version.') +
      '</div>' +
      '<div style="font-size:11px;color:var(--text-dim);margin-bottom:12px;">' +
        changes.created + ' new · ' + changes.modified + ' modified · ' + changes.deleted + ' deleted</div>' +
      blockerHtml +
      '<div style="display:flex;gap:8px;flex-wrap:wrap;">' +
        '<button class="btn btn-secondary" data-legacy-action="sync"' + disabledAttrs + '>' + (recovery ? 'Finish Sync-back' : 'Finish and Sync Back') + '</button>' +
        '<button class="btn btn-secondary" style="color:var(--danger);" data-legacy-action="discard"' + disabledAttrs + '>Discard Local Changes</button>' +
      '</div>';
  }

  function render() {
    var container = document.getElementById('localWorkspaceContent');
    if (!container) return;
    if (activeJob) {
      container.innerHTML = progressMarkup();
      return;
    }
    if (!data) {
      container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">Loading...</span>';
      return;
    }
    if (data.legacy_workspace_session) {
      renderLegacy(data.legacy);
      return;
    }
    if (!data.folder_count) {
      container.innerHTML = '<div style="font-size:12px;color:var(--text-secondary);">Add at least one folder before working locally.</div>';
      return;
    }

    var local = Number(data.local_folder_count || 0);
    var total = Number(data.folder_count || 0);
    var localItems = selectedItems(null, true);
    var changes = localItems.reduce(function(sum, item) {
      var current = item.changes || {};
      sum.created += Number(current.created || 0);
      sum.modified += Number(current.modified || 0);
      sum.deleted += Number(current.deleted || 0);
      return sum;
    }, {created: 0, modified: 0, deleted: 0});
    var shared = localItems.filter(function(item) {
      return (item.workspace_ids || []).length > 1;
    }).length;
    var blocked = !!data.blocking_job;

    var summary = local === 0
      ? 'All ' + total + ' folder' + (total === 1 ? ' is' : 's are') + ' using source storage.'
      : local === total
        ? 'All ' + total + ' folder' + (total === 1 ? ' is' : 's are') + ' using local storage.'
        : local + ' of ' + total + ' folders are using local storage.';
    var html =
      '<div style="font-size:13px;color:var(--text-primary);font-weight:600;margin-bottom:4px;">' + escapeHtml(summary) + '</div>' +
      '<div style="font-size:11px;color:var(--text-dim);line-height:1.5;margin-bottom:12px;">' +
        (local ? changes.created + ' new · ' + changes.modified + ' modified · ' + changes.deleted + ' deleted' :
          'Local copies speed up work on network or slower storage.') +
        (shared ? ' · ' + shared + ' shared local folder' + (shared === 1 ? '' : 's') : '') +
      '</div>';
    if (blocked) {
      html += '<div role="status" style="font-size:12px;color:var(--warning);line-height:1.5;margin-bottom:12px;">' +
        escapeHtml(blockingJobMessage()) + ' <a href="/jobs" style="color:inherit;text-decoration:underline;">View Jobs</a></div>';
    }
    html += '<div style="display:flex;gap:8px;flex-wrap:wrap;">';
    if (local < total) {
      html += '<button class="btn btn-secondary" data-local-folders-action="stage-all"' +
        (blocked ? ' disabled style="opacity:.5;"' : '') + '>' +
        (local ? 'Make All Folders Local' : 'Work Entire Workspace Locally') + '</button>';
    }
    if (local) {
      html += '<button class="btn btn-secondary" data-local-folders-action="manage"' +
        (blocked ? ' disabled style="opacity:.5;"' : '') + '>Finish Local Work...</button>';
    }
    html += '</div>';
    container.innerHTML = html;
  }

  function publishFolderStatus() {
    window.vireoLocalFolderData = data;
    try {
      window.dispatchEvent(new CustomEvent('vireo:local-folder-status-changed', {
        detail: {data: data}
      }));
    } catch (_error) {}
  }

  function trackStartedJob(result, type) {
    if (!data || data.legacy_workspace_session || !result || !result.job_id) return;
    var jobs = (data.jobs || []).filter(function(job) {
      return job.id !== result.job_id;
    });
    jobs.push({
      id: result.job_id,
      type: type,
      folder_ids: (result.folder_ids || []).map(Number)
    });
    data.jobs = jobs;
    publishFolderStatus();
    // Workspace folder rows are rendered independently from the Work Locally
    // panel. Refresh them as soon as the server accepts a transition so the
    // button the user clicked cannot continue to look idle.
    if (typeof loadWsFolders === 'function') loadWsFolders();
  }

  function publishJobProgress(jobId, progress) {
    try {
      window.dispatchEvent(new CustomEvent('vireo:local-folder-job-progress', {
        detail: {jobId: jobId, progress: progress}
      }));
    } catch (_error) {}
  }

  function rememberCompletedSteps(watch, steps) {
    var completed = [];
    (steps || []).forEach(function(step) {
      if (!step || step.status !== 'completed') return;
      var match = /^folder-(\d+)$/.exec(String(step.id || ''));
      if (!match) return;
      var rootId = Number(match[1]);
      var key = String(rootId);
      if (watch.completedRoots[key]) return;
      var previous = watch.progressByRoot[key] || {};
      var progress = Object.assign({}, previous, step.progress || {}, {
        root_folder_id: rootId,
        current_file: step.current_file || previous.current_file || '',
        phase: (step.label || 'Folder') + ' complete'
      });
      watch.progressByRoot[key] = progress;
      watch.completedRoots[key] = true;
      completed.push(progress);
    });
    return completed;
  }

  function applyJobProgress(watch, progress) {
    if (!progress || activeJob !== watch) return;
    watch.progress = progress;
    var completed = rememberCompletedSteps(watch, progress.steps);
    var rootId = Number(progress.root_folder_id);
    if (Number.isFinite(rootId)) {
      watch.progressByRoot[String(rootId)] = progress;
    }
    render();
    completed.forEach(function(item) {
      publishJobProgress(watch.id, item);
    });
    publishJobProgress(watch.id, progress);
  }

  function watchJob(jobId) {
    if (activeJob && activeJob.id === jobId) return;
    if (activeJob && activeJob.source) activeJob.source.close();
    var watch = {
      id: jobId,
      progress: null,
      progressByRoot: {},
      completedRoots: {},
      source: null,
      eventCount: 0
    };
    activeJob = watch;
    watch.source = safeEventSource('/api/jobs/' + encodeURIComponent(jobId) + '/stream', {
      onProgress: function(progress) {
        if (activeJob !== watch) return;
        watch.eventCount += 1;
        applyJobProgress(watch, progress);
      },
      onComplete: async function(event) {
        if (activeJob !== watch) return;
        activeJob = null;
        if (event.status === 'completed') showToast('Local folder update complete', 'success');
        else if (event.status !== 'cancelled') {
          var detail = event.errors && event.errors.length ? event.errors[event.errors.length - 1] : 'The job did not complete';
          showToast(detail, 'error');
        }
        await load();
      },
      onError: async function() {
        if (activeJob !== watch) return;
        activeJob = null;
        await load();
      }
    });
    render();
    // A worker can publish its first update before the POST response gives us
    // the job ID. Wait until Flask has accepted the live subscription before
    // reading the snapshot, closing the gap where an update could otherwise
    // fall between the GET response and the EventSource connection.
    watch.source.addEventListener('open', function() {
      Vireo.api.json(
        '/api/jobs/' + encodeURIComponent(jobId), {}, {toast: false}
      ).then(function(snapshot) {
        if (activeJob !== watch || watch.eventCount || !snapshot) return;
        var progress = Object.assign({}, snapshot.progress || {}, {
          steps: snapshot.steps || []
        });
        if (progress.phase || progress.root_folder_id != null || progress.steps.length) {
          applyJobProgress(watch, progress);
        }
      }).catch(function() {});
    }, {once: true});
  }

  async function load() {
    var generation = ++loadGeneration;
    try {
      var nextData = await Vireo.api.json(
        '/api/workspaces/active/local-folders', {}, {toast: false}
      );
      if (nextData.legacy_workspace_session) {
        nextData.legacy = await Vireo.api.json(
          '/api/workspaces/active/local-workspace', {}, {toast: false}
        );
      }
      // Multiple callers can overlap (notably rapid folder-health events).
      // Only the newest request may publish or trigger job/UI side effects;
      // otherwise an older, slower response can restore obsolete ancestry
      // and recovery badges after the current tree has rendered.
      if (generation !== loadGeneration) return null;
      data = nextData;
      if (data.legacy_workspace_session) {
        if (data.legacy.job) watchJob(data.legacy.job.id);
      } else if (data.jobs && data.jobs.length) {
        watchJob(data.jobs[0].id);
      }
      if (!data.legacy_workspace_session) {
        var runningFolders = new Set();
        (data.jobs || []).forEach(function(job) {
          (job.folder_ids || []).forEach(function(id) { runningFolders.add(Number(id)); });
        });
        (data.folders || []).forEach(function(item) {
          item.job_running = runningFolders.has(Number(item.root_folder_id));
          if (item.state === 'staging' && !item.job_running) {
            item.state = 'recovery';
            item.recovery_kind = 'stage';
            item.sync_available = false;
          }
        });
      }
      publishFolderStatus();
      render();
      if (typeof loadWsFolders === 'function') loadWsFolders();
      updateStageDialogBlocker();
      scheduleBlockingJobRefresh();
      return data;
    } catch (error) {
      if (generation !== loadGeneration) return null;
      var container = document.getElementById('localWorkspaceContent');
      if (container) container.innerHTML = '<span style="color:var(--danger);font-size:13px;">' + escapeHtml(error.message || 'Failed to read local folder status') + '</span>';
      return null;
    }
  }

  async function submitStageDialog() {
    if (actionInFlight || activeJob || !pendingStageItems.length) return;
    var error = document.getElementById('stageLocalFoldersError');
    var button = document.getElementById('confirmStageLocalFolders');
    var request = stageRequestBody();
    var blocker = blockingJobForSelection(request.body.folder_ids);
    if (blocker) {
      stageBlockedByJob = true;
      if (button) button.disabled = true;
      if (error) error.textContent = blockingJobMessage(blocker);
      return;
    }
    if (!request.complete || stagePreflightSignature !== JSON.stringify(request.body)) {
      if (error) error.textContent = 'Wait for the size and free-space check to finish.';
      scheduleStagePreflight(0);
      return;
    }
    actionInFlight = true;
    if (button) button.disabled = true;
    if (error) error.textContent = '';
    try {
      var result = await Vireo.api.json('/api/workspaces/active/local-folders/stage', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(request.body)
      }, {toast: false});
      var modal = document.getElementById('stageLocalFoldersModal');
      if (modal) modal.classList.remove('open');
      pendingStageItems = [];
      watchJob(result.job_id);
      trackStartedJob(result, 'work-locally-folder-stage');
    } catch (requestError) {
      if (error) error.textContent = requestError.message || 'Could not start the local copy.';
    } finally {
      actionInFlight = false;
      if (pendingStageItems.length) scheduleStagePreflight(0);
    }
  }

  async function sync(folderIds) {
    if (actionInFlight || activeJob) return;
    await load();
    if (activeJob) return;
    var blocker = blockingJobForSelection(folderIds);
    if (blocker) {
      showToast(blockingJobMessage(blocker), 'warning');
      return;
    }
    var items = selectedItems(folderIds, true);
    if (!items.length) return;
    if (items.some(function(item) {
      return item.sync_available === false ||
        (item.state === 'recovery' && item.recovery_kind !== 'sync');
    })) {
      showToast('An incomplete local copy cannot be synced; clean it up or discard it first.', 'error');
      return;
    }
    var sessions = {};
    items.forEach(function(item) { sessions[item.root_folder_id] = item; });
    var unique = Object.keys(sessions).map(function(key) { return sessions[key]; });
    var counts = {};
    var deleted = 0;
    var affected = new Set();
    unique.forEach(function(item) {
      var count = Number(((item.changes || {}).deleted) || 0);
      counts[String(item.root_folder_id)] = count;
      deleted += count;
      (item.workspace_ids || []).forEach(function(id) { affected.add(id); });
    });
    var message = 'Sync ' + unique.length + ' local folder' + (unique.length === 1 ? '' : 's') + ' back to source storage and remove the local copies?';
    if (affected.size > 1) message += '\n\nThis updates the folders for ' + affected.size + ' linked workspaces.';
    if (deleted) message += '\n\nThis will delete ' + deleted + ' file' + (deleted === 1 ? '' : 's') + ' from source storage.';
    if (!confirm(message)) return;
    actionInFlight = true;
    try {
      var result = await Vireo.api.json('/api/workspaces/active/local-folders/sync', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          folder_ids: items.map(function(item) { return item.requested_folder_id; }),
          confirmed_deletion_counts: counts
        })
      });
      watchJob(result.job_id);
      trackStartedJob(result, 'work-locally-folder-sync');
      showToast('Sync started. Progress is shown here and in Jobs.', 'info');
    } catch (_error) {
      await load();
    } finally {
      actionInFlight = false;
    }
  }

  async function discard(folderIds) {
    if (actionInFlight || activeJob) return;
    await load();
    if (activeJob) return;
    var blocker = blockingJobForSelection(folderIds);
    if (blocker) {
      showToast(blockingJobMessage(blocker), 'warning');
      return;
    }
    var items = selectedItems(folderIds, true);
    if (!items.length) return;
    var affected = new Set();
    items.forEach(function(item) {
      (item.workspace_ids || []).forEach(function(id) { affected.add(id); });
    });
    var message = 'Discard all changes in the selected local folder' + (items.length === 1 ? '' : 's') + '?\n\nThis cannot be undone. Source storage will not be modified.';
    if (affected.size > 1) message += '\n\nThe local copies are shared by ' + affected.size + ' linked workspaces.';
    if (!confirm(message)) return;
    actionInFlight = true;
    try {
      var result = await Vireo.api.json('/api/workspaces/active/local-folders/discard', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          confirm: true,
          acknowledge_published: items.some(function(item) { return item.recovery_kind === 'sync'; }),
          folder_ids: items.map(function(item) { return item.requested_folder_id; })
        })
      });
      watchJob(result.job_id);
      trackStartedJob(result, 'work-locally-folder-discard');
    } catch (_error) {
      await load();
    } finally {
      actionInFlight = false;
    }
  }

  function openManager() {
    var modal = document.getElementById('localFoldersModal');
    var container = document.getElementById('localFoldersModalItems');
    if (!modal || !container) return;
    var sessions = {};
    selectedItems(null, true).forEach(function(item) {
      if (!sessions[item.root_folder_id]) sessions[item.root_folder_id] = item;
    });
    container.innerHTML = Object.keys(sessions).map(function(key) {
      var item = sessions[key];
      var changes = item.changes || {created: 0, modified: 0, deleted: 0};
      var shared = (item.workspace_ids || []).length;
      return '<label class="setting-row" style="display:flex;align-items:flex-start;gap:9px;cursor:pointer;">' +
        '<input type="checkbox" data-local-folder-selection value="' + item.requested_folder_id + '" checked style="margin-top:3px;accent-color:var(--accent);">' +
        '<span style="min-width:0;flex:1;">' +
          '<span style="display:block;font-family:monospace;font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + escapeAttr(item.display_path || item.source_path || '') + '">' + escapeHtml(item.display_path || item.source_path || '') + '</span>' +
          '<span style="display:block;font-size:10px;color:var(--text-dim);margin-top:3px;">' +
            changes.created + ' new · ' + changes.modified + ' modified · ' + changes.deleted + ' deleted' +
            (shared > 1 ? ' · shared by ' + shared + ' workspaces' : '') +
          '</span>' +
        '</span></label>';
    }).join('');
    modal.classList.add('open');
  }

  function closeManager() {
    var modal = document.getElementById('localFoldersModal');
    if (modal) modal.classList.remove('open');
  }

  function managerSelection() {
    return Array.from(document.querySelectorAll('[data-local-folder-selection]:checked')).map(function(input) {
      return Number(input.value);
    });
  }

  async function legacyAction(action) {
    if (!data || !data.legacy || actionInFlight || activeJob) return;
    await load();
    if (!data || !data.legacy || actionInFlight || activeJob) return;
    if (data.blocking_job) {
      showToast(blockingJobMessage(), 'warning');
      return;
    }
    var legacy = data.legacy;
    var endpoint = action === 'sync' ? 'sync' : 'discard';
    var body;
    if (action === 'sync') {
      var deleted = Number(((legacy.changes || {}).deleted) || 0);
      if (!confirm('Sync the older local workspace back to source storage?' + (deleted ? '\n\nThis deletes ' + deleted + ' source file(s).' : ''))) return;
      body = {confirm_deletions: deleted > 0, confirmed_deletion_count: deleted};
    } else {
      if (!confirm('Discard the older local workspace? Source storage will not be modified.')) return;
      body = {
        confirm: true,
        expected_state: legacy.state === 'recovery' ? 'recovery' : legacy.state,
        acknowledge_published: legacy.recovery_kind === 'sync'
      };
    }
    actionInFlight = true;
    try {
      var result = await Vireo.api.json('/api/workspaces/active/local-workspace/' + endpoint, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
      });
      watchJob(result.job_id);
    } catch (_error) {
      await load();
    } finally {
      actionInFlight = false;
    }
  }

  var panel = document.getElementById('localWorkspaceContent');
  if (panel) {
    panel.addEventListener('click', function(event) {
      var button = event.target.closest('[data-local-folders-action], [data-legacy-action]');
      if (!button || button.disabled) return;
      var action = button.dataset.localFoldersAction;
      if (action === 'stage-all') openStageDialog(null);
      else if (action === 'manage') openManager();
      else if (button.dataset.legacyAction) legacyAction(button.dataset.legacyAction);
    });
  }

  var closeManagerButton = document.getElementById('closeLocalFoldersModal');
  if (closeManagerButton) closeManagerButton.addEventListener('click', closeManager);
  var cancelStageButton = document.getElementById('cancelStageLocalFolders');
  if (cancelStageButton) cancelStageButton.addEventListener('click', closeStageDialog);
  var confirmStageButton = document.getElementById('confirmStageLocalFolders');
  if (confirmStageButton) confirmStageButton.addEventListener('click', submitStageDialog);
  var stageItems = document.getElementById('stageLocalFoldersModalItems');
  if (stageItems) {
    stageItems.addEventListener('input', function(event) {
      if (event.target.matches('[data-local-destination-base]')) {
        updateStageDestinationPreview(event.target);
        scheduleStagePreflight();
      }
    });
    stageItems.addEventListener('click', async function(event) {
      var button = event.target.closest('[data-browse-local-destination]');
      if (!button) return;
      var id = button.dataset.browseLocalDestination;
      var input = document.querySelector('[data-local-destination-base="' + id + '"]');
      if (!input || typeof pickDirectory !== 'function') return;
      var chosen = await pickDirectory('Choose a location for ' + input.dataset.folderName, {
        defaultPath: input.value.trim()
      });
      if (!chosen || Array.isArray(chosen)) return;
      input.value = chosen;
      updateStageDestinationPreview(input);
      scheduleStagePreflight(0);
    });
  }
  var syncSelectedButton = document.getElementById('syncSelectedLocalFolders');
  if (syncSelectedButton) {
    syncSelectedButton.addEventListener('click', function() {
      var selected = managerSelection();
      if (!selected.length) return;
      closeManager();
      sync(selected);
    });
  }
  var discardSelectedButton = document.getElementById('discardSelectedLocalFolders');
  if (discardSelectedButton) {
    discardSelectedButton.addEventListener('click', function() {
      var selected = managerSelection();
      if (!selected.length) return;
      closeManager();
      discard(selected);
    });
  }

  window.addEventListener('pagehide', function() {
    cancelStagePreflight(true);
  });
  window.addEventListener('pageshow', function(event) {
    var modal = document.getElementById('stageLocalFoldersModal');
    if (
      event.persisted &&
      modal && modal.classList.contains('open') &&
      pendingStageItems.length &&
      !stageBlockedByJob &&
      stagePreflightSignature === null
    ) {
      scheduleStagePreflight(0);
    }
  });

  async function stageFromAnywhere(folderId) {
    if (!data) await load();
    if (activeJob || (data && data.legacy_workspace_session)) {
      window.location.href = '/workspace#work-locally';
      return false;
    }
    var item = statusFor(Number(folderId));
    if (!item || item.state !== 'remote') {
      window.location.href = '/workspace#work-locally';
      return false;
    }
    openStageDialog([Number(folderId)]);
    return true;
  }

  window.vireoLocalFolders = {
    load: load,
    statusFor: statusFor,
    jobFor: jobFor,
    blockingJob: function() { return data && data.blocking_job; },
    blockingJobFor: blockingJobForFolder,
    blockingMessage: blockingJobMessage,
    stage: stageFromAnywhere,
    sync: function(folderId) { return sync([Number(folderId)]); },
    discard: function(folderId) { return discard([Number(folderId)]); }
  };

  load();
})();
