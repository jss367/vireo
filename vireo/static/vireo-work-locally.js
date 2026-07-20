(function initFolderScopedWorkLocally() {
  'use strict';

  var data = null;
  var activeJob = null;
  var actionInFlight = false;
  var pendingStageItems = [];

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

  function closeStageDialog() {
    if (actionInFlight) return;
    var modal = document.getElementById('stageLocalFoldersModal');
    if (modal) modal.classList.remove('open');
    pendingStageItems = [];
  }

  function openStageDialog(folderIds) {
    if (actionInFlight || activeJob) return;
    var items = selectedItems(folderIds, false).filter(function(item) {
      return item.state === 'remote';
    });
    if (!items.length) return;
    pendingStageItems = items;
    var container = document.getElementById('stageLocalFoldersModalItems');
    var error = document.getElementById('stageLocalFoldersError');
    var modal = document.getElementById('stageLocalFoldersModal');
    if (!container || !modal) return;
    if (error) error.textContent = '';
    container.innerHTML = items.map(function(item) {
      var id = Number(item.requested_folder_id);
      var source = item.source_path || item.display_path || '';
      var name = folderName(source);
      var localName = item.local_folder_name || name;
      var base = item.default_local_base || '';
      var finalPath = item.default_local_path || joinPath(base, localName);
      var shared = (item.workspace_ids || []).length;
      return '<div class="setting-row" style="display:block;padding:12px 0;">' +
        '<div style="font-size:13px;font-weight:600;color:var(--text-primary);margin-bottom:3px;">' + escapeHtml(name) + '</div>' +
        '<div style="font-family:monospace;font-size:10px;color:var(--text-dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-bottom:9px;" title="' + escapeAttr(source) + '">' + escapeHtml(source) + '</div>' +
        '<label class="modal-label" for="localDestinationBase-' + id + '">Create the local copy in</label>' +
        '<div style="display:flex;gap:7px;align-items:center;">' +
          '<input class="modal-input" id="localDestinationBase-' + id + '" data-local-destination-base="' + id + '" data-folder-name="' + escapeAttr(name) + '" data-local-folder-name="' + escapeAttr(localName) + '" value="' + escapeAttr(base) + '" style="flex:1;font-family:monospace;font-size:12px;">' +
          '<button class="modal-btn" data-browse-local-destination="' + id + '" style="white-space:nowrap;">Browse...</button>' +
        '</div>' +
        '<div style="font-size:10px;color:var(--text-dim);margin-top:5px;">Local folder: <span data-local-destination-preview="' + id + '" style="font-family:monospace;">' + escapeHtml(finalPath) + '</span></div>' +
        (shared > 1 ? '<div style="font-size:10px;color:var(--accent);margin-top:4px;">This copy will be shared by ' + shared + ' linked workspaces.</div>' : '') +
      '</div>';
    }).join('');
    modal.classList.add('open');
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
    if (legacy.state === 'staging') {
      container.innerHTML =
        '<div style="font-size:13px;color:var(--warning);margin-bottom:10px;">An older workspace copy was interrupted before activation.</div>' +
        '<button class="btn btn-secondary" data-legacy-action="discard">Clean Up Incomplete Copy</button>';
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
      '<div style="display:flex;gap:8px;flex-wrap:wrap;">' +
        '<button class="btn btn-secondary" data-legacy-action="sync">' + (recovery ? 'Finish Sync-back' : 'Finish and Sync Back') + '</button>' +
        '<button class="btn btn-secondary" style="color:var(--danger);" data-legacy-action="discard">Discard Local Changes</button>' +
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
      '</div><div style="display:flex;gap:8px;flex-wrap:wrap;">';
    if (local < total) {
      html += '<button class="btn btn-secondary" data-local-folders-action="stage-all">' +
        (local ? 'Make All Folders Local' : 'Work Entire Workspace Locally') + '</button>';
    }
    if (local) {
      html += '<button class="btn btn-secondary" data-local-folders-action="manage">Finish Local Work...</button>';
    }
    html += '</div>';
    container.innerHTML = html;
  }

  function watchJob(jobId) {
    if (activeJob && activeJob.id === jobId) return;
    if (activeJob && activeJob.source) activeJob.source.close();
    var watch = {id: jobId, progress: null, source: null};
    activeJob = watch;
    watch.source = safeEventSource('/api/jobs/' + encodeURIComponent(jobId) + '/stream', {
      onProgress: function(progress) {
        if (activeJob !== watch) return;
        watch.progress = progress;
        render();
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
  }

  async function load() {
    try {
      data = await Vireo.api.json('/api/workspaces/active/local-folders', {}, {toast: false});
      if (data.legacy_workspace_session) {
        data.legacy = await Vireo.api.json('/api/workspaces/active/local-workspace', {}, {toast: false});
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
      window.vireoLocalFolderData = data;
      render();
      if (typeof loadWsFolders === 'function') loadWsFolders();
      return data;
    } catch (error) {
      var container = document.getElementById('localWorkspaceContent');
      if (container) container.innerHTML = '<span style="color:var(--danger);font-size:13px;">' + escapeHtml(error.message || 'Failed to read local folder status') + '</span>';
      return null;
    }
  }

  async function submitStageDialog() {
    if (actionInFlight || activeJob || !pendingStageItems.length) return;
    var error = document.getElementById('stageLocalFoldersError');
    var button = document.getElementById('confirmStageLocalFolders');
    var destinationBases = {};
    for (var i = 0; i < pendingStageItems.length; i += 1) {
      var id = Number(pendingStageItems[i].requested_folder_id);
      var input = document.querySelector('[data-local-destination-base="' + id + '"]');
      var destination = input ? input.value.trim() : '';
      if (!destination) {
        if (error) error.textContent = 'Choose a destination for every local copy.';
        if (input) input.focus();
        return;
      }
      destinationBases[String(id)] = destination;
    }
    actionInFlight = true;
    if (button) button.disabled = true;
    if (error) error.textContent = '';
    try {
      var result = await Vireo.api.json('/api/workspaces/active/local-folders/stage', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          folder_ids: pendingStageItems.map(function(item) { return item.requested_folder_id; }),
          destination_bases: destinationBases
        })
      });
      var modal = document.getElementById('stageLocalFoldersModal');
      if (modal) modal.classList.remove('open');
      pendingStageItems = [];
      watchJob(result.job_id);
    } catch (requestError) {
      if (error) error.textContent = requestError.message || 'Could not start the local copy.';
    } finally {
      actionInFlight = false;
      if (button) button.disabled = false;
    }
  }

  async function sync(folderIds) {
    if (actionInFlight || activeJob) return;
    await load();
    if (activeJob) return;
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

  document.getElementById('localWorkspaceContent').addEventListener('click', function(event) {
    var button = event.target.closest('[data-local-folders-action], [data-legacy-action]');
    if (!button || button.disabled) return;
    var action = button.dataset.localFoldersAction;
    if (action === 'stage-all') openStageDialog(null);
    else if (action === 'manage') openManager();
    else if (button.dataset.legacyAction) legacyAction(button.dataset.legacyAction);
  });

  document.getElementById('closeLocalFoldersModal').addEventListener('click', closeManager);
  document.getElementById('cancelStageLocalFolders').addEventListener('click', closeStageDialog);
  document.getElementById('confirmStageLocalFolders').addEventListener('click', submitStageDialog);
  document.getElementById('stageLocalFoldersModalItems').addEventListener('input', function(event) {
    if (event.target.matches('[data-local-destination-base]')) {
      updateStageDestinationPreview(event.target);
    }
  });
  document.getElementById('stageLocalFoldersModalItems').addEventListener('click', async function(event) {
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
  });
  document.getElementById('syncSelectedLocalFolders').addEventListener('click', function() {
    var selected = managerSelection();
    if (!selected.length) return;
    closeManager();
    sync(selected);
  });
  document.getElementById('discardSelectedLocalFolders').addEventListener('click', function() {
    var selected = managerSelection();
    if (!selected.length) return;
    closeManager();
    discard(selected);
  });

  window.vireoLocalFolders = {
    load: load,
    statusFor: statusFor,
    stage: function(folderId) { return openStageDialog([Number(folderId)]); },
    sync: function(folderId) { return sync([Number(folderId)]); },
    discard: function(folderId) { return discard([Number(folderId)]); }
  };

  load();
})();
