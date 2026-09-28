/* Browse: folder tree sidebar and folder local/archive status.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Folder Tree ---------- */
function reconcileFolderLoadRenders() {
  // Do not render an older successful response while any newer request is
  // still pending. Once every newer request has failed, fall back to the
  // newest success instead of leaving the pre-transition tree in the DOM.
  // This handles both completion orders: success-before-failure and
  // failure-before-success.
  var gen = folderLoadGen;
  while (gen > folderRenderDecisionGen) {
    var state = folderLoadStates[gen];
    if (!state || state.status === 'pending') return;
    if (state.status === 'success') {
      var shouldRender = !state.shouldRender || state.shouldRender();
      folderRenderDecisionGen = gen;
      if (shouldRender) {
        // Update browseWorkspaceId in lockstep with the rendered rows so
        // the destructive Remove action always targets the workspace the
        // user actually sees.
        if (state.workspaceId != null) browseWorkspaceId = state.workspaceId;
        renderFolderTree(state.data);
      }
      Object.keys(folderLoadStates).forEach(function(key) {
        if (Number(key) <= folderLoadGen) delete folderLoadStates[key];
      });
      return;
    }
    gen--;
  }
}

async function loadFolders(opts) {
  var myGen = ++folderLoadGen;
  folderLoadStates[myGen] = {
    status: 'pending',
    shouldRender: opts && opts.shouldRender
  };
  try {
    // One request returns both the tree and the workspace it was scoped
    // to, so ``browseWorkspaceId`` moves in lockstep with
    // ``browseFolderRows`` no matter what another tab does in between.
    // A parallel ``/api/workspaces/active`` call would leave a window
    // where /api/folders and /api/workspaces/active can disagree (Codex
    // review r3799038685); it would also drag the expensive per-root
    // workspace_photo_count query into every sidebar refresh (Codex
    // review r3799038688).
    var payload = await safeFetch(
      '/api/folders?with_workspace=1', {}, { toast: false }
    );
    var data = (payload && payload.folders) || [];
    var state = folderLoadStates[myGen];
    if (!state) return data;
    state.status = 'success';
    state.data = data;
    state.workspaceId = (payload && payload.active_workspace_id != null)
      ? Number(payload.active_workspace_id) : null;
    reconcileFolderLoadRenders();
    return data;
  } catch(e) {
    var failedState = folderLoadStates[myGen];
    if (failedState) {
      failedState.status = 'failure';
      reconcileFolderLoadRenders();
    }
    // Signal failure to callers that gate destructive decisions (e.g. the
    // health refresh clearing ``activeFolderId``) on a successful fetch.
    // A DOM/cache-based check after a swallowed failure sees the stale
    // tree and preserves a folder that just went missing, then reloads
    // its now-unavailable scope into an empty grid — the advanced navbar
    // snapshot means later polls see no transition to repair it
    // (Codex review r3687062925).
    return null;
  }
}

function renderFolderTree(folders) {
  var rows = folders.slice();
  var byParent = {};
  rows.forEach(function(f) {
    var pid = f.parent_id || 'root';
    if (!byParent[pid]) byParent[pid] = [];
    byParent[pid].push(f);
  });

  // Ask folderLocalStatuses for any phantom top-level rows it had to
  // synthesize because a missing local root has no visible ancestor. These
  // rows carry the LOCAL ISSUE / SYNCING badge for a folder /api/folders no
  // longer returns; without injecting them the badge has nowhere to
  // render.
  var statusResult = folderLocalStatuses(rows, byParent, {wantsSynthetic: true});
  (statusResult.synthetic || []).forEach(function(row) {
    if (rows.some(function(f) { return Number(f.id) === Number(row.id); })) return;
    rows.push(row);
    if (!byParent['root']) byParent['root'] = [];
    byParent['root'].push(row);
  });
  browseFolderRows = rows;

  // Single post-order pass: each subtree's rollup is computed once and cached.
  // (Per-node recomputation during render would be O(n^2) on deep trees.)
  var rolledUp = {};
  function computeRollup(f) {
    var total = f.photo_count || 0;
    (byParent[f.id] || []).forEach(function(c) { total += computeRollup(c); });
    rolledUp[f.id] = total;
    return total;
  }
  (byParent['root'] || []).forEach(computeRollup);
  var localStatuses = statusResult.statuses;
  var archiveStatuses = pendingArchiveFolderStatuses();

  function buildTree(parentId, depth) {
    var children = byParent[parentId] || [];
    var html = '';
    children.forEach(function(f) {
      var hasChildren = byParent[f.id] && byParent[f.id].length > 0;
      var indent = '';
      for (var i = 0; i < depth; i++) indent += '<span class="tree-indent"></span>';
      var toggle = hasChildren ? '<span class="tree-toggle" onclick="toggleTree(event,this)">&#9654;</span>' : '<span class="tree-indent"></span>';
      var activeClass = activeFolderId === f.id ? ' active' : '';
      var partialBadge = f.status === 'partial'
        ? '<span class="folder-status-partial" title="Scan did not complete — re-scan to finish">partial</span>'
        : '';
      html += '<div class="tree-item' + activeClass + '" onclick="filterByFolder(' + f.id + ')" data-folder-id="' + f.id + '" data-workspace-root="' + (f.is_workspace_root ? '1' : '0') + '">' +
        indent + toggle +
        // A folder row shows only its leaf name, which can be as opaque as
        // "12" for a date-templated import. The full path on hover is the
        // cheapest way to say which directory the row actually is.
        '<span class="folder-name"' + (f.path ? ' title="' + escapeAttr(f.path) + '"' : '') + '>' +
          escapeHtml(f.name) + '</span>' +
        '<span class="folder-local-status-slot">' + folderLocalStatusMarkup(localStatuses[f.id]) + '</span>' +
        '<span class="folder-archive-status-slot">' + folderLocalStatusMarkup(archiveStatuses[f.id]) + '</span>' +
        partialBadge +
        '<span class="count">' + rolledUp[f.id] + '</span>' +
      '</div>';
      if (hasChildren) {
        html += '<div class="tree-children">' + buildTree(f.id, depth + 1) + '</div>';
      }
    });
    return html;
  }

  document.getElementById('folderTree').innerHTML = buildTree('root', 0);
}

function folderLocalStatuses(folders, byParent, options) {
  var wantsSynthetic = !!(options && options.wantsSynthetic);
  var localData = window.vireoLocalFolderData;
  if (!localData || localData.legacy_workspace_session) {
    return wantsSynthetic ? {statuses: {}, synthetic: []} : {};
  }

  var rowsById = {};
  folders.forEach(function(folder) { rowsById[Number(folder.id)] = folder; });
  var direct = {};
  // Job payloads report their target as the local root_folder_id, but a
  // visible workspace folder may be covered by a shared ancestor whose
  // root_folder_id isn't itself in this workspace's tree. Map each covering
  // root back to the visible requested_folder_id(s) so sync/discard badges
  // update instead of freezing on LOCAL. Each entry carries ``fallback``
  // so job status can inherit whether the visible id is the covering root
  // itself (direct — spread through the subtree) or a fallback anchor
  // standing in for a hidden session (do not spread).
  var visibleIdsByRoot = {};
  var syntheticRoots = [];
  var syntheticIds = {};

  function recordMapping(rootId, visibleId, isFallback) {
    if (!rootId) return;
    if (!visibleIdsByRoot[rootId]) visibleIdsByRoot[rootId] = [];
    for (var i = 0; i < visibleIdsByRoot[rootId].length; i++) {
      if (visibleIdsByRoot[rootId][i].visibleId === visibleId) {
        // A direct match on this visible id trumps a fallback mapping: a
        // job on the covering root then propagates to the folder's own
        // subtree instead of stopping at the fallback anchor.
        if (!isFallback) visibleIdsByRoot[rootId][i].fallback = false;
        return;
      }
    }
    visibleIdsByRoot[rootId].push({visibleId: visibleId, fallback: isFallback});
  }

  (localData.folders || []).forEach(function(item) {
    if (!item) return;
    var requestedId = Number(item.requested_folder_id || item.root_folder_id);
    var visibleId = requestedId;
    var isFallback = false;
    if (!rowsById[visibleId]) {
      // The requested folder isn't in the tree — commonly because
      // check_folder_health flipped its rebased folders.path to 'missing'
      // when the managed local directory was unmounted or deleted, so
      // /api/folders now excludes it. Dropping the item here loses the
      // LOCAL ISSUE / SYNCING badge exactly when the local copy needs
      // attention. Fall back to the nearest visible ancestor so the
      // status surfaces on the enclosing folder instead.
      var ancestorId = Number(item.visible_ancestor_folder_id || 0);
      if (ancestorId && rowsById[ancestorId]) {
        visibleId = ancestorId;
        isFallback = true;
      } else if (item.state === 'remote') {
        // A purely remote workspace root that /api/folders doesn't
        // return (unmounted, never staged) has nothing local to badge.
        // Synthesizing a phantom here would restore every such root as
        // a clickable zero-count folder with no local-status badge,
        // effectively resurrecting missing remote folders in Browse
        // (Codex review r3792082330). Skip it entirely.
        return;
      } else {
        // A top-level workspace root that has gone missing has no
        // visible ancestor to attach the badge to. Without a synthesized
        // entry the recovery state disappears entirely — the user loses
        // the only signal that their local copy needs attention. Emit a
        // phantom top-level row so renderFolderTree can inject a
        // ``.tree-item`` that carries the LOCAL ISSUE / SYNCING badge
        // (Codex review r3792031813).
        if (!syntheticIds[requestedId]) {
          syntheticIds[requestedId] = true;
          var syntheticName = String(item.folder_name || '').trim() ||
            'Missing local folder';
          // Seed the synthesized row with the root's real workspace photo
          // count from workspace_status(). Hard-coding 0 made the only
          // visible entry for a top-level missing local root read as an
          // empty folder even when the recovery session covers hundreds of
          // photos, which mis-cues users toward discarding it (Codex review
          // r3792132683).
          var syntheticCount = Number(item.workspace_photo_count || 0);
          var syntheticRow = {
            id: requestedId,
            parent_id: null,
            name: syntheticName,
            photo_count: syntheticCount,
            status: 'missing',
            __synthetic_missing_local: true
          };
          syntheticRoots.push(syntheticRow);
          rowsById[requestedId] = syntheticRow;
        }
        visibleId = requestedId;
      }
    }
    var rootId = Number(item.root_folder_id || requestedId);
    recordMapping(rootId, visibleId, isFallback);
    if (item.state === 'remote') return;
    var changes = item.changes || {};
    var changeCount = Number(changes.created || 0) +
      Number(changes.modified || 0) + Number(changes.deleted || 0);
    if (item.state === 'recovery' || item.changes_error) {
      direct[visibleId] = {
        kind: 'recovery',
        label: 'LOCAL ISSUE',
        description: item.recovery_kind === 'sync'
          ? 'Local sync needs attention'
          : 'Local copy needs attention',
        fallback: isFallback
      };
      return;
    }
    direct[visibleId] = {
      kind: item.state === 'staging' ? 'updating' : 'local',
      label: item.state === 'staging' ? 'COPYING' : 'LOCAL',
      description: changeCount
        ? 'Working locally · ' + changeCount + ' unsynced change' + (changeCount === 1 ? '' : 's')
        : 'Working locally',
      fallback: isFallback
    };
  });

  (localData.jobs || []).forEach(function(job) {
    var jobStatus = {
      'work-locally-folder-stage': ['COPYING', 'Copying locally'],
      'work-locally-folder-sync': ['SYNCING', 'Syncing local changes to source storage'],
      'work-locally-folder-discard': ['REMOVING', 'Removing the local copy']
    }[job.type];
    if (!jobStatus) return;
    (job.folder_ids || []).forEach(function(rawId) {
      var id = Number(rawId);
      var targets = [];
      if (rowsById[id]) targets.push({visibleId: id, fallback: false});
      (visibleIdsByRoot[id] || []).forEach(function(mapping) {
        for (var ti = 0; ti < targets.length; ti++) {
          if (targets[ti].visibleId === mapping.visibleId) {
            if (!mapping.fallback) targets[ti].fallback = false;
            return;
          }
        }
        targets.push({visibleId: mapping.visibleId, fallback: mapping.fallback});
      });
      targets.forEach(function(target) {
        direct[target.visibleId] = {
          kind: 'updating',
          label: jobStatus[0],
          description: jobStatus[1],
          fallback: target.fallback
        };
      });
    });
  });

  var result = {};
  function markDescendants(folderId, status) {
    result[folderId] = status;
    // Fallback anchors represent a hidden subtree that could not be
    // shown. Propagating the anchor's status through unrelated visible
    // siblings would mislabel healthy remote folders as LOCAL ISSUE /
    // SYNCING / REMOVING (Codex review r3792031821).
    if (status && status.fallback) return;
    (byParent[folderId] || []).forEach(function(child) {
      markDescendants(Number(child.id), status);
    });
  }
  Object.keys(direct).forEach(function(rawId) {
    markDescendants(Number(rawId), direct[rawId]);
  });

  Object.keys(direct).forEach(function(rawId) {
    var current = rowsById[Number(rawId)];
    var parentId = current ? current.parent_id : null;
    var seen = {};
    while (parentId != null && !seen[parentId]) {
      seen[parentId] = true;
      if (!direct[parentId]) {
        result[parentId] = {
          kind: 'mixed',
          label: 'SOME LOCAL',
          description: 'Contains folders that are working locally'
        };
      }
      current = rowsById[Number(parentId)];
      parentId = current ? current.parent_id : null;
    }
  });
  if (wantsSynthetic) {
    return {statuses: result, synthetic: syntheticRoots};
  }
  return result;
}

function pendingArchiveFolderStatuses() {
  // "Kept locally" photos are an import that was processed in Vireo's
  // staging directory and has not been copied to its destination yet. The
  // staging tree is in the catalog like any other folder, so without this
  // badge the sidebar row is indistinguishable from a folder that already
  // lives on the destination storage.
  var items = window.vireoPendingArchives;
  if (!Array.isArray(items)) return {};
  var statuses = {};
  items.forEach(function(item) {
    if (!item || !Array.isArray(item.folder_ids)) return;
    var sending = item.state === 'sending';
    var destination = item.destination
      ? ' to ' + item.destination
      : ' to its destination';
    var description = sending
      ? 'Kept locally — being copied' + destination + ' now'
      : 'Kept locally — still in Vireo’s staging folder, not copied' +
        destination + ' yet. Use "Photos kept locally" at the top of this page to send them.';
    item.folder_ids.forEach(function(rawId) {
      statuses[Number(rawId)] = {
        kind: sending ? 'pending-archive updating' : 'pending-archive',
        label: sending ? 'SENDING' : 'KEPT LOCALLY',
        description: description
      };
    });
  });
  return statuses;
}

function refreshPendingArchiveStatusIndicators() {
  var statuses = pendingArchiveFolderStatuses();
  document.querySelectorAll('#folderTree .tree-item[data-folder-id]').forEach(function(row) {
    var slot = row.querySelector('.folder-archive-status-slot');
    if (slot) slot.innerHTML = folderLocalStatusMarkup(statuses[Number(row.dataset.folderId)]);
  });
}

window.addEventListener('vireo:pending-archives-changed', function() {
  refreshPendingArchiveStatusIndicators();
});

function folderLocalStatusMarkup(status) {
  if (!status) return '';
  return '<span class="folder-local-status ' + status.kind + '" role="img"' +
    ' aria-label="' + escapeAttr(status.description) + '"' +
    ' title="' + escapeAttr(status.description) + '">' +
    escapeHtml(status.label) + '</span>';
}

function refreshFolderLocalStatusIndicators() {
  // Compute statuses from real rows only. If the previously injected
  // phantoms remain in the input, folderLocalStatuses treats them as
  // existing rows and never re-emits them in ``synthetic``, so a still-
  // needed phantom would look identical to a stale one left behind after
  // a recovery discard. Rebuild the input so ``synthetic`` reliably
  // enumerates exactly the phantoms the current local-folder state
  // requires (Codex review r3792082333).
  var realFolders = browseFolderRows.filter(function(f) {
    return !f.__synthetic_missing_local;
  });
  var byParent = {};
  realFolders.forEach(function(folder) {
    var parentId = folder.parent_id || 'root';
    if (!byParent[parentId]) byParent[parentId] = [];
    byParent[parentId].push(folder);
  });
  var statusResult = folderLocalStatuses(
    realFolders, byParent, {wantsSynthetic: true}
  );
  var neededPhantomIds = {};
  (statusResult.synthetic || []).forEach(function(row) {
    neededPhantomIds[Number(row.id)] = true;
  });
  var existingPhantomIds = {};
  browseFolderRows.forEach(function(f) {
    if (f.__synthetic_missing_local) existingPhantomIds[Number(f.id)] = true;
  });
  var newPhantomNeeded = Object.keys(neededPhantomIds).some(function(id) {
    return !existingPhantomIds[id];
  });
  var stalePhantomPresent = Object.keys(existingPhantomIds).some(function(id) {
    return !neededPhantomIds[id];
  });
  // A stale phantom's underlying folder is no longer flagged as a
  // missing local session — a discard-recovery, for example, ends the
  // session and the real folder returns to ``/api/folders``. Re-fetch
  // so the restored row and photo count replace the zero-count
  // ``status: 'missing'`` shell instead of leaving Browse with an
  // orphaned clickable phantom (Codex review r3792082333).
  if (stalePhantomPresent && typeof loadFolders === 'function') {
    loadFolders();
    return;
  }
  // A newly needed phantom means the DOM has no ``.tree-item`` slot to
  // update — a slot-only refresh would silently drop the LOCAL ISSUE
  // badge. Rebuild from real rows and let renderFolderTree re-inject
  // the phantom.
  if (newPhantomNeeded) {
    renderFolderTree(realFolders);
    return;
  }
  var statuses = statusResult.statuses;
  document.querySelectorAll('#folderTree .tree-item[data-folder-id]').forEach(function(row) {
    var slot = row.querySelector('.folder-local-status-slot');
    if (slot) slot.innerHTML = folderLocalStatusMarkup(statuses[Number(row.dataset.folderId)]);
  });
}

window.addEventListener('vireo:local-folder-status-changed', function() {
  refreshFolderLocalStatusIndicators();
});

function toggleTree(e, el) {
  e.stopPropagation();
  var next = el.closest('.tree-item').nextElementSibling;
  if (next && next.classList.contains('tree-children')) {
    next.classList.toggle('open');
    el.innerHTML = next.classList.contains('open') ? '&#9660;' : '&#9654;';
  }
}

function filterByFolder(folderId) {
  browseScopeGen++;
  if (activeFolderId === folderId) {
    activeFolderId = null;
  } else {
    activeFolderId = folderId;
  }
  // If the current keyword highlight came from a sidebar click,
  // filterByKeyword also installed a VireoFilter 'keyword' rule — that
  // rule is now the sidebar's stored state, not activeKeyword. Clearing
  // just activeKeyword drops the highlight but leaves the rule in place,
  // so the folder reload silently applies folder ∩ keyword. Drop the
  // rule before the reload; VireoFilter.removeField fires onChange,
  // which runs the reload path (including timelineMode + summary), so
  // skip the plain reloadBrowseResults() branch only when removeField
  // actually removed something. If the user already dropped the rule
  // via the filter chip, activeKeyword lingers but there's no keyword
  // rule left to remove — removeField returns false and we still need
  // to run our own reload for the folder change.
  var hadSidebarKeyword = activeKeyword !== null;
  activeKeyword = null;
  activeCollectionId = null;
  // Sidebar scope changes leave the "reopened collection" association
  // stale: without this, tagging a keyword next fires
  // refreshActiveCollectionAfterMembershipChange, which sees
  // openedCollectionId still set and calls filterByCollection() —
  // filterByCollection clears activeFolderId, unexpectedly replacing
  // the user's folder-scoped view with the original collection
  // (Codex review r3622204141). filterByKeyword goes through
  // VireoFilter.addRule → onChange → the openedCollectionId clear at
  // browse init, but filterByFolder's reloadBrowseResults() path
  // bypasses VireoFilter, so clear it explicitly here.
  openedCollectionId = null;
  clearOfflineCollectionState();
  document.querySelectorAll('#folderTree .tree-item').forEach(function(el) {
    el.classList.toggle('active', parseInt(el.dataset.folderId) === activeFolderId);
  });
  if (hadSidebarKeyword && window.VireoFilter && VireoFilter.isReady()) {
    if (VireoFilter.removeField('keyword', { reason: 'scopeChanged' })) return;
  }
  if (timelineMode) loadCalendarData();
  reloadBrowseResults();
}
