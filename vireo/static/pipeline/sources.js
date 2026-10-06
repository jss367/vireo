// Source mode, collection picker, folder scope list, and the source summary.
// Classic page script; load boot.js after all definitions.

// -- Collection picker --
async function loadCollections() {
  var sel = document.getElementById('collectionPicker');
  try {
    var collections = await safeFetch('/api/collections', {}, { toast: false });
    if (!Array.isArray(collections)) return;
    collections.forEach(function(c) {
      // Two reasons to disable an option:
      //   count_error — /api/collections could not resolve this collection's
      //   rules, so the downstream /photos endpoint would 500 and the
      //   pipeline would advertise a source it can't actually run.
      //   has_visual — visual collections resolve through the filter bar's
      //   visual clause, but the pipeline consumes them via
      //   get_collection_photos (rules-only). Selecting one here would
      //   silently scope the run to every metadata-matching photo instead
      //   of the visually-matched subset. Open it from Browse instead.
      var disabled = (c.count_error || c.has_visual) ? ' disabled' : '';
      var label = escapeHtml(c.name);
      var title = '';
      if (c.count_error) {
        label += ' (unavailable — edit rules to fix)';
        title = ' title="This collection\'s rules could not be resolved. Edit it in Browse to make it usable."';
      } else if (c.has_visual) {
        label += ' (visual — open in Browse)';
        title = ' title="Visual collections can only be used from Browse, where the filter bar resolves the visual-search clause."';
      }
      sel.innerHTML += '<option value="' + c.id + '"' + disabled + title + '>' + label + '</option>';
    });
  } catch(e) {
    // Don't fail silently: an empty picker used to look like "no collections"
    // even when the endpoint was 500ing. Surface it so the cause is visible.
    console.error('Failed to load collections:', e);
    if (typeof showToast === 'function') {
      showToast('Could not load collections — see console for details', 'error');
    }
  }
}

// -- Card 1: Source --
var _prefillFolderPaths = [];

function selectedFolderIds() {
  var ids = [];
  document.querySelectorAll('#folderScopeList input[type=checkbox]:checked')
    .forEach(function(cb) { ids.push(parseInt(cb.value, 10)); });
  return ids;
}

function hasPipelineSource() {
  if (_sourceMode === 'folders') return selectedFolderIds().length > 0;
  var picker = document.getElementById('collectionPicker');
  return _sourceMode === 'collection' && !!(picker && picker.value);
}

async function loadFolderScopeList() {
  var list = document.getElementById('folderScopeList');
  if (!list) return;
  try {
    var ws = await safeFetch('/api/workspaces/active', {}, { toast: false });
    var folders = await safeFetch(
      '/api/workspaces/' + ws.id + '/folders', {}, { toast: false });
    if (!Array.isArray(folders)) return;
    var byId = {};
    folders.forEach(function(f) { byId[f.id] = f; });
    // Deep links from the audit panel carry the ACTUAL folder path a file
    // sits in (e.g. /root/2026/07), but only top-level roots are rendered
    // here because the server-side subtree expansion covers descendants.
    // An exact-match on cb.checked therefore preselects nothing and Start
    // stays disabled. Map each prefill path to the top-level root whose
    // path it lives under, so /pipeline?folder=/root/2026/07 checks the
    // /root row instead of orphaning the deep link.
    var prefillRootIds = new Set();
    if (_prefillFolderPaths.length) {
      var topLevel = folders.filter(function(f) {
        return !(f.parent_id && byId[f.parent_id]);
      });
      _prefillFolderPaths.forEach(function(prefillPath) {
        topLevel.forEach(function(f) {
          if (prefillPath === f.path
              || prefillPath.indexOf(f.path + '/') === 0) {
            prefillRootIds.add(f.id);
          }
        });
      });
    }
    list.innerHTML = '';
    if (folders.length === 0) {
      list.innerHTML = 'No photos are in this workspace yet. '
        + '<a href="/import" style="color:var(--accent);">Open Import to add photos</a>.';
      return;
    }
    folders.forEach(function(f) {
      // Only offer top-level entries: children are covered by the
      // server-side subtree expansion, and listing every dated folder
      // would bury the roots.
      if (f.parent_id && byId[f.parent_id]) return;
      var label = document.createElement('label');
      label.style.cssText = 'display:flex;align-items:center;gap:6px;cursor:pointer;color:var(--text-primary);';
      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = f.id;
      cb.dataset.path = f.path;
      cb.style.accentColor = 'var(--accent)';
      cb.checked = prefillRootIds.has(f.id);
      cb.onchange = function() {
        // Checking a workspace-folder box is a switch to workspace-folder
        // scope: if we're still in collection or new-images mode,
        // updateStartButton() and startPipeline() would keep gating on the
        // previous scope and POST it instead of the folder_ids the user
        // just picked.
        if (cb.checked && _sourceMode !== 'folders') selectSourceMode('folders');
        updateSourceSummary();
        updateStartButton();
        schedulePlanRefresh();
      };
      var span = document.createElement('span');
      var count = f.workspace_photo_count != null
        ? f.workspace_photo_count : f.photo_count;
      span.textContent = f.path + (count != null ? ' (' + count + ')' : '');
      label.appendChild(cb);
      label.appendChild(span);
      list.appendChild(label);
    });
    if (_prefillFolderPaths.length) {
      updateSourceSummary();
      updateStartButton();
      // A deep link maps to at least one workspace root only when the
      // prefill actually sat under a tracked root; when it did, refresh
      // the plan so readiness pills describe the preselected subtree
      // instead of the blank whole-workspace fallback.
      if (prefillRootIds.size) schedulePlanRefresh();
    }
  } catch (e) {
    list.textContent = 'Could not load workspace folders.';
  }
}

function updateSourceSummary() {
  var el = document.getElementById('txtSource');
  if (!el) return;
  if (_sourceMode === 'folders') {
    var n = selectedFolderIds().length;
    el.textContent = n ? (n + ' folder(s) selected') : '';
  }
}

var _sourceMode = 'folders'; // 'folders' or 'collection'

function selectSourceMode(mode) {
  _sourceMode = mode;
  var isFolders = (mode === 'folders');
  document.getElementById('radioFolders').checked = isFolders;
  document.getElementById('radioCollection').checked = (mode === 'collection');

  var foldersBody = document.getElementById('sourceImportBody');
  var collBody = document.getElementById('sourceCollectionBody');

  foldersBody.classList.toggle('dimmed', !isFolders);
  collBody.classList.toggle('dimmed', mode !== 'collection');

  fetchFolderPreview();
  // Source mode controls whether the plan body sends a collection_id, so
  // pills/summaries can change when the user switches modes (e.g. an
  // "Already done" Classify under whole-workspace becomes "Will run on N
  // photos" once a partially-classified collection is selected).
  schedulePlanRefresh();
}

function onCollectionChange() {
  var collId = document.getElementById('collectionPicker').value;
  var countEl = document.getElementById('collectionPhotoCount');
  if (!collId) {
    countEl.textContent = '';
    updateStartButton();
    fetchFolderPreview();
    schedulePlanRefresh();
    return;
  }
  safeFetch('/api/collections/' + collId + '/photos?per_page=1', {}, { toast: false })
    .then(function(data) {
      countEl.textContent = (data.total || 0) + ' photos';
    })
    .catch(function() { countEl.textContent = ''; });
  updateStartButton();
  fetchFolderPreview();
  schedulePlanRefresh();
}
