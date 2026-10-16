// The destination folder browser modal: browsing, the list, selection, and new folders.
// Classic page script; load boot.js after all definitions.

/* ---------- Folder Browser ---------- */
function openBrowser(targetInputId) {
  browserTargetInput = targetInputId;
  var currentVal = document.getElementById(targetInputId).value.trim();
  browserPath = currentVal || '';
  document.getElementById('moveBrowserModal').classList.add('open');
  document.getElementById('newFolderName').value = '';
  updateBrowserSource();
  browseTo(browserPath || null);
}

function updateBrowserSource() {
  var source = document.getElementById('moveBrowserSource');
  var folder = null;
  if (browserTargetInput === 'quickDestInput') {
    var fid = parseInt(document.getElementById('quickFolderSelect').value);
    folder = allFolders.find(function(f) { return f.id === fid; });
  }
  if (!folder) {
    source.style.display = 'none';
    document.getElementById('moveBrowserSourcePath').textContent = '';
    document.getElementById('moveBrowserSourceCount').textContent = '';
    return;
  }
  var count = folder.photo_count || 0;
  document.getElementById('moveBrowserSourcePath').textContent = folder.path;
  document.getElementById('moveBrowserSourceCount').textContent = ' (' +
    count.toLocaleString() + ' photo' + (count !== 1 ? 's' : '') + ')';
  source.style.display = 'block';
}

function closeBrowser() {
  document.getElementById('moveBrowserModal').classList.remove('open');
  browserTargetInput = null;
}

async function browseTo(path) {
  // Stamp each browse so a slow initial fetch can't clobber a later
  // shortcut click. Without this, opening the modal with a prefilled
  // destination and immediately clicking Pictures/Volumes can leave the
  // list and browserPath silently reverted to the destination's listing.
  var seq = ++browseSeq;
  var url = '/api/browse';
  if (path === '__pictures__') {
    // When the modal opens with a prefilled destination we skip the
    // browseTo(null) call that would otherwise cache the home path, so
    // resolve it on demand here. Without this, '/Pictures' is sent as an
    // absolute path and lands at the wrong root.
    if (!browserHomePath) {
      try {
        var home = await safeFetch('/api/browse');
        if (seq !== browseSeq) return;
        browserHomePath = home.path;
      } catch (e) {
        return;
      }
    }
    url = '/api/browse?path=' + encodeURIComponent(browserHomePath + '/Pictures');
  } else if (path === '__volumes__') {
    // Mac's /Volumes is a real directory the browse endpoint can list.
    // Windows drive letters and Linux mount roots (/media, /run/media,
    // /mnt) vary per host, so use /api/volumes — which scans all of
    // them — and render a synthetic root list.
    if (navigator.userAgent.indexOf('Mac') < 0) {
      try {
        var volumes = await safeFetch('/api/volumes');
        if (seq !== browseSeq) return;
        browserPath = '';
        renderBrowserList({path: '', dirs: volumes}, {syntheticRoot: true});
      } catch (e) {
        // safeFetch handles error toast
      }
      return;
    }
    url = '/api/browse?path=' + encodeURIComponent('/Volumes');
  } else if (path) {
    url = '/api/browse?path=' + encodeURIComponent(path);
  }
  try {
    var data = await safeFetch(url);
    if (seq !== browseSeq) return;
    browserPath = data.path;
    if (!path) browserHomePath = data.path;
    renderBrowserList(data);
  } catch (e) {
    // safeFetch handles error toast
  }
}

function renderBrowserList(data, opts) {
  document.getElementById('moveBrowserBreadcrumb').textContent = data.path || 'Volumes';

  var list = document.getElementById('moveBrowserList');
  var html = '';
  var isSyntheticRoot = opts && opts.syntheticRoot;
  var parent = '';
  if (data.path && data.path.indexOf('\\') !== -1) {
    parent = data.path.replace(/[\\/]?[^\\/]+[\\/]?$/, '') || data.path;
    // Drive-root edge case: 'C:\Users'.replace(...) yields 'C:', which the
    // backend treats as the drive's process-local cwd. Restore the trailing
    // backslash so '..' returns to the drive root instead.
    if (/^[A-Za-z]:$/.test(parent)) parent += '\\';
  } else if (data.path) {
    parent = data.path.substring(0, data.path.lastIndexOf('/')) || '/';
  }
  if (data.path && data.path !== '/' && parent !== data.path) {
    html += '<div class="browser-folder-item" data-browse-path="' + escapeAttr(parent) + '">&#128194; ..</div>';
  }
  if (data.dirs) {
    data.dirs.forEach(function(d) {
      html += '<div class="browser-folder-item" data-browse-path="' + escapeAttr(d.path) + '">&#128194; ' + escapeHtml(d.name) + '</div>';
    });
  }
  if ((!data.dirs || data.dirs.length === 0) && (data.path === '/' || isSyntheticRoot)) {
    html += '<div style="padding:10px;color:var(--text-dim);font-size:13px;">No subdirectories</div>';
  }
  list.innerHTML = html;
}

function selectBrowserFolder() {
  if (!browserTargetInput || !browserPath) return;
  var selectedPath = browserPath;
  document.getElementById(browserTargetInput).value = selectedPath;
  closeBrowser();
  rememberRecentDestination(selectedPath);
  // Trigger any button state updates
  updateQuickMoveBtn();
  updatePhotoSelectionUI();
}

async function createNewFolder() {
  var name = document.getElementById('newFolderName').value.trim();
  if (!name || !browserPath) return;

  var newPath = browserPath + (browserPath.endsWith('/') ? '' : '/') + name;
  try {
    await safeFetch('/api/browse/mkdir', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: newPath}),
    });
    // Refresh the browser to show the new folder
    browseTo(browserPath);
    document.getElementById('newFolderName').value = '';
  } catch (e) {
    // safeFetch handles error toast
  }
}
