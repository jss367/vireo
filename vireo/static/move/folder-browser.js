// The destination folder browser modal: browsing, the list, selection, and new folders.

import { state } from './state.js';
import { updatePhotoSelectionUI } from './photo-move.js';
import { updateQuickMoveBtn } from './quick-move-preview.js';
import { rememberRecentDestination } from './recent-destinations.js';
import { escapeAttr, escapeHtml, safeFetch } from './shared.js';

/* ---------- Folder Browser ---------- */
/** @param {string} targetInputId */
export function openBrowser(targetInputId) {
  state.browserTargetInput = targetInputId;
  var currentVal = /** @type {HTMLInputElement} */ (document.getElementById(targetInputId)).value.trim();
  state.browserPath = currentVal || '';
  /** @type {HTMLElement} */ (document.getElementById('moveBrowserModal')).classList.add('open');
  /** @type {HTMLInputElement} */ (document.getElementById('newFolderName')).value = '';
  updateBrowserSource();
  browseTo(state.browserPath || null);
}

export function updateBrowserSource() {
  var source = /** @type {HTMLElement} */ (document.getElementById('moveBrowserSource'));
  var folder = null;
  if (state.browserTargetInput === 'quickDestInput') {
    var fid = parseInt(/** @type {HTMLSelectElement} */ (document.getElementById('quickFolderSelect')).value);
    folder = state.allFolders.find(function(f) { return f.id === fid; });
  }
  if (!folder) {
    source.style.display = 'none';
    /** @type {HTMLElement} */ (document.getElementById('moveBrowserSourcePath')).textContent = '';
    /** @type {HTMLElement} */ (document.getElementById('moveBrowserSourceCount')).textContent = '';
    return;
  }
  var count = folder.photo_count || 0;
  /** @type {HTMLElement} */ (document.getElementById('moveBrowserSourcePath')).textContent = folder.path;
  /** @type {HTMLElement} */ (document.getElementById('moveBrowserSourceCount')).textContent = ' (' +
    count.toLocaleString() + ' photo' + (count !== 1 ? 's' : '') + ')';
  source.style.display = 'block';
}

export function closeBrowser() {
  /** @type {HTMLElement} */ (document.getElementById('moveBrowserModal')).classList.remove('open');
  state.browserTargetInput = null;
}

/** @param {string | null} path */
export async function browseTo(path) {
  // Stamp each browse so a slow initial fetch can't clobber a later
  // shortcut click. Without this, opening the modal with a prefilled
  // destination and immediately clicking Pictures/Volumes can leave the
  // list and browserPath silently reverted to the destination's listing.
  var seq = ++state.browseSeq;
  var url = '/api/browse';
  if (path === '__pictures__') {
    // When the modal opens with a prefilled destination we skip the
    // browseTo(null) call that would otherwise cache the home path, so
    // resolve it on demand here. Without this, '/Pictures' is sent as an
    // absolute path and lands at the wrong root.
    if (!state.browserHomePath) {
      try {
        var home = await safeFetch('/api/browse');
        if (seq !== state.browseSeq) return;
        state.browserHomePath = home.path;
      } catch (e) {
        return;
      }
    }
    url = '/api/browse?path=' + encodeURIComponent(state.browserHomePath + '/Pictures');
  } else if (path === '__volumes__') {
    // Mac's /Volumes is a real directory the browse endpoint can list.
    // Windows drive letters and Linux mount roots (/media, /run/media,
    // /mnt) vary per host, so use /api/volumes — which scans all of
    // them — and render a synthetic root list.
    if (navigator.userAgent.indexOf('Mac') < 0) {
      try {
        var volumes = await safeFetch('/api/volumes');
        if (seq !== state.browseSeq) return;
        state.browserPath = '';
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
    /** @type {MoveBrowserData} */ var data = await safeFetch(url);
    if (seq !== state.browseSeq) return;
    state.browserPath = data.path;
    if (!path) state.browserHomePath = data.path;
    renderBrowserList(data);
  } catch (e) {
    // safeFetch handles error toast
  }
}

/** @param {MoveBrowserData} data @param {{syntheticRoot?: boolean} | undefined} [opts] */
export function renderBrowserList(data, opts) {
  /** @type {HTMLElement} */ (document.getElementById('moveBrowserBreadcrumb')).textContent = data.path || 'Volumes';

  var list = /** @type {HTMLElement} */ (document.getElementById('moveBrowserList'));
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

export function selectBrowserFolder() {
  if (!state.browserTargetInput || !state.browserPath) return;
  var selectedPath = state.browserPath;
  /** @type {HTMLInputElement} */ (document.getElementById(state.browserTargetInput)).value = selectedPath;
  closeBrowser();
  rememberRecentDestination(selectedPath);
  // Trigger any button state updates
  updateQuickMoveBtn();
  updatePhotoSelectionUI();
}

export async function createNewFolder() {
  var name = /** @type {HTMLInputElement} */ (document.getElementById('newFolderName')).value.trim();
  if (!name || !state.browserPath) return;

  var newPath = state.browserPath + (state.browserPath.endsWith('/') ? '' : '/') + name;
  try {
    await safeFetch('/api/browse/mkdir', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: newPath}),
    });
    // Refresh the browser to show the new folder
    browseTo(state.browserPath);
    /** @type {HTMLInputElement} */ (document.getElementById('newFolderName')).value = '';
  } catch (e) {
    // safeFetch handles error toast
  }
}
