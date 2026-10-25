// Quick Move preview: the summary panel, the Review move button, and the resolved destination path.

import { state } from './state.js';
import {
  quickFolderNameResult, quickFolderTemplateResult, quickUsesDateFolders, sourceFolderName,
} from './quick-move-form.js';
import { scheduleQuickDatePreflight, scheduleQuickPreflight } from './quick-move-preflight.js';
import { quickRemoteTargetId } from './remote-targets.js';
import { rsyncInstallHint } from './shared.js';

/** @param {MoveFolder | null | undefined} folder @param {string} finalPath */
export function setQuickMoveSummary(folder, finalPath) {
  var summary = /** @type {HTMLElement} */ (document.getElementById('quickMoveSummary'));
  if (!folder || !finalPath) {
    summary.replaceChildren();
    summary.style.display = 'none';
    return;
  }
  var count = folder.photo_count || 0;

  summary.replaceChildren();
  var eyebrow = document.createElement('div');
  eyebrow.className = 'move-preview-eyebrow';
  eyebrow.textContent = 'Final location';

  var route = document.createElement('div');
  route.className = 'move-route';
  /** @param {string} label @param {string} path */
  function routePoint(label, path) {
    var point = document.createElement('div');
    var routeLabel = document.createElement('div');
    routeLabel.className = 'move-route-label';
    routeLabel.textContent = label;
    var routePath = document.createElement('div');
    routePath.className = 'move-route-path';
    routePath.textContent = path;
    point.appendChild(routeLabel);
    point.appendChild(routePath);
    return point;
  }
  route.appendChild(routePoint('FROM', folder.path));
  var arrow = document.createElement('div');
  arrow.className = 'move-route-arrow';
  arrow.setAttribute('aria-hidden', 'true');
  arrow.textContent = '→';
  route.appendChild(arrow);
  route.appendChild(routePoint('TO', finalPath));

  var meta = document.createElement('div');
  meta.className = 'move-preview-meta';
  var renamed = quickFolderNameResult().rename;
  meta.textContent = count.toLocaleString() + ' photo' + (count !== 1 ? 's' : '') +
    ' · Includes subfolders' + (renamed ? ' · Folder will be renamed' : '');

  summary.appendChild(eyebrow);
  summary.appendChild(route);
  summary.appendChild(meta);
  summary.style.display = 'block';
}

/** @param {MoveFolder | undefined} folder @param {MovePreflight | null} data */
export function setQuickDateSummary(folder, data) {
  var summary = /** @type {HTMLElement} */ (document.getElementById('quickMoveSummary'));
  if (!folder || !data || !data.date_organized) {
    summary.replaceChildren();
    summary.style.display = 'none';
    return;
  }
  summary.replaceChildren();
  var eyebrow = document.createElement('div');
  eyebrow.className = 'move-preview-eyebrow';
  eyebrow.textContent = 'Resulting folders';
  summary.appendChild(eyebrow);

  var destinations = data.destinations || [];
  destinations.slice(0, 8).forEach(function(item) {
    var row = document.createElement('div');
    row.className = 'move-preview-meta';
    row.textContent = item.path + ' · ' + item.photo_count.toLocaleString() +
      ' photo' + (item.photo_count !== 1 ? 's' : '') +
      (item.exists ? ' · already exists' : '');
    summary.appendChild(row);
  });
  if (destinations.length > 8) {
    var more = document.createElement('div');
    more.className = 'move-preview-meta';
    more.textContent = '+ ' + (destinations.length - 8).toLocaleString() +
      ' more folders';
    summary.appendChild(more);
  }
  var meta = document.createElement('div');
  meta.className = 'move-preview-meta';
  meta.textContent = data.photo_count.toLocaleString() + ' photos across ' +
    data.destination_count.toLocaleString() + ' folder' +
    (data.destination_count !== 1 ? 's' : '') +
    ' · Companion files move with their photos';
  summary.appendChild(meta);
  summary.style.display = 'block';
}

export function updateQuickMoveBtn() {
  var fid = /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderSelect')).value;
  var remoteId = quickRemoteTargetId();
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('quickMoveBtn'));
  var nameResult = quickFolderNameResult();
  var templateResult = quickFolderTemplateResult();
  var nameError = /** @type {HTMLElement} */ (document.getElementById('quickFolderNameError'));
  nameError.textContent = fid ? (nameResult.error || templateResult.error) : '';
  var ready;
  if (remoteId) {
    // Remote needs a usable GNU rsync; no destination text is required (the
    // target carries it). An optional subpath refines where under it.
    ready = !!fid && state.quickRsyncAvailable && !nameResult.error && !templateResult.error;
  } else {
    ready = !!(fid && /** @type {HTMLInputElement} */ (document.getElementById('quickDestInput')).value.trim()
      && !nameResult.error && !templateResult.error);
  }
  btn.disabled = state.quickMoveBusy || !ready;
  btn.innerHTML = state.quickMoveBusy
    ? 'Checking destination…'
    : 'Review move <span class="button-arrow" aria-hidden="true">→</span>';
  updateQuickMovePreview();
}

/** @param {boolean} isBusy */
export function setQuickMoveBusy(isBusy) {
  state.quickMoveBusy = !!isBusy;
  updateQuickMoveBtn();
}

// Compute the final landing path, mirroring move.py move_folder():
// the named folder is placed *inside* the destination. The editable name
// allows a rename during the same safe move.
// Detect Windows-style paths (containing '\') so the preview matches
// os.path.join on a Windows backend, where 'C:\Archive\' + 'Birds' is
// 'C:\Archive\Birds', not 'C:\Archive\/Birds'. Also lets the folder.name
// fallback split a backslash-delimited source path.
/** @param {MoveFolder | undefined} folder @param {string} destination @param {string} destinationName */
export function resolvedMoveDest(folder, destination, destinationName) {
  var srcPath = (folder && folder.path) || '';
  var winStyle = destination.indexOf('\\') !== -1 || srcPath.indexOf('\\') !== -1;
  var sep = winStyle ? '\\' : '/';
  var trimRe = winStyle ? /[\\/]+$/ : /\/+$/;
  var folderName = destinationName || sourceFolderName(folder);
  return destination.replace(trimRe, '') + sep + folderName;
}

export function updateQuickMovePreview() {
  var preview = /** @type {HTMLElement} */ (document.getElementById('quickDestPreview'));
  var fid = parseInt(/** @type {HTMLSelectElement} */ (document.getElementById('quickFolderSelect')).value);
  var remoteId = quickRemoteTargetId();
  if (!fid) {
    preview.textContent = '';
    setQuickMoveSummary(null, '');
    return;
  }
  var folder = state.allFolders.find(function(f) { return f.id === fid; });
  var nameResult = quickFolderNameResult();
  if (nameResult.error) {
    preview.textContent = '';
    setQuickMoveSummary(null, '');
    return;
  }
  var destinationName = nameResult.value;

  if (remoteId) {
    var t = state.quickRemoteTargets.find(function(x) { return x.id === remoteId; });
    if (!t) {
      preview.textContent = '';
      setQuickMoveSummary(null, '');
      return;
    }
    // Use the same normalization the backend does, so the preview path
    // matches what move.py / sanitize_subpath will actually write. Without
    // this the UI can show e.g. "/volume1/Photo/USA" while the move
    // ultimately rejects "/USA" (absolute) or lands at a different path
    // because the backend collapses ".", strips backslashes, etc.
    var subResult = normalizeRemoteSubpath(
      /** @type {HTMLInputElement} */ (document.getElementById('quickSubpathInput')).value);
    if (subResult.error) {
      preview.textContent = '';
      setQuickMoveSummary(null, '');
      preview.appendChild(_destWarn('⚠ ' + subResult.error));
      return;
    }
    var sub = subResult.value;
    var base = t.remote_path.replace(/\/+$/, '') + (sub ? '/' + sub : '');
    // Fall back to the source folder's actual name when the user hasn't
    // renamed: destinationName is intentionally empty in that case so the
    // backend uses the untrimmed folder.name, so the preview must match.
    var finalPath = base + '/' + (destinationName || sourceFolderName(folder));
    var finalDestSpec = rsyncDestSpec(t.user, t.host, finalPath);

    preview.textContent = '';
    var line = document.createElement('div');
    line.textContent = 'Transferring over SSH to: ';
    var span = document.createElement('span');
    span.className = 'dest-preview-path';
    span.textContent = finalDestSpec;
    line.appendChild(span);
    preview.appendChild(line);

    if (!state.quickRsyncAvailable) {
      preview.appendChild(_destWarn('⚠ No GNU rsync found. ' + rsyncInstallHint()));
    }
    if (!t.mount_path) {
      preview.appendChild(_destWarn('⚠ This target has no local mount path, so '
        + 'moved photos can’t stay in your library. Add one in Settings.'));
    } else if (!_isAbsolutePath(t.mount_path)) {
      // The backend refuses a relative mount path before starting the
      // move (the catalog would point at a non-resolving location after).
      // Surface that here so the user sees it before clicking Move.
      preview.appendChild(_destWarn('⚠ This target’s local mount path must be '
        + 'absolute (e.g. "/Volumes/Photos"). Fix it under Settings → '
        + 'Remote targets.'));
    }
    var rstatus = document.createElement('div');
    rstatus.id = 'quickDestStatus';
    rstatus.className = 'dest-status';
    preview.appendChild(rstatus);
    setQuickMoveSummary(folder, finalDestSpec);
    scheduleQuickPreflight(fid, null, remoteId, sub, destinationName);
    return;
  }

  var dest = /** @type {HTMLInputElement} */ (document.getElementById('quickDestInput')).value.trim();
  if (!dest) {
    preview.textContent = '';
    setQuickMoveSummary(null, '');
    return;
  }
  var templateResult = quickFolderTemplateResult();
  if (quickUsesDateFolders()) {
    if (templateResult.error) {
      preview.textContent = '';
      setQuickMoveSummary(null, '');
      return;
    }
    preview.textContent = '';
    var dateStatus = document.createElement('div');
    dateStatus.id = 'quickDestStatus';
    dateStatus.className = 'dest-status';
    dateStatus.textContent = 'Checking capture dates…';
    preview.appendChild(dateStatus);
    setQuickMoveSummary(null, '');
    scheduleQuickDatePreflight(fid, dest, templateResult.value);
    return;
  }
  finalPath = resolvedMoveDest(folder, dest, destinationName);

  preview.textContent = '';
  // Whether the destination already exists (and how much is in it) requires
  // hitting the filesystem, so fetch it from the preflight endpoint and fill
  // in this line asynchronously. Debounced so typing a path doesn't spam it.
  var status = document.createElement('div');
  status.id = 'quickDestStatus';
  status.className = 'dest-status';
  preview.appendChild(status);
  setQuickMoveSummary(folder, finalPath);
  scheduleQuickPreflight(fid, dest, '', '', destinationName);
}

/** @param {string} text */
export function _destWarn(text) {
  var d = document.createElement('div');
  d.className = 'dest-status';
  d.style.color = 'var(--warning)';
  d.textContent = text;
  return d;
}

// True if `p` looks absolute on either backend OS (POSIX leading slash,
// Windows drive like "C:\foo", or UNC "\\server\share"). Matches what
// Python's os.path.isabs would say on the corresponding platform — the
// JS side has no way to know which platform the backend is running, so
// accept any form. Used to warn before the backend refuses a relative
// mount path on the remote target.
/** @param {string | null | undefined} p */
export function _isAbsolutePath(p) {
  if (!p) return false;
  return p.charAt(0) === '/' || /^[A-Za-z]:[\\/]/.test(p) || /^\\\\/.test(p);
}

// Mirror move.py rsync_dest_spec(): rsync's user@host:path form parses the
// FIRST colon as the host/path separator, so an IPv6 literal like
// 2001:db8::1 needs brackets to avoid being chopped at "2001". DNS/IPv4
// hosts have no colons and pass through unchanged. The backend formats the
// /api/move-folder/preflight resolved_dest with the same rule; this helper
// keeps the move-form's local preview honest before the user has typed
// anything that would trigger the preflight call.
/** @param {string} user @param {string} host @param {string} path */
export function rsyncDestSpec(user, host, path) {
  var hostToken = host && host.indexOf(':') !== -1 ? '[' + host + ']' : host;
  return user + '@' + hostToken + ':' + path;
}

// Normalize a remote-target subpath the same way move.py sanitize_subpath
// does: trim, fold backslashes to forward slashes, reject absolute paths
// (POSIX, Windows drive, or UNC), drop empty / `.` segments, and reject any
// `..` traversal. Returns {value, error}: callers either use `value` or
// surface `error`. Sharing one helper between the preview, scheduled
// preflight, and the actual move submission means the path the UI shows
// matches the path the backend will write — a UI-transparency requirement.
/** @param {string} raw */
export function normalizeRemoteSubpath(raw) {
  var trimmed = (raw || '').trim();
  if (!trimmed) return {value: '', error: ''};
  var s = trimmed.replace(/\\/g, '/');
  if (s.charAt(0) === '/' || /^[A-Za-z]:/.test(s)) {
    return {value: '', error: 'Subpath must be relative.'};
  }
  var parts = [];
  var segs = s.split('/');
  for (var i = 0; i < segs.length; i++) {
    var seg = segs[i];
    if (seg === '' || seg === '.') continue;
    if (seg === '..') return {value: '', error: "Subpath may not contain '..'."};
    parts.push(seg);
  }
  return {value: parts.join('/'), error: ''};
}
