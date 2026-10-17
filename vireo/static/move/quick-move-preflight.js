// Quick Move preflight: the debounced /api/move-folder/preflight status line.
// Classic page script; load boot.js after all definitions.

var quickPreflightSeq = 0;
/** @type {ReturnType<typeof setTimeout> | null} */ var quickPreflightTimer = null;

/** @param {number} fid @param {string} destination @param {string} folderTemplate */
function scheduleQuickDatePreflight(fid, destination, folderTemplate) {
  if (quickPreflightTimer) clearTimeout(quickPreflightTimer);
  var seq = ++quickPreflightSeq;
  // An unusable destination doesn't invalidate the dropdown's examples: they
  // are rendered from the source folder's capture dates alone, so editing or
  // clearing the destination cannot change them. Leave the last real values
  // in place — onQuickFolderChange() clears them when the folder itself
  // changes, which is the only input they depend on.
  if (!_isAbsolutePath(destination)) return;
  quickPreflightTimer = setTimeout(async function() {
    /** @type {MovePreflight} */ var data;
    try {
      var resp = await fetch('/api/move-folder/preflight', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          folder_id: fid,
          destination: destination,
          folder_template: folderTemplate,
        }),
      });
      if (!resp.ok) { if (seq === quickPreflightSeq) clearQuickDestStatus(); return; }
      data = await resp.json();
    } catch (e) {
      if (seq === quickPreflightSeq) clearQuickDestStatus();
      return;
    }
    if (seq !== quickPreflightSeq || !quickUsesDateFolders()) return;
    var folder = allFolders.find(function(item) { return item.id === fid; });
    setQuickDateSummary(folder, data);
    // Same response that drew the folder list above, so the dropdown's
    // examples and that list are always the same dates.
    applyQuickDateSamples(data.template_samples);
    var status = /** @type {HTMLElement} */ (document.getElementById('quickDestStatus'));
    if (!status) return;
    if (!data.photo_count) {
      status.textContent = 'No tracked photos were found in this folder.';
      status.style.color = 'var(--warning)';
    } else {
      var existing = (data.destinations || []).filter(function(item) {
        return item.exists;
      }).length;
      status.textContent = existing
        ? '⚠ ' + existing.toLocaleString() + ' resulting folder' +
          (existing !== 1 ? 's already exist' : ' already exists') +
          '. Same-name files will not be overwritten.'
        : '✓ ' + data.destination_count.toLocaleString() + ' date folder' +
          (data.destination_count !== 1 ? 's' : '') + ' will be created.';
      status.style.color = existing ? 'var(--warning)' : 'var(--text-dim)';
    }
  }, 350);
}

/** @param {number} fid @param {string | null} dest @param {string} remoteId @param {string} subpath @param {string} destinationName */
function scheduleQuickPreflight(fid, dest, remoteId, subpath, destinationName) {
  if (quickPreflightTimer) clearTimeout(quickPreflightTimer);
  // Bump the sequence on *every* call, not just when a request starts. Any
  // change to the destination must immediately invalidate an in-flight
  // response for the previous value — otherwise a response landing during the
  // 350ms debounce window, or when the new value is non-absolute (so no new
  // request is scheduled), would still pass the seq guard and write stale
  // exists/new-folder text for the wrong destination.
  var seq = ++quickPreflightSeq;
  if (remoteId) {
    var body = {
      folder_id: fid,
      remote_target_id: remoteId,
      subpath: subpath || '',
      destination_name: destinationName,
    };
    quickPreflightTimer = setTimeout(function() { runQuickPreflight(seq, body); }, 350);
    return;
  }
  // The preflight route rejects non-absolute paths, so wait until the input
  // looks absolute before asking. Accept POSIX '/...', Windows drive paths
  // ('C:\...'), and Windows UNC paths ('\\server\share') — all of which
  // os.path.isabs() treats as absolute on their respective backends.
  dest = dest || '';
  var isAbs = dest.charAt(0) === '/' || /^[A-Za-z]:[\\/]/.test(dest) || /^\\\\/.test(dest);
  if (!isAbs) return;
  quickPreflightTimer = setTimeout(function() {
    runQuickPreflight(seq, {
      folder_id: fid,
      destination: dest,
      destination_name: destinationName,
    });
  }, 350);
}

/** @param {number} seq @param {MoveDestination} body */
async function runQuickPreflight(seq, body) {
  var status = /** @type {HTMLElement} */ (document.getElementById('quickDestStatus'));
  if (status) {
    status.textContent = 'Checking destination…';
    status.style.color = 'var(--text-dim)';
  }
  // Plain fetch (not safeFetch): this fires while the user types, so a
  // transient failure must stay silent rather than raising an error toast.
  /** @type {MovePreflight} */ var data;
  try {
    var resp = await fetch('/api/move-folder/preflight', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    if (!resp.ok) { if (seq === quickPreflightSeq) clearQuickDestStatus(); return; }
    data = await resp.json();
  } catch (e) {
    if (seq === quickPreflightSeq) clearQuickDestStatus();
    return;
  }
  if (seq !== quickPreflightSeq) return;  // a newer keystroke superseded us
  status = /** @type {HTMLElement} */ (document.getElementById('quickDestStatus'));
  if (!status) return;
  if (data.remote && data.reachable === false) {
    status.textContent = '⚠ Couldn’t reach the target over SSH'
      + (data.error ? (': ' + data.error) : '') + '.';
    status.style.color = 'var(--warning)';
  } else if (data.exists) {
    setDestExistsStatus(status, data.file_count, data.file_count_truncated);
    // The fast count caps out at 1000. When it was truncated, fetch the exact
    // number in the background and update the line in place — without blocking
    // the user or holding up typing. Local-path only: the exact walk re-scans
    // a local dir, so for a remote target the capped "at least N" line stands.
    if (!data.remote && data.file_count_truncated) {
      requestExactDestCount(
        body.folder_id, body.destination, body.destination_name, seq);
    }
  } else {
    status.textContent = '✓ New folder — nothing exists at this location yet.';
    status.style.color = 'var(--text-dim)';
  }
}

// Render the "destination already exists" warning. When `truncated` is true
// the count is a floor (the fast scan stopped at its cap), so say "at least N"
// rather than implying an exact total — the line must mean what it says.
/** @param {HTMLElement} status @param {number} n @param {boolean | undefined} truncated */
function setDestExistsStatus(status, n, truncated) {
  var countText = truncated ? ('at least ' + n.toLocaleString()) : n.toLocaleString();
  status.textContent = '⚠ Destination already exists — contains ' + countText + ' file' +
    (n !== 1 || truncated ? 's' : '') + '. Files already there are left untouched; only missing files are copied. If a same-name file differs, the merge is canceled and nothing is changed.';
  status.style.color = 'var(--warning)';
}

// Background, non-blocking exact count. Reuses the keystroke seq guard so a
// result landing after the user has changed the destination is discarded
// instead of overwriting the line for the wrong path. Stays silent on failure:
// the capped "at least N" line is already a valid answer.
/** @param {number | undefined} fid @param {string | undefined} dest @param {string | undefined} destinationName @param {number} seq */
function requestExactDestCount(fid, dest, destinationName, seq) {
  fetch('/api/move-folder/preflight', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      folder_id: fid,
      destination: dest,
      destination_name: destinationName,
      mode: 'exact',
    }),
  }).then(function(resp) {
    return resp.ok ? resp.json() : null;
  }).then(function(data) {
    if (!data || !data.exists) return;
    if (seq !== quickPreflightSeq) return;  // a newer keystroke superseded us
    var status = /** @type {HTMLElement} */ (document.getElementById('quickDestStatus'));
    if (status) setDestExistsStatus(status, data.file_count, data.file_count_truncated);
  }).catch(function() { /* background nicety — leave the capped line in place */ });
}

function clearQuickDestStatus() {
  var status = /** @type {HTMLElement} */ (document.getElementById('quickDestStatus'));
  if (status) status.textContent = '';
}
