/* Filename-collision notice for the export modal (Browse and Photo Editor).
 *
 * Export never overwrites: a name already taken on disk (or by an earlier
 * photo in the same export) is saved with a numbered suffix such as _2.
 * Rather than stating that policy to everyone, the modal asks the server's
 * /api/jobs/export/preflight which names would actually change for the
 * current settings, and shows #exportCollisionNotice only when some would.
 *
 * Each host page defines buildExportPreflightRequest(), returning the
 * name-deciding settings (photos, destination, template, format, ...), or
 * null when they cannot describe an export yet (e.g. an out-of-range custom
 * size). The check re-runs, debounced, whenever that request changes: on
 * any control edit, and when pages call schedule() after changing controls
 * programmatically, which fires no input events.
 *
 * At submit the page runs the preflight again and passes the result to
 * acknowledged(). Export may start only when nothing is renamed or the
 * notice already shows exactly those renames; otherwise the notice is
 * updated and the page stops so the user sees it before clicking again.
 * This replaces a window.confirm, which the desktop webview never shows
 * (it returns a truthy promise, so the export started unannounced).
 */
var VireoExportCollisions = (function() {
  var DEBOUNCE_MS = 350;
  var VISIBLE_RENAMES = 5;
  var timer = null;
  var checkGeneration = 0;
  var shownSignature = null;
  // The request the notice describes (or is being checked for). Controls
  // that do not change names (quality, metadata) leave it unchanged, so
  // editing them neither re-checks nor flickers the notice.
  var checkedKey = null;

  function $(id) { return document.getElementById(id); }
  function overlay() { return $('exportOverlay'); }
  function notice() { return $('exportCollisionNotice'); }

  function signature(preflight) {
    return JSON.stringify([preflight.rename_count, preflight.renames || []]);
  }

  function hide() {
    shownSignature = null;
    var el = notice();
    if (!el) return;
    el.hidden = true;
    el.textContent = '';
  }

  // Wrap between the two names, not inside one, in the narrow summary column.
  function filenameSpan(name) {
    var span = document.createElement('span');
    span.className = 'export-collision-name';
    span.textContent = name;
    return span;
  }

  function render(preflight) {
    var el = notice();
    if (!el) return;
    var count = preflight && preflight.rename_count;
    if (!count) {
      hide();
      return;
    }
    var renames = (preflight.renames || []).slice(0, VISIBLE_RENAMES);
    el.textContent = '';
    var summary = document.createElement('div');
    // "Taken" covers both causes: a file already on disk, and an earlier
    // photo in this export resolving to the same name.
    summary.textContent = count === 1
      ? '1 filename is already taken, so Vireo will add a number to it. Nothing is overwritten.'
      : count.toLocaleString() + ' filenames are already taken, so Vireo will add a number to them. Nothing is overwritten.';
    el.appendChild(summary);
    var list = document.createElement('ul');
    list.className = 'export-collision-list';
    renames.forEach(function(rename) {
      var item = document.createElement('li');
      item.appendChild(filenameSpan(rename.requested_name));
      item.appendChild(document.createTextNode(' → '));
      item.appendChild(filenameSpan(rename.export_name));
      list.appendChild(item);
    });
    if (count > renames.length) {
      var more = document.createElement('li');
      more.textContent = '…and ' + (count - renames.length).toLocaleString() + ' more';
      list.appendChild(more);
    }
    el.appendChild(list);
    el.hidden = false;
    shownSignature = signature(preflight);
  }

  function currentRequest() {
    return typeof buildExportPreflightRequest === 'function'
      ? buildExportPreflightRequest()
      : null;
  }

  async function check(body, generation) {
    timer = null;
    var preflight;
    try {
      preflight = await safeFetch('/api/jobs/export/preflight', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      }, {toast: false});
    } catch (_) {
      // Typing a destination passes through relative and missing paths.
      // Say nothing here: the check at submit reports a real failure.
      return;
    }
    if (generation !== checkGeneration || !isOpen()) return;
    if (!preflight || preflight.error) return;
    render(preflight);
  }

  function isOpen() {
    var el = overlay();
    return !!(el && el.classList.contains('open'));
  }

  // When the settings change, a pending or in-flight check and the notice
  // on screen describe names that no longer apply: drop them until the new
  // result lands.
  function schedule() {
    if (!isOpen()) return;
    var body = currentRequest();
    var key = body ? JSON.stringify(body) : null;
    if (key === checkedKey) return;
    checkedKey = key;
    var generation = ++checkGeneration;
    hide();
    if (timer) clearTimeout(timer);
    timer = null;
    if (body) {
      timer = setTimeout(function() { check(body, generation); }, DEBOUNCE_MS);
    }
  }

  function reset() {
    checkGeneration++;
    checkedKey = null;
    if (timer) clearTimeout(timer);
    timer = null;
    hide();
  }

  // Export was clicked: its own preflight supersedes any live check, so a
  // late live response cannot replace what the submit decides to show.
  function cancelPending() {
    checkGeneration++;
    if (timer) clearTimeout(timer);
    timer = null;
  }

  // Called with the submit-time preflight.
  function acknowledged(preflight) {
    cancelPending();
    if (!preflight.rename_count) {
      hide();
      return true;
    }
    if (shownSignature === signature(preflight)) return true;
    render(preflight);
    var el = notice();
    if (el && el.scrollIntoView) el.scrollIntoView({block: 'nearest'});
    return false;
  }

  function init() {
    if (!overlay()) return;
    ['input', 'change'].forEach(function(type) {
      overlay().addEventListener(type, function(event) {
        var target = event.target;
        if (!target || !target.matches || !target.matches('input, select, textarea')) return;
        schedule();
      });
    });
  }
  init();

  return {
    schedule: schedule,
    reset: reset,
    cancelPending: cancelPending,
    acknowledged: acknowledged,
  };
})();
