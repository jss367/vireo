// Recent destinations shortcuts in the folder browser (/api/recent-destinations).
// Classic page script; load boot.js after all definitions.

/** @param {string} path */
function recentDestinationParts(path) {
  var trimmed = path.replace(/[\\/]+$/, '');
  return trimmed.split(/[\\/]/).filter(Boolean);
}

/** @param {unknown} paths */
function renderRecentDestinations(paths) {
  var section = /** @type {HTMLElement} */ (document.getElementById('moveBrowserRecents'));
  var list = /** @type {HTMLElement} */ (document.getElementById('moveBrowserRecentList'));
  if (!section || !list) return;

  var seen = new Set();
  recentDestinations = (Array.isArray(paths) ? paths : []).filter(function(path) {
    if (typeof path !== 'string' || !path || seen.has(path)) return false;
    seen.add(path);
    return true;
  }).slice(0, 5);

  /** @type {Record<string, number>} */ var leafCounts = {};
  recentDestinations.forEach(function(path) {
    var parts = recentDestinationParts(path);
    var leaf = parts[parts.length - 1] || path;
    leafCounts[leaf] = (leafCounts[leaf] || 0) + 1;
  });

  list.replaceChildren();
  recentDestinations.forEach(function(path) {
    var parts = recentDestinationParts(path);
    var leaf = parts[parts.length - 1] || path;
    var label = leaf;
    if ((leafCounts[leaf] || 0) > 1 && parts.length > 1) {
      label = parts[parts.length - 2] + ' / ' + leaf;
    }
    var button = document.createElement('button');
    button.type = 'button';
    button.textContent = label;
    button.title = path;
    button.setAttribute('aria-label', 'Go to ' + path);
    button.addEventListener('click', function() { browseTo(path); });
    list.appendChild(button);
  });
  section.style.display = recentDestinations.length ? '' : 'none';
}

async function loadRecentDestinations() {
  var seq = ++recentDestinationsSeq;
  try {
    var cfg = await safeFetch('/api/config', undefined, {toast: false});
    if (seq !== recentDestinationsSeq) return;
    var ingest = cfg && cfg.ingest;
    renderRecentDestinations(ingest && ingest.recent_destinations);
  } catch (e) {
    if (seq === recentDestinationsSeq) renderRecentDestinations([]);
  }
}

/** @param {string} path */
async function rememberRecentDestination(path) {
  var seq = ++recentDestinationsSeq;
  renderRecentDestinations(
    [path].concat(recentDestinations.filter(function(item) { return item !== path; }))
  );
  try {
    var data = await safeFetch('/api/recent-destinations', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: path}),
    }, {toast: false});
    if (seq === recentDestinationsSeq && data && Array.isArray(data.recent_destinations)) {
      renderRecentDestinations(data.recent_destinations);
    }
  } catch (e) {
    // The shortcut is a convenience; a settings write failure must not
    // prevent the selected destination from being used for this move.
  }
}
