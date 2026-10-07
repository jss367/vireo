// Explorer: shared state, loading /api/life-list/explorer, the taxonomy download, and honesty states.
// Classic page script; load boot.js after all definitions.

// ---------------- Explorer: data load + honesty states (Task 10) ----------------

var explorerData = null;      // last payload from /api/life-list/explorer
var explorerPath = [];        // [{id,name,rank}] drill path from class root down
var explorerRankView = null;  // {rank,data,search,missingOnly,showAll} when the flat rank view is open, else null
// Token for the requests that fill #explorerBody (the genus species leaf and the
// flat rank view). Each one claims it; every navigation that replaces the body
// bumps it, so a late response for a view the user already left is dropped.
var explorerViewReqId = 0;

// Load the explorer tree for a class (rootId = class taxon id, or omit for
// the default Aves root). Stores the payload and re-renders.
async function loadExplorer(rootId) {
  var panel = document.getElementById('tab-explorer');
  explorerViewReqId++;   // invalidate any in-flight /rank or /species fetch from the old class
  panel.innerHTML = '<div class="ll-exp"><div class="ll-exp-empty">Loading…</div></div>';
  try {
    explorerData = await safeFetch('/api/life-list/explorer' + (rootId ? ('?root=' + encodeURIComponent(rootId)) : ''));
  } catch (e) {
    panel.innerHTML = '<div class="ll-exp"><div class="ll-exp-empty">Failed to load explorer.</div></div>';
    return;
  }
  explorerPath = [];        // reset drill path whenever a new class is loaded
  explorerRankView = null;  // and exit the flat rank view on class change
  renderExplorer();
}

// Trigger the taxonomy download job, then poll it and reload the explorer on
// completion. Mirrors settings.html's downloadTaxonomy() pattern.
async function explorerDownloadTaxonomy(btn) {
  btn.disabled = true;
  btn.textContent = 'Downloading… (see Jobs panel)';
  try {
    var data = await safeFetch('/api/jobs/download-taxonomy', { method: 'POST' }, { toast: false });
    if (typeof safeEventSource === 'function' && data && data.job_id) {
      safeEventSource('/api/jobs/' + data.job_id + '/stream', {
        onComplete: function() { loadExplorer(); }
      });
    }
  } catch (e) {
    btn.disabled = false;
    btn.textContent = 'Download the taxonomy';
  }
}

function renderExplorer() {
  var panel = document.getElementById('tab-explorer');
  var d = explorerData || {};

  // Honesty state 1: reference taxonomy not downloaded — no fake 0/0.
  if (!d.taxonomy_ready) {
    panel.innerHTML =
      '<div class="ll-exp"><div class="ll-exp-cta">'
      + '<h2>Download the taxonomy to see completeness</h2>'
      + '<p>The explorer compares your tagged species against the full iNaturalist '
      + 'reference taxonomy (orders, families, genera, species). That reference '
      + "hasn't been downloaded yet, so there are no totals to measure against.</p>"
      + '<button class="publish-btn" onclick="explorerDownloadTaxonomy(this)">Download the taxonomy</button>'
      + '</div></div>';
    return;
  }

  var html = '<div class="ll-exp">';

  // Class selector — always includes the current root so Birds stays selectable
  // even before the user has tagged any birds.
  html += '<div class="ll-exp-controls">';
  html += '<label for="explorerClass">Class</label>';
  html += '<select id="explorerClass" onchange="loadExplorer(this.value)">';
  var classes = (d.classes || []).slice();
  var root = d.root || {};
  var seenRoot = classes.some(function(c) { return c.id === root.id; });
  if (root.id != null && !seenRoot) {
    classes.unshift({ id: root.id, name: root.name, common_name: root.common_name });
  }
  classes.forEach(function(c) {
    var label = c.common_name || c.name;
    var sel = (root.id != null && c.id === root.id) ? ' selected' : '';
    html += '<option value="' + escapeAttr(c.id) + '"' + sel + '>' + escapeAttr(label) + '</option>';
  });
  html += '</select>';
  html += '</div>';

  var summary = d.summary || {};
  var speciesTotal = summary.species ? summary.species.total : 0;

  // Honesty state 2: taxonomy ready but this class has no reference taxa.
  if (!speciesTotal) {
    html += '<div class="ll-exp-empty">No reference taxa for this class.</div>';
    html += renderUnmatched(d);
    html += '</div>';
    panel.innerHTML = html;
    return;
  }

  // Sunburst goes above the summary chips; rebuilt on every explorer render
  // (class switch / reload) so no stale SVG survives — panel.innerHTML below
  // wipes any prior container.
  html += '<div class="ll-sunburst" id="explorerSunburst"></div>';
  html += renderSummaryBar(summary);
  html += renderUnmatched(d);
  html += '<div id="explorerBody"></div>';
  html += '</div>';
  panel.innerHTML = html;

  wireSummaryBar();
  wireBreadcrumb(document.getElementById('explorerBody'));
  renderExplorerBody();
}

// Single delegated click listener for the summary chips. Attached once to the
// persistent #tab-explorer panel (which survives every renderExplorer()
// innerHTML rebuild), so re-rendering never accumulates duplicate handlers.
// Mirrors wireBreadcrumb's delegation pattern. Reads data-rank off the clicked
// chip and opens the flat rank breakdown for that rank.
function wireSummaryBar() {
  var panel = document.getElementById('tab-explorer');
  if (!panel || panel._llSummaryWired) return;
  panel._llSummaryWired = true;
  panel.addEventListener('click', function(event) {
    var chip = event.target.closest('.ll-sumchip');
    if (!chip || !panel.contains(chip)) return;
    var rank = chip.getAttribute('data-rank');
    if (rank) openRankView(rank);
  });
}

// Single delegated click listener for breadcrumb crumbs. Attached once to the
// persistent #explorerBody container, so re-rendering its innerHTML (cards,
// species leaf, loading states) never accumulates duplicate handlers.
function wireBreadcrumb(container) {
  if (!container) return;
  container.addEventListener('click', function(event) {
    var crumb = event.target.closest('.ll-crumb');
    if (!crumb || !container.contains(crumb)) return;
    var depth = parseInt(crumb.getAttribute('data-depth'), 10);
    if (isNaN(depth) || depth === explorerPath.length) return;   // current crumb: no-op
    explorerPath = explorerPath.slice(0, depth);
    explorerViewReqId++;   // supersede any in-flight /species or /rank fetch
    renderExplorerBody();
  });
}

function uncountedBrowseHref(entry) {
  // The compact token is expanded by Browse into the same server-side
  // ancestor-suppression predicate that produced ``photo_count``. Embedding
  // every matching photo ID here would exceed browser/server URL limits for
  // large catalogs, while a bare keyword rule would include redundant tags.
  var filters = { root: { mode: 'all', rules: [
    { field: 'life_list_uncounted', op: 'is', value: entry.filter_token }
  ] } };
  return '/browse?filters=' + encodeURIComponent(JSON.stringify(filters));
}

function renderUncountedGroup(entries, count, message) {
  if (!count) return '';
  var shown = entries || [];
  var truncated = count > shown.length;
  var html = '<div class="ll-unmatched">' + escapeHtml(message);
  html += '<details><summary>'
    + (truncated ? ('Review the first ' + shown.length + ' of ' + count) : 'Review them')
    + '</summary><ul>';
  shown.forEach(function(entry) {
    var reason = entry.reason === 'higher_rank' && entry.taxon_rank
      ? ('Identified at ' + entry.taxon_rank + ' rank')
      : 'No species-level taxonomy match';
    var photos = entry.photo_count + ' photo' + (entry.photo_count === 1 ? '' : 's');
    html += '<li><span class="ll-unmatched-name">' + escapeHtml(entry.name) + '</span>'
      + '<span class="ll-unmatched-reason">&mdash; ' + escapeHtml(reason)
      + ' &middot; ' + photos + '</span>'
      + '<a href="' + escapeAttr(uncountedBrowseHref(entry)) + '">View photos</a></li>';
  });
  html += '</ul>';
  if (truncated) html += '<div style="margin-top:4px;">…and ' + (count - shown.length) + ' more not shown.</div>';
  html += '</details></div>';
  return html;
}

// Reasoned, class-aware disclosure for accepted identification labels that
// cannot contribute to exact species completeness.
function renderUnmatched(d) {
  var u = d.uncounted_identifications;
  if (!u) return '';
  var root = d.root || {};
  var className = root.common_name || root.name || 'this class';
  var scopedCount = u.scoped_count || 0;
  var workspaceCount = u.workspace_count || 0;
  var html = '';
  html += renderUncountedGroup(
    u.scoped, scopedCount,
    scopedCount + ' identification label' + (scopedCount === 1 ? '' : 's')
      + ' in ' + className + (scopedCount === 1 ? " isn't" : " aren't")
      + ' included in species totals.'
  );
  html += renderUncountedGroup(
    u.workspace, workspaceCount,
    workspaceCount + ' identification label' + (workspaceCount === 1 ? '' : 's')
      + " elsewhere in this workspace can't be assigned to one taxonomic class."
  );
  return html;
}
