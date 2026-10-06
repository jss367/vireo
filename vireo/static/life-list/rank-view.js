// Explorer: the flat rank breakdown opened from a summary chip.
// Classic page script; load boot.js after all definitions.

// ---------------- Explorer: flat rank breakdown (clickable summary chips) ----------------
//
// Clicking a summary chip opens a flat list of EVERY taxon at that rank under
// the current class — seen ones lit, unseen dimmed — with a search box and a
// "Show missing only" filter. Server-authoritative (/api/life-list/explorer/rank)
// so counts always match the chips. A breadcrumb ("Birds › All families")
// returns to the normal drill cards.

var rankSearchTimer = null;   // debounce for the flat-view search input
var explorerRankReqId = 0;    // token to discard stale /rank responses (race guard)

// Fetch the flat rank list for the current class and switch the body into the
// flat view. Threads the current class root id from explorerData.root so the
// endpoint scopes to the same class the drill cards use. Clicking the already-
// open rank's chip toggles the view closed.
async function openRankView(rank) {
  // Drop any pending search debounce so it can't fire against a different view.
  clearTimeout(rankSearchTimer);
  if (explorerRankView && explorerRankView.rank === rank) {
    explorerRankView = null;
    explorerRankReqId++;   // invalidate any in-flight /rank fetch so it can't re-open the view
    renderExplorer();
    return;
  }
  var rootId = (explorerData && explorerData.root && explorerData.root.id != null)
    ? explorerData.root.id : null;
  var body = document.getElementById('explorerBody');
  if (body) body.innerHTML = '<div class="ll-exp-empty">Loading…</div>';
  var url = '/api/life-list/explorer/rank?rank=' + encodeURIComponent(rank)
    + (rootId != null ? ('&root=' + encodeURIComponent(rootId)) : '');
  var myReq = ++explorerRankReqId;   // claim a token; a later click/class-switch bumps it
  var data;
  try {
    data = await safeFetch(url);
  } catch (e) {
    if (myReq !== explorerRankReqId) return;   // superseded — leave the newer view alone
    if (body) body.innerHTML = '<div class="ll-exp-empty">Failed to load rank breakdown.</div>';
    return;
  }
  if (myReq !== explorerRankReqId) return;   // a newer click/class-switch won; discard this stale response
  explorerRankView = { rank: rank, data: data, search: '', missingOnly: false, showAll: false };
  renderExplorer();   // full rebuild so the chips pick up .active and the body flips
}

// Render the flat-view shell (breadcrumb + header + controls) once, then fill
// the grid via renderRankGrid(). The search input and checkbox live outside the
// grid container, so typing only refreshes the grid (renderRankGrid) and never
// steals focus from the input.
function renderRankView() {
  var body = document.getElementById('explorerBody');
  if (!body) return;
  var rv = explorerRankView;
  var data = rv.data || {};
  var rank = rv.rank;
  var pluralLabel = rankPlural(rank);
  var root = (explorerData && explorerData.root) || {};
  var rootLabel = root.common_name || root.name || 'Class';

  var html = '<div class="ll-crumbs">';
  html += '<span class="ll-crumb" id="rankBackCrumb">' + escapeHtml(rootLabel) + '</span>';
  html += '<span class="ll-crumb-sep">›</span>';
  html += '<span class="ll-crumb current">All ' + escapeHtml(pluralLabel) + '</span>';
  html += '</div>';

  html += '<div class="ll-leaf-head">';
  html += '<h3>' + (data.found || 0) + '/' + (data.total || 0) + ' ' + escapeHtml(pluralLabel) + ' seen</h3>';
  html += '<label>Search <input type="search" id="rankSearch" placeholder="Filter…" value="'
    + escapeAttr(rv.search || '') + '"></label>';
  html += '<label><input type="checkbox" id="rankMissingOnly"' + (rv.missingOnly ? ' checked' : '')
    + '> Show missing only</label>';
  html += '<button type="button" class="ll-linkbtn" id="rankBackBtn">← Back to cards</button>';
  html += '</div>';
  html += '<div id="rankNotice"></div>';
  html += '<div id="rankGrid"></div>';
  body.innerHTML = html;

  var back = function() {
    clearTimeout(rankSearchTimer);
    explorerRankView = null;
    explorerRankReqId++;   // invalidate any in-flight /rank fetch so it can't re-open the view
    renderExplorer();
  };
  var bc = document.getElementById('rankBackCrumb');
  if (bc) bc.addEventListener('click', back);
  var bb = document.getElementById('rankBackBtn');
  if (bb) bb.addEventListener('click', back);

  var search = document.getElementById('rankSearch');
  if (search) {
    search.addEventListener('input', function() {
      clearTimeout(rankSearchTimer);
      rankSearchTimer = setTimeout(function() {
        if (!explorerRankView) return;
        explorerRankView.search = search.value;
        explorerRankView.showAll = false;
        renderRankGrid();
      }, 150);
    });
  }
  var chk = document.getElementById('rankMissingOnly');
  if (chk) chk.addEventListener('change', function() {
    if (!explorerRankView) return;
    explorerRankView.missingOnly = chk.checked;
    explorerRankView.showAll = false;
    renderRankGrid();
  });

  renderRankGrid();
}

// Fill #rankNotice + #rankGrid from the current filter state. Kept separate from
// renderRankView so search/checkbox changes never rebuild (and refocus) the
// input. Applies the large-render guard: whenever the FINAL filtered list about
// to render exceeds 800 items — unfiltered, "Show missing only", or a broad
// search — a visible notice shows and the render caps to the first 500 unless
// "Show all" is pressed (No-black-boxes: never truncate silently, and never
// build an ~11k-tile innerHTML that can freeze the WKWebView renderer).
function renderRankGrid() {
  var grid = document.getElementById('rankGrid');
  var notice = document.getElementById('rankNotice');
  if (!grid) return;
  var rv = explorerRankView;
  var data = rv.data || {};
  var items = data.items || [];
  var rank = rv.rank;
  var term = (rv.search || '').trim().toLowerCase();

  var shown = items.filter(function(i) {
    if (rv.missingOnly && i.found) return false;
    if (term) {
      var n = (i.name || '').toLowerCase();
      var c = (i.common_name || '').toLowerCase();
      if (n.indexOf(term) === -1 && c.indexOf(term) === -1) return false;
    }
    return true;
  });

  // Cap based purely on the size of the final filtered list about to render, no
  // matter why it's large. Search narrows `shown` naturally; if that drops it to
  // <=800 there's no cap. N in the notice is the filtered count being capped, so
  // it never implies more or fewer than it actually shows.
  var CAP = 500, LARGE = 800;
  var renderList = shown;
  var noticeHtml = '';
  if (shown.length > LARGE) {
    if (rv.showAll) {
      noticeHtml = '<div class="ll-rank-notice">Showing all ' + shown.length + ' '
        + escapeHtml(rankPlural(rank)) + '.</div>';
    } else {
      noticeHtml = '<div class="ll-rank-notice">Showing the first ' + CAP + ' of ' + shown.length
        + '. Use search to narrow, or '
        + '<button type="button" class="ll-linkbtn" id="rankShowAll">Show all ' + shown.length
        + '</button>.</div>';
      renderList = shown.slice(0, CAP);
    }
  }
  if (notice) notice.innerHTML = noticeHtml;

  var gh = '';
  if (rank === 'species') {
    gh += '<div class="ll-leaf-grid">';
    renderList.forEach(function(i) { gh += renderSpeciesTile(i); });
    gh += '</div>';
  } else {
    gh += '<div class="ll-cards">';
    renderList.forEach(function(i) { gh += renderRankCard(i); });
    gh += '</div>';
  }
  if (!shown.length) gh += '<div class="ll-exp-empty">Nothing to show.</div>';
  grid.innerHTML = gh;

  var showAll = document.getElementById('rankShowAll');
  if (showAll) showAll.addEventListener('click', function() {
    if (!explorerRankView) return;
    explorerRankView.showAll = true;
    renderRankGrid();
  });

  // order/family/genus rows drill into the normal card/species view. Species
  // rows are non-interactive.
  if (rank !== 'species') {
    grid.querySelectorAll('.ll-card').forEach(function(card) {
      card.addEventListener('click', function() {
        var id = parseInt(card.getAttribute('data-id'), 10);
        if (!isNaN(id)) openRankItem(id);
      });
    });
  }
}

// Card for an order/family/genus row in the flat view. Reuses the .ll-card look
// (ring + name + scientific + species count). The `order` context label is only
// present for family/genus rows (null for order-rank items themselves).
function renderRankCard(i) {
  var name = i.common_name || i.name;
  var sci = (i.common_name && i.common_name !== i.name)
    ? '<div class="ll-card-sci">' + escapeHtml(i.name) + '</div>' : '';
  var sub = i.found_species + '/' + i.total_species + ' species';
  if (i.order) sub += ' · ' + escapeHtml(i.order);
  var emptyCls = !i.found ? ' empty' : '';
  return '<div class="ll-card' + emptyCls + '" data-id="' + escapeAttr(i.id) + '">'
    + renderRing(i.found_species, i.total_species)
    + '<div class="ll-card-body">'
    + '<div class="ll-card-name">' + escapeHtml(name) + '</div>'
    + sci
    + '<div class="ll-card-sub">' + sub + '</div>'
    + '</div></div>';
}

// Build the drill lineage (root→node, excluding the class root) for a taxon id
// from explorerData.nodes. Returns [{id,name,rank}] or null if not in the tree
// (species aren't in the tree, so those rows stay non-navigable).
function explorerLineage(id) {
  var path = null;
  (function walk(list, acc) {
    if (path || !list) return;
    for (var k = 0; k < list.length; k++) {
      var n = list[k];
      var next = acc.concat([{ id: n.id, name: n.common_name || n.name, rank: n.rank }]);
      if (n.id === id) { path = next; return; }
      walk(n.children || [], next);
      if (path) return;
    }
  })((explorerData && explorerData.nodes) || [], []);
  return path;
}

// Locate the raw tree node (with children) for a taxon id in explorerData.nodes.
function explorerFindNode(id) {
  var found = null;
  (function walk(list) {
    if (found || !list) return;
    for (var k = 0; k < list.length; k++) {
      var n = list[k];
      if (n.id === id) { found = n; return; }
      walk(n.children || []);
      if (found) return;
    }
  })((explorerData && explorerData.nodes) || []);
  return found;
}

// Click-through from a flat-view order/family/genus row into the drill view.
// Sets explorerPath to the row's lineage and clears the flat view. Genus rows
// open the species leaf. If the id isn't in the tree, do nothing (non-navigable).
function openRankItem(id) {
  var lineage = explorerLineage(id);
  var node = explorerFindNode(id);
  if (!lineage || !node) return;
  explorerRankView = null;
  explorerRankReqId++;   // invalidate any in-flight /rank fetch so it can't re-open the flat view over the drill
  if (node.rank === 'genus') {
    explorerPath = lineage.slice(0, -1);   // drill to the parent family
    renderExplorer();
    loadExplorerSpecies(node);             // then open the genus's species leaf
  } else {
    explorerPath = lineage.slice();
    renderExplorer();
  }
}
