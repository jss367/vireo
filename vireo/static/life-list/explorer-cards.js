// Explorer: summary chips, the drill body, breadcrumbs, and taxon cards.
// Classic page script; load boot.js after all definitions.

// ---------------- Explorer: summary + breadcrumb + cards (Task 11) ----------------

function renderSummaryBar(summary) {
  var order = summary.order || { found: 0, total: 0 };
  var family = summary.family || { found: 0, total: 0 };
  var genus = summary.genus || { found: 0, total: 0 };
  var species = summary.species || { found: 0, total: 0 };
  var activeRank = explorerRankView ? explorerRankView.rank : null;
  // Chips are clickable → open the flat rank breakdown. Fixed rank order:
  // order/family/genus/species. The rank matching the open flat view is marked
  // .active. Clicks are handled by the delegated listener in wireSummaryBar().
  function chip(v, label, rank) {
    var cls = 'll-sumchip' + (rank === activeRank ? ' active' : '');
    return '<span class="' + cls + '" data-rank="' + rank + '"><b>' + v.found + '/' + v.total + '</b> ' + label + '</span>';
  }
  return '<div class="ll-summary">'
    + chip(order, 'orders', 'order')
    + chip(family, 'families', 'family')
    + chip(genus, 'genera', 'genus')
    + chip(species, 'species', 'species')
    + '</div>';
}

// Return the list of nodes at the current drill level, honoring explorerPath.
function currentExplorerNodes() {
  var nodes = explorerData.nodes || [];
  for (var i = 0; i < explorerPath.length; i++) {
    var step = explorerPath[i];
    var match = nodes.find(function(n) { return n.id === step.id; });
    if (!match) return [];
    nodes = match.children || [];
  }
  return nodes;
}

function renderExplorerBody() {
  var body = document.getElementById('explorerBody');
  if (!body) return;
  // Keep the overview in lockstep with the cards. The current taxon becomes
  // the sunburst center and its descendants expand to use the full chart.
  renderSunburst();

  // Flat rank breakdown takes over the body when a summary chip is open.
  if (explorerRankView) { renderRankView(); return; }

  // If the drill path ends at a genus, the current card level has no nodes
  // (species aren't in explorerData.nodes) — render the species leaf instead of
  // an empty grid. This covers returning from the flat rank view via "Back to
  // cards", which clears explorerRankView but leaves explorerPath at the genus.
  var last = explorerPath.length ? explorerPath[explorerPath.length - 1] : null;
  if (last && last.rank === 'genus') {
    if (explorerLeafGenus && explorerLeafGenus.id === last.id && explorerLeafData) {
      // Leaf payload for this genus is already in hand — re-render it directly.
      renderExplorerLeaf(false);
    } else {
      // Load it. loadExplorerSpecies pushes the genus crumb itself, so drop the
      // genus from the path and locate its node under the parent family first
      // (mirrors openRankItem's genus handling).
      explorerPath = explorerPath.slice(0, -1);
      var genusNode = currentExplorerNodes().find(function(n) { return n.id === last.id; });
      if (genusNode) {
        loadExplorerSpecies(genusNode);
      } else {
        // Couldn't resolve the genus node — restore the path and show a
        // non-blank empty state.
        explorerPath.push(last);
        body.innerHTML = renderBreadcrumb() + '<div class="ll-exp-empty">Nothing to show.</div>';
      }
    }
    return;
  }

  var html = renderBreadcrumb();

  var nodes = currentExplorerNodes();
  // Defense-in-depth: never leave a blank card grid. If the level resolves to
  // zero nodes (and it wasn't a genus leaf, handled above), say so explicitly.
  if (!nodes.length) {
    body.innerHTML = html + '<div class="ll-exp-empty">Nothing to show.</div>';
    return;
  }
  html += '<div class="ll-cards">';
  nodes.forEach(function(n) { html += renderExplorerCard(n); });
  html += '</div>';
  body.innerHTML = html;

  body.querySelectorAll('.ll-card').forEach(function(card) {
    card.addEventListener('click', function() {
      var id = parseInt(card.getAttribute('data-id'), 10);
      var node = currentExplorerNodes().find(function(n) { return n.id === id; });
      if (!node) return;
      if (node.rank === 'genus') {
        loadExplorerSpecies(node);
      } else {
        explorerPath.push({ id: node.id, name: node.common_name || node.name, rank: node.rank });
        renderExplorerBody();
      }
    });
  });
}

function renderBreadcrumb() {
  var root = explorerData.root || {};
  var rootLabel = root.common_name || root.name || 'Class';
  var html = '<div class="ll-crumbs">';
  var isRootCurrent = explorerPath.length === 0;
  html += '<span class="ll-crumb' + (isRootCurrent ? ' current' : '') + '" data-depth="0">'
    + escapeHtml(rootLabel) + '</span>';
  explorerPath.forEach(function(step, i) {
    var current = (i === explorerPath.length - 1);
    html += '<span class="ll-crumb-sep">›</span>';
    html += '<span class="ll-crumb' + (current ? ' current' : '') + '" data-depth="' + (i + 1) + '">'
      + escapeHtml(step.name) + '</span>';
  });
  html += '</div>';
  // Crumb click handling is delegated once via wireBreadcrumb() on #explorerBody;
  // no per-render handler attachment here.
  return html;
}

// Inline SVG progress ring: accent arc over a --bg-tertiary track, pct% center.
function renderRing(found, total) {
  var pct = total > 0 ? Math.round((found / total) * 100) : 0;
  var r = 20, cx = 24, cy = 24;
  var circ = 2 * Math.PI * r;
  var dash = circ * (total > 0 ? found / total : 0);
  return '<svg class="ll-ring" width="48" height="48" viewBox="0 0 48 48">'
    + '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="var(--bg-tertiary)" stroke-width="4"/>'
    + '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="var(--accent)" stroke-width="4"'
    + ' stroke-dasharray="' + dash.toFixed(2) + ' ' + circ.toFixed(2) + '"'
    + ' stroke-linecap="round" transform="rotate(-90 ' + cx + ' ' + cy + ')"/>'
    + '<text x="' + cx + '" y="' + cy + '" text-anchor="middle" dominant-baseline="central">' + pct + '%</text>'
    + '</svg>';
}

function rankPlural(rank, n) {
  var map = { order: 'orders', family: 'families', genus: 'genera',
              species: 'species', class: 'classes' };
  return map[rank] || (rank + 's');
}

function renderExplorerCard(n) {
  var name = n.common_name || n.name;
  var sci = (n.common_name && n.common_name !== n.name)
    ? '<div class="ll-card-sci">' + escapeHtml(n.name) + '</div>' : '';
  var childRank = n.child_rank ? rankPlural(n.child_rank) : 'children';
  var sub = n.found_children + '/' + n.total_children + ' ' + childRank + ' · '
    + n.found_species + '/' + n.total_species + ' species';
  var emptyCls = (n.found_species === 0) ? ' empty' : '';
  return '<div class="ll-card' + emptyCls + '" data-id="' + escapeAttr(n.id) + '">'
    + renderRing(n.found_species, n.total_species)
    + '<div class="ll-card-body">'
    + '<div class="ll-card-name">' + escapeHtml(name) + '</div>'
    + sci
    + '<div class="ll-card-sub">' + sub + '</div>'
    + '</div></div>';
}
