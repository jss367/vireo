// Explorer: the zoomable sunburst overview and its tooltip.
// Classic page script; load boot.js after all definitions.

// ---------------- Explorer: sunburst overview (Task 13) ----------------
//
// Zoomable taxonomic completeness: at the class root, three rings show orders
// → families → genera. As the user selects a subset, that taxon moves into the
// center and its descendants expand to fill the available rings. Species are
// not materialized in this tree, so a selected genus is represented by the
// center while its species are listed below. Each node's angular span is
// proportional to its total_species; children partition their parent's arc in
// the same order the cards use (explorerData.nodes is already found-first-then-
// alpha from the server). Arc fill is var(--accent) at an opacity scaled by
// found_species/total_species, over a var(--bg-tertiary) ghost track, so an
// untouched branch reads clearly faded and a complete branch fully saturated.
// Clicking an arc drills in; clicking the center moves up one level.

var explorerSunburstLineage = {};   // node id -> [{id,name,rank}] root→node lineage

// Resolve explorerPath against the materialized tree. Keeping this separate
// from currentExplorerNodes makes the selected taxon's own rollup available to
// the sunburst center as well as its children.
function currentExplorerFocus() {
  var nodes = (explorerData && explorerData.nodes) || [];
  var focus = null;
  var lineage = [];
  for (var i = 0; i < explorerPath.length; i++) {
    var step = explorerPath[i];
    var match = nodes.find(function(n) { return n.id === step.id; });
    if (!match) break;
    focus = match;
    lineage.push({ id: match.id, name: match.common_name || match.name, rank: match.rank });
    nodes = match.children || [];
  }
  return { node: focus, children: focus ? (focus.children || []) : nodes, lineage: lineage };
}

function explorerTreeDepth(nodes) {
  var depth = 0;
  (nodes || []).forEach(function(node) {
    depth = Math.max(depth, 1 + explorerTreeDepth(node.children || []));
  });
  return depth;
}

// Convert a polar point (center + radius + angle in radians, 0 = top,
// clockwise) to SVG x/y. Returns a string "x,y".
function llPolar(cx, cy, r, a) {
  return (cx + r * Math.sin(a)).toFixed(3) + ',' + (cy - r * Math.cos(a)).toFixed(3);
}

// SVG path for a single ring segment (annular sector) spanning [a0,a1] radians
// between inner radius r0 and outer radius r1.
function llArcPath(cx, cy, r0, r1, a0, a1) {
  // A single child spanning (near) a full 360° collapses to a degenerate arc
  // because start==end. Draw a full annulus (two stacked half-arcs) instead.
  if ((a1 - a0) >= 2 * Math.PI - 1e-6) {
    var m = a0 + Math.PI;
    return llArcPath(cx, cy, r0, r1, a0, m) + ' ' + llArcPath(cx, cy, r0, r1, m, a1);
  }
  var largeArc = (a1 - a0) > Math.PI ? 1 : 0;
  var p0 = llPolar(cx, cy, r1, a0);   // outer start
  var p1 = llPolar(cx, cy, r1, a1);   // outer end
  var p2 = llPolar(cx, cy, r0, a1);   // inner end
  var p3 = llPolar(cx, cy, r0, a0);   // inner start
  return 'M' + p0
    + ' A' + r1.toFixed(3) + ',' + r1.toFixed(3) + ' 0 ' + largeArc + ' 1 ' + p1
    + ' L' + p2
    + ' A' + r0.toFixed(3) + ',' + r0.toFixed(3) + ' 0 ' + largeArc + ' 0 ' + p3
    + ' Z';
}

// Accent fill at an opacity scaled by completeness. 0 found → a ghost track so
// untouched branches read faded; 1.0 → full accent. Zero-total arcs (should not
// happen below genus, but guard anyway) render as the plain track.
function llArcFill(found, total) {
  if (!total || total <= 0) return { fill: 'var(--bg-tertiary)', op: 1 };
  var frac = Math.max(0, Math.min(1, found / total));
  if (frac <= 0) return { fill: 'var(--bg-tertiary)', op: 1 };
  return { fill: 'var(--accent)', op: (0.15 + 0.85 * frac) };
}

function renderSunburst() {
  var host = document.getElementById('explorerSunburst');
  if (!host) return;
  var focus = currentExplorerFocus();
  var top = focus.children;
  explorerSunburstLineage = {};

  // Keep the selected center visible even when it has no materialized species
  // below it. layout() partitions zero-total children equally, so incomplete
  // reference branches still retain both their arcs and up-one navigation.
  host.style.display = 'flex';

  var SIZE = 360, cx = SIZE / 2, cy = SIZE / 2;
  var R_CENTER = 46;                       // clickable reset hub
  var R_OUTER = 176;
  var visibleDepth = Math.max(1, explorerTreeDepth(top));
  var rings = [R_CENTER];
  for (var ringIndex = 1; ringIndex <= visibleDepth; ringIndex++) {
    rings.push(R_CENTER + ((R_OUTER - R_CENTER) * ringIndex / visibleDepth));
  }

  var root = (explorerData && explorerData.root) || {};
  var rootLineage = focus.lineage;   // lineage entries accumulate as we recurse
  var arcs = [];          // collected {path, fill, op, tip, lineage}

  var FULL = 2 * Math.PI;

  // Recurse a level, laying children out proportional to total_species within
  // [a0,a1]. The first visible level depends on the active selection.
  function layout(list, a0, a1, depth, lineage) {
    if (depth >= visibleDepth || !list || !list.length) return;
    var span = a1 - a0;
    // Denominator = sum of this level's total_species; fall back to equal
    // partition if every child is zero (degenerate, keeps geometry valid).
    var sum = 0;
    list.forEach(function(n) { sum += (n.total_species || 0); });
    var equal = sum <= 0;
    var cursor = a0;
    list.forEach(function(n) {
      var w = equal ? (span / list.length)
                    : (span * (n.total_species || 0) / sum);
      var s = cursor, e = cursor + w;
      cursor = e;
      if (w <= 0) return;   // zero-weight child gets no arc
      var lin = lineage.concat([{ id: n.id, name: n.common_name || n.name, rank: n.rank }]);
      explorerSunburstLineage[n.id] = lin;
      var shade = llArcFill(n.found_species || 0, n.total_species || 0);
      arcs.push({
        id: n.id,
        path: llArcPath(cx, cy, rings[depth], rings[depth + 1], s, e),
        fill: shade.fill, op: shade.op,
        name: n.common_name || n.name,
        sci: (n.common_name && n.common_name !== n.name) ? n.name : '',
        found: n.found_species || 0, total: n.total_species || 0
      });
      layout(n.children || [], s, e, depth + 1, lin);
    });
  }
  layout(top, 0, FULL, 0, rootLineage);

  var parts = [];
  parts.push('<svg class="ll-sb-svg" width="' + SIZE + '" height="' + SIZE + '" viewBox="0 0 ' + SIZE + ' ' + SIZE + '"'
    + ' role="img" aria-label="Taxonomic completeness sunburst">');
  arcs.forEach(function(a) {
    var pct = a.total > 0 ? Math.round((a.found / a.total) * 100) : 0;
    parts.push('<path class="ll-sb-arc" d="' + a.path + '" fill="' + a.fill + '"'
      + ' fill-opacity="' + a.op.toFixed(3) + '"'
      + ' data-id="' + escapeAttr(a.id) + '"'
      + ' data-name="' + escapeAttr(a.name) + '"'
      + ' data-sci="' + escapeAttr(a.sci) + '"'
      + ' data-found="' + a.found + '" data-total="' + a.total + '" data-pct="' + pct + '"></path>');
  });
  // Center hub = active selection. Clicking it moves up one level.
  var summary = (explorerData && explorerData.summary) || {};
  var selected = focus.node || root;
  var sp = focus.node
    ? { found: focus.node.found_species || 0, total: focus.node.total_species || 0 }
    : (summary.species || { found: 0, total: 0 });
  var centerPct = sp.total > 0 ? Math.round((sp.found / sp.total) * 100) : 0;
  var centerName = selected.common_name || selected.name || 'All';
  var centerLabel = centerName.length > 14 ? centerName.slice(0, 13) + '…' : centerName;
  var centerSci = (selected.common_name && selected.common_name !== selected.name) ? selected.name : '';
  parts.push('<circle class="ll-sb-center" id="explorerSunburstCenter" cx="' + cx + '" cy="' + cy + '" r="' + R_CENTER + '"'
    + ' data-name="' + escapeAttr(centerName) + '" data-sci="' + escapeAttr(centerSci) + '"'
    + ' data-found="' + sp.found + '" data-total="' + sp.total + '" data-pct="' + centerPct + '"></circle>');
  parts.push('<text class="ll-sb-center-label" x="' + cx + '" y="' + (cy - 6) + '">' + escapeHtml(centerLabel) + '</text>');
  parts.push('<text class="ll-sb-center-label" x="' + cx + '" y="' + (cy + 11) + '" style="font-size:11px;fill:var(--text-secondary)">' + centerPct + '%</text>');
  parts.push('</svg>');
  host.innerHTML = parts.join('');

  wireSunburst(host);
}

// Shared floating tooltip element (one per document, reused).
function llSunburstTooltip() {
  var tip = document.getElementById('explorerSunburstTip');
  if (!tip) {
    tip = document.createElement('div');
    tip.id = 'explorerSunburstTip';
    tip.className = 'll-sb-tooltip';
    document.body.appendChild(tip);
  }
  return tip;
}

function wireSunburst(host) {
  var svg = host.querySelector('.ll-sb-svg');
  if (!svg) return;
  var tip = llSunburstTooltip();

  function showTip(el, ev) {
    var name = el.getAttribute('data-name') || '';
    var sci = el.getAttribute('data-sci') || '';
    var found = el.getAttribute('data-found') || '0';
    var total = el.getAttribute('data-total') || '0';
    var pct = el.getAttribute('data-pct') || '0';
    var html = '<b>' + escapeHtml(name) + '</b>';
    if (sci) html += ' <span class="ll-sb-tt-sci">' + escapeHtml(sci) + '</span>';
    html += '<br>' + escapeHtml(found) + '/' + escapeHtml(total) + ' species (' + escapeHtml(pct) + '%)';
    tip.innerHTML = html;
    tip.style.display = 'block';
    moveTip(ev);
  }
  function moveTip(ev) {
    // Offset from cursor; clamp to viewport so the tip never spills off-screen.
    var pad = 14, tw = tip.offsetWidth, th = tip.offsetHeight;
    var x = ev.clientX + pad, y = ev.clientY + pad;
    if (x + tw > window.innerWidth - 6) x = ev.clientX - pad - tw;
    if (y + th > window.innerHeight - 6) y = ev.clientY - pad - th;
    tip.style.left = Math.max(6, x) + 'px';
    tip.style.top = Math.max(6, y) + 'px';
  }
  function hideTip() { tip.style.display = 'none'; }

  svg.addEventListener('mousemove', function(ev) {
    var t = ev.target.closest('.ll-sb-arc, .ll-sb-center');
    if (t) { showTip(t, ev); } else { hideTip(); }
  });
  svg.addEventListener('mouseleave', hideTip);

  // Click an arc → drill via its precomputed lineage; click center → up one.
  svg.addEventListener('click', function(ev) {
    var center = ev.target.closest('.ll-sb-center');
    if (center) {
      if (explorerPath.length) explorerPath = explorerPath.slice(0, -1);
      hideTip();
      renderExplorerBody();
      return;
    }
    var arc = ev.target.closest('.ll-sb-arc');
    if (!arc) return;
    var id = parseInt(arc.getAttribute('data-id'), 10);
    var lineage = explorerSunburstLineage[id];
    if (!lineage) return;
    hideTip();
    // Genus arcs: drilling to a genus loads its species leaf (matches card
    // behavior); anything above genus just sets the path and shows cards.
    var leaf = lineage[lineage.length - 1];
    if (leaf.rank === 'genus') {
      // Set path to the genus's parent chain, then load species (which pushes
      // the genus crumb itself), mirroring a card click on that genus.
      explorerPath = lineage.slice(0, -1);
      var node = currentExplorerNodes().find(function(n) { return n.id === id; });
      if (node) { loadExplorerSpecies(node); return; }
      // Fallback: if the node isn't in the current level, just set the full path.
      explorerPath = lineage.slice();
      renderExplorerBody();
      return;
    }
    explorerPath = lineage.slice();
    renderExplorerBody();
  });
}
