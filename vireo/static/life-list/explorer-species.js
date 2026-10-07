// Explorer: the species leaf for a genus.
// Classic page script; load boot.js after all definitions.

// ---------------- Explorer: species leaf (Task 12) ----------------

var explorerLeafData = null;   // last species-leaf payload
var explorerLeafGenus = null;  // {id,name,rank} of the genus being shown

async function loadExplorerSpecies(node) {
  var myReq = ++explorerViewReqId;   // claim the body token; any later navigation bumps it
  explorerLeafGenus = { id: node.id, name: node.common_name || node.name, rank: node.rank };
  // Drop the previous genus's payload now, so a view that returns to this genus
  // before the response lands (Back to cards) reloads it instead of showing the
  // old genus's species under this genus's name.
  explorerLeafData = null;
  explorerPath.push(explorerLeafGenus);
  renderSunburst();
  var body = document.getElementById('explorerBody');
  if (body) body.innerHTML = renderBreadcrumb() + '<div class="ll-exp-empty">Loading species…</div>';
  var data;
  try {
    data = await safeFetch('/api/life-list/explorer/species?genus=' + encodeURIComponent(node.id));
  } catch (e) {
    if (myReq !== explorerViewReqId) return;   // superseded — leave the newer view alone
    if (body) body.innerHTML = renderBreadcrumb() + '<div class="ll-exp-empty">Failed to load species.</div>';
    return;
  }
  if (myReq !== explorerViewReqId) return;   // the user navigated away; discard this stale response
  explorerLeafData = data;
  renderExplorerLeaf(false);
}

function renderExplorerLeaf(missingOnly) {
  var body = document.getElementById('explorerBody');
  if (!body) return;
  var species = (explorerLeafData && explorerLeafData.species) || [];
  var found = species.filter(function(s) { return s.found; }).length;
  var genusName = explorerLeafGenus ? explorerLeafGenus.name : '';

  var html = renderBreadcrumb();
  html += '<div class="ll-leaf-head">';
  html += '<h3>' + found + '/' + species.length + ' species in ' + escapeHtml(genusName) + '</h3>';
  html += '<label><input type="checkbox" id="explorerMissingOnly"' + (missingOnly ? ' checked' : '') + '> Show missing only</label>';
  html += '</div>';

  var shown = missingOnly ? species.filter(function(s) { return !s.found; }) : species;
  html += '<div class="ll-leaf-grid">';
  shown.forEach(function(s) { html += renderSpeciesTile(s); });
  html += '</div>';
  if (!shown.length) html += '<div class="ll-exp-empty">Nothing to show.</div>';
  body.innerHTML = html;

  // Breadcrumb clicks are handled by the delegated wireBreadcrumb listener; wire the checkbox here.
  var chk = document.getElementById('explorerMissingOnly');
  if (chk) chk.addEventListener('change', function() { renderExplorerLeaf(chk.checked); });
}

function renderSpeciesTile(s) {
  var common = (s.common_name && s.common_name !== s.name)
    ? '<div class="ll-sp-common">' + escapeHtml(s.common_name) + '</div>' : '';
  if (s.found) {
    var thumb = s.photo ? photoThumbnailUrl(s.photo) : '';
    var img = thumb
      ? '<img src="' + escapeAttr(thumb) + '" alt="' + escapeAttr(s.photo ? s.photo.filename : s.name) + '" loading="lazy">'
      : '<div class="ll-sp-ph"></div>';
    return '<div class="ll-sp">' + img
      + '<div class="ll-sp-info">'
      + '<div class="ll-sp-name">' + escapeHtml(s.name) + '</div>'
      + common
      + '<span class="ll-sp-chip seen">✓ seen</span>'
      + '</div></div>';
  }
  return '<div class="ll-sp missing"><div class="ll-sp-ph"></div>'
    + '<div class="ll-sp-info">'
    + '<div class="ll-sp-name">' + escapeHtml(s.name) + '</div>'
    + common
    + '<span class="ll-sp-chip notyet">not yet</span>'
    + '</div></div>';
}
