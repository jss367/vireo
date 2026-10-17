// Merge dialog helpers: OpenStreetMap links, coordinate formatting, species
// option titles, and the radio-choice markup.
// Classic page script; load boot.js after all definitions.

function osmLink(latitude, longitude, label) {
  const url = 'https://www.openstreetmap.org/?mlat=' + encodeURIComponent(latitude) +
    '&mlon=' + encodeURIComponent(longitude) + '#map=14/' + encodeURIComponent(latitude) +
    '/' + encodeURIComponent(longitude);
  return '<a href="' + escapeAttr(url) + '" target="_blank" rel="noopener" ' +
    'onclick="return openExternalLink(event, this.href)">' + escapeHtml(label) + '</a>';
}

function formatCoords(latitude, longitude) {
  return Number(latitude).toFixed(4) + ', ' + Number(longitude).toFixed(4);
}

/* Name the species, so the chooser answers "which species is this" rather
   than showing a row id. An iNat id with no local taxon still has to read
   as its own distinct answer, not borrow the other option's name. */
function speciesTitle(option) {
  if (option.taxon_common_name || option.taxon_name) {
    return option.taxon_common_name || option.taxon_name;
  }
  if (option.source_taxon_id !== null) {
    return 'iNaturalist taxon ' + option.source_taxon_id;
  }
  return option.name;
}

function speciesSubtitle(option) {
  if (option.taxon_name && option.taxon_name !== speciesTitle(option)) {
    return option.taxon_name;
  }
  if (option.taxon_name || option.taxon_common_name) return 'linked taxon';
  if (option.source_taxon_id !== null) return 'not in the local taxonomy';
  return 'no taxon record';
}

/* escapeHtml serializes a text node, which does NOT encode quotes, so it is
   only safe between tags. Anything landing inside an attribute value needs
   escapeAttr or a quote in a keyword name closes the attribute and can add
   an event handler. Keyword names come from imported XMP. */
function mergeChoice(group, value, checked, title, subtitle) {
  return '<label class="kw-merge-choice">' +
    '<input type="radio" name="' + group + '" value="' + escapeAttr(value) + '"' +
    (checked ? ' checked' : '') + '>' +
    '<span>' + title + (subtitle ? '<br><span class="kw-merge-sub">' + subtitle + '</span>' : '') +
    '</span></label>';
}
