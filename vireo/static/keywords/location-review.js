// The location-match review panel: loading matches and combining a keyword
// into its linked place.
// Classic page script; load boot.js after all definitions.

async function loadLocationMatches() {
  const container = /** @type {HTMLElement} */ (document.getElementById('kwLocationMatches'));
  container.textContent = 'Loading matches…';
  try {
    const response = await fetch('/api/keywords/location-matches');
    if (!response.ok) throw new Error('Could not load location matches.');
    /** @type {KeywordLocationMatch[]} */ const matches = await response.json();
    container.innerHTML = matches.length ? matches.map(c =>
      '<div style="margin-bottom:12px">' +
      '<strong>' + escapeHtml(c.source_path.join(' → ')) + '</strong> (' + c.source_count + ' photos)' +
      '<div>Linked place: ' + escapeHtml(c.target_path.join(' → ')) +
      ' · <a target="_blank" rel="noopener" href="https://www.google.com/maps/search/?api=1&amp;query=' +
      encodeURIComponent(c.name) + '&amp;query_place_id=' + encodeURIComponent(c.place_id) + '">View place</a></div>' +
      '<div>' + c.combined_count + ' distinct photos after combining</div>' +
      (c.conflicting_photo_count ? '<div>' + c.conflicting_photo_count + ' photos already have a different linked place. Resolve those locations before combining.</div>' : '') +
      '<button type="button" class="kw-link-btn" ' + (c.conflicting_photo_count ? 'disabled ' : '') +
      'data-source="' + c.source_id + '" data-target="' + c.target_id + '">Use this place</button></div>'
    ).join('') : 'No matching location keywords to review.';
  } catch (error) { container.textContent = keywordErrorMessage(error); }
}

function bindLocationReview() {
  /** @type {HTMLElement} */ (document.getElementById('kwReviewLocations')).addEventListener('click', function() {
    /** @type {HTMLElement} */ (document.getElementById('kwLocationReview')).hidden = false;
    loadLocationMatches();
  });
  /** @type {HTMLElement} */ (document.getElementById('kwLocationMatches')).addEventListener('click', async function(event) {
    if (!(event.target instanceof Element)) return; const button = event.target.closest(/** @type {'button'} */ ('button[data-source]'));
    if (!button) return;
    button.disabled = true;
    try {
      const response = await fetch('/api/keywords/reconcile-location', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({source_id: Number(button.dataset.source), target_id: Number(button.dataset.target)})
      });
      /** @type {KeywordMutation} */ const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Could not combine locations.');
      selectedIds.clear();
      await loadKeywords();
      await loadLocationMatches();
      showToast('Location keywords combined.');
    } catch (error) {
      showToast(keywordErrorMessage(error), 'error');
      button.disabled = false;
    }
  });
}
