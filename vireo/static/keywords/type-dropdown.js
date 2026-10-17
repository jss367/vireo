// Inline type change: the badge-click dropdown, closeDropdown, and updateType.
// Classic page script; load boot.js after all definitions.

// -- Inline type change via badge click --
function bindKeywordTypeBadges() {
  /** @type {HTMLElement} */ (document.getElementById('kwBody')).addEventListener('click', function(e) {
    if (!(e.target instanceof Element)) return; const badge = e.target.closest(/** @type {'span'} */ ('.kw-type-badge'));
    if (!badge || !badge.dataset.id) return;
    e.stopPropagation();
    closeDropdown();

    const kwId = parseInt(badge.dataset.id);
    const currentType = badge.dataset.type;
    const rect = badge.getBoundingClientRect();

    const dd = document.createElement('div');
    dd.className = 'kw-type-dropdown';
    dd.style.top = (rect.bottom + window.scrollY + 2) + 'px';
    dd.style.left = (rect.left + window.scrollX) + 'px';

    TYPES.forEach(t => {
      const item = document.createElement('div');
      item.className = 'kw-type-dropdown-item';
      item.innerHTML = '<span class="kw-type-badge type-' + t + '" style="cursor:inherit;pointer-events:none;">' + TYPE_ICONS[t] + ' ' + t + '</span>';
      if (t === currentType) { item.style.fontWeight = '700'; }
      item.addEventListener('click', function(ev) {
        ev.stopPropagation();
        closeDropdown();
        if (t !== currentType) { updateType(kwId, t); }
      });
      dd.appendChild(item);
    });

    document.body.appendChild(dd);
    activeDropdown = dd;
  });
}

function closeDropdown() {
  if (activeDropdown) { activeDropdown.remove(); activeDropdown = null; }
}

/** @param {number} kwId @param {string} newType */ async function updateType(kwId, newType) {
  try {
    const resp = await fetch('/api/keywords/' + kwId, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({type: newType})
    });
    // Retyping to a slot that already has a normalized-equal peer
    // deletes this keyword row and merges it into the peer's id. Refetch
    // instead of the local optimistic update, or allKeywords keeps a
    // phantom entry pointing at a deleted id.
    /** @type {KeywordMutation} */ const data = await resp.json().catch(() => ({}));
    if (data && data.merged) {
      await loadKeywords();
      return;
    }
    if (!resp.ok) throw new Error(data.error || 'Could not update keyword');
    await loadKeywords();
  } catch(e) { console.error('Failed to update type', e); }
}
