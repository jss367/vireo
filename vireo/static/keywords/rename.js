// Inline rename: the double-click editor and renameKeyword.
// Classic page script; load boot.js after all definitions.

// -- Inline rename (double-click) --
function bindKeywordRename() {
  /** @type {HTMLElement} */ (document.getElementById('kwBody')).addEventListener('dblclick', function(e) {
    if (!(e.target instanceof Element)) return; const nameSpan = e.target.closest(/** @type {'span'} */ ('.name-text'));
    if (!nameSpan) return;

    const kwId = parseInt(nameSpan.dataset.id || '');
    const kw = allKeywords.find(k => k.id === kwId);
    if (!kw) return;

    const td = nameSpan.parentElement; if (!td) return;
    const oldName = kw.name;
    const input = document.createElement('input');
    input.type = 'text';
    input.value = oldName;
    input.className = 'kw-name-input';
    td.innerHTML = '';
    td.appendChild(input);
    input.focus();
    input.select();

    let committed = false;
    function commit() {
      if (committed) return;
      committed = true;
      const newName = input.value.trim();
      if (newName && newName !== oldName) {
        renameKeyword(kwId, newName);
      } else {
        render();
      }
    }
    function cancel() {
      if (committed) return;
      committed = true;
      render();
    }
    input.addEventListener('blur', commit);
    input.addEventListener('keydown', function(ev) {
      if (ev.key === 'Enter') { ev.preventDefault(); input.blur(); }
      if (ev.key === 'Escape') { ev.preventDefault(); cancel(); }
    });
  });
}

/** @param {number} kwId @param {string} newName */ async function renameKeyword(kwId, newName) {
  try {
    const resp = await fetch('/api/keywords/' + kwId, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: newName})
    });
    // Renaming to a spelling that already belongs to a normalized-equal
    // peer merges this row into the peer server-side. The requested id is
    // deleted; refetch so allKeywords/parentMap drop the stale entry and
    // pick up the peer's canonical name.
    /** @type {KeywordMutation} */ const data = await resp.json().catch(() => ({}));
    if (data && data.merged) {
      await loadKeywords();
      return;
    }
    if (!resp.ok) throw new Error(data.error || 'Could not rename keyword');
    await loadKeywords();
  } catch(e) {
    // The row reverts to the old name on render(); say why, or the rename
    // (e.g. a location name containing '|') just silently undoes itself.
    console.error('Failed to rename', e);
    showToast('Could not rename keyword: ' + (keywordErrorMessage(e) || 'request failed'), 'error');
  }
  render();
}
