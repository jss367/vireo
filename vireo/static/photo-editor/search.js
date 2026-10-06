// The editor's photo search box and its results.
// Classic page script; load boot.js after all definitions.

function setSearchStatus(text, isError) {
  var el = document.getElementById('editorSearchStatus');
  if (!el) return;
  el.textContent = text || '';
  el.classList.toggle('error', !!isError);
}

function scheduleEditorSearch() {
  if (editorState.searchTimer) clearTimeout(editorState.searchTimer);
  editorState.searchTimer = null;
  var input = document.getElementById('editorSearchInput');
  var q = input ? input.value.trim() : '';
  if (!q) {
    clearEditorSearchResults();
    return;
  }
  if (q === editorState.searchQuery &&
      editorState.searchQuerySeq === editorState.searchSeq) {
    // Trimmed query unchanged since our last dispatch AND that dispatch is
    // still current (e.g. typing a trailing space after "American Robin"),
    // so its in-flight response will still pass the seq guard. Bumping the
    // seq here would invalidate it while the debounced replacement would
    // early-return on the same-query check, leaving the UI stuck at
    // "Searching...".
    return;
  }
  // Either the trimmed query actually changed, or the previously-dispatched
  // fetch has been invalidated by an intermediate keystroke (user typed a
  // different query then reverted before the debounce fired). In both cases
  // bump the seq so any in-flight response fails the seq guard, then debounce
  // a replacement fetch.
  editorState.searchSeq++;
  editorState.searchTimer = setTimeout(function() {
    editorState.searchTimer = null;
    runEditorSearch();
  }, 300);
}

function clearEditorSearchResults() {
  editorState.searchSeq++;
  editorState.searchQuery = '';
  editorState.searchQuerySeq = 0;
  editorState.navIds = (editorState.baseNavIds || []).slice();
  if (window.vireoEditNav) window.vireoEditNav.setList(editorState.navIds, editorState.photoId);
  setSearchStatus('');
  updateNavControls();
}

async function runEditorSearch(opts) {
  opts = opts || {};
  if (editorState.searchTimer) {
    clearTimeout(editorState.searchTimer);
    editorState.searchTimer = null;
  }
  var input = document.getElementById('editorSearchInput');
  var q = input ? input.value.trim() : '';
  if (!q) {
    clearEditorSearchResults();
    return;
  }
  if (q === editorState.searchQuery && !opts.immediate &&
      editorState.searchQuerySeq === editorState.searchSeq) {
    // Same trimmed query as our last dispatch and that dispatch is still
    // current (nothing invalidated it) — no need to refire. If the seqs no
    // longer match, `scheduleEditorSearch` bumped the seq because an
    // intermediate keystroke invalidated the in-flight response, so we must
    // fall through to dispatch a replacement.
    return;
  }
  if (isEditorDirty()) {
    setSearchStatus('Save or discard this photo before searching.', true);
    return;
  }

  var seq = ++editorState.searchSeq;
  editorState.searchQuery = q;
  editorState.searchQuerySeq = seq;
  setSearchStatus('Searching...');
  try {
    var params = new URLSearchParams();
    params.set('keyword', q);
    params.set('sort', 'name');
    var data = await safeFetch('/api/photos/ids?' + params.toString(), {}, {toast: false});
    if (seq !== editorState.searchSeq) return;
    var ids = (data.photo_ids || [])
      .map(function(id) { return Number(id); })
      .filter(function(id) { return Number.isFinite(id) && id > 0; });
    if (!ids.length) {
      editorState.navIds = [];
      if (window.vireoEditNav) window.vireoEditNav.setList([], null);
      setSearchStatus('No matches for "' + q + '".', true);
      updateNavControls();
      return;
    }

    var current = Number(editorState.photoId);
    var target = ids.indexOf(current) === -1 ? ids[0] : current;
    // Dirty state can change during the fetch above. Run the confirm before
    // any nav/status mutation so a dismissed prompt leaves the editor
    // untouched — otherwise the navIds/nav position/status would already
    // describe the search target while the still-loaded dirty photo isn't in
    // ids, leaving the editor visibly inconsistent.
    if (target !== current && isEditorDirty() &&
        !window.confirm('Discard unsaved edits to this photo?')) {
      return;
    }
    editorState.navIds = ids;
    if (window.vireoEditNav) window.vireoEditNav.setList(ids, target);
    setSearchStatus(ids.length.toLocaleString() + ' match' + (ids.length === 1 ? '' : 'es'));
    updateNavControls();
    if (target !== current) {
      await loadPhoto(target);
    }
  } catch (e) {
    if (seq !== editorState.searchSeq) return;
    setSearchStatus(e.message || 'Search failed', true);
  }
}
