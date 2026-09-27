// Shared, cross-page handoff for the photo editor (/edit). State lives in
// localStorage so it survives the full-page navigation into the editor:
//   - lastPhoto: the most recently viewed photo, so `/edit` (no id) can open
//     "the photo I was just looking at".
//   - nav list: the ordered photo ids of the surface you came from, so the
//     editor can offer Prev/Next.
//   - copied recipe: edit settings copied from one photo, to paste onto a
//     selection elsewhere.
window.vireoEditNav = (function() {
  var LAST_KEY = 'vireo:lastPhoto';
  var NAV_KEY = 'vireo:editNav';
  var COPY_KEY = 'vireo:copiedRecipe';
  var COPY_PENDING_KEY = 'vireo:copiedRecipePending';
  function read(key) {
    try { return JSON.parse(localStorage.getItem(key)); }
    catch (_) { return null; }
  }
  function write(key, value) {
    try {
      if (value == null) localStorage.removeItem(key);
      else localStorage.setItem(key, JSON.stringify(value));
    } catch (_) {}
  }
  function withClipboardLock(callback) {
    if (navigator.locks && typeof navigator.locks.request === 'function') {
      return navigator.locks.request('vireo:editClipboard', callback);
    }
    return Promise.resolve().then(callback);
  }
  return {
    setLastPhoto: function(id) {
      var n = Number(id);
      if (Number.isFinite(n) && n > 0) write(LAST_KEY, n);
    },
    getLastPhoto: function() {
      var n = Number(read(LAST_KEY));
      return Number.isFinite(n) && n > 0 ? n : null;
    },
    setList: function(photoList, currentId) {
      var ids = (photoList || [])
        .map(function(p) { return Number(p && p.id != null ? p.id : p); })
        .filter(function(n) { return Number.isFinite(n) && n > 0; });
      if (ids.length > 1) write(NAV_KEY, {ids: ids, from: Number(currentId) || null});
      else write(NAV_KEY, null);
    },
    getList: function() {
      var nav = read(NAV_KEY);
      return nav && Array.isArray(nav.ids) ? nav.ids : null;
    },
    setCopiedRecipe: function(recipe, meta) {
      return withClipboardLock(function() {
        write(COPY_PENDING_KEY, null);
        if (!recipe || typeof recipe !== 'object') { write(COPY_KEY, null); return; }
        write(COPY_KEY, {recipe: recipe, source: (meta && meta.source) || null, at: (meta && meta.at) || null});
      });
    },
    beginCopiedRecipe: function() {
      return withClipboardLock(function() {
        var token = String(Date.now()) + ':' + Math.random().toString(36).slice(2);
        write(COPY_PENDING_KEY, token);
        return token;
      });
    },
    isCopiedRecipeCurrent: function(token) {
      return !!token && read(COPY_PENDING_KEY) === token;
    },
    setCopiedRecipeIfCurrent: function(recipe, meta, token) {
      return withClipboardLock(function() {
        if (!token || read(COPY_PENDING_KEY) !== token) return false;
        write(COPY_PENDING_KEY, null);
        if (!recipe || typeof recipe !== 'object') {
          write(COPY_KEY, null);
          return true;
        }
        write(COPY_KEY, {recipe: recipe, source: (meta && meta.source) || null, at: (meta && meta.at) || null});
        return true;
      });
    },
    cancelCopiedRecipe: function(token) {
      return withClipboardLock(function() {
        if (!token || read(COPY_PENDING_KEY) !== token) return false;
        write(COPY_PENDING_KEY, null);
        return true;
      });
    },
    getCopiedRecipe: function() { return read(COPY_KEY); },
    // Cursors hold photo ids whose visibility is workspace-scoped (api_photo_detail
    // uses verify_workspace=True); a cursor saved in workspace A becomes "Photo
    // unavailable" / dead Prev-Next ids in workspace B. Drop the cursors on every
    // workspace activation. The copied recipe is just slider/crop values with no
    // photo ids, so it stays.
    clearWorkspaceScopedCursors: function() {
      write(LAST_KEY, null);
      write(NAV_KEY, null);
    },
  };
})();
