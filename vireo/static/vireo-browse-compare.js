/* Comparison overlay controller. Selection and photo lookup belong to the
 * browse page; navigation, zoom, requests and event listeners belong here.
 * Each instance owns its state. destroy() releases listeners and pending work.
 */
(function(root) {
  'use strict';
  function create(options) {
    var window = options.window || root;
    var document = window.document;
    var keymap = options.keymap || window.Keymap;
    var browseCompareIds = [];
    var browseCompareOffset = 0;
    var browseCompareSeq = 0;
    var browseCompareEscToken = null;
    var browseCompareViews = {
      A: { zoom: 1, panX: 0, panY: 0 },
      B: { zoom: 1, panX: 0, panY: 0 }
    };
    var browseComparePointer = null;
    var listeners = [];
    var originalProbes = {};

    function listen(target, type, callback, options) {
      target.addEventListener(type, callback, options);
      listeners.push(function() { target.removeEventListener(type, callback, options); });
    }

    function releaseOriginalProbes() {
      Object.keys(originalProbes).forEach(function(prefix) {
        originalProbes[prefix].onload = null;
        originalProbes[prefix].onerror = null;
      });
      originalProbes = {};
    }

    function destroy() {
      closeBrowseCompare();
    }

    function isBrowseCompareOpen() {
      var overlay = document.getElementById('browseCompareOverlay');
      return !!(overlay && overlay.classList.contains('active'));
    }

    function openBrowseCompare(ids) {
      if (ids.length < 2) {
        options.showToast('Select at least two photos to compare.', 'error');
        return;
      }
      browseCompareIds = ids.slice();
      browseCompareOffset = 0;

      var overlay = document.getElementById('browseCompareOverlay');
      if (!overlay) return;
      var alreadyOpen = overlay.classList.contains('active');
      if (alreadyOpen && browseCompareEscToken && keymap) {
        keymap.popEsc(browseCompareEscToken);
        browseCompareEscToken = null;
      }
      if (keymap) {
        browseCompareEscToken = keymap.pushEsc(function() { closeBrowseCompare(); });
        if (!alreadyOpen) keymap.lockBodyScroll();
      }
      overlay.classList.add('active');
      if (!alreadyOpen) installBrowseCompareZoomHandlers();
      return renderBrowseCompare();
    }

    function closeBrowseCompare(e) {
      if (e) {
        e.stopPropagation();
        if (e.target && e.currentTarget && e.target !== e.currentTarget && !e.target.classList.contains('browse-compare-close')) {
          return;
        }
      }
      var overlay = document.getElementById('browseCompareOverlay');
      var wasOpen = overlay && overlay.classList.contains('active');
      if (overlay) overlay.classList.remove('active');
      ++browseCompareSeq;
      browseComparePointer = null;
      releaseOriginalProbes();
      listeners.splice(0).forEach(function(remove) { remove(); });
      if (browseCompareEscToken && keymap) {
        keymap.popEsc(browseCompareEscToken);
        browseCompareEscToken = null;
      }
      if (wasOpen && keymap) keymap.unlockBodyScroll();
    }

    function browseCompareStep(delta) {
      if (!isBrowseCompareOpen()) return;
      var maxOffset = Math.max(0, browseCompareIds.length - 2);
      var next = Math.max(0, Math.min(maxOffset, browseCompareOffset + delta));
      if (next === browseCompareOffset) return;
      browseCompareOffset = next;
      return renderBrowseCompare();
    }

    function browseCompareElements(prefix) {
      return {
        wrap: document.getElementById('browseCompareWrap' + prefix),
        img: document.getElementById('browseCompareImg' + prefix),
        badge: document.getElementById('browseCompareZoom' + prefix)
      };
    }

    function clampBrowseComparePan(prefix) {
      var view = browseCompareViews[prefix];
      var els = browseCompareElements(prefix);
      if (!view || !els.wrap || !els.img) return;
      if (view.zoom <= 1.001) {
        view.panX = 0;
        view.panY = 0;
        return;
      }
      var maxX = Math.max(0, (els.img.clientWidth * view.zoom - els.wrap.clientWidth) / 2);
      var maxY = Math.max(0, (els.img.clientHeight * view.zoom - els.wrap.clientHeight) / 2);
      view.panX = Math.max(-maxX, Math.min(maxX, view.panX));
      view.panY = Math.max(-maxY, Math.min(maxY, view.panY));
    }

    function applyBrowseCompareView(prefix) {
      var view = browseCompareViews[prefix];
      var els = browseCompareElements(prefix);
      if (!view || !els.wrap || !els.img) return;
      clampBrowseComparePan(prefix);
      els.img.style.transform = 'translate(' + view.panX + 'px, ' + view.panY + 'px) scale(' + view.zoom + ')';
      els.wrap.classList.toggle('zoomed', view.zoom > 1.001);
      if (els.badge) els.badge.textContent = view.zoom <= 1.001 ? 'Fit' : Math.round(view.zoom * 100) + '%';
    }

    function ensureBrowseCompareOriginal(prefix) {
      var els = browseCompareElements(prefix);
      if (!els.img || !els.img.dataset.photoId) return;
      var loadState = els.img.dataset.originalLoaded;
      if (loadState === 'true' || loadState === 'loading' || loadState === 'failed') return;
      var photoId = els.img.dataset.photoId;
      var seq = browseCompareSeq;
      els.img.dataset.originalLoaded = 'loading';
      var original = new window.Image();
      originalProbes[prefix] = original;
      original.onload = function() {
        if (seq !== browseCompareSeq || originalProbes[prefix] !== original || els.img.dataset.photoId !== photoId || !isBrowseCompareOpen()) return;
        els.img.dataset.originalLoaded = 'true';
        els.img.src = original.src;
        delete originalProbes[prefix];
      };
      original.onerror = function() {
        if (seq !== browseCompareSeq || originalProbes[prefix] !== original || els.img.dataset.photoId !== photoId || !isBrowseCompareOpen()) return;
        els.img.dataset.originalLoaded = 'failed';
        delete originalProbes[prefix];
      };
      original.src = '/photos/' + photoId + '/original';
    }

    function setBrowseCompareZoom(prefix, zoom, clientX, clientY) {
      var view = browseCompareViews[prefix];
      var els = browseCompareElements(prefix);
      if (!view || !els.wrap) return;
      var oldZoom = view.zoom;
      var nextZoom = Math.max(1, Math.min(8, zoom));
      if (nextZoom > 1.001 && oldZoom > 0 && clientX != null && clientY != null) {
        var rect = els.wrap.getBoundingClientRect();
        var cursorX = clientX - (rect.left + rect.width / 2);
        var cursorY = clientY - (rect.top + rect.height / 2);
        var ratio = nextZoom / oldZoom;
        view.panX = cursorX - ratio * (cursorX - view.panX);
        view.panY = cursorY - ratio * (cursorY - view.panY);
      }
      view.zoom = nextZoom;
      if (nextZoom <= 1.001) {
        view.panX = 0;
        view.panY = 0;
      } else {
        ensureBrowseCompareOriginal(prefix);
      }
      applyBrowseCompareView(prefix);
      if (options.syncZoom) {
        var other = prefix === 'A' ? 'B' : 'A';
        browseCompareViews[other].zoom = nextZoom;
        browseCompareViews[other].panX = view.panX;
        browseCompareViews[other].panY = view.panY;
        if (nextZoom > 1.001) ensureBrowseCompareOriginal(other);
        applyBrowseCompareView(other);
      }
    }

    function resetBrowseCompareView(prefix) {
      var view = browseCompareViews[prefix];
      if (!view) return;
      view.zoom = 1;
      view.panX = 0;
      view.panY = 0;
      applyBrowseCompareView(prefix);
    }

    function resetBrowseCompareViews() {
      resetBrowseCompareView('A');
      resetBrowseCompareView('B');
    }

    function browseComparePaneFromEvent(e) {
      var wrap = e.target && e.target.closest ? e.target.closest('.browse-compare-image-wrap') : null;
      return wrap && wrap.dataset ? wrap.dataset.pane : null;
    }

    function installBrowseCompareZoomHandlers() {
      listen(document, 'wheel', function(e) {
        if (!isBrowseCompareOpen()) return;
        var prefix = browseComparePaneFromEvent(e);
        if (!prefix || !browseCompareViews[prefix]) return;
        e.preventDefault();
        var sensitivity = e.ctrlKey ? 0.02 : 0.0015;
        var factor = Math.exp(-e.deltaY * sensitivity);
        setBrowseCompareZoom(prefix, browseCompareViews[prefix].zoom * factor, e.clientX, e.clientY);
      }, { passive: false });

      listen(document, 'dblclick', function(e) {
        if (!isBrowseCompareOpen()) return;
        var prefix = browseComparePaneFromEvent(e);
        if (!prefix || !browseCompareViews[prefix]) return;
        e.preventDefault();
        setBrowseCompareZoom(prefix, browseCompareViews[prefix].zoom > 1.001 ? 1 : 2, e.clientX, e.clientY);
      });

      listen(document, 'pointerdown', function(e) {
        if (!isBrowseCompareOpen() || e.button !== 0) return;
        var prefix = browseComparePaneFromEvent(e);
        var view = prefix && browseCompareViews[prefix];
        if (!view || view.zoom <= 1.001) return;
        browseComparePointer = {
          pointerId: e.pointerId,
          prefix: prefix,
          startX: e.clientX,
          startY: e.clientY,
          panX: view.panX,
          panY: view.panY
        };
        if (e.target.setPointerCapture) e.target.setPointerCapture(e.pointerId);
        e.preventDefault();
      });

      listen(document, 'pointermove', function(e) {
        var drag = browseComparePointer;
        if (!drag || drag.pointerId !== e.pointerId) return;
        var view = browseCompareViews[drag.prefix];
        view.panX = drag.panX + e.clientX - drag.startX;
        view.panY = drag.panY + e.clientY - drag.startY;
        applyBrowseCompareView(drag.prefix);
        e.preventDefault();
      });

      function stopBrowseComparePan(e) {
        if (browseComparePointer && (e.pointerId == null || browseComparePointer.pointerId === e.pointerId)) {
          browseComparePointer = null;
        }
      }
      listen(document, 'pointerup', stopBrowseComparePan);
      listen(document, 'pointercancel', stopBrowseComparePan);
      listen(window, 'resize', function() {
        if (!isBrowseCompareOpen()) return;
        applyBrowseCompareView('A');
        applyBrowseCompareView('B');
      });
    }

    async function getBrowseComparePhoto(id) {
      var local = options.findPhoto(id);
      if (local) return local;
      try {
        return await options.fetch('/api/photos/' + id, {}, { toast: false });
      } catch(e) {
        return { id: id, filename: 'Photo ' + id };
      }
    }

    function browseCompareMeta(photo) {
      var parts = [];
      if (photo.width && photo.height) parts.push(photo.width + ' × ' + photo.height);
      if (photo.timestamp) parts.push(photo.timestamp.replace('T', ' ').substring(0, 16));
      if (photo.rating != null && photo.rating > 0) parts.push(photo.rating + ' star' + (photo.rating === 1 ? '' : 's'));
      if (photo.sharpness != null) parts.push('sharpness ' + Math.round(photo.sharpness));
      if (photo.flag && photo.flag !== 'none') parts.push(photo.flag);
      return parts.join(' · ');
    }

    function setBrowseComparePane(prefix, photo) {
      // A user can zoom the outgoing bitmap while the next pair is loading.
      // That probe must lose ownership even if this pane reuses the same id.
      if (originalProbes[prefix]) {
        originalProbes[prefix].onload = null;
        originalProbes[prefix].onerror = null;
        delete originalProbes[prefix];
      }
      var img = document.getElementById('browseCompareImg' + prefix);
      var name = document.getElementById('browseCompareName' + prefix);
      var meta = document.getElementById('browseCompareMeta' + prefix);
      if (img) {
        img.alt = photo.filename || '';
        img.dataset.photoId = photo.id;
        img.dataset.originalLoaded = 'false';
        img.src = '/photos/' + photo.id + '/full';
      }
      if (name) name.textContent = photo.filename || ('Photo ' + photo.id);
      if (meta) meta.textContent = browseCompareMeta(photo);
    }

    async function renderBrowseCompare() {
      if (!browseCompareIds.length) return;
      var seq = ++browseCompareSeq;
      browseComparePointer = null;
      releaseOriginalProbes();
      var leftId = browseCompareIds[browseCompareOffset];
      var rightId = browseCompareIds[browseCompareOffset + 1];
      if (leftId == null || rightId == null) return;
      resetBrowseCompareViews();

      var count = document.getElementById('browseCompareCount');
      if (count) {
        count.textContent = (browseCompareOffset + 1) + '-' + (browseCompareOffset + 2) + ' of ' + browseCompareIds.length;
      }
      var prev = document.getElementById('browseComparePrev');
      var next = document.getElementById('browseCompareNext');
      if (prev) prev.disabled = browseCompareOffset <= 0;
      if (next) next.disabled = browseCompareOffset >= browseCompareIds.length - 2;

      var nameA = document.getElementById('browseCompareNameA');
      var nameB = document.getElementById('browseCompareNameB');
      var metaA = document.getElementById('browseCompareMetaA');
      var metaB = document.getElementById('browseCompareMetaB');
      if (nameA) nameA.textContent = 'Loading...';
      if (nameB) nameB.textContent = 'Loading...';
      if (metaA) metaA.textContent = '';
      if (metaB) metaB.textContent = '';

      var pair = await Promise.all([getBrowseComparePhoto(leftId), getBrowseComparePhoto(rightId)]);
      if (seq !== browseCompareSeq || !isBrowseCompareOpen()) return;
      setBrowseComparePane('A', pair[0]);
      setBrowseComparePane('B', pair[1]);
    }

    return Object.freeze({
      open: openBrowseCompare,
      close: closeBrowseCompare,
      isOpen: isBrowseCompareOpen,
      step: browseCompareStep,
      resetViews: resetBrowseCompareViews,
      destroy: destroy
    });
  }
  root.VireoBrowseCompare = Object.freeze({ create: create });
})(window);
