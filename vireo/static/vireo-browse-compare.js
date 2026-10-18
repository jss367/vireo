/* Comparison overlay controller. Selection and photo lookup belong to the
 * browse page; navigation, zoom, requests and event listeners belong here.
 * Each instance owns its state. destroy() releases listeners and pending work.
 */
(function(root) {
  'use strict';
  /** @param {Document} document @param {ComparePane} prefix @param {ComparePhoto} photo */
  function renderComparePane(document, prefix, photo) {
  var img = /** @type {HTMLImageElement | null} */ (document.getElementById('browseCompareImg' + prefix));
  var name = document.getElementById('browseCompareName' + prefix);
  var meta = document.getElementById('browseCompareMeta' + prefix);
  if (img) {
    img.alt = photo.filename || '';
    img.dataset.photoId = String(photo.id);
    img.dataset.originalLoaded = 'false';
    img.src = '/photos/' + photo.id + '/full';
  }
  if (name) name.textContent = photo.filename || ('Photo ' + photo.id);
  if (meta) meta.textContent = browseCompareMeta(photo);
  }

  /** @param {ComparePhoto} photo */
  function browseCompareMeta(photo) {
    var parts = [];
    if (photo.width && photo.height) parts.push(photo.width + ' × ' + photo.height);
    if (photo.timestamp) parts.push(photo.timestamp.replace('T', ' ').substring(0, 16));
    if (photo.rating != null && photo.rating > 0) parts.push(photo.rating + ' star' + (photo.rating === 1 ? '' : 's'));
    if (photo.sharpness != null) parts.push('sharpness ' + Math.round(photo.sharpness));
    if (photo.flag && photo.flag !== 'none') parts.push(photo.flag);
    return parts.join(' · ');
  }

  /** @param {Document} document @param {ComparePane} prefix */
  function browseCompareElements(document, prefix) {
    return {
      wrap: document.getElementById('browseCompareWrap' + prefix),
      img: /** @type {HTMLImageElement | null} */ (document.getElementById('browseCompareImg' + prefix)),
      badge: document.getElementById('browseCompareZoom' + prefix)
    };
  }

  /** @param {CompareOptions} options */
  function create(options) {
    var window = options.window || root;
    var document = window.document;
    var keymap = options.keymap || window.Keymap;
    /** @type {number[]} */
    var browseCompareIds = [];
    var browseCompareOffset = 0;
    var browseCompareSeq = 0;
    /** @type {number | null} */
    var browseCompareEscToken = null;
    var browseCompareViews = {
      A: { zoom: 1, panX: 0, panY: 0 },
      B: { zoom: 1, panX: 0, panY: 0 }
    };
    /** @type {ComparePointer | null} */
    var browseComparePointer = null;
    /** @type {(() => void)[]} */
    var listeners = [];
    /** @type {Partial<Record<ComparePane, HTMLImageElement>>} */
    var originalProbes = {};

    /** @template {keyof GlobalEventHandlersEventMap} T @param {Pick<EventTarget, 'addEventListener' | 'removeEventListener'>} target @param {T} type @param {(event: GlobalEventHandlersEventMap[T]) => void} callback @param {AddEventListenerOptions} [options] */
    function listen(target, type, callback, options) {
      /** @type {EventListener} */
      var listener = function(event) { callback(/** @type {GlobalEventHandlersEventMap[T]} */ (event)); };
      target.addEventListener(type, listener, options);
      listeners.push(function() { target.removeEventListener(type, listener, options); });
    }

    function releaseOriginalProbes() {
      Object.values(originalProbes).forEach(function(probe) {
        probe.onload = null;
        probe.onerror = null;
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

    /** @param {number[]} ids */
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

    /** @param {Event} [e] */
    function closeBrowseCompare(e) {
      if (e) {
        e.stopPropagation();
        if (e.target instanceof Element && e.currentTarget && e.target !== e.currentTarget && !e.target.classList.contains('browse-compare-close')) {
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

    /** @param {number} delta */
    function browseCompareStep(delta) {
      if (!isBrowseCompareOpen()) return;
      var maxOffset = Math.max(0, browseCompareIds.length - 2);
      var next = Math.max(0, Math.min(maxOffset, browseCompareOffset + delta));
      if (next === browseCompareOffset) return;
      browseCompareOffset = next;
      return renderBrowseCompare();
    }

    /** @param {ComparePane} prefix */
    function clampBrowseComparePan(prefix) {
      var view = browseCompareViews[prefix];
      var els = browseCompareElements(document, prefix);
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

    /** @param {ComparePane} prefix */
    function applyBrowseCompareView(prefix) {
      var view = browseCompareViews[prefix];
      var els = browseCompareElements(document, prefix);
      if (!view || !els.wrap || !els.img) return;
      clampBrowseComparePan(prefix);
      els.img.style.transform = 'translate(' + view.panX + 'px, ' + view.panY + 'px) scale(' + view.zoom + ')';
      els.wrap.classList.toggle('zoomed', view.zoom > 1.001);
      if (els.badge) els.badge.textContent = view.zoom <= 1.001 ? 'Fit' : Math.round(view.zoom * 100) + '%';
    }

    /** @param {ComparePane} prefix */
    function ensureBrowseCompareOriginal(prefix) {
      var els = browseCompareElements(document, prefix);
      const image = els.img;
      if (!image || !image.dataset.photoId) return;
      var loadState = image.dataset.originalLoaded;
      if (loadState === 'true' || loadState === 'loading' || loadState === 'failed') return;
      var photoId = image.dataset.photoId;
      var seq = browseCompareSeq;
      image.dataset.originalLoaded = 'loading';
      var original = new window.Image();
      originalProbes[prefix] = original;
      original.onload = function() {
        if (seq !== browseCompareSeq || originalProbes[prefix] !== original || image.dataset.photoId !== photoId || !isBrowseCompareOpen()) return;
        image.dataset.originalLoaded = 'true';
        image.src = original.src;
        delete originalProbes[prefix];
      };
      original.onerror = function() {
        if (seq !== browseCompareSeq || originalProbes[prefix] !== original || image.dataset.photoId !== photoId || !isBrowseCompareOpen()) return;
        image.dataset.originalLoaded = 'failed';
        delete originalProbes[prefix];
      };
      original.src = '/photos/' + photoId + '/original';
    }

    /** @param {ComparePane} prefix @param {number} zoom @param {number} [clientX] @param {number} [clientY] */
    function setBrowseCompareZoom(prefix, zoom, clientX, clientY) {
      var view = browseCompareViews[prefix];
      var els = browseCompareElements(document, prefix);
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
        const other = prefix === 'A' ? 'B' : 'A';
        browseCompareViews[other].zoom = nextZoom;
        browseCompareViews[other].panX = view.panX;
        browseCompareViews[other].panY = view.panY;
        if (nextZoom > 1.001) ensureBrowseCompareOriginal(other);
        applyBrowseCompareView(other);
      }
    }

    /** @param {ComparePane} prefix */
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

    /** @param {Event} e @returns {ComparePane | null} */
    function browseComparePaneFromEvent(e) {
      var wrap = e.target instanceof Element ? e.target.closest('.browse-compare-image-wrap') : null;
      var pane = wrap && wrap.getAttribute('data-pane');
      return pane === 'A' || pane === 'B' ? pane : null;
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
        if (!prefix) return;
        var view = browseCompareViews[prefix];
        if (!view || view.zoom <= 1.001) return;
        browseComparePointer = {
          pointerId: e.pointerId,
          prefix: prefix,
          startX: e.clientX,
          startY: e.clientY,
          panX: view.panX,
          panY: view.panY
        };
        if (e.target instanceof Element && e.target.setPointerCapture) e.target.setPointerCapture(e.pointerId);
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

      /** @param {PointerEvent} e */
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

    /** @param {number} id @returns {Promise<ComparePhoto>} */
    async function getBrowseComparePhoto(id) {
      var local = options.findPhoto(id);
      if (local) return local;
      try {
        return await options.fetch('/api/photos/' + id, {}, { toast: false });
      } catch(e) {
        return { id: id, filename: 'Photo ' + id };
      }
    }

    /** @param {ComparePane} prefix @param {ComparePhoto} photo */
    function setBrowseComparePane(prefix, photo) {
      // A user can zoom the outgoing bitmap while the next pair is loading.
      // That probe must lose ownership even if this pane reuses the same id.
      var probe = originalProbes[prefix];
      if (probe) {
        probe.onload = null;
        probe.onerror = null;
        delete originalProbes[prefix];
      }
      renderComparePane(document, prefix, photo);
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
      var prev = /** @type {HTMLButtonElement | null} */ (document.getElementById('browseComparePrev'));
      var next = /** @type {HTMLButtonElement | null} */ (document.getElementById('browseCompareNext'));
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

  /**
   * Bind _photo_compare.html's controls (its data-compare-control markup) to
   * the page's comparison. ``getCompare`` returns the page's current
   * comparison, or null before it has one. Clicks inside the toolbar and the
   * stage ("stop") do not close the overlay or reach the document.
   * @param {Document} document
   * @param {() => CompareControls | null | undefined} getCompare
   */
  function bindControls(document, getCompare) {
    var overlay = document.getElementById('browseCompareOverlay');
    if (!overlay) return;
    overlay.addEventListener('click', function(event) {
      var target = event.target instanceof Element ? event.target : null;
      var control = target && target.closest('[data-compare-control]');
      var action = control && control.getAttribute('data-compare-control');
      var compare = getCompare();
      if (compare && action === 'close') compare.close(event);
      else if (compare && action === 'reset') compare.resetViews();
      else if (compare && action === 'prev') compare.step(-1);
      else if (compare && action === 'next') compare.step(1);
      if (target && target.closest('[data-compare-control="stop"]')) event.stopPropagation();
    });
  }

  root.VireoBrowseCompare = Object.freeze({ create: create, bindControls: bindControls });
})(window);
