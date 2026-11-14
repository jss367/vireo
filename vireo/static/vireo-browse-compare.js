/* Comparison overlay controller. Selection and photo lookup belong to the
 * browse page; navigation, zoom, requests and event listeners belong here.
 * Each instance owns its state: create() builds one record, never hands it
 * out, and passes it to the functions below. destroy() releases listeners and
 * pending work.
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
    if (photo.timestamp) parts.push(window.VireoTime.captureDateTime(photo.timestamp));
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


  // Add a listener the comparison removes when it closes.
  /** @template {keyof GlobalEventHandlersEventMap} T @param {CompareContext} compare @param {Pick<EventTarget, 'addEventListener' | 'removeEventListener'>} target @param {T} type @param {(event: GlobalEventHandlersEventMap[T]) => void} callback @param {AddEventListenerOptions} [listenerOptions] */
  function listen(compare, target, type, callback, listenerOptions) {
    /** @type {EventListener} */
    var listener = function(event) { callback(/** @type {GlobalEventHandlersEventMap[T]} */ (event)); };
    target.addEventListener(type, listener, listenerOptions);
    compare.listeners.push(function() { target.removeEventListener(type, listener, listenerOptions); });
  }

  function releaseOriginalProbes(/** @type {CompareContext} */ compare) {
    Object.values(compare.originalProbes).forEach(function(probe) {
      probe.onload = null;
      probe.onerror = null;
    });
    compare.originalProbes = {};
  }

  function isBrowseCompareOpen(/** @type {CompareContext} */ compare) {
    var overlay = compare.document.getElementById('browseCompareOverlay');
    return !!(overlay && overlay.classList.contains('active'));
  }

  /** @param {number[]} ids */
  function openBrowseCompare(/** @type {CompareContext} */ compare, ids) {
    if (ids.length < 2) {
      compare.options.showToast('Select at least two photos to compare.', 'error');
      return;
    }
    compare.ids = ids.slice();
    compare.offset = 0;

    var overlay = compare.document.getElementById('browseCompareOverlay');
    if (!overlay) return;
    var alreadyOpen = overlay.classList.contains('active');
    if (alreadyOpen && compare.escToken && compare.keymap) {
      compare.keymap.popEsc(compare.escToken);
      compare.escToken = null;
    }
    if (compare.keymap) {
      compare.escToken = compare.keymap.pushEsc(function() { closeBrowseCompare(compare); });
      if (!alreadyOpen) compare.keymap.lockBodyScroll();
    }
    overlay.classList.add('active');
    if (!alreadyOpen) installBrowseCompareZoomHandlers(compare);
    return renderBrowseCompare(compare);
  }

  /** @param {Event} [e] */
  function closeBrowseCompare(/** @type {CompareContext} */ compare, e) {
    if (e) {
      e.stopPropagation();
      if (e.target instanceof Element && e.currentTarget && e.target !== e.currentTarget && !e.target.classList.contains('browse-compare-close')) {
        return;
      }
    }
    var overlay = compare.document.getElementById('browseCompareOverlay');
    var wasOpen = overlay && overlay.classList.contains('active');
    if (overlay) overlay.classList.remove('active');
    ++compare.seq;
    compare.pointer = null;
    releaseOriginalProbes(compare);
    compare.listeners.splice(0).forEach(function(remove) { remove(); });
    if (compare.escToken && compare.keymap) {
      compare.keymap.popEsc(compare.escToken);
      compare.escToken = null;
    }
    if (wasOpen && compare.keymap) compare.keymap.unlockBodyScroll();
  }

  /** @param {number} delta */
  function browseCompareStep(/** @type {CompareContext} */ compare, delta) {
    if (!isBrowseCompareOpen(compare)) return;
    var maxOffset = Math.max(0, compare.ids.length - 2);
    var next = Math.max(0, Math.min(maxOffset, compare.offset + delta));
    if (next === compare.offset) return;
    compare.offset = next;
    return renderBrowseCompare(compare);
  }

  /** @param {ComparePane} prefix */
  function clampBrowseComparePan(/** @type {CompareContext} */ compare, prefix) {
    var view = compare.views[prefix];
    var els = browseCompareElements(compare.document, prefix);
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
  function applyBrowseCompareView(/** @type {CompareContext} */ compare, prefix) {
    var view = compare.views[prefix];
    var els = browseCompareElements(compare.document, prefix);
    if (!view || !els.wrap || !els.img) return;
    clampBrowseComparePan(compare, prefix);
    els.img.style.transform = 'translate(' + view.panX + 'px, ' + view.panY + 'px) scale(' + view.zoom + ')';
    els.wrap.classList.toggle('zoomed', view.zoom > 1.001);
    if (els.badge) els.badge.textContent = view.zoom <= 1.001 ? 'Fit' : Math.round(view.zoom * 100) + '%';
  }

  /** @param {ComparePane} prefix */
  function ensureBrowseCompareOriginal(/** @type {CompareContext} */ compare, prefix) {
    var els = browseCompareElements(compare.document, prefix);
    const image = els.img;
    if (!image || !image.dataset.photoId) return;
    var loadState = image.dataset.originalLoaded;
    if (loadState === 'true' || loadState === 'loading' || loadState === 'failed') return;
    var photoId = image.dataset.photoId;
    var seq = compare.seq;
    image.dataset.originalLoaded = 'loading';
    var original = new compare.window.Image();
    compare.originalProbes[prefix] = original;
    original.onload = function() {
      if (seq !== compare.seq || compare.originalProbes[prefix] !== original || image.dataset.photoId !== photoId || !isBrowseCompareOpen(compare)) return;
      image.dataset.originalLoaded = 'true';
      image.src = original.src;
      delete compare.originalProbes[prefix];
    };
    original.onerror = function() {
      if (seq !== compare.seq || compare.originalProbes[prefix] !== original || image.dataset.photoId !== photoId || !isBrowseCompareOpen(compare)) return;
      image.dataset.originalLoaded = 'failed';
      delete compare.originalProbes[prefix];
    };
    original.src = '/photos/' + photoId + '/original';
  }

  /** @param {ComparePane} prefix @param {number} zoom @param {number} [clientX] @param {number} [clientY] */
  function setBrowseCompareZoom(/** @type {CompareContext} */ compare, prefix, zoom, clientX, clientY) {
    var view = compare.views[prefix];
    var els = browseCompareElements(compare.document, prefix);
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
      ensureBrowseCompareOriginal(compare, prefix);
    }
    applyBrowseCompareView(compare, prefix);
    if (compare.options.syncZoom) {
      const other = prefix === 'A' ? 'B' : 'A';
      compare.views[other].zoom = nextZoom;
      compare.views[other].panX = view.panX;
      compare.views[other].panY = view.panY;
      if (nextZoom > 1.001) ensureBrowseCompareOriginal(compare, other);
      applyBrowseCompareView(compare, other);
    }
  }

  /** @param {ComparePane} prefix */
  function resetBrowseCompareView(/** @type {CompareContext} */ compare, prefix) {
    var view = compare.views[prefix];
    if (!view) return;
    view.zoom = 1;
    view.panX = 0;
    view.panY = 0;
    applyBrowseCompareView(compare, prefix);
  }

  function resetBrowseCompareViews(/** @type {CompareContext} */ compare) {
    resetBrowseCompareView(compare, 'A');
    resetBrowseCompareView(compare, 'B');
  }

  /** @param {Event} e @returns {ComparePane | null} */
  function browseComparePaneFromEvent(e) {
    var wrap = e.target instanceof Element ? e.target.closest('.browse-compare-image-wrap') : null;
    var pane = wrap && wrap.getAttribute('data-pane');
    return pane === 'A' || pane === 'B' ? pane : null;
  }

  function installBrowseCompareZoomHandlers(/** @type {CompareContext} */ compare) {
    listen(compare, compare.document, 'wheel', function(e) {
      if (!isBrowseCompareOpen(compare)) return;
      var prefix = browseComparePaneFromEvent(e);
      if (!prefix || !compare.views[prefix]) return;
      e.preventDefault();
      var sensitivity = e.ctrlKey ? 0.02 : 0.0015;
      var factor = Math.exp(-e.deltaY * sensitivity);
      setBrowseCompareZoom(compare, prefix, compare.views[prefix].zoom * factor, e.clientX, e.clientY);
    }, { passive: false });

    listen(compare, compare.document, 'dblclick', function(e) {
      if (!isBrowseCompareOpen(compare)) return;
      var prefix = browseComparePaneFromEvent(e);
      if (!prefix || !compare.views[prefix]) return;
      e.preventDefault();
      setBrowseCompareZoom(compare, prefix, compare.views[prefix].zoom > 1.001 ? 1 : 2, e.clientX, e.clientY);
    });

    listen(compare, compare.document, 'pointerdown', function(e) {
      if (!isBrowseCompareOpen(compare) || e.button !== 0) return;
      var prefix = browseComparePaneFromEvent(e);
      if (!prefix) return;
      var view = compare.views[prefix];
      if (!view || view.zoom <= 1.001) return;
      compare.pointer = {
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

    listen(compare, compare.document, 'pointermove', function(e) {
      var drag = compare.pointer;
      if (!drag || drag.pointerId !== e.pointerId) return;
      var view = compare.views[drag.prefix];
      view.panX = drag.panX + e.clientX - drag.startX;
      view.panY = drag.panY + e.clientY - drag.startY;
      applyBrowseCompareView(compare, drag.prefix);
      e.preventDefault();
    });

    /** @param {PointerEvent} e */
    function stopBrowseComparePan(e) {
      if (compare.pointer && (e.pointerId == null || compare.pointer.pointerId === e.pointerId)) {
        compare.pointer = null;
      }
    }
    listen(compare, compare.document, 'pointerup', stopBrowseComparePan);
    listen(compare, compare.document, 'pointercancel', stopBrowseComparePan);
    listen(compare, compare.window, 'resize', function() {
      if (!isBrowseCompareOpen(compare)) return;
      applyBrowseCompareView(compare, 'A');
      applyBrowseCompareView(compare, 'B');
    });
  }

  /** @param {number} id @returns {Promise<ComparePhoto>} */
  async function getBrowseComparePhoto(/** @type {CompareContext} */ compare, id) {
    var local = compare.options.findPhoto(id);
    if (local) return local;
    try {
      return await compare.options.fetch('/api/photos/' + id, {}, { toast: false });
    } catch(e) {
      return { id: id, filename: 'Photo ' + id };
    }
  }

  /** @param {ComparePane} prefix @param {ComparePhoto} photo */
  function setBrowseComparePane(/** @type {CompareContext} */ compare, prefix, photo) {
    // A user can zoom the outgoing bitmap while the next pair is loading.
    // That probe must lose ownership even if this pane reuses the same id.
    var probe = compare.originalProbes[prefix];
    if (probe) {
      probe.onload = null;
      probe.onerror = null;
      delete compare.originalProbes[prefix];
    }
    renderComparePane(compare.document, prefix, photo);
  }

  async function renderBrowseCompare(/** @type {CompareContext} */ compare) {
    if (!compare.ids.length) return;
    var seq = ++compare.seq;
    compare.pointer = null;
    releaseOriginalProbes(compare);
    var leftId = compare.ids[compare.offset];
    var rightId = compare.ids[compare.offset + 1];
    if (leftId == null || rightId == null) return;
    resetBrowseCompareViews(compare);

    var count = compare.document.getElementById('browseCompareCount');
    if (count) {
      count.textContent = (compare.offset + 1) + '-' + (compare.offset + 2) + ' of ' + compare.ids.length;
    }
    var prev = /** @type {HTMLButtonElement | null} */ (compare.document.getElementById('browseComparePrev'));
    var next = /** @type {HTMLButtonElement | null} */ (compare.document.getElementById('browseCompareNext'));
    if (prev) prev.disabled = compare.offset <= 0;
    if (next) next.disabled = compare.offset >= compare.ids.length - 2;

    var nameA = compare.document.getElementById('browseCompareNameA');
    var nameB = compare.document.getElementById('browseCompareNameB');
    var metaA = compare.document.getElementById('browseCompareMetaA');
    var metaB = compare.document.getElementById('browseCompareMetaB');
    if (nameA) nameA.textContent = 'Loading...';
    if (nameB) nameB.textContent = 'Loading...';
    if (metaA) metaA.textContent = '';
    if (metaB) metaB.textContent = '';

    var pair = await Promise.all([getBrowseComparePhoto(compare, leftId), getBrowseComparePhoto(compare, rightId)]);
    if (seq !== compare.seq || !isBrowseCompareOpen(compare)) return;
    setBrowseComparePane(compare, 'A', pair[0]);
    setBrowseComparePane(compare, 'B', pair[1]);
  }

  /** @param {CompareOptions} options @returns {CompareController} */
  function create(options) {
    var window = options.window || root;
    /** @type {CompareContext} */
    var compare = {
      window: window,
      document: window.document,
      keymap: options.keymap || window.Keymap,
      options: options,
      ids: [],
      offset: 0,
      seq: 0,
      escToken: null,
      views: {
        A: { zoom: 1, panX: 0, panY: 0 },
        B: { zoom: 1, panX: 0, panY: 0 }
      },
      pointer: null,
      listeners: [],
      originalProbes: {}
    };
    return Object.freeze({
      open: openBrowseCompare.bind(null, compare),
      close: closeBrowseCompare.bind(null, compare),
      isOpen: isBrowseCompareOpen.bind(null, compare),
      step: browseCompareStep.bind(null, compare),
      resetViews: resetBrowseCompareViews.bind(null, compare),
      destroy: function() { closeBrowseCompare(compare); }
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
