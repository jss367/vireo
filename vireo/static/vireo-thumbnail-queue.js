/* Browse and Duplicate Files thumbnails share connections with interactive APIs.
 * Keep only two downloads in flight, and discard work outside a small runway.
 * Aborting a fetch frees the client connection; it cannot undo generation that
 * the server has already started. Normal HTTP caching still applies to fetch.
 */
(function() {
  'use strict';
  const rootElement = document.getElementById('gridContainer');
  const gridElement = rootElement ? document.getElementById('grid')
    : document.getElementById('duplicateThumbnailGrid');
  if (!gridElement) return;
  const root = rootElement;
  const grid = gridElement;

  var selector = 'img[data-thumbnail-src]';
  /**
   * @typedef {object} ThumbnailRequestState
   * @property {HTMLImageElement} img
   * @property {string | null} url
   * @property {AbortController | null} controller
   * @property {string | null} objectUrl
   * @property {boolean} loaded
   * @property {boolean} failed
   */
  /** @type {Map<HTMLImageElement, ThumbnailRequestState>} */
  var states = new Map();
  /** @type {Set<ThumbnailRequestState>} */
  var nearby = new Set();
  var active = 0;
  /** @type {number | null} */
  var frame = null;
  var margin = 200;

  function schedule() {
    if (frame === null) frame = requestAnimationFrame(pump);
  }

  /** @param {ThumbnailRequestState} state */
  function cancel(state) {
    if (state.controller) state.controller.abort();
    state.controller = null;
  }

  /** @param {ThumbnailRequestState} state */
  function release(state) {
    if (state.objectUrl) URL.revokeObjectURL(state.objectUrl);
    state.objectUrl = null;
    state.loaded = false;
    state.img.removeAttribute('src');
  }

  /** @param {ThumbnailRequestState} state */
  function leave(state) {
    nearby.delete(state);
    cancel(state);
    state.failed = false; // Retry a failed image when the user returns to it.
    state.img.dispatchEvent(new Event('vireo:thumbnail-cancelled'));
  }

  /** @param {Node} img */
  function sync(img) {
    if (!(img instanceof HTMLImageElement)) return;
    var state = states.get(img);
    if (!grid.contains(img) || !img.matches(selector)) {
      if (state) {
        leave(state);
        release(state);
        observer.unobserve(img);
        states.delete(img);
      }
      return;
    }
    var url = img.getAttribute('data-thumbnail-src');
    if (!state) {
      state = {img: img, url: url, controller: null, objectUrl: null, loaded: false, failed: false};
      states.set(img, state);
      observer.observe(img);
    } else if (state.url !== url) {
      cancel(state);
      release(state);
      state.url = url;
      state.failed = false;
    }
    schedule();
  }

  /** @param {ThumbnailRequestState} state */
  async function download(state) {
    var url = state.url;
    if (!url) return;
    var controller = new AbortController();
    state.controller = controller;
    active++;
    try {
      var response = await fetch(url, {
        signal: controller.signal,
        credentials: 'same-origin'
      });
      if (!response.ok) throw new Error('Thumbnail HTTP ' + response.status);
      var blob = await response.blob();
      // Aborted responses and a previous edit's pixels must never land on a
      // recycled card, even if the response completed just before abort().
      if (state.controller !== controller || !grid.contains(state.img)) return;
      state.objectUrl = URL.createObjectURL(blob);
      state.loaded = true;
      state.img.src = state.objectUrl;
    } catch {
      if (state.controller === controller && !controller.signal.aborted) {
        state.failed = true;
        state.img.dispatchEvent(new Event('error'));
      }
    } finally {
      if (state.controller === controller) state.controller = null;
      // Keep the slot until fetch/body consumption has actually settled.
      active--;
      schedule();
    }
  }

  function pump() {
    frame = null;
    var bounds = root ? root.getBoundingClientRect() : {top: 0, bottom: window.innerHeight};
    /** @type {{state: ThumbnailRequestState, distance: number, visible: boolean}[]} */
    var candidates = [];
    nearby.forEach(function(state) {
      var rect = state.img.getBoundingClientRect();
      var distance = Math.max(bounds.top - rect.bottom, rect.top - bounds.bottom, 0);
      if (!grid.contains(state.img) || !rect.width || !rect.height || distance > margin) {
        leave(state);
        return;
      }
      if (!state.loaded && !state.failed && !state.controller && state.url) {
        candidates.push({
          state: state, distance: distance,
          visible: rect.bottom > bounds.top && rect.top < bounds.bottom
        });
      }
    });
    // Photos actually visible win over prefetches above/below the viewport.
    candidates.sort(function(a, b) {
      return Number(b.visible) - Number(a.visible) || a.distance - b.distance;
    });
    for (var candidate of candidates) {
      if (active >= 2) break;
      download(candidate.state);
    }
  }

  var observer = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (!(entry.target instanceof HTMLImageElement)) return;
      var state = states.get(entry.target);
      if (!state) return;
      if (entry.isIntersecting) nearby.add(state);
      else leave(state);
    });
    schedule();
  }, {root: root, rootMargin: margin + 'px 0px'});

  /** @param {Node} node */
  function visit(node) {
    if (!(node instanceof Element)) return;
    if (node.matches(selector)) sync(node);
    node.querySelectorAll(selector).forEach(sync);
  }

  // Covers appended pages, full grid replacement, stack trays and edits,
  // without rescanning every loaded card on each sidebar or badge update.
  new MutationObserver(function(records) {
    records.forEach(function(record) {
      if (record.type === 'attributes') sync(record.target);
      else {
        record.removedNodes.forEach(visit);
        record.addedNodes.forEach(visit);
      }
    });
    schedule();
  }).observe(grid, {
    childList: true, subtree: true,
    attributes: true, attributeFilter: ['data-thumbnail-src']
  });
  visit(grid);
  (root || window).addEventListener('scroll', schedule, {passive: true});
  window.addEventListener('resize', schedule);
})();
