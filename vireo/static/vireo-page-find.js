/* Page-text search for the desktop webview, which has no browser find bar.
 * Native Edit > Find and keyboard events share this entry point. Pages with
 * an existing search control retain their search behavior.
 */
(function() {
  'use strict';

  var panel = /** @type {HTMLElement} */ (document.getElementById('pageFindPanel'));
  var panelHome = /** @type {ParentNode} */ (panel.parentNode);
  /** @type {HTMLDialogElement | null} */
  var ownerDialog = null;
  var input = /** @type {HTMLInputElement} */ (document.getElementById('pageFindInput'));
  var status = /** @type {HTMLElement} */ (document.getElementById('pageFindStatus'));
  /** @type {Element[][]} */
  var marks = []; // One group of highlight fragments per logical match.
  var activeIndex = -1;
  /** @type {Element | null} */
  var previousFocus = null;
  /** @type {number | null} */
  var escToken = null;
  /** @type {number | undefined} */
  var refreshTimer = undefined;
  var observer = new MutationObserver(function(records) {
    if (records.every(function(record) { return panel.contains(record.target); })) return;
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(function() { refresh(false); }, 100);
  });

  function observe() {
    if (!panel.hidden && input.value.trim()) {
      observer.observe(document.body, {
        childList: true, subtree: true, characterData: true,
        attributes: true, attributeFilter: ['class', 'style', 'hidden', 'aria-hidden', 'open'],
      });
    }
  }

  function clearMarks() {
    observer.disconnect();
    /** @type {Set<Node>} */
    var parents = new Set();
    marks.forEach(function(group) {
      group.forEach(function(mark) {
        var parent = mark.parentNode;
        if (!parent) return;
        parent.replaceChild(document.createTextNode(mark.textContent || ''), mark);
        parents.add(parent);
      });
    });
    parents.forEach(function(parent) { parent.normalize(); });
    marks = [];
  }

  /** @param {Element} el @param {WeakMap<Element, CSSStyleDeclaration>} cache @returns {CSSStyleDeclaration} */
  function computedStyle(el, cache) {
    var style = cache.get(el);
    if (!style) { style = window.getComputedStyle(el); cache.set(el, style); }
    return style;
  }

  /** @param {Node} node @param {WeakMap<Element, CSSStyleDeclaration>} styles */
  function visibleForFind(node, styles) {
    var el = node instanceof Element ? node : node.parentElement;
    if (!el) return false;
    if (el.closest('#pageFindPanel, script, style, noscript, textarea, input, select, [contenteditable], svg title, svg desc')) return false;
    for (var current = /** @type {Element | null} */ (el); current; current = current.parentElement) {
      if ((current instanceof HTMLElement && current.hidden) || current.getAttribute('aria-hidden') === 'true') return false;
      if (current instanceof HTMLDetailsElement && !current.open &&
          (current !== el || node.nodeType === 3)) {
        var summary = /** @type {HTMLElement | null} */ (current.querySelector(':scope > summary'));
        if (!summary || !summary.contains(el)) return false;
      }
      var style = computedStyle(current, styles);
      if (style.display === 'none' || style.visibility === 'hidden' || style.visibility === 'collapse' || style.opacity === '0') return false;
      // Collapsed panels can retain child layout boxes while clipping all
      // their content. Scrollable boxes still participate in page search.
      if (style.display !== 'inline' && style.display !== 'contents' && (
        (/^(hidden|clip)$/.test(style.overflowY) && current.clientHeight === 0) ||
        (/^(hidden|clip)$/.test(style.overflowX) && current.clientWidth === 0)
      )) return false;
    }
    return el.getClientRects().length > 0;
  }

  /** @returns {FindTextRun[]} */
  function collectTextRuns() {
    var styles = new WeakMap();
    /** @type {FindTextRun[]} */
    var runs = [];
    /** @type {FindTextRun | null} */
    var run = null;
    /** @param {Node} node */
    function collect(node) {
      if (node instanceof Text) {
        if (!node.nodeValue) return;
        if (!visibleForFind(node, styles)) { run = null; return; }
        if (!run) {
          run = {text: '', parts: []};
          runs.push(run);
        }
        run.parts.push({node: node, start: run.text.length, end: run.text.length + node.nodeValue.length});
        run.text += node.nodeValue;
        return;
      }
      if (!(node instanceof Element)) return;
      var display = computedStyle(node, styles).display;
      // Inline formatting stays together. Blocks, line breaks, replaced
      // controls/images, and independent SVG labels delimit text runs.
      var boundary = (display !== 'inline' && display !== 'contents') ||
        /^(BR|IMG|HR|INPUT|SELECT|TEXTAREA|IFRAME|VIDEO|AUDIO|CANVAS|TEXT|TEXTPATH)$/.test(node.tagName.toUpperCase());
      if (boundary) run = null;
      node.childNodes.forEach(collect);
      if (boundary) run = null;
    }
    // A modal's backdrop obscures the rest of the page.
    collect(ownerDialog || document.body);
    return runs;
  }

  function updateStatus() {
    status.textContent = marks.length ? (activeIndex + 1) + ' of ' + marks.length : '0 results';
    /** @type {HTMLButtonElement} */ (document.getElementById('pageFindPrevious')).disabled = !marks.length;
    /** @type {HTMLButtonElement} */ (document.getElementById('pageFindNext')).disabled = !marks.length;
  }

  /** @param {number} index @param {boolean} scroll */
  function activate(index, scroll) {
    observer.disconnect();
    marks.forEach(function(group) {
      group.forEach(function(mark) { mark.classList.remove('active'); });
    });
    activeIndex = marks.length ? ((index % marks.length) + marks.length) % marks.length : -1;
    if (activeIndex >= 0) {
      var active = marks[activeIndex];
      if (!active) return;
      active.forEach(function(mark) { mark.classList.add('active'); });
      if (scroll && active[0]) active[0].scrollIntoView({block: 'center', inline: 'nearest'});
    }
    updateStatus();
    observe();
  }

  /** @param {boolean} scroll */
  function refresh(scroll) {
    clearTimeout(refreshTimer);
    clearMarks();
    var query = input.value.trim();
    if (!query || panel.hidden) {
      activeIndex = -1;
      updateStatus();
      return;
    }
    // Match against the original string: case conversion can expand Unicode
    // characters, so offsets into a lowercased copy need not align with text.
    var pattern = query.split(/\s+/).map(function(word) {
      return word.replace(/[.*+?^$(){}|[\]\\]/g, '\\$&');
    }).join('\\s+');
    var matcher = new RegExp(pattern, 'giu');
    /** @type {Map<Text, FindTextSegment[]>} */
    var segmentsByNode = new Map();
    collectTextRuns().forEach(function(run) {
      matcher.lastIndex = 0;
      var partIndex = 0;
      var match;
      while ((match = matcher.exec(run.text)) !== null) {
        /** @type {Element[]} */
        var group = [];
        marks.push(group);
        var start = match.index;
        var end = start + match[0].length;
        while ((run.parts[partIndex]?.end ?? Infinity) <= start) partIndex++;
        for (var i = partIndex; i < run.parts.length; i++) {
          var part = run.parts[i];
          if (!part || part.start >= end) break;
          if (part.end <= start) continue;
          var segments = segmentsByNode.get(part.node);
          if (!segments) { segments = []; segmentsByNode.set(part.node, segments); }
          segments.push({
            start: Math.max(start, part.start) - part.start,
            end: Math.min(end, part.end) - part.start,
            group: group
          });
        }
      }
    });
    segmentsByNode.forEach(function(segments, node) {
      var text = node.data;
      var parent = node.parentNode;
      if (!parent) return;
      var fragment = document.createDocumentFragment();
      var pos = 0;
      segments.forEach(function(segment) {
        fragment.appendChild(document.createTextNode(text.slice(pos, segment.start)));
        var mark = node.parentElement?.namespaceURI === 'http://www.w3.org/2000/svg'
          ? document.createElementNS('http://www.w3.org/2000/svg', 'tspan')
          : document.createElement('mark');
        mark.setAttribute('class', 'page-find-mark');
        mark.textContent = text.slice(segment.start, segment.end);
        segment.group.push(mark);
        fragment.appendChild(mark);
        pos = segment.end;
      });
      fragment.appendChild(document.createTextNode(text.slice(pos)));
      parent.replaceChild(fragment, node);
    });
    activate(Math.max(0, Math.min(activeIndex, marks.length - 1)), scroll);
  }

  function close() {
    if (panel.hidden) return;
    clearTimeout(refreshTimer);
    clearMarks();
    panel.hidden = true;
    if (panel.parentNode !== panelHome) panelHome.appendChild(panel);
    ownerDialog = null;
    input.value = '';
    activeIndex = -1;
    if (escToken !== null) window.Keymap.popEsc(escToken);
    escToken = null;
    if (previousFocus && previousFocus.isConnected && 'focus' in previousFocus && typeof previousFocus.focus === 'function') previousFocus.focus({preventScroll: true});
  }

  function open() {
    if (window.Keymap.isDispatchPaused()) {
      window.Keymap.captureNativeShortcut('ctrl+f');
      return;
    }
    var modal = /** @type {HTMLDialogElement | null} */ (document.querySelector('dialog:modal'));
    if (!modal && typeof window.openSettingsFind === 'function') {
      window.openSettingsFind();
      return;
    }
    var filterInput = /** @type {HTMLInputElement | null} */ (document.querySelector('.vf-search input'));
    if (!modal && filterInput && filterInput.getClientRects().length) {
      filterInput.focus();
      filterInput.select();
      return;
    }
    // The native dialog top layer makes its siblings inert. Keep Find inside
    // that dialog so its input can receive focus and its panel stays visible.
    if (panel.parentNode !== (modal || panelHome)) {
      previousFocus = document.activeElement;
      observer.disconnect();
      (modal || panelHome).appendChild(panel);
    }
    ownerDialog = modal;
    if (panel.hidden) {
      previousFocus = document.activeElement;
      panel.hidden = false;
      escToken = window.Keymap.pushEsc(close);
    }
    input.focus();
    input.select();
    refresh(true);
  }

  /** @param {number} delta */
  function move(delta) { activate(activeIndex + delta, true); }

  document.addEventListener('close', function(e) {
    // Dialog close events are queued. An earlier close can arrive after the
    // same dialog reopens; only clear Find when its owner is still closed.
    if (ownerDialog && e.target === ownerDialog && !ownerDialog.open) close();
  }, true);
  // A class/style mutation can be observed mid-fade; collect again once the
  // effective opacity reaches its final value, including hover-only controls.
  document.addEventListener('transitionend', function(e) {
    if (e.propertyName === 'opacity' && !panel.hidden && input.value.trim()) refresh(false);
  }, true);
  input.addEventListener('input', function() { activeIndex = 0; refresh(true); });
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') { e.preventDefault(); move(e.shiftKey ? -1 : 1); }
  });
  /** @type {HTMLButtonElement} */ (document.getElementById('pageFindPrevious')).addEventListener('click', function() { move(-1); });
  /** @type {HTMLButtonElement} */ (document.getElementById('pageFindNext')).addEventListener('click', function() { move(1); });
  /** @type {HTMLButtonElement} */ (document.getElementById('pageFindClose')).addEventListener('click', close);
  document.addEventListener('keydown', function(e) {
    if (!window.__TAURI_INTERNALS__) return;
    if (window.Keymap.isDispatchPaused()) return;
    if ((e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey && e.key.toLowerCase() === 'f') {
      e.preventDefault();
      e.stopImmediatePropagation();
      open();
    }
  }, true);
  window.VireoPageFind = {open: open, close: close};
  // Standalone Setup has no navbar command dispatcher. The navbar replaces
  // this fallback with its full dispatcher on pages that include it.
  if (typeof window.handleNativeMenuCommand !== 'function') {
    window.handleNativeMenuCommand = function(command) {
      if (command === 'find') open();
    };
  }
})();
