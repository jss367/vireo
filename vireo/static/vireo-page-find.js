/* Page-text search for the desktop webview, which has no browser find bar.
 * Native Edit > Find and keyboard events share this entry point. Pages with
 * an existing search control retain their search behavior.
 */
(function() {
  'use strict';

  var panel = document.getElementById('pageFindPanel');
  var panelHome = panel.parentNode;
  var ownerDialog = null;
  var input = document.getElementById('pageFindInput');
  var status = document.getElementById('pageFindStatus');
  var marks = []; // One group of highlight fragments per logical match.
  var activeIndex = -1;
  var previousFocus = null;
  var escToken = null;
  var refreshTimer = null;
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
    var parents = new Set();
    marks.forEach(function(group) {
      group.forEach(function(mark) {
        var parent = mark.parentNode;
        if (!parent) return;
        parent.replaceChild(document.createTextNode(mark.textContent), mark);
        parents.add(parent);
      });
    });
    parents.forEach(function(parent) { parent.normalize(); });
    marks = [];
  }

  function computedStyle(el, cache) {
    if (!cache.has(el)) cache.set(el, window.getComputedStyle(el));
    return cache.get(el);
  }

  function visibleForFind(node, styles) {
    var el = node.nodeType === 3 ? node.parentElement : node;
    if (!el) return false;
    if (el.closest('#pageFindPanel, script, style, noscript, textarea, input, select, [contenteditable], svg title, svg desc')) return false;
    for (var current = el; current; current = current.parentElement) {
      if (current.hidden || current.getAttribute('aria-hidden') === 'true') return false;
      if (current.tagName === 'DETAILS' && !current.open &&
          (current !== el || node.nodeType === 3)) {
        var summary = current.querySelector(':scope > summary');
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

  function collectTextRuns() {
    var styles = new WeakMap();
    var runs = [];
    var run = null;
    function collect(node) {
      if (node.nodeType === 3) {
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
      if (node.nodeType !== 1) return;
      var display = computedStyle(node, styles).display;
      // Inline formatting stays together. Blocks, line breaks, replaced
      // controls/images, and independent SVG labels delimit text runs.
      var boundary = (display !== 'inline' && display !== 'contents') ||
        /^(BR|IMG|HR|INPUT|SELECT|TEXTAREA|IFRAME|VIDEO|AUDIO|CANVAS|TEXT|TEXTPATH)$/.test(node.tagName.toUpperCase());
      if (boundary) run = null;
      Array.prototype.forEach.call(node.childNodes, collect);
      if (boundary) run = null;
    }
    // A modal's backdrop obscures the rest of the page.
    collect(ownerDialog || document.body);
    return runs;
  }

  function updateStatus() {
    status.textContent = marks.length ? (activeIndex + 1) + ' of ' + marks.length : '0 results';
    document.getElementById('pageFindPrevious').disabled = !marks.length;
    document.getElementById('pageFindNext').disabled = !marks.length;
  }

  function activate(index, scroll) {
    observer.disconnect();
    marks.forEach(function(group) {
      group.forEach(function(mark) { mark.classList.remove('active'); });
    });
    activeIndex = marks.length ? ((index % marks.length) + marks.length) % marks.length : -1;
    if (activeIndex >= 0) {
      var active = marks[activeIndex];
      active.forEach(function(mark) { mark.classList.add('active'); });
      if (scroll) active[0].scrollIntoView({block: 'center', inline: 'nearest'});
    }
    updateStatus();
    observe();
  }

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
    var segmentsByNode = new Map();
    collectTextRuns().forEach(function(run) {
      matcher.lastIndex = 0;
      var partIndex = 0;
      var match;
      while ((match = matcher.exec(run.text)) !== null) {
        var group = [];
        marks.push(group);
        var start = match.index;
        var end = start + match[0].length;
        while (partIndex < run.parts.length && run.parts[partIndex].end <= start) partIndex++;
        for (var i = partIndex; i < run.parts.length && run.parts[i].start < end; i++) {
          var part = run.parts[i];
          if (part.end <= start) continue;
          if (!segmentsByNode.has(part.node)) segmentsByNode.set(part.node, []);
          segmentsByNode.get(part.node).push({
            start: Math.max(start, part.start) - part.start,
            end: Math.min(end, part.end) - part.start,
            group: group
          });
        }
      }
    });
    segmentsByNode.forEach(function(segments, node) {
      var text = node.nodeValue;
      var fragment = document.createDocumentFragment();
      var pos = 0;
      segments.forEach(function(segment) {
        fragment.appendChild(document.createTextNode(text.slice(pos, segment.start)));
        var mark = node.parentElement.namespaceURI === 'http://www.w3.org/2000/svg'
          ? document.createElementNS('http://www.w3.org/2000/svg', 'tspan')
          : document.createElement('mark');
        mark.setAttribute('class', 'page-find-mark');
        mark.textContent = text.slice(segment.start, segment.end);
        segment.group.push(mark);
        fragment.appendChild(mark);
        pos = segment.end;
      });
      fragment.appendChild(document.createTextNode(text.slice(pos)));
      node.parentNode.replaceChild(fragment, node);
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
    if (previousFocus && previousFocus.isConnected) previousFocus.focus({preventScroll: true});
  }

  function open() {
    if (window.Keymap.isDispatchPaused()) {
      window.Keymap.captureNativeShortcut('ctrl+f');
      return;
    }
    var modal = document.querySelector('dialog:modal');
    if (!modal && typeof window.openSettingsFind === 'function') {
      window.openSettingsFind();
      return;
    }
    var filterInput = document.querySelector('.vf-search input');
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
  document.getElementById('pageFindPrevious').addEventListener('click', function() { move(-1); });
  document.getElementById('pageFindNext').addEventListener('click', function() { move(1); });
  document.getElementById('pageFindClose').addEventListener('click', close);
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
