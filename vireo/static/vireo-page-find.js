/* Page-text search for the desktop webview, which has no browser find bar.
 * Native Edit > Find and keyboard events share this entry point. Pages with
 * an existing search control retain their search behavior.
 */
(function() {
  'use strict';

  var panel = document.getElementById('pageFindPanel');
  var input = document.getElementById('pageFindInput');
  var status = document.getElementById('pageFindStatus');
  var marks = [];
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
    marks.forEach(function(group) {
      group.forEach(function(mark) {
        var parent = mark.parentNode;
        if (!parent) return;
        parent.replaceChild(document.createTextNode(mark.textContent), mark);
        parent.normalize();
      });
    });
    marks = [];
  }

  function visibleText(node) {
    var el = node.parentElement;
    if (!el || !node.nodeValue) return false;
    if (el.closest('#pageFindPanel, script, style, noscript, textarea, input, select, [contenteditable], svg title, svg desc')) return false;
    for (var current = el; current; current = current.parentElement) {
      if (current.hidden || current.getAttribute('aria-hidden') === 'true') return false;
      if (current.tagName === 'DETAILS' && !current.open) {
        var summary = current.querySelector(':scope > summary');
        if (!summary || !summary.contains(el)) return false;
      }
      var style = window.getComputedStyle(current);
      if (style.display === 'none' || style.visibility === 'hidden' || style.visibility === 'collapse') return false;
    }
    return el.getClientRects().length > 0;
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
      var group = marks[activeIndex];
      group.forEach(function(mark) { mark.classList.add('active'); });
      var active = group[0];
      if (scroll) active.scrollIntoView({block: 'center', inline: 'nearest'});
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
    var matcher = new RegExp(query.replace(/[.*+?^$(){}|[\]\\]/g, '\\$&'), 'giu');
    // Inline markup belongs to one visible text run; block boundaries and
    // line breaks end a run so unrelated sections cannot form a match.
    var runs = [], run = [];
    function flushRun() { if (run.length) runs.push(run); run = []; }
    function collect(node) {
      if (node.nodeType === 3) {
        if (!node.nodeValue) return;
        if (visibleText(node)) run.push(node); else flushRun();
        return;
      }
      if (node.nodeType !== 1) return;
      var display = window.getComputedStyle(node).display;
      var boundary = (display !== 'inline' && display !== 'contents') ||
        /^(BR|IMG|HR|INPUT|SELECT|TEXTAREA|IFRAME|VIDEO|AUDIO|CANVAS|TEXT|TEXTPATH)$/.test(node.tagName.toUpperCase());
      if (boundary || node.tagName === 'BR') flushRun();
      Array.prototype.forEach.call(node.childNodes, collect);
      if (boundary || node.tagName === 'BR') flushRun();
    }
    collect(document.body);
    runs.forEach(function(nodes) {
      var text = '', ranges = [];
      nodes.forEach(function(node) {
        ranges.push({node: node, start: text.length, end: text.length + node.nodeValue.length, pieces: []});
        text += node.nodeValue;
      });
      matcher.lastIndex = 0;
      var match;
      while ((match = matcher.exec(text)) !== null) {
        var group = [];
        marks.push(group);
        ranges.forEach(function(range) {
          var start = Math.max(range.start, match.index);
          var end = Math.min(range.end, match.index + match[0].length);
          if (start < end) range.pieces.push({start: start - range.start, end: end - range.start, group: group});
        });
      }
      ranges.forEach(function(range) {
        if (!range.pieces.length) return;
        var node = range.node, value = node.nodeValue, pos = 0;
        var fragment = document.createDocumentFragment();
        range.pieces.forEach(function(piece) {
          fragment.appendChild(document.createTextNode(value.slice(pos, piece.start)));
          var mark = node.parentElement.namespaceURI === 'http://www.w3.org/2000/svg'
            ? document.createElementNS('http://www.w3.org/2000/svg', 'tspan') : document.createElement('mark');
          mark.setAttribute('class', 'page-find-mark');
          mark.textContent = value.slice(piece.start, piece.end);
          piece.group.push(mark);
          fragment.appendChild(mark);
          pos = piece.end;
        });
        fragment.appendChild(document.createTextNode(value.slice(pos)));
        node.parentNode.replaceChild(fragment, node);
      });
    });
    activate(Math.max(0, Math.min(activeIndex, marks.length - 1)), scroll);
  }

  function close() {
    if (panel.hidden) return;
    clearTimeout(refreshTimer);
    clearMarks();
    panel.hidden = true;
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
    if (typeof window.openSettingsFind === 'function') {
      window.openSettingsFind();
      return;
    }
    var filterInput = document.querySelector('.vf-search input');
    if (filterInput && filterInput.getClientRects().length) {
      filterInput.focus();
      filterInput.select();
      return;
    }
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

  input.addEventListener('input', function() { activeIndex = 0; refresh(true); });
  input.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') { e.preventDefault(); move(e.shiftKey ? -1 : 1); }
  });
  document.getElementById('pageFindPrevious').addEventListener('click', function() { move(-1); });
  document.getElementById('pageFindNext').addEventListener('click', function() { move(1); });
  document.getElementById('pageFindClose').addEventListener('click', close);
  document.addEventListener('keydown', function(e) {
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
