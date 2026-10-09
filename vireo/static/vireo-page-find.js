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
        attributes: true, attributeFilter: ['class', 'style', 'hidden', 'aria-hidden'],
      });
    }
  }

  function clearMarks() {
    observer.disconnect();
    marks.forEach(function(mark) {
      var parent = mark.parentNode;
      if (!parent) return;
      parent.replaceChild(document.createTextNode(mark.textContent), mark);
      parent.normalize();
    });
    marks = [];
  }

  function visibleText(node) {
    var el = node.parentElement;
    if (!el || !node.nodeValue.trim()) return false;
    if (el.closest('#pageFindPanel, script, style, noscript, textarea, input, select, [contenteditable], svg title, svg desc')) return false;
    for (var current = el; current; current = current.parentElement) {
      if (current.hidden || current.getAttribute('aria-hidden') === 'true') return false;
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
    marks.forEach(function(mark) { mark.classList.remove('active'); });
    activeIndex = marks.length ? ((index % marks.length) + marks.length) % marks.length : -1;
    if (activeIndex >= 0) {
      var active = marks[activeIndex];
      active.classList.add('active');
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
    var lowerQuery = query.toLowerCase();
    var walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode: function(node) {
        return node.nodeValue.toLowerCase().includes(lowerQuery) && visibleText(node)
          ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
      },
    });
    var nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(function(node) {
      var text = node.nodeValue;
      var lower = text.toLowerCase();
      var fragment = document.createDocumentFragment();
      var pos = 0;
      var index = lower.indexOf(lowerQuery);
      while (index !== -1) {
        fragment.appendChild(document.createTextNode(text.slice(pos, index)));
        var mark = node.parentElement.namespaceURI === 'http://www.w3.org/2000/svg'
          ? document.createElementNS('http://www.w3.org/2000/svg', 'tspan')
          : document.createElement('mark');
        mark.setAttribute('class', 'page-find-mark');
        mark.textContent = text.slice(index, index + query.length);
        marks.push(mark);
        fragment.appendChild(mark);
        pos = index + query.length;
        index = lower.indexOf(lowerQuery, pos);
      }
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
    input.value = '';
    activeIndex = -1;
    if (escToken !== null) window.Keymap.popEsc(escToken);
    escToken = null;
    if (previousFocus && previousFocus.isConnected) previousFocus.focus({preventScroll: true});
  }

  function open() {
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
    if ((e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey && e.key.toLowerCase() === 'f') {
      e.preventDefault();
      e.stopImmediatePropagation();
      open();
    }
  }, true);
  window.VireoPageFind = {open: open, close: close};
})();
