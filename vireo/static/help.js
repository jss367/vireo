(function() {
  'use strict';

  /** @type {NavHelpItem[]} */ var helpData = [];
  /** @type {Fuse<NavHelpItem> | null} */ var fuse = null;
  var modal = /** @type {HTMLElement} */ (document.getElementById('helpModal'));
  var input = /** @type {HTMLInputElement} */ (document.getElementById('helpSearchInput'));
  var results = /** @type {HTMLElement} */ (document.getElementById('helpResults'));

  // Load help data and init Fuse
  fetch('/static/help.json')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      helpData = data;
      fuse = new Fuse(data, {
        keys: [
          { name: 'question', weight: 3 },
          { name: 'keywords', weight: 2 },
          { name: 'answer', weight: 1 }
        ],
        threshold: 0.4,
        includeMatches: true
      });
      renderGrouped(helpData);
    })
    .catch(function() {
      results.innerHTML = '<div class="help-empty">Failed to load help data. Please try again later.</div>';
    });

  // Render all entries grouped by category (default state)
  /** @param {NavHelpItem[]} items */
  function renderGrouped(items) {
    /** @type {Record<string,NavHelpItem[]>} */ var groups = {};
    items.forEach(function(item) {
      var cat = item.category || 'Other';
      if (!groups[cat]) groups[cat] = [];
      groups[cat]?.push(item);
    });
    var html = '';
    Object.keys(groups).forEach(function(cat) {
      html += '<div class="help-category">' + escHtml(cat) + '</div>';
      (groups[cat] || []).forEach(function(item) {
        html += renderItem(item);
      });
    });
    results.innerHTML = html;
  }

  // Render a single help item
  /** @param {NavHelpItem} item */
  function renderItem(item) {
    return '<div class="help-item">' +
      '<div class="help-question">' + escHtml(item.question) + '</div>' +
      '<span class="help-badge">' + escHtml(item.category) + '</span>' +
      '<div class="help-answer">' + escHtml(item.answer) + '</div>' +
      '</div>';
  }

  // Search handler
  function onSearch() {
    var query = input.value.trim();
    if (!query) {
      renderGrouped(helpData);
      return;
    }
    if (!fuse) return;
    var options = VireoTextSearch.readOptions('helpSearch');
    var hits;
    if (options.matchCase || options.wholeWord) {
      hits = helpData
        .filter(function(item) {
          return VireoTextSearch.matchesFields(
            [item.question, item.keywords, item.answer],
            query,
            options
          );
        })
        .map(function(item) { return {item: item}; });
    } else {
      hits = fuse.search(query);
    }
    if (hits.length === 0) {
      results.innerHTML = '<div class="help-empty">No results. Try different keywords.</div>';
      return;
    }
    var html = '';
    hits.forEach(function(hit) {
      html += renderItem(hit.item);
    });
    results.innerHTML = html;
  }

  input.addEventListener('input', onSearch);
  window._helpSearchRefresh = onSearch;

  // Open / close
  window.openHelpModal = function() {
    // A managed VireoDialog on the stack has made this overlay's branch inert
    // via updateBackground(), so showing the help modal would present a
    // panel whose search input and controls cannot be focused. Treat the F1
    // or icon press as a no-op until the dialog is dismissed; opening help
    // afterwards keeps the inert overlay in the background until the user
    // closes the managed dialog, which already clears the inert state.
    if (window.VireoDialog && window.VireoDialog.hasActive && window.VireoDialog.hasActive()) return;
    // Push an Esc handler onto the central Keymap stack so the help modal
    // participates in the one-Esc-per-overlay model. Defensive: if keymap.js
    // failed to load, opening still works — Esc just won't close the modal.
    // Track open state via the modal's own class rather than the Esc token so
    // the body-scroll lock pairs cleanly even when Keymap is unavailable.
    var alreadyOpen = modal.classList.contains('active');
    if (window.Keymap && window.Keymap.pushEsc) {
      if (window._helpEscToken) window.Keymap.popEsc(window._helpEscToken);
      window._helpEscToken = window.Keymap.pushEsc(function() { window.closeHelpModal(); });
    }
    modal.classList.add('active');
    if (!alreadyOpen) {
      if (window.Keymap && window.Keymap.lockBodyScroll) window.Keymap.lockBodyScroll();
      else document.body.style.overflow = 'hidden';
    }
    input.value = '';
    onSearch();
    input.focus();
  };

  window.closeHelpModal = function() {
    var wasOpen = modal.classList.contains('active');
    if (window.Keymap && window.Keymap.popEsc && window._helpEscToken) {
      window.Keymap.popEsc(window._helpEscToken);
      window._helpEscToken = null;
    }
    modal.classList.remove('active');
    if (wasOpen) {
      if (window.Keymap && window.Keymap.unlockBodyScroll) window.Keymap.unlockBodyScroll();
      else document.body.style.overflow = '';
    }
  };

  // Close on backdrop click
  modal.addEventListener('click', function(e) {
    if (e.target === modal) window.closeHelpModal();
  });

  // Keyboard: F1 toggles the help modal. Esc is owned by the Keymap.pushEsc
  // stack (registered in openHelpModal) and is intentionally NOT handled here.
  document.addEventListener('keydown', function(e) {
    if (e.key === 'F1') {
      e.preventDefault();
      if (modal.classList.contains('active')) {
        window.closeHelpModal();
      } else {
        window.openHelpModal();
      }
    }
  });

  /** @param {unknown} s */
  function escHtml(s) {
    var d = document.createElement('div');
    d.textContent = String(s ?? '');
    return d.innerHTML;
  }
})();
