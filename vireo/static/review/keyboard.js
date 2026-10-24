// The accept/skip keyboard shortcuts for the first visible pending card.
// Classic page script; load boot.js after all definitions.

/** @type {{accept:string;skip:string}|null} */ var _shortcuts = null;

/* ---------- Keyboard Shortcuts ---------- */
function bindReviewKeyboard() {
  document.addEventListener('keydown', function(e) {
    if ((e.target instanceof Element ? e.target.tagName : '') === 'INPUT' || (e.target instanceof Element ? e.target.tagName : '') === 'TEXTAREA' || (e.target instanceof Element ? e.target.tagName : '') === 'SELECT') return;
    if (mode !== 'review') return;
    if (!_shortcuts) return;
    // Ignore A/S while a filter-driven reload is in flight — the ``predictions``
    // array still points at the pre-reload rows, so a keystroke would
    // accept/skip a card that no longer matches the visible chips.
    if (_predictionsReloading) return;
    // No-op while an overlay owns the keyboard (lightbox, burst modal, etc.) —
    // otherwise A/S would invisibly accept/reject grid cards behind it.
    if (document.querySelector(/** @type {'div'} */ ('.lightbox-overlay.active, .pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .grm-overlay.open, .inspect-overlay.open, .shortcuts-overlay.open, .help-overlay.active, .report-overlay.active'))) return;
    // The toolbar hint promises "first visible card": use the grid's filtered
    // + sorted view, not the API-ordered raw predictions array.
    var pending = visiblePendingCards();
    if (pending.length === 0) return;
    if (matchesShortcut(e, _shortcuts.accept)) { e.preventDefault(); acceptPrediction(pending[0]?.id || 0); }
    else if (matchesShortcut(e, _shortcuts.skip)) { e.preventDefault(); rejectPrediction(pending[0]?.id || 0); }
  });
}
