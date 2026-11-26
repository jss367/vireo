// The accept/skip keyboard shortcuts for the first visible pending card.
// Classic page script; load boot.js after all definitions.

/** @type {{accept:string;reject:string;skip_pending:string}|null} */ var _shortcuts = null;

/* ---------- Keyboard Shortcuts ---------- */
function bindReviewKeyboard() {
  document.addEventListener('keydown', function(e) {
    if ((e.target instanceof Element ? e.target.tagName : '') === 'INPUT' || (e.target instanceof Element ? e.target.tagName : '') === 'TEXTAREA' || (e.target instanceof Element ? e.target.tagName : '') === 'SELECT') return;
    if (e.defaultPrevented || (e.target instanceof HTMLElement && e.target.isContentEditable)) return;
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
    const first = pending[0];
    if (!first) return;
    if (matchesShortcut(e, _shortcuts.accept)) { e.preventDefault(); acceptPrediction(first.id); }
    else if (matchesShortcut(e, _shortcuts.reject)) { e.preventDefault(); rejectPrediction(first.id); }
    else if (matchesShortcut(e, _shortcuts.skip_pending)) {
      e.preventDefault();
      if (pending.length > 1) {
        skippedPredictionOrder.set(first.id, ++predictionSkipSequence);
        renderGrid();
      } else showToast('This is the last pending prediction in this view.', 'info');
    }
  });
}
