// Each scoped action consumes one keystroke. Global shortcuts remain available
// when the active view has no local work (for example, undo after Apply).
var RAPID_SCOPE = 'pipeline_rapid_review';

/** @param {string} action */
function handleRapidShortcutAction(action) {
  if (isQueuePromptOpen()) return true;
  if (document.querySelector('.shortcuts-overlay.open, .help-overlay.active, .report-overlay.active, #commandPalette:not([hidden]), .modal-overlay.open')) return false;
  switch (action) {
    case 'forward': moveRapidPhoto(1); break;
    case 'previous': moveRapidPhoto(-1); break;
    case 'pick': decideCurrent('pick'); break;
    case 'reject': decideCurrent('reject'); break;
    case 'leave': decideCurrent('review'); break;
    case 'clear': clearCurrent(); break;
    case 'undo':
      if (!rapid.history.length) return false;
      undoLast();
      break;
    case 'apply': applyCurrent(false); break;
    case 'exit': window.location.href = '/pipeline/review'; break;
    case 'zoom': toggleRapidZoom(); break;
    default: return false;
  }
  return true;
}

async function loadRapidShortcuts() {
  const {effectiveRapidBindings, SHORTCUT_LABELS} = await import('../shortcuts/labels.js');
  const cfg = await safeFetch('/api/config', {}, {toast: false}).catch(() => (/** @type {RapidShortcutConfig} */ ({})));
  const shortcuts = cfg.keyboard_shortcuts || {};
  const bindings = effectiveRapidBindings(shortcuts);
  window._vireoShortcuts = {...shortcuts, ...window._vireoShortcuts};
  for (const [action, key] of Object.entries(bindings)) {
    if (!key) continue;
    window.Keymap.register?.(RAPID_SCOPE, {
      key, name: action, description: SHORTCUT_LABELS.pipeline_rapid_review?.[action] || action,
      category: 'Rapid Review', action: () => handleRapidShortcutAction(action),
    });
  }
  document.querySelectorAll('[data-rapid-key]').forEach(el => {
    const key = bindings[el.getAttribute('data-rapid-key') || ''];
    el.textContent = key ? formatShortcut(key) : '';
  });
}
