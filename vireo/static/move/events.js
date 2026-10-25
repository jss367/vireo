// The Move page's event bindings. The template's controls name their
// handlers in `data-move-on="event:name"` (with `data-move-arg` for the one
// argument some take) instead of inline on*= attributes; rendered markup
// carries data attributes read by delegated listeners on its container.

import { closeConfirm, confirmOk } from './confirm-modal.js';
import {
  browseTo, closeBrowser, createNewFolder, openBrowser, selectBrowserFolder,
} from './folder-browser.js';
import { switchTab } from './folders.js';
import { rememberMoveVisibility } from './move-visibility.js';
import { clearPhotoSelection, handlePhotoClick, loadPhotosForFolder, startPhotoMove } from './photo-move.js';
import {
  onQuickFolderChange, onQuickFolderModeChange, resetQuickFolderName,
  updateQuickFolderTemplateControls,
} from './quick-move-form.js';
import { updateQuickMoveBtn } from './quick-move-preview.js';
import { startQuickMove } from './quick-move-start.js';
import { onQuickDestModeChange } from './remote-targets.js';
import { hideRuleForm, previewRule, saveRule, showRuleForm } from './rule-form.js';
import { deleteRule, editRule, runRule } from './rules.js';

/**
 * Handlers named by `data-move-on`. Each gets the bound element, whose
 * `data-move-arg` carries an argument; an unknown name throws so a typo
 * fails loudly.
 *
 * @type {Record<string, (element: HTMLElement) => unknown>}
 */
var MOVE_HANDLERS = {
  switchTab: function(el) { switchTab(el.dataset.moveArg || ''); },
  onQuickFolderChange: function() { onQuickFolderChange(); },
  onQuickDestModeChange: function() { onQuickDestModeChange(); },
  onQuickFolderModeChange: function() { onQuickFolderModeChange(); },
  updateQuickFolderTemplateControls: function() { updateQuickFolderTemplateControls(); },
  updateQuickMoveBtn: function() { updateQuickMoveBtn(); },
  openBrowser: function(el) { openBrowser(el.dataset.moveArg || ''); },
  resetQuickFolderName: function() { resetQuickFolderName(); },
  startQuickMove: function() { return startQuickMove(); },
  rememberMoveVisibility: function() { return rememberMoveVisibility(); },
  loadPhotosForFolder: function() { return loadPhotosForFolder(); },
  clearPhotoSelection: function() { clearPhotoSelection(); },
  startPhotoMove: function() { return startPhotoMove(); },
  showRuleForm: function() { showRuleForm(); },
  // browseTo(null) opens the browser's default start; the shortcuts pass a key.
  browseTo: function(el) { return browseTo(el.dataset.moveArg === undefined ? null : el.dataset.moveArg); },
  createNewFolder: function() { return createNewFolder(); },
  selectBrowserFolder: function() { selectBrowserFolder(); },
  closeBrowser: function() { closeBrowser(); },
  closeConfirm: function() { closeConfirm(); },
  confirmOk: function() { confirmOk(); },
};

export function bindMoveMarkup() {
  document.querySelectorAll('[data-move-on]').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    (el.dataset.moveOn || '').split(/\s+/).filter(Boolean).forEach(function(spec) {
      var parts = spec.split(':');
      var found = MOVE_HANDLERS[parts[1] || ''];
      if (!found) throw new Error('Unknown Move handler: ' + spec);
      var handler = found;
      el.addEventListener(parts[0] || '', function() { handler(el); });
    });
  });
}

/** @param {string} id @param {(target: Element, event: MouseEvent) => void} handler */
function onContainerClick(id, handler) {
  /** @type {HTMLElement} */ (document.getElementById(id)).addEventListener('click', function(event) {
    if (event.target instanceof Element) handler(event.target, event);
  });
}

/** Delegated listeners for the photo grid, the rule list and the rule form. */
export function bindMoveRenderedMarkup() {
  onContainerClick('photoGridContainer', function(target, event) {
    var card = target.closest(/** @type {'div'} */ ('.grid-card[data-id]'));
    if (card) handlePhotoClick(event, Number(card.dataset.id), Number(card.dataset.idx));
  });
  onContainerClick('rulesList', function(target) {
    var button = target.closest(/** @type {'button'} */ ('button[data-rule-action]'));
    var item = button && button.closest(/** @type {'div'} */ ('.rule-item[data-id]'));
    if (!button || !item) return;
    var ruleId = Number(item.dataset.id);
    var action = button.dataset.ruleAction;
    if (action === 'run') runRule(ruleId);
    else if (action === 'edit') editRule(ruleId);
    else if (action === 'delete') deleteRule(ruleId);
  });
  onContainerClick('ruleFormContainer', function(target) {
    var button = target.closest(/** @type {'button'} */ ('button[data-rule-form]'));
    if (!button) return;
    var action = button.dataset.ruleForm;
    if (action === 'browse') openBrowser('ruleDestInput');
    else if (action === 'save') saveRule();
    else if (action === 'preview') previewRule();
    else if (action === 'cancel') hideRuleForm();
  });
}
