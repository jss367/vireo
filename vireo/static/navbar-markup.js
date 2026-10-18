// Event handlers for the controls in _navbar.html. The markup carries no
// inline on*= handlers: an element names its handlers in a
// data-navbar-on="event:name ..." attribute, and bindNavbarMarkup() attaches
// the named functions below with addEventListener, directly on the element as
// the inline attributes were, so `this` is the element and the phase and order
// are unchanged. A name missing from HANDLERS throws, so a typo in the
// template fails on load (vireo/tests/test_navbar_modules.py checks the two
// lists against each other too).

import { closeCommandPalette } from './navbar-command-palette-open.js';
import { closeReportModal, openReportModal, sendReport } from './navbar-report-context-menu.js';
import { toggleOverflowMenu } from './navbar-tabs.js';
import { closeShortcutsSheet } from './navbar-tab-reorder-hotkeys.js';
import { vireoWorkspaceSwitcher } from './navbar-workspace-switcher-init.js';
import { lbState } from './lightbox/shared-state.js';
import { vireoLightboxSession, vireoLightboxViewport } from './lightbox/state.js';
import {
  onLightboxAdjustmentInput, resetLightboxAdjustments, toggleLightboxAdjustPanel, vireoTogglePairSource,
} from './lightbox/edits.js';
import {
  _lbOnMaskVariantChange, lightboxSetFlagAction, toggleLightboxBoxes, toggleLightboxChrome,
  toggleLightboxEye, toggleLightboxInfo, toggleLightboxMasks, toggleLightboxTrackEye,
  toggleLightboxViewMenu,
} from './lightbox/overlays.js';
import { lightboxNav } from './lightbox/navigation.js';
import {
  closeLightbox, setLightboxZoomFromSlider, setLightboxZoomToFit, setLightboxZoomToOneToOne,
  stepLightboxZoom, toggleLightboxZoom, toggleLightboxZoomPopover,
} from './lightbox/controls.js';
import { confirmDelete, hideDeleteModal, lightboxDelete } from './lightbox/delete.js';
import { closeInatModal, inatDoSubmit } from './lightbox/inat.js';
import { closePipeline } from './navbar-pipeline-inspector.js';
import { closeSimilar } from './navbar-similar-photos.js';
import {
  dismissRuntimeWarning, doRedo, doUndo, lpApplyFilter, switchBpTab, toggleAdvancedMode,
  toggleBottomPanel, toggleTheme,
} from './navbar-shared-helpers.js';
import {
  closeCascadeConfirm, closeMissingFoldersModal, closeMissingPhotosModal, closeRelocateBrowser,
  closeRescanModal, confirmCascade, confirmRelocate, dismissDupCleanupBanner, dismissMissingBanner,
  dismissMissingPhotosBanner, dismissNewImagesBanner, loadMissingFolders, onRescanScopeChange,
  openMissingFoldersModal, openMissingPhotosModal, openRescanModal, recheckNewImages,
  refreshMissingPhotosNow, removeAllMissingFolders, removeSelectedMissingPhotos,
  reviewNewImagesImport, runRescan, toggleAllMissingPhotos, updateMissingPhotosThumbSize,
} from './navbar-folder-banners.js';
import { openCommandPalette, renderCommandPalette } from './navbar-command-palette.js';

/** @typedef {(this: HTMLElement, event: Event) => unknown} NavbarHandler */

/** Re-render the command palette after a search-option toggle. */
function rerenderCommandPalette() {
  var input = /** @type {HTMLInputElement | null} */ (document.getElementById('cmdPaletteInput'));
  renderCommandPalette(input ? input.value || '' : '');
}

/** @param {HTMLElement} element */
function inputValue(element) {
  return /** @type {HTMLInputElement} */ (element).value;
}

/** @type {Record<string, NavbarHandler>} */
const HANDLERS = {
  // Navbar strip and workspace menu.
  toggleWorkspaceMenu: function() { vireoWorkspaceSwitcher.toggle(); },
  showCreateWorkspace: function() { vireoWorkspaceSwitcher.showCreate(); },
  createWorkspace: function() { vireoWorkspaceSwitcher.createWorkspace(); },
  hideCreateWorkspace: function() { vireoWorkspaceSwitcher.hideCreate(); },
  openRescanModal: function() { openRescanModal(); },
  openCommandPalette: function() { openCommandPalette(); },
  toggleOverflowMenu: function(event) { toggleOverflowMenu(event); },
  openReportModal: function() { openReportModal(); },
  // help.js is a classic script; it publishes openHelpModal itself.
  openHelpModal: function() { window.openHelpModal(); },
  toggleAdvancedMode: function() { toggleAdvancedMode(); },
  toggleTheme: function() { toggleTheme(); },
  toggleBottomPanel: function() { toggleBottomPanel(); },
  dismissRuntimeWarning: function() { dismissRuntimeWarning(); },

  // Command palette, help search, report, color-label and shortcuts overlays.
  closeCommandPaletteOnBackdrop: function(event) { if (event.target === this) closeCommandPalette(); },
  toggleCommandPaletteMatchCase: function() { VireoTextSearch.toggle('cmdPalette', 'match_case', rerenderCommandPalette); },
  toggleCommandPaletteWholeWord: function() { VireoTextSearch.toggle('cmdPalette', 'whole_word', rerenderCommandPalette); },
  toggleHelpMatchCase: function() { VireoTextSearch.toggle('helpSearch', 'match_case', window._helpSearchRefresh); },
  toggleHelpWholeWord: function() { VireoTextSearch.toggle('helpSearch', 'whole_word', window._helpSearchRefresh); },
  closeReportModalOnBackdrop: function(event) { if (event.target === this) closeReportModal(); },
  closeReportModal: function() { closeReportModal(); },
  sendReport: function() { sendReport(); },
  // vireo-color-labels.js is a classic script that publishes VireoColorLabels.
  closeColorLabelEditorOnBackdrop: function(event) { if (event.target === this) window.VireoColorLabels.closeEditor(); },
  closeColorLabelEditor: function() { window.VireoColorLabels.closeEditor(); },
  saveColorLabelEditor: function() { window.VireoColorLabels.saveEditor(); },
  closeShortcutsSheetOnBackdrop: function(event) { if (event.target === this) closeShortcutsSheet(); },
  closeShortcutsSheet: function() { closeShortcutsSheet(); },

  // Missing folders / originals, new images and duplicate banners and modals.
  openMissingFoldersModal: function(event) { openMissingFoldersModal(); event.preventDefault(); },
  dismissMissingBanner: function() { dismissMissingBanner(); },
  openMissingPhotosModal: function(event) { openMissingPhotosModal(); event.preventDefault(); },
  dismissMissingPhotosBanner: function() { dismissMissingPhotosBanner(); },
  reviewNewImagesImport: function() { reviewNewImagesImport(/** @type {HTMLButtonElement} */ (this)); },
  recheckNewImages: function() { recheckNewImages(); },
  dismissNewImagesBanner: function() { dismissNewImagesBanner(); },
  dismissDupCleanupBanner: function() { dismissDupCleanupBanner(); },
  removeAllMissingFolders: function() { removeAllMissingFolders(); },
  loadMissingFolders: function() { loadMissingFolders(); },
  closeMissingFoldersModal: function() { closeMissingFoldersModal(); },
  toggleAllMissingPhotos: function() { toggleAllMissingPhotos(/** @type {HTMLInputElement} */ (this).checked); },
  updateMissingPhotosThumbSize: function() { updateMissingPhotosThumbSize(inputValue(this)); },
  removeSelectedMissingPhotos: function() { removeSelectedMissingPhotos(); },
  refreshMissingPhotosNow: function() { refreshMissingPhotosNow(); },
  closeMissingPhotosModal: function() { closeMissingPhotosModal(); },
  changeRescanScope: function() { onRescanScopeChange(); },
  runRescan: function() { runRescan(); },
  closeRescanModal: function() { closeRescanModal(); },
  confirmRelocate: function() { confirmRelocate(); },
  closeRelocateBrowser: function() { closeRelocateBrowser(); },
  confirmCascade: function() { confirmCascade(); },
  closeCascadeConfirm: function() { closeCascadeConfirm(); },

  // Lightbox.
  closeLightbox: function(event) { closeLightbox(event); },
  stopPropagation: function(event) { event.stopPropagation(); },
  lightboxPrevious: function(event) { event.stopPropagation(); lightboxNav(-1); },
  lightboxNext: function(event) { event.stopPropagation(); lightboxNav(1); },
  // Click-to-zoom on the image, unless a mouseup that zoomed out already
  // handled this click (the latch the viewport's handledClick sets).
  lightboxImageClick: function(event) {
    event.stopPropagation();
    if (!lbState.visualTransitionPending && Math.abs(vireoLightboxViewport.zoom() - 1.0) < 0.001 && !lbState.zoomHandled) {
      toggleLightboxZoom(/** @type {MouseEvent} */ (event));
    }
    lbState.zoomHandled = false;
  },
  toggleLightboxZoomPopover: function() { toggleLightboxZoomPopover(); },
  lightboxZoomOut: function() { stepLightboxZoom(-1); },
  lightboxZoomIn: function() { stepLightboxZoom(1); },
  setLightboxZoomFromSlider: function() { setLightboxZoomFromSlider(inputValue(this)); },
  setLightboxZoomToFit: function() { setLightboxZoomToFit(); },
  setLightboxZoomToOneToOne: function() { setLightboxZoomToOneToOne(); },
  setLightboxZoomToMax: function() { setLightboxZoomFromSlider(1000); },
  toggleLightboxPairSource: function(event) {
    event.stopPropagation();
    var photoId = vireoLightboxSession.requestedPhotoId();
    if (photoId != null) vireoTogglePairSource(photoId);
  },
  toggleLightboxViewMenu: function(event) { event.stopPropagation(); toggleLightboxViewMenu(); },
  lightboxFlag: function(event) { event.stopPropagation(); lightboxSetFlagAction('flagged'); },
  lightboxReject: function(event) { event.stopPropagation(); lightboxSetFlagAction('rejected'); },
  toggleLightboxAdjustPanel: function(event) { event.stopPropagation(); toggleLightboxAdjustPanel(); },
  lightboxDelete: function() { lightboxDelete(); },
  toggleLightboxBoxes: function() { toggleLightboxBoxes(); },
  toggleLightboxMasks: function() { toggleLightboxMasks(); },
  changeLightboxMaskVariant: function(event) { event.stopPropagation(); _lbOnMaskVariantChange(); },
  toggleLightboxEye: function() { toggleLightboxEye(); },
  toggleLightboxTrackEye: function() { toggleLightboxTrackEye(); },
  toggleLightboxInfo: function() { toggleLightboxInfo(); },
  toggleLightboxChrome: function() { toggleLightboxChrome(); },
  resetLightboxAdjustments: function() { resetLightboxAdjustments(); },
  lightboxAdjustmentInput: function() { onLightboxAdjustmentInput(/** @type {HTMLInputElement} */ (this)); },
  hideDeleteModal: function() { hideDeleteModal(); },
  confirmDelete: function() { confirmDelete(); },
  inatDoSubmit: function() { inatDoSubmit(); },
  closeInatModal: function() { closeInatModal(); },

  // Pipeline inspector, similar photos and the bottom panel.
  closePipeline: function() { closePipeline(); },
  closeSimilar: function() { closeSimilar(); },
  showJobsPanel: function() { switchBpTab('jobs'); },
  showHistoryPanel: function() { switchBpTab('history'); },
  showLogsPanel: function() { switchBpTab('logs'); },
  applyLogLevelFilter: function() { lpApplyFilter(); },
  undo: function() { doUndo(); },
  redo: function() { doRedo(); },
};

/** Attach every data-navbar-on handler under root (the document by default). */
export function bindNavbarMarkup(/** @type {ParentNode} */ root = document) {
  root.querySelectorAll('[data-navbar-on]').forEach(function(element) {
    var spec = element.getAttribute('data-navbar-on') || '';
    spec.split(/\s+/).filter(Boolean).forEach(function(pair) {
      var [event, name] = pair.split(':');
      var handler = name ? HANDLERS[name] : undefined;
      if (!event || !handler) throw new Error('Unknown navbar handler: ' + pair);
      element.addEventListener(event, /** @type {EventListener} */ (handler));
    });
  });
}

/** The handler names, for vireo/tests/test_navbar_modules.py. */
export const NAVBAR_HANDLER_NAMES = Object.keys(HANDLERS);
