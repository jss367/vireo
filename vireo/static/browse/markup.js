/* Browse: the page's markup event bindings. browse.html, the
   _panorama_dialog.html it includes, and the markup the page's modules render
   (grid cards, stacks, sidebars, the detail panel, the collection editor) name
   each control's handler in a data-browse-on attribute
   (`data-browse-on="click:setFlag"`; several space-separated `event:name`
   pairs allowed) instead of an inline on*= handler. Arguments come from data
   attributes: data-browse-arg for the template's constants, data-id,
   data-cover-id, data-rule-path and the like for rendered rows. A handler is
   bound to the element itself, as the inline handler was, so it runs in the
   same place in the event's path. ES module (see browse/README.md). */
import { state } from './state.js';
import { findSimilar, openLightbox, openPipeline, startPanorama, closePanoramaModal, vireoTogglePairSource } from './shared.js';
import {
  addToCollection, batchAddKeyword, batchDelete, batchSetFlag, batchSetRating,
  confirmBatchCollection, confirmBatchKeyword, hideBatchCollectionModal, hideBatchKeywordModal,
  hideCaptureTimeModal, pickBatchCollection, reviewLocationsForCollection, startCaptureTimeJob,
  syncCaptureTimeFieldState, updateCaptureTimePreview,
} from './batch-actions.js';
import { openSelectedInBurstReview } from './batch-bar.js';
import {
  applyBestBatchPickAndReject, applyBestBatchPickOnly, hideBestBatch, openBestBatchInReview,
} from './best-batch.js';
import { calendarChangeYear, clearCalendarSelection, toggleTimelineMode } from './calendar.js';
import {
  addRuleGroup, addRuleRow, commitRuleOnEnter, hideCollectionModal, removeRule, saveCollection,
  showCollectionModal, updateRule,
} from './collection-editor.js';
import { filterToBrowseGroup, hideFolderWorkspaces, openBatchMoreMenu, openPendingArchiveMenu } from './context-menus.js';
import { closeDetail, filterMetadata } from './detail.js';
import {
  batchRate, removeKeyword, setColorLabel, setFlag, setKeywordType, setRating,
  toggleDetailWildlifeExcluded, toggleTypeDropdown,
} from './edit-actions.js';
import {
  browseForExportDestination, closeExportModal, exportFolderBrowser, insertTemplateVar,
  openExportModal, startExport,
} from './export.js';
import { loadPreviousPhotos } from './loading.js';
import { openBrowseCompare } from './selection-panel.js';
import { clearSelection, selectPhoto } from './selection.js';
import { filterByCollection, toggleOfflineCollectionPhotos } from './sidebar-collections.js';
import {
  browsePhotoNavigationList, openBrowseStackMember, reviewBrowseStack, selectBrowseStackAll,
  selectBrowseStackMember, toggleBrowseStack,
} from './stacks.js';
import {
  onSortChanged, toggleBrowseStacks, toggleDetectionBoxes, updateThumbSize,
} from './view-controls.js';
import { clearPhotoLocation } from './location.js';
import { filterByKeyword } from './sidebar-keywords.js';
import { filterByFolder, toggleTree } from './sidebar-folders.js';

/** @param {HTMLElement} element */
function arg(element) {
  return element.dataset.browseArg || '';
}

/** @param {HTMLElement} element */
function inputValue(element) {
  return /** @type {HTMLInputElement} */ (element).value;
}

/** The single-photo detail panel's photo. */
function detailPhoto() {
  return /** @type {number} */ (state.detailPhotoId);
}

/**
 * Each handler gets the event and the element that names it.
 * @type {Record<string, (event: Event, element: HTMLElement) => unknown>}
 */
const HANDLERS = {
  // Filter-bar row and view controls
  toggleBrowseStacks: function() { toggleBrowseStacks(); },
  onSortChanged: function() { onSortChanged(); },
  updateThumbSize: function(event, element) { updateThumbSize(inputValue(element)); },
  toggleDetectionBoxes: function() { toggleDetectionBoxes(); },
  toggleTimelineMode: function() { toggleTimelineMode(); },
  toggleOfflineCollectionPhotos: function() { toggleOfflineCollectionPhotos(); },
  calendarChangeYear: function(event, element) { calendarChangeYear(Number(arg(element))); },
  clearCalendarSelection: function() { clearCalendarSelection(); },
  loadPreviousPhotos: function() { loadPreviousPhotos(); },
  // Sidebar (the trees and the collection list are rendered markup)
  toggleTree: function(event, element) { toggleTree(event, element); },
  filterByFolder: function(event, element) { filterByFolder(Number(element.dataset.folderId)); },
  openPendingArchiveMenu: function(event, element) { openPendingArchiveMenu(event, element); },
  filterByKeyword: function(event, element) { filterByKeyword(element.dataset.keyword || ''); },
  filterByCollection: function(event, element) { filterByCollection(Number(element.dataset.collectionId)); },
  reviewCollectionLocations: function(event, element) {
    event.stopPropagation();
    reviewLocationsForCollection(Number(element.dataset.collectionId));
  },
  showCollectionModal: function(event, element) { showCollectionModal(null, element); },
  hideFolderWorkspaces: function() { hideFolderWorkspaces(); },
  hideFolderWorkspacesFromBackdrop: function(event, element) {
    if (event.target === element) hideFolderWorkspaces();
  },
  // Grid cards and stacks (rendered markup)
  selectPhotoCard: function(event, element) {
    selectPhoto(/** @type {MouseEvent} */ (event), Number(element.dataset.id), Number(element.dataset.idx));
  },
  toggleBrowseStack: function(event, element) {
    toggleBrowseStack(/** @type {MouseEvent} */ (event), Number(element.dataset.coverId));
  },
  selectBrowseStackMember: function(event, element) {
    selectBrowseStackMember(/** @type {MouseEvent} */ (event), Number(element.dataset.id), Number(element.dataset.coverId));
  },
  openBrowseStackMember: function(event, element) {
    openBrowseStackMember(/** @type {MouseEvent} */ (event), Number(element.dataset.id), Number(element.dataset.coverId));
  },
  filterToBrowseGroup: function(event, element) { filterToBrowseGroup(Number(element.dataset.coverId)); },
  selectBrowseStackAll: function(event, element) {
    selectBrowseStackAll(/** @type {MouseEvent} */ (event), Number(element.dataset.coverId));
  },
  reviewBrowseStack: function(event, element) {
    reviewBrowseStack(/** @type {MouseEvent} */ (event), Number(element.dataset.coverId));
  },
  // Batch bar
  batchSetRating: function(event, element) { batchSetRating(Number(arg(element))); },
  batchSetFlag: function(event, element) { batchSetFlag(/** @type {'flagged' | 'rejected'} */ (arg(element))); },
  batchAddKeyword: function(event, element) { batchAddKeyword(element); },
  addToCollection: function(event, element) { addToCollection(element); },
  openBatchMoreMenu: function(event, element) { openBatchMoreMenu(element); },
  openBrowseCompare: function() { openBrowseCompare(); },
  openSelectedInBurstReview: function() { openSelectedInBurstReview(); },
  openExportModal: function() { openExportModal(); },
  batchDelete: function() { batchDelete(); },
  clearSelection: function() { clearSelection(); },
  // Detail panel
  closeDetail: function() { closeDetail(); },
  openDetailInLightbox: function() {
    var id = detailPhoto();
    var filename = /** @type {HTMLElement} */ (document.getElementById('detailFilename')).textContent || '';
    openLightbox(id, filename, browsePhotoNavigationList(id));
  },
  openDetailPipeline: function() { openPipeline(detailPhoto()); },
  findSimilarToDetail: function() { findSimilar(detailPhoto()); },
  toggleDetailPairSource: function(event) {
    event.stopPropagation();
    vireoTogglePairSource(detailPhoto());
  },
  setFlag: function(event, element) { setFlag(/** @type {'flagged' | 'rejected' | 'none'} */ (arg(element))); },
  setColorLabel: function(event, element) { setColorLabel(arg(element)); },
  toggleDetailWildlifeExcluded: function() { toggleDetailWildlifeExcluded(); },
  setDetailRating: function(event, element) {
    setRating(Number(element.dataset.photoId), Number(element.dataset.rating));
  },
  batchRate: function(event, element) { batchRate(Number(element.dataset.rating)); },
  setKeywordType: function(event, element) {
    setKeywordType(Number(element.dataset.kwId), element.dataset.kwType || '', element);
  },
  toggleTypeDropdown: function(event, element) { toggleTypeDropdown(element, Number(element.dataset.kwId)); },
  removeKeyword: function(event, element) {
    removeKeyword(Number(element.dataset.photoId), Number(element.dataset.kwId));
  },
  clearPhotoLocation: function() { clearPhotoLocation(); },
  filterMetadata: function(event, element) { filterMetadata(inputValue(element)); },
  // Collection editor: the template's root buttons carry no path; the rule
  // rows collection-editor.js renders name theirs in data-rule-path.
  addRuleRow: function(event, element) { addRuleRow(element.dataset.rulePath); },
  addRuleGroup: function(event, element) { addRuleGroup(element.dataset.rulePath); },
  updateRule: function(event, element) {
    updateRule(element.dataset.rulePath || '',
      /** @type {'mode' | 'field' | 'op' | 'value' | 'value_from' | 'value_to'} */ (element.dataset.ruleKey),
      inputValue(element));
  },
  commitRuleOnEnter: function(event) { commitRuleOnEnter(/** @type {KeyboardEvent} */ (event)); },
  removeRule: function(event, element) { removeRule(element.dataset.rulePath || ''); },
  saveCollection: function() { saveCollection(); },
  hideCollectionModal: function() { hideCollectionModal(); },
  // Best batch
  openBestBatchInReview: function() { openBestBatchInReview(); },
  applyBestBatchPickOnly: function() { applyBestBatchPickOnly(); },
  applyBestBatchPickAndReject: function() { applyBestBatchPickAndReject(); },
  hideBestBatch: function() { hideBestBatch(); },
  pickBatchCollection: function(event, element) { pickBatchCollection(Number(element.dataset.id)); },
  // Batch keyword and collection dialogs
  confirmBatchKeyword: function() { confirmBatchKeyword(); },
  hideBatchKeywordModal: function() { hideBatchKeywordModal(); },
  confirmBatchCollectionOnEnter: function(event) {
    if (/** @type {KeyboardEvent} */ (event).key === 'Enter') confirmBatchCollection();
  },
  confirmBatchCollection: function() { confirmBatchCollection(); },
  hideBatchCollectionModal: function() { hideBatchCollectionModal(); },
  // Export
  browseForExportDestination: function() { browseForExportDestination(); },
  insertTemplateVar: function(event, element) { insertTemplateVar(arg(element)); },
  startExport: function() { startExport(); },
  closeExportModal: function() { closeExportModal(); },
  // Capture time
  syncCaptureTimeFields: function() {
    syncCaptureTimeFieldState();
    updateCaptureTimePreview();
  },
  updateCaptureTimePreview: function() { updateCaptureTimePreview(); },
  startCaptureTimeJob: function() { startCaptureTimeJob(); },
  hideCaptureTimeModal: function() { hideCaptureTimeModal(); },
  // _panorama_dialog.html
  openPanoramaFolderBrowser: function() { exportFolderBrowser.open('panorama'); },
  startPanorama: function() { startPanorama(); },
  closePanoramaModal: function() { closePanoramaModal(); },
};

/** The handler names the markup may use; vireo/tests/test_browse_modules.py checks the templates against it. */
export const BROWSE_HANDLER_NAMES = Object.freeze(Object.keys(HANDLERS).sort());

/** Elements already bound, so binding a container again skips them. */
const bound = new WeakSet();

/**
 * Attach the data-browse-on handlers of ``root`` and everything under it.
 * boot.js binds the document once; code that renders markup into a
 * container binds that container afterwards. An element is bound once, so
 * binding a container again only reaches what is new in it.
 * @param {ParentNode} [root]
 */
export function bindBrowseMarkup(root = document) {
  /** @type {HTMLElement[]} */
  var elements = Array.from(root.querySelectorAll('[data-browse-on]'));
  if (root instanceof HTMLElement && root.dataset.browseOn) elements.unshift(root);
  elements.forEach(function(element) {
    if (bound.has(element)) return;
    bound.add(element);
    for (const binding of (element.dataset.browseOn || '').split(/\s+/).filter(Boolean)) {
      const [type, name] = binding.split(':');
      const handler = name ? HANDLERS[name] : undefined;
      if (!type || !handler) throw new Error('Unknown browse handler: ' + binding);
      element.addEventListener(type, function(event) { handler(event, element); });
    }
  });
}
