// The lightbox state more than one lightbox module writes. An ES module cannot
// assign another module's variable, so these live on one exported object that
// every module writes through (`lbState.readOnly = true`). State a single
// module writes stays a plain variable in that module, exported read-only.
// This module imports nothing, so it is always evaluated first.

export const lbState = {
  // The wrap's click-to-zoom latch: a mouseup that zoomed out sets it so the
  // click that follows does not zoom straight back in.
  /** @type {boolean} */
  zoomHandled: false,
  // The Keymap Esc token while the lightbox is open; non-null means open.
  /** @type {number | null} */
  escToken: null,
  // The list of {id, filename} arrow navigation walks.
  /** @type {LightboxPhoto[]} */
  photoList: [],
  readOnly: false,
  readOnlyMessage: 'This lightbox is read-only',
  // The original photo's size in pixels and its EXIF orientation, when the
  // API metadata has them.
  /** @type {number | null} */
  photoW: null,
  /** @type {number | null} */
  photoH: null,
  /** @type {unknown} */
  photoOrientation: null,
  // The active non-destructive recipe, for layout math.
  /** @type {LightboxRecipe | null} */
  currentEditRecipe: null,
  // The source tier on screen: 'full' | '2560' | '3840' | 'original'.
  /** @type {string | null} */
  currentSrcKey: null,
  // The actual long edge of /full for the current photo (below 1920 when
  // preview_max_size is configured low).
  /** @type {number | null} */
  fullLongEdge: null,
  // Bootstrapped before any photo opens; metadata refreshes the workspace cap
  // later. 0 means original.
  previewMaxSize: window.VIREO_FULL_PREVIEW_MAX_SIZE ?? null,
  // True after /original fails: fall back to the decoded source's dimensions.
  originalUnavailable: false,
  // From metadata: preview_max_size=0 makes /full redirect to /original.
  /** @type {boolean | null} */
  fullUsesOriginal: null,
  currentWildlifeExcluded: false,
  // Bumped by local flag writes so a stale metadata fetch cannot overwrite the
  // flag chip.
  flagEditSeq: 0,
  flagPendingWrites: 0,
  // Keeps the outgoing bitmap and transform frozen until the incoming image
  // has decoded.
  visualTransitionPending: false,
  // The detections/eye render withheld while visualTransitionPending, drained
  // when the transition clears.
  /** @type {(() => void) | null} */
  deferredOverlayApply: null,
  previewLoading: false,
  /** @type {string | null} */
  progressiveTargetKey: null,
  /** @type {boolean | null} */
  sessionFullUsesOriginal: null,
  editRecipeWriteSeq: 0,
  /** @type {LightboxRecipe | null} */
  editRecipe: null,
  editRecipeLoaded: false,
  /** @type {LightboxAdjustmentSource | null} */
  adjustmentSource: null,
  /** @type {number | null} */
  adjustSaveTimer: null,
  adjustSeq: 0,
  /** @type {string | null} */
  desiredSrcKey: null,
  // This photo's first bitmap has not decoded yet.
  initialDecodePending: false,
  // Above 0, the detail-status chip does not repaint mid-transition.
  detailStatusHold: 0,
};
