// The page hooks a page registers with Vireo.registerPageHooks (see
// vireo/static/vireo-page-hooks.js) for the shared navbar and lightbox to call.
// Every hook is optional: shared code asks for one with Vireo.pageHook(name)
// and falls back (or reports the command as unavailable) when it is null.

interface VireoHookPhoto {
  id: number;
  filename?: string;
}

type VireoHookFlag = 'flagged' | 'rejected' | 'none';
type VireoHistoryOperation = 'undo' | 'redo';

interface VireoHookHistoryStatus {
  available: boolean;
  label?: string;
  id?: number;
  description?: string;
}

interface VireoPageHooks {
  // ---- What the native menu and the lightbox act on ----
  /** The photos the page's own selection covers (grid multi-select). */
  getActiveSelection?(): number[];
  /** The photos selected in an open Group Review modal, or [] when none is. */
  getGroupModalSelection?(): number[];
  /** The single photo the page has focused, when it tracks one. */
  getSelectedPhotoId?(): number | null;
  /** The page's loaded photo list, for opening a selected photo in the lightbox. */
  getPhotos?(): VireoHookPhoto[];
  /** The photo whose detail panel is showing, whose image the lightbox refreshes after edits. */
  getDetailPhotoId?(): number | null;
  /** The page's predictions, for the native menu's Accept and Reject. */
  getPredictions?(): {id: number; status: string}[];

  // ---- Native menu photo commands ----
  /** Open the selection's shortcut photo in the lightbox; false when there is none. */
  openBrowseShortcutPhoto?(fullscreen: boolean): boolean;
  openPipelineLightbox?(photoId: number): unknown;
  /** Move the page's single selection by delta cards (native menu Previous/Next). */
  moveBrowseSelection?(delta: number): unknown;
  batchSetRating?(rating: number): unknown;
  batchSetFlag?(flag: VireoHookFlag): unknown;
  /** Rate one photo through the page's model (lightbox, native menu). */
  setRatingFor?(photoId: number, rating: number): unknown;
  setReviewRating?(photoId: number, rating: number): unknown;
  /**
   * Flag one photo through the page's model. Resolves false when the write
   * failed, or {provisional: true} when the page staged it without saving.
   */
  setFlagFor?(photoId: number, flag: VireoHookFlag): Promise<unknown>;
  setReviewFlag?(photoId: number, flag: VireoHookFlag): Promise<unknown>;
  setColorLabelFor?(photoId: number, color: string | null): unknown;
  /** Resolves false when the change was refused. */
  setWildlifeExcludedFor?(photoId: number, excluded: boolean): Promise<boolean | void>;
  openExportModal?(): unknown;
  openPhotoExportModal?(photoIds: number[]): unknown;
  revealPhoto?(photoId: number): unknown;
  revealReviewPhoto?(photoId: number): unknown;
  copyPhotoPaths?(photoIds: number[]): unknown;
  openBrowseCompare?(): unknown;
  /** Open the page's Add Keyword / Add to Collection dialog for the native menu's photos. */
  batchAddKeyword?(): unknown;
  addToCollection?(): unknown;
  openCaptureTimeModal?(): unknown;
  batchDelete?(): unknown;
  acceptPrediction?(predictionId: number): unknown;
  rejectPrediction?(predictionId: number): unknown;
  acceptAllPending?(): unknown;

  // ---- Native menu tool commands ----
  startPipeline?(): unknown;
  startScan?(): unknown;
  runSync?(): unknown;
  verifyAllModels?(): unknown;

  // ---- Lightbox integration ----
  /** The read-only state the lightbox should open the photo in. */
  getLightboxOpenOptions?(photoId: number): {readOnly?: boolean; readOnlyMessage?: string} | null;
  /** Why Open in Browse / Edit must stay disabled for the photo, or null. */
  getLightboxBrowseDisabledHint?(photoId: number | null, willNavigateInPlace?: boolean): string | null;
  /** Why opening photos in an external editor is disabled, or null. */
  getPhotoExternalEditDisabledHint?(): string | null;
  /**
   * Drop a photo the lightbox just deleted from the page's grid. Returns true
   * when the page's list is the lightbox's list, so the photo is already gone
   * from it.
   */
  removeDeletedLightboxPhoto?(photoId: number, lightboxPhotos: readonly unknown[]): boolean;
  /** Handle the lightbox's flag keys for a miss; true when it did. */
  handleMissesLightboxFlagShortcut?(photoId: number, flag: VireoHookFlag): boolean;
  /** Reload the Highlights page after a lightbox curation change. */
  loadHighlights?(): Promise<void>;
  updateHighlightsLightboxControls?(photoId: number): void;

  // ---- Undo/redo transport (navbar-shared-helpers.js) ----
  /** Return false to refuse an undo/redo the page cannot take right now. */
  beforeHistoryChange?(): unknown;
  /** Reload the page's data after an undo/redo changed the catalog. */
  afterHistoryChange?(change: unknown): unknown;
  /** Undo/redo a page-local edit instead of the catalog history; null to pass. */
  changeLocalHistory?(operation: VireoHistoryOperation): Promise<boolean> | boolean | null;
  localHistoryStatus?(operation: VireoHistoryOperation): VireoHookHistoryStatus | null;

  // ---- Missing Originals deletes (navbar-folder-banners.js) ----
  loadMisses?(): unknown;

  // ---- The export modal (vireo-export-presets.js, vireo-export-collisions.js) ----
  /** Apply a built-in preset (``original-jpg``, ...) to the modal's controls. */
  applyExportPreset?(preset: string): void;
  /** Mark the modal's settings as edited away from any preset. */
  markExportCustom?(): void;
  /** Show and hide the controls the chosen format uses. */
  updateExportFormatControls?(): void;
  /** Repaint the file-name preview. */
  updateExportPreview?(): void;
  /** The metadata fields the modal's checkboxes select. */
  selectedExportMetadataFields?(): string[];
  /** The name-deciding settings for the collision preflight, or null when the
   * controls cannot describe an export yet. */
  buildExportPreflightRequest?(): VireoExportPreflightRequest | null;

  // ---- Releasing local originals (vireo-release-originals.js) ----
  /** Reload the Storage overview after a release job frees space. */
  loadStoragePanel?(): Promise<void>;
}

interface VireoExportPreflightRequest {photo_ids: number[]; destination: string}

type VireoPageHookName = keyof VireoPageHooks;

interface VireoPageHookRegistry {
  registerPageHooks(hooks: VireoPageHooks): void;
  pageHook<K extends VireoPageHookName>(name: K): NonNullable<VireoPageHooks[K]> | null;
  pageHookNames: readonly VireoPageHookName[];
}
