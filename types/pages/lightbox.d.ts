// Shared photo and editing data used by the lightbox implementations.
interface LightboxCrop { x: number; y: number; w: number; h: number }
interface LightboxRecipe {
  version?: number;
  rotation?: number;
  straighten?: number;
  flip?: { horizontal?: boolean; vertical?: boolean };
  crop?: LightboxCrop;
  adjustments?: LightboxAdjustments;
  local?: unknown;
}
interface LightboxPhoto {
  id: number;
  filename?: string;
  width?: number | null;
  height?: number | null;
  edit_recipe?: LightboxRecipe | null;
  render_key?: string | null;
}
interface LightboxSessionToken { readonly photoId: number | null; readonly generation: number }
interface LightboxPreloadEntry {
  requestId?: number;
  img: HTMLImageElement;
  photoId: number;
  sourceKey: string;
  url: string;
  status: string;
  bytes: number;
  retrying?: boolean;
}
interface LightboxSessionView {
  currentSrcKey: string | null;
  fullUsesOriginal: boolean | null;
  originalUnavailable: boolean;
  zoom: number;
  desiredSrcKey: string | null;
  nativeZoom: number | null;
  photoW: number | null;
  photoH: number | null;
  visualTransitionPending: boolean;
}
interface LightboxSessionOptions {
  window?: Window & typeof globalThis;
  photos(): readonly LightboxPhoto[];
  photoData(id: number): LightboxPhoto | undefined;
  view(): LightboxSessionView;
  sourceUrl(id: number, key: string, speculative: boolean): string;
  sourceRank(key: string): number;
  fullPreviewLimit(): number;
  pickSourceKey(zoom: number): string;
  rememberEditRecipe(id: number, recipe: LightboxRecipe | null | undefined): void;
  rememberRenderKey(id: number, key: string | null | undefined): void;
  preloadConcurrency?: number;
  preloadBudgetBytes?: number;
}

interface LightboxApi {
  VireoLightboxSession: { create(options: LightboxSessionOptions): LightboxSession };
}
interface LightboxSession {
  begin(photoId: number): LightboxSessionToken;
  commit(token: LightboxSessionToken): boolean;
  close(): number | null;
  capture(): LightboxSessionToken;
  isCurrent(token: LightboxSessionToken): boolean;
  requestedPhotoId(): number | null;
  displayedPhotoId(): number | null;
  noteDirection(delta: number): void;
  cancelSwap(): void;
  scheduleSwap(callback: () => void, delay: number): void;
  hasScheduledSwap(): boolean;
  watchImage(img: HTMLImageElement, onload: () => void, onerror?: (() => void) | null, properties?: boolean, persistent?: boolean): () => void;
  loadSource(url: string, onload: () => void, onerror: (() => void) | null): void;
  watchInitialImage(img: HTMLImageElement, onload: () => void, onerror: () => void): () => void;
  setInitialLoad(commit: () => void, abandon: () => void): void;
  clearInitialLoad(commit?: () => void): void;
  hasInitialLoad(): boolean;
  finishInitialLoad(): boolean;
  abandonInitialLoad(): boolean;
  preloadStatus(): { readonly adjacent: readonly (LightboxPreloadSnapshot | null)[]; readonly original: LightboxPreloadSnapshot | null; readonly inFlight: LightboxPreloadSnapshot | null; readonly activeCount: number; readonly bytes: number; readonly budgetBytes: number; readonly retryCount: number; readonly exhaustedRetries: number; readonly adjacentScheduled: boolean; readonly originalScheduled: boolean; readonly originalWaiting: boolean };
  hasDecodedSource(photoId: number, key: string, url: string): boolean;
  decodedPreview(photoId: number, desired: string): LightboxPreloadSnapshot | null;
  cancelAdjacent(): void;
  clearAdjacent(): void;
  cancelOriginal(): void;
  scheduleOriginal(photoId: number, retryCount?: number): void;
  scheduleAdjacent(sourceKey: string | null, delay?: number): void;
}
type LightboxPreloadSnapshot = Readonly<Pick<LightboxPreloadEntry, 'requestId' | 'photoId' | 'sourceKey' | 'url' | 'status' | 'bytes'>>;
interface LightboxPoint { x: number; y: number }
interface LightboxEyeAnchor { offsetX: number; offsetY: number }
interface LightboxViewportState { zoom: number; centerX: number; centerY: number; oneToOne: boolean; pending1To1: boolean }
interface LightboxOpenOptions { fallbackViewportState?: LightboxViewportState | null; eyeTrackAnchor?: LightboxEyeAnchor | null; preserveOneToOne?: boolean; readOnly?: boolean; readOnlyMessage?: string }
interface LightboxViewportPhoto {
  width: number | null; height: number | null; orientation: unknown;
  recipe: LightboxRecipe | null; originalUnavailable: boolean;
  currentSrcKey: string | null; desiredSrcKey: string | null;
  transitionPending: boolean; trackEyeEnabled: boolean;
  pairKnown: boolean; pairSource: string | null;
}
interface LightboxViewportOptions {
  window?: Window & typeof globalThis;
  photoId(): number | null;
  photo(): LightboxViewportPhoto;
  orientationSwapsAxes(orientation: unknown): boolean;
  pickSource(zoom: number): string;
  scheduleSource(zoom?: number): void;
  cancelSourceSwap(): void;
  keepSource(key: string | null): void;
  scheduleAdjacent(key: string | null): void;
  photoData(id: number | null): LightboxPhoto | null | undefined;
  eyePoint(id: number | null, photo: LightboxPhoto | null | undefined): LightboxPoint | null;
  overlaysAvailable(): boolean;
  updateEyeControl(): void;
  cancelProgressiveLoad(): void;
  handledClick(): void;
}
interface LightboxViewportMetrics { w: number; h: number; wrapW: number; wrapH: number; scale: number }
interface LightboxViewport {
  beginPhoto(photoId: number, options?: LightboxOpenOptions): Readonly<{zoom: number; oneToOne: boolean}>;
  close(): void;
  snapshot(): Readonly<{zoom: number; panX: number; panY: number; nativeZoom: number | null; fitScale: number; pendingOneToOne: boolean; oneToOneAnchor: Readonly<LightboxPoint> | null; pendingRestore: Readonly<LightboxViewportState> | null; pendingEye: Readonly<LightboxEyeAnchor & {photoId: number}> | null; eyeAnchor: Readonly<LightboxEyeAnchor> | null; active: boolean}>;
  resetForEdit(): void;
  cancelPendingZoom(): void;
  clearEyeTracking(): void;
  flushDeferredLayout(): void;
  invalidateGeometry(): void;
  zoom(): number;
  nativeZoom(): number | null;
  fitScale(): number;
  pendingOneToOne(): boolean;
  hasPendingRestore(): boolean;
  savedView(id: number): Readonly<LightboxViewportState> | null;
  applyTransform(): void;
  layoutDims(): {w: number | null; h: number | null} | null;
  layoutMetrics(): LightboxViewportMetrics | null;
  recomputeNativeZoom(): void;
  setZoom(zoom: number, x?: number | null, y?: number | null, preserveSharper?: boolean): void;
  isOneToOne(): boolean;
  currentView(): LightboxViewportState;
  captureEyeAnchor(): LightboxEyeAnchor | null;
  applyPendingEye(photo?: LightboxPhoto | null): boolean;
  save(photoId: number | null): LightboxViewportState | null;
  applyView(state: LightboxViewportState | null): boolean;
  cancelRestore(): void;
  applyPendingRestore(): boolean;
  applyPendingOneToOne(): void;
  deferOneToOne(zoom: number): boolean;
  deferOneToOneFallback(): boolean;
  updateControls(): void;
  setPopoverOpen(open: boolean): void;
  togglePopover(): void;
  setFromSlider(position: string | number): void;
  stepZoom(direction: number): void;
  fit(): void;
  oneToOne(event?: Pick<MouseEvent, 'clientX' | 'clientY'>): void;
  toggleZoom(event?: Pick<MouseEvent, 'clientX' | 'clientY'>): void;
}
interface LightboxApi { VireoLightboxViewport: { create(options: LightboxViewportOptions): LightboxViewport } }

type LightboxFlag = 'none' | 'flagged' | 'rejected';
interface LightboxPhoto {
  flag?: LightboxFlag;
  photo_id?: number;
  extension?: string;
  companion_path?: string | null;
  eye_x?: number | null; eye_y?: number | null; eye_conf?: number | null; eye_tenengrad?: number | null;
  metadata?: LightboxMetadata | null;
}
interface LightboxMetadata { Orientation?: number | string; orientation?: number | string; [tag: string]: string | number | boolean | null | undefined }
interface Window {
  VIREO_FULL_PREVIEW_MAX_SIZE?: number;
}
interface LightboxApi {
  setLightboxReadOnlyMode(readOnly: boolean, message?: string): void;
  setLightboxProvisionalFlag(photoId: number, flag: LightboxFlag, editSeq?: number): void;
  clearLightboxProvisionalFlags(photoIds?: number[]): void;
}
type LightboxPhotoId = number | string;
interface LightboxAdjustments {
  exposure?: number; highlights?: number; shadows?: number; whites?: number;
  blacks?: number; contrast?: number; temperature?: number; tint?: number;
  vibrance?: number; saturation?: number;
  white_balance?: {temperature?: number; tint?: number};
}
type LightboxAdjustmentValues = Required<Omit<LightboxAdjustments, 'white_balance'>>;
interface LightboxAdjustmentSource {url: string; image: HTMLImageElement; ready: Promise<boolean>}
interface LightboxCropDrag {handle: string; startX: number; startY: number; startCrop: LightboxCrop; imgW: number; imgH: number}
interface LightboxApi {
  vireoPhotoIsRawJpegPair(photo: LightboxPhoto | null | undefined): boolean;
  vireoRememberPhotoPair(photo: LightboxPhoto | null | undefined): void;
  vireoUpdatePairSourceControls(photoId: LightboxPhotoId | null): void;
  vireoTogglePairSource(photoId: number): void;
  vireoRefreshPhotoRenders(photoIds: LightboxPhotoId[] | LightboxPhotoId): void;
  vireoRenderedUrl(url: string, photoId: LightboxPhotoId | null, sourceOverride?: string): string;
  vireoThumbnailUrl(photoOrId: LightboxPhoto | LightboxPhotoId | null): string;
  vireoPhotoHasOrientationEdit(photoId: LightboxPhotoId | null): boolean;
  vireoRememberPhotoEditRecipe(photoId: LightboxPhotoId | null, recipe: LightboxRecipe | null | undefined, options?: {skipIfLocallyWritten: boolean}): void;
  vireoRememberPhotoRenderKey(photoId: LightboxPhotoId | null, key: string | null | undefined, options?: {skipIfLocallyWritten: boolean}): void;
}
interface LightboxMaskVariant { variant: string; url: string; created_at?: string }
interface LightboxDetection { detector_model?: string; box_x: number | null; box_y: number | null; box_w: number; box_h: number; category?: string; detector_confidence?: number }
interface LightboxCurationEntry { species: string; is_current_photo?: boolean; is_species_representative?: boolean; is_highlighted?: boolean }
interface LightboxPhoto { life_list?: LightboxCurationEntry[]; highlight_list?: LightboxCurationEntry[]; species?: string | string[] }
interface LightboxCurationOptions {getPhoto?: (id: number) => LightboxPhoto | null | undefined; photoById?: Record<string, LightboxPhoto>; showFetchFallback?: boolean}
interface LightboxMenuItem {label?: string; title?: string; separator?: boolean; disabled?: boolean; disabledHint?: string | null; onClick?: () => unknown; chips?: LightboxMenuItem[]; color?: string | null; colorBaseTitle?: string}
interface LightboxApi {
  buildSpeciesRepresentativeMenuItems(ids: number[], opts?: LightboxCurationOptions): LightboxMenuItem[];
  buildSpeciesHighlightMenuItems(ids: number[], opts?: LightboxCurationOptions): LightboxMenuItem[];
}
interface LightboxPhoto { keywords?: {name: string; type?: string}[]; wildlife_excluded?: boolean | number; full_uses_original?: boolean; full_preview_max_size?: number }

interface Document { webkitFullscreenElement?: Element | null; webkitExitFullscreen?: () => void | Promise<void> }
interface HTMLElement { webkitRequestFullscreen?: () => void | Promise<void> }
interface LightboxDeleteResult {deleted: number; trashed: number; failed_photo_ids?: number[]; trash_failed?: {path: string}[]}
interface LightboxDeleteProgress {phase?: string; total?: number; current?: number; failed?: number; stage_failures?: Record<string, number>; current_file?: string; detail?: string}
interface LightboxDeleteCompletion {status?: string; result?: LightboxDeleteResult; errors?: string[]}
interface LightboxJobStreamOptions<P, R> {onProgress?: (data: P) => void; onComplete?: (data: R) => void; onError?: () => void}
declare function openExternal(url: string): Promise<boolean>;
declare function showExternalOpenFailure(url: string, message: string): void;
declare function copyExternalUrl(url: string): Promise<boolean>;
declare function pickDirectory(title: string): Promise<string | null>;
/** One species on the photo (``/api/inat/prepare``): confirmed keywords first, then each detection's top prediction. */
interface InatSpeciesOption {
  species: string; scientific_name: string; source: 'confirmed' | 'prediction';
  confidence: number | null; primary: boolean;
}
interface InatPreparedPhoto {
  error?: string; mode?: string; scientific_name?: string; species?: string;
  species_options?: InatSpeciesOption[];
  timestamp?: string; latitude?: number | null; longitude?: number | null;
  filename: string; edit_recipe?: LightboxRecipe | null; already_submitted?: boolean;
  existing_observation_url?: string; upload_url?: string;
}
interface InatQueueItem {
  photo_id: number; taxon_name: string; observed_on: string; species_options?: InatSpeciesOption[];
  latitude: number | string; longitude: number | string;
  description: string; geoprivacy: string; filename: string;
  edit_recipe?: LightboxRecipe | null; already_submitted?: boolean;
  existing_url?: string; upload_url?: string;
}
interface InatPreparationFailure {photo_id: number; error: string}
interface InatExportProgress {current?: number; total?: number}
interface InatExportCompletion {status?: string; result?: {exported?: unknown[]; errors?: {error: string}[]; revealed?: boolean}}
