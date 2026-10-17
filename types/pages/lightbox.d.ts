// Shared photo and editing data used by the lightbox implementations.
interface LightboxCrop { x: number; y: number; w: number; h: number }
interface LightboxRecipe {
  version?: number;
  rotation?: number;
  straighten?: number;
  flip?: { horizontal?: boolean; vertical?: boolean };
  crop?: LightboxCrop;
  adjustments?: Record<string, number>;
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

interface Window {
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
  watchImage(img: HTMLImageElement, onload: () => void, onerror: (() => void) | null, properties: boolean, persistent?: boolean): () => void;
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
  width: number | null; height: number | null; orientation: number | null;
  recipe: LightboxRecipe | null; originalUnavailable: boolean;
  currentSrcKey: string | null; desiredSrcKey: string | null;
  transitionPending: boolean; trackEyeEnabled: boolean;
  pairKnown: boolean; pairSource: string;
}
interface LightboxViewportOptions {
  window?: Window & typeof globalThis;
  photoId(): number | null;
  photo(): LightboxViewportPhoto;
  orientationSwapsAxes(orientation: number | null): boolean;
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
interface Window { VireoLightboxViewport: { create(options: LightboxViewportOptions): LightboxViewport } }
