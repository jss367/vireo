interface TextSearchOptions { matchCase?: boolean; wholeWord?: boolean; }
type PipelineDefaults = Record<string, string | number | boolean | null | undefined>;

interface VireoDom {
  setStatus(element: Element | null | undefined, state: string, message?: string): void;
  clear(element: Element | null | undefined): void;
}
interface VireoNamespace { dom?: VireoDom; }
interface Window { VIREO_CONFIG_DEFAULTS?: {pipeline: PipelineDefaults}; }

interface VireoApi {
  fetch: typeof fetch;
  nativeFetch: typeof fetch;
  json<T = unknown>(input: RequestInfo | URL, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  searchLaneHeaders(loader: string, seq: number): Record<string, string>;
  trackHistoryRequest?(input: RequestInfo | URL, init: RequestInit | undefined,
    send: () => Promise<Response>): Promise<Response>;
}
interface VireoNamespace { api?: VireoApi; }

interface MapSelectionSummary {
  selected: number; hidden_by_filters: number; without_location: number;
  folder_unavailable: number; not_in_workspace: number; shown: number;
}
interface MapGeoResponse {
  photos: {id: number; latitude: number; longitude: number}[];
  selection?: MapSelectionSummary;
  total_without_coordinates: number; total_photos: number; total_geolocated: number;
  truncated: boolean; total_filtered: number; render_limit: number;
}
interface DuplicateMember {
  path?: string; filename?: string; file_size?: number;
}
interface DuplicateCategoryGroup {
  winner: DuplicateMember; other_kept?: DuplicateMember[]; losers?: DuplicateMember[];
  empty_file_group?: boolean;
}
interface DuplicateCategorySummary {
  id: string; label: string; groups: number; extras: number; bytes: number;
}
interface Window { VIREO_RSYNC_INSTALL?: {hint?: string}; }

interface KeymapShortcut { key: string; action(event: KeyboardEvent): unknown; }
interface KeymapModifiers { key: string; ctrl: boolean; meta: boolean; shift: boolean; alt: boolean; }
interface VireoKeymap {
  parseShortcut(shortcut: string): KeymapModifiers;
  matchesShortcut(event: KeyboardEvent, shortcut: string): boolean;
  isInputFocused(): boolean;
  register(scope: string, shortcut: KeymapShortcut): void;
  shortcutsForScope(scope: string): KeymapShortcut[];
  setScope(scope: string): void; getScope(): string;
  pushEsc(handler: (event: KeyboardEvent) => void): number;
  popEsc(token: number | null | undefined): boolean;
  lockBodyScroll(): void; unlockBodyScroll(): void;
  pauseDispatch(handler?: (event: KeyboardEvent) => void): void;
  resumeDispatch(): void; isDispatchPaused(): boolean;
  captureNativeShortcut(shortcut: string): boolean;
}
type SearchRule = {field: string; op: string; value: string} |
  {mode: string; rules: SearchRule[]};
interface Window {
  VireoSearch?: {parse(text: string, options?: {field?: string}): SearchRule};
  __TAURI_INTERNALS__?: {invoke<T = unknown>(command: string, args?: Record<string, unknown>): Promise<T>};
  VireoPageFind?: {open(): void; close(): void};
}
declare var VireoSearch: Window['VireoSearch'];
declare const module: {exports: unknown} | undefined;

interface WorkspaceSummary {id: number; name: string; pinned_at?: string | null;}
interface WorkspaceSwitcherOptions {
  window?: Window & typeof globalThis;
  fetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  navigate(path: string): void;
  clearWorkspaceCursors(): void;
}
interface WorkspaceSwitcher {
  loadCurrentName(): Promise<void>;
  toggle(): Promise<void> | undefined;
  close(): void;
  showCreate(): void;
  hideCreate(): void;
  createWorkspace(): Promise<void>;
}
interface Window {VireoWorkspaceSwitcher: {create(options: WorkspaceSwitcherOptions): WorkspaceSwitcher};}

interface FolderBrowserModeBase {
  title?: string | (() => string);
  startPath?: string | (() => string);
  showCounts?: boolean;
  fileTypes?: string | string[] | (() => string | string[]);
}
type FolderBrowserMode = FolderBrowserModeBase & (
  {multiple: true; onSelect?(paths: string[]): void} |
  {multiple?: false; onSelect?(path: string): void}
);
interface FolderBrowserOptions {
  overlayId: string; defaultMode?: string; modes: Record<string, FolderBrowserMode>;
  onError?(message: string): void;
}
interface FolderBrowserDirectory {path: string; name?: string;}
interface FolderBrowserListing {path: string; dirs: FolderBrowserDirectory[];}
interface FolderBrowserCount {count: number; incomplete?: boolean; skipped?: boolean;}
interface Window {VireoFolderBrowser: new(options: FolderBrowserOptions) => {
  readonly sequence: number;
  open(mode: string, options?: {skipInitialBrowse?: boolean}): void;
  close(): void; browse(path: string | null): Promise<void>; confirm(): void;
};}

interface ExportPreflightRequest {photo_ids: number[]; destination: string;}
interface ExportPreflightResult {
  rename_count: number;
  renames?: {requested_name: string; export_name: string}[];
  destination_folders?: string[]; destination_folder_count?: number;
  error?: string;
}

interface PendingArchive {
  id: string | number; name: string; destination: string; collection_id?: number | null;
  state: string; unsynced_photos?: number; unsynced_photos_other_workspaces?: number;
  unsynced_photos_here_with_sibling_edits?: number; source_available?: boolean; error?: string;
}
interface Window {vireoPendingArchives?: PendingArchive[];}

interface VireoColorLabelController {
  ensureLoaded(): Promise<Record<string, unknown>>;
  isValid(color: string): boolean;
  description(color: string): string;
  title(color: string, baseTitle?: string): string;
  refreshControls(root?: ParentNode): void;
  openEditor(color: string): Promise<void>;
  closeEditor(): void; saveEditor(): Promise<void>;
}
interface Window {VireoColorLabels: VireoColorLabelController; closeContextMenu?(): void;}

type ComparePane = 'A' | 'B';
interface ComparePhoto {
  id: number; filename?: string; width?: number; height?: number;
  timestamp?: string; rating?: number; sharpness?: number; flag?: string;
}
interface ComparePointer {
  pointerId: number; prefix: ComparePane; startX: number; startY: number;
  panX: number; panY: number;
}
interface CompareOptions {
  window?: Pick<Window & typeof globalThis, 'document' | 'Image' | 'addEventListener' | 'removeEventListener' | 'Keymap'>;
  keymap?: Pick<VireoKeymap, 'pushEsc' | 'popEsc' | 'lockBodyScroll' | 'unlockBodyScroll'>;
  syncZoom?: boolean;
  findPhoto(id: number): ComparePhoto | null | undefined;
  fetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast(message: string, type?: string): void;
}
interface CompareController {
  open(ids: number[]): Promise<void> | undefined;
  close(event?: Event): void; isOpen(): boolean;
  step(delta: number): Promise<void> | undefined;
  resetViews(): void; destroy(): void;
}
type CompareControls = Pick<CompareController, 'close' | 'resetViews' | 'step'>;
interface Window {VireoBrowseCompare: {
  create(options: CompareOptions): CompareController;
  bindControls(document: Document, getCompare: () => CompareControls | null | undefined): void;
};}

interface FindTextRun {text: string; parts: {node: Text; start: number; end: number}[];}
interface FindTextSegment {start: number; end: number; group: Element[];}
interface Window {openSettingsFind?(): void; handleNativeMenuCommand(command: string): unknown;}

interface PanoramaDone {
  result?: {path: string; width: number; height: number}; status?: string; errors?: string[];
}
interface ReleaseScope {all_cached?: boolean; photo_ids?: number[]; folder_id?: number;}
interface ReleaseResult {
  released?: number; bytes?: number; kept?: number; not_cached?: number; failed?: number;
  total: number; processed: number; kept_reasons?: string[];
}
interface Window {
  _vireoNativeMenuPhotoIdsOverride?: number[] | null;
  VireoReleaseOriginals: {release(scope: ReleaseScope, description: string): Promise<void>;
    storageButton(): string; isRunning(): boolean};
}

// _sync_panel.html's inline sync, registered as a page hook by sync-panel-page-hooks.js.
declare function runSync(changeIds?: number[]): Promise<void>;
