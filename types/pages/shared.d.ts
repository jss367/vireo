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
  popEsc(token: number): boolean;
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
