// Data consumed by the navbar's classic scripts. Page action functions are
// external: each page's independent project checks its own implementations.
interface NavPage {
  id: string;
  label: string;
  href: string;
  keywords?: string;
}
interface NavTabState {
  tabs: string[];
  all_pages: NavPage[];
}
interface NavTabs {
  fetchAndRender(): Promise<NavTabState | void>;
  getTabs(): string[];
  getAllPages(): NavPage[];
  setTabs(order: string[]): Promise<NavTabState | void>;
}
// The navbar's photos are the lightbox's (types/pages/lightbox.d.ts): the
// navbar hands them to openLightbox and the render helpers.
interface NavPhoto extends LightboxPhoto {
  timestamp?: string;
  path?: string;
  eye_tenengrad?: number | null;
}
type NavRecipe = LightboxRecipe;
interface NavRecipeClipboard {
  recipe: NavRecipe;
  source?: string | null;
  at?: number | string | null;
}
interface NavEditorNavigation {
  setLastPhoto(id: number): void;
  getLastPhoto(): number | null;
  setList(photos: (NavPhoto | number)[], currentId?: number | null): void;
  getList(): number[] | null;
  setCopiedRecipe(recipe: NavRecipe | null, meta?: {source?: string; at?: number | string}): Promise<void>;
  beginCopiedRecipe(): Promise<string>;
  isCopiedRecipeCurrent(token: string): boolean;
  setCopiedRecipeIfCurrent(recipe: NavRecipe | null, meta: {source?: string; at?: number | string} | null | undefined, token: string): Promise<boolean>;
  cancelCopiedRecipe(token: string): Promise<boolean>;
  getCopiedRecipe(): NavRecipeClipboard | null;
  clearWorkspaceScopedCursors(): void;
}
interface NavMenuItem {
  label?: string;
  title?: string;
  separator?: boolean;
  disabled?: boolean;
  disabledHint?: string | null;
  onClick?: () => unknown;
  chips?: NavMenuItem[];
  active?: boolean;
  color?: string | null;
  colorBaseTitle?: string;
  danger?: boolean;
  shortcut?: string;
}
interface NavHelpItem {
  question: string;
  answer: string;
  keywords: string;
  category: string;
}
interface NavFuseResult<T> {
  item: T;
  score?: number;
}
declare class Fuse<T> {constructor(items: T[], options: {keys: {name: string; weight: number}[]; threshold: number; includeMatches?: boolean; ignoreLocation?: boolean}); search(query: string): NavFuseResult<T>[];}
interface NavProgress {
  current?: number;
  total?: number;
  phase?: string;
  phase_current?: number;
  phase_total?: number;
  phase_label?: string;
  current_file?: string;
  rate?: number;
}
interface NavJob {
  id: string;
  type: string;
  status: string;
  counts_for_badge?: boolean;
  workspace_id?: number | null;
  workspace_name?: string;
  started_at?: string;
  finished_at?: string;
  created_at?: string;
  progress?: NavProgress | null;
  errors?: string[];
  result?: Record<string,unknown> | null;
  config?: Record<string,unknown>;
  runtime_warning?: NavRuntimeWarning | null;
}
interface NavJobsPayload {
  active?: NavJob[];
  jobs?: NavJob[];
  active_workspace_id?: number | null;
  runtime_warning?: NavRuntimeWarning | null;
}
interface NavRuntimeWarning {
  title?: string;
  detail?: string;
  next_action?: string;
  id: string;
  message: string;
}
interface NavLogRecord {
  time: number;
  level: string;
  message: string;
}
interface NavRevealResult {
  ok?: boolean;
  reason?: string;
}
interface NavExternalEditor {
  name: string;
  path?: string;
  id?: string;
}
interface NavHistoryStatus {
  available: boolean;
  label?: string;
  id?: number;
  description?: string;
}
interface NavHistoryChange {
  photo_ids?: number[];
  entry?: {photo_ids?: number[];};
  ok?: boolean;
  action_type?: string;
  undone?: string;
  redone?: string;
  edit_recipes?: Record<string, NavRecipe | null>;
}
interface NavLightboxToken {
  readonly photoId: number | null;
  readonly generation: number;
}
interface NavSubject {
  id: number;
  is_primary?: boolean;
  box_x: number;
  box_y: number;
  predictions: {species: string; confidence: number}[];
  analysis?: {source_key?: string; crop: NavBox; quality_score: number; exposure_ev: number;};
}
interface NavSubjectData {
  primary_detection_id?: number | null;
  photo_id: number;
  subjects: NavSubject[];
  selection: string;
  choice_unavailable?: boolean;
}
// Real window properties the navbar reads: help.js and vendor globals, and
// server config from /config-defaults.js.
interface Window {
  _helpEscToken?: number | null;
  openHelpModal(): void;
  closeHelpModal(): void;
  _helpSearchRefresh?: () => void;
  Fuse?: typeof Fuse;
  VIREO_FILE_MANAGER_NAME?: string;
  VIREO_REVEAL_LABEL?: string;
  VireoTextSearch?: typeof VireoTextSearch;
}
// The navbar modules' exports, typed once here and referenced with
// `@type {NavbarApi['name']}` where a declaration takes its signature.
interface NavbarApi {
  NAV_ALL_PAGES: NavPage[];
  NAV_DEFAULT_TABS: string[];
  _navTabs?: NavTabs;
  vireoNumberedNavigationMode: string;
  vireoNumberedNavigationPages(): (NavPage | null)[];
  vireoResolveNavigationHref(path: string): string;
  vireoRefreshNavigationHrefs(): void;
  vireoSyncNumberedNavigation(): Promise<void> | undefined;
  vireoSetNumberedNavigationMode(mode: string): Promise<void> | undefined;
  openCommandPalette(): void;
  closeCommandPalette(): void;
  pinTab(navId: string): Promise<NavTabState | void>;
  toggleOverflowMenu(event?: Event): void;
  _applyNavHotkeyHints?: () => void;
  openShortcutsSheet(): void;
  closeShortcutsSheet(): void;
  vireoEditNav: NavEditorNavigation;
  vireoWorkspaceSwitcher: WorkspaceSwitcher;
  openContextMenu(event: MouseEvent, items: NavMenuItem[], options?: {onDismiss?: () => void}): void;
  openInBrowse?: (photoId: number) => unknown;
  invalidateEditorsCache(): void;
  buildOpenInEditorMenuItems(photoIds: number[]): NavMenuItem[];
  handleNativeMenuCommand(command: string): Promise<void>;
  toggleBottomPanel(): void;
  switchBpTab(tab: string): void;
  formatJobType(type?: string): string;
  addLpLine(record: NavLogRecord, bulk?: boolean): void;
  lpApplyFilter(): void;
  getThemeList(): {id: string; name: string; icon?: string}[];
  setTheme(themeId: string): void;
  toggleTheme(): void;
  isAdvancedMode(): boolean;
  toggleAdvancedMode(): void;
  dismissRuntimeWarning(): void;
  refreshHistoryControls(): void;
  renderHistoryControls(): void;
  doUndo(button?: HTMLButtonElement): Promise<void | boolean>;
  doRedo(button?: HTMLButtonElement): Promise<void | boolean>;
  vireoHistoryBusy(): boolean;
  vireoRefreshPhotoRenders?: (photoIds: number[]) => void;
  vireoThumbnailUrl?: (photo: NavPhoto | number) => string;
  coerceSelectionOnContext?: (selection: Set<number>, photoId: number | null) => number[];
  markMissingFoldersReconciliationPending?: () => void;
}
declare var Keymap: VireoKeymap;
declare var VireoWorkspaceSwitcher: Window['VireoWorkspaceSwitcher'];
declare var vireoWorkspaceSwitcher: WorkspaceSwitcher;
declare var VireoViewPreferences: {restoreAll(root?: ParentNode): void; set(name: string, value: string | number): void; get(name: string, fallback?: string | number): string | number;};
declare var VireoTextSearch: {readOptions(namespace: string): TextSearchOptions; matchesFields(fields: unknown, query: string, options?: TextSearchOptions): boolean; toggle(namespace: string, option: string, callback?: (options: TextSearchOptions) => void): void;};

declare function escapeHtml(value: unknown): string;
declare function escapeAttr(value: unknown): string;
declare function formatBytes(value: number): string;
declare function isTauri(): boolean;
declare function openExternalWithRecovery(url: string): unknown;
declare function formatShortcut(value: string): string;
declare function parseShortcut(value: string): KeymapModifiers;
declare function isAdvancedMode(): boolean;
declare function closeContextMenu(): void;
declare function openContextMenu(event: MouseEvent, items: NavMenuItem[], options?: {onDismiss?: () => void}): void;
declare function openHelpModal(): void;
declare function openShortcutsSheet(): void;
declare function closeShortcutsSheet(): void;
declare function switchBpTab(tab: string): void;
declare function addLpLine(record: NavLogRecord, bulk?: boolean): void;
declare function lpApplyFilter(): void;
declare var vireoLightboxSession: {requestedPhotoId(): number | null; capture(): NavLightboxToken; isCurrent(token: NavLightboxToken): boolean;};
declare var _lbVisualTransitionPending: boolean;
declare var _lbCurrentWildlifeExcluded: boolean;
declare var _lbReadOnly: boolean;
declare var _lbReadOnlyMessage: string;
declare var _lbPhotoDataByPhoto: Record<number,NavPhoto>;
declare var _lbEditWritePending: boolean;
declare var _lbFlagEditSeq: number;
declare function _lbApplyFlag(photoId: number, flag: string): void;
declare function _lbGuardReadOnly(): boolean;
declare function _lbFlushPendingAdjustmentSave(): Promise<void>;
declare function _lbWaitForAdjustmentSaveIdle(photoId?: number): Promise<void>;
declare function _lbMarkEditRecipeWrite(photoId: number): number;
declare function _lbRememberEditRecipe(photoId: number, recipe: NavRecipe | null | undefined): void;
declare function _lbRefreshEditRecipeCache(updates?: Record<string, NavRecipe | null>): void;
declare function _lbReloadCurrentRenderAfterEdit(photoId?: number): void;
declare function _vireoBumpRenderVersion(photoId: number): void;
declare function _lbSetEditBusy(busy: boolean): void;
declare function _lbRecordFlag(photoId: number, flag: string | null | undefined): void;
declare function _lbLoadDetections(photoId: number): void;
declare function _lbLoadMaskVariants(photoId: number): void;
declare function _lbRenderEyeCrosshair(photo?: NavPhoto): void;
declare function _lbRenderKeywords(keywords?: NavPhoto["keywords"]): void;
declare function _lbRenderLifeListPanel(photoId?: number): void;
declare function _lbResetMaskOverlay(): void;
declare function _lbTransformBoxByRecipe(box: {x: number; y: number; w: number; h: number}, recipe: NavRecipe): {x: number; y: number; w: number; h: number};
declare function openLightbox(photoId: number, filename?: string, photoList?: NavPhoto[], options?: {readOnly?: boolean; readOnlyMessage?: string}): void;
declare function closeLightbox(): void;
declare function lightboxDelete(): unknown;
declare function lightboxNav(direction: number): void;
interface NavMissingFolder {
  id: number;
  path: string;
  name?: string;
  photo_count?: number;
  status?: string;
  children?: NavMissingFolder[];
}
interface NavMissingPhoto extends NavPhoto {
  has_thumb?: boolean;
  has_preview?: boolean;
  has_working_copy?: boolean;
  folder_path?: string;
  has_xmp_sidecar?: boolean;
  folder_id?: number;
}
interface NavMissingPhotosPayload {
  status?: string;
  error?: string;
  photos?: NavMissingPhoto[];
  pending?: boolean;
}
interface NavMatchRun {
  is_current?: boolean | number;
  state: string;
  classifier_model?: string;
  model?: string;
  detection_id?: number;
  explanation?: string;
  blocks_unlisted?: boolean;
  assessment?: {state: string};
  detector_model?: string;
  detector_confidence?: number;
  top_species?: string;
  max_match_score?: number;
  match_margin?: number;
  label_count?: number;
  score_kind?: string;
}
interface NavMatchSummary {
  state: string;
  unlisted_runs?: NavMatchRun[];
  runs?: NavMatchRun[];
  assessments?: {explanation?: string; model?: string}[];
}
interface NavPipelinePhotoData {
  match_summary?: NavMatchSummary;
  filename?: string;
  width?: number;
  height?: number;
  timestamp?: string;
  detection_box?: NavBox | null;
  crop_box?: NavBox | null;
  detection_conf?: number;
  predictions?: NavPipelinePrediction[];
  match_scores?: NavMatchRun[];
  sharpness?: number;
  subject_sharpness?: number;
  quality_score?: number;
  keywords?: {name: string}[];
  classification_diagnostics?: {detector_confidence_threshold?: number; max_detector_confidence?: number; hidden_detection_count?: number; current_prediction_count?: number; hidden_prediction_count?: number; classifier_run_count?: number; full_image_prediction_count?: number; full_image_classifier_run_count?: number};
}
interface NavPipelinePrediction {
  species: string;
  confidence: number;
  scientific_name?: string;
  match_score?: number;
  model?: string;
  status?: string;
  category?: string;
  taxonomy_kingdom?: string;
  taxonomy_phylum?: string;
  taxonomy_class?: string;
  taxonomy_order?: string;
  taxonomy_family?: string;
  taxonomy_genus?: string;
  group_id?: number;
  vote_count?: number;
  total_votes?: number;
}

interface NavBox {
  x: number;
  y: number;
  w: number;
  h: number;
}

interface NavHistoryEntry {
  id: number;
  action_type?: string;
  created_at?: string;
  description?: string;
}

interface KeymapShortcut {
  name?: string;
  description?: string;
  category?: string;
}

interface Document {
  webkitFullscreenElement?: Element | null;
}
interface NavShortcutDefaults extends Record<string,Record<string,string>> {
  browse: Record<string,string>;
  misses: Record<string,string>;
  lightbox: Record<string,string>;
}

interface NavNewImagesPayload {
  pending?: boolean;
  deferred_reason?: string;
  error?: string;
  workspace_id?: number;
  new_count: number;
  unreachable_roots?: string[];
  local_copy_excluded?: string[];
  checked_at?: string | null;
}
