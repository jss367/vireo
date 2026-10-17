// Browse's catalog, selection and asynchronous page-state contracts.
interface BrowsePhoto {
  id: number; filename: string; rating: number | null;
  flag?: 'none' | 'flagged' | 'rejected' | null;
  width?: number | null; height?: number | null; extension?: string;
  companion_path?: string | null; file_size?: number; timestamp?: string | null;
  folder_id?: number; folder_status?: string; location_status?: 'exif' | 'assigned' | 'none';
  sharpness?: number | null; quality_score?: number | null;
  prediction_confidence?: number | null; prediction_confidence_is_stack_lead?: boolean;
  species?: string[]; keywords?: BrowseKeyword[]; xmp_keywords?: string[];
  wildlife_excluded?: boolean | number; xmp_exists?: boolean;
  latitude?: number | null; longitude?: number | null; location?: BrowseLocation | null;
  metadata?: BrowseMetadata; edit_recipe?: Record<string, unknown> | null;
  browse_stack?: BrowseStack | null; detections?: BrowseDetection[];
  _similarity?: number; similarity?: number;
  life_list?: {species: string; is_current_photo?: boolean; is_species_representative?: boolean}[];
  flag_note?: BrowseFlagNote | null;
}
interface BrowseFlagNote {flag: 'none' | 'flagged' | 'rejected'; source: string | null; reason: string | null; description?: string; at?: string | null}
interface BrowseStack {count: number; kind: string; photo_ids: number[]}
interface BrowseDetection {x: number; y: number; w: number; h: number; box_x: number; box_y: number; box_w: number; box_h: number; species?: string; category?: string; confidence?: number; detector_confidence?: number}
interface BrowseKeyword {id: number; name: string; type: string; parent_id?: number | null; place_id?: number | null; photo_count?: number}
interface BrowseKeywordOption extends BrowseKeyword {search: string; place_id: number | null; photo_count: number}
interface BrowseKeywordAutocomplete {matches: BrowseKeywordOption[]; activeIndex: number; selectedKeyword: BrowseKeywordOption | null; dropdownId: string; submitFn: ((keyword: BrowseKeywordOption | null) => void) | null}
interface BrowseAnchor {photoId: number; topOffset: number; index: number; detailVisible?: boolean; stackSelection?: boolean; stackIds?: number[] | null}
interface BrowseStackRequest {memberIds: Set<number>; stale: boolean}
interface BrowseStackHydration extends BrowseStackRequest {seq: number}
interface BrowseCollection {id: number; name: string; photo_count?: number; offline_photo_count?: number; available_photo_count?: number; count_error?: string; visual_json?: string | null; has_visual?: boolean; collection_type?: string; rules_json?: string | null}
interface BrowseCalendar {year: number; min_year: number; max_year: number; days: Record<string, number>}
interface BrowseLoadState<T> {status: 'pending' | 'success' | 'failure'; data?: T; key?: string; shouldRender?: () => boolean}
interface BrowseFolder {id: number; path: string; name?: string; parent_id?: number | null; photo_count?: number; children?: BrowseFolder[]; status?: string; workspace_id?: number}
interface BrowseFolderResponse {folders: BrowseFolder[]; workspace_id?: number | null}
interface BrowseSummary {total: number; filtered_total: number; classified: number; unclassified: number; top_species?: {species: string, count: number}[]}
interface BrowseBestBatchCard {id: number; filename?: string; role?: string; rank?: number; reasons?: string[]; quality_pct?: number; focus?: number; sharpness?: number}
interface BrowseBestBatch {photo_ids: number[]; best_photo_id?: number; suggested_reject_ids?: number[]; count: number; cards: BrowseBestBatchCard[]; sequence_range?: number[]; best_reasons?: string[]}
interface BrowseTotals {total?: number; underlying_total?: number; stack_count?: number}
interface BrowsePage extends BrowseTotals {total: number; photos: BrowsePhoto[]; page?: number; total_pages?: number; resolved_focus_photo_id?: number | null; focus_photo_id?: number | null}
interface BrowseSearchLane {key: string | null; seq: number}
interface BrowsePrediction {id: number; photo_id: number; species: string; status: string; confidence?: number; model?: string; detection_id?: number; consensus_species?: string; species_key?: string; consensus_species_key?: string; alternatives?: unknown[]; effective_category?: string; category?: string; existing_species?: string[]}
interface BrowsePredictionGroup {species: string; status: string; decided: boolean; ids: number[]; models: Record<string, boolean>; ambiguous: BrowsePrediction[]; confidence: number | null; count: number; unlisted: BrowseMatchRun[]; detections: Record<string, boolean>; unlistedDetections: Record<string, boolean>}
interface BrowseConfig {keyboard_shortcuts?: Record<string, Record<string, string>>; google_maps_api_key?: string; google_maps_prefer_english?: boolean; photos_per_page?: number; browse_thumb_default?: number; browse_card_fields?: string[]}
interface Window {GOOGLE_MAPS_API_KEY: string; GOOGLE_MAPS_PREFER_ENGLISH: boolean; _vireoShortcuts?: Record<string, Record<string, string>>; _cfgPromise?: Promise<BrowseConfig | null>}
declare function showToast(message: string, type?: string): void;
declare function escapeHtml(value: string | number): string;
declare function escapeAttr(value: string | number): string;
declare var VireoViewPreferences: {read(key: string): string | null; write(key: string, value: string): void; restoreAll(root?: Element | null): void};
type BrowseRuleValue = string | number | boolean | (string | number)[] | null;
interface BrowseRule {mode?: string; rules?: BrowseRule[]; field?: string; op?: string; value?: BrowseRuleValue; value_label?: string}
interface BrowseRuleGroup extends BrowseRule {mode: string; rules: BrowseRule[]}
interface BrowseVisual {type?: string; query?: string; photo_id?: number; threshold?: number; text?: string; min_similarity?: number}
interface BrowseFilterChange {reason?: string; transient?: boolean}
interface BrowseFilterOptions {page: string; root: HTMLElement | null; scopeLabel?: string; onChange?: (change: BrowseFilterChange) => void; getScope?: () => {folder_id: number | null; collection_id: number | null}; onCollectionSaved?: () => void}
interface BrowseFilterController {
  isReady(): boolean; hasFilters(): boolean;
  getRules(): BrowseRuleGroup | null;
  getUserRules(): BrowseRuleGroup | null;
  getVisual(): BrowseVisual | null;
  addRule(field: string, op: string, value: BrowseRuleValue): void;
  clearAll(silent?: boolean): void;
  removeField(field: string, options?: {reason: string}): boolean;
  dependsOnMutation(mutation: string[]): boolean;
  describeRule(rule: BrowseRule): string;
  init(options: BrowseFilterOptions): Promise<void>;
  loadExpression(rules: BrowseRule | BrowseRule[], visual?: BrowseVisual | null, options?: {reason: string}): void;
  selectPhotoGroup(ids: number[], label: string): void;
  setResultTotal(total: number): void;
  setVisualInfo(info: unknown): void;
}
declare var VireoFilter: BrowseFilterController;
declare var VireoDialog: {create(root: HTMLElement, options: {initialFocus?: HTMLElement | null; onDismiss?: () => void}): {open(returnFocus?: Element | null): void; close(): void; step(delta: number): void}};
interface VireoNamespace {api?: {searchLaneHeaders(lane: string, sequence: number): Record<string, string>}}
interface Window {
  VireoFilter: BrowseFilterController;
  _vireoNativeMenuPhotoIdsOverride?: number[] | null;
  _lightboxPhotoList: BrowsePhoto[];
  _lbEscToken?: number | null;
  _locationMutationEpoch?: number;
  vireoThumbnailUrl(photo: BrowsePhoto | BrowseBestBatchCard | number): string;
  vireoRememberPhotoPair(photo: BrowsePhoto): void;
  vireoRememberPhotoEditRecipe(id: number, recipe: Record<string, unknown> | null | undefined, options?: {skipIfLocallyWritten?: boolean}): void;
  vireoPhotoHasOrientationEdit(id: number): boolean;
  vireoPhotoIsRawJpegPair(photo: BrowsePhoto): boolean;
  vireoUpdatePairSourceControls(id: number): void;
  vireoEditNav?: BrowseEditNavigation;
  VireoColorLabels?: {isValid(color: string): boolean; title(color: string, fallback: string): string; clear(id: number): void; refreshControls(root: Element): void};
  VIREO_REVEAL_LABEL: string;
  closeLightbox?: () => void;
  closeHelpModal?: () => void;
  closeCommandPalette?: () => void;
  closeReportModal?: () => void;
  doUndo?: () => Promise<void>;
  doRedo?: () => Promise<void>;
  afterHistoryChange?: (change?: string) => void;
  setWildlifeExcludedFor?: (id: number, excluded: boolean) => Promise<boolean | void>;
}
declare function matchesShortcut(event: KeyboardEvent, shortcut: string | undefined): boolean;
declare function preferredScrollBehavior(): ScrollBehavior;
declare function openLightbox(id: number, filename: string | undefined, photos?: BrowsePhoto[], options?: {preserveOneToOne?: boolean; readOnly?: boolean; readOnlyMessage?: string}): void;
declare function closeLightbox(event?: Event | null): void;
declare function exitLightboxFullscreen(): void;
declare function requestLightboxFullscreen(): void;
declare function lightboxNav(delta: number): void;
declare function findSimilar(id: number): void;
declare function checkPendingSync(): Promise<void>;
declare function showRevealFeedback(data: unknown): void;
declare function submitToInatBatch(ids: number[]): Promise<void>;
declare function showDeleteDialog(ids: number[], companionCount: number, callback: (data: {failed_photo_ids?: number[], deleted: number}) => void): void;
declare function developPhotos(ids: number[]): Promise<void>;
declare function openPanoramaModal(ids: number[]): void;
declare function closePanoramaModal(): void;
declare function isTauri(): boolean;
declare function pickDirectory(title: string, options?: {defaultPath?: string}): Promise<string | null>;
declare function _vireoPairSource(id: number): string | null;
declare var _vireoPairKnownByPhoto: Record<string, boolean>;
declare function _lbPhotoData(id: number): BrowsePhoto | null;
interface BrowseLoadOptions {silent?: boolean; focusPhotoId?: number | null; focusPhotoIds?: number[] | null}
interface BrowseReloadOptions {preserveScroll?: boolean; preserveAnchor?: boolean; preserveViewport?: boolean; preserveCollection?: boolean; focusAnchor?: boolean}
interface BrowseLoadState<T> {workspaceId?: number | null}
interface BrowseAnchor {viewportOnly?: boolean}
// browse.html loads request/state/loader owners before any page action runs.
type BrowseInitializedPanels = Required<NonNullable<VireoNamespace['browse']>> & {selectionPanel: BrowseSelectionPanel & {bindActions(): void}};

interface BrowsePhoto {is_species_representative?: boolean}
interface BrowseEditClipboard {recipe: Record<string, unknown>; source?: string | null; at?: number}
interface BrowseEditNavigation {
  setList(photos: BrowsePhoto[], id: number): void; setLastPhoto(id: number): void;
  getCopiedRecipe(): BrowseEditClipboard | null;
  beginCopiedRecipe(): Promise<string>;
  isCopiedRecipeCurrent(token: string): boolean;
  cancelCopiedRecipe(token: string | null): Promise<boolean>;
  setCopiedRecipeIfCurrent(recipe: Record<string, unknown> | null | undefined, meta: {source: string | null, at: number}, token: string): Promise<boolean>;
  setCopiedRecipe(recipe: Record<string, unknown> | null | undefined, meta: {source: string | null, at: number}): Promise<void>;
}
declare var VireoBatchEdits: {open(ids: number[]): Promise<void>; paste(ids: number[]): Promise<{count: number, skipped: number[]} | null>; resultMessage(data: {count: number, skipped: number[]}): string};
declare var VireoBrowseCompare: {create(options: {findPhoto: (id: number) => BrowsePhoto | null; fetch: typeof safeFetch; showToast: typeof showToast}): {open(ids: number[]): Promise<void> | void; isOpen(): boolean; close(): void; step(delta: number): void}};
interface BrowseKeywordRefreshOptions {skipPanels?: boolean; skipDetail?: boolean; reportFailure?: boolean}
interface BrowsePhoto {species_representatives?: {species: string; is_current_photo?: boolean; is_species_representative?: boolean}[]}
interface BrowseCollection {can_add_photos?: boolean; rules?: string | BrowseRule | BrowseRule[]}
interface BrowseCaptureTimeSample {filename?: string; before_time?: string; before_offset?: string; after_time?: string; after_offset?: string; shift_minutes?: number}
interface BrowseCaptureTimePreview {samples?: BrowseCaptureTimeSample[]; shifts_vary?: boolean; shift_minutes?: number; affected?: number; missing_time?: number; total?: number;}
interface BrowseCaptureTimeCompletion {status?: string; errors?: string[]; result?: {skipped?: number; updated?: number; failed?: number}}
declare function safeEventSource<P, R>(url: string, options: {onProgress?: (data: P) => void; onComplete?: (data: R) => void; onError?: () => void}): EventSource;

interface BrowseAvailability extends BrowseTotals {inventory_total?: number; available_total?: number; offline_total?: number}
interface BrowseFolderHealthChange {restored?: number[]; wentMissing?: number[]; workspace_id?: number; [extra: string]: unknown}
interface DocumentEventMap {
  'lifelist:changed': CustomEvent<{photoId?: number; species?: string}>;
  'lightbox:renderchanged': CustomEvent<{photoIds?: number[]}>;
  'lightbox:photochanged': CustomEvent<{photoId: number}>;
  'lightbox:navigationboundary': CustomEvent<{delta: number; photoId: number; index?: number; photoCount?: number}>;
  'lightbox:photodeleted': CustomEvent<{photoId: number}>;
  'lightbox:closed': CustomEvent<{photoId: number | null}>;
  'vireo:folder-health-changed': CustomEvent<BrowseFolderHealthChange>;
}
declare var _missingFoldersSnapshotVersion: number;
declare function _reconcileMissingFoldersInitSnapshot(ids: number[], snapshot: number | null, source: string, serverVersion?: number | null): boolean;
interface Window {markMissingFoldersReconciliationPending?: () => void; vireoLocalFolders?: {load(): Promise<unknown>; stage(id: number): Promise<void>}}
interface BrowseQueryBody {rules: BrowseRuleGroup | BrowseRule[]; sort: string; page?: number; per_page?: number; stacks?: boolean; ids_only?: boolean; include_availability?: boolean; include_offline?: boolean; visual?: BrowseVisual; folder_id?: number; collection_id?: number; focus_photo_id?: number; focus_photo_ids?: number[]}
interface BrowseIdsResponse {photo_ids?: number[]; ids?: number[]}
interface BrowsePage {visual?: unknown; focus_index?: number; focus_page?: number}

interface BrowsePhoto {subject_sharpness?: number | null}
interface BrowseStackGesture {ids: number[]; epoch: number}
interface Window {vireoLightboxSession: {requestedPhotoId(): number | null; displayedPhotoId(): number | null}}

interface BrowseMatchRun {state: string; detection_id?: number; classifier_model?: string; model?: string}
interface BrowseMatchState {state: string; assessments?: {state: string; explanation: string; model: string}[]; judged_models?: number; unlisted_runs?: BrowseMatchRun[]}
interface BrowsePredictionPhotoState {threshold?: number; detector_ran?: boolean; classifier_ran?: boolean; detection_count?: number}
interface BrowseDetailPredictionPayload {predictions?: BrowsePrediction[]; photo_states?: Record<string, BrowsePredictionPhotoState>; match_states?: Record<string, BrowseMatchState>}
interface BrowsePredictionMutation {accepted?: number; rejected?: number; already_decided?: number; skipped_ambiguous?: number; skipped_superseded?: number; skipped_out_of_workspace?: number; skipped_species_drifted?: number}
interface BrowsePredictionRefreshOptions {skipDetail?: boolean}
interface BrowsePredictionMutationOptions {reloadDetail?: boolean; photoId?: number; accepted?: number}

interface BrowseExif {Make?: string; Model?: string; LensModel?: string; FocalLength?: number; FNumber?: number; ExposureTime?: number; ISO?: number; [tag: string]: unknown}
interface BrowseMetadata {EXIF?: BrowseExif; Composite?: {LensID?: string; Lens?: string; [tag: string]: unknown}; [group: string]: Record<string, unknown> | undefined}
interface BrowseLocation {latitude?: number; longitude?: number; name?: string; display_name?: string; place_id?: number; source?: string; hierarchy?: string[]}

interface BrowseLocation {parent_chain?: {name?: string}[]}
interface BrowseGooglePlace {place_id?: string; name?: string; formatted_address?: string; types?: string[]; geometry?: {location?: {lat: number | (() => number); lng: number | (() => number)}}; address_components?: {long_name?: string; name?: string; short_name?: string; types?: string[]}[]}
interface BrowseGooglePlaceSubmit {place_id: string; name: string; types: string[]; lat: number; lng: number; address_components: {name: string; short_name: string; types: string[]}[]}
interface BrowseGoogleAutocomplete {addListener(event: string, listener: () => void): void; getPlace(): BrowseGooglePlace}
interface BrowseGoogle {maps: {places: {Autocomplete: new(input: HTMLInputElement, options: {types: string[]; fields: string[]}) => BrowseGoogleAutocomplete}}}
interface Window {_gmapsCallback?: () => void; google: BrowseGoogle}
interface BrowseLocationBody {place_id?: string | number; place?: BrowseGooglePlaceSubmit; keyword_id?: number; name?: string}
interface BrowseLocationResult {location?: BrowseLocation | null}

interface BrowseExportRequest {photo_ids: number[]; destination: string; export_to_subfolder: boolean; subfolder_name: string; naming_template: string; max_size: number | null; format: string; quality?: number; metadata_fields?: string[]; reveal_after_export?: boolean}
interface BrowseExportPreflight {error?: string; collision_count?: number}
declare var VireoExportCollisions: {reset(): void; schedule(): void; cancelPending(): void; acknowledged(result: BrowseExportPreflight): boolean};
declare var VireoExportPresets: {modalOpened(): void; markCustom(): void; subfolderName(): string};
declare var VireoExportJob: {watch(id: string): void};
declare var VireoFolderBrowser: new(options: {overlayId: string; defaultMode: string; modes: Record<string, {title: string; multiple: boolean; showCounts: boolean; startPath(): string; onSelect(path: string): void}>}) => {sequence: number; open(mode: string, options?: {skipInitialBrowse: boolean}): void};
interface BrowseMenuItem {label?: string; title?: string; disabled?: boolean; disabledHint?: string; separator?: boolean; chips?: BrowseMenuItem[]; onClick?: () => void; color?: string | null; colorBaseTitle?: string}
declare function openContextMenu(event: {clientX: number; clientY: number}, items: BrowseMenuItem[]): void;
declare function buildOpenInEditorMenuItems(ids: number[]): BrowseMenuItem[];
declare function coerceSelectionOnContext(selected: Set<number>, id: number): number[];
declare var VireoReleaseOriginals: {isRunning(): boolean; release(scope: {photo_ids?: number[]; folder_id?: number; collection_id?: number}, description: string): void};
interface Window {buildSpeciesHighlightMenuItems(ids: number[], options: {showFetchFallback: boolean}): BrowseMenuItem[]; buildSpeciesRepresentativeMenuItems(ids: number[], options: {getPhoto(id: number): BrowsePhoto | null}): BrowseMenuItem[]}
interface BrowseFolder {is_workspace_root?: boolean; __synthetic_missing_local?: boolean}
interface BrowsePhoto {path?: string}
interface BrowseFolderStatus {kind: string; label: string; description: string; fallback?: boolean}
interface BrowseLocalFolder {requested_folder_id?: number; root_folder_id: number; visible_ancestor_folder_id?: number; state: string; folder_name?: string; workspace_photo_count?: number; changes?: {created?: number; modified?: number; deleted?: number}; changes_error?: string; recovery_kind?: string}
interface BrowseFolderJob {type: string; folder_ids?: number[]}
interface BrowseLocalFolderData {legacy_workspace_session?: boolean; folders?: BrowseLocalFolder[]; jobs?: BrowseFolderJob[]}
interface Window {vireoLocalFolderData?: BrowseLocalFolderData; vireoPendingArchives?: {folder_ids: number[]; state: string; destination?: string}[]}
interface BrowseFolderStatusResult {statuses: Record<string, BrowseFolderStatus>; synthetic: BrowseFolder[]}
