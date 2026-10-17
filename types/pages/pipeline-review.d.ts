// Pipeline Review page data and the shared helpers supplied by the navbar.
type ReviewPrediction = [string, number, string?, string?];
interface ReviewSpeciesModel {model?: string; confidence: number; photo_count?: number;}
interface ReviewSpeciesRow {species_key?: string; species: string; models: ReviewSpeciesModel[]; count?: number; avg_confidence?: number;}
interface ReviewSubject {detection_id: number; box?: {x?: number; y?: number; w?: number; h?: number}; detection_confidence?: number; predictions?: ReviewPrediction[];}
interface ReviewPhoto {focus_score?: number; exposure_score?: number; composition_score?: number; area_score?: number; noise_score?: number; id: number; filename: string; timestamp?: string; width?: number; height?: number; flag?: string; rating?: number; label?: string; species?: string; species_top5?: ReviewPrediction[]; subjects?: ReviewSubject[]; confirmed_species_identities?: {name: string; key: string}[]; quality_composite?: number | null; rarity_protected?: boolean; reject_reasons?: string[]; is_species_representative?: boolean; life_list?: {species?: string; is_current_photo?: boolean; is_species_representative?: boolean}[]; edit_recipe?: unknown; wildlife_excluded?: boolean | number; eye_x?: number | null; eye_y?: number | null; bg_separation?: number | null; bg_tenengrad?: number | null; box_sharpness?: number | null; crop_complete?: number | null; detection_conf?: number | null; isolated_species_context?: {anchor_species?: string; conflicting_species?: string}; weak_detection_context?: {evidence?: string; support?: string; species?: string}; low_confidence_photo_ids?: number[]; phash_crop?: string; subject_clip_high?: number | null; subject_clip_low?: number | null; subject_size?: number | null; subject_tenengrad?: number | null; subject_y_median?: number | null;}
interface ReviewBurst {photo_ids: number[]; species_override?: {species: string; confirmed: boolean; species_list?: string[]} | null; species_predictions?: ReviewSpeciesRow[];}
interface ReviewEncounter {photo_ids: number[]; photo_count: number; bursts: ReviewBurst[]; burst_count?: number; species?: string[]; confirmed_species?: string | null; species_confirmed?: boolean; species_predictions?: ReviewSpeciesRow[]; time_range?: [string | null, string | null]; missing_timestamp_count?: number; trace?: ReviewTrace[];}
interface ReviewTrace {components?: Record<string,ReviewTraceComponent>;thresholds?: Record<string,number>; dt_seconds?: number | null; score?: number; decision?: string; photo_a_id?: number; photo_b_id?: number; photo_a_filename?: string; photo_b_filename?: string;}
interface ReviewResults {ok?: boolean;photos: ReviewPhoto[]; encounters: ReviewEncounter[]; summary: ReviewSummary; review_mode?: string | null; miss_computed_at?: string; source?: string; error?: string; species_identities?: Record<string,{key: string; display_name?: string}>;}
interface ReviewSummary {keep_count: number; review_count: number; reject_count: number; rarity_protected: number; encounter_count: number; burst_count: number; confirmed_count: number; unconfirmed_count: number; confirmation_unit?: string;total_photos: number; total_encounters?: number; total_bursts?: number; confirmed_encounters?: number; confirmed_bursts?: number; keep?: number; review?: number; reject?: number; [key: string]: unknown;}
interface ReviewCacheInfo {scope?: string; workspace_photo_count?: number; cached_photo_count?: number; missing_photo_count?: number; is_partial?: boolean; [key: string]: unknown;}
interface ReviewReadiness {state: string; total_photos: number; with_masks: number; mask_target_photos?: number; with_embeddings: number; embedding_target_photos?: number; with_eye_keypoint_attempts?: number; with_eye_keypoints: number; eye_keypoint_target_photos?: number; with_predictions: number; prediction_target_photos?: number; enhancing_missing?: string[];}
interface ReviewCollection {can_add_photos?: boolean; available_photo_count?: number; offline_photo_count?: number;id: number; name: string; count_error?: boolean; has_visual?: boolean; rules?: string | Record<string,unknown> | null; photo_count?: number;}
interface ReviewFolder {name: string;id: number; path: string; parent_id?: number | null; photo_count?: number;}
interface ReviewDbPhoto {flag?: string; has_species_keyword?: boolean; is_species_representative?: boolean; rating?: number;}
interface ReviewGroupState {initialSpecies?: string;encIdx: number | null; burstIdx: number | null; items: ReviewPhoto[]; picks: Set<number>; rejects: Set<number>; removed: Set<number>; selected: number | null; selectedIds: Set<number>; selectedSubjectByPhoto: Record<string,number>; selectionAnchor: number | null; touched: Set<number>; sessionId: number; applying: boolean; allowRemove?: boolean; appliedRemovalIds?: number[]; appliedRemovalSessionId?: number; applyFlagsOverride?: boolean | null; confirmSpeciesOverride?: boolean | null; dbState: Record<string,ReviewDbPhoto>; initialConfirmedSpecies?: string | null; keywordStateSpecies?: string; seeded?: boolean; source?: string; speciesFieldTouched?: boolean;}
interface ReviewGroupDiff {species: string; speciesChanged: boolean; flagNew: number; rejectNew: number; clearNew: number; detachNew: number; [key: string]: unknown;}
interface ReviewGroupChecks {flags?: boolean; species?: boolean;}
interface ReviewKeywordStats {species: string; total: number; applied: number; missing: number;}
interface ReviewGroupTarget {key: string; photoIds: number[]; [key: string]: unknown;}
interface ReviewConflict {alternativeKey?: string;severity: string | null; expectedSpecies: string | null; alternativeSpecies?: string | null; expectedSupport: number; alternativeSupport?: number; [key: string]: unknown;}
interface ReviewConflictEvidence { [key: number]: ReviewConflict | null; }
interface ReviewMenuItem {chips?: ReviewMenuItem[]; disabledHint?: string; onClick?: () => unknown;label?: string; icon?: string; title?: string; action?: () => unknown; separator?: boolean; disabled?: boolean; children?: ReviewMenuItem[]; shortcut?: string;}
interface ReviewLightboxOptions {readOnly?: boolean; readOnlyMessage?: string;}
interface Window {setLightboxReadOnlyMode?: (readOnly: boolean, message?: string) => void; setLightboxProvisionalFlag?: (photoId: number, flag: string) => void; clearLightboxProvisionalFlags?: (photoIds?: number[]) => void;developPhotos?: (photoIds: number[]) => unknown; vireoHistoryBusy?: () => boolean; getLightboxBrowseDisabledHint?: (photoId: number, willNavigateInPlace: boolean) => string | null; getPhotoExternalEditDisabledHint?: (photoId: number) => string | null; beforeHistoryChange?: () => unknown; afterHistoryChange?: () => unknown; vireoThumbnailUrl?: (photo: ReviewPhoto | number) => string; vireoEditNav?: {setList(photos: ReviewPhoto[], selectedId: number): void; setLastPhoto(photoId: number): void;}; _vireoNativeMenuPhotoIdsOverride?: number[] | null; setFlagFor?: (photoId: number, flag: string) => unknown; getLightboxOpenOptions?: (photoId: number) => ReviewLightboxOptions; openInBrowse?: (photoId: number) => unknown; findSimilar?: (photoId: number) => unknown; VIREO_REVEAL_LABEL?: string; VIREO_REVEAL_ICON?: string; buildSpeciesHighlightMenuItems?: (photoIds: number[], options?: unknown) => ReviewMenuItem[]; buildSpeciesRepresentativeMenuItems?: (photoIds: number[], options?: unknown) => ReviewMenuItem[]; buildOpenInEditorMenuItems?: (photoIds: number[], options?: unknown) => ReviewMenuItem[]; }
declare function safeFetch<T = unknown>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
declare function escapeHtml(value: unknown): string;
declare function escapeAttr(value: unknown): string;
declare function showToast(message: string, kind?: string): void;
declare function isTauri(): boolean;
declare function nativeMenuActivePhotoIds(): number[];
declare function openContextMenu(event: MouseEvent, items: ReviewMenuItem[]): void;
declare function openLightbox(photoId: number, filename: string, photos?: ReviewPhoto[], options?: ReviewLightboxOptions): void;
declare function showRevealFeedback(response: unknown): void;
declare function checkPendingSync(): void;
declare function _lbRecipeHasGeometricEdit(recipe: unknown): boolean;
declare var vireoLightboxSession: {requestedPhotoId(): number | null;} | null;
declare var VireoTextSearch: {matches(text: string, query: string, options?: ReviewSearchOptions): boolean; matchesFields(field: string | null | undefined, query: string, options?: ReviewSearchOptions): boolean; appendParams(params: URLSearchParams, options: ReviewSearchOptions): void; renderOptions(namespace: string, options: ReviewSearchOptions): string; toggle(namespace: string, option: string, callback: (options: ReviewSearchOptions) => void): void;};
declare var VireoPipelineConfig: {embeddingDistancePercentToThreshold(value: number): number; buildSliderDefaults(config?: unknown): {scoring: Record<string,number>; grouping: Record<string,number>};};
interface ReviewCardDrag {card: HTMLElement; photoId: string; startX: number; startY: number; moved: boolean; targets: ReviewOffsetTarget[];}
interface ReviewLoupeDrag {startX: number; startY: number; moved: boolean; targets: ReviewOffsetTarget[];}
interface ReviewOffsetTarget {card: HTMLElement; photoId: string; origTx: number; origTy: number;}
interface ReviewPageInit {results?: ReviewResults | null; results_cache_info?: ReviewCacheInfo | null; workspace_overrides?: {review_min_confidence?: number} | null; review_readiness?: ReviewReadiness | null;}
declare function safeFetch(url: '/api/pipeline/page-init', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewPageInit>;
declare function safeFetch(url: '/api/pipeline/regroup-live', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewResults>;
declare function safeFetch(url: '/api/pipeline/reflow', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewResults>;
declare function safeFetch(url: '/api/pipeline/selection-results', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewResults>;
declare function safeFetch(url: '/api/pipeline/detach-burst', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewResults>;
declare function safeFetch(url: '/api/pipeline/detach-photo', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewResults>;
declare function safeFetch(url: '/api/pipeline/group/state', init?: RequestInit, options?: {toast?: boolean}): Promise<{photos?: Record<number,ReviewDbPhoto>}>;
declare function safeFetch(url: '/api/pipeline/save-cache', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewResults>;
declare function safeFetch(url: '/api/folders', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewFolder[]>;
declare function safeFetch(url: '/api/collections', init?: RequestInit & {method?: 'GET'}, options?: {toast?: boolean}): Promise<ReviewCollection[]>;

interface ReviewTraceComponent {value?: number; weight: number; used?: boolean; absent_a?: boolean; absent_b?: boolean; uncertain_a?: boolean; uncertain_b?: boolean; missing_a?: boolean; missing_b?: boolean;}

interface ReviewPhotoEvent {photo_id?: number; photo_ids?: number[]; species?: string; [key: string]: unknown;}

declare function safeFetch(url: '/api/collections', init: RequestInit & {method: 'POST'}, options?: {toast?: boolean}): Promise<ReviewCollection>;

interface ReviewSearchOptions {matchCase: boolean; wholeWord: boolean;}
interface ReviewFolderNode {total: number; folder: ReviewFolder; children: ReviewFolderNode[];}
interface ReviewSpeciesResponse {ok?: boolean; encounters?: ReviewEncounter[]; summary?: ReviewSummary; low_confidence_photo_ids?: number[];}
declare function safeFetch(url: '/api/encounters/species', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewSpeciesResponse>;

interface ReviewGroupApplyResponse {photos?: Record<string,ReviewDbPhoto>; encounters?: ReviewEncounter[]; summary?: ReviewSummary; [key: string]: unknown;}
declare function safeFetch(url: '/api/pipeline/group/apply', init?: RequestInit, options?: {toast?: boolean}): Promise<ReviewGroupApplyResponse>;

declare function safeFetch(url: `/api/pipeline/scope-photos?${string}`, init?: RequestInit, options?: {toast?: boolean}): Promise<{photo_ids?: number[]}>;
