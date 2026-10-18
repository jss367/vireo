// Endpoint contracts and shared script dependencies for Process.
interface PipelineStage {suffix: string; label: string; enable: string | null;}
interface PipelineWarning {message: string;}
interface PipelinePlanEntry {state?: string; summary?: string; detail?: {pending?: number; eligible?: number; fingerprint_outdated?: boolean; fingerprint_invalidated?: boolean; fingerprint_reason?: string; import_no_new?: boolean; sam_variant_warning?: PipelineWarning | null};}
interface PipelinePlan {stages?: Record<string,PipelinePlanEntry>;}
interface PipelineStageResult {status?: string; error_count?: number; count?: number; cached?: number; seen?: number; cached_estimate?: number; total?: number; copied?: number; skipped_duplicate?: number; photos_indexed?: number; generated?: number; predictions_stored?: number; masked?: number; skipped?: number | string; unreadable?: number; failed?: number; reason?: string; errors?: string[]; processed?: number;}
interface PipelineResult {notes?: string[]; errors?: string[]; stages?: Record<string,PipelineStageResult>; cancelled?: boolean; predictions_stored?: number; total?: number; labels_file?: string; species_count?: number; embedding_precompute?: PipelinePrecompute;}
interface PipelineCompletion {status: string; result?: PipelineResult | null; errors?: string[];}
interface PipelineProgress {stages?: Record<string,PipelineStageResult>; stage_id?: string; phase?: string; current: number; total: number; current_file?: string; rate?: number; eta_seconds?: number;}
interface PipelinePrecompute {model_id?: string; labels_file?: string; model_name?: string;}
interface PipelinePhoto {path: string; filename: string; photo_id?: number; duplicate?: boolean; size?: number; subfolder?: string; thumb_url?: string;}
interface PipelinePreview {files: PipelinePhoto[]; error?: string; total_size: number; total_count: number; type_breakdown: Record<string,number>;}
interface PipelineRequest {folder_ids?: number[]; collection_id?: number; exclude_photo_ids?: number[]; skip_classify?: boolean; raw_subject_analysis?: boolean; skip_extract_masks?: boolean; skip_eye_keypoints?: boolean; skip_regroup?: boolean; review_mode?: string | null; eye_detect_override?: boolean; model_ids?: string[]; labels_files?: string[]; reclassify?: boolean; strategy?: string; model_id?: string; download_taxonomy?: boolean; miss_enabled?: boolean;}
interface PipelineProcess {id: string; name: string; skip_classify?: boolean; skip_extract_masks?: boolean; skip_eye_keypoints?: boolean; skip_regroup?: boolean; miss_enabled?: boolean; review_mode?: string | null;}
interface PipelineCoverage {variant: string; count?: number; active_count?: number;}
interface PipelineModelReadiness {variant: string; ready?: boolean; size_hint?: string;}
interface PipelineExtractReadiness {sam2?: PipelineModelReadiness; sam2_known?: boolean; dinov2?: PipelineModelReadiness; dinov2_known?: boolean; sam_variant_warning?: PipelineWarning | null;}
interface PipelineConfig {sam2_variant: string; dinov2_variant: string; proxy_longest_edge: number; preview_max_size?: number; eye_detect_enabled?: boolean;}
interface PipelinePageInit {pipeline_config: PipelineConfig; taxonomy_available?: boolean; mask_variant_coverage?: PipelineCoverage[]; sam_variant_warning?: PipelineWarning | null;}
interface PipelineFolder {id: number; parent_id?: number | null; path: string; workspace_photo_count?: number; photo_count?: number;}
interface PipelineReadiness {model_ready?: boolean; needs_download?: boolean; model_name?: string; model_size_mb: number; use_tol?: boolean; labels_blocked?: boolean; labels_name: string; labels_skipped?: number; labels_count: number; embeddings_cached?: boolean; exiftool?: {installed?: boolean; brew_available?: boolean};}
interface PipelineSlotInfo {active: number; queued: number; slot_cap: number;}
interface PipelineLabelFile {labels_file: string; usable_count?: number; species_count?: number; path?: string; name?: string; count?: number; active?: boolean;}
interface Window {_pageInitPending?: boolean;}
declare function safeFetch<T = unknown>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
declare function safeFetch(url: '/api/pipeline/plan', init?: RequestInit, options?: {toast?: boolean}): Promise<PipelinePlan>;
declare function safeFetch(url: '/api/pipeline/config', init?: RequestInit, options?: {toast?: boolean}): Promise<{status?: string}>;
declare function safeFetch(url: '/api/pipeline/page-init', init?: RequestInit, options?: {toast?: boolean}): Promise<PipelinePageInit>;
declare function safeFetch(url: '/api/models', init?: RequestInit, options?: {toast?: boolean}): Promise<{models: {id: string; name: string; downloaded?: boolean; supports_label_lists?: boolean; model_str: string; label_list_tag?: string}[]; active_id?: string}>;
declare function safeFetch(url: '/api/collections', init?: RequestInit, options?: {toast?: boolean}): Promise<{id: number; name: string; count_error?: boolean; has_visual?: boolean}[]>;
declare function safeFetch(url: '/api/workspaces/active', init?: RequestInit, options?: {toast?: boolean}): Promise<{id: number}>;
declare function safeFetch(url: '/api/labels/taxon-groups', init?: RequestInit, options?: {toast?: boolean}): Promise<{key: string; name: string}[]>;
declare function safeFetch(url: '/api/labels/observation-filters', init?: RequestInit, options?: {toast?: boolean}): Promise<{key: string; name: string; description?: string}[]>;
declare function safeFetch(url: '/api/jobs/precompute-embeddings', init?: RequestInit, options?: {toast?: boolean}): Promise<{job_id: number; error?: string; model_warning?: string}>;
declare function safeFetch(url: '/api/jobs/fetch-labels', init?: RequestInit, options?: {toast?: boolean}): Promise<{job_id: number; error?: string; model_warning?: string}>;
declare function safeFetch(url: '/api/jobs/classify', init?: RequestInit, options?: {toast?: boolean}): Promise<{job_id: number; error?: string; model_warning?: string}>;
declare function safeFetch(url: '/api/jobs/extract-masks', init?: RequestInit, options?: {toast?: boolean}): Promise<{job_id: number; error?: string; model_warning?: string}>;
declare function safeFetch(url: '/api/jobs/pipeline', init?: RequestInit, options?: {toast?: boolean}): Promise<{job_id: number; error?: string; model_warning?: string}>;
declare function safeFetch(url: '/api/labels', init?: RequestInit, options?: {toast?: boolean}): Promise<{labels?: PipelineLabelFile[]; active?: {labels_file: string}[]}>;
declare function safeFetch(url: '/api/system/install-exiftool', init?: RequestInit, options?: {toast?: boolean}): Promise<{success?: boolean; error?: string}>;
declare function safeEventSource(url: string, callbacks: {onProgress?(progress: PipelineProgress): void; onComplete?(result: PipelineCompletion): void; onError?(): void;}): EventSource;
declare function escapeHtml(value: unknown): string;
declare function formatBytes(value: number): string;
declare function preferredScrollBehavior(): ScrollBehavior;
declare function showToast(message: string, kind?: string): void;
declare function splitPipelineMessages(errors?: string[], notes?: string[]): {errors: Record<string,string>; notes: Record<string,string>; noteList: string[]};

interface PipelineCardAggregate {running: boolean; failed: boolean; warning: boolean; completed: number; skipped: number; present: number;}
declare function safeFetch(url: '/api/pipeline/slots', init?: RequestInit, options?: {toast?: boolean}): Promise<PipelineSlotInfo>;
// The page-hook registry from vireo-page-hooks.js (types/page-hooks.d.ts).
declare var Vireo: VireoPageHookRegistry;
