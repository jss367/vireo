/// <reference path="../time.d.ts" />
// Jobs endpoint payloads and external helpers loaded by the shared navbar.
interface JobsProgress {
  current?: number; total?: number; phase_current?: number; phase_total?: number;
  phase_label?: string; phase?: string; current_file?: string; pause_reason?: string;
  unit?: string; eta_state?: string; eta_seconds?: number; eta_rate_per_min?: number;
  eta_kind?: string; cache_hits?: number; classified?: number;
}
interface JobsStep {
  id: string; label: string; status: string; error_count?: number; error?: string;
  source_index?: number; progress?: JobsProgress; summary?: string; started_at?: string;
  duration?: number; label_source?: string; current_file?: string;
}
interface JobsConfig {
  source_snapshot_id?: string; sources?: string[]; restarted_from?: string;
  parent_import_job_id?: string; root_import_job_id?: string; chain_already_ran?: boolean;
  chain_process_job_ids?: string[]; chain_process_job_groups?: string[][];
  carry_photo_fingerprints?: Record<number, string>; carry_photo_ids?: number[];
  untagged_photo_ids?: number[]; paid_tag_photo_ids?: number[];
  recover_landed_files?: Record<string, unknown[]>; mode?: string; destination?: string;
  recursive?: boolean; folder_template?: string; file_types?: string; skip_duplicates?: boolean;
  verify_by_hash?: boolean; trust_likely_duplicates?: boolean; after_import?: unknown;
  tags?: unknown[]; location_from_gps?: boolean; location_choices?: unknown[]; allow_missing_exiftool?: boolean;
  include_paths?: string[]; previewed_count?: number; checked_count?: number;
  remote_target_id?: string; remote_subpath?: string;
  after_process_move?: {remote_target_id?: string}; local_processing?: boolean; defer_nas_transfer?: boolean;
  source_path?: string; resolved_destination?: string; date_destinations?: JobsDestination[];
  date_destination_count?: number; date_photo_count?: number; merge?: boolean;
  collection_name?: string; collection_id?: number | string;
}
interface JobsDestination { path?: string; relative_path?: string; photo_count?: number; moved?: number; already_in_place?: number; planned?: number }
interface JobsResult {
  interrupted?: boolean; photo_ids?: number[]; carried_photo_ids?: number[]; recovered_photo_ids?: number[];
  photo_fingerprints?: Record<number,string>; carried_photo_fingerprints?: Record<number,string>;
  landed_files?: Record<string,unknown[]>; failed?: number; tags_applied?: boolean; chained?: boolean;
  ok?: boolean; never_started?: boolean; after_import_skipped?: string; process_job_id?: string;
  tagging?: {errors?: unknown[]}; collection_id?: number | string; collection_name?: string; cancelled?: boolean;
  destinations?: JobsDestination[]; moved?: number; already_in_place?: number; errors?: string[];
  error?: string; last_progress_at?: string;
  stages?: Record<string,{failed_photos?: {id:number;filename?:string;reason?:string}[];failed_photos_truncated?:number}>;
}
interface JobsRecoveryOptions {
  resume_allowed?: boolean; direct_to_nas_allowed?: boolean; error?: string; retryable?: boolean; taken_over_by_job_id?: string;
}
interface JobsRow {
  id: string; type: string; status: string; config?: JobsConfig; result?: JobsResult | null;
  started_at: string; finished_at?: string; duration?: number | null; workspace_id?: number | null;
  steps?: JobsStep[]; tree?: JobsStep[]; progress?: JobsProgress; errors?: string[]; error?: string;
  summary?: string; result_details?: string[]; pausable?: boolean;
  import_resume_pending?: boolean; import_direct_pending?: boolean;
  import_resume_allowed?: boolean; import_recovery_options?: JobsRecoveryOptions;
}
interface JobsSnapshot {active: JobsRow[];active_workspace_id?: number | null; workspace_names?: Record<number,string>;keeping_awake?:boolean}
interface JobsPollLane {generation:number;pending:boolean;loaded:boolean;error:boolean;errorDetail:string;controller:AbortController|null}
interface JobsImportEntry {id:string;status:string;startedAt:string;result:JobsResult;root?:string;config:JobsConfig;parent?:string;tagsApplied?:boolean;chainProcessJobIds?:string[];chained?:boolean;resumable?:boolean;hasFailedFiles?:boolean}
interface JobsTakeover {tagsApplied:boolean;chained:boolean;by:string|null;byStartedAt:string|null;kind:string|null;parentInterrupted:boolean}
interface JobsSourceCleanup {checking?:boolean;state?:string;file_count?:number;xmp_count?:number;message?:string;error?:string|null;files?:{name:string;size:number}[];review_token?:string;confirmed?:boolean;busy?:boolean;directories?:string[];trashed?:number;failures?:{path:string;error:string}[]}
// The shared globals the page's shared.js reads.
interface Window {
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  showToast?(message: string | undefined, kind?: string): void;
  safeFetch<T>(url: string, options?: RequestInit): Promise<T>;
  importFailedBeforeStart(result: JobsResult | null | undefined): boolean;
  canRetryImportDirectToNas(job: JobsRow, rows: JobsRow[]): boolean;
  directNasImportRetryBody(job: JobsRow | undefined, body: JobsConfig | null, rows:JobsRow[]): JobsConfig | null;
  formatJobType(type: string): string;
}
