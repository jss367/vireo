// The Dashboard (stats.html, vireo/static/dashboard/): the scope, the
// /api/stats, /api/coverage and inventory payloads it renders, and the shared
// globals its shared.js reads.
interface DashboardScope {folder_id: string | null; collection_id: string | null; date_from: string; date_to: string}
interface DashboardFolderOption {id: number; path: string; parent_id?: number | null}
interface DashboardCollectionOption {id: number; name: string; degraded?: boolean; has_visual?: boolean}
interface DashboardOptions {folders: DashboardFolderOption[]; collections: DashboardCollectionOption[]}
/** Browse query parameters a drill-down or card adds to the scope. */
type DashboardBrowseParams = Record<string, string | number | null | undefined>;

interface DashboardKeyword {name: string; identity?: string; photo_count: number; is_species?: boolean}
interface DashboardMonth {month: string; count: number}
interface DashboardAttention {
  preview_enabled?: boolean; preview_size?: number; unclassified?: number; missing_location?: number;
  missing_previews?: number; pending_sync?: number; duplicate_groups?: number;
}
interface DashboardStats {
  total_photos?: number; folder_count?: number; keyword_count?: number; pending_changes?: number;
  accessible_photos?: number; missing_folder_count?: number; attention?: DashboardAttention;
  top_keywords?: DashboardKeyword[]; photos_by_month?: DashboardMonth[];
  rating_distribution?: {rating: number; count: number}[]; flag_distribution?: {flag: string; count: number}[];
  prediction_status?: {status: string; count: number}[]; classified_count?: number; detected_count?: number;
  quality_distribution?: {bucket: number; count: number}[]; photos_by_hour?: {hour: number; count: number}[];
}
/** Coverage counts by field key, plus the total they are out of. */
type DashboardCoverageCounts = Record<string, number | undefined> & {total?: number};
interface DashboardCoverageFolder extends Record<string, unknown> {folder_id: number; path: string; total?: number}
interface DashboardCoverageField {key: string; label: string}
interface DashboardOfflineContext {offline_photos: number; missing_folders: number}

interface DashboardInventoryCounts {classified_dets: number; pending_dets: number; coverage_pct: number; total_predictions_rows?: number}
interface DashboardInventoryPair {
  status: string; coverage_pct: number; last_run?: string | null; median_top1_conf?: number | null;
  median_sample_size: number; label_set_path?: string | null; classified_dets: number; pending_dets: number;
  photos_covered: number; label_set: string; fingerprint: string;
}
interface DashboardInventoryModel {
  id?: string; name: string; legacy?: boolean; downloaded?: boolean;
  subtotal: DashboardInventoryCounts; pairs: DashboardInventoryPair[];
}
interface DashboardInventory {
  models?: DashboardInventoryModel[]; total_real_detections?: number; grand_total: DashboardInventoryCounts;
  unusable_label_sets?: {name: string; skipped?: number}[];
  stale?: {model: string; fingerprint: string; stale_count?: number; last_run?: string | null}[];
}
interface DashboardPendingChange {type: string; value?: string; presentation?: {field: string; before: string; after: string; after_detail?: string}}
interface DashboardPendingPreview {
  total_changes: number; total_photos: number; has_more?: boolean;
  photos: {filename: string; folder: string; changes: DashboardPendingChange[]}[];
}

interface Window {
  escapeHtml(value: unknown): string;
  escapeAttr(value: unknown): string;
  safeFetch<T>(url: string, init?: RequestInit, options?: {toast?: boolean}): Promise<T>;
  showToast?(message: string, kind?: string): void;
}
